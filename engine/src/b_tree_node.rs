use crate::data_types::{DbDate, DbDateTime, DbSerializable, DbSerializationError};
use crate::heap_file::RecordPtr;
use crate::slotted_page::{
    InsertResult, PageType, ReprC, Slot, SlotId, SlottedPage, SlottedPageBaseHeader,
    SlottedPageError, SlottedPageHeader, get_base_header,
};
use bytemuck::{Pod, Zeroable};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::marker::PhantomData;
use storage::cache::{PageRead, PageWrite};
use storage::paged_file::PageId;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum BTreeNodeError {
    #[error("internal error from slotted page occurred: {0}")]
    SlottedPageInternalError(#[from] SlottedPageError),
    #[error("error occurred while deserializing: {0}")]
    DeserializationError(#[from] DbSerializationError),
    #[error("corrupt B-tree node detected: {reason}")]
    CorruptNode { reason: String },
    #[error("tried to insert a duplicate key into an internal node")]
    InternalNodeDuplicateKey,
    #[error("tried to access a page with invalid type: {:?}", page_type)]
    InvalidPageType { page_type: PageType },
    #[error("tried to split a node with less than 2 keys")]
    InvalidSplit,
}

pub(crate) enum NodeType {
    Internal,
    Leaf,
}

pub(crate) enum NodeInsertResult {
    Success,
    PageFull,
    KeyAlreadyExists,
}

/// Type aliases for making things a little more readable
pub(crate) type BTreeLeafNode<Page, Key> = BTreeNode<Page, BTreeLeafHeader, Key>;
pub(crate) type BTreeInternalNode<Page, Key> = BTreeNode<Page, BTreeInternalHeader, Key>;

/// Header of internal B-Tree nodes,
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreeInternalHeader {
    base_header: SlottedPageBaseHeader,
    padding: u16,
    /// Stores the page id of the child node with keys lesser than the smallest one in
    /// this node. We store this here, because a btree node with n keys needs n+1 child
    /// pointers and storing the first pointer here is easier than storing it in a
    /// dedicated slot.
    leftmost_child_pointer: PageId,
}

impl BTreeInternalHeader {
    const NO_LEFTMOST_CHILD_POINTER: PageId = PageId::MAX;
}
impl SlottedPageHeader for BTreeInternalHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base_header
    }
}

unsafe impl ReprC for BTreeInternalHeader {}
impl Default for BTreeInternalHeader {
    fn default() -> Self {
        BTreeInternalHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeInternalHeader>() as u16,
                PageType::BTreeInternal,
            ),
            padding: 0,
            leftmost_child_pointer: Self::NO_LEFTMOST_CHILD_POINTER,
        }
    }
}

/// Header of leaf B-Tree nodes,
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreeLeafHeader {
    base_header: SlottedPageBaseHeader,
    padding: u16,
    /// Pointer to the right sibling leaf node (for range queries)
    next_leaf_pointer: PageId,
}
impl BTreeLeafHeader {
    pub const NO_NEXT_LEAF: PageId = PageId::MAX;
}

impl SlottedPageHeader for BTreeLeafHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base_header
    }
}

unsafe impl ReprC for BTreeLeafHeader {}
impl Default for BTreeLeafHeader {
    fn default() -> Self {
        BTreeLeafHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeLeafHeader>() as u16,
                PageType::BTreeLeaf,
            ),
            padding: 0,
            next_leaf_pointer: Self::NO_NEXT_LEAF,
        }
    }
}

#[derive(Debug)]
/// Enum representing possible outcomes of a search operation in leaf node.
pub(crate) enum LeafNodeSearchResult {
    /// Exact match found.
    Found { record_ptr: RecordPtr },

    /// Key not found, but insertion point identified.
    NotFoundLeaf { insert_slot_id: SlotId },
}

pub(crate) struct InternalNodeSearchResult {
    pub(crate) child_ptr: PageId,
    pub(crate) insert_pos: Option<u16>,
}

/// Helper trait that every key of a b-tree must implement.
pub(crate) trait BTreeKey: DbSerializable + Ord + Clone + Debug {
    const MAX_KEY_SIZE: u16;
}

macro_rules! impl_btree_key {
    ($($t:ty),*) => {
        $(impl BTreeKey for $t {
            const MAX_KEY_SIZE: u16 = (size_of::<$t>() + size_of::<RecordPtr>() + size_of::<Slot>()) as u16;
        })*
    };
}

impl_btree_key!(i32, i64, DbDateTime, DbDate);

impl BTreeKey for String {
    // 512 is the max size of the string itself, but we also need to store its length when serializing.
    const MAX_KEY_SIZE: u16 = (512 + size_of::<u16>()) as u16;
}
/// Struct representing a B-Tree node. It is a wrapper on a slotted page, that uses its api for
/// lower level operations.
pub(crate) struct BTreeNode<Page, Header, Key>
where
    Key: BTreeKey,
    Header: SlottedPageHeader,
{
    /// Slotted page whose operations B-Tree node depends on.
    slotted_page: SlottedPage<Page, Header>,
    /// We need to tie a B-Tree node to specific key type, but since there is no sensible field of
    /// type Key that we can create, we need to use PhantomData just to mark that we are using this
    /// type.
    _key_marker: PhantomData<Key>,
}

/// Gets the type of given page without creating an instance the B-Tree node struct.
pub fn get_node_type<Page: PageRead>(page: Page) -> Result<NodeType, BTreeNodeError> {
    match get_base_header(&page)?.page_type()? {
        PageType::BTreeInternal => Ok(NodeType::Internal),
        PageType::BTreeLeaf => Ok(NodeType::Leaf),
        page_type => Err(BTreeNodeError::InvalidPageType { page_type }),
    }
}

impl<Page, Header, Key> BTreeNode<Page, Header, Key>
where
    Header: SlottedPageHeader,
    Key: BTreeKey,
{
    pub const fn max_insert_size() -> u16 {
        Key::MAX_KEY_SIZE + (size_of::<RecordPtr>() + size_of::<Slot>()) as u16
    }
}

impl<Page, Header, Key> BTreeNode<Page, Header, Key>
where
    Page: PageRead,
    Header: SlottedPageHeader,
    Key: BTreeKey,
{
    pub fn new(page: Page) -> Result<Self, BTreeNodeError> {
        Ok(Self {
            slotted_page: SlottedPage::new(page)?,
            _key_marker: PhantomData,
        })
    }

    fn get_btree_header(&self) -> Result<&Header, BTreeNodeError> {
        Ok(self.slotted_page.get_header()?)
    }

    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, BTreeNodeError> {
        Ok(self.get_btree_header()?.base())
    }

    fn has_deleted_slots(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.get_base_header()?.base().has_free_slot())
    }

    /// Gets the key stored in the given slot.
    fn get_key(&self, slot_id: SlotId) -> Result<Key, BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (key, _) = Key::deserialize(record_bytes)?;
        Ok(key)
    }

    /// Returns whether another key can fit in this node. This assumes the worst-case scenario
    /// for String keys (meaning the key is 512 bytes).
    pub fn can_fit_another(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.slotted_page.free_space()? >= Self::max_insert_size())
    }

    /// Finds a new mid for binary search in the case where the slot in the base mid (left + right / 2)
    /// is deleted. Can return None if every node is deleted.
    fn find_new_mid(&self, mid: u16, left: u16, right: u16) -> Result<Option<u16>, BTreeNodeError> {
        for i in (left..mid).rev() {
            if !self.slotted_page.is_slot_deleted(i)? {
                return Ok(Some(i));
            }
        }

        for i in (mid + 1)..=right {
            if !self.slotted_page.is_slot_deleted(i)? {
                return Ok(Some(i));
            }
        }

        Ok(None)
    }
}

impl<Page, Header, Key> BTreeNode<Page, Header, Key>
where
    Page: PageRead + PageWrite,
    Header: SlottedPageHeader,
    Key: BTreeKey,
{
    fn get_btree_header_mut(&mut self) -> Result<&mut Header, BTreeNodeError> {
        Ok(self.slotted_page.get_header_mut()?)
    }

    pub fn batch_insert(&mut self, insert_values: Vec<Vec<u8>>) -> Result<(), BTreeNodeError> {
        for (index, value) in insert_values.iter().enumerate() {
            self.slotted_page
                .insert_at(value.as_slice(), index as SlotId)?;
        }
        Ok(())
    }

    pub fn split_keys(&mut self) -> Result<(Vec<Vec<u8>>, Key), BTreeNodeError> {
        let valid_records = self
            .slotted_page
            .read_all_records_enumerated()?
            .collect::<Vec<_>>();

        if valid_records.len() < 2 {
            return Err(BTreeNodeError::InvalidSplit);
        }

        let mut current_size = 0;

        let split_position = valid_records
            .iter()
            .position(|(_, record)| {
                current_size += record.len();
                current_size > SlottedPage::<Page, Header>::MAX_FREE_SPACE as usize / 2
            })
            .unwrap_or(valid_records.len() / 2);

        let (_, split_records) = valid_records.split_at(split_position);

        let copied_split_records = split_records
            .iter()
            .map(|(_, record)| record.to_vec())
            .collect::<Vec<_>>();

        let split_indexes = split_records
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();

        for index in split_indexes {
            self.slotted_page.delete(index)?;
        }

        self.slotted_page.compact_slots()?;
        self.slotted_page.compact_records()?;

        let (separator_key, _) = Key::deserialize(copied_split_records.first().unwrap())?;
        Ok((copied_split_records, separator_key))
    }

    fn handle_insert_result(
        &mut self,
        insert_result: InsertResult,
        record_buffer: &[u8],
        position: SlotId,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        match insert_result {
            InsertResult::Success(_) => Ok(NodeInsertResult::Success),
            InsertResult::NeedsDefragmentation => {
                self.slotted_page.compact_records()?;
                let new_response = self.slotted_page.insert_at(record_buffer, position)?;
                match new_response {
                    InsertResult::Success(_) => Ok(NodeInsertResult::Success),
                    _ => unreachable!(),
                }
            }
            InsertResult::PageFull => Ok(NodeInsertResult::PageFull),
        }
    }
}

impl<Page, Key> BTreeNode<Page, BTreeInternalHeader, Key>
where
    Page: PageRead,
    Key: BTreeKey,
{
    /// Search returns which child to follow.
    pub fn search(&self, target_key: &Key) -> Result<InternalNodeSearchResult, BTreeNodeError> {
        let num_slots = self.slotted_page.num_slots()?;

        if num_slots == 0 {
            return Ok(InternalNodeSearchResult {
                child_ptr: self.get_btree_header()?.leftmost_child_pointer,
                insert_pos: Some(0),
            });
        }

        let mut left = 0;
        let mut right = num_slots - 1;

        while left <= right {
            let mut mid = (left + right) / 2;
            if self.slotted_page.is_slot_deleted(mid)? {
                match self.find_new_mid(mid, left, right)? {
                    None => {
                        return Err(BTreeNodeError::CorruptNode {
                            reason: "An internal node must contain at least 1 key".to_string(),
                        });
                    }
                    Some(new_mid) => {
                        mid = new_mid;
                    }
                }
            }
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, child_page_id_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let (child_ptr, _) = PageId::deserialize(child_page_id_bytes)?;
                    return Ok(InternalNodeSearchResult {
                        child_ptr,
                        insert_pos: None,
                    });
                }
                Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                }
            }
        }

        let header = self.get_btree_header()?;
        let (child_ptr, insert_pos) = if left == 0 {
            (header.leftmost_child_pointer, 0)
        } else {
            (self.get_child_ptr(left - 1)?, left)
        };

        Ok(InternalNodeSearchResult {
            child_ptr,
            insert_pos: Some(insert_pos),
        })
    }

    fn get_child_ptr(&self, slot_id: SlotId) -> Result<PageId, BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (_, child_ptr_bytes) = Key::deserialize(record_bytes)?;
        let (child_ptr, _) = PageId::deserialize(child_ptr_bytes)?;
        Ok(child_ptr)
    }
}

impl<Page, Key> BTreeNode<Page, BTreeInternalHeader, Key>
where
    Page: PageWrite + PageRead,
    Key: BTreeKey,
{
    pub fn initialize(page: Page, leftmost_child_pointer: PageId) -> Result<Self, BTreeNodeError> {
        let header = BTreeInternalHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeInternalHeader>() as u16,
                PageType::BTreeInternal,
            ),
            padding: 0,
            leftmost_child_pointer,
        };

        let slotted_page = SlottedPage::initialize_with_header(page, header)?;

        Ok(Self {
            slotted_page,
            _key_marker: PhantomData,
        })
    }

    pub fn insert(
        &mut self,
        key: Key,
        new_child_id: PageId,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let position = match self.search(&key)?.insert_pos {
            None => return Err(BTreeNodeError::InternalNodeDuplicateKey),
            Some(pos) => pos,
        };
        let mut buffer = Vec::new();
        key.serialize(&mut buffer);
        new_child_id.serialize(&mut buffer);
        let insert_result = self.slotted_page.insert_at(&buffer, position)?;
        self.handle_insert_result(insert_result, &buffer, position)
    }

    pub fn set_leftmost_child_id(&mut self, child_id: PageId) -> Result<(), BTreeNodeError> {
        let header = self.get_btree_header_mut()?;
        header.leftmost_child_pointer = child_id;
        Ok(())
    }
}
impl<Page, Key> BTreeNode<Page, BTreeLeafHeader, Key>
where
    Page: PageRead,
    Key: BTreeKey,
{
    /// Search either finds record or insert slot.
    pub fn search(&self, target_key: &Key) -> Result<LeafNodeSearchResult, BTreeNodeError> {
        let num_slots = self.slotted_page.num_slots()?;
        if num_slots == 0 {
            return Ok(LeafNodeSearchResult::NotFoundLeaf { insert_slot_id: 0 });
        }

        let mut left = 0;
        let mut right = num_slots - 1;

        while left <= right {
            let mut mid = (left + right) / 2;
            if self.slotted_page.is_slot_deleted(mid)? {
                match self.find_new_mid(mid, left, right)? {
                    None => {
                        return Ok(LeafNodeSearchResult::NotFoundLeaf {
                            insert_slot_id: left,
                        });
                    }
                    Some(new_mid) => {
                        mid = new_mid;
                    }
                }
            }
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, record_ptr_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let (record_ptr, _) = RecordPtr::deserialize(record_ptr_bytes)?;
                    return Ok(LeafNodeSearchResult::Found { record_ptr });
                }
                Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                }
            }
        }

        Ok(LeafNodeSearchResult::NotFoundLeaf {
            insert_slot_id: left,
        })
    }

    pub fn next_leaf_id(&self) -> Result<Option<PageId>, BTreeNodeError> {
        match self.get_btree_header()?.next_leaf_pointer {
            BTreeLeafHeader::NO_NEXT_LEAF => Ok(None),
            val => Ok(Some(val)),
        }
    }
}

impl<Page, Key> BTreeNode<Page, BTreeLeafHeader, Key>
where
    Page: PageWrite + PageRead,
    Key: BTreeKey,
{
    pub fn initialize(page: Page, next_leaf: Option<PageId>) -> Result<Self, BTreeNodeError> {
        let header = BTreeLeafHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeLeafHeader>() as u16,
                PageType::BTreeLeaf,
            ),
            padding: 0,
            next_leaf_pointer: next_leaf.unwrap_or(BTreeLeafHeader::NO_NEXT_LEAF),
        };
        let slotted_page = SlottedPage::initialize_with_header(page, header)?;
        Ok(Self {
            slotted_page,
            _key_marker: PhantomData,
        })
    }

    pub fn insert(
        &mut self,
        key: Key,
        record_pointer: RecordPtr,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let position = match self.search(&key)? {
            // TODO (For indexes): handle duplicated keys
            LeafNodeSearchResult::Found { .. } => return Ok(NodeInsertResult::KeyAlreadyExists),
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => insert_slot_id,
        };
        let mut buffer = Vec::new();
        key.serialize(&mut buffer);
        record_pointer.serialize(&mut buffer);
        let insert_result = self.slotted_page.insert_at(&buffer, position)?;
        self.handle_insert_result(insert_result, &buffer, position)
    }

    pub fn set_next_leaf_id(&mut self, next_leaf_id: Option<PageId>) -> Result<(), BTreeNodeError> {
        let header = self.get_btree_header_mut()?;
        match next_leaf_id {
            Some(next_leaf) => header.next_leaf_pointer = next_leaf,
            None => header.next_leaf_pointer = BTreeLeafHeader::NO_NEXT_LEAF,
        };
        Ok(())
    }
}
mod test {
    use super::*;
    use crate::slotted_page::InsertResult;

    const PAGE_SIZE: usize = 4096;

    struct TestPage {
        data: Vec<u8>,
    }

    impl TestPage {
        fn new(size: usize) -> Self {
            Self {
                data: vec![0; size],
            }
        }
    }

    impl PageRead for TestPage {
        fn data(&self) -> &[u8] {
            &self.data
        }
    }

    impl PageWrite for TestPage {
        fn data_mut(&mut self) -> &mut [u8] {
            &mut self.data
        }
    }

    type LeafNode = BTreeNode<TestPage, BTreeLeafHeader, i32>;

    type InternalNode = BTreeNode<TestPage, BTreeInternalHeader, i32>;

    fn make_leaf_node(keys: &[i32], record_ptrs: &[RecordPtr]) -> LeafNode {
        assert_eq!(keys.len(), record_ptrs.len());

        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        for (k, rec) in keys.iter().zip(record_ptrs.iter()) {
            let mut buf = Vec::new();
            k.serialize(&mut buf);
            rec.serialize(&mut buf);
            node.slotted_page.insert(&buf).unwrap();
        }

        let mut buf = Vec::new();
        let key = 1;
        key.serialize(&mut buf);
        let insert_result = node.slotted_page.insert(&buf).unwrap();
        match insert_result {
            InsertResult::Success(slot) => node.slotted_page.delete(slot).unwrap(),
            _ => panic!(),
        };
        node
    }

    fn make_internal_node(keys: &[i32], child_ptrs: &[PageId]) -> InternalNode {
        assert_eq!(keys.len() + 1, child_ptrs.len());

        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page, child_ptrs[0]).unwrap();

        let header: &mut BTreeInternalHeader = node.get_btree_header_mut().unwrap();
        header.leftmost_child_pointer = child_ptrs[0];

        for (i, k) in keys.iter().enumerate() {
            let mut buf = Vec::new();
            k.serialize(&mut buf);
            child_ptrs[i + 1].serialize(&mut buf);
            let result = node.slotted_page.insert(&buf).unwrap();
            match result {
                InsertResult::Success(_) => {}
                _ => panic!("insert failed"),
            }
        }

        node
    }

    #[test]
    fn test_leaf_search_empty_node() {
        let page = TestPage::new(PAGE_SIZE);
        let node = LeafNode::initialize(page, None).unwrap();

        let res = node.search(&10).unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 0),
            _ => panic!("expected NotFoundLeaf"),
        }
    }

    #[test]
    fn test_leaf_search_found_and_not_found() {
        let keys = vec![10i32, 20i32, 30i32];
        let recs = vec![
            RecordPtr::new(1, 1),
            RecordPtr::new(1, 2),
            RecordPtr::new(2, 3),
        ];

        let node = make_leaf_node(&keys, &recs);

        for (k, rec) in keys.iter().zip(recs.iter()) {
            let res = node.search(k).unwrap();
            match res {
                LeafNodeSearchResult::Found { record_ptr } => {
                    assert_eq!(record_ptr.page_id, rec.page_id);
                    assert_eq!(record_ptr.slot_id, rec.slot_id);
                }
                _ => panic!("expected Found got {:?}", res),
            }
        }

        let res = node.search(&15).unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 1),
            _ => panic!("expected NotFoundLeaf"),
        }

        let res = node.search(&5).unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 0),
            _ => panic!("expected NotFoundLeaf"),
        }

        let res = node.search(&40).unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 3),
            _ => panic!("expected NotFoundLeaf"),
        }
    }

    #[test]
    fn test_internal_search_follow_child() {
        let keys = vec![10i32, 20i32, 30i32];
        let children = vec![100, 200, 300, 400];

        let node = make_internal_node(&keys, &children);

        let res = node.search(&5).unwrap();
        assert_eq!(res.child_ptr, 100);

        let res = node.search(&10).unwrap();
        assert_eq!(res.child_ptr, 200);

        let res = node.search(&15).unwrap();
        assert_eq!(res.child_ptr, 200);

        let res = node.search(&35).unwrap();
        assert_eq!(res.child_ptr, 400);
    }
}
