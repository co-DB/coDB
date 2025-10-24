use crate::cache::{PageRead, PageWrite};
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::paged_file::PageId;
use crate::slotted_page::{
    PageType, ReprC, SlotId, SlottedPage, SlottedPageBaseHeader, SlottedPageError,
    SlottedPageHeader,
};
use bytemuck::{Pod, Zeroable};
use std::cmp::{Ordering, PartialEq};
use std::marker::PhantomData;
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum BTreeError {
    #[error("internal error from slotted page occurred: {0}")]
    SlottedPageInternalError(#[from] SlottedPageError),
    #[error("error occurred while deserializing: {0}")]
    DeserializationError(#[from] DbSerializationError),
    #[error("corrupt B-tree node detected: {reason}")]
    CorruptNode { reason: String },
}

/// Type aliases for making things a little more readable
pub(crate) type BTreeLeafNode<Page, Key> = BTreeNode<Page, BTreeLeafHeader, Key>;
pub(crate) type BTreeInternalNode<Page, Key> = BTreeNode<Page, BTreeInternalHeader, Key>;

/// Header of internal B-Tree nodes,
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
struct BTreeInternalHeader {
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
struct BTreeLeafHeader {
    base_header: SlottedPageBaseHeader,
    padding: u16,
    /// Pointer to the right sibling leaf node (for range queries)
    next_leaf_pointer: PageId,
}
impl BTreeLeafHeader {
    const NO_NEXT_LEAF: PageId = PageId::MAX;
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
/// Enum representing possible outcomes of a search operation.
pub(crate) enum SearchResult {
    /// Leaf node: exact match found.
    Found { record_ptr: RecordPointer },

    /// Leaf node: key not found, but insertion point identified.
    NotFoundLeaf { insert_slot_id: SlotId },

    /// Internal node: follow this child.
    FollowChild { child_ptr: PageId },
}

// TODO: Use the heap file record ptr
#[derive(Debug)]
/// A struct containing all the information necessary to get the actual record data from a heap file.
/// Stored inside leaf nodes of the b-tree.
pub(crate) struct RecordPointer {
    /// Page id of the heap file.
    page_id: PageId,
    /// Slot inside that page.
    slot_id: SlotId,
}

impl DbSerializable for RecordPointer {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(self.page_id.to_le_bytes());
        buf.extend(self.slot_id.to_le_bytes());
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        todo!()
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (page_id, rest) = PageId::deserialize(buffer)?;
        let (slot_id, rest) = SlotId::deserialize(rest)?;
        let record_ptr = Self { page_id, slot_id };
        Ok((record_ptr, rest))
    }

    fn size_serialized(&self) -> usize {
        todo!()
    }
}

/// Helper trait that every key of a b-tree must implement.
pub(crate) trait BTreeKey: DbSerializable + Ord + Clone {}
impl<T> BTreeKey for T where T: DbSerializable + Ord + Clone {}

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

impl<Page, Header, Key> BTreeNode<Page, Header, Key>
where
    Page: PageRead,
    Header: SlottedPageHeader,
    Key: BTreeKey,
{
    pub fn new(page: Page) -> Result<Self, BTreeError> {
        Ok(Self {
            slotted_page: SlottedPage::new(page)?,
            _key_marker: PhantomData,
        })
    }

    fn get_btree_header(&self) -> Result<&Header, BTreeError> {
        Ok(self.slotted_page.get_header()?)
    }

    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, BTreeError> {
        Ok(self.get_btree_header()?.base())
    }

    /// Gets the key stored in the given slot.
    fn get_key(&self, slot_id: SlotId) -> Result<Key, BTreeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (key, _) = Key::deserialize(record_bytes)?;
        Ok(key)
    }
}

impl<Page, Header, Key> BTreeNode<Page, Header, Key>
where
    Page: PageRead + PageWrite,
    Header: SlottedPageHeader,
    Key: BTreeKey,
{
    fn get_btree_header_mut(&mut self) -> Result<&mut Header, BTreeError> {
        Ok(self.slotted_page.get_header_mut()?)
    }
}

impl<Page, Key> BTreeNode<Page, BTreeInternalHeader, Key>
where
    Page: PageRead,
    Key: BTreeKey,
{
    /// Search returns which child to follow.
    pub fn search(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        let num_slots = self.slotted_page.num_slots()?;

        if num_slots == 0 {
            // Edge case: empty internal node (corruption? root?)
            return Err(BTreeError::CorruptNode {
                reason: "internal node has no slots".into(),
            });
        }

        let mut left = 0;
        let mut right = num_slots - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, child_page_id_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let (child_page_id, _) = PageId::deserialize(child_page_id_bytes)?;
                    return Ok(SearchResult::FollowChild {
                        child_ptr: child_page_id,
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
        let child_ptr = if left == 0 {
            header.leftmost_child_pointer
        } else {
            self.get_child_ptr(left - 1)?
        };

        Ok(SearchResult::FollowChild { child_ptr })
    }

    fn get_child_ptr(&self, slot_id: SlotId) -> Result<PageId, BTreeError> {
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
    pub fn initialize(page: Page) -> Result<Self, BTreeError> {
        let slotted_page = SlottedPage::initialize_default(page)?;
        Ok(Self {
            slotted_page,
            _key_marker: PhantomData,
        })
    }
}
impl<Page, Key> BTreeNode<Page, BTreeLeafHeader, Key>
where
    Page: PageRead,
    Key: BTreeKey,
{
    /// Search either finds record or insert slot.
    pub fn search(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        let num_slots = self.slotted_page.num_slots()?;
        if num_slots == 0 {
            return Ok(SearchResult::NotFoundLeaf { insert_slot_id: 0 });
        }

        let mut left = 0;
        let mut right = num_slots - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, record_ptr_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let (record_ptr, _) = RecordPointer::deserialize(record_ptr_bytes)?;
                    return Ok(SearchResult::Found { record_ptr });
                }
                Ordering::Greater => {
                    if mid == 0 {
                        break;
                    }
                    right = mid - 1;
                }
            }
        }

        Ok(SearchResult::NotFoundLeaf {
            insert_slot_id: left,
        })
    }
}

impl<Page, Key> BTreeNode<Page, BTreeLeafHeader, Key>
where
    Page: PageWrite + PageRead,
    Key: BTreeKey,
{
    pub fn initialize(page: Page, next_leaf: Option<PageId>) -> Result<Self, BTreeError> {
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
}
mod test {
    use super::*;
    use crate::cache::{PageRead, PageWrite};
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

    type LeafNode = BTreeNode<TestPage, BTreeLeafHeader, u32>;
    type InternalNode = BTreeNode<TestPage, BTreeInternalHeader, u32>;

    fn make_leaf_node(keys: &[u32], record_ptrs: &[RecordPointer]) -> LeafNode {
        assert_eq!(keys.len(), record_ptrs.len());

        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        for (k, rec) in keys.iter().zip(record_ptrs.iter()) {
            let mut buf = Vec::new();
            k.serialize(&mut buf);
            rec.serialize(&mut buf);
            node.slotted_page.insert(&buf).unwrap();
        }

        node
    }

    fn make_internal_node(keys: &[u32], child_ptrs: &[PageId]) -> InternalNode {
        assert_eq!(keys.len() + 1, child_ptrs.len());

        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page).unwrap();

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
            SearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 0),
            _ => panic!("expected NotFoundLeaf"),
        }
    }

    #[test]
    fn test_leaf_search_found_and_not_found() {
        let keys = vec![10u32, 20u32, 30u32];
        let recs = vec![
            RecordPointer {
                page_id: 1,
                slot_id: 1,
            },
            RecordPointer {
                page_id: 1,
                slot_id: 2,
            },
            RecordPointer {
                page_id: 2,
                slot_id: 3,
            },
        ];

        let node = make_leaf_node(&keys, &recs);

        for (k, rec) in keys.iter().zip(recs.iter()) {
            let res = node.search(k).unwrap();
            match res {
                SearchResult::Found { record_ptr } => {
                    assert_eq!(record_ptr.page_id, rec.page_id);
                    assert_eq!(record_ptr.slot_id, rec.slot_id);
                }
                _ => panic!("expected Found got {:?}", res),
            }
        }

        let res = node.search(&15).unwrap();
        match res {
            SearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 1),
            _ => panic!("expected NotFoundLeaf"),
        }

        let res = node.search(&5).unwrap();
        match res {
            SearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 0),
            _ => panic!("expected NotFoundLeaf"),
        }

        let res = node.search(&40).unwrap();
        match res {
            SearchResult::NotFoundLeaf { insert_slot_id } => assert_eq!(insert_slot_id, 3),
            _ => panic!("expected NotFoundLeaf"),
        }
    }

    #[test]
    fn test_internal_search_follow_child() {
        let keys = vec![10u32, 20u32, 30u32];
        let children = vec![100, 200, 300, 400];

        let node = make_internal_node(&keys, &children);

        let res = node.search(&5).unwrap();
        match res {
            SearchResult::FollowChild { child_ptr } => assert_eq!(child_ptr, 100),
            _ => panic!("expected FollowChild, got {:?}", res),
        }

        let res = node.search(&10).unwrap();
        match res {
            SearchResult::FollowChild { child_ptr } => assert_eq!(child_ptr, 200),
            _ => panic!("expected FollowChild, got {:?}", res),
        }

        let res = node.search(&15).unwrap();
        match res {
            SearchResult::FollowChild { child_ptr } => assert_eq!(child_ptr, 200),
            _ => panic!("expected FollowChild, got {:?}", res),
        }

        let res = node.search(&35).unwrap();
        match res {
            SearchResult::FollowChild { child_ptr } => assert_eq!(child_ptr, 400),
            _ => panic!("expected FollowChild, got {:?}", res),
        }
    }
}
