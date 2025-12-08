use crate::heap_file::RecordPtr;
use crate::slotted_page::{
    InsertResult, PageType, ReprC, SlotId, SlottedPage, SlottedPageBaseHeader, SlottedPageError,
    SlottedPageHeader, get_base_header,
};
use bytemuck::{Pod, Zeroable};
use std::cmp::Ordering;
use storage::cache::{PageRead, PageWrite};
use storage::paged_file::PageId;
use thiserror::Error;
use types::serialization::{DbSerializable, DbSerializationError};

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

pub(crate) enum NodeDeleteResult {
    Success,
    SuccessUnderflow,
    KeyDoesNotExist,
}

/// Represents a child pointer position in an internal B-tree node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildPosition {
    /// The leftmost child pointer, stored in the node header.
    Leftmost,
    /// A child pointer stored in a slot.
    AfterSlot(SlotId),
}

impl ChildPosition {
    /// Returns the slot ID if this is an AfterSlot position, None for Leftmost.
    pub fn slot_id(&self) -> Option<SlotId> {
        match self {
            ChildPosition::Leftmost => None,
            ChildPosition::AfterSlot(slot_id) => Some(*slot_id),
        }
    }
}

/// Type aliases for making things a little more readable
pub(crate) type BTreeLeafNode<Page> = BTreeNode<Page, BTreeLeafHeader>;
pub(crate) type BTreeInternalNode<Page> = BTreeNode<Page, BTreeInternalHeader>;

/// Header of internal B-Tree nodes,
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreeInternalHeader {
    base_header: SlottedPageBaseHeader,
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
            leftmost_child_pointer: Self::NO_LEFTMOST_CHILD_POINTER,
        }
    }
}

/// Header of leaf B-Tree nodes,
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreeLeafHeader {
    base_header: SlottedPageBaseHeader,
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
            next_leaf_pointer: Self::NO_NEXT_LEAF,
        }
    }
}

#[derive(Debug)]
/// Enum representing possible outcomes of a search operation in leaf node.
pub(crate) enum LeafNodeSearchResult {
    /// Exact match found.
    Found {
        position: u16,
        record_ptr: RecordPtr,
    },

    /// Key not found, but insertion point identified.
    NotFoundLeaf { insert_slot_id: SlotId },
}

pub(crate) enum InternalNodeSearchResult {
    /// Key found exactly
    FoundExact {
        child_ptr: PageId,
        /// The position of the child pointer that was followed.
        child_pos: ChildPosition,
    },

    /// Key not found, followed a child pointer during search
    NotFoundInternal {
        child_ptr: PageId,
        /// The position of the child pointer that was followed.
        child_pos: ChildPosition,
    },
}

impl InternalNodeSearchResult {
    pub(crate) fn child_ptr(&self) -> PageId {
        match self {
            InternalNodeSearchResult::FoundExact { child_ptr, .. } => *child_ptr,
            InternalNodeSearchResult::NotFoundInternal { child_ptr, .. } => *child_ptr,
        }
    }

    pub(crate) fn child_pos(&self) -> ChildPosition {
        match self {
            InternalNodeSearchResult::FoundExact { child_pos, .. } => *child_pos,
            InternalNodeSearchResult::NotFoundInternal { child_pos, .. } => *child_pos,
        }
    }
}

/// Struct representing a B-Tree node. It is a wrapper on a slotted page, that uses its api for
/// lower level operations.
pub(crate) struct BTreeNode<Page, Header>
where
    Header: SlottedPageHeader,
{
    /// Slotted page whose operations B-Tree node depends on.
    slotted_page: SlottedPage<Page, Header>,
}

/// Gets the type of given page without creating an instance the B-Tree node struct.
pub fn get_node_type<Page: PageRead>(page: Page) -> Result<NodeType, BTreeNodeError> {
    match get_base_header(&page)?.page_type()? {
        PageType::BTreeInternal => Ok(NodeType::Internal),
        PageType::BTreeLeaf => Ok(NodeType::Leaf),
        page_type => Err(BTreeNodeError::InvalidPageType { page_type }),
    }
}

impl<Page, Header> BTreeNode<Page, Header>
where
    Page: PageRead,
    Header: SlottedPageHeader,
{
    /// Boundary at which we consider the node to need merging/borrowing keys from sibling.
    const UNDERFLOW_BOUNDARY: f32 = 0.33;

    pub fn new(page: Page) -> Result<Self, BTreeNodeError> {
        Ok(Self {
            slotted_page: SlottedPage::new(page)?,
        })
    }

    pub fn can_fit_another(&self) -> Result<bool, BTreeNodeError> {
        // TODO: Figure out how to do this.
        Ok(self.slotted_page.free_space()? > 512)
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

    pub(crate) fn get_all_records(&self) -> Result<Vec<&[u8]>, BTreeNodeError> {
        Ok(self.slotted_page.read_all_records()?.collect())
    }

    pub(crate) fn is_underflow(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.slotted_page.fraction_filled()? > Self::UNDERFLOW_BOUNDARY)
    }

    pub(crate) fn num_keys(&self) -> Result<u16, BTreeNodeError> {
        Ok(self.slotted_page.num_used_slots()?)
    }
}

impl<Page, Header> BTreeNode<Page, Header>
where
    Page: PageRead + PageWrite,
    Header: SlottedPageHeader,
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

    pub(crate) fn delete_at(&mut self, slot_id: SlotId) -> Result<(), BTreeNodeError> {
        Ok(self.slotted_page.delete(slot_id)?)
    }
}

impl<Page> BTreeNode<Page, BTreeInternalHeader>
where
    Page: PageRead,
{
    /// Returns the number of children in this internal node (num_keys + 1).
    pub fn num_children(&self) -> Result<u16, BTreeNodeError> {
        Ok(self.num_keys()? + 1)
    }

    /// Gets the key stored in the given slot.
    pub(crate) fn get_key(&self, slot_id: SlotId) -> Result<&[u8], BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let key_bytes_end = record_bytes.len() - size_of::<PageId>();
        let key_bytes = &record_bytes[..key_bytes_end];
        Ok(key_bytes)
    }

    /// Gets the child pointer stored in the given slot
    fn get_child_ptr(&self, slot_id: SlotId) -> Result<PageId, BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let key_bytes_end = record_bytes.len() - size_of::<PageId>();
        let (child_ptr, _) = PageId::deserialize(&record_bytes[key_bytes_end..])?;
        Ok(child_ptr)
    }

    /// Gets the child pointer at the given position.
    pub fn get_child_ptr_at(&self, pos: ChildPosition) -> Result<PageId, BTreeNodeError> {
        match pos {
            ChildPosition::Leftmost => Ok(self.get_btree_header()?.leftmost_child_pointer),
            ChildPosition::AfterSlot(slot_id) => self.get_child_ptr(slot_id),
        }
    }

    /// Returns the child position immediately preceding the given position,
    /// skipping any deleted slots.
    pub(crate) fn get_preceding_child_pos(
        &self,
        pos: ChildPosition,
    ) -> Result<Option<ChildPosition>, BTreeNodeError> {
        match pos {
            ChildPosition::Leftmost => Ok(None),
            ChildPosition::AfterSlot(slot_id) => {
                // Find the first non-deleted slot before this one
                let preceding_slot = (0..slot_id)
                    .rev()
                    .find(|&s| !self.slotted_page.is_slot_deleted(s).unwrap_or(true));

                match preceding_slot {
                    Some(s) => Ok(Some(ChildPosition::AfterSlot(s))),
                    None => Ok(Some(ChildPosition::Leftmost)),
                }
            }
        }
    }

    /// Returns the child position immediately following the given position,
    /// skipping any deleted slots.
    pub fn get_next_child_pos(
        &self,
        pos: ChildPosition,
    ) -> Result<Option<ChildPosition>, BTreeNodeError> {
        let start_slot = match pos {
            ChildPosition::Leftmost => 0,
            ChildPosition::AfterSlot(slot_id) => slot_id + 1,
        };

        let next_slot = (start_slot..self.slotted_page.num_slots()?)
            .find(|&s| !self.slotted_page.is_slot_deleted(s).unwrap_or(true));

        Ok(next_slot.map(ChildPosition::AfterSlot))
    }

    /// Search returns which child to follow.
    pub fn search(&self, target_key: &[u8]) -> Result<InternalNodeSearchResult, BTreeNodeError> {
        let num_slots = self.slotted_page.num_slots()?;

        if num_slots == 0 {
            return Ok(InternalNodeSearchResult::NotFoundInternal {
                child_ptr: self.get_btree_header()?.leftmost_child_pointer,
                child_pos: ChildPosition::Leftmost,
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
            let key = self.get_key(mid)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let child_ptr = self.get_child_ptr(mid)?;
                    return Ok(InternalNodeSearchResult::FoundExact {
                        child_ptr,
                        child_pos: ChildPosition::AfterSlot(mid),
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
        let (child_ptr, child_pos) = if left == 0 {
            (header.leftmost_child_pointer, ChildPosition::Leftmost)
        } else {
            (
                self.get_child_ptr(left - 1)?,
                ChildPosition::AfterSlot(left - 1),
            )
        };

        Ok(InternalNodeSearchResult::NotFoundInternal {
            child_ptr,
            child_pos,
        })
    }

    pub(crate) fn will_not_underflow_after_delete(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.slotted_page.fraction_filled()? > Self::UNDERFLOW_BOUNDARY)
    }

    /// Check if this internal node can spare a key for redistribution to a sibling.
    pub(crate) fn can_redistribute_key(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.slotted_page.fraction_filled()? > Self::UNDERFLOW_BOUNDARY + 0.05)
    }

    /// Returns the first (smallest) key in this internal node without removing it.
    pub(crate) fn get_first_key(&self) -> Result<Vec<u8>, BTreeNodeError> {
        let (_, record) = self.slotted_page.read_first_non_deleted_record()?;
        let key_bytes_end = record.len() - size_of::<PageId>();
        Ok(record[..key_bytes_end].to_vec())
    }

    /// Returns the last (largest) key in this internal node without removing it.
    pub(crate) fn get_last_key(&self) -> Result<Vec<u8>, BTreeNodeError> {
        let (_, record) = self.slotted_page.read_last_non_deleted_record()?;
        let key_bytes_end = record.len() - size_of::<PageId>();
        Ok(record[..key_bytes_end].to_vec())
    }

    /// Returns the last (largest) key and its child pointer without removing.
    pub(crate) fn get_last_key_and_child(&self) -> Result<(Vec<u8>, PageId), BTreeNodeError> {
        let (_, record) = self.slotted_page.read_last_non_deleted_record()?;
        let key_bytes_end = record.len() - size_of::<PageId>();
        let key = record[..key_bytes_end].to_vec();
        let (child_ptr, _) = PageId::deserialize(&record[key_bytes_end..])?;
        Ok((key, child_ptr))
    }
}

impl<Page> BTreeNode<Page, BTreeInternalHeader>
where
    Page: PageWrite + PageRead,
{
    pub fn initialize(page: Page, leftmost_child_pointer: PageId) -> Result<Self, BTreeNodeError> {
        let header = BTreeInternalHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeInternalHeader>() as u16,
                PageType::BTreeInternal,
            ),
            leftmost_child_pointer,
        };

        let slotted_page = SlottedPage::initialize_with_header(page, header)?;

        Ok(Self { slotted_page })
    }

    pub fn insert(
        &mut self,
        key: &[u8],
        new_child_id: PageId,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let position = match self.search(key)? {
            InternalNodeSearchResult::FoundExact { .. } => {
                return Err(BTreeNodeError::InternalNodeDuplicateKey);
            }
            InternalNodeSearchResult::NotFoundInternal { child_pos, .. } => {
                // Convert child position to slot index for insertion.
                // When inserting after Leftmost, we insert at slot 0.
                // When inserting after slot N, we insert at slot N+1.
                match child_pos {
                    ChildPosition::Leftmost => 0,
                    ChildPosition::AfterSlot(slot_id) => slot_id + 1,
                }
            }
        };
        let mut buffer = Vec::new();
        buffer.extend_from_slice(key);
        new_child_id.serialize(&mut buffer);
        let insert_result = self.slotted_page.insert_at(&buffer, position)?;
        self.handle_insert_result(insert_result, &buffer, position)
    }

    pub fn set_leftmost_child_id(&mut self, child_id: PageId) -> Result<(), BTreeNodeError> {
        let header = self.get_btree_header_mut()?;
        header.leftmost_child_pointer = child_id;
        Ok(())
    }

    /// Updates the separator key at the given slot index while preserving the child pointer.
    /// This is used during redistribution to update the separator between siblings.
    pub fn update_separator_at_slot(
        &mut self,
        slot_id: SlotId,
        new_key: &[u8],
    ) -> Result<(), BTreeNodeError> {
        let child_ptr = self.get_child_ptr(slot_id)?;

        self.slotted_page.delete(slot_id)?;

        let mut buffer = Vec::new();
        buffer.extend_from_slice(new_key);
        child_ptr.serialize(&mut buffer);

        let result = self.slotted_page.insert_at(&buffer, slot_id)?;
        self.handle_insert_result(result, &buffer, slot_id)?;

        Ok(())
    }

    pub fn split_keys(&mut self) -> Result<(Vec<Vec<u8>>, Vec<u8>), BTreeNodeError> {
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
                current_size > SlottedPage::<Page, BTreeInternalHeader>::MAX_FREE_SPACE as usize / 2
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

        // For internal nodes, extract only the key portion (exclude PageId at the end)
        let first_record = copied_split_records.first().unwrap();
        let key_bytes_end = first_record.len() - size_of::<PageId>();
        let separator_key = first_record[..key_bytes_end].to_vec();

        Ok((copied_split_records, separator_key))
    }

    /// Removes the first key from this node.
    /// Returns the (key, old_leftmost_child).
    /// Updates the leftmost child pointer to be the child pointer from the removed slot.
    pub(crate) fn remove_first_key(&mut self) -> Result<(Vec<u8>, PageId), BTreeNodeError> {
        let (slot_id, record) = self.slotted_page.read_first_non_deleted_record()?;
        let key_bytes_end = record.len() - size_of::<PageId>();
        let key = record[..key_bytes_end].to_vec();
        let (new_leftmost, _) = PageId::deserialize(&record[key_bytes_end..])?;

        let old_leftmost = self.get_btree_header()?.leftmost_child_pointer;

        self.slotted_page.delete(slot_id)?;
        self.get_btree_header_mut()?.leftmost_child_pointer = new_leftmost;

        Ok((key, old_leftmost))
    }

    /// Removes the last key from this node.
    /// Returns the (key, child_ptr) of the removed entry.
    pub(crate) fn remove_last_key(&mut self) -> Result<(Vec<u8>, PageId), BTreeNodeError> {
        let (slot_id, record) = self.slotted_page.read_last_non_deleted_record()?;
        let key_bytes_end = record.len() - size_of::<PageId>();
        let key = record[..key_bytes_end].to_vec();
        let (child_ptr, _) = PageId::deserialize(&record[key_bytes_end..])?;

        self.slotted_page.delete(slot_id)?;

        Ok((key, child_ptr))
    }

    /// Inserts a key at the beginning of this node with a new leftmost child.
    /// The old leftmost child becomes the child pointer for the new key.
    pub(crate) fn insert_first_with_new_leftmost(
        &mut self,
        key: &[u8],
        new_leftmost: PageId,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let old_leftmost = self.get_btree_header()?.leftmost_child_pointer;
        self.get_btree_header_mut()?.leftmost_child_pointer = new_leftmost;

        // Insert the key with old_leftmost as its child pointer
        // Since this key should be smaller than all existing keys, it goes at position 0
        let mut buffer = Vec::new();
        buffer.extend_from_slice(key);
        old_leftmost.serialize(&mut buffer);

        let insert_result = self.slotted_page.insert_at(&buffer, 0)?;
        self.handle_insert_result(insert_result, &buffer, 0)
    }
}
impl<Page> BTreeNode<Page, BTreeLeafHeader>
where
    Page: PageRead,
{
    /// Gets the key stored in the given slot.
    fn get_key(&self, slot_id: SlotId) -> Result<&[u8], BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        // RecordPtr is serialized as PageId + SlotId (no padding)
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = record_bytes.len() - serialized_record_ptr_size;
        let key_bytes = &record_bytes[..key_bytes_end];
        Ok(key_bytes)
    }

    fn get_record_ptr(&self, slot_id: SlotId) -> Result<RecordPtr, BTreeNodeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        // RecordPtr is serialized as PageId + SlotId (no padding)
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = record_bytes.len() - serialized_record_ptr_size;
        let (record_ptr, _) = RecordPtr::deserialize(&record_bytes[key_bytes_end..])?;
        Ok(record_ptr)
    }

    /// Search either finds record or insert slot.
    pub fn search(&self, target_key: &[u8]) -> Result<LeafNodeSearchResult, BTreeNodeError> {
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

            let key = self.get_key(mid)?;

            match key.cmp(target_key) {
                Ordering::Less => left = mid + 1,
                Ordering::Equal => {
                    let record_ptr = self.get_record_ptr(mid)?;
                    return Ok(LeafNodeSearchResult::Found {
                        position: mid,
                        record_ptr,
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

    pub(crate) fn can_redistribute_key(&self) -> Result<bool, BTreeNodeError> {
        Ok(self.slotted_page.fraction_filled()? > Self::UNDERFLOW_BOUNDARY + 0.05)
    }

    /// Returns the first (smallest) key in this leaf node without removing it.
    pub fn get_first_key(&self) -> Result<Vec<u8>, BTreeNodeError> {
        let (_, record) = self.slotted_page.read_first_non_deleted_record()?;
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = record.len() - serialized_record_ptr_size;
        Ok(record[..key_bytes_end].to_vec())
    }
}

impl<Page> BTreeNode<Page, BTreeLeafHeader>
where
    Page: PageWrite + PageRead,
{
    pub fn initialize(page: Page, next_leaf: Option<PageId>) -> Result<Self, BTreeNodeError> {
        let header = BTreeLeafHeader {
            base_header: SlottedPageBaseHeader::new(
                size_of::<BTreeLeafHeader>() as u16,
                PageType::BTreeLeaf,
            ),
            next_leaf_pointer: next_leaf.unwrap_or(BTreeLeafHeader::NO_NEXT_LEAF),
        };
        let slotted_page = SlottedPage::initialize_with_header(page, header)?;
        Ok(Self { slotted_page })
    }

    pub(crate) fn insert(
        &mut self,
        key: &[u8],
        record_pointer: RecordPtr,
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let position = match self.search(key)? {
            LeafNodeSearchResult::Found { .. } => return Ok(NodeInsertResult::KeyAlreadyExists),
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => insert_slot_id,
        };
        let mut buffer = Vec::new();
        buffer.extend_from_slice(key);
        record_pointer.serialize(&mut buffer);
        let insert_result = self.slotted_page.insert_at(&buffer, position)?;
        self.handle_insert_result(insert_result, &buffer, position)
    }

    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<NodeDeleteResult, BTreeNodeError> {
        let position = match self.search(key)? {
            LeafNodeSearchResult::Found { position, .. } => position,
            LeafNodeSearchResult::NotFoundLeaf { .. } => {
                return Ok(NodeDeleteResult::KeyDoesNotExist);
            }
        };

        self.slotted_page.delete(position)?;

        if self.slotted_page.fraction_filled()? > Self::UNDERFLOW_BOUNDARY {
            Ok(NodeDeleteResult::Success)
        } else {
            Ok(NodeDeleteResult::SuccessUnderflow)
        }
    }

    pub(crate) fn set_next_leaf_id(
        &mut self,
        next_leaf_id: Option<PageId>,
    ) -> Result<(), BTreeNodeError> {
        let header = self.get_btree_header_mut()?;
        match next_leaf_id {
            Some(next_leaf) => header.next_leaf_pointer = next_leaf,
            None => header.next_leaf_pointer = BTreeLeafHeader::NO_NEXT_LEAF,
        };
        Ok(())
    }

    pub fn split_keys(&mut self) -> Result<(Vec<Vec<u8>>, Vec<u8>), BTreeNodeError> {
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
                current_size > SlottedPage::<Page, BTreeLeafHeader>::MAX_FREE_SPACE as usize / 2
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

        // For leaf nodes, extract only the key portion (exclude RecordPtr at the end)
        let first_record = copied_split_records.first().unwrap();
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = first_record.len() - serialized_record_ptr_size;
        let separator_key = first_record[..key_bytes_end].to_vec();

        Ok((copied_split_records, separator_key))
    }

    pub(crate) fn remove_last_key(&mut self) -> Result<Vec<u8>, BTreeNodeError> {
        let (id, record) = self.slotted_page.read_last_non_deleted_record()?;
        let record = record.to_vec();
        self.slotted_page.delete(id)?;
        Ok(record)
    }

    pub(crate) fn remove_first_key(&mut self) -> Result<Vec<u8>, BTreeNodeError> {
        let (id, record) = self.slotted_page.read_first_non_deleted_record()?;
        let record = record.to_vec();
        self.slotted_page.delete(id)?;
        Ok(record)
    }

    /// Inserts a record that is not separated into key and record pointer.
    pub(crate) fn insert_record(
        &mut self,
        record: &[u8],
    ) -> Result<NodeInsertResult, BTreeNodeError> {
        let serialized_record_ptr_size = size_of::<PageId>() + size_of::<SlotId>();
        let key_bytes_end = record.len() - serialized_record_ptr_size;
        let (key, _) = record.split_at(key_bytes_end);
        let position = match self.search(key)? {
            LeafNodeSearchResult::Found { .. } => return Ok(NodeInsertResult::KeyAlreadyExists),
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => insert_slot_id,
        };
        self.slotted_page.insert_at(record, position)?;
        Ok(NodeInsertResult::Success)
    }
}
#[cfg(test)]
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

    type LeafNode = BTreeNode<TestPage, BTreeLeafHeader>;
    type InternalNode = BTreeNode<TestPage, BTreeInternalHeader>;

    /// Serializes an i32 to a lexicographically comparable byte representation.
    /// This allows proper ordering when comparing keys as byte slices.
    fn serialize_i32_lexicographically(value: i32) -> Vec<u8> {
        let transformed_value = (value as u32) ^ 0x80000000;
        Vec::from(transformed_value.to_be_bytes())
    }

    /// Creates a leaf node with the given keys and record pointers.
    /// Keys are inserted directly into the slotted page (not using the insert method).
    fn make_leaf_node(keys: &[i32], record_ptrs: &[RecordPtr]) -> LeafNode {
        assert_eq!(keys.len(), record_ptrs.len());

        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        for (k, rec) in keys.iter().zip(record_ptrs.iter()) {
            let mut buf = Vec::new();
            buf.extend_from_slice(serialize_i32_lexicographically(*k).as_slice());
            rec.serialize(&mut buf);
            match node.slotted_page.insert(&buf).unwrap() {
                InsertResult::Success(_) => {}
                _ => panic!("Failed to insert key {} into leaf node", k),
            };
        }

        node
    }

    /// Creates an internal node with the given keys and child pointers.
    /// Note: child_ptrs.len() should be keys.len() + 1
    fn make_internal_node(keys: &[i32], child_ptrs: &[PageId]) -> InternalNode {
        assert_eq!(
            keys.len() + 1,
            child_ptrs.len(),
            "Internal node must have n+1 child pointers for n keys"
        );

        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page, child_ptrs[0]).unwrap();

        for (i, k) in keys.iter().enumerate() {
            let mut buf = Vec::new();
            buf.extend_from_slice(serialize_i32_lexicographically(*k).as_slice());
            child_ptrs[i + 1].serialize(&mut buf);
            match node.slotted_page.insert(&buf).unwrap() {
                InsertResult::Success(_) => {}
                _ => panic!("Failed to insert key {} into internal node", k),
            };
        }

        node
    }

    #[test]
    fn test_leaf_search_empty_node() {
        let page = TestPage::new(PAGE_SIZE);
        let node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let res = node.search(key.as_slice()).unwrap();

        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => {
                assert_eq!(
                    insert_slot_id, 0,
                    "Empty node should return insert position 0"
                );
            }
            _ => panic!("Expected NotFoundLeaf for empty node"),
        }
    }

    #[test]
    fn test_leaf_search_found() {
        let keys = vec![10i32, 20i32, 30i32, 40i32];
        let recs = vec![
            RecordPtr::new(1, 1),
            RecordPtr::new(1, 2),
            RecordPtr::new(2, 3),
            RecordPtr::new(3, 4),
        ];

        let node = make_leaf_node(&keys, &recs);
        for (k, rec) in keys.iter().zip(recs.iter()) {
            let res = node
                .search(serialize_i32_lexicographically(*k).as_slice())
                .unwrap();
            match res {
                LeafNodeSearchResult::Found { record_ptr, .. } => {
                    assert_eq!(record_ptr.page_id, rec.page_id);
                    assert_eq!(record_ptr.slot_id, rec.slot_id);
                }
                _ => panic!("Expected to find key {}", k),
            }
        }
    }

    #[test]
    fn test_leaf_search_not_found() {
        let keys = vec![10i32, 20i32, 30i32];
        let recs = vec![
            RecordPtr::new(1, 1),
            RecordPtr::new(1, 2),
            RecordPtr::new(2, 3),
        ];

        let node = make_leaf_node(&keys, &recs);

        let res = node
            .search(serialize_i32_lexicographically(5).as_slice())
            .unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => {
                assert_eq!(insert_slot_id, 0, "Key 5 should insert at position 0");
            }
            _ => panic!("Expected NotFoundLeaf"),
        }

        let res = node
            .search(serialize_i32_lexicographically(15).as_slice())
            .unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => {
                assert_eq!(insert_slot_id, 1, "Key 15 should insert at position 1");
            }
            _ => panic!("Expected NotFoundLeaf"),
        }

        let res = node
            .search(serialize_i32_lexicographically(40).as_slice())
            .unwrap();
        match res {
            LeafNodeSearchResult::NotFoundLeaf { insert_slot_id } => {
                assert_eq!(insert_slot_id, 3, "Key 40 should insert at position 3");
            }
            _ => panic!("Expected NotFoundLeaf"),
        }
    }

    #[test]
    fn test_internal_search_empty_node() {
        let page = TestPage::new(PAGE_SIZE);
        let node = InternalNode::initialize(page, 100).unwrap();

        let key = serialize_i32_lexicographically(10);
        let res = node.search(key.as_slice()).unwrap();

        match res {
            InternalNodeSearchResult::NotFoundInternal {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(
                    child_ptr, 100,
                    "Empty internal node should return leftmost child"
                );
                assert_eq!(child_pos, ChildPosition::Leftmost);
            }
            _ => panic!("Expected NotFoundInternal for empty node"),
        }
    }

    #[test]
    fn test_internal_search_routing() {
        let keys = vec![10i32, 20i32, 30i32];
        let children = vec![100, 200, 300, 400];

        let node = make_internal_node(&keys, &children);

        // Test key < smallest key (should go to leftmost child)
        match node
            .search(serialize_i32_lexicographically(5).as_slice())
            .unwrap()
        {
            InternalNodeSearchResult::NotFoundInternal {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(child_ptr, 100, "Key 5 should route to leftmost child");
                assert_eq!(child_pos, ChildPosition::Leftmost);
            }
            _ => panic!("Expected NotFoundInternal"),
        }

        // Test exact match on key 10
        match node
            .search(serialize_i32_lexicographically(10).as_slice())
            .unwrap()
        {
            InternalNodeSearchResult::FoundExact {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(child_ptr, 200, "Key 10 should route to its child pointer");
                assert_eq!(child_pos, ChildPosition::AfterSlot(0));
            }
            _ => panic!("Expected FoundExact"),
        }

        // Test key between 10 and 20
        match node
            .search(serialize_i32_lexicographically(15).as_slice())
            .unwrap()
        {
            InternalNodeSearchResult::NotFoundInternal {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(child_ptr, 200, "Key 15 should route to child after 10");
                assert_eq!(child_pos, ChildPosition::AfterSlot(0));
            }
            _ => panic!("Expected NotFoundInternal"),
        }

        // Test key between 20 and 30
        match node
            .search(serialize_i32_lexicographically(25).as_slice())
            .unwrap()
        {
            InternalNodeSearchResult::NotFoundInternal {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(child_ptr, 300, "Key 25 should route to child after 20");
                assert_eq!(child_pos, ChildPosition::AfterSlot(1));
            }
            _ => panic!("Expected NotFoundInternal"),
        }

        // Test key > largest key (should go to rightmost child)
        match node
            .search(serialize_i32_lexicographically(35).as_slice())
            .unwrap()
        {
            InternalNodeSearchResult::NotFoundInternal {
                child_ptr,
                child_pos,
            } => {
                assert_eq!(child_ptr, 400, "Key 35 should route to rightmost child");
                assert_eq!(child_pos, ChildPosition::AfterSlot(2));
            }
            _ => panic!("Expected NotFoundInternal"),
        }
    }

    #[test]
    fn test_leaf_insert_into_empty() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let rec = RecordPtr::new(1, 1);

        let result = node.insert(&key, rec).unwrap();
        assert!(matches!(result, NodeInsertResult::Success));

        let search_result = node.search(&key).unwrap();
        match search_result {
            LeafNodeSearchResult::Found { record_ptr, .. } => {
                assert_eq!(record_ptr, rec);
            }
            _ => panic!("Expected to find inserted key"),
        }
    }

    #[test]
    fn test_leaf_insert_multiple_ordered() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let keys_and_recs = vec![
            (10, RecordPtr::new(1, 1)),
            (20, RecordPtr::new(1, 2)),
            (30, RecordPtr::new(2, 3)),
            (40, RecordPtr::new(2, 4)),
        ];

        for (key_val, rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let result = node.insert(&key, *rec).unwrap();
            assert!(matches!(result, NodeInsertResult::Success));
        }

        for (key_val, expected_rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            match search_result {
                LeafNodeSearchResult::Found { record_ptr, .. } => {
                    assert_eq!(record_ptr, *expected_rec);
                }
                _ => panic!("Expected to find key {}", key_val),
            }
        }
    }

    #[test]
    fn test_leaf_insert_multiple_unordered() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let keys_and_recs = vec![
            (30, RecordPtr::new(2, 3)),
            (10, RecordPtr::new(1, 1)),
            (40, RecordPtr::new(2, 4)),
            (20, RecordPtr::new(1, 2)),
        ];

        for (key_val, rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let result = node.insert(&key, *rec).unwrap();
            assert!(matches!(result, NodeInsertResult::Success));
        }

        for (key_val, expected_rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            match search_result {
                LeafNodeSearchResult::Found { record_ptr, .. } => {
                    assert_eq!(record_ptr, *expected_rec);
                }
                _ => panic!("Expected to find key {}", key_val),
            }
        }
    }

    #[test]
    fn test_leaf_insert_duplicate_key() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let rec1 = RecordPtr::new(1, 1);
        let rec2 = RecordPtr::new(2, 2);

        let result = node.insert(&key, rec1).unwrap();
        assert!(matches!(result, NodeInsertResult::Success));

        let result = node.insert(&key, rec2).unwrap();
        assert!(matches!(result, NodeInsertResult::KeyAlreadyExists));

        let search_result = node.search(&key).unwrap();
        match search_result {
            LeafNodeSearchResult::Found { record_ptr, .. } => {
                assert_eq!(record_ptr, rec1);
            }
            _ => panic!("Expected to find original key"),
        }
    }

    #[test]
    fn test_internal_insert_into_empty() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page, 100).unwrap();

        let key = serialize_i32_lexicographically(10);
        let child_id = 200;

        let result = node.insert(&key, child_id).unwrap();
        assert!(matches!(result, NodeInsertResult::Success));

        let search_result = node.search(&key).unwrap();
        match search_result {
            InternalNodeSearchResult::FoundExact { child_ptr, .. } => {
                assert_eq!(child_ptr, child_id);
            }
            _ => panic!("Expected FoundExact after insert"),
        }
    }

    #[test]
    fn test_internal_insert_multiple() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page, 100).unwrap();

        let keys_and_children = vec![(10, 200), (20, 300), (30, 400)];

        for (key_val, child) in &keys_and_children {
            let key = serialize_i32_lexicographically(*key_val);
            let result = node.insert(&key, *child).unwrap();
            assert!(matches!(result, NodeInsertResult::Success));
        }

        for (key_val, expected_child) in &keys_and_children {
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            match search_result {
                InternalNodeSearchResult::FoundExact { child_ptr, .. } => {
                    assert_eq!(child_ptr, *expected_child);
                }
                _ => panic!("Expected FoundExact for key {}", key_val),
            }
        }
    }

    #[test]
    fn test_leaf_next_pointer_none() {
        let page = TestPage::new(PAGE_SIZE);
        let node = LeafNode::initialize(page, None).unwrap();

        let next_id = node.next_leaf_id().unwrap();
        assert_eq!(next_id, None);
    }

    #[test]
    fn test_leaf_next_pointer_some() {
        let page = TestPage::new(PAGE_SIZE);
        let node = LeafNode::initialize(page, Some(42)).unwrap();

        let next_id = node.next_leaf_id().unwrap();
        assert_eq!(next_id, Some(42));
    }

    #[test]
    fn test_leaf_set_next_pointer() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        assert_eq!(node.next_leaf_id().unwrap(), None);

        node.set_next_leaf_id(Some(99)).unwrap();
        assert_eq!(node.next_leaf_id().unwrap(), Some(99));

        node.set_next_leaf_id(None).unwrap();
        assert_eq!(node.next_leaf_id().unwrap(), None);
    }

    #[test]
    fn test_internal_leftmost_child() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = InternalNode::initialize(page, 100).unwrap();

        let header = node.get_btree_header().unwrap();
        assert_eq!(header.leftmost_child_pointer, 100);

        node.set_leftmost_child_id(200).unwrap();
        let header = node.get_btree_header().unwrap();
        assert_eq!(header.leftmost_child_pointer, 200);
    }

    #[test]
    fn test_batch_insert_leaf() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let records = vec![
            {
                let mut buf = Vec::new();
                buf.extend_from_slice(&serialize_i32_lexicographically(10));
                RecordPtr::new(1, 1).serialize(&mut buf);
                buf
            },
            {
                let mut buf = Vec::new();
                buf.extend_from_slice(&serialize_i32_lexicographically(20));
                RecordPtr::new(1, 2).serialize(&mut buf);
                buf
            },
            {
                let mut buf = Vec::new();
                buf.extend_from_slice(&serialize_i32_lexicographically(30));
                RecordPtr::new(2, 3).serialize(&mut buf);
                buf
            },
        ];

        node.batch_insert(records).unwrap();

        for (i, key_val) in [10, 20, 30].iter().enumerate() {
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            match search_result {
                LeafNodeSearchResult::Found { record_ptr, .. } => {
                    assert_eq!(record_ptr.slot_id, (i + 1) as u16);
                }
                _ => panic!("Expected to find key {}", key_val),
            }
        }
    }

    #[test]
    fn test_split_keys_leaf() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        for i in 0..20 {
            let key = serialize_i32_lexicographically(i * 10);
            let rec = RecordPtr::new(i as u32, i as u16);
            node.insert(&key, rec).unwrap();
        }

        let (split_records, separator_key) = node.split_keys().unwrap();

        assert!(
            !split_records.is_empty(),
            "Split should produce records to move"
        );

        assert!(
            !separator_key.is_empty(),
            "Separator key should not be empty"
        );

        let original_count = node.slotted_page.num_slots().unwrap();
        assert!(original_count > 0, "Original node should retain some keys");

        assert_eq!(
            split_records.len() + original_count as usize,
            20,
            "Split should preserve all records"
        );
    }

    #[test]
    fn test_split_keys_with_minimum_keys() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        for i in 0..2 {
            let key = serialize_i32_lexicographically(i * 10);
            let rec = RecordPtr::new(i as u32, i as u16);
            node.insert(&key, rec).unwrap();
        }

        let result = node.split_keys();
        assert!(result.is_ok(), "Split with 2 keys should succeed");
    }

    #[test]
    fn test_split_keys_insufficient_keys() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        // Insert only 1 key
        let key = serialize_i32_lexicographically(10);
        let rec = RecordPtr::new(1, 1);
        node.insert(&key, rec).unwrap();

        let result = node.split_keys();
        assert!(result.is_err(), "Split with 1 key should fail");
        assert!(matches!(result.unwrap_err(), BTreeNodeError::InvalidSplit));
    }

    #[test]
    fn test_leaf_insert_at_boundaries() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(i32::MAX);
        let rec = RecordPtr::new(1, 1);
        let result = node.insert(&key, rec).unwrap();
        assert!(matches!(result, NodeInsertResult::Success));

        let key = serialize_i32_lexicographically(i32::MIN);
        let rec = RecordPtr::new(1, 2);
        let result = node.insert(&key, rec).unwrap();
        assert!(matches!(result, NodeInsertResult::Success));

        let key_max = serialize_i32_lexicographically(i32::MAX);
        let search_result = node.search(&key_max).unwrap();
        assert!(matches!(search_result, LeafNodeSearchResult::Found { .. }));

        let key_min = serialize_i32_lexicographically(i32::MIN);
        let search_result = node.search(&key_min).unwrap();
        assert!(matches!(search_result, LeafNodeSearchResult::Found { .. }));
    }

    #[test]
    fn test_leaf_delete_single_key() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let rec = RecordPtr::new(1, 1);

        node.insert(&key, rec).unwrap();

        let result = node.delete(&key).unwrap();

        // Don't care about this here.
        assert!(matches!(result, NodeDeleteResult::SuccessUnderflow));

        let search_result = node.search(&key).unwrap();
        assert!(matches!(
            search_result,
            LeafNodeSearchResult::NotFoundLeaf { .. }
        ));
    }

    #[test]
    fn test_leaf_delete_nonexistent_key() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);

        let result = node.delete(&key).unwrap();
        assert!(matches!(result, NodeDeleteResult::KeyDoesNotExist));
    }

    #[test]
    fn test_leaf_delete_maintains_order() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let keys_and_recs = vec![
            (10, RecordPtr::new(1, 1)),
            (20, RecordPtr::new(1, 2)),
            (30, RecordPtr::new(2, 3)),
            (40, RecordPtr::new(2, 4)),
            (50, RecordPtr::new(3, 5)),
        ];

        for (key_val, rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            node.insert(&key, *rec).unwrap();
        }

        let key_to_delete = serialize_i32_lexicographically(30);
        let result = node.delete(&key_to_delete).unwrap();

        // Don't care about this here.
        assert!(matches!(result, NodeDeleteResult::SuccessUnderflow));

        let search_result = node.search(&key_to_delete).unwrap();
        assert!(matches!(
            search_result,
            LeafNodeSearchResult::NotFoundLeaf { .. }
        ));

        for (key_val, expected_rec) in &keys_and_recs {
            if *key_val == 30 {
                continue;
            }
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            match search_result {
                LeafNodeSearchResult::Found { record_ptr, .. } => {
                    assert_eq!(record_ptr, *expected_rec);
                }
                _ => panic!("Expected to find key {}", key_val),
            }
        }
    }

    #[test]
    fn test_leaf_delete_all_keys_sequentially() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let keys_and_recs = vec![
            (10, RecordPtr::new(1, 1)),
            (20, RecordPtr::new(1, 2)),
            (30, RecordPtr::new(2, 3)),
        ];

        for (key_val, rec) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            node.insert(&key, *rec).unwrap();
        }

        for (key_val, _) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let result = node.delete(&key).unwrap();
            assert!(matches!(
                result,
                NodeDeleteResult::Success | NodeDeleteResult::SuccessUnderflow
            ));
        }

        for (key_val, _) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let search_result = node.search(&key).unwrap();
            assert!(matches!(
                search_result,
                LeafNodeSearchResult::NotFoundLeaf { .. }
            ));
        }
    }

    #[test]
    fn test_leaf_delete_triggers_underflow() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let mut keys_and_recs = vec![];
        for i in 0..84 {
            keys_and_recs.push((i * 10, RecordPtr::new(1, i as u16)));
        }

        let mut i = 0;
        for (key_val, rec) in &keys_and_recs {
            i += 1;
            let key = serialize_i32_lexicographically(*key_val);
            node.insert(&key, *rec).unwrap();
        }

        assert!(node.slotted_page.fraction_filled().unwrap() > 0.33);

        let mut deletion_count = 0;
        for (key_val, _) in &keys_and_recs {
            let key = serialize_i32_lexicographically(*key_val);
            let result = node.delete(&key).unwrap();
            deletion_count += 1;

            match result {
                NodeDeleteResult::SuccessUnderflow => {
                    assert!(node.slotted_page.fraction_filled().unwrap() <= 0.33);
                    break;
                }
                NodeDeleteResult::Success => {}
                NodeDeleteResult::KeyDoesNotExist => {
                    panic!("Unexpected delete result - key doesn't exist {deletion_count}")
                }
            }
        }

        assert!(deletion_count < keys_and_recs.len());
    }

    #[test]
    fn test_leaf_delete_and_reinsert() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let rec1 = RecordPtr::new(1, 1);
        let rec2 = RecordPtr::new(2, 2);

        node.insert(&key, rec1).unwrap();
        node.delete(&key).unwrap();
        let result = node.insert(&key, rec2).unwrap();

        assert!(matches!(result, NodeInsertResult::Success));

        let search_result = node.search(&key).unwrap();
        match search_result {
            LeafNodeSearchResult::Found { record_ptr, .. } => {
                assert_eq!(record_ptr, rec2);
            }
            _ => panic!("Expected to find re-inserted key"),
        }
    }

    #[test]
    fn test_leaf_delete_multiple_times() {
        let page = TestPage::new(PAGE_SIZE);
        let mut node = LeafNode::initialize(page, None).unwrap();

        let key = serialize_i32_lexicographically(10);
        let rec = RecordPtr::new(1, 1);

        node.insert(&key, rec).unwrap();

        let result = node.delete(&key).unwrap();
        assert!(matches!(
            result,
            NodeDeleteResult::Success | NodeDeleteResult::SuccessUnderflow
        ));

        let result = node.delete(&key).unwrap();
        assert!(matches!(result, NodeDeleteResult::KeyDoesNotExist));

        let result = node.delete(&key).unwrap();
        assert!(matches!(result, NodeDeleteResult::KeyDoesNotExist));
    }

    // TODO: Add tests for get_preceding and get_next index.
}
