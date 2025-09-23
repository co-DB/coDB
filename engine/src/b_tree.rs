use crate::cache::PageRead;
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::paged_file::PageId;
use crate::slotted_page::{
    ReprC, SlotId, SlottedPage, SlottedPageBaseHeader, SlottedPageError, SlottedPageHeader,
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

/// BTree page header that every b-tree node page contains. Is composed of base slotted page header
/// plus b-tree specific fields.
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreePageHeader {
    base_header: SlottedPageBaseHeader,
    /// Whether the page is leaf or internal.
    page_type: u16,
    /// Stores the page id of the child node with keys lesser than the smallest one in
    /// this node. We store this here, because a btree node with n keys needs n+1 child
    /// pointers and storing the first pointer here is easier than storing it in a
    /// dedicated slot.
    leftmost_child_pointer: PageId,
}

/// Enum representing possible page types.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PageType {
    Leaf,
    Internal,
}

/// Enum representing possible outcomes of a search operation.
pub(crate) enum SearchResult {
    /// Leaf node: exact match found.
    Found { record_ptr: RecordPointer },

    /// Leaf node: key not found, but insertion point identified.
    NotFoundLeaf { insert_slot_id: SlotId },

    /// Internal node: follow this child.
    FollowChild { child_ptr: PageId },
}

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

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (page_id, rest) = PageId::deserialize(buffer)?;
        let (slot_id, rest) = SlotId::deserialize(rest)?;
        let record_ptr = Self { page_id, slot_id };
        Ok((record_ptr, rest))
    }
}

/// Helper trait that every key of a b-tree must implement.
pub(crate) trait BTreeKey: DbSerializable + Ord + Clone {}

impl BTreePageHeader {
    const LEAF_PAGE: u16 = 0;
    const INTERNAL_PAGE: u16 = 1;

    fn page_type(&self) -> PageType {
        match self.page_type {
            Self::LEAF_PAGE => PageType::Leaf,
            Self::INTERNAL_PAGE => PageType::Internal,
            _ => unreachable!(),
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.page_type() == PageType::Leaf
    }
}

unsafe impl ReprC for BTreePageHeader {}
impl SlottedPageHeader for BTreePageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base_header
    }
}

/// Struct representing a B-Tree node. It is a wrapper on a slotted page, that uses its api for
/// lower level operations.
pub(crate) struct BTreeNode<Page, Key>
where
    Key: BTreeKey,
{
    /// Slotted page whose operations B-Tree node depends on.
    slotted_page: SlottedPage<Page>,
    /// We need to tie a B-Tree node to specific key type, but since there is no sensible field of
    /// type Key that we can create, we need to use PhantomData just to mark that we are using this
    /// type.
    _key_marker: PhantomData<Key>,
}

impl<Page: PageRead, Key: BTreeKey> BTreeNode<Page, Key> {
    /// Creates a new B-Tree node out of a Page.
    pub fn new(page: Page) -> Self {
        Self {
            // Here allow_slot_compaction is true since in B-Trees only the order of the nodes matter
            // and not the actual ids, thus we can compact the slots (which changes those ids).
            slotted_page: SlottedPage::new(page, true),
            _key_marker: PhantomData,
        }
    }

    /// Gets a reference to B-Tree header.
    fn get_btree_header(&self) -> Result<&BTreePageHeader, BTreeError> {
        Ok(self.slotted_page.get_header::<BTreePageHeader>()?)
    }

    /// Gets a reference to the base header that every slotted page has.
    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, BTreeError> {
        Ok(self.get_btree_header()?.base())
    }

    /// Searches for the given key inside the node and returns:
    /// 1) For internal nodes the id of the child page which must be followed to find the leaf with
    ///    the given key.
    /// 2) For leaf nodes:
    ///    a) If the key is there the result is the record pointer which stores the actual position
    ///    of the record with the given key.
    ///    b) If the key is not there we return the position (slot_id) at which a new record with
    ///    the given key should be inserted.
    pub fn search(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        if self.is_leaf()? {
            self.search_leaf(target_key)
        } else {
            self.search_internal(target_key)
        }
    }

    /// Search function for internal nodes.
    fn search_internal(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        let num_slots = self.slotted_page.num_slots()?;

        if num_slots == 0 {
            return Ok(SearchResult::NotFoundLeaf { insert_slot_id: 0 });
        }

        let mut left = 0;
        let mut right = num_slots - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, child_page_id_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => {
                    left = mid + 1;
                }
                Ordering::Equal => {
                    let (child_page_id, _) = PageId::deserialize(child_page_id_bytes)?;
                    return Ok(SearchResult::FollowChild {
                        child_ptr: child_page_id,
                    });
                }
                Ordering::Greater => {
                    right = mid - 1;
                }
            }
        }

        // Att this point left contains the position of the first key bigger than target_key. If the
        // position is 0 then target_key is smaller than all keys in the node, and thus we need to
        // use the leftmost child pointer from header.
        let child_ptr = if left == 0 {
            self.get_btree_header()?.leftmost_child_pointer
        } else {
            // We use left - 1, because we want to go to the child containing values lesser than the
            // key at position left. Since slot[pos] hold keys >= key[pos] (meaning slot[left] holds
            // keys >= key[left]) we need to use the one before.
            self.get_child_ptr(left - 1)?
        };
        Ok(SearchResult::FollowChild { child_ptr })
    }

    /// Gets the key stored in the given slot.
    fn get_key(&self, slot_id: SlotId) -> Result<Key, BTreeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (key, _) = Key::deserialize(record_bytes)?;
        Ok(key)
    }

    /// Gets the child pointer stored in the given slot. Only applicable for internal nodes.
    fn get_child_ptr(&self, slot_id: SlotId) -> Result<PageId, BTreeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (_, child_ptr_bytes) = Key::deserialize(record_bytes)?;
        let (child_ptr, _) = PageId::deserialize(child_ptr_bytes)?;
        Ok(child_ptr)
    }

    /// Search function for leaf nodes.
    fn search_leaf(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        let mut left = 0;
        let mut right = self.slotted_page.num_slots()? - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let record_bytes = self.slotted_page.read_record(mid)?;
            let (key, child_page_id_bytes) = Key::deserialize(record_bytes)?;

            match key.cmp(target_key) {
                Ordering::Less => {
                    left = mid + 1;
                }
                Ordering::Equal => {
                    let (record_ptr, _) = RecordPointer::deserialize(child_page_id_bytes)?;
                    return Ok(SearchResult::Found { record_ptr });
                }
                Ordering::Greater => {
                    right = mid - 1;
                }
            }
        }

        Ok(SearchResult::NotFoundLeaf {
            insert_slot_id: left,
        })
    }

    /// Returns whether the node is a leaf.
    fn is_leaf(&self) -> Result<bool, BTreeError> {
        Ok(self.get_btree_header()?.is_leaf())
    }
}
