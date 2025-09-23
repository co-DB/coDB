use crate::cache::PageRead;
use crate::data_types::{DbSerializable, DbSerializationError};
use crate::paged_file::PageId;
use crate::slotted_page::{
    ReprC, SlottedPage, SlottedPageBaseHeader, SlottedPageError, SlottedPageHeader,
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
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct BTreePageHeader {
    base_header: SlottedPageBaseHeader,
    page_type: u16,
    leftmost_child_pointer: PageId,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PageType {
    Leaf,
    Internal,
}

pub(crate) enum SearchResult {
    /// Leaf node: exact match found
    Found { record_ptr: RecordPointer },

    /// Leaf node: key not found, but insertion point identified
    NotFoundLeaf { insert_slot_id: u16 },

    /// Internal node: follow this child
    FollowChild { child_ptr: PageId },
}

/// A struct containing all the information necessary to get the actual record data. Stored inside
/// leaf nodes of the b-tree.
pub(crate) struct RecordPointer {
    page_id: PageId,
    slot_id: u16,
}

impl DbSerializable for RecordPointer {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(self.page_id.to_le_bytes());
        buf.extend(self.slot_id.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (page_id, rest) = PageId::deserialize(buffer)?;
        let (slot_id, rest) = u16::deserialize(rest)?;
        let record_ptr = Self { page_id, slot_id };
        Ok((record_ptr, rest))
    }
}
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

    pub fn is_internal(&self) -> bool {
        self.page_type() == PageType::Internal
    }
}

unsafe impl ReprC for BTreePageHeader {}
impl SlottedPageHeader for BTreePageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base_header
    }
}
pub(crate) struct BTreeNode<Page, Key>
where
    Key: BTreeKey,
{
    slotted_page: SlottedPage<Page>,
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
        }
    }

    fn get_btree_header(&self) -> Result<&BTreePageHeader, BTreeError> {
        Ok(self.slotted_page.get_header::<BTreePageHeader>()?)
    }

    fn get_base_header(&self) -> Result<&SlottedPageBaseHeader, BTreeError> {
        Ok(self.get_btree_header()?.base())
    }
    pub fn search(&self, target_key: &Key) -> Result<SearchResult, BTreeError> {
        if self.is_leaf()? {
            self.search_leaf(target_key)
        } else {
            self.search_internal(target_key)
        }
    }

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

        let child_ptr = if left == 0 {
            self.get_btree_header()?.leftmost_child_pointer
        } else {
            self.get_child_ptr(left - 1)?
        };
        Ok(SearchResult::FollowChild { child_ptr })
    }

    fn get_key(&self, slot_id: u16) -> Result<Key, BTreeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (key, _) = Key::deserialize(record_bytes)?;
        Ok(key)
    }

    fn get_child_ptr(&self, slot_id: u16) -> Result<PageId, BTreeError> {
        let record_bytes = self.slotted_page.read_record(slot_id)?;
        let (_, child_ptr_bytes) = Key::deserialize(record_bytes)?;
        let (child_ptr, _) = PageId::deserialize(child_ptr_bytes)?;
        Ok(child_ptr)
    }

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

    fn is_leaf(&self) -> Result<bool, BTreeError> {
        Ok(self.get_btree_header()?.is_leaf())
    }
}
