use std::sync::atomic::AtomicBool;

use bytemuck::{Pod, Zeroable};
use parking_lot::Mutex;
use storage::{
    cache::{PageRead, PageWrite},
    paged_file::PageId,
};

use crate::{
    heap_file::{error::HeapFileError, record::RecordFragment},
    slotted_page::{
        InsertResult, PageType, ReprC, SlotId, SlottedPage, SlottedPageBaseHeader,
        SlottedPageHeader, UpdateResult,
    },
};

/// Metadata of [`HeapFile`]. It's stored using bare [`PagedFile`].
pub(super) struct Metadata {
    pub first_record_page: Mutex<PageId>,
    pub first_overflow_page: Mutex<PageId>,
    pub dirty: AtomicBool,
}

impl From<&MetadataRepr> for Metadata {
    fn from(value: &MetadataRepr) -> Self {
        Metadata {
            first_record_page: Mutex::new(value.first_record_page),
            first_overflow_page: Mutex::new(value.first_overflow_page),
            dirty: AtomicBool::new(false),
        }
    }
}

/// On-disk representation of [`Metadata`].
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub(super) struct MetadataRepr {
    pub first_record_page: PageId,
    pub first_overflow_page: PageId,
}

impl MetadataRepr {
    pub fn load_from_page(page: &impl PageRead) -> Result<Self, HeapFileError> {
        let metadata_size = size_of::<MetadataRepr>();
        let data = page.data();
        let metadata = bytemuck::try_from_bytes(&data[..metadata_size]).map_err(|err| {
            HeapFileError::InvalidMetadataPage {
                error: err.to_string(),
            }
        })?;
        Ok(*metadata)
    }

    pub fn write_to_page(&self, page: &mut impl PageWrite) {
        let bytes = bytemuck::bytes_of(self);
        page.write_at(0, bytes.to_vec());
    }
}

impl From<&Metadata> for MetadataRepr {
    fn from(value: &Metadata) -> Self {
        let first_record_page = *value.first_record_page.lock();
        let first_overflow_page = *value.first_overflow_page.lock();
        MetadataRepr {
            first_record_page,
            first_overflow_page,
        }
    }
}

/// Trait that provides common functionality for all heap page headers.
pub(super) trait BaseHeapPageHeader: SlottedPageHeader {
    const NO_NEXT_PAGE: PageId = 0;

    fn next_page(&self) -> PageId;
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub(super) struct RecordPageHeader {
    base: SlottedPageBaseHeader,
    pub next_page: PageId,
}

impl RecordPageHeader {
    pub fn new(next_page: PageId) -> Self {
        Self {
            base: SlottedPageBaseHeader::new(size_of::<RecordPageHeader>() as _, PageType::Heap),
            next_page,
        }
    }
}

impl BaseHeapPageHeader for RecordPageHeader {
    fn next_page(&self) -> PageId {
        self.next_page
    }
}

impl Default for RecordPageHeader {
    fn default() -> Self {
        Self {
            base: SlottedPageBaseHeader::new(size_of::<RecordPageHeader>() as _, PageType::Heap),
            next_page: Self::NO_NEXT_PAGE,
        }
    }
}

unsafe impl ReprC for RecordPageHeader {}

impl SlottedPageHeader for RecordPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub(super) struct OverflowPageHeader {
    base: SlottedPageBaseHeader,
    pub next_page: PageId,
}

impl OverflowPageHeader {
    pub fn new(next_page: PageId) -> Self {
        Self {
            base: SlottedPageBaseHeader::new(
                size_of::<OverflowPageHeader>() as _,
                PageType::Overflow,
            ),
            next_page,
        }
    }
}

impl BaseHeapPageHeader for OverflowPageHeader {
    fn next_page(&self) -> PageId {
        self.next_page
    }
}

impl Default for OverflowPageHeader {
    fn default() -> Self {
        Self {
            base: SlottedPageBaseHeader::new(
                size_of::<OverflowPageHeader>() as _,
                PageType::Overflow,
            ),
            next_page: Self::NO_NEXT_PAGE,
        }
    }
}

unsafe impl ReprC for OverflowPageHeader {}

impl SlottedPageHeader for OverflowPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

/// Wrapper around [`SlottedPage`] that provides heap file-specific functionality.
pub(super) struct HeapPage<P, H: BaseHeapPageHeader> {
    pub page: SlottedPage<P, H>,
}

impl<P, H> HeapPage<P, H>
where
    H: BaseHeapPageHeader,
{
    pub fn new(page: SlottedPage<P, H>) -> Self {
        HeapPage { page }
    }
}

impl<P, H> HeapPage<P, H>
where
    P: PageRead,
    H: BaseHeapPageHeader,
{
    /// Reads [`RecordFragment`] with `record_id`.
    pub fn record_fragment<'d>(
        &'d self,
        record_id: SlotId,
    ) -> Result<RecordFragment<'d>, HeapFileError> {
        let bytes = self.page.read_record(record_id)?;
        RecordFragment::read_from_bytes(bytes)
    }

    pub fn next_page(&self) -> Result<PageId, HeapFileError> {
        Ok(self.page.get_header()?.next_page())
    }

    pub fn not_deleted_slot_ids(&self) -> Result<impl Iterator<Item = SlotId>, HeapFileError> {
        Ok(self.page.get_not_deleted_slot_ids()?)
    }
}

impl<P, H> HeapPage<P, H>
where
    P: PageWrite + PageRead,
    H: BaseHeapPageHeader,
{
    /// Inserts `data` into the page and returns its [`SlotId`].
    /// If defragmentation is needed to store the data, it's done automatically and insertion is done once again.
    ///
    /// This function assumes that [`self`] has enough free space to insert `data` and its slot (the page should not be chosen by hand, but instead [`FreeSpaceMap`] should be used to get page with enough free space).
    pub fn insert(&mut self, data: &[u8]) -> Result<SlotId, HeapFileError> {
        let result = self.page.insert(data)?;
        match result {
            InsertResult::Success(slot_id) => Ok(slot_id),
            InsertResult::NeedsDefragmentation => {
                self.page.compact_records()?;
                let result = self.page.insert(data)?;
                match result {
                    InsertResult::Success(slot_id) => Ok(slot_id),
                    _ => panic!(
                        "Not enough space after defragmentation, even though 'NeedsDefragmentation' was returned"
                    ),
                }
            }
            // There wasn't enough free space on page, which breaks the invariant. Caller should decide how to handle this,
            // but in correct program this should not happen.
            InsertResult::PageFull => Err(HeapFileError::NotEnoughSpaceOnPage),
        }
    }

    /// Updates record located at `slot` with new `data`.
    /// If defragmentation is needed to store the data, it's done automatically and update is done once again.
    pub fn update(&mut self, slot: SlotId, data: &[u8]) -> Result<(), HeapFileError> {
        let result = self.page.update(slot, data)?;
        match result {
            UpdateResult::Success => Ok(()),
            UpdateResult::NeedsDefragmentation => {
                let result = self.page.defragment_and_update(slot, data)?;
                match result {
                    UpdateResult::Success => Ok(()),
                    _ => panic!(
                        "Not enough space after defragmentation, even though 'NeedsDefragmentation' was returned"
                    ),
                }
            }
            UpdateResult::PageFull => Err(HeapFileError::NotEnoughSpaceOnPage),
        }
    }

    /// Deletes record located at `slot`.
    pub fn delete(&mut self, slot: SlotId) -> Result<(), HeapFileError> {
        self.page.delete(slot)?;
        Ok(())
    }
}
