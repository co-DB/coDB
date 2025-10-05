use std::{marker::PhantomData, sync::Arc};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use thiserror::Error;

use crate::{
    cache::{Cache, CacheError, FilePageRef, PinnedWritePage},
    files_manager::FileKey,
    paged_file::{PAGE_SIZE, PageId},
    slotted_page::{
        ReprC, SlottedPage, SlottedPageBaseHeader, SlottedPageError, SlottedPageHeader,
    },
};

/// Free-space index for a HeapFile.
///
/// Tracks PageId values grouped into lock‑free buckets that act as hints about how much
/// free space each page currently has. Buckets are advisory: whenever a page id is taken
/// from a bucket the page header must be read to verify actual free space (so entries
/// can be stale).
///
/// Buckets split the page free-space range [0, PAGE_SIZE] into BUCKETS_COUNT equal
/// intervals. For BUCKETS_COUNT = 4 the mapping is:
///
///   ```
///   bucket 0 -> [0%, 25%) free
///   bucket 1 -> [25%, 50%) free
///   bucket 2 -> [50%, 75%) free
///   bucket 3 -> [75%,100%] free
///   ```
///
/// Each bucket is a multi-producer / multi-consumer lock-free queue (SegQueue).
/// Updates to this map are in-memory hints only; the authoritative free-space value
/// resides in the slotted page header and is re-read when a page is popped from a bucket.
/// The map is rebuilt from page headers when the heap file is opened.
///
/// There are two FSMs per each heap file: one for record pages and the other one for overflow pages.
struct FreeSpaceMap<const BUCKETS_COUNT: usize, H: SlottedPageHeader> {
    buckets: [SegQueue<PageId>; BUCKETS_COUNT],
    cache: Arc<Cache>,
    file_key: FileKey,
    _page_type_marker: PhantomData<H>,
}

impl<const BUCKETS_COUNT: usize, H: SlottedPageHeader> FreeSpaceMap<BUCKETS_COUNT, H> {
    /// Finds a page that has at least `needed_space` bytes free.
    ///
    /// If a candidate is stale the
    /// search continues. Returns `Ok(None)` if no suitable page is found.
    fn page_with_free_space(
        &self,
        needed_space: usize,
    ) -> Result<Option<SlottedPage<PinnedWritePage, H>>, HeapFileError> {
        let start_bucket_idx = self.bucket_for_space(needed_space);
        for b in start_bucket_idx..BUCKETS_COUNT {
            while let Some(page_id) = self.buckets[b].pop() {
                let key = self.file_page_ref(page_id);
                let page = self.cache.pin_write(&key)?;
                let slotted_page = SlottedPage::new(page, true)?;
                let actual_free_space = slotted_page.free_space()?;
                if actual_free_space >= needed_space as _ {
                    return Ok(Some(slotted_page));
                }
            }
        }
        Ok(None)
    }

    /// Adds a page id to the bucket corresponding to `free_space` (in bytes).
    ///
    /// This is a best-effort, in-memory hint: the page id is pushed into the computed
    /// bucket so future searches may find it. The authoritative free-space value is in
    /// the page header.
    fn update_page_bucket(&self, page_id: PageId, free_space: usize) {
        let bucket_idx = self.bucket_for_space(free_space);
        self.buckets[bucket_idx].push(page_id);
    }

    /// Computes the bucket index for a given amount of free space (in bytes).
    ///
    /// The page free-space range `[0, PAGE_SIZE]` is divided into `BUCKETS_COUNT` equal
    /// intervals. This returns the index of the interval that contains `space`.
    /// The result is clamped to `[0, BUCKETS_COUNT - 1]`.
    fn bucket_for_space(&self, space: usize) -> usize {
        (space * BUCKETS_COUNT / PAGE_SIZE).clamp(0, BUCKETS_COUNT - 1)
    }

    /// Creates a `FilePageRef` for `page_id` using this map's file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef {
            page_id,
            file_key: self.file_key.clone(),
        }
    }
}

/// Logical pointer to a record in a [`HeapFile`].
///
/// It should only be used for referencing start of the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecordPtr {
    page_id: PageId,
    slot: u16,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Metadata {
    first_record_page: PageId,
    first_overflow_page: PageId,
}

struct RecordFragment<'d> {
    data: &'d [u8],
    next_fragment: Option<RecordPtr>,
}

/// Tag preceding record payload that describes whether a RecordPtr follows.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordTag {
    Final = 0x0000,
    HasContinuation = 0xffff,
}

impl RecordTag {
    // TODO:
    // helper for reading from bytes
}

impl TryFrom<u16> for RecordTag {
    type Error = HeapFileError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            v if v == RecordTag::Final as u16 => Ok(RecordTag::Final),
            v if v == RecordTag::HasContinuation as u16 => Ok(RecordTag::HasContinuation),
            _ => Err(HeapFileError::CorruptedRecordEntry {
                error: format!("unexpected record tag: 0x{:04x}", value),
            }),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RecordPageHeader {
    base: SlottedPageBaseHeader,
    // TODO:
}

unsafe impl ReprC for RecordPageHeader {}

impl SlottedPageHeader for RecordPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct OverflowPageHeader {
    base: SlottedPageBaseHeader,
    // TODO:
}

unsafe impl ReprC for OverflowPageHeader {}

impl SlottedPageHeader for OverflowPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

#[derive(Debug, Error)]
pub(crate) enum HeapFileError {
    #[error("corrupted header of heap file's page: {error}")]
    CorruptedHeapPageHeader { error: String },
    #[error("invalid metadata page: {error}")]
    InvalidMetadataPage { error: String },
    #[error("record payload was to small (got {actual}, wanted at least {min}")]
    RecordPayloadTooSmall { min: usize, actual: usize },
    #[error("failed to deserialize record entry: {error}")]
    CorruptedRecordEntry { error: String },
    #[error("cache error occured: {0}")]
    CacheError(#[from] CacheError),
    #[error("slotted page error occured: {0}")]
    SlottedPageError(#[from] SlottedPageError),
}

pub(crate) struct HeapFile<const BUCKETS_COUNT: usize> {
    file_key: FileKey,
    metadata: Metadata,
    record_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, RecordPageHeader>,
    overflow_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, OverflowPageHeader>,
}

impl<const BUCKETS_COUNT: usize> HeapFile<BUCKETS_COUNT> {
    fn first_record_page(&self) -> PageId {
        self.metadata.first_record_page
    }

    fn first_overflow_page(&self) -> PageId {
        self.metadata.first_overflow_page
    }
}
