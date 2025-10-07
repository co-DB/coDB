use std::{marker::PhantomData, mem, sync::Arc};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use metadata::catalog::ColumnMetadata;
use thiserror::Error;

use crate::{
    cache::{Cache, CacheError, FilePageRef, PageRead, PinnedReadPage, PinnedWritePage},
    data_types::DbSerializable,
    files_manager::FileKey,
    paged_file::{PAGE_SIZE, PageId},
    record::{Record, RecordError},
    slotted_page::{
        ReprC, SlotId, SlottedPage, SlottedPageBaseHeader, SlottedPageError, SlottedPageHeader,
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
    /// If a candidate is stale the search continues.
    /// Returns `Ok(None)` if no suitable page is found.
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
                // Note that if the `actual_free_space` is different than we expected it means that other thread
                // already modified this page. We don't need to insert this page to it's actual bucket, as it was done by
                // this other thread that modified it.
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
    slot: SlotId,
}

impl RecordPtr {
    /// Reads [`RecordPtr`] from buffer and returns the rest of the buffer.
    fn read_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), HeapFileError> {
        let (page_id, bytes) =
            PageId::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let (slot, bytes) =
            SlotId::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let ptr = RecordPtr { page_id, slot };
        Ok((ptr, bytes))
    }
}

/// Metadata of [`HeapFile`]. It's stored using bare [`PagedFile`].
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Metadata {
    first_record_page: PageId,
    first_overflow_page: PageId,
}

/// Tag preceding record payload that describes whether a RecordPtr follows.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordTag {
    Final = 0x00,
    HasContinuation = 0xff,
}

impl RecordTag {
    /// Reads [`RecordTag`] from buffer and returns the rest of the buffer.
    fn read_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), HeapFileError> {
        let (value, rest) =
            u8::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let record_tag = RecordTag::try_from(value)?;
        Ok((record_tag, rest))
    }
}

impl TryFrom<u8> for RecordTag {
    type Error = HeapFileError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            v if v == RecordTag::Final as u8 => Ok(RecordTag::Final),
            v if v == RecordTag::HasContinuation as u8 => Ok(RecordTag::HasContinuation),
            _ => Err(HeapFileError::CorruptedRecordEntry {
                error: format!("unexpected record tag: 0x{:02x}", value),
            }),
        }
    }
}

/// [`Record`] is stored across pages using [`RecordFragment`].
/// The first fragment is stored in record page, all next fragments are stored on overflow pages.
struct RecordFragment<'d> {
    /// Part of record from single page.
    data: &'d [u8],
    /// Pointer to the next fragment of record.
    next_fragment: Option<RecordPtr>,
}

impl<'d> RecordFragment<'d> {
    /// Reads [`RecordFragment`] from buffer.
    /// It is assumed that fragment takes the whole buffer.
    fn read_from_bytes(bytes: &'d [u8]) -> Result<Self, HeapFileError> {
        let (tag, bytes) = RecordTag::read_from_bytes(bytes)?;
        let (next_fragment, bytes) = match tag {
            RecordTag::Final => (None, bytes),
            RecordTag::HasContinuation => {
                let (ptr, bytes) = RecordPtr::read_from_bytes(bytes)?;
                (Some(ptr), bytes)
            }
        };
        let fragment = RecordFragment {
            data: bytes,
            next_fragment,
        };
        Ok(fragment)
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

struct HeapPage<P, H: SlottedPageHeader> {
    page: SlottedPage<P, H>,
}

impl<P, H> HeapPage<P, H>
where
    H: SlottedPageHeader,
{
    fn new(page: SlottedPage<P, H>) -> Self {
        HeapPage { page }
    }
}

impl<P, H> HeapPage<P, H>
where
    P: PageRead,
    H: SlottedPageHeader,
{
    /// Reads [`RecordFragment`] with `record_id`.
    fn record_fragment<'d>(
        &'d self,
        record_id: SlotId,
    ) -> Result<RecordFragment<'d>, HeapFileError> {
        let bytes = self.page.read_record(record_id)?;
        RecordFragment::read_from_bytes(bytes)
    }
}

#[derive(Debug, Error)]
pub(crate) enum HeapFileError {
    #[error("corrupted header of heap file's page: {error}")]
    CorruptedHeapPageHeader { error: String },
    #[error("invalid metadata page: {error}")]
    InvalidMetadataPage { error: String },
    #[error("failed to deserialize record entry: {error}")]
    CorruptedRecordEntry { error: String },
    #[error("cache error occurred: {0}")]
    CacheError(#[from] CacheError),
    #[error("slotted page error occurred: {0}")]
    SlottedPageError(#[from] SlottedPageError),
    #[error("failed to serialize/deserialize record: {0}")]
    RecordSerializationHeader(#[from] RecordError),
}

pub(crate) struct HeapFile<const BUCKETS_COUNT: usize> {
    file_key: FileKey,
    metadata: Metadata,
    record_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, RecordPageHeader>,
    overflow_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, OverflowPageHeader>,
    cache: Arc<Cache>,
    columns_metadata: Vec<ColumnMetadata>,
}

impl<const BUCKETS_COUNT: usize> HeapFile<BUCKETS_COUNT> {
    pub(crate) fn new(
        file_key: FileKey,
        cache: Arc<Cache>,
        columns_metadata: Vec<ColumnMetadata>,
    ) -> Result<Self, HeapFileError> {
        todo!()
    }

    /// Reads [`Record`] located at `ptr` and deserializes it.
    pub(crate) fn record(&self, ptr: &RecordPtr) -> Result<Record, HeapFileError> {
        let first_page = self.read_record_page(ptr.page_id)?;
        let fragment = first_page.record_fragment(ptr.slot)?;
        if fragment.next_fragment.is_some() {
            todo!("record with overflow pages not supported yet");
        }
        let record = Record::deserialize(&self.columns_metadata, fragment.data)?;
        Ok(record)
    }

    /// Helper for reading [`HeapFile`]'s pages.
    fn read_page<H>(&self, page_id: PageId) -> Result<HeapPage<PinnedReadPage, H>, HeapFileError>
    where
        H: SlottedPageHeader,
    {
        let file_page_ref = self.file_page_ref(page_id);
        let page = self.cache.pin_read(&file_page_ref)?;
        let slotted_page = SlottedPage::new(page, false)?;
        let heap_node = HeapPage::new(slotted_page);
        Ok(heap_node)
    }

    fn read_record_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, RecordPageHeader>, HeapFileError> {
        self.read_page::<RecordPageHeader>(page_id)
    }

    fn read_overflow_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, OverflowPageHeader>, HeapFileError> {
        self.read_page::<OverflowPageHeader>(page_id)
    }

    /// Creates a `FilePageRef` for `page_id` using heap file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef {
            page_id,
            file_key: self.file_key.clone(),
        }
    }

    fn first_record_page(&self) -> PageId {
        self.metadata.first_record_page
    }

    fn first_overflow_page(&self) -> PageId {
        self.metadata.first_overflow_page
    }
}
