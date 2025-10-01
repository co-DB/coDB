use std::{mem, sync::Arc};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use thiserror::Error;

use crate::{
    cache::{Cache, CacheError, FilePageRef, PageRead, PageWrite, PinnedWritePage},
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
struct FreeSpaceMap<const BUCKETS_COUNT: usize> {
    buckets: [SegQueue<PageId>; BUCKETS_COUNT],
    cache: Arc<Cache>,
    file_key: FileKey,
}

impl<const BUCKETS_COUNT: usize> FreeSpaceMap<BUCKETS_COUNT> {
    /// Finds a page that has at least `needed_space` bytes free.
    ///
    /// If a candidate is stale the
    /// search continues. Returns `Ok(None)` if no suitable page is found.
    fn page_with_free_space(
        &self,
        needed_space: usize,
    ) -> Result<Option<SlottedPage<PinnedWritePage>>, HeapFileError> {
        let start_bucket_idx = self.bucket_for_space(needed_space);
        for b in start_bucket_idx..BUCKETS_COUNT {
            while let Some(page_id) = self.buckets[b].pop() {
                let key = self.file_page_ref(page_id);
                let page = self.cache.pin_write(&key)?;
                let slotted_page = SlottedPage::new(page, true);
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

/// Represents possible types of each [`HeapPage`].
enum HeapPageType {
    /// Stores metadata about the whole [`HeapFile`]. Only one page per file has this type.
    Metadata,
    /// Stores starts of records. Only these pages will be every referenced by [`RecordPtr`].
    Record,
    /// Stores continuation fragments of records.
    Overflow,
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
struct HeapPageHeader {
    base: SlottedPageBaseHeader,
    flags: u16,
    /// [`PageId`] of next record page or overflow page.
    /// Undefined for metadata page.
    next_page: PageId,
}

impl HeapPageHeader {
    const METADATA_TYPE_FLAG: u16 = 1;
    const RECORD_TYPE_FLAG: u16 = 1 << 1;
    const OVERFLOW_TYPE_FLAG: u16 = 1 << 2;

    /// Returns page type encoded in header.
    ///
    /// The type is encoded as a single bit in `flags`. If no known type bit is set
    /// the header is considered corrupted.
    fn page_type(&self) -> Result<HeapPageType, HeapFileError> {
        if self.is_flag_set(Self::METADATA_TYPE_FLAG) {
            return Ok(HeapPageType::Metadata);
        }
        if self.is_flag_set(Self::RECORD_TYPE_FLAG) {
            return Ok(HeapPageType::Record);
        }
        if self.is_flag_set(Self::OVERFLOW_TYPE_FLAG) {
            return Ok(HeapPageType::Overflow);
        }
        return Err(HeapFileError::CorruptedHeapPageHeader {
            error: format!("flags ({}) do not contain any known page type", self.flags),
        });
    }

    /// Returns id of next page.
    ///
    /// This field should not be used for [`HeapPageType::Metadata`] (there is always only one metadata page).
    fn next_page(&self) -> PageId {
        self.next_page
    }

    fn is_flag_set(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }
}

unsafe impl ReprC for HeapPageHeader {}

impl SlottedPageHeader for HeapPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct HeapMetadata {
    first_record_page: PageId,
    first_overflow_page: PageId,
}

/// Page used once per [`HeapFile`] to store global heap metadata.
///
/// The page contains a single record at index [`HeapPageMetadata::METADATA_SLOT_IDX`].
/// That record stores the [`HeapMetadata`] structure.
struct HeapPageMetadata<P> {
    page: SlottedPage<P>,
}

/// Implementation of all [`HeapPageMetadata`] interfaces.
impl<P> HeapPageMetadata<P> {
    const METADATA_SLOT_IDX: u16 = 0;
}

/// Read-only operations for the metadata page.
impl<P: PageRead> HeapPageMetadata<P> {
    fn metadata(&self) -> Result<&HeapMetadata, HeapFileError> {
        let metadata_bytes = self
            .page
            .read_valid_record(Self::METADATA_SLOT_IDX)
            .map_err(|e| HeapFileError::InvalidMetadataPage {
                error: e.to_string(),
            })?;
        bytemuck::try_from_bytes::<HeapMetadata>(metadata_bytes).map_err(|e| {
            HeapFileError::InvalidMetadataPage {
                error: e.to_string(),
            }
        })
    }

    fn first_record_page(&self) -> Result<PageId, HeapFileError> {
        let metadata = self.metadata()?;
        Ok(metadata.first_record_page)
    }

    fn first_overflow_page(&self) -> Result<PageId, HeapFileError> {
        let metadata = self.metadata()?;
        Ok(metadata.first_overflow_page)
    }
}

/// Read/write operations for the metadata page.
impl<P: PageRead + PageWrite> HeapPageMetadata<P> {
    fn set_first_record_page(&mut self, first_record_page: PageId) -> Result<(), HeapFileError> {
        let mut metadata = *self.metadata()?;
        metadata.first_record_page = first_record_page;
        self.set_metadata(&metadata)?;
        Ok(())
    }

    fn set_first_overflow_page(
        &mut self,
        first_overflow_page: PageId,
    ) -> Result<(), HeapFileError> {
        let mut metadata = *self.metadata()?;
        metadata.first_overflow_page = first_overflow_page;
        self.set_metadata(&metadata)?;
        Ok(())
    }

    fn set_metadata(&mut self, metadata: &HeapMetadata) -> Result<(), HeapFileError> {
        let metadata_bytes = bytemuck::bytes_of(metadata);
        self.page.update(Self::METADATA_SLOT_IDX, metadata_bytes)?;
        Ok(())
    }
}

struct HeapPageRecord<P> {
    page: SlottedPage<P>,
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
            v if v == RecordTag::HasContinuation as u16 => Ok(RecordTag::HasContinuation)
            _ => Err(HeapFileError::CorruptedRecordEntry { error: format!("unexpected record tag: 0x{:04x}", value) })
        }
    }
}

impl<P> HeapPageRecord<P>
where
    P: PageRead,
{
    fn record_at<'d>(&'d self, slot_idx: u16) -> Result<RecordFragment<'d>, HeapFileError> {
        let bytes = self.page.read_valid_record(slot_idx)?;
        self.bytes_to_record(bytes)
    }

    fn all_records<'d>(&'d self) -> Result<Vec<RecordFragment<'d>>, HeapFileError> {
        let all_bytes = self.page.read_all_valid_records()?;
        all_bytes
            .map(|bytes| self.bytes_to_record(bytes))
            .collect::<Result<Vec<_>, _>>()
    }

    fn bytes_to_record<'d>(&'d self, bytes: &'d [u8]) -> Result<RecordFragment<'d>, HeapFileError> {
        let ptr_size = mem::size_of::<RecordPtr>();
        if bytes.len() < ptr_size {
            return Err(HeapFileError::RecordPayloadTooSmall {
                min: ptr_size,
                actual: bytes.len(),
            });
        }

        let (ptr_bytes, data) = bytes.split_at(ptr_size);
        let record_ptr = *bytemuck::try_from_bytes::<RecordPtr>(ptr_bytes).map_err(|e| {
            HeapFileError::CorruptedRecordEntry {
                error: e.to_string(),
            }
        })?;

        let next_fragment = if record_ptr.page_id == Self::NO_NEXT_FRAGMENT_PAGE_ID {
            None
        } else {
            Some(record_ptr)
        };

        Ok(RecordFragment {
            data,
            next_fragment,
        })
    }
}

struct HeapPageOverflow<P> {
    page: SlottedPage<P>,
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
    fsm: FreeSpaceMap<BUCKETS_COUNT>,
}
