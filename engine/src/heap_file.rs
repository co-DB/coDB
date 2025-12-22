use std::{
    array,
    collections::VecDeque,
    marker::PhantomData,
    mem,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use dashmap::DashSet;
use metadata::catalog::ColumnMetadata;
use parking_lot::Mutex;
use thiserror::Error;

use crate::{
    record::{Record, RecordError},
    slotted_page::{
        InsertResult, PageType, ReprC, Slot, SlotId, SlottedPage, SlottedPageBaseHeader,
        SlottedPageError, SlottedPageHeader, UpdateResult,
    },
};

use types::{
    data::Value,
    serialization::{DbSerializable, DbSerializationError},
};

use storage::{
    cache::{Cache, CacheError, FilePageRef, PageRead, PageWrite, PinnedReadPage, PinnedWritePage},
    files_manager::FileKey,
    paged_file::{PAGE_SIZE, Page, PageId, PagedFileError},
};

/// Free-space map for a HeapFile.
///
/// Tracks PageId values grouped into lock‑free buckets that act as hints about how much
/// free space each page currently has. Buckets are advisory: whenever a page id is taken
/// from a bucket the page header must be read to verify actual free space (so entries
/// can be stale).
///
/// Buckets split the page free-space range [0, PAGE_SIZE] into BUCKETS_COUNT equal
/// intervals. For BUCKETS_COUNT = 4 the mapping is:
///
///   ```text
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
struct FreeSpaceMap<const BUCKETS_COUNT: usize, H: BaseHeapPageHeader> {
    buckets: [SegQueue<PageId>; BUCKETS_COUNT],
    cache: Arc<Cache>,
    file_key: FileKey,
    /// [`PageId`] of next page that should be read by fsm in case of not finding suitable page in [`FreeSpaceMap::buckets`].
    next_page_to_read: Mutex<PageId>,
    /// Set of page IDs currently tracked by this FSM.
    /// Prevents duplicate entries in buckets.
    tracked_pages: DashSet<PageId>,
    _page_type_marker: PhantomData<H>,
}

type FsmPage<H> = SlottedPage<PinnedWritePage, H>;

impl<const BUCKETS_COUNT: usize, H: BaseHeapPageHeader> FreeSpaceMap<BUCKETS_COUNT, H> {
    /// Maximum number of candidate pages to check per bucket before giving up and trying the next bucket.
    const MAX_CANDIDATES_PER_BUCKET: usize = 32;

    /// Minimum free space that page should have to be added to FSM.
    /// We don't want to use pages with less free space, as most of the time they won't be use anyway.
    const MIN_USEFUL_FREE_SPACE: usize = 32;

    /// Maximum number of pages to load from disk before giving up and allocating new page.
    const MAX_DISK_PAGES_TO_CHECK: usize = 4;

    fn new(cache: Arc<Cache>, file_key: FileKey, first_page_id: PageId) -> Self {
        let buckets: [SegQueue<PageId>; BUCKETS_COUNT] = array::from_fn(|_| SegQueue::new());

        FreeSpaceMap {
            buckets,
            cache,
            file_key,
            next_page_to_read: Mutex::new(first_page_id),
            tracked_pages: DashSet::new(),
            _page_type_marker: PhantomData::<H>,
        }
    }

    /// Finds a page that has at least `needed_space` bytes free.
    fn page_with_free_space(
        &self,
        needed_space: usize,
    ) -> Result<Option<(PageId, FsmPage<H>)>, HeapFileError> {
        let start_bucket_idx = self.bucket_for_space(needed_space);

        for b in start_bucket_idx..BUCKETS_COUNT {
            let mut candidates_checked = 0;
            let mut reinsert_to_current_bucket = Vec::new();

            while candidates_checked < Self::MAX_CANDIDATES_PER_BUCKET {
                let Some(page_id) = self.buckets[b].pop() else {
                    break;
                };

                // Remove from tracked_pages
                self.tracked_pages.remove(&page_id);

                candidates_checked += 1;

                // Quick check with read lock
                let key = self.file_page_ref(page_id);
                let actual_free_space = {
                    let read_page = self.cache.pin_read(&key)?;
                    let slotted_page = SlottedPage::<_, H>::new(read_page)?;
                    slotted_page.free_space()?
                } as usize;

                if actual_free_space >= needed_space {
                    let page = self.cache.pin_write(&key)?;
                    let slotted_page = SlottedPage::new(page)?;
                    let final_free_space = slotted_page.free_space()? as usize;

                    // Check if between read and write lock free space didn't change
                    if final_free_space >= needed_space {
                        // Before returning reinsert the pages
                        for id in reinsert_to_current_bucket {
                            if self.tracked_pages.insert(id) {
                                self.buckets[b].push(id);
                            }
                        }
                        return Ok(Some((page_id, slotted_page)));
                    }

                    // Space was taken between read and write lock
                    if final_free_space >= Self::MIN_USEFUL_FREE_SPACE {
                        let correct_bucket = self.bucket_for_space(final_free_space);
                        if correct_bucket == b {
                            reinsert_to_current_bucket.push(page_id);
                        } else if correct_bucket < b {
                            self.add_to_bucket(page_id, final_free_space);
                        }
                    }
                    continue;
                }

                // Page doesn't have enough space for our request
                if actual_free_space >= Self::MIN_USEFUL_FREE_SPACE {
                    let correct_bucket = self.bucket_for_space(actual_free_space);
                    if correct_bucket == b {
                        reinsert_to_current_bucket.push(page_id);
                    } else {
                        self.add_to_bucket(page_id, actual_free_space);
                    }
                }
            }

            // Reinsert pages that belong in this bucket
            for id in reinsert_to_current_bucket {
                if self.tracked_pages.insert(id) {
                    self.buckets[b].push(id);
                }
            }
        }

        self.page_with_free_space_from_disk(needed_space)
    }

    /// Iterates over pages not yet loaded from disk and returns one that has enough free space.
    /// If no such page is found then `None` is returned.
    fn page_with_free_space_from_disk(
        &self,
        needed_space: usize,
    ) -> Result<Option<(PageId, FsmPage<H>)>, HeapFileError> {
        let mut pages_checked = 0;

        while pages_checked < Self::MAX_DISK_PAGES_TO_CHECK {
            let Some((page_id, page)) = self.load_next_page()? else {
                break;
            };

            pages_checked += 1;
            let free_space = page.free_space()? as usize;

            if free_space >= needed_space {
                return Ok(Some((page_id, page)));
            }

            // Only add to FSM if it has useful space
            if free_space >= Self::MIN_USEFUL_FREE_SPACE {
                self.add_to_bucket(page_id, free_space);
            }
        }

        Ok(None)
    }

    /// Loads page with id [`Self::next_page_to_read`] and updates it to the next pointer.
    /// Returns id and loaded page or `None` if there is no more page to read.
    fn load_next_page(&self) -> Result<Option<(PageId, FsmPage<H>)>, HeapFileError> {
        let mut next_page_lock = self.next_page_to_read.lock();

        if *next_page_lock == H::NO_NEXT_PAGE {
            return Ok(None);
        }

        let page_id = *next_page_lock;

        // Skip pages that are already tracked (already in FSM buckets)
        if self.tracked_pages.contains(&page_id) {
            let key = self.file_page_ref(page_id);
            let read_page = self.cache.pin_read(&key)?;
            let slotted_page = SlottedPage::<_, H>::new(read_page)?;
            *next_page_lock = slotted_page.get_header()?.next_page();
            return self.load_next_page();
        }

        let key = self.file_page_ref(page_id);
        let page = self.cache.pin_write(&key)?;
        let slotted_page = SlottedPage::<_, H>::new(page)?;

        *next_page_lock = slotted_page.get_header()?.next_page();

        Ok(Some((page_id, slotted_page)))
    }

    /// Adds a page to the appropriate bucket and marks it as tracked.
    /// Only adds if the page has useful free space and isn't already tracked.
    fn add_to_bucket(&self, page_id: PageId, free_space: usize) {
        // Don't track pages with very little free space
        if free_space < Self::MIN_USEFUL_FREE_SPACE {
            return;
        }

        // Only add if not already tracked
        if self.tracked_pages.insert(page_id) {
            let bucket_idx = self.bucket_for_space(free_space);
            self.buckets[bucket_idx].push(page_id);
        }
    }

    /// Adds a page id to the bucket corresponding to `free_space`.
    fn update_page_bucket(&self, page_id: PageId, free_space: usize) {
        self.add_to_bucket(page_id, free_space);
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
        FilePageRef::new(page_id, self.file_key.clone())
    }
}

/// Logical pointer to a record in a [`HeapFile`].
///
/// It should only be used for referencing start of the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPtr {
    pub(crate) page_id: PageId,
    pub(crate) slot_id: SlotId,
}

impl RecordPtr {
    /// Placeholder used to reserve a key in the B-tree before inserting actual data.
    pub const PLACEHOLDER: RecordPtr = RecordPtr {
        page_id: PageId::MAX,
        slot_id: SlotId::MAX,
    };

    pub(crate) fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }
}
impl DbSerializable for RecordPtr {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.page_id.serialize(buffer);
        self.slot_id.serialize(buffer);
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        let mut start = 0;
        let mut end = size_of::<PageId>();
        buffer[start..end].copy_from_slice(&self.page_id.to_le_bytes());
        start += size_of::<PageId>();
        end += size_of::<SlotId>();
        buffer[start..end].copy_from_slice(&self.slot_id.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (page_id, rest) = PageId::deserialize(buffer)?;
        let (slot, _) = SlotId::deserialize(rest)?;
        Ok((
            Self {
                page_id,
                slot_id: slot,
            },
            buffer,
        ))
    }

    fn size_serialized(&self) -> usize {
        size_of::<Self>()
    }
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
        let ptr = RecordPtr {
            page_id,
            slot_id: slot,
        };
        Ok((ptr, bytes))
    }
}

impl From<&RecordPtr> for Vec<u8> {
    fn from(value: &RecordPtr) -> Self {
        let mut buffer = Vec::with_capacity(size_of::<RecordPtr>());
        value.page_id.serialize(&mut buffer);
        value.slot_id.serialize(&mut buffer);
        buffer
    }
}

/// Metadata of [`HeapFile`]. It's stored using bare [`PagedFile`].
struct Metadata {
    first_record_page: Mutex<PageId>,
    first_overflow_page: Mutex<PageId>,
    dirty: AtomicBool,
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
struct MetadataRepr {
    first_record_page: PageId,
    first_overflow_page: PageId,
}

impl MetadataRepr {
    fn load_from_page(page: &Page) -> Result<Self, HeapFileError> {
        let metadata_size = size_of::<MetadataRepr>();
        let metadata = bytemuck::try_from_bytes(&page[..metadata_size]).map_err(|err| {
            HeapFileError::InvalidMetadataPage {
                error: err.to_string(),
            }
        })?;
        Ok(*metadata)
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

    /// Returns buffer with added [`RecordTag::Final`] at the beginning of it.
    fn with_final(buffer: &[u8]) -> Vec<u8> {
        let mut new_buffer = Vec::with_capacity(buffer.len() + size_of::<RecordTag>());
        new_buffer.push(RecordTag::Final as _);
        new_buffer.extend_from_slice(buffer);
        new_buffer
    }

    /// Returns buffer with added [`RecordTag::HasContinuation`] at the beginning of it.
    fn with_has_continuation(buffer: &[u8], continuation: &RecordPtr) -> Vec<u8> {
        let mut new_buffer =
            Vec::with_capacity(buffer.len() + size_of::<RecordTag>() + size_of::<RecordPtr>());
        new_buffer.push(RecordTag::HasContinuation as _);
        let mut ptr = Vec::from(continuation);
        new_buffer.append(&mut ptr);
        new_buffer.extend_from_slice(buffer);
        new_buffer
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

/// Struct used for describing update of single field.
pub struct FieldUpdateDescriptor {
    column: ColumnMetadata,
    value: Value,
}

impl FieldUpdateDescriptor {
    /// Creates new [`FieldUpdateDescriptor`].
    ///
    /// Returns an error when `column` and `value` have different types.
    pub fn new(
        column: ColumnMetadata,
        value: Value,
    ) -> Result<FieldUpdateDescriptor, HeapFileError> {
        if column.ty() != value.ty() {
            return Err(HeapFileError::ColumnAndValueTypesDontMatch {
                column_ty: column.ty().to_string(),
                value_ty: value.ty().to_string(),
            });
        }
        Ok(FieldUpdateDescriptor { column, value })
    }
}

/// Helper for managing fragment traversal state during record updates
struct FragmentProcessor<'a> {
    remaining_bytes: &'a [u8],
    bytes_changed: &'a [bool],
}

impl<'a> FragmentProcessor<'a> {
    fn new(remaining_bytes: &'a [u8], bytes_changed: &'a [bool]) -> Self {
        Self {
            remaining_bytes,
            bytes_changed,
        }
    }

    /// Checks if the current fragment has any changes
    fn has_changes(&self, fragment_len: usize) -> bool {
        let check_len = fragment_len.min(self.remaining_bytes.len());
        self.bytes_changed[..check_len].iter().any(|&b| b)
    }

    /// Advances past the current fragment
    fn skip_fragment(&mut self, fragment_len: usize) {
        self.remaining_bytes = &self.remaining_bytes[fragment_len..];
        self.bytes_changed = &self.bytes_changed[fragment_len..];
    }

    /// Returns true if we've processed all bytes
    fn is_complete(&self) -> bool {
        self.remaining_bytes.is_empty()
    }

    /// Returns true if this is the last fragment (remaining bytes fit in fragment)
    fn is_last_fragment(&self, fragment_len: usize) -> bool {
        self.remaining_bytes.len() <= fragment_len
    }
}

/// Trait encapsulating logic behind locking pages.
///
/// When dealing with record that spans multiple pages (multi-fragment record) pages shouldn't be read by hand,
/// but instead struct that implements this trait should be used.
trait PageLockChain<P> {
    /// Locks the next page
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError>;
    /// Gets current record page (undefined when only overflow page is held at the time).
    fn record_page(&self) -> &HeapPage<P, RecordPageHeader>;
    /// Gets mutable current record page (undefined when only overflow page is held at the time).
    fn record_page_mut(&mut self) -> &mut HeapPage<P, RecordPageHeader>;
    /// Gets current overflow page (undefined when only record page is held at the time).
    fn overflow_page(&self) -> &HeapPage<P, OverflowPageHeader>;
    /// Gets mutable current overflow page (undefined when only record page is held at the time).
    fn overflow_page_mut(&mut self) -> &mut HeapPage<P, OverflowPageHeader>;
}

/// Helper struct used for locking pages when working with records.
///
/// Locking pages with this struct works as follows:
/// - read page
/// - do an action on the page
/// - read next page
/// - drop the previous page
struct SinglePageLockChain<'hf, const BUCKETS_COUNT: usize, P> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: Option<HeapPage<P, RecordPageHeader>>,
    overflow_page: Option<HeapPage<P, OverflowPageHeader>>,
}

impl<'hf, const BUCKETS_COUNT: usize, P> SinglePageLockChain<'hf, BUCKETS_COUNT, P> {
    /// Gets current record page.
    ///
    /// Panics if we hold overflow page instead.
    fn _record_page(&self) -> &HeapPage<P, RecordPageHeader> {
        match &self.record_page {
            Some(record_page) => record_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    /// Gets mutable current record page.
    ///
    /// Panics if we hold overflow page instead.
    fn _record_page_mut(&mut self) -> &mut HeapPage<P, RecordPageHeader> {
        match &mut self.record_page {
            Some(record_page) => record_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }

    /// Gets current overflow page.
    ///
    /// Panics if we hold record page instead.
    fn _overflow_page(&self) -> &HeapPage<P, OverflowPageHeader> {
        match &self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }

    /// Gets mutable current overflow page.
    ///
    /// Panics if we hold record page instead.
    fn _overflow_page_mut(&mut self) -> &mut HeapPage<P, OverflowPageHeader> {
        match &mut self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }
}

impl<'hf, const BUCKETS_COUNT: usize> SinglePageLockChain<'hf, BUCKETS_COUNT, PinnedReadPage> {
    /// Creates new [`SinglePageLockChain`] that starts with record page
    fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.read_record_page(page_id)?;
        Ok(SinglePageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
        })
    }
}

impl<'hf, const BUCKETS_COUNT: usize> SinglePageLockChain<'hf, BUCKETS_COUNT, PinnedWritePage> {
    /// Creates new [`SinglePageLockChain`] that starts with record page
    fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.write_record_page(page_id)?;
        Ok(SinglePageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
        })
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageLockChain<PinnedReadPage>
    for SinglePageLockChain<'hf, BUCKETS_COUNT, PinnedReadPage>
{
    /// Locks the next page and only then drops the previous one
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        // Read next page
        let new_page = self.heap_file.read_overflow_page(next_page_id)?;

        // Drop previous page
        self.record_page = None;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedReadPage, RecordPageHeader> {
        self._record_page()
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedReadPage, RecordPageHeader> {
        self._record_page_mut()
    }

    fn overflow_page(&self) -> &HeapPage<PinnedReadPage, OverflowPageHeader> {
        self._overflow_page()
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedReadPage, OverflowPageHeader> {
        self._overflow_page_mut()
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageLockChain<PinnedWritePage>
    for SinglePageLockChain<'hf, BUCKETS_COUNT, PinnedWritePage>
{
    /// Locks the next page and only then drops the previous one
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        // Read next page
        let new_page = self.heap_file.write_overflow_page(next_page_id)?;

        // Drop previous page
        self.record_page = None;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedWritePage, RecordPageHeader> {
        self._record_page()
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader> {
        self._record_page_mut()
    }

    fn overflow_page(&self) -> &HeapPage<PinnedWritePage, OverflowPageHeader> {
        self._overflow_page()
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader> {
        self._overflow_page_mut()
    }
}

struct PageLockChainWithLockedRecordPage<'hf, 'rp, const BUCKETS_COUNT: usize, P> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: &'rp mut HeapPage<P, RecordPageHeader>,
    overflow_page: Option<HeapPage<P, OverflowPageHeader>>,
}

impl<'hf, 'rp, const BUCKETS_COUNT: usize, P>
    PageLockChainWithLockedRecordPage<'hf, 'rp, BUCKETS_COUNT, P>
{
    /// Creates new [`PageLockChainWithLockedRecordPage`] that starts with record page
    fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        record_page: &'rp mut HeapPage<P, RecordPageHeader>,
    ) -> Result<Self, HeapFileError> {
        Ok(PageLockChainWithLockedRecordPage {
            heap_file,
            record_page,
            overflow_page: None,
        })
    }

    /// Gets current record page.
    fn _record_page(&self) -> &HeapPage<P, RecordPageHeader> {
        self.record_page
    }

    /// Gets mutable current record page.
    fn _record_page_mut(&mut self) -> &mut HeapPage<P, RecordPageHeader> {
        self.record_page
    }

    /// Gets current overflow page.
    ///
    /// Panics if we hold record page instead.
    fn _overflow_page(&self) -> &HeapPage<P, OverflowPageHeader> {
        match &self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get overflow page when record page is stored instead"
            ),
        }
    }

    /// Gets mutable current overflow page.
    ///
    /// Panics if we hold record page instead.
    fn _overflow_page_mut(&mut self) -> &mut HeapPage<P, OverflowPageHeader> {
        match &mut self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }
}

impl<'hf, 'rp, const BUCKETS_COUNT: usize> PageLockChain<PinnedReadPage>
    for PageLockChainWithLockedRecordPage<'hf, 'rp, BUCKETS_COUNT, PinnedReadPage>
{
    /// Locks the next page
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        let new_page = self.heap_file.read_overflow_page(next_page_id)?;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedReadPage, RecordPageHeader> {
        self._record_page()
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedReadPage, RecordPageHeader> {
        self._record_page_mut()
    }

    fn overflow_page(&self) -> &HeapPage<PinnedReadPage, OverflowPageHeader> {
        self._overflow_page()
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedReadPage, OverflowPageHeader> {
        self._overflow_page_mut()
    }
}

impl<'hf, 'rp, const BUCKETS_COUNT: usize> PageLockChain<PinnedWritePage>
    for PageLockChainWithLockedRecordPage<'hf, 'rp, BUCKETS_COUNT, PinnedWritePage>
{
    /// Locks the next page
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        let new_page = self.heap_file.write_overflow_page(next_page_id)?;
        self.overflow_page = Some(new_page);
        Ok(())
    }

    fn record_page(&self) -> &HeapPage<PinnedWritePage, RecordPageHeader> {
        self._record_page()
    }

    fn record_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, RecordPageHeader> {
        self._record_page_mut()
    }

    fn overflow_page(&self) -> &HeapPage<PinnedWritePage, OverflowPageHeader> {
        self._overflow_page()
    }

    fn overflow_page_mut(&mut self) -> &mut HeapPage<PinnedWritePage, OverflowPageHeader> {
        self._overflow_page_mut()
    }
}

/// Trait that provides common functionality for all heap page headers.
trait BaseHeapPageHeader: SlottedPageHeader {
    const NO_NEXT_PAGE: PageId = 0;

    fn next_page(&self) -> PageId;
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RecordPageHeader {
    base: SlottedPageBaseHeader,
    next_page: PageId,
}

impl RecordPageHeader {
    fn new(next_page: PageId) -> Self {
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
struct OverflowPageHeader {
    base: SlottedPageBaseHeader,
    next_page: PageId,
}

impl OverflowPageHeader {
    fn new(next_page: PageId) -> Self {
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
struct HeapPage<P, H: BaseHeapPageHeader> {
    page: SlottedPage<P, H>,
}

impl<P, H> HeapPage<P, H>
where
    H: BaseHeapPageHeader,
{
    fn new(page: SlottedPage<P, H>) -> Self {
        HeapPage { page }
    }
}

impl<P, H> HeapPage<P, H>
where
    P: PageRead,
    H: BaseHeapPageHeader,
{
    /// Reads [`RecordFragment`] with `record_id`.
    fn record_fragment<'d>(
        &'d self,
        record_id: SlotId,
    ) -> Result<RecordFragment<'d>, HeapFileError> {
        let bytes = self.page.read_record(record_id)?;
        RecordFragment::read_from_bytes(bytes)
    }

    fn next_page(&self) -> Result<PageId, HeapFileError> {
        Ok(self.page.get_header()?.next_page())
    }

    fn not_deleted_slot_ids(&self) -> Result<impl Iterator<Item = SlotId>, HeapFileError> {
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
    fn insert(&mut self, data: &[u8]) -> Result<SlotId, HeapFileError> {
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
    fn update(&mut self, slot: SlotId, data: &[u8]) -> Result<(), HeapFileError> {
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
    fn delete(&mut self, slot: SlotId) -> Result<(), HeapFileError> {
        self.page.delete(slot)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum HeapFileError {
    #[error("invalid metadata page: {error}")]
    InvalidMetadataPage { error: String },
    #[error("failed to deserialize record entry: {error}")]
    CorruptedRecordEntry { error: String },
    #[error("cache error occurred: {0}")]
    CacheError(#[from] CacheError),
    #[error("slotted page error occurred: {0}")]
    SlottedPageError(#[from] SlottedPageError),
    #[error("failed to serialize/deserialize record: {0}")]
    RecordSerializationError(#[from] RecordError),
    #[error("there was not enough space on page to perform request")]
    NotEnoughSpaceOnPage,
    #[error("column type ({column_ty}) and value type ({value_ty}) do not match")]
    ColumnAndValueTypesDontMatch { column_ty: String, value_ty: String },
    #[error("{0}")]
    DBSerializationError(#[from] DbSerializationError),
}

/// Handle to record and its pointer in heap file.
pub struct RecordHandle {
    pub record: Record,
    pub record_ptr: RecordPtr,
}

impl RecordHandle {
    fn new(record: Record, record_ptr: RecordPtr) -> Self {
        RecordHandle { record, record_ptr }
    }
}

/// Iterator over all records in a heap file
pub struct AllRecordsIterator<'hf, const BUCKETS_COUNT: usize> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    current_page_records: VecDeque<RecordHandle>,
    next_page_id: PageId,
}

impl<'hf, const BUCKETS_COUNT: usize> AllRecordsIterator<'hf, BUCKETS_COUNT> {
    fn new(heap_file: &'hf HeapFile<BUCKETS_COUNT>, next_page_id: PageId) -> Self {
        AllRecordsIterator {
            heap_file,
            current_page_records: VecDeque::new(),
            next_page_id,
        }
    }
}

impl<'hf, const BUCKETS_COUNT: usize> Iterator for AllRecordsIterator<'hf, BUCKETS_COUNT> {
    type Item = Result<RecordHandle, HeapFileError>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have records left from the ones already read just return the first one
        if let Some(record) = self.current_page_records.pop_front() {
            return Some(Ok(record));
        }

        // No more records in the current page, if it was last page we can stop
        if self.next_page_id == RecordPageHeader::NO_NEXT_PAGE {
            return None;
        }

        // Load next page
        match self.heap_file.read_all_record_from_page(self.next_page_id) {
            Ok((next_page_id, records)) => {
                self.next_page_id = next_page_id;
                self.current_page_records = records;

                // Recursively call to return first record from newly loaded page
                self.next()
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// Structure responsible for managing on-disk heap files.
///
/// Each [`HeapFile`] instance corresponds to a single physical file on disk.
/// For concurrent access by multiple threads, wrap the instance in `Arc<HeapFile>`.
pub struct HeapFile<const BUCKETS_COUNT: usize> {
    file_key: FileKey,
    metadata: Metadata,
    record_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, RecordPageHeader>,
    overflow_pages_fsm: FreeSpaceMap<BUCKETS_COUNT, OverflowPageHeader>,
    cache: Arc<Cache>,
    columns_metadata: Vec<ColumnMetadata>,
}

impl<const BUCKETS_COUNT: usize> HeapFile<BUCKETS_COUNT> {
    const METADATA_PAGE_ID: PageId = 1;

    const MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT: usize =
        SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

    const MAX_RECORD_SIZE_WHEN_MANY_FRAGMENTS: usize =
        Self::MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT - size_of::<RecordPtr>();

    /// Reads [`Record`] located at `ptr` and deserializes it.
    pub fn record(&self, ptr: &RecordPtr) -> Result<Record, HeapFileError> {
        let page_chain =
            SinglePageLockChain::<BUCKETS_COUNT, PinnedReadPage>::with_record(self, ptr.page_id)?;
        let record_bytes = self.record_bytes(ptr, page_chain)?;
        let record = Record::deserialize(&self.columns_metadata, &record_bytes)?;
        Ok(record)
    }

    /// Reads all [`Record`]s stored in heap file.
    pub fn all_records(&'_ self) -> AllRecordsIterator<'_, BUCKETS_COUNT> {
        let first_page_id = *self.metadata.first_record_page.lock();
        AllRecordsIterator::new(self, first_page_id)
    }

    /// Reads all [`Record`]s with the given record pointers.
    /// Chunks records by page id to minimize page reads/locks.
    pub fn records_from_ptrs(
        &self,
        mut record_ptrs: Vec<RecordPtr>,
    ) -> Result<Vec<RecordHandle>, HeapFileError> {
        // Sort record pointers by page id to allow for chunking consecutive records from the same
        // page.
        record_ptrs.sort_unstable_by_key(|rp| rp.page_id);

        let mut records = Vec::with_capacity(record_ptrs.len());

        // Process each chunk of records from the same page together.
        for chunk in record_ptrs.chunk_by(|r1, r2| r1.page_id == r2.page_id) {
            let page_id = chunk[0].page_id;
            let mut page = self.read_record_page(page_id)?;

            for record_ptr in chunk {
                // Create PageLockChain that reuses the already locked record page.
                let page_chain =
                    PageLockChainWithLockedRecordPage::<BUCKETS_COUNT, PinnedReadPage>::with_record(
                        self, &mut page,
                    )?;

                // Read record bytes and deserialize.
                let record_bytes = self.record_bytes(record_ptr, page_chain)?;
                let record = Record::deserialize(&self.columns_metadata, &record_bytes)?;
                records.push(RecordHandle::new(record, *record_ptr));
            }
        }

        Ok(records)
    }

    /// Inserts `record` into heap file and returns its [`RecordPtr`].
    pub fn insert(&self, record: Record) -> Result<RecordPtr, HeapFileError> {
        let serialized = record.serialize();
        self.insert_record_internal(
            serialized,
            |heap_file, data| heap_file.insert_to_record_page(data),
            |heap_file, data| heap_file.insert_to_overflow_page(data),
        )
    }

    /// Applies all updates described in `updated_fields` to [`Record`] stored at `ptr`.
    /// Starting position of updated record does not change, only the end can shrink/expand in case
    /// of record changing its size.
    pub fn update(
        &self,
        ptr: &RecordPtr,
        updated_fields: Vec<FieldUpdateDescriptor>,
    ) -> Result<(), HeapFileError> {
        // Read record bytes from disk
        let page_chain =
            SinglePageLockChain::<BUCKETS_COUNT, PinnedReadPage>::with_record(self, ptr.page_id)?;
        let mut record_bytes = self.record_bytes(ptr, page_chain)?;
        let mut bytes_changed = vec![false; record_bytes.len()];

        // Update fields
        for update in updated_fields {
            self.update_field(&mut record_bytes, &mut bytes_changed, update)?;
        }

        if !bytes_changed.iter().any(|&b| b) {
            // No change was made
            return Ok(());
        }

        // Save back to disk at the same ptr
        let page_chain =
            SinglePageLockChain::<BUCKETS_COUNT, PinnedWritePage>::with_record(self, ptr.page_id)?;
        self.update_record_bytes(ptr, &record_bytes, &bytes_changed, page_chain)?;

        Ok(())
    }

    /// Removes [`Record`] located at `ptr`.
    pub fn delete(&self, ptr: &RecordPtr) -> Result<(), HeapFileError> {
        let mut page_chain =
            SinglePageLockChain::<BUCKETS_COUNT, PinnedWritePage>::with_record(self, ptr.page_id)?;
        let first_page = page_chain.record_page_mut();
        let record_first_fragment = first_page.record_fragment(ptr.slot_id)?;

        let next_ptr = record_first_fragment.next_fragment;

        // Delete first fragment of the record
        first_page.delete(ptr.slot_id)?;

        // Follow record chain and delete each fragment
        self.delete_record_fragments_from_overflow_pages_chain(page_chain, next_ptr)?;
        Ok(())
    }

    /// Flushes [`HeapFile::metadata`] content to disk.
    ///
    /// If metadata is not dirty then this is no-op.
    pub fn flush_metadata(&mut self) -> Result<(), HeapFileError> {
        match self
            .metadata
            .dirty
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                // Changed dirty from true to false, so metadata must be flushed
                let key = self.file_page_ref(Self::METADATA_PAGE_ID);
                let mut page = self.cache.pin_write(&key)?;
                let repr = MetadataRepr::from(&self.metadata);
                let bytes = bytemuck::bytes_of(&repr);
                page.page_mut()[..size_of::<MetadataRepr>()].copy_from_slice(bytes);
                Ok(())
            }
            Err(_) => {
                // Metadata was already clean
                Ok(())
            }
        }
    }

    /// Migrates all records to add a new column at the specified position with a default value.
    pub fn add_column_migration(
        &mut self,
        position: u16,
        new_column_min_offset: usize,
        default_value: Value,
        new_column_metadata: Vec<ColumnMetadata>,
    ) -> Result<(), HeapFileError> {
        let old_columns = mem::replace(&mut self.columns_metadata, new_column_metadata);

        // Iterate through all record pages - we don't need to use PageLockChain
        // as this whole function has exclusive access to the whole heap file struct.
        let mut page_id = *self.metadata.first_record_page.lock();
        while page_id != RecordPageHeader::NO_NEXT_PAGE {
            let mut page = self.write_record_page(page_id)?;
            let next_page_id = page.next_page()?;

            let slots: Vec<_> = page.not_deleted_slot_ids()?.collect();

            // Update each record on the page
            for slot_id in slots {
                let ptr = RecordPtr::new(page_id, slot_id);
                self.add_column_to_record(
                    &mut page,
                    &ptr,
                    position,
                    new_column_min_offset,
                    default_value.clone(),
                    &old_columns,
                )?;
            }

            page_id = next_page_id;
        }

        Ok(())
    }

    /// Migrate all records to remove a column at the specified position.
    pub fn remove_column_migration(
        &mut self,
        position: u16,
        // `base_offset` of the first column before the removed one, 0 if removed column was first
        prev_column_min_offset: usize,
        new_column_metadata: Vec<ColumnMetadata>,
    ) -> Result<(), HeapFileError> {
        let old_columns = mem::replace(&mut self.columns_metadata, new_column_metadata);

        // Iterate through all record pages - we don't need to use PageLockChain
        // as this whole function has exclusive access to the whole heap file struct.
        let mut page_id = *self.metadata.first_record_page.lock();
        while page_id != RecordPageHeader::NO_NEXT_PAGE {
            let mut page = self.write_record_page(page_id)?;
            let next_page_id = page.next_page()?;

            let slots: Vec<_> = page.not_deleted_slot_ids()?.collect();

            // Update each record on the page
            for slot_id in slots {
                let ptr = RecordPtr::new(page_id, slot_id);
                self.remove_column_from_record(
                    &mut page,
                    &ptr,
                    position,
                    prev_column_min_offset,
                    &old_columns,
                )?;
            }

            page_id = next_page_id;
        }

        Ok(())
    }

    /// Reads [`Record`] located at `ptr` and returns its bare bytes.
    fn record_bytes<P>(
        &self,
        ptr: &RecordPtr,
        mut page_chain: impl PageLockChain<P>,
    ) -> Result<Vec<u8>, HeapFileError>
    where
        P: PageRead,
    {
        // Read first fragment
        let first_page = page_chain.record_page();
        let fragment = first_page.record_fragment(ptr.slot_id)?;
        let mut full_record_bytes = Vec::from(fragment.data);

        // Follow chain of other fragments
        let mut next_ptr = fragment.next_fragment;
        while let Some(next) = next_ptr {
            page_chain.advance(next.page_id)?;
            let next_fragment_page = page_chain.overflow_page();
            let fragment = next_fragment_page.record_fragment(next.slot_id)?;
            full_record_bytes.extend_from_slice(fragment.data);
            next_ptr = fragment.next_fragment;
        }
        Ok(full_record_bytes)
    }

    /// Reads all records from page.
    /// Returns the records and id of next record page.
    fn read_all_record_from_page(
        &self,
        page_id: PageId,
    ) -> Result<(PageId, VecDeque<RecordHandle>), HeapFileError> {
        let mut page = self.read_record_page(page_id)?;
        let next_page_id = page.next_page()?;

        let records_ptrs: Vec<_> = page
            .not_deleted_slot_ids()?
            .map(|slot_id| RecordPtr::new(page_id, slot_id))
            .collect();

        let mut records = VecDeque::with_capacity(records_ptrs.len());

        for ptr in records_ptrs {
            // Create PageLockChain that reuses the already locked record page.
            let page_chain =
                PageLockChainWithLockedRecordPage::<BUCKETS_COUNT, PinnedReadPage>::with_record(
                    self, &mut page,
                )?;
            let record_bytes = self.record_bytes(&ptr, page_chain)?;
            let record = Record::deserialize(&self.columns_metadata, &record_bytes)?;
            records.push_back(RecordHandle::new(record, ptr));
        }

        Ok((next_page_id, records))
    }

    /// Updates `data` bytes according to description in `update`.
    fn update_field(
        &self,
        data: &mut Vec<u8>,
        bytes_changed: &mut Vec<bool>,
        update: FieldUpdateDescriptor,
    ) -> Result<(), HeapFileError> {
        match update.column.ty().is_fixed_size() {
            true => {
                // If column is fixed-sized we know it's offset in the record bytes just from metadata,
                // we just need to update bytes at that offset.
                let offset = update.column.base_offset();

                bytes_changed[offset..(offset + update.value.size_serialized())].fill(true);

                update.value.serialize_into(&mut data[offset..]);
                Ok(())
            }
            false => {
                // If column is variable-sized then we need to calculate it offset by hand.
                // Record can have multiple variable-sized fields next to each other, e.g.:
                // [int32][string1][string2][string3]
                // Let's assume we want to edit [string3]. Its base_offset is 4 (because of int32),
                // its base_pos is 1 and its pos is 3.
                // We need to calculate offset by hand starting from pos 1 until we reach pos 3.
                let mut current_pos = update.column.base_offset_pos();
                let mut offset = update.column.base_offset();
                while current_pos != update.column.pos() {
                    let (len, _) = u16::deserialize(&data[offset..(offset + size_of::<u16>())])?;
                    offset += size_of::<u16>() + len as usize;
                    current_pos += 1;
                }
                let (before_update_len, _) =
                    u16::deserialize(&data[offset..(offset + size_of::<u16>())])?;
                let before_update_total_size = size_of::<u16>() + before_update_len as usize;

                let mut serialized = Vec::with_capacity(update.value.size_serialized());
                update.value.serialize(&mut serialized);

                let bytes_changed_frag = vec![true; serialized.len()];
                bytes_changed.splice(
                    offset..(offset + before_update_total_size),
                    bytes_changed_frag,
                );
                // When we update a string and its len changes we must update all values that come after it
                if serialized.len() != before_update_total_size {
                    bytes_changed[(offset + serialized.len())..].fill(true);
                }

                data.splice(offset..(offset + before_update_total_size), serialized);
                Ok(())
            }
        }
    }

    /// Updates record bytes at `start` (only those bytes for which flag is set to `true` in `bytes_changed`), following the same split across pages as previous value, e.g.
    /// if record was splitted [page1 -> 80bytes], [page2 -> 24bytes], [page3 -> 12bytes]
    /// then this is preserved if record has the same length.
    /// If length is different then the difference is applied at the end of pages chain.
    ///
    /// This function assumes that `bytes` and `bytes_changed` have the same length.
    fn update_record_bytes<P>(
        &self,
        start: &RecordPtr,
        bytes: &[u8],
        bytes_changed: &[bool],
        mut page_chain: impl PageLockChain<P>,
    ) -> Result<(), HeapFileError>
    where
        P: PageRead + PageWrite,
    {
        let mut processor = FragmentProcessor::new(bytes, bytes_changed);

        let first_page = page_chain.record_page_mut();
        let previous_first_fragment = first_page.record_fragment(start.slot_id)?;

        // Previously record was stored on single page.
        if previous_first_fragment.next_fragment.is_none() {
            let previous_len = previous_first_fragment.data.len();

            // Check if there are any changes in the entire record
            if !processor.has_changes(processor.remaining_bytes.len()) {
                // No changes at all
                return Ok(());
            }

            // Try to update in place
            if self.try_update_fragment_in_place(
                first_page,
                start.slot_id,
                start.page_id,
                processor.remaining_bytes,
                previous_len,
                &self.record_pages_fsm,
            )? {
                return Ok(());
            }

            // Record cannot fit in this page. We store as much as before (`- RecordPtr` to be sure continuation will fit)
            // in the first page and save `rest` in other page(s).
            self.split_and_extend_fragment(
                first_page,
                start.slot_id,
                start.page_id,
                processor.remaining_bytes,
                previous_len,
                &self.record_pages_fsm,
            )?;
            return Ok(());
        }

        let first_frag_len = previous_first_fragment.data.len();
        // We already checked and know that `next_fragment` cannot be `None`
        let next_tag = previous_first_fragment.next_fragment.unwrap();

        // Check if first fragment has any changes or if the record now needs fewer fragments
        if processor.has_changes(first_frag_len) || processor.is_last_fragment(first_frag_len) {
            // Record was stored across more than one page, but it now can be saved only in first page
            if processor.is_last_fragment(first_frag_len) {
                let with_tag = RecordTag::with_final(processor.remaining_bytes);
                self.update_record_page_with_fsm(
                    first_page,
                    start.slot_id,
                    &with_tag,
                    start.page_id,
                )?;
                self.delete_record_fragments_from_overflow_pages_chain(page_chain, Some(next_tag))?;
                return Ok(());
            }

            // Update fragment located on first page
            let (first_page_part, _) = processor.remaining_bytes.split_at(first_frag_len);

            let first_page_part_with_tag =
                RecordTag::with_has_continuation(first_page_part, &next_tag);
            self.update_record_page_with_fsm(
                first_page,
                start.slot_id,
                &first_page_part_with_tag,
                start.page_id,
            )?;
        }

        // Skip first fragment after processing it
        processor.skip_fragment(first_frag_len);
        let mut current_ptr = next_tag;

        while !processor.is_complete() {
            page_chain.advance(current_ptr.page_id)?;
            let current_page = page_chain.overflow_page_mut();
            let current_fragment = current_page.record_fragment(current_ptr.slot_id)?;

            let current_frag_len = current_fragment.data.len();
            let next_frag = current_fragment.next_fragment;

            if !processor.has_changes(current_frag_len) {
                // This was the last fragment with no changes
                if processor.is_last_fragment(current_frag_len) {
                    return Ok(());
                }

                processor.skip_fragment(current_frag_len);

                match next_frag {
                    Some(next_ptr) => {
                        current_ptr = next_ptr;
                        continue;
                    }
                    None => {
                        // No more changes to made
                        if processor.is_complete() {
                            return Ok(());
                        }
                        // Need to allocate new fragments for remaining changed bytes
                        let rest_ptr = self.insert_using_only_overflow_pages(Vec::from(
                            processor.remaining_bytes,
                        ))?;
                        // Update current fragment to point to new chain
                        let current_data = current_fragment.data;
                        let current_with_tag =
                            RecordTag::with_has_continuation(current_data, &rest_ptr);
                        current_page.update(current_ptr.slot_id, &current_with_tag)?;
                        return Ok(());
                    }
                }
            }

            // Try to update in place
            if self.try_update_fragment_in_place(
                current_page,
                current_ptr.slot_id,
                current_ptr.page_id,
                processor.remaining_bytes,
                current_frag_len,
                &self.overflow_pages_fsm,
            )? {
                self.delete_record_fragments_from_overflow_pages_chain(page_chain, next_frag)?;
                return Ok(());
            }

            // At this point we know that remaining_bytes > current_fragment
            match next_frag {
                Some(next_ptr) => {
                    // In this case we have next_ptr, so we update bytes on this page and go to the next one
                    let (current_page_part, _) =
                        processor.remaining_bytes.split_at(current_frag_len);
                    let current_with_tag =
                        RecordTag::with_has_continuation(current_page_part, &next_ptr);
                    current_page.update(current_ptr.slot_id, &current_with_tag)?;
                    current_ptr = next_ptr;
                }
                None => {
                    // This is the last fragment from original chain, so additional bytes can be added wherever we want
                    self.split_and_extend_fragment(
                        current_page,
                        current_ptr.slot_id,
                        current_ptr.page_id,
                        processor.remaining_bytes,
                        current_frag_len,
                        &self.overflow_pages_fsm,
                    )?;
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Attempts to update a fragment with new data, first trying to fit it in available space,
    /// then trying to fit it as a single fragment if possible.
    ///
    /// Returns `Ok(true)` if update was successful, `Ok(false)` if there wasn't enough space
    /// and the caller should handle splitting the record.
    fn try_update_fragment_in_place<P, H>(
        &self,
        page: &mut HeapPage<P, H>,
        slot: SlotId,
        page_id: PageId,
        remaining_bytes: &[u8],
        previous_len: usize,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<bool, HeapFileError>
    where
        P: PageRead + PageWrite,
        H: BaseHeapPageHeader,
    {
        // We can safely store updated record at previous position because we know we have enough space
        if remaining_bytes.len() <= previous_len {
            let with_tag = RecordTag::with_final(remaining_bytes);
            self.update_page_with_fsm(page, slot, &with_tag, page_id, fsm)?;
            return Ok(true);
        }

        // We need more space, but potentially record can fit in this page (it can fail if page has other record inserted)
        if remaining_bytes.len() <= Self::MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT {
            let with_tag = RecordTag::with_final(remaining_bytes);
            match self.update_page_with_fsm(page, slot, &with_tag, page_id, fsm) {
                Ok(_) => {
                    return Ok(true);
                }
                Err(e) => match e {
                    // There wasn't enough space, so we need to split the record
                    HeapFileError::NotEnoughSpaceOnPage => {
                        return Ok(false);
                    }
                    _ => return Err(e),
                },
            }
        }

        Ok(false)
    }

    /// Converts a Final fragment to HasContinuation by splitting the data and allocating
    /// overflow pages for the remaining bytes.
    ///
    /// Takes the maximum amount of data that can fit in the current fragment (accounting for
    /// the RecordPtr overhead), stores it with a continuation tag, and recursively stores
    /// the remaining data in overflow pages.
    fn split_and_extend_fragment<P, H>(
        &self,
        page: &mut HeapPage<P, H>,
        slot: SlotId,
        page_id: PageId,
        remaining_bytes: &[u8],
        previous_fragment_len: usize,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<(), HeapFileError>
    where
        P: PageRead + PageWrite,
        H: BaseHeapPageHeader,
    {
        // When converting from Final to HasContinuation, we need space for RecordPtr
        // So we can only keep (previous_fragment_len - size_of::<RecordPtr>()) bytes in this fragment
        let space_for_data = previous_fragment_len.saturating_sub(size_of::<RecordPtr>());

        let (current_page_part, rest) = remaining_bytes.split_at(space_for_data);
        let rest_ptr = self.insert_using_only_overflow_pages(Vec::from(rest))?;
        let current_with_tag = RecordTag::with_has_continuation(current_page_part, &rest_ptr);

        self.update_page_with_fsm(page, slot, &current_with_tag, page_id, fsm)?;
        Ok(())
    }

    /// Inserts `data` into heap file using only overflow pages and returns its [`RecordPtr`].
    /// It is intended to use during update operation.
    /// We use new allocation, because otherwise we end up with deadlock as we hold write-lock to page and fsm wants to get it as well.
    fn insert_using_only_overflow_pages(
        &self,
        serialized: Vec<u8>,
    ) -> Result<RecordPtr, HeapFileError> {
        self.insert_record_internal(
            serialized,
            |heap_file, data| heap_file.insert_to_overflow_with_new_allocation(data),
            |heap_file, data| heap_file.insert_to_overflow_with_new_allocation(data),
        )
    }

    /// Same as [`Self::insert_to_overflow_page`], but it doesn't check fsm and always allocates page.
    fn insert_to_overflow_with_new_allocation(
        &self,
        data: &[u8],
    ) -> Result<RecordPtr, HeapFileError> {
        let (page_id, mut page) = self.allocate_overflow_page()?;

        let slot_id = match page.insert(data) {
            Ok(slot_id) => slot_id,
            Err(HeapFileError::NotEnoughSpaceOnPage) => {
                panic!("fatal error - not enough space returned on page from FSM/allocated page")
            }
            Err(e) => return Err(e),
        };

        self.overflow_pages_fsm
            .update_page_bucket(page_id, page.page.free_space()? as _);

        Ok(RecordPtr { page_id, slot_id })
    }

    /// Deletes all record fragments from chain of overflow pages starting in `next_ptr`.
    fn delete_record_fragments_from_overflow_pages_chain<P>(
        &self,
        mut page_chain: impl PageLockChain<P>,
        mut next_ptr: Option<RecordPtr>,
    ) -> Result<(), HeapFileError>
    where
        P: PageRead + PageWrite,
    {
        while let Some(next) = next_ptr {
            page_chain.advance(next.page_id)?;
            let page = page_chain.overflow_page_mut();
            let record_fragment = page.record_fragment(next.slot_id)?;
            next_ptr = record_fragment.next_fragment;
            page.delete(next.slot_id)?;
        }
        Ok(())
    }

    /// Generic helper for reading any type of heap page
    fn read_heap_page<H>(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, H>, HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let file_page_ref = self.file_page_ref(page_id);
        let page = self.cache.pin_read(&file_page_ref)?;
        let slotted_page = SlottedPage::new(page)?;
        let heap_node = HeapPage::new(slotted_page);
        Ok(heap_node)
    }

    /// Generic helper for reading a page and updating its FSM
    fn read_page_with_fsm<H>(
        &self,
        page_id: PageId,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<HeapPage<PinnedReadPage, H>, HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let heap_page = self.read_heap_page::<H>(page_id)?;
        let free_space = heap_page.page.free_space()?;
        fsm.update_page_bucket(page_id, free_space as _);
        Ok(heap_page)
    }

    fn read_record_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, RecordPageHeader>, HeapFileError> {
        self.read_page_with_fsm(page_id, &self.record_pages_fsm)
    }

    fn read_overflow_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, OverflowPageHeader>, HeapFileError> {
        self.read_page_with_fsm(page_id, &self.overflow_pages_fsm)
    }

    /// Generic helper for getting a write-locked version of any heap page type
    fn write_heap_page<H>(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedWritePage, H>, HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let file_page_ref = self.file_page_ref(page_id);
        let page = self.cache.pin_write(&file_page_ref)?;
        let slotted_page = SlottedPage::new(page)?;
        let heap_node = HeapPage::new(slotted_page);
        Ok(heap_node)
    }

    fn write_record_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedWritePage, RecordPageHeader>, HeapFileError> {
        self.write_heap_page(page_id)
    }

    fn write_overflow_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedWritePage, OverflowPageHeader>, HeapFileError> {
        self.write_heap_page(page_id)
    }

    /// Generic helper for allocating a page and updating metadata
    fn allocate_page_with_metadata<H, F>(
        &self,
        metadata_lock: &Mutex<PageId>,
        header_fn: F,
    ) -> Result<(PageId, HeapPage<PinnedWritePage, H>), HeapFileError>
    where
        H: BaseHeapPageHeader,
        F: FnOnce(PageId) -> H,
    {
        let (page, page_id) = self.cache.allocate_page(&self.file_key)?;

        let old_first_page = {
            let mut metadata_page_lock = metadata_lock.lock();
            let old = *metadata_page_lock;
            *metadata_page_lock = page_id;
            self.metadata.dirty.store(true, Ordering::Release);
            old
        };

        let header = header_fn(old_first_page);
        let slotted_page = SlottedPage::initialize_with_header(page, header)?;
        let heap_page = HeapPage::new(slotted_page);

        Ok((page_id, heap_page))
    }

    fn allocate_record_page(
        &self,
    ) -> Result<(PageId, HeapPage<PinnedWritePage, RecordPageHeader>), HeapFileError> {
        self.allocate_page_with_metadata(&self.metadata.first_record_page, |next_page| {
            RecordPageHeader::new(next_page)
        })
    }

    fn allocate_overflow_page(
        &self,
    ) -> Result<(PageId, HeapPage<PinnedWritePage, OverflowPageHeader>), HeapFileError> {
        self.allocate_page_with_metadata(&self.metadata.first_overflow_page, |next_page| {
            OverflowPageHeader::new(next_page)
        })
    }

    /// Generic helper for inserting data into any type of heap page
    fn insert_to_page<H>(
        &self,
        data: &[u8],
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
        allocate_fn: impl FnOnce() -> Result<(PageId, HeapPage<PinnedWritePage, H>), HeapFileError>,
    ) -> Result<RecordPtr, HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let total_record_size = data.len() + size_of::<Slot>();
        let fsm_page = fsm.page_with_free_space(total_record_size)?;
        let (page_id, mut page) = if let Some((page_id, page)) = fsm_page {
            let heap_page = HeapPage::new(page);
            (page_id, heap_page)
        } else {
            allocate_fn()?
        };

        let slot_id = match page.insert(data) {
            Ok(slot_id) => slot_id,
            Err(HeapFileError::NotEnoughSpaceOnPage) => {
                panic!("fatal error - not enough space returned on page from FSM/allocated page")
            }
            Err(e) => return Err(e),
        };

        fsm.update_page_bucket(page_id, page.page.free_space()? as _);

        Ok(RecordPtr { page_id, slot_id })
    }

    /// Inserts `data` into first found record page that has enough size.
    fn insert_to_record_page(&self, data: &[u8]) -> Result<RecordPtr, HeapFileError> {
        self.insert_to_page(data, &self.record_pages_fsm, || self.allocate_record_page())
    }

    /// Inserts `data` into first found overflow page that has enough size.
    fn insert_to_overflow_page(&self, data: &[u8]) -> Result<RecordPtr, HeapFileError> {
        self.insert_to_page(data, &self.overflow_pages_fsm, || {
            self.allocate_overflow_page()
        })
    }

    /// Generic helper for inserting serialized record data using first-fragment-insert strategy
    ///
    /// In insert we don't need to use our lock strategy (get next then drop previous), because in insert we write record
    /// starting from the end of it. It means that once we write the first fragment in record page, the rest is already there
    /// and we can safely drop it. During inserts to overflow pages we don't need this locking strategy, as heap file is still not
    /// aware of this record (because its first fragment hasn't been inserted yet).
    fn insert_record_internal<F, G>(
        &self,
        mut serialized: Vec<u8>,
        insert_first_fragment: F,
        insert_next_fragments: G,
    ) -> Result<RecordPtr, HeapFileError>
    where
        F: FnOnce(&Self, &[u8]) -> Result<RecordPtr, HeapFileError>,
        G: Fn(&Self, &[u8]) -> Result<RecordPtr, HeapFileError>,
    {
        if serialized.len() <= Self::MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT {
            // Record can be stored on single page
            let data = RecordTag::with_final(&serialized);
            return insert_first_fragment(self, &data);
        }

        // We need to split record into pieces, so that each piece can fit into one page.
        let piece_size = Self::MAX_RECORD_SIZE_WHEN_MANY_FRAGMENTS;

        // At this point we know that `serialized.len() > Self::MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT`,
        // `Self::MAX_RECORD_SIZE_WHEN_ONE_FRAGMENT > Self::MAX_RECORD_SIZE_WHEN_MANY_FRAGMENTS `,
        // so `serialized.len() Self::MAX_RECORD_SIZE_WHEN_MANY_FRAGMENTS`

        let last_fragment = serialized.split_off(serialized.len() - 1 - piece_size);
        let last_fragment_with_tag = RecordTag::with_final(&last_fragment);
        let mut next_ptr = insert_next_fragments(self, &last_fragment_with_tag)?;

        // It means 2 pieces is enough
        if serialized.len() <= piece_size {
            let data = RecordTag::with_has_continuation(&serialized, &next_ptr);
            return insert_first_fragment(self, &data);
        }

        // We need to split it into more than 2 pieces
        let middle_pieces = serialized.split_off(piece_size);

        // We save middle pieces into overflow pages
        for chunk in middle_pieces.rchunks(piece_size as _) {
            let chunk = RecordTag::with_has_continuation(chunk, &next_ptr);
            next_ptr = insert_next_fragments(self, &chunk)?;
        }

        let data = RecordTag::with_has_continuation(&serialized, &next_ptr);
        insert_first_fragment(self, &data)
    }

    /// Generic helper for updating a record and registering the page in FSM
    fn update_page_with_fsm<P, H>(
        &self,
        page: &mut HeapPage<P, H>,
        slot: SlotId,
        data: &[u8],
        page_id: PageId,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<(), HeapFileError>
    where
        P: PageRead + PageWrite,
        H: BaseHeapPageHeader,
    {
        page.update(slot, data)?;
        fsm.update_page_bucket(page_id, page.page.free_space()? as _);
        Ok(())
    }

    /// Updates a record on a record page and updates FSM
    fn update_record_page_with_fsm<P>(
        &self,
        page: &mut HeapPage<P, RecordPageHeader>,
        slot: SlotId,
        data: &[u8],
        page_id: PageId,
    ) -> Result<(), HeapFileError>
    where
        P: PageRead + PageWrite,
    {
        self.update_page_with_fsm(page, slot, data, page_id, &self.record_pages_fsm)
    }

    /// Adds a column to a single record at `ptr` and writes updated value to the disk
    fn add_column_to_record(
        &self,
        page: &mut HeapPage<PinnedWritePage, RecordPageHeader>,
        ptr: &RecordPtr,
        position: u16,
        new_column_min_offset: usize,
        default_value: Value,
        old_columns: &[ColumnMetadata],
    ) -> Result<(), HeapFileError> {
        // Read the record with the old schema
        let page_chain = PageLockChainWithLockedRecordPage::with_record(self, page)?;
        let record_bytes = self.record_bytes(ptr, page_chain)?;
        let record = Record::deserialize(old_columns, &record_bytes)?;

        let mut fields = record.fields;
        fields.insert(position as _, default_value.into());

        // Serialize with the new schema
        let new_record = Record::new(fields);
        let new_bytes = new_record.serialize();

        let mut bytes_changed = vec![false; new_bytes.len()];
        bytes_changed[new_column_min_offset..].fill(true);

        let page_chain = PageLockChainWithLockedRecordPage::with_record(self, page)?;
        self.update_record_bytes(ptr, &new_bytes, &bytes_changed, page_chain)?;

        Ok(())
    }

    /// Removes a column from a single record at `ptr` and writes updated value to the disk.
    fn remove_column_from_record(
        &self,
        page: &mut HeapPage<PinnedWritePage, RecordPageHeader>,
        ptr: &RecordPtr,
        position: u16,
        prev_column_min_offset: usize,
        old_columns: &[ColumnMetadata],
    ) -> Result<(), HeapFileError> {
        // Read the record with the old schema
        let page_chain = PageLockChainWithLockedRecordPage::with_record(self, page)?;
        let record_bytes = self.record_bytes(ptr, page_chain)?;
        let record = Record::deserialize(old_columns, &record_bytes)?;

        let mut fields = record.fields;
        fields.remove(position as _);

        // Serialize with the new schema
        let new_record = Record::new(fields);
        let new_bytes = new_record.serialize();

        let mut bytes_changed = vec![false; new_bytes.len()];
        bytes_changed[prev_column_min_offset..].fill(true);

        let page_chain = PageLockChainWithLockedRecordPage::with_record(self, page)?;
        self.update_record_bytes(ptr, &new_bytes, &bytes_changed, page_chain)?;

        Ok(())
    }

    /// Creates a `FilePageRef` for `page_id` using heap file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef::new(page_id, self.file_key.clone())
    }
}

impl<const BUCKETS_COUNT: usize> Drop for HeapFile<BUCKETS_COUNT> {
    fn drop(&mut self) {
        if let Err(e) = self.flush_metadata() {
            log::error!("failed to flush metadata while dropping HeapFile: {e}");
        }
    }
}

/// Factory responsible for creating and loading existing [`HeapFile`].
pub struct HeapFileFactory<const BUCKETS_COUNT: usize> {
    file_key: FileKey,
    cache: Arc<Cache>,
    columns_metadata: Vec<ColumnMetadata>,
}

impl<const BUCKETS_COUNT: usize> HeapFileFactory<BUCKETS_COUNT> {
    pub fn new(
        file_key: FileKey,
        cache: Arc<Cache>,
        columns_metadata: Vec<ColumnMetadata>,
    ) -> Self {
        HeapFileFactory {
            file_key,
            cache,
            columns_metadata,
        }
    }

    pub fn create_heap_file(self) -> Result<HeapFile<BUCKETS_COUNT>, HeapFileError> {
        let metadata_repr = self.load_metadata_repr()?;

        let record_pages_fsm = FreeSpaceMap::<BUCKETS_COUNT, RecordPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
            metadata_repr.first_record_page,
        );
        let overflow_pages_fsm = FreeSpaceMap::<BUCKETS_COUNT, OverflowPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
            metadata_repr.first_overflow_page,
        );

        let metadata = Metadata::from(&metadata_repr);

        let heap_file = HeapFile {
            cache: self.cache,
            columns_metadata: self.columns_metadata,
            file_key: self.file_key,
            metadata,
            record_pages_fsm,
            overflow_pages_fsm,
        };
        Ok(heap_file)
    }

    fn load_metadata_repr(&self) -> Result<MetadataRepr, HeapFileError> {
        let key = self.file_page_ref(HeapFile::<BUCKETS_COUNT>::METADATA_PAGE_ID);
        match self.cache.pin_read(&key) {
            Ok(page) => {
                let metadata = MetadataRepr::load_from_page(page.page())?;
                Ok(metadata)
            }
            Err(e) => {
                if let CacheError::PagedFileError(PagedFileError::InvalidPageId(HeapFile::<
                    BUCKETS_COUNT,
                >::METADATA_PAGE_ID)) = e
                {
                    // This mean that metadata page was not allocated yet, so the file was just created
                    let (mut metadata_page, metadata_page_id) =
                        self.cache.allocate_page(key.file_key())?;
                    debug_assert_eq!(
                        metadata_page_id,
                        HeapFile::<BUCKETS_COUNT>::METADATA_PAGE_ID
                    );

                    let (record_page, record_page_id) = self.cache.allocate_page(key.file_key())?;
                    SlottedPage::<_, RecordPageHeader>::initialize_default(record_page).unwrap();

                    let (overflow_page, overflow_page_id) =
                        self.cache.allocate_page(key.file_key())?;
                    SlottedPage::<_, OverflowPageHeader>::initialize_default(overflow_page)
                        .unwrap();

                    let repr = MetadataRepr {
                        first_record_page: record_page_id,
                        first_overflow_page: overflow_page_id,
                    };

                    let bytes = bytemuck::bytes_of(&repr);
                    metadata_page.page_mut()[..size_of::<MetadataRepr>()].copy_from_slice(bytes);

                    Ok(repr)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Creates a `FilePageRef` for `page_id` using heap file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef::new(page_id, self.file_key.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        fs,
        ops::Deref,
        sync::atomic::AtomicUsize,
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use tempfile::tempdir;
    use types::schema::Type;

    use crate::{
        record::Field,
        slotted_page::{InsertResult, PageType},
    };

    use storage::files_manager::FilesManager;

    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Pod, Zeroable)]
    struct TestPageHeader {
        base: SlottedPageBaseHeader,
    }

    impl BaseHeapPageHeader for TestPageHeader {
        fn next_page(&self) -> PageId {
            Self::NO_NEXT_PAGE
        }
    }

    unsafe impl ReprC for TestPageHeader {}

    impl SlottedPageHeader for TestPageHeader {
        fn base(&self) -> &SlottedPageBaseHeader {
            &self.base
        }
    }

    impl Default for TestPageHeader {
        fn default() -> Self {
            Self {
                base: SlottedPageBaseHeader::new(
                    size_of::<TestPageHeader>() as u16,
                    PageType::Generic,
                ),
            }
        }
    }

    /// Helpers for asserting field type
    ///
    fn assert_string(expected: &str, actual: &Field) {
        match actual.deref() {
            Value::String(s) => {
                assert_eq!(expected, *s);
            }
            _ => panic!("expected String, got {:?}", actual),
        }
    }

    fn assert_i32(expected: i32, actual: &Field) {
        match actual.deref() {
            Value::Int32(i) => {
                assert_eq!(expected, *i);
            }
            _ => panic!("expected Int32, got {:?}", actual),
        }
    }

    fn assert_i64(expected: i64, actual: &Field) {
        match actual.deref() {
            Value::Int64(i) => {
                assert_eq!(expected, *i);
            }
            _ => panic!("expected Int64, got {:?}", actual),
        }
    }

    // TODO: This fails to drop the dir after tests are finished.
    // We need to figure it out somehow.
    /// Creates a test cache and files manager in a temporary directory
    fn setup_test_cache() -> (Arc<Cache>, Arc<FilesManager>, FileKey) {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().join("test_db");
        fs::create_dir_all(&db_dir).unwrap();

        let files_manager = Arc::new(FilesManager::new(db_dir).unwrap());
        let cache = Cache::new(100, files_manager.clone());

        // Use a unique file name for each test to avoid conflicts
        let file_key = FileKey::data(format!(
            "test_heap_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        (cache, files_manager, file_key)
    }

    /// Creates a new FSM for testing
    fn create_test_fsm<const BUCKETS: usize>(
        cache: Arc<Cache>,
        file_key: FileKey,
        first_page_id: PageId,
    ) -> FreeSpaceMap<BUCKETS, TestPageHeader> {
        FreeSpaceMap::new(cache, file_key, first_page_id)
    }

    /// Creates a new FSM for testing with RecordPageHeader
    fn create_test_fsm_record_page<const BUCKETS: usize>(
        cache: Arc<Cache>,
        file_key: FileKey,
        first_page_id: PageId,
    ) -> FreeSpaceMap<BUCKETS, RecordPageHeader> {
        FreeSpaceMap::new(cache, file_key, first_page_id)
    }

    /// Allocates a page and initializes it with a slotted page header with specific free space
    fn create_page_with_free_space(
        cache: &Arc<Cache>,
        file_key: &FileKey,
        free_space: usize,
    ) -> PageId {
        let (pinned_page, page_id) = cache.allocate_page(file_key).unwrap();

        // Initialize the page with a slotted page structure
        let mut slotted =
            SlottedPage::<_, TestPageHeader>::initialize_default(pinned_page).unwrap();

        // Fill the page to achieve desired free space
        // It will actually have a little less space, as some space will be taken by the slot itself.
        let current_free = slotted.free_space().unwrap();

        if current_free as usize > free_space {
            let fill_size = current_free as usize - free_space;
            let dummy_data = vec![0u8; fill_size];
            let _ = slotted.insert(&dummy_data);
        }

        page_id
    }

    fn bucket_contains_page<const BUCKETS: usize, H: BaseHeapPageHeader>(
        fsm: &FreeSpaceMap<BUCKETS, H>,
        bucket_id: usize,
        page_id: PageId,
    ) -> bool {
        let mut found = false;
        let mut pages = Vec::new();

        // We need to do it like that because SeqQueue does not have method `contains`.
        while let Some(id) = fsm.buckets[bucket_id].pop() {
            if id == page_id {
                // We don't just break here, because in some tests we check the order.
                found = true;
            }
            pages.push(id);
        }

        // Restore the pages
        for id in pages.into_iter().rev() {
            fsm.buckets[bucket_id].push(id);
        }

        found
    }

    /// Sets up a complete heap file with metadata page and initial record/overflow pages
    fn setup_heap_file_structure(cache: &Arc<Cache>, file_key: &FileKey) -> (PageId, PageId) {
        // Allocate pages
        let (mut metadata_pinned, metadata_page_id) = cache.allocate_page(file_key).unwrap();
        assert_eq!(metadata_page_id, HeapFile::<4>::METADATA_PAGE_ID);

        let (record_pinned, first_record_page_id) = cache.allocate_page(file_key).unwrap();

        let (overflow_pinned, first_overflow_page_id) = cache.allocate_page(file_key).unwrap();

        // Initialize pages
        SlottedPage::<_, RecordPageHeader>::initialize_default(record_pinned).unwrap();

        SlottedPage::<_, OverflowPageHeader>::initialize_default(overflow_pinned).unwrap();

        // Write metadata
        let metadata_repr = MetadataRepr {
            first_record_page: first_record_page_id,
            first_overflow_page: first_overflow_page_id,
        };

        let metadata_bytes = bytemuck::bytes_of(&metadata_repr);
        metadata_pinned.page_mut()[..metadata_bytes.len()].copy_from_slice(metadata_bytes);

        (first_record_page_id, first_overflow_page_id)
    }

    /// Creates a heap file factory with minimal column metadata for testing
    fn create_test_heap_file_factory(cache: Arc<Cache>, file_key: FileKey) -> HeapFileFactory<4> {
        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        HeapFileFactory::new(file_key, cache, columns_metadata)
    }

    /// Helper to insert a record fragment into a page and return the slot id
    fn insert_record_fragment(
        cache: &Arc<Cache>,
        file_key: &FileKey,
        page_id: PageId,
        data: &[u8],
        next_fragment: Option<RecordPtr>,
    ) -> Result<SlotId, HeapFileError> {
        let file_page_ref = FilePageRef::new(page_id, file_key.clone());
        let pinned = cache.pin_write(&file_page_ref)?;
        let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned)?;

        let mut fragment_data = Vec::new();

        match next_fragment {
            None => {
                fragment_data.push(RecordTag::Final as u8);
            }
            Some(ptr) => {
                fragment_data.push(RecordTag::HasContinuation as u8);
                fragment_data.extend_from_slice(&ptr.page_id.to_le_bytes());
                fragment_data.extend_from_slice(&ptr.slot_id.to_le_bytes());
            }
        }
        fragment_data.extend_from_slice(data);

        let result = slotted.insert(&fragment_data)?;
        match result {
            InsertResult::Success(slot_id) => Ok(slot_id),
            _ => panic!(),
        }
    }

    /// Helper to create a simple serialized record for testing
    fn create_test_record_data() -> Vec<u8> {
        create_custom_record_data(42, "name")
    }

    /// Helper to create a record with specific values
    fn create_custom_record_data(id: i32, name: &str) -> Vec<u8> {
        let id_field = Value::Int32(id).into();
        let name_field = Value::String(name.into()).into();

        let record = Record::new(vec![id_field, name_field]);

        record.serialize()
    }

    /// Helper to collect all records from iterator, unwrapping errors
    fn collect_all_records(heap_file: &HeapFile<4>) -> Vec<Record> {
        heap_file
            .all_records()
            .map(|result| result.unwrap().record)
            .collect()
    }

    // FSM

    #[test]
    fn fsm_bucket_for_space_empty_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, TestPageHeader::NO_NEXT_PAGE);

        // Empty page (100% free) should go to last bucket
        let bucket = fsm.bucket_for_space(PAGE_SIZE);
        assert_eq!(bucket, 3);
    }

    #[test]
    fn fsm_bucket_for_space_full_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, TestPageHeader::NO_NEXT_PAGE);

        // Full page (0% free) should go to first bucket
        let bucket = fsm.bucket_for_space(0);
        assert_eq!(bucket, 0);
    }

    #[test]
    fn fsm_bucket_for_space_quarter_ranges() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, TestPageHeader::NO_NEXT_PAGE);

        // [0%, 25%) -> bucket 0
        assert_eq!(fsm.bucket_for_space(0), 0);
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE / 4 - 1), 0);

        // [25%, 50%) -> bucket 1
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE / 4), 1);
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE / 2 - 1), 1);

        // [50%, 75%) -> bucket 2
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE / 2), 2);
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE * 3 / 4 - 1), 2);

        // [75%, 100%] -> bucket 3
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE * 3 / 4), 3);
        assert_eq!(fsm.bucket_for_space(PAGE_SIZE), 3);
    }

    #[test]
    fn fsm_update_page_bucket_adds_to_correct_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        let (_, page_id) = cache.allocate_page(&file_key).unwrap();

        // Add page with 30% free space (should go to bucket 1)
        let free_space = PAGE_SIZE * 30 / 100;
        fsm.update_page_bucket(page_id, free_space);

        let bucket_idx = fsm.bucket_for_space(free_space);
        assert_eq!(bucket_idx, 1);
        assert!(bucket_contains_page(&fsm, bucket_idx, page_id));
    }

    #[test]
    fn fsm_update_page_bucket_multiple_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create pages with different free space amounts
        let pages_and_spaces = vec![
            (PAGE_SIZE * 10 / 100, 0), // 10% -> bucket 0
            (PAGE_SIZE * 35 / 100, 1), // 35% -> bucket 1
            (PAGE_SIZE * 60 / 100, 2), // 60% -> bucket 2
            (PAGE_SIZE * 90 / 100, 3), // 90% -> bucket 3
        ];

        for (free_space, expected_bucket) in pages_and_spaces {
            let (_, page_id) = cache.allocate_page(&file_key).unwrap();
            fsm.update_page_bucket(page_id, free_space);
            assert!(bucket_contains_page(&fsm, expected_bucket, page_id));
        }
    }

    #[test]
    fn fsm_page_with_free_space_empty_fsm() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, TestPageHeader::NO_NEXT_PAGE);

        // Should return None when FSM is empty
        let result = fsm.page_with_free_space(100).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_page_with_free_space_finds_suitable_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create a page with 2000 bytes free
        let free_space = 2000;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space);
        fsm.update_page_bucket(page_id, free_space);

        // Request page with 1500 bytes free - should find just created page
        let result = fsm.page_with_free_space(1500).unwrap();
        assert!(result.is_some());

        let (_, found_page) = result.unwrap();
        let actual_free = found_page.free_space().unwrap() as usize;
        assert!(actual_free > 1500);
    }

    #[test]
    fn fsm_page_with_free_space_skips_insufficient_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create pages with different free space amounts
        let small_free = 500;
        let large_free = 2000;

        let small_page_id = create_page_with_free_space(&cache, &file_key, small_free);
        let large_page_id = create_page_with_free_space(&cache, &file_key, large_free);

        fsm.update_page_bucket(small_page_id, small_free);
        fsm.update_page_bucket(large_page_id, large_free);

        // Request 1500 bytes - should skip small page and find large page
        let result = fsm.page_with_free_space(1500).unwrap();
        assert!(result.is_some());

        let (_, found_page) = result.unwrap();
        let actual_free = found_page.free_space().unwrap() as usize;
        assert!(actual_free > 1500);
    }

    #[test]
    fn fsm_page_with_free_space_searches_higher_buckets() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Add page to bucket 3 (high free space)
        let free_space = PAGE_SIZE * 90 / 100;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space);
        fsm.update_page_bucket(page_id, free_space);

        // Request space that maps to bucket 1, should search up to bucket 3
        let needed = PAGE_SIZE * 30 / 100;
        let result = fsm.page_with_free_space(needed).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn fsm_page_with_free_space_no_suitable_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Add page with small free space
        let free_space = 100;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space);
        fsm.update_page_bucket(page_id, free_space);

        // Request more space than available
        let result = fsm.page_with_free_space(3000).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_handles_stale_entries() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create page with initial free space
        let initial_free = 2000;
        let page_id = create_page_with_free_space(&cache, &file_key, initial_free);
        fsm.update_page_bucket(page_id, initial_free);

        // Modify the page to reduce free space
        {
            let file_page_ref = FilePageRef::new(page_id, file_key.clone());
            let pinned = cache.pin_write(&file_page_ref).unwrap();
            let mut slotted = SlottedPage::<_, TestPageHeader>::new(pinned).unwrap();
            let dummy_data = vec![0u8; 1500];
            slotted.insert(&dummy_data).unwrap();
        }

        let result = fsm.page_with_free_space(1800).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_concurrent_updates_same_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = Arc::new(create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        ));

        let mut handles = vec![];

        // Spawn multiple threads adding pages to similar buckets
        for i in 0..10 {
            let fsm_clone = fsm.clone();
            let cache_clone = cache.clone();
            let file_key_clone = file_key.clone();

            let handle = thread::spawn(move || {
                let (pinned_page, page_id) = cache_clone.allocate_page(&file_key_clone).unwrap();

                let slotted =
                    SlottedPage::<_, TestPageHeader>::initialize_default(pinned_page).unwrap();
                drop(slotted);

                let free_space = PAGE_SIZE * (50 + i) / 100;
                fsm_clone.update_page_bucket(page_id, free_space);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for _ in 0..10 {
            let result = fsm.page_with_free_space(1000).unwrap();
            assert!(result.is_some());
        }

        let result = fsm.page_with_free_space(1000).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_page_with_free_space_reinserts_same_bucket_insufficient_space() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create a page with 80% free space (bucket 3: [75%-100%])
        let free_space_80_percent = PAGE_SIZE * 80 / 100;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space_80_percent);
        fsm.update_page_bucket(page_id, free_space_80_percent);

        // Request 90% free space - page should be popped but re-inserted since it's still in bucket 3
        let needed_90_percent = PAGE_SIZE * 90 / 100;
        let result = fsm.page_with_free_space(needed_90_percent).unwrap();
        // Should not find suitable page
        assert!(result.is_none());

        // Verify the page was re-inserted back into bucket 3
        assert!(bucket_contains_page(&fsm, 3, page_id));
    }

    #[test]
    fn fsm_page_with_free_space_moves_stale_entry_to_correct_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Manually insert a page into bucket 3 that actually belongs to bucket 2
        // (simulating a stale entry)
        let free_space_60_percent = PAGE_SIZE * 60 / 100;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space_60_percent);

        // Add to tracked_pages and push to wrong bucket (bucket 3)
        fsm.tracked_pages.insert(page_id);
        fsm.buckets[3].push(page_id);

        // Request space that requires searching bucket 3
        let needed_80_percent = PAGE_SIZE * 80 / 100;
        let result = fsm.page_with_free_space(needed_80_percent).unwrap();
        assert!(result.is_none());

        // Verify page was moved to the correct bucket (bucket 2)
        // The page should have been removed from tracked_pages when popped,
        // then re-added when moved to correct bucket
        assert!(!bucket_contains_page(&fsm, 3, page_id));
        assert!(bucket_contains_page(&fsm, 2, page_id));
        assert!(fsm.tracked_pages.contains(&page_id));
    }

    #[test]
    fn fsm_page_with_free_space_handles_multiple_stale_entries() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Bucket 0
        let page1_actual = PAGE_SIZE * 20 / 100;
        // Bucket 1
        let page2_actual = PAGE_SIZE * 45 / 100;
        // Bucket 3
        let page3_actual = PAGE_SIZE * 85 / 100;

        let page1 = create_page_with_free_space(&cache, &file_key, page1_actual);
        let page2 = create_page_with_free_space(&cache, &file_key, page2_actual);
        let page3 = create_page_with_free_space(&cache, &file_key, page3_actual);

        fsm.buckets[3].push(page1);
        fsm.buckets[3].push(page2);
        fsm.buckets[3].push(page3);

        // Request 80% space - should find page3, but page1 and page2 should be discarded
        let needed_80_percent = PAGE_SIZE * 80 / 100;
        let result = fsm
            .page_with_free_space(needed_80_percent)
            .unwrap()
            .unwrap();
        assert_eq!(result.0, page3);

        // Verify bucket 3 is now empty (page1 and page2 discarded, page3 returned)
        assert!(!bucket_contains_page(&fsm, 3, page1));
        assert!(!bucket_contains_page(&fsm, 3, page2));
        assert!(!bucket_contains_page(&fsm, 3, page3));
    }

    #[test]
    fn fsm_page_with_free_space_preserves_valid_entries_same_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Create two pages in bucket 2 [50%-75%] (only one with enough space)
        let page1_space = PAGE_SIZE * 55 / 100;
        let page2_space = PAGE_SIZE * 65 / 100;

        let page1 = create_page_with_free_space(&cache, &file_key, page1_space);
        let page2 = create_page_with_free_space(&cache, &file_key, page2_space);

        fsm.update_page_bucket(page1, page1_space);
        fsm.update_page_bucket(page2, page2_space);

        // Request 60% - should return page2, but page1 should be re-inserted
        let needed_60_percent = PAGE_SIZE * 60 / 100;
        let result = fsm
            .page_with_free_space(needed_60_percent)
            .unwrap()
            .unwrap();
        assert_eq!(result.0, page2);

        // Verify page1 is still in bucket 2 (re-inserted)
        assert!(bucket_contains_page(&fsm, 2, page1));

        // page2 should not be in bucket 2 anymore (it was returned)
        assert!(!bucket_contains_page(&fsm, 2, page2));
    }

    #[test]
    fn fsm_fallback_to_disk_when_buckets_empty() {
        let (cache, _, file_key) = setup_test_cache();

        // Create a chain of pages on disk
        let (first_page, first_page_id) = cache.allocate_page(&file_key).unwrap();

        // Initialize page
        let page = SlottedPage::<_, RecordPageHeader>::initialize_default(first_page).unwrap();
        drop(page);

        // Create FSM with empty buckets
        let fsm = create_test_fsm_record_page::<4>(cache.clone(), file_key.clone(), first_page_id);

        // Request space that first page should satisfy (but buckets are empty)
        let needed_space = 2000;
        let result = fsm.page_with_free_space(needed_space).unwrap();

        assert!(result.is_some());
        let (found_page_id, found_page) = result.unwrap();
        assert_eq!(found_page_id, first_page_id);
        assert!(found_page.free_space().unwrap() as usize >= needed_space);
    }

    #[test]
    fn fsm_fallback_scans_multiple_pages_until_suitable_found() {
        let (cache, _, file_key) = setup_test_cache();

        // Create chain: page1 (small space) -> page2 (small space) -> page3 (large space)
        let (first_page, first_page_id) = cache.allocate_page(&file_key).unwrap();
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();
        let (third_page, third_page_id) = cache.allocate_page(&file_key).unwrap();

        // Set up chain
        let mut first_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page).unwrap();
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page).unwrap();
        second_slotted.get_header_mut().unwrap().next_page = third_page_id;

        let mut third_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(third_page).unwrap();
        third_slotted.get_header_mut().unwrap().next_page = RecordPageHeader::NO_NEXT_PAGE;

        // Fill first and second pages to have insufficient space
        let large_dummy = vec![0u8; 3000];
        first_slotted.insert(&large_dummy).unwrap();
        second_slotted.insert(&large_dummy).unwrap();

        drop(first_slotted);
        drop(second_slotted);
        drop(third_slotted);

        let fsm = create_test_fsm_record_page::<4>(cache.clone(), file_key.clone(), first_page_id);

        // Request space that only third page can satisfy
        let needed_space = 2500;
        let result = fsm.page_with_free_space(needed_space).unwrap();

        assert!(result.is_some());
        let (found_page_id, found_page) = result.unwrap();
        assert_eq!(found_page_id, third_page_id);
        assert!(found_page.free_space().unwrap() as usize >= needed_space);

        // Verify that first and second pages were added to FSM during scan
        assert!(bucket_contains_page(&fsm, 1, first_page_id));
        assert!(bucket_contains_page(&fsm, 1, second_page_id));
    }

    #[test]
    fn fsm_fallback_returns_none_when_no_suitable_page_on_disk() {
        let (cache, _, file_key) = setup_test_cache();

        // Create pages with insufficient space
        let (first_page, first_page_id) = cache.allocate_page(&file_key).unwrap();
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();

        let mut first_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page).unwrap();
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page).unwrap();
        second_slotted.get_header_mut().unwrap().next_page = RecordPageHeader::NO_NEXT_PAGE;

        // Fill both pages almost completely
        let large_dummy = vec![0u8; 3500];
        first_slotted.insert(&large_dummy).unwrap();
        second_slotted.insert(&large_dummy).unwrap();

        drop(first_slotted);
        drop(second_slotted);

        let fsm = create_test_fsm_record_page::<4>(cache.clone(), file_key.clone(), first_page_id);

        // Request more space than any page has
        let needed_space = 3000;
        let result = fsm.page_with_free_space(needed_space).unwrap();

        assert!(result.is_none());

        // Both pages should have been added to FSM
        assert!(bucket_contains_page(&fsm, 0, first_page_id));
        assert!(bucket_contains_page(&fsm, 0, second_page_id));
    }

    #[test]
    fn fsm_load_next_page_updates_next_page_pointer() {
        let (cache, _, file_key) = setup_test_cache();

        let (first_page, first_page_id) = cache.allocate_page(&file_key).unwrap();
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();
        let (third_page, third_page_id) = cache.allocate_page(&file_key).unwrap();

        // Set up chain: first -> second -> third -> NO_NEXT_PAGE
        let mut first_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page).unwrap();
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page).unwrap();
        second_slotted.get_header_mut().unwrap().next_page = third_page_id;

        let mut third_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(third_page).unwrap();
        third_slotted.get_header_mut().unwrap().next_page = RecordPageHeader::NO_NEXT_PAGE;

        drop(first_slotted);
        drop(second_slotted);
        drop(third_slotted);

        let fsm = create_test_fsm_record_page::<4>(cache.clone(), file_key.clone(), first_page_id);

        // Load pages one by one
        let (loaded_id_1, _) = fsm.load_next_page().unwrap().unwrap();
        assert_eq!(loaded_id_1, first_page_id);

        let (loaded_id_2, _) = fsm.load_next_page().unwrap().unwrap();
        assert_eq!(loaded_id_2, second_page_id);

        let (loaded_id_3, _) = fsm.load_next_page().unwrap().unwrap();
        assert_eq!(loaded_id_3, third_page_id);

        // Should return None when reaching end of chain
        let result = fsm.load_next_page().unwrap();
        assert!(result.is_none());

        // Subsequent calls should also return None
        let result2 = fsm.load_next_page().unwrap();
        assert!(result2.is_none());
    }

    // Heap file factory

    #[test]
    fn heap_file_factory_loads_metadata() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Verify metadata was loaded
        assert_ne!(*heap_file.metadata.first_record_page.lock(), 0);
        assert_ne!(*heap_file.metadata.first_overflow_page.lock(), 0);
    }

    #[test]
    fn heap_file_factory_invalid_metadata_page() {
        let (cache, _, file_key) = setup_test_cache();

        // Allocate metadata page but write garbage to it
        let (mut metadata_pinned, metadata_page_id) = cache.allocate_page(&file_key).unwrap();
        assert_eq!(metadata_page_id, HeapFile::<4>::METADATA_PAGE_ID);

        metadata_pinned.page_mut()[..8].copy_from_slice(&[0xffu8; 8]);

        drop(metadata_pinned);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let result = factory.create_heap_file();

        // Should succeed but metadata values might be nonsensical
        // The actual error will occur when trying to read from invalid page IDs
        if let Ok(heap_file) = result {
            let invalid_ptr = RecordPtr {
                page_id: *heap_file.metadata.first_record_page.lock(),
                slot_id: 0,
            };
            let read_result = heap_file.record(&invalid_ptr);
            assert!(read_result.is_err());
        }
    }

    #[test]
    fn heap_file_factory_auto_creates_file_on_first_use() {
        let (cache, _, file_key) = setup_test_cache();

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Verify metadata was created with correct initial values
        let first_record_page = *heap_file.metadata.first_record_page.lock();
        let first_overflow_page = *heap_file.metadata.first_overflow_page.lock();
        assert_eq!(first_record_page, 2);
        assert_eq!(first_overflow_page, 3);

        // Verify both pages exist and are properly initialized
        let record_page_ref = FilePageRef::new(first_record_page, file_key.clone());
        let record_page = cache.pin_read(&record_page_ref).unwrap();
        let record_slotted = SlottedPage::<_, RecordPageHeader>::new(record_page).unwrap();

        // Should have maximum free space
        assert_eq!(
            record_slotted.free_space().unwrap(),
            SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE
        );
        drop(record_slotted);

        let overflow_page_ref = FilePageRef::new(first_overflow_page, file_key.clone());
        let overflow_page = cache.pin_read(&overflow_page_ref).unwrap();
        let overflow_slotted = SlottedPage::<_, OverflowPageHeader>::new(overflow_page).unwrap();

        // Should have maximum free space
        assert_eq!(
            overflow_slotted.free_space().unwrap(),
            SlottedPage::<(), OverflowPageHeader>::MAX_FREE_SPACE
        );
        drop(overflow_slotted);

        // Verify we can insert and read a record in the newly created file
        let test_record = Record::new(vec![
            Value::Int32(42).into(),
            Value::String("test".into()).into(),
        ]);

        let record_ptr = heap_file.insert(test_record).unwrap();
        assert_eq!(record_ptr.page_id, first_record_page);

        let retrieved = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved.fields.len(), 2);
        assert_i32(42, &retrieved.fields[0]);
        assert_string("test", &retrieved.fields[1]);
    }

    // Heap file reading

    #[test]
    fn heap_file_read_simple_record() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Insert a record
        let record_data = create_test_record_data();
        let slot_id =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record_data, None)
                .unwrap();

        // Create heap file and read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let record_ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id,
        };

        let record = heap_file.record(&record_ptr).unwrap();

        // Verify the record data
        assert_eq!(record.fields.len(), 2);
        assert_i32(42, &record.fields[0]);
        assert_string("name", &record.fields[1]);
    }

    #[test]
    fn heap_file_read_multiple_records_same_page() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Insert multiple records
        let record1_data = create_custom_record_data(1, "first");
        let record2_data = create_custom_record_data(2, "second");
        let record3_data = create_custom_record_data(3, "third");

        let slot1 =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record1_data, None)
                .unwrap();

        let slot2 =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record2_data, None)
                .unwrap();

        let slot3 =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record3_data, None)
                .unwrap();

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Read all records
        let ptr1 = RecordPtr {
            page_id: first_record_page_id,
            slot_id: slot1,
        };
        let ptr2 = RecordPtr {
            page_id: first_record_page_id,
            slot_id: slot2,
        };
        let ptr3 = RecordPtr {
            page_id: first_record_page_id,
            slot_id: slot3,
        };

        let rec1 = heap_file.record(&ptr1).unwrap();
        let rec2 = heap_file.record(&ptr2).unwrap();
        let rec3 = heap_file.record(&ptr3).unwrap();

        // Verify all records were read correctly
        assert_eq!(rec1.fields.len(), 2);
        assert_i32(1, &rec1.fields[0]);
        assert_string("first", &rec1.fields[1]);

        assert_eq!(rec2.fields.len(), 2);
        assert_i32(2, &rec2.fields[0]);
        assert_string("second", &rec2.fields[1]);

        assert_eq!(rec3.fields.len(), 2);
        assert_i32(3, &rec3.fields[0]);
        assert_string("third", &rec3.fields[1]);
    }

    #[test]
    fn heap_file_read_records_from_different_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create a second record page
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();
        SlottedPage::<_, RecordPageHeader>::initialize_default(second_page).unwrap();

        // Insert records in both pages
        let record1_data = create_custom_record_data(1, "page1");
        let record2_data = create_custom_record_data(2, "page2");

        let slot1 =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record1_data, None)
                .unwrap();

        let slot2 =
            insert_record_fragment(&cache, &file_key, second_page_id, &record2_data, None).unwrap();

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Read records from different pages
        let ptr1 = RecordPtr {
            page_id: first_record_page_id,
            slot_id: slot1,
        };
        let ptr2 = RecordPtr {
            page_id: second_page_id,
            slot_id: slot2,
        };

        let rec1 = heap_file.record(&ptr1).unwrap();
        let rec2 = heap_file.record(&ptr2).unwrap();

        // Verify both records
        assert_eq!(rec1.fields.len(), 2);
        assert_i32(1, &rec1.fields[0]);
        assert_string("page1", &rec1.fields[1]);

        assert_eq!(rec2.fields.len(), 2);
        assert_i32(2, &rec2.fields[0]);
        assert_string("page2", &rec2.fields[1]);
    }

    #[test]
    fn heap_file_read_large_record() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create a large record (but still fits in one page)
        let large_name = "x".repeat(500);
        let record_data = create_custom_record_data(999, &large_name);

        let slot_id =
            insert_record_fragment(&cache, &file_key, first_record_page_id, &record_data, None)
                .unwrap();

        // Create heap file and read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let record_ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id,
        };

        let record = heap_file.record(&record_ptr).unwrap();

        // Verify the large record was read correctly
        assert_eq!(record.fields.len(), 2);
        assert_i32(999, &record.fields[0]);
        assert_string(&large_name, &record.fields[1]);
    }

    #[test]
    fn heap_file_concurrent_reads() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Insert multiple records
        let mut record_ptrs = Vec::new();
        for i in 0..10 {
            let record_data = create_custom_record_data(i, &format!("record_{}", i));
            let slot_id =
                insert_record_fragment(&cache, &file_key, first_record_page_id, &record_data, None)
                    .unwrap();
            record_ptrs.push((
                RecordPtr {
                    page_id: first_record_page_id,
                    slot_id,
                },
                i,
            ));
        }

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        // Spawn multiple threads to read records concurrently
        let mut handles = vec![];
        for (ptr, expected_id) in record_ptrs {
            let hf = heap_file.clone();
            let handle = thread::spawn(move || {
                let record = hf.record(&ptr).unwrap();
                assert_eq!(record.fields.len(), 2);
                assert_i32(expected_id, &record.fields[0]);
                assert_string(&format!("record_{}", expected_id), &record.fields[1]);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn heap_file_read_invalid_page_id() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Try to read from a non-existent page
        let invalid_ptr = RecordPtr {
            page_id: 9999,
            slot_id: 0,
        };

        let result = heap_file.record(&invalid_ptr);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HeapFileError::CacheError(_)));
    }

    #[test]
    fn heap_file_read_invalid_slot_id() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Try to read from a non-existent slot
        let invalid_ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: 999,
        };

        let result = heap_file.record(&invalid_ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(_)
        ));
    }

    #[test]
    fn heap_file_read_uninitialized_page() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Allocate a page but don't initialize it with slotted page header
        let (_, uninitialized_page_id) = cache.allocate_page(&file_key).unwrap();

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: uninitialized_page_id,
            slot_id: 0,
        };

        let result = heap_file.record(&ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(SlottedPageError::UninitializedPage)
        ));
    }

    #[test]
    fn heap_file_read_corrupted_record_tag() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Insert record with invalid tag
        let mut corrupted_data = vec![0x42u8];
        corrupted_data.extend_from_slice(&create_test_record_data());

        let slot_id = {
            let file_page_ref = FilePageRef::new(first_record_page_id, file_key.clone());
            let pinned = cache.pin_write(&file_page_ref).unwrap();
            let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned).unwrap();
            match slotted.insert(&corrupted_data).unwrap() {
                InsertResult::Success(slot_id) => slot_id,
                _ => panic!(),
            }
        };

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id,
        };

        let result = heap_file.record(&ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::CorruptedRecordEntry { .. }
        ));
    }

    #[test]
    fn heap_file_all_records_empty_file() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 0);
    }

    #[test]
    fn heap_file_all_records_single_record_single_page() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("test".into()).into(),
        ]);
        heap_file.insert(record).unwrap();

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 1);
        assert_i32(1, &all_records[0].fields[0]);
        assert_string("test", &all_records[0].fields[1]);
    }

    #[test]
    fn heap_file_all_records_multiple_records_single_page() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        for i in 0..10 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("record_{}", i)).into(),
            ]);
            heap_file.insert(record).unwrap();
        }

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 10);

        for (i, record) in all_records.iter().enumerate() {
            assert_i32(i as i32, &record.fields[0]);
            assert_string(&format!("record_{}", i), &record.fields[1]);
        }
    }

    #[test]
    fn heap_file_all_records_multiple_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead;
        let large_string = "x".repeat(large_string_size);

        // Insert first large record to fill first page
        let record1 = Record::new(vec![
            Value::Int32(1).into(),
            Value::String(large_string.clone()).into(),
        ]);
        heap_file.insert(record1).unwrap();

        // Insert more records to trigger new page allocation
        for i in 2..=5 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("record_{}", i)).into(),
            ]);
            heap_file.insert(record).unwrap();
        }

        let all_records = collect_all_records(&heap_file);

        // Verify all records are present (order may vary due to page allocation)
        let mut found_ids = all_records
            .iter()
            .map(|r| match &r.fields[0].deref() {
                Value::Int32(id) => *id,
                _ => panic!("Expected Int32"),
            })
            .collect::<Vec<_>>();
        found_ids.sort();

        assert_eq!(found_ids, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn heap_file_all_records_includes_multi_fragment_records() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();

        // Insert small record
        let small_record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("small".into()).into(),
        ]);
        heap_file.insert(small_record).unwrap();

        // Insert large multi-fragment record
        let large_string_size = (2 * max_single_page_size) - record_overhead + 100;
        let large_string = "y".repeat(large_string_size);
        let large_record = Record::new(vec![
            Value::Int32(2).into(),
            Value::String(large_string.clone()).into(),
        ]);
        heap_file.insert(large_record).unwrap();

        // Insert another small record
        let small_record2 = Record::new(vec![
            Value::Int32(3).into(),
            Value::String("small2".into()).into(),
        ]);
        heap_file.insert(small_record2).unwrap();

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 3);

        // Find records by ID (order may vary)
        let record1 = all_records
            .iter()
            .find(|r| matches!(r.fields[0].deref(), Value::Int32(1)))
            .unwrap();
        assert_string("small", &record1.fields[1]);

        let record2 = all_records
            .iter()
            .find(|r| matches!(r.fields[0].deref(), Value::Int32(2)))
            .unwrap();
        assert_string(&large_string, &record2.fields[1]);

        let record3 = all_records
            .iter()
            .find(|r| matches!(r.fields[0].deref(), Value::Int32(3)))
            .unwrap();
        assert_string("small2", &record3.fields[1]);
    }

    #[test]
    fn heap_file_all_records_after_deletions() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let mut record_ptrs = vec![];
        for i in 0..5 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("record_{}", i)).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            record_ptrs.push(ptr);
        }

        heap_file.delete(&record_ptrs[1]).unwrap();
        heap_file.delete(&record_ptrs[3]).unwrap();

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 3);

        // Verify remaining IDs (0, 2, 4)
        let mut found_ids = all_records
            .iter()
            .map(|r| match &r.fields[0].deref() {
                Value::Int32(id) => *id,
                _ => panic!("Expected Int32"),
            })
            .collect::<Vec<_>>();
        found_ids.sort();

        assert_eq!(found_ids, vec![0, 2, 4]);

        // Verify content of remaining records
        for id in [0, 2, 4] {
            let record = all_records
                .iter()
                .find(|r| match &r.fields[0].deref() {
                    Value::Int32(rid) => *rid == id,
                    _ => false,
                })
                .unwrap();
            assert_string(&format!("record_{}", id), &record.fields[1]);
        }
    }

    #[test]
    fn heap_file_all_records_after_updates() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let mut record_ptrs = vec![];
        for i in 0..3 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("original_{}", i)).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            record_ptrs.push(ptr);
        }

        // Update record with ID 1
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String("updated_1".into())).unwrap();
        heap_file
            .update(&record_ptrs[1], vec![update_descriptor])
            .unwrap();

        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), 3);

        // Find and verify each record
        for id in 0..3 {
            let record = all_records
                .iter()
                .find(|r| match &r.fields[0].deref() {
                    Value::Int32(rid) => *rid == id,
                    _ => false,
                })
                .unwrap();

            if id == 1 {
                assert_string("updated_1", &record.fields[1]);
            } else {
                assert_string(&format!("original_{}", id), &record.fields[1]);
            }
        }
    }

    #[test]
    fn heap_file_all_records_concurrent_reads() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        for i in 0..20 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("record_{}", i)).into(),
            ]);
            heap_file.insert(record).unwrap();
        }

        let mut handles = vec![];
        for _ in 0..5 {
            let heap_file_clone = heap_file.clone();
            let handle = thread::spawn(move || {
                let all_records = collect_all_records(&heap_file_clone);
                assert_eq!(all_records.len(), 20);

                // Verify all IDs are present
                let mut found_ids = all_records
                    .iter()
                    .map(|r| match &r.fields[0].deref() {
                        Value::Int32(id) => *id,
                        _ => panic!("Expected Int32"),
                    })
                    .collect::<Vec<_>>();
                found_ids.sort();

                assert_eq!(found_ids, (0..20).collect::<Vec<_>>());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    // Heap file reading multi-fragment records

    #[test]
    fn heap_file_read_record_two_fragments() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        // Create a record split into two fragments
        let full_record = create_custom_record_data(1, "test_record");

        let split_point = full_record.len() / 2;
        let first_part = &full_record[..split_point];
        let second_part = &full_record[split_point..];

        let second_slot =
            insert_record_fragment(&cache, &file_key, first_overflow_page_id, second_part, None)
                .unwrap();

        let continuation_ptr = RecordPtr {
            page_id: first_overflow_page_id,
            slot_id: second_slot,
        };
        let first_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_record_page_id,
            first_part,
            Some(continuation_ptr),
        )
        .unwrap();

        // Read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: first_slot,
        };

        let record = heap_file.record(&ptr).unwrap();

        assert_eq!(record.fields.len(), 2);
        assert_i32(1, &record.fields[0]);
        assert_string("test_record", &record.fields[1]);
    }

    #[test]
    fn heap_file_read_record_three_fragments() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        // Create a second overflow page
        let (second_overflow_page, second_overflow_page_id) =
            cache.allocate_page(&file_key).unwrap();
        SlottedPage::<_, OverflowPageHeader>::initialize_default(second_overflow_page).unwrap();

        let full_record = create_custom_record_data(42, "fragmented_record");

        let first_split = full_record.len() / 3;
        let second_split = 2 * full_record.len() / 3;

        let first_part = &full_record[..first_split];
        let second_part = &full_record[first_split..second_split];
        let third_part = &full_record[second_split..];

        let third_slot =
            insert_record_fragment(&cache, &file_key, second_overflow_page_id, third_part, None)
                .unwrap();

        let third_ptr = RecordPtr {
            page_id: second_overflow_page_id,
            slot_id: third_slot,
        };
        let second_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_overflow_page_id,
            second_part,
            Some(third_ptr),
        )
        .unwrap();

        let second_ptr = RecordPtr {
            page_id: first_overflow_page_id,
            slot_id: second_slot,
        };
        let first_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_record_page_id,
            first_part,
            Some(second_ptr),
        )
        .unwrap();

        // Read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: first_slot,
        };

        let record = heap_file.record(&ptr).unwrap();

        assert_eq!(record.fields.len(), 2);
        assert_i32(42, &record.fields[0]);
        assert_string("fragmented_record", &record.fields[1]);
    }

    #[test]
    fn heap_file_read_fragmented_large_record() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        let large_string = "x".repeat(300);
        let full_record = create_custom_record_data(999, &large_string);

        let split1 = full_record.len() / 3;
        let split2 = 2 * full_record.len() / 3;

        let (second_overflow_page, second_overflow_page_id) =
            cache.allocate_page(&file_key).unwrap();
        SlottedPage::<_, OverflowPageHeader>::initialize_default(second_overflow_page).unwrap();

        let third_slot = insert_record_fragment(
            &cache,
            &file_key,
            second_overflow_page_id,
            &full_record[split2..],
            None,
        )
        .unwrap();

        let second_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_overflow_page_id,
            &full_record[split1..split2],
            Some(RecordPtr {
                page_id: second_overflow_page_id,
                slot_id: third_slot,
            }),
        )
        .unwrap();

        let first_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_record_page_id,
            &full_record[..split1],
            Some(RecordPtr {
                page_id: first_overflow_page_id,
                slot_id: second_slot,
            }),
        )
        .unwrap();

        // Read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: first_slot,
        };

        let record = heap_file.record(&ptr).unwrap();

        assert_eq!(record.fields.len(), 2);
        assert_i32(999, &record.fields[0]);
        assert_string(&large_string, &record.fields[1]);
    }

    #[test]
    fn heap_file_read_fragmented_invalid_continuation_page() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        let record = create_custom_record_data(1, "test");
        let split = record.len() / 2;

        // Insert first fragment with invalid continuation pointer
        let invalid_ptr = RecordPtr {
            page_id: 9999,
            slot_id: 0,
        };
        let first_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_record_page_id,
            &record[..split],
            Some(invalid_ptr),
        )
        .unwrap();

        // Try to read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: first_slot,
        };

        let result = heap_file.record(&ptr);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HeapFileError::CacheError(_)));
    }

    #[test]
    fn heap_file_read_fragmented_invalid_continuation_slot() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        let record = create_custom_record_data(1, "test");
        let split = record.len() / 2;

        // Insert first fragment with invalid slot in continuation
        let invalid_ptr = RecordPtr {
            page_id: first_overflow_page_id,
            slot_id: 999,
        };
        let first_slot = insert_record_fragment(
            &cache,
            &file_key,
            first_record_page_id,
            &record[..split],
            Some(invalid_ptr),
        )
        .unwrap();

        // Try to read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot_id: first_slot,
        };

        let result = heap_file.record(&ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(_)
        ));
    }

    #[test]
    fn heap_file_concurrent_read_fragmented_records() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        // Create multiple fragmented records
        let mut record_ptrs = Vec::new();

        for i in 0..5 {
            let record = create_custom_record_data(i, &format!("fragmented_{}", i));
            let split = record.len() / 2;

            let second_slot = insert_record_fragment(
                &cache,
                &file_key,
                first_overflow_page_id,
                &record[split..],
                None,
            )
            .unwrap();

            let first_slot = insert_record_fragment(
                &cache,
                &file_key,
                first_record_page_id,
                &record[..split],
                Some(RecordPtr {
                    page_id: first_overflow_page_id,
                    slot_id: second_slot,
                }),
            )
            .unwrap();

            record_ptrs.push((
                RecordPtr {
                    page_id: first_record_page_id,
                    slot_id: first_slot,
                },
                i,
            ));
        }

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        // Spawn multiple threads to read fragmented records concurrently
        let mut handles = vec![];
        for (ptr, expected_id) in record_ptrs {
            let hf = heap_file.clone();
            let handle = thread::spawn(move || {
                let record = hf.record(&ptr).unwrap();
                assert_eq!(record.fields.len(), 2);
                assert_i32(expected_id, &record.fields[0]);
                assert_string(&format!("fragmented_{}", expected_id), &record.fields[1]);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn heap_file_fsm_deduplication_on_multiple_reads() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // First read of the empty page - should add it to FSM
        let page = heap_file.read_record_page(first_record_page_id).unwrap();
        drop(page);

        assert!(bucket_contains_page(
            &heap_file.record_pages_fsm,
            3,
            first_record_page_id
        ));

        for _ in 0..4 {
            let page = heap_file.read_record_page(first_record_page_id).unwrap();
            drop(page);
        }

        // Should still be only one page in last bucket
        assert!(bucket_contains_page(
            &heap_file.record_pages_fsm,
            3,
            first_record_page_id
        ));
        assert_eq!(heap_file.record_pages_fsm.buckets[3].len(), 1);
    }

    // HeapFile inserting record

    #[test]
    fn heap_file_insert_simple_record() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a simple record
        let id_field = Value::Int32(123).into();
        let name_field = Value::String("test_user".into()).into();
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record was inserted correctly
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot_id, 0);

        // Read back the record to verify it was stored correctly
        let retrieved_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved_record.fields.len(), 2);
        assert_i32(123, &retrieved_record.fields[0]);
        assert_string("test_user", &retrieved_record.fields[1]);

        // Verify the page was added to FSM after insertion
        let file_page_ref = FilePageRef::new(first_record_page_id, file_key.clone());
        let page = cache.pin_read(&file_page_ref).unwrap();
        let slotted_page = SlottedPage::<_, RecordPageHeader>::new(page).unwrap();
        let free_space = slotted_page.free_space().unwrap() as usize;
        let expected_bucket = heap_file.record_pages_fsm.bucket_for_space(free_space);

        assert!(bucket_contains_page(
            &heap_file.record_pages_fsm,
            expected_bucket,
            first_record_page_id
        ));
    }

    #[test]
    fn heap_file_insert_large_single_page_record() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Calculate maximum record size that can fit in a single page
        let max_record_size_on_single_page = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE
            as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        // Create a large string that will make the record use almost all available space
        let record_overhead = size_of::<u32>() + size_of::<u16>();
        let large_string_size = max_record_size_on_single_page - record_overhead;
        let large_string = "x".repeat(large_string_size);

        let id_field = Value::Int32(999).into();
        let name_field = Value::String(large_string.clone()).into();
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record was inserted correctly
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot_id, 0);

        // Read back the record to verify it was stored correctly
        let retrieved_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved_record.fields.len(), 2);
        assert_i32(999, &retrieved_record.fields[0]);
        assert_string(&large_string, &retrieved_record.fields[1]);

        // Verify the page now has no free space left
        let file_page_ref = FilePageRef::new(first_record_page_id, file_key.clone());
        let page = cache.pin_read(&file_page_ref).unwrap();
        let slotted_page = SlottedPage::<_, RecordPageHeader>::new(page).unwrap();
        let free_space = slotted_page.free_space().unwrap() as usize;

        assert_eq!(free_space, 0);
        // Page with 0 free space is below MIN_USEFUL_FREE_SPACE,
        // so it should not be added to FSM at all
        assert!(
            !heap_file
                .record_pages_fsm
                .tracked_pages
                .contains(&first_record_page_id)
        );

        // Verify page is not in any bucket
        for bucket_idx in 0..4 {
            assert!(!bucket_contains_page(
                &heap_file.record_pages_fsm,
                bucket_idx,
                first_record_page_id
            ));
        }
    }

    #[test]
    fn heap_file_insert_record_spanning_two_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        // Create a record that's larger than single page capacity
        let record_overhead = size_of::<u32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead + 500;
        let large_string = "y".repeat(large_string_size);

        let id_field = Value::Int32(777).into();
        let name_field = Value::String(large_string.clone()).into();
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record pointer points to the record page (first fragment)
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot_id, 0);

        // Read back the record to verify it was stored correctly across pages
        let retrieved_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved_record.fields.len(), 2);
        assert_i32(777, &retrieved_record.fields[0]);
        assert_string(&large_string, &retrieved_record.fields[1]);

        // Verify record page was added to FSM (if have some free space left)
        let record_page_ref = FilePageRef::new(first_record_page_id, file_key.clone());
        let record_page = cache.pin_read(&record_page_ref).unwrap();
        let record_slotted_page = SlottedPage::<_, RecordPageHeader>::new(record_page).unwrap();
        let record_free_space = record_slotted_page.free_space().unwrap() as usize;

        // Record page should be in FSM if it has enough free space
        if record_free_space >= FreeSpaceMap::<4, RecordPageHeader>::MIN_USEFUL_FREE_SPACE {
            let record_bucket = heap_file
                .record_pages_fsm
                .bucket_for_space(record_free_space);
            assert!(bucket_contains_page(
                &heap_file.record_pages_fsm,
                record_bucket,
                first_record_page_id
            ));
        } else {
            // Page has too little free space, should not be tracked
            assert!(
                !heap_file
                    .record_pages_fsm
                    .tracked_pages
                    .contains(&first_record_page_id)
            );
        }

        // Verify overflow page handling
        let overflow_page_ref = FilePageRef::new(first_overflow_page_id, file_key.clone());
        let overflow_page = cache.pin_read(&overflow_page_ref).unwrap();
        let overflow_slotted_page =
            SlottedPage::<_, OverflowPageHeader>::new(overflow_page).unwrap();
        let overflow_free_space = overflow_slotted_page.free_space().unwrap() as usize;

        // Overflow page should be in FSM only if it has enough free space
        if overflow_free_space >= FreeSpaceMap::<4, OverflowPageHeader>::MIN_USEFUL_FREE_SPACE {
            let overflow_bucket = heap_file
                .overflow_pages_fsm
                .bucket_for_space(overflow_free_space);
            assert!(bucket_contains_page(
                &heap_file.overflow_pages_fsm,
                overflow_bucket,
                first_overflow_page_id
            ));
        } else {
            // Page has too little free space, should not be tracked
            assert!(
                !heap_file
                    .overflow_pages_fsm
                    .tracked_pages
                    .contains(&first_overflow_page_id)
            );
        }

        // Verify that the first fragment contains a continuation tag
        let first_fragment_data = record_slotted_page.read_record(0).unwrap();
        let first_byte = first_fragment_data[0];
        assert_eq!(first_byte, RecordTag::HasContinuation as u8);

        // Verify that the second fragment contains a final tag
        let second_fragment_data = overflow_slotted_page.read_record(0).unwrap();
        let second_first_byte = second_fragment_data[0];
        assert_eq!(second_first_byte, RecordTag::Final as u8);
    }

    #[test]
    fn heap_file_insert_record_spanning_four_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, first_overflow_page_id) =
            setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();

        let large_string_size = (3 * max_single_page_size) - record_overhead + 100;
        let large_string = "z".repeat(large_string_size);

        let id_field = Value::Int32(555).into();
        let name_field = Value::String(large_string.clone()).into();
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record pointer points to the record page (first fragment)
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot_id, 0);

        // Read back the record to verify it was stored correctly across pages
        let retrieved_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved_record.fields.len(), 2);
        assert_i32(555, &retrieved_record.fields[0]);
        assert_string(&large_string, &retrieved_record.fields[1]);

        // Assert metadata change and allocations were successful
        assert!(heap_file.metadata.dirty.load(Ordering::Acquire));

        let new_first_overflow_page = *heap_file.metadata.first_overflow_page.lock();
        assert_ne!(new_first_overflow_page, first_overflow_page_id);

        let mut overflow_pages_count = 0;
        let mut current_page_id = new_first_overflow_page;

        while current_page_id != OverflowPageHeader::NO_NEXT_PAGE {
            overflow_pages_count += 1;

            let file_page_ref = FilePageRef::new(current_page_id, file_key.clone());
            let page = cache.pin_read(&file_page_ref).unwrap();
            let slotted_page = SlottedPage::<_, OverflowPageHeader>::new(page).unwrap();
            let header = slotted_page.get_header().unwrap();
            current_page_id = header.next_page();
        }

        assert_eq!(overflow_pages_count, 3);
    }

    #[test]
    fn heap_file_insert_forces_new_record_page_allocation() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Store initial metadata state
        let initial_first_record_page = *heap_file.metadata.first_record_page.lock();
        assert_eq!(initial_first_record_page, first_record_page_id);
        assert!(!heap_file.metadata.dirty.load(Ordering::Acquire));

        // Insert a large record that fills the first page
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let record_overhead = size_of::<u32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead;
        let large_string = "x".repeat(large_string_size);

        let large_record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String(large_string.clone()).into(),
        ]);

        let first_record_ptr = heap_file.insert(large_record).unwrap();
        assert_eq!(first_record_ptr.page_id, first_record_page_id);
        assert_eq!(first_record_ptr.slot_id, 0);

        // Insert a small record that won't fit in the remaining space
        let small_record = Record::new(vec![
            Value::Int32(2).into(),
            Value::String("this_record_should_trigger_new_page".into()).into(),
        ]);

        let second_record_ptr = heap_file.insert(small_record).unwrap();

        // Verify a new record page was allocated
        assert_ne!(second_record_ptr.page_id, first_record_page_id);
        assert_eq!(second_record_ptr.slot_id, 0);

        // Verify metadata was changed and is dirty
        assert!(heap_file.metadata.dirty.load(Ordering::Acquire));

        let new_first_record_page = *heap_file.metadata.first_record_page.lock();
        assert_ne!(new_first_record_page, initial_first_record_page);
        assert_eq!(second_record_ptr.page_id, new_first_record_page);

        // Verify the new page is properly linked in the chain
        let new_page_ref = FilePageRef::new(new_first_record_page, file_key.clone());
        let new_page = cache.pin_read(&new_page_ref).unwrap();
        let new_slotted_page = SlottedPage::<_, RecordPageHeader>::new(new_page).unwrap();
        let new_header = new_slotted_page.get_header().unwrap();
        assert_eq!(new_header.next_page(), first_record_page_id);

        // Read back both records to verify they were stored correctly
        let first_retrieved = heap_file.record(&first_record_ptr).unwrap();
        assert_eq!(first_retrieved.fields.len(), 2);
        assert_i32(1, &first_retrieved.fields[0]);
        assert_string(&large_string, &first_retrieved.fields[1]);

        let second_retrieved = heap_file.record(&second_record_ptr).unwrap();
        assert_eq!(second_retrieved.fields.len(), 2);
        assert_i32(2, &second_retrieved.fields[0]);
        assert_string(
            "this_record_should_trigger_new_page",
            &second_retrieved.fields[1],
        );
    }

    #[test]
    fn heap_file_insert_concurrent() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        let record1 = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("thread_1_record".into()).into(),
        ]);

        let record2 = Record::new(vec![
            Value::Int32(200).into(),
            Value::String("thread_2_record".into()).into(),
        ]);

        // Spawn two threads to insert records concurrently
        let heap_file_clone1 = heap_file.clone();
        let heap_file_clone2 = heap_file.clone();

        let handle1 = thread::spawn(move || heap_file_clone1.insert(record1));

        let handle2 = thread::spawn(move || heap_file_clone2.insert(record2));

        // Wait for both insertions to complete
        let result1 = handle1.join().unwrap();
        let result2 = handle2.join().unwrap();

        // Verify both insertions were successful
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let ptr1 = result1.unwrap();
        let ptr2 = result2.unwrap();
        assert!(ptr1 != ptr2);

        // Verify both records can be read back correctly
        let retrieved1 = heap_file.record(&ptr1).unwrap();
        assert_eq!(retrieved1.fields.len(), 2);
        assert_i32(100, &retrieved1.fields[0]);
        assert_string("thread_1_record", &retrieved1.fields[1]);

        let retrieved2 = heap_file.record(&ptr2).unwrap();
        assert_eq!(retrieved2.fields.len(), 2);
        assert_i32(200, &retrieved2.fields[0]);
        assert_string("thread_2_record", &retrieved2.fields[1]);
    }

    // Metadata

    #[test]
    fn heap_file_flush_metadata() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Initially metadata should not be dirty
        assert!(!heap_file.metadata.dirty.load(Ordering::Acquire));

        // Trigger a metadata change by allocating a new record page
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let record_overhead = size_of::<u32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead;
        let large_string = "x".repeat(large_string_size);

        // Fill the first page completely
        let large_record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String(large_string).into(),
        ]);
        heap_file.insert(large_record).unwrap();

        // Insert small record to trigger new page allocation
        let small_record = Record::new(vec![
            Value::Int32(2).into(),
            Value::String("small".into()).into(),
        ]);
        heap_file.insert(small_record).unwrap();

        // Verify metadata is now dirty
        assert!(heap_file.metadata.dirty.load(Ordering::Acquire));

        // Capture current metadata values
        let first_record_page = *heap_file.metadata.first_record_page.lock();
        let first_overflow_page = *heap_file.metadata.first_overflow_page.lock();

        // Flush metadata
        heap_file.flush_metadata().unwrap();

        // Verify metadata is no longer dirty
        assert!(!heap_file.metadata.dirty.load(Ordering::Acquire));

        // Read metadata directly from disk to verify it was written
        let metadata_page_ref = heap_file.file_page_ref(HeapFile::<4>::METADATA_PAGE_ID);
        let metadata_page = cache.pin_read(&metadata_page_ref).unwrap();
        let disk_metadata = MetadataRepr::load_from_page(metadata_page.page()).unwrap();

        assert_eq!(disk_metadata.first_record_page, first_record_page);
        assert_eq!(disk_metadata.first_overflow_page, first_overflow_page);

        // Try flushing again (should be no-op since metadata is clean)
        heap_file.flush_metadata().unwrap();

        assert!(!heap_file.metadata.dirty.load(Ordering::Acquire));
    }

    // HeapFile updating record

    #[test]
    fn heap_file_update_fixed_size_field_single_fragment() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a simple record
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("original_name".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the Int32 field
        let column_metadata = heap_file.columns_metadata[0].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::Int32(200)).unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(200, &updated_record.fields[0]);
        assert_string("original_name", &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_update_multiple_fixed_size_fields_multi_fragment() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file with multiple fixed-size columns
        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("score".into(), Type::I64, 2, 2 * size_of::<i32>(), 2).unwrap(),
            ColumnMetadata::new(
                "name".into(),
                Type::String,
                3,
                2 * size_of::<i32>() + size_of::<i64>(),
                3,
            )
            .unwrap(),
        ];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Create a large record that spans multiple fragments
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let record_overhead = 2 * size_of::<i32>() + size_of::<i64>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead + 500;
        let large_string = "x".repeat(large_string_size);

        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::Int32(25).into(),
            Value::Int64(1000).into(),
            Value::String(large_string.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update multiple fixed-size fields
        let update_id =
            FieldUpdateDescriptor::new(columns_metadata[0].clone(), Value::Int32(200)).unwrap();
        let update_age =
            FieldUpdateDescriptor::new(columns_metadata[1].clone(), Value::Int32(30)).unwrap();
        let update_score =
            FieldUpdateDescriptor::new(columns_metadata[2].clone(), Value::Int64(2000)).unwrap();

        heap_file
            .update(&record_ptr, vec![update_id, update_age, update_score])
            .unwrap();

        // Read back the record to verify all updates
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 4);
        assert_i32(200, &updated_record.fields[0]);
        assert_i32(30, &updated_record.fields[1]);
        assert_i64(2000, &updated_record.fields[2]);
        assert_string(&large_string, &updated_record.fields[3]);
    }

    #[test]
    fn heap_file_update_string_field_same_size_single_fragment() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a simple record
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("original_name".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with same length string
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String("0RIGIN4L_name".into()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string("0RIGIN4L_name", &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_update_string_field_shorter_single_fragment() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a record with a longer string
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("original_long_name".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with a shorter string
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String("short".into())).unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string("short", &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_update_string_field_longer_single_fragment() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a record with a short string
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("short".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with a longer string (but still fits in one page)
        let longer_string =
            "this_is_a_much_longer_string_than_the_original_one_but_still_fits_in_single_page";
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(longer_string.into()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string(longer_string, &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_update_string_field_grows_to_four_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a record with a short string
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("short".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Create a very large string that will span 4 pages
        // The reason it spans 4 pages:
        // - first we have a record on single page of length 11
        // - then we update it, we decide that new record is more than one page so we need to use overflow pages
        // - at prev location we store (prev_len - size of RecordPtr -> 3 in our case)
        // - rest of the bytes (slightly more than 8k) need 3 pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();

        let very_large_string_size = (2 * max_single_page_size) - record_overhead + 100;
        let very_large_string = "z".repeat(very_large_string_size);

        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(very_large_string.clone()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string(&very_large_string, &updated_record.fields[1]);

        // Verify that the record now spans multiple pages by checking fragments
        let first_page = heap_file.read_record_page(record_ptr.page_id).unwrap();
        let first_fragment = first_page.record_fragment(record_ptr.slot_id).unwrap();

        // First fragment should have a continuation
        assert!(first_fragment.next_fragment.is_some());

        let second_ptr = first_fragment.next_fragment.unwrap();
        let second_page = heap_file.read_overflow_page(second_ptr.page_id).unwrap();
        let second_fragment = second_page.record_fragment(second_ptr.slot_id).unwrap();

        // Second fragment should also have a continuation
        assert!(second_fragment.next_fragment.is_some());

        let third_ptr = second_fragment.next_fragment.unwrap();
        let third_page = heap_file.read_overflow_page(third_ptr.page_id).unwrap();
        let third_fragment = third_page.record_fragment(third_ptr.slot_id).unwrap();

        // Third fragment should also have a continuation
        assert!(third_fragment.next_fragment.is_some());

        let fourth_ptr = third_fragment.next_fragment.unwrap();
        let fourth_page = heap_file.read_overflow_page(fourth_ptr.page_id).unwrap();
        let fourth_fragment = fourth_page.record_fragment(fourth_ptr.slot_id).unwrap();

        // Fourth fragment should be the last one
        assert!(fourth_fragment.next_fragment.is_none());
    }

    #[test]
    fn heap_file_update_string_field_shrinks_from_multi_fragment_to_single() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Create a large string that spans multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<i32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead + 500;
        let large_string = "x".repeat(large_string_size);

        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String(large_string.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all fragment locations before update
        let mut original_fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            original_fragment_locations.push((ptr.page_id, ptr.slot_id));

            if original_fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert!(original_fragment_locations.len() >= 2);

        // Update the String field with a small string that fits in one page
        let small_string = "tiny";
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(small_string.into()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string(small_string, &updated_record.fields[1]);

        // Verify that the record now fits in a single page
        let first_page_after = heap_file.read_record_page(record_ptr.page_id).unwrap();
        let first_fragment_after = first_page_after
            .record_fragment(record_ptr.slot_id)
            .unwrap();

        assert!(first_fragment_after.next_fragment.is_none());

        // Verify all overflow fragments were deleted
        for (i, (page_id, slot_id)) in original_fragment_locations.iter().enumerate() {
            if i == 0 {
                // First fragment should still exist
                let page = heap_file.read_record_page(*page_id).unwrap();
                let result = page.page.read_record(*slot_id);
                assert!(result.is_ok());
            } else {
                // All overflow fragments should be deleted
                let page = heap_file.read_overflow_page(*page_id).unwrap();
                let result = page.page.read_record(*slot_id);
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn heap_file_update_string_field_grows_from_three_to_six_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<i32>() + size_of::<u16>();

        // Create a string that spans exactly 3 pages
        let first_fragment_capacity =
            max_single_page_size - record_overhead - size_of::<RecordPtr>();
        let overflow_fragment_capacity =
            max_single_page_size - size_of::<RecordTag>() - size_of::<RecordPtr>();

        let initial_string_size = first_fragment_capacity + overflow_fragment_capacity + 100;
        let initial_string = "x".repeat(initial_string_size);

        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String(initial_string.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update with a string that's 2x larger (should span 6 pages)
        let updated_string_size = initial_string_size * 2;
        let updated_string = "y".repeat(updated_string_size);

        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(updated_string.clone()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string(&updated_string, &updated_record.fields[1]);

        // Verify that the record now spans 6 pages by walking the fragment chain
        let mut fragment_count = 0;
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            fragment_count += 1;

            if fragment_count == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert_eq!(fragment_count, 6);
    }

    #[test]
    fn heap_file_update_string_field_shrinks_from_four_to_two_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<i32>() + size_of::<u16>();

        // Create a string that spans exactly 4 pages
        let first_fragment_capacity =
            max_single_page_size - record_overhead - size_of::<RecordPtr>();
        let overflow_fragment_capacity =
            max_single_page_size - size_of::<RecordTag>() - size_of::<RecordPtr>();

        // Size for 4 pages: first + 2 full overflow + partial last
        let initial_string_size = first_fragment_capacity + 2 * overflow_fragment_capacity + 100;
        let initial_string = "x".repeat(initial_string_size);

        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String(initial_string.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all fragment locations before update (should be 4 pages)
        let mut original_fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            original_fragment_locations.push((ptr.page_id, ptr.slot_id));

            if original_fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert_eq!(original_fragment_locations.len(), 4);

        // Update with a string that's half the size (should span 2 pages)
        let updated_string_size = initial_string_size / 2;
        let updated_string = "y".repeat(updated_string_size);

        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(updated_string.clone()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(100, &updated_record.fields[0]);
        assert_string(&updated_string, &updated_record.fields[1]);

        // Verify that the record now spans only 2 pages
        let mut fragment_count = 0;
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            fragment_count += 1;
            if fragment_count == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert_eq!(fragment_count, 2);

        // Verify that the last 2 fragments were deleted from their pages
        for (i, (page_id, slot_id)) in original_fragment_locations.iter().enumerate() {
            if i < 2 {
                // First 2 fragments should still be readable
                let result = if i == 0 {
                    let page = heap_file.read_record_page(*page_id).unwrap();
                    page.page.read_record(*slot_id).is_ok()
                } else {
                    let page = heap_file.read_overflow_page(*page_id).unwrap();
                    page.page.read_record(*slot_id).is_ok()
                };
                assert!(result);
            } else {
                // Last 2 fragments should be deleted
                let page = heap_file.read_overflow_page(*page_id).unwrap();
                let result = page.page.read_record(*slot_id);
                assert!(result.is_err(),);
            }
        }
    }

    #[test]
    fn heap_file_update_middle_string_field_grows_large() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file with 3 string fields
        let columns_metadata = vec![
            ColumnMetadata::new("first_name".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("middle_name".into(), Type::String, 1, 0, 0).unwrap(),
            ColumnMetadata::new("last_name".into(), Type::String, 2, 0, 0).unwrap(),
        ];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a record with three small strings
        let original_record = Record::new(vec![
            Value::String("John".into()).into(),
            Value::String("Middle".into()).into(),
            Value::String("Doe".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the middle string to be very large (more than one page)
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let large_middle_name = "M".repeat(max_single_page_size + 500);

        let column_metadata = columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(large_middle_name.clone()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_string("John", &updated_record.fields[0]);
        assert_string(&large_middle_name, &updated_record.fields[1]);
        assert_string("Doe", &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_update_middle_string_field_shrinks_small() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file with 3 string fields
        let columns_metadata = vec![
            ColumnMetadata::new("first_name".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("middle_name".into(), Type::String, 1, 0, 0).unwrap(),
            ColumnMetadata::new("last_name".into(), Type::String, 2, 0, 0).unwrap(),
        ];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Create a very large middle string (more than one page)
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let large_middle_name = "M".repeat(max_single_page_size + 500);

        // Insert a record with small first/last names and large middle name
        let original_record = Record::new(vec![
            Value::String("John".into()).into(),
            Value::String(large_middle_name.clone()).into(),
            Value::String("Doe".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the middle string to be very small
        let small_middle_name = "M";

        let column_metadata = columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Value::String(small_middle_name.into()))
                .unwrap();

        heap_file
            .update(&record_ptr, vec![update_descriptor])
            .unwrap();

        // Read back the record to verify the update
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_string("John", &updated_record.fields[0]);
        assert_string(small_middle_name, &updated_record.fields[1]);
        assert_string("Doe", &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_update_all_string_fields_grow_ten_times() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file with 5 string fields
        let columns_metadata = vec![
            ColumnMetadata::new("field1".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("field2".into(), Type::String, 1, 0, 0).unwrap(),
            ColumnMetadata::new("field3".into(), Type::String, 2, 0, 0).unwrap(),
            ColumnMetadata::new("field4".into(), Type::String, 3, 0, 0).unwrap(),
            ColumnMetadata::new("field5".into(), Type::String, 4, 0, 0).unwrap(),
        ];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a record with 5 small strings
        let original_strings = [
            "field1_initial",
            "field2_initial",
            "field3_initial",
            "field4_initial",
            "field5_initial",
        ];

        let original_record = Record::new(
            original_strings
                .iter()
                .map(|s| Value::String(s.to_string()).into())
                .collect(),
        );

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Create updated strings that are 10x larger
        let updated_strings: Vec<String> = original_strings.iter().map(|s| s.repeat(10)).collect();

        // Create update descriptors for all 5 fields
        let update_descriptors: Vec<FieldUpdateDescriptor> = columns_metadata
            .iter()
            .zip(updated_strings.iter())
            .map(|(col, s)| {
                FieldUpdateDescriptor::new(col.clone(), Value::String(s.clone())).unwrap()
            })
            .collect();

        // Perform the update
        heap_file.update(&record_ptr, update_descriptors).unwrap();

        // Read back the record to verify all updates
        let updated_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 5);

        // Verify each field was updated correctly
        for (i, expected_string) in updated_strings.iter().enumerate() {
            assert_string(expected_string, &updated_record.fields[i]);
        }
    }

    #[test]
    fn heap_file_update_appends_bytes_without_changing_existing() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Start with a simple schema
        let initial_columns = vec![ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap()];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), initial_columns.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record with just the ID
        let record = Record::new(vec![Value::Int32(42).into()]);
        let ptr = heap_file.insert(record).unwrap();

        // Verify initial record
        let initial_record = heap_file.record(&ptr).unwrap();
        assert_eq!(initial_record.fields.len(), 1);
        assert_i32(42, &initial_record.fields[0]);

        // Now manually construct an update that only appends new bytes at the end
        // We'll directly manipulate the record bytes to simulate adding a string field
        // without changing the existing Int32 bytes

        // Read current record bytes
        let page_chain =
            SinglePageLockChain::<4, PinnedReadPage>::with_record(&heap_file, ptr.page_id).unwrap();
        let current_bytes = heap_file.record_bytes(&ptr, page_chain).unwrap();

        let new_string = "appended_text";
        let mut new_bytes = current_bytes.clone();

        let string_len = new_string.len() as u16;
        new_bytes.extend_from_slice(&string_len.to_le_bytes());
        new_bytes.extend_from_slice(new_string.as_bytes());

        // Create bytes_changed array (first 4 bytes unchanged, rest are new)
        let mut bytes_changed = vec![false; new_bytes.len()];
        bytes_changed[4..].fill(true);

        // Perform the update
        let page_chain =
            SinglePageLockChain::<4, PinnedWritePage>::with_record(&heap_file, ptr.page_id)
                .unwrap();
        heap_file
            .update_record_bytes(&ptr, &new_bytes, &bytes_changed, page_chain)
            .unwrap();

        // Now update the heap_file's column metadata to match the new schema
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file.columns_metadata = new_columns;

        // Read back and verify both fields
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(42, &updated_record.fields[0]);
        assert_string(new_string, &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_concurrent_read_write_multi_fragment_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("field1".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("field2".into(), Type::String, 1, 0, 0).unwrap(),
            ColumnMetadata::new("field3".into(), Type::String, 2, 0, 0).unwrap(),
        ];

        let factory =
            HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        // Each field should be large enough to ensure multi-page record
        let field_size = max_single_page_size / 2;
        let initial_field1 = "A".repeat(field_size);
        let initial_field2 = "B".repeat(field_size);
        let initial_field3 = "C".repeat(field_size);

        let initial_record = Record::new(vec![
            Value::String(initial_field1.clone()).into(),
            Value::String(initial_field2.clone()).into(),
            Value::String(initial_field3.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(initial_record).unwrap();

        let num_iterations = 50;
        let num_reader_threads = 4;
        let num_writer_threads = 2;

        // Expected patterns
        let expected_field1_abc = "A".repeat(field_size);
        let expected_field2_abc = "B".repeat(field_size);
        let expected_field3_abc = "C".repeat(field_size);

        let expected_field1_xyz = "X".repeat(field_size);
        let expected_field2_xyz = "Y".repeat(field_size);
        let expected_field3_xyz = "Z".repeat(field_size);

        // Spawn reader threads
        let mut reader_handles = vec![];
        for _ in 0..num_reader_threads {
            let heap_file_clone = heap_file.clone();
            let ptr = record_ptr;
            let exp_f1_abc = expected_field1_abc.clone();
            let exp_f2_abc = expected_field2_abc.clone();
            let exp_f3_abc = expected_field3_abc.clone();
            let exp_f1_xyz = expected_field1_xyz.clone();
            let exp_f2_xyz = expected_field2_xyz.clone();
            let exp_f3_xyz = expected_field3_xyz.clone();

            let handle = thread::spawn(move || {
                for _ in 0..num_iterations {
                    let record = heap_file_clone.record(&ptr).unwrap();

                    assert_eq!(record.fields.len(), 3,);

                    // Try to match ABC pattern
                    let is_abc_pattern = matches!(&record.fields[0].deref(), Value::String(s) if s == &exp_f1_abc)
                        && matches!(&record.fields[1].deref(), Value::String(s) if s == &exp_f2_abc)
                        && matches!(&record.fields[2].deref(), Value::String(s) if s == &exp_f3_abc);

                    // Try to match XYZ pattern
                    let is_xyz_pattern = matches!(&record.fields[0].deref(), Value::String(s) if s == &exp_f1_xyz)
                        && matches!(&record.fields[1].deref(), Value::String(s) if s == &exp_f2_xyz)
                        && matches!(&record.fields[2].deref(), Value::String(s) if s == &exp_f3_xyz);

                    assert!(is_abc_pattern || is_xyz_pattern,);

                    thread::sleep(std::time::Duration::from_micros(10));
                }
            });

            reader_handles.push(handle);
        }

        // Spawn writer threads
        let mut writer_handles = vec![];
        for _ in 0..num_writer_threads {
            let heap_file_clone = heap_file.clone();
            let ptr = record_ptr;
            let cols = columns_metadata.clone();

            let handle = thread::spawn(move || {
                for iteration in 0..num_iterations {
                    let (char1, char2, char3) = if iteration % 2 == 0 {
                        ('X', 'Y', 'Z')
                    } else {
                        ('A', 'B', 'C')
                    };

                    let updated_field1 = char1.to_string().repeat(field_size);
                    let updated_field2 = char2.to_string().repeat(field_size);
                    let updated_field3 = char3.to_string().repeat(field_size);

                    let update_descriptors = vec![
                        FieldUpdateDescriptor::new(cols[0].clone(), Value::String(updated_field1))
                            .unwrap(),
                        FieldUpdateDescriptor::new(cols[1].clone(), Value::String(updated_field2))
                            .unwrap(),
                        FieldUpdateDescriptor::new(cols[2].clone(), Value::String(updated_field3))
                            .unwrap(),
                    ];

                    heap_file_clone.update(&ptr, update_descriptors).unwrap();

                    thread::sleep(std::time::Duration::from_micros(10));
                }
            });

            writer_handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in reader_handles {
            handle.join().unwrap();
        }

        for handle in writer_handles {
            handle.join().unwrap();
        }

        let final_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(final_record.fields.len(), 3);

        let is_abc = matches!(&final_record.fields[0].deref(), Value::String(s) if s == &expected_field1_abc)
            && matches!(&final_record.fields[1].deref(), Value::String(s) if s == &expected_field2_abc)
            && matches!(&final_record.fields[2].deref(), Value::String(s) if s == &expected_field3_abc);

        let is_xyz = matches!(&final_record.fields[0].deref(), Value::String(s) if s == &expected_field1_xyz)
            && matches!(&final_record.fields[1].deref(), Value::String(s) if s == &expected_field2_xyz)
            && matches!(&final_record.fields[2].deref(), Value::String(s) if s == &expected_field3_xyz);

        assert!(is_abc || is_xyz);
    }

    // HeapFile deleting record

    #[test]
    fn heap_file_delete_single_page_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Insert a simple record
        let original_record = Record::new(vec![
            Value::Int32(100).into(),
            Value::String("test_record".into()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Delete the record
        heap_file.delete(&record_ptr).unwrap();

        // Verify the record was deleted - reading should fail
        let result = heap_file.record(&record_ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(_)
        ));
    }

    #[test]
    fn heap_file_delete_multi_page_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Create a large record that spans multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();

        // Create a string large enough to span 3 pages
        let large_string_size = (2 * max_single_page_size) - record_overhead + 100;
        let large_string = "x".repeat(large_string_size);

        let original_record = Record::new(vec![
            Value::Int32(42).into(),
            Value::String(large_string).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all page IDs and slots that contain fragments before deletion
        let mut fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            fragment_locations.push((ptr.page_id, ptr.slot_id));

            if fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot_id).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        // Delete the record
        heap_file.delete(&record_ptr).unwrap();

        // Verify all fragments were deleted from their respective pages
        for (i, (page_id, slot_id)) in fragment_locations.iter().enumerate() {
            let result = if i == 0 {
                // First fragment on record page
                let page = heap_file.read_record_page(*page_id).unwrap();
                page.page.read_record(*slot_id).is_err()
            } else {
                // Subsequent fragments on overflow pages
                let page = heap_file.read_overflow_page(*page_id).unwrap();
                page.page.read_record(*slot_id).is_err()
            };

            // Reading a deleted slot should fail
            assert!(result);
        }

        // Verify the record was deleted - reading should fail
        let result = heap_file.record(&record_ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(_)
        ));
    }

    #[test]
    fn heap_file_delete_concurrent_read_multi_page_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        // Create heap file
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        // Create a large record that spans multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<u32>() + size_of::<u16>();

        // Create a string large enough to span 4 pages
        let large_string_size = (3 * max_single_page_size) - record_overhead + 100;
        let large_string = "x".repeat(large_string_size);

        let original_record = Record::new(vec![
            Value::Int32(42).into(),
            Value::String(large_string.clone()).into(),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        let num_reader_threads = 8;
        let mut reader_handles = vec![];

        // Spawn reader threads
        for _ in 0..num_reader_threads {
            let heap_file_clone = heap_file.clone();
            let ptr = record_ptr;
            let expected_string = large_string.clone();

            let handle = thread::spawn(move || {
                match heap_file_clone.record(&ptr) {
                    Ok(record) => {
                        // If we successfully read, verify it's complete and correct
                        assert_eq!(record.fields.len(), 2);
                        assert_i32(42, &record.fields[0]);
                        assert_string(&expected_string, &record.fields[1]);
                    }
                    Err(_) => {
                        // Read failed - this is expected after deletion
                    }
                }
            });

            reader_handles.push(handle);
        }

        // Let readers run for a bit before deleting
        thread::sleep(Duration::from_micros(5));

        // Delete the record
        heap_file.delete(&record_ptr).unwrap();

        // Wait for all reader threads and collect results
        for handle in reader_handles {
            handle.join().unwrap();
        }

        // Verify the record is deleted
        let result = heap_file.record(&record_ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::SlottedPageError(_)
        ));
    }

    #[test]
    fn heap_file_add_column_to_empty_table() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Add a new column to empty table
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .add_column_migration(2, size_of::<i32>(), Value::Int32(25), new_columns.clone())
            .unwrap();

        // Verify metadata was updated
        assert_eq!(heap_file.columns_metadata.len(), 3);
        assert_eq!(heap_file.columns_metadata[2].name(), "age");
    }

    #[test]
    fn heap_file_add_column_single_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Add new column with default value
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let new_column_offset = size_of::<i32>();
        heap_file
            .add_column_migration(2, new_column_offset, Value::Int32(25), new_columns.clone())
            .unwrap();

        // Read back and verify
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
        assert_i32(25, &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_add_column_multiple_records() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert multiple records
        let mut ptrs = vec![];
        for i in 0..5 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("user_{}", i)).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            ptrs.push(ptr);
        }

        // Add new column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .add_column_migration(2, size_of::<i32>(), Value::Int32(30), new_columns)
            .unwrap();

        // Verify all records were updated
        for (i, ptr) in ptrs.iter().enumerate() {
            let record = heap_file.record(ptr).unwrap();
            assert_eq!(record.fields.len(), 3);
            assert_i32(i as i32, &record.fields[0]);
            assert_string(&format!("user_{}", i), &record.fields[1]);
            assert_i32(30, &record.fields[2]);
        }
    }

    #[test]
    fn heap_file_add_column_at_beginning() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Add new column at the beginning
        let new_columns = vec![
            ColumnMetadata::new("prefix".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("id".into(), Type::I32, 1, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 2, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .add_column_migration(0, 0, Value::String("Mr.".into()), new_columns)
            .unwrap();

        // Read and verify order
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_string("Mr.", &updated_record.fields[0]);
        assert_i32(1, &updated_record.fields[1]);
        assert_string("Alice", &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_add_column_multi_fragment_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Create a large record that spans multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<i32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead + 500;
        let large_string = "x".repeat(large_string_size);

        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String(large_string.clone()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Add new column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .add_column_migration(2, size_of::<i32>(), Value::Int32(25), new_columns)
            .unwrap();

        // Verify the large record still works
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_i32(1, &updated_record.fields[0]);
        assert_string(&large_string, &updated_record.fields[1]);
        assert_i32(25, &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_add_column_across_multiple_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert records across multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = size_of::<i32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead;

        let mut ptrs = vec![];

        // Insert 3 large records to span multiple pages
        for i in 0..3 {
            let large_string = format!("{}", i).repeat(large_string_size);
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(large_string).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            ptrs.push(ptr);
        }

        // Add new column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("status".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .add_column_migration(2, size_of::<i32>(), Value::Int32(1), new_columns)
            .unwrap();

        // Verify all records across all pages were updated
        for (i, ptr) in ptrs.iter().enumerate() {
            let record = heap_file.record(ptr).unwrap();
            assert_eq!(record.fields.len(), 3);
            assert_i32(i as i32, &record.fields[0]);
            assert_i32(1, &record.fields[2]);
        }
    }

    #[test]
    fn heap_file_add_multiple_columns_sequentially() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let mut heap_file = factory.create_heap_file().unwrap();

        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Add first column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .add_column_migration(2, size_of::<i32>(), Value::Int32(25), new_columns)
            .unwrap();

        // Add second column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
            ColumnMetadata::new("city".into(), Type::String, 3, 2 * size_of::<i32>(), 3).unwrap(),
        ];

        heap_file
            .add_column_migration(
                3,
                2 * size_of::<i32>(),
                Value::String("NYC".into()),
                new_columns,
            )
            .unwrap();

        // Verify both columns were added
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 4);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
        assert_i32(25, &updated_record.fields[2]);
        assert_string("NYC", &updated_record.fields[3]);
    }

    #[test]
    fn heap_file_remove_column_from_empty_table() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Remove column from empty table
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns.clone())
            .unwrap();

        // Verify metadata was updated
        assert_eq!(heap_file.columns_metadata.len(), 2);
        assert_eq!(heap_file.columns_metadata[0].name(), "id");
        assert_eq!(heap_file.columns_metadata[1].name(), "name");
    }

    #[test]
    fn heap_file_remove_column_single_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
            Value::Int32(25).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Remove the age column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns)
            .unwrap();

        // Read back and verify
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_remove_column_multiple_records() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert multiple records
        let mut ptrs = vec![];
        for i in 0..5 {
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(format!("user_{}", i)).into(),
                Value::Int32(20 + i).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            ptrs.push(ptr);
        }

        // Remove the age column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns)
            .unwrap();

        // Verify all records were updated
        for (i, ptr) in ptrs.iter().enumerate() {
            let record = heap_file.record(ptr).unwrap();
            assert_eq!(record.fields.len(), 2);
            assert_i32(i as i32, &record.fields[0]);
            assert_string(&format!("user_{}", i), &record.fields[1]);
        }
    }

    #[test]
    fn heap_file_remove_first_column() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("prefix".into(), Type::String, 0, 0, 0).unwrap(),
            ColumnMetadata::new("id".into(), Type::I32, 1, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 2, size_of::<i32>(), 1).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::String("Mr.".into()).into(),
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Remove the first column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(0, 0, new_columns)
            .unwrap();

        // Read and verify order
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_remove_middle_column() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 2, 2 * size_of::<i32>(), 2).unwrap(),
            ColumnMetadata::new("email".into(), Type::String, 3, 2 * size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert a record
        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::Int32(25).into(),
            Value::String("Alice".into()).into(),
            Value::String("alice@test.com".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Remove the middle column (age)
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("email".into(), Type::String, 2, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(1, size_of::<i32>(), new_columns)
            .unwrap();

        // Verify order
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 3);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
        assert_string("alice@test.com", &updated_record.fields[2]);
    }

    #[test]
    fn heap_file_remove_column_from_multi_fragment_record() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Create a large record that spans multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = 2 * size_of::<i32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead + 500;
        let large_string = "x".repeat(large_string_size);

        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String(large_string.clone()).into(),
            Value::Int32(25).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Remove the age column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns)
            .unwrap();

        // Verify the large record still works
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(1, &updated_record.fields[0]);
        assert_string(&large_string, &updated_record.fields[1]);
    }

    #[test]
    fn heap_file_remove_column_across_multiple_pages() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("status".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        // Insert records across multiple pages
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();
        let record_overhead = 2 * size_of::<i32>() + size_of::<u16>();
        let large_string_size = max_single_page_size - record_overhead;

        let mut ptrs = vec![];

        // Insert 3 large records to span multiple pages
        for i in 0..3 {
            let large_string = format!("{}", i).repeat(large_string_size);
            let record = Record::new(vec![
                Value::Int32(i).into(),
                Value::String(large_string).into(),
                Value::Int32(1).into(),
            ]);
            let ptr = heap_file.insert(record).unwrap();
            ptrs.push(ptr);
        }

        // Remove the status column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns)
            .unwrap();

        // Verify all records across all pages were updated
        for (i, ptr) in ptrs.iter().enumerate() {
            let record = heap_file.record(ptr).unwrap();
            assert_eq!(record.fields.len(), 2);
            assert_i32(i as i32, &record.fields[0]);
        }
    }

    #[test]
    fn heap_file_remove_multiple_columns_sequentially() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let columns_metadata = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
            ColumnMetadata::new("city".into(), Type::String, 3, 2 * size_of::<i32>(), 3).unwrap(),
        ];

        let factory = HeapFileFactory::<4>::new(file_key.clone(), cache.clone(), columns_metadata);
        let mut heap_file = factory.create_heap_file().unwrap();

        let record = Record::new(vec![
            Value::Int32(1).into(),
            Value::String("Alice".into()).into(),
            Value::Int32(25).into(),
            Value::String("NYC".into()).into(),
        ]);
        let ptr = heap_file.insert(record).unwrap();

        // Remove city column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
            ColumnMetadata::new("age".into(), Type::I32, 2, size_of::<i32>(), 2).unwrap(),
        ];

        heap_file
            .remove_column_migration(3, 2 * size_of::<i32>(), new_columns)
            .unwrap();

        // Remove age column
        let new_columns = vec![
            ColumnMetadata::new("id".into(), Type::I32, 0, 0, 0).unwrap(),
            ColumnMetadata::new("name".into(), Type::String, 1, size_of::<i32>(), 1).unwrap(),
        ];

        heap_file
            .remove_column_migration(2, size_of::<i32>(), new_columns)
            .unwrap();

        // Verify both columns were removed
        let updated_record = heap_file.record(&ptr).unwrap();
        assert_eq!(updated_record.fields.len(), 2);
        assert_i32(1, &updated_record.fields[0]);
        assert_string("Alice", &updated_record.fields[1]);
    }

    #[ignore = "TODO: need to figure out better way to handle benchmark tests"]
    #[test]
    fn heap_file_stress_test_concurrent_inserts_many_threads() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = Arc::new(factory.create_heap_file().unwrap());

        let num_threads = 16;
        let records_per_thread = 3000;
        let total_records = num_threads * records_per_thread;

        println!("\n=== HeapFile Concurrent Insert Stress Test ===");
        println!(
            "Threads: {}, Records per thread: {}, Total: {}",
            num_threads, records_per_thread, total_records
        );

        let successful_inserts = Arc::new(AtomicUsize::new(0));
        let failed_inserts = Arc::new(AtomicUsize::new(0));

        // Track which thread/operation failed
        let error_details = Arc::new(Mutex::new(Vec::new()));

        let start = std::time::Instant::now();

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let heap_file_clone = heap_file.clone();
            let successful_clone = successful_inserts.clone();
            let failed_clone = failed_inserts.clone();
            let error_details_clone = error_details.clone();

            let handle = thread::spawn(move || {
                let mut local_ptrs = Vec::with_capacity(records_per_thread);

                for i in 0..records_per_thread {
                    let record_id = (thread_id * records_per_thread + i) as i32;
                    let name = format!("thread_{}_record_{}", thread_id, i);

                    let record = Record::new(vec![
                        Value::Int32(record_id).into(),
                        Value::String(name.clone()).into(),
                    ]);

                    match heap_file_clone.insert(record) {
                        Ok(ptr) => {
                            successful_clone.fetch_add(1, Ordering::Relaxed);
                            local_ptrs.push((ptr, record_id, name));
                        }
                        Err(e) => {
                            println!(
                                "[Thread {}] INSERT FAILED at record {}: {}",
                                thread_id, i, e
                            );
                            println!("[Thread {}] Error details: {:?}", thread_id, e);
                            error_details_clone
                                .lock()
                                .push(format!("Thread {} insert {} failed: {}", thread_id, i, e));
                            failed_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // if i > 0 && i % 100 == 0 {
                    //     println!("[{thread_id}] at {i}");
                    // }
                }

                local_ptrs
            });

            handles.push(handle);
        }

        // Collect all record pointers from all threads
        let mut all_record_ptrs = Vec::new();
        for (thread_idx, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(thread_ptrs) => {
                    all_record_ptrs.extend(thread_ptrs);
                }
                Err(e) => {
                    println!("Thread {} panicked: {:?}", thread_idx, e);

                    // Print all collected errors before panicking
                    let errors = error_details.lock();
                    println!("\n=== All Errors Collected ===");
                    for err in errors.iter() {
                        println!("{}", err);
                    }

                    panic!("Thread {} panicked during execution", thread_idx);
                }
            }
        }

        let duration = start.elapsed();
        let successful = successful_inserts.load(Ordering::Relaxed);
        let failed = failed_inserts.load(Ordering::Relaxed);

        println!("\n=== Insert Phase Results ===");
        println!("Duration: {:?}", duration);
        println!("Successful inserts: {}", successful);
        println!("Failed inserts: {}", failed);
        println!(
            "Throughput: {:.2} records/sec",
            successful as f64 / duration.as_secs_f64()
        );

        // Print all errors if any
        let errors = error_details.lock();
        if !errors.is_empty() {
            println!("\n=== Errors Encountered ===");
            for err in errors.iter() {
                println!("{}", err);
            }
        }

        // Verify all inserts succeeded
        if successful != total_records || failed != 0 {
            panic!(
                "Expected {} successful inserts, got {}. Failed: {}",
                total_records, successful, failed
            );
        }

        // Verification phase: read back all records concurrently
        println!("\n=== Verification Phase ===");
        let verify_start = std::time::Instant::now();
        let verification_errors = Arc::new(AtomicUsize::new(0));
        let verify_error_details = Arc::new(Mutex::new(Vec::new()));

        let chunks: Vec<_> = all_record_ptrs
            .chunks(all_record_ptrs.len() / num_threads)
            .map(|chunk| chunk.to_vec())
            .collect();

        let mut verify_handles = vec![];

        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            let heap_file_clone = heap_file.clone();
            let errors_clone = verification_errors.clone();
            let verify_error_details_clone = verify_error_details.clone();

            let handle = thread::spawn(move || {
                for (idx, (ptr, expected_id, expected_name)) in chunk.iter().enumerate() {
                    match heap_file_clone.record(ptr) {
                        Ok(record) => {
                            if record.fields.len() != 2 {
                                println!(
                                    "[Verify chunk {}] Wrong field count at idx {}: expected 2, got {}",
                                    chunk_idx,
                                    idx,
                                    record.fields.len()
                                );
                                errors_clone.fetch_add(1, Ordering::Relaxed);
                                verify_error_details_clone.lock().push(format!(
                                    "Chunk {} idx {}: wrong field count",
                                    chunk_idx, idx
                                ));
                                continue;
                            }

                            match &record.fields[0].deref() {
                                Value::Int32(id) if *id == *expected_id => {}
                                Value::Int32(id) => {
                                    println!(
                                        "[Verify chunk {}] Wrong ID at idx {}: expected {}, got {}",
                                        chunk_idx, idx, expected_id, id
                                    );
                                    errors_clone.fetch_add(1, Ordering::Relaxed);
                                    verify_error_details_clone
                                        .lock()
                                        .push(format!("Chunk {} idx {}: wrong ID", chunk_idx, idx));
                                    continue;
                                }
                                _ => {
                                    println!(
                                        "[Verify chunk {}] Wrong field type at idx {}: expected Int32",
                                        chunk_idx, idx
                                    );
                                    errors_clone.fetch_add(1, Ordering::Relaxed);
                                    verify_error_details_clone.lock().push(format!(
                                        "Chunk {} idx {}: wrong field type",
                                        chunk_idx, idx
                                    ));
                                    continue;
                                }
                            }

                            match &record.fields[1].deref() {
                                Value::String(name) if name == expected_name => {}
                                Value::String(name) => {
                                    println!(
                                        "[Verify chunk {}] Wrong name at idx {}: expected {}, got {}",
                                        chunk_idx, idx, expected_name, name
                                    );
                                    errors_clone.fetch_add(1, Ordering::Relaxed);
                                    verify_error_details_clone.lock().push(format!(
                                        "Chunk {} idx {}: wrong name",
                                        chunk_idx, idx
                                    ));
                                }
                                _ => {
                                    println!(
                                        "[Verify chunk {}] Wrong field type at idx {}: expected String",
                                        chunk_idx, idx
                                    );
                                    errors_clone.fetch_add(1, Ordering::Relaxed);
                                    verify_error_details_clone.lock().push(format!(
                                        "Chunk {} idx {}: wrong field type for name",
                                        chunk_idx, idx
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            println!(
                                "[Verify chunk {}] Failed to read at idx {} (page_id={}, slot_id={}): {}",
                                chunk_idx, idx, ptr.page_id, ptr.slot_id, e
                            );
                            errors_clone.fetch_add(1, Ordering::Relaxed);
                            verify_error_details_clone.lock().push(format!(
                                "Chunk {} idx {}: read failed: {}",
                                chunk_idx, idx, e
                            ));
                        }
                    }
                }
            });

            verify_handles.push(handle);
        }

        for handle in verify_handles {
            handle.join().unwrap();
        }

        let verify_duration = verify_start.elapsed();
        let errors = verification_errors.load(Ordering::Relaxed);

        println!("Verification duration: {:?}", verify_duration);
        println!("Verification errors: {}", errors);
        println!(
            "Read throughput: {:.2} records/sec",
            total_records as f64 / verify_duration.as_secs_f64()
        );

        // Print verification errors if any
        let verify_errors = verify_error_details.lock();
        if !verify_errors.is_empty() {
            println!("\n=== Verification Errors ===");
            for err in verify_errors.iter() {
                println!("{}", err);
            }
        }

        assert_eq!(errors, 0, "Found {} verification errors", errors);

        // Verify all_records returns correct count
        let all_records = collect_all_records(&heap_file);
        assert_eq!(all_records.len(), total_records);

        // Verify no duplicate IDs
        let mut seen_ids = HashSet::new();
        for record in &all_records {
            match &record.fields[0].deref() {
                Value::Int32(id) => {
                    assert!(seen_ids.insert(id), "Found duplicate record ID: {}", id);
                }
                _ => panic!("Expected Int32 field"),
            }
        }

        println!("\n=== Test Passed ===");
    }
}
