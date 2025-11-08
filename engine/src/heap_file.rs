use std::{
    array,
    marker::PhantomData,
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
    data_types::{DbSerializable, DbSerializationError},
    record::{Field, Record, RecordError},
    slotted_page::{
        InsertResult, PageType, ReprC, Slot, SlotId, SlottedPage, SlottedPageBaseHeader,
        SlottedPageError, SlottedPageHeader, UpdateResult,
    },
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
    /// Set of all pages that were already read from disk (either directly by FSM or by HeapFile while reading/creating page).
    /// The purpose of this is to minimize duplicates in FSM.
    already_read: DashSet<PageId>,
    _page_type_marker: PhantomData<H>,
}

impl<const BUCKETS_COUNT: usize, H: BaseHeapPageHeader> FreeSpaceMap<BUCKETS_COUNT, H> {
    fn new(cache: Arc<Cache>, file_key: FileKey, first_page_id: PageId) -> Self {
        let buckets: [SegQueue<PageId>; BUCKETS_COUNT] = array::from_fn(|_| SegQueue::new());

        FreeSpaceMap {
            buckets,
            cache,
            file_key,
            next_page_to_read: Mutex::new(first_page_id),
            already_read: DashSet::new(),
            _page_type_marker: PhantomData::<H>,
        }
    }

    /// Finds a page that has at least `needed_space` bytes free.
    ///
    /// If a candidate is stale the search continues.
    /// If page was found it is removed from FSM and should be added back once the thread that removed it finished processing it.
    /// Returns `Ok(None)` if no suitable page is found.
    fn page_with_free_space(
        &self,
        needed_space: usize,
    ) -> Result<Option<(PageId, SlottedPage<PinnedWritePage, H>)>, HeapFileError> {
        let start_bucket_idx = self.bucket_for_space(needed_space);
        let mut insert_back = vec![];
        for b in start_bucket_idx..BUCKETS_COUNT {
            insert_back.clear();
            while let Some(page_id) = self.buckets[b].pop() {
                let key = self.file_page_ref(page_id);
                let page = self.cache.pin_write(&key)?;
                let slotted_page = SlottedPage::new(page)?;
                let actual_free_space = slotted_page.free_space()?;
                if actual_free_space >= needed_space as _ {
                    for id in insert_back {
                        self.buckets[b].push(id);
                    }
                    return Ok(Some((page_id, slotted_page)));
                }
                // If we don't use this page and it still belongs to this bucket we must reinsert it to keep it in FSM.
                // If it now has different bucket it means that it was edited (other thread made insert/delete) and that thread should put it back in fsm.
                let actual_bucket = self.bucket_for_space(actual_free_space as usize);
                if actual_bucket == b {
                    // We cannot insert it right away, because we would end up in infinity loop
                    insert_back.push(page_id);
                }
            }
            for id in &insert_back {
                self.buckets[b].push(*id);
            }
        }

        // We didn't find page in current FSM, we fallback to reading directly from disk
        self.page_with_free_space_from_disk(needed_space)
    }

    /// Iterates over pages not yet loaded from disk and returns one that has enough free space.
    /// If no such page is found then `None` is returned.
    fn page_with_free_space_from_disk(
        &self,
        needed_space: usize,
    ) -> Result<Option<(PageId, SlottedPage<PinnedWritePage, H>)>, HeapFileError> {
        while let Some((page_id, slotted_page)) = self.load_next_page()? {
            // It means this page was already in FSM and we already checked it
            if self.already_read.contains(&page_id) {
                continue;
            }
            self.already_read.insert(page_id);
            let space = slotted_page.free_space()?;
            if space >= needed_space as _ {
                return Ok(Some((page_id, slotted_page)));
            }
            self.update_page_bucket(page_id, space as _);
        }
        Ok(None)
    }

    /// Loads page with id [`Self::next_page_to_read`] and updates it to the next pointer.
    /// Returns id and loaded page or `None` if there is no more page to read.
    fn load_next_page(
        &self,
    ) -> Result<Option<(PageId, SlottedPage<PinnedWritePage, H>)>, HeapFileError> {
        let mut page_id = self.next_page_to_read.lock();
        if *page_id == H::NO_NEXT_PAGE {
            return Ok(None);
        }

        let key = self.file_page_ref(*page_id);
        let page = self.cache.pin_write(&key)?;
        let slotted_page = SlottedPage::<_, H>::new(page)?;

        let read_page_id = *page_id;
        let next_page = slotted_page.get_header()?.next_page();
        *page_id = next_page;

        Ok(Some((read_page_id, slotted_page)))
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

    /// Same as [`Self::update_page_bucket`], but it also inserts `page_id` into [`Self::already_read`].
    ///
    /// It should be used by HeapFile when it reads/creates a page.
    /// The reason why this is a separate function is that using [`Self::already_read`] might require waiting for lock,
    /// so we should do it only when really needed.
    fn update_page_bucket_with_duplicate_check(&self, page_id: PageId, free_space: usize) {
        if self.already_read.contains(&page_id) {
            return;
        }
        self.already_read.insert(page_id);
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
        FilePageRef::new(page_id, self.file_key.clone())
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

impl From<&RecordPtr> for Vec<u8> {
    fn from(value: &RecordPtr) -> Self {
        let mut buffer = Vec::with_capacity(size_of::<RecordPtr>());
        value.page_id.serialize(&mut buffer);
        value.slot.serialize(&mut buffer);
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
        new_buffer.extend_from_slice(&buffer);
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
pub(crate) struct FieldUpdateDescriptor {
    column: ColumnMetadata,
    field: Field,
}

impl FieldUpdateDescriptor {
    /// Creates new [`FieldUpdateDescriptor`].
    ///
    /// Returns an error when `column` and `field` have different types.
    pub(crate) fn new(
        column: ColumnMetadata,
        field: Field,
    ) -> Result<FieldUpdateDescriptor, HeapFileError> {
        if column.ty() != field.ty() {
            return Err(HeapFileError::ColumnAndFieldTypesDontMatch {
                column_ty: column.ty().to_string(),
                field_ty: field.ty().to_string(),
            });
        }
        Ok(FieldUpdateDescriptor { column, field })
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

/// Helper struct used for locking pages when working with records.
///
/// Locking pages in [`HeapFile`] should work as follows:
/// - read page
/// - do an action on the page
/// - read next page
/// - drop the previous page
/// This struct encapsulates this drop-only-after-read strategy.
///
/// When dealing with record that spans multiple pages (multi-fragment record) pages shouldn't be read by hand,
/// but instead this struct should be used.
struct PageLockChain<'hf, const BUCKETS_COUNT: usize, P> {
    heap_file: &'hf HeapFile<BUCKETS_COUNT>,
    record_page: Option<HeapPage<P, RecordPageHeader>>,
    overflow_page: Option<HeapPage<P, OverflowPageHeader>>,
}

impl<'hf, const BUCKETS_COUNT: usize, P> PageLockChain<'hf, BUCKETS_COUNT, P> {
    /// Gets current record page.
    ///
    /// Panics if we hold overflow page instead.
    fn record_page(&self) -> &HeapPage<P, RecordPageHeader> {
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
    fn record_page_mut(&mut self) -> &mut HeapPage<P, RecordPageHeader> {
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
    fn overflow_page(&self) -> &HeapPage<P, OverflowPageHeader> {
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
    fn overflow_page_mut(&mut self) -> &mut HeapPage<P, OverflowPageHeader> {
        match &mut self.overflow_page {
            Some(overflow_page) => overflow_page,
            None => panic!(
                "invalid usage of PageLockChain - tried to get record page when overflow page is stored instead"
            ),
        }
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageLockChain<'hf, BUCKETS_COUNT, PinnedReadPage> {
    /// Creates new [`PageLockChain`] that starts with record page
    fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.read_record_page(page_id)?;
        Ok(PageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
        })
    }

    /// Creates new [`PageLockChain`] that starts with overflow page
    fn with_overflow(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let overflow_page = heap_file.read_overflow_page(page_id)?;
        Ok(PageLockChain {
            heap_file,
            record_page: None,
            overflow_page: Some(overflow_page),
        })
    }

    /// Locks the next page and only then drops the previous one
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        // Read next page
        let new_page = self.heap_file.read_overflow_page(next_page_id)?;

        // Drop previous page
        self.record_page = None;
        self.overflow_page = Some(new_page);
        Ok(())
    }
}

impl<'hf, const BUCKETS_COUNT: usize> PageLockChain<'hf, BUCKETS_COUNT, PinnedWritePage> {
    /// Creates new [`PageLockChain`] that starts with record page
    fn with_record(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let record_page = heap_file.write_record_page(page_id)?;
        Ok(PageLockChain {
            heap_file,
            record_page: Some(record_page),
            overflow_page: None,
        })
    }

    /// Creates new [`PageLockChain`] that starts with overflow page
    fn with_overflow(
        heap_file: &'hf HeapFile<BUCKETS_COUNT>,
        page_id: PageId,
    ) -> Result<Self, HeapFileError> {
        let overflow_page = heap_file.write_overflow_page(page_id)?;
        Ok(PageLockChain {
            heap_file,
            record_page: None,
            overflow_page: Some(overflow_page),
        })
    }

    /// Locks the next page and only then drops the previous one
    fn advance(&mut self, next_page_id: PageId) -> Result<(), HeapFileError> {
        // Read next page
        let new_page = self.heap_file.write_overflow_page(next_page_id)?;

        // Drop previous page
        self.record_page = None;
        self.overflow_page = Some(new_page);
        Ok(())
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
    _padding: u16,
    next_page: PageId,
}

impl RecordPageHeader {
    fn new(next_page: PageId) -> Self {
        Self {
            base: SlottedPageBaseHeader::new(size_of::<RecordPageHeader>() as _, PageType::Heap),
            _padding: Default::default(),
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
            _padding: Default::default(),
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
    _padding: u16,
    next_page: PageId,
}

impl OverflowPageHeader {
    fn new(next_page: PageId) -> Self {
        Self {
            base: SlottedPageBaseHeader::new(
                size_of::<OverflowPageHeader>() as _,
                PageType::Overflow,
            ),
            _padding: Default::default(),
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
            _padding: Default::default(),
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
}

impl<P, H> HeapPage<P, H>
where
    P: PageWrite + PageRead,
    H: BaseHeapPageHeader,
{
    /// Inserts `data` into the page and returns its [`SlotId`].
    /// If defragmentation is needed to store the data, it's done automatically and insertion is done once again.
    ///
    /// This function assumes that [`self`] has enough free space to insert `data` (the page should not be chosen by hand, but instead [`FreeSpaceMap`] should be used to get page with enough free space).
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
pub(crate) enum HeapFileError {
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
    #[error("column type ({column_ty}) and field type ({field_ty}) do not match")]
    ColumnAndFieldTypesDontMatch { column_ty: String, field_ty: String },
    #[error("{0}")]
    DBSerializationError(#[from] DbSerializationError),
}

/// Structure responsible for managing on-disk heap files.
///
/// Each [`HeapFile`] instance corresponds to a single physical file on disk.
/// For concurrent access by multiple threads, wrap the instance in `Arc<HeapFile>`.
pub(crate) struct HeapFile<const BUCKETS_COUNT: usize> {
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
    pub(crate) fn record(&self, ptr: &RecordPtr) -> Result<Record, HeapFileError> {
        let record_bytes = self.record_bytes(ptr)?;
        let record = Record::deserialize(&self.columns_metadata, &record_bytes)?;
        Ok(record)
    }

    /// Inserts `record` into heap file and returns its [`RecordPtr`].
    pub(crate) fn insert(&self, record: Record) -> Result<RecordPtr, HeapFileError> {
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
    pub(crate) fn update(
        &self,
        ptr: &RecordPtr,
        updated_fields: Vec<FieldUpdateDescriptor>,
    ) -> Result<(), HeapFileError> {
        // Read record bytes from disk
        let mut record_bytes = self.record_bytes(ptr)?;
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
        self.update_record_bytes(ptr, &record_bytes, &bytes_changed)?;

        Ok(())
    }

    /// Removes [`Record`] located at `ptr`.
    pub(crate) fn delete(&self, ptr: &RecordPtr) -> Result<(), HeapFileError> {
        let mut page_chain =
            PageLockChain::<BUCKETS_COUNT, PinnedWritePage>::with_record(&self, ptr.page_id)?;
        let first_page = page_chain.record_page_mut();
        let record_first_fragment = first_page.record_fragment(ptr.slot)?;

        let next_ptr = record_first_fragment.next_fragment;

        // Delete first fragment of the record
        first_page.delete(ptr.slot)?;

        // Follow record chain and delete each fragment
        self.delete_record_fragments_from_overflow_pages_chain(page_chain, next_ptr)?;
        Ok(())
    }

    /// Flushes [`HeapFile::metadata`] content to disk.
    ///
    /// If metadata is not dirty then this is no-op.
    pub(crate) fn flush_metadata(&mut self) -> Result<(), HeapFileError> {
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

    /// Reads [`Record`] located at `ptr` and returns its bare bytes.
    fn record_bytes(&self, ptr: &RecordPtr) -> Result<Vec<u8>, HeapFileError> {
        let mut page_chain =
            PageLockChain::<BUCKETS_COUNT, PinnedReadPage>::with_record(&self, ptr.page_id)?;

        // Read first fragment
        let first_page = page_chain.record_page();
        let fragment = first_page.record_fragment(ptr.slot)?;
        let mut full_record_bytes = Vec::from(fragment.data);

        // Follow chain of other fragments
        let mut next_ptr = fragment.next_fragment;
        while let Some(next) = next_ptr {
            page_chain.advance(next.page_id)?;
            let next_fragment_page = page_chain.overflow_page();
            let fragment = next_fragment_page.record_fragment(next.slot)?;
            full_record_bytes.extend_from_slice(fragment.data);
            next_ptr = fragment.next_fragment;
        }
        Ok(full_record_bytes)
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

                bytes_changed[offset..(offset + update.field.size_serialized())].fill(true);

                update.field.serialize_into(&mut data[offset..]);
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

                let mut serialized = Vec::with_capacity(update.field.size_serialized());
                update.field.serialize(&mut serialized);

                let bytes_changed_frag = vec![true; serialized.len()];
                bytes_changed.splice(
                    offset..(offset + before_update_total_size),
                    bytes_changed_frag,
                );
                // When we update a string and its len changes we must update all fields that come after it
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
    fn update_record_bytes(
        &self,
        start: &RecordPtr,
        bytes: &[u8],
        bytes_changed: &[bool],
    ) -> Result<(), HeapFileError> {
        let mut processor = FragmentProcessor::new(bytes, bytes_changed);
        let mut page_chain =
            PageLockChain::<BUCKETS_COUNT, PinnedWritePage>::with_record(&self, start.page_id)?;

        let mut first_page = page_chain.record_page_mut();
        let previous_first_fragment = first_page.record_fragment(start.slot)?;

        // Previously record was stored on single page.
        if previous_first_fragment.next_fragment.is_none() {
            let previous_len = previous_first_fragment.data.len();

            if !processor.has_changes(previous_len) {
                // No changes in this fragment, skip update
                return Ok(());
            }

            // Try to update in place
            if self.try_update_fragment_in_place(
                first_page,
                start.slot,
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
                &mut first_page,
                start.slot,
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

        // Check if first fragment has any changes
        if processor.has_changes(first_frag_len) {
            // Record was stored across more than one page, but it now can be saved only in first page
            if processor.is_last_fragment(first_frag_len) {
                let with_tag = RecordTag::with_final(processor.remaining_bytes);
                self.update_record_page_with_fsm(
                    &mut first_page,
                    start.slot,
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
                &mut first_page,
                start.slot,
                &first_page_part_with_tag,
                start.page_id,
            )?;
        }

        // Skip first fragment after processing it
        processor.skip_fragment(first_frag_len);
        let mut current_ptr = next_tag;

        while !processor.is_complete() {
            page_chain.advance(current_ptr.page_id)?;
            let mut current_page = page_chain.overflow_page_mut();
            let current_fragment = current_page.record_fragment(current_ptr.slot)?;

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
                        current_page.update(current_ptr.slot, &current_with_tag)?;
                        return Ok(());
                    }
                }
            }

            // Try to update in place
            if self.try_update_fragment_in_place(
                &mut current_page,
                current_ptr.slot,
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
                        RecordTag::with_has_continuation(&current_page_part, &next_ptr);
                    current_page.update(current_ptr.slot, &current_with_tag)?;
                    current_ptr = next_ptr;
                }
                None => {
                    // This is the last fragment from original chain, so additional bytes can be added wherever we want
                    self.split_and_extend_fragment(
                        &mut current_page,
                        current_ptr.slot,
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
    fn try_update_fragment_in_place<H>(
        &self,
        page: &mut HeapPage<PinnedWritePage, H>,
        slot: SlotId,
        page_id: PageId,
        remaining_bytes: &[u8],
        previous_len: usize,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<bool, HeapFileError>
    where
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
    fn split_and_extend_fragment<H>(
        &self,
        page: &mut HeapPage<PinnedWritePage, H>,
        slot: SlotId,
        page_id: PageId,
        remaining_bytes: &[u8],
        previous_fragment_len: usize,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<(), HeapFileError>
    where
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
    pub fn insert_using_only_overflow_pages(
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

        return Ok(RecordPtr {
            page_id,
            slot: slot_id,
        });
    }

    /// Deletes all record fragments from chain of overflow pages starting in `next_ptr`.
    fn delete_record_fragments_from_overflow_pages_chain<'hf>(
        &'hf self,
        mut chain: PageLockChain<'hf, BUCKETS_COUNT, PinnedWritePage>,
        mut next_ptr: Option<RecordPtr>,
    ) -> Result<(), HeapFileError> {
        while let Some(next) = next_ptr {
            chain.advance(next.page_id)?;
            let page = chain.overflow_page_mut();
            let record_fragment = page.record_fragment(next.slot)?;
            next_ptr = record_fragment.next_fragment;
            page.delete(next.slot)?;
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
        fsm.update_page_bucket_with_duplicate_check(page_id, free_space as _);
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

    /// Generic helper for allocating any type of heap page
    fn allocate_heap_page<H>(
        &self,
        header: H,
    ) -> Result<(PageId, HeapPage<PinnedWritePage, H>), HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let (page, page_id) = self.cache.allocate_page(&self.file_key)?;
        let slotted_page = SlottedPage::initialize_with_header(page, header)?;
        let heap_page = HeapPage::new(slotted_page);
        Ok((page_id, heap_page))
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
        let mut metadata_page_lock = metadata_lock.lock();
        let header = header_fn(*metadata_page_lock);
        let (page_id, page) = self.allocate_heap_page(header)?;
        *metadata_page_lock = page_id;
        self.metadata.dirty.store(true, Ordering::Release);
        Ok((page_id, page))
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
        let fsm_page = fsm.page_with_free_space(data.len())?;
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

        Ok(RecordPtr {
            page_id,
            slot: slot_id,
        })
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
    fn update_page_with_fsm<H>(
        &self,
        page: &mut HeapPage<PinnedWritePage, H>,
        slot: SlotId,
        data: &[u8],
        page_id: PageId,
        fsm: &FreeSpaceMap<BUCKETS_COUNT, H>,
    ) -> Result<(), HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        page.update(slot, data)?;
        fsm.update_page_bucket_with_duplicate_check(page_id, page.page.free_space()? as _);
        Ok(())
    }

    /// Updates a record on a record page and updates FSM
    fn update_record_page_with_fsm(
        &self,
        page: &mut HeapPage<PinnedWritePage, RecordPageHeader>,
        slot: SlotId,
        data: &[u8],
        page_id: PageId,
    ) -> Result<(), HeapFileError> {
        self.update_page_with_fsm(page, slot, data, page_id, &self.record_pages_fsm)
    }

    /// Updates a record on an overflow page and updates FSM
    fn update_overflow_page_with_fsm(
        &self,
        page: &mut HeapPage<PinnedWritePage, OverflowPageHeader>,
        slot: SlotId,
        data: &[u8],
        page_id: PageId,
    ) -> Result<(), HeapFileError> {
        self.update_page_with_fsm(page, slot, data, page_id, &self.overflow_pages_fsm)
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
struct HeapFileFactory<const BUCKETS_COUNT: usize> {
    file_key: FileKey,
    cache: Arc<Cache>,
    columns_metadata: Vec<ColumnMetadata>,
}

impl<const BUCKETS_COUNT: usize> HeapFileFactory<BUCKETS_COUNT> {
    pub(crate) fn new(
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

    pub(crate) fn create_heap_file(self) -> Result<HeapFile<BUCKETS_COUNT>, HeapFileError> {
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
                        self.cache.allocate_page(&key.file_key())?;
                    debug_assert_eq!(
                        metadata_page_id,
                        HeapFile::<BUCKETS_COUNT>::METADATA_PAGE_ID
                    );

                    let (record_page, record_page_id) =
                        self.cache.allocate_page(&key.file_key())?;
                    SlottedPage::<_, RecordPageHeader>::initialize_default(record_page).unwrap();

                    let (overflow_page, overflow_page_id) =
                        self.cache.allocate_page(&key.file_key())?;
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
        fs, thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use metadata::types::Type;
    use tempfile::tempdir;

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

    fn assert_string(expected: &str, actual: &Field) {
        match actual {
            Field::String(s) => {
                assert_eq!(expected, s);
            }
            _ => panic!("expected String, got {:?}", actual),
        }
    }

    fn assert_i32(expected: i32, actual: &Field) {
        match actual {
            Field::Int32(i) => {
                assert_eq!(expected, *i);
            }
            _ => panic!("expected Int32, got {:?}", actual),
        }
    }

    fn assert_i64(expected: i64, actual: &Field) {
        match actual {
            Field::Int64(i) => {
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

        let files_manager = Arc::new(FilesManager::new(temp_dir.path(), "test_db").unwrap());
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
                fragment_data.extend_from_slice(&ptr.slot.to_le_bytes());
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
        let id_field = Field::Int32(id);
        let name_field = Field::String(name.into());

        let record = Record::new(vec![id_field, name_field]);

        record.serialize()
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
    fn fsm_page_with_free_space_does_not_reinsert_different_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(
            cache.clone(),
            file_key.clone(),
            TestPageHeader::NO_NEXT_PAGE,
        );

        // Manually insert a page into bucket 3 that actually belongs to bucket 2
        let free_space_60_percent = PAGE_SIZE * 60 / 100;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space_60_percent);
        fsm.buckets[3].push(page_id);

        // Request space that requires searching bucket 3
        let needed_80_percent = PAGE_SIZE * 80 / 100;
        let result = fsm.page_with_free_space(needed_80_percent).unwrap();
        assert!(result.is_none());

        // Verify page was not re-inserted into bucket 3
        assert!(!bucket_contains_page(&fsm, 3, page_id));

        // Verify page was not automatically moved to bucket 2 either
        assert!(!bucket_contains_page(&fsm, 2, page_id));
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
                slot: 0,
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
        let test_record = Record::new(vec![Field::Int32(42), Field::String("test".into())]);

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
            slot: slot_id,
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
            slot: slot1,
        };
        let ptr2 = RecordPtr {
            page_id: first_record_page_id,
            slot: slot2,
        };
        let ptr3 = RecordPtr {
            page_id: first_record_page_id,
            slot: slot3,
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
            slot: slot1,
        };
        let ptr2 = RecordPtr {
            page_id: second_page_id,
            slot: slot2,
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
            slot: slot_id,
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
                    slot: slot_id,
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
            slot: 0,
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
            slot: 999,
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
            slot: 0,
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
            slot: slot_id,
        };

        let result = heap_file.record(&ptr);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            HeapFileError::CorruptedRecordEntry { .. }
        ));
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
            slot: second_slot,
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
            slot: first_slot,
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
            slot: third_slot,
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
            slot: second_slot,
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
            slot: first_slot,
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
                slot: third_slot,
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
                slot: second_slot,
            }),
        )
        .unwrap();

        // Read the record
        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        let ptr = RecordPtr {
            page_id: first_record_page_id,
            slot: first_slot,
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
            slot: 0,
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
            slot: first_slot,
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
            slot: 999,
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
            slot: first_slot,
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
                    slot: second_slot,
                }),
            )
            .unwrap();

            record_ptrs.push((
                RecordPtr {
                    page_id: first_record_page_id,
                    slot: first_slot,
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
        let id_field = Field::Int32(123);
        let name_field = Field::String("test_user".into());
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record was inserted correctly
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot, 0);

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

        let id_field = Field::Int32(999);
        let name_field = Field::String(large_string.clone());
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record was inserted correctly
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot, 0);

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
        assert!(bucket_contains_page(
            &heap_file.record_pages_fsm,
            0,
            first_record_page_id
        ));
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

        let id_field = Field::Int32(777);
        let name_field = Field::String(large_string.clone());
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record pointer points to the record page (first fragment)
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot, 0);

        // Read back the record to verify it was stored correctly across pages
        let retrieved_record = heap_file.record(&record_ptr).unwrap();
        assert_eq!(retrieved_record.fields.len(), 2);
        assert_i32(777, &retrieved_record.fields[0]);
        assert_string(&large_string, &retrieved_record.fields[1]);

        // Verify both pages were updated in their respective FSMs
        let record_page_ref = FilePageRef::new(first_record_page_id, file_key.clone());
        let record_page = cache.pin_read(&record_page_ref).unwrap();
        let record_slotted_page = SlottedPage::<_, RecordPageHeader>::new(record_page).unwrap();
        let record_free_space = record_slotted_page.free_space().unwrap() as usize;
        let record_bucket = heap_file
            .record_pages_fsm
            .bucket_for_space(record_free_space);

        let overflow_page_ref = FilePageRef::new(first_overflow_page_id, file_key.clone());
        let overflow_page = cache.pin_read(&overflow_page_ref).unwrap();
        let overflow_slotted_page =
            SlottedPage::<_, OverflowPageHeader>::new(overflow_page).unwrap();
        let overflow_free_space = overflow_slotted_page.free_space().unwrap() as usize;
        let overflow_bucket = heap_file
            .overflow_pages_fsm
            .bucket_for_space(overflow_free_space);

        // Both pages should have been added to their respective FSMs
        assert!(bucket_contains_page(
            &heap_file.record_pages_fsm,
            record_bucket,
            first_record_page_id
        ));
        assert!(bucket_contains_page(
            &heap_file.overflow_pages_fsm,
            overflow_bucket,
            first_overflow_page_id
        ));

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

        let id_field = Field::Int32(555);
        let name_field = Field::String(large_string.clone());
        let record = Record::new(vec![id_field, name_field]);

        let record_ptr = heap_file.insert(record).unwrap();

        // Verify the record pointer points to the record page (first fragment)
        assert_eq!(record_ptr.page_id, first_record_page_id);
        assert_eq!(record_ptr.slot, 0);

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

        let large_record = Record::new(vec![Field::Int32(1), Field::String(large_string.clone())]);

        let first_record_ptr = heap_file.insert(large_record).unwrap();
        assert_eq!(first_record_ptr.page_id, first_record_page_id);
        assert_eq!(first_record_ptr.slot, 0);

        // Insert a small record that won't fit in the remaining space
        let small_record = Record::new(vec![
            Field::Int32(2),
            Field::String("this_record_should_trigger_new_page".into()),
        ]);

        let second_record_ptr = heap_file.insert(small_record).unwrap();

        // Verify a new record page was allocated
        assert_ne!(second_record_ptr.page_id, first_record_page_id);
        assert_eq!(second_record_ptr.slot, 0);

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
            Field::Int32(100),
            Field::String("thread_1_record".into()),
        ]);

        let record2 = Record::new(vec![
            Field::Int32(200),
            Field::String("thread_2_record".into()),
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
        let large_record = Record::new(vec![Field::Int32(1), Field::String(large_string)]);
        heap_file.insert(large_record).unwrap();

        // Insert small record to trigger new page allocation
        let small_record = Record::new(vec![Field::Int32(2), Field::String("small".into())]);
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
            Field::Int32(100),
            Field::String("original_name".into()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the Int32 field
        let column_metadata = heap_file.columns_metadata[0].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::Int32(200)).unwrap();

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
            Field::Int32(100),
            Field::Int32(25),
            Field::Int64(1000),
            Field::String(large_string.clone()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update multiple fixed-size fields
        let update_id =
            FieldUpdateDescriptor::new(columns_metadata[0].clone(), Field::Int32(200)).unwrap();
        let update_age =
            FieldUpdateDescriptor::new(columns_metadata[1].clone(), Field::Int32(30)).unwrap();
        let update_score =
            FieldUpdateDescriptor::new(columns_metadata[2].clone(), Field::Int64(2000)).unwrap();

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
            Field::Int32(100),
            Field::String("original_name".into()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with same length string
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String("0RIGIN4L_name".into()))
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
            Field::Int32(100),
            Field::String("original_long_name".into()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with a shorter string
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String("short".into())).unwrap();

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
        let original_record = Record::new(vec![Field::Int32(100), Field::String("short".into())]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the String field with a longer string (but still fits in one page)
        let longer_string =
            "this_is_a_much_longer_string_than_the_original_one_but_still_fits_in_single_page";
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(longer_string.into()))
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
        let original_record = Record::new(vec![Field::Int32(100), Field::String("short".into())]);

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
            FieldUpdateDescriptor::new(column_metadata, Field::String(very_large_string.clone()))
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
        let first_fragment = first_page.record_fragment(record_ptr.slot).unwrap();

        // First fragment should have a continuation
        assert!(first_fragment.next_fragment.is_some());

        let second_ptr = first_fragment.next_fragment.unwrap();
        let second_page = heap_file.read_overflow_page(second_ptr.page_id).unwrap();
        let second_fragment = second_page.record_fragment(second_ptr.slot).unwrap();

        // Second fragment should also have a continuation
        assert!(second_fragment.next_fragment.is_some());

        let third_ptr = second_fragment.next_fragment.unwrap();
        let third_page = heap_file.read_overflow_page(third_ptr.page_id).unwrap();
        let third_fragment = third_page.record_fragment(third_ptr.slot).unwrap();

        // Third fragment should also have a continuation
        assert!(third_fragment.next_fragment.is_some());

        let fourth_ptr = third_fragment.next_fragment.unwrap();
        let fourth_page = heap_file.read_overflow_page(fourth_ptr.page_id).unwrap();
        let fourth_fragment = fourth_page.record_fragment(fourth_ptr.slot).unwrap();

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

        let original_record =
            Record::new(vec![Field::Int32(100), Field::String(large_string.clone())]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all fragment locations before update
        let mut original_fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            original_fragment_locations.push((ptr.page_id, ptr.slot));

            if original_fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert!(original_fragment_locations.len() >= 2);

        // Update the String field with a small string that fits in one page
        let small_string = "tiny";
        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(small_string.into()))
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
        let first_fragment_after = first_page_after.record_fragment(record_ptr.slot).unwrap();

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
            Field::Int32(100),
            Field::String(initial_string.clone()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update with a string that's 2x larger (should span 6 pages)
        let updated_string_size = initial_string_size * 2;
        let updated_string = "y".repeat(updated_string_size);

        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(updated_string.clone()))
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
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
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
            Field::Int32(100),
            Field::String(initial_string.clone()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all fragment locations before update (should be 4 pages)
        let mut original_fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            original_fragment_locations.push((ptr.page_id, ptr.slot));

            if original_fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            }
        }

        assert_eq!(original_fragment_locations.len(), 4);

        // Update with a string that's half the size (should span 2 pages)
        let updated_string_size = initial_string_size / 2;
        let updated_string = "y".repeat(updated_string_size);

        let column_metadata = heap_file.columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(updated_string.clone()))
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
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
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
            Field::String("John".into()),
            Field::String("Middle".into()),
            Field::String("Doe".into()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the middle string to be very large (more than one page)
        let max_single_page_size = SlottedPage::<(), RecordPageHeader>::MAX_FREE_SPACE as usize
            - size_of::<Slot>()
            - size_of::<RecordTag>();

        let large_middle_name = "M".repeat(max_single_page_size + 500);

        let column_metadata = columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(large_middle_name.clone()))
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
            Field::String("John".into()),
            Field::String(large_middle_name.clone()),
            Field::String("Doe".into()),
        ]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Update the middle string to be very small
        let small_middle_name = "M";

        let column_metadata = columns_metadata[1].clone();
        let update_descriptor =
            FieldUpdateDescriptor::new(column_metadata, Field::String(small_middle_name.into()))
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
        let original_strings = vec![
            "field1_initial",
            "field2_initial",
            "field3_initial",
            "field4_initial",
            "field5_initial",
        ];

        let original_record = Record::new(
            original_strings
                .iter()
                .map(|s| Field::String(s.to_string()))
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
                FieldUpdateDescriptor::new(col.clone(), Field::String(s.clone())).unwrap()
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
            Field::String(initial_field1.clone()),
            Field::String(initial_field2.clone()),
            Field::String(initial_field3.clone()),
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
            let ptr = record_ptr.clone();
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
                    let is_abc_pattern = matches!(&record.fields[0], Field::String(s) if s == &exp_f1_abc)
                        && matches!(&record.fields[1], Field::String(s) if s == &exp_f2_abc)
                        && matches!(&record.fields[2], Field::String(s) if s == &exp_f3_abc);

                    // Try to match XYZ pattern
                    let is_xyz_pattern = matches!(&record.fields[0], Field::String(s) if s == &exp_f1_xyz)
                        && matches!(&record.fields[1], Field::String(s) if s == &exp_f2_xyz)
                        && matches!(&record.fields[2], Field::String(s) if s == &exp_f3_xyz);

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
            let ptr = record_ptr.clone();
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
                        FieldUpdateDescriptor::new(cols[0].clone(), Field::String(updated_field1))
                            .unwrap(),
                        FieldUpdateDescriptor::new(cols[1].clone(), Field::String(updated_field2))
                            .unwrap(),
                        FieldUpdateDescriptor::new(cols[2].clone(), Field::String(updated_field3))
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

        let is_abc = matches!(&final_record.fields[0], Field::String(s) if s == &expected_field1_abc)
            && matches!(&final_record.fields[1], Field::String(s) if s == &expected_field2_abc)
            && matches!(&final_record.fields[2], Field::String(s) if s == &expected_field3_abc);

        let is_xyz = matches!(&final_record.fields[0], Field::String(s) if s == &expected_field1_xyz)
            && matches!(&final_record.fields[1], Field::String(s) if s == &expected_field2_xyz)
            && matches!(&final_record.fields[2], Field::String(s) if s == &expected_field3_xyz);

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
        let original_record =
            Record::new(vec![Field::Int32(100), Field::String("test_record".into())]);

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

        let original_record = Record::new(vec![Field::Int32(42), Field::String(large_string)]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        // Collect all page IDs and slots that contain fragments before deletion
        let mut fragment_locations = vec![];
        let mut current_ptr = Some(record_ptr);

        while let Some(ptr) = current_ptr {
            fragment_locations.push((ptr.page_id, ptr.slot));

            if fragment_locations.len() == 1 {
                let page = heap_file.read_record_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
                current_ptr = fragment.next_fragment;
            } else {
                let page = heap_file.read_overflow_page(ptr.page_id).unwrap();
                let fragment = page.record_fragment(ptr.slot).unwrap();
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

        let original_record =
            Record::new(vec![Field::Int32(42), Field::String(large_string.clone())]);

        let record_ptr = heap_file.insert(original_record).unwrap();

        let num_reader_threads = 8;
        let mut reader_handles = vec![];

        // Spawn reader threads
        for _ in 0..num_reader_threads {
            let heap_file_clone = heap_file.clone();
            let ptr = record_ptr.clone();
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
}
