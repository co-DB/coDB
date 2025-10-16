use std::{array, marker::PhantomData, sync::Arc};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use dashmap::DashSet;
use metadata::catalog::ColumnMetadata;
use parking_lot::Mutex;
use thiserror::Error;

use crate::{
    cache::{Cache, CacheError, FilePageRef, PageRead, PinnedReadPage, PinnedWritePage},
    data_types::DbSerializable,
    files_manager::FileKey,
    paged_file::{PAGE_SIZE, Page, PageId},
    record::{Record, RecordError},
    slotted_page::{
        PageType, ReprC, SlotId, SlottedPage, SlottedPageBaseHeader, SlottedPageError,
        SlottedPageHeader,
    },
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
        if *page_id == NO_NEXT_PAGE {
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

impl Metadata {
    fn load_from_page(page: &Page) -> Result<Self, HeapFileError> {
        let metadata_size = size_of::<Metadata>();
        let metadata = bytemuck::try_from_bytes(&page[..metadata_size]).map_err(|err| {
            HeapFileError::InvalidMetadataPage {
                error: err.to_string(),
            }
        })?;
        Ok(*metadata)
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
            next_page: NO_NEXT_PAGE,
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
            next_page: NO_NEXT_PAGE,
        }
    }
}

unsafe impl ReprC for OverflowPageHeader {}

impl SlottedPageHeader for OverflowPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

/// Const representing "null" page.
const NO_NEXT_PAGE: PageId = 0;

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
    RecordSerializationError(#[from] RecordError),
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
    const METADATA_PAGE_ID: PageId = 1;

    /// Reads [`Record`] located at `ptr` and deserializes it.
    pub(crate) fn record(&self, ptr: &RecordPtr) -> Result<Record, HeapFileError> {
        let first_page = self.read_record_page(ptr.page_id)?;
        let fragment = first_page.record_fragment(ptr.slot)?;

        // Whole record is on single page
        if fragment.next_fragment.is_none() {
            let record = Record::deserialize(&self.columns_metadata, fragment.data)?;
            return Ok(record);
        }

        // Record is saved across many pages - we need to load it fragment by fragment
        let mut full_record_bytes = Vec::with_capacity(fragment.data.len());
        full_record_bytes.extend_from_slice(fragment.data);

        let mut next_ptr = fragment.next_fragment;
        while let Some(next) = next_ptr {
            let next_fragment_page = self.read_overflow_page(next.page_id)?;
            let fragment = next_fragment_page.record_fragment(next.slot)?;
            full_record_bytes.extend_from_slice(fragment.data);
            next_ptr = fragment.next_fragment;
        }

        let record = Record::deserialize(&self.columns_metadata, &full_record_bytes)?;
        Ok(record)
    }

    /// Helper for reading [`HeapFile`]'s pages.
    fn read_page<H>(&self, page_id: PageId) -> Result<HeapPage<PinnedReadPage, H>, HeapFileError>
    where
        H: BaseHeapPageHeader,
    {
        let file_page_ref = self.file_page_ref(page_id);
        let page = self.cache.pin_read(&file_page_ref)?;
        let slotted_page = SlottedPage::new(page)?;
        let heap_node = HeapPage::new(slotted_page);
        Ok(heap_node)
    }

    fn read_record_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, RecordPageHeader>, HeapFileError> {
        let p = self.read_page::<RecordPageHeader>(page_id)?;
        let free_space = p.page.free_space()?;
        self.record_pages_fsm
            .update_page_bucket_with_duplicate_check(page_id, free_space as _);
        Ok(p)
    }

    fn read_overflow_page(
        &self,
        page_id: PageId,
    ) -> Result<HeapPage<PinnedReadPage, OverflowPageHeader>, HeapFileError> {
        let p = self.read_page::<OverflowPageHeader>(page_id)?;
        let free_space = p.page.free_space()?;
        self.overflow_pages_fsm
            .update_page_bucket_with_duplicate_check(page_id, free_space as _);
        Ok(p)
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
        self.load_existing_heap_file()
    }

    fn load_existing_heap_file(self) -> Result<HeapFile<BUCKETS_COUNT>, HeapFileError> {
        let metadata = self.load_metadata()?;

        let record_pages_fsm = FreeSpaceMap::<BUCKETS_COUNT, RecordPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
            metadata.first_record_page,
        );
        let overflow_pages_fsm = FreeSpaceMap::<BUCKETS_COUNT, OverflowPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
            metadata.first_overflow_page,
        );

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

    fn load_metadata(&self) -> Result<Metadata, HeapFileError> {
        let key = self.file_page_ref(HeapFile::<BUCKETS_COUNT>::METADATA_PAGE_ID);
        let page = self.cache.pin_read(&key)?;
        let metadata = Metadata::load_from_page(page.page())?;
        Ok(metadata)
    }

    /// Creates a `FilePageRef` for `page_id` using heap file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef {
            page_id,
            file_key: self.file_key.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs, thread,
        time::{SystemTime, UNIX_EPOCH},
    };

    use metadata::types::Type;
    use tempfile::tempdir;

    use crate::{
        files_manager::FilesManager,
        record::Field,
        slotted_page::{InsertResult, PageType},
    };

    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Pod, Zeroable)]
    struct TestPageHeader {
        base: SlottedPageBaseHeader,
    }

    impl BaseHeapPageHeader for TestPageHeader {
        fn next_page(&self) -> PageId {
            NO_NEXT_PAGE
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
        let mut slotted = SlottedPage::<_, TestPageHeader>::initialize_default(pinned_page, false);

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
        SlottedPage::<_, RecordPageHeader>::initialize_default(record_pinned, false);

        SlottedPage::<_, OverflowPageHeader>::initialize_default(overflow_pinned, false);

        // Write metadata
        let metadata = Metadata {
            first_record_page: first_record_page_id,
            first_overflow_page: first_overflow_page_id,
        };

        let metadata_bytes = bytemuck::bytes_of(&metadata);
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
        let file_page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };
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
        let fsm = create_test_fsm::<4>(cache, file_key, NO_NEXT_PAGE);

        // Empty page (100% free) should go to last bucket
        let bucket = fsm.bucket_for_space(PAGE_SIZE);
        assert_eq!(bucket, 3);
    }

    #[test]
    fn fsm_bucket_for_space_full_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, NO_NEXT_PAGE);

        // Full page (0% free) should go to first bucket
        let bucket = fsm.bucket_for_space(0);
        assert_eq!(bucket, 0);
    }

    #[test]
    fn fsm_bucket_for_space_quarter_ranges() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key, NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache, file_key, NO_NEXT_PAGE);

        // Should return None when FSM is empty
        let result = fsm.page_with_free_space(100).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_page_with_free_space_finds_suitable_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

        // Create page with initial free space
        let initial_free = 2000;
        let page_id = create_page_with_free_space(&cache, &file_key, initial_free);
        fsm.update_page_bucket(page_id, initial_free);

        // Modify the page to reduce free space
        {
            let file_page_ref = FilePageRef {
                page_id,
                file_key: file_key.clone(),
            };
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
            NO_NEXT_PAGE,
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
                    SlottedPage::<_, TestPageHeader>::initialize_default(pinned_page, false);
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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone(), NO_NEXT_PAGE);

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
        let page = SlottedPage::<_, RecordPageHeader>::initialize_default(first_page, false);
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
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page, false);
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);
        second_slotted.get_header_mut().unwrap().next_page = third_page_id;

        let mut third_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(third_page, false);
        third_slotted.get_header_mut().unwrap().next_page = NO_NEXT_PAGE;

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
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page, false);
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);
        second_slotted.get_header_mut().unwrap().next_page = NO_NEXT_PAGE;

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
            SlottedPage::<_, RecordPageHeader>::initialize_default(first_page, false);
        first_slotted.get_header_mut().unwrap().next_page = second_page_id;

        let mut second_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);
        second_slotted.get_header_mut().unwrap().next_page = third_page_id;

        let mut third_slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(third_page, false);
        third_slotted.get_header_mut().unwrap().next_page = NO_NEXT_PAGE;

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
        assert_ne!(heap_file.first_record_page(), 0);
        assert_ne!(heap_file.first_overflow_page(), 0);
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
                page_id: heap_file.first_record_page(),
                slot: 0,
            };
            let read_result = heap_file.record(&invalid_ptr);
            assert!(read_result.is_err());
        }
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
        SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);

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
            let file_page_ref = FilePageRef {
                page_id: first_record_page_id,
                file_key: file_key.clone(),
            };
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
        SlottedPage::<_, OverflowPageHeader>::initialize_default(second_overflow_page, false);

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
        SlottedPage::<_, OverflowPageHeader>::initialize_default(second_overflow_page, false);

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
}
