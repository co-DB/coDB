use std::{marker::PhantomData, sync::Arc};

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use metadata::catalog::ColumnMetadata;
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
    fn new(cache: Arc<Cache>, file_key: FileKey) -> Self {
        let buckets: [SegQueue<PageId>; BUCKETS_COUNT] = std::array::from_fn(|_| SegQueue::new());

        FreeSpaceMap {
            buckets,
            cache,
            file_key,
            _page_type_marker: PhantomData::<H>,
        }
    }

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

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RecordPageHeader {
    base: SlottedPageBaseHeader,
    _padding: u16,
    next_page: PageId,
}

impl RecordPageHeader {
    const NO_NEXT_PAGE: PageId = 0;
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
    const NO_NEXT_PAGE: PageId = 0;
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

        todo!("record with overflow pages not supported yet");
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

        let record_pages_fsm = self.load_record_fsm(metadata.first_record_page)?;
        let overflow_pages_fsm = self.load_overflow_fsm(metadata.first_overflow_page)?;

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

    fn load_record_fsm(
        &self,
        first_page: PageId,
    ) -> Result<FreeSpaceMap<BUCKETS_COUNT, RecordPageHeader>, HeapFileError> {
        let fsm = FreeSpaceMap::<BUCKETS_COUNT, RecordPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
        );
        let mut current_page_id = first_page;
        while current_page_id != RecordPageHeader::NO_NEXT_PAGE {
            let key = self.file_page_ref(current_page_id);
            let page = self.cache.pin_read(&key)?;
            let slotted_page = SlottedPage::<_, RecordPageHeader>::new(page, false)?;
            let free_space = slotted_page.free_space()?;
            fsm.update_page_bucket(current_page_id, free_space as _);
            current_page_id = slotted_page.get_header()?.next_page;
        }
        Ok(fsm)
    }

    fn load_overflow_fsm(
        &self,
        first_page: PageId,
    ) -> Result<FreeSpaceMap<BUCKETS_COUNT, OverflowPageHeader>, HeapFileError> {
        let fsm = FreeSpaceMap::<BUCKETS_COUNT, OverflowPageHeader>::new(
            self.cache.clone(),
            self.file_key.clone(),
        );
        let mut current_page_id = first_page;
        while current_page_id != OverflowPageHeader::NO_NEXT_PAGE {
            let key = self.file_page_ref(current_page_id);
            let page = self.cache.pin_read(&key)?;
            let slotted_page = SlottedPage::<_, OverflowPageHeader>::new(page, false)?;
            let free_space = slotted_page.free_space()?;
            fsm.update_page_bucket(current_page_id, free_space as _);
            current_page_id = slotted_page.get_header()?.next_page;
        }
        Ok(fsm)
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
        let file_key = FileKey::data(&format!(
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
    ) -> FreeSpaceMap<BUCKETS, TestPageHeader> {
        FreeSpaceMap::new(cache, file_key)
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

    fn bucket_contains_page<const BUCKETS: usize>(
        fsm: &FreeSpaceMap<BUCKETS, TestPageHeader>,
        bucket_id: usize,
        page_id: PageId,
    ) -> bool {
        let mut found = false;
        let mut pages = Vec::new();

        // We need to do it like that because SeqQueue does not have method `contains`.
        while let Some(id) = fsm.buckets[bucket_id].pop() {
            if id == page_id {
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
        let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned, false)?;

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
        let fsm = create_test_fsm::<4>(cache, file_key);

        // Empty page (100% free) should go to last bucket
        let bucket = fsm.bucket_for_space(PAGE_SIZE);
        assert_eq!(bucket, 3);
    }

    #[test]
    fn fsm_bucket_for_space_full_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key);

        // Full page (0% free) should go to first bucket
        let bucket = fsm.bucket_for_space(0);
        assert_eq!(bucket, 0);
    }

    #[test]
    fn fsm_bucket_for_space_quarter_ranges() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache, file_key);

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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
        let fsm = create_test_fsm::<4>(cache, file_key);

        // Should return None when FSM is empty
        let result = fsm.page_with_free_space(100).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_page_with_free_space_finds_suitable_page() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

        // Create a page with 2000 bytes free
        let free_space = 2000;
        let page_id = create_page_with_free_space(&cache, &file_key, free_space);
        fsm.update_page_bucket(page_id, free_space);

        // Request page with 1500 bytes free - should find just created page
        let result = fsm.page_with_free_space(1500).unwrap();
        assert!(result.is_some());

        let found_page = result.unwrap();
        let actual_free = found_page.free_space().unwrap() as usize;
        assert!(actual_free > 1500);
    }

    #[test]
    fn fsm_page_with_free_space_skips_insufficient_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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

        let found_page = result.unwrap();
        let actual_free = found_page.free_space().unwrap() as usize;
        assert!(actual_free > 1500);
    }

    #[test]
    fn fsm_page_with_free_space_searches_higher_buckets() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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
        let fsm = create_test_fsm::<4>(cache.clone(), file_key.clone());

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
            let mut slotted = SlottedPage::<_, TestPageHeader>::new(pinned, false).unwrap();
            let dummy_data = vec![0u8; 1500];
            slotted.insert(&dummy_data).unwrap();
        }

        let result = fsm.page_with_free_space(1800).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn fsm_concurrent_updates_same_bucket() {
        let (cache, _, file_key) = setup_test_cache();
        let fsm = Arc::new(create_test_fsm::<4>(cache.clone(), file_key.clone()));

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
    fn heap_file_factory_loads_record_pages_fsm() {
        let (cache, _, file_key) = setup_test_cache();
        setup_heap_file_structure(&cache, &file_key);

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Verify FSMs have the pages
        let result = heap_file
            .record_pages_fsm
            .page_with_free_space(100)
            .unwrap();
        assert!(result.is_some());

        let result = heap_file
            .overflow_pages_fsm
            .page_with_free_space(100)
            .unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn heap_file_factory_loads_multiple_record_pages() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create a second record page linked to the first
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();
        SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);

        // Link first page to second page
        {
            let file_page_ref = FilePageRef {
                page_id: first_record_page_id,
                file_key: file_key.clone(),
            };
            let pinned = cache.pin_write(&file_page_ref).unwrap();
            let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned, false).unwrap();
            let header = slotted.get_header_mut().unwrap();
            header.next_page = second_page_id;
        }

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let heap_file = factory.create_heap_file().unwrap();

        // Both pages should be in FSM
        let result1 = heap_file
            .record_pages_fsm
            .page_with_free_space(100)
            .unwrap();
        assert!(result1.is_some());
        let result2 = heap_file
            .record_pages_fsm
            .page_with_free_space(100)
            .unwrap();
        assert!(result2.is_some());
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

    #[test]
    fn heap_file_factory_loads_corrupted_record_page_list() {
        let (cache, _, file_key) = setup_test_cache();
        let (first_record_page_id, _) = setup_heap_file_structure(&cache, &file_key);

        // Create a second record page with invalid next_page pointer
        let (second_page, second_page_id) = cache.allocate_page(&file_key).unwrap();
        let mut slotted =
            SlottedPage::<_, RecordPageHeader>::initialize_default(second_page, false);
        let header = slotted.get_header_mut().unwrap();
        header.next_page = 9999; // Invalid page id
        drop(slotted);

        // Link first page to second page
        {
            let file_page_ref = FilePageRef {
                page_id: first_record_page_id,
                file_key: file_key.clone(),
            };
            let pinned = cache.pin_write(&file_page_ref).unwrap();
            let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned, false).unwrap();
            let header = slotted.get_header_mut().unwrap();
            header.next_page = second_page_id;
        }

        let factory = create_test_heap_file_factory(cache.clone(), file_key.clone());
        let result = factory.create_heap_file();

        // Should fail when trying to load invalid page
        assert!(result.is_err());
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
            let mut slotted = SlottedPage::<_, RecordPageHeader>::new(pinned, false).unwrap();
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
}
