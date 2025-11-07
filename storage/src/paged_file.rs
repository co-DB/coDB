//! PagedFile module — abstraction layer for managing on-disk paged-files and page operations.

use std::{
    collections::HashSet,
    fs,
    io::{self, ErrorKind, Read, Seek, Write},
    path::Path,
};

use bytemuck::{Pod, Zeroable};
use thiserror::Error;

/// Type representing page id, should be used instead of bare `u32`.
pub type PageId = u32;

/// Size of each page in [`PagedFile`].
pub const PAGE_SIZE: usize = 4096; // 4 kB

/// Type representing page, should be used instead of bare array of bytes.
pub type Page = [u8; PAGE_SIZE];

/// Responsible for managing a single on-disk file.
/// Only this structure should be responsible for directly communicating with disk.
///
/// File managed by [`PagedFile`] is divided into fixed-size pages.
///
/// Page 0 (first page) is a special page that should be used by [`PagedFile`] for storing metadata ([`FileMetadata`]). It means that each file will be at least one page long, even when they have no other content, but this is a trade-off for better pages alignment. For more details about structure of the first page look at [`FileMetadata`].
/// Pages from 1 to N have no defined format from [PagedFile]'s perspective - its sole responsibility is to allow reading, writing and allocating pages.
pub(crate) struct PagedFile {
    /// Handle to underlying file
    handle: fs::File,
    /// File's metadata
    metadata: FileMetadata,
    /// In-memory collection of free pages.
    ///
    /// We use it to be able to check if page is free in O(1).
    /// On disk free pages are stored as linked-list, where each free page stores its node.
    free_pages: HashSet<PageId>,
}

/// Error for [`PagedFile`] related operations.
#[derive(Error, Debug)]
pub enum PagedFileError {
    /// Provided page id was invalid, e.g. tried to read [`METADATA_PAGE_ID`]
    #[error("invalid page id: {0}")]
    InvalidPageId(PageId),
    /// File used for loading [`PagedFile`] has invalid format
    #[error("file has invalid format: {0}")]
    InvalidFileFormat(String),
    /// Underlying IO module returned error
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
}

impl PagedFile {
    /// Metadata page id - page with this id should only be used internally by [`PagedFile`].
    const METADATA_PAGE_ID: PageId = 0;

    /// Creates a new instance of [`PagedFile`]. When `file_path` points to existing file it
    /// tries to load it from there, otherwise it creates new file at `file_path`.
    pub(crate) fn new<P>(file_path: P) -> Result<PagedFile, PagedFileError>
    where
        P: AsRef<Path>,
    {
        // It returns error if existence of the file at `file_path` cannot be checked, e.g.
        // if we don't have permission to read that file. I don't think there is anything we can do about it,
        // so just return underlying error in that case.
        let exists = file_path.as_ref().try_exists()?;
        match exists {
            true => {
                let file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&file_path)?;
                PagedFile::try_from(file)
            }
            false => {
                if let Some(parent) = file_path.as_ref().parent() {
                    fs::create_dir_all(parent)?;
                }

                let file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&file_path)?;
                let metadata = FileMetadata::default();
                let mut pf = PagedFile {
                    handle: file,
                    metadata,
                    free_pages: HashSet::new(),
                };
                pf.update_size()?;
                pf.sync_metadata()?;
                pf.initialize_free_pages()?;
                Ok(pf)
            }
        }
    }

    /// Reads page with id equal to `page_id` from underlying file. Can fail if io error occurs or `page_id` is not valid.
    pub(crate) fn read_page(&mut self, page_id: PageId) -> Result<Page, PagedFileError> {
        if self.is_invalid_page_id(page_id) {
            return Err(PagedFileError::InvalidPageId(page_id));
        }

        self.seek_page(page_id)?;
        let mut buffer = [0u8; PAGE_SIZE];
        self.handle.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes new `page` to page with id `page_id`. It does not flush newly written page to disk. For this check [`PagedFile::flush`].
    /// Page with id `page_id` must be allocated before writing to it. Can fail if io error occurs or `page_id` is not valid.
    pub(crate) fn write_page(&mut self, page_id: PageId, page: Page) -> Result<(), PagedFileError> {
        if self.is_invalid_page_id(page_id) {
            return Err(PagedFileError::InvalidPageId(page_id));
        }

        self.seek_page(page_id)?;
        self.handle.write_all(&page)?;

        Ok(())
    }

    /// Allocates new page and returns its `PageId`. If there is a free page in metadata free page list then the first page
    /// from that list is used. Returned page id is guaranteed to point to page that is not used.
    /// Can fail if io error occurs.
    pub(crate) fn allocate_page(&mut self) -> Result<PageId, PagedFileError> {
        if self.free_pages.is_empty() {
            let page_id = self.metadata.next_page_id;
            self.metadata.next_page_id += 1;
            self.update_size()?;
            self.sync_metadata()?;
            return Ok(page_id);
        }
        let page_id = self.consume_free_page()?;
        self.sync_metadata()?;
        Ok(page_id)
    }

    /// Frees page with `page_id` so that it can be reused later.
    /// `page_id` is added both to disk-level linked list of [`FreePage`]s and in-memory [`PagedFile::free_pages`].
    pub(crate) fn free_page(&mut self, page_id: PageId) -> Result<(), PagedFileError> {
        if self.is_invalid_page_id(page_id) {
            return Err(PagedFileError::InvalidPageId(page_id));
        }
        let previous = FreePage::NO_FREE_PAGE;
        let next = self.metadata.first_free_page_id;
        let free_page = FreePage { next, previous };

        let page = Page::try_from(&free_page)?;
        self.write_page(page_id, page)?;

        if next != FreePage::NO_FREE_PAGE {
            let update_args = UpdateFreePageArgs {
                next: None,
                previous: Some(page_id),
            };
            self.update_free_page(next, &update_args)?;
        }

        self.metadata.first_free_page_id = page_id;
        self.sync_metadata()?;

        self.free_pages.insert(page_id);

        Ok(())
    }

    /// Truncates the file - remove unused allocated pages from the end of the file. Can fail if io error occurs.
    pub(crate) fn truncate(&mut self) -> Result<(), PagedFileError> {
        while self.free_pages.contains(&(self.metadata.next_page_id - 1)) {
            self.metadata.next_page_id -= 1;
            let free_page_to_be_removed = self.metadata.next_page_id;
            self.remove_free_page(free_page_to_be_removed)?;
        }
        self.update_size()?;
        self.sync_metadata()?;
        Ok(())
    }

    /// Flushes file content to disk ensuring it's synced with in-memory state. Can fail if io error occurs.
    pub(crate) fn flush(&mut self) -> Result<(), PagedFileError> {
        self.handle.sync_all()?;
        Ok(())
    }

    /// Seeks underlying file handle to the start of the page with `page_id`.
    fn seek_page(&mut self, page_id: PageId) -> Result<(), PagedFileError> {
        // u64 is used to make sure that this does not overflow
        let start = PAGE_SIZE as u64 * page_id as u64;
        self.handle.seek(io::SeekFrom::Start(start))?;
        Ok(())
    }

    /// Helper to check if page with `page_id` can be read from/write to.
    fn is_invalid_page_id(&self, page_id: PageId) -> bool {
        let is_metadata = page_id == Self::METADATA_PAGE_ID;
        let is_free = self.free_pages.contains(&page_id);
        let is_unallocated = page_id >= self.metadata.next_page_id;
        is_metadata || is_free || is_unallocated
    }

    /// Syncs in-memory metadata with data stored in [`PagedFile::METADATA_PAGE_ID`] page.
    fn sync_metadata(&mut self) -> Result<(), PagedFileError> {
        let metadata_page = Page::try_from(&self.metadata)?;
        self.seek_page(Self::METADATA_PAGE_ID)?;
        self.handle.write_all(&metadata_page)?;
        Ok(())
    }

    /// Updates size of underlying file to hold `next_page_id * PAGE_SIZE` bytes
    fn update_size(&mut self) -> Result<(), PagedFileError> {
        // u64 is used to make sure that this does not overflow
        let new_size = self.metadata.next_page_id as u64 * PAGE_SIZE as u64;
        self.handle.set_len(new_size)?;
        Ok(())
    }

    /// Iterate over pages from 1 (inclusively) to [`FileMetadata::DEFAULT_NEXT_PAGE_ID`] (exclusively) and for each of them
    /// initializes it to be free page.
    /// Should only be used once, when [`PagedFile`] is created.
    ///
    /// Note that we can use [`PagedFile::write_page`] here, because at this point [`PagedFile::free_pages] is empty.
    fn initialize_free_pages(&mut self) -> Result<(), PagedFileError> {
        if FileMetadata::DEFAULT_NEXT_PAGE_ID == 1 {
            return Ok(());
        }

        for page_id in 1..FileMetadata::DEFAULT_NEXT_PAGE_ID {
            let next = if page_id == FileMetadata::DEFAULT_NEXT_PAGE_ID - 1 {
                FreePage::NO_FREE_PAGE
            } else {
                page_id + 1
            };

            let previous = if page_id == 1 {
                FreePage::NO_FREE_PAGE
            } else {
                page_id - 1
            };

            let free = FreePage { next, previous };
            let page = Page::try_from(&free)?;
            self.write_page(page_id, page)?;

            self.free_pages.insert(page_id);
        }

        // We know there is at least one free page, so we must update the metadata
        self.metadata.first_free_page_id = 1;
        self.sync_metadata()?;

        Ok(())
    }

    /// Gets the first free page pointed by [`FileMetadata`] and removes it from the chain of free pages, making it available for usage.
    ///
    /// Caller must ensure that at least one [`FreePage`] exists.
    fn consume_free_page(&mut self) -> Result<PageId, PagedFileError> {
        let page_id = self.metadata.first_free_page_id;
        assert_ne!(
            page_id,
            FreePage::NO_FREE_PAGE,
            "at least one free page must exists"
        );

        if !self.free_pages.remove(&page_id) {
            panic!("free page ('{}') not found in in-memory set", page_id);
        }

        self.seek_page(page_id)?;
        let mut buffer = [0u8; PAGE_SIZE];
        self.handle.read_exact(&mut buffer)?;

        let free_page = FreePage::try_from(&buffer)?;

        let new_head_id = free_page.next;
        self.metadata.first_free_page_id = new_head_id;

        // There was only one free page - no need to update the next element
        if new_head_id == FreePage::NO_FREE_PAGE {
            return Ok(page_id);
        }

        let update_args = UpdateFreePageArgs {
            next: None,
            previous: Some(FreePage::NO_FREE_PAGE),
        };
        self.update_free_page(new_head_id, &update_args)?;

        Ok(page_id)
    }

    /// Reads [`FreePage`] from page.
    ///
    /// Caller must ensure that `free_page_id` actually points to page with [`FreePage`] struct inside, as this is not validated in this function.
    fn read_free_page(&mut self, free_page_id: PageId) -> Result<FreePage, PagedFileError> {
        self.seek_page(free_page_id)?;
        let mut buffer = [0u8; PAGE_SIZE];
        self.handle.read_exact(&mut buffer)?;
        FreePage::try_from(&buffer)
    }

    /// Loads free page from disk, updates it and writes updated version back to disk.
    fn update_free_page(
        &mut self,
        free_page_id: PageId,
        args: &UpdateFreePageArgs,
    ) -> Result<(), PagedFileError> {
        let mut free_page = self.read_free_page(free_page_id)?;
        free_page.update(args);
        let updated_page = Page::try_from(&free_page)?;
        self.seek_page(free_page_id)?;
        self.handle.write_all(&updated_page)?;
        Ok(())
    }

    /// Removes free page with `free_page_id` from both disk-level linked list and in-memory [`PagedFile::free_pages`].
    fn remove_free_page(&mut self, free_page_id: PageId) -> Result<(), PagedFileError> {
        self.free_pages.remove(&free_page_id);

        let free_page = self.read_free_page(free_page_id)?;

        // This is the only free page in the whole file
        if free_page.next == FreePage::NO_FREE_PAGE
            && self.metadata.first_free_page_id == free_page_id
        {
            self.metadata.first_free_page_id = FreePage::NO_FREE_PAGE;
            return Ok(());
        }

        // This is the last free page
        if free_page.next == FreePage::NO_FREE_PAGE {
            let update_args = UpdateFreePageArgs {
                next: Some(FreePage::NO_FREE_PAGE),
                previous: None,
            };
            self.update_free_page(free_page.previous, &update_args)?;
            return Ok(());
        }

        // This is the first free page
        if free_page_id == self.metadata.first_free_page_id {
            let update_args = UpdateFreePageArgs {
                next: None,
                previous: Some(FreePage::NO_FREE_PAGE),
            };
            self.update_free_page(free_page.next, &update_args)?;
            self.metadata.first_free_page_id = free_page.next;
            return Ok(());
        }

        // This is the free page in the middle
        let update_previous_args = UpdateFreePageArgs {
            next: Some(free_page.next),
            previous: None,
        };
        self.update_free_page(free_page.previous, &update_previous_args)?;

        let update_next_args = UpdateFreePageArgs {
            next: None,
            previous: Some(free_page.previous),
        };
        self.update_free_page(free_page.next, &update_next_args)?;

        Ok(())
    }
}

/// Try to deserialize existing [`PagedFile`] from file. Can fail if io error occurs or if underlying file
/// has invalid format.
impl TryFrom<fs::File> for PagedFile {
    type Error = PagedFileError;

    fn try_from(mut value: fs::File) -> Result<Self, Self::Error> {
        let mut page_buffer = [0u8; PAGE_SIZE];
        if let Err(e) = value.read_exact(&mut page_buffer) {
            return match e.kind() {
                ErrorKind::UnexpectedEof => Err(PagedFileError::InvalidFileFormat(
                    "file shorter than one page".into(),
                )),
                _ => Err(PagedFileError::IoError(e)),
            };
        }
        let file_metadata = FileMetadata::try_from(page_buffer)?;

        let mut free_pages = HashSet::new();

        let mut current_free_page = file_metadata.first_free_page_id;
        while current_free_page != FreePage::NO_FREE_PAGE {
            value.seek(io::SeekFrom::Start(
                current_free_page as u64 * PAGE_SIZE as u64,
            ))?;
            value.read_exact(&mut page_buffer)?;
            let free_page = FreePage::try_from(&page_buffer)?;
            free_pages.insert(current_free_page);
            current_free_page = free_page.next;
        }

        Ok(PagedFile {
            handle: value,
            metadata: file_metadata,
            free_pages: free_pages,
        })
    }
}

/// Make sure that all in-memory changes have been flushed to disk before dropping [`PagedFile`].
impl Drop for PagedFile {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            log::error!("failed to flush file content while dropping PagedFile: {e}");
        }
    }
}

/// Storage for file metadata.
///
/// [`FileMetadata`] is always stored in first page of the file. It contains metadata information used by [`PagedFile`] -
/// no other struct should directly use it (it should not be exported outside this module).
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct FileMetadata {
    magic_number: [u8; 4],
    /// Id of first free page.
    ///
    /// Free pages are pages that are already allocated on disk, but are not used.
    /// Such pages cannot be used directly (by calling read/write page), but must be first allocated via [`PagedFile::allocate_page`].
    first_free_page_id: PageId,
    /// Id of next page to be allocated.
    ///
    /// This is used if `first_free_page_id` points to [`FileMetadata::NO_FREE_PAGE`].
    next_page_id: PageId,
}

impl FileMetadata {
    /// Magic number - used for checking if file is (has high chances to be) codb file.
    const CODB_MAGIC_NUMBER: [u8; 4] = [0xC, 0x0, 0xD, 0xB];

    /// Default value used when creating new page.
    ///
    /// Every new page will be created with `DEFAULT_NEXT_PAGE_ID - 2` free pages ready for usage.
    ///
    /// `-1` for metadata page and `-1` because `DEFAULT_NEXT_PAGE_ID` is the id of the next page (not allocated one).
    // TODO: after some tests we can adjust this value
    // FYI: I changed this for tests in heap file. When it was 4 we started the paged file with hash set
    // containing 1,2 and 3 and it would allocate random page. We always want to allocate page with id 1 first (it will work this way once we replace hash set
    // with linked list, its just temporary fix)
    const DEFAULT_NEXT_PAGE_ID: PageId = 2;
}

/// Should be used to deserialize [`FileMetadata`] from [`Page`].
impl TryFrom<Page> for FileMetadata {
    type Error = PagedFileError;

    fn try_from(value: Page) -> Result<Self, Self::Error> {
        let size = size_of::<FileMetadata>();
        let metadata_bytes = &value[..size];
        let metadata: &FileMetadata = bytemuck::try_from_bytes(metadata_bytes)
            .map_err(|e| PagedFileError::InvalidFileFormat(e.to_string()))?;
        if metadata.magic_number != Self::CODB_MAGIC_NUMBER {
            return Err(PagedFileError::InvalidFileFormat(format!(
                "invalid magic number ('{:?}')",
                metadata.magic_number
            )));
        }
        Ok(*metadata)
    }
}

/// Should be used to serialize [`FileMetadata`] into [`Page`]
impl TryFrom<&FileMetadata> for Page {
    type Error = PagedFileError;

    fn try_from(value: &FileMetadata) -> Result<Self, Self::Error> {
        let metadata_bytes = bytemuck::bytes_of(value);
        let mut buffer = Vec::from(metadata_bytes);
        buffer.resize(PAGE_SIZE, 0);
        // We can unwrap here as we are sure that buffer size is `PAGE_SIZE`.
        Ok(buffer.try_into().unwrap())
    }
}

/// Should be used when creating new [`PagedFile`]
impl Default for FileMetadata {
    fn default() -> Self {
        Self {
            magic_number: FileMetadata::CODB_MAGIC_NUMBER,
            first_free_page_id: FreePage::NO_FREE_PAGE,
            next_page_id: FileMetadata::DEFAULT_NEXT_PAGE_ID,
        }
    }
}

/// [`FreePage`] represents page that is allocated on disk, but is not currently used by [`PagedFile`].
///
/// Free pages are stored on disk as linked list. [`FileMetadata`] points to the head of that list.
/// When adding new page, it's added to the front of the list.
/// When removing page (meaning that we want to allocate it for user), it's taken from the front of the list (so it's LIFO).
/// When pages are removed via truncating then they are removed based on the physical position in the file (and this is why we need both `next` and `previous` pointers).
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct FreePage {
    next: PageId,
    previous: PageId,
}

impl FreePage {
    /// Special value used as "null" free page.
    const NO_FREE_PAGE: PageId = 0;

    fn update(&mut self, args: &UpdateFreePageArgs) {
        if let Some(next) = args.next {
            self.next = next;
        }
        if let Some(prev) = args.previous {
            self.previous = prev;
        }
    }
}

/// Should be used to deserialize [`FreePage`] from [`Page`].
impl TryFrom<&Page> for FreePage {
    type Error = PagedFileError;

    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let size = size_of::<FreePage>();
        let free_page = bytemuck::try_from_bytes(&value[..size])
            .map_err(|e| PagedFileError::InvalidFileFormat(e.to_string()))?;
        Ok(*free_page)
    }
}

/// Should be used to serialize [`FreePage`] into [`Page`]
impl TryFrom<&FreePage> for Page {
    type Error = PagedFileError;

    fn try_from(value: &FreePage) -> Result<Self, Self::Error> {
        let free_page = bytemuck::bytes_of(value);
        let mut buffer = Vec::from(free_page);
        buffer.resize(PAGE_SIZE, 0);
        // We can unwrap here as we are sure that buffer size is `PAGE_SIZE`.
        Ok(buffer.try_into().unwrap())
    }
}

struct UpdateFreePageArgs {
    next: Option<PageId>,
    previous: Option<PageId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use tempfile::NamedTempFile;

    fn create_metadata_page(next_page_id: PageId, first_free_page_id: PageId) -> Page {
        let metadata = FileMetadata {
            magic_number: FileMetadata::CODB_MAGIC_NUMBER,
            first_free_page_id,
            next_page_id,
        };
        Page::try_from(&metadata).unwrap()
    }

    fn write_temp_file_with_content(contents: &[u8]) -> NamedTempFile {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        std::fs::write(&path, contents).unwrap();
        temp_file
    }

    fn read_metadata_from_disk(path: &std::path::Path) -> FileMetadata {
        let mut file = std::fs::File::open(path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        file.read_exact(&mut buf).unwrap();
        FileMetadata::try_from(buf).unwrap()
    }

    fn setup_file_with_metadata_and_n_pages(
        next_page_id: PageId,
        first_free_page_id: PageId,
        free_page_chain: &[(PageId, PageId, PageId)], // (page_id, next, previous)
        page_patterns: &[u8],
    ) -> (tempfile::NamedTempFile, Vec<[u8; PAGE_SIZE]>) {
        let metadata_page = create_metadata_page(next_page_id, first_free_page_id);
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);

        let mut pages = Vec::new();

        // Create a map of which pages are free pages for easy lookup
        let free_page_map: HashMap<PageId, (PageId, PageId)> = free_page_chain
            .iter()
            .map(|(page_id, next, prev)| (*page_id, (*next, *prev)))
            .collect();

        for (i, &pattern) in page_patterns.iter().enumerate() {
            // Page IDs start from 1
            let page_id = (i + 1) as PageId;

            let page = if let Some((next, prev)) = free_page_map.get(&page_id) {
                // This is a free page, create FreePage structure
                let free_page = FreePage {
                    next: *next,
                    previous: *prev,
                };
                Page::try_from(&free_page).unwrap()
            } else {
                // This is a regular data page
                [pattern; PAGE_SIZE]
            };

            file_content.extend_from_slice(&page);
            pages.push(page);
        }

        let temp_file = write_temp_file_with_content(&file_content);
        (temp_file, pages)
    }

    #[test]
    fn paged_file_new_creates_file_and_metadata() {
        // given a path to a non-existent file
        let temp_dir = tempfile::tempdir().unwrap();
        let file_path = temp_dir.path().join("new_db_file.codb");
        assert!(!file_path.exists());

        // when creating PagedFile
        let paged_file = PagedFile::new(&file_path).unwrap();

        // then file is created
        assert!(file_path.exists());

        // and metadata is initialized as expected
        assert_eq!(
            paged_file.metadata.next_page_id,
            FileMetadata::DEFAULT_NEXT_PAGE_ID
        );

        // and first free page points to page 1
        if FileMetadata::DEFAULT_NEXT_PAGE_ID > 1 {
            assert_eq!(paged_file.metadata.first_free_page_id, 1);
        } else {
            assert_eq!(
                paged_file.metadata.first_free_page_id,
                FreePage::NO_FREE_PAGE
            );
        }

        // and in-memory free pages set contains the expected pages
        let expected_free: HashSet<_> = (1..FileMetadata::DEFAULT_NEXT_PAGE_ID).collect();
        assert_eq!(paged_file.free_pages, expected_free);

        // and file size is correct
        let metadata = fs::metadata(&file_path).unwrap();
        assert_eq!(
            metadata.len(),
            FileMetadata::DEFAULT_NEXT_PAGE_ID as u64 * PAGE_SIZE as u64
        );

        // and the metadata on disk matches
        let disk_metadata = read_metadata_from_disk(&file_path);
        assert_eq!(
            disk_metadata.next_page_id,
            FileMetadata::DEFAULT_NEXT_PAGE_ID
        );

        // and first free page on disk matches
        if FileMetadata::DEFAULT_NEXT_PAGE_ID > 1 {
            assert_eq!(disk_metadata.first_free_page_id, 1);
        } else {
            assert_eq!(disk_metadata.first_free_page_id, FreePage::NO_FREE_PAGE);
        }
    }

    #[test]
    fn paged_file_from_file_file_too_small() {
        // given file with size < `PAGE_SIZE`
        let temp_file = write_temp_file_with_content(&[1, 2, 3]);

        // when try to load `PagedFile` from it
        let result = PagedFile::new(temp_file.path());

        // then error is returned
        assert!(matches!(
            result.err().unwrap(),
            PagedFileError::InvalidFileFormat(msg)
                if msg == "file shorter than one page"
        ));
    }

    #[test]
    fn paged_file_from_file_invalid_magic_number() {
        // given page with invalid magic number
        let mut bad_page = [0u8; PAGE_SIZE];
        bad_page[..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let temp_file = write_temp_file_with_content(&bad_page);

        // when try to load `PagedFile` from it
        let result = PagedFile::new(temp_file.path());

        // then error is returned
        assert!(matches!(
            result.err().unwrap(),
            PagedFileError::InvalidFileFormat(msg)
                if msg == "invalid magic number ('[222, 173, 190, 239]')"
        ));
    }

    #[test]
    fn paged_file_from_file_valid_metadata_cases() {
        struct TestCase {
            next: PageId,
            first_free: PageId,
            free_chain: Vec<(PageId, PageId, PageId)>,
            expected_free_set: HashSet<PageId>,
        }

        let test_cases = [
            // empty free
            TestCase {
                next: 2,
                first_free: FreePage::NO_FREE_PAGE,
                free_chain: vec![],
                expected_free_set: HashSet::new(),
            },
            // single free page
            TestCase {
                next: 2,
                first_free: 1,
                free_chain: vec![(1, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
                expected_free_set: [1].into_iter().collect(),
            },
            // multiple free pages
            TestCase {
                next: 6,
                first_free: 2,
                free_chain: vec![
                    (2, 4, FreePage::NO_FREE_PAGE),
                    (4, 5, 2),
                    (5, FreePage::NO_FREE_PAGE, 4),
                ],
                expected_free_set: [2, 4, 5].into_iter().collect(),
            },
        ];

        for case in test_cases {
            let page_patterns: Vec<u8> = if case.next > 1 {
                (1..case.next).map(|i| i as u8).collect()
            } else {
                vec![]
            };

            // given valid metadata page and free page chain
            let (temp_file, _) = setup_file_with_metadata_and_n_pages(
                case.next,
                case.first_free,
                &case.free_chain,
                &page_patterns,
            );

            // when try to load `PagedFile` from it
            let paged_file = PagedFile::new(temp_file.path()).unwrap();

            // then `PagedFile` instance is returned with correct metadata
            assert_eq!(paged_file.metadata.next_page_id, case.next,);
            assert_eq!(paged_file.metadata.first_free_page_id, case.first_free,);
            assert_eq!(paged_file.free_pages, case.expected_free_set,);
        }
    }

    #[test]
    fn paged_file_read_page_metadata_page() {
        // given a file with a valid metadata page and one data page
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(1, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and reading page 0 (metadata)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.read_page(0);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(0))));
    }

    #[test]
    fn paged_file_read_page_free_page() {
        // given a file with a valid metadata page and page 2 marked as free
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        // when loading PagedFile and reading page 2 (free)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.read_page(2);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_read_page_unallocated_page() {
        // given a file with a valid metadata page and next_page_id = 2 (only page 1 is allocated)
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and reading page 2 (unallocated)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.read_page(2);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_read_page_valid_page() {
        // given a file with a valid metadata page and one data page
        let (temp_file, pages) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[42]);

        // when loading PagedFile and reading page 1
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let read = paged_file.read_page(1).unwrap();

        // then the page data is correct
        assert_eq!(read, pages[0]);
    }

    #[test]
    fn paged_file_write_page_metadata_page() {
        // given a file with a valid metadata page and one data page
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and writing to page 0 (metadata)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = paged_file.write_page(0, new_page);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(0))));
    }

    #[test]
    fn paged_file_write_page_free_page() {
        // given a file with a valid metadata page and page 2 marked as free
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        // when loading PagedFile and writing to page 2 (free)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = paged_file.write_page(2, new_page);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_write_page_unallocated_page() {
        // given a file with a valid metadata page and next_page_id = 2 (only page 1 is allocated)
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and writing to page 2 (unallocated)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = paged_file.write_page(2, new_page);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_write_page_valid_page() {
        // given a file with a valid metadata page and one data page
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and writing new data to page 1
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        paged_file.write_page(1, new_page).unwrap();
        paged_file.flush().unwrap();

        // then the file contains the new data at page 1
        let mut file = std::fs::File::open(temp_file.path()).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE * 2];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[PAGE_SIZE..], &new_page);
    }

    #[test]
    fn paged_file_allocate_page_new_page() {
        // given a file with a valid metadata page and one data page, no free pages
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and allocating a new page
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let allocated = paged_file.allocate_page().unwrap();
        paged_file.flush().unwrap();

        // then the new page id is 2 (next_page_id before allocation)
        assert_eq!(allocated, 2);

        // and file size increased by one page
        let metadata = fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 3);

        // and metadata on disk is updated
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 3);
    }

    #[test]
    fn paged_file_allocate_page_from_free_list() {
        // given a file with a valid metadata page and page 2 marked as free
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        // when loading PagedFile and allocating a page
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let allocated = paged_file.allocate_page().unwrap();
        paged_file.flush().unwrap();

        // then the allocated page is 2 (from free list)
        assert_eq!(allocated, 2);

        // and file size is unchanged
        let metadata = fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 3);

        // and free pages (both on disk and in-memory) are updated
        assert_eq!(
            paged_file.metadata.first_free_page_id,
            FreePage::NO_FREE_PAGE
        );
        assert!(paged_file.free_pages.is_empty());
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.first_free_page_id, FreePage::NO_FREE_PAGE);
    }

    #[test]
    fn paged_file_allocate_page_multiple() {
        // given a file with a valid metadata page and two free pages
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            4,
            2,
            &[
                (2, 3, FreePage::NO_FREE_PAGE),
                (3, FreePage::NO_FREE_PAGE, 2),
            ],
            &[1, 2, 3],
        );

        // when loading PagedFile and allocating first page
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let first = paged_file.allocate_page().unwrap();

        // then first is properly allocated
        assert_eq!(first, 2);

        // and free page list is updated
        assert_eq!(paged_file.metadata.first_free_page_id, 3);
        assert!(paged_file.free_pages.contains(&3));
        assert_eq!(paged_file.free_pages.len(), 1);

        // when allocating second page
        let second = paged_file.allocate_page().unwrap();
        paged_file.flush().unwrap();

        // then second page is allocated
        assert_eq!(second, 3);
        // and free pages (both on disk and in-memory) are updated
        assert_eq!(
            paged_file.metadata.first_free_page_id,
            FreePage::NO_FREE_PAGE
        );
        assert!(paged_file.free_pages.is_empty());
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.first_free_page_id, FreePage::NO_FREE_PAGE);
    }

    #[test]
    fn paged_file_free_page_metadata_page() {
        // given a file with a valid metadata page and one data page
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and freeing page 0 (metadata)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.free_page(0);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(0))));
    }

    #[test]
    fn paged_file_free_page_free_page() {
        // given a file with a valid metadata page and page 2 already free
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        // when loading PagedFile and freeing page 2 again
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.free_page(2);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_free_page_unallocated_page() {
        // given a file with a valid metadata page and next_page_id = 2 (only page 1 is allocated)
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        // when loading PagedFile and freeing page 2 (unallocated)
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        let result = paged_file.free_page(2);

        // then error is returned
        assert!(matches!(result, Err(PagedFileError::InvalidPageId(2))));
    }

    #[test]
    fn paged_file_free_page_valid() {
        // given a file with a valid metadata page and two data pages
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(3, FreePage::NO_FREE_PAGE, &[], &[1, 2]);

        // when loading PagedFile and freeing page 2
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        paged_file.free_page(2).unwrap();
        paged_file.flush().unwrap();

        // then page 2 is in free_pages in memory
        assert!(paged_file.free_pages.contains(&2));

        // and on disk data is updated
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.first_free_page_id, 2);
    }

    #[test]
    fn paged_file_truncate_removes_free_pages_at_end() {
        // given a file with 5 pages, pages 3 and 4 are free (linked: 3 -> 4)
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            5,
            3,
            &[
                (3, 4, FreePage::NO_FREE_PAGE),
                (4, FreePage::NO_FREE_PAGE, 3),
            ],
            &[1, 2, 3, 4],
        );

        // when loading PagedFile and truncating
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        paged_file.truncate().unwrap();
        paged_file.flush().unwrap();

        // then next_page_id is 3 (pages 3 and 4 removed)
        assert_eq!(paged_file.metadata.next_page_id, 3);

        // and file size is 3 pages (metadata + 2 data)
        let metadata = fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 3);

        // and free pages are removed from in-memory set
        assert!(!paged_file.free_pages.contains(&3));
        assert!(!paged_file.free_pages.contains(&4));
        assert_eq!(
            paged_file.metadata.first_free_page_id,
            FreePage::NO_FREE_PAGE
        );

        // and metadata on disk is updated
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 3);
        assert_eq!(disk_metadata.first_free_page_id, FreePage::NO_FREE_PAGE);
    }

    #[test]
    fn paged_file_truncate_no_free_pages_at_end() {
        // given a file with 5 pages, only page 2 is free (not at the end)
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            5,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2, 3, 4],
        );

        // when loading PagedFile and truncating
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        paged_file.truncate().unwrap();
        paged_file.flush().unwrap();

        // then next_page_id is unchanged
        assert_eq!(paged_file.metadata.next_page_id, 5);

        // and file size is unchanged
        let metadata = std::fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 5);

        // and free page 2 is still in memory
        assert!(paged_file.free_pages.contains(&2));
        assert_eq!(paged_file.metadata.first_free_page_id, 2);

        // and metadata on disk is unchanged
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 5);
        assert_eq!(disk_metadata.first_free_page_id, 2);
    }

    #[test]
    fn paged_file_truncate_all_pages_free() {
        // given a file with 6 pages, pages 2, 4, and 5 are free (linked: 2 -> 4 -> 5)
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            6,
            2, // first_free_page_id
            &[
                (2, 4, FreePage::NO_FREE_PAGE), // page 2: next=4, prev=none (head)
                (4, 5, 2),                      // page 4: next=5, prev=2 (middle)
                (5, FreePage::NO_FREE_PAGE, 4), // page 5: next=none, prev=4 (tail)
            ],
            &[1, 2, 3, 4, 5], // patterns for pages 1, 2, 3, 4, 5
        );

        // when loading PagedFile and truncating
        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();
        paged_file.truncate().unwrap();
        paged_file.flush().unwrap();

        // then next_page_id is 4 (pages 4 and 5 removed, but page 2 remains)
        assert_eq!(paged_file.metadata.next_page_id, 4);

        // and file size is 4 pages (metadata + 3 data pages)
        let metadata = std::fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 4);

        // and free pages at the end are removed from in-memory set
        assert!(paged_file.free_pages.contains(&2)); // page 2 should remain (not at end)
        assert!(!paged_file.free_pages.contains(&4)); // page 4 should be removed
        assert!(!paged_file.free_pages.contains(&5)); // page 5 should be removed

        // and first_free_page_id points to remaining free page
        assert_eq!(paged_file.metadata.first_free_page_id, 2);

        // and metadata on disk is updated
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 4);
        assert_eq!(disk_metadata.first_free_page_id, 2);
    }

    #[test]
    fn paged_file_allocate_then_free_then_allocate() {
        // given a file with a valid metadata page and one data page, no free pages
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(2, FreePage::NO_FREE_PAGE, &[], &[1]);

        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();

        // when allocating a new page
        let allocated = paged_file.allocate_page().unwrap();

        // then the new page id is 2
        assert_eq!(allocated, 2);

        // when freeing the page
        paged_file.free_page(2).unwrap();

        // then page 2 is in free_pages
        assert!(paged_file.free_pages.contains(&2));

        // when allocating again
        let reallocated = paged_file.allocate_page().unwrap();

        // then should get the same page back and it's removed from free_pages
        assert_eq!(reallocated, 2);
        assert!(!paged_file.free_pages.contains(&2));
    }

    #[test]
    fn paged_file_free_page_updates_linked_list() {
        // given a file with a valid metadata page and one free page
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            4,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2, 3],
        );

        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();

        // when freeing page 3
        paged_file.free_page(3).unwrap();

        // then page 3 becomes the new head of the free list
        assert_eq!(paged_file.metadata.first_free_page_id, 3);

        // and the linked list structure is correct
        let free_page_3 = paged_file.read_free_page(3).unwrap();
        assert_eq!(free_page_3.next, 2);
        assert_eq!(free_page_3.previous, FreePage::NO_FREE_PAGE);

        let free_page_2 = paged_file.read_free_page(2).unwrap();
        assert_eq!(free_page_2.next, FreePage::NO_FREE_PAGE);
        assert_eq!(free_page_2.previous, 3);
    }

    #[test]
    fn paged_file_truncate_with_interleaved_free_pages() {
        // given a file with free pages scattered throughout (pages 2, 6, 7 are free)
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            8,
            2,
            &[
                // head -> 2 -> 6 -> 7
                (2, 6, FreePage::NO_FREE_PAGE),
                (6, 7, 2),
                (7, FreePage::NO_FREE_PAGE, 6),
            ],
            &[1, 2, 3, 4, 5, 6, 7],
        );

        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();

        // when truncating
        paged_file.truncate().unwrap();

        // then pages 6 and 7 are removed, but page 2 remains
        assert_eq!(paged_file.metadata.next_page_id, 6);
        assert!(paged_file.free_pages.contains(&2));
        assert!(!paged_file.free_pages.contains(&6));
        assert!(!paged_file.free_pages.contains(&7));
        assert_eq!(paged_file.metadata.first_free_page_id, 2);

        // and page 2 is now the only node in the linked list
        let free_page_2 = paged_file.read_free_page(2).unwrap();
        assert_eq!(free_page_2.next, FreePage::NO_FREE_PAGE);
        assert_eq!(free_page_2.previous, FreePage::NO_FREE_PAGE);
    }

    #[test]
    fn paged_file_large_free_list_allocation_order() {
        // given a file with multiple data pages and no free pages initially
        let (temp_file, _) =
            setup_file_with_metadata_and_n_pages(5, FreePage::NO_FREE_PAGE, &[], &[1, 2, 3, 4]);

        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();

        // when freeing pages in order: 2, 3, 4
        paged_file.free_page(2).unwrap();
        paged_file.free_page(3).unwrap();
        paged_file.free_page(4).unwrap();

        // when allocating pages back
        let first_allocated = paged_file.allocate_page().unwrap();
        let second_allocated = paged_file.allocate_page().unwrap();
        let third_allocated = paged_file.allocate_page().unwrap();

        // then pages are allocated in LIFO order
        assert_eq!(first_allocated, 4);
        assert_eq!(second_allocated, 3);
        assert_eq!(third_allocated, 2);
    }

    #[test]
    fn paged_file_corrupted_free_list_detection() {
        // given a file with corrupted free page chain (points to non-existent page)
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            1,
            &[(1, 999, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        // when trying to load PagedFile
        let result = PagedFile::new(temp_file.path());

        // then loading fails
        assert!(result.is_err());
    }

    #[test]
    fn paged_file_allocation_exhausts_free_list_then_extends() {
        // given a file with one free page
        let (temp_file, _) = setup_file_with_metadata_and_n_pages(
            3,
            2,
            &[(2, FreePage::NO_FREE_PAGE, FreePage::NO_FREE_PAGE)],
            &[1, 2],
        );

        let mut paged_file = PagedFile::new(temp_file.path()).unwrap();

        // when allocating the free page
        let first_allocated = paged_file.allocate_page().unwrap();

        // then it comes from the free list
        assert_eq!(first_allocated, 2);
        assert!(paged_file.free_pages.is_empty());
        assert_eq!(
            paged_file.metadata.first_free_page_id,
            FreePage::NO_FREE_PAGE
        );

        // when allocating another page
        let second_allocated = paged_file.allocate_page().unwrap();

        // then it extends the file
        assert_eq!(second_allocated, 3);
        assert_eq!(paged_file.metadata.next_page_id, 4);
    }
}
