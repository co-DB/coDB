//! Disk module — abstraction layer for managing on-disk files and page operations.

use std::{
    collections::HashSet,
    fs,
    io::{self, Cursor, ErrorKind, Read, Seek, Write},
    path::Path,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

/// Type representing page id, should be used instead of using bare `u64`.
type PageId = u64;

/// Metadata page id - page with this id should only be used internally by [`FileManager`].
const METADATA_PAGE_ID: PageId = 0;

/// Size of each page in [`FileManager`].
const PAGE_SIZE: usize = 4096; // 4 kB

/// Type representing page, should be used instead of bare array of bytes.
type Page = [u8; PAGE_SIZE];

/// Responsible for managing a single on-disk file.
/// Only this structure should be responsible for directly communicating with disk.
///   
/// File managed by [`FileManager`] is divided into fixed-size pages.
///
/// Page 0 (first page) is a special page that should be used by [`FileManager`] for storing metadata ([`FileMetadata`]). It means that each file will be at least one page long, even when they have no other content, but this is a trade-off for better pages alignment. For more details about structure of the first page look at [`FileMetadata`].
/// Pages from 1 to N have no defined format from [FileManager]'s perspective - its sole responsibility is to allow reading, writing and allocating pages.
pub struct FileManager {
    /// handle to underlying file
    handle: fs::File,
    /// file's metadata
    metadata: FileMetadata,
}

/// Error for [`FileManager`] related operations.
#[derive(Error, Debug)]
pub enum FileManagerError {
    /// Provided page id was invalid, e.g. tried to read [`METADATA_PAGE_ID`]
    #[error("invalid page id: {0}")]
    InvalidPageId(PageId),
    /// File used for loading [`FileManager`] has invalid format
    #[error("file has invalid format: {0}")]
    InvalidFileFormat(&'static str),
    /// Underlying IO module returned error
    #[error("io error occured: {0}")]
    IoError(#[source] io::Error),
}

impl From<io::Error> for FileManagerError {
    fn from(err: io::Error) -> Self {
        FileManagerError::IoError(err)
    }
}

impl FileManager {
    /// Creates a new instance of [`FileManager`]. When `file_path` points to existing file it
    /// tries to load it from there, otherwise it creates new file at `file_path`.
    pub fn new<P>(file_path: P) -> Result<FileManager, FileManagerError>
    where
        P: AsRef<Path>,
    {
        // It returns error if existance of the file at `file_path` cannot be checked, e.g.
        // if we don't have permission to read that file. I don't think there is anything we can do about it,
        // so just return underlying error in that case.
        let exists = file_path.as_ref().try_exists()?;
        match exists {
            true => {
                let file = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&file_path)?;
                FileManager::try_from(file)
            }
            false => todo!(),
        }
    }

    /// Reads page with id equal to `page_id` from underlying file. Can fail if io error occurs or `page_id` is not valid.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page, FileManagerError> {
        if self.is_invalid_page_id(page_id) {
            return Err(FileManagerError::InvalidPageId(page_id));
        }

        self.seek_page(page_id)?;
        let mut buffer = [0u8; PAGE_SIZE];
        self.handle.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes new `page` to page with id `page_id`. It flushes the newly written page to disk, so be careful as it might be bottleneck if used incorrectly.
    /// Page with id `page_id` must be allocated before writing to it. Can fail if io error occurs or `page_id` is not valid.
    pub fn write_page(&mut self, page_id: PageId, page: Page) -> Result<(), FileManagerError> {
        if self.is_invalid_page_id(page_id) {
            return Err(FileManagerError::InvalidPageId(page_id));
        }

        self.seek_page(page_id)?;
        self.handle.write_all(&page)?;
        self.handle.flush()?;

        Ok(())
    }

    /// Allocates new page and returns its `PageId`. If there is a free page in [`FileMetadata`]'s `free_pages` it uses it,
    /// otherwise creates new page. Returned page id is guaranteed to point to page that is not used.
    /// Can fail if io error occurs.
    pub fn allocate_page(&mut self) -> Result<PageId, FileManagerError> {
        let page_id = if self.metadata.free_pages.is_empty() {
            let page_id = self.metadata.next_page_id;
            self.metadata.next_page_id += 1;
            let new_size = self.metadata.next_page_id * PAGE_SIZE as u64;
            self.handle.set_len(new_size)?;
            page_id
        } else {
            // We can unwrap here as we check if `free_pages` is empty
            let page_id = self.metadata.free_pages.iter().next().copied().unwrap();
            self.metadata.free_pages.remove(&page_id);
            page_id
        };
        self.sync_metadata()?;
        Ok(page_id)
    }

    /// Frees page with `page_id` so that it can be reused later.
    /// It doesn't erase the page content, but adds its `page_id` to [`FileMetadata`]'s `free_pages`.
    pub fn free_page(&mut self, page_id: PageId) -> Result<(), FileManagerError> {
        todo!()
    }

    /// Truncates the file - remove unused allocated pages from the end of the file. Can fail if io error occurs.
    pub fn truncate(&mut self) -> Result<(), FileManagerError> {
        todo!()
    }

    /// Returns id of root page. Can be `None` if `root_page_id` was not set yet (it's not set automatically when new file is created).
    pub fn root_page_id(&self) -> Option<PageId> {
        todo!()
    }

    /// Sets new root page id. `page_id` must be already pointing to allocated page. Can fail if `page_id` is not valid.
    pub fn set_root_page_id(&mut self, page_id: PageId) -> Result<(), FileManagerError> {
        todo!()
    }

    /// Seeks underlying file handle to the start of the page with `page_id`.
    fn seek_page(&mut self, page_id: PageId) -> Result<(), FileManagerError> {
        let start = PAGE_SIZE as u64 * page_id;
        self.handle.seek(io::SeekFrom::Start(start))?;
        Ok(())
    }

    /// Helper to check if page with `page_id` can be read from/write to.
    fn is_invalid_page_id(&self, page_id: PageId) -> bool {
        let is_metadata = page_id == METADATA_PAGE_ID;
        let is_free = self.metadata.free_pages.contains(&page_id);
        let is_unallocated = page_id >= self.metadata.next_page_id;
        is_metadata || is_free || is_unallocated
    }

    /// Syncs in-memory metadata with data stored in [`METADATA_PAGE_ID`] page.
    fn sync_metadata(&mut self) -> Result<(), FileManagerError> {
        let metadata_page = Page::try_from(&self.metadata)?;
        self.seek_page(METADATA_PAGE_ID)?;
        self.handle.write_all(&metadata_page)?;
        self.handle.flush()?;
        Ok(())
    }
}

/// Try to deserialize existing [`FileManager`] from file. Can fail if io error occurrs or if underlying file
/// has invalid format.
impl TryFrom<fs::File> for FileManager {
    type Error = FileManagerError;

    fn try_from(mut value: fs::File) -> Result<Self, Self::Error> {
        let mut metadata_buffer = [0u8; PAGE_SIZE];
        if let Err(e) = value.read_exact(&mut metadata_buffer) {
            match e.kind() {
                ErrorKind::UnexpectedEof => {
                    return Err(FileManagerError::InvalidFileFormat(
                        "file shorter than one page",
                    ));
                }
                _ => return Err(FileManagerError::IoError(e)),
            }
        }
        let file_metadata = FileMetadata::try_from(metadata_buffer)?;
        Ok(FileManager {
            handle: value,
            metadata: file_metadata,
        })
    }
}

/// Magic number - used for checking if file is (has high chances to be) codb file.
const CODB_MAGIC_NUMBER: [u8; 4] = [0xC, 0x0, 0xD, 0xB];

/// Storage for file metadata.
///
/// [`FileMetadata`] is always stored in first page of the file. It contains metadata information used by [`FileManager`] -
/// no other struct should directly use it (it should not be exported outside this module).
///
/// Format of the first page in the file is as follows:
/// - `magic_number` (4 bytes) - only to verify if it's our file, no need to load it to [`FileMetadata`] as it is constant
/// - `root_page_id` (8 bytes) - 0 when `None`, as it cannot be first page (first page is reserved for metadata)
/// - `next_page_id` (8 bytes)
/// - `free_pages_length` (4 bytes)
/// - `free_pages` (`free_pages_length` * 8 bytes) - should be skipped when `free_pages_length = 0`
struct FileMetadata {
    /// id of root page
    root_page_id: Option<PageId>,
    /// id of next page
    next_page_id: PageId,
    /// set of free pages (already allocated, but not used)
    free_pages: HashSet<PageId>,
}

/// Should be used to deserialize [`FileMetadata`] from [`Page`].
impl TryFrom<Page> for FileMetadata {
    type Error = FileManagerError;

    fn try_from(value: Page) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(value);
        let mut magic_number = [0u8; 4];
        cursor.read_exact(&mut magic_number)?;
        if magic_number != CODB_MAGIC_NUMBER {
            return Err(FileManagerError::InvalidFileFormat("invalid magic number"));
        }
        let root_page_value = cursor.read_u64::<BigEndian>()?;
        let root_page_id = match root_page_value {
            0 => None,
            _ => Some(root_page_value),
        };
        let next_page_id = cursor.read_u64::<BigEndian>()?;
        let free_pages_length = cursor.read_u32::<BigEndian>()? as _;
        let mut free_pages = HashSet::with_capacity(free_pages_length);
        for _ in 0..free_pages_length {
            let free_page_id = cursor.read_u64::<BigEndian>()?;
            free_pages.insert(free_page_id);
        }
        Ok(FileMetadata {
            root_page_id,
            next_page_id,
            free_pages,
        })
    }
}

/// Should be used to serialize [`FileMetadata`] into [`Page`]
impl TryFrom<&FileMetadata> for Page {
    type Error = FileManagerError;

    fn try_from(value: &FileMetadata) -> Result<Self, Self::Error> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE);
        buffer.extend_from_slice(&CODB_MAGIC_NUMBER);
        let root_page_id = value.root_page_id.unwrap_or(0);
        buffer.write_u64::<BigEndian>(root_page_id)?;
        buffer.write_u64::<BigEndian>(value.next_page_id)?;
        buffer.write_u32::<BigEndian>(value.free_pages.len() as _)?;
        for free_page_id in &value.free_pages {
            buffer.write_u64::<BigEndian>(*free_page_id)?;
        }
        buffer.resize(PAGE_SIZE, 0);
        // We can unwrap here as we are sure that buffer size is `PAGE_SIZE`.
        Ok(buffer.try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use std::collections::HashSet;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_metadata_page(
        root_page_id: Option<u64>,
        next_page_id: u64,
        free_pages: &[u64],
    ) -> Page {
        let mut buffer = Vec::with_capacity(PAGE_SIZE);
        buffer.extend_from_slice(&CODB_MAGIC_NUMBER);
        buffer
            .write_u64::<BigEndian>(root_page_id.unwrap_or(0))
            .unwrap();
        buffer.write_u64::<BigEndian>(next_page_id).unwrap();
        buffer
            .write_u32::<BigEndian>(free_pages.len() as u32)
            .unwrap();
        for id in free_pages {
            buffer.write_u64::<BigEndian>(*id).unwrap();
        }
        buffer.resize(PAGE_SIZE, 0);
        buffer.try_into().unwrap()
    }

    fn write_temp_file_with_content(contents: &[u8]) -> NamedTempFile {
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        std::fs::write(&path, contents).unwrap();
        temp_file.flush().unwrap();
        temp_file
    }

    fn read_metadata_from_disk(path: &std::path::Path) -> FileMetadata {
        let mut file = std::fs::File::open(path).unwrap();
        let mut buf = [0u8; PAGE_SIZE];
        file.read_exact(&mut buf).unwrap();
        FileMetadata::try_from(buf).unwrap()
    }

    #[test]
    fn file_manager_from_file_file_too_small() {
        // given file with size < `PAGE_SIZE`
        let temp_file = write_temp_file_with_content(&[1, 2, 3]);

        // when try to load `FileManager` from it
        let result = FileManager::new(temp_file.path());

        // then error is returned
        assert!(matches!(
            result.err().unwrap(),
            FileManagerError::InvalidFileFormat(msg)
                if msg == "file shorter than one page"
        ));
    }

    #[test]
    fn file_manager_from_file_invalid_magic_number() {
        // given page with invalid magic number
        let mut bad_page = [0u8; PAGE_SIZE];
        bad_page[..4].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        let temp_file = write_temp_file_with_content(&bad_page);

        // when try to load `FileManager` from it
        let result = FileManager::new(temp_file.path());

        // then error is returned
        assert!(matches!(
            result.err().unwrap(),
            FileManagerError::InvalidFileFormat(msg)
                if msg == "invalid magic number"
        ));
    }

    #[test]
    fn file_manager_from_file_valid_metadata_cases() {
        struct TestCase {
            name: &'static str,
            root: Option<u64>,
            next: u64,
            free: Vec<u64>,
        }

        let test_cases = [
            TestCase {
                name: "empty root, empty free",
                root: None,
                next: 1,
                free: vec![],
            },
            TestCase {
                name: "some root, empty free",
                root: Some(42),
                next: 2,
                free: vec![],
            },
            TestCase {
                name: "empty root, some free",
                root: None,
                next: 3,
                free: vec![10, 20, 30],
            },
            TestCase {
                name: "some root, some free",
                root: Some(111),
                next: 4,
                free: vec![40, 50, 60],
            },
        ];

        for case in test_cases {
            // given valid metadata page
            let page = create_metadata_page(case.root, case.next, &case.free);
            let temp_file = write_temp_file_with_content(&page);

            // when try to load `FileManager` from it
            let manager = FileManager::new(temp_file.path()).unwrap();

            // then `FileManager` instance is returned
            assert_eq!(
                manager.metadata.root_page_id, case.root,
                "root_page_id failed for case '{}'",
                case.name
            );
            assert_eq!(
                manager.metadata.next_page_id, case.next,
                "next_page_id failed for case '{}'",
                case.name
            );
            assert_eq!(
                manager.metadata.free_pages,
                case.free.iter().cloned().collect::<HashSet<_>>(),
                "free_pages failed for case '{}'",
                case.name
            );
        }
    }

    #[test]
    fn file_manager_read_page_metadata_page() {
        // given a file with a valid metadata page and one data page
        let metadata_page = create_metadata_page(None, 2, &[]);
        let page_data = [1u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page_data);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and reading page 0 (metadata)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let result = manager.read_page(0);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(0))));
    }

    #[test]
    fn file_manager_read_page_free_page() {
        // given a file with a valid metadata page and page 2 marked as free
        let metadata_page = create_metadata_page(None, 3, &[2]);
        let page1 = [1u8; PAGE_SIZE];
        let page2 = [2u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        file_content.extend_from_slice(&page2);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and reading page 2 (free)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let result = manager.read_page(2);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(2))));
    }

    #[test]
    fn file_manager_read_page_unallocated_page() {
        // given a file with a valid metadata page and next_page_id = 2 (only page 1 is allocated)
        let metadata_page = create_metadata_page(None, 2, &[]);
        let page1 = [1u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and reading page 2 (unallocated)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let result = manager.read_page(2);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(2))));
    }

    #[test]
    fn file_manager_read_page_valid_page() {
        // given a file with a valid metadata page and one data page
        let page_data = [42u8; PAGE_SIZE];
        let metadata_page = create_metadata_page(None, 2, &[]);
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page_data);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and reading page 1
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let read = manager.read_page(1).unwrap();

        // then the page data is correct
        assert_eq!(read, page_data);
    }

    #[test]
    fn file_manager_write_page_metadata_page() {
        // given a file with a valid metadata page and one data page
        let metadata_page = create_metadata_page(None, 2, &[]);
        let page_data = [1u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page_data);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and writing to page 0 (metadata)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = manager.write_page(0, new_page);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(0))));
    }

    #[test]
    fn file_manager_write_page_free_page() {
        // given a file with a valid metadata page and page 2 marked as free
        let metadata_page = create_metadata_page(None, 3, &[2]);
        let page1 = [1u8; PAGE_SIZE];
        let page2 = [2u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        file_content.extend_from_slice(&page2);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and writing to page 2 (free)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = manager.write_page(2, new_page);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(2))));
    }

    #[test]
    fn file_manager_write_page_unallocated_page() {
        // given a file with a valid metadata page and next_page_id = 2 (only page 1 is allocated)
        let metadata_page = create_metadata_page(None, 2, &[]);
        let page1 = [1u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and writing to page 2 (unallocated)
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        let result = manager.write_page(2, new_page);

        // then error is returned
        assert!(matches!(result, Err(FileManagerError::InvalidPageId(2))));
    }

    #[test]
    fn file_manager_write_page_valid_page() {
        // given a file with a valid metadata page and one data page
        let orig_page = [1u8; PAGE_SIZE];
        let metadata_page = create_metadata_page(None, 2, &[]);
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&orig_page);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and writing new data to page 1
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let new_page = [42u8; PAGE_SIZE];
        manager.write_page(1, new_page).unwrap();

        // then the file contains the new data at page 1
        let mut file = std::fs::File::open(temp_file.path()).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE * 2];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[PAGE_SIZE..], &new_page);
    }

    #[test]
    fn file_manager_allocate_page_new_page() {
        // given a file with a valid metadata page and one data page, no free pages
        let metadata_page = create_metadata_page(None, 2, &[]);
        let page1 = [1u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and allocating a new page
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let allocated = manager.allocate_page().unwrap();

        // then the new page id is 2 (next_page_id before allocation)
        assert_eq!(allocated, 2);

        // and file size increased by one page
        let metadata = std::fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 3);

        // and metadata on disk is updated
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 3);
    }

    #[test]
    fn file_manager_allocate_page_from_free_list() {
        // given a file with a valid metadata page and page 2 marked as free
        let metadata_page = create_metadata_page(None, 3, &[2]);
        let page1 = [1u8; PAGE_SIZE];
        let page2 = [2u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        file_content.extend_from_slice(&page2);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and allocating a page
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let allocated = manager.allocate_page().unwrap();

        // then the allocated page is 2 (from free list)
        assert_eq!(allocated, 2);

        // and file size is unchanged
        let metadata = std::fs::metadata(temp_file.path()).unwrap();
        assert_eq!(metadata.len(), PAGE_SIZE as u64 * 3);

        // and metadata on disk is updated (free_pages is now empty)
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 3);
        assert!(disk_metadata.free_pages.is_empty());
    }

    #[test]
    fn file_manager_allocate_page_multiple() {
        // given a file with a valid metadata page and two free pages
        let metadata_page = create_metadata_page(None, 4, &[2, 3]);
        let page1 = [1u8; PAGE_SIZE];
        let page2 = [2u8; PAGE_SIZE];
        let page3 = [3u8; PAGE_SIZE];
        let mut file_content = Vec::new();
        file_content.extend_from_slice(&metadata_page);
        file_content.extend_from_slice(&page1);
        file_content.extend_from_slice(&page2);
        file_content.extend_from_slice(&page3);
        let temp_file = write_temp_file_with_content(&file_content);

        // when loading FileManager and allocating two pages
        let mut manager = FileManager::new(temp_file.path()).unwrap();
        let first = manager.allocate_page().unwrap();
        let second = manager.allocate_page().unwrap();

        // then both pages come from the free list (order not guaranteed)
        assert!(first == 2 || first == 3);
        assert!(second == 2 || second == 3);
        assert_ne!(first, second);

        // and metadata on disk is updated (free_pages is now empty)
        let disk_metadata = read_metadata_from_disk(temp_file.path());
        assert_eq!(disk_metadata.next_page_id, 4);
        assert!(disk_metadata.free_pages.is_empty());
    }
}
