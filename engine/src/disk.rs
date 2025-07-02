//! Disk module — abstraction layer for managing on-disk files and page operations.

use std::{collections::HashSet, path::Path};

use thiserror::Error;
use tokio::fs;

/// Type representing page id, should be used instead of using bare `u64`.
type PageId = u64;

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
    // TODO: populate when implementing `FileManager`
}

impl FileManager {
    /// Creates a new instance of [`FileManager`]. When `file_path` points to existing file it
    /// tries to load it from there, otherwise it creates new file at `file_path`.
    pub async fn new<P>(file_path: P) -> Result<FileManager, FileManagerError>
    where
        P: AsRef<Path>,
    {
        todo!()
    }

    /// Reads page with id equal to `page_id` from underlying file. Can fail if io error occurs or `page_id` is not valid.
    pub async fn read_page(&self, page_id: PageId) -> Result<Page, FileManagerError> {
        todo!()
    }

    /// Writes new `page` to page with id `page_id`. It flushes the newly written page to disk, so be careful as it might be bottleneck if used incorrectly.
    /// Page with id `page_id` must be allocated before writing to it. Can fail if io error occurs or `page_id` is not valid.
    pub async fn write_page(
        &mut self,
        page_id: PageId,
        page: Page,
    ) -> Result<(), FileManagerError> {
        todo!()
    }

    /// Allocates new page and returns its `PageId`. If there is a free page in [`FileMetadata`]'s `free_pages` it uses it,
    /// otherwise creates new page. Returned page id is guaranteed to point to page that is not used.
    /// Can fail if io error occurs.
    pub async fn allocate_page(&mut self) -> Result<PageId, FileManagerError> {
        todo!()
    }

    /// Frees page with `page_id` so that it can be reused later.
    /// It doesn't erase the page content, but adds its `page_id` to [`FileMetadata`]'s `free_pages`.
    pub async fn free_page(&mut self, page_id: PageId) -> Result<(), FileManagerError> {
        todo!()
    }

    /// Defragments the file - remove unused allocated pages, move used allocated pages so that the file is truncated to the
    /// minimum number of pages required for it to hold whole data. This function heavily uses the disk and shouldn't be called too often.
    /// Can fail if io error occurs.
    pub async fn defragment(&mut self) -> Result<(), FileManagerError> {
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
