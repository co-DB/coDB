use std::sync::Arc;

use bytemuck::{Pod, Zeroable};
use crossbeam::queue::SegQueue;
use thiserror::Error;

use crate::{
    cache::{Cache, CacheError, FilePageRef, PinnedWritePage},
    consts::CACHE_SIZE,
    files_manager::FileKey,
    paged_file::PageId,
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
struct FreeSpaceMap<const BUCKETS_COUNT: usize, const PAGE_SIZE: usize> {
    buckets: [SegQueue<PageId>; BUCKETS_COUNT],
    cache: Arc<Cache<CACHE_SIZE>>,
    file_key: FileKey,
}

impl<const BUCKETS_COUNT: usize, const PAGE_SIZE: usize> FreeSpaceMap<BUCKETS_COUNT, PAGE_SIZE> {
    /// Finds a page that has at least `needed_space` bytes free.
    ///
    /// Returns a guard that pins the page for writing and will update the [`FreeSpaceMap`]
    /// when dropped. Buckets are only hints: candidates are popped from buckets and the
    /// page header is read to confirm actual free space. If a candidate is stale the
    /// search continues. Returns `Ok(None)` if no suitable page is found.
    fn page_with_free_space<'f>(
        &'f self,
        needed_space: usize,
    ) -> Result<Option<FsmPageGuard<'f, BUCKETS_COUNT, PAGE_SIZE>>, HeapFileError> {
        let start_bucket_idx = self.bucket_for_space(needed_space);
        for b in start_bucket_idx..BUCKETS_COUNT {
            while let Some(page_id) = self.buckets[b].pop() {
                let key = self.file_page_ref(page_id);
                let page = self.cache.pin_write(&key)?;
                let slotted_page = SlottedPage::new(page, true);
                let actual_free_space = slotted_page.free_space()?;
                if actual_free_space >= needed_space as _ {
                    let fpg = FsmPageGuard {
                        page: slotted_page,
                        page_id,
                        fsm: &self,
                    };
                    return Ok(Some(fpg));
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

/// Guard that wraps a pinned-write [`SlottedPage`].
///
/// While held the guard gives exclusive write access to the page. On `Drop` it reads
/// the page's free-space from the header and updates the [`FreeSpaceMap`] (re-inserts the
/// page id into the appropriate bucket) so the map stays a best-effort hint.
struct FsmPageGuard<'f, const BUCKETS_COUNT: usize, const PAGE_SIZE: usize> {
    page: SlottedPage<PinnedWritePage>,
    page_id: PageId,
    fsm: &'f FreeSpaceMap<BUCKETS_COUNT, PAGE_SIZE>,
}

impl<'f, const BUCKETS_COUNT: usize, const PAGE_SIZE: usize> Drop
    for FsmPageGuard<'f, BUCKETS_COUNT, PAGE_SIZE>
{
    fn drop(&mut self) {
        if let Ok(free_space) = self.page.free_space() {
            self.fsm
                .update_page_bucket(self.page_id, free_space as usize);
        }
    }
}

#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub(crate) struct HeapPageHeader {
    base: SlottedPageBaseHeader,
}

unsafe impl ReprC for HeapPageHeader {}

impl SlottedPageHeader for HeapPageHeader {
    fn base(&self) -> &SlottedPageBaseHeader {
        &self.base
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RecordPtr {
    pub(crate) page_id: PageId,
    pub(crate) slot: u16,
}

#[derive(Debug, Error)]
pub(crate) enum HeapFileError {
    #[error("cache error occured: {0}")]
    CacheError(#[from] CacheError),
    #[error("slotted page error occured: {0}")]
    SlottedPageError(#[from] SlottedPageError),
}

pub(crate) struct HeapFile {
    file_key: FileKey,
}
