use std::{array, marker::PhantomData, sync::Arc};

use crossbeam::queue::SegQueue;
use dashmap::DashSet;
use parking_lot::Mutex;
use storage::{
    cache::{Cache, FilePageRef, PinnedWritePage},
    files_manager::FileKey,
    paged_file::{PAGE_SIZE, PageId},
};

use crate::{
    heap_file::{error::HeapFileError, pages::BaseHeapPageHeader},
    slotted_page::SlottedPage,
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
pub(super) struct FreeSpaceMap<const BUCKETS_COUNT: usize, H: BaseHeapPageHeader> {
    pub buckets: [SegQueue<PageId>; BUCKETS_COUNT],
    cache: Arc<Cache>,
    file_key: FileKey,
    /// [`PageId`] of next page that should be read by fsm in case of not finding suitable page in [`FreeSpaceMap::buckets`].
    next_page_to_read: Mutex<PageId>,
    /// Set of page IDs currently tracked by this FSM.
    /// Prevents duplicate entries in buckets.
    pub tracked_pages: DashSet<PageId>,
    _page_type_marker: PhantomData<H>,
}

type FsmPage<H> = SlottedPage<PinnedWritePage, H>;

impl<const BUCKETS_COUNT: usize, H: BaseHeapPageHeader> FreeSpaceMap<BUCKETS_COUNT, H> {
    /// Maximum number of candidate pages to check per bucket before giving up and trying the next bucket.
    const MAX_CANDIDATES_PER_BUCKET: usize = 32;

    /// Minimum free space that page should have to be added to FSM.
    /// We don't want to use pages with less free space, as most of the time they won't be use anyway.
    pub const MIN_USEFUL_FREE_SPACE: usize = 32;

    /// Maximum number of pages to load from disk before giving up and allocating new page.
    const MAX_DISK_PAGES_TO_CHECK: usize = 4;

    pub fn new(cache: Arc<Cache>, file_key: FileKey, first_page_id: PageId) -> Self {
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
    pub fn page_with_free_space(
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
    pub fn load_next_page(&self) -> Result<Option<(PageId, FsmPage<H>)>, HeapFileError> {
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
    pub fn update_page_bucket(&self, page_id: PageId, free_space: usize) {
        self.add_to_bucket(page_id, free_space);
    }

    /// Computes the bucket index for a given amount of free space (in bytes).
    ///
    /// The page free-space range `[0, PAGE_SIZE]` is divided into `BUCKETS_COUNT` equal
    /// intervals. This returns the index of the interval that contains `space`.
    /// The result is clamped to `[0, BUCKETS_COUNT - 1]`.
    pub fn bucket_for_space(&self, space: usize) -> usize {
        (space * BUCKETS_COUNT / PAGE_SIZE).clamp(0, BUCKETS_COUNT - 1)
    }

    /// Creates a `FilePageRef` for `page_id` using this map's file key.
    fn file_page_ref(&self, page_id: PageId) -> FilePageRef {
        FilePageRef::new(page_id, self.file_key.clone())
    }
}
