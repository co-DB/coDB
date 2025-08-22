use std::{
    num::NonZero,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use dashmap::{DashMap, Entry};
use lru::LruCache;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use thiserror::Error;

use crate::{
    files_manager::{FileKey, FilesManager, FilesManagerError},
    paged_file::{Page, PageId, PagedFileError},
};

#[derive(PartialEq, Eq, Hash, Clone)]
pub(crate) struct FilePageRef {
    page_id: PageId,
    file_key: FileKey,
}

pub(crate) struct PageFrame {
    file_page_ref: FilePageRef,
    page: RwLock<Page>,
    dirty: AtomicBool,
    pin_count: AtomicUsize,
}

impl PageFrame {
    /// Creates new [`PageFrame`].
    pub(crate) fn new(file_page_ref: FilePageRef, initial: Page) -> Self {
        Self {
            file_page_ref,
            page: RwLock::new(initial),
            dirty: AtomicBool::new(false),
            pin_count: AtomicUsize::new(0),
        }
    }

    /// Acquires shared read guard on [`PageFrame::page`].
    fn read(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read()
    }

    /// Acquires exclusive write guard on [`PageFrame::page`].
    fn write(&self) -> RwLockWriteGuard<'_, Page> {
        self.dirty.fetch_or(true, Ordering::AcqRel);
        self.page.write()
    }

    /// Increases [`PageFrame::pin_count`] by one.
    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Dencreases [`PageFrame::pin_count`] by one.
    fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::AcqRel);
    }

    /// Returns true if [`PageFrame`] is used by other threads.
    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }
}

pub(crate) struct PinnedPage<G> {
    frame: Arc<PageFrame>,
    guard: G,
}

impl<G> Drop for PinnedPage<G> {
    fn drop(&mut self) {
        // SAFETY: `guard` is dropped before `frame`, so we don't break anything
        self.frame.unpin();
    }
}

pub(crate) type PinnedReadPage = PinnedPage<RwLockReadGuard<'static, Page>>;

impl PinnedReadPage {
    pub fn page(&self) -> &Page {
        &self.guard
    }
}

pub(crate) type PinnedWritePage = PinnedPage<RwLockWriteGuard<'static, Page>>;

impl PinnedWritePage {
    pub fn page(&self) -> &Page {
        &self.guard
    }

    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.guard
    }
}

/// Error for cache related operations.
#[derive(Debug, Error)]
pub(crate) enum CacheError {
    #[error("failed to load file: {0}")]
    FilesManagerError(#[from] FilesManagerError),
    #[error("{0}")]
    PagedFileError(#[from] PagedFileError),
}

pub(crate) struct Cache<const N: usize> {
    frames: DashMap<FilePageRef, Arc<PageFrame>>,
    lru: Arc<RwLock<LruCache<FilePageRef, ()>>>,
    files: Arc<FilesManager>,
}

impl<const N: usize> Cache<N> {
    const CACHE_CAPACITY: usize = N;

    pub(crate) fn new(files: Arc<FilesManager>) -> Self {
        Self {
            frames: DashMap::with_capacity(Self::CACHE_CAPACITY),
            lru: Arc::new(RwLock::new(LruCache::new(
                NonZero::new(Self::CACHE_CAPACITY).unwrap(),
            ))),
            files,
        }
    }

    pub(crate) fn pin_read(&self, id: FilePageRef) -> Result<PinnedReadPage, CacheError> {
        let frame = self.get_pinned_frame(id)?;

        let guard_local = frame.read();
        // SAFETY: we transmute the guard's lifetime to 'static.
        // This is safe because `frame` (Arc<PageFrame>) is owned by the PinnedReadPage,
        // which ensures the underlying RwLock lives at least as long as the guard.
        let guard_static: RwLockReadGuard<'static, Page> = unsafe {
            std::mem::transmute::<RwLockReadGuard<'_, Page>, RwLockReadGuard<'static, Page>>(
                guard_local,
            )
        };
        Ok(PinnedReadPage {
            frame,
            guard: guard_static,
        })
    }

    pub(crate) fn pin_write(&self, id: FilePageRef) -> Result<PinnedWritePage, CacheError> {
        let frame = self.get_pinned_frame(id)?;

        let guard_local = frame.write();
        // SAFETY: we transmute the guard's lifetime to 'static.
        // This is safe because `frame` (Arc<PageFrame>) is owned by the PinnedWritePage,
        // which ensures the underlying RwLock lives at least as long as the guard.
        let guard_static: RwLockWriteGuard<'static, Page> = unsafe {
            std::mem::transmute::<RwLockWriteGuard<'_, Page>, RwLockWriteGuard<'static, Page>>(
                guard_local,
            )
        };
        Ok(PinnedWritePage {
            frame,
            guard: guard_static,
        })
    }

    /// Returns [`Arc<PageFrame>`] and pinnes the underlying [`PageFrame`].
    /// It first looks for frame in [`Cache::frames`]. If it's found there then its key in [`Cache::lru`] is updated (making it MRU).
    /// Otherwise [`PageFrame`] is loaded from disk using [`FilesManager`] and frame's key is inserted into [`Cache::lru`].
    fn get_pinned_frame(&self, id: FilePageRef) -> Result<Arc<PageFrame>, CacheError> {
        if let Some(frame) = self.frames.get(&id) {
            frame.pin();
            self.lru.write().push(id.clone(), ());
            let frame = frame.clone();
            return Ok(frame);
        }

        let pf = self.files.get_or_open_new_file(&id.file_key)?;
        let page = pf.lock().read_page(id.page_id)?;
        let new_frame = Arc::new(PageFrame::new(id.clone(), page));

        let frame = match self.frames.entry(id.clone()) {
            Entry::Occupied(occupied_entry) => {
                // Already inserted by other thread.
                let existing = occupied_entry.get().clone();
                existing.pin();
                self.lru.write().push(id.clone(), ());
                existing
            }
            Entry::Vacant(vacant_entry) => {
                // Not yet inserted.
                // Pin immiedietaly so it's not evicted right after insertion.
                new_frame.pin();
                vacant_entry.insert(new_frame.clone());

                if self.frames.len() > Self::CACHE_CAPACITY {
                    self.try_evict_frame();
                }

                self.lru.write().push(id.clone(), ());
                new_frame
            }
        };
        Ok(frame)
    }

    /// Evicts the first frame (starting from LRU) that has [`PageFrame::pin_count`] equal to 0.
    /// If frame is selected to be evicted (using LRU), but its pin count is greater than 0, it will not be evicted and instead its key is updated in [`Cache::lru`] (making it MRU). In that case the next LRU is picked and so on.
    /// Returns `false` if could not evict any page - every page in cache is pinned.
    fn try_evict_frame(&self) -> bool {
        let max_attemps = Self::CACHE_CAPACITY;

        for _ in 0..max_attemps {
            let victim_id = {
                let mut lru_write = self.lru.write();
                if let Some((id, _)) = lru_write.pop_lru() {
                    id
                } else {
                    // LRU is empty
                    return false;
                }
            };

            if let Some(frame_entry) = self.frames.get(&victim_id) {
                let frame = frame_entry.value().clone();
                if !frame.is_pinned() {
                    drop(frame_entry); // Release the reference before removal

                    if self.frames.remove(&victim_id).is_some() {
                        self.flush_frame(frame);
                        return true;
                    }
                } else {
                    // Frame is pinned, put it back as MRU
                    let _ = self.lru.write().push(victim_id, ());
                }
            }
        }

        false
    }

    /// Flushes the frame to the disk.
    fn flush_frame(&self, frame: Arc<PageFrame>) {
        if !frame.dirty.load(Ordering::Acquire) {
            // Other thread already flushed it.
            return;
        }

        // TODO: do we need to check if its pinned? and if it is then what?

        todo!()
    }
}
