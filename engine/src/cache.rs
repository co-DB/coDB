use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dashmap::DashMap;
use moka::sync::Cache as MokaCache;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    files_manager::FileKey,
    paged_file::{Page, PageId},
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
        // TODO: figure out if its right ordering and explain in comment why
        self.dirty.fetch_or(true, Ordering::Release);
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

    /// Returns current value of [`PageFrame::pin_count`]. Should be used for checking if [`PageFrame`] is used by other threads.
    fn pinned_count(&self) -> usize {
        self.pin_count.load(Ordering::Acquire)
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

pub(crate) struct Cache<const N: usize> {
    frames: DashMap<FilePageRef, Arc<PageFrame>>,
    lru: MokaCache<FilePageRef, ()>,
}

impl<const N: usize> Cache<N> {
    const CACHE_CAPACITY: usize = N;

    pub(crate) fn new() -> Self {
        Self {
            frames: DashMap::with_capacity(Self::CACHE_CAPACITY),
            lru: MokaCache::builder()
                .max_capacity(Self::CACHE_CAPACITY as _)
                .build(),
        }
    }

    pub(crate) fn pin_read(&self, id: FilePageRef) -> PinnedReadPage {
        let frame = self.get_pinned_frame(id);

        let guard_local = frame.read();
        // SAFETY: we transmute the guard's lifetime to 'static.
        // This is safe because `frame` (Arc<PageFrame>) is owned by the PinnedReadPage,
        // which ensures the underlying RwLock lives at least as long as the guard.
        let guard_static: RwLockReadGuard<'static, Page> = unsafe {
            std::mem::transmute::<RwLockReadGuard<'_, Page>, RwLockReadGuard<'static, Page>>(
                guard_local,
            )
        };
        PinnedReadPage {
            frame,
            guard: guard_static,
        }
    }

    pub(crate) fn pin_write(&self, id: FilePageRef) -> PinnedWritePage {
        let frame = self.get_pinned_frame(id);

        let guard_local = frame.write();
        // SAFETY: we transmute the guard's lifetime to 'static.
        // This is safe because `frame` (Arc<PageFrame>) is owned by the PinnedWritePage,
        // which ensures the underlying RwLock lives at least as long as the guard.
        let guard_static: RwLockWriteGuard<'static, Page> = unsafe {
            std::mem::transmute::<RwLockWriteGuard<'_, Page>, RwLockWriteGuard<'static, Page>>(
                guard_local,
            )
        };
        PinnedWritePage {
            frame,
            guard: guard_static,
        }
    }

    /// Returns [`Arc<PageFrame>`] and pinnes the underlying [`PageFrame`].
    /// It first looks for frame in [`Cache::frames`]. If it's found there then its key in [`Cache::lru`] is updated (making it MRU).
    /// Otherwise [`PageFrame`] is loaded from disk using [`FilesManager`] and frame's key is inserted into [`Cache::lru`].
    fn get_pinned_frame(&self, id: FilePageRef) -> Arc<PageFrame> {
        let frame = self.frames.get(&id);
        if let Some(frame) = frame {
            self.lru.insert(id, ());
            frame.pin();
            return frame.clone();
        }

        todo!("implement case when frame not found in cache")
    }

    /// Evicts the first frame (starting from LRU) that has [`PageFrame::pin_count`] equal to 0.
    /// If frame is selected to be evicted (using LRU), but its pin count is greater than 0, it will not be evicted and instead its key is updated in [`Cache::lru`] (making it MRU). In that case the next LRU is picked and so on.
    // TODO: figure out what to do if every page has pin_count > 0 and we are stuck
    fn evict_frame(&self) {
        // Eviction can be cancelled if current cache size is < half of the capacity.
        let target_size = Self::CACHE_CAPACITY / 2;

        for (inspected, (key, _)) in self.lru.iter().enumerate() {
            if self.frames.len() <= target_size {
                return;
            }
            if inspected >= Self::CACHE_CAPACITY {
                break;
            }

            let key = (*key).clone();

            match self.frames.remove(&key) {
                Some((_, frame)) => {
                    let safe_to_delete = frame.pinned_count() == 0;
                    match safe_to_delete {
                        true => {
                            if frame.dirty.load(Ordering::Acquire) {
                                self.flush_frame(frame);
                            }
                            self.lru.invalidate(&key);
                        }
                        false => {
                            // Other thread pinned it - put back to frames + make it MRU.
                            self.frames.insert(key.clone(), frame);
                            self.lru.insert(key.clone(), ());
                        }
                    }
                }
                None => {
                    // Already removed by other thread.
                    continue;
                }
            }
        }

        todo!("We couldn't evict any page - what to do now?")
    }

    /// Flushes the frame to the disk.
    fn flush_frame(&self, frame: Arc<PageFrame>) {
        if !frame.dirty.load(Ordering::Acquire) {
            // Other thread already flushed it.
            return;
        }

        todo!()
    }
}
