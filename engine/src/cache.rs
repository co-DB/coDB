use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    files_manager::FileKey,
    paged_file::{Page, PageId},
};

#[derive(PartialEq, Eq, Hash)]
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

pub(crate) struct Cache {
    cache: DashMap<FilePageRef, Arc<PageFrame>>,
}

impl Cache {
    const CACHE_SIZE: usize = 1024;

    pub(crate) fn new() -> Self {
        Self {
            cache: DashMap::with_capacity(Self::CACHE_SIZE),
        }
    }

    fn get_or_load_frame(&self, id: FilePageRef) -> Arc<PageFrame> {
        todo!()
    }

    pub(crate) fn pin_read(&self, id: FilePageRef) -> PinnedReadPage {
        let frame = self.get_or_load_frame(id);
        frame.pin();

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
        let frame = self.get_or_load_frame(id);
        frame.pin();

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

    // if we want to remove something from cache we first need to check .pin_count() of this, cannot remove somehting that is pinned
}
