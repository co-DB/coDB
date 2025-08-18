use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::paged_file::{Page, PageId, PagedFile};

pub struct PageFrame {
    pub id: PageId, // it may not be needed
    page: RwLock<Page>,
    dirty: AtomicBool,
    pin_count: AtomicUsize,
}

impl PageFrame {
    pub fn new(id: PageId, initial: Page, dirty: bool) -> Self {
        Self {
            id,
            page: RwLock::new(initial),
            dirty: AtomicBool::new(dirty),
            pin_count: AtomicUsize::new(0),
        }
    }

    /// Acquire shared read guard
    fn read(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read()
    }

    // honestly i have no cluse how this ordering works, i think currently its the most restricted option, will need to read about it and decide which
    // variant fits our needs

    /// Acquire exclusive write guard
    fn write(&self) -> RwLockWriteGuard<'_, Page> {
        self.dirty.fetch_or(true, Ordering::AcqRel);
        self.page.write()
    }

    fn pin(&self) {
        self.pin_count.fetch_add(1, Ordering::AcqRel);
    }
    fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::AcqRel);
    }
    fn pinned_count(&self) -> usize {
        self.pin_count.load(Ordering::Acquire)
    }
}

// Both are wrappers around `PageFrame`. we need it so that we can ensure that pin is successfuly updated when this is created/dropped

pub struct PinnedReadPage {
    frame: Arc<PageFrame>,
    guard: RwLockReadGuard<'static, Page>,
}

impl PinnedReadPage {
    pub fn page(&self) -> &Page {
        &self.guard
    }
}

impl Drop for PinnedReadPage {
    fn drop(&mut self) {
        // SAFETY: guard is dropped first
        self.frame.unpin();
    }
}

pub struct PinnedWritePage {
    frame: Arc<PageFrame>,
    guard: RwLockWriteGuard<'static, Page>,
}

impl PinnedWritePage {
    pub fn page(&self) -> &Page {
        &self.guard
    }

    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.guard
    }
}

impl Drop for PinnedWritePage {
    fn drop(&mut self) {
        // SAFETY: guard is dropped first
        self.frame.unpin();
    }
}

/// This holds paged_file (under mutex) + cache for this paged file (list of )
/// I guess we must have this mutext (i mean mutex on file, we do not want more than one thread to write to file)
/// Most read/write will just communicate with cache, so this mutex shouldnt be too painful (need to properly test it, its my wishful thinking)
/// this DashMap is like HashMap but thread-safe, cannot really think of better option for this (there is no way we will implement our own solution that is faster)
pub struct CachedPagedFile {
    cache: DashMap<PageId, Arc<PageFrame>>,
    paged_file: Arc<Mutex<PagedFile>>,
}

impl CachedPagedFile {
    pub fn new(paged_file: PagedFile) -> Self {
        Self {
            cache: DashMap::new(),
            paged_file: Arc::new(Mutex::new(paged_file)),
        }
    }

    fn get_or_load_frame(&self, id: PageId) -> Arc<PageFrame> {
        if let Some(frame) = self.cache.get(&id) {
            return frame.clone();
        }

        let page = self.paged_file.lock().read_page(id).unwrap();
        let frame = Arc::new(PageFrame::new(id, page, false));
        // maybe other thread inserted it already
        let entry = self.cache.entry(id).or_insert(frame);
        entry.value().clone()
    }

    pub fn pin_read(&self, id: PageId) -> PinnedReadPage {
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

    pub fn pin_write(&self, id: PageId) -> PinnedWritePage {
        let frame = self.get_or_load_frame(id);
        frame.pin();

        let guard_local = frame.write();
        // SAFETY: same as above
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

pub fn showcase() {
    // I think the flow is something like:
    // we have a cache of caches :P i mean a global Vector of Arc<CachedPagedFile>
    // if it doesnt exist we open paged file, create Arc<CachedPagedFile> and add it to cache
    // then we can just grab this cachedPagedfile and do whatever we want

    let mut paged_file = PagedFile::new("/tmp/test-cache/paged_file.codb").unwrap();
    paged_file.allocate_page().unwrap();
    let reader = Arc::new(CachedPagedFile::new(paged_file));
    let writer = reader.clone();

    let r_thread = thread::spawn(move || {
        loop {
            let pinned_page = reader.pin_read(1);
            let mut b = [0u8; 8];
            b.copy_from_slice(&pinned_page.page()[0..8]);
            println!("[reader] read {b:?}");
            drop(pinned_page);
            thread::sleep(Duration::from_millis(300));
        }
    });
    let w_thread = thread::spawn(move || {
        for i in 1u8..=13 {
            let mut pinned_page = writer.pin_write(1);
            let b = [i; 8];
            pinned_page.page_mut()[0..8].copy_from_slice(&b);
            drop(pinned_page);
            println!("[writer] write {b:?}");
            thread::sleep(Duration::from_millis(400));
        }
    });
    r_thread.join().unwrap();
    w_thread.join().unwrap();
}
