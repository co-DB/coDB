use std::{
    num::NonZero,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use dashmap::{DashMap, Entry};
use log::warn;
use lru::LruCache;
use parking_lot::{MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use thiserror::Error;

use crate::{
    files_manager::{FileKey, FilesManager, FilesManagerError},
    paged_file::{Page, PageId, PagedFile, PagedFileError},
};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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
        // SAFETY: `guard` cannot outlive `frame`. This is why it must be declared later in the struct,
        // as the order of dropping is the reverse order of declaration in struct.
        // We do not need to do anything manually, but remember not to change the order of the fields in [`PinnedPage`].
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

        // This entry() locks exclusively the slot in `frames`, so when we are inside this match statement we are sure that no other thread will modify it.
        // Check here: https://docs.rs/dashmap/6.1.0/src/dashmap/lib.rs.html#1185-1204
        let frame = match self.frames.entry(id.clone()) {
            Entry::Occupied(occupied_entry) => {
                // Already inserted by other thread.
                // We don't want to reinsert it, as we will lose the information about [`PageFrame::pin_count`]. We need to get the alreaxy existing entry and
                // update its pin count.
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

                if self.frames.len() > Self::CACHE_CAPACITY
                    && !self.try_evict_frame()? {
                        warn!("Cache: cannot evict frame - every frame in cache is pinned.");
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
    fn try_evict_frame(&self) -> Result<bool, CacheError> {
        let max_attemps = Self::CACHE_CAPACITY;

        for _ in 0..max_attemps {
            let victim_id = {
                let mut lru_write = self.lru.write();
                if let Some((id, _)) = lru_write.pop_lru() {
                    id
                } else {
                    // LRU is empty
                    return Ok(false);
                }
            };

            // This `remove_if` locks execusively element in map, so when we are inside `remove_if` closure we are sure that no other thread will be able to get this frame.
            // Check here: https://docs.rs/dashmap/6.1.0/src/dashmap/lib.rs.html#978-1000
            if let Some((_, frame)) = self
                .frames
                .remove_if(&victim_id, |_, frame| !frame.is_pinned())
            {
                // We lock the file here so that we are sure that no other thread will access this file while we are flushing it.
                // Other thread will not be able to access it, as we have a exclusive lock on shard that holds this element in dashmap.
                // When getting page from file (look at [`Cache::get_pinned_frame`]), the order is:
                // - get exclusive lock on the shard
                // - lock the file
                // If we have a exclusive lock on the shard we know other thread cannot have it, thus we are the only thread that can access this page via [`PagedFile`].
                let pf = self
                    .files
                    .get_or_open_new_file(&frame.file_page_ref.file_key)?;
                let file_lock = pf.lock();
                self.flush_frame(frame, file_lock)?;
                return Ok(true);
            };
        }

        Ok(false)
    }

    /// Flushes the frame to the disk.
    fn flush_frame(
        &self,
        frame: Arc<PageFrame>,
        mut file_lock: MutexGuard<'_, PagedFile>,
    ) -> Result<(), CacheError> {
        if !frame.dirty.load(Ordering::Acquire) {
            // Other thread already flushed it.
            return Ok(());
        }

        let page = frame.read();
        file_lock.write_page(frame.file_page_ref.page_id, *page)?;
        frame.dirty.store(false, Ordering::Release);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::tempdir;

    /// Creates new files manager pointing to temporary directory.
    fn create_files_manager() -> Arc<FilesManager> {
        let tmp = tempdir().unwrap();
        let db_dir = tmp.path().join("db");
        std::fs::create_dir_all(&db_dir).unwrap();

        let files = FilesManager::new(tmp.path(), "db").unwrap();
        let files = Arc::new(files);
        files
    }

    /// Spawns a thread that pins `id` from `cache`, reads the first 8 bytes and asserts it equals `expected`.
    fn spawn_check_page<const N: usize>(
        cache: Arc<Cache<N>>,
        id: FilePageRef,
        expected: u64,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let pinned = cache.pin_read(id).expect("pin_read failed");
            let data = pinned.page();
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[0..8]);
            drop(pinned);
            let v = u64::from_be_bytes(buf);
            assert_eq!(v, expected);
        })
    }

    /// Asserts that `id` is present in both frames map and LRU.
    fn assert_cached_and_in_lru<const N: usize>(cache: &Arc<Cache<N>>, id: &FilePageRef) {
        assert!(
            cache.frames.contains_key(id),
            "expected frame present in frames for {:?}",
            id
        );
        let lru_guard = cache.lru.read();
        assert!(
            lru_guard.contains(id),
            "expected key present in LRU for {:?}",
            id
        );
    }

    /// Assert that every frame in the cache is currently unpinned (pin_count == 0).
    fn assert_all_frames_unpinned<const N: usize>(cache: &Arc<Cache<N>>) {
        for entry in cache.frames.iter() {
            let frame = entry.value();
            assert!(
                !frame.is_pinned(),
                "expected frame {:?} to be unpinned, but it was pinned",
                frame.file_page_ref
            );
        }
    }

    /// Allocate a new page in `file_key`, write `val` (big-endian u64) into the first 8 bytes,
    /// flush it and return the new PageId.
    fn alloc_page_with_u64(files: &Arc<FilesManager>, file_key: &FileKey, val: u64) -> PageId {
        let pf_arc = files.get_or_open_new_file(file_key).unwrap();
        let mut pf = pf_arc.lock();
        let id = pf.allocate_page().unwrap();
        let mut page: Page = [0u8; 4096];
        page[0..8].copy_from_slice(&val.to_be_bytes());
        pf.write_page(id, page).unwrap();
        pf.flush().unwrap();
        id
    }

    #[test]
    fn cache_load_single_page() {
        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let page_id = alloc_page_with_u64(&files, &file_key, 7);

        let cache = Arc::new(Cache::<1>::new(files.clone()));

        let id = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };
        let handle = spawn_check_page(cache.clone(), id.clone(), 7);

        handle.join().unwrap();

        assert_cached_and_in_lru(&cache, &id);
        assert_all_frames_unpinned(&cache);
    }

    #[test]
    fn cache_load_single_page_multiple_times() {
        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let page_id = alloc_page_with_u64(&files, &file_key, 7);

        let cache = Arc::new(Cache::<2>::new(files.clone()));

        let id = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };
        let handle = spawn_check_page(cache.clone(), id.clone(), 7);
        handle.join().unwrap();
        let handle = spawn_check_page(cache.clone(), id.clone(), 7);
        handle.join().unwrap();
        let handle = spawn_check_page(cache.clone(), id.clone(), 7);
        handle.join().unwrap();
        let handle = spawn_check_page(cache.clone(), id.clone(), 7);
        handle.join().unwrap();

        assert_cached_and_in_lru(&cache, &id);
        assert_eq!(cache.frames.len(), 1);
        assert_eq!(cache.lru.read().len(), 1);

        assert_all_frames_unpinned(&cache);
    }

    #[test]
    fn cache_load_many_pages_by_single_thread_one_at_the_time() {
        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let page_id1 = alloc_page_with_u64(&files, &file_key, 1);
        let page_id2 = alloc_page_with_u64(&files, &file_key, 2);
        let page_id3 = alloc_page_with_u64(&files, &file_key, 3);

        let cache = Arc::new(Cache::<3>::new(files.clone()));

        let id1 = FilePageRef {
            page_id: page_id1,
            file_key: file_key.clone(),
        };
        let id2 = FilePageRef {
            page_id: page_id2,
            file_key: file_key.clone(),
        };
        let id3 = FilePageRef {
            page_id: page_id3,
            file_key: file_key.clone(),
        };
        let handle = spawn_check_page(cache.clone(), id1.clone(), 1);
        handle.join().unwrap();
        let handle = spawn_check_page(cache.clone(), id2.clone(), 2);
        handle.join().unwrap();
        let handle = spawn_check_page(cache.clone(), id3.clone(), 3);
        handle.join().unwrap();

        assert_cached_and_in_lru(&cache, &id1);
        assert_cached_and_in_lru(&cache, &id2);
        assert_cached_and_in_lru(&cache, &id3);

        assert_eq!(cache.frames.len(), 3);
        assert_eq!(cache.lru.read().len(), 3);

        assert_eq!(cache.lru.write().pop_lru().unwrap().0, id1);
        assert_eq!(cache.lru.write().pop_lru().unwrap().0, id2);
        assert_eq!(cache.lru.write().pop_lru().unwrap().0, id3);

        assert_all_frames_unpinned(&cache);
    }

    #[test]
    fn cache_load_many_pages_at_the_same_time() {
        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let page_id1 = alloc_page_with_u64(&files, &file_key, 1);
        let page_id2 = alloc_page_with_u64(&files, &file_key, 2);
        let page_id3 = alloc_page_with_u64(&files, &file_key, 3);

        let cache = Arc::new(Cache::<3>::new(files.clone()));

        let id1 = FilePageRef {
            page_id: page_id1,
            file_key: file_key.clone(),
        };
        let id2 = FilePageRef {
            page_id: page_id2,
            file_key: file_key.clone(),
        };
        let id3 = FilePageRef {
            page_id: page_id3,
            file_key: file_key.clone(),
        };
        let handle1 = spawn_check_page(cache.clone(), id1.clone(), 1);
        let handle2 = spawn_check_page(cache.clone(), id2.clone(), 2);
        let handle3 = spawn_check_page(cache.clone(), id3.clone(), 3);

        handle1.join().unwrap();
        handle2.join().unwrap();
        handle3.join().unwrap();

        assert_cached_and_in_lru(&cache, &id1);
        assert_cached_and_in_lru(&cache, &id2);
        assert_cached_and_in_lru(&cache, &id3);

        assert_eq!(cache.frames.len(), 3);
        assert_eq!(cache.lru.read().len(), 3);

        assert_all_frames_unpinned(&cache);
    }
}
