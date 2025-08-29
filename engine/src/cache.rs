use std::{
    num::NonZero,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use dashmap::{DashMap, Entry};
use log::{error, warn};
use lru::LruCache;
use parking_lot::{MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use thiserror::Error;

use crate::{
    background_worker::{BackgroundWorker, BackgroundWorkerHandle},
    files_manager::{FileKey, FilesManager, FilesManagerError},
    paged_file::{Page, PageId, PagedFile, PagedFileError},
};

/// Structure for referring to single page in the file.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct FilePageRef {
    page_id: PageId,
    file_key: FileKey,
}

/// Wrapper around the [`Page`] and its metadata used for concurrent usage.
pub(crate) struct PageFrame {
    /// Page id and file identifier - unique per frame.
    file_page_ref: FilePageRef,
    /// Reader-writer lock around [`Page`].
    page: RwLock<Page>,
    /// Set to true if [`PageFrame::page`] was modified and needs to be flushed to disk.
    dirty: AtomicBool,
    /// Number of threads currently using this frame. Frame can only be dropped if its `pin_count` is 0.
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

    /// Decreases [`PageFrame::pin_count`] by one.
    fn unpin(&self) {
        self.pin_count.fetch_sub(1, Ordering::AcqRel);
    }

    /// Returns true if [`PageFrame`] is used by other threads.
    fn is_pinned(&self) -> bool {
        self.pin_count.load(Ordering::Acquire) > 0
    }
}

/// Wrapper around frame and its guard - shared or exclusive lock.
pub(crate) struct PinnedPage<G> {
    /// This field should not be exposed. It's here because [`PinnedPage::guard`] cannot outlive it.
    frame: Arc<PageFrame>,
    /// The content of the [`Page`] wrapped in a guard.
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

/// [`Page`] wrapped in shared lock.
pub(crate) type PinnedReadPage = PinnedPage<RwLockReadGuard<'static, Page>>;

impl PinnedReadPage {
    pub fn page(&self) -> &Page {
        &self.guard
    }
}

/// [`Page`] wrapped in exclusive lock.
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

/// Responsible for caching [`Page`]s and distributing it to other threads.
/// Threads that use [`Cache`] should not have to worry about multithreading problems - all of them should be handled by [`Cache`].
/// [`Cache`] must be used per-database, as it depends on [`FilesManager`] which works that way.
pub(crate) struct Cache<const N: usize> {
    /// List of the [`PageFrame`]s currently stored in cache. This is the source of truth from [`Cache`]'s point of view.
    ///
    /// It is guaranteed that:
    /// - any page that is used by at least one thread will not be removed from the [`Cache`].
    /// - any frame that is selected for eviction and is dirty will have its page flushed to disk.
    ///
    /// [`Cache`] tries to keep the size of it <= [`Cache::CACHE_CAPACITY`], but it is not always true.  
    /// Check [`Cache::get_pinned_frame`] and [`Cache::try_evict_frame`] for more details.
    frames: DashMap<FilePageRef, Arc<PageFrame>>,
    /// LRU list of the [`FilePageRef`]s used for deciding which [`PageFrame`] is best candidate for eviction (it does not mean it will always be picked as the victim - check [`Cache::try_evict_frame`] for details).
    /// It is guaranteed that `lru.keys()` are subset of `frames.keys()` - it means that there might be [`PageFrame`] that is
    /// stored in [`Cache::frames`] but not in [`Cache::lru`]. Such thing may happen if for some reason [`Cache::try_evict_frame`] failed.
    /// This is not a problem as there is a background thread ([`BackgroundCacheCleaner`]) that periodically cleans [`Cache`] from such frames.
    lru: Arc<RwLock<LruCache<FilePageRef, ()>>>,
    /// Pointer to [`FilesManager`], used for file operations when page must be loaded from/flushed to disk.
    files: Arc<FilesManager>,
}

impl<const N: usize> Cache<N> {
    const CACHE_CAPACITY: usize = N;

    /// Creates new [`Cache`] that handles frames for single database.
    pub(crate) fn new(files: Arc<FilesManager>) -> Arc<Self> {
        Arc::new(Self {
            frames: DashMap::with_capacity(Self::CACHE_CAPACITY),
            lru: Arc::new(RwLock::new(LruCache::new(
                NonZero::new(Self::CACHE_CAPACITY).unwrap(),
            ))),
            files,
        })
    }

    /// Creates new [`Cache`] that handles frames for single database and its [`BackgroundCacheCleaner`]'s handle.
    pub(crate) fn with_background_cleaner(
        files: Arc<FilesManager>,
        cleanup_interval: Duration,
    ) -> (Arc<Self>, BackgroundWorkerHandle) {
        let cache = Self::new(files);
        let cleaner = BackgroundCacheCleaner::start(BackgroundCacheCleanerParams {
            cache: cache.clone(),
            cleanup_interval,
        });
        (cache, cleaner)
    }

    /// Returns shared lock to the page. If page was not found in the cache it loads it from disk.
    pub(crate) fn pin_read(&self, id: &FilePageRef) -> Result<PinnedReadPage, CacheError> {
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

    /// Returns exclusive lock to the page. If page was not found in the cache it loads it from disk.
    pub(crate) fn pin_write(&self, id: &FilePageRef) -> Result<PinnedWritePage, CacheError> {
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

    /// Allocates new page in `file` and returns exclusive lock to that page.
    /// In case if lock is not needed the return value should not be assigned, so that lock lives as little as needed.
    pub(crate) fn allocate_page(&self, file: &FileKey) -> Result<PinnedWritePage, CacheError> {
        let pf = self.files.get_or_open_new_file(file)?;
        let page_id = pf.lock().allocate_page()?;
        let id = FilePageRef {
            file_key: file.clone(),
            page_id,
        };
        self.pin_write(&id)
    }

    /// Remove page from file. If there is a lock on the frame this will block
    /// until it gets the exclusive lock on the frame. Any changes made to the page
    /// after calling this function will not be flushed to the disk.
    pub(crate) fn free_page(&self, id: &FilePageRef) -> Result<(), CacheError> {
        match self.frames.entry(id.clone()) {
            Entry::Occupied(occupied_entry) => {
                // We hold exclusive lock on the key and remove it from the dashmap,
                // so no other thread can get this key.
                // Additionaly, we get exclusive lock on the page in the frame to wait until all other threads stop working with the frame.
                let frame = occupied_entry.remove();
                let w = frame.write();
                // We can drop it right away as now we are sure no other thread can access it now. We do not need to flush it first, as it will be discarded anyway (the page will be freed).
                drop(w);
                self.lru.write().pop_entry(id);

                let pf = self.files.get_or_open_new_file(&id.file_key)?;
                pf.lock().free_page(id.page_id)?;
                Ok(())
            }
            Entry::Vacant(_) => {
                // In this case page is not in the cache. We hold exclusive lock
                // on its key in the dashmap, so we can safely just free it
                // in underlying file.
                let pf = self.files.get_or_open_new_file(&id.file_key)?;
                pf.lock().free_page(id.page_id)?;
                Ok(())
            }
        }
    }

    /// Returns root page id of the file. Refer to [`PagedFile`] for more details.
    pub(crate) fn root_page_id(&self, file: &FileKey) -> Result<Option<PageId>, CacheError> {
        let pf = self.files.get_or_open_new_file(file)?;
        Ok(pf.lock().root_page_id())
    }

    /// Sets root page id of the file. Refer to [`PagedFile`] for more details.
    pub(crate) fn set_root_page_id(
        &self,
        file: &FileKey,
        page_id: PageId,
    ) -> Result<(), CacheError> {
        let pf = self.files.get_or_open_new_file(file)?;
        pf.lock().set_root_page_id(page_id)?;
        Ok(())
    }

    /// Returns [`Arc<PageFrame>`] and pinnes the underlying [`PageFrame`].
    /// It first looks for frame in [`Cache::frames`]. If it's found there then its key in [`Cache::lru`] is updated (making it MRU).
    /// Otherwise [`PageFrame`] is loaded from disk using [`FilesManager`] and frame's key is inserted into [`Cache::lru`].
    fn get_pinned_frame(&self, id: &FilePageRef) -> Result<Arc<PageFrame>, CacheError> {
        if let Some(frame) = self.frames.get(id) {
            frame.pin();
            self.push_to_lru(id);
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
                // We don't want to reinsert it, as we will lose the information about [`PageFrame::pin_count`]. We need to get the already existing entry and
                // update its pin count.
                let existing = occupied_entry.get().clone();
                existing.pin();
                self.push_to_lru(id);
                existing
            }
            Entry::Vacant(vacant_entry) => {
                // Not yet inserted.
                // Pin immediately so it's not evicted right after insertion.
                new_frame.pin();
                vacant_entry.insert(new_frame.clone());

                if self.frames.len() > Self::CACHE_CAPACITY && !self.try_evict_frame()? {
                    warn!(
                        "Cache: cannot evict frame - every frame in cache is pinned or lru is empty."
                    );
                }

                self.push_to_lru(id);
                new_frame
            }
        };
        Ok(frame)
    }

    /// Evicts the first frame (starting from LRU) that has [`PageFrame::pin_count`] equal to 0.
    /// If frame is selected to be evicted (using LRU), but its pin count is greater than 0, it will not be evicted and instead its key is updated in [`Cache::lru`] (making it MRU). In that case the next LRU is picked and so on.
    /// Returns `false` if could not evict any page - every page in cache is pinned or LRU is empty.
    fn try_evict_frame(&self) -> Result<bool, CacheError> {
        let max_attempts = Self::CACHE_CAPACITY;

        for _ in 0..max_attempts {
            let victim_id = {
                let mut lru_write = self.lru.write();
                if let Some((id, _)) = lru_write.pop_lru() {
                    id
                } else {
                    // LRU is empty
                    return Ok(false);
                }
            };

            let removed = self.remove_from_cache_if(&victim_id, |_, frame| !frame.is_pinned())?;
            if removed {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Removes frame with id `victim_id` if `predicate` evaluates to `true`. If frame is removed then underlying page if flushed to disk.
    /// Returns `Ok(true)` if frame was removed and `Ok(false)` otherwise.
    fn remove_from_cache_if<F>(
        &self,
        victim_id: &FilePageRef,
        predicate: F,
    ) -> Result<bool, CacheError>
    where
        F: FnOnce(&FilePageRef, &Arc<PageFrame>) -> bool,
    {
        // `remove_if` exclusively locks shard that contains the frame. This means that `predicate` can assume that while it's running
        // no other thread is capable of editing the frame
        // Check here: https://docs.rs/dashmap/6.1.0/src/dashmap/lib.rs.html#978-1000
        if let Some((_, frame)) = self
            .frames
            .remove_if(victim_id, |key, frame| predicate(key, frame))
        {
            // We can skip flushing if frame is not dirty
            if !frame.dirty.load(Ordering::Acquire) {
                return Ok(true);
            }
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
        Ok(false)
    }

    /// Flushes the frame to the disk.
    /// Should be called while holding exclusive lock on the shard in which frame's key was located.
    fn flush_frame(
        &self,
        frame: Arc<PageFrame>,
        mut file_lock: MutexGuard<'_, PagedFile>,
    ) -> Result<(), CacheError> {
        // We do not need to check if pin_count > 0 - at this point this frame was removed from dashmap and we hold the
        // exclusive lock on the shard in which frame's key was, so it cannot be increased. At the same time we hold the lock
        // to underlying file, meaning no other thread can create frame for the same page - we are safe to flush.

        if !frame.dirty.load(Ordering::Acquire) {
            // Other thread already flushed it.
            return Ok(());
        }

        let page = frame.read();
        file_lock.write_page(frame.file_page_ref.page_id, *page)?;
        frame.dirty.store(false, Ordering::Release);

        Ok(())
    }

    /// Iterates over all elements in [`Cache::frames`] and flush them to disk.
    /// It assumes that at this point no other thread will use [`Cache`].
    /// It does not return any error, it logs them instead. This way we don't
    /// stop on first failure and try to flush as many frames as we can.
    fn flush_all_frames(&self) {
        for frame in &self.frames {
            if !frame.dirty.load(Ordering::Acquire) {
                continue;
            }
            match self
                .files
                .get_or_open_new_file(&frame.file_page_ref.file_key)
            {
                Ok(pf) => {
                    let file_lock = pf.lock();
                    if let Err(e) = self.flush_frame(frame.clone(), file_lock) {
                        error!("failed to flush frame: {e}")
                    }
                }
                Err(e) => {
                    error!("failed to get PagedFile: {e}");
                    continue;
                }
            }
        }
    }

    /// Inserts `key` to [`Cache::lru`].
    /// Cannot be called when shared/exclusive lock to [`Cache::lru`] is held (in the same thread).
    fn push_to_lru(&self, key: &FilePageRef) {
        self.lru.write().push(key.clone(), ());
    }
}

impl<const N: usize> Drop for Cache<N> {
    fn drop(&mut self) {
        self.flush_all_frames();
    }
}

/// Responsible for periodically scanning [`Cache`] and removing [`PageFrame`]s from it that are in [`Cache::frames`] but not in [`Cache::lru`].
struct BackgroundCacheCleaner<const N: usize> {
    cache: Arc<Cache<N>>,
    cleanup_interval: Duration,
    shutdown: mpsc::Receiver<()>,
}

struct BackgroundCacheCleanerParams<const N: usize> {
    cache: Arc<Cache<N>>,
    cleanup_interval: Duration,
}

impl<const N: usize> BackgroundWorker for BackgroundCacheCleaner<N> {
    type BackgroundWorkerParams = BackgroundCacheCleanerParams<N>;

    fn start(params: Self::BackgroundWorkerParams) -> BackgroundWorkerHandle {
        let (tx, rx) = mpsc::channel();
        let cleaner = BackgroundCacheCleaner {
            cache: params.cache,
            cleanup_interval: params.cleanup_interval,
            shutdown: rx,
        };
        let handle = thread::spawn(move || {
            cleaner.run();
        });
        BackgroundWorkerHandle::new(handle, tx)
    }
}

impl<const N: usize> BackgroundCacheCleaner<N> {
    fn run(self) {
        loop {
            match self.shutdown.recv_timeout(self.cleanup_interval) {
                Ok(()) => {
                    // Got signal for shutdown.
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(e) = self.sync_frames_and_lru() {
                        error!("failed to sync frames and lru: {e}")
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Sender dropped - trying to shutdown anyway.
                    break;
                }
            }
        }
    }

    /// Iterates over all elements in [`Cache::frames`] and removes those that (at the same time) are not pinned and not in [`Cache::lru`].
    fn sync_frames_and_lru(&self) -> Result<(), CacheError> {
        // We need to clone the keys as we cannot hold the shared lock
        // to shard in the dashmap while trying to get exclusive lock
        // on the same shard.
        let keys_in_cache: Vec<_> = self
            .cache
            .frames
            .iter()
            .map(|frame| frame.key().clone())
            .collect();

        for key in keys_in_cache {
            self.cache.remove_from_cache_if(&key, |key, frame| {
                !frame.is_pinned() && !self.cache.lru.read().contains(key)
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    /// Creates new files manager pointing to temporary directory.
    fn create_files_manager() -> Arc<FilesManager> {
        let tmp = tempdir().unwrap();
        let db_dir = tmp.path().join("db");
        std::fs::create_dir_all(&db_dir).unwrap();

        let files = FilesManager::new(tmp.path(), "db").unwrap();
        Arc::new(files)
    }

    /// Spawns a thread that pins `id` from `cache`, reads the first 8 bytes and asserts it equals `expected`.
    fn spawn_check_page<const N: usize>(
        cache: Arc<Cache<N>>,
        id: FilePageRef,
        expected: u64,
    ) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            let pinned = cache.pin_read(&id).expect("pin_read failed");
            let data = pinned.page();
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&data[0..8]);
            drop(pinned);
            let v = u64::from_be_bytes(buf);
            assert_eq!(v, expected);
        })
    }

    /// Asserts that `id` is present frames map.
    fn assert_cached<const N: usize>(cache: &Arc<Cache<N>>, id: &FilePageRef) {
        assert!(
            cache.frames.contains_key(id),
            "expected frame present in frames for {id:?}",
        );
    }

    /// Asserts that `id` is present in both frames map and LRU.
    fn assert_cached_and_in_lru<const N: usize>(cache: &Arc<Cache<N>>, id: &FilePageRef) {
        assert_cached(cache, id);
        let lru_guard = cache.lru.read();
        assert!(
            lru_guard.contains(id),
            "expected key present in LRU for {id:?}"
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

        let cache = Cache::<1>::new(files.clone());

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

        let cache = Cache::<2>::new(files.clone());

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

        let cache = Cache::<3>::new(files.clone());

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

        let cache = Cache::<3>::new(files.clone());

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

    #[test]
    fn cache_concurrent_readers_same_page() {
        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xdeadbeefu64);

        let cache = Cache::<2>::new(files.clone());

        let id = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let readers = 8;
        let start = Arc::new(Barrier::new(readers + 1));
        let mut handles = Vec::with_capacity(readers);

        for _ in 0..readers {
            let cache_cloned = cache.clone();
            let id_cloned = id.clone();
            let start_cloned = start.clone();

            handles.push(thread::spawn(move || {
                // wait for all threads + main to reach this point to make pin_read run concurrently
                start_cloned.wait();

                // pin for read (should take shared lock)
                let pinned = cache_cloned.pin_read(&id_cloned).expect("pin_read failed");

                let data = pinned.page();
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&data[0..8]);
                let v = u64::from_be_bytes(buf);
                assert_eq!(v, 0xdeadbeefu64);

                // hold the pinned read for a short while to ensure overlap
                thread::sleep(Duration::from_millis(100));

                drop(pinned);
            }));
        }

        // release threads to start pin_read at (almost) the same time
        start.wait();

        for h in handles {
            h.join().unwrap();
        }

        assert_all_frames_unpinned(&cache);
        assert_cached_and_in_lru(&cache, &id);
        assert_eq!(cache.frames.len(), 1);
        assert_eq!(cache.lru.read().len(), 1);
    }

    #[test]
    fn cache_over_capacity_when_pinned_with_concurrent_loaders() {
        use std::sync::Barrier;

        let files = create_files_manager();

        let file_key = FileKey::data("table1");
        let id1 = alloc_page_with_u64(&files, &file_key, 101);
        let id2 = alloc_page_with_u64(&files, &file_key, 102);
        let id3 = alloc_page_with_u64(&files, &file_key, 103);

        let cache = Cache::<1>::new(files.clone());

        let fp1 = FilePageRef {
            page_id: id1,
            file_key: file_key.clone(),
        };
        let fp2 = FilePageRef {
            page_id: id2,
            file_key: file_key.clone(),
        };
        let fp3 = FilePageRef {
            page_id: id3,
            file_key: file_key.clone(),
        };

        // Barriers to coordinate: start all 3 threads approximately together, and then wait
        // until both loaders finished before releasing the holder.
        let start = Arc::new(Barrier::new(3));
        let done = Arc::new(Barrier::new(3));

        // Holder thread: pins fp1 and holds until loaders finish.
        let cache_h = cache.clone();
        let start_h = start.clone();
        let done_h = done.clone();
        let fp1_clone = fp1.clone();
        let holder = thread::spawn(move || {
            let pinned = cache_h.pin_read(&fp1_clone).expect("holder pin failed");
            // ensure loaders will attempt to load while we hold the pin
            start_h.wait();
            // wait until loaders signal they're done loading
            done_h.wait();
            drop(pinned);
        });

        // Loader 1
        let cache_l1 = cache.clone();
        let start_l1 = start.clone();
        let done_l1 = done.clone();
        let fp2_clone = fp2.clone();
        let loader1 = thread::spawn(move || {
            start_l1.wait();
            // this should insert a new frame while fp1 is still pinned
            let pinned = cache_l1.pin_read(&fp2_clone).expect("loader1 pin failed");
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&pinned.page()[0..8]);
            assert_eq!(u64::from_be_bytes(buf), 102u64);
            done_l1.wait();
            drop(pinned);
        });

        // Loader 2
        let cache_l2 = cache.clone();
        let start_l2 = start.clone();
        let done_l2 = done.clone();
        let fp3_clone = fp3.clone();
        let loader2 = thread::spawn(move || {
            start_l2.wait();
            // this should also insert a new frame while fp1 is still pinned
            let pinned = cache_l2.pin_read(&fp3_clone).expect("loader2 pin failed");
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&pinned.page()[0..8]);
            assert_eq!(u64::from_be_bytes(buf), 103u64);
            done_l2.wait();
            drop(pinned);
        });

        // join all threads
        holder.join().unwrap();
        loader1.join().unwrap();
        loader2.join().unwrap();

        // After all finished: frames should contain all three entries (no permanent removal was possible while a frame was pinned).
        assert_eq!(cache.frames.len(), 3);

        // LRU has capacity 1, so only one key should be present (from one of the loaders).
        let lru_guard = cache.lru.read();
        assert_eq!(lru_guard.len(), 1);
        let contains_fp1 = lru_guard.contains(&fp1);
        assert!(
            !contains_fp1,
            "LRU should not contain originally pinned entry fp1"
        );

        assert_all_frames_unpinned(&cache);
        assert_cached(&cache, &fp1);
        assert_cached(&cache, &fp2);
        assert_cached(&cache, &fp3);
    }

    #[test]
    fn cache_eviction_flush_persists() {
        let files = create_files_manager();
        let file_key = FileKey::data("table1");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);
        let cache = Cache::<1>::new(files.clone());
        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        {
            let mut w = cache.pin_write(&id.clone()).expect("pin_write");
            w.page_mut()[0..8].copy_from_slice(&0xBEEFu64.to_be_bytes());
            // drop -> dirty = true, unpinned
        }

        // allocate another page so insertion will trigger eviction of id (capacity=1)
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2222);
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };
        let _p = cache.pin_read(&id2).expect("pin_read");

        // now underlying file should contain 0xBEEF at pid (evicted & flushed)
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let mut pf_lock = pf.lock();
        let page = pf_lock.read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xBEEFu64);
    }

    #[test]
    fn cache_concurrent_writers_same_page() {
        let files = create_files_manager();
        let fk = FileKey::data("table1");
        let pid = alloc_page_with_u64(&files, &fk, 0);
        let id = FilePageRef {
            page_id: pid,
            file_key: fk.clone(),
        };
        let cache = Cache::<1>::new(files.clone());

        let writers = 4;
        let start = Arc::new(Barrier::new(writers + 1));
        let mut handles = Vec::new();
        for i in 0..writers {
            let c = cache.clone();
            let idc = id.clone();
            let s = start.clone();
            handles.push(thread::spawn(move || {
                s.wait();
                let mut w = c.pin_write(&idc).expect("pin_write");
                w.page_mut()[0..8].copy_from_slice(&(1000 + i as u64).to_be_bytes());
                // hold write briefly to increase chance of overlap
                thread::sleep(std::time::Duration::from_millis(10));
            }));
        }

        start.wait();
        for h in handles {
            h.join().unwrap();
        }

        // read final persisted value (force eviction to trigger flush)
        let pid2 = alloc_page_with_u64(&files, &fk, 42);
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: fk.clone(),
        };
        let r = cache.pin_read(&id2).expect("pin_read to trigger eviction");
        drop(r);

        // check last value on disk via fresh file handle
        let pf = files.get_or_open_new_file(&fk).unwrap();
        let page = pf.lock().read_page(pid).unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        let val = u64::from_be_bytes(buf);
        // must be one of written values
        assert!(val >= 1000 && val < 1000 + writers as u64);
    }

    #[test]
    fn cache_allocate_page_via_cache() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_alloc");

        let cache = Cache::<1>::new(files.clone());

        let mut pinned = cache
            .allocate_page(&file_key)
            .expect("allocate_page failed");

        pinned.page_mut()[0..8].copy_from_slice(&0xFEEDu64.to_be_bytes());

        let allocated_ref = pinned.frame.file_page_ref.clone();

        // drop the write guard (unpin) so the page remains in cache but is free
        drop(pinned);

        // read it back via cache to ensure the in-memory page contains our value
        let pinned_read = cache.pin_read(&allocated_ref).expect("pin_read failed");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&pinned_read.page()[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xFEEDu64);
        drop(pinned_read);

        assert_cached_and_in_lru(&cache, &allocated_ref);
        assert_all_frames_unpinned(&cache);
        assert_eq!(cache.frames.len(), 1);
        assert_eq!(cache.lru.read().len(), 1);
    }

    #[test]
    fn cache_free_page_normal() {
        let files = create_files_manager();
        let fk = FileKey::data("table_free");
        let pid = alloc_page_with_u64(&files, &fk, 0xAA);
        let id = FilePageRef {
            page_id: pid,
            file_key: fk.clone(),
        };

        let cache = Cache::<1>::new(files.clone());

        {
            let h = spawn_check_page(cache.clone(), id.clone(), 0xAA);
            h.join().unwrap();
        }

        cache
            .free_page(&id)
            .expect("free_page failed in normal case");

        assert!(cache.frames.is_empty());
        assert!(cache.lru.read().is_empty());
    }

    #[test]
    fn cache_free_page_blocks_until_reader_released() {
        let files = create_files_manager();
        let fk = FileKey::data("table_free_reader");
        let pid = alloc_page_with_u64(&files, &fk, 0xBB);
        let id = FilePageRef {
            page_id: pid,
            file_key: fk.clone(),
        };

        let cache = Cache::<1>::new(files.clone());

        // pin for read and hold it
        let pinned = cache.pin_read(&id).expect("pin_read failed (holder)");

        // spawn free_page which should block until we drop `pinned`
        let cache_cl = cache.clone();
        let id_cl = id.clone();
        let handle = thread::spawn(move || {
            // this call should block until the reader drops its guard
            cache_cl
                .free_page(&id_cl)
                .expect("free_page blocked and failed");
        });

        // give free_page a moment to reach the blocking write-lock call
        thread::sleep(Duration::from_millis(50));

        drop(pinned);

        handle.join().expect("free_page thread panicked");

        assert!(cache.frames.is_empty());
        assert!(cache.lru.read().is_empty());
    }

    #[test]
    fn cache_free_page_blocks_until_writer_released() {
        let files = create_files_manager();
        let fk = FileKey::data("table_free_writer");
        let pid = alloc_page_with_u64(&files, &fk, 0xCC);
        let id = FilePageRef {
            page_id: pid,
            file_key: fk.clone(),
        };

        let cache = Cache::<1>::new(files.clone());

        // pin for write and hold it
        let mut pinned_w = cache.pin_write(&id).expect("pin_write failed (holder)");
        pinned_w.page_mut()[0..8].copy_from_slice(&0xDEADu64.to_be_bytes());

        // spawn free_page which should block until we drop `pinned_w`
        let cache_cl = cache.clone();
        let id_cl = id.clone();
        let handle = std::thread::spawn(move || {
            cache_cl
                .free_page(&id_cl)
                .expect("free_page blocked and failed");
        });

        // let free_page attempt to block
        thread::sleep(Duration::from_millis(50));

        drop(pinned_w);

        handle.join().expect("free_page thread panicked");

        assert!(cache.frames.is_empty());
        assert!(cache.lru.read().is_empty());
    }

    #[test]
    fn cache_lru_skips_pinned_and_evicts_next() {
        let files = create_files_manager();
        let file_key = FileKey::data("table1");

        let pid1 = alloc_page_with_u64(&files, &file_key, 1);
        let pid2 = alloc_page_with_u64(&files, &file_key, 2);
        let pid3 = alloc_page_with_u64(&files, &file_key, 3);

        let cache = Cache::<2>::new(files.clone());

        let fp1 = FilePageRef {
            page_id: pid1,
            file_key: file_key.clone(),
        };
        let fp2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };
        let fp3 = FilePageRef {
            page_id: pid3,
            file_key: file_key.clone(),
        };

        // load fp1 and hold the read pin (so it's pinned)
        let pinned1 = cache.pin_read(&fp1).expect("pin_read fp1 failed");

        // load fp2 and drop immediately (unpins)
        {
            let p2 = cache.pin_read(&fp2).expect("pin_read fp2 failed");
            drop(p2);
        }

        // now load fp3 which should trigger eviction.
        // eviction must not remove pinned fp1, so fp2 should be evicted.
        {
            let p3 = cache.pin_read(&fp3).expect("pin_read fp3 failed");
            drop(p3);
        }

        // verify frames contain fp1 and fp3, and fp2 was evicted
        assert_eq!(cache.frames.len(), 2);
        assert_cached(&cache, &fp1);
        assert_cached(&cache, &fp3);

        drop(pinned1);
        assert_all_frames_unpinned(&cache);
    }

    #[test]
    fn background_cache_cleaner_removes_frames_not_in_lru() {
        let files = create_files_manager();
        let file_key = FileKey::data("bg_clean_table");
        let pid = alloc_page_with_u64(&files, &file_key, 0xAA55);
        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // start cache with a short cleanup interval
        let (cache, mut cleaner) =
            Cache::<2>::with_background_cleaner(files.clone(), Duration::from_millis(50));

        // create a frame and insert it directly into frames map WITHOUT adding to LRU
        let page: Page = [0u8; 4096];
        let frame = Arc::new(PageFrame::new(id.clone(), page));

        frame.pin();

        cache.frames.insert(id.clone(), frame.clone());

        // present in frames but not in lru
        assert!(cache.frames.contains_key(&id));
        assert!(!cache.lru.read().contains(&id));

        frame.unpin();

        // wait for cleaner to run a few times
        thread::sleep(Duration::from_millis(250));

        // cleaner should remove the orphan frame
        assert!(!cache.frames.contains_key(&id));

        cleaner.shutdown().unwrap();
        cleaner.join().unwrap();
    }

    #[test]
    fn background_cache_cleaner_respects_pinned_frames_until_unpinned() {
        let files = create_files_manager();
        let file_key = FileKey::data("bg_clean_table_pinned");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1234);
        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        let (cache, mut cleaner) =
            Cache::<2>::with_background_cleaner(files.clone(), Duration::from_millis(50));

        let page: Page = [0u8; 4096];
        let frame = Arc::new(PageFrame::new(id.clone(), page));

        // pin the frame so cleaner must not remove it
        frame.pin();
        cache.frames.insert(id.clone(), frame.clone());
        assert!(cache.frames.contains_key(&id));
        assert!(!cache.lru.read().contains(&id));

        // wait for cleaner - it should NOT remove pinned frame
        thread::sleep(Duration::from_millis(200));
        assert!(cache.frames.contains_key(&id));

        // unpin - now cleaner should remove it
        frame.unpin();
        thread::sleep(Duration::from_millis(200));
        assert!(!cache.frames.contains_key(&id));

        cleaner.shutdown().unwrap();
        cleaner.join().unwrap();
    }

    #[test]
    fn background_cache_cleaner_preserves_lru_frames() {
        let files = create_files_manager();
        let file_key = FileKey::data("bg_clean_preserve");
        let pid1 = alloc_page_with_u64(&files, &file_key, 0x1);
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2);
        let pid3 = alloc_page_with_u64(&files, &file_key, 0x3);

        let id1 = FilePageRef {
            page_id: pid1,
            file_key: file_key.clone(),
        };
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };
        let id3 = FilePageRef {
            page_id: pid3,
            file_key: file_key.clone(),
        };

        let (cache, mut cleaner) =
            Cache::<3>::with_background_cleaner(files.clone(), Duration::from_millis(50));

        let page: Page = [0u8; 4096];

        // Insert frame1 and put it into LRU (should be preserved)
        let f1 = Arc::new(PageFrame::new(id1.clone(), page));
        cache.frames.insert(id1.clone(), f1);
        cache.push_to_lru(&id1);

        // Insert frame2 but DO NOT put into LRU (should be removed)
        let f2 = Arc::new(PageFrame::new(id2.clone(), page));
        cache.frames.insert(id2.clone(), f2);

        // Insert frame3 and put into LRU (should be preserved)
        let f3 = Arc::new(PageFrame::new(id3.clone(), page));
        cache.frames.insert(id3.clone(), f3);
        cache.push_to_lru(&id3);

        assert!(cache.frames.contains_key(&id1));
        assert!(cache.frames.contains_key(&id2));
        assert!(cache.frames.contains_key(&id3));
        assert!(cache.lru.read().contains(&id1));
        assert!(cache.lru.read().contains(&id3));
        assert!(!cache.lru.read().contains(&id2));

        // wait for cleaner to run
        thread::sleep(Duration::from_millis(300));

        // frames in LRU should remain, orphan not in LRU should be removed
        assert!(cache.frames.contains_key(&id1));
        assert!(!cache.frames.contains_key(&id2));
        assert!(cache.frames.contains_key(&id3));

        cleaner.shutdown().unwrap();
        cleaner.join().unwrap();
    }

    #[test]
    fn cache_drop_flushes_dirty_frames_to_disk() {
        let files = create_files_manager();
        let file_key = FileKey::data("drop_flush_table");
        let pid = alloc_page_with_u64(&files, &file_key, 0xAAAAu64);

        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        let cache = Cache::<1>::new(files.clone());

        {
            let mut w = cache.pin_write(&id).expect("pin_write failed");
            w.page_mut()[0..8].copy_from_slice(&0xDEADu64.to_be_bytes());
        }

        drop(cache);

        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xDEADu64);
    }
}
