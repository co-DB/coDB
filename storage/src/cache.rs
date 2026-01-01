use crossbeam::channel;
use dashmap::{DashMap, Entry};
use log::{error, info, warn};
use lru::LruCache;
use parking_lot::{MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    mem,
    num::NonZero,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use thiserror::Error;

use crate::write_ahead_log::{SinglePageOperation, WalClient, WalHandle, WalRecordData};
use crate::{
    background_worker::{BackgroundWorker, BackgroundWorkerHandle},
    files_manager::{FileKey, FilesManager, FilesManagerError},
    page_diff::PageDiff,
    paged_file::{Lsn, Page, PageId, PagedFile, PagedFileError, get_page_lsn, set_page_lsn},
};
use types::serialization::DbSerializable;

/// Structure for referring to single page in the file.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct FilePageRef {
    pub(crate) page_id: PageId,
    pub(crate) file_key: FileKey,
}

impl FilePageRef {
    pub fn new(page_id: PageId, file_key: FileKey) -> Self {
        Self { page_id, file_key }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn file_key(&self) -> &FileKey {
        &self.file_key
    }
}

impl DbSerializable for FilePageRef {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.file_key.serialize(buffer);
        self.page_id.serialize(buffer);
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        let file_key_size = self.file_key.size_serialized();
        self.file_key.serialize_into(&mut buffer[0..file_key_size]);
        self.page_id.serialize_into(&mut buffer[file_key_size..]);
    }

    fn deserialize(
        data: &[u8],
    ) -> Result<(Self, &[u8]), types::serialization::DbSerializationError> {
        let (file_key, rest) = FileKey::deserialize(data)?;
        let (page_id, rest) = PageId::deserialize(rest)?;
        Ok((FilePageRef { page_id, file_key }, rest))
    }

    fn size_serialized(&self) -> usize {
        self.file_key.size_serialized() + self.page_id.size_serialized()
    }
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

/// Wrapper around frame and its guard (shared lock).
pub struct PinnedReadPage {
    /// The content of the [`Page`] wrapped in a guard.
    guard: RwLockReadGuard<'static, Page>,
    /// This field should not be exposed. It's here because `guard` cannot outlive it.
    frame: Arc<PageFrame>,
}

impl PinnedReadPage {
    pub fn page(&self) -> &Page {
        &self.guard
    }

    /// Gets the LSN of the page.
    pub fn lsn(&self) -> Lsn {
        get_page_lsn(self.page())
    }
}

impl Drop for PinnedReadPage {
    fn drop(&mut self) {
        // SAFETY: `guard` cannot outlive `frame`. This is why it must be declared before it in the struct,
        // as the order of dropping is the order of declaration in struct.
        // (https://doc.rust-lang.org/reference/destructors.html)
        // We do not need to do anything manually, but remember not to change the order of the fields in [`PinnedReadPage`].
        self.frame.unpin();
    }
}

/// Wrapper around frame and its guard (exclusive lock).
pub struct PinnedWritePage {
    wal: Option<Arc<WalClient>>,
    /// Diffs of changes made to page.
    diffs: PageDiff,
    /// The content of the [`Page`] wrapped in a guard.
    guard: RwLockWriteGuard<'static, Page>,
    /// This field should not be exposed. It's here because `guard` cannot outlive it.
    frame: Arc<PageFrame>,
}

impl PinnedWritePage {
    pub fn page(&self) -> &Page {
        &self.guard
    }

    /// Returns mutable reference to the whole page.
    ///
    /// If used, then should be followed by call to [`PinnedWritePage::mark_diff`].
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.guard
    }

    /// Manually adds diff to [`PageDiff`].
    ///
    /// Should be used if page was modified using [`PinnedWritePage::page_mut`].
    pub fn mark_diff(&mut self, from: u16, to: u16) {
        let data = self.page()[from as usize..to as usize].to_vec();
        self.diffs.write_at(from, data);
    }

    /// Inserts `data` to page at `offset`.
    ///
    /// Preferred way of modifying page, as this automatically updates [`PageDiff`].
    pub fn write_at(&mut self, offset: u16, data: Vec<u8>) {
        let end = offset as usize + data.len();
        self.guard[offset as usize..end].copy_from_slice(&data);
        self.diffs.write_at(offset, data);
    }

    /// Gets the LSN of the page.
    pub fn lsn(&self) -> Lsn {
        get_page_lsn(self.page())
    }

    /// Sets the LSN of the page.
    pub fn set_lsn(&mut self, lsn: Lsn) {
        set_page_lsn(self.page_mut(), lsn);
    }
}

impl Drop for PinnedWritePage {
    fn drop(&mut self) {
        // SAFETY: `guard` cannot outlive `frame`. This is why it must be declared before it in the struct,
        // as the order of dropping is the order of declaration in struct.
        // (https://doc.rust-lang.org/reference/destructors.html)
        // We do not need to do anything manually, but remember not to change the order of the fields in [`PinnedWritePage`].

        // If [`PinnedWritePage`] is not manually sent to wal via [`Cache::`] then
        // here we automatically send the diff to wal as [`SinglePageOperation`].
        if !self.diffs.empty()
            && let Some(w) = &self.wal
        {
            let diffs = mem::take(&mut self.diffs);
            let lsn = w.write_single(self.frame.file_page_ref.clone(), diffs);
            match lsn {
                Some(lsn) => {
                    self.set_lsn(lsn);
                }
                None => error!(
                    "failed to add SinglePageOperation to WalClient while dropping PinnedWritePage"
                ),
            }
        }
        self.frame.unpin();
    }
}

pub trait PageRead {
    fn data(&self) -> &[u8];
}

pub trait PageWrite {
    fn data_mut(&mut self) -> &mut [u8];
    fn mark_diff(&mut self, from: u16, to: u16);
    fn write_at(&mut self, offset: u16, data: Vec<u8>);
}

impl PageRead for PinnedReadPage {
    fn data(&self) -> &[u8] {
        self.page()
    }
}

impl PageRead for PinnedWritePage {
    fn data(&self) -> &[u8] {
        self.page()
    }
}
impl<T: PageRead> PageRead for &T {
    fn data(&self) -> &[u8] {
        (*self).data()
    }
}

impl<T: PageWrite> PageWrite for &mut T {
    fn data_mut(&mut self) -> &mut [u8] {
        (*self).data_mut()
    }

    fn mark_diff(&mut self, from: u16, to: u16) {
        (*self).mark_diff(from, to);
    }

    fn write_at(&mut self, offset: u16, data: Vec<u8>) {
        (*self).write_at(offset, data);
    }
}

impl PageWrite for PinnedWritePage {
    fn data_mut(&mut self) -> &mut [u8] {
        self.page_mut()
    }

    fn mark_diff(&mut self, from: u16, to: u16) {
        self.mark_diff(from, to);
    }

    fn write_at(&mut self, offset: u16, data: Vec<u8>) {
        self.write_at(offset, data);
    }
}

/// Error for cache related operations.
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("failed to load file: {0}")]
    FilesManagerError(#[from] FilesManagerError),
    #[error("{0}")]
    PagedFileError(#[from] PagedFileError),
}

/// Responsible for caching [`Page`]s and distributing it to other threads.
/// Threads that use [`Cache`] should not have to worry about multithreading problems - all of them should be handled by [`Cache`].
/// [`Cache`] must be used per-database, as it depends on [`FilesManager`] which works that way.
pub struct Cache {
    /// List of the [`PageFrame`]s currently stored in cache. This is the source of truth from [`Cache`]'s point of view.
    ///
    /// It is guaranteed that:
    /// - any page that is used by at least one thread will not be removed from the [`Cache`].
    /// - any frame that is selected for eviction and is dirty will have its page flushed to disk.
    ///
    /// [`Cache`] tries to keep the size of it <= [`Cache::capacity`], but it is not always true.
    /// Check [`Cache::get_pinned_frame`] and [`Cache::try_evict_frame`] for more details.
    frames: DashMap<FilePageRef, Arc<PageFrame>>,
    /// LRU list of the [`FilePageRef`]s used for deciding which [`PageFrame`] is the best candidate for eviction (it does not mean it will always be picked as the victim - check [`Cache::try_evict_frame`] for details).
    /// It is guaranteed that `lru.keys()` are subset of `frames.keys()` - it means that there might be [`PageFrame`] that is
    /// stored in [`Cache::frames`] but not in [`Cache::lru`]. Such thing may happen if for some reason [`Cache::try_evict_frame`] failed.
    /// This is not a problem as there is a background thread ([`BackgroundCacheCleaner`]) that periodically cleans [`Cache`] from such frames.
    lru: Arc<RwLock<LruCache<FilePageRef, ()>>>,
    /// Pointer to [`FilesManager`], used for file operations when page must be loaded from/flushed to disk.
    files: Arc<FilesManager>,
    /// Client for writing to WAL.
    wal_client: Option<Arc<WalClient>>,
    /// Maximum capacity of the cache (number of frames).
    capacity: usize,
}

impl Cache {
    /// Creates new [`Cache`] that handles frames for single database.
    pub fn new(
        capacity: usize,
        files: Arc<FilesManager>,
        wal_client: Option<WalClient>,
    ) -> Arc<Self> {
        Arc::new(Self {
            frames: DashMap::with_capacity(capacity),
            lru: Arc::new(RwLock::new(LruCache::new(NonZero::new(capacity).unwrap()))),
            files,
            wal_client: wal_client.map(Arc::new),
            capacity,
        })
    }

    /// Creates new [`Cache`] that handles frames for single database and its [`BackgroundCacheCleaner`]'s handle.
    pub fn with_background_cleaner(
        capacity: usize,
        files: Arc<FilesManager>,
        cleanup_interval: Duration,
        wal_handle: Option<WalHandle>,
    ) -> Result<(Arc<Self>, BackgroundWorkerHandle), CacheError> {
        let (wal_client, redo_records) = match wal_handle {
            Some(handle) => (Some(handle.wal_client), Some(handle.redo_records)),
            None => (None, None),
        };
        let cache = Self::new(capacity, files, wal_client);

        if let Some(redo_records) = redo_records {
            cache.apply_redo(redo_records)?;
        }
        let cleaner = BackgroundCacheCleaner::start(BackgroundCacheCleanerParams {
            cache: cache.clone(),
            cleanup_interval,
        });
        Ok((cache, cleaner))
    }

    /// Applies changes to the database files using redo records from write-ahead log. Some of the
    /// records may already be applied and will be skipped.
    ///
    /// Should only be called once during cache initialization.
    fn apply_redo(&self, redo_records: Vec<(Lsn, WalRecordData)>) -> Result<(), CacheError> {
        for (lsn, operation) in redo_records {
            match operation {
                WalRecordData::SinglePageOperation(op) => self.apply_operation(lsn, op)?,
                WalRecordData::MultiPageOperation(multi_op) => {
                    for op in multi_op {
                        self.apply_operation(lsn, op)?;
                    }
                }
                // Checkpoints are skipped over in WAL.
                WalRecordData::Checkpoint { .. } => unreachable!(),
            }
        }

        self.flush_all_frames();

        if let Some(wal_client) = &self.wal_client {
            let _ = wal_client.checkpoint();
        }
        Ok(())
    }

    /// Applies single page operation to the page if its LSN is lower than `lsn`.
    fn apply_operation(&self, lsn: Lsn, operation: SinglePageOperation) -> Result<(), CacheError> {
        let (page_ref, diff) = operation.into_parts();
        let mut page = self.pin_write(&page_ref)?;
        if page.lsn() >= lsn {
            return Ok(());
        }
        diff.apply(&mut page);
        page.set_lsn(lsn);
        Ok(())
    }

    /// Returns shared lock to the page. If page was not found in the cache it loads it from disk.
    pub fn pin_read(&self, id: &FilePageRef) -> Result<PinnedReadPage, CacheError> {
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
    pub fn pin_write(&self, id: &FilePageRef) -> Result<PinnedWritePage, CacheError> {
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
            wal: self.wal_client.clone(),
            diffs: PageDiff::default(),
            frame,
            guard: guard_static,
        })
    }

    /// Allocates new page in `file` and returns exclusive lock to that page and its id.
    /// In case if lock is not needed the return value should not be assigned, so that lock lives as little as needed.
    pub fn allocate_page(&self, file: &FileKey) -> Result<(PinnedWritePage, PageId), CacheError> {
        let pf = self.files.get_or_open_new_file(file)?;
        let page_id = pf.lock().allocate_page()?;
        let id = FilePageRef {
            file_key: file.clone(),
            page_id,
        };
        Ok((self.pin_write(&id)?, page_id))
    }

    /// Remove page from file.
    ///
    /// `page` is only passed to ensure we are the only one using this page and no other thread
    /// can get it in the meantime.
    pub fn free_page(&self, id: &FilePageRef, page: PinnedWritePage) -> Result<(), CacheError> {
        let pf = self.files.get_or_open_new_file(&id.file_key)?;
        pf.lock().free_page(id.page_id)?;
        match self.frames.entry(id.clone()) {
            Entry::Occupied(occupied_entry) => {
                // We hold exclusive lock on the key and remove it from the dashmap,
                // so no other thread can get this key.
                occupied_entry.remove();
                self.lru.write().pop_entry(id);
            }
            Entry::Vacant(_) => {}
        };
        drop(page);
        Ok(())
    }

    /// Removes file from cache.
    ///
    /// First, it removes all pages of this file from cache.
    /// Then it removes it from list of open files in [`FilesManager`].
    pub fn remove_file(&self, file_key: &FileKey) -> Result<(), CacheError> {
        self.remove_all_pages_from_file(file_key, true)?;
        self.files.close_file(file_key);
        Ok(())
    }

    /// Consumes `pages` and send all their diffs to wal as [`MultiPageOperation`].
    pub fn drop_write_pages(&self, mut pages: Vec<PinnedWritePage>) {
        let wal_client = match &self.wal_client {
            Some(w) => w,
            None => return,
        };
        let args: Vec<_> = pages
            .iter_mut()
            .map(|page| (page.frame.file_page_ref.clone(), mem::take(&mut page.diffs)))
            .collect();
        // Now each diff is empty, meaning in `drop` we won't send same page diff again.
        let lsn = wal_client.write_multi(args);
        match lsn {
            Some(lsn) => {
                for mut page in pages {
                    page.set_lsn(lsn);
                }
            }
            None => error!(
                "failed to add MultiPageOperation to WalClient while manually dropping PinnedWritePages"
            ),
        }
    }

    /// Removes all pages from file with `file_key`.
    /// If `flush_to_disk` is set to true then each frame is flushed to the disk before being dropped.
    fn remove_all_pages_from_file(
        &self,
        file_key: &FileKey,
        flush_to_disk: bool,
    ) -> Result<(), CacheError> {
        let pf = self.files.get_or_open_new_file(file_key)?;
        // Acquire lock on the file to prevent other threads from creating new frames for this file
        let mut pf_lock = pf.lock();

        let pages_to_remove: Vec<_> = self
            .frames
            .iter()
            .filter(|entry| entry.key().file_key() == file_key)
            .map(|entry| entry.key().clone())
            .collect();

        for page_ref in pages_to_remove {
            if let Some((_, frame)) = self.frames.remove(&page_ref) {
                if flush_to_disk {
                    // We wait until we got write lock on this frame to be sure that we capture any changes
                    // and flush it to disk.
                    let page = frame.write();

                    // We flush only if frame is dirty
                    if frame.dirty.load(Ordering::Acquire) {
                        pf_lock.write_page(frame.file_page_ref.page_id, *page)?;
                    }
                }

                // Remove from LRU as well
                self.lru.write().pop_entry(&page_ref);
            }
        }
        Ok(())
    }

    /// Removes file from cache without flushing.
    ///
    /// First, it removes all pages of this file from cache (without flushing).
    /// Then it removes it from list of open files in [`FilesManager`].
    pub fn remove_file_without_flushing(&self, file_key: &FileKey) -> Result<(), CacheError> {
        self.remove_all_pages_from_file(file_key, false)?;
        self.files.close_file(file_key);
        Ok(())
    }

    /// Returns [`Arc<PageFrame>`] and pins the underlying [`PageFrame`].
    /// It first looks for frame in [`Cache::frames`]. If it's found there then its key in [`Cache::lru`] is updated (making it MRU).
    /// Otherwise [`PageFrame`] is loaded from disk using [`FilesManager`] and frame's key is inserted into [`Cache::lru`].
    fn get_pinned_frame(&self, id: &FilePageRef) -> Result<Arc<PageFrame>, CacheError> {
        if let Some(frame) = self.frames.get(id) {
            frame.pin();
            let f = frame.clone();
            // Explicitly drop the reference to release the shard lock before acquiring LRU lock
            // to avoid deadlocks (LRU lock -> Shard lock in eviction vs Shard lock -> LRU lock here)
            drop(frame);
            self.push_to_lru(id);
            return Ok(f);
        }

        let pf = self.files.get_or_open_new_file(&id.file_key)?;
        let page = pf.lock().read_page(id.page_id)?;
        let new_frame = Arc::new(PageFrame::new(id.clone(), page));

        // This entry() locks exclusively the slot in `frames`, so when we are inside this match statement we are sure that no other thread will modify it.
        // Check here: https://docs.rs/dashmap/6.1.0/src/dashmap/lib.rs.html#1185-1204
        let (frame, inserted) = match self.frames.entry(id.clone()) {
            Entry::Occupied(occupied_entry) => {
                // Already inserted by other thread.
                // We don't want to reinsert it, as we will lose the information about [`PageFrame::pin_count`]. We need to get the already existing entry and
                // update its pin count.
                let existing = occupied_entry.get().clone();
                existing.pin();
                (existing, false)
            }
            Entry::Vacant(vacant_entry) => {
                // Not yet inserted.
                // Pin immediately so it's not evicted right after insertion.
                new_frame.pin();
                vacant_entry.insert(new_frame.clone());
                (new_frame, true)
            }
        };

        if inserted {
            // We call this outside of the `frames` lock to avoid deadlocks.
            if self.frames.len() > self.capacity && !self.try_evict_frame()? {
                warn!(
                    "Cache: cannot evict frame - every frame in cache is pinned or lru is empty."
                );
            }
        }

        self.push_to_lru(id);
        Ok(frame)
    }

    /// Evicts the first frame (starting from LRU) that has [`PageFrame::pin_count`] equal to 0.
    /// If frame is selected to be evicted (using LRU), but its pin count is greater than 0, it will not be evicted and instead its key is updated in [`Cache::lru`] (making it MRU). In that case the next LRU is picked and so on.
    /// Returns `false` if it could not evict any page - every page in cache is pinned or LRU is empty.
    fn try_evict_frame(&self) -> Result<bool, CacheError> {
        let max_attempts = self.capacity;

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
        // We need to lock the file to prevent race with get_pinned_frame loading from disk.
        // If we don't lock here, get_pinned_frame might load the page from disk (which is old)
        // just after we removed it from frames but before we flushed it.
        let pf = self.files.get_or_open_new_file(&victim_id.file_key)?;
        let file_lock = pf.lock();

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
            // We pass the already acquired lock to flush_frame
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
        // We do not need to check if pin_count > 0 - at this point either this frame was removed from dashmap, and we hold the
        // exclusive lock on the shard in which frame's key was, so it cannot be increased or there is only one thread using cache.
        // At the same time we hold the lock to underlying file, meaning no other thread can create frame for the same page - we are safe to flush.

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

impl Drop for Cache {
    fn drop(&mut self) {
        self.flush_all_frames();
    }
}

/// Responsible for periodically scanning [`Cache`] and removing [`PageFrame`]s from it that are in [`Cache::frames`] but not in [`Cache::lru`].
struct BackgroundCacheCleaner {
    cache: Arc<Cache>,
    cleanup_interval: Duration,
    shutdown: channel::Receiver<()>,
}

struct BackgroundCacheCleanerParams {
    cache: Arc<Cache>,
    cleanup_interval: Duration,
}

impl BackgroundWorker for BackgroundCacheCleaner {
    type BackgroundWorkerParams = BackgroundCacheCleanerParams;

    fn start(params: Self::BackgroundWorkerParams) -> BackgroundWorkerHandle {
        info!("Starting cache background cleaner");
        let (tx, rx) = channel::unbounded();
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

impl BackgroundCacheCleaner {
    fn run(self) {
        loop {
            match self.shutdown.recv_timeout(self.cleanup_interval) {
                Ok(()) => {
                    // Got signal for shutdown.
                    info!("Shutting down cache background cleaner");
                    break;
                }
                Err(channel::RecvTimeoutError::Timeout) => {
                    info!("Cache background cleaner - syncing frames and lru");
                    if let Err(e) = self.sync_frames_and_lru() {
                        error!("failed to sync frames and lru: {e}")
                    }
                }
                Err(channel::RecvTimeoutError::Disconnected) => {
                    // Sender dropped - trying to shut down anyway.
                    info!("Shutting down cache background cleaner (cancellation channel dropped)");
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
            // Lock LRU, and only after lock frames.
            // This way we have same order of locking as in try_evict_frame.
            let lru_guard = self.cache.lru.read();
            if !lru_guard.contains(&key) {
                self.cache
                    .remove_from_cache_if(&key, |_, frame| !frame.is_pinned())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::write_ahead_log::spawn_wal;

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
        let files = FilesManager::new(db_dir).unwrap();
        Arc::new(files)
    }

    /// Spawns a thread that pins `id` from `cache`, reads the first 8 bytes and asserts it equals `expected`.
    fn spawn_check_page(
        cache: Arc<Cache>,
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
    fn assert_cached(cache: &Arc<Cache>, id: &FilePageRef) {
        assert!(
            cache.frames.contains_key(id),
            "expected frame present in frames for {id:?}",
        );
    }

    /// Asserts that `id` is present in both frames map and LRU.
    fn assert_cached_and_in_lru(cache: &Arc<Cache>, id: &FilePageRef) {
        assert_cached(cache, id);
        let lru_guard = cache.lru.read();
        assert!(
            lru_guard.contains(id),
            "expected key present in LRU for {id:?}"
        );
    }

    /// Assert that every frame in the cache is currently unpinned (pin_count == 0).
    fn assert_all_frames_unpinned(cache: &Arc<Cache>) {
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

        let cache = Cache::new(1, files.clone(), None);

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

        let cache = Cache::new(2, files.clone(), None);

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

        let cache = Cache::new(3, files.clone(), None);

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

        let cache = Cache::new(3, files.clone(), None);

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

        let cache = Cache::new(2, files.clone(), None);

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

        let cache = Cache::new(1, files.clone(), None);

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
        let cache = Cache::new(1, files.clone(), None);
        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        {
            let mut w = cache.pin_write(&id.clone()).expect("pin_write");
            w.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());
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
    fn cache_eviction_and_reload_preserves_writes() {
        let files = create_files_manager();
        let file_key = FileKey::data("table1");

        let cache = Cache::new(1, files.clone(), None);

        // Allocate page via cache
        let (mut w, pid) = cache.allocate_page(&file_key).expect("allocate_page");
        w.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());
        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };
        drop(w); // unpin

        // Trigger eviction by loading another page
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2222);
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };
        let _p = cache.pin_read(&id2).expect("pin_read");

        // Re-read the evicted page through cache
        let r = cache.pin_read(&id).expect("pin_read evicted page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&r.page()[0..8]);
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
        let cache = Cache::new(1, files.clone(), None);

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
                w.write_at(0, (1000 + i as u64).to_be_bytes().to_vec());
                // hold write briefly to increase chance of overlap
                thread::sleep(Duration::from_millis(10));
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

        let cache = Cache::new(1, files.clone(), None);

        let (mut pinned, _) = cache
            .allocate_page(&file_key)
            .expect("allocate_page failed");

        pinned.write_at(0, 0xFEEDu64.to_be_bytes().to_vec());

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

        let cache = Cache::new(1, files.clone(), None);

        {
            let h = spawn_check_page(cache.clone(), id.clone(), 0xAA);
            h.join().unwrap();
        }

        let page = cache.pin_write(&id).unwrap();

        cache
            .free_page(&id, page)
            .expect("free_page failed in normal case");

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

        let cache = Cache::new(2, files.clone(), None);

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
            Cache::with_background_cleaner(2, files.clone(), Duration::from_millis(50), None)
                .unwrap();

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
            Cache::with_background_cleaner(2, files.clone(), Duration::from_millis(50), None)
                .unwrap();

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
            Cache::with_background_cleaner(3, files.clone(), Duration::from_millis(50), None)
                .unwrap();

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

        let cache = Cache::new(1, files.clone(), None);

        {
            let mut w = cache.pin_write(&id).expect("pin_write failed");
            w.write_at(0, 0xDEADu64.to_be_bytes().to_vec());
        }

        drop(cache);

        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xDEADu64);
    }

    #[test]
    fn cache_remove_all_pages_without_flushing_single_threaded() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_remove");

        // Create several pages in the file
        let pid1 = alloc_page_with_u64(&files, &file_key, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2222);
        let pid3 = alloc_page_with_u64(&files, &file_key, 0x3333);

        let cache = Cache::new(5, files.clone(), None);

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

        // Load all pages into cache
        {
            let _p1 = cache.pin_read(&id1).expect("pin_read id1");
            let _p2 = cache.pin_read(&id2).expect("pin_read id2");
            let _p3 = cache.pin_read(&id3).expect("pin_read id3");
        }

        // Verify they're all cached
        assert_cached(&cache, &id1);
        assert_cached(&cache, &id2);
        assert_cached(&cache, &id3);
        assert_eq!(cache.frames.len(), 3);

        // Remove all pages from this file
        cache
            .remove_all_pages_from_file(&file_key, false)
            .expect("remove_all_pages failed");

        // Verify all frames for this file are removed
        assert!(!cache.frames.contains_key(&id1));
        assert!(!cache.frames.contains_key(&id2));
        assert!(!cache.frames.contains_key(&id3));
        assert_eq!(cache.frames.len(), 0);

        // Verify LRU is also cleaned
        assert!(!cache.lru.read().contains(&id1));
        assert!(!cache.lru.read().contains(&id2));
        assert!(!cache.lru.read().contains(&id3));
    }

    #[test]
    fn cache_remove_all_pages_without_flushing_does_not_affect_other_files() {
        let files = create_files_manager();
        let file_key1 = FileKey::data("table_remove_1");
        let file_key2 = FileKey::data("table_remove_2");

        // Create pages in both files
        let pid1 = alloc_page_with_u64(&files, &file_key1, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key2, 0x2222);

        let cache = Cache::new(5, files.clone(), None);

        let id1 = FilePageRef {
            page_id: pid1,
            file_key: file_key1.clone(),
        };
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key2.clone(),
        };

        // Load both pages
        {
            let _p1 = cache.pin_read(&id1).expect("pin_read id1");
            let _p2 = cache.pin_read(&id2).expect("pin_read id2");
        }

        assert_eq!(cache.frames.len(), 2);

        // Remove only pages from file_key1
        cache
            .remove_all_pages_from_file(&file_key1, false)
            .expect("remove_all_pages failed");

        // id1 should be gone, id2 should remain
        assert!(!cache.frames.contains_key(&id1));
        assert_cached(&cache, &id2);
        assert_eq!(cache.frames.len(), 1);
    }

    #[test]
    fn cache_remove_all_pages_without_flushing_with_dirty_frames_no_flush() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_remove_dirty");

        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);
        let cache = Cache::new(5, files.clone(), None);

        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Write to page (make it dirty)
        {
            let mut w = cache.pin_write(&id).expect("pin_write");
            w.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());
        }

        // Verify it's dirty and cached
        assert_cached(&cache, &id);
        let frame = cache.frames.get(&id).unwrap();
        assert!(frame.dirty.load(Ordering::Acquire));
        drop(frame);

        // Remove without flushing
        cache
            .remove_all_pages_from_file(&file_key, false)
            .expect("remove_all_pages failed");

        // Frame should be gone
        assert!(!cache.frames.contains_key(&id));

        // Read from disk directly - should still have OLD value (0x1111), not 0xBEEF
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0x1111u64);
    }

    #[test]
    fn cache_remove_all_pages_without_flushing_empty_file() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_empty");

        let cache = Cache::new(5, files.clone(), None);

        // Try to remove pages from a file that has no cached pages
        cache
            .remove_all_pages_from_file(&file_key, false)
            .expect("remove_all_pages should succeed on empty");

        assert_eq!(cache.frames.len(), 0);
    }

    #[test]
    fn cache_remove_all_pages_without_flushing_with_held_frame_no_flush() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_remove_held");

        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);
        let cache = Cache::new(5, files.clone(), None);

        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Pin the page for write and make it dirty
        let mut w = cache.pin_write(&id).expect("pin_write");
        w.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());

        // While still holding the write lock, remove all pages from this file
        cache
            .remove_all_pages_from_file(&file_key, false)
            .expect("remove_all_pages failed");

        // Frame should be removed from cache even though we still hold the lock
        assert!(!cache.frames.contains_key(&id));
        assert!(!cache.lru.read().contains(&id));

        // Continue modifying the page while holding the frame
        w.write_at(8, 0xDEADu64.to_be_bytes().to_vec());

        // Drop the write lock
        drop(w);

        // Read from disk directly - should still have ORIGINAL value (0x1111)
        // because the frame was removed from cache and never flushed
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(
            u64::from_be_bytes(buf),
            0x1111u64,
            "First 8 bytes should be original value"
        );

        buf.copy_from_slice(&page[8..16]);
        assert_eq!(
            u64::from_be_bytes(buf),
            0x0u64,
            "Next 8 bytes should be zeros (unmodified)"
        );
    }

    #[test]
    fn cache_remove_file_flushes_dirty_pages() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_remove_with_flush");

        let pid1 = alloc_page_with_u64(&files, &file_key, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2222);

        let cache = Cache::new(5, files.clone(), None);

        let id1 = FilePageRef {
            page_id: pid1,
            file_key: file_key.clone(),
        };
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };

        // Load pages and modify them
        {
            let mut w1 = cache.pin_write(&id1).expect("pin_write id1");
            w1.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());
        }
        {
            let mut w2 = cache.pin_write(&id2).expect("pin_write id2");
            w2.write_at(0, 0xDEADu64.to_be_bytes().to_vec());
        }

        // Remove file (should flush)
        cache.remove_file(&file_key).expect("remove_file failed");

        // Frames should be gone
        assert!(!cache.frames.contains_key(&id1));
        assert!(!cache.frames.contains_key(&id2));

        // Read from disk - should have NEW values (flushed)
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let mut pf_lock = pf.lock();

        let page1 = pf_lock.read_page(pid1).expect("read_page pid1");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page1[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xBEEFu64);

        let page2 = pf_lock.read_page(pid2).expect("read_page pid2");
        buf.copy_from_slice(&page2[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xDEADu64);
    }

    #[test]
    fn cache_remove_file_vs_remove_file_without_flushing() {
        let files = create_files_manager();
        let file_key1 = FileKey::data("table_with_flush");
        let file_key2 = FileKey::data("table_without_flush");

        let pid1 = alloc_page_with_u64(&files, &file_key1, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key2, 0x2222);

        let cache = Cache::new(5, files.clone(), None);

        let id1 = FilePageRef {
            page_id: pid1,
            file_key: file_key1.clone(),
        };
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key2.clone(),
        };

        // Modify both pages
        {
            let mut w1 = cache.pin_write(&id1).expect("pin_write id1");
            w1.write_at(0, 0xAAAAu64.to_be_bytes().to_vec());
        }
        {
            let mut w2 = cache.pin_write(&id2).expect("pin_write id2");
            w2.write_at(0, 0xBBBBu64.to_be_bytes().to_vec());
        }

        // Remove file1 WITH flushing
        cache.remove_file(&file_key1).expect("remove_file failed");

        // Remove file2 WITHOUT flushing
        cache
            .remove_file_without_flushing(&file_key2)
            .expect("remove_file_without_flushing failed");

        // Read from disk - file1 should have new value, file2 should have old value
        let pf1 = files.get_or_open_new_file(&file_key1).unwrap();
        let page1 = pf1.lock().read_page(pid1).expect("read_page pid1");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page1[0..8]);
        assert_eq!(
            u64::from_be_bytes(buf),
            0xAAAAu64,
            "file1 should be flushed"
        );

        let pf2 = files.get_or_open_new_file(&file_key2).unwrap();
        let page2 = pf2.lock().read_page(pid2).expect("read_page pid2");
        buf.copy_from_slice(&page2[0..8]);
        assert_eq!(
            u64::from_be_bytes(buf),
            0x2222u64,
            "file2 should have original value"
        );
    }

    #[test]
    fn cache_remove_file_with_held_frame() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_remove_held_flush");

        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);
        let cache = Cache::new(5, files.clone(), None);

        let id = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Pin the page for write and modify it
        let mut w = cache.pin_write(&id).expect("pin_write");
        w.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());

        // Spawn thread to remove file (should block until we release the write lock)
        let cache_cl = cache.clone();
        let file_key_cl = file_key.clone();
        let handle = thread::spawn(move || {
            cache_cl
                .remove_file(&file_key_cl)
                .expect("remove_file failed");
        });

        // Give remove_file time to block
        thread::sleep(Duration::from_millis(100));

        // Frame should still be in lru (we removed from cache, but we are waiting for write lock)
        assert!(cache.lru.read().contains(&id));

        // Release the write lock
        drop(w);

        // Wait for remove_file to complete
        handle.join().expect("remove_file thread panicked");

        // Frame should now be gone from both frames and cache
        assert!(!cache.frames.contains_key(&id));
        assert!(!cache.lru.read().contains(&id));

        // Read from disk - should have flushed value
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xBEEFu64);
    }

    #[test]
    fn cache_remove_file_empty() {
        let files = create_files_manager();
        let file_key = FileKey::data("table_empty_remove");

        let cache = Cache::new(5, files.clone(), None);

        // Remove file that has no cached pages
        cache
            .remove_file(&file_key)
            .expect("remove_file should succeed on empty");

        assert_eq!(cache.frames.len(), 0);
    }

    #[test]
    fn cache_remove_file_does_not_affect_other_files() {
        let files = create_files_manager();
        let file_key1 = FileKey::data("table_remove_target");
        let file_key2 = FileKey::data("table_keep");

        let pid1 = alloc_page_with_u64(&files, &file_key1, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key2, 0x2222);

        let cache = Cache::new(5, files.clone(), None);

        let id1 = FilePageRef {
            page_id: pid1,
            file_key: file_key1.clone(),
        };
        let id2 = FilePageRef {
            page_id: pid2,
            file_key: file_key2.clone(),
        };

        // Load both pages
        {
            let _p1 = cache.pin_read(&id1).expect("pin_read id1");
            let _p2 = cache.pin_read(&id2).expect("pin_read id2");
        }

        assert_eq!(cache.frames.len(), 2);

        // Remove only file_key1
        cache.remove_file(&file_key1).expect("remove_file failed");

        // id1 should be gone, id2 should remain
        assert!(!cache.frames.contains_key(&id1));
        assert_cached(&cache, &id2);
        assert_eq!(cache.frames.len(), 1);
    }

    #[test]
    fn cache_apply_redo_single_page_operation() {
        use crate::write_ahead_log::{SinglePageOperation, WalRecordData};

        let files = create_files_manager();
        let file_key = FileKey::data("table_redo");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);

        let page_ref = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Create a redo record that modifies the page
        let mut diff = PageDiff::default();
        diff.write_at(0, 0xBEEFu64.to_be_bytes().to_vec());

        let op = SinglePageOperation::new(page_ref.clone(), diff);
        let redo_records = vec![(1u64, WalRecordData::SinglePageOperation(op))];

        let cache = Cache::new(5, files.clone(), None);
        cache.apply_redo(redo_records).expect("apply_redo failed");

        // Verify the change was applied and flushed to disk
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xBEEFu64);

        // Verify LSN was updated
        buf.copy_from_slice(&page[4088..4096]);
        assert_eq!(u64::from_le_bytes(buf), 1u64);
    }

    #[test]
    fn cache_apply_redo_multi_page_operation() {
        use crate::write_ahead_log::{SinglePageOperation, WalRecordData};

        let files = create_files_manager();
        let file_key = FileKey::data("table_redo_multi");
        let pid1 = alloc_page_with_u64(&files, &file_key, 0x1111);
        let pid2 = alloc_page_with_u64(&files, &file_key, 0x2222);

        let page_ref1 = FilePageRef {
            page_id: pid1,
            file_key: file_key.clone(),
        };
        let page_ref2 = FilePageRef {
            page_id: pid2,
            file_key: file_key.clone(),
        };

        // Create redo records for both pages
        let mut diff1 = PageDiff::default();
        diff1.write_at(0, 0xAAAAu64.to_be_bytes().to_vec());

        let mut diff2 = PageDiff::default();
        diff2.write_at(0, 0xBBBBu64.to_be_bytes().to_vec());

        let op1 = SinglePageOperation::new(page_ref1.clone(), diff1);
        let op2 = SinglePageOperation::new(page_ref2.clone(), diff2);

        let redo_records = vec![(2u64, WalRecordData::MultiPageOperation(vec![op1, op2]))];

        let cache = Cache::new(5, files.clone(), None);
        cache.apply_redo(redo_records).expect("apply_redo failed");

        // Verify changes were applied to both pages
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let mut pf_lock = pf.lock();

        let page1 = pf_lock.read_page(pid1).expect("read_page pid1");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page1[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xAAAAu64);
        buf.copy_from_slice(&page1[4088..4096]);
        assert_eq!(u64::from_le_bytes(buf), 2u64);

        let page2 = pf_lock.read_page(pid2).expect("read_page pid2");
        buf.copy_from_slice(&page2[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xBBBBu64);
        buf.copy_from_slice(&page2[4088..4096]);
        assert_eq!(u64::from_le_bytes(buf), 2u64);
    }

    #[test]
    fn cache_apply_redo_skips_already_applied() {
        use crate::write_ahead_log::{SinglePageOperation, WalRecordData};

        let files = create_files_manager();
        let file_key = FileKey::data("table_redo_skip");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);

        let page_ref = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // First, apply a redo with LSN 5
        let mut diff1 = PageDiff::default();
        diff1.write_at(0, 0xAAAAu64.to_be_bytes().to_vec());
        let op1 = SinglePageOperation::new(page_ref.clone(), diff1);
        let redo_records1 = vec![(5u64, WalRecordData::SinglePageOperation(op1))];

        let cache = Cache::new(5, files.clone(), None);
        cache.apply_redo(redo_records1).expect("apply_redo failed");

        // Now try to apply a redo with lower LSN (should be skipped)
        let mut diff2 = PageDiff::default();
        diff2.write_at(0, 0xBBBBu64.to_be_bytes().to_vec());
        let op2 = SinglePageOperation::new(page_ref.clone(), diff2);
        let redo_records2 = vec![(3u64, WalRecordData::SinglePageOperation(op2))];

        cache.apply_redo(redo_records2).expect("apply_redo failed");

        // Verify the page still has the first value (0xAAAA), not 0xBBBB
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xAAAAu64);

        // Verify LSN is still 5
        buf.copy_from_slice(&page[4088..4096]);
        assert_eq!(u64::from_le_bytes(buf), 5u64);
    }

    #[test]
    fn cache_apply_redo_multiple_records_in_sequence() {
        use crate::write_ahead_log::{SinglePageOperation, WalRecordData};

        let files = create_files_manager();
        let file_key = FileKey::data("table_redo_seq");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);

        let page_ref = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Create multiple redo records
        let mut diff1 = PageDiff::default();
        diff1.write_at(0, 0xAAAAu64.to_be_bytes().to_vec());
        let op1 = SinglePageOperation::new(page_ref.clone(), diff1);

        let mut diff2 = PageDiff::default();
        diff2.write_at(8, 0xBBBBu64.to_be_bytes().to_vec());
        let op2 = SinglePageOperation::new(page_ref.clone(), diff2);

        let mut diff3 = PageDiff::default();
        diff3.write_at(16, 0xCCCCu64.to_be_bytes().to_vec());
        let op3 = SinglePageOperation::new(page_ref.clone(), diff3);

        let redo_records = vec![
            (1u64, WalRecordData::SinglePageOperation(op1)),
            (2u64, WalRecordData::SinglePageOperation(op2)),
            (3u64, WalRecordData::SinglePageOperation(op3)),
        ];

        let cache = Cache::new(5, files.clone(), None);
        cache.apply_redo(redo_records).expect("apply_redo failed");

        // Verify all changes were applied
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");
        let mut buf = [0u8; 8];

        buf.copy_from_slice(&page[0..8]);
        assert_eq!(u64::from_be_bytes(buf), 0xAAAAu64);

        buf.copy_from_slice(&page[8..16]);
        assert_eq!(u64::from_be_bytes(buf), 0xBBBBu64);

        buf.copy_from_slice(&page[16..24]);
        assert_eq!(u64::from_be_bytes(buf), 0xCCCCu64);

        // Verify LSN is the last one applied (3)
        buf.copy_from_slice(&page[4088..4096]);
        assert_eq!(u64::from_le_bytes(buf), 3u64);
    }

    #[test]
    fn cache_apply_redo_with_overlapping_diffs() {
        use crate::write_ahead_log::{SinglePageOperation, WalRecordData};

        let files = create_files_manager();
        let file_key = FileKey::data("table_redo_overlap");
        let pid = alloc_page_with_u64(&files, &file_key, 0x1111);

        let page_ref = FilePageRef {
            page_id: pid,
            file_key: file_key.clone(),
        };

        // Create redo with overlapping writes
        let mut diff = PageDiff::default();
        diff.write_at(0, vec![0x11, 0x22, 0x33, 0x44]);
        diff.write_at(2, vec![0xAA, 0xBB]); // Overlaps with previous write

        let op = SinglePageOperation::new(page_ref.clone(), diff);
        let redo_records = vec![(1u64, WalRecordData::SinglePageOperation(op))];

        let cache = Cache::new(5, files.clone(), None);
        cache.apply_redo(redo_records).expect("apply_redo failed");

        // Verify the final state (overlapping write should merge)
        let pf = files.get_or_open_new_file(&file_key).unwrap();
        let page = pf.lock().read_page(pid).expect("read_page");

        // PageDiff merges writes, so we should have [0x11, 0x22, 0xAA, 0xBB]
        assert_eq!(page[0], 0x11);
        assert_eq!(page[1], 0x22);
        assert_eq!(page[2], 0xAA);
        assert_eq!(page[3], 0xBB);
    }

    #[test]
    fn cache_apply_redo_empty_list() {
        let files = create_files_manager();

        let cache = Cache::new(5, files.clone(), None);

        // This should succeed without error
        cache.apply_redo(vec![]).expect("apply_redo failed");

        // Cache should still be empty
        assert_eq!(cache.frames.len(), 0);
    }

    /// Helper to create a cache with WAL enabled for testing.
    fn create_cache_with_wal(
        capacity: usize,
    ) -> (
        Arc<Cache>,
        Arc<FilesManager>,
        Arc<WalClient>,
        BackgroundWorkerHandle,
    ) {
        let files = create_files_manager();
        let temp_dir = tempdir().expect("create tempdir");

        let (wal_handle, wal_bg_handle) =
            spawn_wal(temp_dir.path(), Duration::from_millis(50), 1024).expect("spawn_wal");

        let cache = Cache::new(capacity, files.clone(), Some(wal_handle.wal_client));

        let wal_client = cache.wal_client.as_ref().unwrap().clone();

        (cache, files, wal_client, wal_bg_handle)
    }

    #[test]
    fn wal_auto_log_on_drop_single_write() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(5);

        let file_key = FileKey::data("table_wal_drop");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xAAAA);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = {
            let page = cache.pin_read(&page_ref).expect("pin_read");
            page.lsn()
        };

        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Modify page and let it drop (should auto-log to WAL)
        {
            let mut page = cache.pin_write(&page_ref).expect("pin_write");
            page.write_at(0, vec![0xBB, 0xBB, 0xBB, 0xBB]);
            // page drops here, should automatically log to WAL
        }

        // Verify LSN was updated
        let page = cache.pin_read(&page_ref).expect("pin_read");
        let new_lsn = page.lsn();
        assert!(
            new_lsn > initial_lsn,
            "LSN should be updated after auto-logging to WAL"
        );

        // Verify WAL received exactly 1 write (LSN incremented by 1)
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 1,
            "WAL should have flushed exactly 1 record"
        );
        assert_eq!(new_lsn, flushed_lsn, "Page LSN should match flushed LSN");

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_auto_log_on_drop_multiple_writes() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(5);

        let file_key = FileKey::data("table_wal_multi");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xCCCC);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = {
            let page = cache.pin_read(&page_ref).expect("pin_read");
            page.lsn()
        };

        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Multiple writes to same page
        {
            let mut page = cache.pin_write(&page_ref).expect("pin_write");
            page.write_at(0, vec![0x11]);
            page.write_at(10, vec![0x22, 0x22]);
            page.write_at(20, vec![0x33, 0x33, 0x33]);
            // All diffs should be merged and logged on drop as single operation
        }

        // Verify LSN was updated
        let page = cache.pin_read(&page_ref).expect("pin_read");
        let new_lsn = page.lsn();
        assert!(
            new_lsn > initial_lsn,
            "LSN should be updated after multiple writes"
        );

        // Verify WAL received exactly 1 write (all diffs merged into one)
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 1,
            "WAL should have flushed exactly 1 record (merged diffs)"
        );

        // Verify the data was actually written
        assert_eq!(page.page()[0], 0x11);
        assert_eq!(page.page()[10], 0x22);
        assert_eq!(page.page()[11], 0x22);
        assert_eq!(page.page()[20], 0x33);
        assert_eq!(page.page()[21], 0x33);
        assert_eq!(page.page()[22], 0x33);

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_no_log_if_no_changes() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(5);

        let file_key = FileKey::data("table_wal_nochange");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xDDDD);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = {
            let page = cache.pin_read(&page_ref).expect("pin_read");
            page.lsn()
        };

        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Pin for write but don't modify
        {
            let _page = cache.pin_write(&page_ref).expect("pin_write");
            // no writes, should not log to WAL on drop
        }

        // LSN should not change if no writes occurred
        let page = cache.pin_read(&page_ref).expect("pin_read");
        let new_lsn = page.lsn();
        assert_eq!(
            new_lsn, initial_lsn,
            "LSN should not change if no writes occurred"
        );

        // Verify WAL received no writes (flushed_lsn unchanged)
        thread::sleep(Duration::from_millis(150)); // Wait for potential flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn, initial_flushed_lsn,
            "WAL should not have any new records"
        );

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_drop_write_pages_multi_page_operation() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(10);

        let file_key = FileKey::data("table_wal_multi_page");

        // Create multiple pages
        let page_id_1 = alloc_page_with_u64(&files, &file_key, 0x1111);
        let page_id_2 = alloc_page_with_u64(&files, &file_key, 0x2222);
        let page_id_3 = alloc_page_with_u64(&files, &file_key, 0x3333);

        let page_ref_1 = FilePageRef {
            page_id: page_id_1,
            file_key: file_key.clone(),
        };
        let page_ref_2 = FilePageRef {
            page_id: page_id_2,
            file_key: file_key.clone(),
        };
        let page_ref_3 = FilePageRef {
            page_id: page_id_3,
            file_key: file_key.clone(),
        };

        // Get initial LSNs
        let initial_lsn_1 = cache.pin_read(&page_ref_1).expect("pin_read").lsn();
        let initial_lsn_2 = cache.pin_read(&page_ref_2).expect("pin_read").lsn();
        let initial_lsn_3 = cache.pin_read(&page_ref_3).expect("pin_read").lsn();

        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Modify multiple pages
        let mut page_1 = cache.pin_write(&page_ref_1).expect("pin_write");
        let mut page_2 = cache.pin_write(&page_ref_2).expect("pin_write");
        let mut page_3 = cache.pin_write(&page_ref_3).expect("pin_write");

        page_1.write_at(0, vec![0xAA, 0xAA]);
        page_2.write_at(0, vec![0xBB, 0xBB]);
        page_3.write_at(0, vec![0xCC, 0xCC]);

        // Use drop_write_pages to log all at once
        cache.drop_write_pages(vec![page_1, page_2, page_3]);

        // All pages should have the same LSN (from multi-page operation)
        let lsn_1 = cache.pin_read(&page_ref_1).expect("pin_read").lsn();
        let lsn_2 = cache.pin_read(&page_ref_2).expect("pin_read").lsn();
        let lsn_3 = cache.pin_read(&page_ref_3).expect("pin_read").lsn();

        assert_eq!(lsn_1, lsn_2, "All pages should have same LSN");
        assert_eq!(lsn_2, lsn_3, "All pages should have same LSN");
        assert!(lsn_1 > initial_lsn_1, "LSN should be updated");
        assert!(lsn_2 > initial_lsn_2, "LSN should be updated");
        assert!(lsn_3 > initial_lsn_3, "LSN should be updated");

        // Verify WAL received exactly 1 write (multi-page operation)
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 1,
            "WAL should have flushed exactly 1 multi-page record"
        );
        assert_eq!(lsn_1, flushed_lsn, "All pages should have flushed LSN");

        // Verify data was written
        let page_1 = cache.pin_read(&page_ref_1).expect("pin_read");
        let page_2 = cache.pin_read(&page_ref_2).expect("pin_read");
        let page_3 = cache.pin_read(&page_ref_3).expect("pin_read");

        assert_eq!(page_1.page()[0], 0xAA);
        assert_eq!(page_2.page()[0], 0xBB);
        assert_eq!(page_3.page()[0], 0xCC);

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_drop_write_pages_no_double_logging() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(5);

        let file_key = FileKey::data("table_wal_no_double");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xEEEE);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = cache.pin_read(&page_ref).expect("pin_read").lsn();
        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Modify page
        let mut page = cache.pin_write(&page_ref).expect("pin_write");
        page.write_at(0, vec![0xFF, 0xFF]);

        // Manually drop via drop_write_pages (diffs are mem::take'd, so empty on actual drop)
        cache.drop_write_pages(vec![page]);

        let lsn_after_manual_drop = cache.pin_read(&page_ref).expect("pin_read").lsn();

        // LSN should be updated once
        assert!(
            lsn_after_manual_drop > initial_lsn,
            "LSN should be updated after manual drop"
        );

        // Verify exactly 1 WAL record was written (no double logging)
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 1,
            "WAL should have exactly 1 record (no double logging)"
        );

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_cache_without_wal_client() {
        let files = create_files_manager();
        let cache = Cache::new(5, files.clone(), None);

        let file_key = FileKey::data("table_no_wal");
        let page_id = alloc_page_with_u64(&files, &file_key, 0xFFFF);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = cache.pin_read(&page_ref).expect("pin_read").lsn();

        // Modify page (should not panic even without WAL)
        {
            let mut page = cache.pin_write(&page_ref).expect("pin_write");
            page.write_at(0, vec![0x99, 0x99]);
        }

        // LSN should not change without WAL
        let page = cache.pin_read(&page_ref).expect("pin_read");
        assert_eq!(
            page.lsn(),
            initial_lsn,
            "LSN should not change without WAL client"
        );

        // Data should still be written to page
        assert_eq!(page.page()[0], 0x99);
        assert_eq!(page.page()[1], 0x99);
    }

    #[test]
    fn wal_drop_write_pages_without_wal_client() {
        let files = create_files_manager();
        let cache = Cache::new(5, files.clone(), None);

        let file_key = FileKey::data("table_no_wal_multi");
        let page_id_1 = alloc_page_with_u64(&files, &file_key, 0x1111);
        let page_id_2 = alloc_page_with_u64(&files, &file_key, 0x2222);

        let page_ref_1 = FilePageRef {
            page_id: page_id_1,
            file_key: file_key.clone(),
        };
        let page_ref_2 = FilePageRef {
            page_id: page_id_2,
            file_key: file_key.clone(),
        };

        let mut page_1 = cache.pin_write(&page_ref_1).expect("pin_write");
        let mut page_2 = cache.pin_write(&page_ref_2).expect("pin_write");

        page_1.write_at(0, vec![0xAA]);
        page_2.write_at(0, vec![0xBB]);

        // Should not panic even without WAL
        cache.drop_write_pages(vec![page_1, page_2]);

        // Verify data was written
        let page_1 = cache.pin_read(&page_ref_1).expect("pin_read");
        let page_2 = cache.pin_read(&page_ref_2).expect("pin_read");

        assert_eq!(page_1.page()[0], 0xAA);
        assert_eq!(page_2.page()[0], 0xBB);
    }

    #[test]
    fn wal_mark_diff_updates_lsn() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(5);

        let file_key = FileKey::data("table_mark_diff");
        let page_id = alloc_page_with_u64(&files, &file_key, 0x5555);

        let page_ref = FilePageRef {
            page_id,
            file_key: file_key.clone(),
        };

        let initial_lsn = cache.pin_read(&page_ref).expect("pin_read").lsn();
        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Use mark_diff instead of write_at
        {
            let mut page = cache.pin_write(&page_ref).expect("pin_write");
            page.page_mut()[0] = 0x88;
            page.page_mut()[1] = 0x88;
            page.mark_diff(0, 2);
        }

        // LSN should be updated
        let page = cache.pin_read(&page_ref).expect("pin_read");
        assert!(
            page.lsn() > initial_lsn,
            "LSN should be updated after mark_diff"
        );
        assert_eq!(page.page()[0], 0x88);
        assert_eq!(page.page()[1], 0x88);

        // Verify WAL received exactly 1 write
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 1,
            "WAL should have flushed exactly 1 record"
        );

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }

    #[test]
    fn wal_concurrent_writes_different_pages() {
        let (cache, files, wal_client, mut bg_wal) = create_cache_with_wal(10);

        let file_key = FileKey::data("table_concurrent_wal");

        let initial_flushed_lsn = wal_client.flushed_lsn();

        // Create multiple pages
        let page_ids: Vec<_> = (0..5)
            .map(|i| alloc_page_with_u64(&files, &file_key, i as u64))
            .collect();

        let start = Arc::new(Barrier::new(page_ids.len() + 1));
        let mut handles = Vec::new();

        for (idx, page_id) in page_ids.iter().enumerate() {
            let cache_clone = cache.clone();
            let file_key_clone = file_key.clone();
            let page_id = *page_id;
            let start_clone = start.clone();

            handles.push(thread::spawn(move || {
                let page_ref = FilePageRef {
                    page_id,
                    file_key: file_key_clone,
                };

                start_clone.wait();

                let mut page = cache_clone.pin_write(&page_ref).expect("pin_write");
                page.write_at(0, vec![idx as u8; 8]);
                // Each page drops independently, logging to WAL
            }));
        }

        start.wait();

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // Verify all pages were updated
        for (idx, page_id) in page_ids.iter().enumerate() {
            let page_ref = FilePageRef {
                page_id: *page_id,
                file_key: file_key.clone(),
            };
            let page = cache.pin_read(&page_ref).expect("pin_read");
            assert_eq!(page.page()[0], idx as u8);
            //   assert!(page.lsn() > 0, "Page should have LSN from WAL");
        }

        // Verify WAL received exactly 5 writes (one per page)
        thread::sleep(Duration::from_millis(150)); // Wait for auto-flush
        let flushed_lsn = wal_client.flushed_lsn();
        assert_eq!(
            flushed_lsn,
            initial_flushed_lsn + 5,
            "WAL should have flushed exactly 5 records (one per page)"
        );

        let _ = bg_wal.shutdown();
        let _ = bg_wal.join();
    }
}
