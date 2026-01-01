use std::time::Duration;

pub(crate) const HEAP_FILE_BUCKET_SIZE: usize = 16;

pub(crate) const CACHE_SIZE: usize = 4096;
/// Interval at which the in-memory cache is scanned for stale entries.
pub(crate) const CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);

/// Interval at which the files manager performs background cleanup (truncate files, removing unused pages).
pub(crate) const FILES_MANAGER_CLEANUP_INTERVAL: Duration = Duration::from_secs(300);

/// Interval for automatic WAL flush (in milliseconds).
pub(crate) const WAL_FLUSH_INTERVAL_MS: Duration = Duration::from_millis(100);

/// Maximum number of unflushed records before forcing a flush.
pub(crate) const WAL_MAX_UNFLUSHED_RECORDS: u64 = 1024;
