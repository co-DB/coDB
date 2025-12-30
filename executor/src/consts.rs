use std::time::Duration;

pub(crate) const HEAP_FILE_BUCKET_SIZE: usize = 16;

pub(crate) const CACHE_SIZE: usize = 4096;
pub(crate) const CACHE_CLEANUP_INTERVAL: Duration = Duration::from_secs(120);
