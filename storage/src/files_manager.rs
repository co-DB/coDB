//! FilesManager module — manages and distributes paged files in a single database.

use crate::background_worker::{BackgroundWorker, BackgroundWorkerHandle};
use crate::paged_file::{PagedFile, PagedFileError};
use dashmap::DashMap;
use log::{error, info};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;
use thiserror::Error;

/// Represents possible file types inside a table directory (refer to `docs/file_structure.md` for more
/// details)
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum FileType {
    Data,
    Index,
}

/// Helper type created to make referring to files easier and cleaner.
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct FileKey {
    table_name: String,
    file_type: FileType,
}

impl FileKey {
    /// Creates a new key for identifying a specific file within a table.
    fn new(table_name: impl Into<String>, file_type: FileType) -> Self {
        FileKey {
            table_name: table_name.into(),
            file_type,
        }
    }

    /// Returns a key for the data file of the given table.
    pub fn data(table_name: impl Into<String>) -> Self {
        Self::new(table_name, FileType::Data)
    }

    /// Returns a key for the index file of the given table.
    pub fn index(table_name: impl Into<String>) -> Self {
        Self::new(table_name, FileType::Index)
    }

    /// Returns an extension of the file.
    fn extension(&self) -> &str {
        match self.file_type {
            FileType::Data => "tbl",
            FileType::Index => "idx",
        }
    }

    /// Returns a full name of the file. Refer to `docs/files_structure.md`.
    pub fn file_name(&self) -> String {
        format!("{}.{}", self.table_name, self.extension())
    }
}

/// Responsible for storing and distributing [`PagedFile`]s of a single database
/// to higher level components.
pub struct FilesManager {
    /// (Almost) All public api of [`PagedFile`] takes `&mut self`, so there is
    /// no point in using [`RwLock`] instead of [`Mutex`] here.
    open_files: DashMap<FileKey, Arc<Mutex<PagedFile>>>,
    database_path: PathBuf,
}

/// Error for [`FilesManager`] related operations
#[derive(Error, Debug)]
pub enum FilesManagerError {
    #[error("couldn't find the data directory")]
    DirectoryNotFound,
    #[error("paged file error: {0}")]
    PagedFileError(#[from] PagedFileError),
}

impl FilesManager {
    /// Creates a new [`FilesManager`] that handles files for a single database.
    ///
    /// Can fail if the directory in which we want to store the data (refer to `docs/file_structure.md` for
    /// OS-specific details) doesn't exist.
    pub fn new(database_path: impl AsRef<Path>) -> Result<Self, FilesManagerError> {
        if let Ok(exists) = database_path.as_ref().try_exists()
            && exists
        {
            Ok(FilesManager {
                open_files: DashMap::new(),
                database_path: database_path.as_ref().into(),
            })
        } else {
            Err(FilesManagerError::DirectoryNotFound)
        }
    }

    /// Creates new [`FilesManager`] with its [`BackgroundFilesManagerCleaner`]'s handle
    pub fn with_background_cleaner(
        database_path: impl AsRef<Path>,
        cleanup_interval: Duration,
    ) -> Result<(Arc<Self>, BackgroundWorkerHandle), FilesManagerError> {
        let files_manager = Arc::new(Self::new(database_path)?);
        let cleaner = BackgroundFilesManagerCleaner::start(BackgroundFilesManagerCleanerParams {
            files_manager: files_manager.clone(),
            cleanup_interval,
        });
        Ok((files_manager, cleaner))
    }

    /// Returns a [`Arc<Mutext<PagedFile>>`] for a specific combination of table name and file type stored in
    /// FileKey or creates and stores it if one didn't exist beforehand.
    ///
    /// Can fail if [`PagedFile`] instantiation didn't succeed (refer to
    /// [`PagedFile`]'s implementation for more details)
    pub(crate) fn get_or_open_new_file(
        &self,
        key: &FileKey,
    ) -> Result<Arc<Mutex<PagedFile>>, FilesManagerError> {
        let file_path = self
            .database_path
            .join(&key.table_name)
            .join(key.file_name());
        Ok(self
            .open_files
            .entry(key.clone())
            .or_insert(Arc::new(Mutex::new(PagedFile::new(file_path)?)))
            .clone())
    }

    /// Closes a file and removes its entry from the stored [`PagedFile`]s. Can be used for when
    /// a table is deleted or renamed.
    pub(crate) fn close_file(&self, key: &FileKey) {
        self.open_files.remove(key);
    }
}

/// Responsible for periodically scanning opened files in [`FilesManager`] and truncating them.
pub(crate) struct BackgroundFilesManagerCleaner {
    files_manager: Arc<FilesManager>,
    cleanup_interval: Duration,
    shutdown: mpsc::Receiver<()>,
}

pub(crate) struct BackgroundFilesManagerCleanerParams {
    files_manager: Arc<FilesManager>,
    cleanup_interval: Duration,
}

impl BackgroundWorker for BackgroundFilesManagerCleaner {
    type BackgroundWorkerParams = BackgroundFilesManagerCleanerParams;

    fn start(params: Self::BackgroundWorkerParams) -> BackgroundWorkerHandle {
        info!("Starting files manager background cleaner");
        let (tx, rx) = mpsc::channel();
        let cleaner = BackgroundFilesManagerCleaner {
            files_manager: params.files_manager,
            cleanup_interval: params.cleanup_interval,
            shutdown: rx,
        };
        let handle = thread::spawn(move || {
            cleaner.run();
        });
        BackgroundWorkerHandle::new(handle, tx)
    }
}

impl BackgroundFilesManagerCleaner {
    fn run(self) {
        loop {
            match self.shutdown.recv_timeout(self.cleanup_interval) {
                Ok(()) => {
                    // Got signal for shutdown.
                    info!("Shutting down files manager background cleaner");
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    info!("Files manager background cleaner - truncating files");
                    if let Err(e) = self.truncate_files() {
                        error!("failed to sync frames and lru: {e}")
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    // Sender dropped - trying to shutdown anyway.
                    info!(
                        "Shutting down files manager background cleaner (cancellation channel dropped)"
                    );
                    break;
                }
            }
        }
    }

    /// Iterate over all files currently opened.
    /// For each file, if we can instantly acquire the lock we truncate them (remove unused pages from the end of the file).
    fn truncate_files(&self) -> Result<(), FilesManagerError> {
        for file in &self.files_manager.open_files {
            if let Some(mut lock) = file.try_lock() {
                lock.truncate()?;
            }
        }
        Ok(())
    }
}
