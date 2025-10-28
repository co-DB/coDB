//! FilesManager module — manages and distributes paged files in a single database.

use crate::paged_file::{PagedFile, PagedFileError};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
pub(crate) struct FileKey {
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
    pub(crate) fn data(table_name: impl Into<String>) -> Self {
        Self::new(table_name, FileType::Data)
    }

    /// Returns a key for the index file of the given table.
    pub(crate) fn index(table_name: impl Into<String>) -> Self {
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
    fn file_name(&self) -> String {
        format!("{}.{}", self.table_name, self.extension())
    }
}

/// Responsible for storing and distributing [`PagedFile`]s of a single database
/// to higher level components.
pub(crate) struct FilesManager {
    /// (Almost) All public api of [`PagedFile`] takes `&mut self`, so there is
    /// no point in using [`RwLock`] instead of [`Mutex`] here.
    open_files: DashMap<FileKey, Arc<Mutex<PagedFile>>>,
    base_path: PathBuf,
}

/// Error for [`FilesManager`] related operations
#[derive(Error, Debug)]
pub(crate) enum FilesManagerError {
    #[error("couldn't find the data directory")]
    DirectoryNotFound,
    #[error("paged file error: {0}")]
    PagedFileError(#[from] PagedFileError),
}

impl FilesManager {
    /// Creates a new [`FilesManager`] that handles files for a single database, whose name is passed to
    /// this function as an argument
    ///
    /// Can fail if the directory in which we want to store the data (refer to `docs/file_structure.md` for
    /// OS-specific details) doesn't exist.
    pub(crate) fn new<P>(base_path: P, database_name: &str) -> Result<Self, FilesManagerError>
    where
        P: AsRef<Path>,
    {
        let base_path = base_path.as_ref().join(database_name);
        if let Ok(exists) = base_path.try_exists()
            && exists
        {
            Ok(FilesManager {
                open_files: DashMap::new(),
                base_path,
            })
        } else {
            Err(FilesManagerError::DirectoryNotFound)
        }
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
        let file_path = self.base_path.join(&key.table_name).join(key.file_name());
        Ok(self
            .open_files
            .entry(key.clone())
            .or_insert(Arc::new(Mutex::new(PagedFile::new(file_path)?)))
            .clone())
    }

    /// Closes a file and removes its entry from the stored [`PagedFile`]s. Can be used for when
    /// a table is deleted or renamed.
    pub(crate) fn close_file(&mut self, key: &FileKey) {
        self.open_files.remove(key);
    }
}
