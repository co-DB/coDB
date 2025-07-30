//! FilesManager module — manages and distributes paged files in a single database.

use crate::paged_file::{PagedFile, PagedFileError};
use directories::ProjectDirs;
use std::collections::HashMap;
use std::path::PathBuf;
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
}

/// Responsible for storing and distributing [`PagedFile`]s of a single database
/// to higher level components.
///
/// As a singleton it allows the [`PagedFile`]s to persist beyond a single query and thus
/// eliminates the time needed to instantiate them each time.
pub struct FilesManager {
    open_files: HashMap<FileKey, PagedFile>,
    base_path: PathBuf,
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
    /// Creates a new [`FilesManager`] that handles files for a single database, whose name is passed to
    /// this function as an argument
    ///
    /// Can fail if the directory in which we want to store the data (refer to `docs/file_structure.md` for
    /// OS-specific details) doesn't exist.
    pub fn new(database_name: &str) -> Result<Self, FilesManagerError> {
        match ProjectDirs::from("", "", "CoDB") {
            None => Err(FilesManagerError::DirectoryNotFound),
            Some(project_dir) => {
                let base_path = project_dir
                    .data_local_dir()
                    .to_path_buf()
                    .join(database_name);
                Ok(FilesManager {
                    open_files: HashMap::new(),
                    base_path,
                })
            }
        }
    }

    /// Returns a [`PagedFile`] for a specific combination of table name and file type stored in
    /// FileKey or creates and stores it if one didn't exist beforehand.
    ///
    /// Can fail if [`PagedFile`] instantiation didn't succeed (refer to
    /// [`PagedFile`]'s implementation for more details)
    pub fn get_or_open_new_file(
        &mut self,
        key: FileKey,
    ) -> Result<&mut PagedFile, FilesManagerError> {
        let file_path = self.base_path.join(&key.table_name);
        Ok(self
            .open_files
            .entry(key)
            .or_insert(PagedFile::new(file_path)?))
    }

    /// Closes a file and removes its entry from the stored [`PagedFile`]s. Can be used for when
    /// a table is deleted or renamed.
    pub fn close_file(&mut self, key: &FileKey) {
        self.open_files.remove(key);
    }
}
