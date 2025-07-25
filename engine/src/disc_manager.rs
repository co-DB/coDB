use std::collections::HashMap;
use std::path::{PathBuf};
use crate::file_manager::{FileManager, FileManagerError};
use directories::ProjectDirs;
use thiserror::Error;

/// Represents possible file types inside a table directory (refer to file_structure.md for more 
/// details)
#[derive(Eq, PartialEq, Hash, Clone,Debug)]
enum FileType{
    Data,
    Index,
}

/// Helper type created to make referring to files easier and cleaner.
#[derive(Eq, Hash, PartialEq,Clone,Debug)]
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

/// Responsible for storing and distributing ['FileManager']s of a single database 
/// to higher level components.
///
/// As a singleton it allows the ['FileManager']s to persist beyond a single query and thus
/// eliminates the time needed to instantiate them each time.
struct DiscManager {
    open_files: HashMap<FileKey, FileManager>,
    base_path: PathBuf,
}

/// Error for ['DiscManager'] related operations
#[derive(Error, Debug)]
pub enum DiscManagerError{
    #[error("couldn't find the data directory")]
    DirectoryNotFound,
    #[error("file manager error: {0}")]
    FileManagerError(#[from] FileManagerError),
}

impl DiscManager {
    /// Creates a new DiscManager that handles files for a single database, whose name is passed to 
    /// this function as an argument 
    /// 
    /// Can fail if the directory in which we want to store the data (refer to file_structure.md for 
    /// OS-specific details) doesn't exist.
    pub fn new(database_name: &str) -> Result<Self, DiscManagerError> {
        match ProjectDirs::from("", "", "CoDB"){
            None => Err(DiscManagerError::DirectoryNotFound),
            Some(project_dir) => {
                let base_path = project_dir.data_local_dir().to_path_buf().join(database_name);
                Ok(DiscManager{
                    open_files: HashMap::new(),
                    base_path,
                })
            }
        }
    }

    /// Returns a file manager for a specific combination of table name and file type stored in 
    /// FileKey or creates and stores it if one didn't exist beforehand. 
    /// 
    /// Can fail if file manager instantiation didn't succeed (refer to 
    /// FileManager implementation file for more details)
    pub fn get_or_open_new_file(&mut self, key: FileKey) -> Result<&mut FileManager, DiscManagerError> {
        let file_path = self.base_path.join(&key.table_name);    
        Ok(self.open_files.entry(key).or_insert(FileManager::new(file_path)?))
    }
    
    /// Closes a file and removes its entry from the stored FileManagers. Can be used for when
    /// a table is deleted or renamed.
    pub fn close_file(&mut self, key: &FileKey) {
        self.open_files.remove(key);
    }

    #[cfg(test)]
    fn with_base_path(base_path: PathBuf) -> Self {
        Self {
            open_files: HashMap::new(),
            base_path,
        }
    }
}