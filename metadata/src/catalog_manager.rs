use crate::catalog::{Catalog, CatalogError, CatalogJson};
use crate::consts::METADATA_FILE_NAME;
use crate::metadata_file_helper::MetadataFileHelper;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::{fs, io};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogManagerError {
    #[error("no home directory found")]
    NoValidCatalogDirectory,
    #[error("the database {database_name} you are trying to create already exists")]
    DatabaseAlreadyExists { database_name: String },
    #[error("the database {database_name} you are trying to access does not exist")]
    DatabaseDoesntExist { database_name: String },
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
    #[error("json error occurred: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("error occurred while opening catalog: {0}")]
    CatalogError(#[from] CatalogError),
}

/// Manages the catalog of databases in the coDB system.
///
/// The `CatalogManager` is responsible for:
/// - Creating and deleting databases
/// - Maintaining a list of all databases in the system
/// - Persisting database metadata to disk
/// - Opening catalogs for individual databases
///
/// All database metadata is stored in the system's local data directory,
/// determined by the platform-specific conventions (e.g., `AppData/Local` on Windows).
pub struct CatalogManager {
    /// In-memory list of all database names in the system.
    database_list: DatabaseList,
    /// Path to the main directory where the main metadata and all database directories are stored.
    main_directory_path: PathBuf,
}

impl CatalogManager {
    /// Creates a new `CatalogManager` instance.
    ///
    /// This method:
    /// 1. Determines the platform-specific data directory for coDB
    /// 2. Creates the directory structure if it doesn't exist
    /// 3. Loads the list of existing databases from disk
    /// 4. Initializes a new catalog if no existing data is found
    pub fn new() -> Result<Self, CatalogManagerError> {
        match ProjectDirs::from("", "", "coDB") {
            None => Err(CatalogManagerError::NoValidCatalogDirectory),
            Some(project_dir) => {
                let base_path = project_dir.data_local_dir().to_path_buf();

                // If the directory doesn't exist yet, initialize a fresh catalog
                if !base_path.exists() {
                    return Self::initialize_catalog(base_path);
                }

                // Try to load existing database list from disk
                let result = MetadataFileHelper::latest_catalog_json(&base_path, |path| {
                    DatabaseList::read_from_json(path)
                });

                match result {
                    Ok(database_list) => Ok(Self {
                        main_directory_path: base_path,
                        database_list,
                    }),
                    // If metadata file doesn't exist, create an empty catalog
                    Err(CatalogManagerError::IoError(ref e))
                        if e.kind() == io::ErrorKind::NotFound =>
                    {
                        let database_list = DatabaseList {
                            names: HashSet::new(),
                        };
                        let path = base_path.join(METADATA_FILE_NAME);
                        database_list.write_to_json(&path)?;
                        Ok(Self {
                            main_directory_path: base_path,
                            database_list,
                        })
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Initializes a new catalog directory structure.
    ///
    /// Creates the base directory and an empty database list file.
    /// This is called when no existing catalog is found.
    fn initialize_catalog(base_path: PathBuf) -> Result<Self, CatalogManagerError> {
        fs::create_dir_all(&base_path)?;
        let path = base_path.join(METADATA_FILE_NAME);
        let initial_list = DatabaseList {
            names: HashSet::new(),
        };
        initial_list.write_to_json(&path)?;
        Ok(Self {
            main_directory_path: base_path,
            database_list: initial_list,
        })
    }

    /// Creates a new database with the given name.
    ///
    /// This operation follows a specific order to ensure consistency:
    /// 1. Create the database directory and metadata files on disk
    /// 2. Add the database name to the in-memory list
    /// 3. Persist the updated list to disk
    ///
    /// If step 3 fails, the in-memory state is rolled back and orphaned
    /// files are cleaned up.
    pub fn create_database(
        &mut self,
        database_name: impl Into<String>,
    ) -> Result<(), CatalogManagerError> {
        let db_name = database_name.into();
        if self.database_list.names.contains(&db_name) {
            return Err(CatalogManagerError::DatabaseAlreadyExists {
                database_name: db_name,
            });
        }

        // Step 1: Create database directory and metadata files
        // If this fails, nothing has changed yet - clean failure
        self.initialize_database_metadata(&db_name)?;

        // Step 2: Update in-memory state
        self.database_list.names.insert(db_name.clone());

        // Step 3: Persist the updated database list to disk
        if let Err(e) = MetadataFileHelper::sync_to_disk::<DatabaseList, CatalogManagerError>(
            &self.metadata_path(),
            &self.database_list,
            |p| Ok(serde_json::to_string_pretty(p)?),
        ) {
            // Rollback: Remove from in-memory list
            self.database_list.names.remove(&db_name);

            // Cleanup: Remove orphaned database directory
            let db_path = self.main_directory_path.join(&db_name);
            let _ = fs::remove_dir_all(db_path);

            return Err(e);
        }

        Ok(())
    }

    /// Initializes the metadata structure for a new database.
    ///
    /// Creates:
    /// - A directory for the database
    /// - An initial metadata file with default catalog structure
    fn initialize_database_metadata(
        &self,
        database_name: impl AsRef<str>,
    ) -> Result<(), CatalogManagerError> {
        let db_path = self.main_directory_path.join(database_name.as_ref());
        fs::create_dir_all(&db_path)?;

        let metadata_path = db_path.join(METADATA_FILE_NAME);

        let catalog_json = CatalogJson::default();
        catalog_json.write_to_json(&metadata_path)?;

        Ok(())
    }

    /// Deletes a database and all its associated data.
    ///
    /// This operation:
    /// 1. Removes the database from the in-memory list
    /// 2. Persists the updated list to disk
    /// 3. Deletes the database directory and all its contents
    ///
    /// The order prioritizes metadata consistency: the database is marked
    /// as deleted before files are removed. If file deletion fails, the
    /// database is still considered deleted (orphaned files remain but
    /// are inaccessible).
    pub fn delete_database(
        &mut self,
        database_name: impl Into<String>,
    ) -> Result<(), CatalogManagerError> {
        let db_name = database_name.into();
        if !self.database_list.names.contains(&db_name) {
            return Err(CatalogManagerError::DatabaseDoesntExist {
                database_name: db_name,
            });
        }

        // Step 1: Remove from in-memory list
        self.database_list.names.remove(&db_name);

        // Step 2: Persist the change
        let result = MetadataFileHelper::sync_to_disk::<DatabaseList, CatalogManagerError>(
            &self.metadata_path(),
            &self.database_list,
            |p| Ok(serde_json::to_string_pretty(p)?),
        );

        if let Err(e) = result {
            // Rollback: Restore the database name
            self.database_list.names.insert(db_name);
            return Err(e);
        }

        // Step 3: Delete the physical files
        // If this fails, the database is still marked as deleted
        // (orphaned files remain but are inaccessible)
        let db_path = self.main_directory_path.join(&db_name);
        if db_path.exists() {
            fs::remove_dir_all(db_path)?;
        }

        Ok(())
    }

    /// Returns the path to the main metadata file.
    fn metadata_path(&self) -> PathBuf {
        self.main_directory_path.join(METADATA_FILE_NAME)
    }

    /// Returns the main directory path.
    pub fn main_directory_path(&self) -> PathBuf {
        self.main_directory_path.clone()
    }

    /// Returns a list of all databases in the system.
    pub fn list_databases(&self) -> DatabaseList {
        self.database_list.clone()
    }

    /// Opens a catalog for the specified database.
    pub fn open_catalog(
        &self,
        database_name: impl AsRef<str>,
    ) -> Result<Catalog, CatalogManagerError> {
        if !self.database_list.names.contains(database_name.as_ref()) {
            return Err(CatalogManagerError::DatabaseDoesntExist {
                database_name: database_name.as_ref().to_string(),
            });
        }
        Ok(Catalog::new(&self.main_directory_path, database_name)?)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DatabaseList {
    pub names: HashSet<String>,
}

impl DatabaseList {
    fn read_from_json(path: impl AsRef<Path>) -> Result<DatabaseList, CatalogManagerError> {
        match fs::read_to_string(path) {
            Ok(content) => {
                let value = serde_json::from_str(&content)?;
                Ok(value)
            }
            Err(e) => Err(CatalogManagerError::IoError(e)),
        }
    }

    fn write_to_json(&self, path: impl AsRef<Path>) -> Result<(), CatalogManagerError> {
        let content = serde_json::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // Helper to create a test CatalogManager with a temporary directory
    struct TestContext {
        temp_dir: TempDir,
        manager: CatalogManager,
    }

    impl CatalogManager {
        fn new_with_path(base_path: &Path) -> Result<Self, CatalogManagerError> {
            let path = base_path.join(METADATA_FILE_NAME);

            let result = MetadataFileHelper::latest_catalog_json(base_path, |path| {
                DatabaseList::read_from_json(path)
            });

            match result {
                Ok(database_list) => Ok(Self {
                    main_directory_path: base_path.into(),
                    database_list,
                }),
                Err(CatalogManagerError::IoError(ref e)) if e.kind() == io::ErrorKind::NotFound => {
                    let database_list = DatabaseList {
                        names: HashSet::new(),
                    };
                    database_list.write_to_json(&path)?;
                    Ok(Self {
                        main_directory_path: base_path.into(),
                        database_list,
                    })
                }
                Err(e) => Err(e),
            }
        }
    }
    impl TestContext {
        fn new() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let manager = CatalogManager::new_with_path(temp_dir.path()).unwrap();
            Self { temp_dir, manager }
        }

        fn metadata_path(&self) -> PathBuf {
            self.temp_dir.path().join(METADATA_FILE_NAME)
        }

        fn db_path(&self, name: &str) -> PathBuf {
            self.temp_dir.path().join(name)
        }
    }

    #[test]
    fn test_new_creates_empty_catalog() {
        let ctx = TestContext::new();
        assert!(ctx.manager.list_databases().names.is_empty());
        assert!(ctx.metadata_path().exists());
    }

    #[test]
    fn test_create_database_success() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("test_db").unwrap();

        assert!(ctx.manager.list_databases().names.contains("test_db"));
        assert!(ctx.db_path("test_db").exists());
        assert!(ctx.metadata_path().exists());
    }

    #[test]
    fn test_create_database_already_exists() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("test_db").unwrap();
        let result = ctx.manager.create_database("test_db");

        assert!(matches!(
            result,
            Err(CatalogManagerError::DatabaseAlreadyExists { .. })
        ));
    }

    #[test]
    fn test_delete_database_success() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("test_db").unwrap();
        ctx.manager.delete_database("test_db").unwrap();

        assert!(!ctx.manager.list_databases().names.contains("test_db"));
        assert!(!ctx.db_path("test_db").exists());
    }

    #[test]
    fn test_delete_database_doesnt_exist() {
        let mut ctx = TestContext::new();

        let result = ctx.manager.delete_database("nonexistent");

        assert!(matches!(
            result,
            Err(CatalogManagerError::DatabaseDoesntExist { .. })
        ));
    }

    #[test]
    fn test_list_databases_empty() {
        let ctx = TestContext::new();
        let list = ctx.manager.list_databases();
        assert_eq!(list.names.len(), 0);
    }

    #[test]
    fn test_list_databases_multiple() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("db1").unwrap();
        ctx.manager.create_database("db2").unwrap();
        ctx.manager.create_database("db3").unwrap();

        let list = ctx.manager.list_databases();
        assert_eq!(list.names.len(), 3);
        assert!(list.names.contains("db1"));
        assert!(list.names.contains("db2"));
        assert!(list.names.contains("db3"));
    }

    #[test]
    fn test_persistence_across_instances() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        {
            let mut manager = CatalogManager::new_with_path(&path).unwrap();
            manager.create_database("persistent_db").unwrap();
        }

        {
            let manager = CatalogManager::new_with_path(&path).unwrap();
            assert!(manager.list_databases().names.contains("persistent_db"));
        }
    }

    #[test]
    fn test_corrupted_metadata_file() {
        let temp_dir = TempDir::new().unwrap();
        let metadata_path = temp_dir.path().join(METADATA_FILE_NAME);

        fs::write(&metadata_path, "{ invalid json ").unwrap();

        let result = CatalogManager::new_with_path(temp_dir.path());
        assert!(matches!(
            result.err().unwrap(),
            CatalogManagerError::JsonError(_)
        ));
    }

    #[test]
    fn test_open_catalog_success() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("test_db").unwrap();
        let result = ctx.manager.open_catalog("test_db");

        assert!(result.is_ok());
    }

    #[test]
    fn test_open_catalog_nonexistent() {
        let ctx = TestContext::new();

        let result = ctx.manager.open_catalog("nonexistent");

        assert!(matches!(
            result,
            Err(CatalogManagerError::DatabaseDoesntExist { .. })
        ));
    }

    #[test]
    fn test_open_catalog_after_delete() {
        let mut ctx = TestContext::new();

        ctx.manager.create_database("test_db").unwrap();
        ctx.manager.delete_database("test_db").unwrap();

        let result = ctx.manager.open_catalog("test_db");
        assert!(matches!(
            result,
            Err(CatalogManagerError::DatabaseDoesntExist { .. })
        ));
    }
}
