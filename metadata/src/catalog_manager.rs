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

pub struct CatalogManager {
    database_list: DatabaseList,
    main_directory_path: PathBuf,
}

impl CatalogManager {
    pub fn new() -> Result<Self, CatalogManagerError> {
        match ProjectDirs::from("", "", "coDB") {
            None => Err(CatalogManagerError::NoValidCatalogDirectory),
            Some(project_dir) => {
                let base_path = project_dir.data_local_dir().to_path_buf();
                if !base_path.exists() {
                    return Self::initialize_catalog(base_path);
                }

                let path = base_path.join(METADATA_FILE_NAME);

                let result = MetadataFileHelper::latest_catalog_json(&base_path, |path| {
                    DatabaseList::read_from_json(path)
                });

                match result {
                    Ok(database_list) => Ok(Self {
                        main_directory_path: base_path,
                        database_list,
                    }),
                    Err(CatalogManagerError::IoError(ref e))
                        if e.kind() == io::ErrorKind::NotFound =>
                    {
                        let database_list = DatabaseList {
                            names: HashSet::new(),
                        };
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
        self.database_list.names.insert(db_name.clone());

        let result = MetadataFileHelper::sync_to_disk::<DatabaseList, CatalogManagerError>(
            &self.metadata_path(),
            &self.database_list,
            |p| Ok(serde_json::to_string_pretty(p)?),
        );

        if let Err(e) = result {
            self.database_list.names.remove(&db_name);
            return Err(e);
        }

        if let Err(e) = self.initialize_database_metadata(&db_name) {
            self.database_list.names.remove(&db_name);
            let _ = MetadataFileHelper::sync_to_disk::<DatabaseList, CatalogManagerError>(
                &self.metadata_path(),
                &self.database_list,
                |p| Ok(serde_json::to_string_pretty(p)?),
            );
            return Err(e);
        }

        Ok(())
    }

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
        self.database_list.names.remove(&db_name);
        let result = MetadataFileHelper::sync_to_disk::<DatabaseList, CatalogManagerError>(
            &self.metadata_path(),
            &self.database_list,
            |p| Ok(serde_json::to_string_pretty(p)?),
        );

        if let Err(e) = result {
            self.database_list.names.insert(db_name);
            return Err(e);
        }

        let db_path = self.main_directory_path.join(&db_name);
        if db_path.exists() {
            fs::remove_dir_all(db_path)?;
        }

        Ok(())
    }

    fn metadata_path(&self) -> PathBuf {
        self.main_directory_path.join(METADATA_FILE_NAME)
    }
    pub fn list_databases(&self) -> DatabaseList {
        self.database_list.clone()
    }

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
