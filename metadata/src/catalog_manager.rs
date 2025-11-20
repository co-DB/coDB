use crate::consts::METADATA_FILE_NAME;
use crate::metadata_file_helper::MetadataFileHelper;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serde_json::error::Category::Data;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{fs, io};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogManagerError {
    #[error("no home directory found")]
    NoValidCatalogDirectory,
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
    #[error("json error occurred: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub struct CatalogManager {
    database_list: DatabaseList,
}

impl CatalogManager {
    fn new() -> Result<Self, CatalogManagerError> {
        match ProjectDirs::from("", "", "coDB") {
            None => Err(CatalogManagerError::NoValidCatalogDirectory),
            Some(project_dir) => {
                let base_path = project_dir.data_local_dir().to_path_buf();
                let path = base_path.join(METADATA_FILE_NAME);

                let result = MetadataFileHelper::latest_catalog_json(&path, |path| {
                    DatabaseList::read_from_json(path)
                })?;

                match result {
                    MaybeLoaded::Loaded(database_list) => Ok(Self { database_list }),
                    MaybeLoaded::NotFound => {
                        let list = DatabaseList { names: Vec::new() };
                        list.write_to_json(&path)?;
                        Ok(Self {
                            database_list: list,
                        })
                    }
                }
            }
        }
    }
}

enum MaybeLoaded {
    Loaded(DatabaseList),
    NotFound,
}

#[derive(Serialize, Deserialize)]
struct DatabaseList {
    pub names: Vec<String>,
}

impl DatabaseList {
    fn read_from_json(path: impl AsRef<Path>) -> Result<MaybeLoaded, CatalogManagerError> {
        match fs::read_to_string(path) {
            Ok(content) => {
                let value = serde_json::from_str(&content)?;
                Ok(MaybeLoaded::Loaded(value))
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(MaybeLoaded::NotFound),
            Err(err) => Err(err.into()),
        }
    }

    fn write_to_json(&self, path: impl AsRef<Path>) -> Result<(), CatalogManagerError> {
        let content = serde_json::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}
