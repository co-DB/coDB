use crate::consts::METADATA_FILE_NAME;
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub enum CatalogManagerError {
    NoValidCatalogDirectory,
}
pub struct CatalogManager {
    database_list: DatabaseList,
    path: PathBuf,
}

impl CatalogManager {
    fn new() -> Result<Self, CatalogManagerError> {
        match ProjectDirs::from("", "", "coDB") {
            None => Err(CatalogManagerError::NoValidCatalogDirectory),
            Some(project_dir) => {
                let base_path = project_dir.data_local_dir().to_path_buf();
                let path = base_path.join(METADATA_FILE_NAME);

                Ok(CatalogManager {
                    database_list: DatabaseList { names: Vec::new() },
                    path,
                })
            }
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct DatabaseList {
    pub names: Vec<String>,
}

impl DatabaseList {}
