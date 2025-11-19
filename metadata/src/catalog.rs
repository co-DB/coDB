//! Catalog module - manages tables metadata.

use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time,
};

use crate::consts::METADATA_FILE_NAME;
use crate::metadata_file_helper::MetadataFileHelper;
use crate::types::Type;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// [`Catalog`] is an in-memory structure that holds information about a database's tables.
/// It maps to the underlying file `{PATH_TO_CODB}/{DATABASE_NAME}/{METADATA_FILE_NAME}`.
/// The on-disk file format is JSON.
///
/// [`Catalog`] is created once at database startup. It is assumed that the number of tables and columns
/// is small enough that [`Catalog`] can be used as an in-memory data structure.
#[derive(Debug)]
pub struct Catalog {
    /// Path to underlying file
    file_path: PathBuf,
    /// Maps each table name to its metadata. Stores all tables from database.
    tables: HashMap<String, TableMetadata>,
}

/// Error for [`Catalog`] related operations
#[derive(Error, Debug)]
pub enum CatalogError {
    /// Table with provided name does not exist in `tables`
    #[error("table '{0}' not found")]
    TableNotFound(String),
    /// Table with provided name already exists in `tables`
    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),
    /// Underlying IO module returned error
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
    /// File contains invalid json
    #[error("json error occurred: {0}")]
    JsonError(#[from] serde_json::Error),
    /// While creating new [`Catalog`] table returned error
    #[error("table returned error: {0}")]
    TableError(#[from] TableMetadataError),
}

// TODO: Add updating columns
// TODO (2): Make table level api (column CRUD) callable from Catalog level
impl Catalog {
    /// Creates new instance of [`Catalog`] for database `database_name`.
    /// Can fail if database does not exist or io error occurs.
    pub fn new<P>(main_dir_path: P, database_name: &str) -> Result<Self, CatalogError>
    where
        P: AsRef<Path>,
    {
        let catalog_json = MetadataFileHelper::latest_catalog_json(
            &main_dir_path.as_ref().join(database_name),
            |path| CatalogJson::read_from_file(path),
        )?;
        let tables = catalog_json
            .tables
            .into_iter()
            .map(|t| {
                let name = t.name.clone();
                TableMetadata::try_from(t).map(|tm| (name, tm))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        let file_path = main_dir_path
            .as_ref()
            .join(database_name)
            .join(METADATA_FILE_NAME);
        Ok(Catalog { file_path, tables })
    }

    /// Returns table with `table_name` name.
    /// Can fail if table with `table_name` name does not exist.
    pub fn table(&self, table_name: &str) -> Result<TableMetadata, CatalogError> {
        self.tables
            .get(table_name)
            .ok_or(CatalogError::TableNotFound(table_name.into()))
            .cloned()
    }

    /// Adds `table` to list of tables in the catalog.
    /// IMPORTANT NOTE: this function is purely for changing contents of metadata file. It is NOT responsible for managing table related files (e.g. creating new b-tree).
    /// Can fail if table with same name already exists.
    pub fn add_table(&mut self, table: TableMetadata) -> Result<(), CatalogError> {
        let already_exists = self.tables.contains_key(&table.name);
        match already_exists {
            true => Err(CatalogError::TableAlreadyExists(table.name)),
            false => {
                self.tables.insert(table.name.clone(), table);
                Ok(())
            }
        }
    }

    /// Removes table with `table_name` name from list of tables in the catalog.
    /// IMPORTANT NOTE: this function is purely for changing contents of metadata file. It is NOT responsible for managing table related files (e.g. removing folder `.{PATH_TO_CODB}/{DATABASE_NAME}/{TABLE_NAME}` and its files).
    /// Can fail if table with `table_name` does not exist.
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        self.tables
            .remove(table_name)
            .ok_or(CatalogError::TableNotFound(table_name.into()))
            .map(|_| ())
    }

    /// Syncs in-memory [`Catalog`] instance with underlying file.
    /// Can fail if io error occurs.
    pub fn sync_to_disk(&mut self) -> Result<(), CatalogError> {
        MetadataFileHelper::sync_to_disk(&self.file_path, self, |catalog| {
            let catalog_json = CatalogJson::from(catalog);
            Ok(serde_json::to_string_pretty(&catalog_json)?)
        })
    }
}

/// [`TableMetadata`] stores the metadata for a single table.
#[derive(Debug, Clone)]
pub struct TableMetadata {
    name: String,
    /// All table's columns sorted by their position in the disk layout.
    columns: Vec<ColumnMetadata>,
    /// Maps each column name to its metadata.
    columns_by_name: HashMap<String, usize>,
    /// Name of the column that is table's primary key
    primary_key_column_name: String,
}

/// Error for [`TableMetadata`] related operations
#[derive(Error, Debug)]
pub enum TableMetadataError {
    /// While creating [`TableMetadata`] there were more than one column with the same name in `columns`
    #[error("column '{0}' was defined more than once")]
    DuplicatedColumn(String),
    /// While creating [`TableMetadata`] `primary_key_column_name` was set to column which name does not appear in `columns`
    #[error("unknown primary key column: {0}")]
    UnknownPrimaryKeyColumn(String),
    /// Column with provided name does not exist in `columns`
    #[error("column '{0}' not found")]
    ColumnNotFound(String),
    /// Column with provided names already exists in `columns`
    #[error("column '{0}' already exists")]
    ColumnAlreadyExists(String),
    /// Invalid column was used for operation, e.g. tried to remove primary key column
    #[error("column '{0}' cannot be used in that context")]
    InvalidColumnUsed(String),
    /// While creating new [`TableMetadata`] column returned error
    #[error("column returned error: {0}")]
    ColumnError(#[from] ColumnMetadataError),
}

impl TableMetadata {
    /// Creates new [`TableMetadata`].
    /// Can fail if the columns slice contains more than one column with the same name, or if `primary_key_column_name` is not in `columns`.
    pub fn new(
        name: impl Into<String>,
        columns: &[ColumnMetadata],
        primary_key_column_name: impl Into<String>,
    ) -> Result<Self, TableMetadataError> {
        let mut table_columns = Vec::with_capacity(columns.len());
        let mut table_columns_by_name = HashMap::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            if table_columns_by_name.contains_key(&column.name) {
                return Err(TableMetadataError::DuplicatedColumn(column.name.clone()));
            }
            table_columns.push(column.clone());
            table_columns_by_name.insert(column.name.clone(), idx);
        }

        let primary_key_column_name = primary_key_column_name.into();
        if !table_columns_by_name.contains_key(&primary_key_column_name) {
            return Err(TableMetadataError::UnknownPrimaryKeyColumn(
                primary_key_column_name,
            ));
        }

        Ok(TableMetadata {
            name: name.into(),
            columns: table_columns,
            columns_by_name: table_columns_by_name,
            primary_key_column_name,
        })
    }

    /// Returns column metadata for column with `column_name`.
    /// Can fail if column with `column_name` does not exist.
    pub fn column(&self, column_name: &str) -> Result<ColumnMetadata, TableMetadataError> {
        self.columns_by_name
            .get(column_name)
            .map(|&idx| self.columns[idx].clone())
            .ok_or(TableMetadataError::ColumnNotFound(column_name.into()))
    }

    /// Returns metadata of each column stored in table sorted by columns position in disk layout.
    pub fn columns(&self) -> impl Iterator<Item = ColumnMetadata> {
        self.columns.iter().cloned()
    }

    /// Adds new column to the table.
    /// IMPORTANT NOTE: this function is not responsible for handling proper data migration after change of table layout. The only purpose of this function is to update underlying metadata file.
    /// Can fail if column with same name already exists.
    pub fn add_column(&mut self, column: ColumnMetadata) -> Result<(), TableMetadataError> {
        let already_exists = self.columns_by_name.contains_key(&column.name);
        match already_exists {
            true => Err(TableMetadataError::ColumnAlreadyExists(column.name)),
            false => {
                self.columns_by_name
                    .insert(column.name.clone(), self.columns.len());
                self.columns.push(column);
                Ok(())
            }
        }
    }

    /// Removes column from the table.
    /// IMPORTANT NOTE: this function is not responsible for handling proper data migration after change of table layout. The only purpose of this function is to update underlying metadata file.
    /// Can fail if column with provided name does not exist or the column is primary key.
    pub fn remove_column(&mut self, column_name: &str) -> Result<(), TableMetadataError> {
        if column_name == self.primary_key_column_name() {
            return Err(TableMetadataError::InvalidColumnUsed(column_name.into()));
        }
        let idx = self.columns_by_name.remove(column_name);
        match idx {
            Some(idx) => {
                self.columns.swap_remove(idx);
                // Removed last element - no need to update any index
                if idx == self.columns.len() {
                    return Ok(());
                }
                // Update index of the element that was moved
                let swapped_name = &self.columns[idx].name;
                self.columns_by_name.insert(swapped_name.clone(), idx);
                Ok(())
            }
            None => Err(TableMetadataError::ColumnNotFound(column_name.into())),
        }
    }

    /// Returns name of the table's primary key column
    pub fn primary_key_column_name(&self) -> &str {
        &self.primary_key_column_name
    }
}

/// [`ColumnMetadata`] stores the metadata for a single column.
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    name: String,
    ty: Type,
    /// Position of the column in the table's disk layout.
    /// For example, for layout:
    /// | colA | colB | colC |
    /// colA has pos 0, colB has 1 and colC has 2
    pos: u16,
    /// Fixed offset of the column in the record (including only fixed-size columns).
    /// In the on-disk record layout we store fixed-size types first, so if column is fixed-size type then `base_offset` is offset inside of the record.
    ///
    /// For example, for layout:
    /// | int32 | int32 | Bool |
    /// `base_offset` of the columns are: 0, 4, 8
    ///
    /// However, when having variable-size columns:
    /// | int32 | string | string|
    /// `base_offset` of the columns are: 0, 4, 4
    base_offset: usize,
    /// Position of the column used for calculating `base_offset`
    /// For fixed-size column `base_offset_pos = pos`.
    /// For variable-size column, `base_offset_pos` is the pos at which we should start calculating real offset.
    ///
    /// Invariant: `base_offset_pos <= pos`.
    base_offset_pos: u16,
}

/// Error for [`ColumnMetadata`] related operations
#[derive(Error, Debug)]
pub enum ColumnMetadataError {
    #[error("base_offset_pos ({0}) greater than pos ({1})")]
    InvalidBaseOffsetPosition(u16, u16),
}

impl ColumnMetadata {
    /// Creates new [`ColumnMetadata`]
    /// Can fail if `base_offset_pos > pos`.
    pub fn new(
        name: String,
        ty: Type,
        pos: u16,
        base_offset: usize,
        base_offset_pos: u16,
    ) -> Result<Self, ColumnMetadataError> {
        if base_offset_pos > pos {
            return Err(ColumnMetadataError::InvalidBaseOffsetPosition(
                base_offset_pos,
                pos,
            ));
        }
        Ok(ColumnMetadata {
            name,
            ty,
            pos,
            base_offset,
            base_offset_pos,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> Type {
        self.ty
    }

    pub fn pos(&self) -> u16 {
        self.pos
    }

    pub fn base_offset(&self) -> usize {
        self.base_offset
    }

    pub fn base_offset_pos(&self) -> u16 {
        self.base_offset_pos
    }
}

/// [`CatalogJson`] is a representation of [`Catalog`] on disk. Used only for serializing to/deserializing from JSON file.
#[derive(Serialize, Deserialize)]
struct CatalogJson {
    tables: Vec<TableJson>,
}

impl CatalogJson {
    pub fn read_from_file(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let content = fs::read_to_string(path)?;
        let catalog_json = serde_json::from_str(&content)?;
        Ok(catalog_json)
    }
}

impl From<&Catalog> for CatalogJson {
    fn from(value: &Catalog) -> Self {
        CatalogJson {
            tables: value.tables.values().map(TableJson::from).collect(),
        }
    }
}

/// [`TableJson`] is a representation of [`TableMetadata`] on disk. Used only for serializing to/deserializing from JSON file.
#[derive(Serialize, Deserialize)]
struct TableJson {
    name: String,
    columns: Vec<ColumnJson>,
    primary_key_column_name: String,
}

impl From<&TableMetadata> for TableJson {
    fn from(value: &TableMetadata) -> Self {
        TableJson {
            name: value.name.clone(),
            columns: value.columns.iter().map(ColumnJson::from).collect(),
            primary_key_column_name: value.primary_key_column_name.clone(),
        }
    }
}

impl TryFrom<TableJson> for TableMetadata {
    type Error = TableMetadataError;

    fn try_from(value: TableJson) -> Result<Self, Self::Error> {
        let columns: Vec<_> = value
            .columns
            .into_iter()
            .map(ColumnMetadata::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let tm = TableMetadata::new(value.name, &columns, value.primary_key_column_name)?;
        Ok(tm)
    }
}

/// [`ColumnJson`] is a representation of [`ColumnMetadata`] on disk. Used only for serializing to/deserializing from JSON file.
#[derive(Serialize, Deserialize)]
struct ColumnJson {
    name: String,
    ty: Type,
    pos: u16,
    base_offset: usize,
    base_offset_pos: u16,
}

impl From<&ColumnMetadata> for ColumnJson {
    fn from(value: &ColumnMetadata) -> Self {
        ColumnJson {
            name: value.name.clone(),
            ty: value.ty,
            pos: value.pos,
            base_offset: value.base_offset,
            base_offset_pos: value.base_offset_pos,
        }
    }
}

impl TryFrom<ColumnJson> for ColumnMetadata {
    type Error = ColumnMetadataError;

    fn try_from(value: ColumnJson) -> Result<Self, Self::Error> {
        ColumnMetadata::new(
            value.name,
            value.ty,
            value.pos,
            value.base_offset,
            value.base_offset_pos,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::*;
    use crate::consts::METADATA_FILE_NAME;
    use filetime::{FileTime, set_file_mtime};
    use serde::de::Error;
    use tempfile::NamedTempFile;

    // Helper to create a dummy column
    fn dummy_column(name: &str, ty: Type, pos: u16) -> ColumnMetadata {
        ColumnMetadata::new(name.to_string(), ty, pos, pos as usize * 4, pos).unwrap()
    }

    // Helper to check if column is as expected
    fn assert_column(expected: &ColumnMetadata, actual: &ColumnMetadata) {
        assert_eq!(expected.name, actual.name, "Column names differ");
        assert_eq!(expected.ty, actual.ty, "Column types differ");
        assert_eq!(expected.pos, actual.pos, "Column positions differ");
        assert_eq!(
            expected.base_offset, actual.base_offset,
            "Column base_offsets differ"
        );
        assert_eq!(
            expected.base_offset_pos, actual.base_offset_pos,
            "Column base_offset_pos differ"
        );
    }

    // Helper to create a dummy table
    fn dummy_table(name: &str, columns: &[ColumnMetadata], pk: &str) -> TableMetadata {
        TableMetadata::new(name, columns, pk).unwrap()
    }

    // Helper to create example users table
    fn users_table() -> TableMetadata {
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
        ];
        dummy_table("users", &columns, "id")
    }

    // Helper to check if table is as expected
    fn assert_table(expected: &TableMetadata, actual: &TableMetadata) {
        assert_eq!(expected.name, actual.name, "Table names differ");
        assert_eq!(
            expected.primary_key_column_name(),
            actual.primary_key_column_name(),
            "Primary key column names differ"
        );
        assert_eq!(
            expected.columns.len(),
            actual.columns.len(),
            "Number of columns differ"
        );
        for (ec, ac) in expected.columns.iter().zip(actual.columns.iter()) {
            assert_column(ec, ac);
        }
    }

    // Helper to create [`Catalog`] that will be used only in-memory
    fn in_memory_catalog(tables: HashMap<String, TableMetadata>) -> Catalog {
        let file = NamedTempFile::new().unwrap();
        Catalog {
            file_path: file.into_temp_path().to_path_buf(),
            tables,
        }
    }

    // Helper to create [`Catalog`] that will be used for on-disk operations
    fn file_backed_catalog(path: PathBuf, tables: HashMap<String, TableMetadata>) -> Catalog {
        let catalog_json = CatalogJson {
            tables: tables.values().map(TableJson::from).collect(),
        };
        let content = serde_json::to_string_pretty(&catalog_json).unwrap();
        std::fs::write(&path, content).unwrap();

        Catalog {
            file_path: path,
            tables,
        }
    }

    // Helper to write json to path
    fn write_json<P: AsRef<std::path::Path>>(path: P, json: &str) {
        std::fs::write(path, json).unwrap();
    }

    // Helper to create path for tmp file
    fn tmp_path(dir: &std::path::Path, db: &str, epoch: u64) -> std::path::PathBuf {
        dir.join(db)
            .join(format!("{}.tmp-{epoch}", METADATA_FILE_NAME))
    }

    // Helper to check if file contains expected json
    fn assert_file_json_eq<P: AsRef<std::path::Path>>(path: P, expected_json: &str) {
        let content = std::fs::read_to_string(path).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&content).unwrap(),
            serde_json::from_str::<serde_json::Value>(expected_json).unwrap()
        );
    }

    // Helper to check if no tmp files are in the directory
    fn assert_no_tmp_files(dir: &std::path::Path, db: &str) {
        let prefix = format!("{}.tmp-", METADATA_FILE_NAME);
        let db_dir = dir.join(db);
        for entry in fs::read_dir(&db_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.starts_with(&prefix),
                "tmp file {name} was not deleted"
            );
        }
    }

    // Helper to check if error variant is as expected
    fn assert_catalog_error_variant(actual: &CatalogError, expected: &CatalogError) {
        assert_eq!(
            std::mem::discriminant(actual),
            std::mem::discriminant(expected),
            "CatalogError variant does not match"
        );
    }

    #[test]
    fn catalog_new_returns_error_when_path_does_not_exist() {
        // given non-existent path
        let tmp_dir = tempfile::tempdir().unwrap();

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "nonexistent_db");

        // then Err(CatalogError::IoError) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::IoError(io::Error::new(io::ErrorKind::NotFound, "")),
        );
    }

    #[test]
    fn catalog_new_returns_error_when_file_is_not_json() {
        // given file with invalid json
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        std::fs::write(&db_path, b"not a json").unwrap();

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "db");

        // then Err(CatalogError::JsonError) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::JsonError(serde_json::Error::custom("")),
        );
    }

    #[test]
    fn catalog_new_returns_error_when_file_is_json_but_not_catalog_json() {
        // given file with valid json but not CatalogJson
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        std::fs::write(&db_path, b"{\"foo\": 123}").unwrap();

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "db");

        // then Err(CatalogError::JsonError) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::JsonError(serde_json::Error::custom("")),
        );
    }

    #[test]
    fn catalog_new_loads_proper_catalog_json() {
        // given file with valid CatalogJson
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let json = r#"
    {
        "tables": [
            {
                "name": "users",
                "columns": [
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                    { "name": "name", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                ],
                "primary_key_column_name": "id"
            },
            {
                "name": "posts",
                "columns": [
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                    { "name": "title", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                ],
                "primary_key_column_name": "id"
            },
            {
                "name": "comments",
                "columns": [
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                    { "name": "body", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                ],
                "primary_key_column_name": "id"
            }
        ]
    }
    "#;

        write_json(&db_path, json);

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "db");

        // then Catalog is returned and tables are loaded
        assert!(result.is_ok());
        let catalog = result.unwrap();
        assert_eq!(catalog.tables.len(), 3);
        assert!(catalog.tables.contains_key("users"));
        assert!(catalog.tables.contains_key("posts"));
        assert!(catalog.tables.contains_key("comments"));
    }

    #[test]
    fn catalog_new_picks_latest_tmp_over_main_file() {
        // given one tmp file with mtime > main and valid json
        let tmp_dir = tempfile::tempdir().unwrap();
        let db = "db";
        let db_dir = tmp_dir.path().join(db);
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let main_json = r#"{
    "tables":[
        {
            "name":"main",
            "columns":[
                { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
            ],
            "primary_key_column_name":"id"
        }
    ]
}"#;
        write_json(&db_path, main_json);

        let tmp_json = r#"{
    "tables":[
        {
            "name":"tmp",
            "columns":[
                { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
            ],
            "primary_key_column_name":"id"
        }
    ]
}"#;
        let tmp_epoch = 12345;
        let tmp_path = tmp_path(tmp_dir.path(), db, tmp_epoch);
        write_json(&tmp_path, tmp_json);

        // ensure tmp file mtime > main mtime
        let now = FileTime::from_system_time(SystemTime::now());
        set_file_mtime(&db_path, now).unwrap();
        let later = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(10));
        set_file_mtime(&tmp_path, later).unwrap();

        // when loading `Catalog`
        let catalog = Catalog::new(tmp_dir.path(), db).unwrap();

        // tmp file should be loaded
        assert!(catalog.tables.contains_key("tmp"));
        assert!(!catalog.tables.contains_key("main"));
        assert_no_tmp_files(tmp_dir.path(), db);

        // main file should now contain the newest tmp json
        assert_file_json_eq(&db_path, tmp_json);
    }

    #[test]
    fn catalog_new_picks_newest_of_multiple_tmp_files() {
        // given multiple tmp files with valid json
        let tmp_dir = tempfile::tempdir().unwrap();
        let db = "db";
        let db_dir = tmp_dir.path().join(db);
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let main_json = r#"{
        "tables":[
            {
                "name":"main",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;
        write_json(&db_path, main_json);

        let tmp_json1 = r#"{
        "tables":[
            {
                "name":"tmp1",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;
        let tmp_json2 = r#"{
        "tables":[
            {
                "name":"tmp2",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;

        let tmp_path1 = tmp_path(tmp_dir.path(), db, 100);
        let tmp_path2 = tmp_path(tmp_dir.path(), db, 200);
        write_json(&tmp_path1, tmp_json1);
        write_json(&tmp_path2, tmp_json2);

        // ensure proper mtimes
        let now = FileTime::from_system_time(SystemTime::now());
        set_file_mtime(&db_path, now).unwrap();
        let later1 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(10));
        set_file_mtime(&tmp_path1, later1).unwrap();
        let later2 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(20));
        set_file_mtime(&tmp_path2, later2).unwrap();

        // when loading `Catalog`
        let catalog = Catalog::new(tmp_dir.path(), db).unwrap();

        // newest tmp file should be loaded and renamed
        assert!(catalog.tables.contains_key("tmp2"));
        assert!(!catalog.tables.contains_key("main"));
        assert!(!catalog.tables.contains_key("tmp1"));
        assert_no_tmp_files(tmp_dir.path(), db);

        // main file should now contain the newest tmp json
        assert_file_json_eq(&db_path, tmp_json2);
    }

    #[test]
    fn catalog_new_picks_latest_valid_json_among_tmp_files() {
        // given multiple tmp files with only one in the middle with valid json
        let tmp_dir = tempfile::tempdir().unwrap();
        let db = "db";
        let db_dir = tmp_dir.path().join(db);
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let main_json = r#"{
        "tables":[
            {
                "name":"main",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;
        write_json(&db_path, main_json);

        let tmp_json_valid = r#"{
        "tables":[
            {
                "name":"valid",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;

        let tmp_path1 = tmp_path(tmp_dir.path(), db, 100);
        let tmp_path2 = tmp_path(tmp_dir.path(), db, 200);
        let tmp_path3 = tmp_path(tmp_dir.path(), db, 300);
        write_json(&tmp_path1, "not json");
        write_json(&tmp_path2, tmp_json_valid);
        write_json(&tmp_path3, "not json");

        // ensure proper mtimes
        let now = FileTime::from_system_time(SystemTime::now());
        set_file_mtime(&db_path, now).unwrap();
        let later1 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(10));
        set_file_mtime(&tmp_path1, later1).unwrap();
        let later2 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(20));
        set_file_mtime(&tmp_path2, later2).unwrap();
        let later3 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(30));
        set_file_mtime(&tmp_path3, later3).unwrap();

        // when loading `Catalog`
        let catalog = Catalog::new(tmp_dir.path(), db).unwrap();

        // valid tmp file should be loaded and renamed
        assert!(catalog.tables.contains_key("valid"));
        assert!(!catalog.tables.contains_key("main"));
        assert_no_tmp_files(tmp_dir.path(), db);

        // main file should now contain the valid tmp json
        assert_file_json_eq(&db_path, tmp_json_valid);
    }

    #[test]
    fn catalog_new_falls_back_to_main_file_if_no_valid_tmp() {
        // given multiple tmp files, none of them with valid json
        let tmp_dir = tempfile::tempdir().unwrap();
        let db = "db";
        let db_dir = tmp_dir.path().join(db);
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let main_json = r#"{
        "tables":[
            {
                "name":"main",
                "columns":[
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 }
                ],
                "primary_key_column_name":"id"
            }
        ]
    }"#;
        write_json(&db_path, main_json);

        let tmp_path1 = tmp_path(tmp_dir.path(), db, 100);
        let tmp_path2 = tmp_path(tmp_dir.path(), db, 200);
        write_json(&tmp_path1, "not json");
        write_json(&tmp_path2, "not json");

        // ensure proper mtimes
        let now = FileTime::from_system_time(SystemTime::now());
        set_file_mtime(&db_path, now).unwrap();
        let later1 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(10));
        set_file_mtime(&tmp_path1, later1).unwrap();
        let later2 = FileTime::from_system_time(SystemTime::now() + Duration::from_secs(20));
        set_file_mtime(&tmp_path2, later2).unwrap();

        // when loading `Catalog`
        let catalog = Catalog::new(tmp_dir.path(), db).unwrap();

        // main file should be loaded
        assert!(catalog.tables.contains_key("main"));
        assert_no_tmp_files(tmp_dir.path(), db);

        // main file should still contain the original json
        assert_file_json_eq(&db_path, main_json);
    }

    #[test]
    fn catalog_new_returns_error_when_column_is_invalid() {
        // given file with a column that has invalid base_offset_pos > pos
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let json = r#"
    {
        "tables": [
            {
                "name": "users",
                "columns": [
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 1 }
                ],
                "primary_key_column_name": "id"
            }
        ]
    }
    "#;
        write_json(&db_path, json);

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "db");

        // then error is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableError(TableMetadataError::ColumnError(
                ColumnMetadataError::InvalidBaseOffsetPosition(1, 0),
            )),
        );
    }

    #[test]
    fn catalog_new_returns_error_when_table_has_duplicate_column_names() {
        // given file with a table that has duplicate column names
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let json = r#"
    {
        "tables": [
            {
                "name": "users",
                "columns": [
                    { "name": "id", "ty": "I32", "pos": 0, "base_offset": 0, "base_offset_pos": 0 },
                    { "name": "id", "ty": "String", "pos": 1, "base_offset": 4, "base_offset_pos": 1 }
                ],
                "primary_key_column_name": "id"
            }
        ]
    }
    "#;
        write_json(&db_path, json);

        // when creating catalog
        let result = Catalog::new(&tmp_dir, "db");

        // then error is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableError(TableMetadataError::DuplicatedColumn(String::new())),
        );
    }

    #[test]
    fn catalog_table_returns_existing_table() {
        // given catalog with table `users`
        let users = users_table();
        let mut tables = HashMap::new();
        tables.insert(users.name.clone(), users.clone());

        let c = in_memory_catalog(tables);

        // when getting table with name `users`
        let result = c.table("users");

        // then table `users` is returned
        assert!(result.is_ok());
        assert_table(&users, &result.unwrap());
    }

    #[test]
    fn catalog_table_returns_error_when_missing_table() {
        // given empty catalog
        let tables = HashMap::new();
        let c = in_memory_catalog(tables);

        // when getting non existing table
        let result = c.table("missing");

        // then Err(CatalogError::TableNotFound) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableNotFound("missing".into()),
        );
    }

    #[test]
    fn catalog_add_table_adds_new_table() {
        // given empty catalog and a new table
        let tables = HashMap::new();
        let mut c = in_memory_catalog(tables);
        let users = users_table();

        // when adding table
        let result = c.add_table(users.clone());

        // then table is present
        assert!(result.is_ok());
        assert_table(&users, c.tables.get("users").unwrap());
    }

    #[test]
    fn catalog_add_table_returns_error_when_table_exists() {
        // given catalog with table `users`
        let users = users_table();
        let mut tables = HashMap::new();
        tables.insert(users.name.clone(), users.clone());
        let mut c = in_memory_catalog(tables);

        // when adding table with same name
        let result = c.add_table(users.clone());

        // then Err(CatalogError::TableAlreadyExists) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableAlreadyExists("users".into()),
        );
    }

    #[test]
    fn catalog_remove_table_removes_existing_table() {
        // given catalog with table `users`
        let users = users_table();
        let mut tables = HashMap::new();
        tables.insert(users.name.clone(), users);
        let mut c = in_memory_catalog(tables);

        // when removing table with name `users`
        let result = c.remove_table("users");

        // then Ok(()) is returned and table is removed
        assert!(result.is_ok());
        assert!(!c.tables.contains_key("users"));
    }

    #[test]
    fn catalog_sync_to_disk_saves_to_file() {
        // given a catalog with one table
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let users = users_table();
        let mut tables = HashMap::new();
        tables.insert(users.name.clone(), users.clone());

        let mut catalog = file_backed_catalog(db_path.clone(), tables);

        // when syncing to disk
        catalog.sync_to_disk().unwrap();

        // then file contains expected JSON
        let content = std::fs::read_to_string(&db_path).unwrap();
        let loaded: CatalogJson = serde_json::from_str(&content).unwrap();
        assert_eq!(loaded.tables.len(), 1);
        assert_eq!(loaded.tables[0].name, "users");
        assert_eq!(loaded.tables[0].columns.len(), 2);
        assert_eq!(loaded.tables[0].primary_key_column_name, "id");
    }

    #[test]
    fn catalog_sync_to_disk_after_adding_table() {
        // given a catalog with one table
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        std::fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);

        let users = users_table();
        let mut tables = HashMap::new();
        tables.insert(users.name.clone(), users.clone());

        let mut catalog = file_backed_catalog(db_path.clone(), tables);

        // sync initial state
        catalog.sync_to_disk().unwrap();

        // add another table
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("title", Type::String, 1),
        ];
        let posts = dummy_table("posts", &columns, "id");
        catalog.add_table(posts.clone()).unwrap();

        // sync again
        catalog.sync_to_disk().unwrap();

        // then file contains both tables
        let content = std::fs::read_to_string(&db_path).unwrap();
        let loaded: CatalogJson = serde_json::from_str(&content).unwrap();
        assert_eq!(loaded.tables.len(), 2);
        assert!(loaded.tables.iter().any(|t| t.name == "users"));
        assert!(loaded.tables.iter().any(|t| t.name == "posts"));
    }

    // Helper to check if error variant is as expected
    fn assert_table_metadata_error_variant(
        actual: &TableMetadataError,
        expected: &TableMetadataError,
    ) {
        assert_eq!(
            std::mem::discriminant(actual),
            std::mem::discriminant(expected),
            "TableMetadataError variant does not match"
        );
    }

    #[test]
    fn table_metadata_new_returns_error_on_duplicate_column_names() {
        // given two columns with the same name
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("id", Type::String, 1),
        ];
        // when creating new [`TableMetadata`]
        let result = TableMetadata::new("users", &columns, "id");
        // error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::DuplicatedColumn(String::new()),
        );
    }

    #[test]
    fn table_metadata_new_returns_error_on_invalid_primary_key_column() {
        // given two columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
        ];
        // when creating [`TableMetadata`] with unknown primary key column name
        let result = TableMetadata::new("users", &columns, "not_a_column");
        // error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::UnknownPrimaryKeyColumn(String::new()),
        );
    }

    #[test]
    fn table_metadata_new_returns_self_on_valid_input() {
        // given valid columns and primary key
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
        ];
        // when creating new [`TableMetadata`]
        let result = TableMetadata::new("users", &columns, "id");
        // [`TableMetadata`] is returned
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.primary_key_column_name(), "id");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.columns[1].name, "name");
    }

    #[test]
    fn table_metadata_column_returns_existing_column() {
        // given table with columns "id" and "name"
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
        ];
        let table = TableMetadata::new("users", &columns, "id").unwrap();

        // when getting column "name"
        let result = table.column("name");

        // then column is returned
        assert!(result.is_ok());
        assert_column(&columns[1], &result.unwrap());
    }

    #[test]
    fn table_metadata_column_returns_error_when_missing() {
        // given table with columns "id" and "name"
        let users = users_table();

        // when getting non-existing column
        let result = users.column("missing");

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::ColumnNotFound(String::new()),
        );
    }

    #[test]
    fn table_metadata_add_column_adds_new_column() {
        // given table with one column "id"
        let id_col = dummy_column("id", Type::I32, 0);
        let columns = vec![id_col.clone()];
        let mut table = TableMetadata::new("users", &columns, "id").unwrap();

        // when adding a new column "name"
        let new_col = dummy_column("name", Type::String, 1);
        let result = table.add_column(new_col.clone());

        // then column "name" is present
        assert!(result.is_ok());
        let col = table.column("name").unwrap();
        assert_column(&new_col, &col);
        // and column "id" still exists
        let col = table.column("id").unwrap();
        assert_column(&id_col, &col)
    }

    #[test]
    fn table_metadata_add_column_returns_error_when_column_exists() {
        // given table with column "id"
        let columns = vec![dummy_column("id", Type::I32, 0)];
        let mut table = TableMetadata::new("users", &columns, "id").unwrap();

        // when adding column with same name
        let duplicate_col = dummy_column("id", Type::I32, 1);
        let result = table.add_column(duplicate_col);

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::ColumnAlreadyExists(String::new()),
        );
    }

    #[test]
    fn table_metadata_remove_column_removes_last_column() {
        // given table with two columns
        let mut table = users_table();

        // when removing last column ("name")
        let result = table.remove_column("name");

        // then only "id" remains
        assert!(result.is_ok());
        assert!(table.column("name").is_err());
        assert!(table.column("id").is_ok());
        assert_eq!(table.columns.len(), 1);
    }

    #[test]
    fn table_metadata_remove_column_removes_middle_column() {
        // given table with three columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("middle", Type::String, 1),
            dummy_column("last", Type::Bool, 2),
        ];
        let mut table = TableMetadata::new("users", &columns, "id").unwrap();

        // when removing the "middle" column
        let result = table.remove_column("middle");

        // then "middle" is gone, "id" and "last" remain
        assert!(result.is_ok());
        assert!(table.column("middle").is_err());
        assert!(table.column("id").is_ok());
        assert!(table.column("last").is_ok());
        assert_eq!(table.columns.len(), 2);

        // and "last" is still accessible and correct
        let last_col = table.column("last").unwrap();
        assert_eq!(last_col.name, "last");
    }

    #[test]
    fn table_metadata_remove_column_returns_error_when_removing_primary_key() {
        // given table with primary key "id"
        let mut table = users_table();

        // when trying to remove primary key column
        let result = table.remove_column("id");

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::InvalidColumnUsed(String::new()),
        );
    }

    #[test]
    fn table_metadata_remove_column_returns_error_when_column_not_found() {
        // given table with two columns
        let mut table = users_table();

        // when trying to remove non-existing column
        let result = table.remove_column("not_found");

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::ColumnNotFound(String::new()),
        );
    }
}
