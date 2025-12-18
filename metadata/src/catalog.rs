//! Catalog module - manages tables metadata.

use std::{
    collections::HashMap,
    fs::{self},
    io::{self},
    path::{Path, PathBuf},
};

use crate::consts::METADATA_FILE_NAME;
use crate::metadata_file_helper::MetadataFileHelper;

use serde::{Deserialize, Serialize};
use storage::files_manager::FileKey;
use thiserror::Error;
use types::schema::{self, Type};

/// [`Catalog`] is an in-memory structure that holds information about a database's tables.
/// It maps to the underlying file `{PATH_TO_CODB}/{DATABASE_NAME}/{METADATA_FILE_NAME}`.
/// The on-disk file format is JSON.
///
/// [`Catalog`] is created once at database startup. It is assumed that the number of tables and columns
/// is small enough that [`Catalog`] can be used as an in-memory data structure.
#[derive(Debug)]
pub struct Catalog {
    /// Path to database directory (where catalog file and tmp files are located)
    dir_path: PathBuf,
    /// Path to underlying catalog file
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

impl Catalog {
    /// Creates new instance of [`Catalog`] for database `database_name`.
    /// Can fail if database does not exist or io error occurs.
    pub fn new<P>(main_dir_path: P, database_name: impl AsRef<str>) -> Result<Self, CatalogError>
    where
        P: AsRef<Path>,
    {
        let db_dir = main_dir_path.as_ref().join(database_name.as_ref());
        let catalog_json = MetadataFileHelper::latest_catalog_json(&db_dir, |path| {
            CatalogJson::read_from_file(path)
        })?;
        let tables = catalog_json.tables()?;
        let file_path = db_dir.join(METADATA_FILE_NAME);
        Ok(Catalog {
            dir_path: db_dir,
            file_path,
            tables,
        })
    }

    /// Returns table with `table_name` name.
    /// Can fail if table with `table_name` name does not exist.
    pub fn table(&self, table_name: &str) -> Result<TableMetadata, CatalogError> {
        self.tables
            .get(table_name)
            .ok_or(CatalogError::TableNotFound(table_name.into()))
            .cloned()
    }

    /// Returns mutable reference to table. Only for internal usage.
    fn table_mut<'c>(
        &'c mut self,
        table_name: &str,
    ) -> Result<&'c mut TableMetadata, CatalogError> {
        self.tables
            .get_mut(table_name)
            .ok_or(CatalogError::TableNotFound(table_name.into()))
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
                self.sync_to_disk()?;
                Ok(())
            }
        }
    }

    /// Removes table with `table_name` name from list of tables in the catalog.
    /// Can fail if table with `table_name` does not exist.
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        self.tables
            .remove(table_name)
            .ok_or(CatalogError::TableNotFound(table_name.into()))
            .map(|_| ())?;
        self.delete_table_content_from_disk(table_name)?;
        self.sync_to_disk()?;
        Ok(())
    }

    /// Deletes `{PATH_TO_CODB}/{DATABASE_NAME}/{TABLE_NAME}` directory and all its content.
    pub fn delete_table_content_from_disk(&mut self, table_name: &str) -> Result<(), CatalogError> {
        let path_to_dir = self.dir_path.join(table_name);
        fs::remove_dir_all(path_to_dir)?;
        Ok(())
    }

    /// Renames table from `prev_name` to `new_name`.
    pub fn rename_table(&mut self, prev_name: &str, new_name: &str) -> Result<(), CatalogError> {
        // Removes table from in memory catalog
        let mut tm = self
            .tables
            .remove(prev_name)
            .ok_or(CatalogError::TableNotFound(prev_name.into()))?;

        // Rename table folder
        let old_dir = self.dir_path.join(prev_name);
        let new_dir = self.dir_path.join(new_name);
        fs::create_dir(&new_dir)?;

        // Rename idx file
        let old_idx = old_dir.join(FileKey::index(prev_name).file_name());
        let new_idx = new_dir.join(FileKey::index(new_name).file_name());
        if let Err(e) = fs::rename(&old_idx, &new_idx) {
            let _ = fs::remove_dir(new_dir);
            self.tables.insert(prev_name.into(), tm);
            return Err(e.into());
        }

        // Rename data file
        let old_tbl = old_dir.join(FileKey::data(prev_name).file_name());
        let new_tbl = new_dir.join(FileKey::data(new_name).file_name());
        if let Err(e) = fs::rename(old_tbl, new_tbl) {
            let _ = fs::rename(&new_idx, &old_idx);
            let _ = fs::remove_dir(new_dir);
            self.tables.insert(prev_name.into(), tm);
            return Err(e.into());
        }

        // We don't care if it fails
        let _ = fs::remove_dir(old_dir);

        tm.name = new_name.into();

        self.tables.insert(new_name.into(), tm);
        self.sync_to_disk()?;
        Ok(())
    }

    /// Adds column to the table.
    /// It works by delegating this to [`TableMetadata::add_column`].
    ///
    /// The purpose of this function is to have only one struct that can modify metadata - [`Catalog`].
    ///
    /// Changes made by this function are flushed to disk, but only to tmp file.
    /// To make sure that this is persisted to main file [`Catalog::commit_staged`] must be called.
    pub fn add_column(
        &mut self,
        table_name: impl AsRef<str>,
        column_request: NewColumnRequest,
    ) -> Result<NewColumnAdded, CatalogError> {
        let table: &mut TableMetadata = self.table_mut(table_name.as_ref())?;
        let new_column = table.add_column(column_request)?;
        self.stage_to_disk()?;
        Ok(new_column)
    }

    /// Removes column from the table.
    /// It works by delegating this to [`TableMetadata::remove_column`].
    ///
    /// The purpose of this function is to have only one struct that can modify metadata - [`Catalog`].
    ///
    /// Changes made by this function are flushed to disk, but only to tmp file.
    /// To make sure that this is persisted to main file [`Catalog::commit_staged`] must be called.
    pub fn remove_column(
        &mut self,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
    ) -> Result<ColumnRemoved, CatalogError> {
        let table = self.table_mut(table_name.as_ref())?;
        let cr = table.remove_column(column_name.as_ref())?;
        self.stage_to_disk()?;
        Ok(cr)
    }

    /// Commits the most recent tmp file by atomically renaming it to the main file.
    /// Removes all other tmp files.
    pub fn commit_staged(&self) -> Result<(), CatalogError> {
        let tmp_file_prefix = format!("{}.tmp-", METADATA_FILE_NAME);
        let mut tmp_files = MetadataFileHelper::list_catalog_tmp_files::<CatalogError>(
            &self.dir_path,
            &tmp_file_prefix,
        )?;

        if let Some(latest_tmp) = tmp_files.pop() {
            MetadataFileHelper::commit_tmp::<CatalogError>(&latest_tmp, &self.file_path)?;
        }

        MetadataFileHelper::remove_tmp_files::<CatalogError>(&mut tmp_files)?;

        Ok(())
    }

    /// Rolls back staged changes by removing all tmp files.
    pub fn rollback_staged(&mut self) -> Result<(), CatalogError> {
        let tmp_file_prefix = format!("{}.tmp-", METADATA_FILE_NAME);

        let mut tmp_files = MetadataFileHelper::list_catalog_tmp_files::<CatalogError>(
            &self.dir_path,
            &tmp_file_prefix,
        )?;

        MetadataFileHelper::remove_tmp_files::<CatalogError>(&mut tmp_files)?;

        let catalog_json = CatalogJson::read_from_file(&self.file_path)?;
        self.tables = catalog_json.tables()?;

        Ok(())
    }

    /// Syncs in-memory [`Catalog`] instance with underlying file.
    /// Can fail if io error occurs.
    fn sync_to_disk(&mut self) -> Result<(), CatalogError> {
        MetadataFileHelper::sync_to_disk(&self.file_path, self, |catalog| {
            let catalog_json = CatalogJson::from(catalog);
            catalog_json.to_pretty_string()
        })
    }

    /// Writes current in-memory state to a tmp file (does not atomically rename).
    fn stage_to_disk(&self) -> Result<(), CatalogError> {
        MetadataFileHelper::write_tmp(&self.file_path, self, |catalog| {
            let catalog_json = CatalogJson::from(catalog);
            catalog_json.to_pretty_string()
        })?;
        Ok(())
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

#[derive(Debug)]
pub struct NewColumnAdded {
    pub pos: u16,
    pub base_offset: usize,
    pub ty: Type,
}

#[derive(Debug)]
pub struct ColumnRemoved {
    pub pos: u16,
    pub ty: Type,
    pub prev_column_base_offset: usize,
}

impl TableMetadata {
    /// Creates new [`TableMetadata`].
    /// Can fail if the columns slice contains more than one column with the same name, or if `primary_key_column_name` is not in `columns`.
    ///
    /// Only for internal usage, other modules should use [`TableMetadataFactory`].
    fn new(
        name: impl Into<String>,
        columns: Vec<ColumnMetadata>,
        primary_key_column_name: impl Into<String>,
    ) -> Result<Self, TableMetadataError> {
        let mut table_columns_by_name = HashMap::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            if table_columns_by_name.contains_key(&column.name) {
                return Err(TableMetadataError::DuplicatedColumn(column.name.clone()));
            }
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
            columns,
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
    fn add_column(
        &mut self,
        column_request: NewColumnRequest,
    ) -> Result<NewColumnAdded, TableMetadataError> {
        let already_exists = self.columns_by_name.contains_key(&column_request.name);
        if already_exists {
            Err(TableMetadataError::ColumnAlreadyExists(column_request.name))
        } else {
            let pos = match column_request.ty.is_fixed_size() {
                true => {
                    self.add_fixed_size_column(column_request.name.clone(), column_request.ty)?
                }
                false => {
                    self.add_variable_size_column(column_request.name.clone(), column_request.ty)?
                }
            };
            self.columns_by_name.insert(column_request.name, pos);
            let base_offset = self.columns[pos].base_offset();
            Ok(NewColumnAdded {
                pos: pos as _,
                base_offset,
                ty: column_request.ty,
            })
        }
    }

    /// Adds fixed size column to list of columns. It works by adding column as the last fixed size column
    /// and updating metadata of every variable column.
    /// Returns index of added element in the list of columns.
    fn add_fixed_size_column(
        &mut self,
        name: impl Into<String>,
        ty: Type,
    ) -> Result<usize, TableMetadataError> {
        let last_fixed_size_pos = self
            .columns
            .iter()
            .enumerate()
            .rev()
            .find(|(_, col)| col.ty.is_fixed_size())
            .map(|(idx, _)| idx)
            .expect("at least one fixed-size column is always in a table");

        let last_fixed_size = &self.columns[last_fixed_size_pos];
        let last_fixed_size_on_disk = schema::type_size_on_disk(&last_fixed_size.ty)
            .expect("fixed size column must have size");
        let base_offset = self.columns[last_fixed_size_pos].base_offset() + last_fixed_size_on_disk;
        let cm = ColumnMetadata::new(
            name.into(),
            ty,
            last_fixed_size_pos as u16 + 1,
            base_offset,
            last_fixed_size.base_offset_pos() + 1,
        )?;

        let only_fixed_size_columns = last_fixed_size_pos == self.columns.len() - 1;
        match only_fixed_size_columns {
            true => {
                // We just append to the end - no need to fix any column
                self.columns.push(cm);
                let pos = self.columns.len() - 1;
                Ok(pos)
            }
            false => {
                let pos = last_fixed_size_pos + 1;
                self.columns.insert(pos, cm);
                self.recalculate_columns_metadata_from(pos + 1);
                Ok(pos)
            }
        }
    }

    /// Adds variable size column to list of columns by simply appending it to the end.
    /// Returns index of added element in the list of columns.
    fn add_variable_size_column(
        &mut self,
        name: impl Into<String>,
        ty: Type,
    ) -> Result<usize, TableMetadataError> {
        // We know columns.len() > 0, as we cannot have table with no columns.
        // Thus, every `columns[prev_pos]` should not panic.
        let pos = self.columns.len();
        let prev_pos = pos - 1;
        let (base_offset, base_offset_pos) =
            match schema::type_size_on_disk(&self.columns[prev_pos].ty) {
                Some(size) => {
                    // This is the first variable size column in the table
                    (
                        self.columns[prev_pos].base_offset() + size,
                        prev_pos as u16 + 1,
                    )
                }
                None => {
                    // Previous column is also variable size, so we can copy base offset
                    (
                        self.columns[prev_pos].base_offset(),
                        self.columns[prev_pos].base_offset_pos(),
                    )
                }
            };
        let cm = ColumnMetadata::new(name.into(), ty, pos as _, base_offset, base_offset_pos)?;
        self.columns.push(cm);
        Ok(pos)
    }

    /// Iterates over [`TableMetadata::columns`] starting at `from` and recalculates metadata of each column
    /// Basically it's doing the same thing as in [`TableMetadataFactory::create_columns`], but only on the subset of columns.
    fn recalculate_columns_metadata_from(&mut self, from: usize) {
        let (mut pos, mut last_fixed_pos, mut base_offset) = match from > 0 {
            true => {
                let pos = from as u16;
                let prev = &self.columns[from - 1];
                let (last_fixed_pos, base_offset) = match schema::type_size_on_disk(&prev.ty) {
                    Some(size) => (from as u16, prev.base_offset() + size),
                    None => (prev.base_offset_pos(), prev.base_offset()),
                };
                (pos, last_fixed_pos, base_offset)
            }
            false => (0, 0, 0),
        };

        for cm in self.columns[from..].iter_mut() {
            cm.pos = pos;
            cm.base_offset = base_offset;
            cm.base_offset_pos = last_fixed_pos;

            self.columns_by_name.insert(cm.name.clone(), cm.pos as _);

            pos += 1;

            if let Some(size) = schema::type_size_on_disk(&cm.ty) {
                last_fixed_pos += 1;
                base_offset += size;
            }
        }
    }

    /// Removes column from the table.
    /// IMPORTANT NOTE: this function is not responsible for handling proper data migration after change of table layout. The only purpose of this function is to update underlying metadata file.
    /// Can fail if column with provided name does not exist or the column is primary key.
    fn remove_column(&mut self, column_name: &str) -> Result<ColumnRemoved, TableMetadataError> {
        if column_name == self.primary_key_column_name() {
            return Err(TableMetadataError::InvalidColumnUsed(column_name.into()));
        }
        let idx = self.columns_by_name.remove(column_name);
        match idx {
            Some(idx) => {
                let cm = self.columns.remove(idx);

                if self.columns.len() != idx {
                    // It was not the last column, we need to fix metadata of other columns that come after the deleted one
                    self.recalculate_columns_metadata_from(idx);
                }

                let pos = cm.pos();
                let prev_column_base_offset = if pos > 0 {
                    self.columns[(pos - 1) as usize].base_offset()
                } else {
                    0
                };

                Ok(ColumnRemoved {
                    pos,
                    ty: cm.ty(),
                    prev_column_base_offset,
                })
            }
            None => Err(TableMetadataError::ColumnNotFound(column_name.into())),
        }
    }

    /// Returns name of the table's primary key column
    pub fn primary_key_column_name(&self) -> &str {
        &self.primary_key_column_name
    }

    /// Returns the position of the primary key column
    pub fn primary_key_pos(&self) -> Result<u16, TableMetadataError> {
        Ok(self.column(&self.primary_key_column_name)?.pos)
    }
}

pub struct NewColumnRequest {
    pub name: String,
    pub ty: Type,
}

/// Structure for creating new [`TableMetadata`].
pub struct TableMetadataFactory {
    column_requests: Vec<NewColumnRequest>,
    name: String,
    primary_key_column_name: String,
    columns: Vec<ColumnMetadata>,
    columns_by_name: HashMap<String, usize>,
}

impl TableMetadataFactory {
    pub fn new(
        name: impl Into<String>,
        column_requests: Vec<NewColumnRequest>,
        primary_key_column_name: impl Into<String>,
    ) -> Self {
        TableMetadataFactory {
            columns: Vec::with_capacity(column_requests.len()),
            columns_by_name: HashMap::with_capacity(column_requests.len()),
            column_requests,
            name: name.into(),
            primary_key_column_name: primary_key_column_name.into(),
        }
    }

    pub fn create_table_metadata(mut self) -> Result<TableMetadata, TableMetadataError> {
        self.create_columns()?;
        self.create_columns_by_name()?;
        self.primary_key_exists()?;
        let tm = TableMetadata {
            columns: self.columns,
            columns_by_name: self.columns_by_name,
            name: self.name,
            primary_key_column_name: self.primary_key_column_name,
        };
        Ok(tm)
    }

    /// Creates [`TableMetadataFactory::columns_by_name`] based on [`TableMetadataFactory::columns`]
    fn create_columns_by_name(&mut self) -> Result<(), TableMetadataError> {
        for (idx, column) in self.columns.iter().enumerate() {
            if self.columns_by_name.contains_key(&column.name) {
                return Err(TableMetadataError::DuplicatedColumn(column.name.clone()));
            }
            self.columns_by_name.insert(column.name.clone(), idx);
        }
        Ok(())
    }

    /// Checks if [`TableMetadataFactory::primary_key_column_name`] exists in [`TableMetadataFactory::columns`]
    fn primary_key_exists(&self) -> Result<(), TableMetadataError> {
        if !self
            .columns_by_name
            .contains_key(&self.primary_key_column_name)
        {
            return Err(TableMetadataError::UnknownPrimaryKeyColumn(
                self.primary_key_column_name.clone(),
            ));
        }
        Ok(())
    }

    /// Creates [`TableMetadataFactory::columns`] based on [`TableMetadataFactory::column_requests`]
    fn create_columns(&mut self) -> Result<(), TableMetadataError> {
        self.sort_column_requests_by_fixed_size();

        let mut last_fixed_pos = 0;
        let mut base_offset = 0;

        for (pos, col) in self.column_requests.iter().enumerate() {
            let column_metadata = ColumnMetadata::new(
                col.name.clone(),
                col.ty,
                pos as _,
                base_offset,
                last_fixed_pos,
            )?;
            self.columns.push(column_metadata);
            if let Some(offset) = schema::type_size_on_disk(&col.ty) {
                last_fixed_pos += 1;
                base_offset += offset;
            }
        }
        Ok(())
    }

    /// Sorts columns by whether they are fixed-size (fixed-size columns are first).
    fn sort_column_requests_by_fixed_size(&mut self) {
        self.column_requests.sort_unstable_by(|a, b| {
            let a_fixed = a.ty.is_fixed_size();
            let b_fixed = b.ty.is_fixed_size();
            b_fixed.cmp(&a_fixed)
        })
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
    /// However, when we have variable-size columns:
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
#[derive(Serialize, Deserialize, Default)]
pub(crate) struct CatalogJson {
    tables: Vec<TableJson>,
}

impl CatalogJson {
    /// Serialize to pretty JSON string.
    pub(crate) fn to_pretty_string(&self) -> Result<String, CatalogError> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub(crate) fn read_from_file(path: impl AsRef<Path>) -> Result<Self, CatalogError> {
        let content = fs::read_to_string(path)?;
        let catalog_json = serde_json::from_str(&content)?;
        Ok(catalog_json)
    }

    pub(crate) fn write_to_json(&self, path: impl AsRef<Path>) -> Result<(), CatalogError> {
        let content = self.to_pretty_string()?;
        fs::write(path, content)?;
        Ok(())
    }

    pub(crate) fn tables(self) -> Result<HashMap<String, TableMetadata>, CatalogError> {
        let tables = self
            .tables
            .into_iter()
            .map(|t| {
                let name = t.name.clone();
                TableMetadata::try_from(t).map(|tm| (name, tm))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;
        Ok(tables)
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
        let tm = TableMetadata::new(value.name, columns, value.primary_key_column_name)?;
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
    use std::{
        mem,
        time::{Duration, SystemTime},
    };

    use super::*;
    use crate::consts::METADATA_FILE_NAME;
    use filetime::{FileTime, set_file_mtime};
    use serde::de::Error;
    use tempfile::NamedTempFile;

    /// Test fixture that provides a file-backed catalog setup
    struct CatalogTestFixture {
        /// Temporary directory - must be kept alive for the duration of the test
        _tmp_dir: tempfile::TempDir,
        /// Path to the catalog metadata file
        db_path: PathBuf,
        /// The catalog instance
        catalog: Catalog,
    }

    impl CatalogTestFixture {
        /// Creates a new test fixture with an empty catalog
        fn new() -> Self {
            let tmp_dir = tempfile::tempdir().unwrap();
            let db_dir = tmp_dir.path().join("db");
            fs::create_dir(&db_dir).unwrap();
            let db_path = db_dir.join(METADATA_FILE_NAME);

            let tables = HashMap::new();
            let catalog = file_backed_catalog(db_path.clone(), tables);

            Self {
                _tmp_dir: tmp_dir,
                db_path,
                catalog,
            }
        }

        /// Creates a new test fixture with a catalog containing the given table
        fn with_table(table: TableMetadata) -> Self {
            let tmp_dir = tempfile::tempdir().unwrap();
            let db_dir = tmp_dir.path().join("db");
            fs::create_dir(&db_dir).unwrap();
            let db_path = db_dir.join(METADATA_FILE_NAME);

            let mut tables = HashMap::new();
            tables.insert(table.name.clone(), table);
            let catalog = file_backed_catalog(db_path.clone(), tables);

            Self {
                _tmp_dir: tmp_dir,
                db_path,
                catalog,
            }
        }

        /// Creates a new test fixture with an in-memory catalog (no disk I/O)
        fn in_memory() -> Self {
            let tmp_dir = tempfile::tempdir().unwrap();
            let tables = HashMap::new();
            let catalog = in_memory_catalog(tables);

            Self {
                _tmp_dir: tmp_dir,
                db_path: PathBuf::new(), // won't be used
                catalog,
            }
        }

        /// Creates a new test fixture with an in-memory catalog containing the given table
        fn in_memory_with_table(table: TableMetadata) -> Self {
            let tmp_dir = tempfile::tempdir().unwrap();
            let mut tables = HashMap::new();
            tables.insert(table.name.clone(), table);
            let catalog = in_memory_catalog(tables);

            Self {
                _tmp_dir: tmp_dir,
                db_path: PathBuf::new(), // won't be used
                catalog,
            }
        }

        /// Returns a reference to the catalog
        fn catalog(&self) -> &Catalog {
            &self.catalog
        }

        /// Returns a mutable reference to the catalog
        fn catalog_mut(&mut self) -> &mut Catalog {
            &mut self.catalog
        }

        /// Returns the path to the database metadata file
        fn db_path(&self) -> &Path {
            &self.db_path
        }
    }

    /// Helper to set up a database with main file and multiple tmp files
    struct TmpFileSetup {
        tmp_dir: tempfile::TempDir,
        db: String,
        db_path: PathBuf,
    }

    impl TmpFileSetup {
        fn new(db_name: &str, main_json: &str) -> Self {
            let tmp_dir = tempfile::tempdir().unwrap();
            let db_dir = tmp_dir.path().join(db_name);
            fs::create_dir(&db_dir).unwrap();
            let db_path = db_dir.join(METADATA_FILE_NAME);
            write_json(&db_path, main_json);

            Self {
                tmp_dir,
                db: db_name.to_string(),
                db_path,
            }
        }

        /// Adds a tmp file with the given epoch and content, setting its mtime
        fn add_tmp_file(&self, epoch: u64, content: &str, seconds_offset: u64) -> PathBuf {
            let tmp_path = tmp_path(self.tmp_dir.path(), &self.db, epoch);
            write_json(&tmp_path, content);
            let mtime =
                FileTime::from_system_time(SystemTime::now() + Duration::from_secs(seconds_offset));
            set_file_mtime(&tmp_path, mtime).unwrap();
            tmp_path
        }

        /// Sets the mtime of the main database file
        fn set_main_mtime(&self, seconds_offset: u64) {
            let mtime =
                FileTime::from_system_time(SystemTime::now() + Duration::from_secs(seconds_offset));
            set_file_mtime(&self.db_path, mtime).unwrap();
        }

        fn path(&self) -> &Path {
            self.tmp_dir.path()
        }

        fn db_name(&self) -> &str {
            &self.db
        }

        fn db_path(&self) -> &Path {
            &self.db_path
        }
    }

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
    fn dummy_table(name: &str, columns: Vec<ColumnMetadata>, pk: &str) -> TableMetadata {
        TableMetadata::new(name, columns, pk).unwrap()
    }

    // Helper to create example users table
    fn users_table() -> TableMetadata {
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
        ];
        dummy_table("users", columns, "id")
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
        let file_path = file.into_temp_path().to_path_buf();
        let dir_path = file_path.parent().unwrap().to_path_buf();
        Catalog {
            dir_path,
            file_path,
            tables,
        }
    }

    // Helper to create [`Catalog`] that will be used for on-disk operations
    fn file_backed_catalog(path: PathBuf, tables: HashMap<String, TableMetadata>) -> Catalog {
        let catalog_json = CatalogJson {
            tables: tables.values().map(TableJson::from).collect(),
        };
        let content = serde_json::to_string_pretty(&catalog_json).unwrap();
        fs::write(&path, content).unwrap();

        let dir_path = path.parent().unwrap().to_path_buf();
        Catalog {
            dir_path,
            file_path: path,
            tables,
        }
    }

    /// Helper to create a test database directory with a metadata file containing the given JSON
    fn setup_db_with_json(json: &str) -> (tempfile::TempDir, PathBuf) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let db_dir = tmp_dir.path().join("db");
        fs::create_dir(&db_dir).unwrap();
        let db_path = db_dir.join(METADATA_FILE_NAME);
        write_json(&db_path, json);
        (tmp_dir, db_path)
    }

    // Helper to write json to path
    fn write_json(path: impl AsRef<Path>, json: &str) {
        fs::write(path, json).unwrap();
    }

    // Helper to create path for tmp file
    fn tmp_path(dir: impl AsRef<Path>, db: &str, epoch: u64) -> PathBuf {
        dir.as_ref()
            .join(db)
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
    fn assert_no_tmp_files(db_dir: impl AsRef<Path>) {
        let prefix = format!("{}.tmp-", METADATA_FILE_NAME);
        for entry in fs::read_dir(&db_dir).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            assert!(
                !name.starts_with(&prefix),
                "tmp file {name} was not deleted"
            );
        }
    }

    /// Helper to assert that [`Catalog`] on disk is as expected
    fn assert_catalog_on_disk(
        db_path: &Path,
        expected_table_count: usize,
        table_assertions: impl FnOnce(&[TableJson]),
    ) {
        let content = fs::read_to_string(db_path).unwrap();
        let loaded: CatalogJson = serde_json::from_str(&content).unwrap();
        assert_eq!(loaded.tables.len(), expected_table_count);
        table_assertions(&loaded.tables);
    }

    // Helper to check if error variant is as expected
    fn assert_catalog_error_variant(actual: &CatalogError, expected: &CatalogError) {
        assert_eq!(
            mem::discriminant(actual),
            mem::discriminant(expected),
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
        let (tmp_dir, _) = setup_db_with_json("not a json");

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
        let json = r#"{"foo": 123}"#;
        let (tmp_dir, _) = setup_db_with_json(json);

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

        let (tmp_dir, _) = setup_db_with_json(json);

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

        let setup = TmpFileSetup::new("db", main_json);
        setup.set_main_mtime(0);
        setup.add_tmp_file(12345, tmp_json, 10);

        // when loading `Catalog`
        let catalog = Catalog::new(setup.path(), setup.db_name()).unwrap();

        // tmp file should be loaded
        assert!(catalog.tables.contains_key("tmp"));
        assert!(!catalog.tables.contains_key("main"));
        assert_no_tmp_files(setup.path().join(setup.db_name()));

        // main file should now contain the newest tmp json
        assert_file_json_eq(setup.db_path(), tmp_json);
    }

    #[test]
    fn catalog_new_picks_newest_of_multiple_tmp_files() {
        // given multiple tmp files with valid json
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

        let setup = TmpFileSetup::new("db", main_json);
        setup.set_main_mtime(0);
        setup.add_tmp_file(100, tmp_json1, 10);
        setup.add_tmp_file(200, tmp_json2, 20);

        // when loading `Catalog`
        let catalog = Catalog::new(setup.path(), setup.db_name()).unwrap();

        // newest tmp file should be loaded and renamed
        assert!(catalog.tables.contains_key("tmp2"));
        assert!(!catalog.tables.contains_key("main"));
        assert!(!catalog.tables.contains_key("tmp1"));
        assert_no_tmp_files(setup.path().join(setup.db_name()));

        // main file should now contain the newest tmp json
        assert_file_json_eq(setup.db_path(), tmp_json2);
    }

    #[test]
    fn catalog_new_picks_latest_valid_json_among_tmp_files() {
        // given multiple tmp files with only one in the middle with valid json
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

        let setup = TmpFileSetup::new("db", main_json);
        setup.set_main_mtime(0);
        setup.add_tmp_file(100, "not json", 10);
        setup.add_tmp_file(200, tmp_json_valid, 20);
        setup.add_tmp_file(300, "not json", 30);

        // when loading `Catalog`
        let catalog = Catalog::new(setup.path(), setup.db_name()).unwrap();

        // valid tmp file should be loaded and renamed
        assert!(catalog.tables.contains_key("valid"));
        assert!(!catalog.tables.contains_key("main"));
        assert_no_tmp_files(setup.path().join(setup.db_name()));

        // main file should now contain the valid tmp json
        assert_file_json_eq(setup.db_path(), tmp_json_valid);
    }

    #[test]
    fn catalog_new_falls_back_to_main_file_if_no_valid_tmp() {
        // given multiple tmp files, none of them with valid json
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

        let setup = TmpFileSetup::new("db", main_json);
        setup.set_main_mtime(0);
        setup.add_tmp_file(100, "not json", 10);
        setup.add_tmp_file(200, "not json", 20);

        // when loading `Catalog`
        let catalog = Catalog::new(setup.path(), setup.db_name()).unwrap();

        // main file should be loaded
        assert!(catalog.tables.contains_key("main"));
        assert_no_tmp_files(setup.path().join(setup.db_name()));

        // main file should still contain the original json
        assert_file_json_eq(setup.db_path(), main_json);
    }

    #[test]
    fn catalog_new_returns_error_when_column_is_invalid() {
        // given file with a column that has invalid base_offset_pos > pos
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
        let (tmp_dir, _) = setup_db_with_json(json);

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
        let (tmp_dir, _) = setup_db_with_json(json);

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
        let fixture = CatalogTestFixture::in_memory_with_table(users_table());

        // when getting table with name `users`
        let result = fixture.catalog().table("users");

        // then table `users` is returned
        assert!(result.is_ok());
        assert_table(&users_table(), &result.unwrap());
    }

    #[test]
    fn catalog_table_returns_error_when_missing_table() {
        // given empty catalog
        let fixture = CatalogTestFixture::in_memory();

        // when getting non existing table
        let result = fixture.catalog().table("missing");

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
        let mut fixture = CatalogTestFixture::new();
        let users = users_table();

        // when adding table
        let result = fixture.catalog_mut().add_table(users.clone());

        // then table is present
        assert!(result.is_ok());
        assert_table(&users, fixture.catalog().tables.get("users").unwrap());

        // and flushed to disk
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].name, "users");
            assert_eq!(tables[0].columns.len(), 2);
            assert_eq!(tables[0].primary_key_column_name, "id");
        });
    }

    #[test]
    fn catalog_add_table_returns_error_when_table_exists() {
        // given catalog with table `users`
        let mut fixture = CatalogTestFixture::in_memory_with_table(users_table());

        // when adding table with same name
        let result = fixture.catalog_mut().add_table(users_table());

        // then Err(CatalogError::TableAlreadyExists) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableAlreadyExists("users".into()),
        );
    }

    #[test]
    fn catalog_remove_table_removes_table_directory_from_disk() {
        // given catalog with table `users`
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // create table directory on disk
        let table_dir = fixture.db_path().parent().unwrap().join("users");
        fs::create_dir(&table_dir).unwrap();

        // create some dummy files in table directory
        fs::write(table_dir.join("data.bin"), b"test data").unwrap();
        fs::write(table_dir.join("index.bin"), b"test index").unwrap();

        // verify directory exists before removal
        assert!(table_dir.exists());

        // when removing table
        let result = fixture.catalog_mut().remove_table("users");

        // then Ok(()) is returned
        assert!(result.is_ok());

        // table is removed from catalog
        assert!(!fixture.catalog().tables.contains_key("users"));

        // table directory is removed from disk
        assert!(!table_dir.exists());

        // and metadata file is updated
        assert_catalog_on_disk(fixture.db_path(), 0, |_| {});
    }

    #[test]
    fn catalog_remove_table_returns_error_when_table_does_not_exist() {
        // given empty catalog
        let mut fixture = CatalogTestFixture::new();

        // when removing non-existent table
        let result = fixture.catalog_mut().remove_table("nonexistent");

        // then Err(CatalogError::TableNotFound) is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableNotFound("nonexistent".into()),
        );
    }

    #[test]
    fn catalog_sync_to_disk_saves_to_file() {
        // given a catalog with one table
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // when syncing to disk
        let result = fixture.catalog_mut().sync_to_disk();

        // then sync succeeds
        assert!(result.is_ok());

        // and file contains expected JSON
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].name, "users");
            assert_eq!(tables[0].columns.len(), 2);
            assert_eq!(tables[0].primary_key_column_name, "id");
        });
    }

    #[test]
    fn catalog_add_column_stages_to_tmp_file() {
        // given catalog with table "users"
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // when adding a column
        let result = fixture.catalog_mut().add_column(
            "users",
            NewColumnRequest {
                name: "email".to_string(),
                ty: Type::String,
            },
        );

        // then operation succeeds and tmp file is created
        assert!(result.is_ok());
        let tmp_files = MetadataFileHelper::list_catalog_tmp_files::<CatalogError>(
            fixture.db_path().parent().unwrap(),
            &format!("{}.tmp-", METADATA_FILE_NAME),
        )
        .unwrap();
        assert_eq!(tmp_files.len(), 1);

        // main file should still have old content
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 2);
        });
    }

    #[test]
    fn catalog_remove_column_stages_to_tmp_file() {
        // given catalog with table "users" with 3 columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
            dummy_column("email", Type::String, 2),
        ];
        let table = dummy_table("users", columns, "id");
        let mut fixture = CatalogTestFixture::with_table(table);

        // when removing a column
        let result = fixture.catalog_mut().remove_column("users", "email");

        // then operation succeeds and tmp file is created
        assert!(result.is_ok());
        let tmp_files = MetadataFileHelper::list_catalog_tmp_files::<CatalogError>(
            fixture.db_path().parent().unwrap(),
            &format!("{}.tmp-", METADATA_FILE_NAME),
        )
        .unwrap();
        assert_eq!(tmp_files.len(), 1);

        // main file should still have old content
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 3);
        });
    }

    #[test]
    fn catalog_commit_staged_commits_latest_tmp_file() {
        // given catalog with staged changes (tmp file exists)
        let mut fixture = CatalogTestFixture::with_table(users_table());
        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "email".to_string(),
                    ty: Type::String,
                },
            )
            .unwrap();

        // when committing staged changes
        let result = fixture.catalog().commit_staged();

        // then commit succeeds
        assert!(result.is_ok());

        // main file now contains new column
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 3);
            assert_eq!(tables[0].columns[2].name, "email");
        });

        // tmp files are removed
        assert_no_tmp_files(fixture.db_path().parent().unwrap());
    }

    #[test]
    fn catalog_commit_staged_commits_latest_of_multiple_tmp_files() {
        // given catalog with multiple staged changes
        let mut fixture = CatalogTestFixture::with_table(users_table());

        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "email".to_string(),
                    ty: Type::String,
                },
            )
            .unwrap();

        // Add another column creating second tmp file
        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "age".to_string(),
                    ty: Type::I32,
                },
            )
            .unwrap();

        // when committing staged changes
        let result = fixture.catalog().commit_staged();

        // then commit succeeds
        assert!(result.is_ok());

        // main file now contains both new columns
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 4);
            assert!(tables[0].columns.iter().any(|c| c.name == "email"));
            assert!(tables[0].columns.iter().any(|c| c.name == "age"));
        });

        // all tmp files are removed
        assert_no_tmp_files(fixture.db_path().parent().unwrap());
    }

    #[test]
    fn catalog_commit_staged_does_nothing_when_no_tmp_files() {
        // given catalog without any staged changes
        let fixture = CatalogTestFixture::with_table(users_table());

        // when committing
        let result = fixture.catalog().commit_staged();

        // then commit succeeds (noop)
        assert!(result.is_ok());

        // main file unchanged
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 2);
        });
    }

    #[test]
    fn catalog_rollback_staged_removes_tmp_files_and_reverts_memory() {
        // given catalog with staged changes
        let mut fixture = CatalogTestFixture::with_table(users_table());

        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "email".to_string(),
                    ty: Type::String,
                },
            )
            .unwrap();

        // verify column exists in memory
        assert!(
            fixture
                .catalog()
                .table("users")
                .unwrap()
                .column("email")
                .is_ok()
        );

        // when rolling back staged changes
        let result = fixture.catalog_mut().rollback_staged();

        // then rollback succeeds
        assert!(result.is_ok());

        // tmp files are removed
        assert_no_tmp_files(fixture.db_path().parent().unwrap());

        // in-memory state is reverted (email column no longer exists)
        let table = fixture.catalog().table("users").unwrap();
        assert!(table.column("email").is_err());
        assert_eq!(table.columns().count(), 2);

        // main file unchanged
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 2);
        });
    }

    #[test]
    fn catalog_rollback_staged_removes_multiple_tmp_files() {
        // given catalog with multiple staged changes
        let mut fixture = CatalogTestFixture::with_table(users_table());

        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "email".to_string(),
                    ty: Type::String,
                },
            )
            .unwrap();

        fixture
            .catalog_mut()
            .add_column(
                "users",
                NewColumnRequest {
                    name: "age".to_string(),
                    ty: Type::I32,
                },
            )
            .unwrap();

        // when rolling back
        let result = fixture.catalog_mut().rollback_staged();

        // then rollback succeeds
        assert!(result.is_ok());

        // all tmp files are removed
        assert_no_tmp_files(fixture.db_path().parent().unwrap());

        // in-memory state is reverted
        let table = fixture.catalog().table("users").unwrap();
        assert!(table.column("email").is_err());
        assert!(table.column("age").is_err());
        assert_eq!(table.columns().count(), 2);
    }

    #[test]
    fn catalog_rollback_after_remove_column_restores_column() {
        // given catalog with table containing 3 columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
            dummy_column("email", Type::String, 2),
        ];
        let table = dummy_table("users", columns, "id");
        let mut fixture = CatalogTestFixture::with_table(table);

        // when removing column and then rolling back
        fixture
            .catalog_mut()
            .remove_column("users", "email")
            .unwrap();

        // verify column removed in memory
        assert!(
            fixture
                .catalog()
                .table("users")
                .unwrap()
                .column("email")
                .is_err()
        );

        let result = fixture.catalog_mut().rollback_staged();

        // then rollback succeeds and column is restored
        assert!(result.is_ok());
        let table = fixture.catalog().table("users").unwrap();
        assert!(table.column("email").is_ok());
        assert_eq!(table.columns().count(), 3);

        // main file still has all 3 columns
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].columns.len(), 3);
        });
    }

    #[test]
    fn catalog_rename_table_renames_existing_table() {
        // given catalog with table "users"
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // create table directory and files on disk
        let old_table_dir = fixture.db_path().parent().unwrap().join("users");
        fs::create_dir(&old_table_dir).unwrap();
        fs::write(old_table_dir.join("users.idx"), b"index data").unwrap();
        fs::write(old_table_dir.join("users.tbl"), b"table data").unwrap();

        // when renaming table
        let result = fixture.catalog_mut().rename_table("users", "accounts");

        // then operation succeeds
        assert!(result.is_ok());

        // old table name no longer exists
        assert!(fixture.catalog().table("users").is_err());

        // new table name exists with correct metadata
        let renamed_table = fixture.catalog().table("accounts").unwrap();
        assert_eq!(renamed_table.name, "accounts");
        assert_eq!(renamed_table.columns.len(), 2);
        assert_eq!(renamed_table.primary_key_column_name(), "id");

        // old directory removed, new directory exists
        assert!(!old_table_dir.exists());
        let new_table_dir = fixture.db_path().parent().unwrap().join("accounts");
        assert!(new_table_dir.exists());

        // files renamed correctly
        assert!(new_table_dir.join("accounts.idx").exists());
        assert!(new_table_dir.join("accounts.tbl").exists());

        // metadata file updated
        assert_catalog_on_disk(fixture.db_path(), 1, |tables| {
            assert_eq!(tables[0].name, "accounts");
        });
    }

    #[test]
    fn catalog_rename_table_returns_error_when_table_does_not_exist() {
        // given empty catalog
        let mut fixture = CatalogTestFixture::new();

        // when renaming non-existent table
        let result = fixture.catalog_mut().rename_table("missing", "new_name");

        // then error is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::TableNotFound("missing".into()),
        );
    }

    #[test]
    fn catalog_rename_table_rolls_back_on_idx_file_error() {
        // given catalog with table "users"
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // create table directory without idx file
        let old_table_dir = fixture.db_path().parent().unwrap().join("users");
        fs::create_dir(&old_table_dir).unwrap();
        fs::write(old_table_dir.join("users.tbl"), b"table data").unwrap();

        // when renaming table (will fail on idx file)
        let result = fixture.catalog_mut().rename_table("users", "accounts");

        // then error is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::IoError(io::Error::new(io::ErrorKind::NotFound, "")),
        );

        // original table still exists in catalog
        assert!(fixture.catalog().table("users").is_ok());
        assert!(fixture.catalog().table("accounts").is_err());

        // new directory cleaned up
        let new_table_dir = fixture.db_path().parent().unwrap().join("accounts");
        assert!(!new_table_dir.exists());

        // old directory still exists
        assert!(old_table_dir.exists());
    }

    #[test]
    fn catalog_rename_table_rolls_back_on_tbl_file_error() {
        // given catalog with table "users"
        let mut fixture = CatalogTestFixture::with_table(users_table());

        // create table directory with idx file but no tbl file
        let old_table_dir = fixture.db_path().parent().unwrap().join("users");
        fs::create_dir(&old_table_dir).unwrap();
        fs::write(old_table_dir.join("users.idx"), b"index data").unwrap();

        // when renaming table (will fail on tbl file)
        let result = fixture.catalog_mut().rename_table("users", "accounts");

        // then error is returned
        assert!(result.is_err());
        assert_catalog_error_variant(
            &result.unwrap_err(),
            &CatalogError::IoError(io::Error::new(io::ErrorKind::NotFound, "")),
        );

        // original table still exists in catalog
        assert!(fixture.catalog().table("users").is_ok());
        assert!(fixture.catalog().table("accounts").is_err());

        // new directory cleaned up
        let new_table_dir = fixture.db_path().parent().unwrap().join("accounts");
        assert!(!new_table_dir.exists());

        // idx file rolled back to old location
        assert!(old_table_dir.join("users.idx").exists());
    }

    #[test]
    fn catalog_rename_table_preserves_column_metadata() {
        // given catalog with table containing multiple columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("name", Type::String, 1),
            dummy_column("email", Type::String, 2),
            dummy_column("age", Type::I32, 3),
        ];
        let table = dummy_table("users", columns.clone(), "id");
        let mut fixture = CatalogTestFixture::with_table(table);

        // create table directory and files
        let old_table_dir = fixture.db_path().parent().unwrap().join("users");
        fs::create_dir(&old_table_dir).unwrap();
        fs::write(old_table_dir.join("users.idx"), b"index data").unwrap();
        fs::write(old_table_dir.join("users.tbl"), b"table data").unwrap();

        // when renaming table
        fixture
            .catalog_mut()
            .rename_table("users", "people")
            .unwrap();

        // then all column metadata preserved
        let renamed_table = fixture.catalog().table("people").unwrap();
        assert_eq!(renamed_table.columns().count(), 4);

        for (expected, actual) in columns.iter().zip(renamed_table.columns()) {
            assert_column(expected, &actual);
        }
    }

    // Helper to check if error variant is as expected
    fn assert_table_metadata_error_variant(
        actual: &TableMetadataError,
        expected: &TableMetadataError,
    ) {
        assert_eq!(
            mem::discriminant(actual),
            mem::discriminant(expected),
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
        let result = TableMetadata::new("users", columns, "id");
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
        let result = TableMetadata::new("users", columns, "not_a_column");
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
        let result = TableMetadata::new("users", columns, "id");
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
        let table = TableMetadata::new("users", columns.clone(), "id").unwrap();

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
    fn table_metadata_add_column_adds_fixed_size_column_to_end_when_no_variable_columns() {
        // given table with only fixed-size columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("age", Type::I32, 1),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when adding new fixed-size column
        let result = table.add_column(NewColumnRequest {
            name: "score".to_string(),
            ty: Type::F64,
        });

        // then column is added at the end
        assert!(result.is_ok());
        let added = result.unwrap();
        assert_eq!(added.pos, 2);
        assert_eq!(added.ty, Type::F64);
        assert_eq!(added.base_offset, 8);

        let score_col = table.column("score").unwrap();
        assert_eq!(score_col.pos(), 2);
        assert_eq!(score_col.base_offset(), 8);
        assert_eq!(score_col.base_offset_pos(), 2);
    }

    #[test]
    fn table_metadata_add_column_adds_fixed_size_column_before_variable_columns() {
        // given table with fixed and variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            ColumnMetadata::new("name".to_string(), Type::String, 1, 4, 1).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when adding new fixed-size column
        let result = table.add_column(NewColumnRequest {
            name: "age".to_string(),
            ty: Type::I32,
        });

        // then column is inserted before variable columns
        assert!(result.is_ok());
        let added = result.unwrap();
        assert_eq!(added.pos, 1);
        assert_eq!(added.ty, Type::I32);
        assert_eq!(added.base_offset, 4);

        let age_col = table.column("age").unwrap();
        assert_eq!(age_col.pos(), 1);
        assert_eq!(age_col.base_offset(), 4);
        assert_eq!(age_col.base_offset_pos(), 1);

        // variable column should be shifted
        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 2);
        assert_eq!(name_col.base_offset(), 8);
        assert_eq!(name_col.base_offset_pos(), 2);
    }

    #[test]
    fn table_metadata_add_column_adds_variable_size_column_to_end() {
        // given table with fixed and variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            ColumnMetadata::new("name".to_string(), Type::String, 1, 4, 1).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when adding new variable-size column
        let result = table.add_column(NewColumnRequest {
            name: "email".to_string(),
            ty: Type::String,
        });

        // then column is added at the end
        assert!(result.is_ok());
        let added = result.unwrap();
        assert_eq!(added.pos, 2);
        assert_eq!(added.ty, Type::String);
        assert_eq!(added.base_offset, 4);

        let email_col = table.column("email").unwrap();
        assert_eq!(email_col.pos(), 2);
        assert_eq!(email_col.base_offset(), 4);
        assert_eq!(email_col.base_offset_pos(), 1);
    }

    #[test]
    fn table_metadata_add_column_recalculates_multiple_variable_columns() {
        // given table with fixed and multiple variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            ColumnMetadata::new("name".to_string(), Type::String, 1, 4, 1).unwrap(),
            ColumnMetadata::new("email".to_string(), Type::String, 2, 4, 1).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when adding new fixed-size column
        let result = table.add_column(NewColumnRequest {
            name: "age".to_string(),
            ty: Type::I32,
        });

        // then all variable columns are recalculated
        assert!(result.is_ok());

        let age_col = table.column("age").unwrap();
        assert_eq!(age_col.pos(), 1);
        assert_eq!(age_col.base_offset(), 4);

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 2);
        assert_eq!(name_col.base_offset(), 8);
        assert_eq!(name_col.base_offset_pos(), 2);

        let email_col = table.column("email").unwrap();
        assert_eq!(email_col.pos(), 3);
        assert_eq!(email_col.base_offset(), 8);
        assert_eq!(email_col.base_offset_pos(), 2);
    }

    #[test]
    fn table_metadata_add_column_returns_error_on_duplicate_name() {
        // given table with column "id"
        let mut table = users_table();

        // when adding column with duplicate name
        let result = table.add_column(NewColumnRequest {
            name: "id".to_string(),
            ty: Type::I32,
        });

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::ColumnAlreadyExists(String::new()),
        );
    }

    #[test]
    fn table_metadata_add_column_first_variable_after_fixed() {
        // given table with only fixed-size columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("age", Type::I32, 1),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when adding first variable-size column
        let result = table.add_column(NewColumnRequest {
            name: "name".to_string(),
            ty: Type::String,
        });

        // then column is added with correct base_offset_pos
        assert!(result.is_ok());

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 2);
        assert_eq!(name_col.base_offset(), 8);
        assert_eq!(name_col.base_offset_pos(), 2);
    }

    #[test]
    fn table_metadata_remove_column_removes_fixed_size_column_from_end() {
        // given table with multiple fixed-size columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("age", Type::I32, 1),
            dummy_column("score", Type::F64, 2),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing last fixed-size column
        let result = table.remove_column("score");

        // then column is removed
        assert!(result.is_ok());
        let removed = result.unwrap();
        assert_eq!(removed.pos, 2);
        assert_eq!(removed.ty, Type::F64);
        assert_eq!(removed.prev_column_base_offset, size_of::<i32>());

        // and column no longer exists
        assert!(table.column("score").is_err());
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn table_metadata_remove_column_removes_fixed_size_column_from_middle() {
        // given table with fixed and variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("age", Type::I32, 1),
            ColumnMetadata::new("name".to_string(), Type::String, 2, 8, 2).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing fixed-size column from middle
        let result = table.remove_column("age");

        // then column is removed and metadata recalculated
        assert!(result.is_ok());

        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.pos(), 0);
        assert_eq!(id_col.base_offset(), 0);

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 1);
        assert_eq!(name_col.base_offset(), 4);
        assert_eq!(name_col.base_offset_pos(), 1);
    }

    #[test]
    fn table_metadata_remove_column_removes_variable_size_column_from_end() {
        // given table with fixed and variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            ColumnMetadata::new("name".to_string(), Type::String, 1, 4, 1).unwrap(),
            ColumnMetadata::new("email".to_string(), Type::String, 2, 4, 1).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing last variable-size column
        let result = table.remove_column("email");

        // then column is removed
        assert!(result.is_ok());
        let removed = result.unwrap();
        assert_eq!(removed.pos, 2);
        assert_eq!(removed.ty, Type::String);
        assert_eq!(removed.prev_column_base_offset, size_of::<i32>());

        // and column no longer exists
        assert!(table.column("email").is_err());
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn table_metadata_remove_column_removes_variable_size_column_from_middle() {
        // given table with multiple variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            ColumnMetadata::new("name".to_string(), Type::String, 1, 4, 1).unwrap(),
            ColumnMetadata::new("email".to_string(), Type::String, 2, 4, 1).unwrap(),
            ColumnMetadata::new("address".to_string(), Type::String, 3, 4, 1).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing variable-size column from middle
        let result = table.remove_column("email");

        // then column is removed and metadata recalculated
        assert!(result.is_ok());

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 1);

        let address_col = table.column("address").unwrap();
        assert_eq!(address_col.pos(), 2);
        assert_eq!(address_col.base_offset(), 4);
        assert_eq!(address_col.base_offset_pos(), 1);
    }

    #[test]
    fn table_metadata_remove_column_returns_error_for_primary_key() {
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
    fn table_metadata_remove_column_returns_error_for_nonexistent_column() {
        // given table with columns
        let mut table = users_table();

        // when trying to remove non-existent column
        let result = table.remove_column("missing");

        // then error is returned
        assert!(result.is_err());
        assert_table_metadata_error_variant(
            &result.unwrap_err(),
            &TableMetadataError::ColumnNotFound(String::new()),
        );
    }

    #[test]
    fn table_metadata_remove_column_recalculates_all_subsequent_columns() {
        // given table with mixed fixed and variable columns
        let columns = vec![
            dummy_column("id", Type::I32, 0),
            dummy_column("age", Type::I32, 1),
            dummy_column("score", Type::F64, 2),
            ColumnMetadata::new("name".to_string(), Type::String, 3, 16, 3).unwrap(),
            ColumnMetadata::new("email".to_string(), Type::String, 4, 16, 3).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing fixed-size column from middle
        let result = table.remove_column("age");

        // then all subsequent columns are recalculated
        assert!(result.is_ok());

        let score_col = table.column("score").unwrap();
        assert_eq!(score_col.pos(), 1);
        assert_eq!(score_col.base_offset(), 4);

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 2);
        assert_eq!(name_col.base_offset(), 12);
        assert_eq!(name_col.base_offset_pos(), 2);

        let email_col = table.column("email").unwrap();
        assert_eq!(email_col.pos(), 3);
        assert_eq!(email_col.base_offset(), 12);
        assert_eq!(email_col.base_offset_pos(), 2);
    }

    #[test]
    fn table_metadata_remove_column_removes_first_non_primary_key_column() {
        // given table where first column is NOT primary key
        let columns = vec![
            dummy_column("age", Type::I32, 0),
            dummy_column("id", Type::I32, 1),
            dummy_column("score", Type::F64, 2),
            ColumnMetadata::new("name".to_string(), Type::String, 3, 16, 3).unwrap(),
        ];
        let mut table = TableMetadata::new("users", columns, "id").unwrap();

        // when removing first column (which is not primary key)
        let result = table.remove_column("age");

        // then column is removed and all subsequent columns recalculated
        assert!(result.is_ok());
        let removed = result.unwrap();
        assert_eq!(removed.pos, 0);
        assert_eq!(removed.ty, Type::I32);
        assert_eq!(removed.prev_column_base_offset, 0);

        // id shifts to position 0
        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.pos(), 0);
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.base_offset_pos(), 0);

        // score shifts down
        let score_col = table.column("score").unwrap();
        assert_eq!(score_col.pos(), 1);
        assert_eq!(score_col.base_offset(), 4);
        assert_eq!(score_col.base_offset_pos(), 1);

        // name shifts down
        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.pos(), 2);
        assert_eq!(name_col.base_offset(), 12);
        assert_eq!(name_col.base_offset_pos(), 2);

        // column no longer exists
        assert!(table.column("age").is_err());
        assert_eq!(table.columns.len(), 3);
    }

    #[test]
    fn test_table_metadata_factory_calculates_correct_offsets() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "name".to_string(),
                ty: Type::String,
            },
            NewColumnRequest {
                name: "score".to_string(),
                ty: Type::F64,
            },
            NewColumnRequest {
                name: "surname".to_string(),
                ty: Type::String,
            },
            NewColumnRequest {
                name: "active".to_string(),
                ty: Type::Bool,
            },
        ];

        let factory = TableMetadataFactory::new("test", columns, "id");
        let table = factory.create_table_metadata().unwrap();

        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.base_offset_pos(), 0);
        assert_eq!(id_col.pos(), 0);

        let score_col = table.column("score").unwrap();
        assert_eq!(score_col.base_offset(), 4);
        assert_eq!(score_col.base_offset_pos(), 1);
        assert_eq!(score_col.pos(), 1);

        let active_col = table.column("active").unwrap();
        assert_eq!(active_col.base_offset(), 12);
        assert_eq!(active_col.base_offset_pos(), 2);
        assert_eq!(active_col.pos(), 2);

        let name_col = table.column("name").unwrap();
        assert_eq!(name_col.base_offset(), 13);
        assert_eq!(name_col.base_offset_pos(), 3);
        assert_eq!(name_col.pos(), 3);

        let surname_col = table.column("surname").unwrap();
        assert_eq!(surname_col.base_offset(), 13);
        assert_eq!(surname_col.base_offset_pos(), 3);
        assert_eq!(surname_col.pos(), 4);
    }

    #[test]
    fn test_table_metadata_factory_all_fixed_size_types() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "value".to_string(),
                ty: Type::I64,
            },
            NewColumnRequest {
                name: "ratio".to_string(),
                ty: Type::F64,
            },
            NewColumnRequest {
                name: "active".to_string(),
                ty: Type::Bool,
            },
        ];

        let factory = TableMetadataFactory::new("test", columns, "id");
        let table = factory.create_table_metadata().unwrap();

        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.ty(), Type::I32);

        let value_col = table.column("value").unwrap();
        assert_eq!(value_col.base_offset(), 4);
        assert_eq!(value_col.ty(), Type::I64);

        let ratio_col = table.column("ratio").unwrap();
        assert_eq!(ratio_col.base_offset(), 12);
        assert_eq!(ratio_col.ty(), Type::F64);

        let active_col = table.column("active").unwrap();
        assert_eq!(active_col.base_offset(), 20);
        assert_eq!(active_col.ty(), Type::Bool);
    }

    #[test]
    fn test_table_metadata_factory_primary_key_column() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "name".to_string(),
                ty: Type::String,
            },
        ];

        let factory = TableMetadataFactory::new("test", columns, "id");
        let table = factory.create_table_metadata().unwrap();

        assert_eq!(table.primary_key_column_name(), "id");
    }

    #[test]
    fn test_table_metadata_factory_duplicate_column_name() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "name".to_string(),
                ty: Type::String,
            },
            NewColumnRequest {
                name: "name".to_string(),
                ty: Type::I32,
            },
        ];

        let factory = TableMetadataFactory::new("test", columns, "id");
        let result = factory.create_table_metadata();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TableMetadataError::DuplicatedColumn(_)
        ));
    }

    #[test]
    fn test_table_metadata_factory_unknown_primary_key() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "name".to_string(),
                ty: Type::String,
            },
        ];

        let factory = TableMetadataFactory::new("test", columns, "unknown_column");
        let result = factory.create_table_metadata();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TableMetadataError::UnknownPrimaryKeyColumn(_)
        ));
    }

    #[test]
    fn test_table_metadata_factory_single_column() {
        let columns = vec![NewColumnRequest {
            name: "id".to_string(),
            ty: Type::I32,
        }];

        let factory = TableMetadataFactory::new("test", columns, "id");
        let table = factory.create_table_metadata().unwrap();

        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.pos(), 0);
        assert_eq!(table.columns().count(), 1);
    }

    #[test]
    fn test_table_metadata_factory_multiple_variable_size_columns() {
        let columns = vec![
            NewColumnRequest {
                name: "id".to_string(),
                ty: Type::I32,
            },
            NewColumnRequest {
                name: "first_name".to_string(),
                ty: Type::String,
            },
            NewColumnRequest {
                name: "last_name".to_string(),
                ty: Type::String,
            },
            NewColumnRequest {
                name: "email".to_string(),
                ty: Type::String,
            },
        ];

        let factory = TableMetadataFactory::new("users", columns, "id");
        let table = factory.create_table_metadata().unwrap();

        let id_col = table.column("id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.pos(), 0);

        // All variable-size columns should share the same base_offset
        let first_name = table.column("first_name").unwrap();
        let last_name = table.column("last_name").unwrap();
        let email = table.column("email").unwrap();

        assert_eq!(first_name.base_offset(), 4);
        assert_eq!(last_name.base_offset(), 4);
        assert_eq!(email.base_offset(), 4);

        // But different positions
        assert_eq!(first_name.pos(), 1);
        assert_eq!(last_name.pos(), 2);
        assert_eq!(email.pos(), 3);
    }
}
