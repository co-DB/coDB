//! Catalog module - manages tables metadata.

use std::{collections::HashMap, fs};

use thiserror::Error;

/// [`Catalog`] is an in-memory structure that holds information about a database's tables.
/// It maps to the underlying file `{PATH_TO_CODB}/{DATABASE_NAME}/metadata.coDB`.
/// The on-disk file format is JSON.
///
/// [`Catalog`] is created once at database startup. It is assumed that the number of tables and columns
/// is small enough that [`Catalog`] can be used as an in-memory data structure.
pub struct Catalog {
    /// Handle to underlying file
    handle: fs::File,
    /// Maps each table name to its metadata. Stores all tables from database.
    tables: HashMap<String, TableMetadata>,
}

/// Error for [`Catalog`] related operations
#[derive(Error, Debug)]
pub enum CatalogError {
    // TODO: fill when implementing catalog
}

impl Catalog {
    /// Creates new instance of [`Catalog`] for database `database_name`.
    /// Can fail if database does not exist or io error occurs.
    pub fn new(database_name: &str) -> Result<Self, CatalogError> {
        todo!()
    }

    /// Returns table with `table_name` name.
    /// Can fail if table with `table_name` name does not exist.
    pub fn table(&self, table_name: &str) -> Result<&TableMetadata, CatalogError> {
        todo!()
    }

    /// Adds `table` to list of tables in the catalog.
    /// IMPORTANT NOTE: this function is purely for changing contents of `metadata.coDB` file. It is NOT responsible for managing table related files (e.g. creating new b-tree).
    /// Can fail if table with same name already exists.
    pub fn add_table(&mut self, table: TableMetadata) -> Result<(), CatalogError> {
        todo!()
    }

    /// Removes table with `table_name` name from list of tables in the catalog.
    /// IMPORTANT NOTE: this function is purely for changing contents of `metadata.coDB` file. It is NOT responsible for managing table related files (e.g. removing folder `.{PATH_TO_CODB}/{DATABASE_NAME}/{TABLE_NAME}` and its files).
    /// Can fail if table with `table_name` does not exist.
    pub fn remove_table(&mut self, table_name: &str) -> Result<(), CatalogError> {
        todo!()
    }

    /// Syncs in-memory [`Catalog`] instance with underlying file.
    /// Can fail if io error occurs.
    pub fn sync_to_disk(&mut self) -> Result<(), CatalogError> {
        todo!()
    }
}

/// [`TableMetadata`] stores the metadata for a single table.
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
    // TODO: fill when implementing TableMetadata
}

impl TableMetadata {
    /// Creates new [`TableMetadata`].
    /// Can fail if columns slice contains more than one column with the same name or `primary_key_column_name` is not in `columns`.
    pub fn new(
        name: &str,
        columns: &[ColumnMetadata],
        primary_key_column_name: String,
    ) -> Result<Self, TableMetadataError> {
        todo!()
    }

    /// Returns column metadata for column with `column_name`.
    /// Can fail if column with `column_name` does not exist.
    pub fn column(&self, column_name: &str) -> Result<&ColumnMetadata, TableMetadataError> {
        todo!()
    }

    /// Returns metadata of each column stored in table sorted by columns position in disk layout.
    pub fn columns(&self) -> impl Iterator<Item = &ColumnMetadata> {
        self.columns.iter()
    }

    /// Adds new column to the table.
    /// IMPORTANT NOTE: this function is not responsible for handling proper data migration after change of table layout. The only purpose of this function is to update underlying metadata file.
    /// Can fail if column with same name already exists.
    pub fn add_column(&mut self, column: ColumnMetadata) -> Result<(), TableMetadataError> {
        todo!()
    }

    /// Removes column from the table.
    /// IMPORTANT NOTE: this function is not responsible for handling proper data migration after change of table layout. The only purpose of this function is to update underlying metadata file.
    /// Can fail if column with provided name does not exist or the column is primary key.
    pub fn remove_column(&mut self, column_name: &str) -> Result<(), TableMetadataError> {
        todo!()
    }

    /// Returns name of the table's primary key column
    pub fn primary_key_column_name(&self) -> &str {
        &self.primary_key_column_name
    }
}

/// [`ColumnMetadata`] stores the metadata for a single column.
pub struct ColumnMetadata {
    name: String,
    ty: ColumnType,
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
    // TODO: fill when implementing ColumnMetadata
}

impl ColumnMetadata {
    /// Creates new [`ColumnMetadata`]
    /// Can fail if `base_offset_pos > pos`.
    pub fn new(
        name: String,
        ty: ColumnType,
        pos: u16,
        base_offset: usize,
        base_offset_pos: u16,
    ) -> Result<Self, ColumnMetadataError> {
        todo!()
    }
}

pub enum ColumnType {
    String,
    F32,
    F64,
    I32,
    I64,
    Bool,
    Date,
    Datetime,
}

impl ColumnType {
    pub fn is_fixed_size(&self) -> bool {
        match self {
            ColumnType::String => false,
            ColumnType::F32 => true,
            ColumnType::F64 => true,
            ColumnType::I32 => true,
            ColumnType::I64 => true,
            ColumnType::Bool => true,
            ColumnType::Date => true,
            ColumnType::Datetime => true,
        }
    }
}
