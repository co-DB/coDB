mod consts;
mod iterators;

use std::{iter::once, path::Path, sync::Arc};

use dashmap::{DashMap, mapref::one::Ref};
use engine::{
    data_types,
    heap_file::{HeapFile, HeapFileError, HeapFileFactory},
    record::Record,
};
use itertools::Itertools;
use metadata::{
    catalog::{Catalog, ColumnMetadata, ColumnMetadataError, TableMetadata},
    types::Type,
};
use parking_lot::RwLock;
use planner::{
    query_plan::{CreateTable, StatementPlan, StatementPlanItem, TableScan},
    resolved_tree::{ResolvedCreateColumnDescriptor, ResolvedTree},
};
use storage::{
    cache::Cache,
    files_manager::{FileKey, FilesManager, FilesManagerError},
};
use thiserror::Error;

use crate::{
    consts::HEAP_FILE_BUCKET_SIZE,
    iterators::{ParseErrorIter, QueryResultIter, StatementIter},
};

pub struct Executor {
    heap_files: DashMap<String, HeapFile<HEAP_FILE_BUCKET_SIZE>>,
    cache: Arc<Cache>,
    catalog: Arc<RwLock<Catalog>>,
}

/// Error for [`Executor`] related operations
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Cannot open files manager: {0}")]
    CannotOpenFilesManager(#[from] FilesManagerError),
}

/// Error for internal executor operations, shouldn't be exported outside of this module.
#[derive(Error, Debug)]
enum InternalExecutorError {
    #[error("Table '{table_name}' does not exist.")]
    TableDoesNotExist { table_name: String },
    #[error("Cannot create heap file: {reason}")]
    CannotCreateHeapFile { reason: String },
    #[error("{0}")]
    HeapFileError(#[from] HeapFileError),
}

#[derive(Debug)]
pub struct ColumnData {
    pub name: String,
    pub ty: Type,
}

#[derive(Debug, PartialEq, Eq)]
pub enum StatementType {
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Truncate,
    Drop,
}

#[derive(Debug)]
pub enum StatementResult {
    OperationSuccessful {
        rows_affected: usize,
        ty: StatementType,
    },
    SelectSuccessful {
        columns: Vec<ColumnData>,
        rows: Vec<Record>,
    },
    ParseError {
        error: String,
    },
    RuntimeError {
        error: String,
    },
}

impl Executor {
    pub fn new(
        base_path: impl AsRef<Path>,
        database_name: &str,
        catalog: Catalog,
    ) -> Result<Self, ExecutorError> {
        let files = Arc::new(FilesManager::new(base_path, database_name)?);
        let cache = Cache::new(consts::CACHE_SIZE, files);
        let catalog = Arc::new(RwLock::new(catalog));
        Ok(Executor {
            heap_files: DashMap::new(),
            cache: cache,
            catalog,
        })
    }

    /// Parses `query` and returns iterator over results for each statement in the `query`.
    pub fn execute<'e>(&'e self, query: &str) -> QueryResultIter<'e> {
        let parse_output = planner::process_query(query, self.catalog.clone());
        match parse_output {
            Ok(query_plan) => StatementIter::new(query_plan.plans, query_plan.tree, &self).into(),
            Err(errors) => ParseErrorIter::new(errors).into(),
        }
    }

    /// Executes single statement by delegating work to [`StatementExecutor`].
    fn execute_statement(&self, statement: &StatementPlan, ast: &ResolvedTree) -> StatementResult {
        let se = StatementExecutor::new(self, statement, ast);
        se.execute()
    }

    /// Helper to create [`StatementResult::RuntimeError`] with provided message.
    fn runtime_error(&self, msg: String) -> StatementResult {
        StatementResult::RuntimeError { error: msg }
    }

    /// Returns heap file for given table. If heap file wasn't used yet it opens it
    /// and inserts to [`Executor::heap_files`].
    ///
    /// It is possible that table was removed just before we started processing current statement
    /// (so [`Analyzer`] didn't report any problem) - in such case we just return an error that
    /// table does not exist.
    fn get_heap_file(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
    ) -> Result<Ref<'_, String, HeapFile<HEAP_FILE_BUCKET_SIZE>>, InternalExecutorError> {
        self.heap_files
            .entry(table_name.clone().into())
            .or_try_insert_with(|| self.open_heap_file(table_name.clone()))?;
        self.heap_files
            .get(table_name.as_ref())
            .ok_or(InternalExecutorError::TableDoesNotExist {
                table_name: table_name.clone().into(),
            })
    }

    /// Creates new heap file for given table.
    ///
    /// As in [`Executor::get_heap_file`] if table was removed just before we started processing
    /// current statement we just return error that table does not exist.
    fn open_heap_file(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
    ) -> Result<HeapFile<HEAP_FILE_BUCKET_SIZE>, InternalExecutorError> {
        let file_key = FileKey::data(table_name.clone());
        let cache = self.cache.clone();
        let columns_metadata = self
            .catalog
            .read()
            .table(table_name.as_ref())
            .map_err(|_| InternalExecutorError::TableDoesNotExist {
                table_name: table_name.clone().into(),
            })?
            .columns()
            .collect();
        let heap_file_factory = HeapFileFactory::new(file_key, cache, columns_metadata);
        let heap_file = heap_file_factory.create_heap_file().map_err(|err| {
            InternalExecutorError::CannotCreateHeapFile {
                reason: err.to_string(),
            }
        })?;
        Ok(heap_file)
    }
}

struct StatementExecutor<'e, 'q> {
    executor: &'e Executor,
    statement: &'q StatementPlan,
    ast: &'q ResolvedTree,
}

impl<'e, 'q> StatementExecutor<'e, 'q> {
    fn new(executor: &'e Executor, statement: &'q StatementPlan, ast: &'q ResolvedTree) -> Self {
        StatementExecutor {
            executor,
            statement,
            ast,
        }
    }

    fn root_statement(&self) -> &StatementPlanItem {
        self.statement.item(self.statement.root())
    }

    fn execute(&self) -> StatementResult {
        match self.root_statement() {
            StatementPlanItem::TableScan(table_scan) => todo!(),
            StatementPlanItem::IndexScan(index_scan) => todo!(),
            StatementPlanItem::Filter(filter) => todo!(),
            StatementPlanItem::Projection(projection) => todo!(),
            StatementPlanItem::Insert(insert) => todo!(),
            StatementPlanItem::CreateTable(create_table) => self.create_table(create_table),
        }
    }

    fn table_scan(&self, table_scan: &TableScan) -> Result<Vec<Record>, InternalExecutorError> {
        let hf = self.executor.get_heap_file(&table_scan.table_name)?;
        let records = hf.all_records()?;
        Ok(records)
    }

    fn create_table(&self, create_table: &CreateTable) -> StatementResult {
        let column_metadatas = match self.process_columns(create_table) {
            Ok(cm) => cm,
            Err(err) => {
                return self
                    .executor
                    .runtime_error(format!("Failed to create column: {err}"));
            }
        };

        let table_metadata = match TableMetadata::new(
            &create_table.name,
            &column_metadatas,
            &create_table.primary_key_column.name,
        ) {
            Ok(tm) => tm,
            Err(err) => {
                return self
                    .executor
                    .runtime_error(format!("Failed to create table: {}", err));
            }
        };

        let mut catalog = self.executor.catalog.write();

        if let Err(err) = catalog.add_table(table_metadata) {
            return self
                .executor
                .runtime_error(format!("Failed to create table: {}", err));
        }

        match catalog.sync_to_disk() {
            Ok(_) => StatementResult::OperationSuccessful {
                rows_affected: 0,
                ty: StatementType::Create,
            },
            Err(err) => self
                .executor
                .runtime_error(format!("Failed to save catalog content to disk: {err}")),
        }
    }

    /// Creates iterator over all columns in [`CreateTable`] (including primary key column)
    /// and sorts them by whether they are fixed-size (fixed-size columns are first).
    fn sort_columns_by_fixed_size<'c>(
        &self,
        create_table: &'c CreateTable,
    ) -> impl Iterator<Item = &'c ResolvedCreateColumnDescriptor> {
        create_table
            .columns
            .iter()
            .chain(once(&create_table.primary_key_column))
            .sorted_by(|a, b| {
                let a_fixed = a.ty.is_fixed_size();
                let b_fixed = b.ty.is_fixed_size();
                b_fixed.cmp(&a_fixed)
            })
    }

    /// Returns vector of [`ColumnMetadata`] that maps to columns in [`CreateTable`].
    fn process_columns(
        &self,
        create_table: &CreateTable,
    ) -> Result<Vec<ColumnMetadata>, ColumnMetadataError> {
        let mut column_metadatas = Vec::with_capacity(create_table.columns.len());

        let cols = self.sort_columns_by_fixed_size(create_table);

        let mut pos = 0;
        let mut last_fixed_pos = 0;
        let mut base_offset = 0;

        for col in cols {
            let column_metadata =
                ColumnMetadata::new(col.name.clone(), col.ty, pos, base_offset, last_fixed_pos)?;
            column_metadatas.push(column_metadata);
            pos += 1;
            if let Some(offset) = data_types::type_size_on_disk(&col.ty) {
                last_fixed_pos += 1;
                base_offset += offset;
            }
        }
        Ok(column_metadatas)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use planner::resolved_tree::ResolvedCreateColumnAddon;
    use tempfile::TempDir;

    use super::*;

    const METADATA_FILE_NAME: &str = "metadata.coDB";
    // Helper to create a test executor
    fn create_test_executor() -> (Executor, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap_or_else(|_| {
            let db_dir = temp_dir.path().join("test_db");
            fs::create_dir(&db_dir).unwrap();
            let db_path = db_dir.join(METADATA_FILE_NAME);

            fs::write(db_path, r#"{"tables":[]}"#).unwrap();
            Catalog::new(temp_dir.path(), "test_db").unwrap()
        });

        let executor = Executor::new(temp_dir.path(), "test_db", catalog).unwrap();
        (executor, temp_dir)
    }

    // Helper to create a valid CreateTable plan
    fn create_table_plan(
        name: &str,
        columns: Vec<(&str, Type)>,
        pk_column: (&str, Type),
    ) -> CreateTable {
        let primary_key_column = ResolvedCreateColumnDescriptor {
            name: pk_column.0.to_string(),
            ty: pk_column.1,
            addon: ResolvedCreateColumnAddon::PrimaryKey,
        };

        let other_columns = columns
            .iter()
            .map(|(n, ty)| ResolvedCreateColumnDescriptor {
                name: n.to_string(),
                ty: *ty,
                addon: ResolvedCreateColumnAddon::None,
            })
            .collect();

        CreateTable {
            name: name.to_string(),
            columns: other_columns,
            primary_key_column,
        }
    }

    fn assert_operation_successful(
        result: StatementResult,
        expected_rows: usize,
        expected_ty: StatementType,
    ) {
        match result {
            StatementResult::OperationSuccessful { rows_affected, ty } => {
                assert_eq!(rows_affected, expected_rows);
                assert_eq!(ty, expected_ty);
            }
            _ => panic!("Expected OperationSuccessful, got {:?}", result),
        }
    }

    #[test]
    fn test_execute_create_table_statement_happy_path() {
        let (executor, _temp_dir) = create_test_executor();
        let create_table = create_table_plan(
            "users",
            vec![("name", Type::String), ("age", Type::I32)],
            ("id", Type::I32),
        );

        let result = executor.execute_create_table_statement(&create_table);

        assert_operation_successful(result, 0, StatementType::Create);

        let catalog = executor.catalog.read();
        let table = catalog.table("users");
        assert!(table.is_ok());
        let table = table.unwrap();
        assert_eq!(table.primary_key_column_name(), "id");
        assert!(table.column("name").is_ok());
        assert!(table.column("age").is_ok());
    }

    #[test]
    fn test_execute_create_table_statement_with_mixed_column_types() {
        let (executor, _temp_dir) = create_test_executor();
        let create_table = create_table_plan(
            "mixed_table",
            vec![
                ("name", Type::String),
                ("score", Type::F64),
                ("description", Type::String),
                ("active", Type::Bool),
            ],
            ("id", Type::I32),
        );

        let result = executor.execute_create_table_statement(&create_table);

        assert_operation_successful(result, 0, StatementType::Create);

        let catalog = executor.catalog.read();
        let table = catalog.table("mixed_table").unwrap();
        let columns: Vec<_> = table.columns().collect();

        // Here it's important that first we have fixed-size elements and only then we have variable-size ones.
        assert_eq!(columns[0].name(), "score");
        assert_eq!(columns[1].name(), "active");
        assert_eq!(columns[2].name(), "id");
        assert_eq!(columns[3].name(), "name");
        assert_eq!(columns[4].name(), "description");
    }

    #[test]
    fn test_process_columns_calculates_correct_offsets() {
        let (executor, _temp_dir) = create_test_executor();
        let create_table = create_table_plan(
            "test",
            vec![
                ("name", Type::String),
                ("score", Type::F64),
                ("surname", Type::String),
                ("active", Type::Bool),
            ],
            ("id", Type::I32),
        );

        let columns = executor.process_columns(&create_table).unwrap();

        let score_col = columns.iter().find(|c| c.name() == "score").unwrap();
        assert_eq!(score_col.base_offset(), 0);
        assert_eq!(score_col.base_offset_pos(), 0);
        assert_eq!(score_col.pos(), 0);

        let active_col = columns.iter().find(|c| c.name() == "active").unwrap();
        assert_eq!(active_col.base_offset(), 8);
        assert_eq!(active_col.base_offset_pos(), 1);
        assert_eq!(active_col.pos(), 1);

        let id_col = columns.iter().find(|c| c.name() == "id").unwrap();
        assert_eq!(id_col.base_offset(), 9);
        assert_eq!(id_col.base_offset_pos(), 2);
        assert_eq!(id_col.pos(), 2);

        let name_col = columns.iter().find(|c| c.name() == "name").unwrap();
        assert_eq!(name_col.base_offset(), 13);
        assert_eq!(name_col.base_offset_pos(), 3);
        assert_eq!(name_col.pos(), 3);

        let surname_col = columns.iter().find(|c| c.name() == "surname").unwrap();
        assert_eq!(surname_col.base_offset(), 13);
        assert_eq!(surname_col.base_offset_pos(), 3);
        assert_eq!(surname_col.pos(), 4);
    }
}
