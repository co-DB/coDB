mod consts;
mod iterators;

use std::{collections::HashMap, iter, mem, path::Path, sync::Arc};

use dashmap::DashMap;
use engine::{
    data_types,
    heap_file::{HeapFile, HeapFileError, HeapFileFactory},
    record::{Field, Record},
};
use itertools::Itertools;
use metadata::{
    catalog::{Catalog, ColumnMetadata, ColumnMetadataError, TableMetadata},
    types::Type,
};
use parking_lot::RwLock;
use planner::{
    query_plan::{CreateTable, Projection, StatementPlan, StatementPlanItem, TableScan},
    resolved_tree::{
        ResolvedColumn, ResolvedCreateColumnDescriptor, ResolvedExpression, ResolvedNodeId,
        ResolvedTree,
    },
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
    #[error("Used invalid operation ({operation}) for data source")]
    InvalidOperationInDataSource { operation: String },
}

#[derive(Debug)]
pub struct ColumnData {
    pub name: String,
    pub ty: Type,
}

impl From<&ResolvedColumn> for ColumnData {
    fn from(value: &ResolvedColumn) -> Self {
        ColumnData {
            name: value.name.clone(),
            ty: value.ty,
        }
    }
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
    pub fn new(database_path: impl AsRef<Path>, catalog: Catalog) -> Result<Self, ExecutorError> {
        let files = Arc::new(FilesManager::new(database_path)?);
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

    /// Gets heap file for given table and passes it to function `f`.
    /// If heap file wasn't used yet it opens it and inserts to [`Executor::heap_files`].
    ///
    /// It is possible that table was removed just before we started processing current statement
    /// (so [`Analyzer`] didn't report any problem) - in such case we just return an error that
    /// table does not exist.
    fn with_heap_file<R>(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
        f: impl FnOnce(&HeapFile<HEAP_FILE_BUCKET_SIZE>) -> R,
    ) -> Result<R, InternalExecutorError> {
        self.heap_files
            .entry(table_name.clone().into())
            .or_try_insert_with(|| self.open_heap_file(table_name.clone()))?;
        let hf = self.heap_files.get(table_name.as_ref()).ok_or(
            InternalExecutorError::TableDoesNotExist {
                table_name: table_name.clone().into(),
            },
        )?;
        Ok(f(hf.value()))
    }

    /// Creates new heap file for given table.
    ///
    /// As in [`Executor::with_heap_file`] if table was removed just before we started processing
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

/// Executes a single statement from a query plan.
///
/// This struct encapsulates all the logic needed to execute one statement, including
/// query operations (SELECT) and mutation operations (CREATE TABLE, INSERT, etc.).
struct StatementExecutor<'e, 'q> {
    executor: &'e Executor,
    statement: &'q StatementPlan,
    ast: &'q ResolvedTree,
}

/// Result of projection operation containing records and their column metadata.
///
/// The records vector contains transformed records with only the projected columns,
/// while the columns vector describes the schema of those projected records.
struct ProjectedRecords {
    records: Vec<Record>,
    columns: Vec<ColumnData>,
}

/// Column metadata used during projection operations.
struct ProjectColumn<'c> {
    rc: &'c ResolvedColumn,
    /// Set to true if this is the last struct that references the [`ResolvedColumn`].
    last: bool,
}

impl<'e, 'q> StatementExecutor<'e, 'q> {
    fn new(executor: &'e Executor, statement: &'q StatementPlan, ast: &'q ResolvedTree) -> Self {
        StatementExecutor {
            executor,
            statement,
            ast,
        }
    }

    /// Executes [`StatementExecutor::statement`] and returns its result.
    fn execute(&self) -> StatementResult {
        let root = self.statement.root();

        match root.produces_result_set() {
            true => self.execute_query(root),
            false => self.execute_mutation(root),
        }
    }

    /// Handler for all statements that only return data.
    fn execute_query(&self, item: &StatementPlanItem) -> StatementResult {
        match item {
            StatementPlanItem::Projection(projection) => self.projection(projection),
            _ => self.executor.runtime_error(format!(
                "Invalid root operation ({:?}) for query statement",
                item
            )),
        }
    }

    /// Handler for all statements that mutate data.
    fn execute_mutation(&self, item: &StatementPlanItem) -> StatementResult {
        match item {
            StatementPlanItem::CreateTable(create_table) => self.create_table(create_table),
            _ => self.executor.runtime_error(format!(
                "Invalid root operation ({:?}) for mutation statement",
                item
            )),
        }
    }

    /// Handler for all statements that are source of data.
    fn execute_data_source(
        &self,
        data_source: &StatementPlanItem,
    ) -> Result<Vec<Record>, InternalExecutorError> {
        match data_source {
            StatementPlanItem::TableScan(table_scan) => self.table_scan(table_scan),
            _ => Err(InternalExecutorError::InvalidOperationInDataSource {
                operation: format!("{:?}", data_source),
            }),
        }
    }

    /// Handler for [`TableScan`] statement.
    fn table_scan(&self, table_scan: &TableScan) -> Result<Vec<Record>, InternalExecutorError> {
        let records = self
            .executor
            .with_heap_file(&table_scan.table_name, |hf| hf.all_records())??;
        Ok(records)
    }

    /// Handler for [`Projection`] statement.
    fn projection(&self, projection: &Projection) -> StatementResult {
        let data_source = self.statement.item(projection.data_source);
        let records = match self.execute_data_source(data_source) {
            Ok(records) => records,
            Err(err) => {
                return StatementResult::RuntimeError {
                    error: err.to_string(),
                };
            }
        };
        match self.project_records(records, &projection.columns) {
            Ok(projected_records) => StatementResult::SelectSuccessful {
                columns: projected_records.columns,
                rows: projected_records.records,
            },
            Err(err) => {
                return StatementResult::RuntimeError {
                    error: err.to_string(),
                };
            }
        }
    }

    /// Transforms `records` so that they only contain specified `columns`.
    fn project_records(
        &self,
        records: Vec<Record>,
        columns: &[ResolvedNodeId],
    ) -> Result<ProjectedRecords, InternalExecutorError> {
        let project_columns: Vec<_> = self.map_expressions_to_project_columns(columns).collect();
        let columns_data: Vec<_> = project_columns
            .iter()
            .map(|sc| ColumnData::from(sc.rc))
            .collect();

        let projected_records: Vec<_> = records
            .into_iter()
            .map(|record| {
                let mut source_fields = record.fields;
                let fields: Vec<_> = project_columns
                    .iter()
                    .map(|select_col| {
                        let pos = select_col.rc.pos as usize;
                        match select_col.last {
                            true => mem::replace(&mut source_fields[pos], Field::Bool(false)),
                            false => source_fields[pos].clone(),
                        }
                    })
                    .collect();
                Record::new(fields)
            })
            .collect();

        Ok(ProjectedRecords {
            records: projected_records,
            columns: columns_data,
        })
    }

    /// Handler for [`CreateTable`] table statement.
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

        StatementResult::OperationSuccessful {
            rows_affected: 0,
            ty: StatementType::Create,
        }
    }

    /// Creates iterator over all columns in [`CreateTable`] (including primary key column)
    /// and sorts them by whether they are fixed-size (fixed-size columns are first).
    fn sort_columns_by_fixed_size<'c>(
        &self,
        create_table: &'c CreateTable,
    ) -> impl Iterator<Item = &'c ResolvedCreateColumnDescriptor> {
        iter::once(&create_table.primary_key_column)
            .chain(create_table.columns.iter())
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

    /// Creates iterator that maps `expressions` into [`ProjectColumn`]s.
    /// It assumes that each expression points to [`ResolvedExpression::ColumnRef`].
    fn map_expressions_to_project_columns(
        &self,
        expressions: &'q [ResolvedNodeId],
    ) -> impl Iterator<Item = ProjectColumn<'q>> {
        let mut last_occurrence = HashMap::new();
        for (idx, &expr) in expressions.iter().enumerate() {
            last_occurrence.insert(expr, idx);
        }

        expressions.iter().enumerate().map(move |(idx, &expr)| {
            let last = last_occurrence.get(&expr) == Some(&idx);
            match self.ast.node(expr) {
                ResolvedExpression::ColumnRef(rc) => ProjectColumn { rc, last },
                _ => unreachable!(),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    const METADATA_FILE_NAME: &str = "metadata.coDB";

    // Helper to create a test catalog
    fn create_catalog() -> (Catalog, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap_or_else(|_| {
            let db_dir = temp_dir.path().join("test_db");
            fs::create_dir(&db_dir).unwrap();
            let db_path = db_dir.join(METADATA_FILE_NAME);

            fs::write(db_path, r#"{"tables":[]}"#).unwrap();
            Catalog::new(temp_dir.path(), "test_db").unwrap()
        });
        (catalog, temp_dir)
    }

    // Helper to create a test executor
    fn create_test_executor() -> (Executor, TempDir) {
        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");
        let executor = Executor::new(db_path, catalog).unwrap();
        (executor, temp_dir)
    }

    // Helper to transform query to single statement
    fn create_single_statement(query: &str, executor: &Executor) -> (StatementPlan, ResolvedTree) {
        let query_plan = planner::process_query(query, executor.catalog.clone()).unwrap();

        (
            query_plan.plans.into_iter().next().unwrap(),
            query_plan.tree,
        )
    }

    fn expect_select_successful(result: StatementResult) -> (Vec<ColumnData>, Vec<Record>) {
        match result {
            StatementResult::SelectSuccessful { columns, rows } => (columns, rows),
            other => panic!("Expected SelectSuccessful, got {:?}", other),
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
        let (plan, ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        let result = executor.execute_statement(&plan, &ast);

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
        let (plan, ast) = create_single_statement(
            "CREATE TABLE mixed_table (name STRING, score FLOAT64, description STRING, active BOOL, id INT32 PRIMARY_KEY);",
            &executor,
        );

        let result = executor.execute_statement(&plan, &ast);

        assert_operation_successful(result, 0, StatementType::Create);

        let catalog = executor.catalog.read();
        let table = catalog.table("mixed_table").unwrap();
        let columns: Vec<_> = table.columns().collect();

        // Here it's important that first we have fixed-size elements and only then we have variable-size ones.
        assert_eq!(columns[0].name(), "id");
        assert_eq!(columns[1].name(), "score");
        assert_eq!(columns[2].name(), "active");
        assert_eq!(columns[3].name(), "name");
        assert_eq!(columns[4].name(), "description");
    }

    #[test]
    fn test_process_columns_calculates_correct_offsets() {
        let (executor, _temp_dir) = create_test_executor();
        let (plan, ast) = create_single_statement(
            "CREATE TABLE test (name STRING, score FLOAT64, surname STRING, active BOOL, id INT32 PRIMARY_KEY);",
            &executor,
        );

        let se = StatementExecutor::new(&executor, &plan, &ast);

        let create_table = match plan.root() {
            StatementPlanItem::CreateTable(ct) => ct,
            _ => panic!("invalid item"),
        };
        let columns = se.process_columns(create_table).unwrap();

        let id_col = columns.iter().find(|c| c.name() == "id").unwrap();
        assert_eq!(id_col.base_offset(), 0);
        assert_eq!(id_col.base_offset_pos(), 0);
        assert_eq!(id_col.pos(), 0);

        let score_col = columns.iter().find(|c| c.name() == "score").unwrap();
        assert_eq!(score_col.base_offset(), 4);
        assert_eq!(score_col.base_offset_pos(), 1);
        assert_eq!(score_col.pos(), 1);

        let active_col = columns.iter().find(|c| c.name() == "active").unwrap();
        assert_eq!(active_col.base_offset(), 12);
        assert_eq!(active_col.base_offset_pos(), 2);
        assert_eq!(active_col.pos(), 2);

        let name_col = columns.iter().find(|c| c.name() == "name").unwrap();
        assert_eq!(name_col.base_offset(), 13);
        assert_eq!(name_col.base_offset_pos(), 3);
        assert_eq!(name_col.pos(), 3);

        let surname_col = columns.iter().find(|c| c.name() == "surname").unwrap();
        assert_eq!(surname_col.base_offset(), 13);
        assert_eq!(surname_col.base_offset_pos(), 3);
        assert_eq!(surname_col.pos(), 4);
    }

    #[test]
    fn test_execute_select_statement_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_execute_select_statement_all_columns() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        let create_result = executor.execute_statement(&create_plan, &create_ast);
        assert_operation_successful(create_result, 0, StatementType::Create);

        // Insert records directly using heap file
        let record1 = Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::String("Alice".into()),
        ]);

        let record2 = Record::new(vec![
            Field::Int32(2),
            Field::Int32(30),
            Field::String("Bob".into()),
        ]);

        let record3 = Record::new(vec![
            Field::Int32(3),
            Field::Int32(35),
            Field::String("Charlie".into()),
        ]);
        executor
            .with_heap_file("users", |hf| {
                hf.insert(record1).unwrap();
                hf.insert(record2).unwrap();
                hf.insert(record3).unwrap();
            })
            .unwrap();

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ty, Type::I32);
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].ty, Type::String);
        assert_eq!(columns[2].name, "age");
        assert_eq!(columns[2].ty, Type::I32);

        assert_eq!(rows.len(), 3);

        let alice = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(&alice.fields[1], Field::String(s) if s == "Alice"));
        assert!(matches!(alice.fields[2], Field::Int32(25)));

        let bob = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(&bob.fields[1], Field::String(s) if s == "Bob"));
        assert!(matches!(bob.fields[2], Field::Int32(30)));

        let charlie = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(&charlie.fields[1], Field::String(s) if s == "Charlie"));
        assert!(matches!(charlie.fields[2], Field::Int32(35)));
    }

    #[test]
    fn test_execute_select_statement_subset_of_columns() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        // Insert records directly using heap file
        let record1 = Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::String("Alice".into()),
        ]);
        let record2 = Record::new(vec![
            Field::Int32(2),
            Field::Int32(30),
            Field::String("Bob".into()),
        ]);
        executor
            .with_heap_file("users", |hf| {
                hf.insert(record1).unwrap();
                hf.insert(record2).unwrap();
            })
            .unwrap();

        // Execute SELECT with only name and age
        let (select_plan, select_ast) =
            create_single_statement("SELECT name, age FROM users;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "name");
        assert_eq!(columns[0].ty, Type::String);
        assert_eq!(columns[1].name, "age");
        assert_eq!(columns[1].ty, Type::I32);

        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| r.fields[1] == Field::Int32(25))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert!(matches!(&alice.fields[0], Field::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| r.fields[1] == Field::Int32(30))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert!(matches!(&bob.fields[0], Field::String(s) if s == "Bob"));
    }

    #[test]
    fn test_execute_select_statement_star_all_columns() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        let create_result = executor.execute_statement(&create_plan, &create_ast);
        assert_operation_successful(create_result, 0, StatementType::Create);

        // Insert records directly using heap file
        let record1 = Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::String("Alice".into()),
        ]);

        let record2 = Record::new(vec![
            Field::Int32(2),
            Field::Int32(30),
            Field::String("Bob".into()),
        ]);

        let record3 = Record::new(vec![
            Field::Int32(3),
            Field::Int32(35),
            Field::String("Charlie".into()),
        ]);

        executor
            .with_heap_file("users", |hf| {
                hf.insert(record1).unwrap();
                hf.insert(record2).unwrap();
                hf.insert(record3).unwrap();
            })
            .unwrap();

        let (select_plan, select_ast) = create_single_statement("SELECT * FROM users;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ty, Type::I32);
        assert_eq!(columns[1].name, "age");
        assert_eq!(columns[1].ty, Type::I32);
        assert_eq!(columns[2].name, "name");
        assert_eq!(columns[2].ty, Type::String);

        assert_eq!(rows.len(), 3);

        let alice = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(alice.fields[1], Field::Int32(25)));
        assert!(matches!(&alice.fields[2], Field::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(bob.fields[1], Field::Int32(30)));
        assert!(matches!(&bob.fields[2], Field::String(s) if s == "Bob"));

        let charlie = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(charlie.fields[1], Field::Int32(35)));
        assert!(matches!(&charlie.fields[2], Field::String(s) if s == "Charlie"));
    }

    #[test]
    fn test_execute_select_statement_duplicate_columns() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        // Insert records directly using heap file
        let record1 = Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::String("Alice".into()),
        ]);
        let record2 = Record::new(vec![
            Field::Int32(2),
            Field::Int32(30),
            Field::String("Bob".into()),
        ]);
        executor
            .with_heap_file("users", |hf| {
                hf.insert(record1).unwrap();
                hf.insert(record2).unwrap();
            })
            .unwrap();

        // Execute SELECT with same column twice
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, id FROM users;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].ty, Type::I32);
        assert_eq!(columns[1].name, "id");
        assert_eq!(columns[1].ty, Type::I32);

        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert_eq!(alice.fields[0], Field::Int32(1));
        assert_eq!(alice.fields[1], Field::Int32(1));

        let bob = rows
            .iter()
            .find(|r| r.fields[0] == Field::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert_eq!(bob.fields[0], Field::Int32(2));
        assert_eq!(bob.fields[1], Field::Int32(2));
    }
}
