mod consts;
mod error_factory;
mod expression_executor;
mod iterators;
pub mod response;
mod statement_executor;

use std::{path::Path, sync::Arc};

use crate::{
    consts::HEAP_FILE_BUCKET_SIZE,
    error_factory::InternalExecutorError,
    iterators::{ParseErrorIter, QueryResultIter, StatementIter},
    response::StatementResult,
    statement_executor::StatementExecutor,
};
use dashmap::DashMap;
use engine::b_tree::{BTree, BTreeFactory};
use engine::heap_file::{HeapFile, HeapFileFactory};
use metadata::catalog::Catalog;
use parking_lot::RwLock;
use planner::{query_plan::StatementPlan, resolved_tree::ResolvedTree};
use storage::cache::CacheError;
use storage::write_ahead_log::{WalError, spawn_wal};
use storage::{
    background_worker::BackgroundWorkerHandle,
    cache::Cache,
    files_manager::{FileKey, FilesManager, FilesManagerError},
};
use thiserror::Error;

pub struct Executor {
    heap_files: DashMap<String, HeapFile<HEAP_FILE_BUCKET_SIZE>>,
    b_trees: DashMap<String, BTree>,
    cache: Arc<Cache>,
    catalog: Arc<RwLock<Catalog>>,
}

/// Error for [`Executor`] related operations
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("Cannot open files manager: {0}")]
    CannotOpenFilesManager(#[from] FilesManagerError),
    #[error("Cannot open write-ahead log: {0}")]
    CannotOpenWAL(#[from] WalError),
    #[error("Error occurred while redoing WAL records in cache: {0}")]
    CannotRedoWALRecords(#[from] CacheError),
}

impl Executor {
    /// Creates new [`Executor`] for database at `database_path`.
    pub fn new(database_path: impl AsRef<Path>, catalog: Catalog) -> Result<Self, ExecutorError> {
        let files = Arc::new(FilesManager::new(database_path)?);
        let cache = Cache::new(consts::CACHE_SIZE, files, None);
        let catalog = Arc::new(RwLock::new(catalog));
        Ok(Executor {
            heap_files: DashMap::new(),
            b_trees: DashMap::new(),
            cache,
            catalog,
        })
    }

    /// Creates new [`Executor`] for database at `database_path` and initializes background threads used by its components.
    /// Returns executor and handles to the background threads (with the distinction between WAL and other
    /// background workers needing to be made).
    pub fn with_background_workers(
        database_path: impl AsRef<Path>,
        catalog: Catalog,
    ) -> Result<(Self, Vec<BackgroundWorkerHandle>, BackgroundWorkerHandle), ExecutorError> {
        let (files, files_background_worker) = FilesManager::with_background_cleaner(
            database_path.as_ref(),
            consts::FILES_MANAGER_CLEANUP_INTERVAL,
        )?;

        let (wal_handle, wal_background_worker) = spawn_wal(
            database_path,
            consts::WAL_FLUSH_INTERVAL_MS,
            consts::WAL_MAX_UNFLUSHED_RECORDS,
        )?;

        let (cache, cache_background_worker) = Cache::with_background_cleaner(
            consts::CACHE_SIZE,
            files,
            consts::CACHE_CLEANUP_INTERVAL,
            Some(wal_handle),
        )?;

        let catalog = Arc::new(RwLock::new(catalog));
        let executor = Executor {
            heap_files: DashMap::new(),
            b_trees: DashMap::new(),
            cache,
            catalog,
        };
        let workers = vec![cache_background_worker, files_background_worker];
        Ok((executor, workers, wal_background_worker))
    }

    /// Parses `query` and returns iterator over results for each statement in the `query`.
    pub fn execute<'e>(&'e self, query: &str) -> QueryResultIter<'e> {
        let parse_output = planner::process_query(query, self.catalog.clone());
        match parse_output {
            Ok(query_plan) => StatementIter::new(query_plan.plans, query_plan.tree, self).into(),
            Err(errors) => ParseErrorIter::new(errors).into(),
        }
    }

    /// Executes single statement by delegating work to [`StatementExecutor`].
    fn execute_statement(&self, statement: &StatementPlan, ast: &ResolvedTree) -> StatementResult {
        let se = StatementExecutor::new(self, statement, ast);
        se.execute()
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
        let entry = self
            .heap_files
            .entry(table_name.clone().into())
            .or_try_insert_with(|| self.open_heap_file(table_name.clone()))?;

        Ok(f(entry.value()))
    }

    /// Same as [`Executor::with_heap_file`], but with mutable reference to [`HeapFile`].
    fn with_heap_file_mut<R>(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
        f: impl FnOnce(&mut HeapFile<HEAP_FILE_BUCKET_SIZE>) -> R,
    ) -> Result<R, InternalExecutorError> {
        self.heap_files
            .entry(table_name.clone().into())
            .or_try_insert_with(|| self.open_heap_file(table_name.clone()))?;
        let mut hf = self.heap_files.get_mut(table_name.as_ref()).ok_or(
            InternalExecutorError::TableDoesNotExist {
                table_name: table_name.clone().into(),
            },
        )?;
        Ok(f(hf.value_mut()))
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

    /// Similar to [`Executor::open_heap_file`], but uses catalog passed in arguments + inserts heap file to dashmap.
    ///
    /// This way we can open heap file while holding write lock to catalog
    /// (this is used in alter column statements).
    fn insert_heap_file_with_catalog_lock(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
        catalog_lock: &Catalog,
    ) -> Result<(), InternalExecutorError> {
        let file_key = FileKey::data(table_name.clone());
        let cache = self.cache.clone();
        let columns_metadata = catalog_lock
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
        self.heap_files.insert(table_name.into(), heap_file);
        Ok(())
    }

    fn remove_heap_file(&self, table_name: impl AsRef<str>) {
        self.heap_files.remove(table_name.as_ref());
    }

    /// Gets B-Tree for given table and passes it to function `f`.
    /// If B-Tree wasn't used yet it opens it and inserts to [`Executor::b_trees`].
    fn with_b_tree<R>(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
        f: impl FnOnce(&BTree) -> R,
    ) -> Result<R, InternalExecutorError> {
        let entry = self
            .b_trees
            .entry(table_name.clone().into())
            .or_try_insert_with(|| self.open_b_tree(table_name.clone()))?;

        Ok(f(entry.value()))
    }

    /// Creates new B-Tree for given table.
    fn open_b_tree(
        &self,
        table_name: impl Into<String> + Clone + AsRef<str>,
    ) -> Result<BTree, InternalExecutorError> {
        let file_key = FileKey::index(table_name.clone());
        let cache = self.cache.clone();
        let b_tree_factory = BTreeFactory::new(file_key, cache);
        let b_tree = b_tree_factory.create_btree().map_err(|err| {
            InternalExecutorError::CannotCreateBTree {
                reason: err.to_string(),
            }
        })?;
        Ok(b_tree)
    }

    fn remove_b_tree(&self, table_name: impl AsRef<str>) {
        self.b_trees.remove(table_name.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::{fs, thread, time};

    use engine::record::Record;
    use tempfile::TempDir;
    use types::{data::Value, schema::Type};

    use crate::response::{ColumnData, StatementType};

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
    pub(crate) fn create_test_executor() -> (Executor, TempDir) {
        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");
        let executor = Executor::new(db_path, catalog).unwrap();
        (executor, temp_dir)
    }

    // Helper to transform query to single statement
    pub(crate) fn create_single_statement(
        query: &str,
        executor: &Executor,
    ) -> (StatementPlan, ResolvedTree) {
        let query_plan = planner::process_query(query, executor.catalog.clone()).unwrap();

        (
            query_plan.plans.into_iter().next().unwrap(),
            query_plan.tree,
        )
    }

    // Helper to execute a single statement and unwrap the result
    fn execute_single(executor: &Executor, query: &str) -> StatementResult {
        executor.execute(query).next().unwrap()
    }

    fn expect_select_successful(result: StatementResult) -> (Vec<ColumnData>, Vec<Record>) {
        match result {
            StatementResult::SelectSuccessful { columns, rows } => (columns, rows),
            other => panic!("Expected SelectSuccessful, got {:?}", other),
        }
    }

    // Helper to check if a plan uses index scan
    fn assert_uses_index_scan(plan: &StatementPlan) {
        use planner::query_plan::StatementPlanItem;

        // Walk through the plan starting from root to find IndexScan
        let current_item = plan.root();
        let mut found_index_scan = matches!(current_item, StatementPlanItem::IndexScan(_));

        // If root is not IndexScan, check if it references a data source that is
        if !found_index_scan && let Some(data_source_id) = get_data_source(current_item) {
            let data_source = plan.item(data_source_id);
            found_index_scan = matches!(data_source, StatementPlanItem::IndexScan(_));

            // Check one more level deep if needed (for chained operators)
            if !found_index_scan && let Some(nested_id) = get_data_source(data_source) {
                let nested = plan.item(nested_id);
                found_index_scan = matches!(nested, StatementPlanItem::IndexScan(_));
            }
        }

        assert!(
            found_index_scan,
            "Expected plan to use IndexScan, but it doesn't. Root: {:?}",
            match current_item {
                StatementPlanItem::TableScan(_) => "TableScan",
                StatementPlanItem::IndexScan(_) => "IndexScan",
                StatementPlanItem::Filter(_) => "Filter",
                StatementPlanItem::Projection(_) => "Projection",
                StatementPlanItem::Delete(_) => "Delete",
                StatementPlanItem::Update(_) => "Update",
                _ => "Other",
            }
        );
    }

    // Helper to check if a plan uses table scan (not index scan)
    fn assert_uses_table_scan(plan: &StatementPlan) {
        use planner::query_plan::StatementPlanItem;

        // Walk through the plan starting from root to find TableScan
        let current_item = plan.root();
        let mut found_table_scan = matches!(current_item, StatementPlanItem::TableScan(_));

        // If root is not TableScan, check if it references a data source that is
        if !found_table_scan && let Some(data_source_id) = get_data_source(current_item) {
            let data_source = plan.item(data_source_id);
            found_table_scan = matches!(data_source, StatementPlanItem::TableScan(_));

            // Check one more level deep if needed
            if !found_table_scan && let Some(nested_id) = get_data_source(data_source) {
                let nested = plan.item(nested_id);
                found_table_scan = matches!(nested, StatementPlanItem::TableScan(_));
            }
        }

        assert!(
            found_table_scan,
            "Expected plan to use TableScan, but it doesn't"
        );
    }

    // Helper to extract data_source from plan items that have one
    fn get_data_source(
        item: &planner::query_plan::StatementPlanItem,
    ) -> Option<planner::query_plan::StatementPlanItemId> {
        use planner::query_plan::StatementPlanItem;

        match item {
            StatementPlanItem::Filter(f) => Some(f.data_source),
            StatementPlanItem::Sort(s) => Some(s.data_source),
            StatementPlanItem::Limit(l) => Some(l.data_source),
            StatementPlanItem::Skip(s) => Some(s.data_source),
            StatementPlanItem::Projection(p) => Some(p.data_source),
            StatementPlanItem::Delete(d) => Some(d.data_source),
            StatementPlanItem::Update(u) => Some(u.data_source),
            _ => None,
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

    fn assert_parse_error_contains(result: StatementResult, expected_message: &str) {
        match result {
            StatementResult::ParseError { error } => {
                assert!(error.contains(expected_message));
            }
            other => panic!("Expected ParseError, got {:?}", other),
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
    fn test_execute_select_statement_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

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

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

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
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(&alice.fields[1].deref(), Value::String(s) if s == "Alice"));
        assert!(matches!(alice.fields[2].deref(), Value::Int32(25)));

        let bob = rows
            .iter()
            .find(|&r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(&bob.fields[1].deref(), Value::String(s) if s == "Bob"));
        assert!(matches!(bob.fields[2].deref(), Value::Int32(30)));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(&charlie.fields[1].deref(), Value::String(s) if s == "Charlie"));
        assert!(matches!(charlie.fields[2].deref(), Value::Int32(35)));
    }

    #[test]
    fn test_execute_select_statement_subset_of_columns() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

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
            .find(|r| *r.fields[1].deref() == Value::Int32(25))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert!(matches!(&alice.fields[0].deref(), Value::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| *r.fields[1].deref() == Value::Int32(30))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert!(matches!(&bob.fields[0].deref(), Value::String(s) if s == "Bob"));
    }

    #[test]
    fn test_execute_select_statement_star_all_columns() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

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
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(alice.fields[1].deref(), Value::Int32(25)));
        assert!(matches!(&alice.fields[2].deref(), Value::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(bob.fields[1].deref(), Value::Int32(30)));
        assert!(matches!(&bob.fields[2].deref(), Value::String(s) if s == "Bob"));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(charlie.fields[1].deref(), Value::Int32(35)));
        assert!(matches!(&charlie.fields[2].deref(), Value::String(s) if s == "Charlie"));
    }

    #[test]
    fn test_execute_select_statement_duplicate_columns() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

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
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert_eq!(*alice.fields[0].deref(), Value::Int32(1));
        assert_eq!(*alice.fields[1].deref(), Value::Int32(1));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert_eq!(*bob.fields[0].deref(), Value::Int32(2));
        assert_eq!(*bob.fields[1].deref(), Value::Int32(2));
    }

    #[test]
    fn test_execute_select_with_where_clause_single_condition() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 25);",
        );

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users WHERE age = 25;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 2);

        assert!(
            rows.iter()
                .all(|r| *r.fields[2].deref() == Value::Int32(25))
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1].deref(), Value::String(s) if s == "Alice"))
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1].deref(), Value::String(s) if s == "Charlie"))
        );
    }

    #[test]
    fn test_execute_select_with_where_clause_comparison_operators() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (1, 'Product A', 100);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (2, 'Product B', 200);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (3, 'Product C', 150);",
        );

        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name FROM products WHERE price > 100;",
            &executor,
        );

        let result = executor.execute_statement(&select_plan, &select_ast);
        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
    }

    #[test]
    fn test_execute_select_with_where_clause_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users WHERE age = 99;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_execute_select_with_where_clause_all_match() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);",
        );

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users WHERE TRUE;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_sorting_integers_ascending() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 30);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 20);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test ORDER BY value ASC;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 3);
        assert_eq!(*rows[0].fields[1].deref(), Value::Int32(10));
        assert_eq!(*rows[1].fields[1].deref(), Value::Int32(20));
        assert_eq!(*rows[2].fields[1].deref(), Value::Int32(30));
    }

    #[test]
    fn test_sorting_integers_descending() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 30);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 20);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test ORDER BY value DESC;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 3);
        assert_eq!(*rows[0].fields[1].deref(), Value::Int32(30));
        assert_eq!(*rows[1].fields[1].deref(), Value::Int32(20));
        assert_eq!(*rows[2].fields[1].deref(), Value::Int32(10));
    }

    #[test]
    fn test_sorting_strings_ascending() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO test (id, name) VALUES (1, 'zebra');",
        );
        execute_single(
            &executor,
            "INSERT INTO test (id, name) VALUES (2, 'apple');",
        );
        execute_single(
            &executor,
            "INSERT INTO test (id, name) VALUES (3, 'banana');",
        );

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM test ORDER BY name ASC;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
        assert_eq!(*rows[0].fields[1].deref(), Value::String("apple".into()));
        assert_eq!(*rows[1].fields[1].deref(), Value::String("banana".into()));
        assert_eq!(*rows[2].fields[1].deref(), Value::String("zebra".into()));
    }

    #[test]
    fn test_sorting_floats_implicit_ascending() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, score FLOAT64);",
        );
        execute_single(&executor, "INSERT INTO test (id, score) VALUES (1, 3.75);");
        execute_single(&executor, "INSERT INTO test (id, score) VALUES (2, 1.5);");
        execute_single(&executor, "INSERT INTO test (id, score) VALUES (3, 2.25);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, score FROM test ORDER BY score;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
        assert_eq!(*rows[0].fields[1].deref(), Value::Float64(1.5));
        assert_eq!(*rows[1].fields[1].deref(), Value::Float64(2.25));
        assert_eq!(*rows[2].fields[1].deref(), Value::Float64(3.75));
    }

    #[test]
    fn test_sorting_booleans() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, active BOOL);",
        );
        execute_single(&executor, "INSERT INTO test (id, active) VALUES (1, TRUE);");
        execute_single(
            &executor,
            "INSERT INTO test (id, active) VALUES (2, FALSE);",
        );
        execute_single(&executor, "INSERT INTO test (id, active) VALUES (3, TRUE);");

        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, active FROM test ORDER BY active ASC;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
        assert_eq!(*rows[0].fields[1].deref(), Value::Bool(false));
        assert_eq!(*rows[1].fields[1].deref(), Value::Bool(true));
        assert_eq!(*rows[2].fields[1].deref(), Value::Bool(true));
    }

    #[test]
    fn test_sorting_duplicate_values() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (4, 20);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test ORDER BY value ASC;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 4);
        assert_eq!(*rows[0].fields[1].deref(), Value::Int32(10));
        assert_eq!(*rows[1].fields[1].deref(), Value::Int32(10));
        assert_eq!(*rows[2].fields[1].deref(), Value::Int32(20));
        assert_eq!(*rows[3].fields[1].deref(), Value::Int32(20));
    }

    #[test]
    fn test_limit_fewer_rows_than_limit() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test LIMIT 5;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_limit_exact_number_of_rows() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test LIMIT 3;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_limit_more_rows_than_limit() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (4, 40);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (5, 50);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test LIMIT 3;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_limit_zero() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test LIMIT 0;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_offset_fewer_rows_than_offset() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test OFFSET 5;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_offset_exact_number_of_rows() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test OFFSET 3;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_offset_skip_some_rows() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (3, 30);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (4, 40);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (5, 50);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test OFFSET 2;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_offset_zero() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);",
        );
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (1, 10);");
        execute_single(&executor, "INSERT INTO test (id, value) VALUES (2, 20);");

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, value FROM test OFFSET 0;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_combined_where_order_limit_offset() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32, category STRING);",
        );

        // Insert test data
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (1, 'Laptop', 1200, 'Electronics');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (2, 'Mouse', 25, 'Electronics');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (3, 'Keyboard', 75, 'Electronics');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (4, 'Monitor', 300, 'Electronics');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (5, 'Desk', 200, 'Furniture');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (6, 'Chair', 150, 'Furniture');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (7, 'Headphones', 100, 'Electronics');",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, category) VALUES (8, 'Webcam', 80, 'Electronics');",
        );

        // Query: Get Electronics products with price > 50, ordered by price DESC, skip first result, take 2
        // Expected: After filtering (Mouse excluded), sorted DESC: Laptop(1200), Monitor(300), Headphones(100), Webcam(80), Keyboard(75)
        // After OFFSET 1: Monitor(300), Headphones(100), Webcam(80), Keyboard(75),
        // After LIMIT 2: Monitor(300), Headphones(100)
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name, price FROM products WHERE category = 'Electronics' AND price > 50 ORDER BY price DESC OFFSET 1 LIMIT 2;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[2].name, "price");

        assert_eq!(rows.len(), 2);

        // First row should be Monitor (300)
        assert_eq!(*rows[0].fields[0].deref(), Value::Int32(4));
        assert_eq!(*rows[0].fields[1].deref(), Value::String("Monitor".into()));
        assert_eq!(*rows[0].fields[2].deref(), Value::Int32(300));

        // Second row should be Headphones (100)
        assert_eq!(*rows[1].fields[0].deref(), Value::Int32(7));
        assert_eq!(
            *rows[1].fields[1].deref(),
            Value::String("Headphones".into())
        );
        assert_eq!(*rows[1].fields[2].deref(), Value::Int32(100));

        assert!(rows.iter().all(|r| {
            if let Value::Int32(price) = *r.fields[2].deref() {
                price > 50
            } else {
                false
            }
        }));
    }

    // TODO: add tests for sorting date and datetimes once they are handled

    #[test]
    fn test_execute_insert_single_row() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );
        executor.execute_statement(&create_plan, &create_ast);

        // Insert a single row
        let (insert_plan, insert_ast) = create_single_statement(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
            &executor,
        );
        let insert_result = executor.execute_statement(&insert_plan, &insert_ast);
        assert_operation_successful(insert_result, 1, StatementType::Insert);

        // Verify the data was inserted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert!(matches!(&row.fields[1].deref(), Value::String(s) if s == "Alice"));
        assert_eq!(*row.fields[2].deref(), Value::Int32(25));
    }

    #[test]
    fn test_execute_insert_multiple_rows() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );
        executor.execute_statement(&create_plan, &create_ast);

        // Insert multiple rows
        let (insert1_plan, insert1_ast) = create_single_statement(
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
            &executor,
        );
        executor.execute_statement(&insert1_plan, &insert1_ast);

        let (insert2_plan, insert2_ast) = create_single_statement(
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
            &executor,
        );
        executor.execute_statement(&insert2_plan, &insert2_ast);

        let (insert3_plan, insert3_ast) = create_single_statement(
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
            &executor,
        );
        let insert_result = executor.execute_statement(&insert3_plan, &insert3_ast);
        assert_operation_successful(insert_result, 1, StatementType::Insert);

        // Verify all data was inserted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 3);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert!(matches!(&alice.fields[1].deref(), Value::String(s) if s == "Alice"));
        assert_eq!(*alice.fields[2].deref(), Value::Int32(25));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert!(matches!(&bob.fields[1].deref(), Value::String(s) if s == "Bob"));
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert!(matches!(&charlie.fields[1].deref(), Value::String(s) if s == "Charlie"));
        assert_eq!(*charlie.fields[2].deref(), Value::Int32(35));
    }

    #[test]
    fn test_execute_insert_with_different_column_order() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );
        executor.execute_statement(&create_plan, &create_ast);

        // Insert with columns in different order
        let (insert_plan, insert_ast) = create_single_statement(
            "INSERT INTO users (age, name, id) VALUES (25, 'Alice', 1);",
            &executor,
        );
        let insert_result = executor.execute_statement(&insert_plan, &insert_ast);
        assert_operation_successful(insert_result, 1, StatementType::Insert);

        // Verify the data was inserted correctly
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert!(matches!(&row.fields[1].deref(), Value::String(s) if s == "Alice"));
        assert_eq!(*row.fields[2].deref(), Value::Int32(25));
    }

    #[test]
    fn test_with_background_workers_starts_and_stoppable() {
        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        let (executor, mut workers, mut wal_worker) =
            Executor::with_background_workers(db_path, catalog)
                .expect("with_background_workers should succeed");

        // We expect three background workers (cache and files manager and wal)
        assert_eq!(workers.len(), 2);

        // Shutdown and join all workers to ensure threads are started and can be stopped.
        while let Some(mut handle) = workers.pop() {
            handle.shutdown().expect("shutdown should succeed");
            handle.join().expect("join should succeed");
        }

        wal_worker.shutdown().expect("shutdown should succeed");
        wal_worker.join().expect("join should succeed");

        // Basic sanity check that executor was created and holds structures
        let _c = executor.catalog.read();
    }

    #[test]
    fn test_execute_insert_twice_with_the_same_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );
        executor.execute_statement(&create_plan, &create_ast);

        // Insert first row
        let (insert_plan, insert_ast) = create_single_statement(
            "INSERT INTO users (age, name, id) VALUES (25, 'Alice', 1);",
            &executor,
        );
        let insert_result = executor.execute_statement(&insert_plan, &insert_ast);
        assert_operation_successful(insert_result, 1, StatementType::Insert);

        // Insert second row with the same primary key
        let (insert_plan, insert_ast) = create_single_statement(
            "INSERT INTO users (age, name, id) VALUES (41, 'Sigma', 1);",
            &executor,
        );
        let insert_result = executor.execute_statement(&insert_plan, &insert_ast);
        match insert_result {
            StatementResult::RuntimeError { error } => {
                assert!(error.contains("key 'Int32(1)' already exists in table 'users'"));
            }
            other_result => panic!(
                "Expected error due to duplicate primary key and instead got {:?}",
                other_result
            ),
        }
    }

    #[test]
    fn test_add_column_to_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Add column to empty table
        let result = execute_single(&executor, "ALTER TABLE users ADD COLUMN age INT32;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify column was added by selecting from table
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[2].name, "age");
        assert_eq!(columns[2].ty, Type::I32);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_add_column_with_existing_records() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );
        execute_single(&executor, "INSERT INTO users (id, name) VALUES (2, 'Bob');");

        // Add column
        let result = execute_single(&executor, "ALTER TABLE users ADD COLUMN age INT32;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify records have default value
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        for row in &rows {
            assert_eq!(row.fields.len(), 3);
            assert_eq!(*row.fields[2].deref(), Value::default_for_ty(&Type::I32));
        }
    }

    #[test]
    fn test_add_multiple_columns_sequentially() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Add first column
        let result = execute_single(&executor, "ALTER TABLE users ADD COLUMN age INT32;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Add second column
        let result = execute_single(&executor, "ALTER TABLE users ADD COLUMN city STRING;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify both columns exist
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age, city FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[2].name, "age");
        assert_eq!(columns[3].name, "city");

        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert_eq!(*row.fields[1].deref(), Value::String("Alice".into()));
        assert_eq!(*row.fields[2].deref(), Value::default_for_ty(&Type::I32));
        assert_eq!(*row.fields[3].deref(), Value::default_for_ty(&Type::String));
    }

    #[test]
    fn test_add_column_can_insert_new_records() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Add column
        execute_single(&executor, "ALTER TABLE users ADD COLUMN age INT32;");

        // Insert new record with the new column
        let result = execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify both records
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[2].deref(), Value::default_for_ty(&Type::I32));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));
    }

    #[test]
    fn test_add_column_to_primary_key_only_table_with_existing_records() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table with only primary key
        execute_single(&executor, "CREATE TABLE users (id INT32 PRIMARY_KEY);");

        // Insert some records
        execute_single(&executor, "INSERT INTO users (id) VALUES (1);");
        execute_single(&executor, "INSERT INTO users (id) VALUES (2);");
        execute_single(&executor, "INSERT INTO users (id) VALUES (3);");

        // Add string column
        let result = execute_single(&executor, "ALTER TABLE users ADD COLUMN name STRING;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify column was added with default values
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 3);

        for row in &rows {
            assert_eq!(row.fields.len(), 2);
            assert_eq!(*row.fields[1].deref(), Value::default_for_ty(&Type::String));
        }
    }

    #[test]
    fn test_remove_column_from_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

        // Remove column from empty table
        let result = execute_single(&executor, "ALTER TABLE users DROP COLUMN age;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify column was removed by selecting from table
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_remove_column_with_existing_records() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Remove column
        let result = execute_single(&executor, "ALTER TABLE users DROP COLUMN age;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify records no longer have the removed column
        let (select_plan, select_ast) = create_single_statement("SELECT * FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert_eq!(*alice.fields[1].deref(), Value::String("Alice".into()));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert_eq!(*bob.fields[1].deref(), Value::String("Bob".into()));
    }

    #[test]
    fn test_remove_multiple_columns_sequentially() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32, city STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age, city) VALUES (1, 'Alice', 25, 'NYC');",
        );

        // Remove first column
        let result = execute_single(&executor, "ALTER TABLE users DROP COLUMN age;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Remove second column
        let result = execute_single(&executor, "ALTER TABLE users DROP COLUMN city;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify only id and name remain
        let (select_plan, select_ast) = create_single_statement("SELECT * FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");

        let row = &rows[0];
        assert_eq!(row.fields.len(), 2);
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert_eq!(*row.fields[1].deref(), Value::String("Alice".into()));
    }

    #[test]
    fn test_remove_column_can_insert_new_records() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );

        // Remove column
        execute_single(&executor, "ALTER TABLE users DROP COLUMN age;");

        // Insert new record without the removed column
        let result = execute_single(&executor, "INSERT INTO users (id, name) VALUES (2, 'Bob');");
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify both records
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert_eq!(*alice.fields[1].deref(), Value::String("Alice".into()));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert_eq!(*bob.fields[1].deref(), Value::String("Bob".into()));
    }

    #[test]
    fn test_add_then_remove_column() {
        let (executor, _temp_dir) = create_test_executor();

        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Add column
        execute_single(&executor, "ALTER TABLE users ADD COLUMN age INT32;");

        // Remove the same column
        let result = execute_single(&executor, "ALTER TABLE users DROP COLUMN age;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify back to original schema
        let (select_plan, select_ast) = create_single_statement("SELECT * FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert_eq!(*row.fields[1].deref(), Value::String("Alice".into()));
    }

    #[test]
    fn test_delete_all_records() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Delete all records
        let result = execute_single(&executor, "DELETE FROM users;");
        assert_operation_successful(result, 3, StatementType::Delete);

        // Verify table is empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_delete_from_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create empty table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

        // Try to delete from empty table
        let result = execute_single(&executor, "DELETE FROM users;");
        assert_operation_successful(result, 0, StatementType::Delete);

        // Verify table is still empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_delete_with_where_clause_single_match() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Delete one record
        let result = execute_single(&executor, "DELETE FROM users WHERE id = 2;");
        assert_operation_successful(result, 1, StatementType::Delete);

        // Verify correct record was deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
    }

    #[test]
    fn test_delete_with_where_clause_multiple_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (4, 'David', 30);",
        );

        // Delete multiple records matching condition
        let result = execute_single(&executor, "DELETE FROM users WHERE age = 30;");
        assert_operation_successful(result, 2, StatementType::Delete);

        // Verify correct records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        // Only users with age 25 should remain
        assert!(
            rows.iter()
                .all(|r| *r.fields[2].deref() == Value::Int32(25))
        );
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
    }

    #[test]
    fn test_delete_with_where_clause_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Try to delete with non-matching condition
        let result = execute_single(&executor, "DELETE FROM users WHERE age = 99;");
        assert_operation_successful(result, 0, StatementType::Delete);

        // Verify no records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_delete_with_comparison_operators() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (1, 'Product A', 100);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (2, 'Product B', 200);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (3, 'Product C', 150);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (4, 'Product D', 50);",
        );

        // Delete products with price > 100
        let result = execute_single(&executor, "DELETE FROM products WHERE price > 100;");
        assert_operation_successful(result, 2, StatementType::Delete);

        // Verify correct records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, price FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(4)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
    }

    #[test]
    fn test_delete_with_string_condition() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, department STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering');",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales');",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Engineering');",
        );

        // Delete users from Engineering department
        let result = execute_single(
            &executor,
            "DELETE FROM users WHERE department = 'Engineering';",
        );
        assert_operation_successful(result, 2, StatementType::Delete);

        // Verify correct records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, department FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(2));
        assert_eq!(*row.fields[1].deref(), Value::String("Bob".into()));
        assert_eq!(*row.fields[2].deref(), Value::String("Sales".into()));
    }

    #[test]
    fn test_delete_then_insert_same_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );

        // Delete the record
        let result = execute_single(&executor, "DELETE FROM users WHERE id = 1;");
        assert_operation_successful(result, 1, StatementType::Delete);

        // Insert new record with the same primary key
        let result = execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Bob', 30);",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify the new record is present
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);

        let row = &rows[0];
        assert_eq!(*row.fields[0].deref(), Value::Int32(1));
        assert_eq!(*row.fields[1].deref(), Value::String("Bob".into()));
        assert_eq!(*row.fields[2].deref(), Value::Int32(30));
    }

    #[test]
    fn test_delete_with_complex_where_clause() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32, in_stock BOOL);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (1, 'Product A', 100, TRUE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (2, 'Product B', 200, FALSE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (3, 'Product C', 150, TRUE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (4, 'Product D', 50, FALSE);",
        );

        // Delete products that are not in stock AND price > 100
        let result = execute_single(
            &executor,
            "DELETE FROM products WHERE in_stock = FALSE AND price > 100;",
        );
        assert_operation_successful(result, 1, StatementType::Delete);

        // Verify correct record was deleted (only Product B should be deleted)
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, price, in_stock FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(4)));
    }

    #[test]
    fn test_delete_all_then_select() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );
        execute_single(&executor, "INSERT INTO users (id, name) VALUES (2, 'Bob');");

        // Delete all records
        execute_single(&executor, "DELETE FROM users;");

        // Insert new records after deletion
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (3, 'Charlie');",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (4, 'David');",
        );

        // Verify only new records are present
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(4)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
    }

    #[test]
    fn test_update_all_records() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Update all records
        let result = execute_single(&executor, "UPDATE users SET age = 40;");
        assert_operation_successful(result, 3, StatementType::Update);

        // Verify all records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);

        // All users should have age = 40
        assert!(
            rows.iter()
                .all(|r| *r.fields[2].deref() == Value::Int32(40))
        );
    }

    #[test]
    fn test_update_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create empty table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

        // Try to update empty table
        let result = execute_single(&executor, "UPDATE users SET age = 40;");
        assert_operation_successful(result, 0, StatementType::Update);

        // Verify table is still empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_update_with_where_clause_single_match() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Update one record
        let result = execute_single(&executor, "UPDATE users SET age = 26 WHERE id = 1;");
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify correct record was updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[2].deref(), Value::Int32(26));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*charlie.fields[2].deref(), Value::Int32(35));
    }

    #[test]
    fn test_update_with_where_clause_multiple_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (4, 'David', 30);",
        );

        // Update multiple records matching condition
        let result = execute_single(&executor, "UPDATE users SET age = 26 WHERE age = 25;");
        assert_operation_successful(result, 2, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 4);

        // Users who had age 25 should now have age 26
        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[2].deref(), Value::Int32(26));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*charlie.fields[2].deref(), Value::Int32(26));

        // Users who had age 30 should still have age 30
        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));

        let david = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(4))
            .unwrap();
        assert_eq!(*david.fields[2].deref(), Value::Int32(30));
    }

    #[test]
    fn test_update_with_where_clause_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Try to update with non-matching condition
        let result = execute_single(&executor, "UPDATE users SET age = 99 WHERE age = 100;");
        assert_operation_successful(result, 0, StatementType::Update);

        // Verify no records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[2].deref(), Value::Int32(25));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));
    }

    #[test]
    fn test_update_with_comparison_operators() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (1, 'Product A', 100);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (2, 'Product B', 200);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (3, 'Product C', 150);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (4, 'Product D', 50);",
        );

        // Update products with price > 100
        let result = execute_single(
            &executor,
            "UPDATE products SET price = 99 WHERE price > 100;",
        );
        assert_operation_successful(result, 2, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, price FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 4);

        // Products with original price > 100 should now have price 99
        let product_b = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*product_b.fields[2].deref(), Value::Int32(99));

        let product_c = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*product_c.fields[2].deref(), Value::Int32(99));

        // Products with original price <= 100 should remain unchanged
        let product_a = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*product_a.fields[2].deref(), Value::Int32(100));

        let product_d = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(4))
            .unwrap();
        assert_eq!(*product_d.fields[2].deref(), Value::Int32(50));
    }

    #[test]
    fn test_update_string_column() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, department STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (1, 'Alice', 'Engineering');",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (2, 'Bob', 'Sales');",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, department) VALUES (3, 'Charlie', 'Engineering');",
        );

        // Update department for Engineering users
        let result = execute_single(
            &executor,
            "UPDATE users SET department = 'R&D' WHERE department = 'Engineering';",
        );
        assert_operation_successful(result, 2, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, department FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[2].deref(), Value::String("R&D".into()));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[2].deref(), Value::String("Sales".into()));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*charlie.fields[2].deref(), Value::String("R&D".into()));
    }

    #[test]
    fn test_update_multiple_columns() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Update multiple columns
        let result = execute_single(
            &executor,
            "UPDATE users SET name = 'Alicia', age = 26 WHERE id = 1;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify both columns were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[1].deref(), Value::String("Alicia".into()));
        assert_eq!(*alice.fields[2].deref(), Value::Int32(26));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[1].deref(), Value::String("Bob".into()));
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));
    }

    #[test]
    fn test_update_with_boolean_column() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, in_stock BOOL);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, in_stock) VALUES (1, 'Product A', TRUE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, in_stock) VALUES (2, 'Product B', FALSE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, in_stock) VALUES (3, 'Product C', TRUE);",
        );

        // Update in_stock for products that are currently in stock
        let result = execute_single(
            &executor,
            "UPDATE products SET in_stock = FALSE WHERE in_stock = TRUE;",
        );
        assert_operation_successful(result, 2, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, in_stock FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);

        // All products should now be out of stock
        assert!(
            rows.iter()
                .all(|r| *r.fields[2].deref() == Value::Bool(false))
        );
    }

    #[test]
    fn test_update_with_complex_where_clause() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32, in_stock BOOL);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (1, 'Product A', 100, TRUE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (2, 'Product B', 200, FALSE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (3, 'Product C', 150, TRUE);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price, in_stock) VALUES (4, 'Product D', 50, FALSE);",
        );

        // Update price for products that are in stock AND price > 100
        let result = execute_single(
            &executor,
            "UPDATE products SET price = 175 WHERE in_stock = TRUE AND price > 100;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify correct record was updated (only Product C should be updated)
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, price, in_stock FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 4);

        let product_a = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*product_a.fields[2].deref(), Value::Int32(100)); // Unchanged

        let product_b = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*product_b.fields[2].deref(), Value::Int32(200)); // Unchanged

        let product_c = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*product_c.fields[2].deref(), Value::Int32(175)); // Updated

        let product_d = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(4))
            .unwrap();
        assert_eq!(*product_d.fields[2].deref(), Value::Int32(50)); // Unchanged
    }

    #[test]
    fn test_update_with_float_column() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price FLOAT64);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (1, 'Product A', 99.99);",
        );
        execute_single(
            &executor,
            "INSERT INTO products (id, name, price) VALUES (2, 'Product B', 149.99);",
        );

        // Update price
        let result = execute_single(
            &executor,
            "UPDATE products SET price = 129.99 WHERE id = 2;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify record was updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, price FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let product_b = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        // Use approximate comparison for float values
        if let Value::Float64(price) = *product_b.fields[2].deref() {
            assert!((price - 129.99).abs() < 0.01);
        } else {
            panic!("Expected Float64 value");
        }
    }

    #[test]
    fn test_update_then_select_with_where() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Update records
        execute_single(&executor, "UPDATE users SET age = 40 WHERE age >= 30;");

        // Select only updated records
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users WHERE age = 40;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(3)));
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
    }

    #[test]
    fn test_update_after_delete() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Delete one record
        execute_single(&executor, "DELETE FROM users WHERE id = 2;");

        // Update remaining records
        let result = execute_single(&executor, "UPDATE users SET age = 50;");
        assert_operation_successful(result, 2, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(
            rows.iter()
                .all(|r| *r.fields[2].deref() == Value::Int32(50))
        );
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
    }

    #[test]
    fn test_drop_table_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Drop the table
        let result = execute_single(&executor, "DROP TABLE users;");
        assert_operation_successful(result, 0, StatementType::Drop);

        // Verify table no longer exists by trying to select from it
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");
    }

    #[test]
    fn test_drop_table_with_data() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Drop the table
        let result = execute_single(&executor, "DROP TABLE users;");
        assert_operation_successful(result, 0, StatementType::Drop);

        // Verify table no longer exists
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");
    }

    #[test]
    fn test_drop_then_recreate_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Drop the table
        execute_single(&executor, "DROP TABLE users;");

        // Recreate table with same name
        let result = execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, email STRING);",
        );
        assert_operation_successful(result, 0, StatementType::Create);

        // Verify new table exists and is empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, email FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "email");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_drop_multiple_tables() {
        let (executor, _temp_dir) = create_test_executor();

        // Create multiple tables
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, title STRING);",
        );

        // Drop first table
        let result = execute_single(&executor, "DROP TABLE users;");
        assert_operation_successful(result, 0, StatementType::Drop);

        // Verify first table is gone but second still exists
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");

        let (select_plan, select_ast) =
            create_single_statement("SELECT * FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);
        let (columns, _) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);

        // Drop second table
        let result = execute_single(&executor, "DROP TABLE products;");
        assert_operation_successful(result, 0, StatementType::Drop);
    }

    #[test]
    fn test_truncate_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create empty table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );

        // Truncate empty table
        let result = execute_single(&executor, "TRUNCATE TABLE users;");
        assert_operation_successful(result, 0, StatementType::Truncate);

        // Verify table still exists and is empty
        let (select_plan, select_ast) = create_single_statement("SELECT * FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_truncate_table_with_data() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Truncate table
        let result = execute_single(&executor, "TRUNCATE TABLE users;");
        assert_operation_successful(result, 0, StatementType::Truncate);

        // Verify table exists but is empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[2].name, "age");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_truncate_then_insert() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Truncate table
        execute_single(&executor, "TRUNCATE TABLE users;");

        // Insert new data with same primary keys as before
        let result = execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Charlie', 35);",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        let result = execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'David', 40);",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify only new data exists
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*charlie.fields[1].deref(), Value::String("Charlie".into()));
        assert_eq!(*charlie.fields[2].deref(), Value::Int32(35));

        let david = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*david.fields[1].deref(), Value::String("David".into()));
        assert_eq!(*david.fields[2].deref(), Value::Int32(40));
    }

    #[test]
    fn test_rename_table_preserves_existing_data() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35);",
        );

        // Rename table
        let result = execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Old table name should not work
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");

        // New table name should work and contain all data
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM people;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 3);

        // Verify all records are present
        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[1].deref(), Value::String("Alice".into()));
        assert_eq!(*alice.fields[2].deref(), Value::Int32(25));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[1].deref(), Value::String("Bob".into()));
        assert_eq!(*bob.fields[2].deref(), Value::Int32(30));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(3))
            .unwrap();
        assert_eq!(*charlie.fields[1].deref(), Value::String("Charlie".into()));
        assert_eq!(*charlie.fields[2].deref(), Value::Int32(35));
    }

    #[test]
    fn test_rename_table_can_insert_after_rename() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Rename table
        execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");

        // Insert into renamed table
        let result = execute_single(
            &executor,
            "INSERT INTO people (id, name) VALUES (2, 'Bob');",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify both records exist
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM people;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(1)));
        assert!(rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(2)));
    }

    #[test]
    fn test_rename_table_can_update_after_rename() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Rename table
        execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");

        // Update records in renamed table
        let result = execute_single(&executor, "UPDATE people SET age = 26 WHERE id = 1;");
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify update worked
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM people WHERE id = 1;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[2].deref(), Value::Int32(26));
    }

    #[test]
    fn test_rename_table_can_delete_after_rename() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );
        execute_single(&executor, "INSERT INTO users (id, name) VALUES (2, 'Bob');");

        // Rename table
        execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");

        // Delete from renamed table
        let result = execute_single(&executor, "DELETE FROM people WHERE id = 1;");
        assert_operation_successful(result, 1, StatementType::Delete);

        // Verify deletion worked
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM people;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[0].deref(), Value::Int32(2));
    }

    #[test]
    fn test_rename_table_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create empty table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Rename table
        let result = execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Old table name should not work
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");

        // New table name should work and be empty
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM people;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 2);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_rename_table_twice() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // First rename
        execute_single(&executor, "ALTER TABLE users RENAME TABLE TO people;");

        // Second rename
        let result = execute_single(&executor, "ALTER TABLE people RENAME TABLE TO persons;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Old names should not work
        let result = execute_single(&executor, "SELECT * FROM users;");
        assert_parse_error_contains(result, "table 'users' was not found in database");

        let result = execute_single(&executor, "SELECT * FROM people;");
        assert_parse_error_contains(result, "table 'people' was not found in database");

        // Final name should work
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM persons;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[0].deref(), Value::Int32(1));
    }

    #[test]
    fn test_rename_table_to_same_name() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Rename table to same name
        let result = execute_single(&executor, "ALTER TABLE users RENAME TABLE TO users;");

        assert_parse_error_contains(result, "table 'users' already exists");
    }

    #[test]
    fn test_rename_column_preserves_data() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);",
        );

        // Rename column
        let result = execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify data is preserved and accessible with new name
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, full_name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1].name, "full_name");
        assert_eq!(rows.len(), 2);

        let alice = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(1))
            .unwrap();
        assert_eq!(*alice.fields[1].deref(), Value::String("Alice".into()));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].deref() == Value::Int32(2))
            .unwrap();
        assert_eq!(*bob.fields[1].deref(), Value::String("Bob".into()));
    }

    #[test]
    fn test_rename_column_old_name_no_longer_works() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Rename column
        execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );

        // Try to select using old column name
        let result = execute_single(&executor, "SELECT id, name FROM users;");
        assert_parse_error_contains(result, "column 'name'");
    }

    #[test]
    fn test_rename_column_can_insert_after_rename() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );

        // Rename column
        execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );

        // Insert using new column name
        let result = execute_single(
            &executor,
            "INSERT INTO users (id, full_name, age) VALUES (2, 'Bob', 30);",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Verify both records exist
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, full_name, age FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_rename_column_can_update_after_rename() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25);",
        );

        // Rename column
        execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );

        // Update using new column name
        let result = execute_single(
            &executor,
            "UPDATE users SET full_name = 'Alicia' WHERE id = 1;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify update worked
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, full_name FROM users WHERE id = 1;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[1].deref(), Value::String("Alicia".into()));
    }

    #[test]
    fn test_rename_primary_key_column() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // Rename primary key column
        let result = execute_single(&executor, "ALTER TABLE users RENAME COLUMN id TO user_id;");
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify primary key column renamed
        let (select_plan, select_ast) =
            create_single_statement("SELECT user_id, name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns[0].name, "user_id");
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[0].deref(), Value::Int32(1));
    }

    #[test]
    fn test_rename_column_multiple_times() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO users (id, name) VALUES (1, 'Alice');",
        );

        // First rename
        execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );

        // Second rename
        let result = execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN full_name TO person_name;",
        );
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify final name works
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, person_name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns[1].name, "person_name");
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[1].deref(), Value::String("Alice".into()));

        // Old names should not work
        let result = execute_single(&executor, "SELECT name FROM users;");
        assert_parse_error_contains(result, "column 'name'");

        let result = execute_single(&executor, "SELECT full_name FROM users;");
        assert_parse_error_contains(result, "column 'full_name'");
    }

    #[test]
    fn test_rename_column_empty_table() {
        let (executor, _temp_dir) = create_test_executor();

        // Create empty table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Rename column
        let result = execute_single(
            &executor,
            "ALTER TABLE users RENAME COLUMN name TO full_name;",
        );
        assert_operation_successful(result, 0, StatementType::Alter);

        // Verify column renamed
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, full_name FROM users;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns[1].name, "full_name");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_rename_column_to_same_name() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
        );

        // Rename column to same name
        let result = execute_single(&executor, "ALTER TABLE users RENAME COLUMN name TO name;");

        assert_parse_error_contains(result, "column 'name' already exists");
    }

    #[test]
    fn test_select_index_scan_equal_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with equality on primary key should use index scan
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name, price FROM products WHERE id = 5;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[0].deref(), Value::Int32(5));
        assert!(matches!(&rows[0].fields[1].deref(), Value::String(s) if s == "Product 5"));
        assert_eq!(*rows[0].fields[2].deref(), Value::Int32(50));
    }

    #[test]
    fn test_select_index_scan_greater_than_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with > on primary key should use index scan
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM products WHERE id > 7;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                id > 7
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_select_index_scan_less_than_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with < on primary key should use index scan
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM products WHERE id < 4;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                id < 4
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_select_index_scan_range_inclusive() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with range on primary key should use index scan
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name FROM products WHERE id >= 3 AND id <= 7;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                (3..=7).contains(&id)
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_select_index_scan_range_exclusive() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with exclusive range on primary key should use index scan
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id FROM products WHERE id > 3 AND id < 7;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                id > 3 && id < 7
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_select_index_scan_no_results() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with primary key condition that doesn't match any records
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM products WHERE id = 999;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_select_index_scan_1000_records_with_wal() {
        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");
        let (executor, mut workers, mut wal_worker) =
            Executor::with_background_workers(&db_path, catalog)
                .expect("with_background_workers should succeed");

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );

        // Insert 1000 records to force B-tree splits
        for i in 0..1000 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // Give WAL time to flush all pending writes
        std::thread::sleep(std::time::Duration::from_millis(200));

        // SELECT with range scan across all records
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name, price FROM products WHERE id >= 0 AND id <= 999;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1000, "Should have exactly 1000 records");

        // Verify each record has correct values
        for (i, row) in rows.iter().enumerate() {
            let id = row.fields[0].as_i32().unwrap();
            let name = row.fields[1].as_string().unwrap();
            let price = row.fields[2].as_i32().unwrap();

            assert_eq!(
                id, i as i32,
                "Record {} should have id={}, but got id={}",
                i, i, id
            );
            assert_eq!(
                name,
                format!("Product {}", i),
                "Record {} should have name='Product {}', but got name='{}'",
                i,
                i,
                name
            );
            assert_eq!(
                price,
                (i as i32) * 10,
                "Record {} should have price={}, but got price={}",
                i,
                i * 10,
                price
            );
        }

        // Also test a subset range scan in the middle
        let (select_plan2, select_ast2) = create_single_statement(
            "SELECT id FROM products WHERE id >= 400 AND id < 600;",
            &executor,
        );

        let result2 = executor.execute_statement(&select_plan2, &select_ast2);
        let (_, rows2) = expect_select_successful(result2);
        assert_eq!(rows2.len(), 200, "Should have exactly 200 records in range");

        // Verify the range is correct
        for (i, row) in rows2.iter().enumerate() {
            let id = row.fields[0].as_i32().unwrap();
            let expected_id = 400 + i as i32;
            assert_eq!(
                id, expected_id,
                "Record at position {} should have id={}, but got id={}",
                i, expected_id, id
            );
        }

        for mut handle in workers.drain(..) {
            handle.shutdown().expect("shutdown should succeed");
            handle.join().expect("join should succeed");
        }
        wal_worker.shutdown().expect("shutdown should succeed");
        wal_worker.join().expect("join should succeed");
    }

    #[test]
    fn test_delete_index_scan_equal_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // DELETE with equality on primary key should use index scan
        let (delete_plan, delete_ast) =
            create_single_statement("DELETE FROM products WHERE id = 5;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&delete_plan);

        let result = executor.execute_statement(&delete_plan, &delete_ast);
        assert_operation_successful(result, 1, StatementType::Delete);

        // Verify correct record was deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 9);
        assert!(!rows.iter().any(|r| *r.fields[0].deref() == Value::Int32(5)));
    }

    #[test]
    fn test_delete_index_scan_greater_than_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // DELETE with > on primary key should use index scan
        let (delete_plan, delete_ast) =
            create_single_statement("DELETE FROM products WHERE id > 7;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&delete_plan);

        let result = executor.execute_statement(&delete_plan, &delete_ast);
        assert_operation_successful(result, 3, StatementType::Delete);

        // Verify correct records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 7);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                id <= 7
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_delete_index_scan_range() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // DELETE with range on primary key should use index scan
        let (delete_plan, delete_ast) =
            create_single_statement("DELETE FROM products WHERE id >= 3 AND id <= 7;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&delete_plan);

        let result = executor.execute_statement(&delete_plan, &delete_ast);
        assert_operation_successful(result, 5, StatementType::Delete);

        // Verify correct records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|r| {
            if let Value::Int32(id) = *r.fields[0].deref() {
                !(3..=7).contains(&id)
            } else {
                false
            }
        }));
    }

    #[test]
    fn test_delete_index_scan_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // DELETE with primary key condition that doesn't match any records
        let (delete_plan, delete_ast) =
            create_single_statement("DELETE FROM products WHERE id = 999;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&delete_plan);

        let result = executor.execute_statement(&delete_plan, &delete_ast);
        assert_operation_successful(result, 0, StatementType::Delete);

        // Verify no records were deleted
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM products;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_update_index_scan_equal_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // UPDATE with equality on primary key should use index scan
        let (update_plan, update_ast) =
            create_single_statement("UPDATE products SET price = 999 WHERE id = 5;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&update_plan);

        let result = executor.execute_statement(&update_plan, &update_ast);
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify correct record was updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, price FROM products WHERE id = 5;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
        assert_eq!(*rows[0].fields[1].deref(), Value::Int32(999));

        // Verify other records unchanged
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM products WHERE price = 999;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_update_index_scan_greater_than_primary_key() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // UPDATE with > on primary key should use index scan
        let (update_plan, update_ast) =
            create_single_statement("UPDATE products SET price = 999 WHERE id > 7;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&update_plan);

        let result = executor.execute_statement(&update_plan, &update_ast);
        assert_operation_successful(result, 3, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, price FROM products WHERE id > 7;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);
        assert!(
            rows.iter()
                .all(|r| *r.fields[1].deref() == Value::Int32(999))
        );
    }

    #[test]
    fn test_update_index_scan_range() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // UPDATE with range on primary key should use index scan
        let (update_plan, update_ast) = create_single_statement(
            "UPDATE products SET price = 888 WHERE id >= 3 AND id <= 7;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&update_plan);

        let result = executor.execute_statement(&update_plan, &update_ast);
        assert_operation_successful(result, 5, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, price FROM products WHERE id >= 3 AND id <= 7;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 5);
        assert!(
            rows.iter()
                .all(|r| *r.fields[1].deref() == Value::Int32(888))
        );

        // Verify other records unchanged
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, price FROM products WHERE id < 3 OR id > 7;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 5);
        assert!(
            rows.iter()
                .all(|r| *r.fields[1].deref() != Value::Int32(888))
        );
    }

    #[test]
    fn test_update_index_scan_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // UPDATE with primary key condition that doesn't match any records
        let (update_plan, update_ast) =
            create_single_statement("UPDATE products SET price = 999 WHERE id = 999;", &executor);

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&update_plan);

        let result = executor.execute_statement(&update_plan, &update_ast);
        assert_operation_successful(result, 0, StatementType::Update);

        // Verify no records were updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT price FROM products WHERE price = 999;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_update_index_scan_multiple_columns() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=10 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // UPDATE multiple columns with primary key condition
        let (update_plan, update_ast) = create_single_statement(
            "UPDATE products SET name = 'Updated', price = 999 WHERE id >= 5 AND id <= 7;",
            &executor,
        );

        // Verify IndexScan is used in the plan
        assert_uses_index_scan(&update_plan);

        let result = executor.execute_statement(&update_plan, &update_ast);
        assert_operation_successful(result, 3, StatementType::Update);

        // Verify correct records were updated
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name, price FROM products WHERE id >= 5 AND id <= 7;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|r| {
            matches!(&r.fields[1].deref(), Value::String(s) if s == "Updated")
                && *r.fields[2].deref() == Value::Int32(999)
        }));
    }

    // Test to verify TableScan is used when condition is NOT on primary key
    #[test]
    fn test_select_uses_table_scan_for_non_primary_key_condition() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
        );
        for i in 1..=5 {
            execute_single(
                &executor,
                &format!(
                    "INSERT INTO products (id, name, price) VALUES ({}, 'Product {}', {});",
                    i,
                    i,
                    i * 10
                ),
            );
        }

        // SELECT with condition on non-primary key should use TableScan
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM products WHERE price > 20;", &executor);

        // Verify TableScan is used (not IndexScan)
        assert_uses_table_scan(&select_plan);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3); // Products 3, 4, 5 have price > 20
    }

    #[test]
    fn test_insert_and_select_date_from_string() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table with Date column
        execute_single(
            &executor,
            "CREATE TABLE events (id INT32 PRIMARY_KEY, event_date DATE, description STRING);",
        );

        // Insert with date string
        let result = execute_single(
            &executor,
            "INSERT INTO events (id, event_date, description) VALUES (1, '2024-01-15', 'New Year Event');",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Select and verify the date was stored correctly
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, event_date, description FROM events;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1].ty, Type::Date);
        assert_eq!(rows.len(), 1);

        // Verify date value
        match rows[0].fields[1].deref() {
            Value::Date(db_date) => {
                assert_eq!(db_date.year(), 2024);
                assert_eq!(db_date.month(), 1);
                assert_eq!(db_date.day(), 15);
            }
            other => panic!("Expected Date value, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_and_select_datetime_from_string() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table with DateTime column
        execute_single(
            &executor,
            "CREATE TABLE logs (id INT32 PRIMARY_KEY, timestamp DATETIME, message STRING);",
        );

        // Insert with datetime string
        let result = execute_single(
            &executor,
            "INSERT INTO logs (id, timestamp, message) VALUES (1, '2024-06-15T14:30:45', 'System started');",
        );
        assert_operation_successful(result, 1, StatementType::Insert);

        // Select and verify the datetime was stored correctly
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, timestamp, message FROM logs;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[1].ty, Type::DateTime);
        assert_eq!(rows.len(), 1);

        // Verify datetime value
        match rows[0].fields[1].deref() {
            Value::DateTime(db_datetime) => {
                assert_eq!(db_datetime.year(), 2024);
                assert_eq!(db_datetime.month(), 6);
                assert_eq!(db_datetime.day(), 15);
                assert_eq!(db_datetime.hour(), 14);
                assert_eq!(db_datetime.minute(), 30);
                assert_eq!(db_datetime.second(), 45);
            }
            other => panic!("Expected DateTime value, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_multiple_dates_and_filter() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE bookings (id INT32 PRIMARY_KEY, booking_date DATE, customer STRING);",
        );

        // Insert multiple records with different dates
        execute_single(
            &executor,
            "INSERT INTO bookings (id, booking_date, customer) VALUES (1, '2024-01-10', 'Alice');",
        );
        execute_single(
            &executor,
            "INSERT INTO bookings (id, booking_date, customer) VALUES (2, '2024-02-15', 'Bob');",
        );
        execute_single(
            &executor,
            "INSERT INTO bookings (id, booking_date, customer) VALUES (3, '2024-03-20', 'Charlie');",
        );
        execute_single(
            &executor,
            "INSERT INTO bookings (id, booking_date, customer) VALUES (4, '2024-04-25', 'David');",
        );

        // Filter by date: bookings after 2024-02-01
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, customer FROM bookings WHERE booking_date > '2024-02-01';",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 3); // Bob, Charlie, David

        // Verify the correct customers
        let customers: Vec<String> = rows
            .iter()
            .map(|r| match r.fields[1].deref() {
                Value::String(s) => s.clone(),
                _ => panic!("Expected string"),
            })
            .collect();
        assert!(customers.contains(&"Bob".to_string()));
        assert!(customers.contains(&"Charlie".to_string()));
        assert!(customers.contains(&"David".to_string()));
    }

    #[test]
    fn test_filter_with_date_equality() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE appointments (id INT32 PRIMARY_KEY, appointment_date DATE, patient STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO appointments (id, appointment_date, patient) VALUES (1, '2024-05-10', 'John');",
        );
        execute_single(
            &executor,
            "INSERT INTO appointments (id, appointment_date, patient) VALUES (2, '2024-05-11', 'Jane');",
        );
        execute_single(
            &executor,
            "INSERT INTO appointments (id, appointment_date, patient) VALUES (3, '2024-05-10', 'Jim');",
        );

        // Filter by exact date
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, patient FROM appointments WHERE appointment_date = '2024-05-10';",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2); // John and Jim
    }

    #[test]
    fn test_filter_with_datetime_comparison() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE transactions (id INT32 PRIMARY_KEY, transaction_time DATETIME, amount INT32);",
        );

        // Insert records
        execute_single(
            &executor,
            "INSERT INTO transactions (id, transaction_time, amount) VALUES (1, '2024-01-15T09:00:00', 100);",
        );
        execute_single(
            &executor,
            "INSERT INTO transactions (id, transaction_time, amount) VALUES (2, '2024-01-15T14:30:00', 200);",
        );
        execute_single(
            &executor,
            "INSERT INTO transactions (id, transaction_time, amount) VALUES (3, '2024-01-15T18:45:00', 150);",
        );

        // Filter by datetime: transactions after 2:00 PM
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, amount FROM transactions WHERE transaction_time > '2024-01-15T14:00:00';",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2); // transactions at 14:30 and 18:45
    }

    #[test]
    fn test_update_date_column_with_string() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table with 2 records
        execute_single(
            &executor,
            "CREATE TABLE schedules (id INT32 PRIMARY_KEY, scheduled_date DATE, task STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO schedules (id, scheduled_date, task) VALUES (1, '2024-01-01', 'Task 1');",
        );
        execute_single(
            &executor,
            "INSERT INTO schedules (id, scheduled_date, task) VALUES (2, '2024-01-02', 'Task 2');",
        );

        // Update only the first record's date using string
        let result = execute_single(
            &executor,
            "UPDATE schedules SET scheduled_date = '2024-12-31' WHERE id = 1;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify the first record was updated
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, scheduled_date FROM schedules WHERE id = 1;",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);

        match rows[0].fields[1].deref() {
            Value::Date(db_date) => {
                assert_eq!(db_date.year(), 2024);
                assert_eq!(db_date.month(), 12);
                assert_eq!(db_date.day(), 31);
            }
            other => panic!("Expected Date value, got {:?}", other),
        }

        // Verify the second record was NOT changed
        let (select_plan2, select_ast2) = create_single_statement(
            "SELECT id, scheduled_date FROM schedules WHERE id = 2;",
            &executor,
        );
        let result2 = executor.execute_statement(&select_plan2, &select_ast2);

        let (_, rows2) = expect_select_successful(result2);
        assert_eq!(rows2.len(), 1);

        match rows2[0].fields[1].deref() {
            Value::Date(db_date) => {
                assert_eq!(db_date.year(), 2024);
                assert_eq!(db_date.month(), 1);
                assert_eq!(db_date.day(), 2);
            }
            other => panic!("Expected Date value, got {:?}", other),
        }
    }

    #[test]
    fn test_update_datetime_column_with_string() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table with 2 records
        execute_single(
            &executor,
            "CREATE TABLE events (id INT32 PRIMARY_KEY, event_time DATETIME, name STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO events (id, event_time, name) VALUES (1, '2024-01-01T10:00:00', 'Event 1');",
        );
        execute_single(
            &executor,
            "INSERT INTO events (id, event_time, name) VALUES (2, '2024-02-15T14:30:00', 'Event 2');",
        );

        // Update only the first record's datetime using string
        let result = execute_single(
            &executor,
            "UPDATE events SET event_time = '2024-06-15T16:30:45' WHERE id = 1;",
        );
        assert_operation_successful(result, 1, StatementType::Update);

        // Verify the first record was updated
        let (select_plan, select_ast) =
            create_single_statement("SELECT id, event_time FROM events WHERE id = 1;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 1);

        match rows[0].fields[1].deref() {
            Value::DateTime(db_datetime) => {
                assert_eq!(db_datetime.year(), 2024);
                assert_eq!(db_datetime.month(), 6);
                assert_eq!(db_datetime.day(), 15);
                assert_eq!(db_datetime.hour(), 16);
                assert_eq!(db_datetime.minute(), 30);
                assert_eq!(db_datetime.second(), 45);
            }
            other => panic!("Expected DateTime value, got {:?}", other),
        }

        // Verify the second record was NOT changed
        let (select_plan2, select_ast2) =
            create_single_statement("SELECT id, event_time FROM events WHERE id = 2;", &executor);
        let result2 = executor.execute_statement(&select_plan2, &select_ast2);

        let (_, rows2) = expect_select_successful(result2);
        assert_eq!(rows2.len(), 1);

        match rows2[0].fields[1].deref() {
            Value::DateTime(db_datetime) => {
                assert_eq!(db_datetime.year(), 2024);
                assert_eq!(db_datetime.month(), 2);
                assert_eq!(db_datetime.day(), 15);
                assert_eq!(db_datetime.hour(), 14);
                assert_eq!(db_datetime.minute(), 30);
                assert_eq!(db_datetime.second(), 0);
            }
            other => panic!("Expected DateTime value, got {:?}", other),
        }
    }

    #[test]
    fn test_delete_with_date_filter() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE orders (id INT32 PRIMARY_KEY, order_date DATE, status STRING);",
        );
        execute_single(
            &executor,
            "INSERT INTO orders (id, order_date, status) VALUES (1, '2024-01-10', 'pending');",
        );
        execute_single(
            &executor,
            "INSERT INTO orders (id, order_date, status) VALUES (2, '2024-02-15', 'completed');",
        );
        execute_single(
            &executor,
            "INSERT INTO orders (id, order_date, status) VALUES (3, '2024-03-20', 'pending');",
        );

        // Delete orders before 2024-02-01
        let result = execute_single(
            &executor,
            "DELETE FROM orders WHERE order_date < '2024-02-01';",
        );
        assert_operation_successful(result, 1, StatementType::Delete);

        // Verify only 2 records remain
        let (select_plan, select_ast) =
            create_single_statement("SELECT id FROM orders;", &executor);
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_date_range_filter() {
        let (executor, _temp_dir) = create_test_executor();

        // Create and populate table
        execute_single(
            &executor,
            "CREATE TABLE sales (id INT32 PRIMARY_KEY, sale_date DATE, amount INT32);",
        );
        execute_single(
            &executor,
            "INSERT INTO sales (id, sale_date, amount) VALUES (1, '2024-01-05', 100);",
        );
        execute_single(
            &executor,
            "INSERT INTO sales (id, sale_date, amount) VALUES (2, '2024-01-15', 200);",
        );
        execute_single(
            &executor,
            "INSERT INTO sales (id, sale_date, amount) VALUES (3, '2024-01-25', 150);",
        );
        execute_single(
            &executor,
            "INSERT INTO sales (id, sale_date, amount) VALUES (4, '2024-02-05', 300);",
        );

        // Filter by date range: between Jan 10 and Jan 30
        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, amount FROM sales WHERE sale_date >= '2024-01-10' AND sale_date <= '2024-01-30';",
            &executor,
        );
        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);
        assert_eq!(rows.len(), 2); // Jan 15 and Jan 25
    }

    #[test]
    fn test_insert_invalid_date_string_error() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE events (id INT32 PRIMARY_KEY, event_date DATE);",
        );

        // Try to insert invalid date
        let result = execute_single(
            &executor,
            "INSERT INTO events (id, event_date) VALUES (1, 'not-a-date');",
        );

        // Should get a parse error
        assert_parse_error_contains(result, "Date");
    }

    #[test]
    fn test_insert_invalid_datetime_string_error() {
        let (executor, _temp_dir) = create_test_executor();

        // Create table
        execute_single(
            &executor,
            "CREATE TABLE logs (id INT32 PRIMARY_KEY, log_time DATETIME);",
        );

        // Try to insert invalid datetime
        let result = execute_single(
            &executor,
            "INSERT INTO logs (id, log_time) VALUES (1, 'invalid-datetime');",
        );

        // Should get a parse error
        assert_parse_error_contains(result, "DateTime");
    }

    #[test]
    fn test_wal_redo_with_deleted_page() {
        // Test scenario: Insert many records to span multiple B-tree pages, then delete all records.
        // On restart, WAL redo should gracefully handle pages that were deallocated.

        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        {
            // Create table, insert many records, then delete all of them
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("with_background_workers should succeed");

            // Create table with data
            execute_single(
                &executor,
                "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);",
            );

            // Insert enough records to ensure multiple B-tree pages
            // PAGE_SIZE = 4096, INT32 = 4 bytes, plus overhead for B-tree structure
            // We insert PAGE_SIZE / 4 + 10 records to be safe
            const NUM_RECORDS: i32 = 4096 / 4 + 10;
            for i in 1..=NUM_RECORDS {
                execute_single(
                    &executor,
                    &format!("INSERT INTO users (id, name) VALUES ({}, 'User{}');", i, i),
                );
            }

            // Force WAL flush to ensure records are written
            thread::sleep(time::Duration::from_millis(150));

            // Delete all records
            execute_single(&executor, "DELETE FROM users;");

            // Force another WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Shutdown cleanly
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }

        {
            // Restart and attempt redo
            // This should NOT panic - it should gracefully handle missing pages
            let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap();
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("Recovery with redo should handle deleted pages gracefully");

            let result = execute_single(&executor, "SELECT * FROM users;");
            // We call it just to trigger wal redo
            let _ = expect_select_successful(result);

            // Cleanup
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }
    }

    #[test]
    fn test_wal_redo_with_dropped_and_recreated_table() {
        // Test scenario: Create table, insert data, drop table.
        // On restart (phase 2), recreate table with same name.
        // WAL redo should not try to apply old records to deleted pages.

        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        {
            // Create table, insert data, then drop it
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("with_background_workers should succeed");

            // Create and populate table
            execute_single(
                &executor,
                "CREATE TABLE products (id INT32 PRIMARY_KEY, price INT32);",
            );
            execute_single(
                &executor,
                "INSERT INTO products (id, price) VALUES (1, 100);",
            );
            execute_single(
                &executor,
                "INSERT INTO products (id, price) VALUES (2, 200);",
            );
            execute_single(
                &executor,
                "INSERT INTO products (id, price) VALUES (3, 300);",
            );

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Drop the table - this deallocates all its pages
            execute_single(&executor, "DROP TABLE products;");

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Shutdown cleanly
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }

        {
            // Restart, attempt redo, then create new table with same name
            // This should NOT panic when trying to redo records for deleted pages
            let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap();
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("Recovery should handle dropped tables gracefully");

            // Create new table with same name but different schema
            execute_single(
                &executor,
                "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING);",
            );
            execute_single(
                &executor,
                "INSERT INTO products (id, name) VALUES (1, 'Widget');",
            );

            // Verify the new table has correct data
            let result = execute_single(&executor, "SELECT * FROM products;");
            let (_, rows) = expect_select_successful(result);

            // Should only have the new table's data
            assert_eq!(rows.len(), 1);

            // Cleanup
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }
            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }
    }

    #[test]
    fn test_wal_redo_with_truncated_table() {
        // Test scenario: Create table, insert data, truncate it.
        // On restart insert new records.
        // WAL redo should handle pages that were deallocated during truncate.

        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        {
            // Create table, insert data, then truncate
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("with_background_workers should succeed");

            // Create and populate table
            execute_single(
                &executor,
                "CREATE TABLE logs (id INT32 PRIMARY_KEY, message STRING);",
            );

            // Insert multiple records to ensure multiple pages
            for i in 1..=100 {
                execute_single(
                    &executor,
                    &format!(
                        "INSERT INTO logs (id, message) VALUES ({}, 'Log entry {}');",
                        i, i
                    ),
                );
            }

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Truncate the table - this deallocates all its pages
            execute_single(&executor, "TRUNCATE TABLE logs;");

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Shutdown cleanly
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }

        {
            // Restart, attempt redo, then insert new data
            // This should NOT panic when trying to redo records for deallocated pages
            let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap();
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("Recovery should handle truncated tables gracefully");

            // Insert new data into the truncated table
            execute_single(
                &executor,
                "INSERT INTO logs (id, message) VALUES (1, 'New log entry');",
            );
            execute_single(
                &executor,
                "INSERT INTO logs (id, message) VALUES (2, 'Another new entry');",
            );

            // Verify table has only new data
            let result = execute_single(&executor, "SELECT * FROM logs;");
            let (_, rows) = expect_select_successful(result);
            assert_eq!(rows.len(), 2);

            // Cleanup
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }
    }

    #[test]
    fn test_wal_redo_with_multiple_table_operations() {
        // Test scenario: Complex sequence of table operations to stress test WAL redo
        // with multiple deletions, drops, and recreations.

        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        {
            // Complex sequence of operations
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("with_background_workers should succeed");

            // Create first table
            execute_single(
                &executor,
                "CREATE TABLE temp1 (id INT32 PRIMARY_KEY, value INT32);",
            );
            execute_single(&executor, "INSERT INTO temp1 (id, value) VALUES (1, 10);");
            execute_single(&executor, "INSERT INTO temp1 (id, value) VALUES (2, 20);");

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Create second table
            execute_single(
                &executor,
                "CREATE TABLE temp2 (id INT32 PRIMARY_KEY, data STRING);",
            );
            execute_single(&executor, "INSERT INTO temp2 (id, data) VALUES (1, 'foo');");

            // Drop first table
            execute_single(&executor, "DROP TABLE temp1;");

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Truncate second table
            execute_single(&executor, "TRUNCATE TABLE temp2;");

            // Recreate first table with different schema
            execute_single(
                &executor,
                "CREATE TABLE temp1 (id INT32 PRIMARY_KEY, name STRING);",
            );
            execute_single(&executor, "INSERT INTO temp1 (id, name) VALUES (1, 'new');");

            // Force final WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Shutdown cleanly
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }

        {
            // Restart and attempt redo
            // This should NOT panic despite complex history of deletions
            let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap();
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("Recovery should handle complex table operations gracefully");

            // Verify final state
            let result = execute_single(&executor, "SELECT * FROM temp1;");
            let (_, rows) = expect_select_successful(result);
            assert_eq!(rows.len(), 1);

            let result = execute_single(&executor, "SELECT * FROM temp2;");
            let (_, rows) = expect_select_successful(result);
            assert_eq!(rows.len(), 0);

            // Cleanup
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }
    }

    #[test]
    fn test_wal_redo_with_page_reallocation() {
        // Test scenario: Insert records, truncate table (deallocates pages),
        // then insert new record that reuses a previously allocated page ID.
        // On restart, WAL redo should only apply the new record, not old ones.

        let (catalog, temp_dir) = create_catalog();
        let db_path = temp_dir.path().join("test_db");

        {
            // Create table, insert records, truncate, then insert one new record
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("with_background_workers should succeed");

            // Create table
            execute_single(
                &executor,
                "CREATE TABLE items (id INT32 PRIMARY_KEY, name STRING);",
            );

            // Insert initial records
            execute_single(
                &executor,
                "INSERT INTO items (id, name) VALUES (1, 'Item One');",
            );
            execute_single(
                &executor,
                "INSERT INTO items (id, name) VALUES (2, 'Item Two');",
            );
            execute_single(
                &executor,
                "INSERT INTO items (id, name) VALUES (3, 'Item Three');",
            );
            execute_single(
                &executor,
                "INSERT INTO items (id, name) VALUES (4, 'Item Four');",
            );

            // Force WAL flush to ensure records are logged
            thread::sleep(time::Duration::from_millis(150));

            // Truncate table - deallocates all pages
            execute_single(&executor, "TRUNCATE TABLE items;");

            // Force WAL flush
            thread::sleep(time::Duration::from_millis(150));

            // Insert ONE new record - this might reuse a previously allocated page ID
            execute_single(
                &executor,
                "INSERT INTO items (id, name) VALUES (1, 'Item One');",
            );

            // Force WAL flush to log the new insert
            thread::sleep(time::Duration::from_millis(150));

            // Shutdown cleanly
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }

        {
            // Restart and verify WAL redo only applies the new record
            // NOT the old 4 records that existed before truncate
            let catalog = Catalog::new(temp_dir.path(), "test_db").unwrap();
            let (executor, mut workers, mut wal_worker) =
                Executor::with_background_workers(&db_path, catalog)
                    .expect("Recovery should handle page reallocation correctly");

            // Verify table has exactly 1 record (the one inserted after truncate)
            let result = execute_single(&executor, "SELECT * FROM items;");
            let (_, rows) = expect_select_successful(result);
            assert_eq!(
                rows.len(),
                1,
                "Expected 1 record after recovery, not the old 4 records"
            );

            // Verify it's the correct record
            let record = &rows[0];
            let id_value = &record.fields[0];
            let name_value = &record.fields[1];

            assert_eq!(id_value.as_i32().unwrap(), 1);
            assert_eq!(name_value.as_string().unwrap(), "Item One");

            // Cleanup
            for mut handle in workers.drain(..) {
                handle.shutdown().expect("shutdown should succeed");
                handle.join().expect("join should succeed");
            }

            wal_worker.shutdown().expect("shutdown should succeed");
            wal_worker.join().expect("join should succeed");
        }
    }
}
