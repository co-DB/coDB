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
use storage::{
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
}

impl Executor {
    pub fn new(database_path: impl AsRef<Path>, catalog: Catalog) -> Result<Self, ExecutorError> {
        let files = Arc::new(FilesManager::new(database_path)?);
        let cache = Cache::new(consts::CACHE_SIZE, files);
        let catalog = Arc::new(RwLock::new(catalog));
        Ok(Executor {
            heap_files: DashMap::new(),
            b_trees: DashMap::new(),
            cache,
            catalog,
        })
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
    use std::fs;
    use std::ops::Deref;

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
                assert!(error.contains("tried to insert a duplicate key"));
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

        // Rename column to same name
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
}
