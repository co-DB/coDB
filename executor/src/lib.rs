mod consts;
mod error_factory;
mod expression_executor;
mod iterators;
pub mod response;
mod statement_executor;

use std::{path::Path, sync::Arc};

use dashmap::DashMap;
use engine::heap_file::{HeapFile, HeapFileFactory};
use metadata::catalog::Catalog;
use parking_lot::RwLock;
use planner::{query_plan::StatementPlan, resolved_tree::ResolvedTree};
use storage::{
    cache::Cache,
    files_manager::{FileKey, FilesManager, FilesManagerError},
};
use thiserror::Error;

use crate::{
    consts::HEAP_FILE_BUCKET_SIZE,
    error_factory::InternalExecutorError,
    iterators::{ParseErrorIter, QueryResultIter, StatementIter},
    response::StatementResult,
    statement_executor::StatementExecutor,
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

#[cfg(test)]
mod tests {
    use std::fs;

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
            .find(|r| *r.fields[0].value() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(&alice.fields[1].value(), Value::String(s) if s == "Alice"));
        assert!(matches!(alice.fields[2].value(), Value::Int32(25)));

        let bob = rows
            .iter()
            .find(|&r| *r.fields[0].value() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(&bob.fields[1].value(), Value::String(s) if s == "Bob"));
        assert!(matches!(bob.fields[2].value(), Value::Int32(30)));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(&charlie.fields[1].value(), Value::String(s) if s == "Charlie"));
        assert!(matches!(charlie.fields[2].value(), Value::Int32(35)));
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
            .find(|r| *r.fields[1].value() == Value::Int32(25))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert!(matches!(&alice.fields[0].value(), Value::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| *r.fields[1].value() == Value::Int32(30))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert!(matches!(&bob.fields[0].value(), Value::String(s) if s == "Bob"));
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
            .find(|r| *r.fields[0].value() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 3);
        assert!(matches!(alice.fields[1].value(), Value::Int32(25)));
        assert!(matches!(&alice.fields[2].value(), Value::String(s) if s == "Alice"));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 3);
        assert!(matches!(bob.fields[1].value(), Value::Int32(30)));
        assert!(matches!(&bob.fields[2].value(), Value::String(s) if s == "Bob"));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(3))
            .unwrap();
        assert_eq!(charlie.fields.len(), 3);
        assert!(matches!(charlie.fields[1].value(), Value::Int32(35)));
        assert!(matches!(&charlie.fields[2].value(), Value::String(s) if s == "Charlie"));
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
            .find(|r| *r.fields[0].value() == Value::Int32(1))
            .unwrap();
        assert_eq!(alice.fields.len(), 2);
        assert_eq!(*alice.fields[0].value(), Value::Int32(1));
        assert_eq!(*alice.fields[1].value(), Value::Int32(1));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(2))
            .unwrap();
        assert_eq!(bob.fields.len(), 2);
        assert_eq!(*bob.fields[0].value(), Value::Int32(2));
        assert_eq!(*bob.fields[1].value(), Value::Int32(2));
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
                .all(|r| *r.fields[2].value() == Value::Int32(25))
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1].value(), Value::String(s) if s == "Alice"))
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1].value(), Value::String(s) if s == "Charlie"))
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
        assert!(rows.iter().any(|r| *r.fields[0].value() == Value::Int32(2)));
        assert!(rows.iter().any(|r| *r.fields[0].value() == Value::Int32(3)));
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
        assert_eq!(*rows[0].fields[1].value(), Value::Int32(10));
        assert_eq!(*rows[1].fields[1].value(), Value::Int32(20));
        assert_eq!(*rows[2].fields[1].value(), Value::Int32(30));
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
        assert_eq!(*rows[0].fields[1].value(), Value::Int32(30));
        assert_eq!(*rows[1].fields[1].value(), Value::Int32(20));
        assert_eq!(*rows[2].fields[1].value(), Value::Int32(10));
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
        assert_eq!(*rows[0].fields[1].value(), Value::String("apple".into()));
        assert_eq!(*rows[1].fields[1].value(), Value::String("banana".into()));
        assert_eq!(*rows[2].fields[1].value(), Value::String("zebra".into()));
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
        assert_eq!(*rows[0].fields[1].value(), Value::Float64(1.5));
        assert_eq!(*rows[1].fields[1].value(), Value::Float64(2.25));
        assert_eq!(*rows[2].fields[1].value(), Value::Float64(3.75));
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
        assert_eq!(*rows[0].fields[1].value(), Value::Bool(false));
        assert_eq!(*rows[1].fields[1].value(), Value::Bool(true));
        assert_eq!(*rows[2].fields[1].value(), Value::Bool(true));
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
        assert_eq!(*rows[0].fields[1].value(), Value::Int32(10));
        assert_eq!(*rows[1].fields[1].value(), Value::Int32(10));
        assert_eq!(*rows[2].fields[1].value(), Value::Int32(20));
        assert_eq!(*rows[3].fields[1].value(), Value::Int32(20));
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
        assert_eq!(*rows[0].fields[0].value(), Value::Int32(4));
        assert_eq!(*rows[0].fields[1].value(), Value::String("Monitor".into()));
        assert_eq!(*rows[0].fields[2].value(), Value::Int32(300));

        // Second row should be Headphones (100)
        assert_eq!(*rows[1].fields[0].value(), Value::Int32(7));
        assert_eq!(
            *rows[1].fields[1].value(),
            Value::String("Headphones".into())
        );
        assert_eq!(*rows[1].fields[2].value(), Value::Int32(100));

        assert!(rows.iter().all(|r| {
            if let Value::Int32(price) = *r.fields[2].value() {
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
        assert_eq!(*row.fields[0].value(), Value::Int32(1));
        assert!(matches!(&row.fields[1].value(), Value::String(s) if s == "Alice"));
        assert_eq!(*row.fields[2].value(), Value::Int32(25));
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
            .find(|r| *r.fields[0].value() == Value::Int32(1))
            .unwrap();
        assert!(matches!(&alice.fields[1].value(), Value::String(s) if s == "Alice"));
        assert_eq!(*alice.fields[2].value(), Value::Int32(25));

        let bob = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(2))
            .unwrap();
        assert!(matches!(&bob.fields[1].value(), Value::String(s) if s == "Bob"));
        assert_eq!(*bob.fields[2].value(), Value::Int32(30));

        let charlie = rows
            .iter()
            .find(|r| *r.fields[0].value() == Value::Int32(3))
            .unwrap();
        assert!(matches!(&charlie.fields[1].value(), Value::String(s) if s == "Charlie"));
        assert_eq!(*charlie.fields[2].value(), Value::Int32(35));
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
        assert_eq!(*row.fields[0].value(), Value::Int32(1));
        assert!(matches!(&row.fields[1].value(), Value::String(s) if s == "Alice"));
        assert_eq!(*row.fields[2].value(), Value::Int32(25));
    }
}
