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

    use engine::record::{Field, Record};
    use metadata::types::Type;
    use tempfile::TempDir;

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

    #[test]
    fn test_execute_select_with_where_clause_single_condition() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        // Insert test data
        let records = vec![
            Record::new(vec![
                Field::Int32(1),
                Field::Int32(25),
                Field::String("Alice".into()),
            ]),
            Record::new(vec![
                Field::Int32(2),
                Field::Int32(30),
                Field::String("Bob".into()),
            ]),
            Record::new(vec![
                Field::Int32(3),
                Field::Int32(25),
                Field::String("Charlie".into()),
            ]),
        ];

        executor
            .with_heap_file("users", |hf| {
                for record in records {
                    hf.insert(record).unwrap();
                }
            })
            .unwrap();

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name, age FROM users WHERE age = 25;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (columns, rows) = expect_select_successful(result);

        assert_eq!(columns.len(), 3);
        assert_eq!(rows.len(), 2);

        assert!(rows.iter().all(|r| r.fields[2] == Field::Int32(25)));
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1], Field::String(s) if s == "Alice"))
        );
        assert!(
            rows.iter()
                .any(|r| matches!(&r.fields[1], Field::String(s) if s == "Charlie"))
        );
    }

    #[test]
    fn test_execute_select_with_where_clause_comparison_operators() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING, price INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        let records = vec![
            Record::new(vec![
                Field::Int32(1),
                Field::Int32(100),
                Field::String("Product A".into()),
            ]),
            Record::new(vec![
                Field::Int32(2),
                Field::Int32(200),
                Field::String("Product B".into()),
            ]),
            Record::new(vec![
                Field::Int32(3),
                Field::Int32(150),
                Field::String("Product C".into()),
            ]),
        ];

        executor
            .with_heap_file("products", |hf| {
                for record in records {
                    hf.insert(record).unwrap();
                }
            })
            .unwrap();

        let (select_plan, select_ast) = create_single_statement(
            "SELECT id, name FROM products WHERE price > 100;",
            &executor,
        );

        let result = executor.execute_statement(&select_plan, &select_ast);
        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r.fields[0] == Field::Int32(2)));
        assert!(rows.iter().any(|r| r.fields[0] == Field::Int32(3)));
    }

    #[test]
    fn test_execute_select_with_where_clause_no_matches() {
        let (executor, _temp_dir) = create_test_executor();

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        let record = Record::new(vec![
            Field::Int32(1),
            Field::Int32(25),
            Field::String("Alice".into()),
        ]);

        executor
            .with_heap_file("users", |hf| {
                hf.insert(record).unwrap();
            })
            .unwrap();

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

        let (create_plan, create_ast) = create_single_statement(
            "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING, age INT32);",
            &executor,
        );

        executor.execute_statement(&create_plan, &create_ast);

        let records = vec![
            Record::new(vec![
                Field::Int32(1),
                Field::Int32(25),
                Field::String("Alice".into()),
            ]),
            Record::new(vec![
                Field::Int32(2),
                Field::Int32(25),
                Field::String("Bob".into()),
            ]),
        ];

        executor
            .with_heap_file("users", |hf| {
                for record in records {
                    hf.insert(record).unwrap();
                }
            })
            .unwrap();

        let (select_plan, select_ast) =
            create_single_statement("SELECT id, name FROM users WHERE TRUE;", &executor);

        let result = executor.execute_statement(&select_plan, &select_ast);

        let (_, rows) = expect_select_successful(result);

        assert_eq!(rows.len(), 2);
    }
}
