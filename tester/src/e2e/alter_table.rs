use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    TesterError,
    suite::{Suite, default_client},
};

use super::response_helpers::{
    expect_error, extract_f64, extract_i32, extract_string, validate_non_select_statement,
    validate_select_query,
};

pub struct AlterTableE2ETest;

pub struct Setup {
    pub database_name: String,
}

pub struct Test {
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

pub struct E2ETestResult {
    pub tests_passed: usize,
}

impl Suite<E2ETestResult> for AlterTableE2ETest {
    type SetupArgs = Setup;

    async fn setup(args: &Self::SetupArgs) -> Result<(), TesterError> {
        info!("Creating database '{}'...", args.database_name);
        let mut client = default_client().await?;

        client
            .execute_and_wait(Request::CreateDatabase {
                database_name: args.database_name.clone(),
            })
            .await?;

        info!("✓ Database created");
        Ok(())
    }

    type TestArgs = Test;

    async fn run(args: &Self::TestArgs) -> Result<E2ETestResult, TesterError> {
        let mut tests_passed = 0;

        // Test 1: Rename table
        info!("\n=== Test 1: Rename table ===");
        if let Err(e) = test_rename_table(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Rename table passed");
        tests_passed += 1;

        // Test 2: Rename table with data
        info!("\n=== Test 2: Rename table with data ===");
        if let Err(e) = test_rename_table_with_data(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Rename table with data passed");
        tests_passed += 1;

        // Test 3: Rename column
        info!("\n=== Test 3: Rename column ===");
        if let Err(e) = test_rename_column(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Rename column passed");
        tests_passed += 1;

        // Test 4: Rename column with data
        info!("\n=== Test 4: Rename column with data ===");
        if let Err(e) = test_rename_column_with_data(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Rename column with data passed");
        tests_passed += 1;

        // Test 5: Add column to empty table
        info!("\n=== Test 5: Add column to empty table ===");
        if let Err(e) = test_add_column_empty_table(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: Add column to empty table passed");
        tests_passed += 1;

        // Test 6: Add column to table with data
        info!("\n=== Test 6: Add column to table with data ===");
        if let Err(e) = test_add_column_with_data(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: Add column to table with data passed");
        tests_passed += 1;

        // Test 7: Add multiple columns
        info!("\n=== Test 7: Add multiple columns ===");
        if let Err(e) = test_add_multiple_columns(args).await {
            error!("Test 7 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 7: Add multiple columns passed");
        tests_passed += 1;

        // Test 8: Drop column from empty table
        info!("\n=== Test 8: Drop column from empty table ===");
        if let Err(e) = test_drop_column_empty_table(args).await {
            error!("Test 8 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 8: Drop column from empty table passed");
        tests_passed += 1;

        // Test 9: Drop column from table with data
        info!("\n=== Test 9: Drop column from table with data ===");
        if let Err(e) = test_drop_column_with_data(args).await {
            error!("Test 9 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 9: Drop column from table with data passed");
        tests_passed += 1;

        // Test 10: Drop multiple columns
        info!("\n=== Test 10: Drop multiple columns ===");
        if let Err(e) = test_drop_multiple_columns(args).await {
            error!("Test 10 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 10: Drop multiple columns passed");
        tests_passed += 1;

        Ok(E2ETestResult { tests_passed })
    }

    type CleanupArgs = Cleanup;

    async fn cleanup(args: &Self::CleanupArgs) -> Result<(), TesterError> {
        info!("Deleting database '{}'...", args.database_name);
        let mut client = default_client().await?;

        client
            .execute_and_wait(Request::DeleteDatabase {
                database_name: args.database_name.clone(),
            })
            .await?;

        info!("✓ Database deleted");
        Ok(())
    }
}

/// Test 1: Rename table
async fn test_rename_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE old_name (id INT32 PRIMARY_KEY, value INT64);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Rename table
    let rename_sql = "ALTER TABLE old_name RENAME TABLE TO new_name;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: rename_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify old name doesn't exist
    let select_old_sql = "SELECT * FROM old_name;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_old_sql.to_string(),
        })
        .await?;

    expect_error(&mut client).await?;
    info!("✓ Old table name correctly not found");

    // Verify new name works
    let select_new_sql = "SELECT * FROM new_name;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_new_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("value", ColumnType::I64)];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_sql = "DROP TABLE new_name;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Table renamed successfully");
    Ok(())
}

/// Test 2: Rename table with data
async fn test_rename_table_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert data
    info!("Inserting 100 records...");
    for i in 0..100 {
        let insert_sql = format!("INSERT INTO users (id, name) VALUES ({}, 'user{}');", i, i);
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    // Rename table
    let rename_sql = "ALTER TABLE users RENAME TABLE TO accounts;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: rename_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify data is still there with new name
    let select_sql = "SELECT * FROM accounts ORDER BY id ASC;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("name", ColumnType::String)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 100 {
        return Err(TesterError::ServerError {
            message: format!("Expected 100 records but got {}", records.len()),
        });
    }

    // Verify all data
    for (idx, record) in records.iter().enumerate() {
        let id = extract_i32(record, 0)?;
        let name = extract_string(record, 1)?;

        let expected_id = idx as i32;
        let expected_name = format!("user{}", idx);

        if id != expected_id || name != expected_name {
            return Err(TesterError::ServerError {
                message: format!(
                    "Record {} mismatch: got ({}, {}), expected ({}, {})",
                    idx, id, name, expected_id, expected_name
                ),
            });
        }
    }

    // Cleanup
    let drop_sql = "DROP TABLE accounts;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Table with data renamed and verified successfully");
    Ok(())
}

/// Test 3: Rename column
async fn test_rename_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE test_rename (id INT32 PRIMARY_KEY, old_col STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Rename column
    let rename_sql = "ALTER TABLE test_rename RENAME COLUMN old_col TO new_col;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: rename_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify new column name works
    let select_sql = "SELECT * FROM test_rename;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("new_col", ColumnType::String)];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_sql = "DROP TABLE test_rename;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column renamed successfully");
    Ok(())
}

/// Test 4: Rename column with data
async fn test_rename_column_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE products (id INT32 PRIMARY_KEY, price FLOAT64);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert data
    info!("Inserting 50 records...");
    for i in 0..50 {
        let insert_sql = format!(
            "INSERT INTO products (id, price) VALUES ({}, {:.2});",
            i,
            (i as f64) * 10.5
        );
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    // Rename column
    let rename_sql = "ALTER TABLE products RENAME COLUMN price TO cost;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: rename_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify data with new column name
    let select_sql = "SELECT * FROM products ORDER BY id ASC;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("cost", ColumnType::F64)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 50 {
        return Err(TesterError::ServerError {
            message: format!("Expected 50 records but got {}", records.len()),
        });
    }

    // Verify all data
    for (idx, record) in records.iter().enumerate() {
        let id = extract_i32(record, 0)?;
        let cost = extract_f64(record, 1)?;

        let expected_id = idx as i32;
        let expected_cost = (idx as f64) * 10.5;

        if id != expected_id || (cost - expected_cost).abs() > 0.001 {
            return Err(TesterError::ServerError {
                message: format!(
                    "Record {} mismatch: got ({}, {}), expected ({}, {})",
                    idx, id, cost, expected_id, expected_cost
                ),
            });
        }
    }

    // Cleanup
    let drop_sql = "DROP TABLE products;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column with data renamed and verified successfully");
    Ok(())
}

/// Test 5: Add column to empty table
async fn test_add_column_empty_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE test_add (id INT32 PRIMARY_KEY);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Add column
    let add_sql = "ALTER TABLE test_add ADD COLUMN name STRING;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: add_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify new column exists
    let select_sql = "SELECT * FROM test_add;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("name", ColumnType::String)];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_sql = "DROP TABLE test_add;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column added to empty table successfully");
    Ok(())
}

/// Test 6: Add column to table with data
async fn test_add_column_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE employees (id INT32 PRIMARY_KEY, name STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert data
    info!("Inserting 75 records...");
    for i in 0..75 {
        let insert_sql = format!(
            "INSERT INTO employees (id, name) VALUES ({}, 'employee{}');",
            i, i
        );
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    // Add column
    let add_sql = "ALTER TABLE employees ADD COLUMN salary FLOAT64;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: add_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify new column exists and old data is intact
    // Note: String columns are always at the end, so order is: id, salary, name
    let select_sql = "SELECT * FROM employees ORDER BY id ASC;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("salary", ColumnType::F64),
        ("name", ColumnType::String),
    ];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 75 {
        return Err(TesterError::ServerError {
            message: format!("Expected 75 records but got {}", records.len()),
        });
    }

    // Verify all data (old columns should be intact, new column should be default)
    for (idx, record) in records.iter().enumerate() {
        let id = extract_i32(record, 0)?;
        let name = extract_string(record, 2)?; // name is now at index 2

        let expected_id = idx as i32;
        let expected_name = format!("employee{}", idx);

        if id != expected_id || name != expected_name {
            return Err(TesterError::ServerError {
                message: format!(
                    "Record {} mismatch: got ({}, {}), expected ({}, {})",
                    idx, id, name, expected_id, expected_name
                ),
            });
        }
    }

    // Now insert a record with the new column
    let insert_new_sql =
        "INSERT INTO employees (id, name, salary) VALUES (100, 'new_employee', 50000.0);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: insert_new_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;

    // Verify the new record
    let select_new_sql = "SELECT * FROM employees WHERE id = 100;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_new_sql.to_string(),
        })
        .await?;

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 1 {
        return Err(TesterError::ServerError {
            message: format!("Expected 1 record but got {}", records.len()),
        });
    }

    let salary = extract_f64(&records[0], 1)?; // salary is now at index 1

    if (salary - 50000.0).abs() > 0.001 {
        return Err(TesterError::ServerError {
            message: format!("Expected salary 50000.0 but got {}", salary),
        });
    }

    // Cleanup
    let drop_sql = "DROP TABLE employees;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column added to table with data and verified successfully");
    Ok(())
}

/// Test 7: Add multiple columns
async fn test_add_multiple_columns(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE multi_add (id INT32 PRIMARY_KEY);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Add first column
    let add_sql1 = "ALTER TABLE multi_add ADD COLUMN col1 STRING;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: add_sql1.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Add second column
    let add_sql2 = "ALTER TABLE multi_add ADD COLUMN col2 INT64;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: add_sql2.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Add third column
    let add_sql3 = "ALTER TABLE multi_add ADD COLUMN col3 BOOL;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: add_sql3.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify all columns exist
    // Note: String columns are always at the end, so order is: id, col2, col3, col1
    let select_sql = "SELECT * FROM multi_add;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("col2", ColumnType::I64),
        ("col3", ColumnType::Bool),
        ("col1", ColumnType::String),
    ];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_sql = "DROP TABLE multi_add;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Multiple columns added successfully");
    Ok(())
}

/// Test 8: Drop column from empty table
async fn test_drop_column_empty_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table with two columns
    let create_sql = "CREATE TABLE test_drop (id INT32 PRIMARY_KEY, old_col STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Drop column
    let drop_sql = "ALTER TABLE test_drop DROP COLUMN old_col;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify column is gone
    let select_sql = "SELECT * FROM test_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32)];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_table_sql = "DROP TABLE test_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column dropped from empty table successfully");
    Ok(())
}

/// Test 9: Drop column from table with data
async fn test_drop_column_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_sql = "CREATE TABLE orders (id INT32 PRIMARY_KEY, product STRING, quantity INT32);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert data
    info!("Inserting 60 records...");
    for i in 0..60 {
        let insert_sql = format!(
            "INSERT INTO orders (id, product, quantity) VALUES ({}, 'product{}', {});",
            i,
            i,
            i * 10
        );
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    // Drop column
    let drop_sql = "ALTER TABLE orders DROP COLUMN quantity;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify column is gone and other data is intact
    let select_sql = "SELECT * FROM orders ORDER BY id ASC;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("product", ColumnType::String)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 60 {
        return Err(TesterError::ServerError {
            message: format!("Expected 60 records but got {}", records.len()),
        });
    }

    // Verify remaining data
    for (idx, record) in records.iter().enumerate() {
        let id = extract_i32(record, 0)?;
        let product = extract_string(record, 1)?;

        let expected_id = idx as i32;
        let expected_product = format!("product{}", idx);

        if id != expected_id || product != expected_product {
            return Err(TesterError::ServerError {
                message: format!(
                    "Record {} mismatch: got ({}, {}), expected ({}, {})",
                    idx, id, product, expected_id, expected_product
                ),
            });
        }
    }

    // Cleanup
    let drop_table_sql = "DROP TABLE orders;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Column dropped from table with data and verified successfully");
    Ok(())
}

/// Test 10: Drop multiple columns
async fn test_drop_multiple_columns(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table with many columns (string at the end)
    let create_sql = "CREATE TABLE multi_drop (id INT32 PRIMARY_KEY, col2 INT64, col3 BOOL, col4 FLOAT32, col1 STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Drop first column
    let drop_sql1 = "ALTER TABLE multi_drop DROP COLUMN col2;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql1.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Drop second column
    let drop_sql2 = "ALTER TABLE multi_drop DROP COLUMN col4;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql2.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::AlterTable).await?;

    // Verify only remaining columns exist (string still at the end)
    let select_sql = "SELECT * FROM multi_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("col3", ColumnType::Bool),
        ("col1", ColumnType::String),
    ];
    validate_select_query(&mut client, &expected_columns).await?;

    // Cleanup
    let drop_table_sql = "DROP TABLE multi_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Multiple columns dropped successfully");
    Ok(())
}
