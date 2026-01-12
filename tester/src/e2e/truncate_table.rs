use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    TesterError,
    suite::{Suite, default_client},
};

use super::response_helpers::{validate_non_select_statement, validate_select_query};

pub struct TruncateTableE2ETest;

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

impl Suite<E2ETestResult> for TruncateTableE2ETest {
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

        // Test 1: Truncate table removes all records
        info!("\n=== Test 1: Truncate table removes all records ===");
        if let Err(e) = test_truncate_removes_all_records(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Truncate removes all records passed");
        tests_passed += 1;

        // Test 2: Truncate empty table
        info!("\n=== Test 2: Truncate empty table ===");
        if let Err(e) = test_truncate_empty_table(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Truncate empty table passed");
        tests_passed += 1;

        // Test 3: Truncate and re-insert
        info!("\n=== Test 3: Truncate and re-insert ===");
        if let Err(e) = test_truncate_and_reinsert(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Truncate and re-insert passed");
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

/// Test 1: Truncate table removes all records
async fn test_truncate_removes_all_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE truncate_test (id INT32 PRIMARY_KEY, value INT64);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert 100 records
    info!("Inserting 100 records...");
    for i in 0..100 {
        let insert_sql = format!(
            "INSERT INTO truncate_test (id, value) VALUES ({}, {});",
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

    // Verify records exist
    let select_sql = "SELECT * FROM truncate_test;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("value", ColumnType::I64)];
    let records_before = validate_select_query(&mut client, &expected_columns).await?;

    if records_before.len() != 100 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected 100 records before truncate but got {}",
                records_before.len()
            ),
        });
    }

    // Truncate table
    info!("Truncating table...");
    let truncate_sql = "TRUNCATE TABLE truncate_test;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: truncate_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::TruncateTable).await?;

    // Verify table is empty
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let records_after = validate_select_query(&mut client, &expected_columns).await?;

    if !records_after.is_empty() {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected 0 records after truncate but got {}",
                records_after.len()
            ),
        });
    }

    info!("✓ All 100 records removed by truncate");
    Ok(())
}

/// Test 2: Truncate empty table
async fn test_truncate_empty_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE empty_truncate (id INT32 PRIMARY_KEY);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Truncate empty table
    let truncate_sql = "TRUNCATE TABLE empty_truncate;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: truncate_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::TruncateTable).await?;

    info!("✓ Truncate on empty table succeeded");
    Ok(())
}

/// Test 3: Truncate and re-insert
async fn test_truncate_and_reinsert(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE reinsert_test (id INT32 PRIMARY_KEY, name STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert some records
    for i in 0..50 {
        let insert_sql = format!(
            "INSERT INTO reinsert_test (id, name) VALUES ({}, 'User_{}');",
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

    // Truncate
    let truncate_sql = "TRUNCATE TABLE reinsert_test;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: truncate_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::TruncateTable).await?;

    // Re-insert records (can reuse same IDs)
    for i in 0..50 {
        let insert_sql = format!(
            "INSERT INTO reinsert_test (id, name) VALUES ({}, 'NewUser_{}');",
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

    // Verify new records exist
    let select_sql = "SELECT * FROM reinsert_test;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("name", ColumnType::String)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 50 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected 50 records after re-insert but got {}",
                records.len()
            ),
        });
    }

    info!("✓ Table truncated and re-inserted successfully");
    Ok(())
}
