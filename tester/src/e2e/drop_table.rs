use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    TesterError,
    suite::{E2ETestResult, Suite, default_client},
};

use super::response_helpers::{expect_error, validate_non_select_statement, validate_select_query};

pub struct DropTableE2ETest;

pub struct Setup {
    pub database_name: String,
}

pub struct Test {
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite<E2ETestResult> for DropTableE2ETest {
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

        // Test 1: Drop empty table
        info!("\n=== Test 1: Drop empty table ===");
        if let Err(e) = test_drop_empty_table(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Drop empty table passed");
        tests_passed += 1;

        // Test 2: Drop table with data
        info!("\n=== Test 2: Drop table with data ===");
        if let Err(e) = test_drop_table_with_data(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Drop table with data passed");
        tests_passed += 1;

        // Test 3: Drop non-existent table (should fail)
        info!("\n=== Test 3: Drop non-existent table (should fail) ===");
        if let Err(e) = test_drop_nonexistent_table(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Drop non-existent table correctly rejected");
        tests_passed += 1;

        // Test 4: Drop multiple tables
        info!("\n=== Test 4: Drop multiple tables ===");
        if let Err(e) = test_drop_multiple_tables(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Drop multiple tables passed");
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

/// Test 1: Drop empty table
async fn test_drop_empty_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE empty_drop (id INT32 PRIMARY_KEY);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Drop table
    let drop_sql = "DROP TABLE empty_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    // Verify table is gone by trying to select from it (should fail)
    let select_sql = "SELECT * FROM empty_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    // Expect error since table doesn't exist
    expect_error(&mut client).await?;
    info!("✓ Table correctly dropped and not found");
    Ok(())
}

/// Test 2: Drop table with data
async fn test_drop_table_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE data_drop (id INT32 PRIMARY_KEY, value INT64);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert data
    info!("Inserting 50 records...");
    for i in 0..50 {
        let insert_sql = format!(
            "INSERT INTO data_drop (id, value) VALUES ({}, {});",
            i,
            i * 100
        );
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    // Verify data exists
    let select_sql = "SELECT * FROM data_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("value", ColumnType::I64)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 50 {
        return Err(TesterError::ServerError {
            message: format!("Expected 50 records before drop but got {}", records.len()),
        });
    }

    // Drop table
    info!("Dropping table with data...");
    let drop_sql = "DROP TABLE data_drop;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;

    info!("✓ Table with data dropped successfully");
    Ok(())
}

/// Test 3: Drop non-existent table (should fail)
async fn test_drop_nonexistent_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Try to drop table that doesn't exist
    let drop_sql = "DROP TABLE nonexistent_table;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: drop_sql.to_string(),
        })
        .await?;

    // We expect an error here
    let message = expect_error(&mut client).await?;
    info!(
        "✓ Got expected error when dropping non-existent table: {}",
        message
    );
    Ok(())
}

/// Test 4: Drop multiple tables
async fn test_drop_multiple_tables(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create three tables
    let tables = vec!["table1", "table2", "table3"];

    for table_name in &tables {
        let create_sql = format!("CREATE TABLE {} (id INT32 PRIMARY_KEY);", table_name);
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: create_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;
    }

    // Drop all three tables
    for table_name in &tables {
        let drop_sql = format!("DROP TABLE {};", table_name);
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: drop_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 0, StatementType::DropTable).await?;
    }

    info!("✓ All three tables dropped successfully");
    Ok(())
}
