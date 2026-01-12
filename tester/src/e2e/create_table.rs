use log::{error, info};
use protocol::{ColumnType, Request, Response, StatementType};

use crate::{
    TesterError,
    client::ReadResult,
    suite::{E2ETestResult, Suite, default_client},
};

use super::response_helpers::{
    expect_acknowledge, validate_non_select_statement, validate_select_query,
};

pub struct CreateTableE2ETest;

pub struct Setup {
    pub database_name: String,
}

pub struct Test {
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite<E2ETestResult> for CreateTableE2ETest {
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

        // Test 1: Create simple table with one column
        info!("\n=== Test 1: Create simple table with one column ===");
        if let Err(e) = test_create_simple_table(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Create simple table passed");
        tests_passed += 1;

        // Test 2: Create table with primary key
        info!("\n=== Test 2: Create table with primary key ===");
        if let Err(e) = test_create_table_with_primary_key(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Create table with primary key passed");
        tests_passed += 1;

        // Test 3: Create table with all data types
        info!("\n=== Test 3: Create table with all data types ===");
        if let Err(e) = test_create_table_all_types(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Create table with all data types passed");
        tests_passed += 1;

        // Test 4: Create table with multiple columns
        info!("\n=== Test 4: Create table with multiple columns ===");
        if let Err(e) = test_create_table_multiple_columns(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Create table with multiple columns passed");
        tests_passed += 1;

        // Test 5: Create multiple tables in same database
        info!("\n=== Test 5: Create multiple tables in same database ===");
        if let Err(e) = test_create_multiple_tables(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: Create multiple tables passed");
        tests_passed += 1;

        // Test 6: Create table that already exists (should fail)
        info!("\n=== Test 6: Create table that already exists (should fail) ===");
        if let Err(e) = test_create_duplicate_table(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: Duplicate table creation correctly rejected");
        tests_passed += 1;

        // Test 7: Verify table by inserting and selecting data
        info!("\n=== Test 7: Verify table by inserting and selecting data ===");
        if let Err(e) = test_verify_table_with_data(args).await {
            error!("Test 7 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 7: Table verification with data passed");
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

/// Test 1: Create simple table with one column
async fn test_create_simple_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let create_table_sql = "CREATE TABLE simple_table (id INT32 PRIMARY_KEY);";

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    info!("✓ Simple table created successfully");
    Ok(())
}

/// Test 2: Create table with primary key
async fn test_create_table_with_primary_key(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let create_table_sql = "CREATE TABLE pk_table (user_id INT64 PRIMARY_KEY, name STRING);";

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    info!("✓ Table with primary key created successfully");
    Ok(())
}

/// Test 3: Create table with all data types
async fn test_create_table_all_types(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let create_table_sql = "CREATE TABLE all_types_table (\
        id INT32 PRIMARY_KEY, \
        big_num INT64, \
        small_price FLOAT32, \
        big_price FLOAT64, \
        is_active BOOL, \
        birth_date DATE, \
        created_at DATETIME, \
        description STRING\
    );";

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    info!("✓ Table with all data types created successfully");
    Ok(())
}

/// Test 4: Create table with multiple columns
async fn test_create_table_multiple_columns(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let create_table_sql = "CREATE TABLE multi_column_table (\
        id INT32 PRIMARY_KEY, \
        col1 STRING, \
        col2 STRING, \
        col3 STRING, \
        col4 STRING, \
        col5 STRING\
    );";

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    info!("✓ Table with multiple columns created successfully");
    Ok(())
}

/// Test 5: Create multiple tables in same database
async fn test_create_multiple_tables(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create first table
    let create_table1_sql = "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table1_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Create second table
    let create_table2_sql = "CREATE TABLE orders (order_id INT32 PRIMARY_KEY, amount FLOAT32);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table2_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Create third table
    let create_table3_sql = "CREATE TABLE products (product_id INT32 PRIMARY_KEY, price FLOAT64);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table3_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    info!("✓ Multiple tables created successfully");
    Ok(())
}

/// Test 6: Create table that already exists (should fail)
async fn test_create_duplicate_table(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // First, create a table
    let create_table_sql = "CREATE TABLE duplicate_test (id INT32 PRIMARY_KEY);";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Now try to create the same table again (should fail)
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    // We expect an error here
    match expect_acknowledge(&mut client).await {
        Ok(_) => {
            // If we got acknowledge, check if we get an error in the next response
            match client.read_response().await? {
                ReadResult::Response(Response::Error { message, .. }) => {
                    info!(
                        "✓ Got expected error when creating duplicate table: {}",
                        message
                    );
                    Ok(())
                }
                ReadResult::Response(Response::StatementCompleted { .. }) => {
                    error!("Creating duplicate table should have failed but succeeded!");
                    Err(TesterError::ServerError {
                        message: "Creating duplicate table should have failed but succeeded"
                            .to_string(),
                    })
                }
                _ => {
                    error!("Unexpected response type when creating duplicate table");
                    Err(TesterError::ServerError {
                        message: "Unexpected response type when creating duplicate table"
                            .to_string(),
                    })
                }
            }
        }
        Err(_) => {
            // Got error immediately, that's good
            info!("✓ Duplicate table creation correctly rejected");
            Ok(())
        }
    }
}

/// Test 7: Verify table by inserting and selecting data
async fn test_verify_table_with_data(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Create table
    let create_table_sql = "CREATE TABLE verification_table (\
        id INT32 PRIMARY_KEY, \
        value INT64, \
        name STRING\
    );";

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: create_table_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;

    // Insert some data
    let insert_sql = "INSERT INTO verification_table (id, value, name) VALUES (1, 1000, 'Test');";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: insert_sql.to_string(),
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;

    // Select the data back
    let select_sql = "SELECT * FROM verification_table;";
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql.to_string(),
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("value", ColumnType::I64),
        ("name", ColumnType::String),
    ];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != 1 {
        return Err(TesterError::ServerError {
            message: format!("Expected 1 record but got {}", records.len()),
        });
    }

    info!("✓ Table verified with data successfully");
    Ok(())
}
