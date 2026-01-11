use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    TesterError,
    suite::{Suite, default_client},
};

use super::response_helpers::{validate_non_select_statement, validate_select_query};

/// Test record structure for DELETE tests
#[derive(Debug, Clone)]
pub struct DeleteTestRecord {
    pub id: i32,
    pub big_id: i64,
    pub price: f32,
    pub precise_price: f64,
    pub active: bool,
    pub birth_date: String,
    pub last_login: String,
    pub name: String,
}

impl DeleteTestRecord {
    /// Generate test records
    pub fn generate(num_records: usize) -> Vec<Self> {
        (0..num_records)
            .map(|i| DeleteTestRecord {
                id: i as i32,
                big_id: (i as i64) * 1000,
                price: (i as f32) * 1.5,
                precise_price: (i as f64) * 2.75,
                active: i % 2 == 0,
                birth_date: format!("2024-01-{:02}", (i % 28) + 1),
                last_login: format!("2024-01-{:02}T12:00:00", (i % 28) + 1),
                name: format!("User_{}", i),
            })
            .collect()
    }

    /// Verify this record does NOT exist in the database
    pub async fn verify_not_in_db(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), TesterError> {
        let mut client = default_client().await?;

        let sql = format!("SELECT * FROM {} WHERE id = {};", table_name, self.id);
        client
            .send_request(&Request::Query {
                database_name: Some(database_name.to_string()),
                sql,
            })
            .await?;

        let expected_columns = vec![
            ("id", ColumnType::I32),
            ("big_id", ColumnType::I64),
            ("price", ColumnType::F32),
            ("precise_price", ColumnType::F64),
            ("active", ColumnType::Bool),
            ("birth_date", ColumnType::Date),
            ("last_login", ColumnType::DateTime),
            ("name", ColumnType::String),
        ];

        let records = validate_select_query(&mut client, &expected_columns).await?;

        if !records.is_empty() {
            return Err(TesterError::ServerError {
                message: format!(
                    "Expected record with id={} to be deleted but it still exists",
                    self.id
                ),
            });
        }

        Ok(())
    }

    /// Verify this record still exists in the database
    pub async fn verify_still_exists(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), TesterError> {
        let mut client = default_client().await?;

        let sql = format!("SELECT id FROM {} WHERE id = {};", table_name, self.id);
        client
            .send_request(&Request::Query {
                database_name: Some(database_name.to_string()),
                sql,
            })
            .await?;

        let expected_columns = vec![("id", ColumnType::I32)];

        let records = validate_select_query(&mut client, &expected_columns).await?;

        if records.is_empty() {
            return Err(TesterError::ServerError {
                message: format!(
                    "Expected record with id={} to still exist but it was deleted",
                    self.id
                ),
            });
        }

        Ok(())
    }
}

/// Helper function to count all records in the table
async fn count_records(database_name: &str, table_name: &str) -> Result<usize, TesterError> {
    let mut client = default_client().await?;

    let sql = format!("SELECT id FROM {};", table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(database_name.to_string()),
            sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32)];
    let records = validate_select_query(&mut client, &expected_columns).await?;

    Ok(records.len())
}

pub struct DeleteE2ETest;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
    pub num_records: usize,
}

pub struct Test {
    pub database_name: String,
    pub table_name: String,
    pub test_data: Vec<DeleteTestRecord>,
}

pub struct Cleanup {
    pub database_name: String,
}

pub struct E2ETestResult {
    pub tests_passed: usize,
}

impl Suite<E2ETestResult> for DeleteE2ETest {
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

        // Create table with all data types
        let create_table_sql = format!(
            "CREATE TABLE {} (\
                id INT32 PRIMARY_KEY, \
                big_id INT64, \
                price FLOAT32, \
                precise_price FLOAT64, \
                active BOOL, \
                birth_date DATE, \
                last_login DATETIME, \
                name STRING\
            );",
            args.table_name
        );

        info!("Creating table with all data types...");
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: create_table_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;
        info!("✓ Table created");

        // Generate and insert test data
        let test_data = DeleteTestRecord::generate(args.num_records);

        info!("Inserting {} records...", test_data.len());
        for record in &test_data {
            let insert_sql = format!(
                "INSERT INTO {} (id, big_id, price, precise_price, active, birth_date, last_login, name) \
                VALUES ({}, {}, {:.1}, {:.2}, {}, '{}', '{}', '{}');",
                args.table_name,
                record.id,
                record.big_id,
                record.price,
                record.precise_price,
                record.active,
                record.birth_date,
                record.last_login,
                record.name
            );

            client
                .send_request(&Request::Query {
                    database_name: Some(args.database_name.clone()),
                    sql: insert_sql,
                })
                .await?;

            validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
        }

        info!("✓ {} records inserted", test_data.len());
        Ok(())
    }

    type TestArgs = Test;

    async fn run(args: &Self::TestArgs) -> Result<E2ETestResult, TesterError> {
        let mut tests_passed = 0;

        // Test 1: Delete one record by primary key
        info!("\n=== Test 1: Delete one record by primary key ===");
        if let Err(e) = test_delete_one_record(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Delete one record passed");
        tests_passed += 1;

        // Test 2: Delete no records (WHERE matches nothing)
        info!("\n=== Test 2: Delete no records (WHERE matches nothing) ===");
        if let Err(e) = test_delete_no_records(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Delete no records passed");
        tests_passed += 1;

        // Test 3: Delete records by INT64 field
        info!("\n=== Test 3: Delete records by INT64 field ===");
        if let Err(e) = test_delete_by_int64(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Delete by INT64 passed");
        tests_passed += 1;

        // Test 4: Delete records by FLOAT32 field
        info!("\n=== Test 4: Delete records by FLOAT32 field ===");
        if let Err(e) = test_delete_by_float32(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Delete by FLOAT32 passed");
        tests_passed += 1;

        // Test 5: Delete records by BOOL field
        info!("\n=== Test 5: Delete records by BOOL field ===");
        if let Err(e) = test_delete_by_bool(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: Delete by BOOL passed");
        tests_passed += 1;

        // Test 6: Delete records by DATE field
        info!("\n=== Test 6: Delete records by DATE field ===");
        if let Err(e) = test_delete_by_date(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: Delete by DATE passed");
        tests_passed += 1;

        // Test 7: Delete records by STRING field
        info!("\n=== Test 7: Delete records by STRING field ===");
        if let Err(e) = test_delete_by_string(args).await {
            error!("Test 7 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 7: Delete by STRING passed");
        tests_passed += 1;

        // Test 8: Delete range of records with WHERE clause
        info!("\n=== Test 8: Delete range of records with WHERE ===");
        if let Err(e) = test_delete_range(args).await {
            error!("Test 8 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 8: Delete range passed");
        tests_passed += 1;

        // Test 9: Delete with complex WHERE clause
        info!("\n=== Test 9: Delete with complex WHERE clause ===");
        if let Err(e) = test_delete_complex_where(args).await {
            error!("Test 9 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 9: Delete with complex WHERE passed");
        tests_passed += 1;

        // Test 10: Delete ALL records (no WHERE clause) - runs last
        info!("\n=== Test 10: Delete ALL records (no WHERE) ===");
        if let Err(e) = test_delete_all_records(args).await {
            error!("Test 10 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 10: Delete ALL records passed");
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

/// Test 1: Delete one record by primary key
async fn test_delete_one_record(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete record with id = 100
    let delete_sql = format!("DELETE FROM {} WHERE id = 100;", args.table_name);

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Delete).await?;

    // Verify record count decreased by exactly 1
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - 1 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - 1,
                count_after
            ),
        });
    }

    // Verify the record is deleted
    args.test_data[100]
        .verify_not_in_db(&args.database_name, &args.table_name)
        .await?;

    // Verify adjacent records still exist
    args.test_data[99]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[101]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Single record deleted and verified");
    Ok(())
}

/// Test 2: Delete no records (WHERE matches nothing)
async fn test_delete_no_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Try to delete record with id that doesn't exist
    let delete_sql = format!("DELETE FROM {} WHERE id = 999999;", args.table_name);

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 0, StatementType::Delete).await?;

    // Verify record count stayed the same
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after no-op deletion but got {}",
                count_before, count_after
            ),
        });
    }

    // Verify some records still exist
    args.test_data[0]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[500]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ No records deleted as expected");
    Ok(())
}

/// Test 3: Delete records by INT64 field
async fn test_delete_by_int64(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where big_id >= 200000 AND big_id < 210000
    // This corresponds to ids 200-209 (10 records)
    let delete_sql = format!(
        "DELETE FROM {} WHERE big_id >= 200000 AND big_id < 210000;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 10, StatementType::Delete).await?;

    // Verify record count decreased by exactly 10
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - 10 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - 10,
                count_after
            ),
        });
    }

    // Verify ALL 10 records are deleted
    info!("Verifying all 10 deleted records...");
    for id in 200..210 {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify records outside the range still exist
    args.test_data[199]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[210]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Records deleted by INT64 field verified");
    Ok(())
}

/// Test 4: Delete records by FLOAT32 field
async fn test_delete_by_float32(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where price >= 450.0 AND price < 465.0
    // This corresponds to ids 300-309 (10 records, since price = id * 1.5)
    let delete_sql = format!(
        "DELETE FROM {} WHERE price >= 450.0 AND price < 465.0;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 10, StatementType::Delete).await?;

    // Verify record count decreased by exactly 10
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - 10 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - 10,
                count_after
            ),
        });
    }

    // Verify ALL 10 records are deleted
    info!("Verifying all 10 deleted records...");
    for id in 300..310 {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify records outside the range still exist
    args.test_data[299]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[310]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Records deleted by FLOAT32 field verified");
    Ok(())
}

/// Test 5: Delete records by BOOL field
async fn test_delete_by_bool(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where active = FALSE AND id >= 400 AND id < 450
    // This will delete odd ids in that range (25 records)
    let delete_sql = format!(
        "DELETE FROM {} WHERE active = FALSE AND id >= 400 AND id < 450;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    let expected_deletes = (400..450).filter(|id| id % 2 != 0).count();
    validate_non_select_statement(&mut client, expected_deletes, StatementType::Delete).await?;

    // Verify record count decreased by expected amount
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - expected_deletes {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - expected_deletes,
                count_after
            ),
        });
    }

    // Verify ALL odd records in range are deleted
    info!("Verifying all {} deleted records...", expected_deletes);
    for id in (400..450).filter(|id| id % 2 != 0) {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify even records in range still exist
    info!("Verifying {} records still exist...", 25);
    for id in (400..450).filter(|id| id % 2 == 0) {
        args.test_data[id]
            .verify_still_exists(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ Records deleted by BOOL field verified");
    Ok(())
}

/// Test 6: Delete records by DATE field
async fn test_delete_by_date(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where birth_date = '2024-01-15'
    // Since birth_date cycles every 28 days, this will delete multiple records
    let delete_sql = format!(
        "DELETE FROM {} WHERE birth_date = '2024-01-15' AND id >= 500 AND id < 600;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    // Calculate expected: ids where (id % 28) + 1 == 15, i.e., id % 28 == 14
    let expected_deletes = (500..600).filter(|id| id % 28 == 14).count();
    validate_non_select_statement(&mut client, expected_deletes, StatementType::Delete).await?;

    // Verify record count decreased by expected amount
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - expected_deletes {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - expected_deletes,
                count_after
            ),
        });
    }

    // Verify ALL matching records are deleted
    info!("Verifying all {} deleted records...", expected_deletes);
    for id in (500..600).filter(|id| id % 28 == 14) {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify some non-matching records still exist
    for id in (500..600).filter(|id| id % 28 != 14).step_by(10) {
        args.test_data[id]
            .verify_still_exists(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ Records deleted by DATE field verified");
    Ok(())
}

/// Test 7: Delete records by STRING field
async fn test_delete_by_string(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete specific records by name pattern (ids 700-709)
    let mut delete_count = 0;
    for id in 700..710 {
        let delete_sql = format!(
            "DELETE FROM {} WHERE name = 'User_{}';",
            args.table_name, id
        );

        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: delete_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Delete).await?;
        delete_count += 1;
    }

    // Verify record count decreased by exactly delete_count
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - delete_count {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - delete_count,
                count_after
            ),
        });
    }

    // Verify ALL 10 records are deleted
    info!("Verifying all {} deleted records...", delete_count);
    for id in 700..710 {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify adjacent records still exist
    args.test_data[699]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[710]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Records deleted by STRING field verified");
    Ok(())
}

/// Test 8: Delete range of records with WHERE clause
async fn test_delete_range(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where id >= 1000 AND id < 1200 (200 records)
    let delete_sql = format!(
        "DELETE FROM {} WHERE id >= 1000 AND id < 1200;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 200, StatementType::Delete).await?;

    // Verify record count decreased by exactly 200
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - 200 {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - 200,
                count_after
            ),
        });
    }

    // Verify ALL 200 records are deleted
    info!("Verifying all 200 deleted records...");
    for id in 1000..1200 {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify records outside the range still exist
    args.test_data[999]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;
    args.test_data[1200]
        .verify_still_exists(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Range delete verified");
    Ok(())
}

/// Test 9: Delete with complex WHERE clause
async fn test_delete_complex_where(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Count records before deletion
    let count_before = count_records(&args.database_name, &args.table_name).await?;

    // Delete records where id >= 2000 AND id < 2100 AND active = TRUE AND price > 3000.0
    // active = TRUE means even ids
    // price > 3000.0 means id > 2000 (since price = id * 1.5)
    // So this will delete even ids from 2001 to 2099 where price > 3000.0
    let delete_sql = format!(
        "DELETE FROM {} WHERE id >= 2000 AND id < 2100 AND active = TRUE AND price > 3000.0;",
        args.table_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    // Calculate expected: even ids in range where id * 1.5 > 3000.0, i.e., id > 2000
    let expected_deletes = (2001..2100).filter(|id| id % 2 == 0).count();
    validate_non_select_statement(&mut client, expected_deletes, StatementType::Delete).await?;

    // Verify record count decreased by expected amount
    let count_after = count_records(&args.database_name, &args.table_name).await?;
    if count_after != count_before - expected_deletes {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records after deletion but got {}",
                count_before - expected_deletes,
                count_after
            ),
        });
    }

    // Verify ALL matching records are deleted
    info!("Verifying all {} deleted records...", expected_deletes);
    for id in (2001..2100).filter(|id| id % 2 == 0) {
        args.test_data[id]
            .verify_not_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify odd records in range still exist
    info!("Verifying {} unaffected records...", 50);
    for id in (2000..2100).filter(|id| id % 2 != 0) {
        args.test_data[id]
            .verify_still_exists(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ Complex WHERE clause delete verified");
    Ok(())
}

/// Test 10: Delete ALL records (no WHERE clause)
async fn test_delete_all_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // First, count how many records are left
    let count_sql = format!("SELECT id FROM {};", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: count_sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32)];
    let records_before = validate_select_query(&mut client, &expected_columns).await?;
    let remaining_count = records_before.len();

    info!("Deleting all {} remaining records...", remaining_count);

    // Delete ALL records (no WHERE clause)
    let delete_sql = format!("DELETE FROM {};", args.table_name);

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: delete_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, remaining_count, StatementType::Delete).await?;

    // Verify table is empty
    let count_sql = format!("SELECT id FROM {};", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: count_sql,
        })
        .await?;

    let records_after = validate_select_query(&mut client, &expected_columns).await?;

    if !records_after.is_empty() {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected table to be empty but found {} records",
                records_after.len()
            ),
        });
    }

    info!("✓ All {} records deleted successfully", remaining_count);
    Ok(())
}
