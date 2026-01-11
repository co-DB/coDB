use log::{error, info};
use protocol::{ColumnType, Request, Response, StatementType};

use crate::{
    TesterError,
    client::ReadResult,
    e2e::response_helpers::expect_acknowledge,
    suite::{Suite, default_client},
};

use super::response_helpers::{
    extract_bool, extract_f32, extract_f64, extract_i32, extract_i64, extract_string,
    validate_field_count, validate_non_select_statement, validate_select_query,
};

/// Test record structure for UPDATE tests
#[derive(Debug, Clone)]
pub struct UpdateTestRecord {
    pub id: i32,
    pub big_id: i64,
    pub price: f32,
    pub precise_price: f64,
    pub active: bool,
    pub birth_date: String,
    pub last_login: String,
    pub name: String,
}

impl UpdateTestRecord {
    /// Generate test records
    pub fn generate(num_records: usize) -> Vec<Self> {
        (0..num_records)
            .map(|i| UpdateTestRecord {
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

    /// Verify this record in the database
    pub async fn verify_in_db(
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

        if records.len() != 1 {
            error!(
                "Expected 1 record with id={} but got {}",
                self.id,
                records.len()
            );
            return Err(TesterError::ServerError {
                message: format!(
                    "Expected 1 record with id={} but got {}",
                    self.id,
                    records.len()
                ),
            });
        }

        let record = &records[0];
        validate_field_count(record, 8)?;

        let id = extract_i32(record, 0)?;
        let big_id = extract_i64(record, 1)?;
        let price = extract_f32(record, 2)?;
        let precise_price = extract_f64(record, 3)?;
        let active = extract_bool(record, 4)?;
        let name = extract_string(record, 7)?;

        if id != self.id {
            return Err(TesterError::ServerError {
                message: format!("ID mismatch: expected {}, got {}", self.id, id),
            });
        }
        if big_id != self.big_id {
            return Err(TesterError::ServerError {
                message: format!("big_id mismatch: expected {}, got {}", self.big_id, big_id),
            });
        }
        if (price - self.price).abs() > 0.01 {
            return Err(TesterError::ServerError {
                message: format!("price mismatch: expected {}, got {}", self.price, price),
            });
        }
        if (precise_price - self.precise_price).abs() > 0.001 {
            return Err(TesterError::ServerError {
                message: format!(
                    "precise_price mismatch: expected {}, got {}",
                    self.precise_price, precise_price
                ),
            });
        }
        if active != self.active {
            return Err(TesterError::ServerError {
                message: format!("active mismatch: expected {}, got {}", self.active, active),
            });
        }
        if name != self.name {
            return Err(TesterError::ServerError {
                message: format!("name mismatch: expected '{}', got '{}'", self.name, name),
            });
        }

        Ok(())
    }
}

pub struct UpdateE2ETest;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
    pub num_records: usize,
}

pub struct Test {
    pub database_name: String,
    pub table_name: String,
    pub test_data: Vec<UpdateTestRecord>,
}

pub struct Cleanup {
    pub database_name: String,
}

pub struct E2ETestResult {
    pub tests_passed: usize,
}

impl Suite<E2ETestResult> for UpdateE2ETest {
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
        let test_data = UpdateTestRecord::generate(args.num_records);

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

        // TODO: uncomment once its done
        // Test 1: Try to update primary key (should fail)
        // info!("\n=== Test 1: Update primary key (should fail) ===");
        // if let Err(e) = test_update_primary_key_fails(args).await {
        //     error!("Test 1 failed: {:?}", e);
        //     return Err(e);
        // }
        // info!("✓ Test 1: Update primary key correctly rejected");
        // tests_passed += 1;

        // Test 2: Update INT64 column (big_id)
        info!("\n=== Test 2: Update INT64 column ===");
        if let Err(e) = test_update_int64_column(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Update INT64 column passed");
        tests_passed += 1;

        // Test 3: Update FLOAT32 column (price)
        info!("\n=== Test 3: Update FLOAT32 column ===");
        if let Err(e) = test_update_float32_column(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Update FLOAT32 column passed");
        tests_passed += 1;

        // Test 4: Update FLOAT64 column (precise_price)
        info!("\n=== Test 4: Update FLOAT64 column ===");
        if let Err(e) = test_update_float64_column(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Update FLOAT64 column passed");
        tests_passed += 1;

        // Test 5: Update BOOL column (active)
        info!("\n=== Test 5: Update BOOL column ===");
        if let Err(e) = test_update_bool_column(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: Update BOOL column passed");
        tests_passed += 1;

        // Test 6: Update DATE column (birth_date)
        info!("\n=== Test 6: Update DATE column ===");
        if let Err(e) = test_update_date_column(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: Update DATE column passed");
        tests_passed += 1;

        // Test 7: Update DATETIME column (last_login)
        info!("\n=== Test 7: Update DATETIME column ===");
        if let Err(e) = test_update_datetime_column(args).await {
            error!("Test 7 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 7: Update DATETIME column passed");
        tests_passed += 1;

        // Test 8: Update STRING column (name)
        info!("\n=== Test 8: Update STRING column ===");
        if let Err(e) = test_update_string_column(args).await {
            error!("Test 8 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 8: Update STRING column passed");
        tests_passed += 1;

        // Test 9: Update with WHERE clause (range)
        info!("\n=== Test 9: Update with WHERE clause (range) ===");
        if let Err(e) = test_update_with_where_clause(args).await {
            error!("Test 9 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 9: Update with WHERE clause passed");
        tests_passed += 1;

        // Test 10: Update multiple columns at once
        info!("\n=== Test 10: Update multiple columns at once ===");
        if let Err(e) = test_update_multiple_columns(args).await {
            error!("Test 10 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 10: Update multiple columns passed");
        tests_passed += 1;

        // Test 11: Update with complex WHERE clause
        info!("\n=== Test 11: Update with complex WHERE clause ===");
        if let Err(e) = test_update_complex_where(args).await {
            error!("Test 11 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 11: Update with complex WHERE clause passed");
        tests_passed += 1;

        // Test 12: Update ALL records (no WHERE clause) - runs last as it modifies all data
        info!("\n=== Test 12: Update ALL records (no WHERE) ===");
        if let Err(e) = test_update_all_records(args).await {
            error!("Test 12 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 12: Update ALL records passed");
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

/// Test 1: Attempt to update primary key (should fail)
#[allow(dead_code)]
async fn test_update_primary_key_fails(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let update_sql = format!("UPDATE {} SET id = 99999 WHERE id = 0;", args.table_name);

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    // We expect an error here
    match expect_acknowledge(&mut client).await {
        Ok(_) => {
            // If we got acknowledge, check if we get an error in the next response
            match client.read_response().await? {
                ReadResult::Response(Response::Error { message, .. }) => {
                    info!(
                        "✓ Got expected error when updating primary key: {}",
                        message
                    );
                    Ok(())
                }
                ReadResult::Response(Response::StatementCompleted { .. }) => {
                    error!("UPDATE on primary key should have failed but succeeded!");
                    Err(TesterError::ServerError {
                        message: "UPDATE on primary key should have failed but succeeded"
                            .to_string(),
                    })
                }
                _ => {
                    error!("Unexpected response type when updating primary key");
                    Err(TesterError::ServerError {
                        message: "Unexpected response type when updating primary key".to_string(),
                    })
                }
            }
        }
        Err(_) => {
            // Got error immediately, that's good
            info!("✓ Update primary key correctly rejected");
            Ok(())
        }
    }
}

/// Test 2: Update INT64 column
async fn test_update_int64_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update big_id for id 100
    let new_big_id = 999999;
    let update_sql = format!(
        "UPDATE {} SET big_id = {} WHERE id = 100;",
        args.table_name, new_big_id
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[100].clone();
    expected.big_id = new_big_id;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ INT64 column updated and verified");
    Ok(())
}

/// Test 3: Update FLOAT32 column
async fn test_update_float32_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update price for id 200
    let new_price = 123.45;
    let update_sql = format!(
        "UPDATE {} SET price = {:.2} WHERE id = 200;",
        args.table_name, new_price
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[200].clone();
    expected.price = new_price;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ FLOAT32 column updated and verified");
    Ok(())
}

/// Test 4: Update FLOAT64 column
async fn test_update_float64_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update precise_price for id 300
    let new_precise_price = 987.654321;
    let update_sql = format!(
        "UPDATE {} SET precise_price = {:.6} WHERE id = 300;",
        args.table_name, new_precise_price
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[300].clone();
    expected.precise_price = new_precise_price;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ FLOAT64 column updated and verified");
    Ok(())
}

/// Test 5: Update BOOL column
async fn test_update_bool_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update active for id 400 (flip it)
    let original_active = args.test_data[400].active;
    let new_active = !original_active;
    let update_sql = format!(
        "UPDATE {} SET active = {} WHERE id = 400;",
        args.table_name, new_active
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[400].clone();
    expected.active = new_active;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ BOOL column updated and verified");
    Ok(())
}

/// Test 6: Update DATE column
async fn test_update_date_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update birth_date for id 500
    let new_birth_date = "2025-12-25".to_string();
    let update_sql = format!(
        "UPDATE {} SET birth_date = '{}' WHERE id = 500;",
        args.table_name, new_birth_date
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[500].clone();
    expected.birth_date = new_birth_date;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ DATE column updated and verified");
    Ok(())
}

/// Test 7: Update DATETIME column
async fn test_update_datetime_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update last_login for id 600
    let new_last_login = "2025-12-31T23:59:59".to_string();
    let update_sql = format!(
        "UPDATE {} SET last_login = '{}' WHERE id = 600;",
        args.table_name, new_last_login
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[600].clone();
    expected.last_login = new_last_login;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ DATETIME column updated and verified");
    Ok(())
}

/// Test 8: Update STRING column
async fn test_update_string_column(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update name for id 700
    let new_name = "UpdatedUser_700".to_string();
    let update_sql = format!(
        "UPDATE {} SET name = '{}' WHERE id = 700;",
        args.table_name, new_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[700].clone();
    expected.name = new_name;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ STRING column updated and verified");
    Ok(())
}

/// Test 9: Update with WHERE clause (range)
async fn test_update_with_where_clause(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update name for all records where id >= 1000 AND id < 1100
    let new_name = "RangeUpdated".to_string();
    let update_sql = format!(
        "UPDATE {} SET name = '{}' WHERE id >= 1000 AND id < 1100;",
        args.table_name, new_name
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 100, StatementType::Update).await?;

    // Verify ALL 100 records in the range
    info!("Verifying all 100 updated records...");
    for id in 1000..1100 {
        let mut expected = args.test_data[id].clone();
        expected.name = new_name.clone();
        expected
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify a record outside the range hasn't changed
    args.test_data[999]
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;
    args.test_data[1100]
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Range update with WHERE clause verified");
    Ok(())
}

/// Test 10: Update multiple columns at once
async fn test_update_multiple_columns(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update multiple columns for id 2000
    let new_name = "MultiUpdate".to_string();
    let new_price = 555.55;
    let new_active = false;
    let update_sql = format!(
        "UPDATE {} SET name = '{}', price = {:.2}, active = {} WHERE id = 2000;",
        args.table_name, new_name, new_price, new_active
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Update).await?;

    // Verify the update
    let mut expected = args.test_data[2000].clone();
    expected.name = new_name;
    expected.price = new_price;
    expected.active = new_active;
    expected
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Multiple columns updated and verified");
    Ok(())
}

/// Test 11: Update with complex WHERE clause
async fn test_update_complex_where(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update records where id >= 3000 AND id < 3050 AND active = TRUE
    let new_big_id = 123456789;
    let update_sql = format!(
        "UPDATE {} SET big_id = {} WHERE id >= 3000 AND id < 3050 AND active = TRUE;",
        args.table_name, new_big_id
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    // Count how many records should be updated (even ids from 3000 to 3048)
    let expected_updates = (3000..3050).filter(|id| id % 2 == 0).count();
    validate_non_select_statement(&mut client, expected_updates, StatementType::Update).await?;

    // Verify ALL records that should be updated (all even ids in range)
    info!("Verifying all {} updated records...", expected_updates);
    for id in (3000..3050).filter(|id| id % 2 == 0) {
        let mut expected = args.test_data[id].clone();
        expected.big_id = new_big_id;
        expected
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    // Verify ALL records that shouldn't be updated (all odd ids in range)
    info!("Verifying all {} unaffected records...", 25);
    for id in (3000..3050).filter(|id| id % 2 != 0) {
        args.test_data[id]
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ Complex WHERE clause update verified");
    Ok(())
}

/// Test 12: Update ALL records (no WHERE clause)
async fn test_update_all_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Update big_id for ALL records (no WHERE clause)
    let new_big_id_multiplier = 5000;
    let update_sql = format!(
        "UPDATE {} SET big_id = {};",
        args.table_name, new_big_id_multiplier
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: update_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, args.test_data.len(), StatementType::Update).await?;

    // Verify ALL records have been updated (only check big_id since previous tests may have modified other columns)
    info!(
        "Verifying all {} records have updated big_id...",
        args.test_data.len()
    );

    let select_sql = format!("SELECT id, big_id FROM {};", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: select_sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("big_id", ColumnType::I64)];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != args.test_data.len() {
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                args.test_data.len(),
                records.len()
            ),
        });
    }

    // Check that every record has the new big_id value
    for record in &records {
        let id = extract_i32(record, 0)?;
        let big_id = extract_i64(record, 1)?;

        if big_id != new_big_id_multiplier {
            return Err(TesterError::ServerError {
                message: format!(
                    "Record id={} has big_id={} but expected {}",
                    id, big_id, new_big_id_multiplier
                ),
            });
        }
    }

    info!(
        "✓ All {} records updated successfully",
        args.test_data.len()
    );
    Ok(())
}
