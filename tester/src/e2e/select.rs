use std::collections::HashMap;

use log::{error, info};
use protocol::{ColumnType, Record, Request, StatementType};

use crate::{
    TesterError,
    suite::{E2ETestResult, Suite, default_client},
};

use super::response_helpers::{
    extract_bool, extract_f32, extract_f64, extract_i32, extract_i64, extract_string,
    validate_field_count, validate_non_select_statement, validate_select_query,
};

/// Test record structure matching the table schema
#[derive(Debug, Clone)]
pub struct TestRecord {
    pub id: i32,
    pub big_id: i64,
    pub price: f32,
    pub precise_price: f64,
    pub active: bool,
    pub birth_date: String, // Format: YYYY-MM-DD
    pub last_login: String, // Format: YYYY-MM-DDTHH:MM:SS
    pub name: String,
}

impl TestRecord {
    /// Generate test records
    pub fn generate(num_records: usize) -> Vec<Self> {
        (0..num_records)
            .map(|i| TestRecord {
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

    /// Validate that a protocol Record matches this test record (all 8 fields)
    pub fn validate_full_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 8)?;

        let id = extract_i32(record, 0)?;
        let big_id = extract_i64(record, 1)?;
        let price = extract_f32(record, 2)?;
        let precise_price = extract_f64(record, 3)?;
        let active = extract_bool(record, 4)?;
        // Skip date/datetime validation for now (fields 5, 6)
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

    /// Validate subset record (id, name, price)
    pub fn validate_subset_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 3)?;

        let id = extract_i32(record, 0)?;
        let name = extract_string(record, 1)?;
        let price = extract_f32(record, 2)?;

        if id != self.id {
            return Err(TesterError::ServerError {
                message: format!("ID mismatch: expected {}, got {}", self.id, id),
            });
        }
        if name != self.name {
            return Err(TesterError::ServerError {
                message: format!("name mismatch: expected '{}', got '{}'", self.name, name),
            });
        }
        if (price - self.price).abs() > 0.01 {
            return Err(TesterError::ServerError {
                message: format!("price mismatch: expected {}, got {}", self.price, price),
            });
        }

        Ok(())
    }

    /// Validate id and name only
    pub fn validate_id_name_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 2)?;

        let id = extract_i32(record, 0)?;
        let name = extract_string(record, 1)?;

        if id != self.id {
            return Err(TesterError::ServerError {
                message: format!("ID mismatch: expected {}, got {}", self.id, id),
            });
        }
        if name != self.name {
            return Err(TesterError::ServerError {
                message: format!("name mismatch: expected '{}', got '{}'", self.name, name),
            });
        }

        Ok(())
    }

    /// Validate id and big_id only
    pub fn validate_id_bigid_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 2)?;

        let id = extract_i32(record, 0)?;
        let big_id = extract_i64(record, 1)?;

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

        Ok(())
    }

    /// Validate record with id and active
    pub fn validate_id_active_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 2)?;

        let id = extract_i32(record, 0)?;
        let active = extract_bool(record, 1)?;

        if id != self.id {
            return Err(TesterError::ServerError {
                message: format!("ID mismatch: expected {}, got {}", self.id, id),
            });
        }
        if active != self.active {
            return Err(TesterError::ServerError {
                message: format!("active mismatch: expected {}, got {}", self.active, active),
            });
        }

        Ok(())
    }

    /// Validate record with id, name, price, active
    pub fn validate_complex_record(&self, record: &Record) -> Result<(), TesterError> {
        validate_field_count(record, 4)?;

        let id = extract_i32(record, 0)?;
        let name = extract_string(record, 1)?;
        let price = extract_f32(record, 2)?;
        let active = extract_bool(record, 3)?;

        if id != self.id {
            return Err(TesterError::ServerError {
                message: format!("ID mismatch: expected {}, got {}", self.id, id),
            });
        }
        if name != self.name {
            return Err(TesterError::ServerError {
                message: format!("name mismatch: expected '{}', got '{}'", self.name, name),
            });
        }
        if (price - self.price).abs() > 0.01 {
            return Err(TesterError::ServerError {
                message: format!("price mismatch: expected {}, got {}", self.price, price),
            });
        }
        if active != self.active {
            return Err(TesterError::ServerError {
                message: format!("active mismatch: expected {}, got {}", self.active, active),
            });
        }

        Ok(())
    }
}

pub struct SelectE2ETest;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
    pub num_records: usize,
}

pub struct Test {
    pub database_name: String,
    pub table_name: String,
    pub test_data: Vec<TestRecord>,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite<E2ETestResult> for SelectE2ETest {
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

        // Generate test data
        let test_data = TestRecord::generate(args.num_records);

        // Insert test data
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

        // Test 1: SELECT * FROM table
        info!("\n=== Test 1: SELECT * ===");
        if let Err(e) = test_select_all(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: SELECT * passed");
        tests_passed += 1;

        // Test 2: SELECT (subset of columns)
        info!("\n=== Test 2: SELECT subset of columns ===");
        if let Err(e) = test_select_subset(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: SELECT subset passed");
        tests_passed += 1;

        // Test 3: SELECT with ORDER BY
        info!("\n=== Test 3: SELECT with ORDER BY ===");
        if let Err(e) = test_select_order_by(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: SELECT with ORDER BY passed");
        tests_passed += 1;

        // Test 4: SELECT with ORDER BY + LIMIT
        info!("\n=== Test 4: SELECT with ORDER BY + LIMIT ===");
        if let Err(e) = test_select_order_by_limit(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: SELECT with ORDER BY + LIMIT passed");
        tests_passed += 1;

        // Test 5: SELECT with WHERE clause
        info!("\n=== Test 5: SELECT with WHERE clause ===");
        if let Err(e) = test_select_where(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: SELECT with WHERE passed");
        tests_passed += 1;

        // Test 6: SELECT with ORDER BY + OFFSET
        info!("\n=== Test 6: SELECT with ORDER BY + OFFSET ===");
        if let Err(e) = test_select_order_by_offset(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: SELECT with ORDER BY + OFFSET passed");
        tests_passed += 1;

        // Test 7: SELECT with everything (WHERE + ORDER BY + OFFSET + LIMIT)
        info!("\n=== Test 7: SELECT with WHERE + ORDER BY + OFFSET + LIMIT ===");
        if let Err(e) = test_select_everything(args).await {
            error!("Test 7 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 7: SELECT with WHERE + ORDER BY + OFFSET + LIMIT passed");
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

/// Test 1: SELECT * FROM table - should return all records with all columns
async fn test_select_all(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let sql = format!("SELECT * FROM {};", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
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

    // Validate we got all records
    if records.len() != args.test_data.len() {
        error!(
            "Expected {} records but got {}",
            args.test_data.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                args.test_data.len(),
                records.len()
            ),
        });
    }

    // Build a map of returned records by id for easy lookup
    let mut returned_by_id = HashMap::new();
    for record in &records {
        let id = extract_i32(record, 0)?;
        returned_by_id.insert(id, record);
    }

    // Validate each expected record is present and correct
    for expected in &args.test_data {
        match returned_by_id.get(&expected.id) {
            Some(actual) => expected.validate_full_record(actual)?,
            None => {
                error!(
                    "Expected record with id={} not found in results",
                    expected.id
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected record with id={} not found in results",
                        expected.id
                    ),
                });
            }
        }
    }

    info!(
        "✓ All {} records retrieved and validated with correct values",
        records.len()
    );
    Ok(())
}

/// Test 2: SELECT subset of columns
async fn test_select_subset(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let sql = format!("SELECT id, name, price FROM {};", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("name", ColumnType::String),
        ("price", ColumnType::F32),
    ];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != args.test_data.len() {
        error!(
            "Expected {} records but got {}",
            args.test_data.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                args.test_data.len(),
                records.len()
            ),
        });
    }

    // Build a map of returned records by id
    let mut returned_by_id = HashMap::new();
    for record in &records {
        let id = extract_i32(record, 0)?;
        returned_by_id.insert(id, record);
    }

    // Validate each expected record
    for expected in &args.test_data {
        match returned_by_id.get(&expected.id) {
            Some(actual) => expected.validate_subset_record(actual)?,
            None => {
                error!(
                    "Expected record with id={} not found in results",
                    expected.id
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected record with id={} not found in results",
                        expected.id
                    ),
                });
            }
        }
    }

    info!(
        "✓ Subset query returned {} records with validated values",
        records.len()
    );
    Ok(())
}

/// Test 3: SELECT with ORDER BY
async fn test_select_order_by(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // Order by id descending
    let sql = format!("SELECT id, name FROM {} ORDER BY id DESC;", args.table_name);
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("name", ColumnType::String)];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != args.test_data.len() {
        error!(
            "Expected {} records but got {}",
            args.test_data.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                args.test_data.len(),
                records.len()
            ),
        });
    }

    // Create expected order (DESC by id)
    let mut expected_order = args.test_data.clone();
    expected_order.sort_by(|a, b| b.id.cmp(&a.id));

    // Validate ordering and values
    for (idx, (expected, actual)) in expected_order.iter().zip(records.iter()).enumerate() {
        expected
            .validate_id_name_record(actual)
            .map_err(|e| TesterError::ServerError {
                message: format!("Record {} mismatch: {}", idx, e),
            })?;
    }

    let first_id = extract_i32(&records[0], 0)?;
    let last_id = extract_i32(&records[records.len() - 1], 0)?;

    info!(
        "✓ Records correctly ordered DESC and validated (first_id={}, last_id={})",
        first_id, last_id
    );
    Ok(())
}

/// Test 4: SELECT with ORDER BY + LIMIT
async fn test_select_order_by_limit(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let limit = 100;
    let sql = format!(
        "SELECT id, name FROM {} ORDER BY id ASC LIMIT {};",
        args.table_name, limit
    );
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("name", ColumnType::String)];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    if records.len() != limit {
        error!("Expected {} records but got {}", limit, records.len());
        return Err(TesterError::ServerError {
            message: format!("Expected {} records but got {}", limit, records.len()),
        });
    }

    // Create expected order (first 100 records, ASC by id)
    let mut expected_order = args.test_data.clone();
    expected_order.sort_by(|a, b| a.id.cmp(&b.id));
    let expected_order: Vec<_> = expected_order.into_iter().take(limit).collect();

    // Validate ordering and values
    for (idx, (expected, actual)) in expected_order.iter().zip(records.iter()).enumerate() {
        expected
            .validate_id_name_record(actual)
            .map_err(|e| TesterError::ServerError {
                message: format!("Record {} mismatch: {}", idx, e),
            })?;
    }

    let first_id = extract_i32(&records[0], 0)?;
    let last_id = extract_i32(&records[records.len() - 1], 0)?;

    info!(
        "✓ LIMIT correctly returned {} validated records (id {} to {})",
        limit, first_id, last_id
    );
    Ok(())
}

/// Test 5: SELECT with WHERE clause
async fn test_select_where(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    // WHERE id >= 100 AND id < 200
    let sql = format!(
        "SELECT id, active FROM {} WHERE id >= 100 AND id < 200;",
        args.table_name
    );
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("active", ColumnType::Bool)];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    // Filter expected records
    let expected_filtered: Vec<_> = args
        .test_data
        .iter()
        .filter(|r| r.id >= 100 && r.id < 200)
        .collect();

    if records.len() != expected_filtered.len() {
        error!(
            "Expected {} records but got {}",
            expected_filtered.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                expected_filtered.len(),
                records.len()
            ),
        });
    }

    // Build a map of returned records by id
    let mut returned_by_id = HashMap::new();
    for record in &records {
        let id = extract_i32(record, 0)?;
        returned_by_id.insert(id, record);
    }

    // Validate each expected record
    for expected in &expected_filtered {
        match returned_by_id.get(&expected.id) {
            Some(actual) => expected.validate_id_active_record(actual)?,
            None => {
                error!(
                    "Expected record with id={} not found in results",
                    expected.id
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected record with id={} not found in results",
                        expected.id
                    ),
                });
            }
        }
    }

    info!(
        "✓ WHERE clause correctly filtered and validated {} records",
        records.len()
    );
    Ok(())
}

/// Test 6: SELECT with ORDER BY + OFFSET
async fn test_select_order_by_offset(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let offset = 50;
    let sql = format!(
        "SELECT id, big_id FROM {} ORDER BY id ASC OFFSET {};",
        args.table_name, offset
    );
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![("id", ColumnType::I32), ("big_id", ColumnType::I64)];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    // Create expected order (ASC by id, skip first 50)
    let mut expected_order = args.test_data.clone();
    expected_order.sort_by(|a, b| a.id.cmp(&b.id));
    let expected_order: Vec<_> = expected_order.into_iter().skip(offset).collect();

    if records.len() != expected_order.len() {
        error!(
            "Expected {} records but got {}",
            expected_order.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                expected_order.len(),
                records.len()
            ),
        });
    }

    // Validate ordering and values
    for (idx, (expected, actual)) in expected_order.iter().zip(records.iter()).enumerate() {
        expected
            .validate_id_bigid_record(actual)
            .map_err(|e| TesterError::ServerError {
                message: format!("Record {} mismatch: {}", idx, e),
            })?;
    }

    if let Some(first_record) = records.first() {
        let first_id = extract_i32(first_record, 0)?;
        info!(
            "✓ OFFSET correctly skipped first {} records and validated (first_id={})",
            offset, first_id
        );
    }

    Ok(())
}

/// Test 7: SELECT with WHERE + ORDER BY + OFFSET + LIMIT
async fn test_select_everything(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let limit = 20;
    let offset = 10;

    // WHERE id >= 100, ORDER BY id DESC, OFFSET 10, LIMIT 20
    let sql = format!(
        "SELECT id, name, price, active FROM {} WHERE id >= 100 ORDER BY id DESC OFFSET {} LIMIT {};",
        args.table_name, offset, limit
    );
    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql,
        })
        .await?;

    let expected_columns = vec![
        ("id", ColumnType::I32),
        ("name", ColumnType::String),
        ("price", ColumnType::F32),
        ("active", ColumnType::Bool),
    ];

    let records = validate_select_query(&mut client, &expected_columns).await?;

    // Create expected order: filter WHERE id >= 100, ORDER BY id DESC, OFFSET 10, LIMIT 20
    let mut expected_order: Vec<_> = args
        .test_data
        .iter()
        .filter(|r| r.id >= 100)
        .cloned()
        .collect();
    expected_order.sort_by(|a, b| b.id.cmp(&a.id)); // DESC
    let expected_order: Vec<_> = expected_order
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();

    if records.len() != expected_order.len() {
        error!(
            "Expected {} records but got {}",
            expected_order.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} records but got {}",
                expected_order.len(),
                records.len()
            ),
        });
    }

    // Validate each record matches expected
    for (idx, (expected, actual)) in expected_order.iter().zip(records.iter()).enumerate() {
        expected
            .validate_complex_record(actual)
            .map_err(|e| TesterError::ServerError {
                message: format!("Record {} mismatch: {}", idx, e),
            })?;
    }

    info!(
        "✓ Complex query (WHERE + ORDER BY + OFFSET + LIMIT) returned {} validated records",
        records.len()
    );
    Ok(())
}
