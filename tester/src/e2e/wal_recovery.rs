use std::time::Duration;
use std::{cell::RefCell, thread};

use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    ServerProcess, TesterError,
    suite::{E2ETestResult, Suite, default_client},
};

use super::response_helpers::{
    extract_bool, extract_f32, extract_f64, extract_i32, extract_i64, extract_string,
    validate_field_count, validate_non_select_statement, validate_select_query,
};

/// Test record structure for WAL recovery tests
#[derive(Debug, Clone)]
pub struct WalTestRecord {
    pub id: i32,
    pub big_id: i64,
    pub price: f32,
    pub precise_price: f64,
    pub active: bool,
    pub birth_date: String,
    pub last_login: String,
    pub name: String,
}

impl WalTestRecord {
    /// Generate test records
    pub fn generate(num_records: usize) -> Vec<Self> {
        (0..num_records)
            .map(|i| WalTestRecord {
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

    /// Validate that a protocol Record matches this test record
    pub fn validate_record(&self, record: &protocol::Record) -> Result<(), TesterError> {
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

pub struct WalRecoveryE2ETest;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
}

pub struct Test {
    pub database_name: String,
    pub table_name: String,
    pub test_data: Vec<WalTestRecord>,
    pub server_path: String,
    pub server: ServerProcess,
}

impl Suite<E2ETestResult> for WalRecoveryE2ETest {
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

        info!("Creating table...");
        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: create_table_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 0, StatementType::CreateTable).await?;
        info!("✓ Table created");

        Ok(())
    }

    type TestArgs = RefCell<Test>;

    async fn run(args: &Self::TestArgs) -> Result<E2ETestResult, TesterError> {
        let mut tests_passed = 0;

        info!("\n=== Test 1: WAL recovery after SIGKILL ===");

        // Clone values to avoid holding RefCell borrow across await
        let (database_name, table_name, test_data) = {
            let borrowed = args.borrow();
            (
                borrowed.database_name.clone(),
                borrowed.table_name.clone(),
                borrowed.test_data.clone(),
            )
        };

        // Insert records
        if let Err(e) = insert_records(&database_name, &table_name, &test_data).await {
            error!("Insert failed: {:?}", e);
            return Err(e);
        }
        info!("✓ All {} records inserted", args.borrow().test_data.len());

        // Wait for WAL flush
        info!("Waiting 500ms for WAL flush...");
        thread::sleep(Duration::from_millis(500));
        info!("✓ WAL should be flushed");

        // Kill the current server
        args.borrow_mut().server.stop()?;
        info!("✓ Server killed");

        // Restart server
        info!("Restarting server for WAL recovery...");
        let mut server = ServerProcess::start(&args.borrow().server_path)?;

        // Wait for WAL recovery to complete
        info!("Waiting 3 seconds for WAL recovery...");
        thread::sleep(Duration::from_secs(3));
        info!("✓ WAL recovery period complete");

        // Verify all records are present after recovery
        if let Err(e) = verify_records_after_recovery(&database_name, &table_name, &test_data).await
        {
            error!("Verification failed: {:?}", e);
            return Err(e);
        }
        info!(
            "✓ All {} records verified after recovery",
            args.borrow().test_data.len()
        );
        tests_passed += 1;

        // We need to do cleanup here because we can't pass it to cleanup function

        info!("Deleting database '{}'...", &database_name);
        let mut client = default_client().await?;

        client
            .execute_and_wait(Request::DeleteDatabase {
                database_name: database_name.clone(),
            })
            .await?;

        info!("✓ Database deleted");

        server.stop()?;

        Ok(E2ETestResult { tests_passed })
    }

    type CleanupArgs = ();

    async fn cleanup(_: &Self::CleanupArgs) -> Result<(), TesterError> {
        Ok(())
    }
}

/// Test WAL recovery: Insert records
async fn insert_records(
    db_name: &str,
    table_name: &str,
    test_data: &[WalTestRecord],
) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    info!("Inserting {} records...", test_data.len());

    // Insert all records
    for record in test_data {
        let insert_sql = format!(
            "INSERT INTO {} (id, big_id, price, precise_price, active, birth_date, last_login, name) \
            VALUES ({}, {}, {:.1}, {:.2}, {}, '{}', '{}', '{}');",
            table_name,
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
                database_name: Some(db_name.to_string()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
    }

    info!("✓ All {} records inserted", test_data.len());

    Ok(())
}

/// Verify all records after server restart
pub async fn verify_records_after_recovery(
    database_name: &str,
    table_name: &str,
    expected_records: &[WalTestRecord],
) -> Result<(), TesterError> {
    info!(
        "Verifying {} records after recovery...",
        expected_records.len()
    );

    let mut client = default_client().await?;

    // Select all records ordered by id
    let sql = format!("SELECT * FROM {} ORDER BY id;", table_name);
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

    // Verify count
    if records.len() != expected_records.len() {
        error!(
            "Record count mismatch: expected {}, got {}",
            expected_records.len(),
            records.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Record count mismatch after recovery: expected {}, got {}",
                expected_records.len(),
                records.len()
            ),
        });
    }

    info!("✓ Record count matches: {}", records.len());

    // Verify each record
    for (i, (expected, actual)) in expected_records.iter().zip(records.iter()).enumerate() {
        if let Err(e) = expected.validate_record(actual) {
            error!("Record {} validation failed: {:?}", i, e);
            return Err(e);
        }
    }

    info!("✓ All {} records verified successfully", records.len());

    Ok(())
}
