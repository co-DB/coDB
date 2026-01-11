use log::{error, info};
use protocol::{ColumnType, Request, StatementType};

use crate::{
    TesterError,
    suite::{Suite, default_client},
};

use super::response_helpers::{
    extract_bool, extract_f32, extract_f64, extract_i32, extract_i64, extract_string,
    validate_field_count, validate_non_select_statement, validate_select_query,
};

/// Test record structure for INSERT tests
#[derive(Debug, Clone)]
pub struct InsertTestRecord {
    pub id: i32,
    pub big_id: i64,
    pub price: f32,
    pub precise_price: f64,
    pub active: bool,
    pub birth_date: String,
    pub last_login: String,
    pub name: String,
}

impl InsertTestRecord {
    /// Validate that this record exists in the database
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

pub struct InsertE2ETest;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
}

pub struct Test {
    pub database_name: String,
    pub table_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

pub struct E2ETestResult {
    pub tests_passed: usize,
}

impl Suite<E2ETestResult> for InsertE2ETest {
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

        Ok(())
    }

    type TestArgs = Test;

    async fn run(args: &Self::TestArgs) -> Result<E2ETestResult, TesterError> {
        let mut tests_passed = 0;

        // Test 1: Insert single record
        info!("\n=== Test 1: Insert single record ===");
        if let Err(e) = test_insert_single_record(args).await {
            error!("Test 1 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 1: Insert single record passed");
        tests_passed += 1;

        // Test 2: Insert 100 more records (cumulative: 101)
        info!("\n=== Test 2: Insert 100 more records ===");
        if let Err(e) = test_insert_100_records(args).await {
            error!("Test 2 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 2: Insert 100 more records passed");
        tests_passed += 1;

        // Test 3: Insert 1000 more records (cumulative: 1101)
        info!("\n=== Test 3: Insert 1000 more records ===");
        if let Err(e) = test_insert_1000_records(args).await {
            error!("Test 3 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 3: Insert 1000 more records passed");
        tests_passed += 1;

        // Test 4: Insert with different column order
        info!("\n=== Test 4: Insert with different column order ===");
        if let Err(e) = test_insert_different_column_order(args).await {
            error!("Test 4 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 4: Insert with different column order passed");
        tests_passed += 1;

        // Test 5: Insert with partial columns (omitting some)
        info!("\n=== Test 5: Insert with reversed column order ===");
        if let Err(e) = test_insert_reversed_column_order(args).await {
            error!("Test 5 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 5: Insert with reversed column order passed");
        tests_passed += 1;

        // Test 6: Insert with random column order
        info!("\n=== Test 6: Insert with random column order ===");
        if let Err(e) = test_insert_random_column_order(args).await {
            error!("Test 6 failed: {:?}", e);
            return Err(e);
        }
        info!("✓ Test 6: Insert with random column order passed");
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

/// Test 1: Insert single record and verify it's stored correctly
async fn test_insert_single_record(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let record = InsertTestRecord {
        id: 1,
        big_id: 1000,
        price: 15.5,
        precise_price: 27.75,
        active: true,
        birth_date: "2024-01-15".to_string(),
        last_login: "2024-01-15T10:30:00".to_string(),
        name: "FirstUser".to_string(),
    };

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

    // Verify the record was inserted correctly
    record
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Single record inserted and verified");
    Ok(())
}

/// Test 2: Insert 100 more records and verify all 101 exist
async fn test_insert_100_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;
    let mut inserted_records = Vec::new();

    // Insert 100 records (id 100-199)
    for i in 100..200 {
        let record = InsertTestRecord {
            id: i,
            big_id: (i as i64) * 1000,
            price: (i as f32) * 1.5,
            precise_price: (i as f64) * 2.75,
            active: i % 2 == 0,
            birth_date: format!("2024-01-{:02}", ((i % 28) + 1)),
            last_login: format!("2024-01-{:02}T12:00:00", ((i % 28) + 1)),
            name: format!("User_{}", i),
        };

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
        inserted_records.push(record);
    }

    info!("✓ 100 records inserted");

    // Verify total count is 101 (1 from test 1 + 100 from this test)
    let mut verify_client = default_client().await?;
    let count_sql = format!("SELECT * FROM {};", args.table_name);
    verify_client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: count_sql,
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

    let records = validate_select_query(&mut verify_client, &expected_columns).await?;

    if records.len() != 101 {
        error!("Expected 101 total records but got {}", records.len());
        return Err(TesterError::ServerError {
            message: format!("Expected 101 total records but got {}", records.len()),
        });
    }

    // Verify all inserted records
    for record in &inserted_records {
        record
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ All 101 records verified in database");
    Ok(())
}

/// Test 3: Insert 1000 more records and verify all 1101 exist
async fn test_insert_1000_records(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;
    let mut inserted_records = Vec::new();

    // Insert 1000 records (id 1000-1999)
    for i in 1000..2000 {
        let record = InsertTestRecord {
            id: i,
            big_id: (i as i64) * 1000,
            price: (i as f32) * 1.5,
            precise_price: (i as f64) * 2.75,
            active: i % 2 == 0,
            birth_date: format!("2024-01-{:02}", ((i % 28) + 1)),
            last_login: format!("2024-01-{:02}T12:00:00", ((i % 28) + 1)),
            name: format!("User_{}", i),
        };

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
        inserted_records.push(record);
    }

    info!("✓ 1000 records inserted");

    // Verify total count is 1101
    let mut verify_client = default_client().await?;
    let count_sql = format!("SELECT * FROM {};", args.table_name);
    verify_client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: count_sql,
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

    let records = validate_select_query(&mut verify_client, &expected_columns).await?;

    if records.len() != 1101 {
        error!("Expected 1101 total records but got {}", records.len());
        return Err(TesterError::ServerError {
            message: format!("Expected 1101 total records but got {}", records.len()),
        });
    }

    // Verify all inserted records
    for record in &inserted_records {
        record
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ All 1101 records verified in database");
    Ok(())
}

/// Test 4: Insert with different column order (swap some columns)
async fn test_insert_different_column_order(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let record = InsertTestRecord {
        id: 5000,
        big_id: 5000000,
        price: 99.9,
        precise_price: 199.99,
        active: false,
        birth_date: "2024-06-15".to_string(),
        last_login: "2024-06-15T14:30:00".to_string(),
        name: "OrderTest1".to_string(),
    };

    // Different order: name, active, id, price, big_id, precise_price, birth_date, last_login
    let insert_sql = format!(
        "INSERT INTO {} (name, active, id, price, big_id, precise_price, birth_date, last_login) \
        VALUES ('{}', {}, {}, {:.1}, {}, {:.2}, '{}', '{}');",
        args.table_name,
        record.name,
        record.active,
        record.id,
        record.price,
        record.big_id,
        record.precise_price,
        record.birth_date,
        record.last_login
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: insert_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;

    // Verify the record was inserted correctly
    record
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Record with different column order inserted and verified");
    Ok(())
}

/// Test 5: Insert with completely reversed column order
async fn test_insert_reversed_column_order(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;

    let record = InsertTestRecord {
        id: 5001,
        big_id: 5001000,
        price: 88.8,
        precise_price: 188.88,
        active: true,
        birth_date: "2024-07-20".to_string(),
        last_login: "2024-07-20T16:45:00".to_string(),
        name: "OrderTest2".to_string(),
    };

    // Reversed order: name, last_login, birth_date, active, precise_price, price, big_id, id
    let insert_sql = format!(
        "INSERT INTO {} (name, last_login, birth_date, active, precise_price, price, big_id, id) \
        VALUES ('{}', '{}', '{}', {}, {:.2}, {:.1}, {}, {});",
        args.table_name,
        record.name,
        record.last_login,
        record.birth_date,
        record.active,
        record.precise_price,
        record.price,
        record.big_id,
        record.id
    );

    client
        .send_request(&Request::Query {
            database_name: Some(args.database_name.clone()),
            sql: insert_sql,
        })
        .await?;

    validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;

    // Verify the record was inserted correctly
    record
        .verify_in_db(&args.database_name, &args.table_name)
        .await?;

    info!("✓ Record with reversed column order inserted and verified");
    Ok(())
}

/// Test 6: Insert with random column order (multiple records)
async fn test_insert_random_column_order(args: &Test) -> Result<(), TesterError> {
    let mut client = default_client().await?;
    let mut inserted_records = Vec::new();

    // Insert 10 records with different column orders
    for i in 6000..6010 {
        let record = InsertTestRecord {
            id: i,
            big_id: (i as i64) * 1000,
            price: (i as f32) * 0.5,
            precise_price: (i as f64) * 0.75,
            active: i % 3 == 0,
            birth_date: format!("2024-{:02}-15", ((i % 12) + 1)),
            last_login: format!("2024-{:02}-15T10:00:00", ((i % 12) + 1)),
            name: format!("RandomOrder_{}", i),
        };

        // Cycle through different column orders
        let insert_sql = match i % 3 {
            0 => {
                // Order 1: id, name, price, active, big_id, precise_price, birth_date, last_login
                format!(
                    "INSERT INTO {} (id, name, price, active, big_id, precise_price, birth_date, last_login) \
                    VALUES ({}, '{}', {:.1}, {}, {}, {:.2}, '{}', '{}');",
                    args.table_name,
                    record.id,
                    record.name,
                    record.price,
                    record.active,
                    record.big_id,
                    record.precise_price,
                    record.birth_date,
                    record.last_login
                )
            }
            1 => {
                // Order 2: active, birth_date, id, last_login, name, big_id, price, precise_price
                format!(
                    "INSERT INTO {} (active, birth_date, id, last_login, name, big_id, price, precise_price) \
                    VALUES ({}, '{}', {}, '{}', '{}', {}, {:.1}, {:.2});",
                    args.table_name,
                    record.active,
                    record.birth_date,
                    record.id,
                    record.last_login,
                    record.name,
                    record.big_id,
                    record.price,
                    record.precise_price
                )
            }
            _ => {
                // Order 3: big_id, precise_price, price, name, last_login, birth_date, active, id
                format!(
                    "INSERT INTO {} (big_id, precise_price, price, name, last_login, birth_date, active, id) \
                    VALUES ({}, {:.2}, {:.1}, '{}', '{}', '{}', {}, {});",
                    args.table_name,
                    record.big_id,
                    record.precise_price,
                    record.price,
                    record.name,
                    record.last_login,
                    record.birth_date,
                    record.active,
                    record.id
                )
            }
        };

        client
            .send_request(&Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: insert_sql,
            })
            .await?;

        validate_non_select_statement(&mut client, 1, StatementType::Insert).await?;
        inserted_records.push(record);
    }

    info!("✓ 10 records with random column orders inserted");

    // Verify all records
    for record in &inserted_records {
        record
            .verify_in_db(&args.database_name, &args.table_name)
            .await?;
    }

    info!("✓ All records with random column orders verified");
    Ok(())
}
