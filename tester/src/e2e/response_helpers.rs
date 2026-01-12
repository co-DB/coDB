use log::{error, info};
use protocol::{ColumnType, Field, Record, Response, StatementType};

use crate::{
    TesterError,
    client::{BinaryClient, ReadResult},
};

/// Helper to expect and validate an Acknowledge response
pub async fn expect_acknowledge(client: &mut BinaryClient) -> Result<(), TesterError> {
    match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected Acknowledge but got disconnected");
            Err(TesterError::Disconnected)
        }
        ReadResult::Response(Response::Acknowledge) => {
            info!("✓ Received Acknowledge");
            Ok(())
        }
        ReadResult::Response(Response::Error {
            message,
            error_type,
        }) => {
            error!(
                "Expected Acknowledge but got Error: {} ({:?})",
                message, error_type
            );
            Err(TesterError::ServerError { message })
        }
        ReadResult::Response(other) => {
            error!("Expected Acknowledge but got: {:?}", other);
            Err(TesterError::ServerError {
                message: format!("Expected Acknowledge but got: {:?}", other),
            })
        }
    }
}

/// Helper to expect and validate a ColumnInfo response
pub async fn expect_column_info(
    client: &mut BinaryClient,
    expected_columns: &[(&str, ColumnType)],
) -> Result<(), TesterError> {
    match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected ColumnInfo but got disconnected");
            Err(TesterError::Disconnected)
        }
        ReadResult::Response(Response::ColumnInfo { column_metadata }) => {
            if column_metadata.len() != expected_columns.len() {
                error!(
                    "Expected {} columns but got {}",
                    expected_columns.len(),
                    column_metadata.len()
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected {} columns but got {}",
                        expected_columns.len(),
                        column_metadata.len()
                    ),
                });
            }

            for (idx, (expected_name, expected_type)) in expected_columns.iter().enumerate() {
                let actual = &column_metadata[idx];
                if actual.name != *expected_name {
                    error!(
                        "Column {} name mismatch: expected '{}', got '{}'",
                        idx, expected_name, actual.name
                    );
                    return Err(TesterError::ServerError {
                        message: format!(
                            "Column {} name mismatch: expected '{}', got '{}'",
                            idx, expected_name, actual.name
                        ),
                    });
                }

                if !types_match(&actual.ty, expected_type) {
                    error!(
                        "Column {} ('{}') type mismatch: expected {:?}, got {:?}",
                        idx, expected_name, expected_type, actual.ty
                    );
                    return Err(TesterError::ServerError {
                        message: format!(
                            "Column {} ('{}') type mismatch: expected {:?}, got {:?}",
                            idx, expected_name, expected_type, actual.ty
                        ),
                    });
                }
            }

            info!(
                "✓ Received ColumnInfo with {} columns",
                column_metadata.len()
            );
            Ok(())
        }
        ReadResult::Response(Response::Error {
            message,
            error_type,
        }) => {
            error!(
                "Expected ColumnInfo but got Error: {} ({:?})",
                message, error_type
            );
            Err(TesterError::ServerError { message })
        }
        ReadResult::Response(other) => {
            error!("Expected ColumnInfo but got: {:?}", other);
            Err(TesterError::ServerError {
                message: format!("Expected ColumnInfo but got: {:?}", other),
            })
        }
    }
}

fn types_match(actual: &ColumnType, expected: &ColumnType) -> bool {
    matches!(
        (actual, expected),
        (ColumnType::String, ColumnType::String)
            | (ColumnType::F32, ColumnType::F32)
            | (ColumnType::F64, ColumnType::F64)
            | (ColumnType::I32, ColumnType::I32)
            | (ColumnType::I64, ColumnType::I64)
            | (ColumnType::Bool, ColumnType::Bool)
            | (ColumnType::Date, ColumnType::Date)
            | (ColumnType::DateTime, ColumnType::DateTime)
    )
}

/// Helper to collect all rows from a SELECT query
/// Returns the total number of rows collected
pub async fn collect_all_rows(client: &mut BinaryClient) -> Result<Vec<Record>, TesterError> {
    let mut all_records = Vec::new();

    loop {
        match client.read_response().await? {
            ReadResult::Disconnected => {
                error!("Connection lost while collecting rows");
                return Err(TesterError::Disconnected);
            }
            ReadResult::Response(Response::Rows { mut records, count }) => {
                info!("✓ Received batch of {} rows", count);
                all_records.append(&mut records);
            }
            ReadResult::Response(Response::StatementCompleted {
                rows_affected,
                statement_type,
            }) => {
                info!(
                    "✓ StatementCompleted: {} rows affected ({:?})",
                    rows_affected, statement_type
                );

                if all_records.len() != rows_affected {
                    error!(
                        "Row count mismatch: collected {} rows but statement says {} rows affected",
                        all_records.len(),
                        rows_affected
                    );
                    return Err(TesterError::ServerError {
                        message: format!(
                            "Row count mismatch: collected {} rows but statement says {} rows affected",
                            all_records.len(),
                            rows_affected
                        ),
                    });
                }

                return Ok(all_records);
            }
            ReadResult::Response(Response::Error {
                message,
                error_type,
            }) => {
                error!(
                    "Error while collecting rows: {} ({:?})",
                    message, error_type
                );
                return Err(TesterError::ServerError { message });
            }
            ReadResult::Response(other) => {
                error!("Unexpected response while collecting rows: {:?}", other);
                return Err(TesterError::ServerError {
                    message: format!("Unexpected response while collecting rows: {:?}", other),
                });
            }
        }
    }
}

/// Helper to expect a StatementCompleted response with specific parameters
pub async fn expect_statement_completed(
    client: &mut BinaryClient,
    expected_rows_affected: usize,
    expected_type: StatementType,
) -> Result<(), TesterError> {
    match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected StatementCompleted but got disconnected");
            Err(TesterError::Disconnected)
        }
        ReadResult::Response(Response::StatementCompleted {
            rows_affected,
            statement_type,
        }) => {
            if rows_affected != expected_rows_affected {
                error!(
                    "Expected {} rows affected but got {}",
                    expected_rows_affected, rows_affected
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected {} rows affected but got {}",
                        expected_rows_affected, rows_affected
                    ),
                });
            }

            // Check statement type matches (using debug format for comparison)
            let actual_type_str = format!("{:?}", statement_type);
            let expected_type_str = format!("{:?}", expected_type);
            if actual_type_str != expected_type_str {
                error!(
                    "Expected statement type {:?} but got {:?}",
                    expected_type, statement_type
                );
                return Err(TesterError::ServerError {
                    message: format!(
                        "Expected statement type {:?} but got {:?}",
                        expected_type, statement_type
                    ),
                });
            }

            info!(
                "✓ Received StatementCompleted: {} rows affected ({:?})",
                rows_affected, statement_type
            );
            Ok(())
        }
        ReadResult::Response(Response::Error {
            message,
            error_type,
        }) => {
            error!(
                "Expected StatementCompleted but got Error: {} ({:?})",
                message, error_type
            );
            Err(TesterError::ServerError { message })
        }
        ReadResult::Response(other) => {
            error!("Expected StatementCompleted but got: {:?}", other);
            Err(TesterError::ServerError {
                message: format!("Expected StatementCompleted but got: {:?}", other),
            })
        }
    }
}

/// Helper to expect a QueryCompleted response
pub async fn expect_query_completed(client: &mut BinaryClient) -> Result<(), TesterError> {
    match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected QueryCompleted but got disconnected");
            Err(TesterError::Disconnected)
        }
        ReadResult::Response(Response::QueryCompleted) => {
            info!("✓ Received QueryCompleted");
            Ok(())
        }
        ReadResult::Response(Response::Error {
            message,
            error_type,
        }) => {
            error!(
                "Expected QueryCompleted but got Error: {} ({:?})",
                message, error_type
            );
            Err(TesterError::ServerError { message })
        }
        ReadResult::Response(other) => {
            error!("Expected QueryCompleted but got: {:?}", other);
            Err(TesterError::ServerError {
                message: format!("Expected QueryCompleted but got: {:?}", other),
            })
        }
    }
}

/// Helper to validate a complete SELECT query flow
/// Returns the collected records
pub async fn validate_select_query(
    client: &mut BinaryClient,
    expected_columns: &[(&str, ColumnType)],
) -> Result<Vec<Record>, TesterError> {
    expect_acknowledge(client).await?;
    expect_column_info(client, expected_columns).await?;
    let records = collect_all_rows(client).await?;
    expect_query_completed(client).await?;
    Ok(records)
}

/// Helper to validate a non-SELECT statement (INSERT, CREATE, DELETE, etc.)
pub async fn validate_non_select_statement(
    client: &mut BinaryClient,
    expected_rows_affected: usize,
    statement_type: StatementType,
) -> Result<(), TesterError> {
    expect_acknowledge(client).await?;
    expect_statement_completed(client, expected_rows_affected, statement_type).await?;
    expect_query_completed(client).await?;
    Ok(())
}

/// Validate that a record has the expected number of fields
pub fn validate_field_count(record: &Record, expected_count: usize) -> Result<(), TesterError> {
    if record.fields.len() != expected_count {
        error!(
            "Expected {} fields but got {}",
            expected_count,
            record.fields.len()
        );
        return Err(TesterError::ServerError {
            message: format!(
                "Expected {} fields but got {}",
                expected_count,
                record.fields.len()
            ),
        });
    }
    Ok(())
}

/// Extract an i32 field from a record at the given index
pub fn extract_i32(record: &Record, index: usize) -> Result<i32, TesterError> {
    match &record.fields.get(index) {
        Some(Field::Int32(val)) => Ok(*val),
        Some(other) => {
            error!("Expected Int32 at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected Int32 at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Extract an i64 field from a record at the given index
pub fn extract_i64(record: &Record, index: usize) -> Result<i64, TesterError> {
    match &record.fields.get(index) {
        Some(Field::Int64(val)) => Ok(*val),
        Some(other) => {
            error!("Expected Int64 at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected Int64 at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Extract a string field from a record at the given index
pub fn extract_string(record: &Record, index: usize) -> Result<String, TesterError> {
    match &record.fields.get(index) {
        Some(Field::String(val)) => Ok(val.clone()),
        Some(other) => {
            error!("Expected String at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected String at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Extract a bool field from a record at the given index
pub fn extract_bool(record: &Record, index: usize) -> Result<bool, TesterError> {
    match &record.fields.get(index) {
        Some(Field::Bool(val)) => Ok(*val),
        Some(other) => {
            error!("Expected Bool at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected Bool at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Extract an f32 field from a record at the given index
pub fn extract_f32(record: &Record, index: usize) -> Result<f32, TesterError> {
    match &record.fields.get(index) {
        Some(Field::Float32(val)) => Ok(*val),
        Some(other) => {
            error!("Expected Float32 at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected Float32 at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Extract an f64 field from a record at the given index
pub fn extract_f64(record: &Record, index: usize) -> Result<f64, TesterError> {
    match &record.fields.get(index) {
        Some(Field::Float64(val)) => Ok(*val),
        Some(other) => {
            error!("Expected Float64 at index {} but got {:?}", index, other);
            Err(TesterError::ServerError {
                message: format!("Expected Float64 at index {} but got {:?}", index, other),
            })
        }
        None => {
            error!("No field at index {}", index);
            Err(TesterError::ServerError {
                message: format!("No field at index {}", index),
            })
        }
    }
}

/// Helper to expect an error response from the server
/// This properly handles the query flow: Acknowledge -> Error -> QueryCompleted
pub async fn expect_error(client: &mut BinaryClient) -> Result<String, TesterError> {
    // First, expect acknowledge
    expect_acknowledge(client).await?;

    // Then expect an error
    let error_message = match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected Error but got disconnected");
            return Err(TesterError::Disconnected);
        }
        ReadResult::Response(Response::Error { message, .. }) => {
            info!("✓ Received expected error: {}", message);
            message
        }
        ReadResult::Response(other) => {
            error!("Expected Error but got: {:?}", other);
            return Err(TesterError::ServerError {
                message: format!("Expected Error but got: {:?}", other),
            });
        }
    };

    // Finally, expect QueryCompleted even after error
    match client.read_response().await? {
        ReadResult::Disconnected => {
            error!("Expected QueryCompleted but got disconnected");
            Err(TesterError::Disconnected)
        }
        ReadResult::Response(Response::QueryCompleted) => {
            info!("✓ Received QueryCompleted after error");
            Ok(error_message)
        }
        ReadResult::Response(other) => {
            error!("Expected QueryCompleted but got: {:?}", other);
            Err(TesterError::ServerError {
                message: format!("Expected QueryCompleted after error but got: {:?}", other),
            })
        }
    }
}
