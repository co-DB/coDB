use crate::{
    TesterError,
    client::ReadResult,
    suite::{E2eTestResult, Suite, default_client},
};
use protocol::{Request, Response, StatementType};

pub struct CreateTableTest;

impl CreateTableTest {
    pub async fn run_suite(&self) -> Result<E2eTestResult, TesterError> {
        self.setup(&()).await?;
        let result = self.run(&()).await?;
        self.cleanup(&()).await?;
        Ok(result)
    }
}

#[allow(unused_variables)]
impl Suite<E2eTestResult> for CreateTableTest {
    type SetupArgs = ();

    async fn setup(&self, _args: &Self::SetupArgs) -> Result<(), TesterError> {
        let db_name = "E2E_CREATE_TABLE".to_string();
        let mut client = default_client().await?;

        client
            .send_request(&Request::CreateDatabase {
                database_name: db_name.clone(),
            })
            .await?;

        loop {
            match client.read_response().await? {
                ReadResult::Disconnected => return Err(TesterError::Disconnected),
                ReadResult::Response(resp) => match resp {
                    Response::DatabaseCreated { database_name } => {
                        if database_name == db_name {
                            return Ok(());
                        } else {
                            return Err(TesterError::ServerError {
                                message: format!("unexpected database created: {}", database_name),
                            });
                        }
                    }
                    Response::Error { message, .. } => {
                        return Err(TesterError::ServerError { message });
                    }
                    _ => continue,
                },
            }
        }
    }

    type TestArgs = ();

    async fn run(&self, _args: &Self::TestArgs) -> Result<E2eTestResult, TesterError> {
        let db_name = "E2E_CREATE_TABLE".to_string();
        let table_name = "E2E_CREATE_TABLE_TABLE".to_string();

        let mut client = default_client().await?;

        let create_sql = format!(
            "CREATE TABLE {} (id INT32 PRIMARY_KEY, value INT32);",
            table_name
        );
        client
            .send_request(&Request::Query {
                database_name: Some(db_name.clone()),
                sql: create_sql,
            })
            .await?;

        // Acknowledge
        match client.read_response().await? {
            ReadResult::Disconnected => {
                return Ok(E2eTestResult {
                    error: Some("disconnected while waiting for Acknowledge".into()),
                });
            }
            ReadResult::Response(resp) => match resp {
                Response::Acknowledge => {}
                Response::Error { message, .. } => {
                    return Ok(E2eTestResult {
                        error: Some(message),
                    });
                }
                other => {
                    return Ok(E2eTestResult {
                        error: Some(format!(
                            "unexpected response instead of Acknowledge: {:?}",
                            other
                        )),
                    });
                }
            },
        }

        // CreateTable statement
        match client.read_response().await? {
            ReadResult::Disconnected => {
                return Ok(E2eTestResult {
                    error: Some("disconnected while waiting for StatementCompleted".into()),
                });
            }
            ReadResult::Response(resp) => match resp {
                Response::StatementCompleted {
                    rows_affected,
                    statement_type,
                } => {
                    if !matches!(statement_type, StatementType::CreateTable) {
                        return Ok(E2eTestResult {
                            error: Some(format!("unexpected statement type: {:?}", statement_type)),
                        });
                    }
                    if rows_affected != 0 {
                        return Ok(E2eTestResult {
                            error: Some(format!(
                                "unexpected rows_affected for CREATE TABLE: {}",
                                rows_affected
                            )),
                        });
                    }
                }
                Response::Error { message, .. } => {
                    return Ok(E2eTestResult {
                        error: Some(message),
                    });
                }
                other => {
                    return Ok(E2eTestResult {
                        error: Some(format!(
                            "unexpected response instead of StatementCompleted: {:?}",
                            other
                        )),
                    });
                }
            },
        }

        // QueryCompleted
        match client.read_response().await? {
            ReadResult::Disconnected => {
                return Ok(E2eTestResult {
                    error: Some("disconnected while waiting for QueryCompleted".into()),
                });
            }
            ReadResult::Response(resp) => match resp {
                Response::QueryCompleted => {}
                Response::Error { message, .. } => {
                    return Ok(E2eTestResult {
                        error: Some(message),
                    });
                }
                other => {
                    return Ok(E2eTestResult {
                        error: Some(format!(
                            "unexpected response instead of QueryCompleted: {:?}",
                            other
                        )),
                    });
                }
            },
        }

        Ok(E2eTestResult { error: None })
    }

    type CleanupArgs = ();

    async fn cleanup(&self, _args: &Self::CleanupArgs) -> Result<(), TesterError> {
        let db_name = "E2E_CREATE_TABLE".to_string();
        let mut client = default_client().await?;

        client
            .send_request(&Request::DeleteDatabase {
                database_name: db_name.clone(),
            })
            .await?;

        loop {
            match client.read_response().await? {
                ReadResult::Disconnected => return Err(TesterError::Disconnected),
                ReadResult::Response(resp) => match resp {
                    Response::DatabaseDeleted { database_name } => {
                        if database_name == db_name {
                            return Ok(());
                        } else {
                            return Err(TesterError::ServerError {
                                message: format!("unexpected database deleted: {}", database_name),
                            });
                        }
                    }
                    Response::Error { message, .. } => {
                        return Err(TesterError::ServerError { message });
                    }
                    _ => continue,
                },
            }
        }
    }
}
