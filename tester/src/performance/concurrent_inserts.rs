use std::time::Instant;

use protocol::Request;

use crate::{
    TesterError,
    suite::{PerformanceTestResult, Suite, default_client},
};

pub struct ConcurrentInserts;

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
}

pub struct Test {
    pub num_of_threads: usize,
    pub records_per_thread: usize,
    pub table_name: String,
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite<PerformanceTestResult> for ConcurrentInserts {
    type SetupArgs = Setup;

    async fn setup(args: &Self::SetupArgs) -> Result<(), TesterError> {
        let mut client = default_client().await?;
        client
            .execute_and_wait(Request::CreateDatabase {
                database_name: args.database_name.clone(),
            })
            .await?;
        client
            .execute_and_wait(Request::Query {
                database_name: Some(args.database_name.clone()),
                sql: format!(
                    "CREATE TABLE {} (id INT32 PRIMARY_KEY, value INT32);",
                    args.table_name
                ),
            })
            .await?;
        Ok(())
    }

    type TestArgs = Test;

    async fn run(args: &Self::TestArgs) -> Result<PerformanceTestResult, TesterError> {
        let start = Instant::now();
        let mut handles = Vec::with_capacity(args.num_of_threads);
        for worker_id in 0..args.num_of_threads {
            let database = args.database_name.clone();
            let table = args.table_name.clone();

            let records_per_thread = args.records_per_thread;
            let handle = tokio::spawn(async move {
                let mut client = default_client().await?;

                for i in 0..records_per_thread {
                    let id = (worker_id * records_per_thread + i) as i32;
                    let sql = format!("INSERT INTO {} (id, value) VALUES ({}, {});", table, id, id);

                    client
                        .execute_and_wait(Request::Query {
                            database_name: Some(database.clone()),
                            sql,
                        })
                        .await?;
                }

                Ok::<(), TesterError>(())
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("worker task panicked")?;
        }

        let elapsed = start.elapsed();

        let test_result = PerformanceTestResult { duration: elapsed };
        Ok(test_result)
    }

    type CleanupArgs = Cleanup;

    async fn cleanup(args: &Self::CleanupArgs) -> Result<(), TesterError> {
        let mut client = default_client().await?;
        client
            .execute_and_wait(Request::DeleteDatabase {
                database_name: args.database_name.clone(),
            })
            .await?;
        Ok(())
    }
}
