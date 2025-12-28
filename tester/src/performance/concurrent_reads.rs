use std::time::Instant;

use protocol::Request;

use crate::{
    TesterError,
    suite::{PerformanceTestResult, Suite, default_client},
};

pub struct ReadMany {
    pub setup: Setup,
    pub test: Test,
    pub cleanup: Cleanup,
}

impl ReadMany {
    pub async fn run_suite(&self) -> Result<PerformanceTestResult, TesterError> {
        self.setup(&self.setup).await?;
        let result = self.run(&self.test).await?;
        self.cleanup(&self.cleanup).await?;
        Ok(result)
    }
}

pub struct Setup {
    pub database_name: String,
    pub table_name: String,
    pub records_to_insert: usize,
}

pub struct Test {
    pub num_of_threads: usize,
    pub table_name: String,
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite<PerformanceTestResult> for ReadMany {
    type SetupArgs = Setup;

    async fn setup(&self, args: &Self::SetupArgs) -> Result<(), TesterError> {
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

        // Insert records on a single thread
        for i in 0..args.records_to_insert {
            let sql = format!(
                "INSERT INTO {} (id, value) VALUES ({}, {});",
                args.table_name, i as i32, i as i32
            );
            client
                .execute_and_wait(Request::Query {
                    database_name: Some(args.database_name.clone()),
                    sql,
                })
                .await?;
        }

        Ok(())
    }

    type TestArgs = Test;

    async fn run(&self, args: &Self::TestArgs) -> Result<PerformanceTestResult, TesterError> {
        let start = Instant::now();

        let mut handles = Vec::with_capacity(args.num_of_threads);
        for _ in 0..args.num_of_threads {
            let database = args.database_name.clone();
            let table = args.table_name.clone();

            let handle = tokio::spawn(async move {
                let mut client = default_client().await?;

                let sql = format!("SELECT * FROM {};", table);
                client
                    .execute_and_wait(Request::Query {
                        database_name: Some(database.clone()),
                        sql,
                    })
                    .await?;

                Ok::<(), TesterError>(())
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("worker task panicked")?;
        }

        let elapsed = start.elapsed();
        Ok(PerformanceTestResult { duration: elapsed })
    }

    type CleanupArgs = Cleanup;

    async fn cleanup(&self, args: &Self::CleanupArgs) -> Result<(), TesterError> {
        let mut client = default_client().await?;
        client
            .execute_and_wait(Request::DeleteDatabase {
                database_name: args.database_name.clone(),
            })
            .await?;
        Ok(())
    }
}
