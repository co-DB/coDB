use std::time::Instant;

use protocol::Request;

use crate::{
    TesterError,
    suite::{Suite, TestResult, default_client},
};

pub struct ReadByIndex {
    pub setup: Setup,
    pub test: Test,
    pub cleanup: Cleanup,
}

impl ReadByIndex {
    pub async fn run_suite(&self) -> Result<TestResult, TesterError> {
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
    pub bound_size: usize,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite for ReadByIndex {
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

        for i in 0..args.records_to_insert {
            let mut c = default_client().await?;
            let sql = format!(
                "INSERT INTO {} (id, value) VALUES ({}, {});",
                args.table_name, i as i32, i as i32
            );
            c.execute_and_wait(Request::Query {
                database_name: Some(args.database_name.clone()),
                sql,
            })
            .await?;
        }

        Ok(())
    }

    type TestArgs = Test;

    async fn run(&self, args: &Self::TestArgs) -> Result<TestResult, TesterError> {
        let start = Instant::now();

        let mut handles = Vec::with_capacity(args.num_of_threads);
        for worker_id in 0..args.num_of_threads {
            let database = args.database_name.clone();
            let table = args.table_name.clone();
            let bound_size = args.bound_size;
            let handle = tokio::spawn(async move {
                let mut client = default_client().await?;

                let start_bound = (worker_id * bound_size) as i32;
                let end_bound = start_bound + bound_size as i32;

                let sql = format!(
                    "SELECT * FROM {} WHERE id >= {} AND id <= {};",
                    table, start_bound, end_bound
                );

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
        Ok(TestResult { duration: elapsed })
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
