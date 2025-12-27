use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use protocol::Request;

use crate::{
    TesterError,
    suite::{Suite, TestResult, default_client},
};

pub struct ConcurrentReadsAndInserts {
    pub setup: Setup,
    pub test: Test,
    pub cleanup: Cleanup,
}

impl ConcurrentReadsAndInserts {
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
}

pub struct Test {
    pub num_of_readers: usize,
    pub num_of_writers: usize,
    pub records_per_writer: usize,
    pub table_name: String,
    pub database_name: String,
}

pub struct Cleanup {
    pub database_name: String,
}

impl Suite for ConcurrentReadsAndInserts {
    type SetupArgs = Setup;

    async fn setup(&self, args: &Self::SetupArgs) -> Result<(), TesterError> {
        let mut client = default_client().await?;
        // create database and table
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

    async fn run(&self, args: &Self::TestArgs) -> Result<TestResult, TesterError> {
        let writers_remaining = Arc::new(AtomicUsize::new(args.num_of_writers));

        let start = Instant::now();

        // spawn writer tasks
        let mut writer_handles = Vec::with_capacity(args.num_of_writers);
        for writer_id in 0..args.num_of_writers {
            let db = args.database_name.clone();
            let table = args.table_name.clone();
            let records = args.records_per_writer;
            let writers_remaining = writers_remaining.clone();

            let handle = tokio::spawn(async move {
                let mut client = default_client().await?;
                for i in 0..records {
                    let id = (writer_id * records + i) as i32;
                    let sql = format!("INSERT INTO {} (id, value) VALUES ({}, {});", table, id, id);
                    client
                        .execute_and_wait(Request::Query {
                            database_name: Some(db.clone()),
                            sql,
                        })
                        .await?;
                }

                writers_remaining.fetch_sub(1, Ordering::SeqCst);
                Ok::<(), TesterError>(())
            });

            writer_handles.push(handle);
        }

        // spawn reader tasks that continuously SELECT * until writers finish
        let mut reader_handles = Vec::with_capacity(args.num_of_readers);
        for _ in 0..args.num_of_readers {
            let db = args.database_name.clone();
            let table = args.table_name.clone();
            let writers_remaining = writers_remaining.clone();

            let handle = tokio::spawn(async move {
                let mut client = default_client().await?;
                loop {
                    let sql = format!("SELECT * FROM {};", table);
                    client
                        .execute_and_wait(Request::Query {
                            database_name: Some(db.clone()),
                            sql,
                        })
                        .await?;

                    if writers_remaining.load(Ordering::SeqCst) == 0 {
                        break;
                    }
                }
                Ok::<(), TesterError>(())
            });

            reader_handles.push(handle);
        }

        // wait for writers and readers to finish
        for h in writer_handles {
            h.await.expect("writer task panicked")?;
        }

        for h in reader_handles {
            h.await.expect("reader task panicked")?;
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
