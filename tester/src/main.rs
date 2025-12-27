use std::io;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rkyv::rancor::Error as RkyvError;
use thiserror::Error;

use crate::performance::concurrent_inserts::{self, ConcurrentInserts};
use crate::suite::TestResult;

mod client;
mod performance;
mod suite;

#[derive(Parser)]
#[command(name = "tester")]
#[command(about = "coDB tester client for e2e & performance tests", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Insert X records by Y threads concurrently
    ConcurrentInserts {
        /// How many times to run the test and average the time
        #[arg(long, default_value_t = 1)]
        runs: u32,

        /// Number of concurrent threads
        #[arg(long, default_value_t = 8)]
        threads: usize,

        /// Records per thread
        #[arg(long, default_value_t = 1000)]
        records_per_thread: usize,
    },
}

#[derive(Debug, Error)]
enum TesterError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("failed to serialize binary message: {0}")]
    BinarySerialization(#[from] RkyvError),

    #[error("failed to deserialize binary message: {0}")]
    BinaryDeserialization(RkyvError),

    #[error("server disconnected unexpectedly")]
    Disconnected,

    #[error("server returned error: {message}")]
    ServerError { message: String },
}

async fn concurrent_inserts(
    runs: u32,
    threads: usize,
    records_per_thread: usize,
) -> Result<Vec<TestResult>, TesterError> {
    let mut test_results = Vec::with_capacity(runs as _);
    let db_name = "CONCURRENT_INSERTS".to_string();
    let table_name = "CONCURRENT_INSERTS_TABLE".to_string();

    let setup = concurrent_inserts::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
    };

    let test = concurrent_inserts::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_of_threads: threads,
        records_per_thread,
    };

    let cleanup = concurrent_inserts::Cleanup {
        database_name: db_name.clone(),
    };

    let suite = ConcurrentInserts {
        setup,
        test,
        cleanup,
    };

    for _ in 0..runs {
        let result = suite.run_suite().await?;
        test_results.push(result);
    }
    Ok(test_results)
}

#[tokio::main]
async fn main() -> Result<(), TesterError> {
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Command::ConcurrentInserts {
            runs,
            threads,
            records_per_thread,
        } => {
            let test_results = concurrent_inserts(runs, threads, records_per_thread).await?;
            report_stats("concurrent-inserts", &test_results);
            Ok(())
        }
    }
}

fn report_stats(test_name: &str, results: &[TestResult]) {
    if results.is_empty() {
        println!("No runs executed for test '{}'.", test_name);
        return;
    }

    let total: Duration = results.iter().map(|r| &r.duration).copied().sum();
    let mean = total / results.len() as u32;

    println!("Test '{}':", test_name);
    println!("  Runs: {}", results.len());
    println!("  Mean time: {:.3?}", mean);

    for (i, d) in results.iter().map(|r| &r.duration).enumerate() {
        println!("  Run {:>3}: {:.3?}", i + 1, d);
    }
}
