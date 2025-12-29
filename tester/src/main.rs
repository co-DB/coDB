use std::io;
use std::time::Duration;

use clap::{Parser, Subcommand};
use rkyv::rancor::Error as RkyvError;
use thiserror::Error;

use crate::performance::concurrent_inserts::{self, ConcurrentInserts};
use crate::performance::concurrent_reads::{self, ReadMany};
use crate::performance::concurrent_reads_and_inserts::{self, ConcurrentReadsAndInserts};
use crate::performance::concurrent_reads_non_index::{self, ReadByNonIndex};
use crate::performance::concurrent_reads_with_index::{self, ReadByIndex};
use crate::suite::{PerformanceTestResult, Suite};

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
#[allow(clippy::enum_variant_names)]
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
        records: usize,
    },

    /// Read all records by X threads concurrently
    ConcurrentReads {
        /// How many times to run the test and average the time
        #[arg(long, default_value_t = 1)]
        runs: u32,

        /// Number of concurrent threads
        #[arg(long, default_value_t = 8)]
        threads: usize,

        /// How many records to insert
        #[arg(long, default_value_t = 1000)]
        records: usize,
    },

    /// Read part of the records with filter that uses only index (primary key)
    ConcurrentReadsIndex {
        /// How many times to run the test and average the time
        #[arg(long, default_value_t = 1)]
        runs: u32,

        /// Number of concurrent threads
        #[arg(long, default_value_t = 8)]
        threads: usize,

        /// How many records to insert
        #[arg(long, default_value_t = 1000)]
        records: usize,

        /// Size of the id range used for filtering (upper - lower)
        #[arg(long, default_value_t = 10)]
        bound_size: usize,
    },

    /// Read part of the records with filter that uses non-index column (`value`)
    ConcurrentReadsNonIndex {
        /// How many times to run the test and average the time
        #[arg(long, default_value_t = 1)]
        runs: u32,

        /// Number of concurrent threads
        #[arg(long, default_value_t = 8)]
        threads: usize,

        /// How many records to insert
        #[arg(long, default_value_t = 1000)]
        records: usize,

        /// Size of the value range used for filtering (upper - lower)
        #[arg(long, default_value_t = 10)]
        bound_size: usize,
    },

    /// Concurrently read all records until all writers finish adding records
    ConcurrentReadsAndInserts {
        /// How many times to run the test and average the time
        #[arg(long, default_value_t = 1)]
        runs: u32,

        /// Number of reader threads
        #[arg(long, default_value_t = 8)]
        readers: usize,

        /// Number of writer threads
        #[arg(long, default_value_t = 4)]
        writers: usize,

        /// Records per writer
        #[arg(long, default_value_t = 1000)]
        records_per_writer: usize,
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
) -> Result<Vec<PerformanceTestResult>, TesterError> {
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

    for _ in 0..runs {
        let result = ConcurrentInserts::run_suite(&setup, &test, &cleanup).await?;
        test_results.push(result);
    }
    Ok(test_results)
}

async fn concurrent_reads(
    runs: u32,
    threads: usize,
    records_to_insert: usize,
) -> Result<Vec<PerformanceTestResult>, TesterError> {
    let mut test_results = Vec::with_capacity(runs as _);
    let db_name = "CONCURRENT_READS".to_string();
    let table_name = "CONCURRENT_READS_TABLE".to_string();

    let setup = concurrent_reads::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        records_to_insert,
    };

    let test = concurrent_reads::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_of_threads: threads,
    };

    let cleanup = concurrent_reads::Cleanup {
        database_name: db_name.clone(),
    };

    for _ in 0..runs {
        let result = ReadMany::run_suite(&setup, &test, &cleanup).await?;
        test_results.push(result);
    }
    Ok(test_results)
}

async fn concurrent_reads_and_inserts(
    runs: u32,
    readers: usize,
    writers: usize,
    records_per_writer: usize,
) -> Result<Vec<PerformanceTestResult>, TesterError> {
    let mut test_results = Vec::with_capacity(runs as _);
    let db_name = "CONCURRENT_RW".to_string();
    let table_name = "CONCURRENT_RW_TABLE".to_string();

    let setup = concurrent_reads_and_inserts::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
    };

    let test = concurrent_reads_and_inserts::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_of_readers: readers,
        num_of_writers: writers,
        records_per_writer,
    };

    let cleanup = concurrent_reads_and_inserts::Cleanup {
        database_name: db_name.clone(),
    };

    for _ in 0..runs {
        let result = ConcurrentReadsAndInserts::run_suite(&setup, &test, &cleanup).await?;
        test_results.push(result);
    }
    Ok(test_results)
}

async fn concurrent_reads_index(
    runs: u32,
    threads: usize,
    records_to_insert: usize,
    bound_size: usize,
) -> Result<Vec<PerformanceTestResult>, TesterError> {
    let mut test_results = Vec::with_capacity(runs as _);
    let db_name = "CONCURRENT_READS_INDEX".to_string();
    let table_name = "CONCURRENT_READS_INDEX_TABLE".to_string();

    let setup = concurrent_reads_with_index::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        records_to_insert,
    };

    let test = concurrent_reads_with_index::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_of_threads: threads,
        bound_size,
    };

    let cleanup = concurrent_reads_with_index::Cleanup {
        database_name: db_name.clone(),
    };

    for _ in 0..runs {
        let result = ReadByIndex::run_suite(&setup, &test, &cleanup).await?;
        test_results.push(result);
    }
    Ok(test_results)
}

async fn concurrent_reads_non_index(
    runs: u32,
    threads: usize,
    records_to_insert: usize,
    bound_size: usize,
) -> Result<Vec<PerformanceTestResult>, TesterError> {
    let mut test_results = Vec::with_capacity(runs as _);
    let db_name = "CONCURRENT_READS_NON_INDEX".to_string();
    let table_name = "CONCURRENT_READS_NON_INDEX_TABLE".to_string();

    let setup = concurrent_reads_non_index::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        records_to_insert,
    };

    let test = concurrent_reads_non_index::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_of_threads: threads,
        bound_size,
    };

    let cleanup = concurrent_reads_non_index::Cleanup {
        database_name: db_name.clone(),
    };

    for _ in 0..runs {
        let result = ReadByNonIndex::run_suite(&setup, &test, &cleanup).await?;
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
            records,
        } => {
            let test_results = concurrent_inserts(runs, threads, records).await?;
            report_stats("concurrent-inserts", &test_results);
            Ok(())
        }
        Command::ConcurrentReads {
            runs,
            threads,
            records,
        } => {
            let test_results = concurrent_reads(runs, threads, records).await?;
            report_stats("concurrent-reads", &test_results);
            Ok(())
        }
        Command::ConcurrentReadsIndex {
            runs,
            threads,
            records,
            bound_size,
        } => {
            let test_results = concurrent_reads_index(runs, threads, records, bound_size).await?;
            report_stats("concurrent-reads-index", &test_results);
            Ok(())
        }
        Command::ConcurrentReadsNonIndex {
            runs,
            threads,
            records,
            bound_size,
        } => {
            let test_results =
                concurrent_reads_non_index(runs, threads, records, bound_size).await?;
            report_stats("concurrent-reads-non-index", &test_results);
            Ok(())
        }
        Command::ConcurrentReadsAndInserts {
            runs,
            readers,
            writers,
            records_per_writer,
        } => {
            let test_results =
                concurrent_reads_and_inserts(runs, readers, writers, records_per_writer).await?;
            report_stats("concurrent-reads-and-inserts", &test_results);
            Ok(())
        }
    }
}

fn report_stats(test_name: &str, results: &[PerformanceTestResult]) {
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
