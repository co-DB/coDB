use std::cell::RefCell;
use std::io;
use std::process::{Child, Command as StdCommand, Stdio};
use std::thread;
use std::time::Duration;

use clap::{Parser, Subcommand};
use thiserror::Error;

use crate::e2e::alter_table::{self, AlterTableE2ETest};
use crate::e2e::create_table::{self, CreateTableE2ETest};
use crate::e2e::delete::{self, DeleteE2ETest};
use crate::e2e::drop_table::{self, DropTableE2ETest};
use crate::e2e::insert::{self, InsertE2ETest};
use crate::e2e::select::{self, SelectE2ETest};
use crate::e2e::truncate_table::{self, TruncateTableE2ETest};
use crate::e2e::update::{self, UpdateE2ETest};
use crate::e2e::wal_recovery::{self, WalRecoveryE2ETest};
use crate::performance::concurrent_inserts::{self, ConcurrentInserts};
use crate::performance::concurrent_reads::{self, ReadMany};
use crate::performance::concurrent_reads_and_inserts::{self, ConcurrentReadsAndInserts};
use crate::performance::concurrent_reads_non_index::{self, ReadByNonIndex};
use crate::performance::concurrent_reads_with_index::{self, ReadByIndex};
use crate::suite::{PerformanceTestResult, Suite};

mod client;
mod e2e;
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

    /// E2E test for SELECT statements with comprehensive validation
    E2eSelect {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for INSERT statements with comprehensive validation
    E2eInsert {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for UPDATE statements with comprehensive validation
    E2eUpdate {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for DELETE statements with comprehensive validation
    E2eDelete {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for CREATE TABLE statements with comprehensive validation
    E2eCreateTable {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for TRUNCATE TABLE statements with comprehensive validation
    E2eTruncateTable {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for DROP TABLE statements with comprehensive validation
    E2eDropTable {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for ALTER TABLE statements with comprehensive validation
    E2eAlterTable {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// E2E test for WAL recovery after SIGKILL
    E2eWalRecovery {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },

    /// Run all E2E tests (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, TRUNCATE TABLE, DROP TABLE, ALTER TABLE, and WAL RECOVERY)
    E2eAll {
        /// Path to the server executable
        #[arg(long)]
        server_path: String,
    },
}

#[derive(Debug, Error)]
enum TesterError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    #[error("failed to serialize binary message: {0}")]
    BinarySerialization(#[from] rmp_serde::encode::Error),

    #[error("failed to deserialize binary message: {0}")]
    BinaryDeserialization(#[from] rmp_serde::decode::Error),

    #[error("server disconnected unexpectedly")]
    Disconnected,

    #[error("server returned error: {message}")]
    ServerError { message: String },
}

/// Helper struct to manage the database server process
struct ServerProcess {
    child: Child,
}

impl ServerProcess {
    /// Start the database server as a child process
    fn start(server_path: &str) -> Result<Self, TesterError> {
        println!("Starting database server...");

        // Start the server process
        let child = StdCommand::new(server_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        // Give the server some time to start up
        thread::sleep(Duration::from_secs(1));

        println!("Database server started (PID: {})", child.id());

        Ok(ServerProcess { child })
    }

    /// Stop the database server gracefully
    fn stop(&mut self) -> Result<(), TesterError> {
        println!("Stopping database server (PID: {})...", self.child.id());

        // Wait for the process to finish
        self.child.kill()?;
        self.child.wait()?;

        println!("Database server stopped");

        Ok(())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        // Ensure the process is killed even if stop() wasn't called
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
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

async fn e2e_select(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "E2E_SELECT_TEST".to_string();
    let table_name = "TEST_TABLE".to_string();

    const NUM_RECORDS: usize = 15000;

    let setup = select::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_records: NUM_RECORDS,
    };

    // Generate test data
    let test_data = select::TestRecord::generate(NUM_RECORDS);

    let test = select::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        test_data,
    };

    let cleanup = select::Cleanup {
        database_name: db_name.clone(),
    };

    let result = SelectE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E SELECT test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_insert(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "E2E_INSERT_TEST".to_string();
    let table_name = "INSERT_TEST_TABLE".to_string();

    let setup = insert::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
    };

    let test = insert::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
    };

    let cleanup = insert::Cleanup {
        database_name: db_name.clone(),
    };

    let result = InsertE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E INSERT test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_update(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "UPDATE_E2E_DB".to_string();
    let table_name = "UPDATE_E2E_TABLE".to_string();

    const NUM_RECORDS: usize = 5000;

    let test_data = update::UpdateTestRecord::generate(NUM_RECORDS);

    let setup = update::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_records: NUM_RECORDS,
    };

    let test = update::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        test_data,
    };

    let cleanup = update::Cleanup {
        database_name: db_name.clone(),
    };

    let result = UpdateE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E UPDATE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_delete(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "DELETE_E2E_DB".to_string();
    let table_name = "DELETE_E2E_TABLE".to_string();
    const NUM_RECORDS: usize = 5000;

    let test_data = delete::DeleteTestRecord::generate(NUM_RECORDS);

    let setup = delete::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        num_records: NUM_RECORDS,
    };

    let test = delete::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        test_data,
    };

    let cleanup = delete::Cleanup {
        database_name: db_name.clone(),
    };

    let result = DeleteE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E DELETE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_create_table(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "CREATE_TABLE_E2E_DB".to_string();

    let setup = create_table::Setup {
        database_name: db_name.clone(),
    };

    let test = create_table::Test {
        database_name: db_name.clone(),
    };

    let cleanup = create_table::Cleanup {
        database_name: db_name.clone(),
    };

    let result = CreateTableE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E CREATE TABLE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_truncate_table(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "TRUNCATE_TABLE_E2E_DB".to_string();

    let setup = truncate_table::Setup {
        database_name: db_name.clone(),
    };

    let test = truncate_table::Test {
        database_name: db_name.clone(),
    };

    let cleanup = truncate_table::Cleanup {
        database_name: db_name.clone(),
    };

    let result = TruncateTableE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E TRUNCATE TABLE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_drop_table(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "DROP_TABLE_E2E_DB".to_string();

    let setup = drop_table::Setup {
        database_name: db_name.clone(),
    };

    let test = drop_table::Test {
        database_name: db_name.clone(),
    };

    let cleanup = drop_table::Cleanup {
        database_name: db_name.clone(),
    };

    let result = DropTableE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E DROP TABLE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_alter_table(server_path: &str) -> Result<(), TesterError> {
    let mut server = ServerProcess::start(server_path)?;

    let db_name = "ALTER_TABLE_E2E_DB".to_string();

    let setup = alter_table::Setup {
        database_name: db_name.clone(),
    };

    let test = alter_table::Test {
        database_name: db_name.clone(),
    };

    let cleanup = alter_table::Cleanup {
        database_name: db_name.clone(),
    };

    let result = AlterTableE2ETest::run_suite(&setup, &test, &cleanup).await?;

    println!("E2E ALTER TABLE test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    server.stop()?;

    Ok(())
}

async fn e2e_wal_recovery(server_path: &str) -> Result<(), TesterError> {
    let server = ServerProcess::start(server_path)?;

    let db_name = "WAL_RECOVERY_E2E_DB".to_string();
    let table_name = "WAL_TEST_TABLE".to_string();

    const NUM_RECORDS: usize = 10000;

    let setup = wal_recovery::Setup {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
    };

    // Generate test data
    let test_data = wal_recovery::WalTestRecord::generate(NUM_RECORDS);

    let test = wal_recovery::Test {
        database_name: db_name.clone(),
        table_name: table_name.clone(),
        test_data: test_data.clone(),
        server_path: server_path.to_string(),
        server,
    };

    let result = WalRecoveryE2ETest::run_suite(&setup, &RefCell::new(test), &()).await?;

    println!("E2E WAL RECOVERY test completed successfully!");
    println!("Tests passed: {}", result.tests_passed);

    // Server is stopped inside wal recovery test, no need to stop it here.

    Ok(())
}

async fn e2e_all(server_path: &str) -> Result<(), TesterError> {
    println!("\n========================================");
    println!("Running ALL E2E Tests");
    println!("========================================\n");

    // Run CREATE TABLE tests
    println!("[1/9] Running CREATE TABLE E2E tests...");
    match e2e_create_table(server_path).await {
        Ok(()) => {
            println!("✓ CREATE TABLE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ CREATE TABLE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run INSERT tests
    println!("[2/9] Running INSERT E2E tests...");
    match e2e_insert(server_path).await {
        Ok(()) => {
            println!("✓ INSERT E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ INSERT E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run SELECT tests
    println!("[3/9] Running SELECT E2E tests...");
    match e2e_select(server_path).await {
        Ok(()) => {
            println!("✓ SELECT E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ SELECT E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run UPDATE tests
    println!("[4/9] Running UPDATE E2E tests...");
    match e2e_update(server_path).await {
        Ok(()) => {
            println!("✓ UPDATE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ UPDATE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run DELETE tests
    println!("[5/9] Running DELETE E2E tests...");
    match e2e_delete(server_path).await {
        Ok(()) => {
            println!("✓ DELETE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ DELETE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run TRUNCATE TABLE tests
    println!("[6/9] Running TRUNCATE TABLE E2E tests...");
    match e2e_truncate_table(server_path).await {
        Ok(()) => {
            println!("✓ TRUNCATE TABLE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ TRUNCATE TABLE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run DROP TABLE tests
    println!("[7/9] Running DROP TABLE E2E tests...");
    match e2e_drop_table(server_path).await {
        Ok(()) => {
            println!("✓ DROP TABLE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ DROP TABLE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }
    // Run ALTER TABLE tests
    println!("[8/9] Running ALTER TABLE E2E tests...");
    match e2e_alter_table(server_path).await {
        Ok(()) => {
            println!("✓ ALTER TABLE E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ ALTER TABLE E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    // Run WAL RECOVERY tests
    println!("[9/9] Running WAL RECOVERY E2E tests...");
    match e2e_wal_recovery(server_path).await {
        Ok(()) => {
            println!("✓ WAL RECOVERY E2E tests passed\n");
        }
        Err(e) => {
            println!("✗ WAL RECOVERY E2E tests failed: {:?}\n", e);
            return Err(e);
        }
    }

    println!("========================================");
    println!("All E2E Tests Completed Successfully!");
    println!("========================================");

    Ok(())
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
        Command::E2eSelect { server_path } => {
            e2e_select(&server_path).await?;
            Ok(())
        }
        Command::E2eInsert { server_path } => {
            e2e_insert(&server_path).await?;
            Ok(())
        }
        Command::E2eUpdate { server_path } => {
            e2e_update(&server_path).await?;
            Ok(())
        }
        Command::E2eDelete { server_path } => {
            e2e_delete(&server_path).await?;
            Ok(())
        }
        Command::E2eCreateTable { server_path } => {
            e2e_create_table(&server_path).await?;
            Ok(())
        }
        Command::E2eTruncateTable { server_path } => {
            e2e_truncate_table(&server_path).await?;
            Ok(())
        }
        Command::E2eDropTable { server_path } => {
            e2e_drop_table(&server_path).await?;
            Ok(())
        }
        Command::E2eAlterTable { server_path } => {
            e2e_alter_table(&server_path).await?;
            Ok(())
        }
        Command::E2eWalRecovery { server_path } => {
            e2e_wal_recovery(&server_path).await?;
            Ok(())
        }
        Command::E2eAll { server_path } => {
            e2e_all(&server_path).await?;
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
