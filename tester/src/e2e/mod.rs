pub mod create_table;

use crate::{TesterError, suite::E2eTestResult};

pub async fn run_all() -> Result<(), TesterError> {
    let mut results: Vec<(&str, E2eTestResult)> = Vec::new();

    // instantiate and run e2e tests
    let create_table = create_table::CreateTableTest;
    let res = create_table.run_suite().await?;
    results.push(("create_table", res));

    // collect and print errors
    let mut any_failed = false;
    for (name, r) in &results {
        if let Some(err) = &r.error {
            any_failed = true;
            println!("E2E test failed: {}: {}", name, err);
        }
    }

    if any_failed {
        return Err(TesterError::ServerError {
            message: "some e2e tests failed".to_string(),
        });
    }

    Ok(())
}
