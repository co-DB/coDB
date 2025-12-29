use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use log::info;
use protocol::BINARY_PROTOCOL_PORT;

use crate::{TesterError, client::BinaryClient};

pub trait Suite<R> {
    type SetupArgs;
    async fn setup(args: &Self::SetupArgs) -> Result<(), TesterError>;

    type TestArgs;
    async fn run(args: &Self::TestArgs) -> Result<R, TesterError>;

    type CleanupArgs;
    async fn cleanup(args: &Self::CleanupArgs) -> Result<(), TesterError>;

    async fn run_suite(
        setup_args: &Self::SetupArgs,
        run_args: &Self::TestArgs,
        cleanup_args: &Self::CleanupArgs,
    ) -> Result<R, TesterError> {
        info!("Starting setup...");
        Self::setup(setup_args).await?;
        info!("Setup completed.");

        info!("Starting test...");
        let result = Self::run(run_args).await?;
        info!("Test completed.");

        info!("Starting cleanup...");
        Self::cleanup(cleanup_args).await?;
        info!("Cleanup completed.");

        Ok(result)
    }
}

pub struct PerformanceTestResult {
    pub duration: Duration,
}

const TEST_HOST: &str = "127.0.0.1";
const TEST_PORT: u16 = BINARY_PROTOCOL_PORT;

pub fn default_addr() -> SocketAddr {
    let ip_addr = TEST_HOST.parse::<IpAddr>().unwrap();
    SocketAddr::new(ip_addr, TEST_PORT)
}

pub async fn default_client() -> Result<BinaryClient, TesterError> {
    BinaryClient::connect(default_addr()).await
}
