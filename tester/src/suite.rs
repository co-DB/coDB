use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use protocol::BINARY_PROTOCOL_PORT;

use crate::{TesterError, client::BinaryClient};

pub trait Suite<R> {
    type SetupArgs;
    async fn setup(&self, args: &Self::SetupArgs) -> Result<(), TesterError>;

    type TestArgs;
    async fn run(&self, args: &Self::TestArgs) -> Result<R, TesterError>;

    type CleanupArgs;
    async fn cleanup(&self, args: &Self::CleanupArgs) -> Result<(), TesterError>;
}

pub struct PerformanceTestResult {
    pub duration: Duration,
}

pub struct E2eTestResult {
    pub error: Option<String>,
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
