mod client_handler;
mod protocol_handler;
mod protocol_mappings;
mod server;

use std::net::{IpAddr, SocketAddr};

use protocol::{BINARY_PROTOCOL_PORT, TEXT_PROTOCOL_PORT};

use crate::server::{Server, ServerError};

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    env_logger::init();

    let ip: IpAddr = "0.0.0.0".parse().unwrap();

    let server = Server::new(
        SocketAddr::new(ip, BINARY_PROTOCOL_PORT),
        SocketAddr::new(ip, TEXT_PROTOCOL_PORT),
    )?;

    server.run_loop().await?;

    Ok(())
}
