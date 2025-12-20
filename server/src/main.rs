mod protocol_mappings;
mod server;
mod text_client_handler;

use crate::server::{Server, ServerError};

#[tokio::main]
async fn main() -> Result<(), ServerError> {
    env_logger::init();

    let server = Server::new(
        "0.0.0.0:5433".parse().unwrap(),
        "0.0.0.0:5434".parse().unwrap(),
    )?;

    server.run_loop().await?;

    Ok(())
}
