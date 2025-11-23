mod server;
mod text_client_handler;
mod text_protocol;

use crate::server::{Server, ServerError};
#[tokio::main]
async fn main() -> Result<(), ServerError> {
    let server = Server::new(
        "0.0.0.0:5432".parse().unwrap(),
        "0.0.0.0:5433".parse().unwrap(),
    )?;

    server.run_loop().await?;

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C");

    println!("Shutting down...");

    Ok(())
}
