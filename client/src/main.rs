use clap::{Parser, ValueEnum};
use log::info;
use protocol::{BINARY_PROTOCOL_PORT, TEXT_PROTOCOL_PORT};
use rkyv::rancor;
use std::io;
use std::net::{IpAddr, SocketAddr};
use thiserror::Error;
use tokio::net::TcpStream;

use crate::console_handler::ConsoleHandler;
use crate::db_client::{BinaryProtocolHandler, DbClient, TextProtocolHandler};

mod console_handler;
mod db_client;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Protocol {
    Binary,
    Text,
}

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    database_name: Option<String>,

    #[arg(short, long)]
    query: Option<String>,

    #[arg(short, long, value_enum, default_value_t = Protocol::Text)]
    protocol: Protocol,
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    env_logger::init();

    let args = Args::parse();
    run_client(&args).await?;

    Ok(())
}

async fn run_client(args: &Args) -> Result<(), ClientError> {
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    match args.protocol {
        Protocol::Binary => {
            info!("Starting client with binary protocol.");
            let socket = TcpStream::connect(SocketAddr::new(ip, BINARY_PROTOCOL_PORT)).await?;
            let protocol = BinaryProtocolHandler::from(socket);
            let client = DbClient::new(protocol);
            let mut console_handler = ConsoleHandler::new(client);
            console_handler.run_loop().await?;
        }
        Protocol::Text => {
            info!("Starting client with text protocol.");
            let socket = TcpStream::connect(SocketAddr::new(ip, TEXT_PROTOCOL_PORT)).await?;
            let protocol = TextProtocolHandler::from(socket);
            let client = DbClient::new(protocol);
            let mut console_handler = ConsoleHandler::new(client);
            console_handler.run_loop().await?;
        }
    };
    Ok(())
}

#[derive(Debug, Error)]
enum ClientError {
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("invalid command: {reason}")]
    InvalidCommand { reason: String },
    #[error("failed to serialize binary message: {0}")]
    BinarySerializationError(rancor::Error),
    #[error("failed to deserialize binary message: {0}")]
    BinaryDeserializationError(rancor::Error),
}
