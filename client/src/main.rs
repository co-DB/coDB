use clap::{Parser, ValueEnum};
use log::info;
use protocol::{BINARY_PROTOCOL_PORT, Request, TEXT_PROTOCOL_PORT};
use rkyv::rancor;
use std::io;
use std::net::{IpAddr, SocketAddr};
use thiserror::Error;
use tokio::net::TcpStream;

use crate::console_handler::{ConsoleHandler, IsFinalMessage, display_response};
use crate::db_client::{
    BinaryProtocolHandler, DbClient, ProtocolHandler, ReadResult, TextProtocolHandler,
};

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
    database: Option<String>,

    #[arg(short, long)]
    query: Option<String>,

    #[arg(short, long, value_enum, default_value_t = Protocol::Text)]
    protocol: Protocol,
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    env_logger::init();

    let args = Args::parse();
    run_client(args).await?;

    Ok(())
}

async fn run_client(args: Args) -> Result<(), ClientError> {
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    match (args.database, args.query) {
        (None, None) => start_console_handler(ip, &args.protocol).await,
        (None, Some(_)) => Err(ClientError::MissingParameter {
            parameter: "database".into(),
        }),
        (Some(_), None) => Err(ClientError::MissingParameter {
            parameter: "query".into(),
        }),
        (Some(database), Some(query)) => {
            handle_single_query(ip, &args.protocol, database, query).await
        }
    }
}

async fn start_console_handler(ip: IpAddr, protocol: &Protocol) -> Result<(), ClientError> {
    match protocol {
        Protocol::Binary => {
            info!("Starting client with binary protocol.");
            let client = start_client::<BinaryProtocolHandler>(ip, BINARY_PROTOCOL_PORT).await?;
            let mut console_handler = ConsoleHandler::new(client);
            console_handler.run_loop().await?;
        }
        Protocol::Text => {
            info!("Starting client with text protocol.");
            let client = start_client::<TextProtocolHandler>(ip, TEXT_PROTOCOL_PORT).await?;
            let mut console_handler = ConsoleHandler::new(client);
            console_handler.run_loop().await?;
        }
    };
    Ok(())
}

async fn start_client<P>(ip: IpAddr, port: u16) -> Result<DbClient<P>, ClientError>
where
    P: ProtocolHandler + From<TcpStream>,
{
    let socket = TcpStream::connect(SocketAddr::new(ip, port)).await?;
    let protocol = P::from(socket);
    let client = DbClient::new(protocol);
    Ok(client)
}

async fn handle_single_query(
    ip: IpAddr,
    protocol: &Protocol,
    database: String,
    query: String,
) -> Result<(), ClientError> {
    match protocol {
        Protocol::Binary => {
            let client = start_client::<BinaryProtocolHandler>(ip, BINARY_PROTOCOL_PORT).await?;
            handle_query_with_client(client, database, query).await?;
        }
        Protocol::Text => {
            let client = start_client::<TextProtocolHandler>(ip, TEXT_PROTOCOL_PORT).await?;
            handle_query_with_client(client, database, query).await?;
        }
    }

    Ok(())
}

async fn handle_query_with_client<P>(
    mut client: DbClient<P>,
    database: String,
    query: String,
) -> Result<(), ClientError>
where
    P: ProtocolHandler,
{
    let query_request = Request::Query {
        database_name: Some(database),
        sql: query,
    };
    client.send_request(query_request).await?;
    handle_client_response(&mut client).await?;
    Ok(())
}

async fn handle_client_response<P>(client: &mut DbClient<P>) -> Result<(), ClientError>
where
    P: ProtocolHandler,
{
    loop {
        match client.read_response().await? {
            ReadResult::Disconnected => break,
            ReadResult::Response(resp) => {
                display_response(&resp);
                if resp.is_final_message() {
                    break;
                } else {
                    continue;
                }
            }
        }
    }
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
    #[error("missing parameter: {parameter}")]
    MissingParameter { parameter: String },
}
