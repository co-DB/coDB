use clap::Parser;
use log::{error, info};
use protocol::{ErrorType, Request, Response};
use std::io;
use std::io::{BufRead, Write};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

#[derive(Parser)]
struct Args {
    database_name: String,

    #[arg(short, long)]
    query: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    env_logger::init();

    let addr = "127.0.0.1:5434";

    let client = DbClient::connect(addr).await?;

    let mut console_handler = ConsoleHandler::new(client);

    console_handler.run_loop().await?;

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
}

enum ReadResult {
    Disconnected,
    Response(Response),
}

struct DbClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl DbClient {
    async fn connect(addr: impl AsRef<str>) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr.as_ref()).await?;

        let (reader, writer) = stream.into_split();

        Ok(Self {
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
        })
    }

    async fn send_request(&mut self, request: Request) -> Result<(), ClientError> {
        info!("Sending request: {:?}", request);

        let json = serde_json::to_string(&request)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<ReadResult, ClientError> {
        let mut buffer = String::new();

        match self.reader.read_line(&mut buffer).await {
            Ok(0) => Ok(ReadResult::Disconnected),
            Ok(_) => {
                let trimmed = buffer.trim();
                let response = serde_json::from_str(trimmed)?;
                Ok(ReadResult::Response(response))
            }
            Err(e) => Err(ClientError::IoError(e)),
        }
    }
}

struct ConsoleHandler {
    db_client: DbClient,
    current_input: String,
}

impl ConsoleHandler {
    const ENDING_INPUT: &'static str = "exit";
    const COMMAND_START: char = '/';

    fn new(db_client: DbClient) -> Self {
        Self {
            db_client,
            current_input: String::new(),
        }
    }
    async fn run_loop(&mut self) -> Result<(), ClientError> {
        let mut read_handle = io::stdin().lock();
        let mut write_handle = io::stdout().lock();

        loop {
            self.current_input.clear();

            print!("> ");
            write_handle.flush()?;

            read_handle.read_line(&mut self.current_input)?;

            let trimmed = self.current_input.trim();

            if trimmed.is_empty() {
                continue;
            }

            if trimmed == Self::ENDING_INPUT {
                break;
            }

            let request = match Self::parse_request(trimmed) {
                Ok(request) => request,
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
            };

            self.db_client.send_request(request).await?;

            self.handle_response().await?;
        }

        Ok(())
    }

    async fn handle_response(&mut self) -> Result<(), ClientError> {
        loop {
            match self.db_client.read_response().await? {
                ReadResult::Disconnected => break,
                ReadResult::Response(resp) => {
                    ConsoleHandler::display_response(&resp);

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

    fn parse_request(request: impl AsRef<str>) -> Result<Request, ClientError> {
        let is_command = request.as_ref().starts_with(Self::COMMAND_START);

        if is_command {
            let (_, command) = request.as_ref().split_at(1);
            Self::parse_command(command)
        } else {
            Ok(Request::Query {
                sql: request.as_ref().trim().to_string(),
                database_name: None,
            })
        }
    }

    fn parse_command(command: impl AsRef<str>) -> Result<Request, ClientError> {
        let command = command.as_ref();
        let command_segments = command.split_whitespace().collect::<Vec<_>>();

        if command_segments.is_empty() {
            return Err(ClientError::InvalidCommand {
                reason: "empty command".to_string(),
            });
        }

        match command_segments[0] {
            "create" => Self::parse_create_database(command_segments),
            "delete" => Self::parse_delete_database(command_segments),
            "list" => Self::parse_list_databases(command_segments),
            "connect" => Self::parse_connect(command_segments),
            "query" => Self::parse_query(command),
            _ => Err(ClientError::InvalidCommand {
                reason: format!("{} is not a command", command_segments[0]),
            }),
        }
    }

    fn parse_create_database(command_segments: Vec<&str>) -> Result<Request, ClientError> {
        if command_segments.len() != 2 {
            return Err(ClientError::InvalidCommand {
                reason: "create command takes one argument".to_string(),
            });
        }

        let database_name = command_segments[1].to_string();

        Ok(Request::CreateDatabase { database_name })
    }

    fn parse_delete_database(command_segments: Vec<&str>) -> Result<Request, ClientError> {
        if command_segments.len() != 2 {
            return Err(ClientError::InvalidCommand {
                reason: "delete command takes one argument".to_string(),
            });
        }

        let database_name = command_segments[1].to_string();

        Ok(Request::DeleteDatabase { database_name })
    }

    fn parse_list_databases(command_segments: Vec<&str>) -> Result<Request, ClientError> {
        if command_segments.len() != 1 {
            return Err(ClientError::InvalidCommand {
                reason: "list command takes no arguments".to_string(),
            });
        }

        Ok(Request::ListDatabases)
    }

    fn parse_connect(command_segments: Vec<&str>) -> Result<Request, ClientError> {
        if command_segments.len() != 2 {
            return Err(ClientError::InvalidCommand {
                reason: "connect takes one argument".to_string(),
            });
        }

        let database_name = command_segments[1].to_string();
        Ok(Request::Connect { database_name })
    }

    fn parse_query(original: &str) -> Result<Request, ClientError> {
        let without_prefix = original.trim_start_matches("/query").trim();

        if let Some((db_name, sql_start)) = without_prefix.split_once(' ')
            && !sql_start.trim().is_empty()
        {
            return Ok(Request::Query {
                database_name: Some(db_name.to_string()),
                sql: sql_start.trim().to_string(),
            });
        }

        Ok(Request::Query {
            database_name: None,
            sql: without_prefix.to_string(),
        })
    }
    fn display_response(response: &Response) {
        match response {
            Response::Connected { database_name } => {
                println!("Connected to database: {}", database_name);
            }

            Response::Acknowledge => {
                println!("✔ Query acknowledged by server, executing...");
            }

            Response::ColumnInfo { column_metadata } => {
                println!("Columns:");
                for col in column_metadata {
                    println!("  - {} ({:?})", col.name, col.ty);
                }
            }

            Response::Rows {
                records,
                count: _count,
            } => {
                print!("Row: ");
                for (i, v) in records.iter().enumerate() {
                    if i > 0 {
                        print!(", ");
                    }
                    print!("{:?}", v);
                }
                println!();
            }

            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                println!(
                    "Statement completed: {:?}, affected rows: {}",
                    statement_type, rows_affected
                );
            }

            Response::QueryCompleted => {
                println!("✔ Query completed.");
            }

            Response::Error {
                message,
                error_type: _error_type,
            } => {
                eprintln!("❌ ERROR: {}", message);
            }

            Response::DatabaseCreated { database_name } => {
                println!("Database created: {}", database_name);
            }

            Response::DatabaseDeleted { database_name } => {
                println!("Database deleted: {}", database_name);
            }

            Response::DatabasesListed { database_names } => {
                if database_names.is_empty() {
                    println!("No databases found.");
                } else {
                    println!("Databases:");
                    for name in database_names {
                        println!("  - {}", name);
                    }
                }
            }
        }
    }
}

trait IsFinalMessage {
    fn is_final_message(&self) -> bool;
}

impl IsFinalMessage for Response {
    fn is_final_message(&self) -> bool {
        match self {
            Response::Acknowledge
            | Response::ColumnInfo { .. }
            | Response::Rows { .. }
            | Response::StatementCompleted { .. } => false,
            Response::Error {
                message: _message,
                error_type,
            } => matches!(error_type, ErrorType::Network),
            _ => true,
        }
    }
}
