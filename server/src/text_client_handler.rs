use crate::text_protocol_mappings::IntoTextProtocol;
use dashmap::{DashMap, Entry};
use engine::record::Record as EngineRecord;
use executor::response::StatementResult;
use executor::{Executor, ExecutorError};
use itertools::Itertools;
use log::error;
use metadata::catalog_manager::{CatalogManager, CatalogManagerError};
use parking_lot::RwLock;
use protocol::text_protocol::{
    ErrorType, Record as ProtocolRecord, Request, Response, StatementType,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;

pub(crate) struct TextClientHandler {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    current_database: Option<String>,
    executors: Arc<DashMap<String, Arc<Executor>>>,
    catalog_manager: Arc<RwLock<CatalogManager>>,
}

enum ReadResult {
    Request(Request),
    Disconnected,
    Empty,
}

#[derive(Error, Debug)]
enum ClientError {
    #[error("invalid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("no database selected")]
    NoDatabaseSelected,

    #[error("database '{0}' does not exist")]
    DatabaseDoesNotExist(String),

    #[error("catalog error: {0}")]
    CatalogError(#[from] CatalogManagerError),

    #[error("executor error: {0}")]
    ExecutorError(#[from] ExecutorError),
}

impl ClientError {
    fn to_error_type(&self) -> ErrorType {
        match self {
            ClientError::InvalidJson(_) => ErrorType::InvalidRequest,
            ClientError::NoDatabaseSelected => ErrorType::InvalidRequest,
            ClientError::DatabaseDoesNotExist(_) => ErrorType::InvalidRequest,
            ClientError::IoError(_) => ErrorType::Network,
            ClientError::CatalogError(_) => ErrorType::Catalog,
            ClientError::ExecutorError(_) => ErrorType::Execution,
        }
    }
}

impl TextClientHandler {
    const CHANNEL_BUFFER_CAPACITY: usize = 16;

    const ROWS_CHUNK_SIZE: usize = 10000;

    pub(crate) fn new(
        socket: TcpStream,
        executors: Arc<DashMap<String, Arc<Executor>>>,
        catalog_manager: Arc<RwLock<CatalogManager>>,
    ) -> Self {
        let (read, write) = socket.into_split();
        Self {
            reader: BufReader::new(read),
            writer: BufWriter::new(write),
            current_database: None,
            executors,
            catalog_manager,
        }
    }

    pub(crate) async fn run(mut self) {
        loop {
            match self.read_request().await {
                Ok(ReadResult::Request(request)) => {
                    if let Err(e) = self.handle_request(request).await {
                        error!("Error handling request: {}", e);
                        let _ = self.send_error(e.to_string(), e.to_error_type()).await;
                    }
                }
                Ok(ReadResult::Disconnected) => {
                    break;
                }
                Ok(ReadResult::Empty) => {
                    continue;
                }
                Err(e) => {
                    error!("Error reading request: {}", e);
                    let _ = self.send_error(e.to_string(), e.to_error_type()).await;
                }
            }
        }
    }

    async fn read_request(&mut self) -> Result<ReadResult, ClientError> {
        let mut buffer = String::new();

        match self.reader.read_line(&mut buffer).await {
            Ok(0) => Ok(ReadResult::Disconnected),
            Ok(_) => {
                let trimmed = buffer.trim();
                if trimmed.is_empty() {
                    Ok(ReadResult::Empty)
                } else {
                    let request = serde_json::from_str(trimmed)?;
                    Ok(ReadResult::Request(request))
                }
            }
            Err(e) => Err(ClientError::IoError(e)),
        }
    }

    async fn handle_request(&mut self, request: Request) -> Result<(), ClientError> {
        match request {
            Request::CreateDatabase { database_name } => {
                self.catalog_manager
                    .write()
                    .create_database(&database_name)?;
                self.send_response(Response::DatabaseCreated { database_name })
                    .await?;
            }

            Request::DeleteDatabase { database_name } => {
                self.catalog_manager
                    .write()
                    .delete_database(&database_name)?;
                self.executors.remove(&database_name);
                if self.current_database.as_deref() == Some(&database_name) {
                    self.current_database = None;
                }
                self.send_response(Response::DatabaseDeleted { database_name })
                    .await?;
            }

            Request::ListDatabases => {
                let databases = self.catalog_manager.read().list_databases();
                let vec: Vec<String> = databases.names.iter().cloned().collect();
                self.send_response(Response::DatabasesListed {
                    database_names: vec,
                })
                .await?;
            }

            Request::Connect { database_name } => {
                let database_exists = self.catalog_manager.read().database_exists(&database_name);
                if !database_exists {
                    return Err(ClientError::DatabaseDoesNotExist(database_name));
                }
                self.current_database = Some(database_name.clone());
                self.send_response(Response::Connected { database_name })
                    .await?;
            }

            Request::Query { database_name, sql } => {
                self.handle_query(database_name, sql).await?;
            }
        }
        Ok(())
    }

    async fn handle_query(
        &mut self,
        database_name: Option<String>,
        sql: String,
    ) -> Result<(), ClientError> {
        self.send_response(Response::Acknowledge).await?;

        let database = match database_name {
            Some(name) => name,
            None => self
                .current_database
                .clone()
                .ok_or(ClientError::NoDatabaseSelected)?,
        };

        let executor = self.get_or_create_executor(&database)?;

        let (sender, mut receiver) =
            mpsc::channel::<StatementResult>(Self::CHANNEL_BUFFER_CAPACITY);

        let handle = tokio::task::spawn_blocking(move || {
            for result in executor.execute(&sql) {
                if sender.blocking_send(result).is_err() {
                    break;
                }
            }
        });

        while let Some(result) = receiver.recv().await {
            if let Err(e) = self.handle_statement_result(result).await {
                handle.abort();
                return Err(e);
            }
        }

        self.send_response(Response::QueryCompleted).await?;
        Ok(())
    }

    fn get_or_create_executor(
        &self,
        database: impl AsRef<str> + Into<String> + Clone,
    ) -> Result<Arc<Executor>, ClientError> {
        let database_key = database.clone().into();

        match self.executors.entry(database_key) {
            Entry::Occupied(executor) => Ok(executor.get().clone()),
            Entry::Vacant(vacant_entry) => {
                let (catalog, main_path) = {
                    let catalog_guard = self.catalog_manager.read();
                    let main_path = catalog_guard.main_directory_path();
                    let catalog = catalog_guard.open_catalog(database.as_ref())?;
                    (catalog, main_path)
                };

                let db_directory_path = main_path.join(database.as_ref());
                let executor = Arc::new(
                    Executor::new(db_directory_path, catalog)
                        .map_err(ClientError::ExecutorError)?,
                );

                vacant_entry.insert(executor.clone());
                Ok(executor)
            }
        }
    }

    async fn handle_statement_result(
        &mut self,
        statement: StatementResult,
    ) -> Result<(), ClientError> {
        match statement {
            StatementResult::OperationSuccessful { rows_affected, ty } => {
                self.send_response(Response::StatementCompleted {
                    rows_affected,
                    statement_type: ty.into_text_protocol(),
                })
                .await?;
            }

            StatementResult::SelectSuccessful { rows, columns } => {
                self.send_response(Response::ColumnInfo {
                    column_metadata: columns
                        .into_iter()
                        .map(|cm| cm.into_text_protocol())
                        .collect(),
                })
                .await?;

                let rows_affected = rows.len();

                self.batch_send_rows(rows).await?;

                self.send_response(Response::StatementCompleted {
                    rows_affected,
                    statement_type: StatementType::Select,
                })
                .await?;
            }

            StatementResult::ParseError { error } => {
                self.send_error(error.to_string(), ErrorType::Query).await?;
            }

            StatementResult::RuntimeError { error } => {
                self.send_error(error.to_string(), ErrorType::Execution)
                    .await?;
            }
        }
        Ok(())
    }

    async fn batch_send_rows(&mut self, rows: Vec<EngineRecord>) -> Result<(), ClientError> {
        let batches: Vec<Vec<_>> = rows
            .into_iter()
            .chunks(Self::ROWS_CHUNK_SIZE)
            .into_iter()
            .map(|chunk| chunk.collect::<Vec<_>>())
            .collect();

        for batch in batches {
            let count = batch.len();
            let mapped_records: Vec<ProtocolRecord> = batch
                .into_iter()
                .map(|record| record.into_text_protocol())
                .collect();
            self.send_response(Response::Rows {
                records: mapped_records,
                count,
            })
            .await?;
        }
        Ok(())
    }
    async fn send_response(&mut self, response: Response) -> Result<(), ClientError> {
        let json = serde_json::to_string(&response)?;
        self.writer.write_all(json.as_bytes()).await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_error(
        &mut self,
        error_msg: impl Into<String>,
        error_type: ErrorType,
    ) -> Result<(), ClientError> {
        let error_response = Response::Error {
            message: error_msg.into(),
            error_type,
        };
        self.send_response(error_response).await
    }
}
