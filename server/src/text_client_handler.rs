use crate::text_protocol::{
    ColumnMetadata, ColumnType, Date, DateTime, ErrorType, Field, Record, Request, Response,
    StatementType,
};
use dashmap::DashMap;
use engine::data_types::{DbDate, DbDateTime};
use executor::{ColumnData, Executor, ExecutorError, StatementResult};
use itertools::Itertools;
use metadata::catalog_manager::{CatalogManager, CatalogManagerError};
use metadata::types::Type;
use parking_lot::RwLock;
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

    #[error("catalog error: {0}")]
    CatalogError(#[from] CatalogManagerError),

    #[error("executor error: {0}")]
    ExecutorError(#[from] ExecutorError),
}

impl ClientError {
    fn to_error_type(&self) -> ErrorType {
        match self {
            ClientError::InvalidJson(_) => ErrorType::Communication,
            ClientError::IoError(_) => ErrorType::Communication,
            ClientError::NoDatabaseSelected => ErrorType::Communication,
            ClientError::CatalogError(_) => ErrorType::Catalog,
            ClientError::ExecutorError(_) => ErrorType::Execution,
        }
    }
}

impl TextClientHandler {
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
                        eprintln!("Error handling request: {}", e);
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
                    eprintln!("Error reading request: {}", e);
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

        let (sender, mut receiver) = mpsc::channel::<StatementResult>(16);

        tokio::task::spawn_blocking(move || {
            for result in executor.execute(&sql) {
                if sender.blocking_send(result).is_err() {
                    break;
                }
            }
        });

        while let Some(result) = receiver.recv().await {
            self.handle_statement_result(result).await?;
        }

        self.send_response(Response::QueryCompleted).await?;
        Ok(())
    }

    fn get_or_create_executor(
        &self,
        database: impl AsRef<str> + Into<String>,
    ) -> Result<Arc<Executor>, ClientError> {
        if let Some(executor) = self.executors.get(database.as_ref()) {
            return Ok(executor.value().clone());
        }

        let (catalog, main_path) = {
            let catalog_guard = self.catalog_manager.read();
            let main_path = catalog_guard.main_directory_path();
            let catalog = catalog_guard.open_catalog(database.as_ref())?;
            (catalog, main_path)
        };

        let db_directory_path = main_path.join(database.as_ref());
        let executor = Arc::new(
            Executor::new(db_directory_path, catalog).map_err(ClientError::ExecutorError)?,
        );

        self.executors.insert(database.into(), executor.clone());
        Ok(executor)
    }

    async fn handle_statement_result(
        &mut self,
        statement: StatementResult,
    ) -> Result<(), ClientError> {
        match statement {
            StatementResult::OperationSuccessful { rows_affected, ty } => {
                self.send_response(Response::StatementCompleted {
                    rows_affected,
                    statement_type: ty.into(),
                })
                .await?;
            }

            StatementResult::SelectSuccessful { rows, columns } => {
                self.send_response(Response::ColumnInfo {
                    column_metadata: columns.into_iter().map(|cm| cm.into()).collect(),
                })
                .await?;

                let rows_affected = rows.len();
                const CHUNK_SIZE: usize = 10_000;

                let batches: Vec<Vec<_>> = rows
                    .into_iter()
                    .chunks(CHUNK_SIZE)
                    .into_iter()
                    .map(|chunk| chunk.collect::<Vec<_>>())
                    .collect();

                for batch in batches {
                    let count = batch.len();
                    let mapped_records: Vec<Record> =
                        batch.into_iter().map(|record| record.into()).collect();
                    self.send_response(Response::Rows {
                        records: mapped_records,
                        count,
                    })
                    .await?;
                }

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

impl From<Type> for ColumnType {
    fn from(value: Type) -> Self {
        match value {
            Type::String => ColumnType::String,
            Type::F32 => ColumnType::F32,
            Type::F64 => ColumnType::F64,
            Type::I32 => ColumnType::I32,
            Type::I64 => ColumnType::I64,
            Type::Bool => ColumnType::Bool,
            Type::Date => ColumnType::Date,
            Type::DateTime => ColumnType::DateTime,
        }
    }
}

impl From<ColumnData> for ColumnMetadata {
    fn from(value: ColumnData) -> Self {
        Self {
            name: value.name,
            ty: value.ty.into(),
        }
    }
}

impl From<engine::record::Record> for Record {
    fn from(value: engine::record::Record) -> Self {
        Self {
            fields: value.fields.into_iter().map(|field| field.into()).collect(),
        }
    }
}

impl From<engine::record::Field> for Field {
    fn from(value: engine::record::Field) -> Self {
        match value {
            engine::record::Field::Int32(i) => Field::Int32(i),
            engine::record::Field::Int64(i) => Field::Int64(i),
            engine::record::Field::Float32(f) => Field::Float32(f),
            engine::record::Field::Float64(f) => Field::Float64(f),
            engine::record::Field::DateTime(dt) => Field::DateTime(dt.into()),
            engine::record::Field::Date(d) => Field::Date(d.into()),
            engine::record::Field::String(s) => Field::String(s),
            engine::record::Field::Bool(b) => Field::Bool(b),
        }
    }
}

impl From<DbDateTime> for DateTime {
    fn from(value: DbDateTime) -> Self {
        Self {
            days_since_epoch: value.days_since_epoch(),
            milliseconds_since_midnight: value.milliseconds_since_midnight(),
        }
    }
}

impl From<DbDate> for Date {
    fn from(value: DbDate) -> Self {
        Self {
            days_since_epoch: value.days_since_epoch(),
        }
    }
}
