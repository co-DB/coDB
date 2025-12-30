use crate::protocol_handler::{ProtocolHandler, ReadResult};
use crate::protocol_mappings::IntoProtocol;
use crate::workers_container::WorkersContainer;
use dashmap::{DashMap, Entry};
use engine::record::Record as EngineRecord;
use executor::response::StatementResult;
use executor::{Executor, ExecutorError};
use itertools::Itertools;
use log::{error, info};
use metadata::catalog_manager::{CatalogManager, CatalogManagerError};
use parking_lot::RwLock;
use protocol::{ErrorType, Record as ProtocolRecord, Request, Response, StatementType};
use rkyv::rancor;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub(crate) struct ClientHandler<P>
where
    P: ProtocolHandler,
{
    current_database: Option<String>,
    executors: Arc<DashMap<String, Arc<Executor>>>,
    catalog_manager: Arc<RwLock<CatalogManager>>,
    protocol_handler: P,
    workers: Arc<WorkersContainer>,
    shutdown: CancellationToken,
}

#[derive(Error, Debug)]
pub(crate) enum ClientError {
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

    #[error("failed to serialize binary message: {0}")]
    BinarySerializationError(rancor::Error),

    #[error("failed to deserialize binary message: {0}")]
    BinaryDeserializationError(rancor::Error),
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
            ClientError::BinarySerializationError(_) => ErrorType::Execution,
            ClientError::BinaryDeserializationError(_) => ErrorType::InvalidRequest,
        }
    }
}

impl<P> ClientHandler<P>
where
    P: ProtocolHandler,
{
    const CHANNEL_BUFFER_CAPACITY: usize = 16;

    const ROWS_CHUNK_SIZE: usize = 10000;

    pub(crate) fn new(
        executors: Arc<DashMap<String, Arc<Executor>>>,
        catalog_manager: Arc<RwLock<CatalogManager>>,
        protocol_handler: P,
        workers: Arc<WorkersContainer>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            protocol_handler,
            current_database: None,
            executors,
            catalog_manager,
            workers,
            shutdown,
        }
    }

    pub(crate) async fn run(mut self) {
        let shutdown = self.shutdown.clone();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Client handler received shutdown signal");
                    let _ = self.send_error(
                        "Server is shutting down",
                        ErrorType::Execution,
                    ).await;
                    break;
                }

                res = self.read_request() => {
                    match res {
                        Ok(ReadResult::Request(request)) => {
                            if let Err(e) = self.handle_request(request).await {
                                error!("Error handling request: {}", e);
                                let _ = self.send_error(e.to_string(), e.to_error_type()).await;
                            }
                        }
                        Ok(ReadResult::Disconnected) => break,
                        Ok(ReadResult::Empty) => continue,
                        Err(e) => {
                            error!("Error reading request: {}", e);
                            let _ = self.send_error(e.to_string(), e.to_error_type()).await;
                        }
                    }
                }
            }
        }
    }

    async fn read_request(&mut self) -> Result<ReadResult, ClientError> {
        self.protocol_handler.read_request().await
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
                let (executor, background_workers) =
                    Executor::with_background_workers(db_directory_path, catalog)
                        .map_err(ClientError::ExecutorError)?;

                let executor = Arc::new(executor);
                vacant_entry.insert(executor.clone());

                self.workers.add_many(background_workers.into_iter());

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
                    statement_type: ty.into_protocol(),
                })
                .await?;
            }

            StatementResult::SelectSuccessful { rows, columns } => {
                self.send_response(Response::ColumnInfo {
                    column_metadata: columns.into_iter().map(|cm| cm.into_protocol()).collect(),
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
                .map(|record| record.into_protocol())
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
        self.protocol_handler.send_response(response).await
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

#[cfg(test)]
mod client_handler_tests {
    use crate::client_handler::{ClientError, ClientHandler};
    use crate::protocol_handler::{BinaryProtocolHandler, TextProtocolHandler};
    use crate::workers_container::WorkersContainer;
    use dashmap::DashMap;
    use executor::Executor;
    use metadata::catalog_manager::CatalogManager;
    use parking_lot::RwLock;
    use protocol::{Request, Response, StatementType};
    use rkyv::rancor::Error;
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::sync::CancellationToken;

    /// Trait for test clients that can work with different protocols
    trait TestClient: Sized {
        async fn connect(addr: &str) -> Result<Self, ClientError>
        where
            Self: Sized;
        async fn send_request(&mut self, request: &Request) -> Result<(), ClientError>;
        async fn receive_response(&mut self) -> Result<Response, ClientError>;

        async fn send_and_receive(&mut self, request: &Request) -> Result<Response, ClientError> {
            self.send_request(request).await?;
            self.receive_response().await
        }
    }

    /// Text protocol test client
    struct TestTextClient {
        reader: BufReader<ReadHalf<TcpStream>>,
        writer: WriteHalf<TcpStream>,
    }

    impl TestTextClient {
        fn new(socket: TcpStream) -> Self {
            let (read_half, write_half) = tokio::io::split(socket);
            Self {
                reader: BufReader::new(read_half),
                writer: write_half,
            }
        }
    }

    impl TestClient for TestTextClient {
        async fn connect(addr: &str) -> Result<Self, ClientError> {
            let socket = TcpStream::connect(addr).await?;
            socket.set_nodelay(true)?;
            Ok(Self::new(socket))
        }

        async fn send_request(&mut self, request: &Request) -> Result<(), ClientError> {
            let request_json = serde_json::to_string(request)?;
            self.writer.write_all(request_json.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await?;
            Ok(())
        }

        async fn receive_response(&mut self) -> Result<Response, ClientError> {
            let mut response_line = String::new();
            self.reader.read_line(&mut response_line).await?;
            let response: Response = serde_json::from_str(response_line.trim())?;
            Ok(response)
        }
    }

    /// Binary protocol test client
    struct TestBinaryClient {
        reader: BufReader<ReadHalf<TcpStream>>,
        writer: WriteHalf<TcpStream>,
    }

    impl TestBinaryClient {
        fn new(socket: TcpStream) -> Self {
            let (read_half, write_half) = tokio::io::split(socket);
            Self {
                reader: BufReader::new(read_half),
                writer: write_half,
            }
        }
    }

    impl TestClient for TestBinaryClient {
        async fn connect(addr: &str) -> Result<Self, ClientError> {
            let socket = TcpStream::connect(addr).await?;
            Ok(Self::new(socket))
        }

        async fn send_request(&mut self, request: &Request) -> Result<(), ClientError> {
            let bytes =
                rkyv::to_bytes::<Error>(request).map_err(ClientError::BinarySerializationError)?;
            self.writer.write_u32(bytes.len() as u32).await?;
            self.writer.write_all(&bytes).await?;
            self.writer.flush().await?;
            Ok(())
        }

        async fn receive_response(&mut self) -> Result<Response, ClientError> {
            let length = self.reader.read_u32().await?;
            let mut buffer = vec![0u8; length as usize];
            self.reader.read_exact(&mut buffer).await?;

            let archived_response = rkyv::access::<protocol::ArchivedResponse, Error>(&buffer)
                .map_err(ClientError::BinaryDeserializationError)?;
            let response = rkyv::deserialize::<Response, Error>(archived_response)
                .map_err(ClientError::BinaryDeserializationError)?;
            Ok(response)
        }
    }

    /// Helper function to create test server components
    async fn setup_test_server() -> (
        Arc<DashMap<String, Arc<Executor>>>,
        Arc<RwLock<CatalogManager>>,
    ) {
        let executors = Arc::new(DashMap::new());

        let temp_dir = std::env::temp_dir().join(format!(
            "codb_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let catalog_manager = Arc::new(RwLock::new(
            CatalogManager::with_path(temp_dir).expect("Failed to create catalog manager"),
        ));
        (executors, catalog_manager)
    }

    /// Helper to spawn a test server with text protocol
    async fn spawn_test_text_handler() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let (executors, catalog_manager) = setup_test_server().await;

        let handle = tokio::spawn(async move {
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    let text_handler = TextProtocolHandler::from(socket);
                    let handler = ClientHandler::new(
                        executors.clone(),
                        catalog_manager.clone(),
                        text_handler,
                        Arc::new(WorkersContainer::new()),
                        CancellationToken::new(),
                    );
                    tokio::spawn(async move {
                        handler.run().await;
                    });
                }
            }
        });

        (addr.to_string(), handle)
    }

    /// Helper to spawn a test server with binary protocol
    async fn spawn_test_binary_handler() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let (executors, catalog_manager) = setup_test_server().await;

        let handle = tokio::spawn(async move {
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    let binary_handler = BinaryProtocolHandler::from(socket);
                    let handler = ClientHandler::new(
                        executors.clone(),
                        catalog_manager.clone(),
                        binary_handler,
                        Arc::new(WorkersContainer::new()),
                        CancellationToken::new(),
                    );
                    tokio::spawn(async move {
                        handler.run().await;
                    });
                }
            }
        });

        (addr.to_string(), handle)
    }

    // Generic test functions that work with any TestClient implementation
    async fn test_create_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");
        let request = Request::CreateDatabase {
            database_name: "test_create_db_unique".to_string(),
        };
        let response = client.send_and_receive(&request).await.unwrap();

        match response {
            Response::DatabaseCreated { database_name } => {
                assert_eq!(database_name, "test_create_db_unique");
            }
            _ => panic!("Expected DatabaseCreated response, got: {:?}", response),
        }
    }

    async fn test_list_databases_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_request = Request::CreateDatabase {
            database_name: "test_list_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        let list_request = Request::ListDatabases;
        let response = client.send_and_receive(&list_request).await.unwrap();

        match response {
            Response::DatabasesListed { database_names } => {
                assert!(
                    database_names.contains(&"test_list_db".to_string()),
                    "Database list should contain test_list_db"
                );
            }
            _ => panic!("Expected DatabasesListed response, got: {:?}", response),
        }
    }

    async fn test_connect_to_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_request = Request::CreateDatabase {
            database_name: "test_connect_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_connect_db".to_string(),
        };
        let response = client.send_and_receive(&connect_request).await.unwrap();

        match response {
            Response::Connected { database_name } => {
                assert_eq!(database_name, "test_connect_db");
            }
            _ => panic!("Expected Connected response, got: {:?}", response),
        }
    }

    async fn test_connect_to_nonexistent_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let connect_request = Request::Connect {
            database_name: "nonexistent_db".to_string(),
        };
        let response = client.send_and_receive(&connect_request).await.unwrap();

        match response {
            Response::Error { message, .. } => {
                assert!(message.contains("does not exist"));
            }
            _ => panic!("Expected Error response, got: {:?}", response),
        }
    }

    async fn test_delete_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_request = Request::CreateDatabase {
            database_name: "test_delete_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        let delete_request = Request::DeleteDatabase {
            database_name: "test_delete_db".to_string(),
        };
        let response = client.send_and_receive(&delete_request).await.unwrap();

        match response {
            Response::DatabaseDeleted { database_name } => {
                assert_eq!(database_name, "test_delete_db");
            }
            _ => panic!("Expected DatabaseDeleted response, got: {:?}", response),
        }
    }

    async fn test_query_without_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let query_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM test".to_string(),
        };

        let response = client.send_and_receive(&query_request).await.unwrap();
        match response {
            Response::Acknowledge => {}
            _ => panic!(
                "Expected Acknowledge as first response, got: {:?}",
                response
            ),
        }

        let error_response = client.receive_response().await.unwrap();
        match error_response {
            Response::Error { message, .. } => {
                assert!(message.contains("no database selected"));
            }
            _ => panic!(
                "Expected Error response for no database, got: {:?}",
                error_response
            ),
        }
    }

    async fn test_create_table_query_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_create_table_unique_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_create_table_unique_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let query_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };

        let response1 = client.send_and_receive(&query_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 0);
                assert!(matches!(statement_type, StatementType::CreateTable));
            }
            _ => panic!("Expected StatementCompleted response, got: {:?}", response2),
        }

        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));
    }

    async fn test_insert_query_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_insert_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_insert_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap();
        client.receive_response().await.unwrap();

        let insert_request = Request::Query {
            database_name: None,
            sql: "INSERT INTO products (id, name) VALUES (1, 'Product A');".to_string(),
        };

        let response1 = client.send_and_receive(&insert_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 1);
                assert!(matches!(statement_type, StatementType::Insert));
            }
            _ => panic!("Expected StatementCompleted response, got: {:?}", response2),
        }

        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));
    }

    async fn test_select_with_data_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_select_data_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_select_data_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE employees (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap();
        client.receive_response().await.unwrap();

        for i in 1..=3 {
            let insert_request = Request::Query {
                database_name: None,
                sql: format!(
                    "INSERT INTO employees (id, name) VALUES ({}, 'Employee {}');",
                    i, i
                ),
            };
            client.send_and_receive(&insert_request).await.unwrap();
            client.receive_response().await.unwrap();
            client.receive_response().await.unwrap();
        }

        let select_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM employees;".to_string(),
        };

        let response1 = client.send_and_receive(&select_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::ColumnInfo { column_metadata } => {
                assert_eq!(column_metadata.len(), 2);
            }
            _ => panic!("Expected ColumnInfo response, got: {:?}", response2),
        }

        let response3 = client.receive_response().await.unwrap();
        match response3 {
            Response::Rows { records, count } => {
                assert_eq!(count, 3);
                assert_eq!(records.len(), 3);
            }
            _ => panic!("Expected Rows response, got: {:?}", response3),
        }

        let response4 = client.receive_response().await.unwrap();
        match response4 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 3);
                assert!(matches!(statement_type, StatementType::Select));
            }
            _ => panic!("Expected StatementCompleted response, got: {:?}", response4),
        }

        let response5 = client.receive_response().await.unwrap();
        assert!(matches!(response5, Response::QueryCompleted));
    }

    async fn test_query_with_connected_database_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_query_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_query_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE items (id INT32 PRIMARY_KEY, value STRING);".to_string(),
        };

        let response1 = client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        assert!(matches!(response2, Response::StatementCompleted { .. }));

        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));
    }

    async fn test_client_disconnect_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_disconnect_db".to_string(),
        };
        let response = client.send_and_receive(&create_db_request).await.unwrap();

        match response {
            Response::DatabaseCreated { database_name } => {
                assert_eq!(database_name, "test_disconnect_db");
            }
            _ => panic!("Expected DatabaseCreated response, got: {:?}", response),
        }

        // Client will be dropped here, simulating disconnect
    }

    async fn test_select_empty_table_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_empty_select_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_empty_select_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE empty_table (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap();
        client.receive_response().await.unwrap();

        let select_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM empty_table;".to_string(),
        };

        let response1 = client.send_and_receive(&select_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::ColumnInfo { column_metadata } => {
                assert_eq!(column_metadata.len(), 2);
            }
            _ => panic!("Expected ColumnInfo response, got: {:?}", response2),
        }

        let response3 = client.receive_response().await.unwrap();
        match response3 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 0);
                assert!(matches!(statement_type, StatementType::Select));
            }
            _ => panic!("Expected StatementCompleted response, got: {:?}", response3),
        }

        let response4 = client.receive_response().await.unwrap();
        assert!(matches!(response4, Response::QueryCompleted));
    }

    async fn test_query_parse_error_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_parse_error_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_parse_error_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let invalid_query_request = Request::Query {
            database_name: None,
            sql: "INVALID SQL SYNTAX HERE".to_string(),
        };

        let response1 = client
            .send_and_receive(&invalid_query_request)
            .await
            .unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::Error {
                message,
                error_type,
            } => {
                assert!(matches!(error_type, protocol::ErrorType::Query));
                assert!(!message.is_empty());
            }
            _ => panic!("Expected Error response, got: {:?}", response2),
        }

        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));
    }

    async fn test_multiple_statements_in_query_generic<C: TestClient>(addr: &str) {
        let mut client = C::connect(addr).await.expect("Failed to connect");

        let create_db_request = Request::CreateDatabase {
            database_name: "test_multi_stmt_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_multi_stmt_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        // Create table first
        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE test (id INT32 PRIMARY_KEY);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted for CREATE
        client.receive_response().await.unwrap(); // QueryCompleted

        // Now test multiple statements: INSERT then SELECT
        let multi_statement_request = Request::Query {
            database_name: None,
            sql: "INSERT INTO test (id) VALUES (1); SELECT * FROM test;".to_string(),
        };

        let response1 = client
            .send_and_receive(&multi_statement_request)
            .await
            .unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // First statement: INSERT
        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 1);
                assert!(matches!(statement_type, StatementType::Insert));
            }
            _ => panic!(
                "Expected StatementCompleted for INSERT, got: {:?}",
                response2
            ),
        }

        // Second statement: SELECT - should have ColumnInfo first
        let response3 = client.receive_response().await.unwrap();
        match response3 {
            Response::ColumnInfo { column_metadata } => {
                assert_eq!(column_metadata.len(), 1); // Only 'id' column
            }
            _ => panic!("Expected ColumnInfo for SELECT, got: {:?}", response3),
        }

        // Then Rows
        let response4 = client.receive_response().await.unwrap();
        match response4 {
            Response::Rows { records, count } => {
                assert_eq!(count, 1);
                assert_eq!(records.len(), 1);
            }
            _ => panic!("Expected Rows for SELECT, got: {:?}", response4),
        }

        // Then StatementCompleted for SELECT
        let response5 = client.receive_response().await.unwrap();
        match response5 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 1);
                assert!(matches!(statement_type, StatementType::Select));
            }
            _ => panic!(
                "Expected StatementCompleted for SELECT, got: {:?}",
                response5
            ),
        }

        // Finally QueryCompleted
        let response6 = client.receive_response().await.unwrap();
        assert!(matches!(response6, Response::QueryCompleted));
    }

    // Text protocol tests
    #[tokio::test]
    async fn test_text_create_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_create_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_list_databases() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_list_databases_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_connect_to_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_connect_to_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_connect_to_nonexistent_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_connect_to_nonexistent_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_delete_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_delete_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_query_without_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_query_without_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_create_table_query() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_create_table_query_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_insert_query() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_insert_query_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_select_with_data() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_select_with_data_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_query_with_connected_database() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_query_with_connected_database_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_multiple_clients() {
        let (addr, handle) = spawn_test_text_handler().await;
        // Use unique names based on current time to avoid conflicts
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db1_name = format!("client1_db_{}", timestamp);
        let db2_name = format!("client2_db_{}", timestamp);

        // Spawn multiple clients concurrently
        let client1 = tokio::spawn({
            let addr = addr.to_string();
            let db_name = db1_name.clone();
            async move {
                let mut client = TestTextClient::connect(&addr).await.unwrap();
                let request = Request::CreateDatabase {
                    database_name: db_name,
                };
                client.send_and_receive(&request).await.unwrap()
            }
        });

        let client2 = tokio::spawn({
            let addr = addr.to_string();
            let db_name = db2_name.clone();
            async move {
                let mut client = TestTextClient::connect(&addr).await.unwrap();
                let request = Request::CreateDatabase {
                    database_name: db_name,
                };
                client.send_and_receive(&request).await.unwrap()
            }
        });

        let response1 = client1.await.unwrap();
        let response2 = client2.await.unwrap();

        match (&response1, &response2) {
            (
                Response::DatabaseCreated { database_name: db1 },
                Response::DatabaseCreated { database_name: db2 },
            ) => {
                assert_eq!(db1, &db1_name);
                assert_eq!(db2, &db2_name);
            }
            _ => panic!(
                "Expected both clients to successfully create databases, got: {:?} and {:?}",
                response1, response2
            ),
        }

        // Verify both databases exist by listing them
        let mut verify_client = TestTextClient::connect(&addr).await.unwrap();
        let list_request = Request::ListDatabases;
        let list_response = verify_client.send_and_receive(&list_request).await.unwrap();

        match list_response {
            Response::DatabasesListed { database_names } => {
                assert!(
                    database_names.contains(&db1_name),
                    "Database {} not found in list",
                    db1_name
                );
                assert!(
                    database_names.contains(&db2_name),
                    "Database {} not found in list",
                    db2_name
                );
            }
            _ => panic!(
                "Expected DatabasesListed response, got: {:?}",
                list_response
            ),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_client_disconnect() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_client_disconnect_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_select_empty_table() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_select_empty_table_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_query_parse_error() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_query_parse_error_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_multiple_statements_in_query() {
        let (addr, handle) = spawn_test_text_handler().await;
        test_multiple_statements_in_query_generic::<TestTextClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_empty_line_handling() {
        let (addr, handle) = spawn_test_text_handler().await;
        let mut client = TestTextClient::connect(&addr)
            .await
            .expect("Failed to connect");

        // Send empty line
        client.writer.write_all(b"\n").await.unwrap();
        client.writer.flush().await.unwrap();

        // Send valid request after empty line
        let request = Request::ListDatabases;
        let response = client.send_and_receive(&request).await.unwrap();

        assert!(matches!(response, Response::DatabasesListed { .. }));
        handle.abort();
    }

    #[tokio::test]
    async fn test_text_invalid_json_request() {
        let (addr, handle) = spawn_test_text_handler().await;
        let mut client = TestTextClient::connect(&addr)
            .await
            .expect("Failed to connect");

        // Send invalid JSON
        client.writer.write_all(b"{invalid json}\n").await.unwrap();
        client.writer.flush().await.unwrap();

        let response = client.receive_response().await.unwrap();
        match response {
            Response::Error {
                message,
                error_type,
            } => {
                assert!(matches!(error_type, protocol::ErrorType::InvalidRequest));
                assert!(message.contains("JSON") || message.contains("json"));
            }
            _ => panic!(
                "Expected Error response for invalid JSON, got: {:?}",
                response
            ),
        }
        handle.abort();
    }

    // Binary protocol tests
    #[tokio::test]
    async fn test_binary_create_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_create_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_list_databases() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_list_databases_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_connect_to_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_connect_to_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_connect_to_nonexistent_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_connect_to_nonexistent_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_delete_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_delete_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_query_without_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_query_without_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_create_table_query() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_create_table_query_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_insert_query() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_insert_query_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_select_with_data() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_select_with_data_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_query_with_connected_database() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_query_with_connected_database_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_multiple_clients() {
        let (addr, handle) = spawn_test_binary_handler().await;
        // Use unique names based on current time to avoid conflicts
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db1_name = format!("client1_db_{}", timestamp);
        let db2_name = format!("client2_db_{}", timestamp);

        // Spawn multiple clients concurrently
        let client1 = tokio::spawn({
            let addr = addr.to_string();
            let db_name = db1_name.clone();
            async move {
                let mut client = TestBinaryClient::connect(&addr).await.unwrap();
                let request = Request::CreateDatabase {
                    database_name: db_name,
                };
                client.send_and_receive(&request).await.unwrap()
            }
        });

        let client2 = tokio::spawn({
            let addr = addr.to_string();
            let db_name = db2_name.clone();
            async move {
                let mut client = TestBinaryClient::connect(&addr).await.unwrap();
                let request = Request::CreateDatabase {
                    database_name: db_name,
                };
                client.send_and_receive(&request).await.unwrap()
            }
        });

        let response1 = client1.await.unwrap();
        let response2 = client2.await.unwrap();

        match (&response1, &response2) {
            (
                Response::DatabaseCreated { database_name: db1 },
                Response::DatabaseCreated { database_name: db2 },
            ) => {
                assert_eq!(db1, &db1_name);
                assert_eq!(db2, &db2_name);
            }
            _ => panic!(
                "Expected both clients to successfully create databases, got: {:?} and {:?}",
                response1, response2
            ),
        }

        // Verify both databases exist by listing them
        let mut verify_client = TestBinaryClient::connect(&addr).await.unwrap();
        let list_request = Request::ListDatabases;
        let list_response = verify_client.send_and_receive(&list_request).await.unwrap();

        match list_response {
            Response::DatabasesListed { database_names } => {
                assert!(
                    database_names.contains(&db1_name),
                    "Database {} not found in list",
                    db1_name
                );
                assert!(
                    database_names.contains(&db2_name),
                    "Database {} not found in list",
                    db2_name
                );
            }
            _ => panic!(
                "Expected DatabasesListed response, got: {:?}",
                list_response
            ),
        }
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_client_disconnect() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_client_disconnect_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_select_empty_table() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_select_empty_table_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_query_parse_error() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_query_parse_error_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_multiple_statements_in_query() {
        let (addr, handle) = spawn_test_binary_handler().await;
        test_multiple_statements_in_query_generic::<TestBinaryClient>(&addr).await;
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_invalid_length() {
        let (addr, handle) = spawn_test_binary_handler().await;
        let socket = TcpStream::connect(&addr).await.expect("Failed to connect");
        let (_, mut writer) = tokio::io::split(socket);

        // Send invalid length (e.g., length that doesn't match actual data)
        writer.write_u32(1000).await.unwrap(); // Claim 1000 bytes
        writer.write_all(b"short data").await.unwrap(); // But send much less
        writer.flush().await.unwrap();

        // Connection should be closed or error should occur
        // We just verify the server doesn't crash
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_zero_length() {
        let (addr, handle) = spawn_test_binary_handler().await;
        let socket = TcpStream::connect(&addr).await.expect("Failed to connect");
        let (_, mut writer) = tokio::io::split(socket);

        // Send zero length (empty message)
        writer.write_u32(0).await.unwrap();
        writer.flush().await.unwrap();

        // Send valid request after
        let mut client = TestBinaryClient::new(TcpStream::connect(&addr).await.unwrap());
        let request = Request::ListDatabases;
        let response = client.send_and_receive(&request).await.unwrap();

        assert!(matches!(response, Response::DatabasesListed { .. }));
        handle.abort();
    }

    #[tokio::test]
    async fn test_binary_malformed_data() {
        let (addr, handle) = spawn_test_binary_handler().await;
        let socket = TcpStream::connect(&addr).await.expect("Failed to connect");
        let (_, mut writer) = tokio::io::split(socket);

        // Send random bytes that can't be deserialized
        let random_data = vec![0xFF; 100];
        writer.write_u32(random_data.len() as u32).await.unwrap();
        writer.write_all(&random_data).await.unwrap();
        writer.flush().await.unwrap();

        // Wait a bit to see if server crashes
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        handle.abort();
    }
}
