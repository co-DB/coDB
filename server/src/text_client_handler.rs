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

#[cfg(test)]
mod text_client_handler_tests {
    use crate::text_client_handler::{ClientError, TextClientHandler};
    use dashmap::DashMap;
    use executor::Executor;
    use metadata::catalog_manager::CatalogManager;
    use parking_lot::RwLock;
    use protocol::text_protocol::{Request, Response, StatementType};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
    use tokio::net::{TcpListener, TcpStream};

    /// Helper struct to manage split TCP socket for testing
    struct TestClient {
        reader: BufReader<ReadHalf<TcpStream>>,
        writer: WriteHalf<TcpStream>,
    }

    impl TestClient {
        /// Create a new test client from a TCP stream
        fn new(socket: TcpStream) -> Self {
            let (read_half, write_half) = tokio::io::split(socket);
            Self {
                reader: BufReader::new(read_half),
                writer: write_half,
            }
        }

        /// Connect to a server address
        async fn connect(addr: &str) -> Result<Self, ClientError> {
            let socket = TcpStream::connect(addr).await?;
            Ok(Self::new(socket))
        }

        /// Send a request to the server
        async fn send_request(&mut self, request: &Request) -> Result<(), ClientError> {
            let request_json = serde_json::to_string(request)?;
            self.writer.write_all(request_json.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await?;
            Ok(())
        }

        /// Receive a response from the server
        async fn receive_response(&mut self) -> Result<Response, ClientError> {
            let mut response_line = String::new();
            self.reader.read_line(&mut response_line).await?;
            let response: Response = serde_json::from_str(response_line.trim())?;
            Ok(response)
        }

        /// Send a request and receive a response
        async fn send_and_receive(&mut self, request: &Request) -> Result<Response, ClientError> {
            self.send_request(request).await?;
            self.receive_response().await
        }
    }

    /// Helper function to create test server components
    async fn setup_test_server() -> (
        Arc<DashMap<String, Arc<Executor>>>,
        Arc<RwLock<CatalogManager>>,
    ) {
        let executors = Arc::new(DashMap::new());

        // Create a unique temporary directory for each test
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

    /// Helper to spawn a test server and return the address and handles
    async fn spawn_test_handler() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let (executors, catalog_manager) = setup_test_server().await;

        let handle = tokio::spawn(async move {
            loop {
                if let Ok((socket, _)) = listener.accept().await {
                    let handler =
                        TextClientHandler::new(socket, executors.clone(), catalog_manager.clone());
                    tokio::spawn(async move {
                        handler.run().await;
                    });
                }
            }
        });

        (addr.to_string(), handle)
    }

    #[tokio::test]
    async fn test_create_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

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

        handle.abort();
    }

    #[tokio::test]
    async fn test_list_databases() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // First create a database
        let create_request = Request::CreateDatabase {
            database_name: "test_list_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        // Now list databases
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

        handle.abort();
    }

    #[tokio::test]
    async fn test_connect_to_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // First create a database
        let create_request = Request::CreateDatabase {
            database_name: "test_connect_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        // Now connect to it
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

        handle.abort();
    }

    #[tokio::test]
    async fn test_connect_to_nonexistent_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

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

        handle.abort();
    }

    #[tokio::test]
    async fn test_delete_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // First create a database
        let create_request = Request::CreateDatabase {
            database_name: "test_delete_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        // Now delete it
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

        handle.abort();
    }

    #[tokio::test]
    async fn test_query_without_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        let query_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM test".to_string(),
        };

        // First response should be Acknowledge
        let response = client.send_and_receive(&query_request).await.unwrap();
        match response {
            Response::Acknowledge => {
                // Expected - query was accepted
            }
            _ => panic!(
                "Expected Acknowledge as first response, got: {:?}",
                response
            ),
        }

        // Second response should be an error about no database selected
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

        handle.abort();
    }

    #[tokio::test]
    async fn test_query_with_connected_database() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Create database
        let create_request = Request::CreateDatabase {
            database_name: "test_query_db".to_string(),
        };
        client.send_and_receive(&create_request).await.unwrap();

        // Connect to database
        let connect_request = Request::Connect {
            database_name: "test_query_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        // Send a query (should get Acknowledge first)
        let query_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE test (id INT32)".to_string(),
        };
        let response = client.send_and_receive(&query_request).await.unwrap();

        match response {
            Response::Acknowledge => {
                // This is expected as first response
            }
            _ => panic!("Expected Acknowledge response, got: {:?}", response),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        let (addr, handle) = spawn_test_handler().await;

        // Use unique names based on current time to avoid conflicts
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db1_name = format!("client1_db_{}", timestamp);
        let db2_name = format!("client2_db_{}", timestamp);

        // Spawn multiple clients
        let client1 = tokio::spawn({
            let addr = addr.clone();
            let db_name = db1_name.clone();
            async move {
                let mut client = TestClient::connect(&addr).await.unwrap();
                let request = Request::CreateDatabase {
                    database_name: db_name,
                };
                client.send_and_receive(&request).await.unwrap()
            }
        });

        let client2 = tokio::spawn({
            let addr = addr.clone();
            let db_name = db2_name.clone();
            async move {
                let mut client = TestClient::connect(&addr).await.unwrap();
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

        handle.abort();
    }

    #[tokio::test]
    async fn test_client_disconnect() {
        let (addr, handle) = spawn_test_handler().await;

        let socket = TcpStream::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Simply drop the socket to simulate disconnect
        drop(socket);

        // Give the server a moment to process the disconnect
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Server should still be running and accept new connections
        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect after first client disconnected");

        let request = Request::ListDatabases;
        let response = client.send_and_receive(&request).await.unwrap();

        match response {
            Response::DatabasesListed { .. } => {
                // Expected
            }
            _ => panic!("Expected DatabasesListed response"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_empty_line_handling() {
        let (addr, handle) = spawn_test_handler().await;

        let socket = TcpStream::connect(&addr)
            .await
            .expect("Failed to connect to server");

        let (read_half, mut write_half) = tokio::io::split(socket);
        let mut reader = BufReader::new(read_half);

        // Send empty line
        write_half.write_all(b"\n").await.unwrap();
        write_half.flush().await.unwrap();

        // Now send actual request
        let request = Request::ListDatabases;
        let json = serde_json::to_string(&request).unwrap();
        write_half.write_all(json.as_bytes()).await.unwrap();
        write_half.write_all(b"\n").await.unwrap();
        write_half.flush().await.unwrap();

        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let response: Response = serde_json::from_str(line.trim()).unwrap();

        match response {
            Response::DatabasesListed { .. } => {
                // Server should handle empty lines gracefully
            }
            _ => panic!("Expected DatabasesListed response after empty line"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_invalid_json_request() {
        let (addr, handle) = spawn_test_handler().await;

        let socket = TcpStream::connect(&addr)
            .await
            .expect("Failed to connect to server");

        let (read_half, mut write_half) = tokio::io::split(socket);
        let mut reader = BufReader::new(read_half);

        // Send invalid JSON
        write_half.write_all(b"not a valid json\n").await.unwrap();
        write_half.flush().await.unwrap();

        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let response: Response = serde_json::from_str(line.trim()).unwrap();

        match response {
            Response::Error { error_type, .. } => {
                assert!(matches!(
                    error_type,
                    protocol::text_protocol::ErrorType::InvalidRequest
                ));
            }
            _ => panic!("Expected Error response for invalid JSON"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_create_table_query() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Create database
        let create_db_request = Request::CreateDatabase {
            database_name: "test_create_table_unique_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        // Connect to database
        let connect_request = Request::Connect {
            database_name: "test_create_table_unique_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        // Create table
        let query_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE users (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&query_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be StatementCompleted
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

        // Third response should be QueryCompleted
        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_insert_query() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Create and connect to database
        let create_db_request = Request::CreateDatabase {
            database_name: "test_insert_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_insert_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        // Create table
        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE products (id INT32 PRIMARY_KEY, name STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Insert data
        let insert_request = Request::Query {
            database_name: None,
            sql: "INSERT INTO products (id, name) VALUES (1, 'Product A');".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&insert_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be StatementCompleted
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

        // Third response should be QueryCompleted
        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_select_empty_table() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Setup database and table
        let create_db_request = Request::CreateDatabase {
            database_name: "test_select_empty_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_select_empty_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE items (id INT32 PRIMARY_KEY, description STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Select from empty table
        let select_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM items;".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&select_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be ColumnInfo
        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::ColumnInfo { column_metadata } => {
                assert_eq!(column_metadata.len(), 2);
            }
            _ => panic!("Expected ColumnInfo response, got: {:?}", response2),
        }

        // Third response should be StatementCompleted (with 0 rows)
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

        // Fourth response should be QueryCompleted
        let response4 = client.receive_response().await.unwrap();
        assert!(matches!(response4, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_select_with_data() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Setup database and table
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
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Insert some data
        for i in 1..=3 {
            let insert_request = Request::Query {
                database_name: None,
                sql: format!(
                    "INSERT INTO employees (id, name) VALUES ({}, 'Employee {}');",
                    i, i
                ),
            };
            client.send_and_receive(&insert_request).await.unwrap();
            client.receive_response().await.unwrap(); // StatementCompleted
            client.receive_response().await.unwrap(); // QueryCompleted
        }

        // Select data
        let select_request = Request::Query {
            database_name: None,
            sql: "SELECT * FROM employees;".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&select_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be ColumnInfo
        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::ColumnInfo { column_metadata } => {
                assert_eq!(column_metadata.len(), 2);
            }
            _ => panic!("Expected ColumnInfo response, got: {:?}", response2),
        }

        // Third response should be Rows
        let response3 = client.receive_response().await.unwrap();
        match response3 {
            Response::Rows { records, count } => {
                assert_eq!(count, 3);
                assert_eq!(records.len(), 3);
            }
            _ => panic!("Expected Rows response, got: {:?}", response3),
        }

        // Fourth response should be StatementCompleted
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

        // Fifth response should be QueryCompleted
        let response5 = client.receive_response().await.unwrap();
        assert!(matches!(response5, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_delete_query() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Setup database and table with data
        let create_db_request = Request::CreateDatabase {
            database_name: "test_delete_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_delete_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        let create_table_request = Request::Query {
            database_name: None,
            sql: "CREATE TABLE orders (id INT32 PRIMARY_KEY, status STRING);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Insert data
        let insert_request = Request::Query {
            database_name: None,
            sql: "INSERT INTO orders (id, status) VALUES (1, 'pending');".to_string(),
        };
        client.send_and_receive(&insert_request).await.unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Delete data
        let delete_request = Request::Query {
            database_name: None,
            sql: "DELETE FROM orders WHERE id = 1;".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&delete_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be StatementCompleted
        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 1);
                assert!(matches!(statement_type, StatementType::Delete));
            }
            _ => panic!("Expected StatementCompleted response, got: {:?}", response2),
        }

        // Third response should be QueryCompleted
        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_query_parse_error() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Setup database
        let create_db_request = Request::CreateDatabase {
            database_name: "test_parse_error_db".to_string(),
        };
        client.send_and_receive(&create_db_request).await.unwrap();

        let connect_request = Request::Connect {
            database_name: "test_parse_error_db".to_string(),
        };
        client.send_and_receive(&connect_request).await.unwrap();

        // Send invalid SQL
        let query_request = Request::Query {
            database_name: None,
            sql: "INVALID SQL SYNTAX HERE".to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&query_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be Error
        let response2 = client.receive_response().await.unwrap();
        match response2 {
            Response::Error { error_type, .. } => {
                assert!(matches!(
                    error_type,
                    protocol::text_protocol::ErrorType::Query
                ));
            }
            _ => panic!(
                "Expected Error response for parse error, got: {:?}",
                response2
            ),
        }

        // Third response should be QueryCompleted
        let response3 = client.receive_response().await.unwrap();
        assert!(matches!(response3, Response::QueryCompleted));

        handle.abort();
    }

    #[tokio::test]
    async fn test_multiple_statements_in_query() {
        let (addr, handle) = spawn_test_handler().await;

        let mut client = TestClient::connect(&addr)
            .await
            .expect("Failed to connect to server");

        // Setup database
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
            sql: "CREATE TABLE test (id INT32 PRIMARY_KEY, value INT32);".to_string(),
        };
        client
            .send_and_receive(&create_table_request)
            .await
            .unwrap();
        client.receive_response().await.unwrap(); // StatementCompleted
        client.receive_response().await.unwrap(); // QueryCompleted

        // Send multiple INSERT statements
        let query_request = Request::Query {
            database_name: None,
            sql: "INSERT INTO test (id, value) VALUES (1, 10); INSERT INTO test (id, value) VALUES (2, 20);"
                .to_string(),
        };

        // First response should be Acknowledge
        let response1 = client.send_and_receive(&query_request).await.unwrap();
        assert!(matches!(response1, Response::Acknowledge));

        // Second response should be StatementCompleted for first INSERT
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
                "Expected StatementCompleted for first INSERT, got: {:?}",
                response2
            ),
        }

        // Third response should be StatementCompleted for second INSERT
        let response3 = client.receive_response().await.unwrap();
        match response3 {
            Response::StatementCompleted {
                rows_affected,
                statement_type,
            } => {
                assert_eq!(rows_affected, 1);
                assert!(matches!(statement_type, StatementType::Insert));
            }
            _ => panic!(
                "Expected StatementCompleted for second INSERT, got: {:?}",
                response3
            ),
        }

        // Fourth response should be QueryCompleted
        let response4 = client.receive_response().await.unwrap();
        assert!(matches!(response4, Response::QueryCompleted));

        handle.abort();
    }
}
