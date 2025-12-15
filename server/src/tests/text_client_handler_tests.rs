use crate::text_client_handler::{ClientError, TextClientHandler};
use dashmap::DashMap;
use executor::Executor;
use metadata::catalog_manager::CatalogManager;
use parking_lot::RwLock;
use protocol::text_protocol::{Request, Response};
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
    let catalog_manager = Arc::new(RwLock::new(
        CatalogManager::new().expect("Failed to create catalog manager"),
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
        database_name: "test_db".to_string(),
    };

    let response = client.send_and_receive(&request).await.unwrap();

    match response {
        Response::DatabaseCreated { database_name } => {
            assert_eq!(database_name, "test_db");
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
        Response::Error { message, .. } => {
            // Also acceptable if there are parse/execution errors
            println!("Query resulted in error: {}", message);
        }
        _ => panic!(
            "Expected Acknowledge or Error response, got: {:?}",
            response
        ),
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
