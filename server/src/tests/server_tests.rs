use crate::server::Server;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_server_creation() {
    // Test that we can create a server successfully
    let result = Server::new(
        "127.0.0.1:0".parse().unwrap(), // Port 0 means OS will assign a free port
        "127.0.0.1:0".parse().unwrap(),
    );

    assert!(result.is_ok(), "Server creation should succeed");
}

#[tokio::test]
async fn test_server_creation_with_different_addresses() {
    // Test server creation with different valid addresses
    let result = Server::new("0.0.0.0:0".parse().unwrap(), "0.0.0.0:0".parse().unwrap());

    assert!(result.is_ok(), "Server should accept 0.0.0.0 addresses");
}

#[tokio::test]
async fn test_server_starts_listeners() {
    // Test that the server starts and binds to the specified ports
    let server = Server::new(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    )
    .expect("Server creation failed");

    // Spawn the server in a background task
    let server_handle = tokio::spawn(async move {
        // We'll timeout after a short duration since we can't actually send Ctrl+C
        tokio::select! {
            _ = server.run_loop() => {},
            _ = sleep(Duration::from_millis(100)) => {},
        }
    });

    // Give the server a moment to start
    sleep(Duration::from_millis(50)).await;

    // The server should still be running
    assert!(!server_handle.is_finished(), "Server should be running");

    // Clean up
    server_handle.abort();
}
