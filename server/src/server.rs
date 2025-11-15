use crate::text_protocol::{ErrorType, Request, Response};
use dashmap::DashMap;
use executor::{Executor, StatementResult};
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};

#[derive(Error, Debug)]
pub(crate) enum ServerError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}
pub(crate) struct Server {
    binary_addr: SocketAddr,
    text_addr: SocketAddr,
    executors: Arc<DashMap<String, Arc<Executor>>>,
}
impl Server {
    fn new(binary_addr: SocketAddr, text_addr: SocketAddr) -> Self {
        Self {
            binary_addr,
            text_addr,
            executors: Arc::new(DashMap::new()),
        }
    }
    async fn run_loop(&self) -> Result<(), ServerError> {
        let text_listener = TcpListener::bind(self.text_addr).await?;
        let binary_listener = TcpListener::bind(self.binary_addr).await?;

        let text_executors = self.executors.clone();
        tokio::spawn(async move {
            loop {
                match text_listener.accept().await {
                    Ok((socket, _)) => {
                        let exec = text_executors.clone();
                        tokio::spawn(async move {
                            Self::handle_text_client(socket, exec).await;
                        });
                    }
                    Err(err) => {
                        eprintln!("Error while handling text protocol client: {}", err);
                    }
                }
            }
        });

        let binary_executors = self.executors.clone();
        tokio::spawn(async move {
            loop {
                match binary_listener.accept().await {
                    Ok((socket, _)) => {
                        let exec = binary_executors.clone();
                        tokio::spawn(async move {
                            let _ = Self::handle_binary_client(socket, exec).await;
                        });
                    }
                    Err(err) => {
                        eprintln!("Error while handling binary protocol client: {}", err);
                    }
                }
            }
        });

        Ok(())
    }
    async fn handle_text_client(socket: TcpStream, executors: Arc<DashMap<String, Arc<Executor>>>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<StatementResult>(16);

        let (read_socket, write_socket) = socket.into_split();

        let mut reader = BufReader::new(read_socket);
        let mut writer = BufWriter::new(write_socket);

        let mut buffer = String::new();

        while reader.read_line(&mut buffer).await.is_ok() {
            let request = serde_json::from_str::<Request>(buffer.as_str());
            let deserialized_request = match request {
                Ok(req) => req,
                Err(err) => {
                    let error_response = Response::Error {
                        message: err.to_string(),
                        error_type: ErrorType::Communication,
                    };
                    writer
                        .write_all(serde_json::to_string(&error_response).unwrap().as_bytes())
                        .await
                        .unwrap();
                    writer.write_all("\n".as_bytes()).await.unwrap();
                    writer.flush().await.unwrap();
                    continue;
                }
            };

            match deserialized_request {
                Request::CreateDatabase { database_name } => {}
                Request::DeleteDatabase { database_name } => {}
                Request::ListDatabases => {}
                Request::Connect { database_name } => {}
                Request::Query { database_name, sql } => {}
            }
        }
        tokio::task::spawn_blocking(move || {
            /*if let Some(exec_arc) = executors.get("db1") {
                let exec = exec_arc.clone(); // clone Arc to get ownership
                for i in exec.execute("query") {
                    tx.blocking_send(i).unwrap();
                } // call method on Executor
            }*/
        });

        while let Some(result) = rx.recv().await {}
    }

    async fn handle_binary_client(
        socket: TcpStream,
        executors: Arc<DashMap<String, Arc<Executor>>>,
    ) {
    }
}
