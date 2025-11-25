use crate::text_client_handler::TextClientHandler;
use dashmap::DashMap;
use executor::Executor;
use metadata::catalog_manager::{CatalogManager, CatalogManagerError};
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};

#[derive(Error, Debug)]
pub(crate) enum ServerError {
    #[error("io error occurred: {0}")]
    IoError(#[from] std::io::Error),
    #[error("error occurred while using catalog manager: {0}")]
    CatalogManagerError(#[from] CatalogManagerError),
}
pub(crate) struct Server {
    binary_addr: SocketAddr,
    text_addr: SocketAddr,
    catalog_manager: Arc<RwLock<CatalogManager>>,
    executors: Arc<DashMap<String, Arc<Executor>>>,
}
impl Server {
    pub(crate) fn new(binary_addr: SocketAddr, text_addr: SocketAddr) -> Result<Self, ServerError> {
        Ok(Self {
            binary_addr,
            text_addr,
            catalog_manager: Arc::new(RwLock::new(CatalogManager::new()?)),
            executors: Arc::new(DashMap::new()),
        })
    }

    pub(crate) async fn run_loop(&self) -> Result<(), ServerError> {
        let text_listener = TcpListener::bind(self.text_addr).await?;
        let binary_listener = TcpListener::bind(self.binary_addr).await?;

        let text_executors = self.executors.clone();
        let catalog_manager = self.catalog_manager.clone();

        println!(
            "Listening to text protocol communication on {}",
            self.text_addr
        );
        tokio::spawn(async move {
            loop {
                match text_listener.accept().await {
                    Ok((socket, addr)) => {
                        println!("Accepted connection from {}", addr);
                        let handler = TextClientHandler::new(
                            socket,
                            text_executors.clone(),
                            catalog_manager.clone(),
                        );
                        tokio::spawn(async move {
                            handler.run().await;
                        });
                    }
                    Err(err) => {
                        eprintln!("Error while handling text protocol client: {}", err);
                    }
                }
            }
        });

        let binary_executors = self.executors.clone();
        let catalog_manager = self.catalog_manager.clone();
        println!(
            "Listening to binary protocol communication on {}",
            self.binary_addr
        );
        tokio::spawn(async move {
            loop {
                match binary_listener.accept().await {
                    Ok((socket, _)) => {
                        let exec = binary_executors.clone();
                        let manager = catalog_manager.clone();
                        tokio::spawn(async move {
                            let _ = Self::handle_binary_client(socket, exec, manager).await;
                        });
                    }
                    Err(err) => {
                        eprintln!("Error while handling binary protocol client: {}", err);
                    }
                }
            }
        });

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");

        Ok(())
    }

    // TODO: Move that to its own file and create it similarly to text client handler.
    async fn handle_binary_client(
        _socket: TcpStream,
        _executors: Arc<DashMap<String, Arc<Executor>>>,
        _catalog_manager: Arc<RwLock<CatalogManager>>,
    ) {
    }
}
