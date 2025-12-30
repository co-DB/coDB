use crate::{
    client_handler::ClientHandler,
    protocol_handler::{BinaryProtocolHandler, TextProtocolHandler},
    workers_container::WorkersContainer,
};
use dashmap::DashMap;
use executor::Executor;
use log::{error, info};
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
    background_workers: Arc<WorkersContainer>,
}

impl Server {
    pub(crate) fn new(binary_addr: SocketAddr, text_addr: SocketAddr) -> Result<Self, ServerError> {
        Ok(Self {
            binary_addr,
            text_addr,
            catalog_manager: Arc::new(RwLock::new(CatalogManager::new()?)),
            executors: Arc::new(DashMap::new()),
            background_workers: Arc::new(WorkersContainer::new()),
        })
    }

    pub(crate) async fn run_loop(&self) -> Result<(), ServerError> {
        self.start_listener(
            self.text_addr,
            self.executors.clone(),
            self.catalog_manager.clone(),
            |socket, executors, manager| {
                tokio::spawn(async move {
                    let text_handler = TextProtocolHandler::from(socket);
                    let handler = ClientHandler::new(executors, manager, text_handler);
                    handler.run().await;
                });
            },
        )
        .await?;

        self.start_listener(
            self.binary_addr,
            self.executors.clone(),
            self.catalog_manager.clone(),
            |socket, executors, manager| {
                tokio::spawn(async move {
                    let binary_handler = BinaryProtocolHandler::from(socket);
                    let handler = ClientHandler::new(executors, manager, binary_handler);
                    handler.run().await;
                });
            },
        )
        .await?;

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");

        Ok(())
    }

    async fn start_listener<F>(
        &self,
        addr: SocketAddr,
        executors: Arc<DashMap<String, Arc<Executor>>>,
        catalog_manager: Arc<RwLock<CatalogManager>>,
        handler: F,
    ) -> Result<(), ServerError>
    where
        F: Fn(TcpStream, Arc<DashMap<String, Arc<Executor>>>, Arc<RwLock<CatalogManager>>)
            + Send
            + Sync
            + 'static,
    {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}", addr);

        let handler = Arc::new(handler);

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((socket, addr)) => {
                        info!("Accepted connection from {}", addr);
                        if let Err(e) = socket.set_nodelay(true) {
                            error!("Failed to set TCP_NODELAY on {}: {}", addr, e);
                        }
                        let h = handler.clone();
                        let exec = executors.clone();
                        let manager = catalog_manager.clone();

                        h(socket, exec, manager);
                    }
                    Err(err) => {
                        error!("Error while accepting connection on {}: {}", addr, err);
                    }
                }
            }
        });

        Ok(())
    }
}
