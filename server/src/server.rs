use crate::{
    client_handler::ClientHandler,
    protocol_handler::{BinaryProtocolHandler, TextProtocolHandler},
    tasks_container::TasksContainer,
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
use tokio_util::sync::CancellationToken;

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
    tasks: Arc<TasksContainer>,
    shutdown: CancellationToken,
}

impl Server {
    pub(crate) fn new(binary_addr: SocketAddr, text_addr: SocketAddr) -> Result<Self, ServerError> {
        Ok(Self {
            binary_addr,
            text_addr,
            catalog_manager: Arc::new(RwLock::new(CatalogManager::new()?)),
            executors: Arc::new(DashMap::new()),
            background_workers: Arc::new(WorkersContainer::new()),
            tasks: Arc::new(TasksContainer::new()),
            shutdown: CancellationToken::new(),
        })
    }

    pub(crate) async fn run_loop(&self) -> Result<(), ServerError> {
        self.start_listener(
            self.text_addr,
            self.executors.clone(),
            self.catalog_manager.clone(),
            self.tasks.clone(),
            self.shutdown.child_token(),
            |socket, executors, manager, tasks, shutdown| {
                let handle = tokio::spawn(async move {
                    let text_handler = TextProtocolHandler::from(socket);
                    let handler = ClientHandler::new(executors, manager, text_handler, shutdown);
                    handler.run().await;
                });
                tasks.add(handle);
            },
        )
        .await?;

        self.start_listener(
            self.binary_addr,
            self.executors.clone(),
            self.catalog_manager.clone(),
            self.tasks.clone(),
            self.shutdown.child_token(),
            |socket, executors, manager, tasks, shutdown| {
                let handle = tokio::spawn(async move {
                    let binary_handler = BinaryProtocolHandler::from(socket);
                    let handler = ClientHandler::new(executors, manager, binary_handler, shutdown);
                    handler.run().await;
                });
                tasks.add(handle);
            },
        )
        .await?;

        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for Ctrl+C");

        info!("Starting server shutdown...");
        // Stop other tokio threads
        self.shutdown.cancel();
        self.tasks.join_all().await;

        // Stop background workers
        self.background_workers.shutdown();

        Ok(())
    }

    async fn start_listener<F>(
        &self,
        addr: SocketAddr,
        executors: Arc<DashMap<String, Arc<Executor>>>,
        catalog_manager: Arc<RwLock<CatalogManager>>,
        tasks: Arc<TasksContainer>,
        shutdown: CancellationToken,
        handler: F,
    ) -> Result<(), ServerError>
    where
        F: Fn(
                TcpStream,
                Arc<DashMap<String, Arc<Executor>>>,
                Arc<RwLock<CatalogManager>>,
                Arc<TasksContainer>,
                CancellationToken,
            ) + Send
            + Sync
            + 'static,
    {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on {}", addr);

        let handler = Arc::new(handler);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        info!("Listener on {} shutting down", addr);
                        break;
                    }
                    res = listener.accept() => {
                        let (socket, addr) = match res {
                            Ok(v) => {
                                info!("Accepted connection from {}", addr);
                                v
                            },
                            Err(e) => {
                                error!("Error while accepting connection on {}: {}", addr, e);
                                continue;
                            }
                        };
                        if let Err(e) = socket.set_nodelay(true) {
                            error!("Failed to set TCP_NODELAY on {}: {}", addr, e);
                        }

                        let child_shutdown = shutdown.child_token();
                        handler(socket, executors.clone(), catalog_manager.clone(), tasks.clone(), child_shutdown);
                    }
                }
            }
        });

        self.tasks.add(handle);

        Ok(())
    }
}
