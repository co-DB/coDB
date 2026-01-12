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
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::sync::Arc;
use storage::background_worker::BackgroundWorkerHandle;
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
    executors: Arc<DashMap<String, Arc<ExecutorWithWorkers>>>,
    tasks: Arc<TasksContainer>,
    shutdown: CancellationToken,
}

pub(crate) struct ExecutorWithWorkers {
    executor: ManuallyDrop<Executor>,
    workers: WorkersContainer,
    wal_worker: Option<BackgroundWorkerHandle>,
}

impl ExecutorWithWorkers {
    pub(crate) fn new(
        executor: Executor,
        workers: WorkersContainer,
        wal_worker: Option<BackgroundWorkerHandle>,
    ) -> Self {
        Self {
            executor: ManuallyDrop::new(executor),
            workers,
            wal_worker,
        }
    }

    pub(crate) fn shutdown_workers(&self) {
        self.workers.shutdown()
    }

    pub(crate) fn shutdown_wal_worker(&mut self) {
        let mut wal_worker = match self.wal_worker.take() {
            Some(worker) => worker,
            None => return,
        };

        if let Err(e) = wal_worker.shutdown() {
            error!("Failed to stop WAL background worker: {e}");
        }

        if let Err(e) = wal_worker.join() {
            error!("Failed to join background worker: {e}");
        }
    }

    pub(crate) fn executor(&self) -> &Executor {
        &self.executor
    }
}

impl Drop for ExecutorWithWorkers {
    fn drop(&mut self) {
        // 1. Shutdown workers
        self.shutdown_workers();

        // 2. Explicitly drop executor BEFORE shutting down WAL worker
        // SAFETY: This is the Drop implementation, so this is the last time
        // we access executor. The ManuallyDrop prevents automatic drop.
        unsafe {
            ManuallyDrop::drop(&mut self.executor);
        }

        // 3. Shutdown WAL worker
        self.shutdown_wal_worker();
    }
}

struct ListenerContext {
    addr: SocketAddr,
    executors: Arc<DashMap<String, Arc<ExecutorWithWorkers>>>,
    catalog_manager: Arc<RwLock<CatalogManager>>,
    tasks: Arc<TasksContainer>,
    shutdown: CancellationToken,
}

impl Server {
    pub(crate) fn new(binary_addr: SocketAddr, text_addr: SocketAddr) -> Result<Self, ServerError> {
        Ok(Self {
            binary_addr,
            text_addr,
            executors: Arc::new(DashMap::new()),
            catalog_manager: Arc::new(RwLock::new(CatalogManager::new()?)),
            tasks: Arc::new(TasksContainer::new()),
            shutdown: CancellationToken::new(),
        })
    }

    pub(crate) async fn run_loop(&self) -> Result<(), ServerError> {
        self.start_listener(
            self.text_listener_context(),
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
            self.binary_listener_context(),
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

        Ok(())
    }

    async fn start_listener<F>(
        &self,
        context: ListenerContext,
        handler: F,
    ) -> Result<(), ServerError>
    where
        F: Fn(
                TcpStream,
                Arc<DashMap<String, Arc<ExecutorWithWorkers>>>,
                Arc<RwLock<CatalogManager>>,
                Arc<TasksContainer>,
                CancellationToken,
            ) + Send
            + Sync
            + 'static,
    {
        let listener = TcpListener::bind(context.addr).await?;
        info!("Listening on {}", context.addr);

        let handler = Arc::new(handler);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = context.shutdown.cancelled() => {
                        info!("Listener on {} shutting down", context.addr);
                        break;
                    }
                    res = listener.accept() => {
                        let (socket, client_addr) = match res {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Error while accepting connection on listener {}: {}", context.addr, e);
                                continue;
                            }
                        };
                        info!("Accepted connection from {}", client_addr);

                        if let Err(e) = socket.set_nodelay(true) {
                            error!("Failed to set TCP_NODELAY on {}: {}", client_addr, e);
                        }

                        let child_shutdown = context.shutdown.child_token();
                        handler(socket, context.executors.clone(), context.catalog_manager.clone(), context.tasks.clone(), child_shutdown);
                    }
                }
            }
        });

        self.tasks.add(handle);

        Ok(())
    }

    fn listener_context(&self, addr: SocketAddr) -> ListenerContext {
        ListenerContext {
            addr,
            executors: self.executors.clone(),
            catalog_manager: self.catalog_manager.clone(),
            tasks: self.tasks.clone(),
            shutdown: self.shutdown.child_token(),
        }
    }

    fn binary_listener_context(&self) -> ListenerContext {
        self.listener_context(self.binary_addr)
    }

    fn text_listener_context(&self) -> ListenerContext {
        self.listener_context(self.text_addr)
    }
}
