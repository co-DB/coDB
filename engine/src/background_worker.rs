use std::{sync::mpsc, thread};

use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum BackgroundWorkerError {
    #[error("failed to shutdown: {0}")]
    FailedToShutdown(String),
    #[error("failed to joined background worker thread")]
    FailedToJoin,
}

pub(crate) struct BackgroundWorkerHandle {
    handle: thread::JoinHandle<()>,
    shutdown: mpsc::Sender<()>,
}

impl BackgroundWorkerHandle {
    pub(crate) fn new(handle: thread::JoinHandle<()>, shutdown: mpsc::Sender<()>) -> Self {
        BackgroundWorkerHandle { handle, shutdown }
    }

    pub(crate) fn shutdown(&self) -> Result<(), BackgroundWorkerError> {
        self.shutdown
            .send(())
            .map_err(|e| BackgroundWorkerError::FailedToShutdown(e.to_string()))
    }

    pub(crate) fn join(self) -> Result<(), BackgroundWorkerError> {
        self.handle
            .join()
            .map_err(|_| BackgroundWorkerError::FailedToJoin)
    }
}

pub(crate) trait BackgroundWorker {
    type BackgroundWorkerParams;

    fn start(params: Self::BackgroundWorkerParams) -> BackgroundWorkerHandle;
}
