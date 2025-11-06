use std::{mem, sync::mpsc, thread};

use thiserror::Error;

/// Error for [`BackgroundWorker`] related operations.
#[derive(Debug, Error)]
pub enum BackgroundWorkerError {
    #[error("failed to shutdown: {0}")]
    FailedToShutdown(String),
    #[error("background worker was already shutdown")]
    AlreadyShutdown,
    #[error("failed to join background worker thread")]
    FailedToJoin,
}

/// Handle returned by starting [`BackgroundWorker`].
/// Can be used for shutting down worker and awaiting its completion.
pub struct BackgroundWorkerHandle {
    /// Handle to the [`BackgroundWorker`]'s thread.
    handle: thread::JoinHandle<()>,
    /// Sender end of the channel used for shutting down [`BackgroundWorker`].
    shutdown: Option<mpsc::Sender<()>>,
}

impl BackgroundWorkerHandle {
    /// Creates new [`BackgroundWorkerHandle`].
    pub(crate) fn new(handle: thread::JoinHandle<()>, shutdown: mpsc::Sender<()>) -> Self {
        BackgroundWorkerHandle {
            handle,
            shutdown: Some(shutdown),
        }
    }

    /// Sends signal via [`mpsc::Channel`] to [`BackgroundWorkerHandle::handle`] to shutdown.
    /// This function can only be called once for the whole lifetime of this struct.
    pub(crate) fn shutdown(&mut self) -> Result<(), BackgroundWorkerError> {
        let tx = mem::take(&mut self.shutdown);
        tx.ok_or(BackgroundWorkerError::AlreadyShutdown)?
            .send(())
            .map_err(|e| BackgroundWorkerError::FailedToShutdown(e.to_string()))
    }

    /// Awaits the completion of [`BackgroundWorkerHandle::handle`].
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
