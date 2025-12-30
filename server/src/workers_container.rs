use crossbeam::queue::SegQueue;
use log::error;
use storage::background_worker::BackgroundWorkerHandle;

pub(crate) struct WorkersContainer {
    workers: SegQueue<BackgroundWorkerHandle>,
}

impl WorkersContainer {
    pub fn new() -> Self {
        WorkersContainer {
            workers: SegQueue::new(),
        }
    }

    pub fn add(&self, handle: BackgroundWorkerHandle) {
        self.workers.push(handle);
    }

    pub fn shutdown(&self) {
        let mut stopped = Vec::with_capacity(self.workers.len());

        // Send shutdown signal to each worker
        while let Some(mut worker) = self.workers.pop() {
            if let Err(e) = worker.shutdown() {
                error!("Failed to stop background worker: {e}");
                continue;
            }
            stopped.push(worker);
        }

        // Wait for all workers to complete
        for worker in stopped {
            if let Err(e) = worker.join() {
                error!("Failed to join background worker: {e}");
            }
        }
    }
}
