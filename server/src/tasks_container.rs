use crossbeam::queue::SegQueue;
use log::error;
use tokio::task::JoinHandle;

pub(crate) struct TasksContainer {
    tasks: SegQueue<JoinHandle<()>>,
}

impl TasksContainer {
    pub fn new() -> Self {
        TasksContainer {
            tasks: SegQueue::new(),
        }
    }

    pub fn add(&self, handle: JoinHandle<()>) {
        self.tasks.push(handle);
    }

    pub async fn join_all(&self) {
        while let Some(handle) = self.tasks.pop() {
            if let Err(e) = handle.await {
                error!("Failed to join tokio task: {e}");
            };
        }
    }
}
