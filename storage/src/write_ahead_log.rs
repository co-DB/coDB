use crate::paged_file::{Lsn, PageId};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, mpsc};

struct WalManager {
    current_lsn: Lsn,
    flushed_lsn: Arc<AtomicLsn>,
    unflushed_records: Vec<WalRecord>,
    recv: mpsc::Receiver<WalRequest>,
    log_file: BufWriter<File>,
}

impl WalManager {
    const FLUSH_THRESHOLD: usize = 1024;
    pub fn new(
        recv: mpsc::Receiver<WalRequest>,
        flushed_lsn: Arc<AtomicLsn>,
        log_path: impl AsRef<Path>,
    ) -> Self {
        let log_file = File::options()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();

        WalManager {
            current_lsn: 0,
            flushed_lsn,
            unflushed_records: Vec::with_capacity(Self::FLUSH_THRESHOLD),
            recv,
            log_file: BufWriter::new(log_file),
        }
    }

    pub fn run(&mut self) {
        while let Ok(req) = self.recv.recv() {
            match req {
                WalRequest::Write(write) => {
                    let lsn = self.append_record(write.data);
                    write.send.send(lsn).unwrap();
                }
                WalRequest::ForceFlush { response } => {
                    self.log_file.flush().unwrap();
                    self.flushed_lsn
                        .store(self.current_lsn, std::sync::atomic::Ordering::SeqCst);
                    response.send(()).unwrap()
                }
            }
        }
    }

    fn append_record(&mut self, record: WalRecordData) -> Lsn {
        let record_lsn = self.current_lsn + 1;
        self.current_lsn += 1;
        record_lsn
    }
}

type AtomicLsn = AtomicU64;

enum WalRequest {
    Write(WalRecord),
    ForceFlush { response: mpsc::SyncSender<()> },
}

struct WalRecord {
    data: WalRecordData,
    send: mpsc::SyncSender<Lsn>,
}

enum WalRecordData {
    SinglePageOperation(SinglePageOperation),
    MultiPageOperation(Vec<SinglePageOperation>),
}

struct SinglePageOperation {
    page_id: PageId,
    diff: Vec<u8>,
}
