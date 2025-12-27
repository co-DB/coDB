use crate::page_diff::PageDiff;
use crate::paged_file::{Lsn, PageId};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, mpsc};
use types::serialization::DbSerializable;

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
    diff: PageDiff,
}

/// Wal record is serialized as:
/// 1. LSN (u64)
/// 2. Total length (u32)
/// 2. Record type (u8)
/// 3. Number of page operations (if multipage)
/// 4. For each page operation:
///    a. Page ID (u32)
///    b. Number of diffs (u16)
///    c. For each diff:
///    i. Offset (u16)
///    ii. Length (u16)
///    iii. Data (variable length)
impl WalRecordData {
    fn type_id(&self) -> u8 {
        match &self {
            WalRecordData::SinglePageOperation(_) => 1,
            WalRecordData::MultiPageOperation(_) => 2,
        }
    }

    fn serialize(&self, lsn: Lsn) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(256);

        lsn.serialize(&mut buffer);

        // Placeholder for total length.
        buffer.extend(&[0; size_of::<u32>()]);

        self.type_id().serialize(&mut buffer);

        match &self {
            WalRecordData::SinglePageOperation(op) => {
                op.page_id.serialize(&mut buffer);
                op.diff.serialize(&mut buffer);
            }
            WalRecordData::MultiPageOperation(ops) => {
                (ops.len() as u16).serialize(&mut buffer);
                for op in ops {
                    op.page_id.serialize(&mut buffer);
                    op.diff.serialize(&mut buffer);
                }
            }
        }

        let total_length = (buffer.len() - size_of::<Lsn>()) as u32;

        buffer[size_of::<Lsn>()..size_of::<Lsn>() + size_of::<u32>()]
            .copy_from_slice(&total_length.to_le_bytes());

        buffer
    }
}
