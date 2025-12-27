use crate::page_diff::PageDiff;
use crate::paged_file::{Lsn, PageId};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use std::{io, time};
use thiserror::Error;
use types::serialization::DbSerializable;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error occurred: {0}")]
    IoError(#[from] io::Error),
    #[error("Corrupted record found at lsn: {lsn} - {reason}")]
    CorruptedRecord { lsn: Lsn, reason: String },
    #[error("Failed to send response: {0}")]
    SendError(String),
    #[error("WAL recovery failed: {0}")]
    RecoveryFailed(String),
}

/// Result of WAL recovery process.
pub struct RecoveryResult {
    /// Records that need to be replayed (after last checkpoint).
    pub records_to_replay: Vec<(Lsn, WalRecordData)>,
    /// The LSN to continue from.
    pub last_lsn: Lsn,
    /// The last checkpoint LSN found.
    pub last_checkpoint_lsn: Lsn,
}

pub struct WalManager {
    /// The highest LSN assigned so far.
    current_lsn: Lsn,
    /// The highest LSN for which all records have been flushed to disk both from cache and WAL.
    checkpoint_lsn: Lsn,
    /// The highest LSN that has been flushed to disk. Atomic for cross-thread access.
    flushed_lsn: Arc<AtomicLsn>,
    /// Receiver for incoming WAL requests.
    recv: mpsc::Receiver<WalRequest>,
    /// Path to the file where WAL records are stored.
    log_path: PathBuf,
    /// Buffered writer for the WAL file.
    log_file: BufWriter<File>,
    /// Number of records written since last checkpoint.
    records_since_checkpoint: usize,
}

impl WalManager {
    const FLUSH_THRESHOLD: usize = 1024;

    /// Creates WalManager after recovering from existing log file.
    /// Returns the manager and recovery result for replay.
    pub fn with_recovery(
        recv: mpsc::Receiver<WalRequest>,
        flushed_lsn: Arc<AtomicLsn>,
        log_path: impl AsRef<Path>,
    ) -> Result<(Self, RecoveryResult), WalError> {
        let path = log_path.as_ref();
        let recovery = Self::recover_from_log(path)?;

        let log_file = File::options().create(true).append(true).open(path)?;

        let manager = WalManager {
            current_lsn: recovery.last_lsn,
            checkpoint_lsn: recovery.last_checkpoint_lsn,
            flushed_lsn,
            recv,
            log_path: path.to_path_buf(),
            log_file: BufWriter::new(log_file),
            records_since_checkpoint: recovery.records_to_replay.len(),
        };

        Ok((manager, recovery))
    }

    pub fn run(&mut self) {
        while let Ok(req) = self.recv.recv() {
            if let Err(err) = self.handle_request(req) {
                // Figure out what to do on error.
                eprintln!("WAL error: {}", err);
            }
        }
    }

    fn handle_request(&mut self, req: WalRequest) -> Result<(), WalError> {
        match req {
            WalRequest::Write(record) => self.handle_write(record),
            WalRequest::ForceFlush { sender } => self.handle_flush(sender),
            WalRequest::Checkpoint { sender } => self.handle_checkpoint(sender),
        }
    }

    fn handle_write(&mut self, record: WalRecord) -> Result<(), WalError> {
        match self.append_record(record.data) {
            Ok(lsn) => {
                self.records_since_checkpoint += 1;
                // Ignore send error - client may have disconnected
                let _ = record.send.send(Some(lsn));
                Ok(())
            }
            Err(err) => {
                // Notify client of failure (None = failed)
                let _ = record.send.send(None);
                Err(err)
            }
        }
    }

    fn append_record(&mut self, record: WalRecordData) -> Result<Lsn, WalError> {
        self.current_lsn += 1;
        let record_lsn = self.current_lsn;
        let serialized_record = record.serialize(record_lsn);
        self.log_file.write_all(&serialized_record)?;
        Ok(record_lsn)
    }

    fn handle_flush(&mut self, sender: SyncSender<bool>) -> Result<(), WalError> {
        let result = self.flush_internal();
        let _ = sender.send(result.is_ok());
        result
    }

    fn flush_internal(&mut self) -> Result<(), WalError> {
        self.log_file.flush()?;
        self.flushed_lsn.store(self.current_lsn, Ordering::SeqCst);
        Ok(())
    }

    fn handle_checkpoint(&mut self, sender: SyncSender<Option<Lsn>>) -> Result<(), WalError> {
        let result = self.perform_checkpoint();
        let lsn = match &result {
            Ok(lsn) => Some(*lsn),
            Err(_) => None,
        };
        let _ = sender.send(lsn);
        result.map(|_| {})
    }

    /// Performs a checkpoint - writes checkpoint record and truncates WAL file.
    ///
    /// The caller must ensure all dirty pages have been flushed to disk before calling this.
    fn perform_checkpoint(&mut self) -> Result<Lsn, WalError> {
        // Write checkpoint record
        let checkpoint_lsn = self.current_lsn;
        let checkpoint_record = WalRecordData::Checkpoint { checkpoint_lsn };
        let new_lsn = self.append_record(checkpoint_record)?;

        // Flush the checkpoint record
        self.log_file.flush()?;

        // Truncate WAL - everything before checkpoint is no longer needed
        self.truncate_before_checkpoint()?;

        self.checkpoint_lsn = checkpoint_lsn;
        self.records_since_checkpoint = 0;

        Ok(new_lsn)
    }

    /// Truncates WAL file, keeping only the checkpoint record.
    /// This is done by writing to a new file and swapping.
    fn truncate_before_checkpoint(&mut self) -> Result<(), WalError> {
        let epoch = time::SystemTime::now()
            .duration_since(time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let temp_path = self.log_path.with_extension(format!("tmp-{}", epoch));

        // Create new file with just the checkpoint record
        let mut new_file = File::create(&temp_path)?;
        let checkpoint_record = WalRecordData::Checkpoint {
            checkpoint_lsn: self.checkpoint_lsn,
        };
        new_file.write_all(&checkpoint_record.serialize(self.current_lsn))?;
        new_file.sync_all()?;

        // Drop so we can rename.
        drop(new_file);

        // Swap files
        std::fs::rename(&temp_path, &self.log_path)?;

        let log_file = File::options().append(true).open(&self.log_path)?;
        self.log_file = BufWriter::new(log_file);

        Ok(())
    }

    /// Recovers WAL state from log file.
    fn recover_from_log(path: &Path) -> Result<RecoveryResult, WalError> {
        if !path.exists() {
            return Ok(RecoveryResult {
                records_to_replay: Vec::new(),
                last_lsn: 0,
                last_checkpoint_lsn: 0,
            });
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut records_to_replay: Vec<(Lsn, WalRecordData)> = Vec::new();
        let mut last_checkpoint_lsn: Lsn = 0;
        let mut last_lsn: Lsn = 0;

        loop {
            match Self::read_record(&mut reader) {
                Ok(Some((lsn, record))) => {
                    last_lsn = lsn;

                    match record {
                        WalRecordData::Checkpoint { checkpoint_lsn } => {
                            // Update checkpoint, clear any records before it
                            // This shouldn't happen unless the truncating after checkpoint fails.
                            last_checkpoint_lsn = checkpoint_lsn;
                            records_to_replay.clear();
                        }
                        _ => {
                            records_to_replay.push((lsn, record));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(RecoveryResult {
            records_to_replay,
            last_lsn,
            last_checkpoint_lsn,
        })
    }

    /// Reads a single WAL record from the reader.
    fn read_record(reader: &mut BufReader<File>) -> Result<Option<(Lsn, WalRecordData)>, WalError> {
        let mut lsn_buf = [0u8; 8];
        match reader.read_exact(&mut lsn_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let lsn = Lsn::from_le_bytes(lsn_buf);

        let mut len_buf = [0u8; 4];
        if let Err(e) = reader.read_exact(&mut len_buf) {
            return Err(WalError::CorruptedRecord {
                lsn,
                reason: format!("Failed to read length: {}", e),
            });
        }
        let total_length = u32::from_le_bytes(len_buf) as usize;

        let mut data = vec![0u8; total_length];
        if let Err(e) = reader.read_exact(&mut data) {
            return Err(WalError::CorruptedRecord {
                lsn,
                reason: format!("Failed to read data: {}", e),
            });
        }

        let (record_type, content) =
            u8::deserialize(&data).map_err(|e| WalError::CorruptedRecord {
                lsn,
                reason: format!("Failed to deserialize record type: {}", e),
            })?;

        let record = match record_type {
            1 => Self::deserialize_single_page_op(content, lsn)?,
            2 => Self::deserialize_multi_page_op(content, lsn)?,
            3 => Self::deserialize_checkpoint(content, lsn)?,
            _ => {
                return Err(WalError::CorruptedRecord {
                    lsn,
                    reason: format!("Unknown record type: {}", record_type),
                });
            }
        };

        Ok(Some((lsn, record)))
    }

    fn deserialize_single_page_op(data: &[u8], lsn: Lsn) -> Result<WalRecordData, WalError> {
        let (page_id, rest) =
            PageId::deserialize(data).map_err(|err| WalError::CorruptedRecord {
                lsn,
                reason: format!("failed to deserialize PageId: {}", err),
            })?;

        let (diff, _) = PageDiff::deserialize(rest).map_err(|e| WalError::CorruptedRecord {
            lsn,
            reason: format!("failed to deserialize PageDiff: {}", e),
        })?;

        Ok(WalRecordData::SinglePageOperation(SinglePageOperation {
            page_id,
            diff,
        }))
    }

    fn deserialize_multi_page_op(data: &[u8], lsn: Lsn) -> Result<WalRecordData, WalError> {
        let (count, mut data) = u16::deserialize(data).map_err(|e| WalError::CorruptedRecord {
            lsn,
            reason: format!("Failed to deserialize multi-page op count: {}", e),
        })?;

        let mut ops = Vec::with_capacity(count as usize);

        for _ in 0..count {
            let (page_id, rest) =
                PageId::deserialize(data).map_err(|e| WalError::CorruptedRecord {
                    lsn,
                    reason: format!("Failed to deserialize PageId: {}", e),
                })?;
            data = rest;

            let (diff, rest) =
                PageDiff::deserialize(data).map_err(|e| WalError::CorruptedRecord {
                    lsn,
                    reason: format!("Failed to deserialize PageDiff: {}", e),
                })?;
            data = rest;

            ops.push(SinglePageOperation { page_id, diff });
        }

        Ok(WalRecordData::MultiPageOperation(ops))
    }

    fn deserialize_checkpoint(data: &[u8], lsn: Lsn) -> Result<WalRecordData, WalError> {
        let (checkpoint_lsn, _) =
            Lsn::deserialize(data).map_err(|err| WalError::CorruptedRecord {
                lsn,
                reason: format!("failed to deserialize checkpoint LSN: {}", err),
            })?;
        Ok(WalRecordData::Checkpoint { checkpoint_lsn })
    }
}

type AtomicLsn = AtomicU64;

pub enum WalRequest {
    Write(WalRecord),
    ForceFlush { sender: SyncSender<bool> },
    Checkpoint { sender: SyncSender<Option<Lsn>> },
}

pub struct WalRecord {
    data: WalRecordData,
    send: SyncSender<Option<Lsn>>,
}

pub enum WalRecordData {
    SinglePageOperation(SinglePageOperation),
    MultiPageOperation(Vec<SinglePageOperation>),
    Checkpoint { checkpoint_lsn: Lsn },
}

pub struct SinglePageOperation {
    page_id: PageId,
    diff: PageDiff,
}

impl SinglePageOperation {
    pub fn new(page_id: PageId, diff: PageDiff) -> Self {
        Self { page_id, diff }
    }
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
            WalRecordData::Checkpoint { .. } => 3,
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
            WalRecordData::Checkpoint { checkpoint_lsn } => {
                checkpoint_lsn.serialize(&mut buffer);
            }
        }

        let total_length = (buffer.len() - size_of::<Lsn>() - size_of::<u32>()) as u32;

        buffer[size_of::<Lsn>()..size_of::<Lsn>() + size_of::<u32>()]
            .copy_from_slice(&total_length.to_le_bytes());

        buffer
    }
}
