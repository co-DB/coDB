use crate::page_diff::PageDiff;
use crate::paged_file::{AtomicLsn, Lsn, PageId};
use log::error;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use std::thread::JoinHandle;
use std::{io, thread, time};
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
struct RecoveryResult {
    /// Records that need to be redone (after last checkpoint).
    pub redo_records: Vec<(Lsn, WalRecordData)>,
    /// The LSN to continue from.
    pub last_lsn: Lsn,
}

pub struct StartHandle {
    handle: JoinHandle<()>,
    pub(crate) wal_client: WalClient,
    pub(crate) redo_records: Vec<(Lsn, WalRecordData)>,
}

impl StartHandle {
    fn new(
        handle: JoinHandle<()>,
        wal_client: WalClient,
        redo_records: Vec<(Lsn, WalRecordData)>,
    ) -> Self {
        Self {
            handle,
            wal_client,
            redo_records,
        }
    }

    /// Waits for the WAL thread to finish (when either WalClient is dropped or an error occurs)
    /// and joins it.
    pub fn join(self) -> thread::Result<()> {
        self.handle.join()
    }
}

struct WalManager {
    /// The highest LSN assigned so far.
    current_lsn: Lsn,
    /// The highest LSN that has been flushed to disk. Atomic for cross-thread access.
    flushed_lsn: Arc<AtomicLsn>,
    /// Receiver for incoming WAL requests.
    recv: mpsc::Receiver<WalRequest>,
    /// Path to the file where WAL records are stored.
    log_path: PathBuf,
    /// Buffered writer for the WAL file.
    log_file: BufWriter<File>,
}

/// Spawns the WAL manager thread and returns a handle for interaction.
/// This is the main entry point for using WAL.
pub fn spawn_wal(log_path: impl AsRef<Path>) -> Result<StartHandle, WalError> {
    let (sender, recv) = mpsc::sync_channel(1024);

    let (mut manager, recovery, flushed_lsn) = WalManager::with_recovery(recv, log_path)?;

    let handle = thread::spawn(move || {
        manager.run();
    });

    let client = WalClient {
        sender,
        flushed_lsn,
    };

    let handle = StartHandle::new(handle, client, recovery.redo_records);
    Ok(handle)
}

impl WalManager {
    /// Creates WalManager after recovering from existing log file.
    /// Returns the manager, recovery result, and shared flushed_lsn.
    fn with_recovery(
        recv: mpsc::Receiver<WalRequest>,
        log_path: impl AsRef<Path>,
    ) -> Result<(Self, RecoveryResult, Arc<AtomicLsn>), WalError> {
        let path = log_path.as_ref();
        let recovery = Self::recover_from_log(path)?;

        let flushed_lsn = Arc::new(AtomicLsn::new(recovery.last_lsn));

        let log_file = File::options().create(true).append(true).open(path)?;

        let manager = WalManager {
            current_lsn: recovery.last_lsn,
            flushed_lsn: flushed_lsn.clone(),
            recv,
            log_path: path.to_path_buf(),
            log_file: BufWriter::new(log_file),
        };

        Ok((manager, recovery, flushed_lsn))
    }

    fn run(&mut self) {
        while let Ok(req) = self.recv.recv() {
            if let Err(err) = self.handle_request(req) {
                error!("WAL error occurred: {}", err);
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
                let _ = record.send.send(Some(lsn));
                Ok(())
            }
            Err(err) => {
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
        self.log_file.get_ref().sync_data()?;
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
        let checkpoint_record = WalRecordData::Checkpoint {
            checkpoint_lsn: self.current_lsn,
        };
        let new_lsn = self.append_record(checkpoint_record)?;

        self.flush_internal()?;

        // Truncate WAL - everything before checkpoint is no longer needed
        self.truncate_before_checkpoint()?;

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

        // Guard to ensure temp file is deleted if something goes wrong.
        let guard = TempFileGuard::new(temp_path.clone());

        // Create new file with just the checkpoint record
        let mut new_file = File::create(&temp_path)?;
        let checkpoint_record = WalRecordData::Checkpoint {
            checkpoint_lsn: self.current_lsn,
        };
        new_file.write_all(&checkpoint_record.serialize(self.current_lsn))?;
        new_file.sync_all()?;

        // Drop so we can rename.
        drop(new_file);

        // Swap files
        std::fs::rename(&temp_path, &self.log_path)?;

        // Commit the guard so it doesn't delete the temp file.
        guard.commit();

        let log_file = File::options().append(true).open(&self.log_path)?;
        self.log_file = BufWriter::new(log_file);

        Ok(())
    }

    /// Recovers WAL state from log file.
    fn recover_from_log(path: &Path) -> Result<RecoveryResult, WalError> {
        if !path.exists() {
            return Ok(RecoveryResult {
                redo_records: Vec::new(),
                last_lsn: 0,
            });
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut redo_records: Vec<(Lsn, WalRecordData)> = Vec::with_capacity(1024);
        let mut last_lsn: Lsn = 0;

        loop {
            match Self::read_record(&mut reader) {
                Ok(Some((lsn, record))) => {
                    last_lsn = lsn;

                    match record {
                        WalRecordData::Checkpoint { .. } => {
                            // Clear any records before the checkpoint as they were persisted.
                            // This shouldn't happen unless the truncating after checkpoint fails.
                            redo_records.clear();
                        }
                        _ => {
                            redo_records.push((lsn, record));
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(RecoveryResult {
            redo_records,
            last_lsn,
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

        let record = WalRecordData::deserialize(&data, lsn)?;
        Ok(Some((lsn, record)))
    }
}

/// Represents a request to the WAL manager.
enum WalRequest {
    Write(WalRecord),
    ForceFlush { sender: SyncSender<bool> },
    Checkpoint { sender: SyncSender<Option<Lsn>> },
}

/// Represents a WAL record write request along with a channel to send back the assigned LSN.
struct WalRecord {
    data: WalRecordData,
    send: SyncSender<Option<Lsn>>,
}

/// Represents the data contained in a WAL record. Can be single-page if only one page is modified,
/// or multipage for operations like split in B-Tree or an insert with overflow in heap file.
/// Checkpoint is for recording when all pages up to a certain LSN have been flushed from both
/// WAL and cache.
pub(crate) enum WalRecordData {
    SinglePageOperation(SinglePageOperation),
    MultiPageOperation(Vec<SinglePageOperation>),
    Checkpoint { checkpoint_lsn: Lsn },
}

/// Represents a single page operation in WAL.
pub(crate) struct SinglePageOperation {
    page_id: PageId,
    diff: PageDiff,
}

/// Guard for temporary files that ensures deletion on drop unless committed.
struct TempFileGuard {
    path: PathBuf,
    keep: bool,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }

    fn commit(mut self) {
        self.keep = true;
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if !self.keep {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Wal record is serialized as:
/// 1. LSN (u64)
/// 2. Total length (u32)
/// 3. Record type (u8)
/// 4. Number of page operations (if multipage)
/// 5. For each page operation:
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

    fn deserialize(data: &[u8], lsn: Lsn) -> Result<WalRecordData, WalError> {
        let (record_type, content) =
            u8::deserialize(data).map_err(|e| WalError::CorruptedRecord {
                lsn,
                reason: format!("Failed to deserialize record type: {}", e),
            })?;

        match record_type {
            1 => Self::deserialize_single_page_op(content, lsn),
            2 => Self::deserialize_multi_page_op(content, lsn),
            3 => Self::deserialize_checkpoint(content, lsn),
            _ => Err(WalError::CorruptedRecord {
                lsn,
                reason: format!("Unknown record type: {}", record_type),
            }),
        }
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

/// Client handle for interacting with the WAL from other threads.
/// This is the public API for WAL operations.
pub(crate) struct WalClient {
    sender: SyncSender<WalRequest>,
    flushed_lsn: Arc<AtomicLsn>,
}

impl WalClient {
    /// Writes a single page operation to WAL and returns the assigned LSN.
    /// Returns None if the write operation failed.
    pub(crate) fn write_single(&self, page_id: PageId, diff: PageDiff) -> Option<Lsn> {
        let (send, recv) = mpsc::sync_channel(1);
        let record = WalRecord {
            data: WalRecordData::SinglePageOperation(SinglePageOperation { page_id, diff }),
            send,
        };
        self.sender.send(WalRequest::Write(record)).ok()?;
        recv.recv().ok()?
    }

    /// Writes multiple page operations as single record to WAL and returns the assigned LSN.
    /// Returns None if the write operation failed.
    pub(crate) fn write_multi(&self, ops: Vec<(PageId, PageDiff)>) -> Option<Lsn> {
        let (send, recv) = mpsc::sync_channel(1);
        let ops = ops
            .into_iter()
            .map(|(page_id, diff)| SinglePageOperation { page_id, diff })
            .collect();
        let record = WalRecord {
            data: WalRecordData::MultiPageOperation(ops),
            send,
        };
        self.sender.send(WalRequest::Write(record)).ok()?;
        recv.recv().ok()?
    }

    /// Forces a flush of all pending WAL records to disk.
    /// Returns true if flush succeeded.
    pub(crate) fn flush(&self) -> bool {
        let (sender, recv) = mpsc::sync_channel(1);
        if self.sender.send(WalRequest::ForceFlush { sender }).is_err() {
            return false;
        }
        recv.recv().unwrap_or(false)
    }

    /// Requests a checkpoint. Caller must ensure all dirty pages have been flushed first.
    /// Returns the checkpoint LSN on success.
    pub(crate) fn checkpoint(&self) -> Option<Lsn> {
        let (sender, recv) = mpsc::sync_channel(1);
        self.sender.send(WalRequest::Checkpoint { sender }).ok()?;
        recv.recv().ok()?
    }

    /// Returns the highest LSN that has been flushed to disk.
    pub(crate) fn flushed_lsn(&self) -> Lsn {
        self.flushed_lsn.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::tempdir;

    // ==================== Serialization / Deserialization ====================

    fn make_single_page_op(page_id: PageId, data: Vec<u8>) -> WalRecordData {
        let mut diff = PageDiff::default();
        diff.write_at(0, data);
        WalRecordData::SinglePageOperation(SinglePageOperation { page_id, diff })
    }

    fn make_multi_page_op(ops: Vec<(PageId, Vec<u8>)>) -> WalRecordData {
        let ops = ops
            .into_iter()
            .map(|(page_id, data)| {
                let mut diff = PageDiff::default();
                diff.write_at(0, data);
                SinglePageOperation { page_id, diff }
            })
            .collect();
        WalRecordData::MultiPageOperation(ops)
    }

    fn roundtrip_record(record: WalRecordData, lsn: Lsn) -> WalRecordData {
        let serialized = record.serialize(lsn);
        // Skip LSN (8 bytes) and length (4 bytes) to get to the record data
        let data = &serialized[size_of::<Lsn>() + size_of::<u32>()..];
        WalRecordData::deserialize(data, lsn).expect("deserialize should succeed")
    }

    #[test]
    fn serialize_deserialize_single_page_operation() {
        let record = make_single_page_op(42, vec![1, 2, 3, 4]);
        let result = roundtrip_record(record, 100);

        match result {
            WalRecordData::SinglePageOperation(op) => {
                assert_eq!(op.page_id, 42);
            }
            _ => panic!("expected SinglePageOperation"),
        }
    }

    #[test]
    fn serialize_deserialize_multi_page_operation() {
        let record = make_multi_page_op(vec![(1, vec![10, 20]), (2, vec![30, 40]), (3, vec![50])]);
        let result = roundtrip_record(record, 200);

        match result {
            WalRecordData::MultiPageOperation(ops) => {
                assert_eq!(ops.len(), 3);
                assert_eq!(ops[0].page_id, 1);
                assert_eq!(ops[1].page_id, 2);
                assert_eq!(ops[2].page_id, 3);
            }
            _ => panic!("expected MultiPageOperation"),
        }
    }

    #[test]
    fn serialize_deserialize_checkpoint() {
        let record = WalRecordData::Checkpoint {
            checkpoint_lsn: 999,
        };
        let result = roundtrip_record(record, 1000);

        match result {
            WalRecordData::Checkpoint { checkpoint_lsn } => {
                assert_eq!(checkpoint_lsn, 999);
            }
            _ => panic!("expected Checkpoint"),
        }
    }

    #[test]
    fn serialized_record_contains_correct_lsn_and_length() {
        let record = make_single_page_op(1, vec![1, 2, 3]);
        let serialized = record.serialize(42);

        let lsn = Lsn::from_le_bytes(serialized[0..8].try_into().unwrap());
        let len = u32::from_le_bytes(serialized[8..12].try_into().unwrap());

        assert_eq!(lsn, 42);
        assert_eq!(len as usize, serialized.len() - 12); // total - lsn - len
    }

    // ==================== Recovery from Log ====================

    fn write_record_to_file(file: &mut File, record: WalRecordData, lsn: Lsn) {
        let serialized = record.serialize(lsn);
        file.write_all(&serialized).unwrap();
    }

    #[test]
    fn recover_from_empty_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        File::create(&log_path).unwrap();

        let result = WalManager::recover_from_log(&log_path).unwrap();

        assert!(result.redo_records.is_empty());
        assert_eq!(result.last_lsn, 0);
    }

    #[test]
    fn recover_from_nonexistent_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("nonexistent.log");

        let result = WalManager::recover_from_log(&log_path).unwrap();

        assert!(result.redo_records.is_empty());
        assert_eq!(result.last_lsn, 0);
    }

    #[test]
    fn recover_single_record() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let mut file = File::create(&log_path).unwrap();

        write_record_to_file(&mut file, make_single_page_op(1, vec![1, 2, 3]), 1);
        drop(file);

        let result = WalManager::recover_from_log(&log_path).unwrap();

        assert_eq!(result.redo_records.len(), 1);
        assert_eq!(result.redo_records[0].0, 1);
        assert_eq!(result.last_lsn, 1);
    }

    #[test]
    fn recover_multiple_records() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let mut file = File::create(&log_path).unwrap();

        write_record_to_file(&mut file, make_single_page_op(1, vec![1]), 1);
        write_record_to_file(&mut file, make_single_page_op(2, vec![2]), 2);
        write_record_to_file(&mut file, make_single_page_op(3, vec![3]), 3);
        drop(file);

        let result = WalManager::recover_from_log(&log_path).unwrap();

        assert_eq!(result.redo_records.len(), 3);
        assert_eq!(result.last_lsn, 3);
    }

    #[test]
    fn recover_clears_records_on_checkpoint() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let mut file = File::create(&log_path).unwrap();

        write_record_to_file(&mut file, make_single_page_op(1, vec![1]), 1);
        write_record_to_file(&mut file, make_single_page_op(2, vec![2]), 2);
        write_record_to_file(
            &mut file,
            WalRecordData::Checkpoint { checkpoint_lsn: 2 },
            3,
        );
        write_record_to_file(&mut file, make_single_page_op(3, vec![3]), 4);
        drop(file);

        let result = WalManager::recover_from_log(&log_path).unwrap();

        // Only record after checkpoint should be in redo_records
        assert_eq!(result.redo_records.len(), 1);
        assert_eq!(result.redo_records[0].0, 4);
        assert_eq!(result.last_lsn, 4);
    }

    #[test]
    fn recover_with_only_checkpoint() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let mut file = File::create(&log_path).unwrap();

        write_record_to_file(
            &mut file,
            WalRecordData::Checkpoint { checkpoint_lsn: 10 },
            10,
        );
        drop(file);

        let result = WalManager::recover_from_log(&log_path).unwrap();

        assert!(result.redo_records.is_empty());
        assert_eq!(result.last_lsn, 10);
    }

    // ==================== WalClient / WalManager Integration ====================

    #[test]
    fn spawn_wal_returns_valid_handle() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");

        let handle = spawn_wal(&log_path).expect("spawn_wal should succeed");

        assert!(handle.redo_records.is_empty());
        drop(handle.wal_client);
        handle
            .handle
            .join()
            .expect("WAL thread should join cleanly");
    }

    #[test]
    fn write_single_returns_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        let mut diff = PageDiff::default();
        diff.write_at(0, vec![1, 2, 3]);

        let lsn = handle.wal_client.write_single(1, diff);

        assert_eq!(lsn, Some(1));
        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn write_single_increments_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        let lsn1 = handle.wal_client.write_single(1, PageDiff::default());
        let lsn2 = handle.wal_client.write_single(2, PageDiff::default());
        let lsn3 = handle.wal_client.write_single(3, PageDiff::default());

        assert_eq!(lsn1, Some(1));
        assert_eq!(lsn2, Some(2));
        assert_eq!(lsn3, Some(3));

        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn write_multi_returns_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        let mut diff1 = PageDiff::default();
        diff1.write_at(0, vec![1]);
        let mut diff2 = PageDiff::default();
        diff2.write_at(0, vec![2]);

        let lsn = handle.wal_client.write_multi(vec![(1, diff1), (2, diff2)]);

        assert_eq!(lsn, Some(1));
        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn flush_updates_flushed_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        assert_eq!(handle.wal_client.flushed_lsn(), 0);

        handle.wal_client.write_single(1, PageDiff::default());
        handle.wal_client.write_single(2, PageDiff::default());

        let success = handle.wal_client.flush();
        assert!(success);
        assert_eq!(handle.wal_client.flushed_lsn(), 2);

        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn checkpoint_returns_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        handle.wal_client.write_single(1, PageDiff::default());
        handle.wal_client.write_single(2, PageDiff::default());

        let checkpoint_lsn = handle.wal_client.checkpoint();

        assert_eq!(checkpoint_lsn, Some(3));

        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn checkpoint_truncates_wal_file() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        // Write several records
        for _ in 0..10 {
            handle.wal_client.write_single(1, PageDiff::default());
        }
        handle.wal_client.flush();

        let size_before = std::fs::metadata(&log_path).unwrap().len();

        handle.wal_client.checkpoint();

        let size_after = std::fs::metadata(&log_path).unwrap().len();

        // After checkpoint, file should be smaller (contains only checkpoint record)
        assert!(size_after < size_before);

        drop(handle.wal_client);
        handle.handle.join().unwrap();
    }

    #[test]
    fn recovery_after_checkpoint_only_contains_post_checkpoint_records() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");

        // First session: write records and checkpoint
        {
            let handle = spawn_wal(&log_path).unwrap();
            handle.wal_client.write_single(1, PageDiff::default());
            handle.wal_client.write_single(2, PageDiff::default());
            handle.wal_client.checkpoint();
            handle.wal_client.write_single(3, PageDiff::default());
            handle.wal_client.write_single(4, PageDiff::default());
            handle.wal_client.flush();
            drop(handle.wal_client);
            handle.handle.join().unwrap();
        }

        // Second session: recover
        {
            let handle = spawn_wal(&log_path).unwrap();

            // Should only have records after checkpoint (LSN 4 and 5)
            assert_eq!(handle.redo_records.len(), 2);
            assert_eq!(handle.redo_records[0].0, 4);
            assert_eq!(handle.redo_records[1].0, 5);

            drop(handle.wal_client);
            handle.handle.join().unwrap();
        }
    }

    #[test]
    fn flushed_lsn_persists_across_recovery() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");

        // First session
        {
            let handle = spawn_wal(&log_path).unwrap();
            handle.wal_client.write_single(1, PageDiff::default());
            handle.wal_client.write_single(2, PageDiff::default());
            handle.wal_client.write_single(3, PageDiff::default());
            handle.wal_client.flush();
            drop(handle.wal_client);
            handle.handle.join().unwrap();
        }

        // Second session
        {
            let handle = spawn_wal(&log_path).unwrap();

            // flushed_lsn should be set to last_lsn from recovery
            assert_eq!(handle.wal_client.flushed_lsn(), 3);

            drop(handle.wal_client);
            handle.handle.join().unwrap();
        }
    }

    // ==================== Concurrent Access ====================

    /// Wrapper to simulate how Cache would hold WalClient behind Arc.
    struct SharedWalClient(Arc<WalClient>);

    impl SharedWalClient {
        fn new(client: WalClient) -> Self {
            Self(Arc::new(client))
        }

        fn clone_arc(&self) -> Arc<WalClient> {
            self.0.clone()
        }
    }

    #[test]
    fn multiple_writers_concurrent() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        // Simulate how Cache would share WalClient via Arc
        let shared = SharedWalClient::new(handle.wal_client);

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let client = shared.clone_arc();
                thread::spawn(move || {
                    for _ in 0..10 {
                        let lsn = client.write_single(i, PageDiff::default());
                        assert!(lsn.is_some());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        shared.0.flush();
        assert_eq!(shared.0.flushed_lsn(), 100);

        drop(shared);
        handle.handle.join().unwrap();
    }

    #[test]
    fn arc_shared_client_sees_same_flushed_lsn() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("wal.log");
        let handle = spawn_wal(&log_path).unwrap();

        let shared = SharedWalClient::new(handle.wal_client);
        let client1 = shared.clone_arc();
        let client2 = shared.clone_arc();

        client1.write_single(1, PageDiff::default());
        client1.write_single(2, PageDiff::default());
        client1.flush();

        // Both Arc references should see the same flushed_lsn
        assert_eq!(client1.flushed_lsn(), 2);
        assert_eq!(client2.flushed_lsn(), 2);

        drop(client1);
        drop(client2);
        drop(shared);
        handle.handle.join().unwrap();
    }

    // ==================== Error Cases ====================

    #[test]
    fn deserialize_invalid_record_type_returns_error() {
        let data = [255u8]; // Invalid record type

        let result = WalRecordData::deserialize(&data, 1);

        assert!(result.is_err());
        match result {
            Err(WalError::CorruptedRecord { lsn, reason }) => {
                assert_eq!(lsn, 1);
                assert!(reason.contains("Unknown record type"));
            }
            _ => panic!("expected CorruptedRecord error"),
        }
    }

    #[test]
    fn deserialize_truncated_data_returns_error() {
        // Valid record type but no data for single page op
        let data = [1u8]; // Single page op type

        let result = WalRecordData::deserialize(&data, 1);

        assert!(result.is_err());
    }

    // ==================== TempFileGuard ====================

    #[test]
    fn temp_file_guard_deletes_on_drop() {
        let dir = tempdir().unwrap();
        let temp_path = dir.path().join("temp.file");

        File::create(&temp_path).unwrap();
        assert!(temp_path.exists());

        {
            let _guard = TempFileGuard::new(temp_path.clone());
            // Guard drops here
        }

        assert!(!temp_path.exists());
    }

    #[test]
    fn temp_file_guard_keeps_on_commit() {
        let dir = tempdir().unwrap();
        let temp_path = dir.path().join("temp.file");

        File::create(&temp_path).unwrap();

        {
            let guard = TempFileGuard::new(temp_path.clone());
            guard.commit();
        }

        assert!(temp_path.exists());
    }
}
