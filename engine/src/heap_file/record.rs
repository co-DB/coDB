use storage::paged_file::PageId;
use types::serialization::{DbSerializable, DbSerializationError};

use crate::{heap_file::error::HeapFileError, record::Record, slotted_page::SlotId};

/// Logical pointer to a record in a [`HeapFile`].
///
/// It should only be used for referencing start of the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordPtr {
    pub(crate) page_id: PageId,
    pub(crate) slot_id: SlotId,
}

impl RecordPtr {
    /// Placeholder used to reserve a key in the B-tree before inserting actual data.
    pub const PLACEHOLDER: RecordPtr = RecordPtr {
        page_id: PageId::MAX,
        slot_id: SlotId::MAX,
    };

    pub(crate) fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }
}

impl DbSerializable for RecordPtr {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        self.page_id.serialize(buffer);
        self.slot_id.serialize(buffer);
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        let mut start = 0;
        let mut end = size_of::<PageId>();
        buffer[start..end].copy_from_slice(&self.page_id.to_le_bytes());
        start += size_of::<PageId>();
        end += size_of::<SlotId>();
        buffer[start..end].copy_from_slice(&self.slot_id.to_le_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (page_id, rest) = PageId::deserialize(buffer)?;
        let (slot, _) = SlotId::deserialize(rest)?;
        Ok((
            Self {
                page_id,
                slot_id: slot,
            },
            buffer,
        ))
    }

    fn size_serialized(&self) -> usize {
        size_of::<Self>()
    }
}

impl RecordPtr {
    /// Reads [`RecordPtr`] from buffer and returns the rest of the buffer.
    pub(crate) fn read_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), HeapFileError> {
        let (page_id, bytes) =
            PageId::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let (slot, bytes) =
            SlotId::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let ptr = RecordPtr {
            page_id,
            slot_id: slot,
        };
        Ok((ptr, bytes))
    }
}

impl From<&RecordPtr> for Vec<u8> {
    fn from(value: &RecordPtr) -> Self {
        let mut buffer = Vec::with_capacity(size_of::<RecordPtr>());
        value.page_id.serialize(&mut buffer);
        value.slot_id.serialize(&mut buffer);
        buffer
    }
}

/// Tag preceding record payload that describes whether a RecordPtr follows.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RecordTag {
    Final = 0x00,
    HasContinuation = 0xff,
}

impl RecordTag {
    /// Reads [`RecordTag`] from buffer and returns the rest of the buffer.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<(Self, &[u8]), HeapFileError> {
        let (value, rest) =
            u8::deserialize(bytes).map_err(|err| HeapFileError::CorruptedRecordEntry {
                error: err.to_string(),
            })?;
        let record_tag = RecordTag::try_from(value)?;
        Ok((record_tag, rest))
    }

    /// Returns buffer with added [`RecordTag::Final`] at the beginning of it.
    pub fn with_final(buffer: &[u8]) -> Vec<u8> {
        let mut new_buffer = Vec::with_capacity(buffer.len() + size_of::<RecordTag>());
        new_buffer.push(RecordTag::Final as _);
        new_buffer.extend_from_slice(buffer);
        new_buffer
    }

    /// Returns buffer with added [`RecordTag::HasContinuation`] at the beginning of it.
    pub fn with_has_continuation(buffer: &[u8], continuation: &RecordPtr) -> Vec<u8> {
        let mut new_buffer =
            Vec::with_capacity(buffer.len() + size_of::<RecordTag>() + size_of::<RecordPtr>());
        new_buffer.push(RecordTag::HasContinuation as _);
        let mut ptr = Vec::from(continuation);
        new_buffer.append(&mut ptr);
        new_buffer.extend_from_slice(buffer);
        new_buffer
    }
}

impl TryFrom<u8> for RecordTag {
    type Error = HeapFileError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            v if v == RecordTag::Final as u8 => Ok(RecordTag::Final),
            v if v == RecordTag::HasContinuation as u8 => Ok(RecordTag::HasContinuation),
            _ => Err(HeapFileError::CorruptedRecordEntry {
                error: format!("unexpected record tag: 0x{:02x}", value),
            }),
        }
    }
}

/// [`Record`] is stored across pages using [`RecordFragment`].
/// The first fragment is stored in record page, all next fragments are stored on overflow pages.
pub(super) struct RecordFragment<'d> {
    /// Part of record from single page.
    pub data: &'d [u8],
    /// Pointer to the next fragment of record.
    pub next_fragment: Option<RecordPtr>,
}

impl<'d> RecordFragment<'d> {
    /// Reads [`RecordFragment`] from buffer.
    /// It is assumed that fragment takes the whole buffer.
    pub fn read_from_bytes(bytes: &'d [u8]) -> Result<Self, HeapFileError> {
        let (tag, bytes) = RecordTag::read_from_bytes(bytes)?;
        let (next_fragment, bytes) = match tag {
            RecordTag::Final => (None, bytes),
            RecordTag::HasContinuation => {
                let (ptr, bytes) = RecordPtr::read_from_bytes(bytes)?;
                (Some(ptr), bytes)
            }
        };
        let fragment = RecordFragment {
            data: bytes,
            next_fragment,
        };
        Ok(fragment)
    }
}

/// Handle to record and its pointer in heap file.
pub struct RecordHandle {
    pub record: Record,
    pub record_ptr: RecordPtr,
}

impl RecordHandle {
    pub(super) fn new(record: Record, record_ptr: RecordPtr) -> Self {
        RecordHandle { record, record_ptr }
    }
}
