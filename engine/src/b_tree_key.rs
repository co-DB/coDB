use thiserror::Error;
use types::data::{DbDate, DbDateTime, Value};
use types::lexicographic_serialization::{DecodeError, SortableSerialize};
use types::schema::Type;

#[derive(Debug, Error)]
pub enum BTreeKeyError {
    #[error("unsupported key type: {ty}")]
    UnsupportedKeyType { ty: Type },
    #[error("failed to decode key: {0}")]
    DecodeError(#[from] DecodeError),
}
pub struct Key {
    bytes: Vec<u8>,
}

impl TryFrom<Value> for Key {
    type Error = BTreeKeyError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let bytes = match value {
            Value::Int32(i) => i.encode_key(),
            Value::Int64(i) => i.encode_key(),
            Value::DateTime(dt) => dt.encode_key(),
            Value::Date(d) => d.encode_key(),
            Value::String(s) => s.encode_key(),
            _ => return Err(Self::Error::UnsupportedKeyType { ty: value.ty() }),
        };
        Ok(Key { bytes })
    }
}

impl Key {
    pub fn decode(bytes: &[u8], ty: &Type) -> Result<Value, BTreeKeyError> {
        match ty {
            Type::String => Ok(Value::String(String::decode_key(bytes)?)),
            Type::I32 => Ok(Value::Int32(i32::decode_key(bytes)?)),
            Type::I64 => Ok(Value::Int64(i64::decode_key(bytes)?)),
            Type::Date => Ok(Value::Date(DbDate::decode_key(bytes)?)),
            Type::DateTime => Ok(Value::DateTime(DbDateTime::decode_key(bytes)?)),
            _ => Err(BTreeKeyError::UnsupportedKeyType { ty: *ty }),
        }
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}
