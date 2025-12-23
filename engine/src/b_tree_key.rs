use thiserror::Error;
use types::data::Value;
use types::lexicographic_serialization::{DecodeError, SortableSerialize};
use types::schema::Type;

#[derive(Debug, Error)]
pub enum BTreeKeyError {
    #[error("unsupported key type: {ty}")]
    UnsupportedKeyType { ty: Type },
    #[error("failed to decode key: {0}")]
    DecodeError(#[from] DecodeError),
}

#[derive(Clone, Debug)]
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

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Key {
    /// Returns the raw bytes of the key.
    pub fn as_bytes(&self) -> &[u8] {
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

#[cfg(test)]
mod test_helpers {
    use super::*;

    impl From<i32> for Key {
        fn from(value: i32) -> Self {
            Key {
                bytes: value.encode_key(),
            }
        }
    }

    impl From<&str> for Key {
        fn from(value: &str) -> Self {
            Key {
                bytes: value.to_string().encode_key(),
            }
        }
    }
}
