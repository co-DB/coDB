use thiserror::Error;

use crate::data::{DbDate, DbDateTime};

macro_rules! impl_db_serializable_for {
    ($($t:ty),*) => {
        $(
            impl DbSerializable for $t {
                fn serialize(&self, buffer: &mut Vec<u8>) {
                    buffer.extend(self.to_le_bytes());
                }

                fn serialize_into(&self, buffer: &mut [u8]) {
                    buffer[..self.size_serialized()].copy_from_slice(&self.to_le_bytes());
                }

                fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
                    Self::read_fixed_and_convert::<$t, { size_of::<$t>() }>(buffer, <$t>::from_le_bytes)
                }

                fn size_serialized(&self) -> usize {
                    size_of::<Self>()
                }
            }
        )*
    };
}

impl_db_serializable_for!(i32, i64, u8, u16, u32, u64, f32, f64);

macro_rules! impl_db_serializable_fixed_size_for {
    ($($t:ty),*) => {
        $(
            impl DbSerializableFixedSize for $t {
                fn fixed_size_serialized() -> usize {
                    size_of::<Self>()
                }
            }
        )*
    };
}

impl_db_serializable_fixed_size_for!(i32, i64, f32, f64);

/// A trait for types that can be serialized to and deserialized from bytes
/// for database storage.
pub trait DbSerializable: Sized {
    /// Serializes the value into the provided buffer appending serialized value at the end of it.
    fn serialize(&self, buffer: &mut Vec<u8>);

    /// Serializes the value at the beginning of the provided buffer.
    /// The buffer must have enough space to hold serialized value, otherwise function should panic.
    fn serialize_into(&self, buffer: &mut [u8]);

    /// Deserializes a value from the given byte slice.
    ///
    /// Returns a tuple containing the deserialized value and a slice
    /// of the remaining unconsumed bytes.
    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError>;

    /// Returns number of bytes that [`self`] will take when serialized.
    fn size_serialized(&self) -> usize;

    /// Helper function to read a fixed number of bytes and convert them to a value.
    fn read_fixed_and_convert<T, const N: usize>(
        buffer: &[u8],
        convert: fn([u8; N]) -> T,
    ) -> Result<(T, &[u8]), DbSerializationError> {
        if buffer.len() < N {
            return Err(DbSerializationError::UnexpectedEnd {
                expected: N,
                actual: buffer.len(),
            });
        }
        let arr: [u8; N] = buffer[..N].try_into().unwrap();
        Ok((convert(arr), &buffer[N..]))
    }
}

pub trait DbSerializableFixedSize: DbSerializable {
    /// Returns number of bytes that any instance of this type will take when serialized.
    /// Should only be implemented for types that are fixed-size.
    fn fixed_size_serialized() -> usize;
}

#[derive(Error, Debug)]
pub enum DbSerializationError {
    #[error("expected to read {expected} bytes, but only {actual} were left in the buffer")]
    UnexpectedEnd { expected: usize, actual: usize },
    #[error("failed to deserialize")]
    FailedToDeserialize,
}

impl DbSerializable for String {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend((self.len() as u16).to_le_bytes());
        buffer.extend(self.as_bytes());
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        let len_size = size_of::<u16>();
        buffer[0..len_size].copy_from_slice(&(self.len() as u16).to_le_bytes());
        buffer[len_size..(len_size + self.len())].copy_from_slice(&self.as_bytes());
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (len, rest) = u16::deserialize(buffer)?;
        let string_len = len as usize;
        if rest.len() < string_len {
            return Err(DbSerializationError::UnexpectedEnd {
                expected: string_len,
                actual: rest.len(),
            });
        }
        let string_bytes = &rest[..string_len];
        let string = std::str::from_utf8(string_bytes)
            .map_err(|_| DbSerializationError::FailedToDeserialize)?
            .into();
        Ok((string, &rest[string_len..]))
    }

    fn size_serialized(&self) -> usize {
        size_of::<u16>() + self.len()
    }
}

impl DbSerializable for bool {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.push(*self as u8)
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        buffer[0] = *self as u8;
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        Self::read_fixed_and_convert::<u8, { size_of::<u8>() }>(buffer, |bytes| bytes[0]).and_then(
            |(val, rest)| match val {
                0 => Ok((false, rest)),
                1 => Ok((true, rest)),
                _ => Err(DbSerializationError::FailedToDeserialize),
            },
        )
    }

    fn size_serialized(&self) -> usize {
        size_of::<u8>()
    }
}

impl DbSerializableFixedSize for bool {
    fn fixed_size_serialized() -> usize {
        size_of::<u8>()
    }
}

impl DbSerializable for DbDate {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.days_since_epoch().to_le_bytes());
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        self.days_since_epoch().serialize_into(buffer);
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        Self::read_fixed_and_convert::<i32, { size_of::<i32>() }>(buffer, i32::from_le_bytes)
            .map(|(val, rest)| (DbDate::new(val), rest))
    }

    fn size_serialized(&self) -> usize {
        size_of::<i32>()
    }
}

impl DbSerializableFixedSize for DbDate {
    fn fixed_size_serialized() -> usize {
        size_of::<i32>()
    }
}

impl DbSerializable for DbDateTime {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.days_since_epoch().to_le_bytes());
        buffer.extend(self.milliseconds_since_midnight().to_le_bytes());
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        self.days_since_epoch().serialize_into(buffer);
        let days_since_epoch_size = size_of::<i32>();
        self.milliseconds_since_midnight()
            .serialize_into(&mut buffer[days_since_epoch_size..]);
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (days, rest) = i32::deserialize(buffer)?;
        let (milliseconds, rest) = u32::deserialize(rest)?;
        Ok((DbDateTime::new(DbDate::new(days), milliseconds), rest))
    }

    fn size_serialized(&self) -> usize {
        size_of::<i32>() + size_of::<u32>()
    }
}

impl DbSerializableFixedSize for DbDateTime {
    fn fixed_size_serialized() -> usize {
        size_of::<i32>() + size_of::<u32>()
    }
}
