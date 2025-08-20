use crate::catalog::{ColumnMetadata, ColumnType};
use thiserror::Error;

/// Error for record related operations
#[derive(Error, Debug)]
enum RecordError {
    #[error(
        "while reading field {field_name}: expected to read {expected} bytes, but only {actual} were left"
    )]
    UnexpectedEnd {
        field_name: String,
        expected: usize,
        actual: usize,
    },
    #[error("failed to deserialize field {field_name}")]
    FailedToDeserialize { field_name: String },
}

/// Represents a database record containing multiple typed fields.
///
/// A record is a collection of fields that correspond to the columns
/// defined in a table schema. Records can be serialized to bytes for
/// storage and deserialized back to their structured form.
struct Record {
    fields: Vec<Field>,
}

impl Record {
    /// Creates a new record with the given fields.
    fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Serializes the record into a byte representation.
    ///
    /// Fields are serialized in order using little-endian byte ordering.
    /// Variable-length fields (like strings) are prefixed with their length.
    fn serialize(self) -> Result<Vec<u8>, RecordError> {
        let mut bytes = vec![];
        for field in self.fields {
            field.serialize(&mut bytes);
        }
        Ok(bytes)
    }

    /// Deserializes bytes into a record using the provided columns' metadata.
    ///
    /// The byte buffer must contain fields in the same order as the column
    /// metadata. Each field is deserialized according to its type specified
    /// in the corresponding column metadata.
    fn deserialize(columns: &[ColumnMetadata], bytes: &[u8]) -> Result<Self, RecordError> {
        let mut bytes = bytes;
        let fields = columns
            .iter()
            .map(|col| {
                let (field, remaining) = Field::deserialize(bytes, col)?;
                bytes = remaining;
                Ok(field)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self::new(fields))
    }
}

/// Represents a typed database field value.
///
/// Each variant corresponds to a supported database column type.
enum Field {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    DateTime(i64),
    Date(i32),
    String(String),
    Bool(bool),
}

impl Field {
    /// Serializes this field into the provided buffer.
    ///
    /// All numeric types use little-endian byte ordering.
    fn serialize(self, buffer: &mut Vec<u8>) {
        match self {
            Field::Int32(i) => buffer.extend(i.to_le_bytes()),
            Field::Int64(i) => buffer.extend(i.to_le_bytes()),
            Field::Float32(f) => buffer.extend(f.to_le_bytes()),
            Field::Float64(f) => buffer.extend(f.to_le_bytes()),
            Field::DateTime(d) => buffer.extend(d.to_le_bytes()),
            Field::Date(d) => buffer.extend(d.to_le_bytes()),
            Field::String(s) => {
                buffer.extend((s.len() as u16).to_le_bytes());
                buffer.extend(s.as_bytes());
            }
            Field::Bool(b) => buffer.push(b as u8),
        }
    }

    /// Deserializes a field from bytes using the provided column metadata.
    ///
    /// Returns both the deserialized field and the remaining unconsumed bytes.
    fn deserialize<'a>(
        buffer: &'a [u8],
        column_metadata: &ColumnMetadata,
    ) -> Result<(Self, &'a [u8]), RecordError> {
        let name = column_metadata.name();
        match column_metadata.ty() {
            ColumnType::Bool => {
                // we know a bool is a single byte so we can just compare it to 0.
                Self::read_fixed_and_convert::<bool, 1>(buffer, |b| b[0] != 0, name)
                    .map(|(val, rest)| (Field::Bool(val), rest))
            }
            ColumnType::I32 => {
                Self::read_fixed_and_convert::<i32, 4>(buffer, i32::from_le_bytes, name)
                    .map(|(val, rest)| (Field::Int32(val), rest))
            }
            ColumnType::I64 => {
                Self::read_fixed_and_convert::<i64, 8>(buffer, i64::from_le_bytes, name)
                    .map(|(val, rest)| (Field::Int64(val), rest))
            }
            ColumnType::F32 => {
                Self::read_fixed_and_convert::<f32, 4>(buffer, f32::from_le_bytes, name)
                    .map(|(val, rest)| (Field::Float32(val), rest))
            }
            ColumnType::F64 => {
                Self::read_fixed_and_convert::<f64, 8>(buffer, f64::from_le_bytes, name)
                    .map(|(val, rest)| (Field::Float64(val), rest))
            }
            ColumnType::Date => {
                Self::read_fixed_and_convert::<i32, 4>(buffer, i32::from_le_bytes, name)
                    .map(|(val, rest)| (Field::Date(val), rest))
            }
            ColumnType::DateTime => {
                Self::read_fixed_and_convert::<i64, 8>(buffer, i64::from_le_bytes, name)
                    .map(|(val, rest)| (Field::DateTime(val), rest))
            }
            ColumnType::String => {
                let (len, rest) =
                    Self::read_fixed_and_convert::<u16, 2>(buffer, u16::from_le_bytes, name)?;
                let string_len = len as usize;
                if rest.len() < string_len {
                    return Err(RecordError::UnexpectedEnd {
                        field_name: column_metadata.name().to_owned(),
                        expected: string_len,
                        actual: rest.len(),
                    });
                }
                let string_bytes = &rest[..string_len];
                let string = std::str::from_utf8(string_bytes)
                    .map_err(|_| RecordError::FailedToDeserialize {
                        field_name: name.to_string(),
                    })?
                    .to_string();
                Ok((Field::String(string), &rest[string_len..]))
            }
        }
    }

    /// Helper function to read a fixed number of bytes and convert them to a value.
    fn read_fixed_and_convert<'a, T, const N: usize>(
        buffer: &'a [u8],
        convert: fn([u8; N]) -> T,
        field_name: &str,
    ) -> Result<(T, &'a [u8]), RecordError> {
        // try_into() attempts to create a fixed-size array from the slice
        // This will fail if the buffer doesn't have at least N bytes
        let arr: [u8; N] = buffer.try_into().map_err(|_| RecordError::UnexpectedEnd {
            field_name: field_name.to_owned(),
            expected: N,
            actual: buffer.len(),
        })?;
        Ok((convert(arr), &buffer[N..]))
    }
}
