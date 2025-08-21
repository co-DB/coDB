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
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Serializes the record into a byte representation.
    ///
    /// Fields are serialized in order using little-endian byte ordering.
    /// Variable-length fields (like strings) are prefixed with their length.
    pub fn serialize(self) -> Vec<u8> {
        let mut bytes = vec![];
        for field in self.fields {
            field.serialize(&mut bytes);
        }
        bytes
    }

    /// Deserializes bytes into a record using the provided columns' metadata.
    ///
    /// The byte buffer must contain fields in the same order as the column
    /// metadata. Each field is deserialized according to its type specified
    /// in the corresponding column metadata.
    pub fn deserialize(columns: &[ColumnMetadata], bytes: &[u8]) -> Result<Self, RecordError> {
        let mut bytes = bytes;
        let fields = columns
            .iter()
            .map(|col| {
                let (field, remaining) = Field::deserialize(bytes, col.ty(), col.name())?;
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
#[derive(PartialEq, Debug)]
#[cfg_attr(test, derive(Clone))]
pub enum Field {
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
        column_type: ColumnType,
        name: &str,
    ) -> Result<(Self, &'a [u8]), RecordError> {
        match column_type {
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
                        field_name: name.into(),
                        expected: string_len,
                        actual: rest.len(),
                    });
                }
                let string_bytes = &rest[..string_len];
                let string = std::str::from_utf8(string_bytes)
                    .map_err(|_| RecordError::FailedToDeserialize {
                        field_name: name.into(),
                    })?
                    .into();
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
        if buffer.len() < N {
            return Err(RecordError::UnexpectedEnd {
                field_name: field_name.to_owned(),
                expected: N,
                actual: buffer.len(),
            });
        }
        let arr: [u8; N] = buffer[..N].try_into().unwrap();
        Ok((convert(arr), &buffer[N..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create ColumnMetadata for testing
    fn col(name: &str, ty: ColumnType) -> ColumnMetadata {
        ColumnMetadata::new(name.to_string(), ty, 0, 0, 0).unwrap()
    }
    #[test]
    fn fails_when_buffer_smaller_than_expected() {
        let buffer = [0x32, 0x33];
        let result = Field::deserialize(&buffer, ColumnType::I32, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn fails_when_string_bytes_are_invalid_utf8() {
        let buffer = [0x02, 0x00, 0xC2, 0x00];
        let result = Field::deserialize(&buffer, ColumnType::String, "name");
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(RecordError::FailedToDeserialize { .. })
        ));
    }

    #[test]
    fn fails_when_string_length_is_too_big() {
        let buffer = [0x03, 0x00, 0x01, 0x01];
        let result = Field::deserialize(&buffer, ColumnType::String, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn fails_when_deserializing_string_from_1_byte() {
        let buffer = [0x01];
        let result = Field::deserialize(&buffer, ColumnType::String, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn serializing_fixed_length_types_works() {
        let test_cases = [
            (Field::Int32(0x12345678), vec![0x78, 0x56, 0x34, 0x12]),
            (
                Field::Int64(0x123456789ABCDEF0),
                vec![0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12],
            ),
            (
                Field::Float32(std::f32::consts::PI),
                std::f32::consts::PI.to_le_bytes().to_vec(),
            ),
            (
                Field::Float64(-std::f64::consts::E),
                (-std::f64::consts::E).to_le_bytes().to_vec(),
            ),
            (Field::Bool(true), vec![0x01]),
            (Field::Bool(false), vec![0x00]),
            (Field::Date(19000), 19000_i32.to_le_bytes().to_vec()),
            (
                Field::DateTime(1640995200),
                1640995200_i64.to_le_bytes().to_vec(),
            ),
        ];

        for (field, expected_bytes) in test_cases {
            let mut buffer = Vec::new();
            field.serialize(&mut buffer);
            assert_eq!(buffer, expected_bytes);
        }
    }

    #[test]
    fn serializing_strings_works() {
        let string = "field 1 where id = 1";
        let field = Field::String(string.into());
        let mut buffer = Vec::new();
        field.serialize(&mut buffer);
        let buffer_len: [u8; 2] = buffer[0..2].try_into().unwrap();
        assert_eq!(u16::from_le_bytes(buffer_len), string.len() as u16);
        let string_bytes = &buffer[2..];
        assert_eq!(string_bytes, string.as_bytes());
    }

    #[test]
    fn test_deserialize_known_bytes() {
        let buffer: Vec<u8> = vec![
            42, 0, 0, 0, // Int32(42)
            1, // Bool(true)
            2, 0, 72, 105, // String("Hi")
            0, 0, 0, 0, 0, 0, 4, 64, // Float64(2.5)
        ];

        let mut rest = buffer.as_slice();
        let test_cases = [
            (ColumnType::I32, "age", Field::Int32(42)),
            (ColumnType::Bool, "flag", Field::Bool(true)),
            (ColumnType::String, "greeting", Field::String("Hi".into())),
            (ColumnType::F64, "pi", Field::Float64(2.5)),
        ];

        for case in test_cases {
            let (field, r) = Field::deserialize(rest, case.0, case.1).unwrap();
            assert_eq!(field, case.2);
            rest = r;
        }

        assert!(rest.is_empty());
    }

    #[test]
    fn deserialize_empty_string() {
        let buffer = [0x00, 0x00];
        let (field, rest) = Field::deserialize(&buffer, ColumnType::String, "empty").unwrap();
        assert_eq!(field, Field::String("".into()));
        assert!(rest.is_empty());
    }

    #[test]
    fn test_roundtrip_serialization_then_deserialization() {
        let fields = vec![
            Field::Int32(-42),
            Field::Int64(-10000000),
            Field::Date(2500),
            Field::DateTime(-13569),
            Field::Bool(true),
            Field::String("true".into()),
        ];

        let column_metadata = vec![
            col("int32", ColumnType::I32),
            col("int64", ColumnType::I64),
            col("date", ColumnType::Date),
            col("datetime", ColumnType::DateTime),
            col("flag", ColumnType::Bool),
            col("string", ColumnType::String),
        ];

        let cloned_fields = fields.clone();

        let record = Record::new(fields);
        let buffer = record.serialize();

        let deserialized_record = Record::deserialize(&column_metadata, buffer.as_slice());
        assert!(deserialized_record.is_ok());
        let deserialized_record = deserialized_record.unwrap();
        assert_eq!(deserialized_record.fields, cloned_fields);
    }
}
