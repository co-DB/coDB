use crate::data_types::{DbDate, DbDateTime, DbSerializable, DbSerializationError};

use metadata::{catalog::ColumnMetadata, types::Type};
use thiserror::Error;

/// Error for record related operations
#[derive(Error, Debug)]
pub(crate) enum RecordError {
    #[error(
        "while reading field {field_name}: expected to read {expected} bytes, but only {actual} were left in the buffer"
    )]
    UnexpectedEnd {
        field_name: String,
        expected: usize,
        actual: usize,
    },
    #[error("failed to deserialize field {field_name}")]
    FailedToDeserialize { field_name: String },
}

impl RecordError {
    /// Returns a mapped serialization error that now includes the name of the field being (de)serialized.
    fn map_serialization_error(
        db_serialization_error: DbSerializationError,
        field_name: &str,
    ) -> Self {
        match db_serialization_error {
            DbSerializationError::UnexpectedEnd { expected, actual } => Self::UnexpectedEnd {
                field_name: field_name.to_string(),
                expected,
                actual,
            },
            DbSerializationError::FailedToDeserialize => Self::FailedToDeserialize {
                field_name: field_name.to_string(),
            },
        }
    }
}

/// Represents a database record containing multiple typed fields.
///
/// A record is a collection of fields that correspond to the columns
/// defined in a table schema. Records can be serialized to bytes for
/// storage and deserialized back to their structured form.
#[derive(Debug)]
pub(crate) struct Record {
    pub fields: Vec<Field>,
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
        // TODO: Figure out where and when to return serialized record size for slotted page header and deserialization
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
    DateTime(DbDateTime),
    Date(DbDate),
    String(String),
    Bool(bool),
}

impl Field {
    /// Serializes this field into the provided buffer.
    ///
    /// All numeric types use little-endian byte ordering.
    fn serialize(self, buffer: &mut Vec<u8>) {
        match self {
            Field::Int32(i) => i.serialize(buffer),
            Field::Int64(i) => i.serialize(buffer),
            Field::Float32(f) => f.serialize(buffer),
            Field::Float64(f) => f.serialize(buffer),
            Field::DateTime(d) => d.serialize(buffer),
            Field::Date(d) => d.serialize(buffer),
            Field::String(s) => s.serialize(buffer),
            Field::Bool(b) => b.serialize(buffer),
        }
    }

    /// Deserializes raw data type, wraps it into the corresponding field and maps the error to the
    /// field error type (adding the name of the field we are deserializing)
    fn deserialize_and_wrap<'a, T, F>(
        buffer: &'a [u8],
        constructor: F,
        name: &str,
    ) -> Result<(Field, &'a [u8]), RecordError>
    where
        T: DbSerializable,
        F: FnOnce(T) -> Field,
    {
        T::deserialize(buffer)
            .map(|(val, rest)| (constructor(val), rest))
            .map_err(|err| RecordError::map_serialization_error(err, name))
    }

    /// Deserializes a field from bytes using the provided column metadata.
    ///
    /// Returns both the deserialized field and the remaining unconsumed bytes.
    fn deserialize<'a>(
        buffer: &'a [u8],
        column_type: Type,
        name: &str,
    ) -> Result<(Self, &'a [u8]), RecordError> {
        match column_type {
            Type::Bool => Self::deserialize_and_wrap(buffer, Field::Bool, name),
            Type::I32 => Self::deserialize_and_wrap(buffer, Field::Int32, name),
            Type::I64 => Self::deserialize_and_wrap(buffer, Field::Int64, name),
            Type::F32 => Self::deserialize_and_wrap(buffer, Field::Float32, name),
            Type::F64 => Self::deserialize_and_wrap(buffer, Field::Float64, name),
            Type::Date => Self::deserialize_and_wrap(buffer, Field::Date, name),
            Type::DateTime => Self::deserialize_and_wrap(buffer, Field::DateTime, name),
            Type::String => Self::deserialize_and_wrap(buffer, Field::String, name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create ColumnMetadata for testing
    fn col(name: &str, ty: Type) -> ColumnMetadata {
        ColumnMetadata::new(name.to_string(), ty, 0, 0, 0).unwrap()
    }
    #[test]
    fn fails_when_buffer_smaller_than_expected() {
        let buffer = [0x32, 0x33];
        let result = Field::deserialize(&buffer, Type::I32, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn fails_when_string_bytes_are_invalid_utf8() {
        let buffer = [0x02, 0x00, 0xC2, 0x00];
        let result = Field::deserialize(&buffer, Type::String, "name");
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(RecordError::FailedToDeserialize { .. })
        ));
    }

    #[test]
    fn fails_when_string_length_is_too_big() {
        let buffer = [0x03, 0x00, 0x01, 0x01];
        let result = Field::deserialize(&buffer, Type::String, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn fails_when_deserializing_string_from_1_byte() {
        let buffer = [0x01];
        let result = Field::deserialize(&buffer, Type::String, "name");
        assert!(result.is_err());
        assert!(matches!(result, Err(RecordError::UnexpectedEnd { .. })));
    }

    #[test]
    fn fails_for_invalid_bool() {
        let buffer = [0x02];
        let result = Field::deserialize(&buffer, Type::Bool, "name");
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(RecordError::FailedToDeserialize { .. })
        ));
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
            (
                Field::Date(DbDate::new(19000)),
                19000i32.to_le_bytes().to_vec(),
            ),
            (
                Field::DateTime(DbDateTime::new(DbDate::new(1234), 451)),
                [1234i32.to_le_bytes(), 451u32.to_le_bytes()].concat(),
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
            (Type::I32, "age", Field::Int32(42)),
            (Type::Bool, "flag", Field::Bool(true)),
            (Type::String, "greeting", Field::String("Hi".into())),
            (Type::F64, "pi", Field::Float64(2.5)),
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
        let (field, rest) = Field::deserialize(&buffer, Type::String, "empty").unwrap();
        assert_eq!(field, Field::String("".into()));
        assert!(rest.is_empty());
    }

    #[test]
    fn test_roundtrip_serialization_then_deserialization() {
        let fields = vec![
            Field::Int32(-42),
            Field::Int64(-10000000),
            Field::Date(DbDate::new(2500)),
            Field::DateTime(DbDateTime::new(DbDate::new(-12456), 1244)),
            Field::Bool(true),
            Field::String("true".into()),
        ];

        let column_metadata = vec![
            col("int32", Type::I32),
            col("int64", Type::I64),
            col("date", Type::Date),
            col("datetime", Type::DateTime),
            col("flag", Type::Bool),
            col("string", Type::String),
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
