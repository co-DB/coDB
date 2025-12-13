use metadata::catalog::ColumnMetadata;
use std::ops::Deref;
use thiserror::Error;
use types::{data::Value, schema::Type, serialization::DbSerializationError};

/// Error for record related operations
#[derive(Error, Debug)]
pub enum RecordError {
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
        field_name: impl Into<String>,
    ) -> Self {
        match db_serialization_error {
            DbSerializationError::UnexpectedEnd { expected, actual } => Self::UnexpectedEnd {
                field_name: field_name.into(),
                expected,
                actual,
            },
            DbSerializationError::FailedToDeserialize => Self::FailedToDeserialize {
                field_name: field_name.into(),
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
pub struct Record {
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
#[derive(PartialEq, Debug, Clone)]
pub struct Field(Value);

impl Field {
    /// Serializes this field into the provided buffer.
    pub fn serialize(self, buffer: &mut Vec<u8>) {
        self.0.serialize(buffer);
    }

    /// Deserializes a field from bytes using the provided type.
    ///
    /// Returns both the deserialized field and the remaining unconsumed bytes.
    pub(crate) fn deserialize(
        buffer: &[u8],
        column_type: Type,
        column_name: impl Into<String>,
    ) -> Result<(Self, &[u8]), RecordError> {
        let (value, rest) = Value::deserialize(buffer, column_type)
            .map_err(|err| RecordError::map_serialization_error(err, column_name))?;
        Ok((value.into(), rest))
    }
}

impl Deref for Field {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Value> for Field {
    fn from(value: Value) -> Self {
        Field(value)
    }
}

impl From<Field> for Value {
    fn from(value: Field) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use types::data::{DbDate, DbDateTime};

    use super::*;

    // Helper function to create ColumnMetadata for testing
    fn col(name: &str, ty: Type) -> ColumnMetadata {
        ColumnMetadata::new(name.to_string(), ty, 0, 0, 0).unwrap()
    }

    #[test]
    fn test_roundtrip_serialization_then_deserialization() {
        let fields = vec![
            Field(Value::Int32(-42)),
            Field(Value::Int64(-10000000)),
            Field(Value::Date(DbDate::new(2500))),
            Field(Value::DateTime(DbDateTime::new(DbDate::new(-12456), 1244))),
            Field(Value::Bool(true)),
            Field(Value::String("true".into())),
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
