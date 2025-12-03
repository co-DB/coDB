use engine::record::{Field as EngineField, Record as EngineRecord};
use executor::response::{ColumnData, StatementType as ExecutorStatementType};
use protocol::text_protocol::{
    ColumnMetadata, ColumnType, Date, DateTime, Field, Record, StatementType,
};
use types::{
    data::{DbDate, DbDateTime},
    schema::Type,
};

pub(crate) trait IntoTextProtocol<T> {
    fn into_text_protocol(self) -> T;
}

impl IntoTextProtocol<ColumnType> for Type {
    fn into_text_protocol(self) -> ColumnType {
        match self {
            Type::String => ColumnType::String,
            Type::F32 => ColumnType::F32,
            Type::F64 => ColumnType::F64,
            Type::I32 => ColumnType::I32,
            Type::I64 => ColumnType::I64,
            Type::Bool => ColumnType::Bool,
            Type::Date => ColumnType::Date,
            Type::DateTime => ColumnType::DateTime,
        }
    }
}

impl IntoTextProtocol<ColumnMetadata> for ColumnData {
    fn into_text_protocol(self) -> ColumnMetadata {
        ColumnMetadata {
            name: self.name,
            ty: self.ty.into_text_protocol(),
        }
    }
}

impl IntoTextProtocol<Record> for EngineRecord {
    fn into_text_protocol(self) -> Record {
        Record {
            fields: self
                .fields
                .into_iter()
                .map(|f| f.into_text_protocol())
                .collect(),
        }
    }
}

impl IntoTextProtocol<Field> for EngineField {
    fn into_text_protocol(self) -> Field {
        match self {
            EngineField::Int32(i) => Field::Int32(i),
            EngineField::Int64(i) => Field::Int64(i),
            EngineField::Float32(f) => Field::Float32(f),
            EngineField::Float64(f) => Field::Float64(f),
            EngineField::DateTime(dt) => Field::DateTime(dt.into_text_protocol()),
            EngineField::Date(d) => Field::Date(d.into_text_protocol()),
            EngineField::String(s) => Field::String(s),
            EngineField::Bool(b) => Field::Bool(b),
        }
    }
}

impl IntoTextProtocol<Date> for DbDate {
    fn into_text_protocol(self) -> Date {
        Date {
            days_since_epoch: self.days_since_epoch(),
        }
    }
}

impl IntoTextProtocol<DateTime> for DbDateTime {
    fn into_text_protocol(self) -> DateTime {
        DateTime {
            days_since_epoch: self.days_since_epoch(),
            milliseconds_since_midnight: self.milliseconds_since_midnight(),
        }
    }
}

impl IntoTextProtocol<StatementType> for ExecutorStatementType {
    fn into_text_protocol(self) -> StatementType {
        match self {
            ExecutorStatementType::Insert => StatementType::Insert,
            ExecutorStatementType::Update => StatementType::Update,
            ExecutorStatementType::Delete => StatementType::Delete,
            ExecutorStatementType::Create => StatementType::CreateTable,
            ExecutorStatementType::Alter => StatementType::AlterTable,
            ExecutorStatementType::Truncate => StatementType::TruncateTable,
            ExecutorStatementType::Drop => StatementType::DropTable,
        }
    }
}
