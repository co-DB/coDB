use engine::record::{Field as EngineField, Record as EngineRecord};
use executor::response::{ColumnData, StatementType as ExecutorStatementType};
use protocol::{ColumnMetadata, ColumnType, Date, DateTime, Field, Record, StatementType};
use types::{
    data::{DbDate, DbDateTime, Value},
    schema::Type,
};

pub(crate) trait IntoProtocol<T> {
    fn into_protocol(self) -> T;
}

impl IntoProtocol<ColumnType> for Type {
    fn into_protocol(self) -> ColumnType {
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

impl IntoProtocol<ColumnMetadata> for ColumnData {
    fn into_protocol(self) -> ColumnMetadata {
        ColumnMetadata {
            name: self.name,
            ty: self.ty.into_protocol(),
        }
    }
}

impl IntoProtocol<Record> for EngineRecord {
    fn into_protocol(self) -> Record {
        Record {
            fields: self.fields.into_iter().map(|f| f.into_protocol()).collect(),
        }
    }
}

impl IntoProtocol<Field> for EngineField {
    fn into_protocol(self) -> Field {
        match self.into() {
            Value::Int32(i) => Field::Int32(i),
            Value::Int64(i) => Field::Int64(i),
            Value::Float32(f) => Field::Float32(f),
            Value::Float64(f) => Field::Float64(f),
            Value::DateTime(dt) => Field::DateTime(dt.into_protocol()),
            Value::Date(d) => Field::Date(d.into_protocol()),
            Value::String(s) => Field::String(s),
            Value::Bool(b) => Field::Bool(b),
        }
    }
}

impl IntoProtocol<Date> for DbDate {
    fn into_protocol(self) -> Date {
        Date {
            days_since_epoch: self.days_since_epoch(),
        }
    }
}

impl IntoProtocol<DateTime> for DbDateTime {
    fn into_protocol(self) -> DateTime {
        DateTime {
            days_since_epoch: self.days_since_epoch(),
            milliseconds_since_midnight: self.milliseconds_since_midnight(),
        }
    }
}

impl IntoProtocol<StatementType> for ExecutorStatementType {
    fn into_protocol(self) -> StatementType {
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
