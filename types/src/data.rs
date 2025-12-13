use time::{Date, Duration, PrimitiveDateTime, Time};

use crate::lexicographic_serialization::{DecodeError, SortableSerialize};
use crate::{
    schema::Type,
    serialization::{DbSerializable, DbSerializationError},
};

// 1970-01-01 - the date we measure our date against.
const EPOCH_DATE: Date = match Date::from_ordinal_date(1970, 1) {
    Ok(date) => date,
    Err(_) => panic!("Failed to create epoch date"),
};

/// Wrapper struct for internal representation of the Date type (days since epoch).
///
/// Exposes functions for extracting parts of the date (year,month,day) for convenience
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Copy, Clone, Default)]
pub struct DbDate {
    days_since_epoch: i32,
}

impl DbDate {
    pub fn new(days_since_epoch: i32) -> DbDate {
        Self { days_since_epoch }
    }

    pub fn year(&self) -> i32 {
        Date::from(*self).year()
    }

    pub fn month(&self) -> u8 {
        Date::from(*self).month() as u8
    }

    pub fn day(&self) -> u8 {
        Date::from(*self).day()
    }

    pub fn days_since_epoch(&self) -> i32 {
        self.days_since_epoch
    }
}

/// This conversion is defined for usage in database inserts/updates where we want to convert the
/// coSQL representation of Date used in queries into the internal one.
impl From<Date> for DbDate {
    fn from(dt: Date) -> Self {
        let days_since_epoch = (dt - EPOCH_DATE).whole_days() as i32;
        Self { days_since_epoch }
    }
}

/// This conversion is defined for usage in returning the queried data to the client (if they want
/// to use a structured representation instead of raw days since epoch)
impl From<DbDate> for Date {
    fn from(value: DbDate) -> Self {
        EPOCH_DATE + Duration::days(value.days_since_epoch as i64)
    }
}

/// Wrapper struct for internal representation of the DateTime type (days since epoch + seconds since
/// midnight). Uses the [`DbDate`] struct for representing the day part for easy access to y/m/d methods.
///
/// Exposes functions for extracting parts of the date (year,month,day) and time (hours,minutes,seconds)
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Copy, Default)]
pub struct DbDateTime {
    date: DbDate,
    milliseconds_since_midnight: u32,
}

impl DbDateTime {
    const MILLISECONDS_IN_HOUR: u32 = 1000 * 60 * 60;
    const MILLISECONDS_IN_MINUTE: u32 = 1000 * 60;
    const MILLISECONDS_IN_SECOND: u32 = 1000;

    pub fn new(date: DbDate, milliseconds_since_midnight: u32) -> DbDateTime {
        Self {
            date,
            milliseconds_since_midnight,
        }
    }

    pub fn year(&self) -> i32 {
        self.date.year()
    }

    pub fn month(&self) -> u8 {
        self.date.month()
    }

    pub fn day(&self) -> u8 {
        self.date.day()
    }

    pub fn days_since_epoch(&self) -> i32 {
        self.date.days_since_epoch()
    }

    pub fn milliseconds_since_midnight(&self) -> u32 {
        self.milliseconds_since_midnight
    }
    pub fn hour(&self) -> u8 {
        (self.milliseconds_since_midnight / Self::MILLISECONDS_IN_HOUR) as u8
    }

    pub fn minute(&self) -> u8 {
        ((self.milliseconds_since_midnight / Self::MILLISECONDS_IN_MINUTE) % 60) as u8
    }
    pub fn second(&self) -> u8 {
        ((self.milliseconds_since_midnight / Self::MILLISECONDS_IN_SECOND) % 60) as u8
    }

    pub fn millisecond(&self) -> u16 {
        (self.milliseconds_since_midnight % 1000) as u16
    }
}

/// This conversion is defined for usage in database inserts/updates where we want to convert the
/// coSQL representation of DateTime used in queries into the internal one.
impl From<PrimitiveDateTime> for DbDateTime {
    fn from(pdt: PrimitiveDateTime) -> Self {
        let days_since_epoch = (pdt.date() - EPOCH_DATE).whole_days() as i32;

        let time = pdt.time();
        let milliseconds_since_midnight = time.hour() as u32 * Self::MILLISECONDS_IN_HOUR
            + time.minute() as u32 * Self::MILLISECONDS_IN_MINUTE
            + time.second() as u32 * Self::MILLISECONDS_IN_SECOND
            + time.millisecond() as u32;

        Self {
            date: DbDate::new(days_since_epoch),
            milliseconds_since_midnight,
        }
    }
}

/// This conversion is defined for usage in returning the queried data to the client (if they want
/// to use a structured representation instead of raw days since epoch)
impl From<DbDateTime> for PrimitiveDateTime {
    fn from(dt: DbDateTime) -> Self {
        let base_date = EPOCH_DATE + Duration::days(dt.days_since_epoch() as i64);

        let hours = dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_HOUR;
        let minutes = (dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_MINUTE) % 60;
        let seconds = (dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_SECOND) % 60;
        let millis = dt.milliseconds_since_midnight % 1000;

        let base_time =
            Time::from_hms_milli(hours as u8, minutes as u8, seconds as u8, millis as u16).unwrap();

        PrimitiveDateTime::new(base_date, base_time)
    }
}

/// Represents a typed value that can be used in coSQL.
#[derive(PartialEq, Debug, Clone)]
pub enum Value {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    DateTime(DbDateTime),
    Date(DbDate),
    String(String),
    Bool(bool),
}

impl Value {
    pub fn ty(&self) -> Type {
        match self {
            Value::Int32(_) => Type::I32,
            Value::Int64(_) => Type::I64,
            Value::Float32(_) => Type::F32,
            Value::Float64(_) => Type::F64,
            Value::DateTime(_) => Type::DateTime,
            Value::Date(_) => Type::Date,
            Value::String(_) => Type::String,
            Value::Bool(_) => Type::Bool,
        }
    }

    pub fn default_for_ty(ty: &Type) -> Self {
        match ty {
            Type::String => Value::String(String::default()),
            Type::F32 => Value::Float32(f32::default()),
            Type::F64 => Value::Float64(f64::default()),
            Type::I32 => Value::Int32(i32::default()),
            Type::I64 => Value::Int64(i64::default()),
            Type::Bool => Value::Bool(bool::default()),
            Type::Date => Value::Date(DbDate::default()),
            Type::DateTime => Value::DateTime(DbDateTime::default()),
        }
    }

    /// Serializes this value into the provided buffer.
    pub fn serialize(self, buffer: &mut Vec<u8>) {
        match self {
            Value::Int32(i) => i.serialize(buffer),
            Value::Int64(i) => i.serialize(buffer),
            Value::Float32(f) => f.serialize(buffer),
            Value::Float64(f) => f.serialize(buffer),
            Value::DateTime(d) => d.serialize(buffer),
            Value::Date(d) => d.serialize(buffer),
            Value::String(s) => s.serialize(buffer),
            Value::Bool(b) => b.serialize(buffer),
        }
    }

    /// Serializes this value at the beginning of the provided buffer.
    ///
    /// This function panics if buffer has not enough space to store the value.
    pub fn serialize_into(self, buffer: &mut [u8]) {
        match self {
            Value::Int32(i) => i.serialize_into(buffer),
            Value::Int64(i) => i.serialize_into(buffer),
            Value::Float32(f) => f.serialize_into(buffer),
            Value::Float64(f) => f.serialize_into(buffer),
            Value::DateTime(d) => d.serialize_into(buffer),
            Value::Date(d) => d.serialize_into(buffer),
            Value::String(s) => s.serialize_into(buffer),
            Value::Bool(b) => b.serialize_into(buffer),
        }
    }

    /// Returns number of bytes that serialized value will take on disk.
    pub fn size_serialized(&self) -> usize {
        match self {
            Value::Int32(i) => i.size_serialized(),
            Value::Int64(i) => i.size_serialized(),
            Value::Float32(f) => f.size_serialized(),
            Value::Float64(f) => f.size_serialized(),
            Value::DateTime(d) => d.size_serialized(),
            Value::Date(d) => d.size_serialized(),
            Value::String(s) => s.size_serialized(),
            Value::Bool(b) => b.size_serialized(),
        }
    }

    /// Deserializes a value from bytes using the provided type.
    ///
    /// Returns both the deserialized value and the remaining unconsumed bytes.
    pub fn deserialize(
        buffer: &[u8],
        column_type: Type,
    ) -> Result<(Self, &[u8]), DbSerializationError> {
        match column_type {
            Type::Bool => Self::deserialize_and_wrap(buffer, Value::Bool),
            Type::I32 => Self::deserialize_and_wrap(buffer, Value::Int32),
            Type::I64 => Self::deserialize_and_wrap(buffer, Value::Int64),
            Type::F32 => Self::deserialize_and_wrap(buffer, Value::Float32),
            Type::F64 => Self::deserialize_and_wrap(buffer, Value::Float64),
            Type::Date => Self::deserialize_and_wrap(buffer, Value::Date),
            Type::DateTime => Self::deserialize_and_wrap(buffer, Value::DateTime),
            Type::String => Self::deserialize_and_wrap(buffer, Value::String),
        }
    }

    /// Deserializes raw data type, wraps it into the corresponding value.
    fn deserialize_and_wrap<T, F>(
        buffer: &[u8],
        constructor: F,
    ) -> Result<(Value, &[u8]), DbSerializationError>
    where
        T: DbSerializable,
        F: FnOnce(T) -> Value,
    {
        T::deserialize(buffer).map(|(val, rest)| (constructor(val), rest))
    }

    /// Encodes this value as a key for use in indexes.
    pub fn encode_key(self) -> Vec<u8> {
        match self {
            Value::Int32(i) => i.encode_key(),
            Value::Int64(i) => i.encode_key(),
            Value::DateTime(dt) => dt.encode_key(),
            Value::Date(d) => d.encode_key(),
            Value::String(s) => s.encode_key(),
            _ => unreachable!(),
        }
    }

    /// Decodes a value from a key buffer using the provided type.
    pub fn decode_key(buffer: &[u8], column_type: Type) -> Result<Self, DecodeError> {
        match column_type {
            Type::I32 => i32::decode_key(buffer).map(Value::Int32),
            Type::I64 => i64::decode_key(buffer).map(Value::Int64),
            Type::Date => DbDate::decode_key(buffer).map(Value::Date),
            Type::DateTime => DbDateTime::decode_key(buffer).map(Value::DateTime),
            Type::String => String::decode_key(buffer).map(Value::String),
            ty => Err(DecodeError::UnsupportedType { ty: ty.to_string() }),
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<&DbDate> {
        match self {
            Value::Date(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<&DbDateTime> {
        match self {
            Value::DateTime(v) => Some(v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time_from_milliseconds_since_midnight(milliseconds_since_midnight: u32) -> Time {
        let hours = milliseconds_since_midnight / 3_600_000;
        let minutes = (milliseconds_since_midnight / 60_000) % 60;
        let seconds = (milliseconds_since_midnight / 1000) % 60;
        let millis = milliseconds_since_midnight % 1000;

        Time::from_hms_milli(hours as u8, minutes as u8, seconds as u8, millis as u16).unwrap()
    }
    #[test]
    fn test_db_date_works() {
        let known_date = EPOCH_DATE + Duration::days(1000);
        let date = DbDate::new(1000);

        assert_eq!(known_date.year(), date.year());
        assert_eq!(known_date.month() as u8, date.month());
        assert_eq!(known_date.day(), date.day());

        // conversion to time::Date works
        let converted_date = Date::from(date);
        assert_eq!(converted_date, known_date);

        // conversion from time::Date works
        let converted_db_date = DbDate::from(known_date);
        assert_eq!(converted_db_date, date);
    }

    #[test]
    fn test_db_datetime_works() {
        const MILLISECONDS_SINCE_MIDNIGHT: u32 = 156124;
        let known_datetime = PrimitiveDateTime::new(
            EPOCH_DATE + Duration::days(1000),
            time_from_milliseconds_since_midnight(MILLISECONDS_SINCE_MIDNIGHT),
        );
        let datetime = DbDateTime::new(DbDate::new(1000), MILLISECONDS_SINCE_MIDNIGHT);

        assert_eq!(known_datetime.year(), datetime.year());
        assert_eq!(known_datetime.month() as u8, datetime.month());
        assert_eq!(known_datetime.day(), datetime.day());
        assert_eq!(known_datetime.hour(), datetime.hour());
        assert_eq!(known_datetime.minute(), datetime.minute());
        assert_eq!(known_datetime.second(), datetime.second());
        assert_eq!(known_datetime.millisecond(), datetime.millisecond());

        // conversion to time::PrimitiveDateTime works
        let converted_datetime = PrimitiveDateTime::from(datetime);
        assert_eq!(converted_datetime, known_datetime);

        // conversion from time::PrimitiveDateTime works
        let converted_db_datetime = DbDateTime::from(known_datetime);
        assert_eq!(converted_db_datetime, datetime);
    }
}
