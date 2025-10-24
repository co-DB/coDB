use thiserror::Error;
use time::{Date, Duration, PrimitiveDateTime, Time};

macro_rules! impl_db_serializable_for {
    ($($t:ty),*) => {
        $(
            impl DbSerializable for $t {
                fn serialize(&self, buffer: &mut Vec<u8>) {
                    buffer.extend(self.to_le_bytes());
                }

                fn serialize_into(&self, buffer: &mut [u8]) {
                    buffer[..size_of::<Self>()].copy_from_slice(&self.to_le_bytes());
                }

                fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
                    Self::read_fixed_and_convert::<$t, { size_of::<$t>() }>(buffer, <$t>::from_le_bytes)
                }
            }
        )*
    };
}

impl_db_serializable_for!(i32, i64, u8, u16, u32, u64, f32, f64);

/// A trait for types that can be serialized to and deserialized from bytes
/// for database storage.
pub(crate) trait DbSerializable: Sized {
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

#[derive(Error, Debug)]
pub(crate) enum DbSerializationError {
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
}

// 1970-01-01 - the date we measure our date against.
const EPOCH_DATE: Date = match Date::from_ordinal_date(1970, 1) {
    Ok(date) => date,
    Err(_) => panic!("Failed to create epoch date"),
};

/// Wrapper struct for internal representation of the Date type (days since epoch).
///
/// Exposes functions for extracting parts of the date (year,month,day) for convenience
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Copy, Clone)]
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
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Copy)]
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

impl DbSerializable for DbDateTime {
    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend(self.days_since_epoch().to_le_bytes());
        buffer.extend(self.milliseconds_since_midnight().to_le_bytes());
    }

    fn serialize_into(&self, buffer: &mut [u8]) {
        self.days_since_epoch().serialize_into(buffer);
        let days_since_epoch_size = size_of::<i32>();
        self.milliseconds_since_midnight().serialize_into(&mut buffer[days_since_epoch_size..]);
    }

    fn deserialize(buffer: &[u8]) -> Result<(Self, &[u8]), DbSerializationError> {
        let (days, rest) = i32::deserialize(buffer)?;
        let (milliseconds, rest) = u32::deserialize(rest)?;
        Ok((DbDateTime::new(DbDate::new(days), milliseconds), rest))
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
