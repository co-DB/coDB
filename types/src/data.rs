use time::{Date, Duration, PrimitiveDateTime, Time};

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
