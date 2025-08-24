use time::{Date, Duration, PrimitiveDateTime, Time};
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

impl From<Date> for DbDate {
    fn from(dt: Date) -> Self {
        let days_since_epoch = (dt - Date::from_ordinal_date(1970, 1).unwrap()).whole_days() as i32;
        Self { days_since_epoch }
    }
}

impl From<DbDate> for Date {
    fn from(value: DbDate) -> Self {
        let epoch = Date::from_ordinal_date(1970, 1).unwrap();
        epoch + Duration::days(value.days_since_epoch as i64)
    }
}
#[derive(Debug, PartialEq, Ord, PartialOrd, Eq, Clone, Copy)]
pub struct DbDateTime {
    days_since_epoch: i32,
    milliseconds_since_midnight: u32,
}
impl DbDateTime {
    const MILLISECONDS_IN_HOUR: u32 = 1000 * 60 * 60;
    const MILLISECONDS_IN_MINUTE: u32 = 1000 * 60;
    const MILLISECONDS_IN_SECOND: u32 = 1000;

    pub fn new(days_since_epoch: i32, milliseconds_since_midnight: u32) -> DbDateTime {
        Self {
            days_since_epoch,
            milliseconds_since_midnight,
        }
    }

    fn get_date(days_since_epoch: i32) -> Date {
        let epoch = Date::from_ordinal_date(1970, 1).unwrap();
        epoch + Duration::days(days_since_epoch as i64)
    }
    pub fn year(&self) -> i32 {
        Self::get_date(self.days_since_epoch).year()
    }

    pub fn month(&self) -> u8 {
        Self::get_date(self.days_since_epoch).month() as u8
    }

    pub fn day(&self) -> u8 {
        Self::get_date(self.days_since_epoch).day()
    }

    pub fn days_since_epoch(&self) -> i32 {
        self.days_since_epoch
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
}

impl From<PrimitiveDateTime> for DbDateTime {
    fn from(pdt: PrimitiveDateTime) -> Self {
        let days_since_epoch =
            (pdt.date() - Date::from_ordinal_date(1970, 1).unwrap()).whole_days() as i32;

        let time = pdt.time();
        let milliseconds_since_midnight = time.hour() as u32 * Self::MILLISECONDS_IN_HOUR
            + time.minute() as u32 * Self::MILLISECONDS_IN_MINUTE
            + time.second() as u32 * Self::MILLISECONDS_IN_SECOND
            + time.millisecond() as u32;

        Self {
            days_since_epoch,
            milliseconds_since_midnight,
        }
    }
}

impl From<DbDateTime> for PrimitiveDateTime {
    fn from(dt: DbDateTime) -> Self {
        let base_date =
            Date::from_ordinal_date(1970, 1).unwrap() + Duration::days(dt.days_since_epoch as i64);

        let hours = dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_HOUR;
        let minutes = (dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_MINUTE) % 60;
        let seconds = (dt.milliseconds_since_midnight / DbDateTime::MILLISECONDS_IN_SECOND) % 60;
        let millis = dt.milliseconds_since_midnight % 1000;

        let base_time =
            Time::from_hms_milli(hours as u8, minutes as u8, seconds as u8, millis as u16).unwrap();

        PrimitiveDateTime::new(base_date, base_time)
    }
}
