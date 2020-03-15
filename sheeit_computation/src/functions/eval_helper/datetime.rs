use chrono::{DateTime, Duration, TimeZone, Utc};
use lazy_static::lazy_static;

use std::time::SystemTime;

// TODO: This is not entirely accurate for any date before Feb 28th, 1900 I believe.
// We should be more precise. But this seems to be the implementation of SheetJS.
// https://github.com/tafia/calamine/issues/116
lazy_static! {
    static ref EXCEL_EPOCH: DateTime<Utc> = Utc.ymd(1899, 12, 30).and_hms(0, 0, 0);
}

pub fn to_excel_datetime(instant: SystemTime) -> f64 {
    let instant_datetime: DateTime<Utc> = instant.into();

    let duration_since_excel_epoch = instant_datetime.signed_duration_since(*EXCEL_EPOCH);

    let days = duration_since_excel_epoch.num_days();
    let milliseconds = duration_since_excel_epoch.num_milliseconds();

    let milliseconds_after_days = milliseconds - Duration::days(days).num_milliseconds();
    let fraction = Duration::milliseconds(milliseconds_after_days).num_milliseconds() as f64
        / Duration::days(1).num_milliseconds() as f64;

    days as f64 + fraction
}

// Can be included in the main binary once there is a function that consumes this
#[cfg(test)]
pub fn from_excel_datetime(since_excel_epoch: f64) -> SystemTime {
    let days_since_excel_epoch = Duration::days(since_excel_epoch.floor() as i64);
    let fraction = since_excel_epoch - since_excel_epoch.floor();

    let fraction_milliseconds =
        (Duration::days(1).num_milliseconds() as f64 * fraction).floor() as i64;

    let datetime =
        *EXCEL_EPOCH + days_since_excel_epoch + Duration::milliseconds(fraction_milliseconds);

    SystemTime::from(datetime)
}

// TODO: Add roundtrip tests. Should be fairly straightforward.
#[cfg(test)]
mod tests {
    use crate::functions::eval_helper::datetime::{from_excel_datetime, to_excel_datetime};
    use chrono::{TimeZone, Utc};
    use std::time::SystemTime;

    #[test]
    fn test_to_and_from_excel_datetime_whole_days() {
        let instant = SystemTime::from(Utc.ymd(2000, 1, 1).and_hms_milli(0, 0, 0, 0));
        let result = to_excel_datetime(instant);
        assert_eq!(result, 36526.0);
        assert_eq!(from_excel_datetime(result), instant);
    }

    #[test]
    fn test_to_and_from_excel_datetime_fractional() {
        let instant = SystemTime::from(Utc.ymd(2000, 1, 1).and_hms(1, 1, 1));
        let result = to_excel_datetime(instant);
        assert_eq!(result, 36526.04237268519);
        assert_eq!(from_excel_datetime(result), instant);
    }
}
