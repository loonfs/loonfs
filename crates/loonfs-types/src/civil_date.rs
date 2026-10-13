//! Calendar dates for Unix day counts, for code that renders a timestamp
//! without a date library.

/// A proleptic Gregorian calendar date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CivilDate {
    /// The year, which can be zero or negative before 1 CE.
    pub year: i64,
    /// The month, from 1 to 12.
    pub month: i64,
    /// The day of the month, from 1 to 31.
    pub day: i64,
}

/// Returns the calendar date `days` days after 1970-01-01.
///
/// This is Howard Hinnant's civil date conversion.
pub fn civil_from_days(days: i64) -> CivilDate {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    CivilDate {
        year: y + if m <= 2 { 1 } else { 0 },
        month: m,
        day: d,
    }
}
