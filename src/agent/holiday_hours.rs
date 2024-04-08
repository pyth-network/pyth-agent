//! Holiday hours metadata parsing and evaluation logic

use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    chrono::{
        naive::NaiveTime,
        DateTime,
        Datelike,
        Duration,
        Utc,
    },
    chrono_tz::Tz,
    lazy_static::lazy_static,
    std::str::FromStr,
};

lazy_static! {
    /// Helper time value representing 24:00:00 as 00:00:00 minus 1
    /// nanosecond (underflowing to 23:59:59.999(...) ). While chrono
    /// has this value internally exposed as NaiveTime::MAX, it is not
    /// exposed outside the crate.
    static ref MAX_TIME_INSTANT: NaiveTime = NaiveTime::MIN.overflowing_sub_signed(Duration::nanoseconds(1)).0;
}

/// Holiday hours schedule
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct HolidaySchedule {
    pub timezone: Option<Tz>,
    pub days:     Vec<HolidayDaySchedule>,
}

impl HolidaySchedule {
    pub fn all_closed() -> Self {
        Self {
            timezone: Default::default(),
            days:     vec![],
        }
    }

    pub fn can_publish_at(&self, when: DateTime<Utc>) -> bool {
        // Convert to time local to the market
        let when_local = when.with_timezone(&self.timezone.unwrap());

        let market_month = when_local.date_naive().month0() + 1;
        let market_day = when_local.date_naive().day0() + 1;

        let market_time = when_local.time();

        for day in &self.days {
            // Check if the day matches
            if day.month == market_month && day.day == market_day {
                return day.kind.can_publish_at(market_time);
            }
        }
        true
    }
}

impl FromStr for HolidaySchedule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let split_by_commas = s.split(",");
        let mut days = Vec::new();

        for day_str in split_by_commas {
            let day = day_str.parse()?;
            days.push(day);
        }

        Ok(HolidaySchedule {
            days,
            timezone: None,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolidayDaySchedule {
    pub month: u32,
    pub day:   u32,
    pub kind:  HolidayDayKind,
}

impl FromStr for HolidayDaySchedule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let date_time_parts: Vec<&str> = s.split("/").collect();
        if date_time_parts.len() != 2 {
            return Err(anyhow!("Invalid format"));
        }

        if date_time_parts[0].len() != 4 {
            return Err(anyhow!("Invalid date format"));
        }

        let month: u32 = date_time_parts[0][..2]
            .parse()
            .map_err(|_| anyhow!("Invalid month"))?;
        let day: u32 = date_time_parts[0][2..]
            .parse()
            .map_err(|_| anyhow!("Invalid day"))?;
        let kind: HolidayDayKind = date_time_parts[1].parse()?;

        Ok(HolidayDaySchedule { month, day, kind })
    }
}

/// Helper enum for denoting per-day schedules: time range, all-day open and all-day closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HolidayDayKind {
    Open,
    Closed,
    TimeRange(NaiveTime, NaiveTime),
}

impl HolidayDayKind {
    pub fn can_publish_at(&self, when_market_local: NaiveTime) -> bool {
        match self {
            Self::Open => true,
            Self::Closed => false,
            Self::TimeRange(start, end) => start <= &when_market_local && &when_market_local <= end,
        }
    }
}

impl Default for HolidayDayKind {
    fn default() -> Self {
        Self::Open
    }
}

impl FromStr for HolidayDayKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "O" => Ok(HolidayDayKind::Open),
            "C" => Ok(HolidayDayKind::Closed),
            other => {
                let (start_str, end_str) = other.split_once("-").ok_or(anyhow!(
                    "Missing '-' delimiter between start and end of range"
                ))?;

                let start = NaiveTime::parse_from_str(start_str, "%H%M")
                    .context("start time does not match HHMM format")?;

                // The chrono crate is unable to parse 24:00 as
                // previous day's perspective of midnight, so we use
                // the next best thing - see MAX_TIME_INSTANT for
                // details.
                let end = if end_str.contains("2400") {
                    MAX_TIME_INSTANT.clone()
                } else {
                    NaiveTime::parse_from_str(end_str, "%H%M")
                        .context("end time does not match HHMM format")?
                };

                if start < end {
                    Ok(HolidayDayKind::TimeRange(start, end))
                } else {
                    Err(anyhow!("Incorrect time range: start must come before end"))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        chrono::NaiveDateTime,
    };

    #[test]
    fn test_parsing_single_day() -> Result<()> {
        let s = "0422/0900-1700";

        let parsed: HolidaySchedule = s.parse()?;

        let expected = HolidaySchedule {
            timezone: None,
            days:     vec![HolidayDaySchedule {
                month: 4,
                day:   22,
                kind:  HolidayDayKind::TimeRange(
                    NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                    NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
                ),
            }],
        };

        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn test_parsing_multiple_days() -> Result<()> {
        let s = "0422/0900-1700,1109/0930-1730,1201/O,1225/C,1231/0900-1700";

        let parsed: HolidaySchedule = s.parse()?;

        let expected = HolidaySchedule {
            timezone: None,
            days:     vec![
                HolidayDaySchedule {
                    month: 4,
                    day:   22,
                    kind:  HolidayDayKind::TimeRange(
                        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                        NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
                    ),
                },
                HolidayDaySchedule {
                    month: 11,
                    day:   9,
                    kind:  HolidayDayKind::TimeRange(
                        NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
                        NaiveTime::from_hms_opt(17, 30, 0).unwrap(),
                    ),
                },
                HolidayDaySchedule {
                    month: 12,
                    day:   1,
                    kind:  HolidayDayKind::Open,
                },
                HolidayDaySchedule {
                    month: 12,
                    day:   25,
                    kind:  HolidayDayKind::Closed,
                },
                HolidayDaySchedule {
                    month: 12,
                    day:   31,
                    kind:  HolidayDayKind::TimeRange(
                        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                        NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
                    ),
                },
            ],
        };

        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn test_parsing_month_without_leading_zero_is_error() {
        let s = "422/0900-1700";

        let parsing_result: Result<HolidaySchedule> = s.parse();
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_hour_without_leading_zero_is_error() {
        let s = "0422/900-1700";

        let parsing_result: Result<HolidaySchedule> = s.parse();
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_wrong_delimiter_is_error() {
        let s = "0422-0900/1700";

        let parsing_result: Result<HolidaySchedule> = s.parse();
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_wrong_format_is_error() {
        let s = "0422/09:00-17:00";

        let parsing_result: Result<HolidaySchedule> = s.parse();
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_wrong_time_range_is_error() {
        let s = "0422/1700-0900";

        let parsing_result: Result<HolidaySchedule> = s.parse();
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parse_24_hour() {
        let s = "0422/0900-2400";

        let parsed: HolidaySchedule = s.parse().unwrap();
        let expected = HolidaySchedule {
            timezone: None,
            days:     vec![HolidayDaySchedule {
                month: 4,
                day:   22,
                kind:  HolidayDayKind::TimeRange(
                    NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                    MAX_TIME_INSTANT.clone(),
                ),
            }],
        };
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_holiday_schedule_can_publish_at() -> Result<()> {
        // Prepare a schedule of narrow ranges
        let mut holiday_hours: HolidaySchedule =
            "0422/0900-1700,1109/0930-1730,1201/O,1225/C,1231/0900-1700"
                .parse()
                .unwrap();

        holiday_hours.timezone = Some(Tz::UTC);
        let format = "%Y-%m-%d %H:%M";

        // Date no match
        assert!(holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-11-20 05:30", format)?.and_utc()));

        // Date match before range
        assert!(!holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 08:59", format)?.and_utc()));

        // Date match at start of range
        assert!(holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 09:00", format)?.and_utc()));

        // Date match in range
        assert!(holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 12:00", format)?.and_utc()));

        // Date match at end of range
        assert!(holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 17:00", format)?.and_utc()));

        // Date match after range
        assert!(!holiday_hours
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 17:01", format)?.and_utc()));

        Ok(())
    }

    /// Verify desired 24:00 behavior.
    #[test]
    fn test_market_hours_midnight_00_24() -> Result<()> {
        // Prepare a schedule of midnight-neighboring ranges
        let mut holiday_schedule: HolidaySchedule = "0422/0900-2400".parse()?;

        holiday_schedule.timezone = Some(Tz::UTC);

        let format = "%Y-%m-%d %H:%M";
        // Date match before range
        assert!(!holiday_schedule
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 08:59", format)?.and_utc()));

        // Date match at start of range
        assert!(holiday_schedule
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 09:00", format)?.and_utc()));

        // Date match in range
        assert!(holiday_schedule
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 12:00", format)?.and_utc()));

        // Date match at end of range
        assert!(holiday_schedule
            .can_publish_at(NaiveDateTime::parse_from_str("2023-04-22 23:59", format)?.and_utc()));

        Ok(())
    }
}
