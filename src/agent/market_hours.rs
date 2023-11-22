//! Market hours metadata parsing and evaluation logic

use {
    anyhow::{
        anyhow,
        Context,
        Result,
    },
    chrono::{
        naive::NaiveTime,
        DateTime,
        Duration,
        TimeZone,
        Weekday,
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

/// Weekly market hours schedule
#[derive(Default, Debug, Eq, PartialEq)]
pub struct MarketHours {
    pub timezone: Tz,
    pub mon:      MHKind,
    pub tue:      MHKind,
    pub wed:      MHKind,
    pub thu:      MHKind,
    pub fri:      MHKind,
    pub sat:      MHKind,
    pub sun:      MHKind,
}

impl MarketHours {
    pub fn all_closed() -> Self {
        Self {
            timezone: Default::default(),
            mon:      MHKind::Closed,
            tue:      MHKind::Closed,
            wed:      MHKind::Closed,
            thu:      MHKind::Closed,
            fri:      MHKind::Closed,
            sat:      MHKind::Closed,
            sun:      MHKind::Closed,
        }
    }

    pub fn can_publish_at<Tz: TimeZone>(&self, when: &DateTime<Tz>) -> Result<bool> {
        // Convert to time local to the market
        let when_market_local = when.with_timezone(&self.timezone);

        // NOTE(2023-11-21): Strangely enough, I couldn't find a
        // method that gets the programmatic Weekday from a DateTime.
        let market_weekday: Weekday = when_market_local.format("%A").to_string().parse()?;

        let market_time = when_market_local.time();

        let ret = match market_weekday {
            Weekday::Mon => self.mon.can_publish_at(market_time),
            Weekday::Tue => self.tue.can_publish_at(market_time),
            Weekday::Wed => self.wed.can_publish_at(market_time),
            Weekday::Thu => self.thu.can_publish_at(market_time),
            Weekday::Fri => self.fri.can_publish_at(market_time),
            Weekday::Sat => self.sat.can_publish_at(market_time),
            Weekday::Sun => self.sun.can_publish_at(market_time),
        };

        Ok(ret)
    }
}

impl FromStr for MarketHours {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let mut split_by_commas = s.split(",");

        // Timezone id, e.g. Europe/Paris
        let tz_str = split_by_commas.next().ok_or(anyhow!(
            "Market hours schedule ends before mandatory timezone field"
        ))?;
        let tz: Tz = tz_str
            .trim()
            .parse()
            .map_err(|e: String| anyhow!(e))
            .context(format!("Could parse timezone from {:?}", tz_str))?;

        let mut weekday_schedules = Vec::with_capacity(7);

        for weekday in &[
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ] {
            let mhkind_str = split_by_commas.next().ok_or(anyhow!(
                "Market hours schedule ends before mandatory {} field",
                weekday
            ))?;

            let mhkind: MHKind = mhkind_str.trim().parse().context(format!(
                "Could not parse {} field from {:?}",
                weekday, mhkind_str
            ))?;

            weekday_schedules.push(mhkind);
        }

        // We expect specifying wrong (incl. too large) amount of days
        // to be an easy mistake. We should catch it to avoid acting
        // on ambiguous schedule when there's too many day schedules
        // specified.
        if let Some(one_too_many) = split_by_commas.next() {
            return Err(anyhow!("Found unexpected 8th day spec {:?}", one_too_many));
        }

        // The compiler was not too happy with moving values via plain [] access
        let mut weekday_sched_iter = weekday_schedules.into_iter();

        let result = Self {
            timezone: tz,
            // These unwraps failing would be an internal error, but
            // panicking here does not seem wise.
            mon:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            tue:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            wed:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            thu:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            fri:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            sat:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
            sun:      weekday_sched_iter
                .next()
                .ok_or(anyhow!("INTERNAL: weekday_sched_iter too short"))?,
        };

        if let Some(_i_wish_lol) = weekday_sched_iter.next() {
            Err(anyhow!("INTERNAL: weekday_sched_iter too long"))
        } else {
            Ok(result)
        }
    }
}

/// Helper enum for denoting per-day schedules: time range, all-day open and all-day closed.
#[derive(Debug, Eq, PartialEq)]
pub enum MHKind {
    Open,
    Closed,
    TimeRange(NaiveTime, NaiveTime),
}

impl MHKind {
    pub fn can_publish_at(&self, when_market_local: NaiveTime) -> bool {
        match self {
            Self::Open => true,
            Self::Closed => false,
            Self::TimeRange(start, end) => start <= &when_market_local && &when_market_local <= end,
        }
    }
}

impl Default for MHKind {
    fn default() -> Self {
        Self::Open
    }
}

impl FromStr for MHKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "O" => Ok(MHKind::Open),
            "C" => Ok(MHKind::Closed),
            other => {
                let (start_str, end_str) = other.split_once("-").ok_or(anyhow!(
                    "Missing '-' delimiter between start and end of range"
                ))?;

                let start = NaiveTime::parse_from_str(start_str, "%H:%M")
                    .context("start time does not match HH:MM format")?;

                // The chrono crate is unable to parse 24:00 as
                // previous day's perspective of midnight, so we use
                // the next best thing - see MAX_TIME_INSTANT for
                // details.
                let end = if end_str.contains("24:00") {
                    MAX_TIME_INSTANT.clone()
                } else {
                    NaiveTime::parse_from_str(end_str, "%H:%M")
                        .context("end time does not match HH:MM format")?
                };

                if start < end {
                    Ok(MHKind::TimeRange(start, end))
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
        chrono::{
            NaiveDate,
            NaiveDateTime,
        },
    };

    #[test]
    fn test_parsing_happy_path() -> Result<()> {
        // Mon-Fri 9-5, inconsistent leading space on Tuesday, leading 0 on Friday (expected to be fine)
        let s = "Europe/Warsaw,9:00-17:00, 9:00-17:00,9:00-17:00,9:00-17:00,09:00-17:00,C,C";

        let parsed: MarketHours = s.parse()?;

        let expected = MarketHours {
            timezone: Tz::Europe__Warsaw,
            mon:      MHKind::TimeRange(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            ),
            tue:      MHKind::TimeRange(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            ),
            wed:      MHKind::TimeRange(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            ),
            thu:      MHKind::TimeRange(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            ),
            fri:      MHKind::TimeRange(
                NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
                NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            ),
            sat:      MHKind::Closed,
            sun:      MHKind::Closed,
        };

        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn test_parsing_no_timezone_is_error() {
        // Valid but missing a timezone
        let s = "O,C,O,C,O,C,O";

        let parsing_result: Result<MarketHours> = s.parse();

        dbg!(&parsing_result);
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_missing_sunday_is_error() {
        // One day short
        let s = "Asia/Hong_Kong,C,O,C,O,C,O";

        let parsing_result: Result<MarketHours> = s.parse();

        dbg!(&parsing_result);
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_gibberish_timezone_is_error() {
        // Pretty sure that one's extinct
        let s = "Pangea/New_Dino_City,O,O,O,O,O,O,O";
        let parsing_result: Result<MarketHours> = s.parse();

        dbg!(&parsing_result);
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_gibberish_day_schedule_is_error() {
        let s = "Europe/Amsterdam,mondays are alright I guess,O,O,O,O,O,O";
        let parsing_result: Result<MarketHours> = s.parse();

        dbg!(&parsing_result);
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_parsing_too_many_days_is_error() {
        // One day too many
        let s = "Europe/Lisbon,O,O,O,O,O,O,O,O,C";
        let parsing_result: Result<MarketHours> = s.parse();

        dbg!(&parsing_result);
        assert!(parsing_result.is_err());
    }

    #[test]
    fn test_market_hours_happy_path() -> Result<()> {
        // Prepare a schedule of narrow ranges
        let mh: MarketHours = "America/New_York,00:00-1:00,1:00-2:00,2:00-3:00,3:00-4:00,4:00-5:00,5:00-6:00,6:00-7:00".parse()?;

        // Prepare UTC datetimes that fall before, within and after market hours
        let format = "%Y-%m-%d %H:%M";
        let bad_datetimes_before = vec![
            NaiveDateTime::parse_from_str("2023-11-20 04:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-21 05:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-22 06:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-23 07:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-24 08:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-25 09:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-26 10:30", format)?.and_utc(),
        ];

        let ok_datetimes = vec![
            NaiveDateTime::parse_from_str("2023-11-20 05:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-21 06:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-22 07:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-23 08:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-24 09:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-25 10:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-26 11:30", format)?.and_utc(),
        ];

        let bad_datetimes_after = vec![
            NaiveDateTime::parse_from_str("2023-11-20 06:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-21 07:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-22 08:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-23 09:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-24 10:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-25 11:30", format)?.and_utc(),
            NaiveDateTime::parse_from_str("2023-11-26 12:30", format)?.and_utc(),
        ];

        dbg!(&mh);

        for ((before_dt, ok_dt), after_dt) in bad_datetimes_before
            .iter()
            .zip(ok_datetimes.iter())
            .zip(bad_datetimes_after.iter())
        {
            dbg!(&before_dt);
            dbg!(&ok_dt);
            dbg!(&after_dt);

            assert!(!mh.can_publish_at(before_dt)?);
            assert!(mh.can_publish_at(ok_dt)?);
            assert!(!mh.can_publish_at(after_dt)?);
        }

        Ok(())
    }

    /// Verify desired 24:00 behavior.
    #[test]
    fn test_market_hours_midnight_00_24() -> Result<()> {
        // Prepare a schedule of midnight-neighboring ranges
        let mh: MarketHours = "Europe/Amsterdam,23:00-24:00,00:00-01:00,C,C,C,C,C".parse()?;

        let format = "%Y-%m-%d %H:%M";
        let ok_datetimes = vec![
            NaiveDate::from_ymd_opt(2023, 11, 20)
                .unwrap()
                .and_time(MAX_TIME_INSTANT.clone())
                .and_local_timezone(Tz::Europe__Amsterdam)
                .unwrap(),
            NaiveDateTime::parse_from_str("2023-11-21 00:00", format)?
                .and_local_timezone(Tz::Europe__Amsterdam)
                .unwrap(),
        ];

        let bad_datetimes = vec![
            // Start of Monday Nov 20th, must not be confused for MAX_TIME_INSTANT on that day
            NaiveDateTime::parse_from_str("2023-11-20 00:00", format)?
                .and_local_timezone(Tz::Europe__Amsterdam)
                .unwrap(),
            // End of Tuesday Nov 21st, borders Wednesday, must not be
            // confused for Wednesday 00:00 which is open.
            NaiveDate::from_ymd_opt(2023, 11, 21)
                .unwrap()
                .and_time(MAX_TIME_INSTANT.clone())
                .and_local_timezone(Tz::Europe__Amsterdam)
                .unwrap(),
        ];

        dbg!(&mh);

        for (ok_dt, bad_dt) in ok_datetimes.iter().zip(bad_datetimes.iter()) {
            dbg!(&ok_dt);
            dbg!(&bad_dt);

            assert!(mh.can_publish_at(ok_dt)?);
            assert!(!mh.can_publish_at(bad_dt)?);
        }

        Ok(())
    }
}
