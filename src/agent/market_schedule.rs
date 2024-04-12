//! Holiday hours metadata parsing and evaluation logic

use {
    super::legacy_schedule::{
        LegacySchedule,
        MHKind,
    },
    anyhow::{
        anyhow,
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
    proptest::{
        arbitrary::any,
        prop_compose,
        proptest,
    },
    std::{
        fmt::Display,
        str::FromStr,
    },
    winnow::{
        combinator::{
            alt,
            separated,
            seq,
        },
        stream::ToUsize,
        token::{
            take,
            take_till,
        },
        PResult,
        Parser,
    },
};


/// Helper time value representing 24:00:00 as 00:00:00 minus 1
/// nanosecond (underflowing to 23:59:59.999(...) ). While chrono
/// has this value internally exposed as NaiveTime::MAX, it is not
/// exposed outside the crate.
const MAX_TIME_INSTANT: NaiveTime = NaiveTime::MIN
    .overflowing_sub_signed(Duration::nanoseconds(1))
    .0;


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketSchedule {
    pub timezone:        Tz,
    pub weekly_schedule: Vec<ScheduleDayKind>,
    pub holidays:        Vec<HolidayDaySchedule>,
}

impl Default for MarketSchedule {
    fn default() -> Self {
        Self {
            timezone:        Tz::UTC,
            weekly_schedule: vec![ScheduleDayKind::Open; 7],
            holidays:        vec![],
        }
    }
}

impl Display for MarketSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{};", self.timezone)?;
        for (i, day) in self.weekly_schedule.iter().enumerate() {
            write!(f, "{}", day)?;
            if i < 6 {
                write!(f, ",")?;
            }
        }
        write!(f, ";")?;
        for (i, holiday) in self.holidays.iter().enumerate() {
            write!(f, "{}", holiday)?;
            if i < self.holidays.len() - 1 {
                write!(f, ",")?;
            }
        }
        Ok(())
    }
}

impl MarketSchedule {
    pub fn can_publish_at(&self, when: &DateTime<Utc>) -> bool {
        let when_local = when.with_timezone(&self.timezone);

        let month = when_local.date_naive().month0() + 1;
        let day = when_local.date_naive().day0() + 1;
        let time = when_local.time();
        let weekday = when_local.weekday().number_from_monday().to_usize();

        for holiday in &self.holidays {
            // Check if the day matches
            if holiday.month == month && holiday.day == day {
                return holiday.kind.can_publish_at(time);
            }
        }

        self.weekly_schedule[weekday].can_publish_at(time)
    }
}

fn market_schedule_parser<'s>(input: &mut &'s str) -> PResult<MarketSchedule> {
    seq!(
        MarketSchedule {
            timezone: take_till(0.., ';').verify_map(|s| Tz::from_str(s).ok()),
            _: ';',
            weekly_schedule: separated(7, schedule_day_kind_parser, ","),
            _: ';',
            holidays: separated(0.., holiday_day_schedule_parser, ","),
        }
    )
    .parse_next(input)
}

impl FromStr for MarketSchedule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        market_schedule_parser
            .parse_next(&mut s.to_owned().as_str())
            .map_err(|e| anyhow!(e))
    }
}

impl From<LegacySchedule> for MarketSchedule {
    fn from(legacy: LegacySchedule) -> Self {
        Self {
            timezone:        legacy.timezone,
            weekly_schedule: vec![
                legacy.mon.into(),
                legacy.tue.into(),
                legacy.wed.into(),
                legacy.thu.into(),
                legacy.fri.into(),
                legacy.sat.into(),
                legacy.sun.into(),
            ],
            holidays:        vec![],
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolidayDaySchedule {
    pub month: u32,
    pub day:   u32,
    pub kind:  ScheduleDayKind,
}

fn two_digit_parser<'s>(input: &mut &'s str) -> PResult<u32> {
    take(2usize)
        .verify_map(|s| u32::from_str(s).ok())
        .parse_next(input)
}

fn holiday_day_schedule_parser<'s>(input: &mut &'s str) -> PResult<HolidayDaySchedule> {
    // day and month are not validated to be correct dates
    // if they are invalid, it will be ignored since there
    // are no real dates that match the invalid input
    seq!(
        HolidayDaySchedule {
            month: two_digit_parser,
            day: two_digit_parser,
            _: "/",
            kind: schedule_day_kind_parser,
        }
    )
    .parse_next(input)
}

impl FromStr for HolidayDaySchedule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        holiday_day_schedule_parser
            .parse_next(&mut s.to_owned().as_str())
            .map_err(|e| anyhow!(e))
    }
}

impl Display for HolidayDaySchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:02}{:02}/{}", self.month, self.day, self.kind)
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Copy)]
pub enum ScheduleDayKind {
    Open,
    Closed,
    TimeRange(NaiveTime, NaiveTime),
}

impl ScheduleDayKind {
    pub fn can_publish_at(&self, when_local: NaiveTime) -> bool {
        match self {
            Self::Open => true,
            Self::Closed => false,
            Self::TimeRange(start, end) => start <= &when_local && &when_local <= end,
        }
    }
}

impl Default for ScheduleDayKind {
    fn default() -> Self {
        Self::Open
    }
}

impl Display for ScheduleDayKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Open => write!(f, "O"),
            Self::Closed => write!(f, "C"),
            Self::TimeRange(start, end) => {
                write!(f, "{}-{}", start.format("%H%M"), end.format("%H%M"))
            }
        }
    }
}

fn time_parser<'s>(input: &mut &'s str) -> PResult<NaiveTime> {
    alt(("2400", take(4usize)))
        .verify_map(|time_str| match time_str {
            "2400" => Some(MAX_TIME_INSTANT),
            _ => NaiveTime::parse_from_str(time_str, "%H%M").ok(),
        })
        .parse_next(input)
}

fn time_range_parser<'s>(input: &mut &'s str) -> PResult<ScheduleDayKind> {
    seq!(
        time_parser,
        _: "-",
        time_parser,
    )
    .map(|s| ScheduleDayKind::TimeRange(s.0, s.1))
    .parse_next(input)
}

fn schedule_day_kind_parser<'s>(input: &mut &'s str) -> PResult<ScheduleDayKind> {
    alt((
        "C".map(|_| ScheduleDayKind::Closed),
        "O".map(|_| ScheduleDayKind::Open),
        time_range_parser,
    ))
    .parse_next(input)
}

impl FromStr for ScheduleDayKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        schedule_day_kind_parser
            .parse_next(&mut s.to_owned().as_str())
            .map_err(|e| anyhow!(e))
    }
}

impl From<MHKind> for ScheduleDayKind {
    fn from(mhkind: MHKind) -> Self {
        match mhkind {
            MHKind::Open => ScheduleDayKind::Open,
            MHKind::Closed => ScheduleDayKind::Closed,
            MHKind::TimeRange(start, end) => ScheduleDayKind::TimeRange(start, end),
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
    fn test_parsing_schedule_day_kind() -> Result<()> {
        // Mon-Fri 9-5, inconsistent leading space on Tuesday, leading 0 on Friday (expected to be fine)
        let open = "O";
        let closed = "C";
        let valid = "1234-1347";
        let valid2400 = "1234-2400";
        let invalid = "1234-5668";
        let invalid_format = "1234-56";

        assert_eq!(
            open.parse::<ScheduleDayKind>().unwrap(),
            ScheduleDayKind::Open
        );
        assert_eq!(
            closed.parse::<ScheduleDayKind>().unwrap(),
            ScheduleDayKind::Closed
        );
        assert_eq!(
            valid.parse::<ScheduleDayKind>().unwrap(),
            ScheduleDayKind::TimeRange(
                NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                NaiveTime::from_hms_opt(13, 47, 0).unwrap(),
            )
        );
        assert_eq!(
            valid2400.parse::<ScheduleDayKind>().unwrap(),
            ScheduleDayKind::TimeRange(
                NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                MAX_TIME_INSTANT,
            )
        );
        assert!(invalid.parse::<ScheduleDayKind>().is_err());
        assert!(invalid_format.parse::<ScheduleDayKind>().is_err());

        Ok(())
    }

    #[test]
    fn test_parsing_holiday_day_schedule() -> Result<()> {
        let input = "0412/O";
        let expected = HolidayDaySchedule {
            month: 04,
            day:   12,
            kind:  ScheduleDayKind::Open,
        };

        let parsed = input.parse::<HolidayDaySchedule>()?;
        assert_eq!(parsed, expected);

        let input = "0412/C";
        let expected = HolidayDaySchedule {
            month: 04,
            day:   12,
            kind:  ScheduleDayKind::Closed,
        };
        let parsed = input.parse::<HolidayDaySchedule>()?;
        assert_eq!(parsed, expected);

        let input = "0412/1234-1347";
        let expected = HolidayDaySchedule {
            month: 04,
            day:   12,
            kind:  ScheduleDayKind::TimeRange(
                NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                NaiveTime::from_hms_opt(13, 47, 0).unwrap(),
            ),
        };
        let parsed = input.parse::<HolidayDaySchedule>()?;
        assert_eq!(parsed, expected);

        let input = "0412/1234-5332";
        assert!(input.parse::<HolidayDaySchedule>().is_err());

        let input = "0412/1234-53";
        assert!(input.parse::<HolidayDaySchedule>().is_err());

        Ok(())
    }

    #[test]
    fn test_parsing_market_schedule() -> Result<()> {
        let input = "America/New_York;O,1234-1347,0930-2400,C,C,C,O;0412/O,0413/C,0414/1234-1347,1230/0930-2400";
        let expected = MarketSchedule {
            timezone:        Tz::America__New_York,
            weekly_schedule: vec![
                ScheduleDayKind::Open,
                ScheduleDayKind::TimeRange(
                    NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                    NaiveTime::from_hms_opt(13, 47, 0).unwrap(),
                ),
                ScheduleDayKind::TimeRange(
                    NaiveTime::from_hms_opt(09, 30, 0).unwrap(),
                    MAX_TIME_INSTANT,
                ),
                ScheduleDayKind::Closed,
                ScheduleDayKind::Closed,
                ScheduleDayKind::Closed,
                ScheduleDayKind::Open,
            ],
            holidays:        vec![
                HolidayDaySchedule {
                    month: 04,
                    day:   12,
                    kind:  ScheduleDayKind::Open,
                },
                HolidayDaySchedule {
                    month: 04,
                    day:   13,
                    kind:  ScheduleDayKind::Closed,
                },
                HolidayDaySchedule {
                    month: 04,
                    day:   14,
                    kind:  ScheduleDayKind::TimeRange(
                        NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                        NaiveTime::from_hms_opt(13, 47, 0).unwrap(),
                    ),
                },
                HolidayDaySchedule {
                    month: 12,
                    day:   30,
                    kind:  ScheduleDayKind::TimeRange(
                        NaiveTime::from_hms_opt(09, 30, 0).unwrap(),
                        MAX_TIME_INSTANT,
                    ),
                },
            ],
        };

        let parsed = input.parse::<MarketSchedule>()?;
        assert_eq!(parsed, expected);

        Ok(())
    }

    #[test]
    fn invalid_timezone_is_err() {
        let input = "Invalid/Timezone;O,C,C,C,C,C,O;0412/O,0413/C,0414/1234-1347";
        assert!(input.parse::<MarketSchedule>().is_err());
    }

    #[test]
    fn test_market_schedule_can_publish_at() -> Result<()> {
        // Prepare a schedule of narrow ranges
        let market_schedule: MarketSchedule =
            "UTC;O,O,O,O,O,O,O;0422/0900-1700,1109/0930-1730,1201/O,1225/C,1231/0930-2400"
                .parse()
                .unwrap();

        let format = "%Y-%m-%d %H:%M";

        // Date no match
        assert!(market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-11-20 05:30", format)?.and_utc()));

        // Date match before range
        assert!(!market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-04-22 08:59", format)?.and_utc()));

        // Date match at start of range
        assert!(market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-04-22 09:00", format)?.and_utc()));

        // Date match in range
        assert!(market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-04-22 12:00", format)?.and_utc()));

        // Date match at end of range
        assert!(market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-04-22 17:00", format)?.and_utc()));

        // Date match after range
        assert!(!market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-04-22 17:01", format)?.and_utc()));

        // Date 2400 range
        assert!(market_schedule
            .can_publish_at(&NaiveDateTime::parse_from_str("2023-12-31 23:59", format)?.and_utc()));
        Ok(())
    }
}

prop_compose! {
    fn schedule_day_kind()(
        r in any::<u8>(),
        t1 in any::<u32>(),
        t2 in any::<u32>(),
    ) -> ScheduleDayKind {
        match r % 3 {
            0 => ScheduleDayKind::Open,
            1 => ScheduleDayKind::Closed,
            _ => ScheduleDayKind::TimeRange(
                NaiveTime::from_hms_opt(t1 % 24, t1 / 24 % 60, 0).unwrap(),
                NaiveTime::from_hms_opt(t2 % 24, t2 / 24 % 60, 0).unwrap(),
            ),
        }
    }
}

prop_compose! {
    fn holiday_day_schedule()(
        m in 1..=12u32,
        d in 1..=31u32,
        s in schedule_day_kind(),
    ) -> HolidayDaySchedule {
        HolidayDaySchedule {
            month: m,
            day: d,
            kind: s,
        }
    }
}

prop_compose! {
    fn market_schedule()(
        tz in proptest::sample::select(vec![
            Tz::UTC,
            Tz::America__New_York,
            Tz::America__Los_Angeles,
            Tz::America__Chicago,
            Tz::Singapore,
            Tz::Australia__Sydney,
        ]),
        weekly_schedule in proptest::collection::vec(schedule_day_kind(), 7..=7),
        holidays in proptest::collection::vec(holiday_day_schedule(), 0..12),
    ) -> MarketSchedule {
        MarketSchedule {
            timezone: tz,
            weekly_schedule,
            holidays,
        }
    }
}

// Matches C or O or hhmm-hhmm with 24-hour time
const VALID_SCHEDULE_DAY_KIND_REGEX: &str =
    "C|O|([01][1-9]|2[0-3])([0-5][0-9])-([01][1-9]|2[0-3])([0-5][0-9])";

// Matches MMDD with MM and DD being 01-12 and 01-31 respectively
const VALID_MONTH_DAY_REGEX: &str = "(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])";

proptest!(
    #[test]
    fn doesnt_crash(s in "\\PC*") {
        _ = s.parse::<MarketSchedule>();
        _ = s.parse::<HolidayDaySchedule>();
        _ = s.parse::<ScheduleDayKind>();
    }

    #[test]
    fn parse_valid_schedule_day_kind(s in VALID_SCHEDULE_DAY_KIND_REGEX) {
        assert!(s.parse::<ScheduleDayKind>().is_ok());
    }

    #[test]
    fn test_valid_schedule_day_kind(s in schedule_day_kind()) {
        assert_eq!(s, s.to_string().parse::<ScheduleDayKind>().unwrap());
    }

    #[test]
    fn parse_valid_holiday_day_schedule(s in VALID_SCHEDULE_DAY_KIND_REGEX, d in VALID_MONTH_DAY_REGEX) {
        let valid_holiday_day = format!("{}/{}", d, s);
        assert!(valid_holiday_day.parse::<HolidayDaySchedule>().is_ok());
    }

    #[test]
    fn test_valid_holiday_day_schedule(s in holiday_day_schedule()) {
        assert_eq!(s, s.to_string().parse::<HolidayDaySchedule>().unwrap());
    }

    #[test]
    fn test_valid_market_schedule(s in market_schedule()) {
        assert_eq!(s, s.to_string().parse::<MarketSchedule>().unwrap());
    }
);
