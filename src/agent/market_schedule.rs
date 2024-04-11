//! Holiday hours metadata parsing and evaluation logic

use {
    super::market_hours::MHKind,
    anyhow::{
        anyhow,
        Result,
    },
    chrono::{
        naive::NaiveTime,
        DateTime,
        Datelike,
        Utc,
    },
    chrono_tz::Tz,
    std::str::FromStr,
    winnow::{
        combinator::{
            alt,
            separated,
            separated_pair,
            seq,
        },
        error::{
            ErrMode,
            ErrorKind,
            ParserError,
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
            timezone: take_till(0.., ';').verify(|s| Tz::from_str(s).is_ok()).map(|s| Tz::from_str(s).unwrap()),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HolidayDaySchedule {
    pub month: u32,
    pub day:   u32,
    pub kind:  ScheduleDayKind,
}


fn holiday_day_schedule_parser<'s>(input: &mut &'s str) -> PResult<HolidayDaySchedule> {
    let ((month_str, day_str), kind) =
        separated_pair((take(2usize), take(2usize)), "/", schedule_day_kind_parser)
            .parse_next(input)?;

    let month = month_str.parse::<u32>().unwrap();
    let day = day_str.parse::<u32>().unwrap();

    Ok(HolidayDaySchedule { month, day, kind })
}

impl FromStr for HolidayDaySchedule {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        holiday_day_schedule_parser
            .parse_next(&mut s.to_owned().as_str())
            .map_err(|e| anyhow!(e))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

    pub fn from_mhkind(mhkind: MHKind) -> Self {
        match mhkind {
            MHKind::Open => ScheduleDayKind::Open,
            MHKind::Closed => ScheduleDayKind::Closed,
            MHKind::TimeRange(start, end) => ScheduleDayKind::TimeRange(start, end),
        }
    }
}

impl Default for ScheduleDayKind {
    fn default() -> Self {
        Self::Open
    }
}

fn time_range_parser<'s>(input: &mut &'s str) -> PResult<ScheduleDayKind> {
    let (start_str, end_str) = separated_pair(take(4usize), "-", take(4usize)).parse_next(input)?;

    let start_time = NaiveTime::parse_from_str(start_str, "%H%M")
        .map_err(|_| ErrMode::from_error_kind(input, ErrorKind::Verify))?;
    let end_time = NaiveTime::parse_from_str(end_str, "%H%M")
        .map_err(|_| ErrMode::from_error_kind(input, ErrorKind::Verify))?;

    Ok(ScheduleDayKind::TimeRange(start_time, end_time))
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
        let input = "America/New_York;O,1234-1347,C,C,C,C,O;0412/O,0413/C,0414/1234-1347";
        let expected = MarketSchedule {
            timezone:        Tz::America__New_York,
            weekly_schedule: vec![
                ScheduleDayKind::Open,
                ScheduleDayKind::TimeRange(
                    NaiveTime::from_hms_opt(12, 34, 0).unwrap(),
                    NaiveTime::from_hms_opt(13, 47, 0).unwrap(),
                ),
                ScheduleDayKind::Closed,
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
            "UTC;O,O,O,O,O,O,O;0422/0900-1700,1109/0930-1730,1201/O,1225/C,1231/0900-1700"
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
        Ok(())
    }
}
