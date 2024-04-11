//! Holiday hours metadata parsing and evaluation logic

use {
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
            repeat,
            separated_pair,
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


#[derive(Clone, Debug)]
pub struct MarketSchedule {
    pub timezone:        Tz,
    pub weekly_schedule: Vec<ScheduleDayKind>,
    pub holidays:        Vec<HolidayDaySchedule>,
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
    let timezone: Tz = take_till(0.., ';').parse_next(input)?.parse().unwrap();
    let weekly_schedule = repeat(7, schedule_day_kind_parser).parse_next(input)?;
    let holidays = repeat(0.., holiday_day_schedule_parser).parse_next(input)?;

    Ok(MarketSchedule {
        timezone,
        weekly_schedule,
        holidays,
    })
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

    dbg!(month_str, day_str, kind.clone());

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
}

impl Default for ScheduleDayKind {
    fn default() -> Self {
        Self::Open
    }
}

fn time_range_parser<'s>(input: &mut &'s str) -> PResult<ScheduleDayKind> {
    let (start_str, end_str) = separated_pair(take(4usize), "-", take(4usize)).parse_next(input)?;

    let start_time = NaiveTime::parse_from_str(start_str, "%H%M")
        .map_err(|e| ErrMode::from_error_kind(input, ErrorKind::Verify))?;
    let end_time = NaiveTime::parse_from_str(end_str, "%H%M")
        .map_err(|e| ErrMode::from_error_kind(input, ErrorKind::Verify))?;

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
    use super::*;

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
}
