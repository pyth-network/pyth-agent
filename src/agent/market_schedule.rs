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
    winnow::{
        ascii::digit1,
        combinator::{
            alt,
            repeat,
            separated_pair,
        },
        error::ContextError,
        token::take,
        PResult,
        Parser,
    },
};


#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDayKind {
    Open,
    Closed,
    TimeRange(NaiveTime, NaiveTime),
}

impl ScheduleDayKind {
    pub fn can_publish_at(&self, when_market_local: NaiveTime) -> bool {
        match self {
            Self::Open => true,
            Self::Closed => false,
            Self::TimeRange(start, end) => start <= &when_market_local && &when_market_local <= end,
        }
    }
}

impl Default for ScheduleDayKind {
    fn default() -> Self {
        Self::Open
    }
}

fn time_range_parser<'s>(input: &mut &'s str) -> PResult<ScheduleDayKind> {
    let (start_str, end_str) = separated_pair(take(2usize), "-", take(2usize)).parse_next(input)?;

    let start_time = NaiveTime::parse_from_str(start_str, "%H%M").unwrap();
    let end_time = NaiveTime::parse_from_str(end_str, "%H%M").unwrap();

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
        crate::agent::schedule::holiday_hours::HolidayDayKind,
        chrono::{
            NaiveDate,
            NaiveDateTime,
        },
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
            open.parse::<HolidayDayKind>().unwrap(),
            HolidayDayKind::Open
        );
        assert_eq!(
            closed.parse::<HolidayDayKind>().unwrap(),
            HolidayDayKind::Closed
        );
        assert_eq!(
            valid.parse::<HolidayDayKind>().unwrap(),
            HolidayDayKind::TimeRange(
                NaiveTime::from_hms(12, 34, 0),
                NaiveTime::from_hms(13, 47, 0)
            )
        );
        assert!(invalid.parse::<HolidayDayKind>().is_err());
        assert!(invalid_format.parse::<HolidayDayKind>().is_err());

        Ok(())
    }
}
