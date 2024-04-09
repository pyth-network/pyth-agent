pub mod holiday_hours;
pub mod market_hours;


use crate::agent::schedule::{
    holiday_hours::HolidaySchedule,
    market_hours::WeeklySchedule,
};

#[derive(Debug, Clone, Default)]
pub struct Schedule {
    pub market_hours:  WeeklySchedule,
    pub holiday_hours: HolidaySchedule,
}
