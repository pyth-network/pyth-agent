use holiday_hours::HolidaySchedule;
use market_hours::WeeklySchedule;

pub mod holiday_hours;
pub mod market_hours;

#[derive(Debug, Clone, Default)]
pub struct Schedule {
    pub market_hours:  WeeklySchedule,
    pub holiday_hours: HolidaySchedule,
}
