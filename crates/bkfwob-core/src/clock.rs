//! Time, injectable so rate limits, daily budgets and grace windows are testable without sleeping.

use std::sync::{Arc, Mutex};

use jiff::civil::Date;
use jiff::tz::TimeZone;
use jiff::{SignedDuration, Timestamp};

/// The exchange calendar's zone: "today" for daily limits and journal files is the New York date,
/// so a session never straddles two of either.
pub const EXCHANGE_TZ: &str = "America/New_York";

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

pub type SharedClock = Arc<dyn Clock>;

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}

pub fn system() -> SharedClock {
    Arc::new(SystemClock)
}

/// A clock that only moves when told to.
pub struct ManualClock(Mutex<Timestamp>);

impl ManualClock {
    pub fn new(start: Timestamp) -> Arc<Self> {
        Arc::new(Self(Mutex::new(start)))
    }
    pub fn advance_secs(&self, secs: i64) {
        let mut now = self.0.lock().unwrap();
        *now = now.saturating_add(SignedDuration::from_secs(secs)).unwrap();
    }
    pub fn set(&self, at: Timestamp) {
        *self.0.lock().unwrap() = at;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        *self.0.lock().unwrap()
    }
}

fn exchange_tz() -> TimeZone {
    TimeZone::get(EXCHANGE_TZ).unwrap_or(TimeZone::UTC)
}

/// The exchange-local calendar date of an instant.
pub fn exchange_date(at: Timestamp) -> Date {
    at.to_zoned(exchange_tz()).date()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_exchange_date_is_new_york_not_utc() {
        // 02:00 UTC on the 5th is still the evening of the 4th in New York.
        let at: Timestamp = "2026-10-05T02:00:00Z".parse().unwrap();
        assert_eq!(exchange_date(at), jiff::civil::date(2026, 10, 4));
    }
}
