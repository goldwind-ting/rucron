use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Weekday};
use std::{fmt, sync::atomic::Ordering};

use crate::{metric::Metric, METRIC_STORAGE};
/// Time unit.
pub(crate) enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl TimeUnit {
    #[inline(always)]
    fn granularity(&self) -> Duration {
        match self {
            Self::Second => Duration::seconds(1),
            Self::Minute => Duration::minutes(1),
            Self::Hour => Duration::hours(1),
            Self::Day => Duration::days(1),
            Self::Week => Duration::weeks(1),
        }
    }
    #[inline(always)]
    fn number_from_zero(&self) -> i8 {
        match self {
            Self::Second => 0,
            Self::Minute => 1,
            Self::Hour => 2,
            Self::Day => 3,
            Self::Week => 4,
        }
    }
}

impl fmt::Debug for TimeUnit {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Second => write!(fmt, "Second"),
            Self::Minute => write!(fmt, "Minute"),
            Self::Hour => write!(fmt, "Hour"),
            Self::Day => write!(fmt, "Day"),
            Self::Week => write!(fmt, "Week"),
        }
    }
}

/// A periodic job used by Scheduler.
pub struct Job {
    /// Pause `interval * unit` seconds between runs
    call_interval: i32,
    /// Datetime of next run.
    next_run: DateTime<Local>,
    /// Datetime of last run.
    last_run: DateTime<Local>,
    /// Time unit.
    time_unit: Option<TimeUnit>,
    /// Wheather run at specific time
    is_at: bool,
    /// Specific day of the week to start on.
    weekday: Option<Weekday>,
    /// Specific time of the day to start on.
    at_time: Option<Duration>,
    /// Wheather run at job immediately.
    is_immediately_run: bool,
    /// Job name or function name.
    job_name: String,
    /// A distributed locker,
    locker: bool,
    /// This function returns a `Duration` which is equal to `next_time` minus `last_time`.
    interval_fn: Option<fn(&Metric, &DateTime<Local>) -> Duration>,
    /// Numbers of thread to run a job parallely
    n_threads: u8,
}

impl Job {
    pub(crate) fn new(call_interval: i32, is_at: bool) -> Job {
        let now = Local::now();
        Job {
            call_interval,
            next_run: now,
            last_run: now,
            time_unit: None,
            weekday: None,
            is_immediately_run: false,
            at_time: None,
            is_at,
            job_name: "".into(),
            locker: false,
            interval_fn: None,
            n_threads: 1,
        }
    }

    #[inline(always)]
    pub(crate) fn get_job_name(&self) -> String {
        self.job_name.clone()
    }

    #[inline(always)]
    pub(crate) fn set_name(&mut self, name: String) {
        self.job_name = name;
    }

    #[inline(always)]
    pub(crate) fn need_locker(&mut self) {
        self.locker = true;
    }

    #[inline(always)]
    pub(crate) fn threads(&mut self, n: u8) {
        self.n_threads = n;
    }

    #[inline(always)]
    pub(crate) fn is_need_lock(&self) -> bool {
        self.locker
    }

    #[inline(always)]
    pub(crate) fn n_threads(&self) -> u8 {
        self.n_threads
    }

    #[inline(always)]
    pub(crate) fn set_unit(&mut self, unit: TimeUnit) {
        self.time_unit = Some(unit);
    }

    #[inline(always)]
    pub(crate) fn set_interval_fn(&mut self, f: fn(&Metric, &DateTime<Local>) -> Duration) {
        self.interval_fn = Some(f);
    }

    #[inline(always)]
    pub(crate) fn has_interval_fn(&self) -> bool {
        self.interval_fn.is_some()
    }

    #[inline(always)]
    pub(crate) fn get_unit(&self) -> Option<&TimeUnit> {
        self.time_unit.as_ref()
    }

    #[inline(always)]
    pub(crate) fn is_at(&self) -> bool {
        self.is_at
    }

    #[inline(always)]
    pub(crate) fn get_at_time(&self) -> Option<Duration> {
        self.at_time
    }

    #[inline(always)]
    pub(crate) fn immediately_run(&mut self) {
        self.is_immediately_run = true
    }

    #[inline(always)]
    pub(crate) fn get_immediately_run(&self) -> bool {
        self.is_immediately_run
    }

    #[inline(always)]
    pub(crate) fn runnable(&self) -> bool {
        self.next_run.le(&Local::now())
    }

    #[inline(always)]
    pub(crate) fn set_at_time(&mut self, h: i64, m: i64, s: i64) {
        self.at_time = Some(Duration::hours(h) + Duration::minutes(m) + Duration::seconds(s));
    }

    #[inline(always)]
    pub(crate) fn set_weekday(&mut self, w: i64) {
        self.weekday = match w {
            1 => Some(Weekday::Mon),
            2 => Some(Weekday::Tue),
            3 => Some(Weekday::Wed),
            4 => Some(Weekday::Thu),
            5 => Some(Weekday::Fri),
            6 => Some(Weekday::Sat),
            7 => Some(Weekday::Sun),
            _ => None,
        };
    }

    #[inline(always)]
    pub(crate) fn get_next_run(&self) -> DateTime<Local> {
        self.next_run
    }

    #[inline(always)]
    pub(crate) fn get_last_run(&self) -> DateTime<Local> {
        self.last_run
    }

    #[inline(always)]
    pub(crate) fn get_weekday(&self) -> Option<Weekday> {
        self.weekday
    }

    #[inline(always)]
    pub(crate) fn get_time_unit(&self) -> Option<i8> {
        self.time_unit
            .as_ref()
            .map_or(None, |t| Some(t.number_from_zero()))
    }

    /// Compute the time when the job should run next time.
    pub(crate) fn schedule_run_time(&mut self) {
        let granularity = self.cmp_time_granularity();
        let mut next_run = match self.time_unit {
            Some(TimeUnit::Second) | Some(TimeUnit::Minute) | Some(TimeUnit::Hour) => self.last_run,
            Some(TimeUnit::Day) => {
                let mut midnight = Local
                    .with_ymd_and_hms(
                        self.last_run.year(),
                        self.last_run.month(),
                        self.last_run.day(),
                        0,
                        0,
                        0,
                    )
                    .unwrap();
                midnight = midnight + self.at_time.unwrap_or(Duration::zero());
                midnight
            }
            Some(TimeUnit::Week) => {
                let mut midnight = Local
                    .with_ymd_and_hms(
                        self.last_run.year(),
                        self.last_run.month(),
                        self.last_run.day(),
                        0,
                        0,
                        0,
                    )
                    .unwrap();
                midnight = midnight + self.at_time.unwrap_or(Duration::zero());
                println!("name: {}, weekday: {:?}", self.job_name, self.weekday);
                let deviation: i32 = self.weekday.unwrap().number_from_sunday() as i32
                    - midnight.weekday().number_from_sunday() as i32;
                if deviation != 0 {
                    midnight = midnight + Duration::days(1) * deviation;
                }
                midnight
            }
            None => self.last_run + granularity, // todo: handle this condition
        };
        let now = Local::now();
        while (next_run.le(&now) || next_run.le(&self.last_run)) && self.interval_fn.is_none() {
            next_run = next_run + granularity;
        }
        if next_run.gt(&self.next_run) {
            self.last_run = self.next_run;
            self.next_run = next_run;
        }
        METRIC_STORAGE.get(&self.job_name).map_or_else(
            || unreachable!("unreachable"),
            |m| {
                m.n_scheduled.fetch_add(1, Ordering::SeqCst);
            },
        );
    }

    #[inline(always)]
    fn cmp_time_granularity(&self) -> Duration {
        self.time_unit.as_ref().map_or_else(
            || {
                let f = self.interval_fn.expect(
                    "Please set time unit or provide interval_fn in [cmp_time_granularity].",
                );
                f(&METRIC_STORAGE.get(&self.job_name).unwrap(), &self.last_run)
            },
            |tu| tu.granularity() * self.call_interval,
        )
    }
}

impl fmt::Debug for Job {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Job")
            .field("job_name", &self.job_name)
            .field("is_immediately_run", &self.is_immediately_run)
            .field("at_time", &self.at_time)
            .field("weekday", &self.weekday)
            .field("is_at", &self.is_at)
            .field("time_unit", &self.time_unit)
            .field("last_run", &self.last_run)
            .field("next_run", &self.next_run)
            .field("call_interval", &self.call_interval)
            .field("locker", &self.locker)
            .field("n_threads", &self.n_threads)
            .finish()
    }
}
