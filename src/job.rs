use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Weekday};
use std::fmt;

/// Time unit.
pub(crate) enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl TimeUnit {
    fn granularity(&self) -> Duration {
        match self {
            Self::Second => Duration::seconds(1),
            Self::Minute => Duration::minutes(1),
            Self::Hour => Duration::hours(1),
            Self::Day => Duration::days(1),
            Self::Week => Duration::weeks(1),
        }
    }
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
    // pause interval * unit between runs
    call_interval: i32,
    // datetime of next run.
    next_run: DateTime<Local>,
    // datetime of last run.
    last_run: DateTime<Local>,
    // time unit.
    time_unit: Option<TimeUnit>,
    // wheather run at specific time
    is_at: bool,
    // specific day of the week to start on.
    weekday: Option<Weekday>,
    // specific time of the day to start on.
    at_time: Option<Duration>,
    // wheather run at job immediately.
    is_immediately_run: bool,
    // job name or function name.
    job_name: String,
    // job_core: Arc<RwLock<JobArcHandler<L>>>,
    locker: bool,
}

impl Job {
    pub(crate) fn new(call_interval: i32, is_at: bool) -> Job {
        let now = Local::now();
        Job {
            // job_core: Arc::new(RwLock::new(JobArcHandler::new())),
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
        }
    }

    pub(crate) fn get_job_name(&self) -> String {
        self.job_name.clone()
    }

    pub(crate) fn set_name(&mut self, name: String) {
        self.job_name = name;
    }

    pub(crate) fn need_locker(&mut self) {
        self.locker = true;
    }

    pub(crate) fn has_locker(&self) -> bool {
        self.locker
    }
    #[inline]
    pub(crate) fn set_unit(&mut self, unit: TimeUnit) {
        self.time_unit = Some(unit);
    }

    #[inline]
    pub(crate) fn get_unit(&self) -> Option<&TimeUnit> {
        self.time_unit.as_ref()
    }

    #[inline]
    pub(crate) fn get_is_at(&self) -> bool {
        self.is_at
    }

    pub(crate) fn get_at_time(&self) -> Option<Duration> {
        self.at_time
    }

    #[inline]
    pub(crate) fn immediately_run(&mut self) {
        self.is_immediately_run = true
    }

    #[inline]
    pub(crate) fn get_immediately_run(&self) -> bool {
        self.is_immediately_run
    }

    #[inline]
    pub(crate) fn runable(&self) -> bool {
        self.next_run.le(&Local::now())
    }

    pub(crate) fn set_at_time(&mut self, h: i64, m: i64, s: i64) {
        self.at_time = Some(Duration::hours(h) + Duration::minutes(m) + Duration::seconds(s));
    }

    #[inline]
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

    pub(crate) fn get_next_run(&self) -> DateTime<Local> {
        self.next_run
    }

    #[inline]
    pub(crate) fn get_last_run(&self) -> DateTime<Local> {
        self.last_run
    }

    #[inline]
    pub(crate) fn get_weekday(&self) -> Option<Weekday> {
        self.weekday
    }

    pub(crate) fn get_time_unit(&self) -> Option<i8> {
        self.time_unit
            .as_ref()
            .map_or(None, |t| Some(t.number_from_zero()))
    }

    /// Compute the time when the job should run next time.
    pub(crate) fn schedule_run_time(&mut self) {
        let now = Local::now();
        self.last_run = now;
        let granularity = self.cmp_time_granularity();
        self.next_run = match self.time_unit {
            Some(TimeUnit::Second) | Some(TimeUnit::Minute) | Some(TimeUnit::Hour) => self.last_run,
            Some(TimeUnit::Day) => {
                let mut midnight = Local
                    .ymd(
                        self.last_run.year(),
                        self.last_run.month(),
                        self.last_run.day(),
                    )
                    .and_hms(0, 0, 0);
                midnight = midnight + self.at_time.unwrap_or(Duration::zero());
                midnight
            }
            Some(TimeUnit::Week) => {
                let mut midnight = Local
                    .ymd(
                        self.last_run.year(),
                        self.last_run.month(),
                        self.last_run.day(),
                    )
                    .and_hms(0, 0, 0);
                midnight = midnight + self.at_time.unwrap_or(Duration::zero());
                let deviation: i32 = self.weekday.unwrap().number_from_sunday() as i32
                    - midnight.weekday().number_from_sunday() as i32;
                if deviation != 0 {
                    midnight = midnight + Duration::days(1) * deviation;
                }
                midnight
            }
            None => self.last_run, // todo: handle this condition
        };

        while self.next_run.le(&now) || self.next_run.le(&self.last_run) {
            self.next_run = self.next_run + granularity;
        }
    }

    fn cmp_time_granularity(&self) -> Duration {
        self.time_unit
            .as_ref()
            .and_then(|tu| Some(tu.granularity() * self.call_interval))
            .unwrap()
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
            .finish()
    }
}
