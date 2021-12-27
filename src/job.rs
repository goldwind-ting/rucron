use chrono::{DateTime, Datelike, Duration, Local, TimeZone, Weekday};
use futures::future::{BoxFuture, Future};
use std::{any::type_name, fmt, sync::Arc, panic};
use tokio::sync::RwLock;

use crate::locker::Locker;

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

struct JobArcHandler<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    // scheduled function or job.
    job: Option<Box<dyn Fn() -> BoxFuture<'static, R> + Send + Sync + 'static>>,
    // distributed lock.
    locker: Option<L>,
    // error handeler used by Locker.
    err_callback: Box<dyn Fn(<L as Locker>::Error) + Send + Sync + 'static>,
}

impl<R, L> JobArcHandler<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    fn new() -> Self {
        Self {
            job: None,
            locker: None,
            err_callback: Box::new(|e| {
                println!("{:?}", e);
            }),
        }
    }
}

/// A periodic job used by Scheduler.
pub struct Job<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
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
    job_core: Arc<RwLock<JobArcHandler<R, L>>>,
}

impl<R, L> Job<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    pub(crate) fn new(call_interval: i32, is_at: bool) -> Job<R, L> {
        let now = Local::now();
        Job {
            job_core: Arc::new(RwLock::new(JobArcHandler::new())),
            call_interval,
            next_run: now,
            last_run: now,
            time_unit: None,
            weekday: None,
            is_immediately_run: false,
            at_time: None,
            is_at,
            job_name: "".into(),
        }
    }

    pub(crate) fn get_job_name(&self) -> String {
        self.job_name.clone()
    }

    pub(crate) async fn set_job<F>(&mut self, f: fn() -> F, unlock: bool)
    where
        F: Future<Output = R> + Send + 'static,
    {
        if let None = self.time_unit {
            panic!("must set time unit!");
        }
        if self.is_at && self.at_time.is_none() && self.weekday.is_none() {
            panic!("please set run time of job: day or weekday!");
        }
        {
            let mut job_guard = self.job_core.write().await;
            if unlock {
                (*job_guard).locker.as_ref().and_then(|l| {
                    if let Err(e) = l.unlock(&self.job_name[..]) {
                        panic!("FAILED TO UNLOCK, {}", e);
                    };
                    Some(1)
                });
            }
            (*job_guard).job = Some(Box::new(move || Box::pin(f())));
        }
        let job_name = type_name::<F>();
        let tokens: Vec<&str> = job_name.split("::").collect();
        match (*tokens).get(tokens.len() - 2) {
            None => panic!("INVALID JOB NAME: {:?}", tokens),
            Some(s) => {
                self.job_name = (*s).into();
            }
        };
        if self.is_immediately_run {
            self.run().await;
        }
        self.schedule_run_time();
    }

    pub(crate) fn set_unit(&mut self, unit: TimeUnit) {
        self.time_unit = Some(unit);
    }

    pub(crate) fn get_is_at(&self) -> bool {
        self.is_at
    }

    #[inline]
    pub(crate) fn immediately_run(&mut self) {
        self.is_immediately_run = true
    }

    pub(crate) fn runable(&self) -> bool {
        self.next_run.le(&Local::now())
    }

    pub(crate) fn set_at_time(&mut self, h: i64, m: i64, s: i64) {
        self.at_time = Some(Duration::hours(h) + Duration::minutes(m) + Duration::seconds(s));
    }

    pub(crate) async fn set_locker(&mut self, locker: L) {
        let mut job_guard = self.job_core.write().await;
        (*job_guard).locker = Some(locker);
    }

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

    pub(crate) async fn set_err_callback(
        &mut self,
        f: Box<dyn Fn(<L as Locker>::Error) + Send + Sync + 'static>,
    ) {
        let mut job_guard = self.job_core.write().await;
        (*job_guard).err_callback = f;
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

    /// Run job without locking and unlocking.
    async fn run_without_locker(&self) {
        let job = self.job_core.clone();
        tokio::spawn(async move {
            let job_guard = job.read().await;
            if let Some(f) = job_guard.job.as_ref() {
                f().await;
            }
        });
    }

    /// Run job with locking and unlocking.
    async fn run_with_locker(&self, key: String) {
        let job = self.job_core.clone();

        tokio::spawn(async move {
            let job_guard = job.read().await;
            let f = job_guard.job.as_ref().unwrap();
            let locker = job_guard.locker.as_ref().unwrap();
            let callback = &job_guard.err_callback;
            f().await;
            if let Err(e) = locker.unlock(&key[..]) {
                callback(e);
            };
        });
    }

    pub(crate) async fn run(&self) {
        let job = self.job_core.read().await;
        let locker = job.locker.as_ref();
        match locker {
            Some(locker) => match locker.lock(&self.job_name[..]) {
                Err(e) => {
                    let callback = &job.err_callback;
                    callback(e);
                }
                Ok(flag) if flag => {
                    self.run_with_locker(self.job_name.clone()).await;
                }
                _ => {
                    eprintln!("CAN'T UNLOCK!");
                    return;
                }
            },
            None => {
                self.run_without_locker().await;
            }
        }
    }
}

impl<R, L> fmt::Debug for Job<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
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
