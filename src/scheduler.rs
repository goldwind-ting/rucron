use crate::job::{Job, TimeUnit};
use crate::locker::Locker;

use std::{any::type_name, fmt, sync::Arc};
use tokio::sync::{mpsc::channel, RwLock};
use tokio::time::{self, sleep, Duration, Interval};
extern crate lazy_static;
use futures::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(windows))]
use signal_hook::consts::SIGTSTP; // catch ctrl + z signal
#[cfg(windows)]
use signal_hook::consts::SIGINT;


lazy_static! {
    static ref DEFAULT_ZERO_CALL_INTERVAL: Duration = Duration::from_secs(1);
}

/// A `Scheduler` type to represent a span of time, typically used for system
/// timeouts.
pub struct Scheduler<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    scan_interval: u64,
    size: usize,
    jobs: Arc<RwLock<Vec<Job<R, L>>>>,
}

impl<R, L> Scheduler<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    /// Creates a new instance of an `Scheduler<R, L>`. `interval` representent how much times to checkout runnable jobs.
    /// `capacity` is the capicity of jobs. Please note `interval` is 1 by default.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// let mut sch = Scheduler::<(), ()>::new(2, 10);
    /// ```
    pub fn new(interval: u64, capacity: usize) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(Vec::with_capacity(capacity))),
            scan_interval: interval,
            size: 0,
        }
    }

    /// Returns the number of jobs in the Scheduler.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(2).await.second().await.todo(bar).await;
    ///     assert_eq!(sch.len(), 2);
    /// }
    /// ```
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns `true` if the function had been added to job list `jobs`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     assert!(sch.is_scheduled(foo).await);
    /// }
    /// ```
    pub async fn is_scheduled<F>(&self, _job: fn() -> F) -> bool
    where
        F: Future<Output = R> + Send + 'static,
    {
        let job_name_tokens: Vec<&str> = type_name::<F>().split("::").collect();
        let guard = self.jobs.read().await;
        for i in 0..guard.len() {
            if guard[i].get_job_name() == job_name_tokens[job_name_tokens.len() - 2] {
                return true;
            }
        }
        return false;
    }

    /// Return all names of job which is added in `jobs`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(2).await.second().await.todo(bar).await;
    ///     assert_eq!(sch.get_job_names().await, vec!["foo", "bar"]);
    /// }
    /// ```
    pub async fn get_job_names(&self) -> Vec<String> {
        let mut job_names = Vec::new();
        let guard = self.jobs.read().await;
        guard.iter().for_each(|job| {
            job_names.push(job.get_job_name());
        });
        job_names
    }

    /// Return the timestamp of next upcoming job, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(6).await.second().await.todo(bar).await;
    ///     assert!(sch.idle_seconds().await.is_some());
    ///     let idle = sch.idle_seconds().await.unwrap();
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(idle - now, 2);
    /// }
    /// ```
    pub async fn idle_seconds(&self) -> Option<i64> {
        let guard = self.jobs.read().await;
        guard
            .iter()
            .min_by(|x, y| x.get_next_run().cmp(&y.get_next_run()))
            .and_then(|job| Some(job.get_next_run().timestamp()))
    }

    async fn insert_job(&mut self, job: Job<R, L>) {
        self.jobs.write().await.push(job);
        self.size += 1;
    }

    /// Schedule a new periodic job.`interval` is a certain time unit, e.g. second, minute, hour, day or week.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use  chrono::{Local, Datelike, Timelike};
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let now = Local::now().timestamp();
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(10).await.second().await.todo(foo).await;
    ///     assert_eq!(Some(now+10), sch.next_run_with_name("foo").await);
    /// }
    ///
    /// ```
    pub async fn every(&mut self, interval: i32) -> &Self {
        let job = Job::new(interval, false);
        self.insert_job(job).await;
        self
    }

    /// schedules job at specific time of every day or week.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use  chrono::{Local, Datelike, Timelike};
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let now = Local::now();
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.at().await.week(now.weekday().number_from_monday() as i64, now.hour() as i64,now.minute() as i64, now.second() as i64,)
    ///     .await.todo(foo).await;
    ///     let real_time = sch.next_run_with_name("foo").await.unwrap();
    ///     let expect_next_run = now + chrono::Duration::weeks(1);
    ///     assert_eq!(real_time, expect_next_run.timestamp());
    /// }
    ///
    /// ```
    pub async fn at(&mut self) -> &Self {
        let job = Job::new(1, true);
        self.insert_job(job).await;
        self
    }

    /// Set optional config for `Scheduler`.`is_immediately_run` represents wheather to run the job immediately.
    /// `err_callback` set the error handeler which is called when exception occurs, if it is `None`, `Scheduler` print error by default.
    /// `locker` set the distributed lock.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, Locker};
    ///
    /// struct SchLocker;
    ///
    /// impl Locker for SchLocker{
    ///     type Error=std::io::Error;
    /// }
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// fn err_callback_mock(err: std::io::Error){
    ///     println!("error: {}", err);
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), SchLocker>::new(2, 10);
    ///     sch.every(2).await.second().await.with_opts(false, Some(Box::new(err_callback_mock)), Some(SchLocker)).await.todo(foo).await;
    /// }
    /// ```
    pub async fn with_opts<E: Fn(<L as Locker>::Error) + Send + Sync + 'static>(
        &self,
        is_immediately_run: bool,
        err_callback: Option<Box<E>>,
        locker: Option<L>,
    ) -> &Self {
        let mut guard = self.jobs.write().await;
        guard
            .get_mut(self.size - 1)
            .map_or_else(
                || {
                    unreachable!("cann't get the job, job index: {}", self.size - 1);
                },
                |job| async move {
                    if is_immediately_run {
                        job.immediately_run();
                    };
                    if let Some(call_back) = err_callback {
                        job.set_err_callback(call_back).await;
                    };
                    if let Some(l) = locker {
                        job.set_locker(l).await;
                    }
                },
            )
            .await;
        self
    }

    /// Set the time unit to hour.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.hour().await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(2));
    /// }
    /// ```
    pub async fn hour(&self) -> &Self {
        let mut guard = self.jobs.write().await;
        guard.get_mut(self.size - 1).map_or_else(
            || {
                unreachable!("cann't get the job, job index: {}", self.size - 1);
            },
            |job| {
                if job.get_is_at() {
                    panic!(
                        "At is only allowed daily or weekly job, job index: {}",
                        self.size - 1
                    );
                }
                job.set_unit(TimeUnit::Hour);
            },
        );
        self
    }

    /// Set the time unit to minute.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.minute().await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(1));
    /// }
    /// ```
    pub async fn minute(&self) -> &Self {
        let mut guard = self.jobs.write().await;
        guard.get_mut(self.size - 1).map_or_else(
            || {
                unreachable!("cann't get the job, index: {}", self.size - 1);
            },
            |job| {
                if job.get_is_at() {
                    panic!(
                        "At is only allowed daily or weekly job, job index: {}",
                        self.size - 1
                    );
                }
                job.set_unit(TimeUnit::Minute);
            },
        );
        self
    }

    /// Set the time unit to second.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(0));
    /// }
    /// ```
    pub async fn second(&self) -> &Self {
        let mut guard = self.jobs.write().await;
        guard.get_mut(self.size - 1).map_or_else(
            || {
                unreachable!("cann't get the job, job index: {}", self.size - 1);
            },
            |job| {
                if job.get_is_at() {
                    panic!(
                        "At is only allowed daily or weekly job, job index: {}",
                        self.size - 1
                    );
                }
                job.set_unit(TimeUnit::Second);
            },
        );
        self
    }

    /// Set the time unit to day.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.day(0, 59, 59).await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(3));
    /// }
    /// ```
    pub async fn day(&self, h: i64, m: i64, s: i64) -> &Self {
        let mut guard = self.jobs.write().await;
        guard.get_mut(self.size - 1).map_or_else(
            || {
                unreachable!("cann't get the job, job index: {}", self.size - 1);
            },
            |job| {
                job.set_unit(TimeUnit::Day);
                job.set_at_time(h, m, s);
            },
        );
        self
    }

    /// Set the time unit to week.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.week(1, 0, 59, 59).await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(4));
    /// }
    /// ```
    pub async fn week(&self, w: i64, h: i64, m: i64, s: i64) -> &Self {
        let mut guard = self.jobs.write().await;
        guard.get_mut(self.size - 1).map_or_else(
            || {
                unreachable!("cann't get the job, job index: {}", self.size - 1);
            },
            |job| {
                job.set_unit(TimeUnit::Week);
                job.set_at_time(h, m, s);
                job.set_weekday(w);
            },
        );
        self
    }

    /// Config the job.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(6).await.second().await.todo(bar).await;
    /// }
    /// ```
    pub async fn todo<F>(&self, f: fn() -> F)
    where
        F: Future<Output = R> + Send + 'static,
    {
        let mut guard = self.jobs.write().await;
        guard
            .get_mut(self.size - 1)
            .map_or_else(
                || {
                    unreachable!("cann't get the job, job index: {}", self.size - 1);
                },
                |job| async move {
                    job.set_job(f, false).await;
                },
            )
            .await;
    }

    /// Config the job, and execute `unlock` before running.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, Locker};
    ///
    /// struct SchLocker;
    ///
    /// impl Locker for SchLocker{
    ///     type Error=std::io::Error;
    /// }
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), SchLocker>::new(2, 10);
    ///     sch.every(2).await.second().await.todo_with_unlock(foo).await;
    /// }
    /// ```
    /// ```
    pub async fn todo_with_unlock<F>(&self, f: fn() -> F)
    where
        F: Future<Output = R> + Send + 'static,
    {
        let mut guard = self.jobs.write().await;
        guard
            .get_mut(self.size - 1)
            .map_or_else(
                || {
                    unreachable!("cann't get the job, job index: {}", self.size - 1);
                },
                |job| async move {
                    job.set_job(f, true).await;
                },
            )
            .await;
    }

    /// Return the name of next upcoming job, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(6).await.second().await.todo(bar).await;
    ///     assert!(sch.next_run().await.is_some());
    ///     assert_eq!(sch.next_run().await, Some("foo".to_string()));
    /// }
    /// ```
    pub async fn next_run(&self) -> Option<String> {
        let guard = self.jobs.read().await;
        guard
            .iter()
            .min_by(|a, b| a.get_next_run().cmp(&b.get_next_run()))
            .map_or(None, |job| Some(job.get_job_name()))
    }

    /// Returns a day-of-week number of job starting from Monday = 1 by job name.
    ///
    /// `w`:                      | `Mon` | `Tue` | `Wed` | `Thu` | `Fri` | `Sat` | `Sun`
    /// ------------------------- | ----- | ----- | ----- | ----- | ----- | ----- | -----
    /// `w.weekday_with_name()`:  | 1     | 2     | 3     | 4     | 5     | 6     | 7
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.week(1, 0, 59, 59).await.todo(foo).await;
    ///     assert_eq!(sch.weekday_with_name("foo").await, Some(1));
    /// }
    /// ```
    pub async fn weekday_with_name(&self, job_name: &str) -> Option<u32> {
        let guard = self.jobs.read().await;
        for job in guard.iter() {
            if job.get_job_name().as_str() == job_name {
                return job.get_weekday().and_then(|w| Some(w.number_from_monday()));
            }
        }
        None
    }

    /// Return the run time unit of job by job name, return `None` if job does not exist.
    ///
    /// `unit`:                    | `Second` | `Minute` |  `Hour`  |  `Day`   |  `Week`
    /// -------------------------  | -------- | -------- | -------- | -------- | --------
    /// `w.time_unit_with_name()`: |  Some(0) |  Some(0) |  Some(0) |  Some(0) |  Some(0)   
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     assert_eq!(sch.time_unit_with_name("foo").await, Some(0));
    /// }
    /// ```
    pub async fn time_unit_with_name(&self, job_name: &str) -> Option<i8> {
        let guard = self.jobs.read().await;
        for job in guard.iter() {
            if job.get_job_name().as_str() == job_name {
                return job.get_time_unit();
            }
        }
        None
    }

    /// Return next run time of job by job name, return None if the job does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(sch.next_run_with_name("foo").await.unwrap(), now + 2);
    /// }
    /// ```
    pub async fn next_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self.jobs.read().await;
        for job in guard.iter() {
            if job.get_job_name().as_str() == job_name {
                return Some(job.get_next_run().timestamp());
            }
        }
        None
    }

    /// Return last run time of job by job name, return None if the job does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(sch.last_run_with_name("foo").await.unwrap(), now);
    /// }
    /// ```
    pub async fn last_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self.jobs.read().await;
        for job in guard.iter() {
            if job.get_job_name().as_str() == job_name {
                return Some(job.get_last_run().timestamp());
            }
        }
        None
    }

    /// Delete a scheduled job.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(6).await.second().await.todo(bar).await;
    ///     assert_eq!(sch.get_job_names().await, vec!["foo", "bar"]);
    ///     sch.cancel_job("foo".into()).await;
    ///     assert_eq!(sch.get_job_names().await, vec!["bar"]);
    /// }
    /// ```
    pub async fn cancel_job(&mut self, job_name: String) {
        let mut guard = self.jobs.write().await;
        match guard.iter().position(|j| j.get_job_name() == job_name) {
            Some(i) => {
                guard.remove(i);
                self.size -= 1;
            }
            None => {}
        };
    }

    async fn check_job_name(&self) {
        let guard = self.jobs.read().await;
        guard.iter().enumerate().for_each(|(ind, job)| {
            if job.get_job_name().len() <= 0 {
                unreachable!("please set job, job index: {}", ind);
            }
        });
    }

    fn gen_call_interval(&self, interval: u64) -> Interval {
        if interval <= 0 {
            time::interval(*DEFAULT_ZERO_CALL_INTERVAL)
        } else {
            time::interval(time::Duration::from_secs(interval))
        }
    }

    /// Start scheduler and run all jobs.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
    /// }
    ///
    /// async fn bar(){
    ///     println!("bar");
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let mut sch = Scheduler::<(), ()>::new(2, 10);
    ///     sch.every(2).await.second().await.todo(foo).await;
    ///     sch.every(6).await.second().await.todo(bar).await;
    ///     assert!(sch.idle_seconds().await.is_some());
    ///     sch.start();
    /// }
    /// ```
    pub async fn start(&self) {
        self.check_job_name().await;
        let (send, mut recv) = channel(1);
        let term = Arc::new(AtomicBool::new(false));
        #[cfg(not(windows))]
        let _ = signal_hook::flag::register(SIGTSTP, Arc::clone(&term)).map_err(|e| {
            unreachable!("cann't register signal: {}", e);
        });
        #[cfg(windows)]
        let _ = signal_hook::flag::register(SIGINT, Arc::clone(&term)).map_err(|e| {
            unreachable!("cann't register signal: {}", e);
        });
        let send_trigger = tokio::spawn(async move {
            while !term.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1)).await;
            }
            let _ = send.send(()).await.map_err(|e| {
                unreachable!("cann't send signal to channel, {}", e);
            });
        });
        let jobs = self.jobs.clone();
        let inter = self.scan_interval;
        let mut interval: Interval = self.gen_call_interval(inter);
        let recv_trigger = tokio::spawn(async move {
            loop {
                let jobs_loop = jobs.clone();
                tokio::select! {
                _ = interval.tick() => {
                    tokio::spawn(async move{
                        let mut size:i64 = -1;
                        {
                            let mut guard = jobs_loop.write().await;
                            guard.sort_by(|a, b|{
                                a.get_next_run().cmp(&b.get_next_run())
                            });
                        }
                        {
                            let guard = jobs_loop.read().await;
                            for (ind, job) in guard.iter().enumerate(){
                                if job.runable(){
                                    size = (ind + 1) as i64;
                                }else{
                                    break;
                                }
                            }
                        }
                        if size < 0 {
                            return;
                        }
                        {
                            for i in (0..size as usize).into_iter(){
                                {
                                    let mut guard = jobs_loop.write().await;
                                    guard[i].schedule_run_time();
                                }

                                {
                                    let guard = jobs_loop.read().await;
                                    guard[i].run().await;
                                }
                            };
                        }
                    });
                },
                Some(_) = recv.recv() => {
                        println!("shutting down!!!");
                        break;
                },
                }
            }
        });

        let _ = tokio::join!(send_trigger, recv_trigger);
        return;
    }
}

impl<R, L> fmt::Debug for Scheduler<R, L>
where
    R: 'static + Send + Sync,
    L: 'static + Send + Sync + Locker,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let guard = self.jobs.try_read().unwrap();
        fmt.debug_struct("Scheduler")
            .field("scan_interval", &self.scan_interval)
            .field("size", &self.size)
            .field("jobs", &guard)
            .finish()
    }
}
