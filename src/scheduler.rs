use crate::async_rt::{channel, sleep, spawn, wait, Interval, RwLock};
use crate::handler::{ArgStorage, JobHandler, Task};
use crate::job::{Job, TimeUnit};
use crate::{
    locker::Locker, metric::Metric, unlock_and_record, DEFAULT_ZERO_CALL_INTERVAL, METRIC_STORAGE,
};

use chrono::{DateTime, Duration as duration, Local};
use std::{
    fmt,
    sync::Arc,
    time::{self, Duration},
};

extern crate lazy_static;
use async_trait::async_trait;
use select_macro::{count, select, select_variant};

#[cfg(windows)]
use signal_hook::consts::{SIGINT, SIGTERM};

#[cfg(not(windows))]
use signal_hook::consts::TERM_SIGNALS;
use std::sync::atomic::{AtomicBool, Ordering};

/// `Scheduler` strores all jobs and number of jobs.
pub struct Scheduler<T, L>
where
    T: 'static + Send,
    L: 'static + Send,
{
    scan_interval: u64,
    size: usize,
    task: T,
    locker: Option<L>,
    arg_storage: Option<Arc<ArgStorage>>,
    jobs: Arc<RwLock<Vec<Job>>>,
}

#[async_trait]
impl<T, L> JobHandler for Scheduler<T, L>
where
    T: JobHandler + 'static + Send + Sync,
    L: Locker + 'static + Send + Sync,
{
    async fn call(self, args: Arc<ArgStorage>, name: String) {
        JobHandler::call(self.task, args, name).await;
    }

    fn name(&self) -> String {
        self.task.name()
    }
}

/// Placeholder of `Task` for `Scheduler` initialization.
pub struct EmptyTask;

impl EmptyTask {
    fn fake_task() -> Self {
        Self
    }
}

impl Clone for EmptyTask {
    fn clone(&self) -> Self {
        Self
    }
}

#[async_trait]
impl JobHandler for EmptyTask {
    async fn call(self, _args: Arc<ArgStorage>, _name: String) {}
    fn name(&self) -> String {
        "EmptyTask".into()
    }
}

impl<L: Send> Scheduler<EmptyTask, L> {
    /// Creates a new instance of `Scheduler`.
    /// - `interval` set the interval how often `jobs` should be checked, default to 1.
    /// - `capacity` is the capicity of jobs. Please note `interval` is 1 by default.
    ///
    pub fn new(interval: u64, size: usize) -> Self {
        Self {
            scan_interval: interval,
            size: 0,
            task: EmptyTask::fake_task(),
            arg_storage: Some(Arc::new(ArgStorage::new())),
            jobs: Arc::new(RwLock::new(Vec::with_capacity(size))),
            locker: None,
        }
    }
}

impl<TH, L> Scheduler<TH, L>
where
    TH: JobHandler + Send + Sync + 'static + Clone,
    L: Locker + 'static + Send + Sync + Clone,
{
    fn map<F, T2: Send + Sync + Clone>(self, f: F) -> Scheduler<T2, L>
    where
        F: FnOnce(TH, Option<L>) -> T2,
    {
        Scheduler {
            task: f(self.task, self.locker.clone()).clone(),
            arg_storage: self.arg_storage,
            jobs: self.jobs,
            locker: self.locker,
            scan_interval: self.scan_interval,
            size: self.size,
        }
    }

    /// Returns the number of jobs in the `Scheduler`.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns `true` if the job which name is `name` had been added to the `Scheduler`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await;
    ///     assert!(sch.is_scheduled("foo"));
    /// }
    /// ```
    pub fn is_scheduled(&self, name: &str) -> bool {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [is_scheduled]");
        for i in 0..guard.len() {
            if guard[i].get_job_name() == name {
                return true;
            }
        }
        false
    }

    /// Set a job with `ArgStorage` which stores all arguments jobs need.
    #[inline(always)]
    pub fn set_arg_storage(&mut self, storage: ArgStorage) {
        self.arg_storage.replace(Arc::new(storage));
    }

    /// Set a distributed locker for `Scheduler`.If a job should not be run parallely, you could set a locker for the job.
    #[inline(always)]
    pub fn set_locker(&mut self, locker: L) {
        self.locker = Some(locker);
    }

    /// Set a distributed locker for job which should not be run parallely.
    pub fn need_lock(self) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [need_lock]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!(
                        "Cann't get the job in [need_lock], index: {}",
                        self.size - 1
                    )
                },
                |job| job.need_locker(),
            );
        }
        self
    }
    /// If set a job with `n_threads`, the `Scheduler` will spawn `n` threads to run the job in runtime every time.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().n_threads(3).todo(execute(foo)).await;
    ///     assert!(sch.is_scheduled("foo"));
    /// }
    /// ```
    pub fn n_threads(self, n: u8) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [n_threads]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!(
                        "Cann't get the job in [n_threads], index: {}",
                        self.size - 1
                    )
                },
                |job| job.threads(n),
            );
        }
        self
    }

    /// Return all names of job which is added in `Scheduler`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo"); Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(2).second().todo(execute(bar)).await;
    ///     assert_eq!(sch.get_job_names(), vec!["foo", "bar"]);
    /// }
    /// ```
    pub fn get_job_names(&self) -> Vec<String> {
        let mut job_names = Vec::new();
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get write lock in [get_job_names]");
        guard.iter().for_each(|job| {
            job_names.push(job.get_job_name());
        });
        job_names
    }

    /// Return Number of seconds until next upcoming job starts running, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert!(sch.idle_seconds().is_some());
    ///     let idle = sch.idle_seconds().unwrap();
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(idle - now, 2);
    /// }
    /// ```
    pub fn idle_seconds(&self) -> Option<i64> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get write lock in [idle_seconds]");
        guard
            .iter()
            .min_by(|x, y| x.get_next_run().cmp(&y.get_next_run()))
            .and_then(|job| Some(job.get_next_run().timestamp()))
    }

    /// Add a new job to `Scheduler`.
    fn insert_job(&mut self, job: Job) {
        self.jobs.try_write().unwrap().push(job);
        self.size += 1;
    }

    /// Schedule a new periodic job.  
    /// - `interval` is a certain integer, and it's unit is second, minute, hour, day or week.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use  chrono::{Local, Datelike, Timelike};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let now = Local::now().timestamp();
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(10).second().todo(execute(foo)).await;
    ///     assert_eq!(Some(now+10), sch.next_run_with_name("foo"));
    /// }
    ///
    /// ```
    pub fn every(mut self, interval: i32) -> Self {
        let job = Job::new(interval, false);
        self.insert_job(job);
        self
    }

    /// schedules job at specific time of every day or week.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use  chrono::{Local, Datelike, Timelike};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let now = Local::now();
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.at().week(now.weekday().number_from_monday() as i64, now.hour() as i64,now.minute() as i64, now.second() as i64,)
    ///     .todo(execute(foo)).await;
    ///     let real_time = sch.next_run_with_name("foo").unwrap();
    ///     let expect_next_run = now + chrono::Duration::weeks(1);
    ///     assert_eq!(real_time, expect_next_run.timestamp());
    /// }
    ///
    /// ```
    pub fn at(mut self) -> Self {
        let job = Job::new(1, true);
        self.insert_job(job);
        self
    }

    /// According to the Duration generated by `f` to update next run time of the job.
    /// - `f` receives Metric and last run time, the caller could provide arbitrarily Duration base on the parameters.
    /// Time unit of the job is `None` in this circumstances.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute, Metric};
    /// use  chrono::{Local, DateTime, Duration};
    /// use std::{error::Error, sync::atomic::Ordering};
    ///
    ///
    /// fn once(m: &Metric, last: &DateTime<Local>) -> Duration {
    ///     let n = m.n_scheduled.load(Ordering::Relaxed);
    ///     if n < 1 {
    ///         Duration::seconds(2)
    ///     } else if n == 1 {
    ///         Duration::seconds(last.timestamp() * 2)
    ///     } else {
    ///         Duration::seconds(0)
    ///     }
    /// }
    ///
    /// async fn counter() -> Result<(), Box<dyn Error>> {
    ///     println!("counter");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    ///     let now = Local::now();
    ///     let sch = sch.by(once).todo(execute(counter)).await;
    ///     assert_eq!(sch.time_unit_with_name("counter"), None);
    ///     assert_eq!(sch.weekday_with_name("counter"), None);
    ///     let next = now + Duration::seconds(2);
    ///     assert_eq!(sch.last_run_with_name("counter").unwrap(), now.timestamp());
    ///     assert_eq!(sch.next_run_with_name("counter").unwrap(), next.timestamp());
    /// }
    ///
    /// ```
    ///
    pub fn by(mut self, f: fn(m: &Metric, last: &DateTime<Local>) -> duration) -> Self {
        let mut job = Job::new(0, false);
        job.set_interval_fn(f);
        self.insert_job(job);
        self
    }

    /// Run the job when call `todo` instead of waiting until  the `Scheduler` starts.  
    pub fn immediately_run(self) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't get write lock in [immediately_run]");
            guard
                .get_mut(self.size - 1)
                .and_then(|job| {
                    job.immediately_run();
                    Some(())
                })
                .expect("Please create task firstly [immediately_run]");
        }
        self
    }

    /// Set the time unit to hour.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1 or `is_at` inside the job is true.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).hour().todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(2));
    /// }
    /// ```
    pub fn hour(self) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [hour]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("Cann't get the job in [hour], job index: {}", self.size - 1);
                },
                |job| {
                    if job.is_at() {
                        panic!(
                            "At is only allowed daily or weekly job, job index: {}",
                            self.size - 1
                        );
                    }
                    job.set_unit(TimeUnit::Hour);
                },
            );
        }
        self
    }

    /// Set the time unit to minute.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1 or `is_at` inside the job is true.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).minute().todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(1));
    /// }
    /// ```
    pub fn minute(self) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [minute]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("Cann't get the job in [minute], index: {}", self.size - 1);
                },
                |job| {
                    if job.is_at() {
                        panic!(
                            "At is only allowed daily or weekly job, job index: {}",
                            self.size - 1
                        );
                    }
                    job.set_unit(TimeUnit::Minute);
                },
            );
        }
        self
    }

    /// Set the time unit to second.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1 or `is_at` inside the job is true.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(0));
    /// }
    /// ```
    pub fn second(self) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [second]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!(
                        "Cann't get the job in [second], job index: {}",
                        self.size - 1
                    );
                },
                |job| {
                    if job.is_at() {
                        panic!(
                            "At is only allowed daily or weekly job, job index: {}",
                            self.size - 1
                        );
                    }
                    job.set_unit(TimeUnit::Second);
                },
            );
        }
        self
    }

    /// Set the time unit to day.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).day(0, 59, 59).todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(3));
    /// }
    /// ```
    pub fn day(self, h: i64, m: i64, s: i64) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [day]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("Cann't get the job in [day], job index: {}", self.size - 1);
                },
                |job| {
                    job.set_unit(TimeUnit::Day);
                    job.set_at_time(h, m, s);
                },
            );
        }
        self
    }

    /// Set the time unit to week.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).week(1, 0, 59, 59).todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(4));
    /// }
    /// ```
    pub fn week(self, w: i64, h: i64, m: i64, s: i64) -> Self {
        {
            let mut guard = self
                .jobs
                .try_write()
                .expect("Cann't acquire write lock in [week]");
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("Cann't get the job in [week], job index: {}", self.size - 1);
                },
                |job| {
                    job.set_unit(TimeUnit::Week);
                    job.set_at_time(h, m, s);
                    job.set_weekday(w);
                },
            );
        }
        self
    }

    fn schedule_and_set_name(&self, name: String) {
        let mut guard = self
            .jobs
            .try_write()
            .expect("Cann't acquire write lock in [todo]");
        guard.get_mut(self.size - 1).map_or_else(
            || {
                panic!(
                    "Cann't acquire write lock in [todo], job index: {}",
                    self.size - 1
                )
            },
            |job| {
                METRIC_STORAGE.insert(name.clone(), Metric::default());
                job.set_name(name.clone());
                job.schedule_run_time();
            },
        );
    }

    /// Config function to be executed for `job` and add it to the `Shceduler`.
    ///
    /// If config `immediately_run` the `Scheduler` runs the job immediately.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1, and do not set time unit or `interval_fn`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let _ = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    /// }
    /// ```
    pub async fn todo<T: JobHandler + Send + Sync + Clone>(
        self,
        task: T,
    ) -> Scheduler<Task<T, TH, L>, L> {
        let name = task.name();
        self.schedule_and_set_name(name.clone());
        let need_lock: bool;
        let n_threads: u8;
        {
            let guard = self.jobs.read().await;
            let cur_job = guard.get(self.size - 1).unwrap();
            if cur_job.get_unit().is_none() && !cur_job.has_interval_fn() {
                panic!("Must set time unit or custom function!");
            }
            need_lock = cur_job.is_need_lock();
            n_threads = cur_job.n_threads();
            if need_lock && self.locker.is_none() {
                panic!("Please set locker!");
            }
            if cur_job.is_at() && cur_job.get_at_time().is_none() && cur_job.get_weekday().is_none()
            {
                panic!("Please set run time of job: day or weekday!");
            }
            if cur_job.get_immediately_run() {
                let cur_task = task.clone();
                let args = self.arg_storage.clone().unwrap();
                spawn(async move {
                    JobHandler::call(cur_task, args, name).await;
                });
            }
        }
        self.map(|fallback, locker| Task {
            name: task.name().clone(),
            task,
            fallback,
            locker,
            need_lock,
            n_threads,
        })
    }

    /// Return the name of next upcoming job, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert!(sch.next_run().is_some());
    ///     assert_eq!(sch.next_run(), Some("foo".to_string()));
    /// }
    /// ```
    pub fn next_run(&self) -> Option<String> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [next_run]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).week(1, 0, 59, 59).todo(execute(foo)).await;
    ///     assert_eq!(sch.weekday_with_name("foo"), Some(1));
    /// }
    /// ```
    pub fn weekday_with_name(&self, job_name: &str) -> Option<u32> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [weekday_with_name]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await;
    ///     assert_eq!(sch.time_unit_with_name("foo"), Some(0));
    /// }
    /// ```
    pub fn time_unit_with_name(&self, job_name: &str) -> Option<i8> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [time_unit_with_name]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await;
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(sch.next_run_with_name("foo").unwrap(), now + 2);
    /// }
    /// ```
    pub fn next_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [next_run_with_name]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await;
    ///     let now = Local::now().timestamp();
    ///     assert_eq!(sch.last_run_with_name("foo").unwrap(), now);
    /// }
    /// ```
    pub fn last_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't get read lock in [last_run_with_name]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let mut sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert_eq!(sch.get_job_names(), vec!["foo", "bar"]);
    ///     sch.cancel_job("foo".into());
    ///     assert_eq!(sch.get_job_names(), vec!["bar"]);
    /// }
    /// ```
    pub fn cancel_job(&mut self, job_name: &str) {
        let mut guard = self
            .jobs
            .try_write()
            .expect("Cann't get read lock in [cancel_job].");
        match guard.iter().position(|j| j.get_job_name() == job_name) {
            Some(i) => {
                guard.remove(i);
                self.size -= 1;
            }
            None => {}
        };
    }

    /// When `scheduler` catched termination signals, it will stop scheduling jobs, and if some jobs need locker, it will execute `unlock`.
    fn drop(&self) {
        let guard = self
            .jobs
            .try_read()
            .expect("Cann't acquire read lock in [drop]");
        guard.iter().for_each(|job| {
            if job.is_need_lock() {
                let name = job.get_job_name();
                unlock_and_record(
                    self.locker.clone().unwrap(),
                    &name,
                    self.arg_storage.clone().unwrap(),
                )
            }
        });
    }

    fn check_job_name(&self) {
        if self.get_job_names().iter().any(|name| name.is_empty()) {
            panic!("Empty job name!");
        }
    }

    /// Start scheduler and run all jobs,  
    ///
    /// The scheduler will spawn a asynchronous task for each *runnable* job to execute the job, and
    /// The program will stop if it catches terminate signals, in windows it is [`SIGINT, SIGTERM`], in linux it is [`SIGTERM, SIGQUIT, SIGINT`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    /// use std::error::Error;
    ///
    ///
    /// async fn foo() -> Result<(), Box<dyn Error>> {
    ///     println!("foo");
    ///     Ok(())
    /// }
    ///
    /// async fn bar() -> Result<(), Box<dyn Error>> {
    ///     println!("bar");
    ///     Ok(())
    /// }
    ///
    /// #[tokio::main]
    /// async fn main(){
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert!(sch.idle_seconds().is_some());
    ///     sch.start();
    /// }
    /// ```
    pub async fn start(&self) {
        self.check_job_name();
        #[cfg(feature = "tokio")]
        let (send, mut recv) = channel::<()>();
        #[cfg(feature = "smol")]
        let (send, recv) = channel::<()>();
        let term = Arc::new(AtomicBool::new(false));
        {
            #[cfg(not(windows))]
            for sig in TERM_SIGNALS {
                signal_hook::flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term))
                    .unwrap();
                signal_hook::flag::register(*sig, Arc::clone(&term)).unwrap();
            }
        }
        {
            #[cfg(windows)]
            for sig in vec![SIGINT, SIGTERM] {
                signal_hook::flag::register_conditional_shutdown(sig, 1, Arc::clone(&term))
                    .unwrap();
                signal_hook::flag::register(sig, Arc::clone(&term)).unwrap();
            }
        }
        log::info!("[INFO] Have registered [TERM_SIGNALS] signal!");
        let send_trigger = wait(async move {
            while !term.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1)).await;
            }
            let _ = send.send(()).await.map_err(|e| {
                panic!("Cann't send signal to channel, {}", e);
            });
        });
        let jobs = self.jobs.clone();
        let inter = if self.scan_interval == 0 {
            *DEFAULT_ZERO_CALL_INTERVAL
        } else {
            time::Duration::from_secs(self.scan_interval)
        };
        let task = self.task.clone();
        let storage = self.arg_storage.clone();
        let mut interval: Interval = Interval::new(inter).await;
        let recv_trigger = wait(async move {
            loop {
                let jobs_loop = jobs.clone();
                let task_loop = task.clone();
                let storage_loop = storage.clone();
                select! {
                _ = interval.tick() => {
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
                            if job.runnable(){
                                size = (ind + 1) as i64;
                            }else{
                                break;
                            }
                        }
                    }
                    if size < 0 {
                        continue;
                    }
                    {
                        let mut guard = jobs_loop.write().await;
                        for i in (0..size as usize).into_iter(){
                            guard[i].schedule_run_time();
                        }
                    }
                    spawn(async move{
                        let guard = jobs_loop.read().await;
                        for i in (0..size as usize).into_iter(){
                            let task_copy = task_loop.clone();
                            let sl = storage_loop.clone().unwrap();
                            JobHandler::call(task_copy, sl, guard[i].get_job_name()).await;
                        };
                    });
                },
                _ = recv.recv() => {
                        log::info!("[INFO] Shutting down!!!");
                        break;
                },
                }
            }
        });
        log::info!("[INFO] Have started!");
        let _ = futures::join!(send_trigger, recv_trigger);
        self.drop();
        return;
    }
}

impl<T, L> fmt::Debug for Scheduler<T, L>
where
    T: JobHandler + 'static + Send + Sync,
    L: Locker + 'static + Send + Sync,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let guard = self.jobs.try_read().unwrap();
        fmt.debug_struct("Scheduler")
            .field("scan_interval", &self.scan_interval)
            .field("size", &self.size)
            .field("jobs", &guard)
            .field("arg_storage", &self.arg_storage)
            .finish()
    }
}
