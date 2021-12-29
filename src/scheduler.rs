use crate::handler::{ArgStorage, JobHandler, Task};
use crate::job::{Job, TimeUnit};
use crate::locker::Locker;

use std::{fmt, sync::Arc};
use tokio::sync::{mpsc::channel, RwLock};
use tokio::time::{self, sleep, Duration, Interval};
extern crate lazy_static;
use async_trait::async_trait;

#[cfg(windows)]
use signal_hook::consts::SIGINT;

#[cfg(not(windows))]
use signal_hook::consts::SIGTSTP; // catch ctrl + z signal
use std::sync::atomic::{AtomicBool, Ordering};

lazy_static! {
    static ref DEFAULT_ZERO_CALL_INTERVAL: Duration = Duration::from_secs(1);
}

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
    async fn call(&self, args: Arc<ArgStorage>, name: String) {
        JobHandler::call(&self.task, args, name).await;
    }

    fn name(&self) -> String {
        return self.task.name();
    }
}

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
    async fn call(&self, _args: Arc<ArgStorage>, _name: String) {}
    fn name(&self) -> String {
        return "FakeJob".into();
    }
}

impl<L: Send> Scheduler<EmptyTask, L> {
    /// Creates a new instance of an `Scheduler<R, L>`.
    /// - `interval` set the interval how often `jobs` should be checked, default to 1.
    /// - `capacity` is the capicity of jobs. Please note `interval` is 1 by default.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::Scheduler;
    ///
    ///
    /// let mut sch = Scheduler::<(), ()>::new(2, 10);
    /// ```

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
    #[inline]
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
    pub fn is_scheduled(&self, name: &str) -> bool {
        let guard = self.jobs.try_read().unwrap();
        for i in 0..guard.len() {
            if guard[i].get_job_name() == name {
                return true;
            }
        }
        return false;
    }

    pub fn set_arg_storage(&mut self, storage: ArgStorage) {
        self.arg_storage.replace(Arc::new(storage));
    }

    pub fn set_locker(&mut self, locker: L) {
        self.locker = Some(locker);
    }

    pub fn need_lock(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).unwrap().need_locker();
        }
        self
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

    /// Return Number of seconds until next upcoming job starts running, return `None` if `size` is 0.
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
    pub fn idle_seconds(&self) -> Option<i64> {
        let guard = self.jobs.try_read().unwrap();
        guard
            .iter()
            .min_by(|x, y| x.get_next_run().cmp(&y.get_next_run()))
            .and_then(|job| Some(job.get_next_run().timestamp()))
    }

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
    pub fn at(mut self) -> Self {
        let job = Job::new(1, true);
        self.insert_job(job);
        self
    }

    pub fn immediately_run(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            let cur_job = guard.get_mut(self.size - 1).unwrap();
            cur_job.immediately_run();
        }
        self
    }

    pub fn with_unlock(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            let name = guard.get_mut(self.size - 1).unwrap().get_job_name();
            self.locker
                .clone()
                .and_then(|l| Some(l.unlock(&name[..], self.arg_storage.clone().unwrap())));
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
    pub fn hour(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("cann't get the job, job index: {}", self.size - 1);
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
    pub fn minute(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("cann't get the job, index: {}", self.size - 1);
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
    pub fn second(self) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("cann't get the job, job index: {}", self.size - 1);
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
    pub fn day(self, h: i64, m: i64, s: i64) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("cann't get the job, job index: {}", self.size - 1);
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
    pub fn week(self, w: i64, h: i64, m: i64, s: i64) -> Self {
        {
            let mut guard = self.jobs.try_write().unwrap();
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!("cann't get the job, job index: {}", self.size - 1);
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

    /// Config function to be executed for `job`.
    ///
    /// # Panics
    ///
    /// Panics if `size` is less than 1.
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
    pub async fn todo<T: JobHandler + Send + Sync + Clone>(
        self,
        task: T,
    ) -> Scheduler<Task<T, TH, L>, L> {
        let name = task.name();
        {
            let mut guard = self.jobs.write().await;
            let cur_job = guard.get_mut(self.size - 1).unwrap();
            cur_job.set_name(name.clone());
            cur_job.schedule_run_time();
        }
        let need_lock: bool;
        {
            let guard = self.jobs.read().await;
            let cur_job = guard.get(self.size - 1).unwrap();
            if let None = cur_job.get_unit() {
                panic!("must set time unit!");
            }
            need_lock = cur_job.has_locker();
            if cur_job.get_is_at()
                && cur_job.get_at_time().is_none()
                && cur_job.get_weekday().is_none()
            {
                panic!("please set run time of job: day or weekday!");
            }
            if cur_job.get_immediately_run() {
                let cur_task = task.clone();
                let arg = self.arg_storage.clone().unwrap();
                tokio::spawn(async move {
                    JobHandler::call(&cur_task, arg, name).await;
                });
            }
        }
        self.map(|fallback, locker| Task {
            name: task.name().clone(),
            task,
            fallback,
            locker,
            need_lock,
        })
    }

    // pub async fn todo_with_unlock<F>(&self, f: H)
    // {
    //     let mut guard = self.jobs.write().await;
    //     guard
    //         .get_mut(self.size - 1)
    //         .map_or_else(
    //             || {
    //                 panic!("cann't get the job, job index: {}", self.size - 1);
    //             },
    //             |job| async move {
    //                 job.set_job(f, true).await;
    //             },
    //         )
    //         .await;
    // }

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
    pub fn next_run(&self) -> Option<String> {
        let guard = self.jobs.try_read().unwrap();
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
    pub fn weekday_with_name(&self, job_name: &str) -> Option<u32> {
        let guard = self.jobs.try_read().unwrap();
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
    pub fn time_unit_with_name(&self, job_name: &str) -> Option<i8> {
        let guard = self.jobs.try_read().unwrap();
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
    pub fn next_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self.jobs.try_read().unwrap();
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
    pub fn last_run_with_name(&self, job_name: &str) -> Option<i64> {
        let guard = self.jobs.try_read().unwrap();
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
    pub fn cancel_job(&mut self, job_name: &str) {
        let mut guard = self.jobs.try_write().unwrap();
        match guard.iter().position(|j| j.get_job_name() == job_name) {
            Some(i) => {
                guard.remove(i);
                self.size -= 1;
            }
            None => {}
        };
    }

    fn check_job_name(&self) {
        let guard = self.jobs.try_read().unwrap();
        guard.iter().enumerate().for_each(|(ind, job)| {
            if job.get_job_name().len() <= 0 {
                panic!("please set job, job index: {}", ind);
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

    /// Start scheduler and run all jobs,  
    ///
    /// The scheduler will spawn a asynchronous task for each *runable* job to execute the job, and
    /// The program will stop if it catches `SIGTSTP` signal in linux or `SIGINT` signal in windows.
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
        self.check_job_name();
        let (send, mut recv) = channel(1);
        let term = Arc::new(AtomicBool::new(false));
        #[cfg(not(windows))]
        let _ = signal_hook::flag::register(SIGTSTP, Arc::clone(&term)).map_err(|e| {
            panic!("cann't register signal: {}", e);
        });
        #[cfg(windows)]
        let _ = signal_hook::flag::register(SIGINT, Arc::clone(&term)).map_err(|e| {
            panic!("cann't register signal: {}", e);
        });
        let send_trigger = tokio::spawn(async move {
            while !term.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1)).await;
            }
            let _ = send.send(()).await.map_err(|e| {
                panic!("cann't send signal to channel, {}", e);
            });
        });
        let jobs = self.jobs.clone();
        let inter = self.scan_interval;
        let task = self.task.clone();
        let storage = self.arg_storage.clone();
        let mut interval: Interval = self.gen_call_interval(inter);
        let recv_trigger = tokio::spawn(async move {
            loop {
                let jobs_loop = jobs.clone();
                let task_loop = task.clone();
                let storage_loop = storage.clone();
                tokio::select! {
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
                            if job.runable(){
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
                    tokio::spawn(async move{
                        let guard = jobs_loop.read().await;

                        for i in (0..size as usize).into_iter(){
                            let sl = storage_loop.clone().unwrap();
                            println!("{} next is {}", guard[i].get_job_name(), guard[i].get_next_run().timestamp());
                            JobHandler::call(&task_loop, sl, guard[i].get_job_name()).await;
                        };
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
