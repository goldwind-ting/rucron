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
    async fn call(&self, _args: Arc<ArgStorage>, _name: String) {}
    fn name(&self) -> String {
        return "EmptyTask".into();
    }
}

impl<L: Send> Scheduler<EmptyTask, L> {
    /// Creates a new instance of an `Scheduler<TH, L>`.
    /// - `interval` set the interval how often `jobs` should be checked, default to 1.
    /// - `capacity` is the capicity of jobs. Please note `interval` is 1 by default.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask};
    ///
    ///
    /// let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
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
    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns `true` if the function had been added to job list `jobs`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                for i in 0..guard.len() {
                    if guard[i].get_job_name() == name {
                        return Ok(true);
                    }
                }
                return Ok(false);
            })
            .expect("Cann't get read lock in [is_scheduled]")
    }

    pub fn set_arg_storage(&mut self, storage: ArgStorage) {
        self.arg_storage.replace(Arc::new(storage));
    }

    pub fn set_locker(&mut self, locker: L) {
        self.locker = Some(locker);
    }

    pub fn need_lock(self) -> Self {
        {
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [need_lock]"),
                |mut guard| {
                    guard.get_mut(self.size - 1).map_or_else(
                        || {
                            panic!(
                                "Cann't get the job in [need_lock], index: {}",
                                self.size - 1
                            )
                        },
                        |job| job.need_locker(),
                    );
                },
            );
        }
        self
    }
    /// Return all names of job which is added in `jobs`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
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
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(2).second().todo(execute(bar)).await;
    ///     assert_eq!(sch.get_job_names(), vec!["foo", "bar"]);
    /// }
    /// ```
    pub fn get_job_names(&self) -> Vec<String> {
        let mut job_names = Vec::new();
        self.jobs
            .try_read()
            .and_then(|guard| {
                guard.iter().for_each(|job| {
                    job_names.push(job.get_job_name());
                });
                Ok(job_names)
            })
            .expect("Cann't get write lock in [get_job_names]")
    }

    /// Return Number of seconds until next upcoming job starts running, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                Ok(guard
                    .iter()
                    .min_by(|x, y| x.get_next_run().cmp(&y.get_next_run()))
                    .and_then(|job| Some(job.get_next_run().timestamp())))
            })
            .expect("Cann't get write lock in [idle_seconds]")
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
    /// use rucron::{Scheduler, EmptyTask, execute};
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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

    pub fn immediately_run(self) -> Self {
        {
            self.jobs
                .try_write()
                .and_then(|mut guard| {
                    Ok(guard.get_mut(self.size - 1).and_then(|job| {
                        job.immediately_run();
                        Some(())
                    }))
                })
                .expect("Cann't get write lock in [immediately_run]");
        }
        self
    }

    pub fn with_unlock(self) -> Self {
        {
            let name = self
                .jobs
                .try_write()
                .and_then(|guard| {
                    Ok(guard.get(self.size - 1).map_or_else(
                        || {
                            panic!(
                                "Cann't get the job in [with_unlock], index: {}",
                                self.size - 1
                            )
                        },
                        |job| job.get_job_name(),
                    ))
                })
                .expect("Cann't get write lock in [with_unlock]");
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
    /// use rucron::{Scheduler, EmptyTask, execute};
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [hour]"),
                |mut guard| {
                    guard.get_mut(self.size - 1).map_or_else(
                        || {
                            panic!("Cann't get the job in [hour], job index: {}", self.size - 1);
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [minute]"),
                |mut guard| {
                    guard.get_mut(self.size - 1).map_or_else(
                        || {
                            panic!("Cann't get the job in [minute], index: {}", self.size - 1);
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [second]"),
                |mut guard| {
                    guard.get_mut(self.size - 1).map_or_else(
                        || {
                            panic!(
                                "Cann't get the job in [second], job index: {}",
                                self.size - 1
                            );
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [day]"),
                |mut guard| {
                    guard.get_mut(self.size - 1).map_or_else(
                        || {
                            panic!("Cann't get the job in [day], job index: {}", self.size - 1);
                        },
                        |job| {
                            job.set_unit(TimeUnit::Day);
                            job.set_at_time(h, m, s);
                        },
                    );
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
            self.jobs.try_write().map_or_else(
                |_| panic!("Cann't get write lock in [week]"),
                |mut guard| {
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
    /// use rucron::{Scheduler, EmptyTask, execute};
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
        {
            let mut guard = self.jobs.write().await;
            guard.get_mut(self.size - 1).map_or_else(
                || {
                    panic!(
                        "Cann't get write lock in [todo], job index: {}",
                        self.size - 1
                    )
                },
                |job| {
                    job.set_name(name.clone());
                    job.schedule_run_time();
                },
            );
        }
        let need_lock: bool;
        {
            let guard = self.jobs.read().await;
            let cur_job = guard.get(self.size - 1).unwrap();
            if let None = cur_job.get_unit() {
                panic!("Must set time unit!");
            }
            need_lock = cur_job.has_locker();
            if cur_job.get_is_at()
                && cur_job.get_at_time().is_none()
                && cur_job.get_weekday().is_none()
            {
                panic!("Please set run time of job: day or weekday!");
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

    /// Return the name of next upcoming job, return `None` if `size` is 0.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
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
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert!(sch.next_run().is_some());
    ///     assert_eq!(sch.next_run(), Some("foo".to_string()));
    /// }
    /// ```
    pub fn next_run(&self) -> Option<String> {
        self.jobs
            .try_read()
            .and_then(|guard| {
                Ok(guard
                    .iter()
                    .min_by(|a, b| a.get_next_run().cmp(&b.get_next_run()))
                    .map_or(None, |job| Some(job.get_job_name())))
            })
            .expect("Cann't get read lock in [next_run]")
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                for job in guard.iter() {
                    if job.get_job_name().as_str() == job_name {
                        return Ok(job.get_weekday().and_then(|w| Some(w.number_from_monday())));
                    }
                }
                Ok(None)
            })
            .expect("Cann't get read lock in [weekday_with_name]")
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
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                for job in guard.iter() {
                    if job.get_job_name().as_str() == job_name {
                        return Ok(job.get_time_unit());
                    }
                }
                Ok(None)
            })
            .expect("Cann't get read lock in [time_unit_with_name]")
    }

    /// Return next run time of job by job name, return None if the job does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                for job in guard.iter() {
                    if job.get_job_name().as_str() == job_name {
                        return Ok(Some(job.get_next_run().timestamp()));
                    }
                }
                Ok(None)
            })
            .expect("Cann't get read lock in [next_run_with_name]")
    }

    /// Return last run time of job by job name, return None if the job does not exist.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
    /// use chrono::Local;
    ///
    ///
    /// async fn foo(){
    ///     println!("foo");
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
        self.jobs
            .try_read()
            .and_then(|guard| {
                for job in guard.iter() {
                    if job.get_job_name().as_str() == job_name {
                        return Ok(Some(job.get_last_run().timestamp()));
                    }
                }
                Ok(None)
            })
            .expect("Cann't get read lock in [last_run_with_name]")
    }

    /// Delete a scheduled job.
    ///
    /// # Examples
    ///
    /// ```
    /// use rucron::{Scheduler, EmptyTask, execute};
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
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let mut sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert_eq!(sch.get_job_names(), vec!["foo", "bar"]);
    ///     sch.cancel_job("foo".into());
    ///     assert_eq!(sch.get_job_names(), vec!["bar"]);
    /// }
    /// ```
    pub fn cancel_job(&mut self, job_name: &str) {
        self.jobs.try_write().map_or_else(
            |_| panic!("Cann't get read lock in [cancel_job]"),
            |mut guard| {
                match guard.iter().position(|j| j.get_job_name() == job_name) {
                    Some(i) => {
                        guard.remove(i);
                        self.size -= 1;
                    }
                    None => {}
                };
            },
        );
    }

    async fn check_job_name(&self) {
        let guard = self.jobs.read().await;
        guard.iter().enumerate().for_each(|(ind, job)| {
            if job.get_job_name().len() <= 0 {
                panic!("Please set job in [check_job_name], job index: {}", ind);
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
    /// use rucron::{Scheduler, EmptyTask, execute};
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
    ///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
    ///     let sch = sch.every(2).second().todo(execute(foo)).await
    ///     .every(6).second().todo(execute(bar)).await;
    ///     assert!(sch.idle_seconds().is_some());
    ///     sch.start();
    /// }
    /// ```
    pub async fn start(&self) {
        self.check_job_name().await;
        let (send, mut recv) = channel(1);
        let term = Arc::new(AtomicBool::new(false));
        {
            #[cfg(not(windows))]
            let _ = signal_hook::flag::register(SIGTSTP, Arc::clone(&term)).map_err(|e| {
                panic!("Cann't register signal: {}", e);
            });
            log::info!("[INFO] Have registered [SIGTSTP] signal!");
        }
        {
            #[cfg(windows)]
            let _ = signal_hook::flag::register(SIGINT, Arc::clone(&term)).map_err(|e| {
                panic!("Cann't register signal: {}", e);
            }).map(|_|log::info!("[INFO] Have registered [SIGTSTP] signal!"));
            
        }
        let send_trigger = tokio::spawn(async move {
            while !term.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1)).await;
            }
            let _ = send.send(()).await.map_err(|e| {
                panic!("Cann't send signal to channel, {}", e);
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
                            JobHandler::call(&task_loop, sl, guard[i].get_job_name()).await;
                        };
                    });
                },
                Some(_) = recv.recv() => {
                        log::info!("[INFO] Shutting down!!!");
                        break;
                },
                }
            }
        });
        log::info!("[INFO] Have started!");
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
