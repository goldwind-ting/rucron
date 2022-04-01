//! Rucron is a lightweight job scheduler, it is similar to [`gocron`] or linux crontab,
//! and it is very easy to use.
//! But now it only supports asynchronous job by the runtime provided by tokio.
//!
//!
//! # Usage
//!
//! Add this to your Cargo.toml
//!
//! ```toml
//! [dependencies]
//! rucron = "*"
//! ```
//!
//! Quick start:
//!
//! ``` rust
//!
//! use rucron::{execute, sync_execute, EmptyTask, Metric, Scheduler};
//! use std::{error::Error, sync::atomic::Ordering};
//! use chrono::{Local, DateTime, Duration};
//!
//!
//! async fn foo() -> Result<(), Box<dyn Error>>{
//!     println!("foo");
//!     Ok(())
//! }
//!
//! async fn bar() -> Result<(), Box<dyn Error>>{
//!     println!("bar");
//!     Ok(())
//! }
//!
//! async fn ping() -> Result<(), Box<dyn Error>>{
//!     println!("ping");
//!     Ok(())
//! }
//!
//! fn sync_task() -> Result<(), Box<dyn Error>>{
//!     std::thread::sleep(std::time::Duration::from_secs(2));
//!     println!("sync");
//!     Ok(())
//! }
//!
//! fn once(m: &Metric, last: &DateTime<Local>) -> Duration {
//!     let n = m.n_scheduled.load(Ordering::Relaxed);
//!     if n < 1 {
//!         Duration::seconds(2)
//!     } else if n == 1 {
//!         Duration::seconds(last.timestamp() * 2)
//!     } else {
//!         Duration::seconds(0)
//!    }
//! }
//! 
//! 
//! fn sync_task() -> Result<(), Box<dyn Error>> {
//!     std::thread::sleep(std::time::Duration::from_secs(2));
//!     println!("sync task");
//!     Ok(())
//! }
//!
//! 
//! #[tokio::main]
//! async fn main(){
//!     // Create a scheduler with 10 capacity, it will checkout all runnable jobs every second
//!     let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
//!     let sch = sch
//!                 // Scheduler runs foo every second.
//!                 .every(1).second().todo(execute(foo)).await
//!                 // Scheduler runs bar every monday at 9 am.
//!                 .at().week(1, 9, 0, 0).todo(execute(bar)).await
//!                 // Scheduler runs ping only once.
//!                 .by(once).todo(execute(ping)).await
//!                 // Scheduler a CPU-bound or blocking task.
//!                 .every(2).second().todo(sync_execute(sync_task)).await
//!                 // Schedule a CPU-bound or blocking task.
//!                 .every(2).second().todo(sync_execute(sync_task)).await;
//!     // Start running all jobs.
//!     // sch.start().await;
//! }
//! ```
//!
//! Schedule parameterized job.
//!
//! ```rust
//!
//! use rucron::{execute, ArgStorage, EmptyTask, ParseArgs, Scheduler};
//! use std::error::Error;
//! use async_trait::async_trait;
//!
//!
//! #[derive(Clone)]
//! struct Person {
//!     age: i32,
//! }
//!
//! #[async_trait]
//! impl ParseArgs for Person {
//!     type Err = std::io::Error;
//!     fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
//!         return Ok(args.get::<Person>().unwrap().clone());
//!     }
//! }
//!
//! async fn is_eight_years_old(p: Person) -> Result<(), Box<dyn Error>> {
//!     if p.age == 8 {
//!         println!("I am eight years old!");
//!     } else {
//!         println!("Oops!");
//!     };
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let child = Person { age: 8 };
//!     // Storage stores all arguments.
//!     let mut arg = ArgStorage::new();
//!     arg.insert(child);
//!     let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
//!     sch.set_arg_storage(arg);
//!     let sch = sch.every(2).second().todo(execute(is_eight_years_old)).await;
//!     // sch.start().await;
//! }
//!```
//!
//! You could also schedule blocking or CPU-bound tasks.
//!
//! ```rust
//! use rucron::{sync_execute, ArgStorage, EmptyTask, ParseArgs, Scheduler};
//! use std::error::Error;
//!
//!
//! #[derive(Clone)]
//! struct Person {
//!     age: i32,
//! }
//!
//! impl ParseArgs for Person {
//!     type Err = std::io::Error;
//!     fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
//!         return Ok(args.get::<Person>().unwrap().clone());
//!     }
//! }
//!
//! fn sync_set_age(p: Person) -> Result<(), Box<dyn Error>> {
//!     if p.age == 8 {
//!         println!("I am eight years old!");
//!     };
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let child = Person { age: 8 };
//!     let mut arg = ArgStorage::new();
//!     arg.insert(child);
//!     let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
//!     sch.set_arg_storage(arg);
//!     let sch = sch.every(2).second().todo(sync_execute(sync_set_age)).await;
//!     sch.start().await;
//! }
//!
//! ```
//! [`gocron`]: https://github.com/go-co-op/gocron

pub mod error;
pub mod handler;
pub(crate) mod job;
pub mod locker;
pub mod metric;
pub mod scheduler;

#[macro_use]
extern crate lazy_static;
pub use crate::error::RucronError;
pub use crate::handler::{execute, sync_execute, ArgStorage, ParseArgs};
pub use crate::locker::Locker;
pub use crate::metric::Metric;
pub use crate::scheduler::{EmptyTask, Scheduler};

use crate::metric::NumberType;
use dashmap::DashMap;
use serde_json;
use std::sync::Arc;
use tokio::time::Duration;

lazy_static! {
    static ref DEFAULT_ZERO_CALL_INTERVAL: Duration = Duration::from_secs(1);
    static ref METRIC_STORAGE: DashMap<String, Metric> = DashMap::new();
}

/// Get the `Metric` data and serialize it to `String`, return NotFound error if can not find the metric with `name`.
pub fn get_metric_with_name(name: &str) -> Result<String, RucronError> {
    let m = METRIC_STORAGE.get(name).ok_or(RucronError::NotFound)?;
    serde_json::to_string(&*m).map_err(|e| RucronError::SerdeError(e.to_string()))
}

pub(crate) fn unlock_and_record<L: Locker>(locker: L, key: &str, args: Arc<ArgStorage>) {
    match locker.unlock(key, args.clone()) {
        Ok(b) if !b => METRIC_STORAGE.get(key).map_or_else(
            || unreachable!("unreachable"),
            |m| m.add_failure(NumberType::Unlock),
        ),
        Ok(_) => {}
        Err(e) => {
            log::error!("{}", e);
            METRIC_STORAGE.get(key).map_or_else(
                || unreachable!("unreachable"),
                |m| m.add_failure(NumberType::Error),
            )
        }
    };
}
