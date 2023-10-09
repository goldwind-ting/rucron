#![allow(dead_code)]

use chrono::{DateTime, Duration as duration, Local};
use lazy_static::lazy_static;
use rucron::{channel, sleep, spawn, RwLock, Scheduler, Sender};
use rucron::{handler::JobHandler, ArgStorage, Locker, Metric, ParseArgs, RucronError};
use serde::Deserialize;
use std::sync::{
    mpsc::{sync_channel, SyncSender},
    RwLock as sync_rwlock,
};
use std::{error::Error, sync::atomic::Ordering, sync::Arc, time::Duration};

pub(crate) fn sync_start_scheldure() {
    let (tx, rx) = sync_channel(1);
    *SYNC_BROADCAST_CONNECT.write().unwrap() = Some(tx);

    loop {
        std::thread::sleep(Duration::from_micros(100));
        if let Ok(_) = rx.recv() {
            break;
        };
    }
}

pub(crate) fn sync_interval_job() -> Result<(), Box<dyn Error>> {
    let now = Local::now().timestamp();
    {
        let mut guard = SYNC_TIME_COUNAINER_SECOND_INTERVAL.write().unwrap();
        guard.push(now);
    }
    {
        let guard = SYNC_TIME_COUNAINER_SECOND_INTERVAL.read().unwrap();
        if SYNC_EXECUTION_TIMES.le(&(guard.len() as i8)) {
            let tx = SYNC_BROADCAST_CONNECT.read().unwrap();
            let _ = tx.as_ref().unwrap().send(true).unwrap();
        }
    }
    Ok(())
}

lazy_static! {
    pub(crate) static ref BROADCAST_CONNECT: RwLock<Option<Sender<u8>>> = RwLock::new(None);
    pub(crate) static ref EXECUTION_TIMES: i8 = 3;
    pub(crate) static ref EXECUTION_TIMES_IMMEDIATELY: i8 = 1;
    pub(crate) static ref INTERVAL: i32 = 2;
    pub(crate) static ref TIME_COUNAINER_LOCKEROK: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    pub(crate) static ref TIME_COUNAINER_SECOND_INTERVAL: RwLock<Vec<i64>> =
        RwLock::new(Vec::new());
    pub(crate) static ref TIME_COUNAINER_ERROR_CALLBACK: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    pub(crate) static ref TIME_COUNAINER_MINUTE_INTERVAL: RwLock<Vec<i64>> =
        RwLock::new(Vec::new());
    pub(crate) static ref TIME_COUNAINER_IMMEDIATIALY: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    pub(crate) static ref EIGHT: RwLock<i8> = RwLock::new(0);
    pub(crate) static ref LOCKED_FLAG: RwLock<bool> = RwLock::new(false);
    pub(crate) static ref UNLOCKED: RwLock<bool> = RwLock::new(false);
    pub(crate) static ref SYNC_BROADCAST_CONNECT: sync_rwlock<Option<SyncSender<bool>>> =
        sync_rwlock::new(None);
    pub(crate) static ref SYNC_EXECUTION_TIMES: i8 = 3;
    pub(crate) static ref SYNC_INTERVAL: i32 = 2;
    pub(crate) static ref SYNC_TIME_COUNAINER_SECOND_INTERVAL: sync_rwlock<Vec<i64>> =
        sync_rwlock::new(Vec::new());
    pub(crate) static ref SYNC_EIGHT: sync_rwlock<i8> = sync_rwlock::new(0);
}

macro_rules! interval_job {
    ($fn_name:ident, $t:expr, $times:expr, $id:expr) => {
        pub(crate) async fn $fn_name() -> Result<(), Box<dyn Error>> {
            let now = Local::now().timestamp();
            {
                let mut guard = $t.write().await;
                guard.push(now);
                drop(guard);
            }
            {
                let guard = $t.read().await;
                if $times.le(&(guard.len() as i8)) {
                    drop(guard);
                    let tx = BROADCAST_CONNECT.read().await;
                    let _ = tx.as_ref().unwrap().send($id).await.unwrap();
                    drop(tx);
                }
            }
            Ok(())
        }
    };
}

#[derive(Deserialize, Debug)]
pub(crate) struct MetricTest {
    pub(crate) n_scheduled: i8,
    pub(crate) n_success: i8,
    pub(crate) t_total_elapsed: i8,
    pub(crate) t_maximum_elapsed: i8,
    pub(crate) t_minimum_elapsed: i8,
    pub(crate) t_average_elapsed: i8,
    pub(crate) n_error: i8,
    pub(crate) n_failure_of_unlock: i8,
    pub(crate) n_failure_of_lock: i8,
}

#[derive(Clone)]
pub(crate) struct RedisLockerOk;

impl Locker for RedisLockerOk {
    fn lock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(true)
    }
    fn unlock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(true)
    }
}

pub(crate) struct LockedLocker;

impl Locker for LockedLocker {
    fn lock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        let mut guard = LOCKED_FLAG.try_write().unwrap();
        if *guard {
            *guard = !*guard;
            return Ok(true);
        }
        Ok(false)
    }
    fn unlock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        let mut guard = LOCKED_FLAG.try_write().unwrap();
        println!("unlock {}", guard);
        if !*guard {
            *guard = !*guard;
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Clone)]
pub(crate) struct RedisLockerFlase;

impl Locker for RedisLockerFlase {
    fn lock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(false)
    }
    fn unlock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(false)
    }
}

pub(crate) async fn learn_rust() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am learning rust!");
    Ok(())
}

pub(crate) async fn panic_job() -> Result<(), Box<dyn Error>> {
    let mut guard = LOCKED_FLAG.try_write().unwrap();
    *guard = !*guard;
    Ok(())
}

pub(crate) async fn sing() -> Result<(), Box<dyn Error>> {
    println!("I am singing!");
    Ok(())
}

pub(crate) async fn cooking() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am cooking!");
    Ok(())
}

pub(crate) async fn error_job() -> Result<(), Box<dyn Error>> {
    Err(Box::new(RucronError::NotFound))
}

pub(crate) async fn counter() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(2)).await;
    {
        let mut guard = EIGHT.write().await;
        *guard += 1;
        drop(guard);
    }
    Ok(())
}

pub(crate) fn once(m: &Metric, last: &DateTime<Local>) -> duration {
    let n = m.n_scheduled.load(Ordering::Relaxed);
    if n < 1 {
        duration::seconds(2)
    } else if n == 1 {
        duration::seconds(last.timestamp() * 2)
    } else {
        duration::seconds(0)
    }
}

#[derive(Clone)]
pub(crate) struct Person {
    pub(crate) age: i32,
}

impl ParseArgs for Person {
    type Err = std::io::Error;
    fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
        return Ok(args.get::<Person>().unwrap().clone());
    }
}

pub(crate) async fn employee(p: Person) -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am {} years old", p.age);
    Ok(())
}

pub(crate) async fn working() -> Result<(), Box<dyn Error>> {
    println!("I am working!");
    Ok(())
}

pub(crate) async fn is_eight_years_old(p: Person) -> Result<(), Box<dyn Error>> {
    if p.age != 8 {
        let mut guard = EIGHT.write().await;
        *guard = 8;
        drop(guard);
    } else {
        let mut guard = EIGHT.write().await;
        *guard = 0;
        drop(guard);
    };
    Ok(())
}

pub(crate) async fn start_scheldure<
    T: JobHandler + 'static + Send + Sync + Clone,
    L: Locker + 'static + Send + Sync + Clone,
>(
    sch: Scheduler<T, L>,
    id: u8,
) {
    #[cfg(feature = "tokio")]
    let (tx, mut rx) = channel();
    #[cfg(feature = "smol")]
    let (tx, rx) = channel();
    {
        *BROADCAST_CONNECT.write().await = Some(tx);
    }
    spawn(async move {
        sch.start().await;
    });
    #[cfg(feature = "tokio")]
    while let Some(v) = rx.recv().await {
        if v == id {
            break;
        }
    }
    #[cfg(feature = "smol")]
    while let Ok(v) = rx.recv().await {
        if v == id {
            break;
        }
    }
}

pub(crate) async fn start_scheldure_with_cancel<
    T: JobHandler + 'static + Send + Sync + Clone,
    L: Locker + 'static + Send + Sync + Clone,
>(
    sch: Scheduler<T, L>,
    interval: u64,
) {
    spawn(async move {
        sch.start().await;
    });
    sleep(Duration::from_secs(interval)).await;
}

interval_job!(
    do_second_interval_job,
    TIME_COUNAINER_SECOND_INTERVAL,
    EXECUTION_TIMES,
    1
);

interval_job!(do_lockerok_job, TIME_COUNAINER_LOCKEROK, EXECUTION_TIMES, 2);
interval_job!(
    do_minute_interval_job,
    TIME_COUNAINER_MINUTE_INTERVAL,
    EXECUTION_TIMES,
    3
);

interval_job!(
    immediately_job,
    TIME_COUNAINER_IMMEDIATIALY,
    EXECUTION_TIMES_IMMEDIATELY,
    4
);
