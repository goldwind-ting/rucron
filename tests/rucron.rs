// cargo test -- --test-threads 1

use chrono::{Datelike, Local, Timelike};
use rucron::{locker::Locker, scheduler::Scheduler};
use std::io::{prelude::*, Error as ioerr};
use std::{fs::File, path::Path};
use tokio::sync::{
    mpsc::{channel, Sender},
    RwLock,
};
use tokio::time::{sleep, Duration};
#[macro_use]
extern crate lazy_static;

lazy_static! {
    static ref BROADCAST_CONNECT: RwLock<Option<Sender<bool>>> = RwLock::new(None);
    static ref EXECUTION_TIMES: i8 = 3;
    static ref EXECUTION_TIMES_IMMEDIATELY: i8 = 1;
    static ref INTERVAL: i32 = 2;
    static ref TIME_COUNAINER_LOCKEROK: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref TIME_COUNAINER_SECOND_INTERVAL: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref TIME_COUNAINER_ERROR_CALLBACK: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref TIME_COUNAINER_MINUTE_INTERVAL: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref TIME_COUNAINER_IMMEDIATIALY: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref ERROR_LOG_FILE: &'static str = "error.log";
    static ref LOCKED_FLAG: RwLock<bool> = RwLock::new(false);
    static ref UNLOCKED: RwLock<bool> = RwLock::new(false);
}

macro_rules! interval_job {
    ($fn_name:ident, $t:expr, $times:expr) => {
        async fn $fn_name() {
            let now = Local::now().timestamp();
            {
                let mut guard = $t.write().await;
                guard.push(now);
            }

            {
                let guard = $t.read().await;
                if $times.le(&(guard.len() as i8)) {
                    let tx = BROADCAST_CONNECT.read().await;
                    let _ = tx.as_ref().unwrap().send(true).await.unwrap();
                }
            }
        }
    };
}

struct RedisLockerOk;

impl Locker for RedisLockerOk {
    type Error = ioerr;
    fn lock(&self, _key: &str) -> Result<bool, Self::Error> {
        Ok(true)
    }
    fn unlock(&self, _key: &str) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

struct LockedLocker;

impl Locker for LockedLocker {
    type Error = ioerr;
    fn lock(&self, _key: &str) -> Result<bool, Self::Error> {
        let mut guard = LOCKED_FLAG.try_write().unwrap();
        if *guard {
            *guard = !*guard;
            return Ok(true);
        }
        Ok(false)
    }
    fn unlock(&self, _key: &str) -> Result<bool, Self::Error> {
        let mut guard = LOCKED_FLAG.try_write().unwrap();
        println!("unlock {}", guard);
        if !*guard {
            *guard = !*guard;
            return Ok(true);
        }
        Ok(false)
    }
}

struct RedisLockerErr;

impl Locker for RedisLockerErr {
    type Error = ioerr;
    fn lock(&self, _key: &str) -> Result<bool, Self::Error> {
        Err(std::io::Error::from_raw_os_error(22))
    }
    fn unlock(&self, _key: &str) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

async fn learn_rust() {
    println!("I am learning rust!");
}

async fn sing() {
    println!("I am singing!");
}

async fn cooking() {
    println!("I am cooking!");
}

async fn exercising() {
    println!("I am exercising!");
}

async fn working() {
    println!("I am working!");
}

fn err_callback_mock(err: ioerr) {
    let path = Path::new("./error.log");
    let display = path.display();

    let mut file = match File::create(&path) {
        Err(e) => panic!("couldn't create {}: {}", display, e.to_string()),
        Ok(file) => file,
    };

    match file.write_all(err.to_string().as_bytes()) {
        Err(e) => {
            panic!("couldn't write to {}: {}", display, e.to_string())
        }
        Ok(_) => {}
    }
}

async fn start_scheldure<L: Locker + 'static + Send + Sync>(sch: Scheduler<(), L>) {
    let (tx, mut rx) = channel(1);
    *BROADCAST_CONNECT.write().await = Some(tx);
    tokio::spawn(async move {
        sch.start().await;
    });
    loop {
        sleep(Duration::from_micros(100)).await;
        if let Some(_) = rx.recv().await {
            break;
        };
    }
}

async fn start_scheldure_with_cancel<L: Locker + 'static + Send + Sync>(
    sch: Scheduler<(), L>,
    interval: u64,
) {
    let (tx, mut rx) = channel(1);
    *BROADCAST_CONNECT.write().await = Some(tx);
    tokio::spawn(async move {
        sch.start().await;
    });
    tokio::spawn(async move {
        let tx = BROADCAST_CONNECT.read().await;
        sleep(Duration::from_secs(interval)).await;
        let _ = tx.as_ref().unwrap().send(true).await.unwrap();
    });
    loop {
        sleep(Duration::from_micros(100)).await;
        if let Some(_) = rx.recv().await {
            break;
        };
    }
}

interval_job!(
    do_second_interval_job,
    TIME_COUNAINER_SECOND_INTERVAL,
    EXECUTION_TIMES
);
interval_job!(
    do_error_callback_job,
    TIME_COUNAINER_ERROR_CALLBACK,
    EXECUTION_TIMES
);
interval_job!(do_lockerok_job, TIME_COUNAINER_LOCKEROK, EXECUTION_TIMES);
interval_job!(
    do_minute_interval_job,
    TIME_COUNAINER_MINUTE_INTERVAL,
    EXECUTION_TIMES
);
interval_job!(
    immediately_job,
    TIME_COUNAINER_IMMEDIATIALY,
    EXECUTION_TIMES_IMMEDIATELY
);

#[tokio::test]
async fn test_second_interval() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(0, 10);
    sch.every(*INTERVAL)
        .await
        .second()
        .await
        .todo(do_second_interval_job)
        .await;
    start_scheldure(sch).await;
    let guard = TIME_COUNAINER_SECOND_INTERVAL.read().await;
    let leng = guard.len();
    assert_eq!(guard.len(), 3);
    for i in 1..leng {
        assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
        assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
    }
}

#[tokio::test]
async fn test_minute_interval() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(*INTERVAL / 2)
        .await
        .minute()
        .await
        .todo(do_minute_interval_job)
        .await;
    start_scheldure(sch).await;
    let guard = TIME_COUNAINER_MINUTE_INTERVAL.read().await;
    let leng = guard.len();
    assert_eq!(guard.len(), 3);
    for i in 1..leng {
        assert!((guard[i] - guard[i - 1]) as i32 >= 30 * *INTERVAL);
        assert!((guard[i] - guard[i - 1]) as i32 <= 30 * *INTERVAL + 1);
    }
}

#[tokio::test]
async fn test_idle_seconds() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;
    sch.every(8)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(exercising)
        .await;

    assert!(sch.idle_seconds().await.is_some());
    let idle = sch.idle_seconds().await.unwrap();
    let now = Local::now().timestamp();
    assert_eq!(idle - now, 2);
    sch.every(1)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;
    let idle = sch.idle_seconds().await.unwrap();
    let now = Local::now().timestamp();
    assert_eq!(idle - now, 1);
}

#[tokio::test]
async fn test_get_job_names() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;

    sch.every(4)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;

    sch.every(6)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(cooking)
        .await;

    sch.every(8)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(exercising)
        .await;
    let names = sch.get_job_names().await;
    assert_eq!(names, vec!["learn_rust", "sing", "cooking", "exercising"]);
}

#[tokio::test]
async fn test_is_scheduled() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;
    sch.every(8)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(exercising)
        .await;
    assert!(sch.is_scheduled(learn_rust).await);
    assert!(sch.is_scheduled(exercising).await);
}

#[tokio::test]
async fn test_length() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;
    sch.every(8)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(exercising)
        .await;

    assert_eq!(sch.len(), 2);
    assert!(sch.is_scheduled(exercising).await);
    assert!(sch.is_scheduled(learn_rust).await);

    sch.cancel_job("exercising".into()).await;
    assert!(!sch.is_scheduled(exercising).await);
}

#[tokio::test]
async fn test_next_run() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(60)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;

    let job = sch.next_run().await.unwrap();
    assert_eq!(job, String::from("learn_rust"));
    sch.every(4)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;
    let job = sch.next_run().await.unwrap();
    assert_eq!(job, String::from("sing"));
    sch.every(6)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(cooking)
        .await;

    let job = sch.next_run().await.unwrap();
    assert_eq!(job, String::from("sing"));

    sch.every(1)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(exercising)
        .await;
    let job = sch.next_run().await.unwrap();
    assert_eq!(job, String::from("exercising"));
}

#[tokio::test]
async fn test_error_callback() {
    let mut sch = Scheduler::<(), RedisLockerErr>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts(
            false,
            Some(Box::new(err_callback_mock)),
            Some(RedisLockerErr),
        )
        .await
        .todo(do_error_callback_job)
        .await;
    start_scheldure_with_cancel(sch, 2).await;

    let path = Path::new(*ERROR_LOG_FILE);
    assert!(path.exists());

    let mut f = File::open(path).unwrap();
    let mut buf = String::new();
    let n = f.read_to_string(&mut buf).unwrap();
    assert_eq!(buf, "Invalid argument (os error 22)".to_string());
    assert_eq!(n, 30);
}

#[tokio::test]
async fn test_locker_ok() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts(
            false,
            Some(Box::new(err_callback_mock)),
            Some(RedisLockerOk),
        )
        .await
        .todo(do_lockerok_job)
        .await;
    start_scheldure(sch).await;

    let guard = TIME_COUNAINER_LOCKEROK.read().await;
    let leng = guard.len();
    assert_eq!(guard.len(), 3);
    for i in 1..leng {
        assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
        assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
    }
}

#[tokio::test]
async fn test_immediately_run() {
    let (tx, mut rx) = channel(1);
    *BROADCAST_CONNECT.write().await = Some(tx);
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(true, None, None)
        .await
        .todo(immediately_job)
        .await;

    loop {
        sleep(Duration::from_micros(10)).await;
        if let Some(_) = rx.recv().await {
            break;
        };
    }

    let guard = TIME_COUNAINER_IMMEDIATIALY.read().await;
    assert_eq!(guard.len(), 1);
    assert_eq!(0, guard[0] - Local::now().timestamp());
}

#[tokio::test]
async fn test_every_time_unit() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.every(1).await.second().await.todo(learn_rust).await;
    sch.every(1).await.minute().await.todo(sing).await;
    sch.every(1).await.hour().await.todo(cooking).await;
    sch.every(1)
        .await
        .day(0, 59, 59)
        .await
        .todo(exercising)
        .await;
    sch.every(1)
        .await
        .week(1, 0, 59, 59)
        .await
        .todo(working)
        .await;

    assert_eq!(sch.time_unit_with_name("learn_rust").await.unwrap(), 0);
    assert_eq!(sch.time_unit_with_name("sing").await.unwrap(), 1);
    assert_eq!(sch.time_unit_with_name("cooking").await.unwrap(), 2);
    assert_eq!(sch.time_unit_with_name("exercising").await.unwrap(), 3);
    assert_eq!(sch.time_unit_with_name("working").await.unwrap(), 4);
}

#[tokio::test]
async fn test_at_time_unit() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.at().await.day(0, 59, 59).await.todo(learn_rust).await;
    sch.at().await.week(1, 0, 59, 59).await.todo(sing).await;
    assert_eq!(sch.time_unit_with_name("learn_rust").await.unwrap(), 3);
    assert_eq!(sch.time_unit_with_name("sing").await.unwrap(), 4);
}

#[tokio::test]
async fn test_next_run_with_name() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now() + chrono::Duration::hours(1);
    sch.every(1).await.hour().await.todo(learn_rust).await;
    let next_run_time = sch.next_run_with_name("learn_rust").await.unwrap();
    assert_eq!(next_run_time, now.timestamp());
}

#[tokio::test]
async fn test_last_run() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now();
    sch.every(1)
        .await
        .hour()
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(true, None, None)
        .await
        .todo(learn_rust)
        .await;
    let last_run_time = sch.last_run_with_name("learn_rust").await.unwrap();
    assert_eq!(last_run_time, now.timestamp());
}

#[tokio::test]
async fn test_every_day_job() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now();
    sch.every(2)
        .await
        .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;
    let learn_rust_time = sch.next_run_with_name("learn_rust").await.unwrap();
    assert_eq!(
        learn_rust_time,
        (now + chrono::Duration::days(2)).timestamp()
    );

    let after = now + chrono::Duration::minutes(10);
    sch.every(1)
        .await
        .day(
            after.hour() as i64,
            after.minute() as i64,
            after.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;

    let sing_time = sch.next_run_with_name("sing").await.unwrap();
    assert_eq!(after.timestamp(), sing_time);
}

#[tokio::test]
async fn test_every_week_job() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now();
    sch.every(1)
        .await
        .week(
            now.weekday().number_from_monday() as i64,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;
    let learn_rust_time = sch.next_run_with_name("learn_rust").await.unwrap();
    let expect_next_run = now + chrono::Duration::weeks(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    sch.every(1)
        .await
        .week(
            (now.weekday().number_from_monday() as i64 + 1) % 7,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;
    let sing_time = sch.next_run_with_name("sing").await.unwrap();
    assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
}

#[tokio::test]
async fn test_at_week_job() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now();
    sch.at()
        .await
        .week(
            now.weekday().number_from_monday() as i64,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").await.unwrap();
    let expect_next_run = now + chrono::Duration::weeks(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    sch.at()
        .await
        .week(
            (now.weekday().number_from_monday() as i64 + 1) % 7,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;

    let sing_time = sch.next_run_with_name("sing").await.unwrap();
    assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
}

#[tokio::test]
async fn test_at_day_job() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    let now = Local::now();
    sch.at()
        .await
        .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").await.unwrap();
    let expect_next_run = now + chrono::Duration::days(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    sch.at()
        .await
        .day(
            now.hour() as i64 + 1,
            now.minute() as i64,
            now.second() as i64,
        )
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").await.unwrap();
    let sing_time = sch.next_run_with_name("sing").await.unwrap();
    assert_ne!(learn_rust_time, sing_time);
    assert_eq!(learn_rust_time, sing_time + 23 * 3600);
}

#[tokio::test]
async fn test_debug_scheduler() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.at()
        .await
        .day(0, 59, 59)
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(learn_rust)
        .await;

    sch.at()
        .await
        .day(1, 59, 59)
        .await
        .with_opts::<Box<dyn Fn(std::io::Error) + Send + Sync>>(false, None, None)
        .await
        .todo(sing)
        .await;
    println!("{:?}", sch);
}

#[tokio::test]
#[should_panic]
async fn test_is_at() {
    let mut sch = Scheduler::<(), RedisLockerOk>::new(1, 10);
    sch.at().await.second().await;

    sch.at().await.minute().await;
    sch.at().await.hour().await;
}

async fn panic_job() {
    let mut guard = LOCKED_FLAG.try_write().unwrap();
    *guard = !*guard;
}

#[tokio::test]
async fn test_start_with_unlock() {
    let mut sch = Scheduler::<(), LockedLocker>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts(false, Some(Box::new(err_callback_mock)), Some(LockedLocker))
        .await
        .todo_with_unlock(panic_job)
        .await;
    start_scheldure_with_cancel(sch, 3).await;
    let guard = LOCKED_FLAG.try_read().unwrap();
    assert_eq!(*guard, true);
}

#[tokio::test]
async fn test_start_without_unlock() {
    let mut sch = Scheduler::<(), LockedLocker>::new(1, 10);
    sch.every(2)
        .await
        .second()
        .await
        .with_opts(true, Some(Box::new(err_callback_mock)), Some(LockedLocker))
        .await
        .todo(panic_job)
        .await;
    start_scheldure_with_cancel(sch, 2).await;
    let guard = UNLOCKED.try_read().unwrap();
    assert_eq!(*guard, false);
}
