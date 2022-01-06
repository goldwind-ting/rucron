use async_trait::async_trait;
use chrono::Duration as duration;
use chrono::{DateTime, Datelike, Local, Timelike};
use rucron::handler::JobHandler;
use rucron::{execute, ArgStorage, EmptyTask, Locker, Metric, ParseArgs, RucronError, Scheduler};
use std::{error::Error, sync::atomic::Ordering, sync::Arc};
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
    static ref EIGHT: RwLock<i8> = RwLock::new(0);
    static ref LOCKED_FLAG: RwLock<bool> = RwLock::new(false);
    static ref UNLOCKED: RwLock<bool> = RwLock::new(false);
}

macro_rules! interval_job {
    ($fn_name:ident, $t:expr, $times:expr) => {
        async fn $fn_name() -> Result<(), Box<dyn Error>> {
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
            Ok(())
        }
    };
}

#[derive(Clone)]
struct RedisLockerOk;

impl Locker for RedisLockerOk {
    fn lock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(true)
    }
    fn unlock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(true)
    }
}

struct LockedLocker;

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
struct RedisLockerFlase;

impl Locker for RedisLockerFlase {
    fn lock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(false)
    }
    fn unlock(&self, _key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        Ok(false)
    }
}

async fn learn_rust() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am learning rust!");
    Ok(())
}

async fn sing() -> Result<(), Box<dyn Error>> {
    println!("I am singing!");
    Ok(())
}

async fn cooking() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am cooking!");
    Ok(())
}

#[derive(Clone)]
struct Person {
    age: i32,
}

#[async_trait]
impl ParseArgs for Person {
    type Err = std::io::Error;
    async fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
        return Ok(args.get::<Person>().unwrap().clone());
    }
}

async fn employee(p: Person) -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1)).await;
    println!("I am {} years old", p.age);
    Ok(())
}

async fn working() -> Result<(), Box<dyn Error>> {
    println!("I am working!");
    Ok(())
}

async fn is_eight_years_old(p: Person) -> Result<(), Box<dyn Error>> {
    if p.age != 8 {
        let mut guard = EIGHT.write().await;
        *guard = 8;
    };
    Ok(())
}

async fn start_scheldure<
    T: JobHandler + 'static + Send + Sync + Clone,
    L: Locker + 'static + Send + Sync + Clone,
>(
    sch: Scheduler<T, L>,
) {
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

async fn start_scheldure_with_cancel<
    T: JobHandler + 'static + Send + Sync + Clone,
    L: Locker + 'static + Send + Sync + Clone,
>(
    sch: Scheduler<T, L>,
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
    let sch = Scheduler::<EmptyTask, ()>::new(0, 10);
    let sch = sch
        .every(*INTERVAL)
        .second()
        .todo(execute(do_second_interval_job))
        .await;
    start_scheldure(sch).await;
    let guard = TIME_COUNAINER_SECOND_INTERVAL.read().await;
    let leng = guard.len();
    assert_eq!(guard.len(), 3);
    println!("guard: {:?}", guard);
    for i in 1..leng {
        assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
        assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
    }
}

#[tokio::test]
async fn test_minute_interval() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(*INTERVAL / 2)
        .minute()
        .todo(execute(do_minute_interval_job))
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
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .second()
        .todo(execute(learn_rust))
        .await
        .every(8)
        .second()
        .todo(execute(sing))
        .await;

    assert!(sch.idle_seconds().is_some());
    let idle = sch.idle_seconds().unwrap();
    let now = Local::now().timestamp();
    assert_eq!(idle - now, 2);
    let sch = sch.every(1).second().todo(execute(sing)).await;
    let idle = sch.idle_seconds().unwrap();
    let now = Local::now().timestamp();
    assert_eq!(idle - now, 1);
}

#[tokio::test]
async fn test_get_job_names() {
    let programmer = Person { age: 18 };
    let mut storage = ArgStorage::new();
    storage.insert(programmer);
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let mut sch = sch
        .every(2)
        .second()
        .todo(execute(learn_rust))
        .await
        .every(4)
        .second()
        .todo(execute(sing))
        .await
        .every(6)
        .second()
        .todo(execute(cooking))
        .await
        .every(8)
        .second()
        .todo(execute(employee))
        .await;
    sch.set_arg_storage(storage);
    let names = sch.get_job_names();
    assert_eq!(names, vec!["learn_rust", "sing", "cooking", "employee"]);
}

#[tokio::test]
async fn test_is_scheduled() {
    let sch = Scheduler::new(2, 10);
    let sch = sch.every(2).second().todo(execute(learn_rust)).await;
    let sch = sch.every(3).second().todo(execute(cooking)).await;
    let mut sch = sch.every(3).second().todo(execute(employee)).await;
    sch.set_locker(());
    let mut storage = ArgStorage::new();
    storage.insert(Person { age: 7 });
    sch.set_arg_storage(storage);
    assert!(sch.is_scheduled("learn_rust"));
    assert!(sch.is_scheduled("cooking"));
    assert!(sch.is_scheduled("employee"));
    assert_eq!(
        vec!["learn_rust", "cooking", "employee"],
        sch.get_job_names()
    );
}

#[tokio::test]
async fn test_length() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let mut sch = sch
        .every(2)
        .second()
        .todo(execute(learn_rust))
        .await
        .every(8)
        .second()
        .todo(execute(employee))
        .await;

    assert_eq!(sch.len(), 2);
    assert!(sch.is_scheduled("learn_rust"));
    assert!(sch.is_scheduled("employee"));
    assert!(!sch.is_scheduled("cooking"));

    sch.cancel_job("employee");
    assert!(!sch.is_scheduled("employee"));
}

#[tokio::test]
async fn test_next_run() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch.every(60).second().todo(execute(learn_rust)).await;

    let job = sch.next_run().unwrap();
    assert_eq!(job, String::from("learn_rust"));
    let sch = sch.every(4).second().todo(execute(sing)).await;
    let job = sch.next_run().unwrap();
    assert_eq!(job, String::from("sing"));
    let sch = sch.every(6).second().todo(execute(cooking)).await;

    let job = sch.next_run().unwrap();
    assert_eq!(job, String::from("sing"));

    let sch = sch.every(1).second().todo(execute(employee)).await;
    let job = sch.next_run().unwrap();
    assert_eq!(job, String::from("employee"));
}

#[tokio::test]
async fn test_locker_ok() {
    let locker = RedisLockerOk;
    let mut sch = Scheduler::new(1, 10);
    sch.set_locker(locker);
    let sch = sch.every(2).second().todo(execute(do_lockerok_job)).await;
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
async fn test_fail_to_lock() {
    let locker = RedisLockerFlase;
    let mut sch = Scheduler::new(1, 10);
    sch.set_locker(locker);
    let sch = sch
        .every(2)
        .second()
        .need_lock()
        .todo(execute(do_lockerok_job))
        .await;
    tokio::spawn(async move {
        sleep(Duration::from_secs(5)).await;
        let tx = BROADCAST_CONNECT.read().await;
        let _ = tx.as_ref().unwrap().send(true).await.unwrap();
    });
    start_scheldure(sch).await;

    let guard = TIME_COUNAINER_LOCKEROK.read().await;

    assert_eq!(guard.len(), 0);
}

#[tokio::test]
async fn test_immediately_run() {
    let (tx, mut rx) = channel(1);
    *BROADCAST_CONNECT.write().await = Some(tx);
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.every(2)
        .second()
        .immediately_run()
        .todo(execute(immediately_job))
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
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(1)
        .second()
        .todo(execute(learn_rust))
        .await
        .every(1)
        .minute()
        .todo(execute(sing))
        .await
        .every(1)
        .hour()
        .todo(execute(cooking))
        .await
        .every(1)
        .day(0, 59, 59)
        .todo(execute(employee))
        .await
        .every(1)
        .week(1, 0, 59, 59)
        .todo(execute(working))
        .await;

    assert_eq!(sch.time_unit_with_name("learn_rust").unwrap(), 0);
    assert_eq!(sch.time_unit_with_name("sing").unwrap(), 1);
    assert_eq!(sch.time_unit_with_name("cooking").unwrap(), 2);
    assert_eq!(sch.time_unit_with_name("employee").unwrap(), 3);
    assert_eq!(sch.time_unit_with_name("working").unwrap(), 4);
}

#[tokio::test]
async fn test_weekday_with_name() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .week(1, 0, 59, 59)
        .todo(execute(learn_rust))
        .await
        .every(2)
        .week(3, 0, 59, 59)
        .todo(execute(cooking))
        .await
        .every(2)
        .week(5, 0, 59, 59)
        .todo(execute(employee))
        .await
        .every(2)
        .week(7, 0, 59, 59)
        .todo(execute(working))
        .await
        .every(1)
        .second()
        .todo(execute(sing))
        .await;
    assert_eq!(sch.weekday_with_name("learn_rust"), Some(1));
    assert_eq!(sch.weekday_with_name("cooking"), Some(3));
    assert_eq!(sch.weekday_with_name("employee"), Some(5));
    assert_eq!(sch.weekday_with_name("working"), Some(7));
    assert_eq!(sch.weekday_with_name("sing"), None);
}

#[tokio::test]
async fn test_at_time_unit() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .at()
        .day(0, 59, 59)
        .todo(execute(learn_rust))
        .await
        .at()
        .week(1, 0, 59, 59)
        .todo(execute(sing))
        .await;
    assert_eq!(sch.time_unit_with_name("learn_rust").unwrap(), 3);
    assert_eq!(sch.time_unit_with_name("sing").unwrap(), 4);
}

#[tokio::test]
async fn test_next_run_with_name() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now() + chrono::Duration::hours(1);
    let sch = sch.every(1).hour().todo(execute(learn_rust)).await;
    let next_run_time = sch.next_run_with_name("learn_rust").unwrap();
    assert_eq!(next_run_time, now.timestamp());
}

#[tokio::test]
async fn test_last_run() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now();
    let sch = sch.every(1).hour().todo(execute(learn_rust)).await;
    let last_run_time = sch.last_run_with_name("learn_rust").unwrap();
    assert_eq!(last_run_time, now.timestamp());
}

#[tokio::test]
async fn test_every_day_job() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now();
    let sch = sch
        .every(2)
        .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
        .todo(execute(learn_rust))
        .await;
    let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
    assert_eq!(
        learn_rust_time,
        (now + chrono::Duration::days(2)).timestamp()
    );

    let after = now + chrono::Duration::minutes(10);
    let sch = sch
        .every(1)
        .day(
            after.hour() as i64,
            after.minute() as i64,
            after.second() as i64,
        )
        .todo(execute(sing))
        .await;

    let sing_time = sch.next_run_with_name("sing").unwrap();
    assert_eq!(after.timestamp(), sing_time);
}

#[tokio::test]
async fn test_every_week_job() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now();
    let sch = sch
        .every(1)
        .week(
            now.weekday().number_from_monday() as i64,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .todo(execute(learn_rust))
        .await;
    let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
    let expect_next_run = now + chrono::Duration::weeks(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    let sch = sch
        .every(1)
        .week(
            (now.weekday().number_from_monday() as i64 + 1) % 7,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .todo(execute(sing))
        .await;
    let sing_time = sch.next_run_with_name("sing").unwrap();
    assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
}

#[tokio::test]
async fn test_at_week_job() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now();
    let sch = sch
        .at()
        .week(
            now.weekday().number_from_monday() as i64,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .todo(execute(learn_rust))
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
    let expect_next_run = now + chrono::Duration::weeks(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    let sch = sch
        .at()
        .week(
            (now.weekday().number_from_monday() as i64 + 1) % 7,
            now.hour() as i64,
            now.minute() as i64,
            now.second() as i64,
        )
        .todo(execute(sing))
        .await;

    let sing_time = sch.next_run_with_name("sing").unwrap();
    assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
}

#[tokio::test]
async fn test_at_day_job() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let now = Local::now();
    let sch = sch
        .at()
        .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
        .todo(execute(learn_rust))
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
    let expect_next_run = now + chrono::Duration::days(1);
    assert_eq!(learn_rust_time, expect_next_run.timestamp());

    let sch = sch
        .at()
        .day(
            now.hour() as i64 + 1,
            now.minute() as i64,
            now.second() as i64,
        )
        .todo(execute(sing))
        .await;

    let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
    let sing_time = sch.next_run_with_name("sing").unwrap();
    assert_ne!(learn_rust_time, sing_time);
    assert_eq!(learn_rust_time, sing_time + 23 * 3600);
}

#[tokio::test]
async fn test_debug_scheduler() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .at()
        .day(0, 59, 59)
        .todo(execute(learn_rust))
        .await
        .at()
        .day(1, 59, 59)
        .todo(execute(sing))
        .await;
    println!("{:?}", sch);
}

#[tokio::test]
#[should_panic]
async fn test_is_at() {
    let rl = RedisLockerOk;
    let mut sch = Scheduler::<EmptyTask, RedisLockerOk>::new(1, 10);
    sch.set_locker(rl);
    sch.at().second().at().minute().at().hour();
}

async fn panic_job() -> Result<(), Box<dyn Error>> {
    let mut guard = LOCKED_FLAG.try_write().unwrap();
    *guard = !*guard;
    Ok(())
}

#[tokio::test]
async fn test_start_without_unlock() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch.every(2).second().todo(execute(panic_job)).await;
    start_scheldure_with_cancel(sch, 2).await;
    let guard = UNLOCKED.try_read().unwrap();
    assert_eq!(*guard, false);
}

#[tokio::test]
async fn test_job_with_arguments() {
    let child = Person { age: 8 };
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch
        .every(2)
        .second()
        .todo(execute(is_eight_years_old))
        .await;
    start_scheldure_with_cancel(sch, 2).await;
}

#[tokio::test]
async fn test_panic_job_with_arguments() {
    let child = Person { age: 2 };
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch
        .every(2)
        .second()
        .todo(execute(is_eight_years_old))
        .await;
    start_scheldure_with_cancel(sch, 2).await;
    let guard = EIGHT.try_read().unwrap();
    assert_eq!(*guard, 8);
}

async fn counter() -> Result<(), Box<dyn Error>> {
    std::thread::sleep(std::time::Duration::from_secs(2));
    println!("counter");
    let mut guard = EIGHT.write().await;
    *guard += 1;
    Ok(())
}

fn once(m: &Metric, last: &DateTime<Local>) -> duration {
    let n = m.scheduled_numbers.load(Ordering::Relaxed);
    if n < 1 {
        duration::seconds(2)
    } else if n == 1 {
        duration::seconds(last.timestamp() * 2)
    } else {
        duration::seconds(0)
    }
}

#[tokio::test]
async fn test_by() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch.by(once).todo(execute(counter)).await;
    start_scheldure_with_cancel(sch, 6).await;
    let guard = EIGHT.read().await;
    assert_eq!(*guard, 1);
}

#[tokio::test]
async fn test_multiple_thread() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch.by(once).todo(execute(counter)).await;
    start_scheldure_with_cancel(sch, 6).await;
    let guard = EIGHT.read().await;
    assert_eq!(*guard, 3);
}

