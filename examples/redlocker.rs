extern crate rucron;

use redis::{Client, Commands};
use rucron::{
    execute, get_metric_with_name, sleep, ArgStorage, EmptyTask, Locker, RucronError, Scheduler,
};
use std::{error::Error, sync::Arc};

/// Distributed locks with redis
#[derive(Clone)]
struct RedisLocker {
    client: Client,
}

impl RedisLocker {
    fn new() -> Self {
        Self {
            client: redis::Client::open("redis://127.0.0.1/").unwrap(),
        }
    }
}

impl Locker for RedisLocker {
    fn lock(&self, key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        let mut con = self.client.get_connection().unwrap();
        match con.set_nx::<&str, i8, bool>(key, 1) {
            Err(e) => Err(RucronError::LockError(e.to_string())),
            Ok(b) => Ok(b),
        }
    }
    fn unlock(&self, key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        let mut con = self.client.get_connection().unwrap();
        match con.del::<&str, bool>(key) {
            Err(e) => Err(RucronError::UnLockError(e.to_string())),
            Ok(b) => Ok(b),
        }
    }
}

fn gen_int() -> u64 {
    fastrand::u64(8..14)
}

async fn a_job() -> Result<(), Box<dyn Error>> {
    let sec = gen_int();
    sleep(std::time::Duration::from_secs(sec as u64)).await;
    println!("end job!");
    Ok(())
}

async fn record_metric() -> Result<(), Box<dyn Error>> {
    let m = get_metric_with_name("a_job").unwrap();
    println!("{}", m);
    Ok(())
}

#[tokio::main]
async fn main() {
    let rl = RedisLocker::new();
    let mut sch = Scheduler::<EmptyTask, RedisLocker>::new(1, 10);
    sch.set_locker(rl);
    let sch = sch
        .every(15)
        .second()
        .immediately_run()
        .need_lock()
        .todo(execute(a_job))
        .await
        .every(5)
        .second()
        .todo(execute(record_metric))
        .await;
    sch.start().await;
}
