extern crate rucron;

use redis::{Client, Commands};
use rucron::{
    handler::{execute, ArgStorage},
    locker::Locker,
    scheduler::Scheduler,
    EmptyTask,
};

use std::sync::Arc;
use tokio::sync::mpsc::channel;

/// Distributed lock implementation with
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
    fn lock(&self, key: &str, _storage: Arc<ArgStorage>) -> bool {
        let mut con = self.client.get_connection().unwrap();
        match con.set_nx::<&str, i8, bool>(key, 1) {
            Err(e) => {
                println!("{:?}", e);
                false
            }
            Ok(_) => true,
        }
    }
    fn unlock(&self, key: &str, _storage: Arc<ArgStorage>) -> bool {
        let mut con = self.client.get_connection().unwrap();
        match con.set_nx::<&str, i8, bool>(key, 1) {
            Err(e) => {
                println!("{:?}", e);
                false
            }
            Ok(_) => true,
        }
    }
}

async fn learn_rust() {
    let (tx, mut rx) = channel(1);
    println!("I am learning rust!");
    tokio::spawn(async move {
        std::thread::sleep(std::time::Duration::from_secs(10));
        tx.send(1).await.unwrap();
    });
    if let Some(v) = rx.recv().await {
        println!("end job! {}", v);
    };
}

#[tokio::main]
async fn main() {
    let rl = RedisLocker::new();
    let sch = Scheduler::<EmptyTask, RedisLocker>::new(1, 10);
    let mut sch = sch.every(10).second().todo(execute(learn_rust)).await;
    sch.set_locker(rl);
    // let storage = ArgStorage::new();
    // sch.set_arg_storage(storage);
    sch.start().await;
}
