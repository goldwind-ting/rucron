extern crate rucron;

use redis::{Client, Commands, RedisError};
use rucron::locker::Locker;
use rucron::Scheduler;
use std::fs::OpenOptions;
use std::io::prelude::*;
use tokio::sync::mpsc::channel;

/// Distributed lock implementation with
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
    type Error = RedisError;
    fn lock(&self, key: &str) -> Result<bool, Self::Error> {
        let mut con = self.client.get_connection()?;
        con.set_nx(key, 1)
    }
    fn unlock(&self, key: &str) -> Result<bool, Self::Error> {
        let mut con = self.client.get_connection()?;
        con.expire(key, 0)
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

fn err_callback(err: RedisError) {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("./error.log")
        .unwrap();

    if let Err(e) = writeln!(file, "{}", err.to_string()) {
        panic!("couldn't write to log {}", e.to_string())
    }
}

#[tokio::main]
async fn main() {
    let rl = RedisLocker::new();
    let mut sch = Scheduler::<(), RedisLocker>::new(1, 10);
    sch.every(10)
        .await
        .second()
        .await
        .with_opts(true, Some(Box::new(err_callback)), Some(rl))
        .await
        .todo(learn_rust)
        .await;
    sch.start().await;
}
