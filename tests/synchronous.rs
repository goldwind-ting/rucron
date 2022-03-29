use rucron::{
    sync_execute, EmptyTask, Scheduler,ArgStorage,ParseArgs,RucronError, execute
};
use chrono::prelude::*;
use std::{error::Error, thread::sleep, time::Duration};
use std::sync::{mpsc::{SyncSender, sync_channel}, RwLock};
use lazy_static::lazy_static;

lazy_static! {
    static ref BROADCAST_CONNECT: RwLock<Option<SyncSender <bool>>> = RwLock::new(None);
    static ref EXECUTION_TIMES: i8 = 3;
    static ref INTERVAL: i32 = 2;
    static ref TIME_COUNAINER_SECOND_INTERVAL: RwLock<Vec<i64>> = RwLock::new(Vec::new());
    static ref EIGHT: RwLock<i8> = RwLock::new(0);
}


fn start_scheldure() {
    let (tx, rx) = sync_channel(1);
    *BROADCAST_CONNECT.write().unwrap() = Some(tx);
    
    loop {
        sleep(Duration::from_micros(100));
        if let Ok(_) = rx.recv() {
            break;
        };
    }
}


fn sync_interval_job() ->Result<(), Box<dyn Error>>{
    let now = Local::now().timestamp();
    {
        let mut guard = TIME_COUNAINER_SECOND_INTERVAL.write().unwrap();
        guard.push(now);
    }
    {
        let guard = TIME_COUNAINER_SECOND_INTERVAL.read().unwrap();
        if EXECUTION_TIMES.le(&(guard.len() as i8)) {
            let tx = BROADCAST_CONNECT.read().unwrap();
            let _ = tx.as_ref().unwrap().send(true).unwrap();
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_synronous_func() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .second()
        .todo(sync_execute(sync_interval_job))
        .await;
    
    std::thread::spawn(move||start_scheldure());
    tokio::spawn(async move{
        sch.start().await
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;
    let guard = TIME_COUNAINER_SECOND_INTERVAL.read().unwrap();
    assert_eq!(guard.len(), 3);
    for i in 1..guard.len() {
        assert!((guard[i] - guard[i - 1]) as i32 == *INTERVAL);
        assert!((guard[i] - guard[i - 1]) as i32 == *INTERVAL);
    }
}


fn learn_rust() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1));
    println!("I am learning rust!");
    Ok(())
}

fn sing() -> Result<(), Box<dyn Error>> {
    println!("I am singing!");
    Ok(())
}

fn cooking() -> Result<(), Box<dyn Error>> {
    sleep(Duration::from_secs(1));
    println!("I am cooking!");
    Ok(())
}

fn error_job() -> Result<(), Box<dyn Error>> {
    Err(Box::new(RucronError::NotFound))
}

async fn async_foo() -> Result<(), Box<dyn Error>> {
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    println!("foo");
    Ok(())
}


#[derive(Clone)]
struct Person {
    age: i32,
}

impl ParseArgs for Person {
    type Err = std::io::Error;
    fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
        return Ok(args.get::<Person>().unwrap().clone());
    }
}


fn sync_set_age(p: Person) ->Result<(), Box<dyn Error>>{
    if p.age == 8 {
        let mut guard = EIGHT.write().unwrap();
        *guard = 8;
    }
    Ok(())
}

#[tokio::test]
async fn test_sync_set_age(){
    let child = Person { age: 8 };
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch
        .every(2)
        .second()
        .todo(sync_execute(sync_set_age)).await;
    
    tokio::spawn(async move{
        sch.start().await
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let guard = EIGHT.read().unwrap();
    assert_eq!(*guard, 8);
}



#[tokio::test]
async fn test_multiple_job(){
    let child = Person { age: 8 };
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch
        .every(2)
        .second()
        .todo(sync_execute(sync_set_age)).await
        .every(5)
        .second()
        .todo(sync_execute(learn_rust)).await
        .every(4)
        .second()
        .todo(sync_execute(cooking)).await
        .every(3)
        .second()
        .todo(sync_execute(error_job)).await
        .every(1)
        .second()
        .todo(sync_execute(sing)).await;
    
    tokio::spawn(async move{
        sch.start().await
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    let guard = EIGHT.read().unwrap();
    assert_eq!(*guard, 8);
}


#[tokio::test]
async fn test_mix_async(){
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .second()
        .todo(execute(async_foo)).await
        .every(1)
        .second()
        .immediately_run()
        .todo(sync_execute(sing)).await;
    
    tokio::spawn(async move{
        sch.start().await
    });
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
}