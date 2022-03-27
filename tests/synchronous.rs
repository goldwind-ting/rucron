use rucron::{
    scheduler::{EmptyTask, Scheduler},
    sync_execute,
};
use std::error::Error;

fn say_synchronous() -> Result<(), Box<dyn Error>> {
    println!("This is a synchronous task!");
    Ok(())
}

#[tokio::test]
async fn test_synronous_func() {
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .second()
        .todo(sync_execute(say_synchronous))
        .await;
    sch.start().await;
}
