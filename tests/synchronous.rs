use rucron::{scheduler::{Scheduler, EmptyTask}, execute};
use std::error::Error;


fn say_synchronous()->Result<(), Box<dyn Error>>{
    println!("This is a synchronous task!");
    Ok(())
}

#[tokio::test]
async fn test_synronous_func(){
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
        .every(2)
        .second()
        .todo(execute(say_synchronous)).await;
    sch.start().await;
}

// struct Unchecked;
// struct CheckCRC;

// pub trait Validator<T:?Sized> {
//     fn is_ok(_:&T)->bool { true }
// }

// impl<T> Validator<T> for Unchecked where T:CanDoSomething {}
// impl<T> Validator<T> for CheckCRC where T:CanDoSomethingElse {}

// trait CanDoSomething {}
// trait CanDoSomethingElse {}

// pub trait DoesSomething {
//     type Validator: Validator<Self>;
//     fn do_something(&self) {
//         assert!(Self::Validator::is_ok(self));
//     }
// }

// struct ExampleNoCRC;
// impl CanDoSomething for ExampleNoCRC {}
// impl DoesSomething for ExampleNoCRC { type Validator = Unchecked; }

// struct ExampleWithCRC;
// impl CanDoSomethingElse for ExampleWithCRC {}
// impl DoesSomething for ExampleWithCRC { type Validator = CheckCRC; }