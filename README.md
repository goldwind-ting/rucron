## Rucron: A Pure Rust Implementation Job Scheduling Package.
<a href="https://crates.io/crates/rucron">
    <img src="https://img.shields.io/crates/v/rucron.svg?style=flat-square"
    alt="Crates.io version" />
  </a>  
<!-- Docs -->
  <a href="https://docs.rs/rucron/latest/rucron/">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square"
      alt="docs.rs docs" />
  </a>
  <!-- Downloads -->
  <a href="https://crates.io/crates/rucron">
    <img src="https://img.shields.io/crates/d/rucron.svg?style=flat-square"
      alt="Download" />
  </a>  

Rucron is a lightweight job scheduler, it is similar to [gocron](https://github.com/go-co-op/gocron) or linux crontab, and it is very easy to use.
But now it only supports asynchronous job by the runtime provided by `tokio`.

## Usage
Add this to your Cargo.toml

```toml
[dependencies]
rucron = "*"
```

## Quick start:

```rust
use rucron::{execute, EmptyTask, Metric, Scheduler};
use std::{error::Error, sync::atomic::Ordering};
use chrono::{Local, DateTime, Duration};


async fn foo() -> Result<(), Box<dyn Error>>{
    println!("foo");
    Ok(())
}

async fn bar() -> Result<(), Box<dyn Error>>{
    println!("bar");
    Ok(())
}

async fn ping() -> Result<(), Box<dyn Error>>{
    println!("ping");
    Ok(())
}

fn once(m: &Metric, last: &DateTime<Local>) -> Duration {
    let n = m.n_scheduled.load(Ordering::Relaxed);
    if n < 1 {
        Duration::seconds(2)
    } else if n == 1 {
        Duration::seconds(last.timestamp() * 2)
    } else {
        Duration::seconds(0)
   }
}

#[tokio::main]
async fn main(){
    // Create a scheduler with 10 capacity, it will checkout all runnable jobs every second
    let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    let sch = sch
                // Scheduler runs foo every second.
                .every(1).second().todo(execute(foo)).await
                // Scheduler runs bar every monday at 9 am.
                .at().week(1, 9, 0, 0).todo(execute(bar)).await
                // Scheduler runs ping only once.
                .by(once).todo(execute(ping)).await;
    // Start running all jobs.
    sch.start().await;
}
```
Schedule parameterized job:

```rust
use rucron::{execute, ArgStorage, EmptyTask, ParseArgs, Scheduler};
use std::error::Error;
use async_trait::async_trait;


#[derive(Clone)]
struct Person {
    age: i32,
}

#[async_trait]
impl ParseArgs for Person {
    type Err = std::io::Error;
    fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
        return Ok(args.get::<Person>().unwrap().clone());
    }
}

async fn is_eight_years_old(p: Person) -> Result<(), Box<dyn Error>> {
    if p.age == 8 {
        println!("I am eight years old!");
    } else {
        println!("Oops!");
    };
    Ok(())
}

#[tokio::main]
async fn main() {
    let child = Person { age: 8 };
    // Storage stores all arguments.
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch.every(2).second().todo(execute(is_eight_years_old)).await;
    sch.start().await;
}
```

You could also schedule blocking or CPU-bound tasks.

```rust
use rucron::{sync_execute, ArgStorage, EmptyTask, ParseArgs, Scheduler};
use std::error::Error;


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

fn sync_set_age(p: Person) -> Result<(), Box<dyn Error>> {
    if p.age == 8 {
        println!("I am eight years old!");
    };
    Ok(())
}

#[tokio::test]
async fn test_sync_set_age() {
    let child = Person { age: 8 };
    let mut arg = ArgStorage::new();
    arg.insert(child);
    let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
    sch.set_arg_storage(arg);
    let sch = sch.every(2).second().todo(sync_execute(sync_set_age)).await;
    sch.start().await;
}
```

If you want to schedule jobs with distributed locks, please see [examples] directory.

## To Do List
- [ ] Support smol and async-std Ecology.
- [ ] Enable change timezone.
- [x] Support synchronous job.
- [ ] Improve readability of annotation.
- [ ] Add benchmark.

## License
Rucron is licensed under the [MIT license](https://opensource.org/licenses/MIT).

## Contributing

Contributions are welcome. Unless you explicitly state otherwise, 
any contribution intentionally submitted for inclusion in the work by you, as defined in the MIT license, shall be dual licensed as above, 
without any additional terms or conditions.
