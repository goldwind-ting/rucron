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

A job scheduling library writed by rust, which is similar to [gocron](https://github.com/go-co-op/gocron) or linux crontab.  
The crate currently only supports asynchronous task which uses `tokio` runtime.

## Usage
See the example in `examples` folder.

## Quickstart
```toml
[dependencies]
rucron = "0.1.1"
```

```rust
use rucron::locker::Locker;
use rucron::Scheduler;

async fn foo() {
    println!("foo");
}


#[tokio::main]
async fn main() {
    let mut sch = Scheduler::<(), ()>::new(1, 10);
    sch.every(10)
        .await
        .second()
        .await
        .todo(foo)
        .await;
    sch.start().await;
}
```

## License
Rucron is licensed under the [MIT license](https://opensource.org/licenses/MIT).

## Contributing

Contributions are welcome.
