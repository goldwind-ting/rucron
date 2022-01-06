pub mod error;
pub mod handler;
pub(crate) mod job;
pub mod locker;
pub mod metric;
pub mod scheduler;

#[macro_use]
extern crate lazy_static;
pub use crate::error::RucronError;
pub use crate::handler::{execute, ArgStorage, Executor, ParseArgs};
pub use crate::locker::Locker;
pub use crate::metric::Metric;
pub use crate::scheduler::{EmptyTask, Scheduler};

use crate::metric::NumberType;
use dashmap::DashMap;
use serde_json;
use std::sync::Arc;
use tokio::time::Duration;

lazy_static! {
    static ref DEFAULT_ZERO_CALL_INTERVAL: Duration = Duration::from_secs(1);
    static ref METRIC_STORAGE: DashMap<String, Metric> = DashMap::new();
}

pub fn get_metric_with_name(name: &str) -> String {
    let m = METRIC_STORAGE.get(name).unwrap();
    serde_json::to_string(&*m).unwrap()
}

pub(crate) fn unlock_and_record<L: Locker>(locker: L, key: &str, args: Arc<ArgStorage>) {
    match locker.unlock(key, args.clone()) {
        Ok(b) if !b => METRIC_STORAGE
            .get(key)
            .unwrap()
            .add_failure(NumberType::Unlock),
        Ok(_) => {}
        Err(e) => {
            log::error!("{}", e);
            METRIC_STORAGE
                .get(key)
                .unwrap()
                .add_failure(NumberType::Error)
        }
    };
}
