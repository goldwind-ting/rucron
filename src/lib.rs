pub mod error;
pub mod handler;
pub(crate) mod job;
pub mod locker;
pub mod metric;
pub mod scheduler;

#[macro_use]
extern crate lazy_static;
pub use crate::error::RucronError;
pub use crate::handler::{execute, ArgStorage, ParseArgs};
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

/// Get the `Metric` data and serialize it to `String`, return NotFound error if can not find the metric with `name`.
pub fn get_metric_with_name(name: &str) -> Result<String, RucronError> {
    let m = METRIC_STORAGE.get(name).ok_or(RucronError::NotFound)?;
    serde_json::to_string(&*m).map_err(|e| RucronError::SerdeError(e.to_string()))
}

pub(crate) fn unlock_and_record<L: Locker>(locker: L, key: &str, args: Arc<ArgStorage>) {
    match locker.unlock(key, args.clone()) {
        Ok(b) if !b => METRIC_STORAGE.get(key).map_or_else(
            || unreachable!("unreachable"),
            |m| m.add_failure(NumberType::Unlock),
        ),
        Ok(_) => {}
        Err(e) => {
            log::error!("{}", e);
            METRIC_STORAGE.get(key).map_or_else(
                || unreachable!("unreachable"),
                |m| m.add_failure(NumberType::Error),
            )
        }
    };
}
