use crate::{error::RucronError, handler::ArgStorage};
use std::sync::Arc;

/// A trait which provides a distributed lock.
pub trait Locker {
    /// attempts to get the lock.
    fn lock(&self, key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        log::info!("[INFO] Key: {}", key);
        Ok(true)
    }
    /// attempts to release the lock.
    fn unlock(&self, key: &str, _storage: Arc<ArgStorage>) -> Result<bool, RucronError> {
        log::info!("[INFO] Key: {}", key);
        Ok(true)
    }
}

impl Locker for () {}
