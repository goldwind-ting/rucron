use crate::handler::ArgStorage;
use std::sync::Arc;

/// A trait which provides a distributed lock.
pub trait Locker {
    /// attempts to get the lock.
    fn lock(&self, key: &str, _storage: Arc<ArgStorage>) -> bool {
        println!("{}", key);
        true
    }
    /// attempts to release the lock.
    fn unlock(&self, key: &str, _storage: Arc<ArgStorage>) -> bool {
        println!("{}", key);
        true
    }
}

impl Locker for () {}
