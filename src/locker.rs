use std::error;

/// A trait which provides a distributed lock.
pub trait Locker {
    type Error: error::Error + Send + Sync + 'static;
    /// attempts to get the lock.
    fn lock(&self, key: &str) -> Result<bool, Self::Error> {
        println!("{}", key);
        Ok(true)
    }
    /// attempts to release the lock.
    fn unlock(&self, key: &str) -> Result<bool, Self::Error> {
        println!("{}", key);
        Ok(true)
    }
}

impl Locker for () {
    type Error = std::io::Error;
    fn lock(&self, _key: &str) -> Result<bool, Self::Error> {
        panic!("please implement Locker mannually");
    }
    fn unlock(&self, _key: &str) -> Result<bool, Self::Error> {
        panic!("please implement Locker mannually");
    }
}
