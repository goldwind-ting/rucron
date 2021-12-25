use std::error;

/// A distributed lock.
pub trait Locker {
    type Error: error::Error + Send + Sync + 'static;
    fn lock(&self, key: &str) -> Result<bool, Self::Error> {
        println!("{}", key);
        Ok(true)
    }
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
