use std::time::Duration;
#[cfg(feature = "tokio")]
pub use tokio::sync::mpsc::{self, Receiver, Sender};
#[cfg(feature = "tokio")]
pub use tokio::sync::RwLock;

#[cfg(feature = "smol")]
pub use async_lock::RwLock;
#[cfg(feature = "smol")]
use smol::stream::StreamExt;

#[cfg(feature = "smol")]
pub use async_channel::Sender;

#[cfg(feature = "tokio")]
pub fn spawn<T>(future: T)
where
    T: core::future::Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(feature = "tokio")]
pub fn wait<T>(future: T) -> tokio::task::JoinHandle<T::Output>
where
    T: core::future::Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(feature = "smol")]
pub fn spawn<T>(future: impl std::future::Future<Output = T> + Send + 'static)
where
    T: Send + 'static,
{
    smol::spawn(future).detach();
}

#[cfg(feature = "smol")]
pub fn wait<T>(future: impl std::future::Future<Output = T> + Send + 'static) -> smol::Task<T>
where
    T: Send + 'static,
{
    smol::spawn(future)
}

#[cfg(feature = "tokio")]
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    mpsc::channel(1)
}

#[cfg(feature = "smol")]
pub fn channel<T>() -> (async_channel::Sender<T>, async_channel::Receiver<T>) {
    async_channel::bounded(1)
}

#[cfg(feature = "tokio")]
pub fn sleep(dur: Duration) -> tokio::time::Sleep {
    tokio::time::sleep(dur)
}

#[cfg(feature = "smol")]
pub fn sleep(dur: Duration) -> smol::Timer {
    smol::Timer::after(dur)
}

pub struct Interval {
    #[cfg(feature = "tokio")]
    interval: tokio::time::Interval,
    #[cfg(feature = "smol")]
    interval: smol::Timer,
}

impl Interval {
    pub async fn new(dur: Duration) -> Self {
        #[cfg(feature = "tokio")]
        return Self {
            interval: {
                let mut inter = tokio::time::interval(dur);
                inter.tick().await;
                inter
            },
        };
        #[cfg(feature = "smol")]
        return Self {
            interval: smol::Timer::interval(dur),
        };
    }

    pub async fn tick(&mut self) {
        #[cfg(feature = "tokio")]
        self.interval.tick().await;
        #[cfg(feature = "smol")]
        self.interval.next().await;
    }
}
