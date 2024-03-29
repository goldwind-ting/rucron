use crate::async_rt::spawn;
use crate::{
    error::RucronError, locker::Locker, metric::MetricType, unlock_and_record, METRIC_STORAGE,
};

use async_trait::async_trait;
use chrono::{DateTime, Local};
use futures::future::Future;
use http::Extensions;
use std::{
    any::type_name,
    error::Error,
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    sync::Arc,
};

/// The trait is the task to be run in fact, the task must be asynchronous.
///
/// The function with no parameters and less than 16 parameters has implemented the trait.
///
/// The `Scheduler` will call this function when a job is `runnable`.
///
#[async_trait]
pub trait Executor<T>: Send + Sized + 'static {
    async fn call(self, args: &ArgStorage, name: String);
}

/// The trait is similar to `Executor`, the task must be synchronous.
pub trait SyncExecutor<T> {
    fn call(self, args: &ArgStorage, name: String);
}

fn handle_result(res: Result<(), Box<dyn Error>>, name: &str, start: DateTime<Local>) {
    res.map_err(|e| RucronError::RunTimeError(e.to_string()))
        .map_or_else(
            |e| {
                log::error!("{}", e);
                METRIC_STORAGE
                    .get(name)
                    .unwrap()
                    .add_failure(MetricType::Error);
            },
            |_| {
                METRIC_STORAGE.get(name).unwrap().swap_time_and_add_runs(
                    Local::now().signed_duration_since(start).num_seconds() as usize,
                );
            },
        );
}

#[async_trait]
impl<F, Fut> Executor<()> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), Box<dyn Error>>> + Send + 'static,
{
    async fn call(self, _args: &ArgStorage, name: String) {
        let start = Local::now();
        spawn(async move {
            handle_result(self().await, &name, start);
        });
    }
}

impl<Func> SyncExecutor<()> for Func
where
    Func: Fn() -> Result<(), Box<dyn Error>> + Send + Sync + 'static,
{
    fn call(self, _args: &ArgStorage, name: String) {
        let start = Local::now();
        rayon::spawn(move || {
            handle_result(self(), &name, start);
        });
    }
}

/// `Scheduler` mangages all jobs by this trait. When a job is runnable,
///
/// the `Schedluler` find recursively the job by name and parse arguments the job need from `args`.
#[async_trait]
pub trait JobHandler: Send + Sized + 'static {
    async fn call(self, args: Arc<ArgStorage>, name: String);
    fn name(&self) -> String;
}

/// Implement the trait to parse or get arguments from [`ArgStorage`].
///
/// The [`Scheduler`](super::Scheduler) will call `parse_args` and pass arguments to `job` when run job.
/// # Examples
///
/// ```
/// use rucron::{Scheduler, EmptyTask, execute, ArgStorage, ParseArgs};
/// use async_trait::async_trait;
/// use std::error::Error;
///
///
/// #[derive(Clone)]
/// struct Person {
///     age: i32,
/// }
///
/// #[async_trait]
/// impl ParseArgs for Person {
///     type Err = std::io::Error;
///     fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
///         return Ok(args.get::<Person>().unwrap().clone());
///     }
/// }
/// async fn say_age(p: Person) -> Result<(), Box<dyn Error>>  {
///     println!("I am {} years old", p.age);
///     Ok(())
/// }
///
/// #[tokio::main]
/// async fn main(){
///     let mut sch = Scheduler::<EmptyTask, ()>::new(2, 10);
///     let mut storage = ArgStorage::new();
///     storage.insert(Person { age: 7 });
///     sch.set_arg_storage(storage);
///     let sch = sch.every(2).second().immediately_run().todo(execute(say_age)).await;
///     assert!(sch.is_scheduled("say_age"));
/// }
/// ```
pub trait ParseArgs: Sized + Clone {
    type Err: Error;

    fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err>;
}

macro_rules! impl_executor {
    ( $($ty:ident),* $(,)? ) => {
        #[async_trait]
        #[allow(non_snake_case)]
        impl<F, Fut, $($ty,)*> Executor<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> Fut + Clone + Send + Sync + 'static,
            Fut: Future<Output = Result<(), Box<dyn Error>>> + Send,
            $($ty: ParseArgs + Send + 'static,)*
        {
            async fn call(self, args:&ArgStorage, name: String) {
                let start = Local::now();
                $(
                    let $ty = match $ty::parse_args(args) {
                        Ok(value) => value,
                        Err(e) => {
                            handle_result(Err(Box::new(e)), &name, start);
                            return;
                        },
                    };
                )*
                spawn(async move {
                    handle_result(self($($ty,)*).await, &name, start);
                });
            }
        }
    };
}

macro_rules! impl_sync_executor {
    ( $($ty:ident),* $(,)? ) => {
        #[allow(non_snake_case)]
        impl<F, $($ty,)*> SyncExecutor<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> Result<(), Box<dyn Error>> + Clone + Send + Sync + 'static,
            $($ty: ParseArgs + Send + 'static,)*
        {
            fn call(self, args:&ArgStorage, name: String) {
                let start = Local::now();
                $(
                    let $ty = match $ty::parse_args(args) {
                        Ok(value) => value,
                        Err(e) => {
                            handle_result(Err(Box::new(e)), &name, start);
                            return;
                        },
                    };
                )*
                rayon::spawn(move ||{
                    handle_result(self($($ty,)*), &name, start);
                });
            }
        }
    };
}

impl_executor!(T1);
impl_executor!(T1, T2);
impl_executor!(T1, T2, T3);
impl_executor!(T1, T2, T3, T4);
impl_executor!(T1, T2, T3, T4, T5);
impl_executor!(T1, T2, T3, T4, T5, T6);
impl_executor!(T1, T2, T3, T4, T5, T6, T7);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

impl_sync_executor!(T1);
impl_sync_executor!(T1, T2);
impl_sync_executor!(T1, T2, T3);
impl_sync_executor!(T1, T2, T3, T4);
impl_sync_executor!(T1, T2, T3, T4, T5);
impl_sync_executor!(T1, T2, T3, T4, T5, T6);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_sync_executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

/// `Task` stores runtime parameters of a job which name is `name`.
#[derive(Debug, Clone)]
pub struct Task<T, TH, L> {
    pub(crate) name: String,
    pub(crate) task: T,
    pub(crate) fallback: TH,
    pub(crate) locker: Option<L>,
    pub(crate) need_lock: bool,
    pub(crate) n_threads: u8,
}

#[async_trait]
impl<T, TH, L> JobHandler for Task<T, TH, L>
where
    T: JobHandler + Send + Sync + Clone,
    TH: JobHandler + Send + Sync + 'static,
    L: Locker + 'static + Send + Sync + Clone,
{
    async fn call(self, args: Arc<ArgStorage>, name: String) {
        if self.name == name {
            if self.need_lock && self.locker.is_some() {
                let locker = self.locker.clone();
                match locker {
                    Some(locker) => match locker.lock(&name[..], args.clone()) {
                        Ok(b) if b => {
                            log::debug!(
                                "[DEBUG] Spawns a new asynchronous task to run: {}",
                                &name[..]
                            );
                            JobHandler::call(self.task, args.clone(), name.clone()).await;
                            log::debug!("[DEBUG] Had finished running: {}", &name[..]);
                            unlock_and_record(locker, &name[..], args);
                        }
                        Ok(b) if !b => {
                            METRIC_STORAGE
                                .get(&self.name())
                                .unwrap()
                                .add_failure(MetricType::Lock);
                        }
                        Ok(_) => {
                            unreachable!("unreachable!")
                        }
                        Err(e) => {
                            log::error!("{}", e);
                            METRIC_STORAGE
                                .get(&self.name())
                                .unwrap()
                                .add_failure(MetricType::Error);
                        }
                    },
                    _ => {}
                };
            } else if !self.need_lock {
                log::debug!(
                    "[DEBUG] Spawns a new asynchronous task to run: {}",
                    &name[..]
                );
                for _ in (0..self.n_threads).into_iter() {
                    let name_copy = name.clone();
                    let task_copy = self.task.clone();
                    let args_copy = args.clone();
                    JobHandler::call(task_copy, args_copy, name_copy.clone()).await;
                    log::debug!("[DEBUG] Had finished running: {}", name_copy);
                }
            };
        } else {
            JobHandler::call(self.fallback, args, name).await;
        }
    }
    #[inline(always)]
    fn name(&self) -> String {
        self.name.clone()
    }
}

/// [`ExecutorWrapper`] wraps the `Executor` and stores it's name.
#[derive(Clone)]
pub struct ExecutorWrapper<E, T> {
    executor: E,
    executor_name: String,
    _marker: PhantomData<T>,
}

/// [`SyncExecutorWrapper`] wraps the `SyncExecutor` and stores it's name.
#[derive(Clone)]
pub struct SyncExecutorWrapper<E, T> {
    executor: E,
    executor_name: String,
    _marker: PhantomData<T>,
}

/// Create a `ExecutorWrapper` and add this job to the `Scheduler`.
///
/// - `executor` is the job to be run.
///
/// # Panics
///
/// Panics if cann't parse name of `E` by `type_name`.
///
/// # Examples
///
/// ```
/// use rucron::{execute, Scheduler, EmptyTask};
/// use std::error::Error;
/// use std::sync::Arc;
///
/// async fn foo() -> Result<(), Box<dyn Error>> {
///     println!("{}", "foo");
///     Ok(())
/// }
/// #[tokio::main]
/// async fn main(){
///     let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
///     sch.every(2).second().todo(execute(foo)).await;
/// }
/// ```
pub fn execute<E, T>(executor: E) -> ExecutorWrapper<E, T> {
    let tname = type_name::<E>();
    let tokens: Vec<&str> = tname.split("::").collect();
    let name = match (*tokens).get(tokens.len() - 1) {
        None => panic!("Invalid name: {:?}", tokens),
        Some(s) => (*s).into(),
    };
    ExecutorWrapper {
        executor,
        executor_name: name,
        _marker: PhantomData,
    }
}

impl<E, T> From<ExecutorWrapper<E, T>> for SyncExecutorWrapper<E, T> {
    fn from(executor: ExecutorWrapper<E, T>) -> Self {
        SyncExecutorWrapper {
            executor: executor.executor,
            executor_name: executor.executor_name,
            _marker: executor._marker,
        }
    }
}

/// Create a `SyncExecutorWrapper` and add this job to the `Scheduler`.
/// It is similar to `execute`, but the `executor` needs to be implemented `SyncExecutor`.
/// If you want to run expensive CPU-bound tasks, please utilize this method to add tasks.
/// In fact, it uses `rayon` to spawn thread to run the tasks.For more details see:
/// [rayon](https://github.com/rayon-rs/rayon) and [blocking task](https://ryhl.io/blog/async-what-is-blocking).
///
/// # Panics
///
/// Panics if cann't parse name of `E` by `type_name`.
///
/// # Examples
///
/// ```
/// use rucron::{sync_execute, Scheduler, EmptyTask};
/// use std::{error::Error, sync::Arc, thread::sleep, time::Duration};
///
///
/// fn foo() -> Result<(), Box<dyn Error>> {
///     sleep(Duration::from_secs(2));
///     println!("{}", "foo");
///     Ok(())
/// }
/// #[tokio::main]
/// async fn main(){
///     let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
///     sch.every(2).second().todo(sync_execute(foo)).await;
/// }
/// ```
pub fn sync_execute<E, T>(executor: E) -> SyncExecutorWrapper<E, T> {
    SyncExecutorWrapper::from(execute(executor))
}

#[async_trait]
impl<E, T> JobHandler for ExecutorWrapper<E, T>
where
    E: Executor<T> + Send + 'static + Sync + Clone,
    T: Send + 'static + Sync,
{
    async fn call(self, args: Arc<ArgStorage>, name: String) {
        let exe = self.executor.clone();
        Executor::call(exe, &*args, name).await;
    }

    #[inline(always)]
    fn name(&self) -> String {
        self.executor_name.clone()
    }
}

#[async_trait]
impl<E, T> JobHandler for SyncExecutorWrapper<E, T>
where
    E: SyncExecutor<T> + Send + 'static + Sync + Clone,
    T: Send + 'static + Sync,
{
    async fn call(self, args: Arc<ArgStorage>, name: String) {
        let exe = self.executor.clone();
        SyncExecutor::call(exe, &*args, name);
    }

    #[inline(always)]
    fn name(&self) -> String {
        self.executor_name.clone()
    }
}

/// The storage  stores all the arguments that `jobs` needed.
///
/// It uses the extensions to store arguments, see the [docs] for more details.
///
/// [docs]: https://docs.rs/http/latest/http/struct.Extensions.html
///
#[derive(Debug)]
pub struct ArgStorage(Extensions);

impl ArgStorage {
    pub fn new() -> Self {
        ArgStorage(Extensions::new())
    }
}

impl Deref for ArgStorage {
    type Target = Extensions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ArgStorage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
