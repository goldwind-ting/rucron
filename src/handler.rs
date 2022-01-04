use crate::locker::Locker;

use async_trait::async_trait;
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

#[async_trait]
pub trait Executor<T>: Send + Sized + 'static {
    async fn call(&self, args: &ArgStorage);
}

#[async_trait]
impl<F, Fut> Executor<()> for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    async fn call(&self, _args: &ArgStorage) {
        self().await;
    }
}

#[async_trait]
pub trait JobHandler: Send + Sized + 'static {
    async fn call(&self, args: Arc<ArgStorage>, name: String);
    fn name(&self) -> String;
}

/// Implement the trait to parse or get arguments from [`ArgStorage`]. The [`Scheduler`] will call `parse_args` when runs job
/// and pass arguments to `job`.
#[async_trait]
pub trait ParseArgs: Sized + Clone {
    type Err: Error;

    async fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err>;
}
macro_rules! impl_Executor {
    ( $($ty:ident),* $(,)? ) => {
        #[async_trait]
        #[allow(non_snake_case)]
        impl<F, Fut, $($ty,)*> Executor<($($ty,)*)> for F
        where
            F: Fn($($ty,)*) -> Fut + Clone + Send + Sync + 'static,
            Fut: Future<Output = ()> + Send,
            $($ty: ParseArgs + Send,)*
        {
            async fn call(&self, args:&ArgStorage){
                $(
                    let $ty = match $ty::parse_args(args).await {
                        Ok(value) => value,
                        Err(e) => {
                            log::error!("[ERROR] Cann't parse argument from ArgStorage, error: {}", e);
                            return
                        },
                    };
                )*
                self($($ty,)*).await;
            }
        }
    };
}

impl_Executor!(T1);
impl_Executor!(T1, T2);
impl_Executor!(T1, T2, T3);
impl_Executor!(T1, T2, T3, T4);
impl_Executor!(T1, T2, T3, T4, T5);
impl_Executor!(T1, T2, T3, T4, T5, T6);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_Executor!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

#[derive(Debug, Clone)]
pub struct Task<T, TH, L> {
    pub(crate) name: String,
    pub(crate) task: T,
    pub(crate) fallback: TH,
    pub(crate) locker: Option<L>,
    pub(crate) need_lock: bool,
}

#[async_trait]
impl<T, TH, L> JobHandler for Task<T, TH, L>
where
    T: JobHandler + Send + Sync + Clone,
    TH: JobHandler + Send + Sync + 'static,
    L: Locker + 'static + Send + Sync + Clone,
{
    async fn call(&self, args: Arc<ArgStorage>, name: String) {
        if self.name == name {
            let task = self.task.clone();
            if self.need_lock && self.locker.is_some() {
                let locker = self.locker.clone();
                match locker {
                    Some(locker) => {
                        if locker.lock(&name[..], args.clone()) {
                            log::debug!("[DEBUG] Spawns a new asynchronous task to run: {}", &name[..]);
                            tokio::spawn(async move {
                                JobHandler::call(&task, args.clone(), name.clone()).await;
                                log::debug!("[DEBUG] Had finished running: {}", &name[..]);
                                if !locker.unlock(&name[..], args) {
                                    log::warn!("[WARN] Cann't unlock: {}!", &name[..]);
                                };
                            });
                        } else {
                            log::warn!("[WARN] Cann't get lock: {}!", &name[..]);
                        }
                    }
                    _ => {}
                };
                {
                    // todo: metric
                };
            } else if !self.need_lock {
                log::debug!("[DEBUG] Spawns a new asynchronous task to run: {}", &name[..]);
                tokio::spawn(async move {
                    JobHandler::call(&task, args, name.clone()).await;
                    log::debug!("[DEBUG] Had finished running: {}", name);
                });
            } else {
                log::warn!("[WARN] Please config locker for {}!", self.name);
            }
            return;
        } else {
            JobHandler::call(&self.fallback, args, name).await;
        }
    }
    fn name(&self) -> String {
        return self.name.clone();
    }
}

#[derive(Clone)]
pub struct ExecutorWrapper<E, T> {
    executor: E,
    executor_name: String,
    _marker: PhantomData<T>,
}

pub fn execute<E, T>(executor: E) -> ExecutorWrapper<E, T>
where
    E: Executor<T>,
{
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

#[async_trait]
impl<E, T> JobHandler for ExecutorWrapper<E, T>
where
    E: Executor<T> + Send + 'static + Sync,
    T: Send + 'static + Sync,
{
    async fn call(&self, args: Arc<ArgStorage>, _name: String) {
        Executor::call(&self.executor, &*args).await;
    }

    fn name(&self) -> String {
        self.executor_name.clone()
    }
}

/// The storage of arguments, which stores all the arguments [`jobs`] needed.
/// 
/// # Examples
/// 
/// ```
/// use rucron::{Scheduler, EmptyTask, execute, ArgStorage, ParseArgs};
/// use async_trait::async_trait;
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
///     async fn parse_args(args: &ArgStorage) -> Result<Self, Self::Err> {
///         return Ok(args.get::<Person>().unwrap().clone());
///     }
/// }
/// async fn say_age(p: Person) {
///     println!("I am {} years old", p.age);
/// }
/// 
/// #[tokio::main]
/// async fn main(){
///     let sch = Scheduler::<EmptyTask, ()>::new(2, 10);
///     let sch = sch.every(2).second().immediately_run().todo(execute(say_age)).await;
///     assert!(sch.is_scheduled("say_age"));
/// }
/// ```
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
