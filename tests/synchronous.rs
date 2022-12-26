mod common;

#[cfg(feature = "tokio")]
#[cfg(test)]
mod sync_test {
    use crate::common::*;
    use rucron::{
        execute, get_metric_with_name, sync_execute, ArgStorage, EmptyTask, RucronError, Scheduler,
    };
    use std::error::Error;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_synronous_func() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(2)
            .second()
            .todo(sync_execute(sync_interval_job))
            .await;

        std::thread::spawn(move || sync_start_scheldure());
        tokio::spawn(async move { sch.start().await });
        tokio::time::sleep(tokio::time::Duration::from_secs(8)).await;
        let guard = SYNC_TIME_COUNAINER_SECOND_INTERVAL.read().unwrap();
        assert_eq!(guard.len(), 3);
        for i in 1..guard.len() {
            assert!((guard[i] - guard[i - 1]) as i32 == *SYNC_INTERVAL);
            assert!((guard[i] - guard[i - 1]) as i32 == *SYNC_INTERVAL);
        }
    }

    fn sync_learn_rust() -> Result<(), Box<dyn Error>> {
        std::thread::sleep(Duration::from_secs(1));
        println!("I am learning rust!");
        Ok(())
    }

    fn sync_sing() -> Result<(), Box<dyn Error>> {
        println!("I am sync_singing!");
        Ok(())
    }

    fn sync_cooking() -> Result<(), Box<dyn Error>> {
        std::thread::sleep(Duration::from_secs(2));
        println!("I am sync_cooking!");
        Ok(())
    }

    fn sync_error_job() -> Result<(), Box<dyn Error>> {
        Err(Box::new(RucronError::NotFound))
    }

    async fn async_foo() -> Result<(), Box<dyn Error>> {
        sleep(Duration::from_secs(2)).await;
        println!("foo");
        Ok(())
    }

    fn sync_set_age(p: Person) -> Result<(), Box<dyn Error>> {
        if p.age == 8 {
            let mut guard = SYNC_EIGHT.write().unwrap();
            *guard = 8;
        };
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_set_age() {
        let child = Person { age: 8 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.set_arg_storage(arg);
        let sch = sch.every(2).second().todo(sync_execute(sync_set_age)).await;

        tokio::spawn(async move { sch.start().await });
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        let guard = SYNC_EIGHT.read().unwrap();
        assert_eq!(*guard, 8);
    }

    #[tokio::test]
    async fn test_multiple_job() {
        let child = Person { age: 8 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.set_arg_storage(arg);
        let sch = sch
            .every(2)
            .second()
            .todo(sync_execute(sync_set_age))
            .await
            .every(4)
            .second()
            .todo(sync_execute(sync_learn_rust))
            .await
            .every(2)
            .second()
            .todo(sync_execute(sync_cooking))
            .await
            .every(3)
            .second()
            .todo(sync_execute(sync_error_job))
            .await
            .every(1)
            .second()
            .todo(sync_execute(sync_sing))
            .await;

        tokio::spawn(async move { sch.start().await });
        tokio::time::sleep(tokio::time::Duration::from_secs(11)).await;
        let guard = SYNC_EIGHT.read().unwrap();
        assert_eq!(*guard, 8);
        let js = get_metric_with_name("sync_error_job").unwrap();
        let m: MetricTest = serde_json::from_str(&js).unwrap();
        assert_eq!(4, m.n_scheduled);
        assert_eq!(0, m.n_success);
        assert_eq!(0, m.t_total_elapsed);
        assert_eq!(0, m.t_maximum_elapsed);
        assert_eq!(0, m.t_minimum_elapsed);
        assert_eq!(0, m.t_average_elapsed);
        assert_eq!(3, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        let lr = get_metric_with_name("sync_learn_rust").unwrap();
        let m: MetricTest = serde_json::from_str(&lr).unwrap();
        assert_eq!(3, m.n_scheduled);
        assert_eq!(2, m.n_success);
        assert_eq!(2, m.t_total_elapsed);
        assert_eq!(1, m.t_maximum_elapsed);
        assert_eq!(1, m.t_minimum_elapsed);
        assert_eq!(1, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        let cook = get_metric_with_name("sync_cooking").unwrap();
        let m: MetricTest = serde_json::from_str(&cook).unwrap();
        assert_eq!(6, m.n_scheduled);
        assert_eq!(4, m.n_success);
        assert_eq!(8, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        let sync_sing = get_metric_with_name("sync_sing").unwrap();
        let m: MetricTest = serde_json::from_str(&sync_sing).unwrap();
        assert_eq!(11, m.n_scheduled);
        assert_eq!(10, m.n_success);
    }

    #[tokio::test]
    async fn test_mix_async() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(2)
            .second()
            .todo(execute(async_foo))
            .await
            .every(4)
            .second()
            .todo(sync_execute(sync_cooking))
            .await;
        tokio::spawn(async move { sch.start().await });
        tokio::time::sleep(tokio::time::Duration::from_secs(11)).await;
        let foo = get_metric_with_name("async_foo").unwrap();
        let m: MetricTest = serde_json::from_str(&foo).unwrap();
        assert_eq!(6, m.n_scheduled);
        assert_eq!(4, m.n_success);
        assert_eq!(8, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        let sync_cooking = get_metric_with_name("sync_cooking").unwrap();
        let m: MetricTest = serde_json::from_str(&sync_cooking).unwrap();
        println!("{:?}", &m);
        assert_eq!(3, m.n_scheduled);
        assert_eq!(2, m.n_success);
        assert_eq!(4, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
    }
}
