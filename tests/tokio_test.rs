mod common;

#[cfg(feature = "tokio")]
#[cfg(test)]
mod tokio_test {
    use crate::common::*;
    use chrono::Duration as duration;
    use chrono::{Datelike, Local, Timelike};
    use rucron::{channel, sleep, spawn};
    use rucron::{execute, get_metric_with_name, ArgStorage, EmptyTask, Scheduler};
    use serde::Deserialize;
    use std::time::Duration;

    #[tokio::test]
    async fn test_second_interval() {
        let sch = Scheduler::<EmptyTask, ()>::new(0, 10);
        let sch = sch
            .every(*INTERVAL)
            .second()
            .todo(execute(do_second_interval_job))
            .await;
        start_scheldure(sch, 1).await;
        let guard = TIME_COUNAINER_SECOND_INTERVAL.read().await;
        let leng = guard.len();
        assert_eq!(guard.len(), 3);
        println!("guard: {:?}", guard);
        for i in 1..leng {
            assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
            assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
        }
    }

    #[tokio::test]
    async fn test_minute_interval() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(*INTERVAL / 2)
            .minute()
            .todo(execute(do_minute_interval_job))
            .await;
        start_scheldure(sch, 3).await;
        let guard = TIME_COUNAINER_MINUTE_INTERVAL.read().await;
        let leng = guard.len();
        assert_eq!(guard.len(), 3);
        for i in 1..leng {
            assert!((guard[i] - guard[i - 1]) as i32 >= 29 * *INTERVAL + 1);
            assert!((guard[i] - guard[i - 1]) as i32 <= 30 * *INTERVAL + 1);
        }
    }

    #[tokio::test]
    async fn test_idle_seconds() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(2)
            .second()
            .todo(execute(learn_rust))
            .await
            .every(8)
            .second()
            .todo(execute(sing))
            .await;

        assert!(sch.idle_seconds().is_some());
        let idle = sch.idle_seconds().unwrap();
        let now = Local::now().timestamp();
        assert_eq!(idle - now, 2);
        let sch = sch.every(1).second().todo(execute(sing)).await;
        let idle = sch.idle_seconds().unwrap();
        let now = Local::now().timestamp();
        assert_eq!(idle - now, 1);
    }

    #[tokio::test]
    async fn test_get_job_names() {
        let programmer = Person { age: 18 };
        let mut storage = ArgStorage::new();
        storage.insert(programmer);
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let mut sch = sch
            .every(2)
            .second()
            .todo(execute(learn_rust))
            .await
            .every(4)
            .second()
            .todo(execute(sing))
            .await
            .every(6)
            .second()
            .todo(execute(cooking))
            .await
            .every(8)
            .second()
            .todo(execute(employee))
            .await;
        sch.set_arg_storage(storage);
        let names = sch.get_job_names();
        assert_eq!(names, vec!["learn_rust", "sing", "cooking", "employee"]);
    }

    #[tokio::test]
    async fn test_is_scheduled() {
        let sch = Scheduler::new(2, 10);
        let sch = sch.every(2).second().todo(execute(learn_rust)).await;
        let sch = sch.every(3).second().todo(execute(cooking)).await;
        let mut sch = sch.every(3).second().todo(execute(employee)).await;
        sch.set_locker(());
        let mut storage = ArgStorage::new();
        storage.insert(Person { age: 7 });
        sch.set_arg_storage(storage);
        assert!(sch.is_scheduled("learn_rust"));
        assert!(sch.is_scheduled("cooking"));
        assert!(sch.is_scheduled("employee"));
        assert_eq!(
            vec!["learn_rust", "cooking", "employee"],
            sch.get_job_names()
        );
    }

    #[tokio::test]
    async fn test_length() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let mut sch = sch
            .every(2)
            .second()
            .todo(execute(learn_rust))
            .await
            .every(8)
            .second()
            .todo(execute(employee))
            .await;

        assert_eq!(sch.len(), 2);
        assert!(sch.is_scheduled("learn_rust"));
        assert!(sch.is_scheduled("employee"));
        assert!(!sch.is_scheduled("cooking"));

        sch.cancel_job("employee");
        assert!(!sch.is_scheduled("employee"));
    }

    #[tokio::test]
    async fn test_next_run() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.every(60).second().todo(execute(learn_rust)).await;

        let job = sch.next_run().unwrap();
        assert_eq!(job, String::from("learn_rust"));
        let sch = sch.every(4).second().todo(execute(sing)).await;
        let job = sch.next_run().unwrap();
        assert_eq!(job, String::from("sing"));
        let sch = sch.every(6).second().todo(execute(cooking)).await;

        let job = sch.next_run().unwrap();
        assert_eq!(job, String::from("sing"));

        let sch = sch.every(1).second().todo(execute(employee)).await;
        let job = sch.next_run().unwrap();
        assert_eq!(job, String::from("employee"));
    }

    #[tokio::test]
    async fn test_locker_ok() {
        let locker = RedisLockerOk;
        let mut sch = Scheduler::new(1, 10);
        sch.set_locker(locker);
        let sch = sch.every(2).second().todo(execute(do_lockerok_job)).await;
        start_scheldure(sch, 2).await;

        let guard = TIME_COUNAINER_LOCKEROK.read().await;
        let leng = guard.len();
        assert_eq!(guard.len(), 3);
        for i in 1..leng {
            assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
            assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
        }
    }

    #[tokio::test]
    async fn test_fail_to_lock() {
        let locker = RedisLockerFlase;
        let mut sch = Scheduler::new(1, 10);
        sch.set_locker(locker);
        let sch = sch
            .every(2)
            .second()
            .need_lock()
            .todo(execute(do_lockerok_job))
            .await;
        spawn(async move {
            sleep(Duration::from_secs(5)).await;
            let tx = BROADCAST_CONNECT.read().await;
            let _ = tx.as_ref().unwrap().send(3).await.unwrap();
        });
        start_scheldure(sch, 3).await;

        let guard = TIME_COUNAINER_LOCKEROK.read().await;

        assert_eq!(guard.len(), 0);
    }

    #[tokio::test]
    async fn test_immediately_run() {
        let (tx, mut rx) = channel();
        *BROADCAST_CONNECT.write().await = Some(tx);
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.every(2)
            .second()
            .immediately_run()
            .todo(execute(immediately_job))
            .await;
        #[cfg(feature = "tokio")]
        while let Some(v) = rx.recv().await {
            if v == 4 {
                break;
            }
        }
        let guard = TIME_COUNAINER_IMMEDIATIALY.read().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(0, guard[0] - Local::now().timestamp());
    }

    #[tokio::test]
    async fn test_every_time_unit() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(1)
            .second()
            .todo(execute(learn_rust))
            .await
            .every(1)
            .minute()
            .todo(execute(sing))
            .await
            .every(1)
            .hour()
            .todo(execute(cooking))
            .await
            .every(1)
            .day(0, 59, 59)
            .todo(execute(employee))
            .await
            .every(1)
            .week(1, 0, 59, 59)
            .todo(execute(working))
            .await;

        assert_eq!(sch.time_unit_with_name("learn_rust").unwrap(), 0);
        assert_eq!(sch.time_unit_with_name("sing").unwrap(), 1);
        assert_eq!(sch.time_unit_with_name("cooking").unwrap(), 2);
        assert_eq!(sch.time_unit_with_name("employee").unwrap(), 3);
        assert_eq!(sch.time_unit_with_name("working").unwrap(), 4);
    }

    #[tokio::test]
    async fn test_weekday_with_name() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .every(2)
            .week(1, 0, 59, 59)
            .todo(execute(learn_rust))
            .await
            .every(2)
            .week(3, 0, 59, 59)
            .todo(execute(cooking))
            .await
            .every(2)
            .week(5, 0, 59, 59)
            .todo(execute(employee))
            .await
            .every(2)
            .week(7, 0, 59, 59)
            .todo(execute(working))
            .await
            .every(1)
            .second()
            .todo(execute(sing))
            .await;
        assert_eq!(sch.weekday_with_name("learn_rust"), Some(1));
        assert_eq!(sch.weekday_with_name("cooking"), Some(3));
        assert_eq!(sch.weekday_with_name("employee"), Some(5));
        assert_eq!(sch.weekday_with_name("working"), Some(7));
        assert_eq!(sch.weekday_with_name("sing"), None);
    }

    #[tokio::test]
    async fn test_at_time_unit() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .at()
            .day(0, 59, 59)
            .todo(execute(learn_rust))
            .await
            .at()
            .week(1, 0, 59, 59)
            .todo(execute(sing))
            .await;
        assert_eq!(sch.time_unit_with_name("learn_rust").unwrap(), 3);
        assert_eq!(sch.time_unit_with_name("sing").unwrap(), 4);
    }

    #[tokio::test]
    async fn test_next_run_with_name() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now() + chrono::Duration::hours(1);
        let sch = sch.every(1).hour().todo(execute(learn_rust)).await;
        let next_run_time = sch.next_run_with_name("learn_rust").unwrap();
        assert_eq!(next_run_time, now.timestamp());
    }

    #[tokio::test]
    async fn test_last_run() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch.every(1).hour().todo(execute(learn_rust)).await;
        let last_run_time = sch.last_run_with_name("learn_rust").unwrap();
        assert_eq!(last_run_time, now.timestamp());
    }

    #[tokio::test]
    async fn test_every_day_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch
            .every(2)
            .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
            .todo(execute(learn_rust))
            .await;
        let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
        assert_eq!(
            learn_rust_time,
            (now + chrono::Duration::days(2)).timestamp()
        );

        let after = now + chrono::Duration::minutes(10);
        let sch = sch
            .every(1)
            .day(
                after.hour() as i64,
                after.minute() as i64,
                after.second() as i64,
            )
            .todo(execute(sing))
            .await;

        let sing_time = sch.next_run_with_name("sing").unwrap();
        assert_eq!(after.timestamp(), sing_time);
    }

    #[tokio::test]
    async fn test_every_week_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch
            .every(1)
            .week(
                now.weekday().number_from_monday() as i64,
                now.hour() as i64,
                now.minute() as i64,
                now.second() as i64,
            )
            .todo(execute(learn_rust))
            .await;
        let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
        let expect_next_run = now + chrono::Duration::weeks(1);
        assert_eq!(learn_rust_time, expect_next_run.timestamp());

        let mut week = now.weekday().number_from_monday() as i64;
        if week == 6 {
            week = 7
        } else {
            week = (week + 1) % 7
        }
        let sch = sch
            .every(1)
            .week(
                week,
                now.hour() as i64,
                now.minute() as i64,
                now.second() as i64,
            )
            .todo(execute(sing))
            .await;
        let sing_time = sch.next_run_with_name("sing").unwrap();
        assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
    }

    #[tokio::test]
    async fn test_at_week_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch
            .at()
            .week(
                now.weekday().number_from_monday() as i64,
                now.hour() as i64,
                now.minute() as i64,
                now.second() as i64,
            )
            .todo(execute(learn_rust))
            .await;

        let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
        let expect_next_run = now + chrono::Duration::weeks(1);
        assert_eq!(learn_rust_time, expect_next_run.timestamp());

        let mut week = now.weekday().number_from_monday() as i64;
        if week == 6 {
            week = 7
        } else {
            week = (week + 1) % 7
        }

        let sch = sch
            .at()
            .week(
                week,
                now.hour() as i64,
                now.minute() as i64,
                now.second() as i64,
            )
            .todo(execute(sing))
            .await;

        let sing_time = sch.next_run_with_name("sing").unwrap();
        assert_eq!(learn_rust_time, sing_time + 6 * 24 * 60 * 60);
    }

    #[tokio::test]
    async fn test_at_day_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch
            .at()
            .day(now.hour() as i64, now.minute() as i64, now.second() as i64)
            .todo(execute(learn_rust))
            .await;

        let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
        let expect_next_run = now + chrono::Duration::days(1);
        assert_eq!(learn_rust_time, expect_next_run.timestamp());

        let sch = sch
            .at()
            .day(
                now.hour() as i64 + 1,
                now.minute() as i64,
                now.second() as i64,
            )
            .todo(execute(sing))
            .await;

        let learn_rust_time = sch.next_run_with_name("learn_rust").unwrap();
        let sing_time = sch.next_run_with_name("sing").unwrap();
        assert_ne!(learn_rust_time, sing_time);
        assert_eq!(learn_rust_time, sing_time + 23 * 3600);
    }

    #[tokio::test]
    async fn test_debug_scheduler() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch
            .at()
            .day(0, 59, 59)
            .todo(execute(learn_rust))
            .await
            .at()
            .day(1, 59, 59)
            .todo(execute(sing))
            .await;
        println!("{:?}", sch);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_is_at() {
        let rl = RedisLockerOk;
        let mut sch = Scheduler::<EmptyTask, RedisLockerOk>::new(1, 10);
        sch.set_locker(rl);
        sch.at().second().at().minute().at().hour();
    }

    #[tokio::test]
    async fn test_start_without_unlock() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.every(2).second().todo(execute(panic_job)).await;
        start_scheldure_with_cancel(sch, 2).await;
        let guard = UNLOCKED.try_read().unwrap();
        assert_eq!(*guard, false);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_without_job_name() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.every(2).second();
        sch.start().await;
    }

    #[tokio::test]
    async fn test_job_with_arguments() {
        let child = Person { age: 8 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.set_arg_storage(arg);
        let sch = sch
            .every(2)
            .second()
            .todo(execute(is_eight_years_old))
            .await;
        start_scheldure_with_cancel(sch, 2).await;
        let guard = EIGHT.try_read().unwrap();
        assert_eq!(*guard, 0);
    }

    #[tokio::test]
    async fn test_panic_job_with_arguments() {
        let child = Person { age: 2 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.set_arg_storage(arg);
        let sch = sch
            .every(2)
            .second()
            .todo(execute(is_eight_years_old))
            .await;
        start_scheldure_with_cancel(sch, 3).await;
        {
            let guard = EIGHT.try_read().unwrap();
            assert_eq!(*guard, 8);
        }
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_by() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let now = Local::now();
        let sch = sch.by(once).todo(execute(counter)).await;
        assert_eq!(sch.time_unit_with_name("counter"), None);
        assert_eq!(sch.weekday_with_name("counter"), None);
        let next = now + duration::seconds(2);
        assert_eq!(sch.last_run_with_name("counter").unwrap(), now.timestamp());
        assert_eq!(sch.next_run_with_name("counter").unwrap(), next.timestamp());
        start_scheldure_with_cancel(sch, 6).await;
        {
            let guard = EIGHT.read().await;
            assert_eq!(*guard, 1);
        }
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_by_immediately_run() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.by(once).immediately_run().todo(execute(counter)).await;
        start_scheldure_with_cancel(sch, 6).await;
        {
            let guard = EIGHT.read().await;
            assert_eq!(*guard, 2);
        }
        let js = get_metric_with_name("counter").unwrap();
        let m: MetricTest = serde_json::from_str(&js).unwrap();
        assert_eq!(2, m.n_scheduled);
        assert_eq!(2, m.n_success);
        assert_eq!(4, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_by_need_locker() {
        let rl = RedisLockerOk;
        let mut sch = Scheduler::<EmptyTask, RedisLockerOk>::new(1, 10);
        sch.set_locker(rl);
        let sch = sch.by(once).need_lock().todo(execute(counter)).await;
        start_scheldure_with_cancel(sch, 6).await;
        {
            let guard = EIGHT.read().await;
            assert_eq!(*guard, 1);
        }
        let js = get_metric_with_name("counter").unwrap();
        let m: MetricTest = serde_json::from_str(&js).unwrap();
        assert_eq!(2, m.n_scheduled);
        assert_eq!(1, m.n_success);
        assert_eq!(2, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_by_with_args() {
        let child = Person { age: 2 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        sch.set_arg_storage(arg);
        let sch = sch.by(once).todo(execute(is_eight_years_old)).await;
        start_scheldure_with_cancel(sch, 3).await;
        {
            let guard = EIGHT.try_read().unwrap();
            assert_eq!(*guard, 8);
        }
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_n_threads() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.by(once).n_threads(3).todo(execute(counter)).await;
        start_scheldure_with_cancel(sch, 6).await;
        {
            let guard = EIGHT.read().await;
            assert_eq!(*guard, 3);
        }
        *EIGHT.write().await = 0;
    }

    #[tokio::test]
    async fn test_error_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.every(2).second().todo(execute(error_job)).await;
        start_scheldure_with_cancel(sch, 7).await;
        let js = get_metric_with_name("error_job").unwrap();
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
    }

    #[tokio::test]
    async fn test_record_locker_false() {
        let rlf = RedisLockerFlase;
        let mut sch = Scheduler::<EmptyTask, RedisLockerFlase>::new(1, 10);
        sch.set_locker(rlf);
        let sch = sch
            .every(2)
            .second()
            .need_lock()
            .todo(execute(error_job))
            .await;
        start_scheldure_with_cancel(sch, 7).await;
        let js = get_metric_with_name("error_job").unwrap();
        let m: MetricTest = serde_json::from_str(&js).unwrap();
        assert_eq!(4, m.n_scheduled);
        assert_eq!(0, m.n_success);
        assert_eq!(0, m.t_total_elapsed);
        assert_eq!(0, m.t_maximum_elapsed);
        assert_eq!(0, m.t_minimum_elapsed);
        assert_eq!(0, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(3, m.n_failure_of_lock);
    }

    #[derive(Deserialize, Debug)]
    struct MetricTest {
        n_scheduled: i8,
        n_success: i8,
        t_total_elapsed: i8,
        t_maximum_elapsed: i8,
        t_minimum_elapsed: i8,
        t_average_elapsed: i8,
        n_error: i8,
        n_failure_of_unlock: i8,
        n_failure_of_lock: i8,
    }

    #[tokio::test]
    async fn test_metric() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = sch.by(once).n_threads(3).todo(execute(counter)).await;
        start_scheldure_with_cancel(sch, 6).await;
        {
            let guard = EIGHT.read().await;
            assert_eq!(*guard, 3);
        }
        let js = get_metric_with_name("counter").unwrap();
        let m: MetricTest = serde_json::from_str(&js).unwrap();
        assert_eq!(2, m.n_scheduled);
        assert_eq!(3, m.n_success);
        assert_eq!(6, m.t_total_elapsed);
        assert_eq!(2, m.t_maximum_elapsed);
        assert_eq!(2, m.t_minimum_elapsed);
        assert_eq!(2, m.t_average_elapsed);
        assert_eq!(0, m.n_error);
        assert_eq!(0, m.n_failure_of_unlock);
        assert_eq!(0, m.n_failure_of_lock);
        *EIGHT.write().await = 0;
    }
}
