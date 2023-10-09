mod common;

#[cfg(feature = "smol")]
#[cfg(test)]
mod smol_test {
    use crate::common::*;
    use chrono::{Datelike, Local, Timelike};
    use rucron::{channel, execute, get_metric_with_name, sleep, ArgStorage, EmptyTask, Scheduler};
    use serde_json;
    use std::time::Duration;

    #[test]
    fn test_smol_weekday_with_name() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        let sch = smol::block_on(async move {
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
            sch
        });
        assert_eq!(sch.weekday_with_name("learn_rust"), Some(1));
        assert_eq!(sch.weekday_with_name("cooking"), Some(3));
        assert_eq!(sch.weekday_with_name("employee"), Some(5));
        assert_eq!(sch.weekday_with_name("working"), Some(7));
        assert_eq!(sch.weekday_with_name("sing"), None);
    }

    #[test]
    fn test_smol_metric() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        smol::block_on(async move {
            let sch = sch.by(once).n_threads(3).todo(execute(counter)).await;
            start_scheldure_with_cancel(sch, 6).await;
            {
                let guard = EIGHT.read().await;
                assert_eq!(*guard, 3);
                // *guard = 0;
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
        });
    }

    #[test]
    fn test_smol_panic_job_with_arguments() {
        let child = Person { age: 2 };
        let mut arg = ArgStorage::new();
        arg.insert(child);
        let mut sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        smol::block_on(async move {
            sch.set_arg_storage(arg);
            let sch = sch
                .every(2)
                .second()
                .todo(execute(is_eight_years_old))
                .await;
            start_scheldure_with_cancel(sch, 3).await;
        });
        let guard = EIGHT.try_read().unwrap();
        assert_eq!(*guard, 8);
    }

    #[test]
    fn test_start_without_unlock() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        smol::block_on(async move {
            let sch = sch.every(2).second().todo(execute(panic_job)).await;
            start_scheldure_with_cancel(sch, 2).await;
        });
        let guard = UNLOCKED.try_read().unwrap();
        assert_eq!(*guard, false);
    }

    #[test]
    fn test_error_job() {
        let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
        smol::block_on(async move {
            let sch = sch.every(2).second().todo(execute(error_job)).await;
            start_scheldure_with_cancel(sch, 7).await;
        });
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

    #[test]
    fn test_smol_second_interval() {
        let sch = Scheduler::<EmptyTask, ()>::new(0, 10);
        smol::block_on(async move {
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
        })
    }

    #[test]
    fn test_locker_ok() {
        let locker = RedisLockerOk;
        let mut sch = Scheduler::new(1, 10);
        sch.set_locker(locker);
        smol::block_on(async move {
            let sch = sch.every(2).second().todo(execute(do_lockerok_job)).await;
            start_scheldure(sch, 2).await;

            let guard = TIME_COUNAINER_LOCKEROK.read().await;
            let leng = guard.len();
            assert_eq!(guard.len(), 3);
            for i in 1..leng {
                assert!((guard[i] - guard[i - 1]) as i32 >= *INTERVAL);
                assert!((guard[i] - guard[i - 1]) as i32 <= *INTERVAL + 1);
            }
        });
    }

    #[test]
    fn test_immediately_run() {
        smol::block_on(async {
            let (tx, rx) = channel();
            *BROADCAST_CONNECT.write().await = Some(tx);
            let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
            sch.every(2)
                .second()
                .immediately_run()
                .todo(execute(immediately_job))
                .await;
            loop {
                sleep(Duration::from_micros(10)).await;
                if let Ok(_) = rx.recv().await {
                    break;
                };
            }

            let guard = TIME_COUNAINER_IMMEDIATIALY.read().await;
            assert_eq!(guard.len(), 1);
            assert_eq!(0, guard[0] - Local::now().timestamp());
        });
    }

    #[test]
    fn test_minute_interval() {
        smol::block_on(async {
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
        })
    }

    #[test]
    fn test_next_run_with_name() {
        smol::block_on(async {
            let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
            let now = Local::now() + chrono::Duration::hours(1);
            let sch = sch.every(1).hour().todo(execute(learn_rust)).await;
            let next_run_time = sch.next_run_with_name("learn_rust").unwrap();
            assert_eq!(next_run_time, now.timestamp());
        })
    }

    #[test]
    fn test_at_day_job() {
        smol::block_on(async {
            let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
            let now = chrono::Local::now();
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
        })
    }

    #[test]
    fn test_every_week_job() {
        smol::block_on(async {
            let sch = Scheduler::<EmptyTask, ()>::new(1, 10);
            let now = chrono::Local::now();
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
        })
    }
}
