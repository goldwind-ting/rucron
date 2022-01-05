use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) enum NumberType {
    Error,
    Unlock,
    Lock,
}

#[derive(Debug, Serialize)]
pub(crate) struct Metric {
    success_numbers: AtomicUsize,
    total_elapsed_time: AtomicUsize,
    maximum_elapsed_time: AtomicUsize,
    minimum_elapsed_time: AtomicUsize,
    average_elapsed_time: AtomicUsize,
    error_numbers: AtomicUsize,
    failure_of_unlock_numbers: AtomicUsize,
    failure_of_lock_numbers: AtomicUsize,
}

impl Default for Metric {
    fn default() -> Self {
        Self {
            success_numbers: AtomicUsize::default(),
            total_elapsed_time: AtomicUsize::default(),
            maximum_elapsed_time: AtomicUsize::default(),
            minimum_elapsed_time: AtomicUsize::default(),
            average_elapsed_time: AtomicUsize::default(),
            error_numbers: AtomicUsize::default(),
            failure_of_unlock_numbers: AtomicUsize::default(),
            failure_of_lock_numbers: AtomicUsize::default(),
        }
    }
}

impl Metric {
    pub(crate) fn swap_time_and_add_runs(&self, time: usize) {
        let previous_max = self.maximum_elapsed_time.fetch_max(time, Ordering::SeqCst);
        if previous_max == 0 {
            self.minimum_elapsed_time.swap(time, Ordering::SeqCst);
        } else {
            self.minimum_elapsed_time.fetch_min(time, Ordering::SeqCst);
        }
        let previous_runs = self.success_numbers.fetch_add(1, Ordering::SeqCst);
        let previous_total_time = self.total_elapsed_time.fetch_add(time, Ordering::SeqCst);
        self.average_elapsed_time.swap(
            (previous_total_time + time) / (previous_runs + 1),
            Ordering::SeqCst,
        );
    }

    pub(crate) fn add_failure(&self, number_type: NumberType) {
        match number_type {
            NumberType::Error => self.error_numbers.fetch_add(1, Ordering::SeqCst),
            NumberType::Unlock => self
                .failure_of_unlock_numbers
                .fetch_add(1, Ordering::SeqCst),
            NumberType::Lock => self.failure_of_lock_numbers.fetch_add(1, Ordering::SeqCst),
        };
    }
}

#[test]
fn test_metric() {
    let metric = Metric::default();
    metric.swap_time_and_add_runs(1641362330);
    metric.swap_time_and_add_runs(1641362328);
    metric.swap_time_and_add_runs(1641362326);
    metric.swap_time_and_add_runs(1241362326);
    metric.swap_time_and_add_runs(1741362326);
    metric.add_failure(NumberType::Error);
    metric.add_failure(NumberType::Unlock);
    println!(
        "{:?}, {:?}, {:?}, {:?}, {:?}, {:?}, {:?}",
        metric.error_numbers,
        metric.failure_of_unlock_numbers,
        metric.maximum_elapsed_time,
        metric.minimum_elapsed_time,
        metric.average_elapsed_time,
        metric.total_elapsed_time,
        metric.success_numbers
    );
}
