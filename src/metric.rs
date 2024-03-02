use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) enum MetricType {
    Error,
    Unlock,
    Lock,
}

/// `Metric` record various metrics generated in the runtime.  
/// - `n_scheduled` shows total number of times the job has been scheduled.
/// - `n_success` shows total successfully run times.
/// - `t_total_elapsed` shows total running time .
/// - `t_maximum_elapsed` shows maximum running time.
/// - `t_minimum_elapsed` shows minimum running time.
/// - `t_average_elapsed` shows average running time.
/// - `n_error` shows total numbers of `error`.
/// - `n_failure_of_unlock` shows total numbers.
/// - `n_failure_of_lock` shows total numbers.
///
#[derive(Debug, Serialize)]
pub struct Metric {
    pub n_scheduled: AtomicUsize,
    pub n_success: AtomicUsize,
    pub t_total_elapsed: AtomicUsize,
    pub t_maximum_elapsed: AtomicUsize,
    pub t_minimum_elapsed: AtomicUsize,
    pub t_average_elapsed: AtomicUsize,
    pub n_error: AtomicUsize,
    pub n_failure_of_unlock: AtomicUsize,
    pub n_failure_of_lock: AtomicUsize,
}

impl Default for Metric {
    /// Create a Metric by default value.
    fn default() -> Self {
        Self {
            n_scheduled: AtomicUsize::default(),
            n_success: AtomicUsize::default(),
            t_total_elapsed: AtomicUsize::default(),
            t_maximum_elapsed: AtomicUsize::default(),
            t_minimum_elapsed: AtomicUsize::default(),
            t_average_elapsed: AtomicUsize::default(),
            n_error: AtomicUsize::default(),
            n_failure_of_unlock: AtomicUsize::default(),
            n_failure_of_lock: AtomicUsize::default(),
        }
    }
}

impl Metric {
    /// Update `t_maximum_elapsed`, `t_minimum_elapsed`, `n_success`, `t_total_elapsed`, `t_average_elapsed`.
    pub(crate) fn swap_time_and_add_runs(&self, time: usize) {
        let previous_max = self.t_maximum_elapsed.fetch_max(time, Ordering::SeqCst);
        if previous_max == 0 {
            self.t_minimum_elapsed.swap(time, Ordering::SeqCst);
        } else {
            self.t_minimum_elapsed.fetch_min(time, Ordering::SeqCst);
        }
        let previous_runs = self.n_success.fetch_add(1, Ordering::SeqCst);
        let previous_total_time = self.t_total_elapsed.fetch_add(time, Ordering::SeqCst);
        self.t_average_elapsed.swap(
            (previous_total_time + time) / (previous_runs + 1),
            Ordering::SeqCst,
        );
    }

    /// Update `n_error`, `n_failure_of_unlock`, `n_failure_of_lock`.
    pub(crate) fn add_failure(&self, metric_type: MetricType) {
        match metric_type {
            MetricType::Error => self.n_error.fetch_add(1, Ordering::SeqCst),
            MetricType::Unlock => self.n_failure_of_unlock.fetch_add(1, Ordering::SeqCst),
            MetricType::Lock => self.n_failure_of_lock.fetch_add(1, Ordering::SeqCst),
        };
    }
}
