//! The metrics that the scheduler reports.

use crate::scheduler::retries::MaintenanceCall;

use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use std::sync::Arc;

const SWEEP_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// The instruments that the scheduler reports, registered when it is built.
pub(super) struct SchedulerMetrics {
    pub(super) sweeps_ok: Arc<dyn CounterHandle>,
    pub(super) sweeps_failed: Arc<dyn CounterHandle>,
    pub(super) sweep_seconds: Arc<dyn HistogramHandle>,
    pub(super) call_failures: [Arc<dyn CounterHandle>; MaintenanceCall::ALL.len()],
    pub(super) panics: Arc<dyn CounterHandle>,
    pub(super) namespaces: Arc<dyn GaugeHandle>,
}

impl SchedulerMetrics {
    pub(super) fn register(recorder: &dyn MetricsRecorder) -> Self {
        let sweeps = |result: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.sweeps",
                "Maintenance sweeps, by whether the namespace listing finished",
                &[("result", result)],
            )
        };
        Self {
            sweeps_ok: sweeps(RESULT_OK),
            sweeps_failed: sweeps(RESULT_ERROR),
            sweep_seconds: recorder.register_histogram(
                "loonfs.maintenance.sweep_seconds",
                "Maintenance sweep duration in seconds",
                &[],
                SWEEP_SECONDS_BOUNDARIES,
            ),
            call_failures: MaintenanceCall::ALL.map(|call| {
                recorder.register_counter(
                    "loonfs.maintenance.sweep_failures",
                    "Maintenance calls that failed on one namespace, by call",
                    &[("call", call.as_str())],
                )
            }),
            panics: recorder.register_counter(
                "loonfs.maintenance.sweep_panics",
                "Maintenance calls and idle-session closes that panicked",
                &[],
            ),
            namespaces: recorder.register_gauge(
                "loonfs.maintenance.sweep_namespaces",
                "Namespaces that the last finished maintenance sweep listed",
                &[],
            ),
        }
    }
}
