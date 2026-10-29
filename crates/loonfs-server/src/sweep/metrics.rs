//! The metrics that a sweep reports.

use crate::sweep::retries::SweepCall;

use loonfs::metrics::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, RESULT_ERROR, RESULT_OK,
};
use std::sync::Arc;

const SWEEP_PASS_SECONDS_BOUNDARIES: &[f64] =
    &[1.0, 10.0, 60.0, 300.0, 900.0, 3_600.0, 14_400.0, 86_400.0];

/// The instruments one sweep reports, registered when it is built.
pub(super) struct SweepMetrics {
    pub(super) passes_ok: Arc<dyn CounterHandle>,
    pub(super) passes_failed: Arc<dyn CounterHandle>,
    pub(super) pass_seconds: Arc<dyn HistogramHandle>,
    pub(super) visit_failures: [Arc<dyn CounterHandle>; SweepCall::ALL.len()],
    pub(super) panics: Arc<dyn CounterHandle>,
    pub(super) namespaces: Arc<dyn GaugeHandle>,
}

impl SweepMetrics {
    pub(super) fn register(recorder: &dyn MetricsRecorder) -> Self {
        let passes = |result: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.sweep_passes",
                "Maintenance sweep passes, by whether the namespace listing finished",
                &[("result", result)],
            )
        };
        Self {
            passes_ok: passes(RESULT_OK),
            passes_failed: passes(RESULT_ERROR),
            pass_seconds: recorder.register_histogram(
                "loonfs.maintenance.sweep_pass_seconds",
                "Maintenance sweep pass duration in seconds",
                &[],
                SWEEP_PASS_SECONDS_BOUNDARIES,
            ),
            visit_failures: SweepCall::ALL.map(|call| {
                recorder.register_counter(
                    "loonfs.maintenance.sweep_visit_failures",
                    "Maintenance sweep calls that failed on one namespace, by call",
                    &[("call", call.as_str())],
                )
            }),
            panics: recorder.register_counter(
                "loonfs.maintenance.sweep_panics",
                "Maintenance sweep calls and idle-session closes that panicked",
                &[],
            ),
            namespaces: recorder.register_gauge(
                "loonfs.maintenance.sweep_namespaces",
                "Namespaces the last finished maintenance sweep pass listed",
                &[],
            ),
        }
    }
}
