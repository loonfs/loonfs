//! The instruments of an execution budget: its admitted totals, its permit
//! pools, and its read working memory.

use crate::metrics::{CounterHandle, GaugeHandle, MetricsRecorder};
use loonfs_core::cache::ReadWorkingMemoryObserver;
use loonfs_core::PermitPoolObserver;
use std::sync::Arc;

/// The instruments of one execution budget, registered when the budget is
/// built.
pub(crate) struct ExecutionBudgetInstruments {
    pub(crate) read_working_memory: Arc<dyn ReadWorkingMemoryObserver>,
    pub(crate) admission: AdmissionInstruments,
    pub(crate) reads: Arc<dyn PermitPoolObserver>,
    pub(crate) publications: Arc<dyn PermitPoolObserver>,
    pub(crate) folds: Arc<dyn PermitPoolObserver>,
    pub(crate) compactions: Arc<dyn PermitPoolObserver>,
    pub(crate) content_writes: Arc<dyn PermitPoolObserver>,
}

impl ExecutionBudgetInstruments {
    /// Registers every budget instrument now.
    pub(crate) fn new(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            read_working_memory: crate::metrics::read_working_memory::register(recorder),
            admission: AdmissionInstruments {
                requests: recorder.register_gauge(
                    "loonfs.execution_budget.admitted_requests",
                    "Publication requests admitted and not yet settled",
                    &[],
                ),
                bytes: recorder.register_gauge(
                    "loonfs.execution_budget.admitted_bytes",
                    "Estimated retained bytes of publication requests admitted and not yet settled",
                    &[],
                ),
                rejections: recorder.register_counter(
                    "loonfs.execution_budget.admission_rejections",
                    "Publication requests refused because an admitted total was full",
                    &[],
                ),
            },
            reads: PermitPoolGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.reads_running",
                    "Public reads holding a read permit",
                ),
                (
                    "loonfs.execution_budget.reads_waiting",
                    "Public reads waiting for a read permit",
                ),
            ),
            publications: PermitPoolGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.publications_running",
                    "Publication batches holding a publication permit",
                ),
                (
                    "loonfs.execution_budget.publications_waiting",
                    "Namespace publication workers waiting for a publication permit",
                ),
            ),
            folds: PermitPoolGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.folds_running",
                    "Folds, and operations that may fold first, holding a fold permit",
                ),
                (
                    "loonfs.execution_budget.folds_waiting",
                    "Folds, and operations that may fold first, waiting for a fold permit",
                ),
            ),
            compactions: PermitPoolGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.compactions_running",
                    "Metadata merges holding a compaction permit",
                ),
                (
                    "loonfs.execution_budget.compactions_waiting",
                    "Metadata merges waiting for a compaction permit",
                ),
            ),
            content_writes: PermitPoolGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.content_writes_running",
                    "Content object writes holding a content write permit",
                ),
                (
                    "loonfs.execution_budget.content_writes_waiting",
                    "Content object writes waiting for a content write permit",
                ),
            ),
        }
    }
}

/// The admitted totals and the total-level refusals of one execution budget.
pub(crate) struct AdmissionInstruments {
    requests: Arc<dyn GaugeHandle>,
    bytes: Arc<dyn GaugeHandle>,
    rejections: Arc<dyn CounterHandle>,
}

impl AdmissionInstruments {
    pub(crate) fn report(&self, requests: usize, bytes: usize) {
        self.requests
            .set(i64::try_from(requests).unwrap_or(i64::MAX));
        self.bytes.set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    pub(crate) fn rejection(&self) {
        self.rejections.increment(1);
    }
}

/// The running and waiting gauges of one kind of budget permit.
struct PermitPoolGauges {
    running: Arc<dyn GaugeHandle>,
    waiting: Arc<dyn GaugeHandle>,
}

impl PermitPoolGauges {
    /// Registers one pool's gauges now.
    fn register(
        recorder: &dyn MetricsRecorder,
        (running_name, running_description): (&'static str, &'static str),
        (waiting_name, waiting_description): (&'static str, &'static str),
    ) -> Arc<dyn PermitPoolObserver> {
        Arc::new(Self {
            running: recorder.register_gauge(running_name, running_description, &[]),
            waiting: recorder.register_gauge(waiting_name, waiting_description, &[]),
        })
    }
}

impl PermitPoolObserver for PermitPoolGauges {
    fn report(&self, running: usize, waiting: usize) {
        self.running.set(i64::try_from(running).unwrap_or(i64::MAX));
        self.waiting.set(i64::try_from(waiting).unwrap_or(i64::MAX));
    }
}
