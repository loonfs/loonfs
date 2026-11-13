//! The instruments of an execution budget: its admitted totals, its
//! permits, and its read working memory.

use crate::metrics::{CounterHandle, GaugeHandle, MetricsRecorder};
use loonfs_core::cache::ReadWorkingMemoryObserver;
use loonfs_core::PermitObserver;
use std::sync::Arc;

/// The instruments of one execution budget, registered when the budget is
/// built.
pub(crate) struct ExecutionBudgetInstruments {
    pub(crate) read_working_memory: Arc<dyn ReadWorkingMemoryObserver>,
    pub(crate) admission: AdmissionInstruments,
    pub(crate) reads: Arc<dyn PermitObserver>,
    pub(crate) publications: Arc<dyn PermitObserver>,
    pub(crate) folds: Arc<dyn PermitObserver>,
    pub(crate) compactions: Arc<dyn PermitObserver>,
    pub(crate) content_writes: Arc<dyn PermitObserver>,
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
                merges: recorder.register_gauge(
                    "loonfs.execution_budget.admitted_merges",
                    "Merges admitted and not yet settled",
                    &[],
                ),
                merge_bytes: recorder.register_gauge(
                    "loonfs.execution_budget.admitted_merge_bytes",
                    "Bytes of the merge allowance that admitted merges reserve",
                    &[],
                ),
                rejections: recorder.register_counter(
                    "loonfs.execution_budget.admission_rejections",
                    "Publication requests refused because an admitted total was full",
                    &[],
                ),
            },
            reads: PermitGauges::register(
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
            publications: PermitGauges::register(
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
            folds: PermitGauges::register(
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
            compactions: PermitGauges::register(
                recorder,
                (
                    "loonfs.execution_budget.compactions_running",
                    "Metadata compactions holding a compaction permit",
                ),
                (
                    "loonfs.execution_budget.compactions_waiting",
                    "Metadata compactions waiting for a compaction permit",
                ),
            ),
            content_writes: PermitGauges::register(
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
    merges: Arc<dyn GaugeHandle>,
    merge_bytes: Arc<dyn GaugeHandle>,
    rejections: Arc<dyn CounterHandle>,
}

impl AdmissionInstruments {
    pub(crate) fn report(&self, requests: usize, bytes: usize, merges: usize, merge_bytes: usize) {
        self.requests
            .set(i64::try_from(requests).unwrap_or(i64::MAX));
        self.bytes.set(i64::try_from(bytes).unwrap_or(i64::MAX));
        self.merges.set(i64::try_from(merges).unwrap_or(i64::MAX));
        self.merge_bytes
            .set(i64::try_from(merge_bytes).unwrap_or(i64::MAX));
    }

    pub(crate) fn rejection(&self) {
        self.rejections.increment(1);
    }
}

/// The running and waiting gauges of one kind of budget permit.
struct PermitGauges {
    running: Arc<dyn GaugeHandle>,
    waiting: Arc<dyn GaugeHandle>,
}

impl PermitGauges {
    /// Registers the gauges of one set of permits now.
    fn register(
        recorder: &dyn MetricsRecorder,
        (running_name, running_description): (&'static str, &'static str),
        (waiting_name, waiting_description): (&'static str, &'static str),
    ) -> Arc<dyn PermitObserver> {
        Arc::new(Self {
            running: recorder.register_gauge(running_name, running_description, &[]),
            waiting: recorder.register_gauge(waiting_name, waiting_description, &[]),
        })
    }
}

impl PermitObserver for PermitGauges {
    fn report(&self, running: usize, waiting: usize) {
        self.running.set(i64::try_from(running).unwrap_or(i64::MAX));
        self.waiting.set(i64::try_from(waiting).unwrap_or(i64::MAX));
    }
}
