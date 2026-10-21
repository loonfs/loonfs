//! Runtime, execution budget, and object-store metric reporting.
//!
//! Each runtime has one [`RuntimeInstruments`] instance, and each execution
//! budget one [`ExecutionBudgetInstruments`]. Instruments with dynamic labels
//! are cached so each label set is registered once. Reporting returns
//! immediately when no recorder is configured.

use super::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, ObjectStoreMetricSample,
    ObjectStoreMetricsRecorder, SMALL_COUNT_BOUNDARIES,
};
use crate::metrics::{LATENCY_SECONDS_BOUNDARIES, RESULT_ERROR, RESULT_OK};
use crate::GcResponse;
use loonfs_core::CompactionUnitOutcome;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

trait MetricLabel: Copy + PartialEq + 'static {
    const VALUES: &'static [Self];

    fn as_str(self) -> &'static str;

    fn index(self) -> usize {
        Self::VALUES
            .iter()
            .position(|value| *value == self)
            .expect("`MetricLabel::VALUES` should contain every label")
    }
}

#[derive(Clone)]
struct LabeledCounters<E> {
    handles: Vec<Arc<dyn CounterHandle>>,
    marker: PhantomData<fn(E)>,
}

impl<E: MetricLabel> LabeledCounters<E> {
    fn register(
        recorder: &dyn MetricsRecorder,
        name: &'static str,
        description: &'static str,
        label_name: &'static str,
        common_labels: &[(&'static str, &'static str)],
    ) -> Self {
        let handles = E::VALUES
            .iter()
            .map(|value| {
                let mut labels = common_labels.to_vec();
                labels.push((label_name, value.as_str()));
                recorder.register_counter(name, description, &labels)
            })
            .collect();
        Self {
            handles,
            marker: PhantomData,
        }
    }

    fn get(&self, label: E) -> &Arc<dyn CounterHandle> {
        &self.handles[label.index()]
    }
}

/// The closed outcome vocabulary for a finished compaction unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    Completed,
    Failed,
    Abandoned,
    Cancelled,
    Fenced,
}

impl MetricLabel for CompactionOutcome {
    const VALUES: &'static [Self] = &[
        Self::Completed,
        Self::Failed,
        Self::Abandoned,
        Self::Cancelled,
        Self::Fenced,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
            Self::Cancelled => "cancelled",
            Self::Fenced => "fenced",
        }
    }
}

/// The closed outcome vocabulary for a publication delivered to its caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    Ok,
    Error,
}

impl PublishOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => RESULT_OK,
            Self::Error => RESULT_ERROR,
        }
    }
}

impl MetricLabel for PublishOutcome {
    const VALUES: &'static [Self] = &[Self::Ok, Self::Error];

    fn as_str(self) -> &'static str {
        PublishOutcome::as_str(self)
    }
}

/// Where a read took the head it started from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadHeadSource {
    /// The position of the namespace's writer session in this runtime.
    Position,
    /// The namespace's cached head, confirmed or loaded for the read.
    Anchor,
    /// The namespace's cached head as it was, for an eventual read within
    /// its maximum staleness, with no request to confirm the head.
    Eventual,
}

impl MetricLabel for ReadHeadSource {
    const VALUES: &'static [Self] = &[Self::Position, Self::Anchor, Self::Eventual];

    fn as_str(self) -> &'static str {
        match self {
            Self::Position => "position",
            Self::Anchor => "anchor",
            Self::Eventual => "eventual",
        }
    }
}

/// One reclaimable family: its label, and the count a pass reports for it.
type GcKind = (&'static str, fn(&GcResponse) -> u64);

/// Counts each deletion once, with pins grouped by owner.
const GC_CATEGORIES: [GcKind; 9] = [
    ("deleted_wal_objects", |gc| gc.deleted.wal_objects),
    ("deleted_metadata_segments", |gc| {
        gc.deleted.metadata_segments
    }),
    ("deleted_manifests", |gc| gc.deleted.manifests),
    ("deleted_fork_checkpoints", |gc| {
        gc.deleted_checkpoints_by_owner.fork
    }),
    ("deleted_expired_checkpoints", |gc| {
        gc.deleted_checkpoints_by_owner.user
    }),
    ("deleted_upload_sessions", |gc| gc.deleted.upload_sessions),
    ("deleted_content_objects", |gc| gc.deleted.content_objects),
    ("deleted_temporary_objects", |gc| {
        gc.deleted.temporary_objects
    }),
    ("deleted_retired_content_objects", |gc| {
        gc.deleted.retired_content_objects
    }),
];

/// Every instrument one runtime reports, or nothing at all.
pub(crate) struct RuntimeInstruments {
    installed: Option<Installed>,
}

struct Installed {
    recorder: Arc<dyn MetricsRecorder>,
    object_store: Mutex<HashMap<&'static str, ObjectStoreOperationInstruments>>,
    compactions: CompactionInstruments,
    publisher: PublisherInstruments,
    gc: GcInstruments,
    read_head_sources: LabeledCounters<ReadHeadSource>,
}

impl RuntimeInstruments {
    /// Builds the instrument set a handle reports through, registering the
    /// fully-labeled ones now and the rest as their labels first appear.
    pub(crate) fn new(recorder: Option<Arc<dyn MetricsRecorder>>) -> Arc<Self> {
        Arc::new(Self {
            installed: recorder.map(|recorder| Installed {
                compactions: CompactionInstruments::register(recorder.as_ref()),
                publisher: PublisherInstruments::register(recorder.as_ref()),
                gc: GcInstruments::register(recorder.as_ref()),
                read_head_sources: LabeledCounters::register(
                    recorder.as_ref(),
                    "loonfs.read_head.source",
                    "Reads by where they took the head they started from",
                    "source",
                    &[],
                ),
                object_store: Mutex::new(HashMap::new()),
                recorder,
            }),
        })
    }

    /// Records where one read took the head it started from.
    pub(crate) fn read_head(&self, source: ReadHeadSource) {
        if let Some(installed) = &self.installed {
            installed.read_head_sources.get(source).increment(1);
        }
    }

    /// An object-store recorder that bridges samples into these
    /// instruments, or `None` when nothing is installed.
    pub(crate) fn object_store_recorder(
        self: &Arc<Self>,
    ) -> Option<Arc<dyn ObjectStoreMetricsRecorder>> {
        self.installed.as_ref()?;
        Some(Arc::new(RecorderBridge {
            instruments: Arc::clone(self),
        }) as Arc<dyn ObjectStoreMetricsRecorder>)
    }

    /// Reports one finished compaction unit and any merge it completed. A
    /// unit that found nothing to merge, or a window too large for its call,
    /// reports nothing.
    pub(crate) fn compaction_finished(
        &self,
        outcome: &crate::Result<CompactionUnitOutcome>,
        elapsed_ms: u64,
    ) {
        let (outcome, totals) = match outcome {
            Ok(CompactionUnitOutcome::NotNeeded | CompactionUnitOutcome::TooLarge { .. }) => return,
            Ok(CompactionUnitOutcome::Published {
                rows_read,
                rows_written,
                input_bytes,
                output_bytes,
                ..
            }) => (
                CompactionOutcome::Completed,
                Some((*rows_read, *rows_written, *input_bytes, *output_bytes)),
            ),
            Ok(CompactionUnitOutcome::Abandoned) => (CompactionOutcome::Abandoned, None),
            Ok(CompactionUnitOutcome::Cancelled) => (CompactionOutcome::Cancelled, None),
            Ok(CompactionUnitOutcome::Fenced) => (CompactionOutcome::Fenced, None),
            Err(_) => (CompactionOutcome::Failed, None),
        };
        self.record_compaction(outcome, elapsed_ms, totals);
    }

    fn record_compaction(
        &self,
        outcome: CompactionOutcome,
        elapsed_ms: u64,
        totals: Option<(u64, u64, u64, u64)>,
    ) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.compactions.outcomes.get(outcome).increment(1);
        installed
            .compactions
            .outcome_seconds(outcome)
            .record(seconds_from_ms(elapsed_ms));
        let Some((input_rows, output_rows, input_bytes, output_bytes)) = totals else {
            return;
        };
        installed.compactions.input_rows.increment(input_rows);
        installed.compactions.output_rows.increment(output_rows);
        installed.compactions.input_bytes.increment(input_bytes);
        installed.compactions.output_bytes.increment(output_bytes);
    }

    /// Reports the candidates a namespace has admitted but not yet taken.
    pub(crate) fn publisher_queue_depth(&self, queue_depth: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .queue_depth
            .set(i64::try_from(queue_depth).unwrap_or(i64::MAX));
    }

    pub(crate) fn publisher_sessions(&self, open: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .sessions_open
            .set(i64::try_from(open).unwrap_or(i64::MAX));
    }

    /// Reports one batch taken for publication.
    pub(crate) fn publisher_batch(&self, batch_size: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.batches.increment(1);
        installed.publisher.batch_size.record(batch_size as f64);
    }

    /// Reports one WAL-tail fold on the publication path that published a
    /// manifest.
    pub(crate) fn publisher_wal_fold(&self) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.wal_folds.increment(1);
    }

    pub(crate) fn publisher_wal_fold_duration(&self, elapsed_ms: u64) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .wal_fold_seconds
            .record(seconds_from_ms(elapsed_ms));
    }

    /// Reports commits that waited in the queue at the write stop for the
    /// first time.
    pub(crate) fn publisher_write_stop_wait(&self, commits: u64) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.write_stop_waits.increment(commits);
    }

    /// Reports one wait of a namespace's queue at the write stop.
    pub(crate) fn publisher_write_stop_wait_duration(&self, elapsed_ms: u64) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .write_stop_wait_seconds
            .record(seconds_from_ms(elapsed_ms));
    }

    /// Reports commits that returned `maintenance_required` because they
    /// waited for the whole write-stop wait.
    pub(crate) fn publisher_write_stop_refusal(&self, commits: u64) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.write_stop_refusals.increment(commits);
    }

    pub(crate) fn publisher_tail_replay(&self) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.tail_replays.increment(1);
    }

    /// Reports one publication result delivered to its caller.
    pub(crate) fn publisher_publish(&self, outcome: PublishOutcome) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.publishes.get(outcome).increment(1);
    }

    /// Reports what one collection pass reclaimed and retained.
    ///
    /// Every runtime collection path records at the shared maintenance pass before
    /// its caller can drop the response.
    pub(crate) fn gc_pass(&self, gc: &GcResponse) {
        let Some(installed) = &self.installed else {
            return;
        };
        for (category, count) in GC_CATEGORIES.iter().zip(installed.gc.reclaimed.iter()) {
            let reclaimed = (category.1)(gc);
            if reclaimed > 0 {
                count.increment(reclaimed);
            }
        }
        if gc.retained.total() > 0 {
            installed.gc.retained.increment(gc.retained.total());
        }
    }

    fn object_store_sample(&self, sample: &ObjectStoreMetricSample) {
        let Some(installed) = &self.installed else {
            return;
        };
        let instruments = installed.object_store_reading(
            sample.operation.as_str(),
            sample.result.as_str(),
            sample.key_class.as_str(),
        );
        instruments.outcome.increment(1);
        instruments
            .seconds
            .record(sample.elapsed_micros as f64 / 1_000_000.0);
        if let Some(bytes_in) = sample.bytes_in {
            instruments.bytes_in.increment(bytes_in);
        }
        if let Some(bytes_out) = sample.bytes_out {
            instruments.bytes_out.increment(bytes_out);
        }
        instruments
            .retries
            .increment(u64::from(sample.attempts.saturating_sub(1)));
    }
}

impl Installed {
    /// Returns the handles one object-store sample reports through. A missing
    /// handle is registered before the registry lock is taken, because the
    /// recorder is caller code. When two first samples race, the handle
    /// stored first is kept.
    fn object_store_reading(
        &self,
        operation: &'static str,
        result: &'static str,
        key_class: &'static str,
    ) -> ObjectStoreReading {
        let operation_registered = match self.lock_object_store().get(operation) {
            Some(instruments) => match instruments.reading(result, key_class) {
                Some(reading) => return reading,
                None => true,
            },
            None => false,
        };
        let recorder = self.recorder.as_ref();
        let registered = (!operation_registered)
            .then(|| ObjectStoreOperationInstruments::register(recorder, operation));
        let outcome = ObjectStoreOperationInstruments::register_outcome(
            recorder, operation, result, key_class,
        );
        let mut registry = self.lock_object_store();
        let instruments = registry.entry(operation).or_insert_with(|| {
            registered.expect("an operation found unregistered should be registered here")
        });
        instruments
            .outcomes
            .entry((result, key_class))
            .or_insert(outcome);
        instruments
            .reading(result, key_class)
            .expect("the outcome counter should be stored by now")
    }

    fn lock_object_store(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<&'static str, ObjectStoreOperationInstruments>> {
        self.object_store
            .lock()
            .expect("the object-store instrument lock should not be poisoned")
    }
}

/// Composes the object-store recorder a handle installs on its store.
///
/// A handle may be given a recorder for the raw samples, a general recorder
/// the samples are bridged into, both, or neither. Both means one wrapper
/// feeding two sinks rather than two wrappers double-counting the same call;
/// neither means no wrapper at all, which is what keeps an uninstrumented
/// handle exactly as fast as it was before this existed.
pub(crate) fn fan_out_object_store_recorder(
    samples: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
    bridge: Option<Arc<dyn ObjectStoreMetricsRecorder>>,
) -> Option<Arc<dyn ObjectStoreMetricsRecorder>> {
    match (samples, bridge) {
        (None, None) => None,
        (Some(only), None) | (None, Some(only)) => Some(only),
        (Some(samples), Some(bridge)) => {
            Some(Arc::new(FanOutRecorder { samples, bridge }) as Arc<dyn ObjectStoreMetricsRecorder>)
        }
    }
}

/// Turns object-store samples into the runtime's instruments.
struct RecorderBridge {
    instruments: Arc<RuntimeInstruments>,
}

impl ObjectStoreMetricsRecorder for RecorderBridge {
    fn record(&self, sample: ObjectStoreMetricSample) {
        self.instruments.object_store_sample(&sample);
    }
}

/// Delivers one sample to both sinks, in the order they were configured.
struct FanOutRecorder {
    samples: Arc<dyn ObjectStoreMetricsRecorder>,
    bridge: Arc<dyn ObjectStoreMetricsRecorder>,
}

impl ObjectStoreMetricsRecorder for FanOutRecorder {
    fn record(&self, sample: ObjectStoreMetricSample) {
        self.samples.record(sample.clone());
        self.bridge.record(sample);
    }
}

/// The instruments one object-store operation reports, plus the counters for
/// each outcome and key class that operation has seen.
struct ObjectStoreOperationInstruments {
    seconds: Arc<dyn HistogramHandle>,
    bytes_in: Arc<dyn CounterHandle>,
    bytes_out: Arc<dyn CounterHandle>,
    retries: Arc<dyn CounterHandle>,
    outcomes: HashMap<(&'static str, &'static str), Arc<dyn CounterHandle>>,
}

/// One sample's worth of handles, cloned out from under the registry lock so
/// a recorder's own reporting never runs while the runtime holds it.
struct ObjectStoreReading {
    outcome: Arc<dyn CounterHandle>,
    seconds: Arc<dyn HistogramHandle>,
    bytes_in: Arc<dyn CounterHandle>,
    bytes_out: Arc<dyn CounterHandle>,
    retries: Arc<dyn CounterHandle>,
}

impl ObjectStoreOperationInstruments {
    fn register(recorder: &dyn MetricsRecorder, operation: &'static str) -> Self {
        let labels = [("operation", operation)];
        Self {
            seconds: recorder.register_histogram(
                "loonfs.object_store.operation_seconds",
                "Object-store call latency in seconds, including provider retries",
                &labels,
                LATENCY_SECONDS_BOUNDARIES,
            ),
            bytes_in: recorder.register_counter(
                "loonfs.object_store.bytes_in",
                "Payload bytes sent to the object store",
                &labels,
            ),
            bytes_out: recorder.register_counter(
                "loonfs.object_store.bytes_out",
                "Payload bytes read back from the object store",
                &labels,
            ),
            retries: recorder.register_counter(
                "loonfs.object_store.retries",
                "Object-store attempts beyond the first",
                &labels,
            ),
            outcomes: HashMap::new(),
        }
    }

    fn register_outcome(
        recorder: &dyn MetricsRecorder,
        operation: &'static str,
        result: &'static str,
        key_class: &'static str,
    ) -> Arc<dyn CounterHandle> {
        recorder.register_counter(
            "loonfs.object_store.operations",
            "Object-store calls by operation, outcome, and key class",
            &[
                ("operation", operation),
                ("result", result),
                ("key_class", key_class),
            ],
        )
    }

    /// The handles for one outcome and key class, or `None` before that
    /// pair's counter is stored.
    fn reading(&self, result: &'static str, key_class: &'static str) -> Option<ObjectStoreReading> {
        let outcome = self.outcomes.get(&(result, key_class))?;
        Some(ObjectStoreReading {
            outcome: Arc::clone(outcome),
            seconds: Arc::clone(&self.seconds),
            bytes_in: Arc::clone(&self.bytes_in),
            bytes_out: Arc::clone(&self.bytes_out),
            retries: Arc::clone(&self.retries),
        })
    }
}

/// The instruments of one execution budget, registered when the budget is
/// built.
pub(crate) struct ExecutionBudgetInstruments {
    pub(crate) read_working_memory: Option<Arc<dyn loonfs_core::cache::ReadWorkingMemoryObserver>>,
    pub(crate) admission: AdmissionInstruments,
    pub(crate) reads: PermitPoolGauges,
    pub(crate) publications: PermitPoolGauges,
    pub(crate) folds: PermitPoolGauges,
    pub(crate) compactions: PermitPoolGauges,
}

impl ExecutionBudgetInstruments {
    /// Registers every budget instrument now, or nothing without a recorder.
    pub(crate) fn new(recorder: Option<&dyn MetricsRecorder>) -> Self {
        Self {
            read_working_memory: recorder.map(super::read_working_memory::register),
            admission: AdmissionInstruments {
                installed: recorder.map(|recorder| InstalledAdmissionInstruments {
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
                }),
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
        }
    }
}

/// The admitted totals and the total-level refusals of one execution budget.
pub(crate) struct AdmissionInstruments {
    installed: Option<InstalledAdmissionInstruments>,
}

struct InstalledAdmissionInstruments {
    requests: Arc<dyn GaugeHandle>,
    bytes: Arc<dyn GaugeHandle>,
    rejections: Arc<dyn CounterHandle>,
}

impl AdmissionInstruments {
    pub(crate) fn report(&self, requests: usize, bytes: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .requests
            .set(i64::try_from(requests).unwrap_or(i64::MAX));
        installed
            .bytes
            .set(i64::try_from(bytes).unwrap_or(i64::MAX));
    }

    pub(crate) fn rejection(&self) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.rejections.increment(1);
    }
}

/// The running and waiting gauges of one kind of budget permit.
pub(crate) struct PermitPoolGauges {
    installed: Option<InstalledPoolGauges>,
}

struct InstalledPoolGauges {
    running: Arc<dyn GaugeHandle>,
    waiting: Arc<dyn GaugeHandle>,
}

impl PermitPoolGauges {
    fn register(
        recorder: Option<&dyn MetricsRecorder>,
        (running_name, running_description): (&'static str, &'static str),
        (waiting_name, waiting_description): (&'static str, &'static str),
    ) -> Self {
        Self {
            installed: recorder.map(|recorder| InstalledPoolGauges {
                running: recorder.register_gauge(running_name, running_description, &[]),
                waiting: recorder.register_gauge(waiting_name, waiting_description, &[]),
            }),
        }
    }

    pub(crate) fn report(&self, running: usize, waiting: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .running
            .set(i64::try_from(running).unwrap_or(i64::MAX));
        installed
            .waiting
            .set(i64::try_from(waiting).unwrap_or(i64::MAX));
    }
}

/// The gauges and durable totals for this process's compaction units.
struct CompactionInstruments {
    outcomes: LabeledCounters<CompactionOutcome>,
    outcome_seconds: Vec<Arc<dyn HistogramHandle>>,
    input_rows: Arc<dyn CounterHandle>,
    output_rows: Arc<dyn CounterHandle>,
    input_bytes: Arc<dyn CounterHandle>,
    output_bytes: Arc<dyn CounterHandle>,
}

impl CompactionInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let rows = |direction: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.compaction_rows",
                "Rows processed by metadata compaction units",
                &[("direction", direction)],
            )
        };
        let bytes = |direction: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.compaction_bytes",
                "Bytes processed by metadata compaction units",
                &[("direction", direction)],
            )
        };
        Self {
            outcomes: LabeledCounters::register(
                recorder,
                "loonfs.maintenance.compactions",
                "Metadata compaction units by outcome",
                "outcome",
                &[],
            ),
            outcome_seconds: CompactionOutcome::VALUES
                .iter()
                .map(|outcome| {
                    recorder.register_histogram(
                        "loonfs.maintenance.compaction_seconds",
                        "Duration of a finished metadata compaction unit in seconds",
                        &[("outcome", outcome.as_str())],
                        LATENCY_SECONDS_BOUNDARIES,
                    )
                })
                .collect(),
            input_rows: rows("input"),
            output_rows: rows("output"),
            input_bytes: bytes("input"),
            output_bytes: bytes("output"),
        }
    }

    fn outcome_seconds(&self, outcome: CompactionOutcome) -> &Arc<dyn HistogramHandle> {
        &self.outcome_seconds[outcome.index()]
    }
}

/// Publication instruments. Every label here is closed and known at
/// construction, so all of them register once.
struct PublisherInstruments {
    batches: Arc<dyn CounterHandle>,
    wal_folds: Arc<dyn CounterHandle>,
    wal_fold_seconds: Arc<dyn HistogramHandle>,
    write_stop_waits: Arc<dyn CounterHandle>,
    write_stop_wait_seconds: Arc<dyn HistogramHandle>,
    write_stop_refusals: Arc<dyn CounterHandle>,
    tail_replays: Arc<dyn CounterHandle>,
    batch_size: Arc<dyn HistogramHandle>,
    queue_depth: Arc<dyn GaugeHandle>,
    sessions_open: Arc<dyn GaugeHandle>,
    publishes: LabeledCounters<PublishOutcome>,
}

impl PublisherInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            batches: recorder.register_counter(
                "loonfs.publisher.batches",
                "Mutation batches taken for publication",
                &[],
            ),
            wal_folds: recorder.register_counter(
                "loonfs.publisher.wal_folds",
                "WAL tails folded by namespace publishers",
                &[],
            ),
            wal_fold_seconds: recorder.register_histogram(
                "loonfs.publisher.wal_fold_seconds",
                "Duration of a WAL-tail fold in seconds",
                &[],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            write_stop_waits: recorder.register_counter(
                "loonfs.publisher.write_stop_waits",
                "Commits that waited in a namespace's queue at the write stop for a fold",
                &[],
            ),
            write_stop_wait_seconds: recorder.register_histogram(
                "loonfs.publisher.write_stop_wait_seconds",
                "Duration of a namespace queue's wait at the write stop in seconds",
                &[],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            write_stop_refusals: recorder.register_counter(
                "loonfs.publisher.write_stop_refusals",
                "Commits refused because they waited for the whole write-stop wait",
                &[],
            ),
            tail_replays: recorder.register_counter(
                "loonfs.publisher.tail_replays",
                "Publishes that read the WAL tail from the store because the writer session had no position to start from",
                &[],
            ),
            batch_size: recorder.register_histogram(
                "loonfs.publisher.batch_size",
                "Candidates in one published batch",
                &[],
                SMALL_COUNT_BOUNDARIES,
            ),
            // Sampled rather than totalled: one process serves many
            // namespaces and each has its own queue, so this reads as the
            // depth of whichever publisher last admitted or took work.
            queue_depth: recorder.register_gauge(
                "loonfs.publisher.queue_depth",
                "Candidates queued at the namespace publisher that last admitted or took work",
                &[],
            ),
            sessions_open: recorder.register_gauge(
                "loonfs.publisher.sessions_open",
                "Namespace writer sessions that a handle holds or whose admitted work is still running",
                &[],
            ),
            publishes: LabeledCounters::register(
                recorder,
                "loonfs.publisher.publishes",
                "Publication results delivered to their callers",
                "result",
                &[],
            ),
        }
    }
}

/// Collection instruments, one counter per reclaimable family.
struct GcInstruments {
    reclaimed: Vec<Arc<dyn CounterHandle>>,
    retained: Arc<dyn CounterHandle>,
}

impl GcInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            reclaimed: GC_CATEGORIES
                .iter()
                .map(|(category, _)| {
                    recorder.register_counter(
                        "loonfs.gc.reclaimed",
                        "Objects one collection pass reclaimed, by family",
                        &[("category", *category)],
                    )
                })
                .collect(),
            retained: recorder.register_counter(
                "loonfs.gc.retained",
                "Candidates a collection pass retained rather than deleting",
                &[],
            ),
        }
    }
}

fn seconds_from_ms(milliseconds: u64) -> f64 {
    milliseconds as f64 / 1_000.0
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::metrics::{
        DefaultMetricsRecorder, KeyClass, MetricValue, MetricsSnapshot, ObjectStoreOperation,
        ObjectStoreResultKind, PutModeKind, VecObjectStoreMetricsRecorder,
    };
    use std::assert_matches;

    fn sample(
        operation: ObjectStoreOperation,
        result: ObjectStoreResultKind,
        attempts: u32,
    ) -> ObjectStoreMetricSample {
        classified_sample(operation, result, KeyClass::Content, attempts)
    }

    fn classified_sample(
        operation: ObjectStoreOperation,
        result: ObjectStoreResultKind,
        key_class: KeyClass,
        attempts: u32,
    ) -> ObjectStoreMetricSample {
        ObjectStoreMetricSample {
            operation,
            elapsed_micros: 250_000,
            attempts,
            result,
            bytes_in: Some(64),
            bytes_out: Some(16),
            item_count: None,
            key_class,
            range_class: None,
            put_mode: Some(PutModeKind::CreateIfAbsent),
            store_kind: None,
        }
    }

    fn counter(snapshot: &MetricsSnapshot, name: &str, labels: &[(&str, &str)]) -> u64 {
        let entry = snapshot
            .by_name(name)
            .find(|entry| entry.labels == labels)
            .unwrap_or_else(|| panic!("no `{name}` registered with labels {labels:?}"));
        match entry.value {
            MetricValue::Counter(value) => value,
            ref other => panic!("expected a counter, found {other:?}"),
        }
    }

    /// Checks that each read-head source reaches its own labeled counter,
    /// and that every source is registered before any read records one.
    #[test]
    fn read_head_sources_reach_the_registered_instruments() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        instruments.read_head(ReadHeadSource::Position);
        instruments.read_head(ReadHeadSource::Eventual);
        instruments.read_head(ReadHeadSource::Eventual);

        let snapshot = recorder.snapshot();
        for (source, value) in [("position", 1), ("anchor", 0), ("eventual", 2)] {
            assert_eq!(
                counter(&snapshot, "loonfs.read_head.source", &[("source", source)]),
                value,
                "source `{source}`"
            );
        }
    }

    #[test]
    fn a_bridged_sample_moves_every_object_store_instrument() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let bridge = instruments
            .object_store_recorder()
            .expect("an installed recorder bridges its samples");

        bridge.record(sample(
            ObjectStoreOperation::Put,
            ObjectStoreResultKind::Ok,
            3,
        ));

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.operations",
                &[
                    ("key_class", "content"),
                    ("operation", "put"),
                    ("result", "ok")
                ],
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.bytes_in",
                &[("operation", "put")],
            ),
            64
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.bytes_out",
                &[("operation", "put")],
            ),
            16
        );
        // Three attempts is two retries: the first attempt is the call.
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.retries",
                &[("operation", "put")],
            ),
            2
        );
        let latency = snapshot
            .by_name("loonfs.object_store.operation_seconds")
            .next()
            .expect("the latency histogram is registered");
        match &latency.value {
            MetricValue::Histogram { count, sum, .. } => {
                assert_eq!(*count, 1);
                assert!((sum - 0.25).abs() < 1e-9, "unexpected sum {sum}");
            }
            other => panic!("expected a histogram, found {other:?}"),
        }
    }

    #[test]
    fn outcomes_and_key_classes_of_one_operation_count_separately() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let bridge = instruments.object_store_recorder().expect("bridge");

        for (result, key_class) in [
            (ObjectStoreResultKind::Ok, KeyClass::WalObject),
            (ObjectStoreResultKind::NotFound, KeyClass::WalObject),
            (ObjectStoreResultKind::NotFound, KeyClass::WalObject),
            (ObjectStoreResultKind::Ok, KeyClass::NamespaceManifest),
        ] {
            bridge.record(classified_sample(
                ObjectStoreOperation::Get,
                result,
                key_class,
                1,
            ));
        }

        let snapshot = recorder.snapshot();
        for (result, key_class, count) in [
            ("ok", "wal_object", 1),
            ("not_found", "wal_object", 2),
            ("ok", "namespace_manifest", 1),
        ] {
            assert_eq!(
                counter(
                    &snapshot,
                    "loonfs.object_store.operations",
                    &[
                        ("key_class", key_class),
                        ("operation", "get"),
                        ("result", result)
                    ],
                ),
                count,
                "{result} {key_class}"
            );
        }
        assert_eq!(
            snapshot
                .by_name("loonfs.object_store.operation_seconds")
                .count(),
            1,
            "one operation keeps one latency histogram whatever it returned"
        );
    }

    #[test]
    fn a_handle_with_both_recorders_delivers_each_sample_to_both() {
        let samples = Arc::new(VecObjectStoreMetricsRecorder::default());
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let composed = fan_out_object_store_recorder(
            Some(samples.clone()),
            instruments.object_store_recorder(),
        )
        .expect("a configured handle installs a recorder");

        composed.record(sample(
            ObjectStoreOperation::Delete,
            ObjectStoreResultKind::Ok,
            1,
        ));

        assert_eq!(samples.samples().len(), 1);
        assert_eq!(
            counter(
                &recorder.snapshot(),
                "loonfs.object_store.operations",
                &[
                    ("key_class", "content"),
                    ("operation", "delete"),
                    ("result", "ok")
                ],
            ),
            1
        );
    }

    #[test]
    fn a_handle_with_no_recorder_installs_nothing() {
        let instruments = RuntimeInstruments::new(None);
        assert!(instruments.object_store_recorder().is_none());
        assert!(fan_out_object_store_recorder(None, instruments.object_store_recorder()).is_none());

        instruments.publisher_batch(4);
        instruments.publisher_wal_fold();
        instruments.publisher_wal_fold_duration(5);
        instruments.publisher_write_stop_wait(2);
        instruments.publisher_write_stop_wait_duration(5);
        instruments.publisher_write_stop_refusal(1);
        instruments.publisher_publish(PublishOutcome::Ok);
    }

    #[test]
    fn compaction_instruments_register_the_closed_vocabularies() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));

        instruments.compaction_finished(
            &Ok(CompactionUnitOutcome::Published {
                group: loonfs_core::MetadataFamilyGroup::Bindings,
                input_runs: 2,
                manifest: loonfs_core::PublishedManifest {
                    manifest: loonfs_types::format::control::ManifestRef {
                        owner_namespace_id: loonfs_types::NamespaceId::parse("demo")
                            .expect("valid namespace id"),
                        manifest_no: loonfs_types::ManifestNo(1),
                        head_seq: loonfs_types::ChangeSeq(1),
                        payload_checksum: String::new(),
                    },
                    writer_epoch: loonfs_types::WriterEpoch(1),
                    status: loonfs_types::format::control::NamespaceStatus::Active {},
                    folded_wal_no: loonfs_types::WalNo(1),
                    started: None,
                },
                rows_read: 20,
                rows_written: 10,
                input_bytes: 2_000,
                output_bytes: 1_000,
                output_segments: 1,
            }),
            25,
        );
        instruments.compaction_finished(&Ok(CompactionUnitOutcome::Abandoned), 10);
        instruments.compaction_finished(&Ok(CompactionUnitOutcome::NotNeeded), 10);

        let snapshot = recorder.snapshot();
        assert_eq!(
            snapshot.by_name("loonfs.maintenance.compactions").count(),
            5
        );
        assert_eq!(
            snapshot
                .by_name("loonfs.maintenance.compaction_seconds")
                .count(),
            5
        );
        assert_eq!(
            snapshot
                .by_name("loonfs.maintenance.compaction_rows")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .by_name("loonfs.maintenance.compaction_bytes")
                .count(),
            2
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.compactions",
                &[("outcome", "completed")],
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.compactions",
                &[("outcome", "abandoned")],
            ),
            1
        );
        let duration = snapshot
            .by_name("loonfs.maintenance.compaction_seconds")
            .find(|entry| entry.labels == [("outcome", "abandoned")])
            .expect("abandoned compaction duration");
        assert_matches!(duration.value, MetricValue::Histogram { count: 1, .. });
        assert!(snapshot
            .all()
            .iter()
            .all(|entry| !entry.labels.contains(&("outcome", "superseded"))));
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.compaction_rows",
                &[("direction", "input")],
            ),
            20
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.compaction_bytes",
                &[("direction", "output")],
            ),
            1_000
        );
    }

    #[test]
    fn a_collection_pass_counts_what_it_reclaimed_and_retained() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let mut gc = GcResponse {
            namespace_id: loonfs_test_support::ids::namespace_id("demo"),
            deleted: loonfs_types::DeletedObjectCounts {
                wal_objects: 3,
                content_objects: 5,
                temporary_objects: 11,
                retired_content_objects: 13,
                ..loonfs_types::DeletedObjectCounts::default()
            },
            deleted_checkpoints_by_owner: loonfs_types::DeletedCheckpointsByOwner {
                fork: 2,
                user: 3,
            },
            retained: loonfs_types::RetainedCandidates {
                referenced: 2,
                ..loonfs_types::RetainedCandidates::default()
            },
            next_reclamation_at_ms: None,
            reclaimable_at_ms: None,
        };

        instruments.gc_pass(&gc);
        gc.deleted.wal_objects = 1;
        instruments.gc_pass(&gc);

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.gc.reclaimed",
                &[("category", "deleted_wal_objects")],
            ),
            4
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.gc.reclaimed",
                &[("category", "deleted_content_objects")],
            ),
            10
        );
        assert_eq!(counter(&snapshot, "loonfs.gc.retained", &[]), 4);
        // Every family registers at construction, so a scrape names the
        // whole reclaimable vocabulary rather than only what has happened.
        assert_eq!(snapshot.by_name("loonfs.gc.reclaimed").count(), 9);
        for (category, expected) in [
            ("deleted_fork_checkpoints", 4),
            ("deleted_expired_checkpoints", 6),
            ("deleted_temporary_objects", 22),
            ("deleted_retired_content_objects", 26),
        ] {
            assert_eq!(
                counter(&snapshot, "loonfs.gc.reclaimed", &[("category", category)]),
                expected
            );
        }
    }
}
