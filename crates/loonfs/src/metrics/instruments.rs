//! The instruments the runtime reports, and the bridge that turns
//! object-store samples into them.
//!
//! One [`RuntimeInstruments`] is built per handle and reaches the reporting
//! sites through the read core. Without a configured recorder it holds
//! nothing and every report returns on a null check, which is what makes
//! "no recorder" cost nothing rather than cost a no-op registration for
//! every instrument in the set.
//!
//! Instruments whose label value is only known when the first report
//! arrives — the object-store operation that just finished, the id an
//! extension registered its maintenance job under — are memoized here, so a
//! recorder still sees exactly one registration per label set as
//! [`MetricsRecorder`] promises. The memo costs one uncontended lock on
//! paths whose unit of work is a provider round trip.

use super::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, ObjectStoreMetricSample,
    ObjectStoreMetricsRecorder, SMALL_COUNT_BOUNDARIES,
};
use crate::metrics::LATENCY_SECONDS_BOUNDARIES;
use crate::{GcResponse, MaintenanceJobId, MaintenanceStepConclusion};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// One reclaimable family: its label, and the count a pass reports for it.
type GcCategory = (&'static str, fn(&GcResponse) -> u64);

/// Reclaimable object families one collection pass counts, named exactly as
/// [`GcResponse`] names them.
const GC_CATEGORIES: [GcCategory; 9] = [
    ("deleted_wal_segments", |gc| gc.deleted_wal_segments),
    ("deleted_metadata_tables", |gc| gc.deleted_metadata_tables),
    ("deleted_manifests", |gc| gc.deleted_manifests),
    ("deleted_checkpoint_records", |gc| {
        gc.deleted_checkpoint_records
    }),
    ("released_fork_checkpoints", |gc| {
        gc.released_fork_checkpoints
    }),
    ("released_expired_checkpoints", |gc| {
        gc.released_expired_checkpoints
    }),
    ("deleted_upload_sessions", |gc| gc.deleted_upload_sessions),
    ("deleted_content_objects", |gc| gc.deleted_content_objects),
    ("released_missing_basis_checkpoints", |gc| {
        gc.released_missing_basis_checkpoints
    }),
];

/// Every instrument one runtime reports, or nothing at all.
pub(crate) struct RuntimeInstruments {
    installed: Option<Installed>,
}

struct Installed {
    recorder: Arc<dyn MetricsRecorder>,
    object_store: Mutex<HashMap<&'static str, ObjectStoreOperationInstruments>>,
    maintenance: Mutex<HashMap<&'static str, MaintenanceJobInstruments>>,
    compactions: CompactionInstruments,
    publisher: PublisherInstruments,
    gc: GcInstruments,
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
                object_store: Mutex::new(HashMap::new()),
                maintenance: Mutex::new(HashMap::new()),
                recorder,
            }),
        })
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

    /// Reports one settled maintenance step.
    pub(crate) fn maintenance_step(
        &self,
        job: MaintenanceJobId,
        conclusion: MaintenanceStepConclusion,
        queued_ms: u64,
        elapsed_ms: u64,
    ) {
        let Some(installed) = &self.installed else {
            return;
        };
        let instruments = installed.maintenance_job(job);
        instruments.step(conclusion).increment(1);
        instruments.step_seconds.record(seconds_from_ms(elapsed_ms));
        instruments
            .queue_wait_seconds
            .record(seconds_from_ms(queued_ms));
    }

    /// Reports one maintenance step that failed before concluding.
    pub(crate) fn maintenance_step_failed(&self, job: MaintenanceJobId, queued_ms: u64) {
        let Some(installed) = &self.installed else {
            return;
        };
        let instruments = installed.maintenance_job(job);
        instruments.failures.increment(1);
        instruments
            .queue_wait_seconds
            .record(seconds_from_ms(queued_ms));
    }

    /// Reports one reconciliation probe that could not say whether a job has
    /// work.
    ///
    /// A probe takes no permit, so there is no queue wait to file beside it.
    pub(crate) fn maintenance_probe_failed(&self, job: MaintenanceJobId) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.maintenance_job(job).probe_failures.increment(1);
    }

    /// Reports the streaming metadata compactions this process is running and
    /// the ones holding a namespace slot while they wait for a permit.
    ///
    /// Queued is the number that matters on its own: jobs take as long as
    /// they take, so a queue that never empties is a process serving more
    /// namespaces than its compaction limit admits.
    pub(crate) fn compactions(&self, running: usize, queued: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .compactions
            .running
            .set(i64::try_from(running).unwrap_or(i64::MAX));
        installed
            .compactions
            .queued
            .set(i64::try_from(queued).unwrap_or(i64::MAX));
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

    /// Reports the WAL-tail projections this writer retains across its
    /// namespace publishers, after one publish or eviction settled them.
    pub(crate) fn publisher_retained_projections(&self, projections: usize, decoded_bytes: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .retained_projections
            .set(i64::try_from(projections).unwrap_or(i64::MAX));
        installed
            .publisher
            .retained_projection_bytes
            .set(i64::try_from(decoded_bytes).unwrap_or(i64::MAX));
    }

    /// Reports one batch taken for publication.
    pub(crate) fn publisher_batch(&self, batch_size: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.batches.increment(1);
        installed.publisher.batch_size.record(batch_size as f64);
    }

    /// Reports one publication result delivered to its caller.
    pub(crate) fn publisher_publish(&self, result: &'static str) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.publish(result).increment(1);
    }

    /// Reports what one collection pass reclaimed and retained.
    ///
    /// The automatic path reads a pass as one conclusion and drops its
    /// counts; this is where they survive.
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
        if gc.retained_candidates > 0 {
            installed.gc.retained.increment(gc.retained_candidates);
        }
    }

    fn object_store_sample(&self, sample: &ObjectStoreMetricSample) {
        let Some(installed) = &self.installed else {
            return;
        };
        let operation = sample.operation.as_str();
        let instruments = {
            let mut registry = lock(&installed.object_store);
            let instruments = registry.entry(operation).or_insert_with(|| {
                ObjectStoreOperationInstruments::register(installed.recorder.as_ref(), operation)
            });
            instruments.reading(installed.recorder.as_ref(), sample.result.as_str())
        };
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
    fn maintenance_job(&self, job: MaintenanceJobId) -> MaintenanceJobInstruments {
        let mut registry = lock(&self.maintenance);
        registry
            .entry(job.as_str())
            .or_insert_with(|| MaintenanceJobInstruments::register(self.recorder.as_ref(), job))
            .clone()
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

/// The instruments one object-store operation reports, plus the per-outcome
/// counters that operation has seen.
struct ObjectStoreOperationInstruments {
    operation: &'static str,
    seconds: Arc<dyn HistogramHandle>,
    bytes_in: Arc<dyn CounterHandle>,
    bytes_out: Arc<dyn CounterHandle>,
    retries: Arc<dyn CounterHandle>,
    outcomes: HashMap<&'static str, Arc<dyn CounterHandle>>,
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
            operation,
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

    fn reading(
        &mut self,
        recorder: &dyn MetricsRecorder,
        result: &'static str,
    ) -> ObjectStoreReading {
        let operation = self.operation;
        let outcome = self
            .outcomes
            .entry(result)
            .or_insert_with(|| {
                recorder.register_counter(
                    "loonfs.object_store.operations",
                    "Object-store calls by operation and outcome",
                    &[("operation", operation), ("result", result)],
                )
            })
            .clone();
        ObjectStoreReading {
            outcome,
            seconds: Arc::clone(&self.seconds),
            bytes_in: Arc::clone(&self.bytes_in),
            bytes_out: Arc::clone(&self.bytes_out),
            retries: Arc::clone(&self.retries),
        }
    }
}

/// The instruments one maintenance job reports.
///
/// Job ids are `&'static str` by construction — the runtime's own are
/// associated constants and an extension names its own with
/// [`MaintenanceJobId::new`] — so the label stays bounded by the jobs a
/// build registers.
#[derive(Clone)]
struct MaintenanceJobInstruments {
    progressed: Arc<dyn CounterHandle>,
    idle: Arc<dyn CounterHandle>,
    blocked: Arc<dyn CounterHandle>,
    superseded: Arc<dyn CounterHandle>,
    not_enabled: Arc<dyn CounterHandle>,
    failures: Arc<dyn CounterHandle>,
    probe_failures: Arc<dyn CounterHandle>,
    step_seconds: Arc<dyn HistogramHandle>,
    queue_wait_seconds: Arc<dyn HistogramHandle>,
}

impl MaintenanceJobInstruments {
    fn register(recorder: &dyn MetricsRecorder, job: MaintenanceJobId) -> Self {
        let job = job.as_str();
        let steps = |conclusion: MaintenanceStepConclusion| {
            recorder.register_counter(
                "loonfs.maintenance.steps",
                "Maintenance steps by job and conclusion",
                &[("job", job), ("conclusion", conclusion.as_str())],
            )
        };
        Self {
            progressed: steps(MaintenanceStepConclusion::Progressed),
            idle: steps(MaintenanceStepConclusion::Idle),
            blocked: steps(MaintenanceStepConclusion::Blocked),
            superseded: steps(MaintenanceStepConclusion::Superseded),
            not_enabled: steps(MaintenanceStepConclusion::NotEnabled),
            failures: recorder.register_counter(
                "loonfs.maintenance.step_failures",
                "Maintenance steps that failed before concluding",
                &[("job", job)],
            ),
            probe_failures: recorder.register_counter(
                "loonfs.maintenance.probe_failures",
                "Reconciliation probes that failed to say whether a job has work",
                &[("job", job)],
            ),
            step_seconds: recorder.register_histogram(
                "loonfs.maintenance.step_seconds",
                "Maintenance step duration in seconds",
                &[("job", job)],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            queue_wait_seconds: recorder.register_histogram(
                "loonfs.maintenance.queue_wait_seconds",
                "Seconds a maintenance step waited for a permit",
                &[("job", job)],
                LATENCY_SECONDS_BOUNDARIES,
            ),
        }
    }

    fn step(&self, conclusion: MaintenanceStepConclusion) -> &Arc<dyn CounterHandle> {
        match conclusion {
            MaintenanceStepConclusion::Progressed => &self.progressed,
            MaintenanceStepConclusion::Idle => &self.idle,
            MaintenanceStepConclusion::Blocked => &self.blocked,
            MaintenanceStepConclusion::Superseded => &self.superseded,
            MaintenanceStepConclusion::NotEnabled => &self.not_enabled,
        }
    }
}

/// The two numbers that describe this process's streaming metadata
/// compactions: how many are running, and how many are waiting to.
struct CompactionInstruments {
    running: Arc<dyn GaugeHandle>,
    queued: Arc<dyn GaugeHandle>,
}

impl CompactionInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            running: recorder.register_gauge(
                "loonfs.maintenance.compactions_running",
                "Streaming metadata compactions this process is running",
                &[],
            ),
            queued: recorder.register_gauge(
                "loonfs.maintenance.compactions_queued",
                "Streaming metadata compactions holding a namespace slot while they wait for a \
                 process permit",
                &[],
            ),
        }
    }
}

/// Publication instruments. Every label here is closed and known at
/// construction, so all of them register once.
struct PublisherInstruments {
    batches: Arc<dyn CounterHandle>,
    batch_size: Arc<dyn HistogramHandle>,
    queue_depth: Arc<dyn GaugeHandle>,
    retained_projections: Arc<dyn GaugeHandle>,
    retained_projection_bytes: Arc<dyn GaugeHandle>,
    published_ok: Arc<dyn CounterHandle>,
    published_error: Arc<dyn CounterHandle>,
}

impl PublisherInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let publishes = |result: &'static str| {
            recorder.register_counter(
                "loonfs.publisher.publishes",
                "Publication results delivered to their callers",
                &[("result", result)],
            )
        };
        Self {
            batches: recorder.register_counter(
                "loonfs.publisher.batches",
                "Mutation batches taken for publication",
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
            // Totals, not samples: these are what the writer holds across
            // every namespace it has published to.
            retained_projections: recorder.register_gauge(
                "loonfs.publisher.retained_projections",
                "WAL-tail projections this writer's namespace publishers retain",
                &[],
            ),
            retained_projection_bytes: recorder.register_gauge(
                "loonfs.publisher.retained_projection_bytes",
                "Decoded bytes held by the retained WAL-tail projections",
                &[],
            ),
            published_ok: publishes("ok"),
            published_error: publishes("error"),
        }
    }

    fn publish(&self, result: &'static str) -> &Arc<dyn CounterHandle> {
        if result == "ok" {
            &self.published_ok
        } else {
            &self.published_error
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

// A poisoned instrument registry is recovered rather than propagated:
// reporting a metric must never be the thing that takes a runtime down.
fn lock<T>(registry: &Mutex<T>) -> MutexGuard<'_, T> {
    registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // A reading of the wrong kind or an unregistered instrument is a bug in
    // the test, not a case to handle.

    use super::*;
    use crate::metrics::{
        DefaultMetricsRecorder, KeyClass, MetricValue, MetricsSnapshot, ObjectStoreOperation,
        ObjectStoreResultClass, PutModeClass, VecObjectStoreMetricsRecorder,
    };
    use crate::{MaintenanceJobId, MaintenanceStepConclusion};

    fn sample(
        operation: ObjectStoreOperation,
        result: ObjectStoreResultClass,
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
            key_class: KeyClass::Content,
            range_class: None,
            put_mode: Some(PutModeClass::Overwrite),
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

    #[test]
    fn a_bridged_sample_moves_every_object_store_instrument() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let bridge = instruments
            .object_store_recorder()
            .expect("an installed recorder bridges its samples");

        bridge.record(sample(
            ObjectStoreOperation::Put,
            ObjectStoreResultClass::Ok,
            3,
        ));

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.operations",
                &[("operation", "put"), ("result", "ok")],
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

    /// Outcomes share one operation's latency and byte instruments and get a
    /// counter each, so a failing operation is visible without splitting its
    /// timing in two.
    #[test]
    fn outcomes_of_one_operation_count_separately() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let bridge = instruments.object_store_recorder().expect("bridge");

        bridge.record(sample(
            ObjectStoreOperation::Get,
            ObjectStoreResultClass::Ok,
            1,
        ));
        bridge.record(sample(
            ObjectStoreOperation::Get,
            ObjectStoreResultClass::NotFound,
            1,
        ));
        bridge.record(sample(
            ObjectStoreOperation::Get,
            ObjectStoreResultClass::NotFound,
            1,
        ));

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.operations",
                &[("operation", "get"), ("result", "ok")],
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.object_store.operations",
                &[("operation", "get"), ("result", "not_found")],
            ),
            2
        );
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
            ObjectStoreResultClass::Ok,
            1,
        ));

        assert_eq!(samples.samples().len(), 1);
        assert_eq!(
            counter(
                &recorder.snapshot(),
                "loonfs.object_store.operations",
                &[("operation", "delete"), ("result", "ok")],
            ),
            1
        );
    }

    /// What the null check buys: a handle built without a recorder installs
    /// no wrapper at all, and reporting through it is a returned call.
    #[test]
    fn a_handle_with_no_recorder_installs_nothing() {
        let instruments = RuntimeInstruments::new(None);
        assert!(instruments.object_store_recorder().is_none());
        assert!(fan_out_object_store_recorder(None, instruments.object_store_recorder()).is_none());

        instruments.publisher_batch(4);
        instruments.publisher_publish("ok");
        instruments.maintenance_step(MaintenanceJobId::GC, MaintenanceStepConclusion::Idle, 1, 2);
    }

    #[test]
    fn a_settled_step_counts_against_its_job_and_conclusion() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));

        instruments.maintenance_step(
            MaintenanceJobId::METADATA,
            MaintenanceStepConclusion::Progressed,
            5,
            30,
        );
        instruments.maintenance_step(
            MaintenanceJobId::METADATA,
            MaintenanceStepConclusion::Idle,
            0,
            1,
        );
        instruments.maintenance_step_failed(MaintenanceJobId::GC, 7);

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.steps",
                &[("conclusion", "progressed"), ("job", "metadata")],
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.steps",
                &[("conclusion", "idle"), ("job", "metadata")],
            ),
            1
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.step_failures",
                &[("job", "gc")],
            ),
            1
        );
        // A failed step has no duration to report, but it waited like any
        // other, so its queue wait still counts. A job's whole instrument
        // set registers the first time it reports anything, so `gc` has a
        // step-duration histogram with nothing filed in it.
        for (name, job, count) in [
            ("loonfs.maintenance.step_seconds", "metadata", 2),
            ("loonfs.maintenance.step_seconds", "gc", 0),
            ("loonfs.maintenance.queue_wait_seconds", "metadata", 2),
            ("loonfs.maintenance.queue_wait_seconds", "gc", 1),
        ] {
            let entry = snapshot
                .by_name(name)
                .find(|entry| entry.labels == [("job", job)])
                .unwrap_or_else(|| panic!("no `{name}` for job `{job}`"));
            match entry.value {
                MetricValue::Histogram { count: filed, .. } => assert_eq!(filed, count),
                ref other => panic!("expected a histogram, found {other:?}"),
            }
        }
    }

    /// A probe that cannot answer leaves the key admitted and says nothing
    /// else, so this counter is the only standing record that a namespace's
    /// reconciliation is stuck. It is counted per job, beside the step
    /// failures, and it is not filed as one: a probe ran no step.
    #[test]
    fn a_failed_reconciliation_probe_counts_against_its_job() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));

        instruments.maintenance_probe_failed(MaintenanceJobId::METADATA);
        instruments.maintenance_probe_failed(MaintenanceJobId::METADATA);

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.probe_failures",
                &[("job", "metadata")],
            ),
            2
        );
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.maintenance.step_failures",
                &[("job", "metadata")],
            ),
            0
        );
    }

    #[test]
    fn a_collection_pass_counts_what_it_reclaimed_and_retained() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        let mut gc = GcResponse {
            namespace_id: loonfs_test_support::ids::namespace_id("demo"),
            deleted_wal_segments: 3,
            deleted_metadata_tables: 0,
            deleted_manifests: 0,
            deleted_checkpoint_records: 0,
            released_fork_checkpoints: 0,
            released_expired_checkpoints: 0,
            deleted_upload_sessions: 0,
            deleted_content_objects: 5,
            released_missing_basis_checkpoints: 0,
            retained_candidates: 2,
            retained: loonfs_api::RetainedCandidates {
                referenced: 2,
                ..loonfs_api::RetainedCandidates::default()
            },
            degraded_retention: false,
            content_reclamation_deferred: false,
            budget_exhausted: false,
            next_cursor: None,
            next_reclamation_at_ms: None,
        };

        instruments.gc_pass(&gc);
        gc.deleted_wal_segments = 1;
        instruments.gc_pass(&gc);

        let snapshot = recorder.snapshot();
        assert_eq!(
            counter(
                &snapshot,
                "loonfs.gc.reclaimed",
                &[("category", "deleted_wal_segments")],
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
    }
}
