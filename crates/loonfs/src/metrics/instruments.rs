//! Runtime, metadata cache, and object-store metric reporting.
//!
//! Each runtime has one [`RuntimeInstruments`] instance, and each metadata
//! cache one [`MetadataCacheInstruments`]. Instruments with dynamic labels
//! are cached so each label set is registered once. Reporting returns
//! immediately when no recorder is configured.

use super::{
    CounterHandle, GaugeHandle, HistogramHandle, MetricsRecorder, ObjectStoreMetricSample,
    ObjectStoreMetricsRecorder, SMALL_COUNT_BOUNDARIES,
};
use crate::metrics::{
    LATENCY_SECONDS_BOUNDARIES, RESULT_ERROR, RESULT_HIT, RESULT_MISS, RESULT_OK,
};
use crate::{GcResponse, MetadataCompactionJobOutcome};
use loonfs_core::cache::DecodedBlockCacheObserver;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

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

/// The closed outcome vocabulary for a finished streaming compaction.
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

/// One reclaimable family: its label, and the count a pass reports for it.
type GcCategory = (&'static str, fn(&GcResponse) -> u64);

/// Counts each deletion once, with pins grouped by owner.
const GC_CATEGORIES: [GcCategory; 8] = [
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
    ("deleted_snapshot_checkpoints", |gc| {
        gc.deleted_checkpoints_by_owner.snapshot
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
    views: ViewReadInstruments,
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
                views: ViewReadInstruments::register(recorder.as_ref()),
                object_store: Mutex::new(HashMap::new()),
                recorder,
            }),
        })
    }

    /// Records one latest-view read.
    pub(crate) fn latest_metadata_view_read(&self) {
        if let Some(installed) = &self.installed {
            installed.views.latest_metadata_view_reads.increment(1);
        }
    }

    /// Records one snapshot-backed metadata view.
    pub(crate) fn snapshot_view_read(&self) {
        if let Some(installed) = &self.installed {
            installed.views.snapshot_view_reads.increment(1);
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

    /// Reports one finished streaming compaction and any merge it completed.
    pub(crate) fn compaction_finished(
        &self,
        outcome: &crate::Result<MetadataCompactionJobOutcome>,
        elapsed_ms: u64,
    ) {
        let (outcome, totals) = match outcome {
            Ok(MetadataCompactionJobOutcome::Published {
                rows_read,
                rows_written,
                input_bytes,
                output_bytes,
                ..
            }) => (
                CompactionOutcome::Completed,
                Some((*rows_read, *rows_written, *input_bytes, *output_bytes)),
            ),
            Ok(MetadataCompactionJobOutcome::Abandoned) => (CompactionOutcome::Abandoned, None),
            Ok(MetadataCompactionJobOutcome::Cancelled) => (CompactionOutcome::Cancelled, None),
            Ok(MetadataCompactionJobOutcome::Fenced) => (CompactionOutcome::Fenced, None),
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

    /// Reports one WAL-tail fold on the publication path.
    pub(crate) fn publisher_wal_fold(&self) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.wal_folds.increment(1);
    }

    pub(crate) fn publisher_wal_folds_waiting(&self, waiting: usize) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed
            .publisher
            .wal_folds_waiting
            .set(i64::try_from(waiting).unwrap_or(i64::MAX));
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

    pub(crate) fn publisher_write_stop_refusal(&self) {
        let Some(installed) = &self.installed else {
            return;
        };
        installed.publisher.write_stop_refusals.increment(1);
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
        let operation = sample.operation.as_str();
        let instruments = {
            let mut registry = lock(&installed.object_store);
            let instruments = registry.entry(operation).or_insert_with(|| {
                ObjectStoreOperationInstruments::register(installed.recorder.as_ref(), operation)
            });
            instruments.reading(
                installed.recorder.as_ref(),
                sample.result.as_str(),
                sample.key_class.as_str(),
            )
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
    operation: &'static str,
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
        key_class: &'static str,
    ) -> ObjectStoreReading {
        let operation = self.operation;
        let outcome = self
            .outcomes
            .entry((result, key_class))
            .or_insert_with(|| {
                recorder.register_counter(
                    "loonfs.object_store.operations",
                    "Object-store calls by operation, outcome, and key class",
                    &[
                        ("operation", operation),
                        ("result", result),
                        ("key_class", key_class),
                    ],
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

/// The metadata views one runtime reads through.
struct ViewReadInstruments {
    latest_metadata_view_reads: Arc<dyn CounterHandle>,
    snapshot_view_reads: Arc<dyn CounterHandle>,
}

/// Every instrument one metadata cache reports, or nothing at all.
pub(crate) struct MetadataCacheInstruments {
    installed: Option<InstalledCache>,
}

struct InstalledCache {
    namespace_head: NamespaceHeadCacheInstruments,
    metadata_segment: Arc<MetadataSegmentCacheInstruments>,
    wal_tail_projection: Arc<WalTailProjectionCacheInstruments>,
    head_state: Arc<HeadStateCacheInstruments>,
}

impl MetadataCacheInstruments {
    /// Registers every cache instrument now, or nothing without a recorder.
    pub(crate) fn new(recorder: Option<&dyn MetricsRecorder>) -> Self {
        Self {
            installed: recorder.map(|recorder| InstalledCache {
                namespace_head: NamespaceHeadCacheInstruments::register(recorder),
                metadata_segment: Arc::new(MetadataSegmentCacheInstruments::register(recorder)),
                wal_tail_projection: Arc::new(WalTailProjectionCacheInstruments::register(
                    recorder,
                )),
                head_state: Arc::new(HeadStateCacheInstruments::register(recorder)),
            }),
        }
    }

    pub(crate) fn namespace_head_cache_hit(&self) {
        if let Some(installed) = &self.installed {
            installed.namespace_head.hits.increment(1);
        }
    }

    pub(crate) fn namespace_head_cache_miss(&self) {
        if let Some(installed) = &self.installed {
            installed.namespace_head.misses.increment(1);
        }
    }

    /// Returns the metadata-segment cache metrics observer, if metrics are enabled.
    pub(crate) fn metadata_segment_cache_observer(
        &self,
    ) -> Option<Arc<dyn DecodedBlockCacheObserver>> {
        let observer = Arc::clone(&self.installed.as_ref()?.metadata_segment);
        Some(observer)
    }

    /// Returns the observer for WAL-tail lookups and inserts, if metrics are
    /// enabled.
    pub(crate) fn wal_tail_projection_cache_observer(
        &self,
    ) -> Option<Arc<dyn DecodedBlockCacheObserver>> {
        let observer = Arc::clone(&self.installed.as_ref()?.wal_tail_projection);
        Some(observer)
    }

    /// Returns the observer for evictions, rejections, and retained bytes
    /// across head anchors and WAL-tail projections, if metrics are enabled.
    pub(crate) fn head_state_cache_observer(&self) -> Option<Arc<dyn DecodedBlockCacheObserver>> {
        let observer = Arc::clone(&self.installed.as_ref()?.head_state);
        Some(observer)
    }
}

struct NamespaceHeadCacheInstruments {
    hits: Arc<dyn CounterHandle>,
    misses: Arc<dyn CounterHandle>,
}

struct MetadataSegmentCacheInstruments {
    hits: Arc<dyn CounterHandle>,
    misses: Arc<dyn CounterHandle>,
    inserts: Arc<dyn CounterHandle>,
    evictions: Arc<dyn CounterHandle>,
    filter_skips: Arc<dyn CounterHandle>,
    filter_false_positives: Arc<dyn CounterHandle>,
    retained_decoded_bytes: Arc<dyn GaugeHandle>,
}

struct WalTailProjectionCacheInstruments {
    hits: Arc<dyn CounterHandle>,
    misses: Arc<dyn CounterHandle>,
    inserts: Arc<dyn CounterHandle>,
}

struct HeadStateCacheInstruments {
    evictions: Arc<dyn CounterHandle>,
    evicted_decoded_bytes: Arc<dyn CounterHandle>,
    rejections: Arc<dyn CounterHandle>,
    rejected_decoded_bytes: Arc<dyn CounterHandle>,
    retained_decoded_bytes: Arc<dyn GaugeHandle>,
}

impl ViewReadInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            latest_metadata_view_reads: recorder.register_counter(
                "loonfs.runtime_cache.latest_metadata_view_reads",
                "Latest metadata reads served through the metadata-view path",
                &[],
            ),
            snapshot_view_reads: recorder.register_counter(
                "loonfs.runtime_cache.snapshot_view_reads",
                "Snapshot-backed metadata views created by the runtime",
                &[],
            ),
        }
    }
}

impl NamespaceHeadCacheInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let get = |result| {
            recorder.register_counter(
                "loonfs.namespace_head_cache.gets",
                "Namespace head cache lookups, by outcome",
                &[("result", result)],
            )
        };
        Self {
            hits: get(RESULT_HIT),
            misses: get(RESULT_MISS),
        }
    }
}

impl MetadataSegmentCacheInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let get = |result| {
            recorder.register_counter(
                "loonfs.metadata_segment_cache.gets",
                "Decoded metadata-segment cache lookups, by outcome",
                &[("result", result)],
            )
        };
        Self {
            hits: get(RESULT_HIT),
            misses: get(RESULT_MISS),
            inserts: recorder.register_counter(
                "loonfs.metadata_segment_cache.inserts",
                "Blocks inserted into the decoded metadata-segment cache",
                &[],
            ),
            evictions: recorder.register_counter(
                "loonfs.metadata_segment_cache.evictions",
                "Blocks evicted from the decoded metadata-segment cache",
                &[],
            ),
            filter_skips: recorder.register_counter(
                "loonfs.metadata_segment_cache.filter_skips",
                "Segments skipped after their filter ruled out a lookup",
                &[],
            ),
            filter_false_positives: recorder.register_counter(
                "loonfs.metadata_segment_cache.filter_false_positives",
                "Filter admissions that matched no metadata rows",
                &[],
            ),
            retained_decoded_bytes: recorder.register_gauge(
                "loonfs.metadata_segment_cache.retained_decoded_bytes",
                "Decoded bytes currently retained in the metadata-segment cache",
                &[],
            ),
        }
    }
}

impl DecodedBlockCacheObserver for MetadataSegmentCacheInstruments {
    fn hit(&self) {
        self.hits.increment(1);
    }

    fn miss(&self) {
        self.misses.increment(1);
    }

    fn insert(&self) {
        self.inserts.increment(1);
    }

    fn evict(&self, _decoded_bytes: usize) {
        self.evictions.increment(1);
    }

    fn filter_skip(&self) {
        self.filter_skips.increment(1);
    }

    fn filter_false_positive(&self) {
        self.filter_false_positives.increment(1);
    }

    fn retained(&self, decoded_bytes: usize) {
        self.retained_decoded_bytes.set(metric_level(decoded_bytes));
    }
}

impl WalTailProjectionCacheInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        let get = |result| {
            recorder.register_counter(
                "loonfs.wal_tail_projection_cache.gets",
                "WAL-tail projection cache lookups, by outcome",
                &[("result", result)],
            )
        };
        Self {
            hits: get(RESULT_HIT),
            misses: get(RESULT_MISS),
            inserts: recorder.register_counter(
                "loonfs.wal_tail_projection_cache.inserts",
                "WAL-tail projections inserted into the cache",
                &[],
            ),
        }
    }
}

impl DecodedBlockCacheObserver for WalTailProjectionCacheInstruments {
    fn hit(&self) {
        self.hits.increment(1);
    }

    fn miss(&self) {
        self.misses.increment(1);
    }

    fn insert(&self) {
        self.inserts.increment(1);
    }
}

impl HeadStateCacheInstruments {
    fn register(recorder: &dyn MetricsRecorder) -> Self {
        Self {
            evictions: recorder.register_counter(
                "loonfs.head_state_cache.evictions",
                "Head anchors and WAL-tail projections evicted to stay within the head-state budget",
                &[],
            ),
            evicted_decoded_bytes: recorder.register_counter(
                "loonfs.head_state_cache.evicted_decoded_bytes",
                "Decoded bytes dropped with evicted head state",
                &[],
            ),
            rejections: recorder.register_counter(
                "loonfs.head_state_cache.rejections",
                "Head anchors and WAL-tail projections heavier than the whole head-state budget",
                &[],
            ),
            rejected_decoded_bytes: recorder.register_counter(
                "loonfs.head_state_cache.rejected_decoded_bytes",
                "Decoded bytes in rejected head state",
                &[],
            ),
            retained_decoded_bytes: recorder.register_gauge(
                "loonfs.head_state_cache.retained_decoded_bytes",
                "Decoded bytes currently retained as head anchors and WAL-tail projections",
                &[],
            ),
        }
    }
}

impl DecodedBlockCacheObserver for HeadStateCacheInstruments {
    fn evict(&self, decoded_bytes: usize) {
        self.evictions.increment(1);
        self.evicted_decoded_bytes
            .increment(metric_count(decoded_bytes));
    }

    fn reject(&self, decoded_bytes: usize) {
        self.rejections.increment(1);
        self.rejected_decoded_bytes
            .increment(metric_count(decoded_bytes));
    }

    fn retained(&self, decoded_bytes: usize) {
        self.retained_decoded_bytes.set(metric_level(decoded_bytes));
    }
}

fn metric_count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn metric_level(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The gauges and durable totals for this process's streaming compactions.
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
                "Rows processed by streaming metadata compactions",
                &[("direction", direction)],
            )
        };
        let bytes = |direction: &'static str| {
            recorder.register_counter(
                "loonfs.maintenance.compaction_bytes",
                "Bytes processed by streaming metadata compactions",
                &[("direction", direction)],
            )
        };
        Self {
            outcomes: LabeledCounters::register(
                recorder,
                "loonfs.maintenance.compactions",
                "Streaming metadata compactions by outcome",
                "outcome",
                &[],
            ),
            outcome_seconds: CompactionOutcome::VALUES
                .iter()
                .map(|outcome| {
                    recorder.register_histogram(
                        "loonfs.maintenance.compaction_seconds",
                        "Duration of a finished streaming metadata compaction in seconds",
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
    wal_folds_waiting: Arc<dyn GaugeHandle>,
    wal_fold_seconds: Arc<dyn HistogramHandle>,
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
            wal_folds_waiting: recorder.register_gauge(
                "loonfs.publisher.wal_folds_waiting",
                "WAL-tail folds waiting for a writer permit",
                &[],
            ),
            wal_fold_seconds: recorder.register_histogram(
                "loonfs.publisher.wal_fold_seconds",
                "Duration of a WAL-tail fold in seconds",
                &[],
                LATENCY_SECONDS_BOUNDARIES,
            ),
            write_stop_refusals: recorder.register_counter(
                "loonfs.publisher.write_stop_refusals",
                "Mutation batches refused because the WAL tail reached its write-stop bound",
                &[],
            ),
            tail_replays: recorder.register_counter(
                "loonfs.publisher.tail_replays",
                "Publishes that reread the WAL tail from the store instead of finding it in the head-state cache",
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

    fn sample(
        operation: ObjectStoreOperation,
        result: ObjectStoreResultClass,
        attempts: u32,
    ) -> ObjectStoreMetricSample {
        classified_sample(operation, result, KeyClass::Content, attempts)
    }

    fn classified_sample(
        operation: ObjectStoreOperation,
        result: ObjectStoreResultClass,
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

    fn gauge(snapshot: &MetricsSnapshot, name: &str) -> i64 {
        let entry = snapshot
            .by_name(name)
            .next()
            .unwrap_or_else(|| panic!("no `{name}` registered"));
        match entry.value {
            MetricValue::Gauge(value) => value,
            ref other => panic!("expected a gauge, found {other:?}"),
        }
    }

    #[test]
    fn view_and_cache_events_reach_the_registered_instruments() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));
        instruments.latest_metadata_view_read();
        instruments.snapshot_view_read();
        let cache = MetadataCacheInstruments::new(Some(recorder.as_ref()));

        let metadata = cache
            .metadata_segment_cache_observer()
            .expect("metadata observer");
        metadata.hit();
        metadata.miss();
        metadata.insert();
        metadata.evict(0);
        metadata.filter_skip();
        metadata.filter_false_positive();
        metadata.retained(170);

        let wal = cache
            .wal_tail_projection_cache_observer()
            .expect("WAL-tail observer");
        wal.hit();
        wal.miss();
        wal.insert();

        let head_state = cache
            .head_state_cache_observer()
            .expect("head-state observer");
        head_state.evict(70);
        head_state.reject(110);
        head_state.retained(130);

        let snapshot = recorder.snapshot();
        for (name, labels, value) in [
            (
                "loonfs.runtime_cache.latest_metadata_view_reads",
                &[][..],
                1,
            ),
            ("loonfs.runtime_cache.snapshot_view_reads", &[][..], 1),
            (
                "loonfs.metadata_segment_cache.gets",
                &[("result", "hit")][..],
                1,
            ),
            (
                "loonfs.metadata_segment_cache.gets",
                &[("result", "miss")][..],
                1,
            ),
            ("loonfs.metadata_segment_cache.inserts", &[][..], 1),
            ("loonfs.metadata_segment_cache.evictions", &[][..], 1),
            ("loonfs.metadata_segment_cache.filter_skips", &[][..], 1),
            (
                "loonfs.metadata_segment_cache.filter_false_positives",
                &[][..],
                1,
            ),
            (
                "loonfs.wal_tail_projection_cache.gets",
                &[("result", "hit")][..],
                1,
            ),
            (
                "loonfs.wal_tail_projection_cache.gets",
                &[("result", "miss")][..],
                1,
            ),
            ("loonfs.wal_tail_projection_cache.inserts", &[][..], 1),
            ("loonfs.head_state_cache.evictions", &[][..], 1),
            ("loonfs.head_state_cache.evicted_decoded_bytes", &[][..], 70),
            ("loonfs.head_state_cache.rejections", &[][..], 1),
            (
                "loonfs.head_state_cache.rejected_decoded_bytes",
                &[][..],
                110,
            ),
        ] {
            assert_eq!(counter(&snapshot, name, labels), value, "counter `{name}`");
        }
        assert_eq!(
            gauge(
                &snapshot,
                "loonfs.metadata_segment_cache.retained_decoded_bytes"
            ),
            170
        );
        assert_eq!(
            gauge(&snapshot, "loonfs.head_state_cache.retained_decoded_bytes"),
            130
        );
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
            (ObjectStoreResultClass::Ok, KeyClass::WalObject),
            (ObjectStoreResultClass::NotFound, KeyClass::WalObject),
            (ObjectStoreResultClass::NotFound, KeyClass::WalObject),
            (ObjectStoreResultClass::Ok, KeyClass::NamespaceManifest),
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
            ObjectStoreResultClass::Ok,
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
        instruments.publisher_wal_folds_waiting(1);
        instruments.publisher_wal_fold_duration(5);
        instruments.publisher_write_stop_refusal();
        instruments.publisher_publish(PublishOutcome::Ok);
    }

    #[test]
    fn compaction_instruments_register_the_closed_vocabularies() {
        let recorder = Arc::new(DefaultMetricsRecorder::new());
        let instruments = RuntimeInstruments::new(Some(recorder.clone()));

        instruments.compaction_finished(
            &Ok(MetadataCompactionJobOutcome::Published {
                manifest_no: loonfs_types::ManifestNo(1),
                rows_read: 20,
                rows_written: 10,
                input_bytes: 2_000,
                output_bytes: 1_000,
                output_segments: 1,
            }),
            25,
        );
        instruments.compaction_finished(&Ok(MetadataCompactionJobOutcome::Abandoned), 10);

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
        assert!(matches!(
            duration.value,
            MetricValue::Histogram { count: 1, .. }
        ));
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
                ..loonfs_types::DeletedObjectCounts::default()
            },
            deleted_checkpoints_by_owner: loonfs_types::DeletedCheckpointsByOwner {
                fork: 2,
                user: 3,
                snapshot: 7,
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
        assert_eq!(snapshot.by_name("loonfs.gc.reclaimed").count(), 8);
        for (category, expected) in [
            ("deleted_fork_checkpoints", 4),
            ("deleted_expired_checkpoints", 6),
            ("deleted_snapshot_checkpoints", 14),
        ] {
            assert_eq!(
                counter(&snapshot, "loonfs.gc.reclaimed", &[("category", category)]),
                expected
            );
        }
    }
}
