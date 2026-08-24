//! Runtime metrics API.
//!
//! Embedders can implement [`MetricsRecorder`] for an existing registry or
//! use [`DefaultMetricsRecorder`] to collect a [`MetricsSnapshot`]. Without a
//! recorder, the runtime does not register or update instruments.
//!
//! Metric names use `loonfs.<subsystem>.<metric>`. Names and labels are static
//! strings so request IDs, paths, and other unbounded values cannot become
//! labels. This module also re-exports the object-store metrics API.

mod instruments;

pub use loonfs_objectstore::metrics::{
    InstrumentedObjectStore, JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricSample,
    ObjectStoreMetricsRecorder, ObjectStoreOperation, ObjectStoreResultClass, PutModeClass,
    RangeClass, VecObjectStoreMetricsRecorder,
};

pub(crate) use instruments::{fan_out_object_store_recorder, PublishOutcome, RuntimeInstruments};

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// The closed vocabulary of the `result` label: an operation that completed.
///
/// Every instrument that labels an outcome `result` takes its value from one
/// of these four constants, so one query counts every failure the runtime and
/// its hosts report. The object store keeps its own richer classification for
/// the same label: a store call reports which failure it hit, not only that
/// it failed.
pub const RESULT_OK: &str = "ok";
/// `result` label for an operation that failed.
pub const RESULT_ERROR: &str = "error";
/// `result` label for a lookup served from the cache.
pub const RESULT_HIT: &str = "hit";
/// `result` label for a lookup the cache did not hold.
pub const RESULT_MISS: &str = "miss";

/// Bucket upper bounds for a latency histogram, in seconds.
///
/// Covers latencies from one millisecond to two minutes.
pub const LATENCY_SECONDS_BOUNDARIES: &[f64] = &[
    0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 60.0, 120.0,
];

/// Bucket upper bounds for histograms over small counts.
pub const SMALL_COUNT_BOUNDARIES: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0];

/// Registers instruments used by the runtime.
///
/// Embedders may adapt this trait to an existing metrics registry. The runtime
/// registers each instrument once and uses the returned handles on hot paths.
///
/// Registering the same `(name, labels)` pair twice returns a handle to the
/// same underlying value.
///
/// Static names and labels prevent unbounded runtime values from increasing
/// metric cardinality.
pub trait MetricsRecorder: Send + Sync + 'static {
    /// Registers a monotonically increasing count.
    fn register_counter(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn CounterHandle>;

    /// Registers a value that rises and falls.
    fn register_gauge(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn GaugeHandle>;

    /// Registers a distribution over `boundaries`, which are inclusive
    /// bucket upper bounds in ascending order.
    fn register_histogram(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
        boundaries: &'static [f64],
    ) -> Arc<dyn HistogramHandle>;
}

/// A registered counter.
pub trait CounterHandle: Send + Sync {
    /// Adds `value` to the count.
    fn increment(&self, value: u64);
}

/// A registered gauge.
pub trait GaugeHandle: Send + Sync {
    /// Replaces the reported value.
    fn set(&self, value: i64);
}

/// A registered histogram.
pub trait HistogramHandle: Send + Sync {
    /// Files one observation.
    fn record(&self, value: f64);
}

/// The recorder that discards everything, and the runtime's default.
///
/// Its handles are shared singletons whose methods are empty, so a runtime
/// built without a recorder pays one virtual call per report and allocates
/// nothing.
#[derive(Debug, Default)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn register_counter(
        &self,
        _name: &'static str,
        _description: &'static str,
        _labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn CounterHandle> {
        Arc::new(NoopInstrument)
    }

    fn register_gauge(
        &self,
        _name: &'static str,
        _description: &'static str,
        _labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn GaugeHandle> {
        Arc::new(NoopInstrument)
    }

    fn register_histogram(
        &self,
        _name: &'static str,
        _description: &'static str,
        _labels: &[(&'static str, &'static str)],
        _boundaries: &'static [f64],
    ) -> Arc<dyn HistogramHandle> {
        Arc::new(NoopInstrument)
    }
}

struct NoopInstrument;

impl CounterHandle for NoopInstrument {
    fn increment(&self, _value: u64) {}
}

impl GaugeHandle for NoopInstrument {
    fn set(&self, _value: i64) {}
}

impl HistogramHandle for NoopInstrument {
    fn record(&self, _value: f64) {}
}

/// Identity of one registered instrument: its name and its label set.
///
/// Labels are sorted at registration, so two registrations that name the
/// same labels in different orders are the same instrument.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct InstrumentKey {
    name: &'static str,
    labels: Vec<(&'static str, &'static str)>,
}

impl InstrumentKey {
    fn new(name: &'static str, labels: &[(&'static str, &'static str)]) -> Self {
        let mut labels = labels.to_vec();
        labels.sort_unstable();
        Self { name, labels }
    }
}

/// In-process metrics recorder backed by atomic values.
#[derive(Debug, Default)]
pub struct DefaultMetricsRecorder {
    registry: Mutex<BTreeMap<InstrumentKey, RegisteredInstrument>>,
}

#[derive(Debug, Clone)]
enum RegisteredInstrument {
    Counter(Arc<CounterValue>),
    Gauge(Arc<GaugeValue>),
    Histogram(Arc<HistogramValue>),
}

impl DefaultMetricsRecorder {
    /// Creates an empty recorder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads every registered instrument's current value.
    ///
    /// Instruments are read independently, so a snapshot is not atomic across
    /// all values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let registry = self.lock_registry();
        MetricsSnapshot {
            entries: registry
                .iter()
                .map(|(key, instrument)| MetricEntry {
                    name: key.name,
                    labels: key.labels.clone(),
                    description: instrument.description(),
                    value: instrument.value(),
                })
                .collect(),
        }
    }

    // A poisoned registry is recovered rather than propagated: every
    // critical section here inserts one map entry, and one panicked
    // registration must not take the runtime's metrics down with it.
    fn lock_registry(
        &self,
    ) -> std::sync::MutexGuard<'_, BTreeMap<InstrumentKey, RegisteredInstrument>> {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns the instrument registered under `key`, installing `build`'s
    /// if there is none.
    ///
    /// A second registration of the same name and labels shares the first
    /// one's value. A registration that reuses a name with a different
    /// instrument kind keeps the first kind and hands back a detached
    /// instrument, so a mistake costs the new instrument's readings rather
    /// than corrupting the established one.
    fn register(
        &self,
        key: InstrumentKey,
        build: impl FnOnce() -> RegisteredInstrument,
    ) -> RegisteredInstrument {
        self.lock_registry()
            .entry(key)
            .or_insert_with(build)
            .clone()
    }
}

impl RegisteredInstrument {
    fn description(&self) -> &'static str {
        match self {
            Self::Counter(counter) => counter.description,
            Self::Gauge(gauge) => gauge.description,
            Self::Histogram(histogram) => histogram.description,
        }
    }

    fn value(&self) -> MetricValue {
        match self {
            Self::Counter(counter) => MetricValue::Counter(counter.value.load(Ordering::Relaxed)),
            Self::Gauge(gauge) => MetricValue::Gauge(gauge.value.load(Ordering::Relaxed)),
            Self::Histogram(histogram) => histogram.value(),
        }
    }
}

impl MetricsRecorder for DefaultMetricsRecorder {
    fn register_counter(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn CounterHandle> {
        let fresh = Arc::new(CounterValue::new(description));
        match self.register(InstrumentKey::new(name, labels), || {
            RegisteredInstrument::Counter(Arc::clone(&fresh))
        }) {
            RegisteredInstrument::Counter(counter) => counter,
            _ => fresh,
        }
    }

    fn register_gauge(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
    ) -> Arc<dyn GaugeHandle> {
        let fresh = Arc::new(GaugeValue::new(description));
        match self.register(InstrumentKey::new(name, labels), || {
            RegisteredInstrument::Gauge(Arc::clone(&fresh))
        }) {
            RegisteredInstrument::Gauge(gauge) => gauge,
            _ => fresh,
        }
    }

    fn register_histogram(
        &self,
        name: &'static str,
        description: &'static str,
        labels: &[(&'static str, &'static str)],
        boundaries: &'static [f64],
    ) -> Arc<dyn HistogramHandle> {
        let fresh = Arc::new(HistogramValue::new(description, boundaries));
        match self.register(InstrumentKey::new(name, labels), || {
            RegisteredInstrument::Histogram(Arc::clone(&fresh))
        }) {
            RegisteredInstrument::Histogram(histogram) => histogram,
            _ => fresh,
        }
    }
}

#[derive(Debug)]
struct CounterValue {
    description: &'static str,
    value: AtomicU64,
}

impl CounterValue {
    fn new(description: &'static str) -> Self {
        Self {
            description,
            value: AtomicU64::new(0),
        }
    }
}

impl CounterHandle for CounterValue {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct GaugeValue {
    description: &'static str,
    value: AtomicI64,
}

impl GaugeValue {
    fn new(description: &'static str) -> Self {
        Self {
            description,
            value: AtomicI64::new(0),
        }
    }
}

impl GaugeHandle for GaugeValue {
    fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }
}

/// A fixed-boundary histogram over atomics.
///
/// The sum accumulates as a fixed-point integer stepping by one millionth
/// rather than as a float: every value this runtime files is a duration in
/// seconds or a small count, both of which that step holds exactly at the
/// scales involved, and integer addition needs no compare-and-swap loop the
/// way a float sum would. A negative observation cannot arise from any
/// instrument here and contributes nothing to the sum if one ever does.
#[derive(Debug)]
struct HistogramValue {
    description: &'static str,
    boundaries: &'static [f64],
    /// One counter per boundary plus one for the overflow bucket.
    bucket_counts: Vec<AtomicU64>,
    count: AtomicU64,
    sum_millionths: AtomicU64,
}

impl HistogramValue {
    fn new(description: &'static str, boundaries: &'static [f64]) -> Self {
        Self {
            description,
            boundaries,
            bucket_counts: (0..=boundaries.len()).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_millionths: AtomicU64::new(0),
        }
    }

    fn value(&self) -> MetricValue {
        MetricValue::Histogram {
            boundaries: self.boundaries,
            bucket_counts: self
                .bucket_counts
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum: self.sum_millionths.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        }
    }
}

impl HistogramHandle for HistogramValue {
    fn record(&self, value: f64) {
        let bucket = self
            .boundaries
            .iter()
            .position(|boundary| value <= *boundary)
            .unwrap_or(self.boundaries.len());
        self.bucket_counts[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        if value > 0.0 {
            let millionths = (value * 1_000_000.0).round().min(u64::MAX as f64) as u64;
            self.sum_millionths.fetch_add(millionths, Ordering::Relaxed);
        }
    }
}

/// One instrument's current reading.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// A monotonically increasing count.
    Counter(u64),
    /// A value that rises and falls.
    Gauge(i64),
    /// A fixed-boundary distribution.
    Histogram {
        /// Inclusive bucket upper bounds, ascending.
        boundaries: &'static [f64],
        /// Observations per bucket, one longer than `boundaries`: the last
        /// entry counts observations above the highest boundary.
        bucket_counts: Vec<u64>,
        /// Observations filed.
        count: u64,
        /// Sum of the observed values.
        sum: f64,
    },
}

/// One instrument in a snapshot.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricEntry {
    /// The instrument's dotted name.
    pub name: &'static str,
    /// Its label set, sorted by key.
    pub labels: Vec<(&'static str, &'static str)>,
    /// What the instrument measures, as given at registration.
    pub description: &'static str,
    /// The reading.
    pub value: MetricValue,
}

/// Every instrument a [`DefaultMetricsRecorder`] holds, read at one moment.
///
/// Entries are ordered by name and then by labels, so a host that renders a
/// snapshot renders it deterministically without sorting again.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricsSnapshot {
    entries: Vec<MetricEntry>,
}

impl MetricsSnapshot {
    /// Every entry, ordered by name and then by labels.
    pub fn all(&self) -> &[MetricEntry] {
        &self.entries
    }

    /// The entries registered under `name`, one per label set.
    pub fn by_name<'snapshot>(
        &'snapshot self,
        name: &'snapshot str,
    ) -> impl Iterator<Item = &'snapshot MetricEntry> + 'snapshot {
        self.entries.iter().filter(move |entry| entry.name == name)
    }
}

#[cfg(test)]
mod tests;
