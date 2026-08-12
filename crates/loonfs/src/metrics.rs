//! The runtime's metrics surface: a push-based recorder the embedder
//! implements, and the instruments the runtime registers against it.
//!
//! LoonFS defines instruments and reports values. It does not export them,
//! and it depends on no metrics crate to do so. An embedder that already
//! has a metrics pipeline implements [`MetricsRecorder`] over its own
//! registry and the runtime's numbers land there directly; an embedder that
//! has none installs [`DefaultMetricsRecorder`], which aggregates in atomics
//! and hands back a [`MetricsSnapshot`] to render however the host likes.
//! Installing nothing costs nothing: with no recorder configured the runtime
//! registers no instruments and no hot path touches this module at all.
//!
//! The reference server is LoonFS's own first embedder — it installs the
//! default recorder and serves its snapshot as Prometheus text on
//! `GET /metrics`.
//!
//! ## Names and labels
//!
//! Metric names are `loonfs.<subsystem>.<metric>` — `loonfs.gc.reclaimed`,
//! `loonfs.maintenance.steps`. The subsystem is the part of the runtime that
//! owns the number, and it is the same word the module that reports it is
//! called.
//!
//! Every name, description, label key, and label value in this API is
//! `&'static str`, and that is the whole cardinality policy expressed in the
//! type system: a namespace id, a commit id, a path, or a request id cannot
//! be borrowed for `'static`, so it cannot become a label. Label values come
//! from closed enumerations' `as_str` methods and from nowhere else. There
//! is no escape hatch, deliberately — a metrics backend does not survive one
//! unbounded label, and "we will be careful" is not a mechanism.
//!
//! ## Object-store metrics
//!
//! This module also re-exports the object-store sampling vocabulary from
//! [`loonfs_objectstore::metrics`]. The two surfaces compose rather than
//! compete: a handle given a [`MetricsRecorder`] bridges every object-store
//! sample into the instruments below, and a handle given an
//! [`ObjectStoreMetricsRecorder`] as well receives the raw samples too.
//!
//! ## Deviations from the model this follows
//!
//! The design is SlateDB's (their RFC 0021), with two deliberate omissions:
//!
//! - **No up-down counter.** Nothing in the v1 instrument set needs one — a
//!   quantity that falls is a gauge here. Adding the instrument later is
//!   purely additive.
//! - **No metric levels.** SlateDB filters instruments by verbosity because
//!   it has enough of them for that to matter. This set is small enough to
//!   read whole, and a level knob nobody turns is a knob that rots.

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

/// Bucket upper bounds for a latency histogram, in seconds.
///
/// Twelve buckets from a millisecond to two minutes, stepping by roughly
/// three: that spans a warm cache hit at one end and a provider call about
/// to spend its whole operation deadline at the other, and no two adjacent
/// buckets are so close that a shifted distribution hides between them.
pub const LATENCY_SECONDS_BOUNDARIES: &[f64] = &[
    0.001, 0.003, 0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0, 30.0, 60.0, 120.0,
];

/// Bucket upper bounds for a histogram over small counts — how many
/// candidates a publication batched, and the like.
pub const SMALL_COUNT_BOUNDARIES: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0];

/// Where the runtime reports its instruments.
///
/// An embedder implements this over whatever registry it already has. The
/// runtime registers each instrument once, at handle construction, and then
/// only ever calls the handle it was given — so an implementation may do
/// real work in the `register_*` methods and must do as little as possible
/// in the handles', which run on hot paths.
///
/// Registering the same `(name, labels)` pair twice returns a handle to the
/// same underlying value. Two parts of the runtime reporting the same
/// instrument are reporting one number, not two.
///
/// Everything here is `&'static str` on purpose: it makes a runtime id
/// impossible to use as a name or a label value, which is the cardinality
/// policy this crate enforces in the type system rather than by convention.
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

/// The in-process recorder: every instrument is a few atomics, and
/// [`Self::snapshot`] reads them all.
///
/// This is what an embedder installs when it has no metrics pipeline of its
/// own and wants one anyway. Reporting is lock-free — registration takes a
/// lock, the handles never do — so hot paths pay an atomic add and nothing
/// else.
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
    /// The read is per instrument, not a stop-the-world barrier: two
    /// instruments in one snapshot may have been read a few nanoseconds
    /// apart. Nothing an operator asks of these numbers needs them to agree
    /// to that resolution.
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
