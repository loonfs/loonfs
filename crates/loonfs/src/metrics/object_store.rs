//! The object-store instruments of a runtime, which turn object-store
//! samples into the runtime's metrics, and the recorder that feeds one
//! sample to two sinks.

use super::{
    CounterHandle, HistogramHandle, MetricsRecorder, ObjectStoreMetricSample,
    ObjectStoreMetricsRecorder, LATENCY_SECONDS_BOUNDARIES,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The object-store instruments of one runtime, registered as each
/// operation, outcome, and key class first appears.
pub(crate) struct ObjectStoreInstruments {
    recorder: Arc<dyn MetricsRecorder>,
    operations: Mutex<HashMap<&'static str, ObjectStoreOperationInstruments>>,
}

impl ObjectStoreInstruments {
    pub(crate) fn new(recorder: Arc<dyn MetricsRecorder>) -> Self {
        Self {
            recorder,
            operations: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the handles one object-store sample reports through. A missing
    /// handle is registered before the registry lock is taken, because the
    /// recorder is caller code. When two first samples race, the handle
    /// stored first is kept.
    fn reading(
        &self,
        operation: &'static str,
        result: &'static str,
        key_class: &'static str,
    ) -> ObjectStoreReading {
        let operation_registered = match self.lock_operations().get(operation) {
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
        let mut registry = self.lock_operations();
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

    fn lock_operations(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<&'static str, ObjectStoreOperationInstruments>> {
        self.operations
            .lock()
            .expect("the object-store instrument lock should not be poisoned")
    }
}

impl ObjectStoreMetricsRecorder for ObjectStoreInstruments {
    fn record(&self, sample: ObjectStoreMetricSample) {
        let instruments = self.reading(
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

/// Composes the object-store recorder a handle installs on its store.
///
/// A handle may be given a recorder for the raw samples, a general recorder
/// the samples are bridged into, both, or neither. Both means one wrapper
/// feeding two sinks rather than two wrappers double-counting the same call,
/// and neither means no wrapper at all, so an uninstrumented handle reads
/// the store directly.
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
/// a recorder's own reporting never runs while the runtime keeps it locked.
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
