use crate::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One object-store call sample delivered to an object-store metrics recorder.
///
/// Samples intentionally classify keys and errors instead of exposing raw object keys or provider
/// error strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectStoreMetricSample {
    pub operation: ObjectStoreOperation,
    pub elapsed_micros: u128,
    pub result: ObjectStoreResultClass,
    pub bytes_in: Option<u64>,
    pub bytes_out: Option<u64>,
    pub item_count: Option<u64>,
    pub key_class: KeyClass,
    pub range_class: Option<RangeClass>,
    pub put_mode: Option<PutModeClass>,
    pub store_kind: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreOperation {
    Head,
    HeadWithChecksum,
    Get,
    Put,
    Delete,
    ListPrefix,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreResultClass {
    Ok,
    NotFound,
    InvalidKey,
    InvalidRange,
    PreconditionFailed,
    Conflict,
    Unsupported,
    Transport,
    OtherError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyClass {
    Content,
    Metadata,
    NamespaceHead,
    Lease,
    DerivedProgress,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeClass {
    FullObject,
    Prefix,
    Suffix,
    Bounded,
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PutModeClass {
    Overwrite,
    CreateIfAbsent,
    CompareAndSwap,
}

/// Receives object-store metrics samples from `InstrumentedObjectStore`.
///
/// Implementations should aggregate or export samples without blocking the object-store hot path.
pub trait ObjectStoreMetricsRecorder: Send + Sync + 'static {
    fn record(&self, sample: ObjectStoreMetricSample);
}

/// In-memory recorder intended for tests and small local diagnostics.
#[derive(Default)]
pub struct VecObjectStoreMetricsRecorder {
    samples: Mutex<Vec<ObjectStoreMetricSample>>,
}

impl VecObjectStoreMetricsRecorder {
    pub fn samples(&self) -> Vec<ObjectStoreMetricSample> {
        self.samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl ObjectStoreMetricsRecorder for VecObjectStoreMetricsRecorder {
    fn record(&self, sample: ObjectStoreMetricSample) {
        self.samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(sample);
    }
}

/// Buffered JSONL recorder for process-level diagnostics and benchmarks.
///
/// Recording is best-effort: I/O errors while writing a sample are ignored so instrumentation does
/// not change object-store behavior.
pub struct JsonlObjectStoreMetricsRecorder {
    writer: Mutex<BufWriter<File>>,
}

impl JsonlObjectStoreMetricsRecorder {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(Self {
            writer: Mutex::new(BufWriter::new(File::create(path)?)),
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .flush()
    }
}

impl ObjectStoreMetricsRecorder for JsonlObjectStoreMetricsRecorder {
    fn record(&self, sample: ObjectStoreMetricSample) {
        let mut writer = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = serde_json::to_writer(&mut *writer, &sample);
        let _ = writer.write_all(b"\n");
    }
}

pub struct InstrumentedObjectStore<S> {
    inner: S,
    recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    store_kind: Option<String>,
}

impl<S> InstrumentedObjectStore<S> {
    pub fn new(inner: S, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        Self {
            inner,
            recorder,
            store_kind: None,
        }
    }

    pub fn with_store_kind(mut self, store_kind: impl Into<String>) -> Self {
        self.store_kind = Some(store_kind.into());
        self
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> ObjectStore for InstrumentedObjectStore<S>
where
    S: ObjectStore,
{
    fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head(key);
        self.record_head_like(ObjectStoreOperation::Head, key, start.elapsed(), &result);
        result
    }

    fn head_with_checksum(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head_with_checksum(key);
        self.record_head_like(
            ObjectStoreOperation::HeadWithChecksum,
            key,
            start.elapsed(),
            &result,
        );
        result
    }

    fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.get(key, range.clone());
        self.record_get(key, range.as_ref(), start.elapsed(), &result);
        result
    }

    fn put(
        &self,
        key: &str,
        bytes: &[u8],
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put(key, bytes, mode.clone());
        self.record_put(key, bytes.len() as u64, &mode, start.elapsed(), &result);
        result
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put_overwrite(key, bytes);
        self.record_put(
            key,
            bytes.len() as u64,
            &PutMode::Overwrite,
            start.elapsed(),
            &result,
        );
        result
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.put_if_absent(key, bytes);
        self.record_put(
            key,
            bytes.len() as u64,
            &PutMode::CreateIfAbsent,
            start.elapsed(),
            &result,
        );
        result
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: &[u8],
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let mode = PutMode::CompareAndSwap {
            expected_etag: expected_etag.to_owned(),
        };
        let result = self.inner.compare_and_swap(key, expected_etag, bytes);
        self.record_put(key, bytes.len() as u64, &mode, start.elapsed(), &result);
        result
    }

    fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.delete(key);
        self.record_unit(ObjectStoreOperation::Delete, key, start.elapsed(), &result);
        result
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.list_prefix(prefix);
        self.record_list(prefix, start.elapsed(), &result);
        result
    }
}

impl<S> InstrumentedObjectStore<S> {
    fn record_head_like(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        elapsed: Duration,
        result: &Result<Option<ObjectMetadata>, ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation,
            elapsed_micros: elapsed.as_micros(),
            result: classify_optional_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: None,
            key_class: classify_key(key),
            range_class: None,
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_get(
        &self,
        key: &str,
        range: Option<&ByteRange>,
        elapsed: Duration,
        result: &Result<Option<Vec<u8>>, ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation: ObjectStoreOperation::Get,
            elapsed_micros: elapsed.as_micros(),
            result: classify_optional_result(result),
            bytes_in: None,
            bytes_out: result
                .as_ref()
                .ok()
                .and_then(|bytes| bytes.as_ref().map(|bytes| bytes.len() as u64)),
            item_count: None,
            key_class: classify_key(key),
            range_class: Some(classify_range(range)),
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_put(
        &self,
        key: &str,
        bytes_in: u64,
        mode: &PutMode,
        elapsed: Duration,
        result: &Result<ObjectMetadata, ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation: ObjectStoreOperation::Put,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: Some(bytes_in),
            bytes_out: None,
            item_count: None,
            key_class: classify_key(key),
            range_class: None,
            put_mode: Some(classify_put_mode(mode)),
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_unit(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        elapsed: Duration,
        result: &Result<(), ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: None,
            key_class: classify_key(key),
            range_class: None,
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record_list(
        &self,
        prefix: &str,
        elapsed: Duration,
        result: &Result<Vec<String>, ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation: ObjectStoreOperation::ListPrefix,
            elapsed_micros: elapsed.as_micros(),
            result: classify_result(result),
            bytes_in: None,
            bytes_out: None,
            item_count: result.as_ref().ok().map(|items| items.len() as u64),
            key_class: classify_key(prefix),
            range_class: Some(RangeClass::Prefix),
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }

    fn record(&self, sample: ObjectStoreMetricSample) {
        self.recorder.record(sample);
    }
}

fn classify_key(key: &str) -> KeyClass {
    let segments = key.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        ["content-stores", _, "blobs", ..] => KeyClass::Content,
        ["content-stores", ..] => KeyClass::Metadata,
        ["namespaces", _, "head.json"] => KeyClass::NamespaceHead,
        ["namespaces", _, "lease.json"] => KeyClass::Lease,
        ["namespaces", _, "derived", .., "progress.json"] => KeyClass::DerivedProgress,
        ["namespaces", ..] | ["queue", ..] => KeyClass::Metadata,
        _ => KeyClass::Unknown,
    }
}

fn classify_optional_result<T>(
    result: &Result<Option<T>, ObjectStoreError>,
) -> ObjectStoreResultClass {
    match result {
        Ok(Some(_)) => ObjectStoreResultClass::Ok,
        Ok(None) => ObjectStoreResultClass::NotFound,
        Err(error) => classify_error(error),
    }
}

fn classify_result<T>(result: &Result<T, ObjectStoreError>) -> ObjectStoreResultClass {
    match result {
        Ok(_) => ObjectStoreResultClass::Ok,
        Err(error) => classify_error(error),
    }
}

fn classify_error(error: &ObjectStoreError) -> ObjectStoreResultClass {
    match error {
        ObjectStoreError::NotFound => ObjectStoreResultClass::NotFound,
        ObjectStoreError::InvalidKey(_) => ObjectStoreResultClass::InvalidKey,
        ObjectStoreError::InvalidRange => ObjectStoreResultClass::InvalidRange,
        ObjectStoreError::PreconditionFailed => ObjectStoreResultClass::PreconditionFailed,
        ObjectStoreError::Conflict => ObjectStoreResultClass::Conflict,
        ObjectStoreError::Unsupported(_) => ObjectStoreResultClass::Unsupported,
        ObjectStoreError::Transport(_) => ObjectStoreResultClass::Transport,
    }
}

fn classify_range(range: Option<&ByteRange>) -> RangeClass {
    let Some(range) = range else {
        return RangeClass::FullObject;
    };
    if range.start_inclusive == range.end_exclusive {
        RangeClass::Empty
    } else if range.start_inclusive == 0 {
        RangeClass::Prefix
    } else if range.end_exclusive == u64::MAX {
        RangeClass::Suffix
    } else {
        RangeClass::Bounded
    }
}

fn classify_put_mode(mode: &PutMode) -> PutModeClass {
    match mode {
        PutMode::Overwrite => PutModeClass::Overwrite,
        PutMode::CreateIfAbsent => PutModeClass::CreateIfAbsent,
        PutMode::CompareAndSwap { .. } => PutModeClass::CompareAndSwap,
    }
}
