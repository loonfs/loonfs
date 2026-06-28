use crate::layout::{parse_object_key, DurableObjectFamily};
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::fmt;
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
    GetWithMetadata,
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
    InvalidContentRef,
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
    WalSegment,
    NamespaceManifest,
    MetadataSst,
    GcControl,
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

impl<S: fmt::Debug> fmt::Debug for InstrumentedObjectStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstrumentedObjectStore")
            .field("inner", &self.inner)
            .field("store_kind", &self.store_kind)
            .finish_non_exhaustive()
    }
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

// Instrumentation is the explicit timing boundary for object-store latency metrics.
#[async_trait]
#[allow(clippy::disallowed_methods)]
impl<S> ObjectStore for InstrumentedObjectStore<S>
where
    S: ObjectStore,
{
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head(key).await;
        self.record_head_like(ObjectStoreOperation::Head, key, start.elapsed(), &result);
        result
    }

    async fn head_with_checksum(
        &self,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.head_with_checksum(key).await;
        self.record_head_like(
            ObjectStoreOperation::HeadWithChecksum,
            key,
            start.elapsed(),
            &result,
        );
        result
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.get(key, range.clone()).await;
        self.record_get(key, range.as_ref(), start.elapsed(), &result);
        result
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.get_with_metadata(key).await;
        self.record_get_with_metadata(key, start.elapsed(), &result);
        result
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let bytes_in = bytes.len() as u64;
        let result = self.inner.put(key, bytes, mode.clone()).await;
        self.record_put(key, bytes_in, &mode, start.elapsed(), &result);
        result
    }

    async fn put_overwrite(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let bytes_in = bytes.len() as u64;
        let result = self.inner.put_overwrite(key, bytes).await;
        self.record_put(key, bytes_in, &PutMode::Overwrite, start.elapsed(), &result);
        result
    }

    async fn put_if_absent(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let bytes_in = bytes.len() as u64;
        let result = self.inner.put_if_absent(key, bytes).await;
        self.record_put(
            key,
            bytes_in,
            &PutMode::CreateIfAbsent,
            start.elapsed(),
            &result,
        );
        result
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_etag: &str,
        bytes: Bytes,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        let start = Instant::now();
        let bytes_in = bytes.len() as u64;
        let result = self.inner.compare_and_swap(key, expected_etag, bytes).await;
        self.record_put(
            key,
            bytes_in,
            &PutMode::CompareAndSwap {
                expected_etag: expected_etag.to_owned(),
            },
            start.elapsed(),
            &result,
        );
        result
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        let start = Instant::now();
        let result = self.inner.delete(key).await;
        self.record_unit(ObjectStoreOperation::Delete, key, start.elapsed(), &result);
        result
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
        let start = Instant::now();
        let result: Result<Vec<_>, _> = self
            .inner
            .list_prefix_stream(prefix)
            .try_collect()
            .await
            .map(|mut keys: Vec<String>| {
                keys.sort();
                keys
            });
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
        result: &Result<Option<Bytes>, ObjectStoreError>,
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

    fn record_get_with_metadata(
        &self,
        key: &str,
        elapsed: Duration,
        result: &Result<Option<ObjectBody>, ObjectStoreError>,
    ) {
        self.record(ObjectStoreMetricSample {
            operation: ObjectStoreOperation::GetWithMetadata,
            elapsed_micros: elapsed.as_micros(),
            result: classify_optional_result(result),
            bytes_in: None,
            bytes_out: result
                .as_ref()
                .ok()
                .and_then(|body| body.as_ref().map(|body| body.bytes.len() as u64)),
            item_count: None,
            key_class: classify_key(key),
            range_class: Some(RangeClass::FullObject),
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
    let Some(parsed) = parse_object_key(key) else {
        return KeyClass::Unknown;
    };

    match parsed.family() {
        DurableObjectFamily::ContentBlob => KeyClass::Content,
        DurableObjectFamily::NamespaceHead => KeyClass::NamespaceHead,
        DurableObjectFamily::WalSegment => KeyClass::WalSegment,
        DurableObjectFamily::NamespaceManifest => KeyClass::NamespaceManifest,
        DurableObjectFamily::CompactedMetadataSst => KeyClass::MetadataSst,
        DurableObjectFamily::NamespaceGcBoundary | DurableObjectFamily::GcPin => {
            KeyClass::GcControl
        }
        DurableObjectFamily::NamespaceDescriptor
        | DurableObjectFamily::UploadSession
        | DurableObjectFamily::ContentStoreDescriptor => KeyClass::Metadata,
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
        ObjectStoreError::InvalidContentRef(_) => ObjectStoreResultClass::InvalidContentRef,
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
