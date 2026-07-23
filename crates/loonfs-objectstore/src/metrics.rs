//! A metrics-recording wrapper around any object store: per-operation
//! samples classified by object family.

use crate::layout::{parse_object_key, DurableObjectFamily};
use crate::object_store::Result;
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
    /// Contract operation that produced the sample.
    pub operation: ObjectStoreOperation,
    /// End-to-end operation latency in microseconds, including provider waits.
    pub elapsed_micros: u128,
    /// Cardinality-bounded success or failure classification.
    pub result: ObjectStoreResultClass,
    /// Request payload bytes for a put, including failed attempts; otherwise `None`.
    pub bytes_in: Option<u64>,
    /// Response payload bytes for a successful get, including zero; otherwise `None`.
    pub bytes_out: Option<u64>,
    /// Keys yielded by a listing before completion or drop; otherwise `None`.
    pub item_count: Option<u64>,
    /// Durable-family grouping derived without retaining the raw key.
    pub key_class: KeyClass,
    /// Read-range or listing shape when applicable; otherwise `None`.
    pub range_class: Option<RangeClass>,
    /// Requested conditional-write class for a put; otherwise `None`.
    pub put_mode: Option<PutModeClass>,
    /// Deployment-supplied provider label, or `None` when the wrapper was left unlabeled.
    pub store_kind: Option<String>,
}

/// Classifies the method measured by one object-store sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreOperation {
    /// Measures a metadata-only point read.
    Head,
    /// Measures a self-consistent full-object read with identity metadata.
    GetWithMetadata,
    /// Measures a full or ranged byte read.
    Get,
    /// Measures an overwrite or conditional write.
    Put,
    /// Measures an idempotent object delete.
    Delete,
    /// Measures a listing collected to completion.
    ListPrefix,
    /// Measures a listing stream until completion or early drop.
    ListPrefixStream,
}

/// Collapses object-store outcomes into a bounded metrics vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreResultClass {
    /// Indicates the operation returned its requested value or mutation result.
    Ok,
    /// Indicates an optional read observed absence or an explicit lookup returned not found.
    NotFound,
    /// Indicates key or prefix validation failed before provider IO.
    InvalidKey,
    /// Indicates a content reference could not resolve to an immutable key.
    InvalidContentRef,
    /// Indicates requested byte-range bounds were invalid for the object.
    InvalidRange,
    /// Indicates a create-if-absent or compare-and-swap condition did not hold.
    PreconditionFailed,
    /// Indicates a test-injected concurrency conflict.
    Conflict,
    /// Indicates the provider rejected identity or authorization.
    PermissionDenied,
    /// Indicates the configured store lacks a required capability.
    Unsupported,
    /// Indicates configuration, IO, timeout, protocol, or provider transport failure.
    Transport,
    /// Reserves a forward-compatible bucket for errors outside the current registry.
    OtherError,
}

/// Groups durable keys into low-cardinality operational families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyClass {
    /// Groups immutable whole-file byte objects.
    Content,
    /// Groups small control records not assigned a more specific class.
    Metadata,
    /// Groups the authoritative namespace WAL head.
    NamespaceHead,
    /// Groups immutable WAL segment payloads.
    WalSegment,
    /// Groups namespace manifests and their mutable root pointer.
    NamespaceManifest,
    /// Groups immutable metadata SST segments.
    MetadataSst,
    /// Groups checkpoint records and retained-history floors consulted by garbage collection.
    GcControl,
    /// Groups unrecognized keys and coarse listing prefixes.
    Unknown,
}

/// Classifies the shape of bytes requested by a get or listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeClass {
    /// Indicates a get without explicit range bounds.
    FullObject,
    /// Indicates bytes starting at zero or a prefix-listing operation.
    Prefix,
    /// Indicates a range extending from a nonzero start to the sentinel maximum end.
    Suffix,
    /// Indicates a finite nonempty range with both bounds inside the keyspace.
    Bounded,
    /// Indicates equal range bounds and therefore a zero-byte read.
    Empty,
}

/// Classifies the precondition semantics requested by a put.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PutModeClass {
    /// Indicates an unconditional replacement.
    Overwrite,
    /// Indicates creation only while the key is absent.
    CreateIfAbsent,
    /// Indicates replacement only while an opaque compare token matches.
    CompareAndSwap,
}

/// Receives object-store metrics samples from `InstrumentedObjectStore`.
///
/// Implementations should aggregate or export samples without blocking the object-store hot path.
pub trait ObjectStoreMetricsRecorder: Send + Sync + 'static {
    /// Accepts one completed sample without access to raw keys or provider error text.
    fn record(&self, sample: ObjectStoreMetricSample);
}

/// In-memory recorder intended for tests and small local diagnostics.
#[derive(Default)]
pub struct VecObjectStoreMetricsRecorder {
    samples: Mutex<Vec<ObjectStoreMetricSample>>,
}

impl VecObjectStoreMetricsRecorder {
    /// Returns a snapshot copy of every sample recorded so far.
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
    /// Creates or truncates a JSONL output file, creating missing parent directories.
    ///
    /// The operation fails when directories or the output file cannot be created.
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

    /// Flushes buffered samples to the underlying file.
    ///
    /// The operation fails when the filesystem cannot accept pending bytes.
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

/// Records one bounded-cardinality sample around each operation on an inner store.
///
/// Results and storage semantics pass through unchanged; streamed listings emit
/// their sample when the stream is completed or dropped.
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
    /// Wraps a store with the supplied synchronous recorder and no provider label.
    pub fn new(inner: S, recorder: Arc<dyn ObjectStoreMetricsRecorder>) -> Self {
        Self {
            inner,
            recorder,
            store_kind: None,
        }
    }

    /// Attaches a low-cardinality provider label copied into subsequent samples.
    pub fn store_kind(mut self, store_kind: impl Into<String>) -> Self {
        self.store_kind = Some(store_kind.into());
        self
    }

    /// Removes instrumentation and returns ownership of the wrapped store.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

#[allow(clippy::disallowed_methods)]
fn sample_clock() -> Instant {
    // Metrics sampling reads the wall clock at this recorder boundary so
    // protocol replay stays deterministic.
    Instant::now()
}

#[async_trait]
impl<S> ObjectStore for InstrumentedObjectStore<S>
where
    S: ObjectStore,
{
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let start = sample_clock();
        let result = self.inner.head(key).await;
        self.record_head_like(ObjectStoreOperation::Head, key, start.elapsed(), &result);
        result
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        let start = sample_clock();
        let result = self.inner.get(key, range.clone()).await;
        self.record_get(key, range.as_ref(), start.elapsed(), &result);
        result
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        let start = sample_clock();
        let result = self.inner.get_with_metadata(key).await;
        self.record_get_with_metadata(key, start.elapsed(), &result);
        result
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let start = sample_clock();
        let bytes_in = bytes.len() as u64;
        let result = self.inner.put(key, bytes, mode.clone()).await;
        self.record_put(key, bytes_in, &mode, start.elapsed(), &result);
        result
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let start = sample_clock();
        let result = self.inner.delete(key).await;
        self.record_unit(ObjectStoreOperation::Delete, key, start.elapsed(), &result);
        result
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        // Streamed listings (WAL replay, GC) must not be invisible in the
        // metrics. The wrapper records one sample when the stream is
        // dropped — finished or abandoned — carrying the item count and
        // the first error's class.
        Box::pin(RecordedListStream {
            inner: self.inner.list_prefix_stream(prefix),
            recorder: Arc::clone(&self.recorder),
            store_kind: self.store_kind.clone(),
            key_class: classify_key(prefix),
            started: sample_clock(),
            items: 0,
            first_error: None,
        })
    }

    async fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let start = sample_clock();
        let result: Result<Vec<_>> = self
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
        result: &Result<Option<ObjectMetadata>>,
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
        result: &Result<Option<Bytes>>,
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
        result: &Result<Option<ObjectBody>>,
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
        result: &Result<ObjectMetadata>,
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
        result: &Result<()>,
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

    fn record_list(&self, prefix: &str, elapsed: Duration, result: &Result<Vec<String>>) {
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

struct RecordedListStream {
    inner: BoxStream<'static, Result<String>>,
    recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    store_kind: Option<String>,
    key_class: KeyClass,
    started: Instant,
    items: u64,
    first_error: Option<ObjectStoreResultClass>,
}

impl futures::Stream for RecordedListStream {
    type Item = Result<String>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let polled = self.inner.as_mut().poll_next(cx);
        if let std::task::Poll::Ready(Some(item)) = &polled {
            match item {
                Ok(_) => self.items += 1,
                Err(error) => {
                    if self.first_error.is_none() {
                        self.first_error = Some(classify_error(error));
                    }
                }
            }
        }
        polled
    }
}

impl Drop for RecordedListStream {
    fn drop(&mut self) {
        self.recorder.record(ObjectStoreMetricSample {
            operation: ObjectStoreOperation::ListPrefixStream,
            elapsed_micros: self.started.elapsed().as_micros(),
            result: self.first_error.unwrap_or(ObjectStoreResultClass::Ok),
            bytes_in: None,
            bytes_out: None,
            item_count: Some(self.items),
            key_class: self.key_class,
            range_class: None,
            put_mode: None,
            store_kind: self.store_kind.clone(),
        });
    }
}

fn classify_key(key: &str) -> KeyClass {
    let Some(parsed) = parse_object_key(key) else {
        return KeyClass::Unknown;
    };

    match parsed.family() {
        DurableObjectFamily::ContentBlob => KeyClass::Content,
        DurableObjectFamily::WalHead => KeyClass::NamespaceHead,
        DurableObjectFamily::WalSegment => KeyClass::WalSegment,
        DurableObjectFamily::MetadataManifest => KeyClass::NamespaceManifest,
        DurableObjectFamily::MetadataTable => KeyClass::MetadataSst,
        DurableObjectFamily::CheckpointRecord | DurableObjectFamily::WalFloor => {
            KeyClass::GcControl
        }
        DurableObjectFamily::MetadataRoot => KeyClass::NamespaceManifest,
        DurableObjectFamily::NamespaceConfig
        | DurableObjectFamily::WalIndex
        | DurableObjectFamily::WalIndexRun
        | DurableObjectFamily::UploadSession
        | DurableObjectFamily::ContentStoreDescriptor => KeyClass::Metadata,
    }
}

fn classify_optional_result<T>(result: &Result<Option<T>>) -> ObjectStoreResultClass {
    match result {
        Ok(Some(_)) => ObjectStoreResultClass::Ok,
        Ok(None) => ObjectStoreResultClass::NotFound,
        Err(error) => classify_error(error),
    }
}

fn classify_result<T>(result: &Result<T>) -> ObjectStoreResultClass {
    match result {
        Ok(_) => ObjectStoreResultClass::Ok,
        Err(error) => classify_error(error),
    }
}

fn classify_error(error: &ObjectStoreError) -> ObjectStoreResultClass {
    match error {
        ObjectStoreError::NotFound { .. } => ObjectStoreResultClass::NotFound,
        ObjectStoreError::InvalidKey { .. } => ObjectStoreResultClass::InvalidKey,
        ObjectStoreError::InvalidContentRef(_) => ObjectStoreResultClass::InvalidContentRef,
        ObjectStoreError::InvalidRange { .. } => ObjectStoreResultClass::InvalidRange,
        ObjectStoreError::PreconditionFailed { .. } => ObjectStoreResultClass::PreconditionFailed,
        ObjectStoreError::Conflict { .. } => ObjectStoreResultClass::Conflict,
        ObjectStoreError::PermissionDenied { .. } => ObjectStoreResultClass::PermissionDenied,
        ObjectStoreError::Unsupported(_) => ObjectStoreResultClass::Unsupported,
        // Configuration failures happen at store construction, before any
        // metered operation; classify defensively as transport.
        ObjectStoreError::Configuration(_) => ObjectStoreResultClass::Transport,
        ObjectStoreError::Transport { .. } => ObjectStoreResultClass::Transport,
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
