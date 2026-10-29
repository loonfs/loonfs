//! A metrics-recording wrapper around any object store: per-operation
//! samples classified by object family.

use crate::attempts::counting_attempts;
use crate::clock::monotonic_instant;
use crate::layout::{parse_object_key, DurableObjectFamily};
use crate::object_store::Result;
use crate::ListedObject;
use crate::{
    AssemblySource, ByteRange, ByteStream, MultipartPart, ObjectBody, ObjectMetadata, ObjectStore,
    ObjectStoreErrorKind, PutMode,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs_types::{Checksum, EffectiveLimit, Page};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::fs::{self, File};
use std::future::Future;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

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
    /// Provider attempts this one call made, counting the first: `1` is a
    /// call that never retried.
    ///
    /// Retries are what makes an otherwise unexplained `elapsed_micros`
    /// readable. The count covers LoonFS's own retries inside the store this
    /// wrapper measures, including the retries of
    /// [`crate::ObjectStore::put_immutable_verified`], which records one
    /// sample for the whole write. The provider client's internal retries
    /// of reads, listings, and deletes aren't visible here, and a listing
    /// stream always reports `1`.
    pub attempts: u32,
    /// Cardinality-bounded success or failure classification.
    pub result: ObjectStoreResultKind,
    /// Request payload bytes for a put or an assembly, including failed
    /// attempts; otherwise `None`.
    pub bytes_in: Option<u64>,
    /// Response payload bytes for a successful get, including zero; otherwise `None`.
    pub bytes_out: Option<u64>,
    /// Keys yielded by a listing before completion or drop; otherwise `None`.
    pub item_count: Option<u64>,
    /// Durable-family grouping derived without retaining the raw key.
    pub key_class: ObjectKeyKind,
    /// Read-range or listing shape when applicable; otherwise `None`.
    pub range_class: Option<RangeKind>,
    /// Requested conditional-write class for a put; otherwise `None`.
    pub put_mode: Option<PutModeKind>,
    /// Deployment-supplied provider label, or `None` when the wrapper was left unlabeled.
    pub store_kind: Option<Cow<'static, str>>,
}

impl ObjectStoreMetricSample {
    /// Builds a sample with the fields shared by every object-store operation.
    pub fn new(
        operation: ObjectStoreOperation,
        key: &str,
        elapsed: Duration,
        attempts: u32,
        result: ObjectStoreResultKind,
        store_kind: Option<&'static str>,
    ) -> Self {
        Self {
            operation,
            elapsed_micros: elapsed.as_micros(),
            attempts,
            result,
            bytes_in: None,
            bytes_out: None,
            item_count: None,
            key_class: classify_key(key),
            range_class: None,
            put_mode: None,
            store_kind: store_kind.map(Cow::Borrowed),
        }
    }
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
    /// Measures a conditional write.
    Put,
    /// Measures a write whose payload arrived as a stream.
    PutStreamed,
    /// Measures copying source ranges followed by tail bytes into a new object.
    Assemble,
    /// Measures an idempotent object delete.
    Delete,
    /// Measures opening a client-driven multipart upload.
    CreateMultipartUpload,
    /// Measures asking a provider to assemble a multipart upload.
    CompleteMultipartUpload,
    /// Measures abandoning a multipart upload and its parts.
    AbortMultipartUpload,
    /// Measures one page of child prefixes.
    ListPrefix,
    /// Measures a listing stream until completion or early drop.
    ListPrefixStream,
}

impl ObjectStoreOperation {
    /// Returns the label an aggregating recorder groups by.
    ///
    /// Identical to the serde name: one operation has one spelling wherever it
    /// is reported.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Head => "head",
            Self::GetWithMetadata => "get_with_metadata",
            Self::Get => "get",
            Self::Put => "put",
            Self::PutStreamed => "put_streamed",
            Self::Assemble => "assemble",
            Self::Delete => "delete",
            Self::CreateMultipartUpload => "create_multipart_upload",
            Self::CompleteMultipartUpload => "complete_multipart_upload",
            Self::AbortMultipartUpload => "abort_multipart_upload",
            Self::ListPrefix => "list_prefix",
            Self::ListPrefixStream => "list_prefix_stream",
        }
    }
}

/// Collapses object-store outcomes into a bounded metrics vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectStoreResultKind {
    /// Indicates the operation returned its requested value or mutation result.
    Ok,
    /// Indicates an optional read observed absence or an explicit lookup returned not found.
    NotFound,
    /// Indicates key or prefix validation failed before provider IO.
    InvalidKey,
    /// Indicates a content reference or byte range was invalid.
    InvalidRequest,
    /// Indicates a create-if-absent or compare-and-swap condition did not hold.
    PreconditionFailed,
    /// Indicates the provider rejected identity or authorization.
    PermissionDenied,
    /// Indicates the configured store lacks a required capability.
    Unsupported,
    /// Indicates an object lacks required stored checksum metadata.
    StoredChecksumMissing,
    /// Indicates store construction or configuration failed.
    Configuration,
    /// Indicates a transient transport failure may succeed when repeated.
    RetryableTransport,
    /// Indicates a non-retryable transport or provider protocol failure.
    Other,
}

impl ObjectStoreResultKind {
    /// Returns the label an aggregating recorder groups by.
    ///
    /// Identical to the serde name: one outcome has one spelling wherever it is
    /// reported.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "not_found",
            Self::InvalidKey => "invalid_key",
            Self::InvalidRequest => "invalid_request",
            Self::PreconditionFailed => "precondition_failed",
            Self::PermissionDenied => "permission_denied",
            Self::Unsupported => "unsupported",
            Self::StoredChecksumMissing => "stored_checksum_missing",
            Self::Configuration => "configuration",
            Self::RetryableTransport => "retryable_transport",
            Self::Other => "other",
        }
    }
}

impl From<ObjectStoreErrorKind> for ObjectStoreResultKind {
    fn from(class: ObjectStoreErrorKind) -> Self {
        match class {
            ObjectStoreErrorKind::NotFound => Self::NotFound,
            ObjectStoreErrorKind::InvalidRequest => Self::InvalidRequest,
            ObjectStoreErrorKind::InvalidKey => Self::InvalidKey,
            ObjectStoreErrorKind::PreconditionFailed => Self::PreconditionFailed,
            ObjectStoreErrorKind::PermissionDenied => Self::PermissionDenied,
            ObjectStoreErrorKind::StoredChecksumMissing => Self::StoredChecksumMissing,
            ObjectStoreErrorKind::Unsupported => Self::Unsupported,
            ObjectStoreErrorKind::Configuration => Self::Configuration,
            ObjectStoreErrorKind::RetryableTransport => Self::RetryableTransport,
            ObjectStoreErrorKind::Other => Self::Other,
        }
    }
}

/// Groups durable keys into low-cardinality operational families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectKeyKind {
    /// Groups immutable whole-file byte objects.
    Content,
    /// Groups small control records not assigned a more specific class.
    Metadata,
    /// Groups immutable WAL object payloads.
    WalObject,
    /// Groups namespace manifests.
    NamespaceManifest,
    /// Groups immutable metadata segments.
    MetadataSegment,
    /// Groups pins and retained-history floors consulted by garbage collection.
    GcControl,
    /// Groups unrecognized keys and coarse listing prefixes.
    Unknown,
}

impl ObjectKeyKind {
    /// Returns the label an aggregating recorder groups by.
    ///
    /// Identical to the serde name: one class has one spelling wherever it is
    /// reported.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::Metadata => "metadata",
            Self::WalObject => "wal_object",
            Self::NamespaceManifest => "namespace_manifest",
            Self::MetadataSegment => "metadata_segment",
            Self::GcControl => "gc_control",
            Self::Unknown => "unknown",
        }
    }
}

/// Classifies the shape of bytes requested by a get or listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeKind {
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
pub enum PutModeKind {
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
///
/// It keeps every sample until it is dropped.
#[derive(Default)]
pub struct VecObjectStoreMetricsRecorder {
    samples: Mutex<Vec<ObjectStoreMetricSample>>,
}

impl VecObjectStoreMetricsRecorder {
    /// Returns a copy of all recorded samples.
    pub fn samples(&self) -> Vec<ObjectStoreMetricSample> {
        self.samples
            .lock()
            .expect("the metric sample lock should not be poisoned")
            .clone()
    }
}

impl ObjectStoreMetricsRecorder for VecObjectStoreMetricsRecorder {
    fn record(&self, sample: ObjectStoreMetricSample) {
        self.samples
            .lock()
            .expect("the metric sample lock should not be poisoned")
            .push(sample);
    }
}

/// Buffered JSONL recorder for process-level diagnostics and benchmarks.
///
/// Recording is best-effort: I/O errors while writing a sample are ignored so instrumentation does
/// not change object-store behavior. Every request takes a process-wide lock and writes to a
/// buffered file on the calling thread.
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
            .expect("the metric writer lock should not be poisoned")
            .flush()
    }
}

impl ObjectStoreMetricsRecorder for JsonlObjectStoreMetricsRecorder {
    fn record(&self, sample: ObjectStoreMetricSample) {
        let mut writer = self
            .writer
            .lock()
            .expect("the metric writer lock should not be poisoned");
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
    store_kind: Option<&'static str>,
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
    pub fn store_kind(mut self, store_kind: &'static str) -> Self {
        self.store_kind = Some(store_kind);
        self
    }

    /// Removes instrumentation and returns ownership of the wrapped store.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Times `call`, counts its attempts, and records one sample of
    /// `operation` on `key`. `classify` sets the sample's result, and `fill`
    /// sets the fields that only this operation reports.
    async fn measure<T>(
        &self,
        operation: ObjectStoreOperation,
        key: &str,
        call: impl Future<Output = Result<T>>,
        classify: fn(&Result<T>) -> ObjectStoreResultKind,
        fill: impl FnOnce(&Result<T>, &mut ObjectStoreMetricSample),
    ) -> Result<T> {
        let start = monotonic_instant();
        let counted = counting_attempts(call).await;
        let mut sample = ObjectStoreMetricSample::new(
            operation,
            key,
            start.elapsed(),
            counted.attempts,
            classify(&counted.output),
            self.store_kind,
        );
        fill(&counted.output, &mut sample);
        self.recorder.record(sample);
        counted.output
    }
}

#[async_trait]
impl<S> ObjectStore for InstrumentedObjectStore<S>
where
    S: ObjectStore,
{
    fn checksum_algorithm(&self) -> loonfs_types::ChecksumAlgorithm {
        self.inner.checksum_algorithm()
    }

    fn identity(&self) -> Option<crate::StoreIdentity> {
        self.inner.identity()
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.measure(
            ObjectStoreOperation::Head,
            key,
            self.inner.head(key),
            classify_optional_result,
            |_, _| {},
        )
        .await
    }

    // The multipart control calls move no payload of their own: the parts
    // travel from the client straight to the provider. Timing them is the
    // only thing there is to record.
    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        self.measure(
            ObjectStoreOperation::CreateMultipartUpload,
            key,
            self.inner.create_multipart_upload(key),
            classify_result,
            |_, _| {},
        )
        .await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        self.measure(
            ObjectStoreOperation::CompleteMultipartUpload,
            key,
            self.inner
                .complete_multipart_upload(key, provider_upload_id, parts, checksum),
            classify_result,
            |_, _| {},
        )
        .await
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        self.measure(
            ObjectStoreOperation::AbortMultipartUpload,
            key,
            self.inner.abort_multipart_upload(key, provider_upload_id),
            classify_result,
            |_, _| {},
        )
        .await
    }

    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        let range_class = classify_range(range.as_ref());
        self.measure(
            ObjectStoreOperation::Get,
            key,
            self.inner.get(key, range),
            classify_optional_result,
            |result, sample| {
                sample.bytes_out = result
                    .as_ref()
                    .ok()
                    .and_then(|bytes| bytes.as_ref().map(|bytes| bytes.len() as u64));
                sample.range_class = Some(range_class);
            },
        )
        .await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        self.measure(
            ObjectStoreOperation::GetWithMetadata,
            key,
            self.inner.get_with_metadata(key),
            classify_optional_result,
            |result, sample| {
                sample.bytes_out = result
                    .as_ref()
                    .ok()
                    .and_then(|body| body.as_ref().map(|body| body.bytes.len() as u64));
                sample.range_class = Some(RangeKind::FullObject);
            },
        )
        .await
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let bytes_in = bytes.len() as u64;
        let put_mode = classify_put_mode(&mode);
        self.measure(
            ObjectStoreOperation::Put,
            key,
            self.inner.put(key, bytes, mode),
            classify_result,
            |_, sample| {
                sample.bytes_in = Some(bytes_in);
                sample.put_mode = Some(put_mode);
            },
        )
        .await
    }

    /// Streamed writes get their own operation rather than being folded
    /// into `put`: their request bytes are only known once the stream ends,
    /// and a deployment reading these samples needs to see which write path
    /// its content is taking.
    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        let put_mode = classify_put_mode(&mode);
        self.measure(
            ObjectStoreOperation::PutStreamed,
            key,
            self.inner.put_streamed(key, body, mode),
            classify_result,
            |result, sample| {
                sample.bytes_in = result.as_ref().ok().copied();
                sample.put_mode = Some(put_mode);
            },
        )
        .await
    }

    /// Forwarded so the inner store's own verified write runs, which for a
    /// provider sends a large body through the conditional multipart create.
    /// One `put` sample covers the whole write, retries included.
    async fn put_immutable_verified(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        let bytes_in = bytes.len() as u64;
        self.measure(
            ObjectStoreOperation::Put,
            key,
            self.inner.put_immutable_verified(key, bytes),
            classify_result,
            |_, sample| {
                sample.bytes_in = Some(bytes_in);
                sample.put_mode = Some(PutModeKind::CreateIfAbsent);
            },
        )
        .await
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        let bytes_in = crate::assembly::tail_bytes(&tail);
        self.measure(
            ObjectStoreOperation::Assemble,
            key,
            self.inner.assemble(key, sources, tail, expected),
            classify_result,
            |_, sample| {
                sample.bytes_in = Some(bytes_in);
                sample.put_mode = Some(PutModeKind::CreateIfAbsent);
            },
        )
        .await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.measure(
            ObjectStoreOperation::Delete,
            key,
            self.inner.delete(key),
            classify_result,
            |_, _| {},
        )
        .await
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject>> {
        // Streamed listings (WAL replay, GC) must not be invisible in the
        // metrics. The wrapper records one sample when the stream is
        // dropped — finished or abandoned — carrying the item count and
        // the first error's class.
        Box::pin(RecordedListStream {
            inner: self.inner.list_entries_from_stream(prefix, start_after),
            recorder: Arc::clone(&self.recorder),
            store_kind: self.store_kind,
            prefix: prefix.to_owned(),
            started: monotonic_instant(),
            items: 0,
            first_error: None,
        })
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: EffectiveLimit,
    ) -> Result<Page<String, String>> {
        self.measure(
            ObjectStoreOperation::ListPrefix,
            prefix,
            self.inner.list_child_prefixes(prefix, start_after, limit),
            classify_result,
            |result, sample| {
                sample.item_count = result.as_ref().ok().map(|page| page.items.len() as u64);
                sample.range_class = Some(RangeKind::Prefix);
            },
        )
        .await
    }
}

struct RecordedListStream {
    inner: BoxStream<'static, Result<ListedObject>>,
    recorder: Arc<dyn ObjectStoreMetricsRecorder>,
    store_kind: Option<&'static str>,
    prefix: String,
    started: Instant,
    items: u64,
    first_error: Option<ObjectStoreResultKind>,
}

impl futures::Stream for RecordedListStream {
    type Item = Result<ListedObject>;

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
                        self.first_error = Some(error.kind().into());
                    }
                }
            }
        }
        polled
    }
}

impl Drop for RecordedListStream {
    fn drop(&mut self) {
        let mut sample = ObjectStoreMetricSample::new(
            ObjectStoreOperation::ListPrefixStream,
            &self.prefix,
            self.started.elapsed(),
            1,
            self.first_error.unwrap_or(ObjectStoreResultKind::Ok),
            self.store_kind,
        );
        sample.item_count = Some(self.items);
        self.recorder.record(sample);
    }
}

fn classify_key(key: &str) -> ObjectKeyKind {
    let Some(parsed) = parse_object_key(key) else {
        return ObjectKeyKind::Unknown;
    };

    match parsed.family() {
        DurableObjectFamily::ContentBlob | DurableObjectFamily::TemporaryObject => {
            ObjectKeyKind::Content
        }
        DurableObjectFamily::WalObject => ObjectKeyKind::WalObject,
        DurableObjectFamily::MetadataManifest | DurableObjectFamily::Hint => {
            ObjectKeyKind::NamespaceManifest
        }
        DurableObjectFamily::MetadataSegment => ObjectKeyKind::MetadataSegment,
        DurableObjectFamily::Pin => ObjectKeyKind::GcControl,
        DurableObjectFamily::UploadSession | DurableObjectFamily::UploadOutcome => {
            ObjectKeyKind::Metadata
        }
    }
}

fn classify_optional_result<T>(result: &Result<Option<T>>) -> ObjectStoreResultKind {
    match result {
        Ok(Some(_)) => ObjectStoreResultKind::Ok,
        Ok(None) => ObjectStoreResultKind::NotFound,
        Err(error) => error.kind().into(),
    }
}

fn classify_result<T>(result: &Result<T>) -> ObjectStoreResultKind {
    match result {
        Ok(_) => ObjectStoreResultKind::Ok,
        Err(error) => error.kind().into(),
    }
}

fn classify_range(range: Option<&ByteRange>) -> RangeKind {
    let Some(range) = range else {
        return RangeKind::FullObject;
    };
    if range.start_inclusive == range.end_exclusive {
        RangeKind::Empty
    } else if range.start_inclusive == 0 {
        RangeKind::Prefix
    } else if range.end_exclusive == u64::MAX {
        RangeKind::Suffix
    } else {
        RangeKind::Bounded
    }
}

fn classify_put_mode(mode: &PutMode) -> PutModeKind {
    match mode {
        PutMode::CreateIfAbsent => PutModeKind::CreateIfAbsent,
        PutMode::CompareAndSwap { .. } => PutModeKind::CompareAndSwap,
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectKeyKind, ObjectStoreOperation, ObjectStoreResultKind};
    use serde::de::DeserializeOwned;
    use serde::Serialize;
    use std::fmt::Debug;

    fn assert_spelling<T>(value: T, spelling: &str)
    where
        T: Serialize + DeserializeOwned + PartialEq + Debug,
    {
        let expected = serde_json::Value::from(spelling);
        assert_eq!(
            serde_json::to_value(&value).expect("serialize the value"),
            expected,
            "{value:?}"
        );
        assert_eq!(
            serde_json::from_value::<T>(expected).expect("deserialize the spelling"),
            value,
            "{spelling}"
        );
    }

    #[test]
    fn every_metric_label_matches_its_serde_spelling() {
        for operation in [
            ObjectStoreOperation::Head,
            ObjectStoreOperation::GetWithMetadata,
            ObjectStoreOperation::Get,
            ObjectStoreOperation::Put,
            ObjectStoreOperation::PutStreamed,
            ObjectStoreOperation::Assemble,
            ObjectStoreOperation::Delete,
            ObjectStoreOperation::CreateMultipartUpload,
            ObjectStoreOperation::CompleteMultipartUpload,
            ObjectStoreOperation::AbortMultipartUpload,
            ObjectStoreOperation::ListPrefix,
            ObjectStoreOperation::ListPrefixStream,
        ] {
            assert_spelling(operation, operation.as_str());
        }
        for result in [
            ObjectStoreResultKind::Ok,
            ObjectStoreResultKind::NotFound,
            ObjectStoreResultKind::InvalidKey,
            ObjectStoreResultKind::InvalidRequest,
            ObjectStoreResultKind::PreconditionFailed,
            ObjectStoreResultKind::PermissionDenied,
            ObjectStoreResultKind::Unsupported,
            ObjectStoreResultKind::StoredChecksumMissing,
            ObjectStoreResultKind::Configuration,
            ObjectStoreResultKind::RetryableTransport,
            ObjectStoreResultKind::Other,
        ] {
            assert_spelling(result, result.as_str());
        }
        for class in [
            ObjectKeyKind::Content,
            ObjectKeyKind::Metadata,
            ObjectKeyKind::WalObject,
            ObjectKeyKind::NamespaceManifest,
            ObjectKeyKind::MetadataSegment,
            ObjectKeyKind::GcControl,
            ObjectKeyKind::Unknown,
        ] {
            assert_spelling(class, class.as_str());
        }
    }
}
