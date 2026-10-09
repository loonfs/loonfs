//! The shared provider transport: timeouts, bounded retries for replay-safe
//! delete and multipart stages, multipart upload for large payloads, and the
//! write attestation kept in provider user metadata.

use crate::keyspace::{
    normalize_key_prefix, scope_child_listing_prefix, scope_list_prefix, scope_object_key,
    unscope_listed_key,
};
use crate::object_store::{collect_stream, Result};
use crate::retry::{provider_transport_retryable, with_transport_retry, DEFAULT};
use crate::store_io_runtime::StoreIoRuntime;
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{
    ByteRange, ByteStream, ConfiguredObjectStoreKind, ExtendBase, ExtendedObject, MultipartPart,
    ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode, StoredObjectChecksum,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt};
use loonfs_types::{Checksum, ChecksumAlgorithm};
use loonfs_types::{EffectiveLimit, OperationDeadline, Page, TransportRetryPolicy};
use object_store as provider_store;
use provider_store::list::{PaginatedListOptions, PaginatedListStore};
use provider_store::multipart::{MultipartStore, PartId};
use provider_store::path::Path;
use provider_store::{
    Attribute, Attributes, GetOptions, GetRange, ObjectMeta, ObjectStoreExt, PutOptions,
    PutPayload, PutResult, UpdateVersion,
};
use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

/// Configures logical key scoping for a generic provider client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderObjectStoreConfig {
    /// Prefix prepended to every provider key, or `None` to expose the bucket root.
    pub key_prefix: Option<String>,
}

/// Bound for one control-plane HTTP attempt's request phase, and the
/// response-body idle bound for every request. An attempt that makes no
/// progress for this long fails and counts against the operation deadline
/// instead of consuming it invisibly.
pub const PROVIDER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);

/// One HTTP attempt's connect timeout.
pub const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Deadline shared by all retries of one logical object-store operation.
///
/// Reads apply it through the provider client. Verified immutable writes and
/// deletes apply it through `TransportRetryPolicy`. A new outer attempt may
/// start only while time remains. Multipart uploads apply it to each control
/// call and to each part, so one part's outer retries cannot outlast it. The
/// garbage-collection grace period is longer than the maximum publication
/// operation.
pub const PROVIDER_OPERATION_DEADLINE: Duration = Duration::from_secs(120);

/// Maximum retry delay after the provider client admits a final retry.
pub const PROVIDER_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(15);

/// Minimum payload size for native multipart uploads.
///
/// An overwrite at or above it uploads parts. A create-if-absent at or above
/// it uses the provider's own conditional path where the provider has one,
/// and one request elsewhere. A compare-and-swap is always one request.
pub const PROVIDER_MULTIPART_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Fixed size of every multipart part except the last. Cloudflare R2
/// requires all non-final parts to share one size, and every supported
/// provider requires at least 5 MiB per non-final part; 8 MiB matches the
/// part size mainstream storage clients default to, and keeps every part a
/// cheap retry that fits comfortably inside one flat attempt bound.
pub const PROVIDER_MULTIPART_PART_BYTES: u64 = 8 * 1024 * 1024;

/// Concurrent in-flight parts per multipart upload.
pub const PROVIDER_MULTIPART_PART_WINDOW: usize = 4;

/// The smallest part AWS S3 and Cloudflare R2 accept anywhere but last, and
/// so the shortest base prefix a provider copies instead of receiving it
/// again.
pub const PROVIDER_MIN_COPIED_PART_BYTES: u64 = 5 * 1024 * 1024;

/// Parts one provider multipart upload accepts. Every supported provider
/// stops at 10,000, which with the part size sets the largest object a
/// multipart write can produce.
pub(crate) const MAX_PROVIDER_MULTIPART_PARTS: usize = 10_000;

/// Request-phase timeout for one HTTP attempt that carries a payload.
///
/// Upload progress is not observable while a request body is being sent, so
/// this fixed timeout treats an excessively slow part as stalled. Multipart
/// parts are bounded by [`PROVIDER_MULTIPART_PART_BYTES`].
pub const PROVIDER_TRANSFER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(120);

/// Request-phase allowance for a single-request publication.
///
/// The provider client checks its retry deadline before backoff, so a
/// resent request may finish one backoff and one payload attempt after that
/// deadline. A conditional WAL or manifest put is one attempt, even with a
/// payload-sized body, so the bound still holds for it. This does not bound
/// response-body consumption or prove that a timed-out request cannot take
/// effect remotely.
pub const PROVIDER_PUBLICATION_REQUEST_BOUND: Duration = PROVIDER_OPERATION_DEADLINE
    .saturating_add(PROVIDER_MAX_RETRY_BACKOFF)
    .saturating_add(PROVIDER_TRANSFER_ATTEMPT_TIMEOUT);

/// Request bodies at least this large are payload transfers and get
/// [`PROVIDER_TRANSFER_ATTEMPT_TIMEOUT`] as their request-phase bound;
/// smaller bodies are control-plane traffic bounded by
/// [`PROVIDER_ATTEMPT_TIMEOUT`]. Sits well below the part size so multipart
/// tail parts classify with their siblings.
pub(crate) const PROVIDER_TRANSFER_BODY_MIN_BYTES: u64 = 1024 * 1024;

/// Bound for one HTTP attempt's request phase (connect, request-body
/// upload, response headers), by request body size: flat and small for
/// control-plane requests, flat and generous for payload transfers.
pub(crate) fn request_phase_bound(request_body_bytes: u64) -> Duration {
    if request_body_bytes >= PROVIDER_TRANSFER_BODY_MIN_BYTES {
        PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
    } else {
        PROVIDER_ATTEMPT_TIMEOUT
    }
}

/// Client options every provider builder applies: an explicit per-attempt
/// total-request timeout and connect timeout, so a client built from these
/// options alone is bounded by named constants instead of upstream defaults.
/// [`crate::transfer_timeouts::TransferTimeoutConnector`] strips the
/// total-request timeout and replaces it with payload-aware request bounds
/// and response-body idle bounds.
pub(crate) fn provider_client_options() -> provider_store::ClientOptions {
    provider_store::ClientOptions::new()
        .with_timeout(PROVIDER_ATTEMPT_TIMEOUT)
        .with_connect_timeout(PROVIDER_CONNECT_TIMEOUT)
}

/// Retry configuration every provider builder applies: the client's internal
/// read retries consume [`PROVIDER_OPERATION_DEADLINE`] as one per-operation budget,
/// matching the write loops above.
pub(crate) fn provider_retry_config() -> provider_store::RetryConfig {
    provider_store::RetryConfig {
        retry_timeout: PROVIDER_OPERATION_DEADLINE,
        backoff: provider_store::BackoffConfig {
            init_backoff: DEFAULT.initial_backoff,
            max_backoff: PROVIDER_MAX_RETRY_BACKOFF,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The size routing for multipart writes: payloads at or above the
/// threshold are uploaded as fixed-size parts. One production value
/// ([`PROVIDER_MULTIPART_THRESHOLD_BYTES`], [`PROVIDER_MULTIPART_PART_BYTES`]);
/// tests shrink it to exercise the machinery without allocating gigabytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MultipartGeometry {
    threshold_bytes: u64,
    part_bytes: u64,
}

impl MultipartGeometry {
    const DEFAULT: Self = Self {
        threshold_bytes: PROVIDER_MULTIPART_THRESHOLD_BYTES,
        part_bytes: PROVIDER_MULTIPART_PART_BYTES,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompareToken {
    Etag,
    Generation,
}

#[async_trait]
pub(crate) trait StoredChecksumReader: Send + Sync {
    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>>;
}

/// User-metadata name under which providers keep a write's attestation.
pub(crate) const SHA256_METADATA_KEY: &str = "sha256";

/// Reads an attestation kept in user metadata; a value that is not a SHA-256
/// is no attestation.
pub(crate) fn attested_sha256(value: &str) -> Option<Checksum> {
    let sha256 = Checksum {
        algorithm: ChecksumAlgorithm::Sha256,
        value: value.to_owned(),
    };
    sha256.validate().is_ok().then_some(sha256)
}

/// The provider's own multi-request writes, sent as requests this crate
/// signs: client-driven multipart uploads, large conditional creates, and
/// extensions.
#[async_trait]
pub(crate) trait MultipartController: Send + Sync {
    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        let _ = key;
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        let _ = (key, provider_upload_id, parts, checksum);
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        let _ = (key, provider_upload_id);
        Err(ObjectStoreError::Unsupported(
            "client-driven multipart upload",
        ))
    }

    /// Writes `head` and then every part of `rest` to `key` only while `key`
    /// is absent, attesting `sha256` when given, with at most `part_window`
    /// parts in flight. A provider whose upload takes its parts in order
    /// sends them one at a time.
    async fn put_if_absent(
        &self,
        key: &str,
        head: Bytes,
        rest: PartReader<'_>,
        sha256: Option<&Checksum>,
        part_window: usize,
    ) -> Result<ObjectMetadata>;

    /// Extends `key` without moving its base through this process, or
    /// answers `None` when the provider must rewrite this base instead.
    async fn extend_object(
        &self,
        key: &str,
        base: &ExtendBase,
        pieces: Bytes,
        result: &ExtendedObject,
    ) -> Result<Option<ObjectMetadata>>;

    /// Creates `key` from a prefix of the object at `base_key` without
    /// moving it through this process, or answers `None` when the provider
    /// cannot copy this base.
    async fn put_immutable_extended(
        &self,
        key: &str,
        base_key: &str,
        base: &ExtendBase,
        pieces: Bytes,
        result: &ExtendedObject,
    ) -> Result<Option<ObjectMetadata>>;
}

/// Adapts the upstream `object_store` provider surface to the narrower LoonFS contract.
#[derive(Clone)]
pub struct ProviderObjectStore {
    inner: Arc<dyn provider_store::ObjectStore>,
    one_attempt: Arc<dyn provider_store::ObjectStore>,
    multipart: Arc<dyn MultipartStore>,
    paginated: Arc<dyn PaginatedListStore>,
    checksum_reader: Option<Arc<dyn StoredChecksumReader>>,
    multipart_controller: Option<Arc<dyn MultipartController>>,
    compare_token: CompareToken,
    kind: ConfiguredObjectStoreKind,
    io_runtime: StoreIoRuntime,
    multipart_geometry: MultipartGeometry,
    key_prefix: Option<String>,
    transport_retry: TransportRetryPolicy,
    timer: Arc<dyn MonotonicTimer>,
}

impl fmt::Debug for ProviderObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderObjectStore")
            .field("kind", &self.kind.as_str())
            .field("key_prefix", &self.key_prefix)
            .field("io_runtime", &self.io_runtime)
            .finish_non_exhaustive()
    }
}

impl ProviderObjectStore {
    /// Wraps a provider client, the same client built to never resend a
    /// request, and the native multipart and paged delimiter listing
    /// surfaces.
    ///
    /// Conditional flat puts go to `one_attempt`; everything else uses
    /// `inner`. Construction fails when `config.key_prefix` is not a
    /// normalized, non-escaping logical prefix.
    pub(crate) fn new(
        inner: Arc<dyn provider_store::ObjectStore>,
        one_attempt: Arc<dyn provider_store::ObjectStore>,
        multipart: Arc<dyn MultipartStore>,
        paginated: Arc<dyn PaginatedListStore>,
        config: ProviderObjectStoreConfig,
        kind: ConfiguredObjectStoreKind,
        io_runtime: StoreIoRuntime,
    ) -> Result<Self> {
        Ok(Self {
            inner,
            one_attempt,
            multipart,
            paginated,
            checksum_reader: None,
            multipart_controller: None,
            compare_token: CompareToken::Etag,
            kind,
            io_runtime,
            multipart_geometry: MultipartGeometry::DEFAULT,
            key_prefix: normalize_key_prefix(config.key_prefix.as_deref())?,
            transport_retry: DEFAULT,
            timer: Arc::new(StdMonotonicTimer::default()),
        })
    }

    pub(crate) fn compare_token(mut self, compare_token: CompareToken) -> Self {
        self.compare_token = compare_token;
        self
    }

    pub(crate) fn checksum_reader(
        mut self,
        checksum_reader: Arc<dyn StoredChecksumReader>,
    ) -> Self {
        self.checksum_reader = Some(checksum_reader);
        self
    }

    pub(crate) fn multipart_controller(
        mut self,
        multipart_controller: Arc<dyn MultipartController>,
    ) -> Self {
        self.multipart_controller = Some(multipart_controller);
        self
    }

    #[cfg(test)]
    fn transport_retry(mut self, transport_retry: TransportRetryPolicy) -> Self {
        self.transport_retry = transport_retry;
        self
    }

    #[cfg(test)]
    fn multipart_geometry(mut self, threshold_bytes: u64, part_bytes: u64) -> Self {
        self.multipart_geometry = MultipartGeometry {
            threshold_bytes,
            part_bytes,
        };
        self
    }

    #[cfg(test)]
    fn monotonic_timer(mut self, timer: Arc<dyn MonotonicTimer>) -> Self {
        self.timer = timer;
        self
    }

    fn to_path(&self, key: &str) -> Result<Path> {
        let scoped = scope_object_key(self.key_prefix.as_deref(), key)?;
        Path::parse(scoped).map_err(|err| ObjectStoreError::InvalidKey {
            object_key: key.to_owned(),
            message: err.to_string(),
        })
    }

    fn list_path(&self, prefix: &str) -> Result<Option<Path>> {
        let scoped = scope_list_prefix(self.key_prefix.as_deref(), prefix)?;
        if scoped.is_empty() {
            return Ok(None);
        }
        Path::parse(scoped)
            .map(Some)
            .map_err(|err| ObjectStoreError::InvalidKey {
                object_key: prefix.to_owned(),
                message: err.to_string(),
            })
    }

    fn unscoped(&self, scoped_key: &str) -> Option<String> {
        match self.key_prefix.as_deref() {
            Some(key_prefix) => unscope_listed_key(Some(key_prefix), scoped_key),
            None => Some(scoped_key.to_owned()),
        }
    }

    fn metadata_from_meta(&self, meta: ObjectMeta, attributes: &Attributes) -> ObjectMetadata {
        self.with_compare_token(ObjectMetadata {
            etag: meta.e_tag,
            version: meta.version,
            size_bytes: meta.size,
            last_modified_ms: last_modified_ms(meta.last_modified.timestamp_millis()),
            sha256: attributes
                .get(&Attribute::Metadata(SHA256_METADATA_KEY.into()))
                .and_then(|value| attested_sha256(value)),
        })
    }

    fn metadata_from_put_result(&self, result: PutResult, size_bytes: u64) -> ObjectMetadata {
        self.with_compare_token(ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            last_modified_ms: None,
            sha256: None,
        })
    }

    fn with_compare_token(&self, metadata: ObjectMetadata) -> ObjectMetadata {
        match self.compare_token {
            CompareToken::Etag => metadata,
            CompareToken::Generation => ObjectMetadata {
                etag: metadata.version.clone(),
                ..metadata
            },
        }
    }

    fn validate_compare_token(&self, key: &str, mode: &PutMode) -> Result<()> {
        if self.compare_token == CompareToken::Generation {
            if let PutMode::CompareAndSwap { expected_etag } = mode {
                expected_etag
                    .parse::<u64>()
                    .map_err(|_| ObjectStoreError::PreconditionFailed {
                        object_key: key.to_owned(),
                    })?;
            }
        }
        Ok(())
    }

    async fn ranged_get(&self, path: &Path, start: u64, end: u64) -> RangedGet {
        let options = GetOptions {
            range: Some(GetRange::Bounded(Range { start, end })),
            ..Default::default()
        };
        match self.inner.get_opts(path, options).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => RangedGet::Bytes(bytes),
                Err(err) => RangedGet::Refused(err),
            },
            Err(err) if provider_not_found(&err) => RangedGet::NotFound,
            Err(err) => RangedGet::Refused(err),
        }
    }

    /// Uploads a large overwrite through the provider's multipart API.
    ///
    /// Parts use stable indices, bounded concurrency, and bounded retries. A
    /// failed upload is aborted on a best-effort basis, and an ambiguous
    /// completion is returned as the transport failure it is.
    ///
    /// Each control call and each part is bounded by the operation deadline.
    /// Conditional write modes do not use this path.
    async fn put_large_multipart(
        &self,
        multipart: Arc<dyn MultipartStore>,
        key: &str,
        path: &Path,
        bytes: Bytes,
    ) -> Result<ObjectMetadata> {
        let size_bytes = bytes.len() as u64;
        let upload = MultipartWrite {
            store: self,
            multipart,
            key,
            path,
        };

        let (upload_id, mut abort_on_drop) = upload.create(size_bytes).await?;
        let result = upload.upload_parts_and_complete(&upload_id, &bytes).await;
        match result {
            Ok(metadata) => {
                abort_on_drop.disarm();
                Ok(metadata)
            }
            Err(err) => {
                // Best effort, and harmless when the failure raced a landed
                // completion: the upload id no longer exists then, and the
                // abort cannot touch the completed object.
                upload.abort(&upload_id).await;
                abort_on_drop.disarm();
                Err(err)
            }
        }
    }
}

/// Cuts a byte stream into fixed-size parts, holding one at a time.
///
/// Chunk boundaries in the source stream carry no meaning, so a chunk that
/// straddles a part boundary is split and its tail carried into the next
/// part. A stream that ends exactly on a boundary produces no final part.
pub(crate) struct PartReader<'a> {
    body: BoxStream<'a, Result<Bytes>>,
    /// The tail of a chunk that overran the part being cut.
    carry: Option<Bytes>,
    part_bytes: usize,
    exhausted: bool,
}

impl<'a> PartReader<'a> {
    pub(crate) fn new(body: BoxStream<'a, Result<Bytes>>, part_bytes: usize) -> Self {
        Self {
            body,
            carry: None,
            part_bytes,
            exhausted: false,
        }
    }

    /// Cuts the next part: exactly `part_bytes`, or whatever is left when
    /// the stream ends. `None` once nothing is left.
    ///
    /// A full part is returned without polling the stream again, so a
    /// caller cannot conclude from a full part that more is coming — only
    /// a short part proves the stream ended.
    pub(crate) async fn next_part(&mut self) -> Result<Option<Bytes>> {
        let mut buffer = bytes::BytesMut::with_capacity(self.part_bytes);
        while buffer.len() < self.part_bytes {
            let mut chunk = match self.carry.take() {
                Some(chunk) => chunk,
                None if self.exhausted => break,
                None => match self.body.next().await {
                    Some(chunk) => chunk?,
                    None => {
                        self.exhausted = true;
                        break;
                    }
                },
            };
            let take = (self.part_bytes - buffer.len()).min(chunk.len());
            buffer.extend_from_slice(&chunk.split_to(take));
            if !chunk.is_empty() {
                self.carry = Some(chunk);
            }
        }
        Ok((!buffer.is_empty()).then(|| buffer.freeze()))
    }

    /// Whether the stream has already reported its end.
    pub(crate) fn exhausted(&self) -> bool {
        self.exhausted && self.carry.is_none()
    }
}

/// Aborts a provider upload when its write is abandoned.
///
/// A write that finishes, or that aborts explicitly, disarms this guard. A
/// failed or cancelled write that does neither has no cleanup `await` point,
/// so `Drop` starts `abort` on the current Tokio runtime. Without a runtime,
/// the provider's cleanup of incomplete uploads must remove what was sent.
pub(crate) struct AbortUploadOnDrop {
    object_key: String,
    abort: Option<BoxFuture<'static, Result<()>>>,
}

impl AbortUploadOnDrop {
    pub(crate) fn new(object_key: &str, abort: BoxFuture<'static, Result<()>>) -> Self {
        Self {
            object_key: object_key.to_owned(),
            abort: Some(abort),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.abort = None;
    }
}

impl Drop for AbortUploadOnDrop {
    fn drop(&mut self) {
        let Some(abort) = self.abort.take() else {
            return;
        };
        let object_key = std::mem::take(&mut self.object_key);
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                object_key,
                operation = "abort_multipart",
                "abandoned write has no runtime to abort its upload on; \
                 what it sent remains until the provider's cleanup collects it",
            );
            return;
        };
        handle.spawn(async move {
            if abort.await.is_err() {
                tracing::warn!(
                    object_key,
                    operation = "abort_multipart",
                    "failed to abort the upload of an abandoned write",
                );
            }
        });
    }
}

/// One in-progress multipart write: the store, the provider multipart
/// surface, and the object being written.
struct MultipartWrite<'op> {
    store: &'op ProviderObjectStore,
    multipart: Arc<dyn MultipartStore>,
    key: &'op str,
    path: &'op Path,
}

impl MultipartWrite<'_> {
    async fn create(
        &self,
        payload_bytes: u64,
    ) -> Result<(provider_store::MultipartId, AbortUploadOnDrop)> {
        let deadline = OperationDeadline::start(
            self.store.timer.as_ref(),
            self.store.transport_retry.operation_deadline,
        );
        let upload_id = with_transport_retry(
            &self.store.transport_retry,
            self.key,
            "create_multipart",
            payload_bytes,
            Some(&deadline),
            provider_transport_retryable,
            || self.multipart.create_multipart(self.path),
        )
        .await
        .map_err(|error| map_provider_error(self.key, error))?;
        let multipart = Arc::clone(&self.multipart);
        let path = self.path.clone();
        let key = self.key.to_owned();
        let aborted_id = upload_id.clone();
        let abort = async move {
            multipart
                .abort_multipart(&path, &aborted_id)
                .await
                .map_err(|error| map_provider_error(&key, error))
        };
        Ok((upload_id, AbortUploadOnDrop::new(self.key, abort.boxed())))
    }

    /// Uploads a stream as fixed-size parts while retaining at most one part.
    ///
    /// `head` is the first part. Later parts are buffered only after the previous
    /// part has been uploaded and released. The final part may be shorter.
    /// Conditional modes are checked after the stream is consumed and
    /// immediately before completion.
    async fn upload_stream_and_complete(
        &self,
        upload_id: &provider_store::MultipartId,
        head: Bytes,
        mut parts_reader: PartReader<'_>,
        mode: &PutMode,
    ) -> Result<ObjectMetadata> {
        let mut size_bytes = head.len() as u64;
        let mut parts = vec![self.upload_part(upload_id, 0, head).await?];

        while let Some(payload) = parts_reader.next_part().await? {
            if parts.len() >= MAX_PROVIDER_MULTIPART_PARTS {
                return Err(ObjectStoreError::transport(
                    self.key,
                    format!(
                        "streamed payload needs more than the provider's \
                         {MAX_PROVIDER_MULTIPART_PARTS}-part limit at this part size"
                    ),
                ));
            }
            size_bytes += payload.len() as u64;
            parts.push(self.upload_part(upload_id, parts.len(), payload).await?);
        }

        self.precondition_holds(mode).await?;

        match self
            .multipart
            .complete_multipart(self.path, upload_id, parts)
            .await
        {
            Ok(result) => Ok(self.store.metadata_from_put_result(result, size_bytes)),
            Err(err) => Err(map_provider_error(self.key, err)),
        }
    }

    /// Checks a streamed multipart write's condition immediately before
    /// completion.
    ///
    /// Providers complete multipart uploads unconditionally, so this separate
    /// read is required for create-if-absent and compare-and-swap modes. It runs
    /// after the complete stream has been consumed. The check is not atomic with
    /// completion; callers requiring stronger exclusion must prevent concurrent
    /// writes to the key.
    async fn precondition_holds(&self, mode: &PutMode) -> Result<()> {
        let refused = || {
            Err(ObjectStoreError::PreconditionFailed {
                object_key: self.key.to_owned(),
            })
        };
        match mode {
            PutMode::Overwrite => Ok(()),
            PutMode::CreateIfAbsent => match self.store.head(self.key).await? {
                None => Ok(()),
                Some(_) => refused(),
            },
            PutMode::CompareAndSwap { expected_etag } => match self.store.head(self.key).await? {
                Some(current) if current.etag.as_deref() == Some(expected_etag.as_str()) => Ok(()),
                _ => refused(),
            },
        }
    }

    async fn upload_parts_and_complete(
        &self,
        upload_id: &provider_store::MultipartId,
        bytes: &Bytes,
    ) -> Result<ObjectMetadata> {
        let part_size = self.store.multipart_geometry.part_bytes as usize;
        let part_count = bytes.len().div_ceil(part_size);
        let mut part_ids: Vec<Option<PartId>> = vec![None; part_count];
        let mut in_flight = FuturesUnordered::new();
        let mut next_part = 0usize;

        loop {
            while in_flight.len() < PROVIDER_MULTIPART_PART_WINDOW && next_part < part_count {
                let part_index = next_part;
                let start = part_index * part_size;
                let end = (start + part_size).min(bytes.len());
                let payload = bytes.slice(start..end);
                in_flight.push(async move {
                    let uploaded = self.upload_part(upload_id, part_index, payload).await;
                    (part_index, uploaded)
                });
                next_part += 1;
            }
            match in_flight.next().await {
                Some((part_index, Ok(part_id))) => part_ids[part_index] = Some(part_id),
                // Dropping the window cancels the sibling part uploads; the
                // caller aborts the upload so no parts are stranded.
                Some((_, Err(err))) => return Err(err),
                None => break,
            }
        }

        let parts = part_ids
            .into_iter()
            .map(|part_id| part_id.expect("every part completed before the window drained"))
            .collect();
        self.multipart
            .complete_multipart(self.path, upload_id, parts)
            .await
            .map(|result| {
                self.store
                    .metadata_from_put_result(result, bytes.len() as u64)
            })
            .map_err(|err| map_provider_error(self.key, err))
    }

    async fn upload_part(
        &self,
        upload_id: &provider_store::MultipartId,
        part_index: usize,
        payload: Bytes,
    ) -> Result<PartId> {
        let payload_bytes = payload.len() as u64;
        let deadline = OperationDeadline::start(
            self.store.timer.as_ref(),
            self.store.transport_retry.operation_deadline,
        );
        with_transport_retry(
            &self.store.transport_retry,
            self.key,
            "put_part",
            payload_bytes,
            Some(&deadline),
            provider_transport_retryable,
            || {
                self.multipart.put_part(
                    self.path,
                    upload_id,
                    part_index,
                    PutPayload::from(payload.clone()),
                )
            },
        )
        .await
        .map_err(|error| map_provider_error(self.key, error))
    }

    async fn abort(&self, upload_id: &provider_store::MultipartId) {
        // Best effort: an unaborted upload only strands parts until the
        // bucket's lifecycle rule for incomplete multipart uploads collects
        // them, so an abort failure is logged rather than surfaced. An
        // already-gone upload is the no-op success it reads as — typically
        // its completion landed.
        match self.multipart.abort_multipart(self.path, upload_id).await {
            Ok(()) => {}
            Err(err) if provider_not_found(&err) => {}
            Err(_err) => {
                tracing::warn!(
                    object_key = self.key,
                    operation = "abort_multipart",
                    "failed to abort multipart upload; parts remain until the bucket lifecycle rule collects them",
                );
            }
        }
    }
}

#[async_trait]
impl ObjectStore for ProviderObjectStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        let path = self.to_path(key)?;
        let options = GetOptions {
            head: true,
            ..Default::default()
        };
        match self.inner.get_opts(&path, options).await {
            Ok(result) => Ok(Some(
                self.metadata_from_meta(result.meta, &result.attributes),
            )),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    async fn head_stored_checksum(&self, key: &str) -> Result<Option<StoredObjectChecksum>> {
        match &self.checksum_reader {
            Some(reader) => reader.head_stored_checksum(key).await,
            None => Err(ObjectStoreError::Unsupported(
                "stored full-object checksum readback",
            )),
        }
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        match &self.multipart_controller {
            Some(controller) => controller.create_multipart_upload(key).await,
            None => Err(ObjectStoreError::Unsupported(
                "client-driven multipart upload",
            )),
        }
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        match &self.multipart_controller {
            Some(controller) => {
                controller
                    .complete_multipart_upload(key, provider_upload_id, parts, checksum)
                    .await
            }
            None => Err(ObjectStoreError::Unsupported(
                "client-driven multipart upload",
            )),
        }
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        match &self.multipart_controller {
            Some(controller) => {
                controller
                    .abort_multipart_upload(key, provider_upload_id)
                    .await
            }
            None => Err(ObjectStoreError::Unsupported(
                "client-driven multipart upload",
            )),
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        let path = self.to_path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => {
                let metadata = self.metadata_from_meta(result.meta.clone(), &result.attributes);
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| map_provider_error(key, err))?;
                Ok(Some(ObjectBody {
                    metadata,
                    bytes: bytes.to_vec(),
                }))
            }
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    /// Bounded reads issue the ranged GET directly — one round trip, not a
    /// sizing HEAD plus a GET — and pay a single HEAD only on the failure
    /// path, to decide whether the range or the transport was the problem.
    /// The contract matches the local reference provider exactly: a descending
    /// range is `InvalidRange` before existence is consulted, a missing object
    /// is otherwise `Ok(None)` however the request was shaped, an end past the
    /// object clamps, `start == size` reads empty, and `start > size` is
    /// `InvalidRange`.
    async fn get(&self, key: &str, range: Option<ByteRange>) -> Result<Option<Bytes>> {
        let path = self.to_path(key)?;
        let Some(range) = range else {
            return match self.inner.get(&path).await {
                Ok(result) => result
                    .bytes()
                    .await
                    .map(Some)
                    .map_err(|err| map_provider_error(key, err)),
                Err(err) if provider_not_found(&err) => Ok(None),
                Err(err) => Err(map_provider_error(key, err)),
            };
        };
        if range.end_exclusive < range.start_inclusive {
            return Err(ObjectStoreError::InvalidRange {
                object_key: key.to_owned(),
            });
        }
        if range.end_exclusive == range.start_inclusive {
            // A zero-length request needs no bytes; existence and size
            // alone answer it.
            return match self.head(key).await? {
                None => Ok(None),
                Some(metadata) if range.start_inclusive > metadata.size_bytes => {
                    Err(ObjectStoreError::InvalidRange {
                        object_key: key.to_owned(),
                    })
                }
                Some(_) => Ok(Some(Bytes::new())),
            };
        }
        match self
            .ranged_get(&path, range.start_inclusive, range.end_exclusive)
            .await
        {
            RangedGet::Bytes(bytes) => Ok(Some(bytes)),
            RangedGet::NotFound => Ok(None),
            RangedGet::Refused(err) => {
                // The provider refused; one HEAD decides whether the range
                // was the problem, matching the reference semantics.
                match self.head(key).await? {
                    None => Ok(None),
                    Some(metadata) if range.start_inclusive > metadata.size_bytes => {
                        Err(ObjectStoreError::InvalidRange {
                            object_key: key.to_owned(),
                        })
                    }
                    Some(metadata) if range.start_inclusive == metadata.size_bytes => {
                        Ok(Some(Bytes::new()))
                    }
                    Some(metadata) if range.end_exclusive > metadata.size_bytes => {
                        // A strict provider rejected the over-long end
                        // instead of clamping; clamp and retry once.
                        match self
                            .ranged_get(&path, range.start_inclusive, metadata.size_bytes)
                            .await
                        {
                            RangedGet::Bytes(bytes) => Ok(Some(bytes)),
                            RangedGet::NotFound => Ok(None),
                            RangedGet::Refused(err) => Err(map_provider_error(key, err)),
                        }
                    }
                    Some(_) => Err(map_provider_error(key, err)),
                }
            }
        }
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let sha256 = matches!(mode, PutMode::CreateIfAbsent).then(|| Checksum::sha256(&bytes));
        self.put_attested(key, bytes, mode, sha256).await
    }

    /// A verified write of one part or less is the single-request create
    /// that [`Self::put`] sends. A longer one goes through the provider's
    /// own multipart requests where it has them, so the create stays
    /// conditional at every size. Its parts are replay-safe and each is
    /// bounded by the operation deadline, so the whole takes longer than
    /// one request's allowance: content and segments come through here,
    /// never a publication.
    async fn put_immutable_verified(
        &self,
        key: &str,
        bytes: Bytes,
    ) -> std::result::Result<ObjectMetadata, crate::ImmutableWriteError> {
        let Some(controller) = self
            .multipart_controller
            .as_ref()
            .filter(|_| bytes.len() as u64 >= self.multipart_geometry.threshold_bytes)
        else {
            return crate::immutable_write::put(self, key, bytes).await;
        };
        let sha256 = Checksum::sha256(&bytes);
        crate::immutable_write::put_with(self, key, &bytes, || {
            let bytes = bytes.clone();
            let sha256 = &sha256;
            async move {
                let mut parts = PartReader::new(
                    stream::once(async { Ok(bytes) }).boxed(),
                    self.multipart_geometry.part_bytes as usize,
                );
                let head = parts.next_part().await?.unwrap_or_default();
                controller
                    .put_if_absent(
                        key,
                        head,
                        parts,
                        Some(sha256),
                        PROVIDER_MULTIPART_PART_WINDOW,
                    )
                    .await
            }
        })
        .await
    }

    /// Cuts the payload into parts as it arrives and uploads them one at a
    /// time, so a large object costs one part of memory instead of its own
    /// size. The server budgets a client's upload at that one part.
    ///
    /// The first part is cut before anything is decided. A payload that
    /// ends inside it is an ordinary [`ObjectStore::put`] with the caller's
    /// mode enforced by the provider — which is the same size line `put`
    /// itself draws, so a small streamed write behaves exactly like a small
    /// buffered one. Anything longer goes through the provider's multipart
    /// upload. A create-if-absent completes conditionally through the
    /// provider's own requests where it has them. Elsewhere a provider
    /// assembles the upload unconditionally, so a conditional mode is checked
    /// against a separate read of the key after the payload is consumed and
    /// immediately before completion.
    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.to_path(key)?;
        self.validate_compare_token(key, &mode)?;
        let mut reader = PartReader::new(body, self.multipart_geometry.part_bytes as usize);
        let head = reader.next_part().await?.unwrap_or_else(Bytes::new);
        if reader.exhausted() {
            let size_bytes = head.len() as u64;
            self.put(key, head, mode).await?;
            return Ok(size_bytes);
        }
        self.put_parts(key, head, reader, mode, None, 1)
            .await
            .map(|metadata| metadata.size_bytes)
    }

    /// A body below the multipart threshold is buffered and takes the
    /// attested single-request create. A longer one is cut into parts as it
    /// arrives and goes through [`Self::put_parts`].
    async fn put_immutable_verified_stream(
        &self,
        key: &str,
        size_bytes: u64,
        sha256: Option<&Checksum>,
        body: BoxStream<'_, Result<Bytes>>,
    ) -> std::result::Result<ObjectMetadata, crate::ImmutableWriteError> {
        let created = async {
            if size_bytes < self.multipart_geometry.threshold_bytes {
                let bytes = collect_stream(body).await?;
                return self
                    .put_attested(key, bytes, PutMode::CreateIfAbsent, sha256.cloned())
                    .await;
            }
            let mut reader = PartReader::new(body, self.multipart_geometry.part_bytes as usize);
            let head = reader.next_part().await?.unwrap_or_default();
            self.put_parts(
                key,
                head,
                reader,
                PutMode::CreateIfAbsent,
                sha256,
                PROVIDER_MULTIPART_PART_WINDOW,
            )
            .await
        }
        .await;
        crate::immutable_write::decide_created(self, key, sha256, created).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.to_path(key)?;
        let deadline =
            OperationDeadline::start(self.timer.as_ref(), self.transport_retry.operation_deadline);
        with_transport_retry(
            &self.transport_retry,
            key,
            "delete",
            0,
            Some(&deadline),
            provider_transport_retryable,
            || async {
                match self.inner.delete(&path).await {
                    Err(error) if provider_not_found(&error) => Ok(()),
                    result => result,
                }
            },
        )
        .await
        .map_err(|error| map_provider_error(key, error))
    }

    /// A provider without a path that leaves the base in place rewrites it:
    /// one ranged read of the base, then one compare-and-swap of base and
    /// pieces.
    async fn extend_object(
        &self,
        key: &str,
        base: &ExtendBase,
        pieces: Bytes,
        result: &ExtendedObject,
    ) -> Result<ObjectMetadata> {
        if let Some(controller) = &self.multipart_controller {
            if let Some(extended) = controller
                .extend_object(key, base, pieces.clone(), result)
                .await?
            {
                return Ok(extended);
            }
        }
        let precondition_failed = || ObjectStoreError::PreconditionFailed {
            object_key: key.to_owned(),
        };
        // The read names the version by its ETag and reports the object's
        // whole length, so a base that moved on, or one longer than the
        // caller believes, fails here instead of being rewritten short.
        let existing = if base.length == 0 {
            let metadata = self.head(key).await?.ok_or_else(precondition_failed)?;
            if metadata.size_bytes != 0 || metadata.etag.as_deref() != Some(&base.etag) {
                return Err(precondition_failed());
            }
            Bytes::new()
        } else {
            let path = self.to_path(key)?;
            let options = GetOptions {
                if_match: Some(base.etag.clone()),
                range: Some(GetRange::Bounded(Range {
                    start: 0,
                    end: base.length,
                })),
                ..Default::default()
            };
            let result = match self.inner.get_opts(&path, options).await {
                Ok(result) => result,
                Err(err) if provider_not_found(&err) => return Err(precondition_failed()),
                Err(err) => return Err(map_provider_error(key, err)),
            };
            if result.meta.size != base.length {
                return Err(precondition_failed());
            }
            result
                .bytes()
                .await
                .map_err(|err| map_provider_error(key, err))?
        };
        let bytes = Bytes::from([existing.as_ref(), pieces.as_ref()].concat());
        if result.crc.as_ref().is_some_and(|crc| !crc.matches(&bytes)) {
            return Err(ObjectStoreError::ChecksumMismatch {
                object_key: key.to_owned(),
            });
        }
        let mode = PutMode::CompareAndSwap {
            expected_etag: base.etag.clone(),
        };
        self.put_attested(key, bytes, mode, Some(result.sha256.clone()))
            .await
    }

    async fn put_immutable_extended(
        &self,
        key: &str,
        base_key: &str,
        base: &ExtendBase,
        pieces: Bytes,
        result: &ExtendedObject,
    ) -> Result<Option<ObjectMetadata>> {
        match &self.multipart_controller {
            Some(controller) => {
                controller
                    .put_immutable_extended(key, base_key, base, pieces, result)
                    .await
            }
            None => Ok(None),
        }
    }

    fn list_prefix_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<String>> {
        let prefix_path = match self.list_path(prefix) {
            Ok(prefix_path) => prefix_path,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let offset = match start_after.map(|key| self.to_path(key)).transpose() {
            Ok(offset) => offset,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let key_prefix = self.key_prefix.clone();
        let listed_prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        let listed = match offset.as_ref() {
            Some(offset) => self.inner.list_with_offset(prefix_path.as_ref(), offset),
            None => self.inner.list(prefix_path.as_ref()),
        };
        listed
            .filter_map(move |result| {
                let key_prefix = key_prefix.clone();
                let listed_prefix = listed_prefix.clone();
                let start_after = start_after.clone();
                async move {
                    match result {
                        Ok(meta) => {
                            let key = meta.location.as_ref();
                            let key = match key_prefix.as_deref() {
                                Some(prefix) => unscope_listed_key(Some(prefix), key).map(Ok),
                                None => Some(Ok(key.to_owned())),
                            };
                            match key {
                                Some(Ok(key))
                                    if start_after
                                        .as_deref()
                                        .is_some_and(|start_after| key.as_str() <= start_after) =>
                                {
                                    None
                                }
                                other => other,
                            }
                        }
                        Err(err) => Some(Err(map_provider_error(&listed_prefix, err))),
                    }
                }
            })
            .boxed()
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: EffectiveLimit,
    ) -> Result<Page<String, String>> {
        let scoped_prefix = scope_child_listing_prefix(self.key_prefix.as_deref(), prefix)?;
        let offset = start_after
            .map(|key| {
                // Every key under a child `p/c/` sorts before `p/c0`, because
                // `0` is the byte after `/`. Starting after `p/c0` skips the
                // whole child; the only other key it skips is the object
                // `p/c0`, which is never a child.
                let resume_key = match key.strip_suffix('/') {
                    Some(child) => format!("{child}0"),
                    None => key.to_owned(),
                };
                scope_object_key(self.key_prefix.as_deref(), &resume_key)
            })
            .transpose()?;
        let options = PaginatedListOptions {
            offset,
            delimiter: Some(Cow::Borrowed("/")),
            max_keys: Some(limit.as_usize()),
            ..Default::default()
        };
        let listed = self
            .paginated
            .list_paginated(
                Some(scoped_prefix.as_str()).filter(|scoped| !scoped.is_empty()),
                options,
            )
            .await
            .map_err(|err| map_provider_error(prefix, err))?;
        let children: Vec<String> = listed
            .result
            .common_prefixes
            .iter()
            .filter_map(|child| self.unscoped(&format!("{child}/")))
            .collect();
        let next_cursor = match listed.page_token {
            None => None,
            Some(_) => {
                let last_object = listed
                    .result
                    .objects
                    .last()
                    .and_then(|object| self.unscoped(object.location.as_ref()));
                let last_entry = children.last().cloned().max(last_object);
                Some(last_entry.ok_or_else(|| {
                    ObjectStoreError::transport(
                        prefix,
                        "provider reported more entries after an empty listing page",
                    )
                })?)
            }
        };
        Ok(Page {
            items: children,
            next_cursor,
        })
    }
}

impl ProviderObjectStore {
    /// Writes one object and records `sha256` as its attestation when given.
    ///
    /// A large create-if-absent uses the provider's own conditional requests
    /// where it has them; every other conditional write is one request.
    async fn put_attested(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
        sha256: Option<Checksum>,
    ) -> Result<ObjectMetadata> {
        let path = self.to_path(key)?;
        self.validate_compare_token(key, &mode)?;
        let size_bytes = bytes.len() as u64;
        if size_bytes >= self.multipart_geometry.threshold_bytes
            && matches!(mode, PutMode::Overwrite)
        {
            return self
                .put_large_multipart(Arc::clone(&self.multipart), key, &path, bytes)
                .await;
        }
        // A conditional write is one request at every size, so its
        // precondition failure is never its own landing, and a publication
        // ends within one request's allowance.
        let client = match mode {
            PutMode::Overwrite => &self.inner,
            PutMode::CreateIfAbsent | PutMode::CompareAndSwap { .. } => &self.one_attempt,
        };
        let compare_and_swap = matches!(mode, PutMode::CompareAndSwap { .. });
        let mut attributes = Attributes::new();
        if let Some(sha256) = &sha256 {
            attributes.insert(
                Attribute::Metadata(SHA256_METADATA_KEY.into()),
                sha256.value.clone().into(),
            );
        }
        let options = PutOptions {
            mode: self.map_put_mode(mode),
            attributes,
            ..Default::default()
        };
        match client
            .put_opts(&path, PutPayload::from(bytes), options)
            .await
        {
            Ok(result) => Ok(ObjectMetadata {
                sha256,
                ..self.metadata_from_put_result(result, size_bytes)
            }),
            Err(err) if compare_and_swap && provider_not_found(&err) => {
                Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                })
            }
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    /// Uploads `head` and then every part of `rest`. A create-if-absent goes
    /// through the provider's own conditional create where it has one, which
    /// records `sha256` when the upload starts and keeps at most
    /// `part_window` parts in flight. Elsewhere the upstream multipart upload
    /// sends one part at a time, checks `mode` against a `head` before
    /// completion, and records no attestation.
    async fn put_parts(
        &self,
        key: &str,
        head: Bytes,
        rest: PartReader<'_>,
        mode: PutMode,
        sha256: Option<&Checksum>,
        part_window: usize,
    ) -> Result<ObjectMetadata> {
        if let (PutMode::CreateIfAbsent, Some(controller)) = (&mode, &self.multipart_controller) {
            return controller
                .put_if_absent(key, head, rest, sha256, part_window)
                .await;
        }
        let path = self.to_path(key)?;
        let upload = MultipartWrite {
            store: self,
            multipart: Arc::clone(&self.multipart),
            key,
            path: &path,
        };
        let (upload_id, mut abort_on_drop) = upload.create(0).await?;
        let result = upload
            .upload_stream_and_complete(&upload_id, head, rest, &mode)
            .await;
        if result.is_err() {
            // Best effort, and harmless when the failure raced a landed
            // completion: the upload id no longer exists then.
            upload.abort(&upload_id).await;
        }
        abort_on_drop.disarm();
        result
    }

    fn map_put_mode(&self, mode: PutMode) -> provider_store::PutMode {
        match mode {
            PutMode::Overwrite => provider_store::PutMode::Overwrite,
            PutMode::CreateIfAbsent => provider_store::PutMode::Create,
            PutMode::CompareAndSwap { expected_etag } => {
                let version = match self.compare_token {
                    CompareToken::Etag => UpdateVersion {
                        e_tag: Some(expected_etag),
                        version: None,
                    },
                    CompareToken::Generation => UpdateVersion {
                        e_tag: None,
                        version: Some(expected_etag),
                    },
                };
                provider_store::PutMode::Update(version)
            }
        }
    }
}

/// Converts a provider timestamp to object age information.
///
/// Some AWS-compatible clients represent a missing `Last-Modified` header as
/// Unix epoch zero. Returning that value would make garbage collection treat
/// the object as extremely old. Non-positive timestamps are therefore
/// returned as `None`, which causes unknown-age objects to be retained.
fn last_modified_ms(timestamp_millis: i64) -> Option<u64> {
    match timestamp_millis > 0 {
        true => u64::try_from(timestamp_millis).ok(),
        false => None,
    }
}

enum RangedGet {
    Bytes(Bytes),
    NotFound,
    Refused(provider_store::Error),
}

fn provider_not_found(err: &provider_store::Error) -> bool {
    matches!(err, provider_store::Error::NotFound { .. })
}

pub(crate) fn map_provider_error(object_key: &str, err: provider_store::Error) -> ObjectStoreError {
    match err {
        provider_store::Error::NotFound { .. } => ObjectStoreError::NotFound {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::AlreadyExists { .. }
        | provider_store::Error::Precondition { .. }
        | provider_store::Error::NotModified { .. } => ObjectStoreError::PreconditionFailed {
            object_key: object_key.to_owned(),
        },
        provider_store::Error::InvalidPath { source } => ObjectStoreError::InvalidKey {
            object_key: object_key.to_owned(),
            message: source.to_string(),
        },
        provider_store::Error::NotSupported { .. }
        | provider_store::Error::NotImplemented { .. } => {
            ObjectStoreError::Unsupported("provider object store operation")
        }
        provider_store::Error::UnknownConfigurationKey { key, store } => {
            ObjectStoreError::transport(
                object_key,
                format!("unknown {store} configuration key `{key}`"),
            )
        }
        provider_store::Error::Generic { source, .. } => {
            match source.downcast::<ObjectStoreError>() {
                Ok(error) => *error,
                Err(source) => ObjectStoreError::retryable_transport(
                    object_key,
                    sanitize_provider_message(&source.to_string()),
                ),
            }
        }
        provider_store::Error::JoinError { source } => {
            ObjectStoreError::transport(object_key, sanitize_provider_message(&source.to_string()))
        }
        provider_store::Error::PermissionDenied { source, .. }
        | provider_store::Error::Unauthenticated { source, .. } => {
            ObjectStoreError::PermissionDenied {
                object_key: object_key.to_owned(),
                message: sanitize_provider_message(&source.to_string()),
            }
        }
        other => {
            ObjectStoreError::transport(object_key, sanitize_provider_message(&other.to_string()))
        }
    }
}

/// Credential query parameters that providers may include in error messages.
const CREDENTIAL_QUERY_PARAMS: &[&str] = &[
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "Signature",
    "sig",
];

/// XML elements that may contain signing data in authentication errors.
const CREDENTIAL_XML_ELEMENTS: &[&str] = &[
    "StringToSign",
    "StringToSignBytes",
    "CanonicalRequest",
    "SignatureProvided",
    "AWSAccessKeyId",
];

/// Redacts credential and signing data from provider error messages.
fn sanitize_provider_message(message: &str) -> String {
    let mut sanitized = message.to_owned();
    for param in CREDENTIAL_QUERY_PARAMS {
        sanitized = mask_query_param_values(&sanitized, param);
    }
    for element in CREDENTIAL_XML_ELEMENTS {
        sanitized = mask_xml_element_text(&sanitized, element);
    }
    sanitized
}

/// Redacts a query parameter without matching it inside a longer name.
fn mask_query_param_values(message: &str, param: &str) -> String {
    let needle = format!("{param}=");
    let mut out = String::with_capacity(message.len());
    let mut cursor = 0;
    while let Some(found) = message[cursor..].find(&needle) {
        let start = cursor + found;
        let value_start = start + needle.len();
        out.push_str(&message[cursor..value_start]);
        cursor = value_start;
        let at_boundary = start > 0 && matches!(message.as_bytes()[start - 1], b'?' | b'&');
        if at_boundary {
            let value_len = message[cursor..]
                .find(|c: char| {
                    matches!(c, '&' | '"' | '\'' | ')' | '<' | '>' | ':' | ',') || c.is_whitespace()
                })
                .unwrap_or(message.len() - cursor);
            out.push_str("<redacted>");
            cursor += value_len;
        }
    }
    out.push_str(&message[cursor..]);
    out
}

/// Redacts an XML element, including a truncated element without a closing tag.
fn mask_xml_element_text(message: &str, element: &str) -> String {
    let open = format!("<{element}>");
    let close = format!("</{element}>");
    let mut out = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(position) = rest.find(&open) {
        let text_start = position + open.len();
        out.push_str(&rest[..text_start]);
        out.push_str("<redacted>");
        rest = &rest[text_start..];
        match rest.find(&close) {
            Some(text_end) => rest = &rest[text_end..],
            None => rest = "",
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::aws_credentials::{aws_credentials_source, ObjectStoreAwsCredentialProvider};
    use crate::metrics::{
        InstrumentedObjectStore, ObjectStoreOperation, VecObjectStoreMetricsRecorder,
    };
    use crate::test_support::{aws_environment_lock, isolated_aws_environment, SteppingTimer};
    use futures::{StreamExt, TryStreamExt};
    use loonfs_types::transport_retry_backoff;
    use object_store::client::CredentialProvider;
    use object_store::memory::InMemory;
    use provider_store::list::PaginatedListResult;

    fn memory_store() -> ProviderObjectStore {
        memory_store_over(Arc::new(InMemory::default()))
    }

    fn memory_store_over(inner: Arc<InMemory>) -> ProviderObjectStore {
        ProviderObjectStore::new(
            Arc::clone(&inner) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&inner) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&inner) as Arc<dyn MultipartStore>,
            Arc::new(DelimiterPages(inner)),
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
            },
            ConfiguredObjectStoreKind::LocalFs,
            StoreIoRuntime::new().expect("store io runtime"),
        )
        .expect("provider store")
    }

    /// Answers paged delimiter listings over an in-memory provider, which
    /// does not offer them itself.
    struct DelimiterPages(Arc<InMemory>);

    #[async_trait]
    impl PaginatedListStore for DelimiterPages {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> provider_store::Result<PaginatedListResult> {
            delimiter_page(&self.0, prefix, options).await
        }
    }

    /// Builds one page the way S3 does: keys after `offset` roll up into a
    /// common prefix at the first `/` past `prefix`, and objects and common
    /// prefixes share `max_keys`.
    async fn delimiter_page(
        store: &InMemory,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> provider_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let max_keys = options.max_keys.unwrap_or(1_000);
        let mut listed: Vec<ObjectMeta> = provider_store::ObjectStore::list(store, None)
            .try_collect()
            .await?;
        listed.sort_by(|left, right| left.location.as_ref().cmp(right.location.as_ref()));
        let mut objects = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut truncated = false;
        for meta in listed {
            let key = meta.location.as_ref().to_owned();
            let after_offset = options
                .offset
                .as_deref()
                .is_none_or(|offset| key.as_str() > offset);
            if !key.starts_with(prefix) || !after_offset {
                continue;
            }
            let common = key[prefix.len()..]
                .find('/')
                .map(|end| key[..prefix.len() + end + 1].to_owned());
            if common.is_some() && common.as_ref() == common_prefixes.last() {
                continue;
            }
            if objects.len() + common_prefixes.len() == max_keys {
                truncated = true;
                break;
            }
            match common {
                Some(common) => common_prefixes.push(common),
                None => objects.push(meta),
            }
        }
        Ok(PaginatedListResult {
            result: ListResult {
                common_prefixes: common_prefixes
                    .iter()
                    .map(Path::parse)
                    .collect::<std::result::Result<_, _>>()?,
                objects,
                extensions: Default::default(),
            },
            page_token: truncated.then(|| "more".to_owned()),
        })
    }

    #[test]
    fn a_synthesized_epoch_stamp_reads_as_no_timestamp_at_all() {
        assert_eq!(last_modified_ms(0), None);
        assert_eq!(last_modified_ms(-1), None);
        assert_eq!(last_modified_ms(i64::MIN), None);
        assert_eq!(last_modified_ms(1), Some(1));
        assert_eq!(last_modified_ms(1_754_000_000_000), Some(1_754_000_000_000));
    }

    #[test]
    fn provider_client_failures_are_classified_at_the_adapter_boundary() {
        let path = Path::from("private-key");
        assert_eq!(
            map_provider_error("private-key", transport_glitch()).class(),
            crate::ObjectStoreErrorClass::RetryableTransport
        );
        assert_eq!(
            map_provider_error("private-key", auth_rejection(&path)).class(),
            crate::ObjectStoreErrorClass::PermissionDenied
        );
    }

    #[tokio::test]
    async fn credential_provider_failures_preserve_configuration_class_and_public_message() {
        let _lock = aws_environment_lock().await;
        let tempdir =
            tempfile::tempdir().expect("temporary AWS config directory should be created");
        let _environment = isolated_aws_environment(&tempdir, None);
        let source = aws_credentials_source(&crate::AwsS3Credentials::Ambient {}, "us-east-1")
            .expect("ambient credential source should be constructed");
        let provider = ObjectStoreAwsCredentialProvider::new(source);
        let error = provider
            .get_credential()
            .await
            .expect_err("isolated credential chain should be empty");

        let error = map_provider_error("private-key", error);

        assert_eq!(error.class(), crate::ObjectStoreErrorClass::Configuration);
        assert_eq!(
            error.to_string(),
            "invalid object store configuration: could not resolve `store.credentials` through the standard AWS credential chain"
        );
        assert_eq!(
            error.public_message(),
            "object-store configuration is invalid; verify the provider, bucket, credentials, endpoint, and key prefix fields"
        );
    }

    #[test]
    fn compare_tokens_populate_only_the_matching_provider_field() {
        for (compare_token, expected_etag, expected_version) in [
            (CompareToken::Etag, Some("token"), None),
            (CompareToken::Generation, None, Some("token")),
        ] {
            let store = memory_store().compare_token(compare_token);
            let provider_store::PutMode::Update(version) =
                store.map_put_mode(PutMode::CompareAndSwap {
                    expected_etag: "token".to_owned(),
                })
            else {
                panic!("compare-and-swap maps to an update")
            };

            assert_eq!(version.e_tag.as_deref(), expected_etag);
            assert_eq!(version.version.as_deref(), expected_version);
        }
    }

    #[test]
    fn provider_messages_drop_credential_material_and_keep_the_diagnosis() {
        let presigned = sanitize_provider_message(
            "Generic S3 error: error sending request for url \
             (https://bucket.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20260726%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Signature=deadbeefcafe): operation timed out",
        );
        assert!(!presigned.contains("AKIAIOSFODNN7EXAMPLE"), "{presigned}");
        assert!(!presigned.contains("deadbeefcafe"), "{presigned}");
        assert!(
            presigned.contains("X-Amz-Signature=<redacted>"),
            "{presigned}"
        );
        assert!(presigned.contains("operation timed out"), "{presigned}");
        assert!(
            presigned.contains("bucket.s3.amazonaws.com/k"),
            "{presigned}"
        );

        let signature_mismatch = sanitize_provider_message(
            "Client error with status 403 Forbidden: <Error>\
             <Code>SignatureDoesNotMatch</Code>\
             <StringToSign>AWS4-HMAC-SHA256 20260726T000000Z scope digest</StringToSign>\
             <SignatureProvided>cafe0123</SignatureProvided>\
             <AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId></Error>",
        );
        assert!(
            !signature_mismatch.contains("AKIAIOSFODNN7EXAMPLE"),
            "{signature_mismatch}"
        );
        assert!(
            !signature_mismatch.contains("cafe0123"),
            "{signature_mismatch}"
        );
        assert!(
            !signature_mismatch.contains("20260726T000000Z"),
            "{signature_mismatch}"
        );
        assert!(
            signature_mismatch.contains("SignatureDoesNotMatch"),
            "{signature_mismatch}"
        );
        assert!(
            signature_mismatch.contains("403 Forbidden"),
            "{signature_mismatch}"
        );

        let azure_sas = sanitize_provider_message(
            "error for url https://account.blob.core.windows.net/c/k?sv=2021-08-06\
             &se=2026-07-26&sig=aGVsbG8: 403",
        );
        assert!(!azure_sas.contains("aGVsbG8"), "{azure_sas}");
        assert!(azure_sas.contains("sig=<redacted>"), "{azure_sas}");

        // A bare name inside a longer parameter is not a boundary match.
        let unrelated = sanitize_provider_message("policy?ResponseSignature=keep&sigil=keep2");
        assert!(unrelated.contains("keep2"), "{unrelated}");
        assert!(!unrelated.contains("Signature=<redacted>"), "{unrelated}");
    }

    #[tokio::test]
    async fn provider_store_preserves_put_get_head_and_prefix_scoping() {
        let store = memory_store();
        let key = "namespaces/demo/hint.json";

        let metadata = store
            .put_if_absent(key, Bytes::from_static(b"head"))
            .await
            .expect("put");
        assert_eq!(metadata.size_bytes, 4);
        assert!(metadata.etag.is_some());

        let head = store.head(key).await.expect("head").expect("head exists");
        assert_eq!(head.size_bytes, 4);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"head"))
        );
        assert_eq!(
            store.list_prefix("namespaces/demo/").await.expect("list"),
            vec![key.to_owned()]
        );
    }

    #[tokio::test]
    async fn child_prefix_pages_resume_after_whole_children_inside_the_key_prefix() {
        let inner = Arc::new(InMemory::default());
        let store = memory_store_over(Arc::clone(&inner));
        for key in [
            "namespaces/a/hint.json",
            "namespaces/a1",
            "namespaces/b/hint.json",
            "namespaces/b/wal/00000000000000000001.wal.zst",
            "namespaces/b0/hint.json",
            "namespaces/c/hint.json",
        ] {
            store
                .put_overwrite(key, Bytes::from_static(b"listed"))
                .await
                .expect("put");
        }
        provider_store::ObjectStoreExt::put(
            inner.as_ref(),
            &Path::from("tenant-b/namespaces/z/hint.json"),
            PutPayload::from_static(b"outside"),
        )
        .await
        .expect("put outside the key prefix");

        let first = child_page(&store, "namespaces/", None, 2).await;
        assert_eq!(first.items, ["namespaces/a/"]);
        assert_eq!(first.next_cursor.as_deref(), Some("namespaces/a1"));
        let second = child_page(&store, "namespaces/", Some("namespaces/a1"), 2).await;
        assert_eq!(second.items, ["namespaces/b/", "namespaces/b0/"]);
        assert_eq!(second.next_cursor.as_deref(), Some("namespaces/b0/"));
        let third = child_page(&store, "namespaces/", Some("namespaces/b0/"), 2).await;
        assert_eq!(third.items, ["namespaces/c/"]);
        assert_eq!(third.next_cursor, None);

        let after_child = child_page(&store, "namespaces/", Some("namespaces/b/"), 2).await;
        assert_eq!(after_child.items, ["namespaces/b0/", "namespaces/c/"]);
        let objects_only = child_page(&store, "namespaces/", Some("namespaces/a/"), 1).await;
        assert!(objects_only.items.is_empty());
        assert_eq!(objects_only.next_cursor.as_deref(), Some("namespaces/a1"));
        let root = child_page(&store, "", None, 10).await;
        assert_eq!(root.items, ["namespaces/"]);
    }

    async fn child_page(
        store: &ProviderObjectStore,
        prefix: &str,
        start_after: Option<&str>,
        limit: u32,
    ) -> Page<String, String> {
        let limit = EffectiveLimit::new(std::num::NonZeroU32::new(limit).expect("nonzero limit"));
        store
            .list_child_prefixes(prefix, start_after, limit)
            .await
            .expect("list child prefixes")
    }

    #[tokio::test]
    async fn ranged_reads_match_the_reference_contract_in_one_round_trip() {
        let store = memory_store();
        let key = "namespaces/demo/segments/seg_abc.sst.zst";
        store
            .put_if_absent(key, Bytes::from_static(b"0123456789"))
            .await
            .expect("put");

        let range = |start, end| {
            Some(ByteRange {
                start_inclusive: start,
                end_exclusive: end,
            })
        };

        // In-bounds slice.
        assert_eq!(
            store.get(key, range(2, 6)).await.expect("bounded"),
            Some(Bytes::from_static(b"2345"))
        );
        // An end past the object clamps.
        assert_eq!(
            store.get(key, range(6, 99)).await.expect("clamped"),
            Some(Bytes::from_static(b"6789"))
        );
        // Reading at the exact end is empty, not an error.
        assert_eq!(
            store.get(key, range(10, 12)).await.expect("at end"),
            Some(Bytes::new())
        );
        // A start past the end is an invalid range.
        assert!(matches!(
            store.get(key, range(11, 12)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
        // An inverted range is rejected without any store call.
        assert!(matches!(
            store.get(key, range(6, 2)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
        // Zero-length reads answer from existence and size alone.
        assert_eq!(
            store.get(key, range(4, 4)).await.expect("zero length"),
            Some(Bytes::new())
        );

        // A missing object is `Ok(None)` however the request is shaped —
        // the same answer the unranged read and the local provider give.
        let missing = "namespaces/demo/segments/seg_missing.sst.zst";
        assert_eq!(
            store.get(missing, range(0, 4)).await.expect("missing"),
            None
        );
        assert_eq!(
            store.get(missing, range(3, 3)).await.expect("missing zero"),
            None
        );
    }

    #[tokio::test]
    async fn provider_store_enforces_create_and_cas_preconditions() {
        let store = memory_store();
        let key = "namespaces/demo/hint.json";
        let first = store
            .put_if_absent(key, Bytes::from_static(b"one"))
            .await
            .expect("first put");

        assert!(matches!(
            store.put_if_absent(key, Bytes::from_static(b"two")).await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        assert!(matches!(
            store
                .compare_and_swap(key, "stale", Bytes::from_static(b"two"))
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        assert!(matches!(
            store
                .compare_and_swap(
                    "namespaces/missing/hint.json",
                    "missing",
                    Bytes::from_static(b"two")
                )
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        let etag = first.etag.expect("etag");
        store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect("cas");
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"two"))
        );
    }

    #[tokio::test]
    async fn provider_stream_reports_invalid_prefix() {
        let store = memory_store();
        let mut stream = store.list_prefix_stream("../");
        assert!(matches!(
            stream.next().await,
            Some(Err(ObjectStoreError::InvalidKey { .. }))
        ));
    }

    use provider_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
    };
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    #[derive(Debug)]
    enum WriteScript {
        FailWithoutLanding,
        LandThenFail,
        FailAuth,
    }

    /// Provider double that fails scripted attempts before delegating to an
    /// in-memory store, so retry behavior is observable per attempt.
    ///
    /// Multipart state is held here rather than delegated: the in-memory
    /// provider requires parts to arrive in index order, while real
    /// providers (and this transport's retry-driven interleavings) allow
    /// any order.
    #[derive(Default)]
    struct FlakyStore {
        inner: InMemory,
        put_script: Mutex<VecDeque<WriteScript>>,
        delete_script: Arc<Mutex<VecDeque<WriteScript>>>,
        part_script: Mutex<HashMap<usize, VecDeque<WriteScript>>>,
        complete_script: Mutex<VecDeque<WriteScript>>,
        puts: AtomicUsize,
        gets: AtomicUsize,
        heads: AtomicUsize,
        deletes: Arc<AtomicUsize>,
        multipart_creates: AtomicUsize,
        part_attempts: Mutex<HashMap<usize, usize>>,
        multipart_completes: AtomicUsize,
        multipart_aborts: AtomicUsize,
        block_part_uploads: AtomicBool,
        part_started: Notify,
        multipart_aborted: Notify,
        next_upload_id: AtomicUsize,
        multipart_uploads: Mutex<HashMap<String, BTreeMap<usize, Bytes>>>,
    }

    impl fmt::Debug for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FlakyStore").finish_non_exhaustive()
        }
    }

    impl fmt::Display for FlakyStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "FlakyStore")
        }
    }

    fn transport_glitch() -> provider_store::Error {
        provider_store::Error::Generic {
            store: "flaky",
            source: "error sending request".into(),
        }
    }

    fn auth_rejection(location: &Path) -> provider_store::Error {
        provider_store::Error::PermissionDenied {
            path: location.to_string(),
            source: "access denied".into(),
        }
    }

    #[async_trait]
    impl provider_store::ObjectStore for FlakyStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> provider_store::Result<PutResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let script = self.put_script.lock().expect("put script").pop_front();
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(location)),
                None => self.inner.put_opts(location, payload, opts).await,
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> provider_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> provider_store::Result<GetResult> {
            let reads = if options.head {
                &self.heads
            } else {
                &self.gets
            };
            reads.fetch_add(1, Ordering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, provider_store::Result<Path>>,
        ) -> BoxStream<'static, provider_store::Result<Path>> {
            let inner = self.inner.clone();
            let deletes = Arc::clone(&self.deletes);
            let delete_script = Arc::clone(&self.delete_script);
            locations
                .then(move |location| {
                    let inner = inner.clone();
                    let deletes = Arc::clone(&deletes);
                    let delete_script = Arc::clone(&delete_script);
                    async move {
                        let location = location?;
                        deletes.fetch_add(1, Ordering::SeqCst);
                        let script = delete_script.lock().expect("delete script").pop_front();
                        match script {
                            Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                            Some(WriteScript::LandThenFail) => {
                                inner.delete(&location).await?;
                                Err(transport_glitch())
                            }
                            Some(WriteScript::FailAuth) => Err(auth_rejection(&location)),
                            None => inner.delete(&location).await.map(|()| location),
                        }
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, provider_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> provider_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> provider_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    impl FlakyStore {
        fn store_part(
            &self,
            id: &provider_store::MultipartId,
            part_idx: usize,
            data: PutPayload,
        ) -> provider_store::Result<()> {
            let mut uploads = self.multipart_uploads.lock().expect("uploads");
            let upload =
                uploads
                    .get_mut(id.as_str())
                    .ok_or_else(|| provider_store::Error::NotFound {
                        path: id.clone(),
                        source: "no such upload".into(),
                    })?;
            upload.insert(part_idx, Bytes::from(data));
            Ok(())
        }

        async fn land_completion(
            &self,
            path: &Path,
            id: &provider_store::MultipartId,
            parts: &[PartId],
        ) -> provider_store::Result<PutResult> {
            let upload = self
                .multipart_uploads
                .lock()
                .expect("uploads")
                .remove(id.as_str())
                .ok_or_else(|| provider_store::Error::NotFound {
                    path: id.clone(),
                    source: "no such upload".into(),
                })?;
            assert_eq!(
                upload.len(),
                parts.len(),
                "completion must list exactly the uploaded parts"
            );
            let mut buf = Vec::new();
            for part in upload.values() {
                buf.extend_from_slice(part);
            }
            provider_store::ObjectStore::put_opts(
                &self.inner,
                path,
                buf.into(),
                PutOptions::default(),
            )
            .await
        }
    }

    #[async_trait]
    impl PaginatedListStore for FlakyStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> provider_store::Result<PaginatedListResult> {
            delimiter_page(&self.inner, prefix, options).await
        }
    }

    #[async_trait]
    impl MultipartStore for FlakyStore {
        async fn create_multipart(
            &self,
            _path: &Path,
        ) -> provider_store::Result<provider_store::MultipartId> {
            self.multipart_creates.fetch_add(1, Ordering::SeqCst);
            let id = self
                .next_upload_id
                .fetch_add(1, Ordering::SeqCst)
                .to_string();
            self.multipart_uploads
                .lock()
                .expect("uploads")
                .insert(id.clone(), BTreeMap::new());
            Ok(id)
        }

        async fn put_part(
            &self,
            path: &Path,
            id: &provider_store::MultipartId,
            part_idx: usize,
            data: PutPayload,
        ) -> provider_store::Result<PartId> {
            if self.block_part_uploads.load(Ordering::SeqCst) {
                self.part_started.notify_one();
                return std::future::pending().await;
            }
            *self
                .part_attempts
                .lock()
                .expect("part attempts")
                .entry(part_idx)
                .or_default() += 1;
            let script = self
                .part_script
                .lock()
                .expect("part script")
                .get_mut(&part_idx)
                .and_then(VecDeque::pop_front);
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.store_part(id, part_idx, data)?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(path)),
                None => {
                    self.store_part(id, part_idx, data)?;
                    Ok(PartId {
                        content_id: part_idx.to_string(),
                    })
                }
            }
        }

        async fn complete_multipart(
            &self,
            path: &Path,
            id: &provider_store::MultipartId,
            parts: Vec<PartId>,
        ) -> provider_store::Result<PutResult> {
            self.multipart_completes.fetch_add(1, Ordering::SeqCst);
            let script = self
                .complete_script
                .lock()
                .expect("complete script")
                .pop_front();
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.land_completion(path, id, &parts).await?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(path)),
                None => self.land_completion(path, id, &parts).await,
            }
        }

        async fn abort_multipart(
            &self,
            _path: &Path,
            id: &provider_store::MultipartId,
        ) -> provider_store::Result<()> {
            self.multipart_aborts.fetch_add(1, Ordering::SeqCst);
            self.multipart_uploads
                .lock()
                .expect("uploads")
                .remove(id.as_str());
            self.multipart_aborted.notify_one();
            Ok(())
        }
    }

    fn retrying_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
        ProviderObjectStore::new(
            Arc::clone(&flaky) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&flaky) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&flaky) as Arc<dyn MultipartStore>,
            flaky,
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
            },
            ConfiguredObjectStoreKind::LocalFs,
            StoreIoRuntime::new().expect("store io runtime"),
        )
        .expect("provider store")
        .transport_retry(TransportRetryPolicy {
            max_retries: 4,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            operation_deadline: PROVIDER_OPERATION_DEADLINE,
        })
    }

    fn script_puts(flaky: &FlakyStore, script: impl IntoIterator<Item = WriteScript>) {
        flaky.put_script.lock().expect("put script").extend(script);
    }

    #[test]
    fn transport_retry_backoff_doubles_and_caps() {
        let policy = DEFAULT;
        assert_eq!(
            transport_retry_backoff(&policy, 1),
            Duration::from_millis(100)
        );
        assert_eq!(
            transport_retry_backoff(&policy, 2),
            Duration::from_millis(200)
        );
        assert_eq!(
            transport_retry_backoff(&policy, 8),
            Duration::from_millis(12_800)
        );
        assert_eq!(transport_retry_backoff(&policy, 9), Duration::from_secs(15));
        assert_eq!(
            transport_retry_backoff(&policy, 10),
            Duration::from_secs(15)
        );
    }

    #[tokio::test]
    async fn mutable_overwrite_transport_failure_is_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);
        let key = "namespaces/demo/uploads/upl_1.json";

        let error = store
            .put_overwrite(key, Bytes::from_static(b"session"))
            .await
            .expect_err("mutable overwrite must surface an ambiguous outcome");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn only_a_create_carries_an_attestation_that_head_and_get_report() {
        let store = memory_store();
        let created = "namespaces/demo/content/con_00000000000000000000000000000001";
        let replaced = "namespaces/demo/hint.json";
        store
            .put_if_absent(created, Bytes::from_static(b"created"))
            .await
            .expect("create");
        store
            .put_overwrite(replaced, Bytes::from_static(b"replaced"))
            .await
            .expect("overwrite");

        let attested = Some(Checksum::sha256(b"created"));
        let head = store.head(created).await.expect("head").expect("created");
        let body = store
            .get_with_metadata(created)
            .await
            .expect("get")
            .expect("created");
        assert_eq!(head.sha256, attested);
        assert_eq!(body.metadata.sha256, attested);
        let replaced = store.head(replaced).await.expect("head").expect("replaced");
        assert_eq!(replaced.sha256, None);
    }

    #[tokio::test]
    async fn an_immutable_write_creates_at_every_size_and_attests_its_bytes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        for (key, payload) in [
            ("namespaces/demo/uploads/upl_2.json", multipart_payload(15)),
            (
                MULTIPART_KEY,
                multipart_payload(MULTIPART_TEST_THRESHOLD as usize + 1),
            ),
        ] {
            let written = store
                .put_immutable_verified(key, Bytes::from(payload.clone()))
                .await
                .expect("immutable write");
            let head = store.head(key).await.expect("head").expect("written");
            assert_eq!(written.sha256, Some(Checksum::sha256(&payload)));
            assert_eq!(head.sha256, written.sha256);
        }

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_occupied_key_is_decided_by_one_head_of_its_attestation() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let ours = Bytes::from_static(b"ours");
        let identical = "namespaces/demo/segments/seg_1.sst.zst";
        let different = "namespaces/demo/segments/seg_2.sst.zst";
        let unattested = "namespaces/demo/segments/seg_3.sst.zst";
        store
            .put_if_absent(identical, ours.clone())
            .await
            .expect("seed identical");
        store
            .put_if_absent(different, Bytes::from_static(b"theirs"))
            .await
            .expect("seed different");
        seed_scoped_object(&flaky, unattested, ours.clone()).await;
        flaky.puts.store(0, Ordering::SeqCst);

        let accepted = store
            .put_immutable_verified(identical, ours.clone())
            .await
            .expect("an equal attestation is this object");
        assert_eq!(accepted.sha256, Some(Checksum::sha256(&ours)));
        assert!(matches!(
            store.put_immutable_verified(different, ours.clone()).await,
            Err(crate::ImmutableWriteError::DifferentObject { object_key }) if object_key == different
        ));
        assert!(matches!(
            store.put_immutable_verified(unattested, ours).await,
            Err(crate::ImmutableWriteError::Unattested { object_key }) if object_key == unattested
        ));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 3);
        assert_eq!(
            flaky.gets.load(Ordering::SeqCst),
            0,
            "no bytes are read back"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_landed_write_whose_answer_was_lost_resolves_through_its_attestation() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::LandThenFail]);
        let key = "namespaces/demo/uploads/upl_3.json";

        store
            .put_immutable_verified(key, Bytes::from_static(b"payload"))
            .await
            .expect("the retry finds its own landed write");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn immutable_transport_failures_retry_inside_the_operation() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(
            &flaky,
            [
                WriteScript::FailWithoutLanding,
                WriteScript::FailWithoutLanding,
            ],
        );
        let key = "namespaces/demo/uploads/upl_4.json";

        store
            .put_immutable_verified(key, Bytes::from_static(b"payload"))
            .await
            .expect("transient failures are retried");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn immutable_transport_failure_surfaces_after_the_retry_budget() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, (0..11).map(|_| WriteScript::FailWithoutLanding));
        let key = "namespaces/demo/uploads/upl_9.json";

        let error = store
            .put_immutable_verified(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("persistent failure exhausts the immutable retry budget");

        assert!(matches!(
            error,
            crate::ImmutableWriteError::Transport {
                source: ObjectStoreError::Transport { .. },
                ..
            }
        ));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 11);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn compare_and_swap_never_retries_transport_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/hint.json";
        let seeded = store
            .put_overwrite(key, Bytes::from_static(b"one"))
            .await
            .expect("seed head");
        let etag = seeded.etag.expect("etag");
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect_err("compare-and-swap surfaces the transport failure");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"one"))
        );
    }

    #[tokio::test]
    async fn a_conditional_put_goes_once_to_the_client_that_never_resends() {
        let resending = Arc::new(FlakyStore::default());
        let one_attempt = Arc::new(FlakyStore {
            inner: resending.inner.clone(),
            ..FlakyStore::default()
        });
        let store = ProviderObjectStore::new(
            Arc::clone(&resending) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&one_attempt) as Arc<dyn provider_store::ObjectStore>,
            Arc::clone(&resending) as Arc<dyn MultipartStore>,
            Arc::clone(&resending) as Arc<dyn PaginatedListStore>,
            ProviderObjectStoreConfig { key_prefix: None },
            ConfiguredObjectStoreKind::LocalFs,
            StoreIoRuntime::new().expect("store io runtime"),
        )
        .expect("provider store");
        script_puts(&one_attempt, [WriteScript::LandThenFail]);
        let key = "namespaces/demo/uploads/upl_11.json";

        let error = store
            .put_if_absent(key, Bytes::from_static(b"claim"))
            .await
            .expect_err("the landed write's lost answer reaches the caller");
        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);

        store
            .put_overwrite(key, Bytes::from_static(b"session"))
            .await
            .expect("overwrite");
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"session"))
        );
        assert_eq!(resending.puts.load(Ordering::SeqCst), 1);
        assert_eq!(resending.gets.load(Ordering::SeqCst), 1);
        assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);
        assert_eq!(one_attempt.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn delete_retries_stop_once_the_operation_deadline_is_spent() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky))
            .monotonic_timer(Arc::new(SteppingTimer::new(45_000)));
        let key = "namespaces/demo/uploads/upl_10.json";
        store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        for _ in 0..6 {
            flaky
                .delete_script
                .lock()
                .expect("delete script")
                .push_back(WriteScript::FailWithoutLanding);
        }

        let error = store
            .delete(key)
            .await
            .expect_err("deadline exhaustion surfaces the transport failure");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        let attempts = flaky.deletes.load(Ordering::SeqCst);
        assert!(
            attempts < 5,
            "deadline must stop the loop before the count budget ({attempts} attempts)"
        );
    }

    #[tokio::test]
    async fn non_transport_provider_errors_are_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::FailAuth]);
        let key = "namespaces/demo/uploads/upl_5.json";

        let error = store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("auth rejection surfaces immediately");

        assert!(matches!(error, ObjectStoreError::PermissionDenied { .. }));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn delete_retries_transient_failures_and_landed_deletes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));

        let landed_key = "namespaces/demo/uploads/upl_6.json";
        store
            .put_overwrite(landed_key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(WriteScript::LandThenFail);
        store
            .delete(landed_key)
            .await
            .expect("landed delete converges to success");
        assert_eq!(flaky.deletes.load(Ordering::SeqCst), 2);
        assert!(store.head(landed_key).await.expect("head").is_none());

        let transient_key = "namespaces/demo/uploads/upl_7.json";
        store
            .put_overwrite(transient_key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(WriteScript::FailWithoutLanding);
        store
            .delete(transient_key)
            .await
            .expect("transient delete failure is retried");
        assert_eq!(flaky.deletes.load(Ordering::SeqCst), 4);
        assert!(store.head(transient_key).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn a_retried_call_reports_its_attempts_to_the_metrics_wrapper() {
        let flaky = Arc::new(FlakyStore::default());
        let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
        let store =
            InstrumentedObjectStore::new(retrying_store(Arc::clone(&flaky)), recorder.clone());
        let key = "namespaces/demo/uploads/upl_8.json";

        store
            .put_overwrite(key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");
        flaky
            .delete_script
            .lock()
            .expect("delete script")
            .push_back(WriteScript::FailWithoutLanding);
        store.delete(key).await.expect("the delete converges");

        let samples = recorder.samples();
        let delete = samples
            .iter()
            .find(|sample| sample.operation == ObjectStoreOperation::Delete)
            .expect("the delete is sampled");
        assert_eq!(
            delete.attempts, 2,
            "one failed attempt, then the one that landed"
        );
        let seed = samples
            .iter()
            .find(|sample| sample.operation == ObjectStoreOperation::Put)
            .expect("the seed write is sampled");
        assert_eq!(
            seed.attempts, 1,
            "a call that never retried made one attempt"
        );
    }

    #[test]
    fn request_phase_bound_has_two_flat_tiers() {
        assert_eq!(request_phase_bound(0), PROVIDER_ATTEMPT_TIMEOUT);
        assert_eq!(
            request_phase_bound(PROVIDER_TRANSFER_BODY_MIN_BYTES - 1),
            PROVIDER_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            request_phase_bound(PROVIDER_TRANSFER_BODY_MIN_BYTES),
            PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
        );
        assert_eq!(
            request_phase_bound(PROVIDER_MULTIPART_PART_BYTES),
            PROVIDER_TRANSFER_ATTEMPT_TIMEOUT
        );
    }

    const MULTIPART_TEST_THRESHOLD: u64 = 1024;
    const MULTIPART_TEST_PART: u64 = 512;
    const MULTIPART_KEY: &str = "namespaces/demo/content/con_abcdef0123456789abcdef0123456789";

    /// Retrying store with a test-sized multipart geometry: payloads of
    /// 1024+ bytes go multipart in 512-byte parts.
    fn multipart_test_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
        retrying_store(flaky).multipart_geometry(MULTIPART_TEST_THRESHOLD, MULTIPART_TEST_PART)
    }

    fn multipart_payload(len: usize) -> Vec<u8> {
        (0..len).map(|index| (index % 251) as u8).collect()
    }

    fn script_part(
        flaky: &FlakyStore,
        part_index: usize,
        script: impl IntoIterator<Item = WriteScript>,
    ) {
        flaky
            .part_script
            .lock()
            .expect("part script")
            .entry(part_index)
            .or_default()
            .extend(script);
    }

    fn part_attempts(flaky: &FlakyStore, part_index: usize) -> usize {
        flaky
            .part_attempts
            .lock()
            .expect("part attempts")
            .get(&part_index)
            .copied()
            .unwrap_or(0)
    }

    fn script_complete(flaky: &FlakyStore, script: impl IntoIterator<Item = WriteScript>) {
        flaky
            .complete_script
            .lock()
            .expect("complete script")
            .extend(script);
    }

    /// Places an object at `key` directly on the inner store, bypassing the
    /// scripted transport and its counters.
    async fn seed_scoped_object(flaky: &FlakyStore, key: &str, bytes: Bytes) {
        provider_store::ObjectStore::put_opts(
            &flaky.inner,
            &Path::from(format!("tenant-a/{key}")),
            bytes.into(),
            PutOptions::default(),
        )
        .await
        .expect("seed object");
    }

    #[tokio::test]
    async fn an_extension_without_a_provider_path_rewrites_the_base_under_its_version() {
        let store = memory_store();
        let pieces = Bytes::from_static(b" and pieces");
        let written = store
            .put_if_absent(MULTIPART_KEY, Bytes::from_static(b"base"))
            .await
            .expect("base");
        let base = ExtendBase {
            length: 4,
            etag: written.etag.expect("etag"),
        };
        let extended_bytes = b"base and pieces";
        let result = ExtendedObject {
            sha256: Checksum::sha256(extended_bytes),
            crc: Some(Checksum::crc64nvme(extended_bytes)),
        };
        let wrong_crc = ExtendedObject {
            crc: Some(Checksum::crc64nvme(b"other bytes")),
            ..result.clone()
        };

        assert!(matches!(
            store
                .extend_object(MULTIPART_KEY, &base, pieces.clone(), &wrong_crc)
                .await,
            Err(ObjectStoreError::ChecksumMismatch { .. })
        ));
        let shorter = ExtendBase {
            length: 2,
            ..base.clone()
        };
        assert!(matches!(
            store
                .extend_object(MULTIPART_KEY, &shorter, pieces.clone(), &result)
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from_static(b"base")),
            "a length the version does not have writes nothing"
        );
        let extended = store
            .extend_object(MULTIPART_KEY, &base, pieces.clone(), &result)
            .await
            .expect("the refused claims wrote nothing, so the base still matches");
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from_static(extended_bytes))
        );
        let head = store
            .head(MULTIPART_KEY)
            .await
            .expect("head")
            .expect("extended");
        assert_eq!(head.etag, extended.etag);
        assert_eq!(head.sha256, Some(result.sha256.clone()));
        assert_eq!(extended.sha256, head.sha256);
        assert!(matches!(
            store
                .extend_object(MULTIPART_KEY, &base, pieces, &result)
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        ));
    }

    #[tokio::test]
    async fn a_store_without_a_provider_path_copies_no_base_into_a_new_key() {
        let store = memory_store();
        let written = store
            .put_if_absent(MULTIPART_KEY, Bytes::from_static(b"base"))
            .await
            .expect("base");
        let key = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
        let copied = store
            .put_immutable_extended(
                key,
                MULTIPART_KEY,
                &ExtendBase {
                    length: 4,
                    etag: written.etag.expect("etag"),
                },
                Bytes::from_static(b" and pieces"),
                &ExtendedObject {
                    sha256: Checksum::sha256(b"base and pieces"),
                    crc: None,
                },
            )
            .await
            .expect("a store without a provider path declines");
        assert_eq!(copied, None);
        assert_eq!(store.head(key).await.expect("head"), None);
    }

    #[tokio::test]
    async fn large_put_routes_through_multipart_and_preserves_bytes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        // Unaligned tail: parts of 512, 512, and 276 bytes.
        let payload = multipart_payload(1300);

        let metadata = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("multipart put");

        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 0);
        assert_eq!(
            flaky.puts.load(Ordering::SeqCst),
            0,
            "no whole-object PUT for a payload above the threshold"
        );
        assert_eq!(part_attempts(&flaky, 0), 1);
        assert_eq!(part_attempts(&flaky, 1), 1);
        assert_eq!(part_attempts(&flaky, 2), 1);
        assert_eq!(metadata.size_bytes, 1300);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn dropping_a_buffered_multipart_write_aborts_its_provider_upload() {
        let flaky = Arc::new(FlakyStore::default());
        flaky.block_part_uploads.store(true, Ordering::SeqCst);
        let store = multipart_test_store(Arc::clone(&flaky));
        let part_started = flaky.part_started.notified();
        let multipart_aborted = flaky.multipart_aborted.notified();

        {
            let write = store.put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)));
            tokio::pin!(write);
            let completed = tokio::select! {
                () = part_started => None,
                result = &mut write => Some(result),
            };
            assert!(
                completed.is_none(),
                "blocked multipart write completed unexpectedly: {completed:?}"
            );
        }

        multipart_aborted.await;
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
        assert!(
            flaky.multipart_uploads.lock().expect("uploads").is_empty(),
            "cancelling the write must leave no provider upload behind"
        );
    }

    #[tokio::test]
    async fn multipart_threshold_boundary_routes_exactly() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));

        store
            .put_overwrite(
                "namespaces/demo/uploads/upl_small.bin",
                Bytes::from(multipart_payload(MULTIPART_TEST_THRESHOLD as usize - 1)),
            )
            .await
            .expect("below-threshold put");
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        store
            .put_overwrite(
                MULTIPART_KEY,
                Bytes::from(multipart_payload(MULTIPART_TEST_THRESHOLD as usize)),
            )
            .await
            .expect("at-threshold put");
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    }

    /// A controller that records the part window of each conditional create
    /// handed to it and answers with the attestation it was given.
    #[derive(Default)]
    struct CountingController {
        part_windows: Mutex<Vec<usize>>,
    }

    impl CountingController {
        fn part_windows(&self) -> Vec<usize> {
            self.part_windows.lock().expect("part windows").clone()
        }
    }

    #[async_trait]
    impl MultipartController for CountingController {
        async fn put_if_absent(
            &self,
            _key: &str,
            head: Bytes,
            mut rest: PartReader<'_>,
            sha256: Option<&Checksum>,
            part_window: usize,
        ) -> Result<ObjectMetadata> {
            self.part_windows
                .lock()
                .expect("part windows")
                .push(part_window);
            let mut size_bytes = head.len() as u64;
            while let Some(part) = rest.next_part().await? {
                size_bytes += part.len() as u64;
            }
            Ok(ObjectMetadata {
                etag: Some("\"controller\"".to_owned()),
                version: None,
                size_bytes,
                last_modified_ms: None,
                sha256: sha256.cloned(),
            })
        }

        async fn extend_object(
            &self,
            _key: &str,
            _base: &ExtendBase,
            _pieces: Bytes,
            _result: &ExtendedObject,
        ) -> Result<Option<ObjectMetadata>> {
            Ok(None)
        }

        async fn put_immutable_extended(
            &self,
            _key: &str,
            _base_key: &str,
            _base: &ExtendBase,
            _pieces: Bytes,
            _result: &ExtendedObject,
        ) -> Result<Option<ObjectMetadata>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn large_creates_go_through_the_controller_and_only_verified_ones_get_the_window() {
        let flaky = Arc::new(FlakyStore::default());
        let controller = Arc::new(CountingController::default());
        let store = multipart_test_store(Arc::clone(&flaky))
            .multipart_controller(Arc::clone(&controller) as Arc<dyn MultipartController>);
        let payload = multipart_payload(1300);

        store
            .put_if_absent(
                "namespaces/demo/wal/000000000000000001.wal",
                Bytes::from(payload.clone()),
            )
            .await
            .expect("a publication is one conditional request at every size");
        assert!(controller.part_windows().is_empty());
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);

        let written = store
            .put_immutable_verified(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("a large verified write creates through the controller");
        assert_eq!(controller.part_windows(), [PROVIDER_MULTIPART_PART_WINDOW]);
        assert_eq!(written.size_bytes, payload.len() as u64);
        assert_eq!(written.sha256, Some(Checksum::sha256(&payload)));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        let written = store
            .put_immutable_verified_stream(
                MULTIPART_KEY,
                payload.len() as u64,
                Some(&Checksum::sha256(&payload)),
                streamed(&payload, 100),
            )
            .await
            .expect("a large streamed verified write creates through the controller");
        assert_eq!(
            controller.part_windows(),
            [PROVIDER_MULTIPART_PART_WINDOW; 2]
        );
        assert_eq!(written.size_bytes, payload.len() as u64);
        assert_eq!(written.sha256, Some(Checksum::sha256(&payload)));

        let small = "namespaces/demo/content/con_0123456789abcdef0123456789abcdef";
        store
            .put_immutable_verified_stream(small, 10, None, streamed(&payload[..10], 4))
            .await
            .expect("a small streamed verified write is one request");
        assert_eq!(
            controller.part_windows(),
            [PROVIDER_MULTIPART_PART_WINDOW; 2]
        );
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        let held = store.head(small).await.expect("head").expect("object");
        assert_eq!(held.sha256, None, "no attestation was asked for");

        let uploaded = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 100),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("a large streamed upload creates through the controller");
        assert_eq!(uploaded, payload.len() as u64);
        assert_eq!(
            controller.part_windows(),
            [
                PROVIDER_MULTIPART_PART_WINDOW,
                PROVIDER_MULTIPART_PART_WINDOW,
                1
            ],
            "a client's upload keeps one part in flight"
        );
    }

    #[tokio::test]
    async fn large_create_if_absent_stays_single_request() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(1300);

        store
            .put_if_absent(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("create absent large object");
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        let error = store
            .put_if_absent(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("existing object fails the create precondition");
        assert!(matches!(error, ObjectStoreError::PreconditionFailed { .. }));
        assert_eq!(
            flaky.multipart_creates.load(Ordering::SeqCst),
            0,
            "the conflict is decided by the provider precondition, not a pre-check"
        );
    }

    /// Cuts a payload into stream chunks that deliberately do not line up
    /// with the part size, since a caller's chunk boundaries never do.
    fn streamed(payload: &[u8], chunk_bytes: usize) -> ByteStream {
        let chunks: Vec<Bytes> = payload
            .chunks(chunk_bytes)
            .map(Bytes::copy_from_slice)
            .collect();
        stream::iter(chunks.into_iter().map(Ok)).boxed()
    }

    #[tokio::test]
    async fn a_streamed_put_cuts_the_stream_into_parts_and_preserves_bytes() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        // Parts of 512, 512, and 276 bytes, delivered in 100-byte chunks so
        // every part boundary falls inside a chunk.
        let payload = multipart_payload(1300);

        let size_bytes = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 100),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("streamed multipart put");

        assert_eq!(size_bytes, 1300);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 0);
        assert_eq!(
            flaky.puts.load(Ordering::SeqCst),
            0,
            "a payload past one part never becomes a whole-object PUT"
        );
        assert_eq!(part_attempts(&flaky, 0), 1);
        assert_eq!(part_attempts(&flaky, 1), 1);
        assert_eq!(part_attempts(&flaky, 2), 1);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn a_short_streamed_put_is_one_request_that_keeps_its_precondition() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(MULTIPART_TEST_PART as usize - 1);

        store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 64),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("short streamed put");
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        let error = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 64),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect_err("the key is taken and create-only means it");
        assert!(matches!(error, ObjectStoreError::PreconditionFailed { .. }));
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn a_streamed_multipart_put_refuses_an_occupied_key() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let occupant = multipart_payload(7);
        seed_scoped_object(&flaky, MULTIPART_KEY, Bytes::from(occupant.clone())).await;
        let payload = multipart_payload(1300);

        let error = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 100),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect_err("the key is taken and create-only means it");

        assert!(matches!(error, ObjectStoreError::PreconditionFailed { .. }));
        assert_eq!(
            part_attempts(&flaky, 2),
            1,
            "the whole payload is consumed before the condition is evaluated",
        );
        assert_eq!(
            flaky.heads.load(Ordering::SeqCst),
            1,
            "one head decides the condition",
        );
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 0);
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "the refused upload is aborted, not left holding parts",
        );
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(occupant)),
        );
    }

    #[tokio::test]
    async fn a_streamed_multipart_overwrite_reads_nothing() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(1300);

        store
            .put_streamed(MULTIPART_KEY, streamed(&payload, 100), PutMode::Overwrite)
            .await
            .expect("streamed multipart overwrite");

        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn an_empty_streamed_put_writes_an_empty_object() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));

        let size_bytes = store
            .put_streamed(
                MULTIPART_KEY,
                stream::empty().boxed(),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("empty streamed put");

        assert_eq!(size_bytes, 0);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::new())
        );
    }

    #[tokio::test]
    async fn a_streamed_put_that_fails_mid_stream_abandons_its_upload() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let head = multipart_payload(MULTIPART_TEST_PART as usize);
        let body = stream::iter([
            Ok(Bytes::from(head)),
            Ok(Bytes::from(multipart_payload(64))),
            Err(ObjectStoreError::transport(
                MULTIPART_KEY,
                "the client stopped sending",
            )),
        ])
        .boxed();

        let error = store
            .put_streamed(MULTIPART_KEY, body, PutMode::CreateIfAbsent)
            .await
            .expect_err("a payload that stops is not a write");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 0);
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "the abandoned upload is aborted, not left holding parts"
        );
        assert_eq!(store.get(MULTIPART_KEY, None).await.expect("get"), None);
    }

    #[tokio::test]
    async fn multipart_part_failures_are_retried_in_place() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_part(
            &flaky,
            1,
            [
                WriteScript::FailWithoutLanding,
                WriteScript::FailWithoutLanding,
            ],
        );
        let payload = multipart_payload(1300);

        store
            .put_overwrite(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("multipart put survives transient part failures");

        assert_eq!(part_attempts(&flaky, 0), 1);
        assert_eq!(
            part_attempts(&flaky, 1),
            3,
            "the failing part retries in place under the same index"
        );
        assert_eq!(part_attempts(&flaky, 2), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn multipart_part_budget_exhaustion_aborts_the_upload() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_part(&flaky, 0, (0..6).map(|_| WriteScript::FailWithoutLanding));

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("persistent part failure surfaces after the retry budget");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(part_attempts(&flaky, 0), 5, "1 attempt + max_retries");
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 0);
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "a failed upload is aborted so no parts are stranded"
        );
        assert!(store.head(MULTIPART_KEY).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn multipart_auth_failure_is_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_part(&flaky, 0, [WriteScript::FailAuth]);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("auth rejection surfaces immediately");

        // Auth failures carry their own classification — never mistaken
        // for network weather, and never retried.
        assert!(matches!(error, ObjectStoreError::PermissionDenied { .. }));
        assert_eq!(part_attempts(&flaky, 0), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_ambiguous_multipart_completion_is_reported_without_reading_the_object() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_complete(&flaky, [WriteScript::LandThenFail]);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("an unanswered completion is not a success");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multipart_part_retries_stop_once_the_operation_deadline_is_spent() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky))
            .monotonic_timer(Arc::new(SteppingTimer::new(45_000)));
        script_part(&flaky, 0, (0..6).map(|_| WriteScript::FailWithoutLanding));

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("persistent part failure surfaces after the operation deadline");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        let attempts = part_attempts(&flaky, 0);
        assert!(
            attempts < 5,
            "deadline must stop the loop before the count budget ({attempts} attempts)"
        );
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }
}
