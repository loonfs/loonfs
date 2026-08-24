//! The shared provider transport: timeouts, bounded retries for replay-safe
//! delete and multipart stages, and multipart upload for large immutable
//! payloads.

use crate::immutable_write::{readback, ImmutableReadback};
use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::object_store::Result;
use crate::retry::{
    next_retry_backoff, transport_retry_pause, OperationDeadline, TransportRetryPolicy,
};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{
    ByteRange, ByteStream, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, FuturesUnordered, StreamExt};
use object_store as provider_store;
use provider_store::multipart::{MultipartStore, PartId};
use provider_store::path::Path;
use provider_store::{
    GetOptions, GetRange, ObjectMeta, PutOptions, PutPayload, PutResult, UpdateVersion,
};
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
/// start only while time remains. Multipart uploads are excluded because
/// each part has its own timeout and retry limit. The garbage-collection
/// grace period is longer than the maximum publication operation.
pub const PROVIDER_OPERATION_DEADLINE: Duration = Duration::from_secs(120);

/// Minimum payload size for native multipart overwrite uploads.
///
/// Create-if-absent and compare-and-swap writes always use a single request
/// because multipart completion cannot enforce those provider preconditions.
pub const PROVIDER_MULTIPART_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Fixed size of every multipart part except the last. Cloudflare R2
/// requires all non-final parts to share one size, and every supported
/// provider requires at least 5 MiB per non-final part; 8 MiB matches the
/// part size mainstream storage clients default to, and keeps every part a
/// cheap retry that fits comfortably inside one flat attempt bound.
pub const PROVIDER_MULTIPART_PART_BYTES: u64 = 8 * 1024 * 1024;

/// Concurrent in-flight parts per multipart upload.
pub const PROVIDER_MULTIPART_PART_WINDOW: usize = 4;

/// Number of multipart buffers retained by a streamed write.
///
/// Streamed writes upload and release each part before buffering the next, so
/// peak payload memory remains one part regardless of object size.
pub const PROVIDER_STREAMED_PART_WINDOW: usize = 1;

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

/// Adapts the upstream `object_store` provider surface to the narrower LoonFS contract.
#[derive(Clone)]
pub struct ProviderObjectStore {
    inner: Arc<dyn provider_store::ObjectStore>,
    /// The provider's native multipart surface, used for payloads at or
    /// above [`PROVIDER_MULTIPART_THRESHOLD_BYTES`]. `None` only for
    /// providers without one; their large puts stay whole-object PUTs under
    /// the payload-scaled bounds.
    multipart: Option<Arc<dyn MultipartStore>>,
    multipart_geometry: MultipartGeometry,
    key_prefix: Option<String>,
    transport_retry: TransportRetryPolicy,
    timer: Arc<dyn MonotonicTimer>,
}

impl fmt::Debug for ProviderObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderObjectStore")
            .field("key_prefix", &self.key_prefix)
            .field("multipart_upload", &self.multipart.is_some())
            .finish_non_exhaustive()
    }
}

impl ProviderObjectStore {
    /// Wraps a provider client and optional native multipart surface.
    ///
    /// Construction fails when `config.key_prefix` is not a normalized,
    /// non-escaping logical prefix.
    pub fn new(
        inner: Arc<dyn provider_store::ObjectStore>,
        multipart: Option<Arc<dyn MultipartStore>>,
        config: ProviderObjectStoreConfig,
    ) -> Result<Self> {
        Ok(Self {
            inner,
            multipart,
            multipart_geometry: MultipartGeometry::DEFAULT,
            key_prefix: normalize_key_prefix(config.key_prefix.as_deref())?,
            transport_retry: TransportRetryPolicy::DEFAULT,
            timer: Arc::new(StdMonotonicTimer::default()),
        })
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

    pub(crate) fn validate_key(&self, key: &str) -> Result<()> {
        self.to_path(key).map(|_| ())
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

    fn from_meta(meta: ObjectMeta) -> ObjectMetadata {
        ObjectMetadata {
            etag: meta.e_tag,
            version: meta.version,
            size_bytes: meta.size,
            last_modified_ms: last_modified_ms(meta.last_modified.timestamp_millis()),
        }
    }

    fn from_put_result(result: PutResult, size_bytes: u64) -> ObjectMetadata {
        ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            last_modified_ms: None,
        }
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
    /// failed upload is aborted on a best-effort basis. If completion returns an
    /// ambiguous transport error, immutable writes verify the stored bytes before
    /// deciding whether the write succeeded.
    ///
    /// Multipart uploads have per-part limits rather than one total deadline.
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

        let mut abort_on_drop = upload.create(size_bytes).await?;
        let result = upload
            .upload_parts_and_complete(abort_on_drop.upload_id(), &bytes)
            .await;
        match result {
            Ok(metadata) => {
                abort_on_drop.disarm();
                Ok(metadata)
            }
            Err(err) => {
                // Best effort, and harmless when the failure raced a landed
                // completion: the upload id no longer exists then, and the
                // abort cannot touch the completed object.
                upload.abort(abort_on_drop.upload_id()).await;
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
struct PartReader {
    body: ByteStream,
    /// The tail of a chunk that overran the part being cut.
    carry: Option<Bytes>,
    part_bytes: usize,
    exhausted: bool,
}

impl PartReader {
    fn new(body: ByteStream, part_bytes: usize) -> Self {
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
    async fn next_part(&mut self) -> Result<Option<Bytes>> {
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
    fn exhausted(&self) -> bool {
        self.exhausted && self.carry.is_none()
    }
}

/// Aborts a provider multipart upload when its write is cancelled.
///
/// Normal return paths abort explicitly and disable this guard. Cancellation
/// has no cleanup `await` point, so `Drop` starts the abort on the current
/// Tokio runtime. Without a runtime, the bucket's incomplete-upload lifecycle
/// policy must remove the parts.
struct AbortUploadOnDrop {
    multipart: Option<Arc<dyn MultipartStore>>,
    path: Path,
    upload_id: provider_store::MultipartId,
}

impl AbortUploadOnDrop {
    fn new(
        multipart: Arc<dyn MultipartStore>,
        path: Path,
        upload_id: provider_store::MultipartId,
    ) -> Self {
        Self {
            multipart: Some(multipart),
            path,
            upload_id,
        }
    }

    fn upload_id(&self) -> &provider_store::MultipartId {
        &self.upload_id
    }

    fn disarm(&mut self) {
        self.multipart = None;
    }
}

impl Drop for AbortUploadOnDrop {
    fn drop(&mut self) {
        let Some(multipart) = self.multipart.take() else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                object_key = %self.path,
                operation = "abort_multipart",
                "abandoned write has no runtime to abort its multipart upload on; \
                 parts remain until the bucket lifecycle rule collects them",
            );
            return;
        };
        let path = self.path.clone();
        let upload_id = std::mem::take(&mut self.upload_id);
        handle.spawn(async move {
            if let Err(_err) = multipart.abort_multipart(&path, &upload_id).await {
                tracing::warn!(
                    object_key = %path,
                    operation = "abort_multipart",
                    "failed to abort the multipart upload of an abandoned write",
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
    async fn create(&self, payload_bytes: u64) -> Result<AbortUploadOnDrop> {
        let mut retries: u32 = 0;
        loop {
            let err = match self.multipart.create_multipart(self.path).await {
                Ok(upload_id) => {
                    return Ok(AbortUploadOnDrop::new(
                        Arc::clone(&self.multipart),
                        self.path.clone(),
                        upload_id,
                    ));
                }
                Err(err) => err,
            };
            if !provider_transport_retryable(&err) {
                return Err(map_provider_error(self.key, err));
            }
            let Some(backoff) = next_retry_backoff(
                &self.store.transport_retry,
                self.key,
                "create_multipart",
                payload_bytes,
                &mut retries,
                None,
            ) else {
                return Err(map_provider_error(self.key, err));
            };
            transport_retry_pause(backoff).await;
        }
    }

    /// Uploads a stream as fixed-size parts while retaining at most one part.
    ///
    /// `head` is the first part. Later parts are buffered only after the previous
    /// part has been uploaded and released. The final part may be shorter.
    ///
    /// Because the original payload is no longer available, an ambiguous
    /// completion cannot be verified by reading the object back. The caller
    /// treats it as a failure. Conditional modes are checked after the stream is
    /// consumed and immediately before completion.
    async fn upload_stream_and_complete(
        &self,
        upload_id: &provider_store::MultipartId,
        head: Bytes,
        mut parts_reader: PartReader,
        mode: &PutMode,
    ) -> Result<u64> {
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
            Ok(_) => Ok(size_bytes),
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
        self.complete(upload_id, parts, bytes).await
    }

    async fn upload_part(
        &self,
        upload_id: &provider_store::MultipartId,
        part_index: usize,
        payload: Bytes,
    ) -> Result<PartId> {
        let payload_bytes = payload.len() as u64;
        let mut retries: u32 = 0;
        loop {
            let err = match self
                .multipart
                .put_part(
                    self.path,
                    upload_id,
                    part_index,
                    PutPayload::from(payload.clone()),
                )
                .await
            {
                Ok(part_id) => return Ok(part_id),
                Err(err) => err,
            };
            if !provider_transport_retryable(&err) {
                return Err(map_provider_error(self.key, err));
            }
            let Some(backoff) = next_retry_backoff(
                &self.store.transport_retry,
                self.key,
                "put_part",
                payload_bytes,
                &mut retries,
                None,
            ) else {
                return Err(map_provider_error(self.key, err));
            };
            transport_retry_pause(backoff).await;
        }
    }

    async fn complete(
        &self,
        upload_id: &provider_store::MultipartId,
        parts: Vec<PartId>,
        bytes: &Bytes,
    ) -> Result<ObjectMetadata> {
        let size_bytes = bytes.len() as u64;
        match self
            .multipart
            .complete_multipart(self.path, upload_id, parts)
            .await
        {
            Ok(result) => Ok(ProviderObjectStore::from_put_result(result, size_bytes)),
            Err(err) if provider_transport_retryable(&err) => {
                self.resolve_ambiguous_completion(upload_id, bytes, err)
                    .await
            }
            Err(err) => Err(map_provider_error(self.key, err)),
        }
    }

    /// Resolves an ambiguous multipart completion by comparing the stored bytes
    /// with the original in-memory payload.
    ///
    /// Size and etag equality are insufficient because an older object may share
    /// them. Exact byte equality proves the write's postcondition. After a
    /// successful comparison, the upload id is aborted on a best-effort basis to
    /// remove any remaining parts.
    async fn resolve_ambiguous_completion(
        &self,
        upload_id: &provider_store::MultipartId,
        bytes: &Bytes,
        final_err: provider_store::Error,
    ) -> Result<ObjectMetadata> {
        match readback(self.store, self.key, bytes).await {
            Ok(ImmutableReadback::Identical(metadata)) => {
                self.abort(upload_id).await;
                Ok(metadata)
            }
            Ok(ImmutableReadback::Different) => self.unproven_completion(
                final_err,
                "the object at the key does not hold the payload bytes",
            ),
            Ok(ImmutableReadback::Missing) => {
                self.unproven_completion(final_err, "no object exists at the key")
            }
            Err(verify_err) => {
                let original = map_provider_error(self.key, final_err).message();
                Err(ObjectStoreError::transport(
                    self.key,
                    format!(
                        "{original}; failed to verify multipart completion outcome: {verify_err}"
                    ),
                ))
            }
        }
    }

    fn unproven_completion(
        &self,
        final_err: provider_store::Error,
        outcome: &'static str,
    ) -> Result<ObjectMetadata> {
        tracing::warn!(
            object_key = self.key,
            operation = "complete_multipart",
            outcome,
            "ambiguous multipart completion did not land",
        );
        Err(map_provider_error(self.key, final_err))
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
        match self.inner.head(&path).await {
            Ok(meta) => Ok(Some(Self::from_meta(meta))),
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        let path = self.to_path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => {
                let metadata = Self::from_meta(result.meta.clone());
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
        let path = self.to_path(key)?;
        let size_bytes = bytes.len() as u64;
        if matches!(mode, PutMode::Overwrite)
            && size_bytes >= self.multipart_geometry.threshold_bytes
        {
            if let Some(multipart) = self.multipart.clone() {
                return self.put_large_multipart(multipart, key, &path, bytes).await;
            }
        }
        // Raw flat writes are deliberately one attempt. In particular, a
        // mutable overwrite cannot be replayed after an ambiguous transport
        // outcome without changing write ordering. Immutable callers use
        // `put_immutable_verified`, whose name supplies the retry invariant.
        let compare_and_swap = matches!(mode, PutMode::CompareAndSwap { .. });
        let options = PutOptions {
            mode: map_put_mode(mode),
            ..Default::default()
        };
        match self
            .inner
            .put_opts(&path, PutPayload::from(bytes), options)
            .await
        {
            Ok(result) => Ok(Self::from_put_result(result, size_bytes)),
            Err(err) if compare_and_swap && provider_not_found(&err) => {
                Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                })
            }
            Err(err) => Err(map_provider_error(key, err)),
        }
    }

    /// Cuts the payload into parts as it arrives and uploads them one at a
    /// time, so a large object costs one part of memory instead of its own
    /// size.
    ///
    /// The first part is cut before anything is decided. A payload that
    /// ends inside it is an ordinary [`ObjectStore::put`] with the caller's
    /// mode enforced by the provider — which is the same size line `put`
    /// itself draws, so a small streamed write behaves exactly like a small
    /// buffered one. Anything longer goes through the provider's multipart
    /// upload. A provider assembles one unconditionally, so a conditional
    /// mode is checked against a separate read of the key after the payload is
    /// consumed and immediately before completion.
    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        let path = self.to_path(key)?;
        let mut reader = PartReader::new(body, self.multipart_geometry.part_bytes as usize);
        let head = reader.next_part().await?.unwrap_or_else(Bytes::new);
        let Some(multipart) = self.multipart.clone() else {
            // No provider multipart surface: fall back to the buffered
            // contract rather than pretend, exactly as the default does.
            let mut bytes = bytes::BytesMut::from(head.as_ref());
            while let Some(part) = reader.next_part().await? {
                bytes.extend_from_slice(&part);
            }
            let bytes = bytes.freeze();
            let size_bytes = bytes.len() as u64;
            self.put(key, bytes, mode).await?;
            return Ok(size_bytes);
        };
        if reader.exhausted() {
            let size_bytes = head.len() as u64;
            self.put(key, head, mode).await?;
            return Ok(size_bytes);
        }

        let upload = MultipartWrite {
            store: self,
            multipart,
            key,
            path: &path,
        };
        let mut abort_on_drop = upload.create(0).await?;
        let result = upload
            .upload_stream_and_complete(abort_on_drop.upload_id(), head, reader, &mode)
            .await;
        match result {
            Ok(size_bytes) => {
                abort_on_drop.disarm();
                Ok(size_bytes)
            }
            Err(err) => {
                // Best effort, and harmless when the failure raced a landed
                // completion: the upload id no longer exists then.
                upload.abort(abort_on_drop.upload_id()).await;
                abort_on_drop.disarm();
                Err(err)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.to_path(key)?;
        let deadline =
            OperationDeadline::start(self.timer.as_ref(), self.transport_retry.operation_deadline);
        let mut retries: u32 = 0;
        loop {
            // Delete is idempotent under this contract: not-found already
            // reports success, so a retry after an attempt that landed
            // converges to the same outcome.
            let err = match self.inner.delete(&path).await {
                Ok(()) => return Ok(()),
                Err(err) if provider_not_found(&err) => return Ok(()),
                Err(err) => err,
            };
            if !provider_transport_retryable(&err) {
                return Err(map_provider_error(key, err));
            }
            let Some(backoff) = next_retry_backoff(
                &self.transport_retry,
                key,
                "delete",
                0,
                &mut retries,
                Some(&deadline),
            ) else {
                return Err(map_provider_error(key, err));
            };
            transport_retry_pause(backoff).await;
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
}

fn map_put_mode(mode: PutMode) -> provider_store::PutMode {
    match mode {
        PutMode::Overwrite => provider_store::PutMode::Overwrite,
        PutMode::CreateIfAbsent => provider_store::PutMode::Create,
        PutMode::CompareAndSwap { expected_etag } => {
            // The compare token is opaque and provider-issued: S3-family
            // backends condition on `e_tag`, GCS conditions on `version`
            // (its generation). Populate both so each backend reads the
            // field it understands.
            provider_store::PutMode::Update(UpdateVersion {
                e_tag: Some(expected_etag.clone()),
                version: Some(expected_etag),
            })
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

fn provider_transport_retryable(err: &provider_store::Error) -> bool {
    // `Generic` is where the provider client surfaces request failures after
    // its own retry policy gives up: for writes that includes mid-flight
    // transport errors it refuses to re-send because a non-idempotent HTTP
    // request may already have reached the store. The remaining variants are
    // definite outcomes (not-found, already-exists, precondition), hard
    // rejections (auth, invalid path, unsupported), or the store IO runtime
    // shutting down (join error); re-sending those is wrong or futile.
    matches!(err, provider_store::Error::Generic { .. })
}

fn map_provider_error(object_key: &str, err: provider_store::Error) -> ObjectStoreError {
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
        provider_store::Error::NotSupported { .. } | provider_store::Error::NotImplemented => {
            ObjectStoreError::Unsupported("provider object store operation")
        }
        provider_store::Error::UnknownConfigurationKey { key, store } => {
            ObjectStoreError::transport(
                object_key,
                format!("unknown {store} configuration key `{key}`"),
            )
        }
        provider_store::Error::Generic { source, .. } => ObjectStoreError::retryable_transport(
            object_key,
            sanitize_provider_message(&source.to_string()),
        ),
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
    use crate::metrics::{
        InstrumentedObjectStore, ObjectStoreOperation, VecObjectStoreMetricsRecorder,
    };
    use crate::retry::transport_retry_backoff;
    use crate::test_support::SteppingTimer;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    fn memory_store() -> ProviderObjectStore {
        let inner = Arc::new(InMemory::default());
        ProviderObjectStore::new(
            Arc::clone(&inner) as Arc<dyn provider_store::ObjectStore>,
            Some(inner),
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
            },
        )
        .expect("provider store")
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
        let key = "namespaces/demo/wal/head.json";

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
    async fn ranged_reads_match_the_reference_contract_in_one_round_trip() {
        let store = memory_store();
        let key = "namespaces/demo/metadata/segments/seg_abc.sst.zst";
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
        let missing = "namespaces/demo/metadata/segments/seg_missing.sst.zst";
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
        let key = "namespaces/demo/wal/head.json";
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
                    "namespaces/demo/control/missing-head.json",
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

    use provider_store::{GetResult, ListResult, MultipartUpload, PutMultipartOptions};
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::sync::Notify;

    #[derive(Debug)]
    enum WriteScript {
        FailWithoutLanding,
        LandThenFail,
        FailAuth,
        /// The upload vanishes (as a lifecycle rule reaping it would make
        /// it) and the attempt reports a transport failure. Only meaningful
        /// as a completion script.
        VanishThenFail,
    }

    #[derive(Debug)]
    enum ReadScript {
        Transport,
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
        get_script: Mutex<VecDeque<ReadScript>>,
        delete_script: Mutex<VecDeque<WriteScript>>,
        part_script: Mutex<HashMap<usize, VecDeque<WriteScript>>>,
        complete_script: Mutex<VecDeque<WriteScript>>,
        puts: AtomicUsize,
        gets: AtomicUsize,
        deletes: AtomicUsize,
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
                Some(WriteScript::VanishThenFail) => {
                    panic!("VanishThenFail is a completion script")
                }
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
            self.gets.fetch_add(1, Ordering::SeqCst);
            let script = self.get_script.lock().expect("get script").pop_front();
            match script {
                Some(ReadScript::Transport) => Err(transport_glitch()),
                None => self.inner.get_opts(location, options).await,
            }
        }

        async fn delete(&self, location: &Path) -> provider_store::Result<()> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            let script = self
                .delete_script
                .lock()
                .expect("delete script")
                .pop_front();
            match script {
                Some(WriteScript::FailWithoutLanding) => Err(transport_glitch()),
                Some(WriteScript::LandThenFail) => {
                    self.inner.delete(location).await?;
                    Err(transport_glitch())
                }
                Some(WriteScript::FailAuth) => Err(auth_rejection(location)),
                Some(WriteScript::VanishThenFail) => {
                    panic!("VanishThenFail is a completion script")
                }
                None => self.inner.delete(location).await,
            }
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

        async fn copy(&self, from: &Path, to: &Path) -> provider_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> provider_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
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
                Some(WriteScript::VanishThenFail) => {
                    panic!("VanishThenFail is a completion script")
                }
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
                Some(WriteScript::VanishThenFail) => {
                    self.multipart_uploads
                        .lock()
                        .expect("uploads")
                        .remove(id.as_str());
                    Err(transport_glitch())
                }
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
            Some(flaky),
            ProviderObjectStoreConfig {
                key_prefix: Some("tenant-a".to_owned()),
            },
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
        let policy = TransportRetryPolicy::DEFAULT;
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
    async fn immutable_small_write_uses_create_if_absent() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/uploads/upl_2.json";

        store
            .put_immutable_verified(key, Bytes::from_static(b"immutable bytes"))
            .await
            .expect("small immutable write");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 0);
        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"immutable bytes"))
        );
    }

    #[tokio::test]
    async fn immutable_already_present_identical_is_accepted_without_rewrite() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/00000001.cbor.zst";
        let bytes = Bytes::from_static(b"identical immutable bytes");
        seed_scoped_object(&flaky, key, bytes.clone()).await;

        store
            .put_immutable_verified(key, bytes)
            .await
            .expect("identical object is accepted");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn immutable_different_bytes_at_key_are_corruption_class() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/00000002.cbor.zst";
        seed_scoped_object(&flaky, key, Bytes::from_static(b"theirs")).await;

        let error = store
            .put_immutable_verified(key, Bytes::from_static(b"mine"))
            .await
            .expect_err("different immutable bytes are rejected");

        assert!(matches!(
            error,
            crate::ImmutableWriteError::DifferentObject { object_key } if object_key == key
        ));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn immutable_ambiguous_landed_write_is_accepted_by_readback() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::LandThenFail]);
        let key = "namespaces/demo/uploads/upl_3.json";

        store
            .put_immutable_verified(key, Bytes::from_static(b"payload"))
            .await
            .expect("readback proves the first attempt landed");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn immutable_ambiguous_outcome_rejects_different_readback() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/00000003.cbor.zst";
        seed_scoped_object(&flaky, key, Bytes::from_static(b"theirs")).await;
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .put_immutable_verified(key, Bytes::from_static(b"mine"))
            .await
            .expect_err("ambiguous write cannot adopt different bytes");

        assert!(matches!(
            error,
            crate::ImmutableWriteError::DifferentObject { .. }
        ));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
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
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn compare_and_swap_never_retries_transport_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/wal/head.json";
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
    const MULTIPART_KEY: &str =
        "content-stores/cs_0123456789abcdef0123456789abcdef/objects/ab/cd/con_abcdef0123456789abcdef0123456789";

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
    async fn immutable_large_write_routes_through_existing_multipart_path() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let payload =
            multipart_payload(usize::try_from(PROVIDER_MULTIPART_THRESHOLD_BYTES).expect("usize"));

        store
            .put_immutable_verified(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("large immutable write");

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 1);
        assert_eq!(part_attempts(&flaky, 0), 1);
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn immutable_large_write_owns_completion_retry() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let payload =
            multipart_payload(usize::try_from(PROVIDER_MULTIPART_THRESHOLD_BYTES).expect("usize"));
        script_complete(&flaky, [WriteScript::FailWithoutLanding]);

        store
            .put_immutable_verified(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("immutable operation retries the whole multipart write");

        assert_eq!(flaky.multipart_creates.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 2);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
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
            flaky.gets.load(Ordering::SeqCst),
            1,
            "one read decides the condition",
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
    async fn multipart_complete_transport_failure_resolves_landed_completion() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_complete(&flaky, [WriteScript::LandThenFail]);
        let payload = multipart_payload(1300);

        let metadata = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("landed completion reported as the success it was");

        assert_eq!(
            flaky.multipart_completes.load(Ordering::SeqCst),
            1,
            "completion is attempted once before byte-identity resolution"
        );
        assert_eq!(
            flaky.gets.load(Ordering::SeqCst),
            1,
            "one read-back proves the landed write by byte identity"
        );
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "the proven completion still aborts the gone upload id best-effort"
        );
        assert_eq!(metadata.size_bytes, 1300);
        let head = store
            .head(MULTIPART_KEY)
            .await
            .expect("head")
            .expect("object exists");
        assert_eq!(
            metadata.etag, head.etag,
            "resolution reports the landed object's own metadata"
        );
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn raw_multipart_complete_failure_without_landing_is_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_complete(&flaky, [WriteScript::FailWithoutLanding]);
        let payload = multipart_payload(1300);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect_err("raw overwrite surfaces the ambiguous completion");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert!(store.head(MULTIPART_KEY).await.expect("head").is_none());
    }

    #[tokio::test]
    async fn multipart_complete_rejects_stale_same_size_object() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let stale = Bytes::from(vec![0xAA_u8; 1300]);
        seed_scoped_object(&flaky, MULTIPART_KEY, stale.clone()).await;
        script_complete(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("an unproven completion fails instead of adopting the stale object");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(
            flaky.multipart_completes.load(Ordering::SeqCst),
            1,
            "raw overwrite does not replay completion"
        );
        assert_eq!(
            flaky.gets.load(Ordering::SeqCst),
            1,
            "one read-back tested the outcome"
        );
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "the failed upload is aborted so no parts are stranded"
        );
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(stale),
            "the stale object is untouched"
        );
    }

    #[tokio::test]
    async fn multipart_complete_accepts_identical_object_and_aborts() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(1300);
        seed_scoped_object(&flaky, MULTIPART_KEY, Bytes::from(payload.clone())).await;
        script_complete(&flaky, [WriteScript::FailWithoutLanding]);

        let metadata = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(payload))
            .await
            .expect("byte-identical object proves the outcome");

        assert_eq!(metadata.size_bytes, 1300);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(
            flaky.multipart_aborts.load(Ordering::SeqCst),
            1,
            "the dangling upload is aborted on proven success"
        );
        assert!(
            flaky.multipart_uploads.lock().expect("uploads").is_empty(),
            "no parts remain stranded"
        );
    }

    #[tokio::test]
    async fn multipart_complete_gone_upload_with_stale_object_fails_as_transport() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let stale = Bytes::from(vec![0xAA_u8; 1300]);
        seed_scoped_object(&flaky, MULTIPART_KEY, stale.clone()).await;
        script_complete(&flaky, [WriteScript::VanishThenFail]);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("a vanished upload with a stale object is a failed write");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(stale),
            "the stale object is untouched"
        );
    }

    #[tokio::test]
    async fn multipart_complete_first_attempt_rejection_skips_verification() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_complete(&flaky, [WriteScript::FailAuth]);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("auth rejection surfaces immediately");

        assert!(matches!(error, ObjectStoreError::PermissionDenied { .. }));
        assert_eq!(flaky.multipart_completes.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multipart_complete_unverifiable_outcome_surfaces_both_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        script_complete(&flaky, [WriteScript::FailWithoutLanding]);
        flaky
            .get_script
            .lock()
            .expect("get script")
            .push_back(ReadScript::Transport);

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("an unverifiable outcome is an error, not a success");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        let message = error.message();
        assert!(
            message.contains("failed to verify multipart completion outcome"),
            "message names the verification failure: {message}"
        );
        assert_eq!(flaky.gets.load(Ordering::SeqCst), 1);
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn multipart_part_retries_are_count_bounded_not_clock_bounded() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky))
            .monotonic_timer(Arc::new(SteppingTimer::new(45_000)));
        script_part(&flaky, 0, (0..6).map(|_| WriteScript::FailWithoutLanding));

        let error = store
            .put_overwrite(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("persistent part failure surfaces after the retry budget");

        assert!(matches!(error, ObjectStoreError::Transport { .. }));
        assert_eq!(
            part_attempts(&flaky, 0),
            5,
            "1 attempt + max_retries, unaffected by elapsed time"
        );
        assert_eq!(flaky.multipart_aborts.load(Ordering::SeqCst), 1);
    }
}
