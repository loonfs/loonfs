//! The shared provider transport: timeouts, bounded retries for replay-safe
//! delete and multipart stages, and multipart upload for large immutable
//! payloads.

use crate::immutable_write::{readback, ImmutableReadback};
use crate::keyspace::{
    normalize_key_prefix, scope_list_prefix, scope_object_key, unscope_listed_key,
};
use crate::object_store::Result;
use crate::retry::{
    transport_retry_backoff, transport_retry_pause, OperationDeadline, TransportRetryPolicy,
};
use crate::timing::{MonotonicTimer, StdMonotonicTimer};
use crate::{ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
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

/// Hard deadline for one logical object-store operation, consumed across
/// every retry of that operation rather than restarting per attempt. Reads
/// get it as the provider client's retry timeout; verified immutable writes
/// and deletes share it through `TransportRetryPolicy`. The deadline gates
/// starting another attempt, and one outer attempt may itself contain the
/// inner client's full retry budget for status-code retries — so one
/// operation's total wall time is bounded by the deadline plus the inner
/// retry budget plus one attempt bound (worst case roughly six minutes),
/// still a hard bound. The GC grace window is derived
/// above this bound (format spec, "Garbage collection", rule 1); multipart
/// uploads deliberately carry no whole-operation clock (their parts are
/// individually bounded), which leaves the floor inequality untouched
/// because everything it times — WAL segments inside the publish budget,
/// the root compare-and-swap — is a small control object on the
/// single-request path.
pub const PROVIDER_OPERATION_DEADLINE: Duration = Duration::from_secs(120);

/// Payload size at and above which overwrite puts use the provider's native
/// multipart upload instead of one whole-object PUT, matching the multipart
/// thresholds mainstream storage clients ship. The format spec allows this
/// for large immutable file data and forbids relying on it for small
/// mutable control objects: create-if-absent and compare-and-swap puts
/// never take this path, because providers complete multipart uploads as
/// unconditional overwrites and those modes exist to carry real provider
/// preconditions.
pub const PROVIDER_MULTIPART_THRESHOLD_BYTES: u64 = 8 * 1024 * 1024;

/// Fixed size of every multipart part except the last. Cloudflare R2
/// requires all non-final parts to share one size, and every supported
/// provider requires at least 5 MiB per non-final part; 8 MiB matches the
/// part size mainstream storage clients default to, and keeps every part a
/// cheap retry that fits comfortably inside one flat attempt bound.
pub const PROVIDER_MULTIPART_PART_BYTES: u64 = 8 * 1024 * 1024;

/// Concurrent in-flight parts per multipart upload.
pub const PROVIDER_MULTIPART_PART_WINDOW: usize = 4;

/// Bound for one payload-bearing HTTP attempt's request phase. A request
/// body is opaque to progress observation while it uploads, so a flat
/// generous bound stands in for stall detection: parts are at most
/// [`PROVIDER_MULTIPART_PART_BYTES`], and an 8 MiB body that cannot finish
/// inside this bound is moving slower than roughly 70 KiB/s — treated as
/// stalled and retried on a fresh connection.
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
            last_modified_ms: u64::try_from(meta.last_modified.timestamp_millis()).ok(),
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

    /// Writes one large payload through the provider's native multipart
    /// upload: fixed-size parts uploaded through a bounded window, each part
    /// retried in place on transient failures (part indices are stable, so a
    /// retry re-sends the same part), and a best-effort abort so a failed
    /// upload does not strand parts. An ambiguous completion — a transport
    /// failure whose attempt may have landed — is resolved by the immutable
    /// operation's shared exact-byte read-back
    /// ([`MultipartWrite::resolve_ambiguous_completion`]).
    ///
    /// There is deliberately no whole-operation clock: every part attempt is
    /// individually bounded and every retry loop is count-bounded, so a
    /// healthy transfer takes as long as the link needs while a stuck one
    /// still fails within one part's retry budget. Completion itself is one
    /// attempt: only [`ObjectStore::put_immutable_verified`] may replay an
    /// object-publishing write. Conditional modes stay on the single-request
    /// path where real provider preconditions exist.
    async fn put_large_multipart(
        &self,
        multipart: &dyn MultipartStore,
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

        let upload_id = upload.create(size_bytes).await?;
        let result = upload.upload_parts_and_complete(&upload_id, &bytes).await;
        match result {
            Ok(metadata) => Ok(metadata),
            Err(err) => {
                // Best effort, and harmless when the failure raced a landed
                // completion: the upload id no longer exists then, and the
                // abort cannot touch the completed object.
                upload.abort(&upload_id).await;
                Err(err)
            }
        }
    }

    /// Applies the shared retry gate for one failed write attempt: `None`
    /// means the budget is spent and the caller must surface the error;
    /// `Some` carries the backoff to sleep before the next attempt. The
    /// budget is the retry count, plus the operation deadline when the
    /// operation carries one (multipart transfers deliberately do not — see
    /// [`Self::put_large_multipart`]). Exhaustion logs name the payload
    /// size so a too-slow-link failure is attributable instead of reading
    /// as weather.
    fn next_write_backoff(
        &self,
        key: &str,
        operation: &'static str,
        payload_bytes: u64,
        retries: &mut u32,
        deadline: Option<&OperationDeadline<'_>>,
        err: &provider_store::Error,
    ) -> Option<Duration> {
        if *retries >= self.transport_retry.max_retries {
            tracing::warn!(
                object_key = key,
                operation,
                retry = *retries,
                payload_bytes,
                error = %err,
                "object store write retry budget exhausted; not retrying",
            );
            return None;
        }
        let mut remaining = Duration::MAX;
        if let Some(deadline) = deadline {
            let Some(deadline_remaining) = deadline.remaining() else {
                tracing::warn!(
                    object_key = key,
                    operation,
                    retry = *retries,
                    payload_bytes,
                    error = %err,
                    "object store operation deadline exhausted; not retrying",
                );
                return None;
            };
            remaining = deadline_remaining;
        }
        *retries += 1;
        let backoff = transport_retry_backoff(&self.transport_retry, *retries).min(remaining);
        tracing::info!(
            object_key = key,
            operation,
            retry = *retries,
            max_retries = self.transport_retry.max_retries,
            backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            error = %err,
            "transient object store write failure, backing off before retry",
        );
        Some(backoff)
    }
}

/// One in-progress multipart write: the store, the provider multipart
/// surface, and the object being written.
struct MultipartWrite<'op> {
    store: &'op ProviderObjectStore,
    multipart: &'op dyn MultipartStore,
    key: &'op str,
    path: &'op Path,
}

impl MultipartWrite<'_> {
    async fn create(&self, payload_bytes: u64) -> Result<provider_store::MultipartId> {
        let mut retries: u32 = 0;
        loop {
            let err = match self.multipart.create_multipart(self.path).await {
                Ok(upload_id) => return Ok(upload_id),
                Err(err) => err,
            };
            if !provider_transport_retryable(&err) {
                return Err(map_provider_error(self.key, err));
            }
            let Some(backoff) = self.store.next_write_backoff(
                self.key,
                "create_multipart",
                payload_bytes,
                &mut retries,
                None,
                &err,
            ) else {
                return Err(map_provider_error(self.key, err));
            };
            transport_retry_pause(backoff).await;
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
            let Some(backoff) = self.store.next_write_backoff(
                self.key,
                "put_part",
                payload_bytes,
                &mut retries,
                None,
                &err,
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

    /// Decides an ambiguous completion by reading the object back: byte
    /// equality with the payload is the only accepted proof that the write
    /// landed. Size or etag agreement is never identity — this store serves
    /// generic overwrite keys, so a stale object of the same length must
    /// not pass as the new write. The payload is still in memory, so the
    /// read-back costs one GET on this failure path and proves the put's
    /// postcondition itself.
    ///
    /// A proven completion still aborts the upload id: when this upload's
    /// completion landed the id is already gone and the abort is a no-op,
    /// and when the proof came from an identical object some earlier writer
    /// committed, the abort reclaims this upload's stranded parts.
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
            Err(err) => {
                tracing::warn!(
                    object_key = self.key,
                    operation = "abort_multipart",
                    error = %err,
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
    /// The contract matches the local reference provider exactly: a missing
    /// object is `Ok(None)` however the request was shaped, an end past the
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
                return self
                    .put_large_multipart(multipart.as_ref(), key, &path, bytes)
                    .await;
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
            let Some(backoff) =
                self.next_write_backoff(key, "delete", 0, &mut retries, Some(&deadline), &err)
            else {
                return Err(map_provider_error(key, err));
            };
            transport_retry_pause(backoff).await;
        }
    }

    fn list_prefix_stream(&self, prefix: &str) -> BoxStream<'static, Result<String>> {
        let prefix_path = match self.list_path(prefix) {
            Ok(prefix_path) => prefix_path,
            Err(err) => return stream::once(async { Err(err) }).boxed(),
        };
        let key_prefix = self.key_prefix.clone();
        let listed_prefix = prefix.to_owned();
        self.inner
            .list(prefix_path.as_ref())
            .filter_map(move |result| {
                let key_prefix = key_prefix.clone();
                let listed_prefix = listed_prefix.clone();
                async move {
                    match result {
                        Ok(meta) => {
                            let key = meta.location.as_ref();
                            match key_prefix.as_deref() {
                                Some(prefix) => unscope_listed_key(Some(prefix), key).map(Ok),
                                None => Some(Ok(key.to_owned())),
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
        provider_store::Error::Generic { source, .. } => {
            ObjectStoreError::transport(object_key, sanitize_provider_message(&source.to_string()))
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

/// Query parameters whose values are credential material when they appear in
/// a URL a provider error echoes back (signed and presigned requests).
const CREDENTIAL_QUERY_PARAMS: &[&str] = &[
    "X-Amz-Signature",
    "X-Amz-Credential",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "Signature",
    "sig",
];

/// Response-body XML elements that echo signing inputs back to the caller in
/// provider auth failures (`SignatureDoesNotMatch` and friends).
const CREDENTIAL_XML_ELEMENTS: &[&str] = &[
    "StringToSign",
    "StringToSignBytes",
    "CanonicalRequest",
    "SignatureProvided",
    "AWSAccessKeyId",
];

/// Strips credential material from free-text provider errors before they
/// enter the error chain: signature/credential query parameters in echoed
/// URLs, and the signing-input elements auth-failure bodies quote back.
/// The diagnosable parts (status, provider error code, cause) stay.
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

/// Replaces every `?param=value` / `&param=value` occurrence's value with
/// `<redacted>`. Matches only at a query-parameter boundary so `Signature=`
/// does not fire inside `X-Amz-Signature=`.
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

/// Replaces the text inside `<element>...</element>` with `<redacted>`; if
/// the closing tag never arrives (truncated body), everything after the
/// opening tag goes.
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
mod tests {
    use super::*;
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
        let key = "namespaces/demo/metadata/tables/tbl_abc.sst.zst";
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
        let missing = "namespaces/demo/metadata/tables/tbl_missing.sst.zst";
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
    async fn provider_store_range_semantics_match_blocking_contract() {
        let store = memory_store();
        let key = "content-stores/cs_0123456789abcdef0123456789abcdef/blobs/sha256/ab/cd/abcdef";
        store
            .put_overwrite(key, Bytes::from_static(b"abcdef"))
            .await
            .expect("put");

        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 2,
                        end_exclusive: 4,
                    }),
                )
                .await
                .expect("range"),
            Some(Bytes::from_static(b"cd"))
        );
        assert_eq!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 6,
                        end_exclusive: 10,
                    }),
                )
                .await
                .expect("empty"),
            Some(Bytes::new())
        );
        assert!(matches!(
            store
                .get(
                    key,
                    Some(ByteRange {
                        start_inclusive: 7,
                        end_exclusive: 8,
                    }),
                )
                .await,
            Err(ObjectStoreError::InvalidRange { .. })
        ));
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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
                    unreachable!("VanishThenFail is a completion script")
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
                    unreachable!("VanishThenFail is a completion script")
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
                    unreachable!("VanishThenFail is a completion script")
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
        "content-stores/cs_0123456789abcdef0123456789abcdef/blobs/sha256/ab/cd/abcdef0123456789";

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

    /// Conditional modes never ride multipart: providers complete multipart
    /// uploads as unconditional overwrites, so create-if-absent keeps its
    /// real provider precondition on the single-request path at any size.
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

    /// The regression this fix exists for: an unproven completion must
    /// never adopt a pre-existing object of the same size. The pre-fix code
    /// reconciled by size identity and reported success for bytes that were
    /// never written.
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

    /// The content-addressed re-put shape: when the object already holds
    /// exactly the payload bytes, byte identity proves the put's
    /// postcondition even though this upload's completion never landed —
    /// and the dangling upload is reclaimed rather than stranded.
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

    /// The lifecycle-abort race: the upload vanishes while the completion's
    /// outcome is ambiguous and a stale object sits at the key. The stale
    /// object is never accepted as this write.
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

    /// A first-attempt refusal is definite: nothing can have landed, so no
    /// read-back runs and the refusal surfaces directly.
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

    /// When the read-back itself fails, the outcome stays unknown and
    /// surfaces as a transport error carrying both failures — never as a
    /// success the store cannot prove.
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

    /// Multipart transfers carry no whole-operation clock: with a stepping
    /// timer that would spend the single-request deadline almost instantly,
    /// part retries still run to their full count budget.
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
