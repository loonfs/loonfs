//! [`ProviderObjectStore`]: the adapter from the upstream `object_store`
//! provider clients to the LoonFS [`ObjectStore`] contract, and the routing
//! of large creates to the provider's own conditional requests.

use crate::keyspace::KeyScope;
use crate::object_store::{error_stream, unsupported_multipart, Result};
use crate::presign::DirectTransferIssuers;
use crate::provider_error::{map_provider_error, sanitize_provider_message, ProviderClient};
use crate::provider_limits::{
    provider_client_options, PROVIDER_MULTIPART_PARTS_IN_FLIGHT, PROVIDER_MULTIPART_PART_BYTES,
    PROVIDER_MULTIPART_THRESHOLD_BYTES,
};
use crate::retry::TransportRetry;
use crate::signed_upload::{ClientMultipart, PartReader, ProviderSigner};
use crate::store_io_runtime::StoreIoRuntime;
use crate::transfer_timeouts::is_transfer_timeout;
use crate::ListedObject;
use crate::{
    AssemblySource, ByteRange, ByteStream, ConfiguredObjectStoreKind, GetIfChangedOutcome,
    MultipartPart, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    StoreIdentity,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use loonfs_types::{Checksum, ChecksumAlgorithm};
use loonfs_types::{EffectiveLimit, Page};
use object_store as provider_store;
use object_store::client::{HttpClient, HttpConnector};
use provider_store::list::{PaginatedListOptions, PaginatedListStore};
use provider_store::path::Path;
use provider_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStoreExt, PutOptions, PutPayload, PutResult,
    UpdateVersion,
};
use std::borrow::Cow;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Connects the HTTP client that sends the requests a provider adapter signs
/// itself, on the store's IO runtime and under [`provider_client_options`].
pub(crate) fn provider_http(io_runtime: &StoreIoRuntime) -> Result<HttpClient> {
    io_runtime
        .connector()
        .connect(&provider_client_options())
        .map_err(configuration_error)
}

/// Returns the error for a provider client that couldn't be built.
pub(crate) fn configuration_error(error: impl fmt::Display) -> ObjectStoreError {
    ObjectStoreError::Configuration(error.to_string())
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

/// The version field that a provider's conditional writes compare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompareToken {
    /// The ETag, as S3 and Cloudflare R2 compare it.
    Etag,
    /// The object generation, as GCS compares it.
    Generation,
}

/// The requests that create an object while its key is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CreatePath {
    /// The provider client's conditional put.
    ProviderClient,
    /// A request that the store's signer sends with the object's checksum.
    Signer,
}

/// A provider store and the issuers that sign direct transfers against it.
pub(crate) struct ConfiguredProvider {
    pub(crate) store: ProviderObjectStore,
    pub(crate) direct_transfers: DirectTransferIssuers,
}

/// The clients and provider facts that one provider store is built from.
pub(crate) struct ProviderParts<P> {
    /// The client that retries each request it sends. Reads, deletes, and
    /// listings go through it.
    pub(crate) provider: Arc<P>,
    /// The same client built to send each request once. Every single-request
    /// put goes through it, so [`map_provider_error`] can tell which client
    /// an error came from.
    pub(crate) one_attempt: Arc<dyn provider_store::ObjectStore>,
    pub(crate) signer: Arc<dyn ProviderSigner>,
    /// The client-driven multipart requests, or `None` for a provider whose
    /// adapter signs none.
    pub(crate) client_multipart: Option<Arc<dyn ClientMultipart>>,
    pub(crate) kind: ConfiguredObjectStoreKind,
    pub(crate) key_scope: KeyScope,
    /// The bucket, with the endpoint when the configuration sets one, as
    /// [`StoreIdentity::location`] describes.
    pub(crate) location: String,
    pub(crate) io_runtime: StoreIoRuntime,
    pub(crate) retry: Arc<TransportRetry>,
}

/// Adapts the upstream `object_store` provider surface to the narrower LoonFS contract.
#[derive(Clone)]
pub struct ProviderObjectStore {
    inner: Arc<dyn provider_store::ObjectStore>,
    one_attempt: Arc<dyn provider_store::ObjectStore>,
    paginated: Arc<dyn PaginatedListStore>,
    signer: Arc<dyn ProviderSigner>,
    client_multipart: Option<Arc<dyn ClientMultipart>>,
    kind: ConfiguredObjectStoreKind,
    io_runtime: StoreIoRuntime,
    multipart_geometry: MultipartGeometry,
    key_scope: KeyScope,
    location: String,
    retry: Arc<TransportRetry>,
}

impl fmt::Debug for ProviderObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderObjectStore")
            .field("kind", &self.kind.as_str())
            .field("key_prefix", &self.key_scope.prefix())
            .field("io_runtime", &self.io_runtime)
            .finish_non_exhaustive()
    }
}

impl ProviderObjectStore {
    /// Builds a store from a provider's clients, its signer, and its facts.
    pub(crate) fn new<P>(parts: ProviderParts<P>) -> Self
    where
        P: provider_store::ObjectStore + PaginatedListStore,
    {
        Self {
            inner: Arc::clone(&parts.provider) as Arc<dyn provider_store::ObjectStore>,
            one_attempt: parts.one_attempt,
            paginated: parts.provider,
            signer: parts.signer,
            client_multipart: parts.client_multipart,
            kind: parts.kind,
            io_runtime: parts.io_runtime,
            multipart_geometry: MultipartGeometry::DEFAULT,
            key_scope: parts.key_scope,
            location: parts.location,
            retry: parts.retry,
        }
    }

    #[cfg(test)]
    fn signer(mut self, signer: Arc<dyn ProviderSigner>) -> Self {
        self.signer = signer;
        self
    }

    #[cfg(test)]
    pub(crate) fn multipart_geometry(mut self, threshold_bytes: u64, part_bytes: u64) -> Self {
        self.multipart_geometry = MultipartGeometry {
            threshold_bytes,
            part_bytes,
        };
        self
    }

    fn to_path(&self, key: &str) -> Result<Path> {
        provider_path(key, self.key_scope.object_key(key)?)
    }

    fn validate_key(&self, key: &str) -> Result<()> {
        self.to_path(key).map(|_| ())
    }

    fn client_multipart(&self) -> Result<&dyn ClientMultipart> {
        self.client_multipart
            .as_deref()
            .ok_or_else(unsupported_multipart)
    }

    fn metadata_from_provider(&self, meta: ObjectMeta) -> ObjectMetadata {
        self.with_compare_token(ObjectMetadata {
            etag: meta.e_tag,
            version: meta.version,
            size_bytes: meta.size,
            last_modified_at_ms: last_modified_at_ms(meta.last_modified.into()),
            checksum: None,
        })
    }

    fn metadata_from_put_result(&self, result: PutResult, size_bytes: u64) -> ObjectMetadata {
        self.with_compare_token(ObjectMetadata {
            etag: result.e_tag,
            version: result.version,
            size_bytes,
            last_modified_at_ms: None,
            checksum: None,
        })
    }

    fn with_compare_token(&self, metadata: ObjectMetadata) -> ObjectMetadata {
        match self.kind.compare_token() {
            CompareToken::Etag => metadata,
            CompareToken::Generation => ObjectMetadata {
                etag: metadata.version.clone(),
                ..metadata
            },
        }
    }

    fn validate_compare_token(&self, key: &str, mode: &PutMode) -> Result<()> {
        if self.kind.compare_token() == CompareToken::Generation {
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
                // The provider already accepted the range, so a body error is
                // a transport failure and never needs a sizing HEAD.
                Err(err) => RangedGet::Failed(err),
            },
            Err(err) if provider_not_found(&err) => RangedGet::NotFound,
            Err(err) if may_be_range_refusal(&err) => RangedGet::Refused(err),
            Err(err) => RangedGet::Failed(err),
        }
    }

    /// Streams the objects of one listing, after checking its prefix and
    /// its resume key.
    fn entries_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<BoxStream<'static, Result<ListedObject>>> {
        let listing = self.key_scope.listing(prefix, start_after)?;
        provider_path(prefix, listing.prefix.clone())?;
        let offset = match (start_after, listing.start_after) {
            (Some(key), Some(scoped)) => Some(provider_path(key, scoped)?.to_string()),
            _ => None,
        };
        let scoped_prefix = listing.prefix;
        let key_scope = self.key_scope.clone();
        let listed_prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        let paginated = Arc::clone(&self.paginated);
        let options = PaginatedListOptions {
            offset,
            ..Default::default()
        };
        Ok(stream::try_unfold(Some(options), move |options| {
            let paginated = Arc::clone(&paginated);
            let scoped_prefix = scoped_prefix.clone();
            async move {
                let Some(options) = options else {
                    return Ok::<_, provider_store::Error>(None);
                };
                let next_options = options.clone();
                let page = paginated
                    .list_paginated(
                        Some(scoped_prefix.as_str()).filter(|prefix| !prefix.is_empty()),
                        options,
                    )
                    .await?;
                let next = page.page_token.map(|page_token| PaginatedListOptions {
                    page_token: Some(page_token),
                    ..next_options
                });
                Ok(Some((page.result.objects, next)))
            }
        })
        .map_ok(|objects| stream::iter(objects.into_iter().map(Ok)))
        .try_flatten()
        .filter_map(move |result| {
            let listed = match result {
                Ok(meta) => key_scope
                    .unscope(meta.location.as_ref())
                    .filter(|key| {
                        key.starts_with(&listed_prefix)
                            && start_after
                                .as_deref()
                                .is_none_or(|after| key.as_str() > after)
                    })
                    .map(|key| {
                        Ok(ListedObject {
                            key,
                            last_modified_at_ms: last_modified_at_ms(meta.last_modified.into()),
                            size_bytes: meta.size,
                        })
                    }),
                Err(err) => Some(Err(map_provider_error(
                    &listed_prefix,
                    err,
                    ProviderClient::Retrying,
                ))),
            };
            std::future::ready(listed)
        })
        .boxed())
    }
}

/// Parses the provider path of `scoped`, and reports a failure against the
/// caller's `key`.
pub(crate) fn provider_path(key: &str, scoped: String) -> Result<Path> {
    Path::parse(scoped).map_err(|err| ObjectStoreError::InvalidKey {
        object_key: key.to_owned(),
        message: err.to_string(),
    })
}

#[async_trait]
impl ObjectStore for ProviderObjectStore {
    fn checksum_algorithm(&self) -> ChecksumAlgorithm {
        self.kind.checksum_algorithm()
    }

    fn identity(&self) -> Option<StoreIdentity> {
        Some(StoreIdentity {
            provider: self.kind.as_str(),
            location: self.location.clone(),
            key_prefix: self.key_scope.prefix().map(str::to_owned),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>> {
        self.validate_key(key)?;
        self.signer.head_metadata(key).await
    }

    async fn create_multipart_upload(&self, key: &str) -> Result<String> {
        self.client_multipart()?.create_multipart_upload(key).await
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        provider_upload_id: &str,
        parts: &[MultipartPart],
        checksum: &Checksum,
    ) -> Result<()> {
        self.client_multipart()?
            .complete_multipart_upload(key, provider_upload_id, parts, checksum)
            .await
    }

    async fn abort_multipart_upload(&self, key: &str, provider_upload_id: &str) -> Result<()> {
        self.client_multipart()?
            .abort_multipart_upload(key, provider_upload_id)
            .await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>> {
        let path = self.to_path(key)?;
        match self.inner.get(&path).await {
            Ok(result) => {
                let metadata = self.metadata_from_provider(result.meta.clone());
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying))?;
                Ok(Some(ObjectBody { metadata, bytes }))
            }
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err, ProviderClient::Retrying)),
        }
    }

    async fn get_if_changed(&self, key: &str, etag: &str) -> Result<Option<GetIfChangedOutcome>> {
        // S3 and R2 compare the ETag, so they send it as `If-None-Match`.
        // LoonFS keeps the object generation as the compare token on GCS,
        // because GCS conditional writes compare the generation. The
        // provider client's read options have no condition on a generation,
        // and a generation never equals a GCS ETag, so a GCS read is a full
        // read and a compare.
        if self.kind.compare_token() == CompareToken::Generation {
            return Ok(self
                .get_with_metadata(key)
                .await?
                .map(|body| GetIfChangedOutcome::from_body(body, etag)));
        }
        let path = self.to_path(key)?;
        let options = GetOptions {
            if_none_match: Some(etag.to_owned()),
            ..Default::default()
        };
        match self.inner.get_opts(&path, options).await {
            Ok(result) => {
                let metadata = self.metadata_from_provider(result.meta.clone());
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying))?;
                Ok(Some(GetIfChangedOutcome::Changed(ObjectBody {
                    metadata,
                    bytes,
                })))
            }
            Err(provider_store::Error::NotModified { .. }) => {
                Ok(Some(GetIfChangedOutcome::Unchanged))
            }
            Err(err) if provider_not_found(&err) => Ok(None),
            Err(err) => Err(map_provider_error(key, err, ProviderClient::Retrying)),
        }
    }

    /// Bounded reads issue the ranged GET directly (one round trip, not a
    /// sizing HEAD plus a GET) and pay a single HEAD only on the failure
    /// path, to determine whether the range or the transport was the problem.
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
                    .map_err(|err| map_provider_error(key, err, ProviderClient::Retrying)),
                Err(err) if provider_not_found(&err) => Ok(None),
                Err(err) => Err(map_provider_error(key, err, ProviderClient::Retrying)),
            };
        };
        if range.end_exclusive < range.start_inclusive {
            return Err(ObjectStoreError::InvalidRange {
                object_key: key.to_owned(),
            });
        }
        if range.end_exclusive == range.start_inclusive {
            // A zero-length request needs no bytes; existence and size
            // alone settle it.
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
            RangedGet::Failed(err) => Err(map_provider_error(key, err, ProviderClient::Retrying)),
            RangedGet::Refused(err) => {
                // The provider refused; one HEAD determines whether the range
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
                            RangedGet::Refused(err) | RangedGet::Failed(err) => {
                                Err(map_provider_error(key, err, ProviderClient::Retrying))
                            }
                        }
                    }
                    Some(_) => Err(map_provider_error(key, err, ProviderClient::Retrying)),
                }
            }
        }
    }

    async fn put(&self, key: &str, bytes: Bytes, mode: PutMode) -> Result<ObjectMetadata> {
        let path = self.to_path(key)?;
        self.validate_compare_token(key, &mode)?;
        let checksum = Checksum::compute(self.checksum_algorithm(), &bytes);
        if matches!(mode, PutMode::CreateIfAbsent) && self.kind.create_path() == CreatePath::Signer
        {
            return self.signer.create_checked(key, bytes, &checksum).await;
        }
        let size_bytes = bytes.len() as u64;
        // A conditional write is one request at every size, so its
        // precondition failure is never its own write, and a publication
        // ends within one request's allowance.
        let compare_and_swap = matches!(mode, PutMode::CompareAndSwap { .. });
        let options = PutOptions {
            mode: self.map_put_mode(mode),
            ..Default::default()
        };
        match self
            .one_attempt
            .put_opts(&path, PutPayload::from(bytes), options)
            .await
        {
            Ok(result) => Ok(ObjectMetadata {
                checksum: Some(checksum),
                ..self.metadata_from_put_result(result, size_bytes)
            }),
            Err(err) if compare_and_swap && provider_not_found(&err) => {
                Err(ObjectStoreError::PreconditionFailed {
                    object_key: key.to_owned(),
                })
            }
            Err(err) => Err(map_provider_error(key, err, ProviderClient::OneAttempt)),
        }
    }

    /// A verified write of one part or less is the single-request create
    /// that [`Self::put`] sends. A longer one goes through the provider's
    /// own multipart requests where it has them, so the create stays
    /// conditional at every size. Its parts are replay-safe and each is
    /// bounded by the operation deadline, so the whole takes longer than
    /// one request's allowance: content and segments come through here,
    /// never a publication.
    async fn put_immutable_verified(&self, key: &str, bytes: Bytes) -> Result<ObjectMetadata> {
        let multipart = bytes.len() as u64 >= self.multipart_geometry.threshold_bytes;
        crate::immutable_write::put_with(self, &self.retry, key, &bytes, || {
            let bytes = bytes.clone();
            async move {
                if !multipart {
                    return self.put(key, bytes, PutMode::CreateIfAbsent).await;
                }
                let mut parts = PartReader::new(
                    stream::once(async { Ok(bytes) }).boxed(),
                    loonfs_types::usize_from_u64(self.multipart_geometry.part_bytes),
                );
                let head = parts.next_part().await?.unwrap_or_default();
                self.signer
                    .put_if_absent(key, head, parts, PROVIDER_MULTIPART_PARTS_IN_FLIGHT)
                    .await
            }
        })
        .await
    }

    /// Cuts the payload into parts as it arrives and uploads them one at a
    /// time, so a large object costs one part of memory instead of its own
    /// size. The server budgets a client's upload at that one part.
    ///
    /// The first part is cut before anything is determined. A payload that
    /// ends inside it is an ordinary [`ObjectStore::put`] with the caller's
    /// mode enforced by the provider, so a small streamed write behaves
    /// exactly like a small buffered one. A longer create-if-absent
    /// completes conditionally through the provider's own requests. A
    /// longer compare-and-swap is refused as unsupported, because no
    /// provider path here completes a multipart upload under a compare
    /// token, and the store doesn't emulate the condition with a separate
    /// read.
    async fn put_streamed(&self, key: &str, body: ByteStream, mode: PutMode) -> Result<u64> {
        self.validate_key(key)?;
        self.validate_compare_token(key, &mode)?;
        let mut reader = PartReader::new(
            body,
            loonfs_types::usize_from_u64(self.multipart_geometry.part_bytes),
        );
        let head = reader.next_part().await?.unwrap_or_else(Bytes::new);
        if reader.exhausted() {
            let size_bytes = head.len() as u64;
            self.put(key, head, mode).await?;
            return Ok(size_bytes);
        }
        match mode {
            PutMode::CreateIfAbsent => self
                .signer
                .put_if_absent(key, head, reader, 1)
                .await
                .map(|metadata| metadata.size_bytes),
            PutMode::CompareAndSwap { .. } => Err(ObjectStoreError::Unsupported(
                "streamed compare-and-swap longer than one part",
            )),
        }
    }

    /// Sends a delete through the retrying client, and sends it again when
    /// its attempt timed out.
    ///
    /// The retrying client retries a delete's connection failures and 5xx
    /// responses, but it doesn't resend a DELETE that timed out, because it
    /// doesn't mark DELETE idempotent. A delete is idempotent here (deleting
    /// a missing object succeeds), so LoonFS resends a delete whose attempt
    /// timed out, under one retry budget. Every other failure from the
    /// retrying client returns at once.
    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.to_path(key)?;
        self.retry
            .run(key, "delete", 0, || async {
                match self.inner.delete(&path).await {
                    Ok(()) => Ok(()),
                    Err(error) if provider_not_found(&error) => Ok(()),
                    Err(error) if is_transfer_timeout(&error) => {
                        Err(ObjectStoreError::retryable_transport(
                            key,
                            sanitize_provider_message(&error.to_string()),
                        ))
                    }
                    Err(error) => Err(map_provider_error(key, error, ProviderClient::Retrying)),
                }
            })
            .await
    }

    async fn assemble(
        &self,
        key: &str,
        sources: &[AssemblySource],
        tail: Vec<Bytes>,
        expected: &Checksum,
    ) -> Result<ObjectMetadata> {
        crate::assembly::validate_algorithms(self.checksum_algorithm(), key, sources, expected)?;
        // Below the threshold the create is one conditional PUT, and only an
        // occupied key costs a HEAD, after the conflict. A multipart assembly
        // checks first, so a retry copies nothing, and so that it never
        // completes over an object it saw on a provider whose conditional
        // completion is unconfirmed (R2).
        if crate::assembly::assembled_size_bytes(sources, &tail)
            >= PROVIDER_MULTIPART_THRESHOLD_BYTES
        {
            if let Some(existing) = self.head(key).await? {
                return crate::immutable_write::verify_stored_checksum(key, expected, existing);
            }
        }
        let created = self.signer.assemble(key, sources, tail, expected).await;
        crate::immutable_write::verify_created(self, key, expected, created).await
    }

    fn list_entries_from_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ListedObject>> {
        self.entries_stream(prefix, start_after)
            .unwrap_or_else(error_stream)
    }

    async fn list_child_prefixes(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: EffectiveLimit,
    ) -> Result<Page<String, String>> {
        let scoped_prefix = self.key_scope.child_listing_prefix(prefix)?;
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
                self.key_scope.object_key(&resume_key)
            })
            .transpose()?;
        let mut options = PaginatedListOptions {
            offset,
            delimiter: Some(Cow::Borrowed("/")),
            max_keys: Some(limit.as_usize()),
            ..Default::default()
        };
        // A page can be empty and still have a continuation token. Azure
        // starts a listing at the cursor itself and object_store removes that
        // entry, so a page of one is empty when the cursor is an object. An
        // empty page lists nothing to resume from, so the listing follows
        // the token until a page has an entry or the token runs out.
        let listed = loop {
            let page = self
                .paginated
                .list_paginated(
                    Some(scoped_prefix.as_str()).filter(|scoped| !scoped.is_empty()),
                    options.clone(),
                )
                .await
                .map_err(|err| map_provider_error(prefix, err, ProviderClient::Retrying))?;
            let empty = page.result.objects.is_empty() && page.result.common_prefixes.is_empty();
            match page.page_token {
                Some(page_token) if empty => {
                    options = PaginatedListOptions {
                        page_token: Some(page_token),
                        ..options
                    };
                }
                _ => break page,
            }
        };
        let children: Vec<String> = listed
            .result
            .common_prefixes
            .iter()
            .filter_map(|child| self.key_scope.unscope(&format!("{child}/")))
            .collect();
        let next_cursor = match listed.page_token {
            None => None,
            Some(_) => {
                let last_object = listed
                    .result
                    .objects
                    .last()
                    .and_then(|object| self.key_scope.unscope(object.location.as_ref()));
                let last_entry = children.last().cloned().max(last_object);
                Some(last_entry.ok_or_else(|| {
                    ObjectStoreError::transport(
                        prefix,
                        "provider listed an entry outside the key prefix",
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
    fn map_put_mode(&self, mode: PutMode) -> provider_store::PutMode {
        match mode {
            PutMode::CreateIfAbsent => provider_store::PutMode::Create,
            PutMode::CompareAndSwap { expected_etag } => {
                let version = match self.kind.compare_token() {
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
/// the object as extremely old. Timestamps at or before the epoch are
/// therefore returned as `None`, which causes unknown-age objects to be
/// retained.
pub(crate) fn last_modified_at_ms(last_modified: SystemTime) -> Option<u64> {
    let elapsed = last_modified.duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(elapsed.as_millis())
        .ok()
        .filter(|elapsed_ms| *elapsed_ms > 0)
}

enum RangedGet {
    Bytes(Bytes),
    NotFound,
    Refused(provider_store::Error),
    Failed(provider_store::Error),
}

fn provider_not_found(error: &provider_store::Error) -> bool {
    matches!(error, provider_store::Error::NotFound { .. })
}

/// Returns whether a provider error from a ranged GET might be an HTTP 416 or
/// range rejection rather than a definite non-range failure (auth, timeout, or
/// unsupported operation).
fn may_be_range_refusal(error: &provider_store::Error) -> bool {
    match error {
        // Local timeout wrappers embed `ObjectStoreError` inside `Generic`.
        provider_store::Error::Generic { source, .. } => !source.is::<ObjectStoreError>(),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
