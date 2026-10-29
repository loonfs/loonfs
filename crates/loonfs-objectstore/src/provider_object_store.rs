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
    AssemblySource, ByteRange, ByteStream, ConfiguredObjectStoreKind, MultipartPart, ObjectBody,
    ObjectMetadata, ObjectStore, ObjectStoreError, PutMode, StoreIdentity,
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
                Err(err) => RangedGet::Refused(err),
            },
            Err(err) if provider_not_found(&err) => RangedGet::NotFound,
            Err(err) => RangedGet::Refused(err),
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
fn provider_path(key: &str, scoped: String) -> Result<Path> {
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
                            RangedGet::Refused(err) => {
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
        // precondition failure is never its own landing, and a publication
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
    /// The first part is cut before anything is decided. A payload that
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
            .map_err(|err| map_provider_error(prefix, err, ProviderClient::Retrying))?;
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
}

fn provider_not_found(error: &provider_store::Error) -> bool {
    matches!(error, provider_store::Error::NotFound { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{InstrumentedObjectStore, VecObjectStoreMetricsRecorder};
    use crate::test_support::{auth_rejection, transport_glitch};
    use crate::ObjectStoreExtension as _;
    use futures::{StreamExt, TryStreamExt};
    use object_store::memory::InMemory;
    use provider_store::list::PaginatedListResult;
    use std::time::Duration;

    fn memory_store() -> ProviderObjectStore {
        retrying_store(Arc::new(FlakyStore::default()))
    }

    /// Builds a store whose provider clients are scripted doubles, and whose
    /// signer is a [`TestSigner`] over `provider`.
    fn provider_store(
        provider: Arc<FlakyStore>,
        one_attempt: Arc<FlakyStore>,
        kind: ConfiguredObjectStoreKind,
        key_prefix: Option<&str>,
    ) -> ProviderObjectStore {
        let key_scope = KeyScope::new(key_prefix).expect("key prefix");
        ProviderObjectStore::new(ProviderParts {
            provider: Arc::clone(&provider),
            one_attempt,
            signer: Arc::new(TestSigner {
                provider,
                key_scope: key_scope.clone(),
            }),
            client_multipart: None,
            kind,
            key_scope,
            location: "bucket".to_owned(),
            io_runtime: StoreIoRuntime::new().expect("store io runtime"),
            retry: Arc::new(TransportRetry::new(Arc::new(
                loonfs_types::StdMonotonicTimer::default(),
            ))),
        })
    }

    /// Reads stored checksums from the provider double and creates a
    /// multipart payload with one conditional put on the provider double,
    /// standing in for a provider's signed requests.
    struct TestSigner {
        provider: Arc<FlakyStore>,
        key_scope: KeyScope,
    }

    impl TestSigner {
        async fn create(&self, key: &str, bytes: Vec<u8>) -> Result<ObjectMetadata> {
            let (size_bytes, checksum) = (bytes.len() as u64, Checksum::crc64nvme(&bytes));
            let options = PutOptions {
                mode: provider_store::PutMode::Create,
                ..Default::default()
            };
            let path = Path::from(self.key_scope.object_key(key)?);
            let result = provider_store::ObjectStore::put_opts(
                self.provider.as_ref(),
                &path,
                PutPayload::from(bytes),
                options,
            )
            .await
            .map_err(|error| map_provider_error(key, error, ProviderClient::OneAttempt))?;
            Ok(ObjectMetadata {
                etag: result.e_tag,
                version: result.version,
                size_bytes,
                last_modified_at_ms: None,
                checksum: Some(checksum),
            })
        }
    }

    #[async_trait]
    impl ProviderSigner for TestSigner {
        async fn head_metadata(&self, key: &str) -> Result<Option<ObjectMetadata>> {
            let provider = &self.provider;
            provider.heads.fetch_add(1, Ordering::SeqCst);
            let path = Path::from(self.key_scope.object_key(key)?);
            let mut metadata = match provider.inner.get(&path).await {
                Ok(result) => {
                    let meta = result.meta.clone();
                    let bytes = result.bytes().await.map_err(|error| {
                        map_provider_error(key, error, ProviderClient::Retrying)
                    })?;
                    Some(ObjectMetadata {
                        etag: meta.e_tag,
                        version: meta.version,
                        size_bytes: meta.size,
                        last_modified_at_ms: last_modified_at_ms(meta.last_modified.into()),
                        checksum: Some(Checksum::crc64nvme(&bytes)),
                    })
                }
                Err(error) if provider_not_found(&error) => None,
                Err(error) => return Err(map_provider_error(key, error, ProviderClient::Retrying)),
            };
            if provider
                .missing_checksum
                .lock()
                .expect("checksum fault")
                .as_deref()
                == Some(key)
            {
                if let Some(metadata) = &mut metadata {
                    metadata.checksum = None;
                }
            }
            Ok(metadata)
        }

        async fn create_checked(
            &self,
            key: &str,
            bytes: Bytes,
            _checksum: &Checksum,
        ) -> Result<ObjectMetadata> {
            self.create(key, bytes.to_vec()).await
        }

        async fn put_if_absent(
            &self,
            key: &str,
            head: Bytes,
            mut rest: PartReader<'_>,
            _part_window: usize,
        ) -> Result<ObjectMetadata> {
            let mut bytes = head.to_vec();
            while let Some(part) = rest.next_part().await? {
                bytes.extend_from_slice(&part);
            }
            self.create(key, bytes).await
        }

        async fn assemble(
            &self,
            _key: &str,
            _sources: &[AssemblySource],
            _tail: Vec<Bytes>,
            _expected: &Checksum,
        ) -> Result<ObjectMetadata> {
            Err(ObjectStoreError::Unsupported("test assembly"))
        }
    }

    async fn memory_page(
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
        let mut last_key = None;
        for meta in listed {
            let key = meta.location.as_ref().to_owned();
            let after_offset = options
                .offset
                .as_deref()
                .is_none_or(|offset| key.as_str() > offset);
            let after_page = options
                .page_token
                .as_deref()
                .is_none_or(|token| key.as_str() > token);
            if !key.starts_with(prefix) || !after_offset || !after_page {
                continue;
            }
            let common = options.delimiter.as_deref().and_then(|delimiter| {
                key[prefix.len()..]
                    .find(delimiter)
                    .map(|end| key[..prefix.len() + end + delimiter.len()].to_owned())
            });
            if common.is_some() && common.as_ref() == common_prefixes.last() {
                continue;
            }
            if objects.len() + common_prefixes.len() == max_keys {
                truncated = true;
                break;
            }
            last_key = Some(key);
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
            page_token: if truncated { last_key } else { None },
        })
    }

    #[test]
    fn a_synthesized_epoch_stamp_reads_as_no_timestamp_at_all() {
        let after_epoch = |ms| UNIX_EPOCH + Duration::from_millis(ms);
        assert_eq!(last_modified_at_ms(UNIX_EPOCH), None);
        assert_eq!(
            last_modified_at_ms(UNIX_EPOCH - Duration::from_millis(1)),
            None
        );
        assert_eq!(last_modified_at_ms(after_epoch(1)), Some(1));
        assert_eq!(
            last_modified_at_ms(after_epoch(1_754_000_000_000)),
            Some(1_754_000_000_000)
        );
    }

    #[test]
    fn compare_tokens_populate_only_the_matching_provider_field() {
        for (kind, expected_etag, expected_version) in [
            (ConfiguredObjectStoreKind::AwsS3, Some("token"), None),
            (ConfiguredObjectStoreKind::GcpGcs, None, Some("token")),
        ] {
            let flaky = Arc::new(FlakyStore::default());
            let store = provider_store(Arc::clone(&flaky), flaky, kind, None);
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
    async fn partial_key_prefixes_keep_times_and_resume_across_provider_pages() {
        let store = memory_store();
        let prefix = "objects/con_a";
        let keys: Vec<_> = (0..1002)
            .map(|index| format!("{prefix}{index:04x}/full"))
            .collect();
        for key in keys
            .iter()
            .map(String::as_str)
            .chain(["objects/con_b/full"])
        {
            store
                .put_if_absent(key, Bytes::from_static(b"listed"))
                .await
                .expect("put");
        }
        let entries: Vec<_> = store
            .list_entries_from_stream(prefix, None)
            .try_collect()
            .await
            .expect("list pages");
        assert_eq!(
            entries.iter().map(|entry| &entry.key).collect::<Vec<_>>(),
            keys.iter().collect::<Vec<_>>()
        );
        for entry in entries {
            let head = store.head(&entry.key).await.expect("head").expect("exists");
            assert_eq!(entry.last_modified_at_ms, head.last_modified_at_ms);
            assert_eq!(entry.size_bytes, head.size_bytes);
        }
        let remaining: Vec<_> = store
            .list_prefix_from_stream(prefix, Some(&keys[999]))
            .try_collect()
            .await
            .expect("resume");
        assert_eq!(remaining, keys[1000..]);
    }

    #[tokio::test]
    async fn child_prefix_pages_resume_after_whole_children_inside_the_key_prefix() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        for key in [
            "namespaces/a/hint.json",
            "namespaces/a1",
            "namespaces/b/hint.json",
            "namespaces/b/wal/00000000000000000001.wal.zst",
            "namespaces/b0/hint.json",
            "namespaces/c/hint.json",
        ] {
            store
                .put_if_absent(key, Bytes::from_static(b"listed"))
                .await
                .expect("put");
        }
        provider_store::ObjectStoreExt::put(
            &flaky.inner,
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
        assert_matches!(
            store.get(key, range(11, 12)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        );
        // An inverted range is rejected without any store call.
        assert_matches!(
            store.get(key, range(6, 2)).await,
            Err(ObjectStoreError::InvalidRange { .. })
        );
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

        assert_matches!(
            store.put_if_absent(key, Bytes::from_static(b"two")).await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        );
        assert_matches!(
            store
                .compare_and_swap(key, "stale", Bytes::from_static(b"two"))
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        );
        assert_matches!(
            store
                .compare_and_swap(
                    "namespaces/missing/hint.json",
                    "missing",
                    Bytes::from_static(b"two")
                )
                .await,
            Err(ObjectStoreError::PreconditionFailed { .. })
        );
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
        assert_matches!(
            stream.next().await,
            Some(Err(ObjectStoreError::InvalidKey { .. }))
        );
    }

    use provider_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
    };
    use std::assert_matches;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug)]
    enum WriteScript {
        FailWithoutLanding,
        LandThenFail,
        FailAuth,
    }

    /// Provider double that fails scripted attempts before delegating to an
    /// in-memory store, so retry behavior is observable per attempt.
    #[derive(Default)]
    struct FlakyStore {
        inner: InMemory,
        put_script: Mutex<VecDeque<WriteScript>>,
        delete_script: Arc<Mutex<VecDeque<WriteScript>>>,
        puts: AtomicUsize,
        gets: AtomicUsize,
        heads: AtomicUsize,
        deletes: Arc<AtomicUsize>,
        missing_checksum: Mutex<Option<String>>,
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

    #[async_trait]
    impl PaginatedListStore for FlakyStore {
        async fn list_paginated(
            &self,
            prefix: Option<&str>,
            options: PaginatedListOptions,
        ) -> provider_store::Result<PaginatedListResult> {
            memory_page(&self.inner, prefix, options).await
        }
    }

    fn retrying_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
        provider_store(
            Arc::clone(&flaky),
            flaky,
            ConfiguredObjectStoreKind::LocalFs,
            Some("tenant-a"),
        )
    }

    fn script_puts(flaky: &FlakyStore, script: impl IntoIterator<Item = WriteScript>) {
        flaky.put_script.lock().expect("put script").extend(script);
    }

    #[tokio::test]
    async fn head_reports_the_stored_checksum_after_a_create() {
        let store = memory_store();
        let created = "namespaces/demo/content/con_00000000000000000000000001";
        store
            .put_if_absent(created, Bytes::from_static(b"created"))
            .await
            .expect("create");

        let head = store.head(created).await.expect("head").expect("created");
        let body = store
            .get_with_metadata(created)
            .await
            .expect("get")
            .expect("created");
        assert_eq!(head.checksum, Some(Checksum::crc64nvme(b"created")));
        assert_eq!(body.bytes, b"created".as_slice());
    }

    #[tokio::test]
    async fn an_immutable_write_creates_at_every_size_and_stores_its_checksum() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        for (key, payload) in [
            ("namespaces/demo/uploads/upl_2.json", multipart_payload(15)),
            (
                MULTIPART_KEY,
                multipart_payload(MULTIPART_TEST_THRESHOLD + 1),
            ),
        ] {
            let written = store
                .put_immutable_verified(key, Bytes::from(payload.clone()))
                .await
                .expect("immutable write");
            let head = store.head(key).await.expect("head").expect("written");
            assert_eq!(written.checksum, Some(Checksum::crc64nvme(&payload)));
            assert_eq!(head.checksum, written.checksum);
        }

        assert_eq!(flaky.puts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_occupied_key_is_checked_by_one_head_of_its_stored_checksum() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let ours = Bytes::from_static(b"ours");
        let identical = "namespaces/demo/segments/seg_1.sst.zst";
        let different = "namespaces/demo/segments/seg_2.sst.zst";
        let without_checksum = "namespaces/demo/segments/seg_3.sst.zst";
        store
            .put_if_absent(identical, ours.clone())
            .await
            .expect("seed identical");
        store
            .put_if_absent(different, Bytes::from_static(b"theirs"))
            .await
            .expect("seed different");
        seed_scoped_object(&flaky, without_checksum, ours.clone()).await;
        *flaky.missing_checksum.lock().expect("checksum fault") = Some(without_checksum.to_owned());
        flaky.puts.store(0, Ordering::SeqCst);

        let accepted = store
            .put_immutable_verified(identical, ours.clone())
            .await
            .expect("an equal stored checksum identifies this object");
        assert_eq!(accepted.checksum, Some(Checksum::crc64nvme(&ours)));
        assert_matches!(
            store.put_immutable_verified(different, ours.clone()).await,
            Err(ObjectStoreError::DifferentObject { object_key })
                if object_key == different
        );
        assert_matches!(
            store.put_immutable_verified(without_checksum, ours).await,
            Err(ObjectStoreError::StoredChecksumMissing { object_key })
                if object_key == without_checksum
        );
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 3);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 3);
        assert_eq!(
            flaky.gets.load(Ordering::SeqCst),
            0,
            "no bytes are read back"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_landed_write_whose_answer_was_lost_resolves_through_its_stored_checksum() {
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

        assert_matches!(error, ObjectStoreError::Transport { .. });
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 11);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn compare_and_swap_never_retries_transport_failures() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/hint.json";
        let seeded = store
            .put_if_absent(key, Bytes::from_static(b"one"))
            .await
            .expect("seed head");
        let etag = seeded.etag.expect("etag");
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);

        let error = store
            .compare_and_swap(key, &etag, Bytes::from_static(b"two"))
            .await
            .expect_err("compare-and-swap surfaces the transport failure");

        assert_matches!(error, ObjectStoreError::Transport { .. });
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
        let store = provider_store(
            Arc::clone(&resending),
            Arc::clone(&one_attempt),
            ConfiguredObjectStoreKind::LocalFs,
            None,
        );
        script_puts(&one_attempt, [WriteScript::LandThenFail]);
        let key = "namespaces/demo/uploads/upl_11.json";

        let error = store
            .put_if_absent(key, Bytes::from_static(b"claim"))
            .await
            .expect_err("the landed write's lost answer reaches the caller");
        assert_matches!(error, ObjectStoreError::Transport { .. });
        assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);

        assert_eq!(
            store.get(key, None).await.expect("get"),
            Some(Bytes::from_static(b"claim"))
        );
        assert_eq!(resending.puts.load(Ordering::SeqCst), 0);
        assert_eq!(resending.gets.load(Ordering::SeqCst), 1);
        assert_eq!(one_attempt.puts.load(Ordering::SeqCst), 1);
        assert_eq!(one_attempt.gets.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn non_transport_provider_errors_are_not_retried() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        script_puts(&flaky, [WriteScript::FailAuth]);
        let key = "namespaces/demo/uploads/upl_5.json";

        let error = store
            .put_immutable_verified(key, Bytes::from_static(b"payload"))
            .await
            .expect_err("auth rejection surfaces immediately");

        assert_matches!(error, ObjectStoreError::PermissionDenied { .. });
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
    }

    /// Checks that a delete is one call to the client that retries its own
    /// requests, so a failure from that client other than a timeout returns
    /// at once and isn't classified as retryable.
    #[tokio::test]
    async fn a_delete_is_one_call_to_the_client_that_retries_it() {
        let flaky = Arc::new(FlakyStore::default());
        let store = retrying_store(Arc::clone(&flaky));
        let key = "namespaces/demo/uploads/upl_6.json";
        store
            .put_if_absent(key, Bytes::from_static(b"payload"))
            .await
            .expect("seed object");

        for script in [WriteScript::FailWithoutLanding, WriteScript::LandThenFail] {
            flaky.deletes.store(0, Ordering::SeqCst);
            flaky
                .delete_script
                .lock()
                .expect("delete script")
                .push_back(script);
            let error = store
                .delete(key)
                .await
                .expect_err("the retrying client's failure is the delete's");
            assert_eq!(error.kind(), crate::ObjectStoreErrorKind::Other);
            assert_eq!(flaky.deletes.load(Ordering::SeqCst), 1);
        }
        assert!(store.head(key).await.expect("head").is_none());
        store
            .delete(key)
            .await
            .expect("a delete of a missing object succeeds");
    }

    #[tokio::test(start_paused = true)]
    async fn a_retried_call_reports_its_attempts_to_the_metrics_wrapper() {
        let flaky = Arc::new(FlakyStore::default());
        let recorder = Arc::new(VecObjectStoreMetricsRecorder::default());
        let store =
            InstrumentedObjectStore::new(retrying_store(Arc::clone(&flaky)), recorder.clone());

        store
            .put_if_absent(
                "namespaces/demo/uploads/upl_8.json",
                Bytes::from_static(b"payload"),
            )
            .await
            .expect("one conditional put");
        script_puts(&flaky, [WriteScript::FailWithoutLanding]);
        store
            .put_immutable_verified(
                "namespaces/demo/segments/seg_8.sst.zst",
                Bytes::from_static(b"payload"),
            )
            .await
            .expect("the immutable write converges");

        let attempts: Vec<_> = recorder
            .samples()
            .iter()
            .map(|sample| sample.attempts)
            .collect();
        assert_eq!(
            attempts,
            [1, 2],
            "a call that never retried made one attempt, and the retried write two"
        );
    }

    const MULTIPART_TEST_THRESHOLD: usize = 1024;
    const MULTIPART_TEST_PART: usize = 512;
    const MULTIPART_KEY: &str = "namespaces/demo/content/con_5bsqqg28t5cy4tqkff04hmasw9";

    /// Retrying store with a test-sized multipart geometry: payloads of
    /// 1024+ bytes go multipart in 512-byte parts.
    fn multipart_test_store(flaky: Arc<FlakyStore>) -> ProviderObjectStore {
        retrying_store(flaky)
            .multipart_geometry(MULTIPART_TEST_THRESHOLD as u64, MULTIPART_TEST_PART as u64)
    }

    fn multipart_payload(len: usize) -> Vec<u8> {
        (0..251_u8).cycle().take(len).collect()
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

    #[derive(Default)]
    struct CountingSigner {
        part_windows: Mutex<Vec<usize>>,
    }

    impl CountingSigner {
        fn part_windows(&self) -> Vec<usize> {
            self.part_windows.lock().expect("part windows").clone()
        }
    }

    #[async_trait]
    impl ProviderSigner for CountingSigner {
        async fn head_metadata(&self, _key: &str) -> Result<Option<ObjectMetadata>> {
            Ok(None)
        }

        async fn create_checked(
            &self,
            _key: &str,
            _bytes: Bytes,
            _checksum: &Checksum,
        ) -> Result<ObjectMetadata> {
            Err(ObjectStoreError::Unsupported("test create"))
        }

        async fn put_if_absent(
            &self,
            _key: &str,
            head: Bytes,
            mut rest: PartReader<'_>,
            part_window: usize,
        ) -> Result<ObjectMetadata> {
            self.part_windows
                .lock()
                .expect("part windows")
                .push(part_window);
            let mut checksum = loonfs_types::ChecksumDigest::new(ChecksumAlgorithm::Crc64nvme);
            checksum.update(&head);
            let mut size_bytes = head.len() as u64;
            while let Some(part) = rest.next_part().await? {
                checksum.update(&part);
                size_bytes += part.len() as u64;
            }
            Ok(ObjectMetadata {
                etag: Some("\"controller\"".to_owned()),
                version: None,
                size_bytes,
                last_modified_at_ms: None,
                checksum: Some(checksum.finish()),
            })
        }

        async fn assemble(
            &self,
            _key: &str,
            _sources: &[AssemblySource],
            _tail: Vec<Bytes>,
            _expected: &Checksum,
        ) -> Result<ObjectMetadata> {
            Err(ObjectStoreError::Unsupported("test assembly"))
        }
    }

    #[tokio::test]
    async fn a_publication_is_one_put_and_only_verified_creates_get_the_part_window() {
        let flaky = Arc::new(FlakyStore::default());
        let controller = Arc::new(CountingSigner::default());
        let store = multipart_test_store(Arc::clone(&flaky))
            .signer(Arc::clone(&controller) as Arc<dyn ProviderSigner>);
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

        let written = store
            .put_immutable_verified(MULTIPART_KEY, Bytes::from(payload.clone()))
            .await
            .expect("a large verified write creates through the controller");
        assert_eq!(
            controller.part_windows(),
            [PROVIDER_MULTIPART_PARTS_IN_FLIGHT]
        );
        assert_eq!(written.size_bytes, payload.len() as u64);
        assert_eq!(written.checksum, Some(Checksum::crc64nvme(&payload)));
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

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
            [PROVIDER_MULTIPART_PARTS_IN_FLIGHT, 1],
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
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        let error = store
            .put_if_absent(MULTIPART_KEY, Bytes::from(multipart_payload(1300)))
            .await
            .expect_err("existing object fails the create precondition");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
        assert_eq!(
            flaky.heads.load(Ordering::SeqCst),
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
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn a_short_streamed_put_is_one_request_that_keeps_its_precondition() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(MULTIPART_TEST_PART - 1);

        store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 64),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect("short streamed put");
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);

        let error = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 64),
                PutMode::CreateIfAbsent,
            )
            .await
            .expect_err("the key is taken and create-only means it");
        assert_matches!(error, ObjectStoreError::PreconditionFailed { .. });
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::from(payload))
        );
    }

    #[tokio::test]
    async fn a_streamed_compare_and_swap_longer_than_one_part_is_unsupported() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let payload = multipart_payload(1300);

        let error = store
            .put_streamed(
                MULTIPART_KEY,
                streamed(&payload, 100),
                PutMode::CompareAndSwap {
                    expected_etag: "\"etag\"".to_owned(),
                },
            )
            .await
            .expect_err("no store emulates a multipart compare-and-swap");

        let ObjectStoreError::Unsupported(capability) = error else {
            panic!("expected Unsupported, got {error:?}");
        };
        assert_eq!(capability, "streamed compare-and-swap longer than one part");
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 0);
        assert_eq!(flaky.heads.load(Ordering::SeqCst), 0);
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
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 1);
        assert_eq!(
            store.get(MULTIPART_KEY, None).await.expect("get"),
            Some(Bytes::new())
        );
    }

    #[tokio::test]
    async fn a_streamed_put_that_fails_mid_stream_writes_nothing() {
        let flaky = Arc::new(FlakyStore::default());
        let store = multipart_test_store(Arc::clone(&flaky));
        let head = multipart_payload(MULTIPART_TEST_PART);
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

        assert_matches!(error, ObjectStoreError::Transport { .. });
        assert_eq!(flaky.puts.load(Ordering::SeqCst), 0);
        assert_eq!(store.get(MULTIPART_KEY, None).await.expect("get"), None);
    }
}
