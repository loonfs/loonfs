//! Async HTTP client for a LoonFS server.
//!
//! Use this crate when your process should talk to a hosted LoonFS runtime
//! instead of embedding the runtime directly. The client keeps paths simple:
//! pass a [`NamespacePath`] for filesystem operations and use explicit commit
//! helpers when you need retry control.
//!
//! The public surface is [`Client`] and the values it takes: [`ClientConfig`],
//! [`ClientError`], [`NamespacePath`], and the per-operation option structs
//! re-exported below. There is no transport abstraction to implement — a
//! process that wants the runtime in-process uses the `loonfs` crate instead,
//! and the two surfaces share one definition of every option struct (they
//! live in `loonfs-api`) so their arguments cannot drift apart.

mod config;
mod error;
mod payload;
mod transport;

use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_api::{
    v0::{
        AbortUploadResponse, BeginDownloadRequest, BeginDownloadResponse, BeginUploadRequest,
        BeginUploadResponse, ChangesResponse, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, CompletedUploadPart,
        DirectMultipartContentClaim, DirectMultipartUploadOptions, DirectPutContentClaim,
        DisableGrepIndexResponse, EnableGrepIndexResponse, GrepGcRequest, GrepGcResponse,
        GrepIndexStatusResponse, ObjectTransferAccess, SignUploadPartsRequest,
        SignUploadPartsResponse, SignedUploadPart, StoreProbeRequest, StoreProbeResponse,
        UploadContentResponse, UploadPartChecksumClaim, UploadStatusResponse,
        ValidatedContentToken,
    },
    AbsolutePath, AuthoritativePathEntry, CapabilityDocument, ChangeSeq, CheckpointId,
    ChecksumAlgorithm, CommitId, CommitRequest, ContentEvidence, ContentRef, Crc64Nvme,
    CreateCheckpointRequest, CreateCheckpointResponse, CreateNamespaceRequest,
    DeleteNamespaceResponse, ErrorCode, FilesystemOperation, ForkNamespaceRequest, GrepRequest,
    GrepResponse, InodeId, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListPathEntriesResponse, ListTrashResponse, MaintenanceStepRequest, MaintenanceStepResponse,
    NamespaceId, NamespaceStatusResponse, NamespaceSummary, PutRetryAttempt,
    PutRetryErrorClassification, PutRetryReceipt, ReleaseCheckpointResponse, RevisionNo,
    SecretString, StorageChecksum, StreamingChecksum, UploadId, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_UPLOADS_DIRECT_MULTIPART, LIMIT_DOWNLOAD_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES, LIMIT_UPLOAD_MAX_CONTENT_BYTES,
};
use payload::PartReader;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Payload size from which a put stops holding its bytes whole.
///
/// It mirrors the server's multipart part size, and that one number answers
/// two questions the same way: below it a direct multipart upload would be a
/// one-part upload with extra round trips and nothing to gain, and a payload
/// that fits in a single part is not worth streaming either. At or above it
/// — and for any payload whose length is not known in advance — a put reads
/// its source once, in bounded pieces.
pub const STREAMING_PUT_MIN_BYTES: u64 = 8 * 1024 * 1024;

/// Parts in flight at once. Each holds its bytes, so a one-pass upload's
/// memory is this many parts and no more.
const DIRECT_MULTIPART_PARTS_IN_FLIGHT: usize = 4;

/// Attempts one part gets before its upload gives up. A retry re-asks for
/// the part's URL, because the first thing that goes stale about a part is
/// its signature.
const DIRECT_MULTIPART_PART_ATTEMPTS: usize = 3;

fn classify_put_retry_error(error: &ClientError) -> PutRetryErrorClassification {
    match error.code() {
        Some(ErrorCode::CommitIdReuseConflict) => {
            let receipt = match error {
                ClientError::Api { details, .. } => details.as_ref().and_then(|details| {
                    Some(PutRetryReceipt {
                        committed_seq: details.committed_seq?,
                        committed_fingerprint: details.committed_fingerprint.clone()?,
                    })
                }),
                _ => None,
            };
            PutRetryErrorClassification::CommitIdReuseConflict(receipt)
        }
        Some(ErrorCode::RebootstrapRequired) => PutRetryErrorClassification::RebootstrapRequired,
        _ => PutRetryErrorClassification::Other,
    }
}

pub use config::ClientConfig;
pub use error::ClientError;
pub use payload::{PayloadSource, PayloadStream};
use transport::{StdMonotonicTimer, TransportRetryPolicy, WireRequest, IO_INACTIVITY_TIMEOUT};
pub use ClientError as Error;

/// Per-operation options, defined once in `loonfs-api` and shared with the
/// embedded `loonfs` runtime so the two surfaces cannot drift a field apart.
pub use loonfs_api::options::{
    CopyOptions, CreateDirectoryOptions, DeleteOptions, ListPathEntriesOptions, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, StatPathOptions, UndeleteOptions,
    UpdateAttributesOptions,
};

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, ClientError>;

/// Async HTTP client for LoonFS.
///
/// Cloning is cheap: clones share one connection pool and one capability
/// cache.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<SecretString>,
    http: reqwest::Client,
    /// Caller-selected whole-request deadline, retained so retry attempts can
    /// use the stricter of it and the remaining operation budget.
    request_timeout: Option<Duration>,
    /// Whether retryable transport and server errors are retried (see
    /// [`ClientConfig::disable_transient_retry`]).
    transport_retry_enabled: bool,
    /// Count, backoff, and elapsed-time bounds for replay-safe control calls.
    transport_retry: TransportRetryPolicy,
    /// Monotonic elapsed-time source for the operation deadline.
    timer: Arc<dyn transport::MonotonicTimer>,
    /// Capability document cache, shared by clones and filled on first use.
    capabilities: Arc<OnceLock<CapabilityDocument>>,
}

/// One direct download response, delivered in bounded chunks and verified
/// against the content reference carried by its grant.
///
/// Verification completes only when [`Self::next_chunk`] returns `None`.
/// A caller that stops earlier has received provisional bytes, just as with
/// any streaming read whose digest cannot be known until the end.
pub struct DirectDownloadStream {
    body: payload::PayloadStream,
    expected: ContentRef,
    path: AbsolutePath,
    /// The checksum the complete object must produce, folded so far. The
    /// grant's reference names it, so an object nobody hashed for us — a
    /// provider-assembled multipart, or a direct transfer described only by
    /// the provider's own CRC — is checked on the way past rather than
    /// taken on trust.
    digest: StreamingChecksum,
    size_bytes: u64,
    /// Offset this stream was opened at: zero for the whole object, and the
    /// length of what the caller already holds for a resumed download.
    resumed_from: u64,
    /// How much of that head start the caller has folded in. Nothing is
    /// read until it reaches `resumed_from`, because the verdict is over
    /// the whole object either way.
    prefix_folded: u64,
    finished: bool,
}

impl DirectDownloadStream {
    /// Hands the stream part of what the caller already holds, in order,
    /// from the object's first byte.
    ///
    /// A resumed download still checks the whole object's digest, so the
    /// bytes it will never receive have to be folded into the same digest as
    /// the ones it does. Feeding the wrong bytes fails the download at its
    /// end, which is right: the grant's reference is the authority on what
    /// the object holds, not the partial copy on the caller's disk.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
        self.prefix_folded = self.prefix_folded.saturating_add(bytes.len() as u64);
    }

    /// Returns the next response-body chunk, or `None` once the complete
    /// object has passed its declared-length and whole-file digest checks.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.prefix_folded != self.resumed_from {
            return Err(ClientError::Http(format!(
                "a download of `{}` resumed at offset {} was given {} bytes of what it \
                 skipped; verification covers the whole object, so all of them are needed \
                 first",
                self.path, self.resumed_from, self.prefix_folded
            )));
        }
        if self.finished {
            return Ok(None);
        }
        match self.body.next().await {
            Some(Ok(chunk)) => {
                self.size_bytes = self.size_bytes.saturating_add(chunk.len() as u64);
                if self.size_bytes > self.expected.size_bytes {
                    self.finished = true;
                    return Err(ClientError::Protocol(format!(
                        "direct download of `{}` sent more than the {} bytes the grant named",
                        self.path, self.expected.size_bytes
                    )));
                }
                self.digest.update(&chunk);
                Ok(Some(chunk))
            }
            Some(Err(error)) => {
                self.finished = true;
                Err(ClientError::Io(format!(
                    "read of `{}` failed: {error}",
                    self.path
                )))
            }
            None => {
                self.finished = true;
                if self.size_bytes != self.expected.size_bytes {
                    return Err(ClientError::Protocol(format!(
                        "direct download of `{}` ended after {} bytes, not the {} the grant named",
                        self.path, self.size_bytes, self.expected.size_bytes
                    )));
                }
                let expected = self.expected.verifiable_checksum();
                // Closing consumes the digest; `finished` is what keeps this
                // from running a second time over an empty one.
                let observed = std::mem::replace(
                    &mut self.digest,
                    StreamingChecksum::for_algorithm(expected.algorithm),
                )
                .finish();
                if observed != expected {
                    return Err(ClientError::Protocol(format!(
                        "direct download of `{}` produced {}:{}, not the {}:{} the grant named",
                        self.path,
                        observed.algorithm,
                        observed.value,
                        expected.algorithm,
                        expected.value
                    )));
                }
                Ok(None)
            }
        }
    }
}

/// State required to resume an interrupted direct multipart upload.
///
/// The server records session geometry but not completed parts, so the
/// client retains the upload id, part size, and accepted part metadata.
/// Missing entries are safely re-uploaded; incorrect entries cause completion
/// to fail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartUploadResume {
    pub upload_id: UploadId,
    /// The part size the session was opened with. A resumed upload must cut
    /// the payload exactly as the interrupted one did, or the parts it
    /// sends will not line up with the ones already there.
    pub part_size_bytes: u64,
    pub parts: Vec<CompletedUploadPart>,
}

/// Where a caller records a direct multipart upload as it happens, so a
/// later run can resume it.
///
/// Both methods are called on the thread driving the upload, between
/// network round trips, so an implementation that writes to disk is doing
/// it at the right moment: after the part it describes is durable, and
/// before the next one starts.
pub trait MultipartUploadJournal: Send + Sync {
    /// A session was opened, with the part geometry the server chose.
    fn began(&self, upload_id: &UploadId, part_size_bytes: u64);
    /// One part landed in object storage.
    fn part_completed(&self, part: &CompletedUploadPart);
}

/// How one upload survives an interruption: what an earlier run got
/// through, and where this one writes down what it gets through.
#[derive(Clone, Copy, Default)]
struct UploadContinuity<'a> {
    resume: Option<&'a MultipartUploadResume>,
    journal: Option<&'a dyn MultipartUploadJournal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StagedContent {
    content_ref: ContentRef,
    validated_content_token: Option<ValidatedContentToken>,
}

/// Upload path selected from the server capability document.
///
/// Direct PUT and direct multipart are independent capabilities; a
/// deployment may support either one without the other. Proxied upload is
/// used when no suitable direct path is available.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UploadTransport {
    /// Parts, straight to object storage, retried one at a time.
    Multipart,
    /// One whole-object request, straight to object storage, carrying the
    /// checksum this deployment's provider will enforce on it.
    DirectPut(ChecksumAlgorithm),
    /// Through the server, which writes the content object itself.
    Proxied,
}

/// A payload length this client has actually measured, as against a source's
/// file-metadata hint.
///
/// Only a measured length may end an upload. A file can change before it is
/// read, so a refusal built on metadata would turn a stale hint into a failed
/// upload. Threading the distinction through the type is what makes
/// [`ClientError::UploadTooLarge`] unconstructible from a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MeasuredBytes(u64);

/// No transport this deployment offers can carry a payload of the length it
/// was asked about.
struct NoTransportFits {
    /// The caps it passed, named so a caller can act on them.
    reason: String,
    /// The checksum a whole-object write would use, when one is offered at
    /// all — even though this payload is past its ceiling. A length that was
    /// only a hint routes through that transport anyway: its first pass
    /// measures the payload without sending a byte, and the refusal that may
    /// follow is built on what it found.
    direct_put_algorithm: Option<ChecksumAlgorithm>,
}

/// What a whole-object write sends, and where it reads it from.
///
/// The two arms are the two ways a payload's digest can already exist when
/// the session opens: the caller was holding the bytes, or a measuring pass
/// left them somewhere they can be read again. Neither holds the payload on
/// this client's account.
enum DirectPutBody<'a> {
    /// Bytes the caller already holds.
    Held(&'a [u8]),
    /// A measured payload, re-read from disk in pieces.
    Rewound(PayloadSource),
}

/// A measuring pass wrote or found the payload, and then could not read it
/// back — the upload cannot proceed without the bytes it just measured.
fn read_back_failed(error: std::io::Error) -> ClientError {
    ClientError::Io(format!("could not re-read the measured payload: {error}"))
}

/// Builds the presigned whole-object write both upload forms send.
fn presigned_put_request(access: &ObjectTransferAccess) -> Result<WireRequest> {
    let ObjectTransferAccess::PresignedUrl {
        method,
        url,
        headers,
        ..
    } = access;
    if method != "PUT" {
        return Err(ClientError::Protocol(format!(
            "unsupported presigned upload method `{method}`"
        )));
    }
    let mut request = WireRequest::presigned(reqwest::Method::PUT, url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    Ok(request)
}

/// What a multipart upload has to work with.
///
/// The two arms exist so that neither caller pays for the other's shape: a
/// caller holding its payload should not have it copied whole to be
/// uploaded, and a caller reading a stream cannot be asked for a length it
/// does not have. Past this point the upload cannot tell them apart.
enum MultipartPayload<'a> {
    /// A payload the caller already holds.
    Held(&'a [u8]),
    /// A payload read once, in pieces, as it is uploaded.
    Streamed(PayloadStream),
}

impl<'a> MultipartPayload<'a> {
    /// Binds the payload to the geometry the server chose.
    fn into_parts_of(self, part_bytes: usize) -> MultipartParts<'a> {
        match self {
            Self::Held(bytes) => MultipartParts::Held {
                bytes,
                offset: 0,
                part_bytes: part_bytes.max(1),
            },
            Self::Streamed(stream) => MultipartParts::Streamed(PartReader::new(stream, part_bytes)),
        }
    }
}

/// A payload cut into the parts it will be uploaded as.
enum MultipartParts<'a> {
    Held {
        bytes: &'a [u8],
        offset: usize,
        part_bytes: usize,
    },
    Streamed(PartReader),
}

impl MultipartParts<'_> {
    /// The next part, or `None` once the payload is spent. Both arms hand
    /// out one part's worth of bytes and no more.
    async fn next_part(&mut self) -> Result<Option<Bytes>> {
        match self {
            Self::Held {
                bytes,
                offset,
                part_bytes,
            } => {
                if *offset >= bytes.len() {
                    return Ok(None);
                }
                let end = bytes.len().min(*offset + *part_bytes);
                let part = Bytes::copy_from_slice(&bytes[*offset..end]);
                *offset = end;
                Ok(Some(part))
            }
            Self::Streamed(reader) => reader
                .next_part()
                .await
                .map_err(|error| ClientError::Io(format!("reading the payload failed: {error}"))),
        }
    }
}

/// One part waiting to be uploaded, with the checksum its URL is signed
/// against.
struct PendingPart {
    claim: UploadPartChecksumClaim,
    bytes: Bytes,
}

/// What one pass over a payload produced: the assembled object's length and
/// digest, and the parts it was written as.
struct UploadedObject {
    size_bytes: u64,
    crc64nvme: StorageChecksum,
    parts: Vec<CompletedUploadPart>,
}

/// Picks one part's authorization out of a signing response.
fn signed_access(signed: &[SignedUploadPart], part_number: u32) -> Result<ObjectTransferAccess> {
    signed
        .iter()
        .find(|part| part.part_number == part_number)
        .map(|part| part.access.clone())
        .ok_or_else(|| {
            ClientError::Protocol(format!(
                "server authorized no upload for part {part_number}"
            ))
        })
}

/// A begin-upload response that is not the transport the request asked for.
///
/// Which transport carries a payload is settled before the request goes
/// out, from what the deployment advertises, so a different mode coming
/// back is a broken server rather than something to fall back from.
fn negotiated_a_different_upload_mode() -> ClientError {
    ClientError::Protocol(
        "the server answered with a different upload mode than negotiated".to_owned(),
    )
}

/// A path qualified by namespace.
///
/// Both parts are validated at construction — [`NamespacePath::parse`] for
/// strings, [`NamespacePath::new`] for already-typed parts — so a value of
/// this type always names a well-formed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePath {
    namespace: NamespaceId,
    absolute_path: AbsolutePath,
}

impl Client {
    /// Creates a client, validating the config exactly as
    /// [`ClientConfig::load`] does — direct Rust construction cannot bypass
    /// validation.
    pub fn new(config: ClientConfig) -> Result<Self> {
        config.validate()?;
        let request_timeout = config.request_timeout_ms.map(Duration::from_millis);
        let mut builder = reqwest::Client::builder()
            // Bounds a stalled connection without cutting off a slow but
            // progressing transfer, which a whole-request deadline would.
            .read_timeout(IO_INACTIVITY_TIMEOUT)
            .connect_timeout(IO_INACTIVITY_TIMEOUT);
        if let Some(request_timeout) = request_timeout {
            builder = builder.timeout(request_timeout);
        }
        // Additive: the platform roots stay in place, so one configured
        // private CA does not cost this client every public one.
        for certificate in config.extra_root_certificates()? {
            builder = builder.add_root_certificate(certificate);
        }
        Ok(Self {
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            http: builder
                .build()
                .map_err(|err| ClientError::ConfigValidation {
                    field: "http_client",
                    reason: format!("failed to build: {err}"),
                })?,
            request_timeout,
            transport_retry_enabled: !config.disable_transient_retry,
            transport_retry: TransportRetryPolicy::DEFAULT,
            timer: Arc::new(StdMonotonicTimer::default()),
            capabilities: Arc::new(OnceLock::new()),
        })
    }

    /// Returns the server's capability document, fetched once and cached for
    /// the life of this client and its clones (API spec, "Capability
    /// discovery").
    ///
    /// The cache is what lets every upload consult the deployment's real
    /// limits instead of assuming any of them: the round trip is paid at
    /// most once per client, however many puts follow. It is never
    /// invalidated, so a long-lived embedder that needs to see a
    /// redeployment's new capabilities builds a new client. The CLI is
    /// one-shot, so its view is always fresh.
    ///
    /// Feature keys that are not parented by an advertised profile are
    /// dropped rather than trusted, per the spec's client guidance for
    /// malformed documents.
    pub async fn capabilities(&self) -> Result<CapabilityDocument> {
        if let Some(document) = self.capabilities.get() {
            return Ok(document.clone());
        }
        let url = format!("{}/v0/capabilities", self.base_url);
        let mut document: CapabilityDocument =
            self.request_json::<(), _>(self.get(&url), None).await?;
        document.retain_well_formed();
        // If a racing clone fetched first, keep its copy; both came from the
        // same server.
        let _ = self.capabilities.set(document);
        Ok(self
            .capabilities
            .get()
            .expect("capability cache was just filled")
            .clone())
    }

    pub async fn create_namespace(&self, namespace_id: &NamespaceId) -> Result<NamespaceSummary> {
        let url = format!("{}/v0/namespaces", self.base_url);
        // Namespace creation has no durable request identity to reconcile an ambiguous success.
        self.request_json_once::<_, NamespaceSummary>(
            self.post(&url),
            Some(&CreateNamespaceRequest {
                namespace_id: namespace_id.clone(),
            }),
        )
        .await
    }

    pub async fn namespace_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<NamespaceStatusResponse> {
        // Validated namespace ids are URL-safe by construction, like the
        // other parsed id segments interpolated into paths here and below.
        let url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        self.request_json::<(), NamespaceStatusResponse>(self.get(&url), None)
            .await
    }

    /// Deletes a namespace (feature `core.namespaces.delete`): terminal,
    /// and the id is permanently retired. Pass `expected_head_seq` to delete
    /// only if the namespace is still where you last observed it
    /// (`stale_head` on mismatch). Deleting an already-deleted namespace
    /// fails with `namespace_deleted`.
    pub async fn delete_namespace(
        &self,
        namespace_id: &NamespaceId,
        expected_head_seq: Option<ChangeSeq>,
    ) -> Result<DeleteNamespaceResponse> {
        let mut url = format!("{}/v0/namespaces/{namespace_id}", self.base_url);
        if let Some(expected) = expected_head_seq {
            url.push_str(&format!("?expected_head_seq={}", expected.0));
        }
        // The expected head is a precondition, not an idempotency key for an ambiguous delete.
        self.request_json_once::<(), DeleteNamespaceResponse>(self.delete(&url), None)
            .await
    }

    pub async fn fork_namespace(
        &self,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
    ) -> Result<NamespaceSummary> {
        let url = format!(
            "{}/v0/namespaces/{source_namespace_id}/forks",
            self.base_url
        );
        // Namespace forks have no durable request identity to replay after an ambiguous success.
        self.request_json_once::<_, NamespaceSummary>(
            self.post(&url),
            Some(&ForkNamespaceRequest {
                new_namespace_id: new_namespace_id.clone(),
            }),
        )
        .await
    }

    /// Lists a directory by aggregating every page into one response.
    ///
    /// Listing cursors tolerate commits landing mid-listing — each page
    /// resumes in name-key order against the head the server has loaded —
    /// so aggregation never restarts. The envelope's `head_seq` reports the
    /// newest head that served a page. Use
    /// [`Self::list_path_entries_page`] for page-level control. `options`
    /// selects the entry projection.
    pub async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let first_page = self
            .list_path_entries_page(spec, None, None, options)
            .await?;
        let mut envelope = ListPathEntriesResponse {
            namespace_id: first_page.namespace_id,
            absolute_path: first_page.absolute_path,
            head_seq: first_page.head_seq,
            entries: first_page.entries,
            next_cursor: None,
        };
        let mut next_cursor = first_page.next_cursor;
        while let Some(cursor) = next_cursor {
            let page = self
                .list_path_entries_page(spec, None, Some(&cursor), options)
                .await?;
            envelope.head_seq = envelope.head_seq.max(page.head_seq);
            envelope.entries.extend(page.entries);
            next_cursor = page.next_cursor;
        }
        // Pages arrive in canonical name-key order; concatenation preserves
        // it, so aggregation must not re-sort.
        Ok(envelope)
    }

    /// Lists one page of a directory, projecting what `options` asks for.
    pub async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
        options: &ListPathEntriesOptions,
    ) -> Result<ListPathEntriesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/list?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    /// Stats one path, projecting what `options` asks for.
    pub async fn stat_path(
        &self,
        spec: &NamespacePath,
        options: &StatPathOptions,
    ) -> Result<AuthoritativePathEntry> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_query_param(
            &mut url,
            &mut has_query,
            "include_attributes",
            &options.include_attributes.to_string(),
        );
        self.request_json::<(), _>(self.get(&url), None).await
    }

    pub async fn get_file_bytes(&self, spec: &NamespacePath) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_bytes(&url).await
    }

    pub async fn get_file_revision_bytes(
        &self,
        spec: &NamespacePath,
        revision_no: RevisionNo,
    ) -> Result<Vec<u8>> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/content?path={}&revision_no={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str()),
            revision_no.0
        );
        self.request_bytes(&url).await
    }

    /// Whether this deployment would refuse to proxy a file of this size
    /// but can authorize a direct read of it.
    ///
    /// The two halves are one question. Under the advertised proxy cap the
    /// proxied read is the simpler path and stays the default; over it the
    /// proxied read answers `content_too_large`, and a deployment that
    /// advertises `core.downloads.direct_get` can hand the object back
    /// instead — which is the whole point of the capability, because that
    /// same deployment is one that let a client create the object directly.
    ///
    /// A deployment that advertises no cap is left on the proxied path:
    /// nothing here knows it would refuse.
    pub async fn offers_direct_download(&self, size_bytes: u64) -> Result<bool> {
        let capabilities = self.capabilities().await?;
        Ok(capabilities.supports(FEATURE_DOWNLOADS_DIRECT_GET)
            && capabilities
                .limits
                .get(LIMIT_DOWNLOAD_MAX_CONTENT_BYTES)
                .is_some_and(|proxy_cap| size_bytes > *proxy_cap))
    }

    /// Asks for one short-lived capability to read a file's content object
    /// straight from the store.
    pub async fn begin_download(
        &self,
        spec: &NamespacePath,
        revision_no: Option<RevisionNo>,
    ) -> Result<BeginDownloadResponse> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/downloads",
            self.base_url,
            spec.namespace().as_str()
        );
        let request = match revision_no {
            Some(revision_no) => {
                BeginDownloadRequest::for_revision(spec.absolute_path().clone(), revision_no)
            }
            None => BeginDownloadRequest::for_path(spec.absolute_path().clone()),
        };
        // A grant creates nothing and names nothing new, so asking twice
        // costs two URLs and changes no state: this one may be resent.
        self.request_json::<_, BeginDownloadResponse>(self.post(&url), Some(&request))
            .await
    }

    /// Opens the response body authorized by a download grant as a bounded,
    /// verified stream.
    pub async fn open_direct_download(
        &self,
        download: &BeginDownloadResponse,
    ) -> Result<DirectDownloadStream> {
        self.open_direct_download_at(download, 0).await
    }

    /// Opens a download grant's body from `start_offset`, for a caller that
    /// already holds the bytes below it.
    ///
    /// The offset rides a `Range` header, which the presigned signature does
    /// not cover: one grant serves the whole object or any part of it, so a
    /// resumed download needs no different grant than a fresh one. The
    /// stream still reports on the whole object, so a nonzero offset obliges
    /// the caller to hand over what it holds through
    /// [`DirectDownloadStream::fold_resumed_prefix`] before driving it.
    pub async fn open_direct_download_at(
        &self,
        download: &BeginDownloadResponse,
        start_offset: u64,
    ) -> Result<DirectDownloadStream> {
        let ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            ..
        } = &download.access;
        if method != "GET" {
            return Err(ClientError::Protocol(format!(
                "unsupported presigned download method `{method}`"
            )));
        }
        if start_offset > download.content_ref.size_bytes {
            return Err(ClientError::Http(format!(
                "cannot resume a download of `{}` at offset {start_offset} of {} bytes",
                download.absolute_path, download.content_ref.size_bytes
            )));
        }
        let mut request = WireRequest::presigned(reqwest::Method::GET, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if start_offset > 0 {
            request = request.header("range", format!("bytes={start_offset}-"));
        }
        let body = self.call_for_response_stream(&request).await?;
        Ok(DirectDownloadStream {
            body,
            expected: download.content_ref.clone(),
            path: download.absolute_path.clone(),
            digest: StreamingChecksum::for_algorithm(
                download.content_ref.verifiable_checksum().algorithm,
            ),
            // The counter measures the whole object, not this response, so
            // the length check at the end lands where it always did.
            size_bytes: start_offset,
            resumed_from: start_offset,
            prefix_folded: 0,
            finished: false,
        })
    }

    /// Streams a granted object's bytes into `sink`, checking them against
    /// the reference the grant carried, and reports how many arrived.
    ///
    /// The payload is never held: each chunk is hashed and written as it
    /// arrives, so this costs one chunk of memory whatever the object's
    /// length. That is the entire reason the grant exists — a file past the
    /// deployment's proxy cap has no other way home.
    ///
    /// Verification is what keeps a direct read no weaker than a proxied
    /// one: the length always, and the whole-file SHA-256 whenever the
    /// reference carries one. A direct-multipart object carries none —
    /// nobody ever hashed it that way — and its length is then the whole
    /// check, exactly as it is for the server's own reads.
    ///
    /// A failure is reported *after* the sink has already received bytes,
    /// because that is the only order a streamed read allows. Callers must
    /// treat the sink as provisional until this returns: write to a
    /// temporary and install it on success.
    pub async fn download_via_presigned_url<W>(
        &self,
        download: &BeginDownloadResponse,
        sink: &mut W,
    ) -> Result<u64>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt as _;
        let path = &download.absolute_path;
        let mut download = self.open_direct_download(download).await?;
        let mut size_bytes = 0u64;
        while let Some(chunk) = download.next_chunk().await? {
            size_bytes += chunk.len() as u64;
            sink.write_all(&chunk)
                .await
                .map_err(|err| ClientError::Io(format!("write of `{path}` failed: {err}")))?;
        }
        sink.flush()
            .await
            .map_err(|err| ClientError::Io(format!("write of `{path}` failed: {err}")))?;
        Ok(size_bytes)
    }

    pub async fn list_file_revisions_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListFileRevisionsResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/revisions?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let mut has_query = true;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListFileRevisionsResponse>(self.get(&url), None)
            .await
    }

    pub async fn list_trash_page(
        &self,
        namespace_id: &NamespaceId,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListTrashResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/trash",
            self.base_url,
            namespace_id.as_str()
        );
        let mut has_query = false;
        append_optional_pagination_query(&mut url, &mut has_query, limit, cursor);
        self.request_json::<(), ListTrashResponse>(self.get(&url), None)
            .await
    }

    pub async fn health(&self) -> Result<()> {
        let url = format!("{}/health", self.base_url);
        self.call_with_transport_retry(&self.get(&url), None)
            .await?;
        Ok(())
    }

    pub async fn begin_upload(
        &self,
        namespace_id: &NamespaceId,
        request: &BeginUploadRequest,
    ) -> Result<BeginUploadResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/uploads", self.base_url);
        // Beginning an upload mints a new session id, so a resend could create a second session.
        self.request_json_once::<_, BeginUploadResponse>(self.post(&url), Some(request))
            .await
    }

    /// Starts a direct upload of bytes the caller already has.
    ///
    /// The claim is what the client can know about its own bytes; the
    /// server answers with the content object it minted for them, and that
    /// reference — not this claim — is what completion and the later commit
    /// name.
    pub async fn begin_direct_put(
        &self,
        namespace_id: &NamespaceId,
        claim: DirectPutContentClaim,
    ) -> Result<BeginUploadResponse> {
        self.begin_upload(
            namespace_id,
            &BeginUploadRequest::DirectPut { content: claim },
        )
        .await
    }

    /// Opens a direct multipart upload session.
    ///
    /// Nothing about the payload is declared here — not its length, not its
    /// digest — so one pass over the bytes is enough and a stream of unknown
    /// length can start uploading immediately. The server answers with the
    /// part geometry to cut to and nothing else: not the bucket, not the
    /// key, not the provider's upload id, and not the content identity,
    /// which it names back at completion.
    pub async fn begin_direct_multipart(
        &self,
        namespace_id: &NamespaceId,
        options: DirectMultipartUploadOptions,
    ) -> Result<BeginUploadResponse> {
        self.begin_upload(
            namespace_id,
            &BeginUploadRequest::DirectMultipart {
                multipart: Some(options),
            },
        )
        .await
    }

    /// Asks for one wave of checksum-bound part-upload capabilities.
    ///
    /// Asking again for a part already uploaded is how a client retries it:
    /// a repeated part is last-write-wins at the provider, and the object's
    /// checksum follows the bytes that stuck.
    pub async fn sign_upload_parts(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        parts: Vec<UploadPartChecksumClaim>,
    ) -> Result<SignUploadPartsResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
            self.base_url
        );
        // Signing writes nothing down, so asking twice costs two signatures
        // and changes nothing.
        self.request_json::<_, SignUploadPartsResponse>(
            self.post(&url),
            Some(&SignUploadPartsRequest { parts }),
        )
        .await
    }

    /// Uploads one part and reports what the provider recorded for it.
    ///
    /// The etag comes back to the caller rather than to the server: parts
    /// are the uploader's bookkeeping all the way to completion, exactly as
    /// they are in the provider's own multipart API.
    pub async fn upload_part_via_presigned_url(
        &self,
        part_number: u32,
        access: &ObjectTransferAccess,
        crc64nvme: String,
        bytes: Bytes,
    ) -> Result<CompletedUploadPart> {
        let ObjectTransferAccess::PresignedUrl {
            method,
            url,
            headers,
            ..
        } = access;
        if method != "PUT" {
            return Err(ClientError::Protocol(format!(
                "unsupported presigned part method `{method}`"
            )));
        }
        let mut request = WireRequest::presigned(reqwest::Method::PUT, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        // A part upload is safe to repeat: it is not create-only, the
        // provider takes the last write, and the checksum rides the
        // signature either way.
        let response = self
            .call_content_with_transport_retry_headers(&request, Some(&bytes))
            .await?;
        let etag = response
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                ClientError::Protocol(format!("part {part_number} upload returned no etag"))
            })?
            .to_owned();
        Ok(CompletedUploadPart {
            part_number,
            etag,
            crc64nvme,
        })
    }

    pub async fn upload_via_presigned_url(
        &self,
        access: &ObjectTransferAccess,
        bytes: &[u8],
    ) -> Result<()> {
        let request = presigned_put_request(access)?;
        // A successful create-only PUT may replay as a provider precondition error, not success.
        self.call_once(&request, Some(&Bytes::copy_from_slice(bytes)))
            .await
            .map(|_| ())
    }

    /// Writes one whole object to a presigned URL from a source read in
    /// pieces, so the payload crosses the network without ever being held.
    ///
    /// Like the buffered form this never retries: a create-only PUT that
    /// succeeded can come back as a provider precondition error on a second
    /// attempt, and a source is consumed by the attempt that reads it.
    pub async fn upload_streamed_via_presigned_url(
        &self,
        access: &ObjectTransferAccess,
        source: PayloadSource,
    ) -> Result<()> {
        let request = presigned_put_request(access)?;
        let (body, size_bytes) = source.into_stream();
        self.call_streamed_once(&request, body, size_bytes)
            .await
            .map(|_| ())
    }

    pub async fn upload_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        bytes: &[u8],
    ) -> Result<UploadContentResponse> {
        let request = self.upload_content_request(namespace_id, upload_id);
        // Proxied uploads are the request most likely to hit the server's
        // concurrency cap; staging the same bytes again is idempotent.
        let response = self
            .call_content_with_transport_retry(&request, Some(&Bytes::copy_from_slice(bytes)))
            .await?;
        serde_json::from_slice(&response).map_err(|err| ClientError::Json(err.to_string()))
    }

    /// Stages a payload that arrives in pieces, forwarding it to the server
    /// as it is read.
    ///
    /// This is [`Self::upload_content`] for a caller that does not hold its
    /// bytes: the payload crosses the client in bounded chunks and the
    /// server hashes it as it forwards it on, so neither side ever holds the
    /// object. A source whose length is unknown is sent with chunked
    /// transfer encoding, and the server's own limit is what bounds it.
    ///
    /// Unlike the buffered call this one never resends: a stream is consumed
    /// by the attempt that reads it, so a failure here is the caller's to
    /// handle with a fresh source.
    pub async fn upload_streamed_content(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        source: PayloadSource,
    ) -> Result<UploadContentResponse> {
        let request = self.upload_content_request(namespace_id, upload_id);
        let (stream, size_bytes) = source.into_stream();
        let response = self
            .call_streamed_once(&request, stream, size_bytes)
            .await?;
        serde_json::from_slice(&response).map_err(|err| ClientError::Json(err.to_string()))
    }

    fn upload_content_request(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> WireRequest {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            self.base_url
        );
        self.put(&url)
            .header("content-type", "application/octet-stream")
    }

    /// Ends an open upload session and deletes the object it was writing.
    ///
    /// Repeating it succeeds and reports the abort that stands. This is what
    /// a one-pass upload does when its source fails partway: the session it
    /// opened must not be left holding a half-written object.
    pub async fn abort_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<AbortUploadResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
            self.base_url
        );
        // Aborting is idempotent: a repeat reports the abort that stands.
        self.request_json::<(), AbortUploadResponse>(self.post(&url), None)
            .await
    }

    /// Reads one upload session back.
    ///
    /// A completed session answers with the exact content reference it
    /// settled on and a freshly minted validation token, so a caller that
    /// lost its completion response — or the whole process — can commit
    /// that content without re-uploading a byte. The upload id is the only
    /// thing it has to have kept.
    pub async fn read_upload_status(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
    ) -> Result<UploadStatusResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}",
            self.base_url
        );
        self.request_json::<(), UploadStatusResponse>(self.get(&url), None)
            .await
    }

    pub async fn complete_upload(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        request: &CompleteUploadRequest,
    ) -> Result<CompleteUploadResponse> {
        let url = format!(
            "{}/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            self.base_url
        );
        // The durable completed-session record replays an identical completion without new effect.
        self.request_json::<_, CompleteUploadResponse>(self.post(&url), Some(request))
            .await
    }

    pub async fn list_changes(
        &self,
        namespace_id: &NamespaceId,
        after_seq: ChangeSeq,
        limit: Option<u32>,
    ) -> Result<ChangesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{namespace_id}/changes?after_seq={}",
            self.base_url, after_seq.0
        );
        if let Some(limit) = limit {
            url.push_str(&format!("&limit={limit}"));
        }
        self.request_json::<(), ChangesResponse>(self.get(&url), None)
            .await
    }

    /// Creates or reuses a named, user-owned checkpoint pinning the
    /// namespace's current view (admin plane). This is a maintenance
    /// operation, not a file mutation. The record is a garbage-collection
    /// root until released or expired.
    pub async fn create_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        request: &CreateCheckpointRequest,
    ) -> Result<CreateCheckpointResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Lists the namespace's active checkpoint records, oldest first (admin
    /// plane).
    ///
    /// A checkpoint name is a label rather than a key, so this is how a pin
    /// is found again once its creation response is gone. An expired record
    /// that no collection pass has released yet is still listed, with its
    /// expiry in the entry.
    pub async fn list_checkpoints(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<ListCheckpointsResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints",
            self.base_url
        );
        self.request_json::<(), ListCheckpointsResponse>(self.get(&url), None)
            .await
    }

    /// Releases a user-owned checkpoint pin by id (admin plane). Idempotent:
    /// releasing an already-released or reaped record succeeds.
    pub async fn release_checkpoint(
        &self,
        namespace_id: &NamespaceId,
        checkpoint_id: &CheckpointId,
    ) -> Result<ReleaseCheckpointResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/checkpoints/{checkpoint_id}/release",
            self.base_url
        );
        self.request_json::<(), ReleaseCheckpointResponse>(self.post(&url), None)
            .await
    }

    /// Runs one bounded maintenance step against a namespace (admin plane).
    ///
    /// The request selects the actions by naming them, and a request that
    /// names none is rejected. Absent overrides inside a selected action use
    /// the server's defaults.
    pub async fn maintenance_step(
        &self,
        namespace_id: &NamespaceId,
        request: &MaintenanceStepRequest,
    ) -> Result<MaintenanceStepResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/maintenance/step",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Proves the server's backing store honours the object-store contract
    /// LoonFS depends on (admin plane).
    ///
    /// The probe writes and deletes objects under a scratch prefix, so it
    /// runs only when asked. A store that fails a check answers with that
    /// check reported failed rather than with an error: the probe ran, and
    /// the answer is that the store is wrong.
    pub async fn probe_store(&self, request: &StoreProbeRequest) -> Result<StoreProbeResponse> {
        let url = format!("{}/v0/admin/store/probe", self.base_url);
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Content search over the namespace's grep index (query plane).
    /// Gate on the `query.grep` capability before calling against unknown
    /// deployments; the namespace must also have a materialized steady-state
    /// grep root or the server answers `not_supported`.
    pub async fn grep(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepRequest,
    ) -> Result<GrepResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/query/grep", self.base_url);
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Reads the namespace's grep-index lifecycle (admin plane): disabled,
    /// backfilling toward a captured sequence, or steady at a watermark.
    /// One grep root read on the server, with no side effects.
    pub async fn grep_index_status(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<GrepIndexStatusResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index",
            self.base_url
        );
        self.request_json::<(), GrepIndexStatusResponse>(self.get(&url), None)
            .await
    }

    /// Enables the namespace's grep root (admin plane); embedded mode starts
    /// that namespace's event-driven backfill. Idempotent.
    pub async fn enable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<EnableGrepIndexResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/enable",
            self.base_url
        );
        self.request_json::<(), EnableGrepIndexResponse>(self.post(&url), None)
            .await
    }

    /// Disables the namespace's grep root (admin plane); garbage collection
    /// reclaims the segments. Idempotent.
    pub async fn disable_grep_index(
        &self,
        namespace_id: &NamespaceId,
    ) -> Result<DisableGrepIndexResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/disable",
            self.base_url
        );
        self.request_json::<(), DisableGrepIndexResponse>(self.post(&url), None)
            .await
    }

    /// Runs one explicit grep-index garbage-collection pass for a namespace.
    ///
    /// `max_objects` bounds the reads the pass spends; when keys remain the
    /// response carries a `next_cursor` to resume from.
    pub async fn gc_grep_index(
        &self,
        namespace_id: &NamespaceId,
        request: &GrepGcRequest,
    ) -> Result<GrepGcResponse> {
        let url = format!(
            "{}/v0/admin/namespaces/{namespace_id}/grep/index/gc",
            self.base_url
        );
        self.request_json(self.post(&url), Some(request)).await
    }

    /// Applies one commit: its operations land together, in order, under
    /// one commit id.
    ///
    /// The convenience methods below are the one-element case of this call.
    /// Operations that introduce new external content carry their proofs in
    /// the request's `content_tokens`; stage the bytes with the upload
    /// methods first.
    pub async fn commit(
        &self,
        namespace_id: &NamespaceId,
        request: &CommitRequest,
    ) -> Result<ApiCommitResponse> {
        let url = format!("{}/v0/namespaces/{namespace_id}/commits", self.base_url);
        // The request's commit id resolves an ambiguous resend through a durable receipt.
        self.request_json::<_, ApiCommitResponse>(self.post(&url), Some(request))
            .await
    }

    /// Makes bytes durable, choosing the transport the payload and the
    /// deployment allow.
    ///
    /// A large payload goes straight to object storage in parallel parts
    /// where the server can authorize that; everything else goes through
    /// the server. Either way the caller gets back one content reference
    /// plus the receipt that admits it at commit.
    async fn stage_bytes_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        // A payload already in hand has been measured by definition, so this
        // may refuse.
        match self
            .transport_for_measured(MeasuredBytes(bytes.len() as u64))
            .await?
        {
            UploadTransport::Multipart => {
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Held(bytes),
                    UploadContinuity::default(),
                )
                .await
            }
            UploadTransport::DirectPut(algorithm) => {
                self.stage_via_direct_put(namespace_id, bytes, algorithm)
                    .await
            }
            UploadTransport::Proxied => self.stage_bytes_via_server(namespace_id, bytes).await,
        }
    }

    /// Makes a streamed payload durable, choosing the transport the source
    /// and the deployment allow.
    ///
    /// Either way the source is read once, forward, and never held whole.
    /// A deployment that can authorize direct part uploads gets them; one
    /// that cannot receives the same source as a streaming request body.
    async fn stage_source_as_content_ref(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // The length a source declares is a hint, so it only routes; it
        // never refuses. Both streaming transports measure the payload as
        // they send it, and the one that cannot — a whole-object write —
        // measures it in the pass it has to make anyway.
        match self.provisional_transport(source.size_bytes()).await? {
            UploadTransport::Multipart => {
                let (stream, _) = source.into_stream();
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Streamed(stream),
                    continuity,
                )
                .await
            }
            UploadTransport::DirectPut(algorithm) => {
                self.stage_source_via_direct_put(namespace_id, source, algorithm, continuity)
                    .await
            }
            // Nothing to resume off this path: a proxied upload is one
            // request with no session behind it, so there are no parts to
            // have landed.
            UploadTransport::Proxied => self.stage_source_via_server(namespace_id, source).await,
        }
    }

    /// Stages a streamed payload through one presigned whole-object write.
    ///
    /// Two passes, neither of which holds the payload. The first folds the
    /// deployment's own checksum and counts the bytes, spooling them only if
    /// the source cannot be read again; the second streams them into the
    /// signed request. Between the two, what the first pass *measured*
    /// re-decides the transport — the hint that routed here may have been
    /// wrong in either direction, and only now is there a length worth
    /// refusing on.
    async fn stage_source_via_direct_put(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
        algorithm: ChecksumAlgorithm,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        let mut digest = StreamingChecksum::for_algorithm(algorithm);
        let measured = source
            .measure(&mut digest)
            .await
            .map_err(|error| ClientError::Io(error.to_string()))?;
        let size = MeasuredBytes(measured.size_bytes());
        let storage_checksum = digest.finish();

        match self.transport_for_measured(size).await? {
            UploadTransport::DirectPut(_) => {
                self.direct_put_transfer(
                    namespace_id,
                    size,
                    storage_checksum,
                    DirectPutBody::Rewound(measured.reread().await.map_err(read_back_failed)?),
                )
                .await
            }
            // Reachable only if the deployment's answer changed under us;
            // the rewound payload serves either transport unchanged.
            UploadTransport::Multipart => {
                let (stream, _) = measured
                    .reread()
                    .await
                    .map_err(read_back_failed)?
                    .into_stream();
                self.stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Streamed(stream),
                    continuity,
                )
                .await
            }
            UploadTransport::Proxied => {
                self.stage_source_via_server(
                    namespace_id,
                    measured.reread().await.map_err(read_back_failed)?,
                )
                .await
            }
        }
    }

    /// Routes a payload whose length is only a hint. Never refuses.
    ///
    /// A hint that says nothing fits may simply be wrong, and only a
    /// measured length may end an upload. So where a whole-object write is
    /// on offer this takes it — that transport's first pass measures the
    /// payload without sending a byte, and any refusal comes after. Where
    /// one is not, the payload streams to the service, which measures it as
    /// it receives it and answers `content_too_large` if it must.
    async fn provisional_transport(&self, size_hint: Option<u64>) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Ok(match Self::transport_for(&capabilities, size_hint) {
            Ok(transport) => transport,
            Err(no_fit) => no_fit
                .direct_put_algorithm
                .map_or(UploadTransport::Proxied, UploadTransport::DirectPut),
        })
    }

    /// Routes a payload whose length this client measured, refusing when no
    /// transport can carry it.
    ///
    /// The refusal names the caps it passed. It is raised before any byte
    /// moves rather than after the capped proxy has read and rejected the
    /// whole payload.
    async fn transport_for_measured(&self, size: MeasuredBytes) -> Result<UploadTransport> {
        let capabilities = self.capabilities().await?;
        Self::transport_for(&capabilities, Some(size.0)).map_err(|no_fit| {
            ClientError::UploadTooLarge {
                size_bytes: size.0,
                reason: no_fit.reason,
            }
        })
    }

    /// The best transport for a payload of this length, against what the
    /// deployment actually advertises.
    ///
    /// Parts win wherever they are offered and the payload is worth cutting:
    /// a part is retried on its own, and nothing has to know the length in
    /// advance. A whole-object write is the next rung — it exists for a
    /// provider that can sign a write but has no multipart API to open — and
    /// it earns its extra pass over the payload only when the payload is
    /// large, or when the service would not take it anyway. Everything else
    /// goes through the service.
    ///
    /// Every limit here is read from the capability document. Nothing is
    /// assumed about how the deployment is configured, which is why the
    /// document is fetched even for a small payload: the fetch happens once
    /// per client and is cached, so the round trip is paid at most once.
    fn transport_for(
        capabilities: &CapabilityDocument,
        size_bytes: Option<u64>,
    ) -> std::result::Result<UploadTransport, NoTransportFits> {
        // Below one part there is nothing to cut, and a length nobody knows
        // cannot rule parts out.
        let worth_cutting = size_bytes.is_none_or(|size| size >= STREAMING_PUT_MIN_BYTES);
        if worth_cutting && capabilities.supports(FEATURE_UPLOADS_DIRECT_MULTIPART) {
            return Ok(UploadTransport::Multipart);
        }
        let direct_put_algorithm = capabilities.direct_put_checksum_algorithm();
        // A length nobody knows cannot be checked against a cap. Where a
        // whole-object write is on offer that is no reason to give up on
        // it: the transport's first pass measures the payload without
        // sending a byte, and that measurement re-decides the transport
        // before anything moves. Falling straight to the service here would
        // stream an unmeasured payload at a cap it may not fit, and where the
        // deployment signs no multipart it would be discarding the only rung
        // that could have carried it.
        let Some(size_bytes) = size_bytes else {
            return Ok(
                direct_put_algorithm.map_or(UploadTransport::Proxied, UploadTransport::DirectPut)
            );
        };
        let proxy_cap = capabilities
            .limits
            .get(LIMIT_UPLOAD_MAX_CONTENT_BYTES)
            .copied();
        let fits_proxy = proxy_cap.is_none_or(|cap| size_bytes <= cap);
        let direct_put_cap = capabilities.direct_put_max_content_bytes();
        if worth_cutting || !fits_proxy {
            if let Some(algorithm) = direct_put_algorithm {
                if direct_put_cap.is_none_or(|cap| size_bytes <= cap) {
                    return Ok(UploadTransport::DirectPut(algorithm));
                }
            }
        }
        if fits_proxy {
            return Ok(UploadTransport::Proxied);
        }
        // Only a deployment that advertises a proxy cap reaches here.
        let proxy_cap = proxy_cap.unwrap_or(size_bytes);
        Err(NoTransportFits {
            reason: match direct_put_cap {
                Some(cap) => format!(
                    "the service takes at most {proxy_cap} bytes \
                     (`{LIMIT_UPLOAD_MAX_CONTENT_BYTES}`), `direct_put` at most {cap} \
                     (`{LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES}`), and \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` is not advertised"
                ),
                None => format!(
                    "the service takes at most {proxy_cap} bytes \
                     (`{LIMIT_UPLOAD_MAX_CONTENT_BYTES}`), and neither \
                     `{FEATURE_UPLOADS_DIRECT_MULTIPART}` nor a usable `direct_put` checksum is \
                     advertised"
                ),
            },
            direct_put_algorithm,
        })
    }

    /// Uploads one whole object the caller already holds straight to object
    /// storage.
    ///
    /// The digest is signed into the write the provider will enforce, so it
    /// exists before the session does. A payload already in hand needs no
    /// second pass to produce it: it is folded here, over the bytes the
    /// caller is holding anyway.
    async fn stage_via_direct_put(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
        algorithm: ChecksumAlgorithm,
    ) -> Result<StagedContent> {
        let mut digest = StreamingChecksum::for_algorithm(algorithm);
        digest.update(bytes);
        self.direct_put_transfer(
            namespace_id,
            MeasuredBytes(bytes.len() as u64),
            digest.finish(),
            DirectPutBody::Held(bytes),
        )
        .await
    }

    /// Opens a `direct_put` session, writes its object, and completes it.
    ///
    /// The claim is the measured length and the digest folded over the same
    /// bytes; nothing here is taken from a source's declared length. A
    /// session whose transfer fails is aborted rather than left open — it
    /// owns an object nothing will finish writing, exactly as a multipart
    /// session does.
    async fn direct_put_transfer(
        &self,
        namespace_id: &NamespaceId,
        size: MeasuredBytes,
        storage_checksum: StorageChecksum,
        body: DirectPutBody<'_>,
    ) -> Result<StagedContent> {
        let begin = self
            .begin_direct_put(
                namespace_id,
                DirectPutContentClaim {
                    size_bytes: size.0,
                    storage_checksum,
                },
            )
            .await?;
        let BeginUploadResponse::DirectPut {
            upload_id,
            direct_put,
            ..
        } = begin
        else {
            return Err(negotiated_a_different_upload_mode());
        };
        let written = match body {
            DirectPutBody::Held(bytes) => {
                self.upload_via_presigned_url(&direct_put.access, bytes)
                    .await
            }
            DirectPutBody::Rewound(source) => {
                self.upload_streamed_via_presigned_url(&direct_put.access, source)
                    .await
            }
        };
        if let Err(error) = written {
            let _ = self.abort_upload(namespace_id, &upload_id).await;
            return Err(error);
        }
        let response = self
            .complete_upload(
                namespace_id,
                &upload_id,
                &CompleteUploadRequest::for_content_ref(direct_put.content_ref),
            )
            .await?;
        Ok(Self::staged_from_completion(response))
    }

    /// Uploads one object straight to object storage in bounded waves of
    /// parts.
    ///
    /// The whole-object checksum is folded part by part as the payload is
    /// cut, so the same pass that produces what the provider enforces on
    /// each part also produces what completion verifies the assembly
    /// against — and because the claim is only needed at completion, that
    /// one pass is the only pass over the bytes anyone has to make. Nothing
    /// here needs the payload's length in advance, which is what lets a
    /// stream with no length take this path unchanged.
    ///
    /// A session that fails partway is aborted rather than left open.
    async fn stage_via_multipart(
        &self,
        namespace_id: &NamespaceId,
        payload: MultipartPayload<'_>,
        continuity: UploadContinuity<'_>,
    ) -> Result<StagedContent> {
        // A resumed upload rejoins the session a previous run opened, at the
        // part size that run was given. Asking for a new one would open a
        // second session and orphan the parts already in object storage.
        let (upload_id, part_size_bytes) = match continuity.resume {
            Some(resume) => (resume.upload_id.clone(), resume.part_size_bytes),
            None => {
                let begin = self
                    .begin_direct_multipart(namespace_id, DirectMultipartUploadOptions::default())
                    .await?;
                let BeginUploadResponse::DirectMultipart {
                    upload_id,
                    direct_multipart,
                    ..
                } = begin
                else {
                    return Err(negotiated_a_different_upload_mode());
                };
                if let Some(journal) = continuity.journal {
                    journal.began(&upload_id, direct_multipart.part_size_bytes);
                }
                (upload_id, direct_multipart.part_size_bytes)
            }
        };
        let uploaded = self
            .upload_every_part(
                namespace_id,
                &upload_id,
                payload,
                part_size_bytes,
                continuity,
            )
            .await;
        let uploaded = match uploaded {
            Ok(uploaded) => uploaded,
            Err(error) => {
                // The session owns an object this upload will never finish
                // writing. Ending it is best-effort: the original failure is
                // what the caller needs to see, and abandoned sessions are
                // collected either way.
                let _ = self.abort_upload(namespace_id, &upload_id).await;
                return Err(error);
            }
        };
        if uploaded.parts.is_empty() {
            // The source was empty, and a provider has no empty assembly to
            // make. The payload is nothing, so staging it costs nothing.
            let _ = self.abort_upload(namespace_id, &upload_id).await;
            return self.stage_bytes_via_server(namespace_id, &[]).await;
        }

        let response = self
            .complete_upload(
                namespace_id,
                &upload_id,
                &CompleteUploadRequest::for_multipart(
                    DirectMultipartContentClaim {
                        size_bytes: uploaded.size_bytes,
                        crc64nvme: uploaded.crc64nvme.value,
                    },
                    uploaded.parts,
                ),
            )
            .await?;
        Ok(Self::staged_from_completion(response))
    }

    /// Cuts the payload into parts and uploads them, holding at most
    /// [`DIRECT_MULTIPART_PARTS_IN_FLIGHT`] of them at a time.
    ///
    /// The window is the memory bound: each in-flight part holds its bytes,
    /// and nothing outside the window does. One wave asks for its part URLs
    /// in a single request, uploads them together, and only then reads the
    /// next wave — so the payload's length never enters into how much of it
    /// is resident.
    /// A resumed upload still reads every byte: the whole-object checksum
    /// completion verifies the assembly against is folded over the payload
    /// in one forward pass, so a part already in object storage is cut,
    /// folded, and then let go rather than sent again. What resuming saves
    /// is the network, which is the part that was expensive.
    async fn upload_every_part(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        payload: MultipartPayload<'_>,
        part_size_bytes: u64,
        continuity: UploadContinuity<'_>,
    ) -> Result<UploadedObject> {
        let part_size = usize::try_from(part_size_bytes).map_err(|_| {
            ClientError::Protocol("part size does not fit this platform".to_owned())
        })?;
        let landed = continuity
            .resume
            .map_or::<&[CompletedUploadPart], _>(&[], |resume| &resume.parts);
        let mut source = payload.into_parts_of(part_size);
        let mut whole_object = Crc64Nvme::new();
        let mut size_bytes = 0u64;
        let mut parts = Vec::new();
        let mut next_part_number = 1u32;

        loop {
            let mut wave = Vec::with_capacity(DIRECT_MULTIPART_PARTS_IN_FLIGHT);
            let mut source_ended = false;
            while wave.len() < DIRECT_MULTIPART_PARTS_IN_FLIGHT {
                let Some(bytes) = source.next_part().await? else {
                    source_ended = true;
                    break;
                };
                whole_object.update(&bytes);
                size_bytes += bytes.len() as u64;
                let part_number = next_part_number;
                next_part_number += 1;
                if let Some(landed) = landed.iter().find(|part| part.part_number == part_number) {
                    parts.push(landed.clone());
                    continue;
                }
                wave.push(PendingPart {
                    claim: UploadPartChecksumClaim {
                        part_number,
                        crc64nvme: StorageChecksum::crc64nvme(&bytes).value,
                    },
                    bytes,
                });
            }
            if !wave.is_empty() {
                let uploaded = self.upload_wave(namespace_id, upload_id, wave).await?;
                if let Some(journal) = continuity.journal {
                    for part in &uploaded {
                        journal.part_completed(part);
                    }
                }
                parts.extend(uploaded);
            }
            if source_ended {
                break;
            }
        }

        parts.sort_by_key(|part| part.part_number);
        Ok(UploadedObject {
            size_bytes,
            crc64nvme: whole_object.finish(),
            parts,
        })
    }

    /// Authorizes and uploads one wave of parts together.
    async fn upload_wave(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        wave: Vec<PendingPart>,
    ) -> Result<Vec<CompletedUploadPart>> {
        let claims = wave.iter().map(|part| part.claim.clone()).collect();
        let signed = self
            .sign_upload_parts(namespace_id, upload_id, claims)
            .await?;
        let authorized = wave
            .into_iter()
            .map(|part| {
                let access = signed_access(&signed.parts, part.claim.part_number)?;
                Ok((part, access))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut in_flight = tokio::task::JoinSet::new();
        for (part, access) in authorized {
            let client = self.clone();
            let namespace_id = namespace_id.clone();
            let upload_id = upload_id.clone();
            in_flight.spawn(async move {
                client
                    .upload_one_part(&namespace_id, &upload_id, part, access)
                    .await
            });
        }
        let mut uploaded = Vec::new();
        let mut failure = None;
        while let Some(joined) = in_flight.join_next().await {
            match joined.map_err(|err| ClientError::Io(format!("part upload task failed: {err}"))) {
                // Every task is drained before the first failure surfaces,
                // so no part upload outlives the wave that started it.
                Ok(Ok(part)) => uploaded.push(part),
                Ok(Err(error)) | Err(error) => failure = failure.or(Some(error)),
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(uploaded),
        }
    }

    /// Uploads one part, re-asking for its URL if the upload fails.
    ///
    /// Re-asking is the retry: a part's signature is the first thing about
    /// it that goes stale, and a repeated part is last-write-wins at the
    /// provider, so nothing is lost by writing it again.
    async fn upload_one_part(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        part: PendingPart,
        mut access: ObjectTransferAccess,
    ) -> Result<CompletedUploadPart> {
        let part_number = part.claim.part_number;
        for attempt in 1..DIRECT_MULTIPART_PART_ATTEMPTS {
            match self
                .upload_part_via_presigned_url(
                    part_number,
                    &access,
                    part.claim.crc64nvme.clone(),
                    part.bytes.clone(),
                )
                .await
            {
                Ok(uploaded) => return Ok(uploaded),
                Err(error) => {
                    tracing::warn!(
                        part_number,
                        attempt,
                        error = %error,
                        "part upload failed; refreshing authorization before retry"
                    );
                    let signed = self
                        .sign_upload_parts(namespace_id, upload_id, vec![part.claim.clone()])
                        .await?;
                    access = signed_access(&signed.parts, part_number)?;
                }
            }
        }
        self.upload_part_via_presigned_url(part_number, &access, part.claim.crc64nvme, part.bytes)
            .await
    }

    async fn stage_bytes_via_server(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        let upload = self
            .begin_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
            .await?;
        let staged = self
            .upload_content(namespace_id, upload.upload_id(), bytes)
            .await?;
        self.complete_staged(namespace_id, upload.upload_id(), staged)
            .await
    }

    /// Stages a streamed payload through the server, which hashes it as it
    /// forwards it on.
    ///
    /// The session is aborted if the transfer fails, for the same reason a
    /// multipart session is: it owns an object nothing will finish writing.
    async fn stage_source_via_server(
        &self,
        namespace_id: &NamespaceId,
        source: PayloadSource,
    ) -> Result<StagedContent> {
        let upload = self
            .begin_upload(namespace_id, &BeginUploadRequest::ServiceProxied {})
            .await?;
        let staged = self
            .upload_streamed_content(namespace_id, upload.upload_id(), source)
            .await;
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = self.abort_upload(namespace_id, upload.upload_id()).await;
                return Err(error);
            }
        };
        self.complete_staged(namespace_id, upload.upload_id(), staged)
            .await
    }

    async fn complete_staged(
        &self,
        namespace_id: &NamespaceId,
        upload_id: &UploadId,
        staged: UploadContentResponse,
    ) -> Result<StagedContent> {
        let response = self
            .complete_upload(
                namespace_id,
                upload_id,
                &CompleteUploadRequest::for_content_ref(staged.content_ref),
            )
            .await?;
        Ok(Self::staged_from_completion(response))
    }

    fn staged_from_completion(response: CompleteUploadResponse) -> StagedContent {
        let validated_content_token =
            response
                .validated_content_token
                .map(|token| ValidatedContentToken {
                    content_ref: response.content_ref.clone(),
                    token,
                });
        StagedContent {
            content_ref: response.content_ref,
            validated_content_token,
        }
    }

    /// Uploads bytes and commits them at a path.
    ///
    /// Re-running this with a `commit_id` that already committed is safe
    /// when the request is the same. A commit's identity names *which*
    /// content object it wrote, and a re-run necessarily uploads a fresh
    /// one, so the server sees a different commit and reports
    /// `commit_id_reuse_conflict`. This resolves that the only honest way
    /// available: it reads back what the commit id actually committed and
    /// compares it against the request just made. The same content under
    /// the same message means the operation had already succeeded and the
    /// original answer is returned; anything else — different bytes or a
    /// different message — surfaces the conflict.
    ///
    /// The freshly uploaded duplicate object is then referenced by nothing.
    /// That is by design, not a leak: content garbage collection reclaims an
    /// unreferenced completed upload once its grace passes.
    pub async fn put_file_bytes(
        &self,
        spec: &NamespacePath,
        bytes: &[u8],
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        let staged = self
            .stage_bytes_as_content_ref(spec.namespace(), bytes)
            .await?;
        self.commit_staged_file(spec, staged, options, ContentEvidence::Bytes(bytes))
            .await
    }

    /// Uploads a payload read once from its source and commits it at a path.
    ///
    /// This is [`Self::put_file_bytes`] for a caller that should not hold
    /// its payload: the source is read forward in bounded pieces, hashed as
    /// it goes, and never assembled. What the memory costs is the transport's
    /// window and nothing about the payload's size — so a file larger than
    /// this process could hold, or a pipe whose length nobody knows, both
    /// upload the same way.
    ///
    /// Where the deployment authorizes direct part uploads the payload goes
    /// straight to object storage in parts, and otherwise it streams through
    /// the server. One bound is worth knowing on the direct path: a provider
    /// assembles at most 10,000 parts, so a session carries at most
    /// `part_size_bytes × 10_000` and a longer payload is refused when it
    /// asks to authorize the part past that ceiling. A caller that knows its
    /// payload is enormous should not be taking the default geometry.
    ///
    /// Retrying with a `commit_id` that already committed is safe here in
    /// the same way it is for [`Self::put_file_bytes`], and by the same
    /// evidence: this pass measured the payload's length and folded its
    /// digest, which is what the reconciliation compares. The digest is
    /// CRC-64/NVME on the direct path, because that is what a provider
    /// computes over an assembled object.
    pub async fn put_file_stream(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        self.put_file_stream_continuing(spec, source, options, UploadContinuity::default())
            .await
    }

    /// [`Self::put_file_stream`] for a caller that intends to survive an
    /// interruption.
    ///
    /// `journal` is told the session's id and geometry when it opens and
    /// told every part as it lands; `resume` hands back what an earlier run
    /// of the same upload recorded, so this one sends only the parts still
    /// missing. Both apply to the direct multipart transport alone: a
    /// proxied upload is a single request with no session behind it and
    /// nothing to resume from, and it ignores them.
    ///
    /// The source is read from its first byte either way. That is not
    /// waste — the checksum the assembly is verified against covers the
    /// whole object, so every byte has to be folded whether or not it also
    /// has to be sent — and it means the source has to be one that can be
    /// opened again. A pipe cannot, which is why nothing that reads one
    /// should be calling this.
    pub async fn put_file_stream_resumable(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        journal: &dyn MultipartUploadJournal,
        resume: Option<&MultipartUploadResume>,
    ) -> Result<ApiCommitResponse> {
        self.put_file_stream_continuing(
            spec,
            source,
            options,
            UploadContinuity {
                resume,
                journal: Some(journal),
            },
        )
        .await
    }

    async fn put_file_stream_continuing(
        &self,
        spec: &NamespacePath,
        source: PayloadSource,
        options: &PutFileOptions,
        continuity: UploadContinuity<'_>,
    ) -> Result<ApiCommitResponse> {
        let staged = self
            .stage_source_as_content_ref(spec.namespace(), source, continuity)
            .await?;
        // The staged reference is what the server verified about the bytes
        // that just went past, and with the payload gone it is the only
        // description of them that still exists.
        let uploaded = staged.content_ref.clone();
        self.commit_staged_file(
            spec,
            staged,
            options,
            ContentEvidence::ContentRef(&uploaded),
        )
        .await
    }

    /// Commits content an upload session already completed, for a caller
    /// that finished the transfer and was interrupted before the commit.
    ///
    /// The reference and the token come from
    /// [`Self::read_upload_status`] reporting the session `Completed`: the
    /// bytes are in object storage and admitted, so the only thing left to
    /// do is name them at a path. Nothing is re-uploaded.
    pub async fn commit_completed_upload(
        &self,
        spec: &NamespacePath,
        content_ref: ContentRef,
        validated_content_token: Option<String>,
        options: &PutFileOptions,
    ) -> Result<ApiCommitResponse> {
        let uploaded = content_ref.clone();
        let staged = StagedContent {
            validated_content_token: validated_content_token.map(|token| ValidatedContentToken {
                content_ref: content_ref.clone(),
                token,
            }),
            content_ref,
        };
        self.commit_staged_file(
            spec,
            staged,
            options,
            ContentEvidence::ContentRef(&uploaded),
        )
        .await
    }

    /// Commits one staged payload at a path, reconciling a reused commit id
    /// against what was just uploaded.
    async fn commit_staged_file(
        &self,
        spec: &NamespacePath,
        staged: StagedContent,
        options: &PutFileOptions,
        uploaded: ContentEvidence<'_>,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest {
                    commit_id: commit_id.clone(),
                    message: options.message.clone(),
                    content_tokens: staged.validated_content_token.into_iter().collect(),
                    operations: vec![FilesystemOperation::PutFile {
                        path: spec.absolute_path().clone(),
                        content_ref: staged.content_ref,
                        behavior: options.behavior,
                        expected_revision_no: options.expected_revision_no,
                    }],
                },
            )
            .await;
        match response {
            Ok(response) => Ok(response),
            Err(error) if error.code() == Some(ErrorCode::CommitIdReuseConflict) => {
                self.reconcile_commit_id_reuse(spec, &commit_id, options, uploaded, error)
                    .await
            }
            Err(error) => Err(error),
        }
    }

    /// Decides whether a reused commit id already did this exact work.
    ///
    /// The proof is the commit's whole semantic identity, not a selection of
    /// its parts. The conflict reports the fingerprint the server's receipt
    /// holds; this reads the one change that commit id landed, rebuilds this
    /// request's fingerprint with the committed content reference in place
    /// of the freshly uploaded one, and requires the two to be equal. That
    /// covers the path, the replacement behavior, the expected revision, the
    /// annotation, and that the original commit was this one put — every
    /// field a future request gains is covered the day it joins the
    /// preimage. What the fingerprint cannot speak to is whether the two
    /// content objects hold the same bytes, so digest evidence answers that
    /// separately.
    ///
    /// Nothing weaker counts: every way of failing to prove the two requests
    /// are the same — including the upload having no answer to give — leaves
    /// the original conflict standing, never agreement.
    async fn reconcile_commit_id_reuse(
        &self,
        spec: &NamespacePath,
        commit_id: &CommitId,
        options: &PutFileOptions,
        uploaded: ContentEvidence<'_>,
        conflict: ClientError,
    ) -> Result<ApiCommitResponse> {
        let namespace_id = spec.namespace();
        loonfs_api::reconcile_put_commit_id_reuse(
            PutRetryAttempt {
                namespace_id,
                path: spec.absolute_path(),
                commit_id,
                options,
                staged: uploaded,
            },
            conflict,
            |after_seq| self.list_changes(namespace_id, after_seq, Some(1)),
            classify_put_retry_error,
        )
        .await
    }

    pub async fn create_directory(
        &self,
        spec: &NamespacePath,
        options: &CreateDirectoryOptions,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::CreateDirectory {
                        path: spec.absolute_path().clone(),
                        parents: options.parents,
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    pub async fn delete_path(
        &self,
        spec: &NamespacePath,
        options: &DeleteOptions,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::DeletePath {
                        path: spec.absolute_path().clone(),
                        behavior: options.behavior,
                        expected_inode_id: options.expected_inode_id,
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    /// Writes and removes attributes on the inode a path resolves to. The
    /// target may be a file or a directory.
    pub async fn update_attributes(
        &self,
        spec: &NamespacePath,
        options: &UpdateAttributesOptions,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::UpdateAttributes {
                        path: spec.absolute_path().clone(),
                        set: options.set.clone(),
                        remove: options.remove.clone(),
                        expected_inode_id: options.expected_inode_id,
                        expected_attributes_revision_no: options.expected_attributes_revision_no,
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    pub async fn move_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &MoveOptions,
    ) -> Result<ApiCommitResponse> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot move across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                from.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::MovePath {
                        from_path: from.absolute_path().clone(),
                        to_path: to.absolute_path().clone(),
                        behavior: options.behavior,
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    pub async fn copy_path(
        &self,
        from: &NamespacePath,
        to: &NamespacePath,
        options: &CopyOptions,
    ) -> Result<ApiCommitResponse> {
        if from.namespace() != to.namespace() {
            return Err(ClientError::InvalidNamespacePath(format!(
                "cannot copy across namespaces: {} -> {}",
                from.namespace(),
                to.namespace()
            )));
        }
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                from.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::CopyPath {
                        from_path: from.absolute_path().clone(),
                        to_path: to.absolute_path().clone(),
                        behavior: options.behavior,
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    /// Recovers a deleted file or subtree: clears the tombstone rooted at
    /// `inode_id` (the id the delete reported) and re-binds it at the spec's
    /// path.
    pub async fn undelete(
        &self,
        namespace: &NamespaceId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        path: Option<&AbsolutePath>,
        options: &UndeleteOptions,
    ) -> Result<ApiCommitResponse> {
        // An absent destination restores in place: the entry re-binds under
        // the parent and name its deletion recorded.
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                namespace,
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::Undelete {
                        inode_id,
                        deleted_at_seq,
                        path: path.cloned(),
                    },
                ),
            )
            .await?;
        Ok(response)
    }

    pub async fn restore_file_revision(
        &self,
        spec: &NamespacePath,
        source_revision_no: RevisionNo,
        options: &RestoreRevisionOptions,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::RestoreRevision {
                        path: spec.absolute_path().clone(),
                        source_revision_no,
                    },
                ),
            )
            .await?;
        Ok(response)
    }
}

impl NamespacePath {
    /// Parses and validates both parts of a namespace-qualified path.
    pub fn parse(namespace: &str, absolute_path: &str) -> Result<Self> {
        let namespace = NamespaceId::parse(namespace)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        let absolute_path = AbsolutePath::parse(absolute_path)
            .map_err(|error| ClientError::InvalidNamespacePath(error.to_string()))?;
        Ok(Self {
            namespace,
            absolute_path,
        })
    }

    /// Pairs already-validated parts without re-parsing.
    pub fn new(namespace: NamespaceId, absolute_path: AbsolutePath) -> Self {
        Self {
            namespace,
            absolute_path,
        }
    }

    /// Namespace the path is scoped to.
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    /// Absolute path inside the namespace.
    pub fn absolute_path(&self) -> &AbsolutePath {
        &self.absolute_path
    }
}

fn append_optional_pagination_query(
    url: &mut String,
    has_query: &mut bool,
    limit: Option<u32>,
    cursor: Option<&str>,
) {
    if let Some(limit) = limit {
        append_query_param(url, has_query, "limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        append_query_param(url, has_query, "cursor", cursor);
    }
}

fn append_query_param(url: &mut String, has_query: &mut bool, name: &str, value: &str) {
    url.push(if *has_query { '&' } else { '?' });
    *has_query = true;
    url.push_str(name);
    url.push('=');
    url.push_str(&urlencoding::encode(value));
}

#[cfg(test)]
mod download_tests;

#[cfg(test)]
mod streaming_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{retryable_transport_failure, TransportRetryPolicy};
    use loonfs_api::{ContentId, ErrorCode};
    use std::fs;
    use tempfile::tempdir;

    fn test_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef::blob_v1(ContentId::generate(), bytes)
    }

    fn direct_put_claim(bytes: &[u8]) -> DirectPutContentClaim {
        let content_ref = test_content_ref(bytes);
        DirectPutContentClaim {
            size_bytes: content_ref.size_bytes,
            storage_checksum: content_ref.storage_checksum,
        }
    }

    /// `Client::new` runs the same validation as `ClientConfig::load`, so a
    /// directly built config cannot bypass it.
    #[test]
    fn construction_validates_config_like_load_does() {
        let error = super::Client::new(super::ClientConfig {
            server_url: "ftp://example.com".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect_err("ftp scheme must be rejected");
        assert!(
            matches!(
                &error,
                super::ClientError::ConfigValidation {
                    field: "server_url",
                    ..
                }
            ),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn client_and_config_debug_redact_the_auth_token() {
        let raw_token = "client-debug-secret";
        let config = ClientConfig {
            server_url: "http://example.com".to_owned(),
            auth_token: Some(raw_token.into()),
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        };

        let config_debug = format!("{config:?}");
        assert!(!config_debug.contains(raw_token), "{config_debug}");
        assert!(config_debug.contains("<redacted>"), "{config_debug}");

        let client = Client::new(config).expect("valid client config");
        let client_debug = format!("{client:?}");
        assert!(!client_debug.contains(raw_token), "{client_debug}");
        assert!(client_debug.contains("<redacted>"), "{client_debug}");
    }

    /// A CA bundle that cannot be read or is not PEM fails at construction,
    /// naming the path. Falling back to the platform roots would move the
    /// failure to the first request and blame the server for it.
    #[test]
    fn an_unusable_ca_bundle_fails_construction_and_names_the_path() {
        let dir = tempdir().expect("tempdir");
        let missing = dir.path().join("absent.crt");
        let garbage = dir.path().join("garbage.crt");
        fs::write(&garbage, b"this is not a certificate\n").expect("write garbage");

        for path in [missing, garbage] {
            let display = path.display().to_string();
            let error = super::Client::new(super::ClientConfig {
                server_url: "https://example.com".to_owned(),
                auth_token: None,
                request_timeout_ms: None,
                disable_transient_retry: false,
                ca_cert_path: Some(display.clone()),
            })
            .expect_err("unusable ca bundle");
            match &error {
                super::ClientError::ConfigValidation {
                    field: "ca_cert_path",
                    reason,
                } => assert!(
                    reason.contains(&display),
                    "the reason must name the path, got: {reason}"
                ),
                other => unreachable!("unexpected error: {other:?}"),
            }
        }
    }

    /// Configs are strict like everywhere else in the workspace: a typo'd
    /// key fails decode instead of silently producing an unauthenticated
    /// client.
    #[test]
    fn client_config_rejects_unknown_keys() {
        let error = toml::from_str::<ClientConfig>(
            "server_url = \"http://localhost:1\"\nauth_tokn = \"oops\"\n",
        )
        .expect_err("unknown key must fail decode");
        assert!(error.to_string().contains("auth_tokn"), "{error}");

        let config: ClientConfig =
            toml::from_str("server_url = \"http://localhost:1\"\n").expect("minimal config");
        assert!(config.auth_token.is_none());
    }
    /// The retry policy in one place: network-level transport failures and
    /// the retryable-unavailability codes resend; everything else — including
    /// a served status whose body was not the error envelope — surfaces
    /// immediately.
    #[test]
    fn retryable_transport_failure_covers_transport_and_unavailability_only() {
        let api = |code: &str| ClientError::Api {
            status: 503,
            code: code.to_owned(),
            feature: None,
            message: String::new(),
            request_id: None,
            details: None,
        };
        assert!(retryable_transport_failure(
            true,
            &ClientError::Http("reset".to_owned())
        ));
        assert!(retryable_transport_failure(false, &api("server_busy")));
        assert!(retryable_transport_failure(
            false,
            &api("commit_queue_full")
        ));
        assert!(retryable_transport_failure(false, &api("shutting_down")));
        assert!(!retryable_transport_failure(false, &api("server_error")));
        assert!(!retryable_transport_failure(
            false,
            &api("checkpoint_unavailable")
        ));
        assert!(!retryable_transport_failure(
            false,
            &api("maintenance_required")
        ));
        assert!(!retryable_transport_failure(false, &api("index_lagging")));
        assert!(!retryable_transport_failure(
            false,
            &ClientError::Http("http status 502 with a non-envelope body".to_owned())
        ));
    }

    /// A network-level transport failure resends up to the attempt cap when
    /// retry is enabled; with it disabled the first failure surfaces.
    #[tokio::test]
    async fn transport_failures_resend_up_to_the_attempt_cap() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let max_attempts = TransportRetryPolicy::DEFAULT.max_retries as usize + 1;
        let transport = crate::transport::test_transport::failures(max_attempts);
        let retrying = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config");
        let error = retrying
            .namespace_status(&namespace_id)
            .await
            .expect_err("dropped connections must fail");
        assert!(matches!(error, ClientError::Http(_)), "{error:?}");
        assert_eq!(transport.attempts(), max_attempts);
        drop(transport);

        let transport = crate::transport::test_transport::failures(1);
        let single_shot = Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: true,
            ca_cert_path: None,
        })
        .expect("valid client config");
        single_shot
            .namespace_status(&namespace_id)
            .await
            .expect_err("dropped connection must fail without retry");
        assert_eq!(transport.attempts(), 1);
    }

    fn retry_policy_client() -> Client {
        Client::new(ClientConfig {
            server_url: "http://example.invalid".to_owned(),
            auth_token: None,
            request_timeout_ms: None,
            disable_transient_retry: false,
            ca_cert_path: None,
        })
        .expect("valid client config")
    }

    /// Installs a transport that fails once then succeeds, so a call that
    /// stops after one attempt surfaces the failure and a call that retries
    /// would succeed instead.
    fn single_attempt_probe() -> (crate::transport::test_transport::Guard, Client) {
        (
            crate::transport::test_transport::failure_then_success(b"{}".to_vec()),
            retry_policy_client(),
        )
    }

    fn assert_single_attempt<T>(
        result: Result<T>,
        transport: &crate::transport::test_transport::Guard,
    ) {
        assert!(
            matches!(result, Err(ClientError::Http(_))),
            "expected the first transport failure to surface"
        );
        assert_eq!(transport.attempts(), 1);
    }

    #[tokio::test]
    async fn retry_policy_lifecycle_mutations_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let fork_id = NamespaceId::parse("fork").expect("valid id");

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(client.create_namespace(&namespace_id).await, &transport);
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client.fork_namespace(&namespace_id, &fork_id).await,
            &transport,
        );
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .delete_namespace(&namespace_id, Some(ChangeSeq(7)))
                .await,
            &transport,
        );
    }

    #[tokio::test]
    async fn retry_policy_commit_id_filesystem_mutation_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let commit_id =
            CommitId::parse("c_00000000000000000000000000000001").expect("valid commit id");
        let response = ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: commit_id.clone(),
            committed_seq: ChangeSeq(1),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();
        let spec = NamespacePath::parse("demo", "/docs").expect("valid namespace path");

        let actual = client
            .create_directory(
                &spec,
                &CreateDirectoryOptions {
                    commit_id: Some(commit_id),
                    message: None,
                    ..CreateDirectoryOptions::default()
                },
            )
            .await
            .expect("commit-id mutation should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_read_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let response = NamespaceStatusResponse {
            namespace_id: namespace_id.clone(),
            head_seq: ChangeSeq(0),
            current_manifest_id: None,
            wal_tail_segments: 0,
            retention_floor_seq: ChangeSeq(0),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .namespace_status(&namespace_id)
            .await
            .expect("read should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_upload_begins_are_single_attempt() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .begin_upload(&namespace_id, &BeginUploadRequest::ServiceProxied {})
                .await,
            &transport,
        );
        drop(transport);

        let (transport, client) = single_attempt_probe();
        assert_single_attempt(
            client
                .begin_direct_put(&namespace_id, direct_put_claim(b"direct"))
                .await,
            &transport,
        );
    }

    #[tokio::test]
    async fn retry_policy_presigned_upload_is_single_attempt() {
        let transport = crate::transport::test_transport::failure_then_success(Vec::new());
        let client = retry_policy_client();
        let access = ObjectTransferAccess::PresignedUrl {
            method: "PUT".to_owned(),
            url: "http://example.invalid/upload".to_owned(),
            headers: std::collections::BTreeMap::new(),
            expires_at_ms: 1,
        };

        let result = client.upload_via_presigned_url(&access, b"direct").await;

        assert!(matches!(result, Err(ClientError::Http(_))), "{result:?}");
        assert_eq!(transport.attempts(), 1);
    }

    #[tokio::test]
    async fn retry_policy_proxied_upload_content_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let response = UploadContentResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: test_content_ref(b"content"),
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .upload_content(&namespace_id, &upload_id, b"content")
            .await
            .expect("identical content staging should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    #[tokio::test]
    async fn retry_policy_upload_completion_retries() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let upload_id = loonfs_api::UploadId::parse("upl_00000000000000000000000000000001")
            .expect("valid upload id");
        let content_ref = test_content_ref(b"content");
        let response = CompleteUploadResponse {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            content_ref: content_ref.clone(),
            validated_content_token: None,
        };
        let transport = crate::transport::test_transport::failure_then_success(
            serde_json::to_vec(&response).expect("serialize response"),
        );
        let client = retry_policy_client();

        let actual = client
            .complete_upload(
                &namespace_id,
                &upload_id,
                &CompleteUploadRequest::for_content_ref(content_ref),
            )
            .await
            .expect("completed-session replay should retry");
        assert_eq!(actual, response);
        assert_eq!(transport.attempts(), 2);
    }

    /// An intermediary answering with a non-envelope body (a load balancer's
    /// HTML 502) must keep its status in the surfaced error — the status is
    /// the only signal the response carried.
    #[test]
    fn status_errors_keep_the_status_when_the_body_is_not_the_envelope() {
        let error = crate::transport::map_status_error(502, b"<html>upstream error</html>");

        let ClientError::Http(message) = error else {
            unreachable!("expected Http error, got {error:?}");
        };
        assert!(message.contains("502"), "{message}");
        assert!(message.contains("non-envelope body"), "{message}");
    }

    fn api_error(status: u16, code: &str) -> ClientError {
        ClientError::Api {
            status,
            code: code.to_owned(),
            feature: None,
            message: "test".to_owned(),
            request_id: None,
            details: None,
        }
    }

    #[test]
    fn api_errors_expose_known_registry_codes() {
        for (wire, expected) in [
            ("stale_revision", ErrorCode::StaleRevision),
            ("content_not_prepared", ErrorCode::ContentNotPrepared),
            ("namespace_deleted", ErrorCode::NamespaceDeleted),
            ("commit_outcome_unknown", ErrorCode::CommitOutcomeUnknown),
            ("index_corrupt", ErrorCode::IndexCorrupt),
        ] {
            assert_eq!(api_error(500, wire).code(), Some(expected));
        }
    }

    #[test]
    fn api_errors_tolerate_unknown_registry_codes() {
        assert_eq!(api_error(503, "code_from_a_newer_server").code(), None);
    }

    #[test]
    fn non_api_errors_have_no_code() {
        let error = ClientError::Http("connection refused".to_owned());
        assert_eq!(error.code(), None);
    }

    #[test]
    fn load_rejects_invalid_server_url() {
        let path = write_config(
            r#"
server_url = "ftp://example.com"
auth_token = "dev-token"
"#,
        );

        let error = ClientConfig::load(&path).expect_err("invalid server url");

        assert!(
            matches!(error, ClientError::ConfigValidation { field, .. } if field == "server_url"),
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_rejects_blank_auth_token() {
        let path = write_config(
            r#"
server_url = "http://127.0.0.1:9400"
auth_token = "   "
"#,
        );

        let error = ClientConfig::load(&path).expect_err("blank auth token");

        assert!(
            matches!(error, ClientError::ConfigValidation { field, .. } if field == "auth_token"),
            "expected config validation error, got {error:?}"
        );
    }

    #[test]
    fn load_preserves_missing_file_as_config_io() {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("missing.toml");

        let error = ClientConfig::load(&path).expect_err("missing config");

        assert!(matches!(error, ClientError::ConfigIo(_)));
    }

    #[test]
    fn load_preserves_decode_error() {
        let path = write_config("server_url = [");

        let error = ClientConfig::load(&path).expect_err("decode error");

        assert!(matches!(error, ClientError::ConfigDecode(_)));
    }

    #[test]
    fn namespace_path_parse_rejects_invalid_namespace_id() {
        for namespace in ["bad/name", "Demo", "..", "demo?"] {
            assert!(
                matches!(
                    NamespacePath::parse(namespace, "/notes.txt"),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path for id {namespace:?}"
            );
        }
    }

    /// Construction is the only door: the fields are private, so a bad id
    /// or a bad path fails `parse` with the same error the string-shuttling
    /// client surfaced before the fields were typed.
    #[test]
    fn namespace_path_parse_rejects_invalid_paths() {
        for path in ["notes.txt", "", "/docs/../a.txt", "/docs/./a.txt"] {
            assert!(
                matches!(
                    NamespacePath::parse("demo", path),
                    Err(ClientError::InvalidNamespacePath(_))
                ),
                "expected invalid namespace path for path {path:?}"
            );
        }
    }

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("client.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }
}
