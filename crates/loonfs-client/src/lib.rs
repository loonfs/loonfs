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
        BeginUploadResponse, ChangesResponse, CommitResponse as ApiCommitResponse, CommittedChange,
        CompleteUploadRequest, CompleteUploadResponse, CompletedUploadPart,
        DirectMultipartContentClaim, DirectMultipartUploadOptions, DirectPutContentClaim,
        DisableGrepIndexResponse, EnableGrepIndexResponse, FilesystemChange, GrepGcRequest,
        GrepGcResponse, GrepIndexStatusResponse, ObjectTransferAccess, SignUploadPartsRequest,
        SignUploadPartsResponse, SignedUploadPart, StoreProbeRequest, StoreProbeResponse,
        UploadContentResponse, UploadMode, UploadPartChecksumClaim, UploadStatusResponse,
        ValidatedContentToken,
    },
    AbsolutePath, AuthoritativePathEntry, CapabilityDocument, ChangeSeq, CheckpointId,
    ChecksumAlgorithm, CommitId, CommitRequest, ContentRef, Crc64Nvme, CreateCheckpointRequest,
    CreateCheckpointResponse, CreateNamespaceRequest, DeleteNamespaceResponse, ErrorCode,
    FilesystemOperation, ForkNamespaceRequest, GrepRequest, GrepResponse, InodeId,
    ListCheckpointsResponse, ListFileRevisionsResponse, ListPathEntriesResponse, ListTrashResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, NamespaceId, NamespaceStatusResponse,
    NamespaceSummary, ReleaseCheckpointResponse, RevisionNo, Sha256, StorageChecksum, UploadId,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_UPLOADS_DIRECT_MULTIPART,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES,
};
use payload::PartReader;
use std::sync::{Arc, OnceLock};

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

/// What a retry can still say about the payload it just uploaded.
///
/// A buffered put can answer any question about its bytes by hashing them
/// again. A one-pass put cannot: its payload went by once and is gone, and
/// what it kept instead is the verified description the server gave back.
/// Both are evidence about the same bytes; they differ only in which
/// questions they can answer, and a question neither can answer is reported
/// as such rather than guessed at.
#[derive(Debug, Clone, Copy)]
enum UploadedContent<'a> {
    /// The payload itself, which can produce any digest this build knows.
    Bytes(&'a [u8]),
    /// The reference the server minted for a payload that was streamed
    /// past: its length, and the digest whoever hashed it reported.
    Streamed(&'a ContentRef),
}

impl UploadedContent<'_> {
    /// Complete length of what was uploaded.
    fn size_bytes(&self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes.len() as u64,
            Self::Streamed(content_ref) => content_ref.size_bytes,
        }
    }

    /// Whether the upload's bytes produce this checksum.
    ///
    /// `None` is a refusal to answer — the algorithm is one this build
    /// cannot recompute, or one nobody computed over the streamed payload —
    /// and a caller must never read it as agreement.
    fn matches(&self, expected: &StorageChecksum) -> Option<bool> {
        match self {
            Self::Bytes(bytes) => expected.matches(bytes),
            Self::Streamed(content_ref) => {
                let observed = digest_of(content_ref, expected.algorithm)?;
                Some(observed == expected.value)
            }
        }
    }
}

/// The digest a reference carries under one algorithm, if it carries one.
fn digest_of(content_ref: &ContentRef, algorithm: ChecksumAlgorithm) -> Option<&str> {
    if content_ref.storage_checksum.algorithm == algorithm {
        return Some(&content_ref.storage_checksum.value);
    }
    match algorithm {
        ChecksumAlgorithm::Sha256 => content_ref.whole_file_sha256.as_deref(),
        _ => None,
    }
}

/// Whether the committed reference provably holds the bytes just uploaded.
///
/// The evidence is the trusted whole-file digest when the server has one,
/// and otherwise the reference's own storage checksum — which for a
/// provider-assembled multipart object is the only full-object evidence
/// that exists. Only a digest this client can recompute, over bytes that
/// agree, proves anything: a digest that disagrees and a digest this client
/// cannot recompute are both unproven, and both leave the conflict
/// standing.
fn uploaded_matches_committed(uploaded: &UploadedContent<'_>, content_ref: &ContentRef) -> bool {
    let evidence = match &content_ref.whole_file_sha256 {
        Some(digest) => StorageChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: digest.clone(),
        },
        None => content_ref.storage_checksum.clone(),
    };
    uploaded.matches(&evidence) == Some(true)
}

/// Where a reuse conflict says the commit id already landed.
///
/// The server decides the conflict against a durable receipt, and the
/// receipt holds the sequence, so the error body carries it. It is absent
/// only when nothing has committed under the id yet — two conflicting
/// requests claiming it at once — and then there is nothing to read back.
fn reported_committed_seq(error: &ClientError) -> Option<ChangeSeq> {
    match error {
        ClientError::Api { details, .. } => details.as_ref()?.committed_seq,
        _ => None,
    }
}

/// The content one committed change wrote, if it wrote any.
fn committed_content_ref(change: &CommittedChange) -> Option<&ContentRef> {
    change.events.iter().find_map(|event| match event {
        FilesystemChange::Created { content_ref, .. } => content_ref.as_ref(),
        FilesystemChange::ContentChanged { content_ref, .. } => Some(content_ref),
        _ => None,
    })
}

pub use config::ClientConfig;
pub use error::ClientError;
pub use payload::{PayloadSource, PayloadStream};
use transport::{WireRequest, IO_INACTIVITY_TIMEOUT};
pub use ClientError as Error;

/// Per-operation options, defined once in `loonfs-api` and shared with the
/// embedded `loonfs` runtime so the two surfaces cannot drift a field apart.
pub use loonfs_api::options::{
    CopyOptions, CreateDirectoryOptions, DeleteOptions, MoveOptions, PutFileOptions,
    RestoreRevisionOptions, UndeleteOptions,
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
    auth_token: Option<String>,
    http: reqwest::Client,
    /// Whether transient server errors are retried (see
    /// [`ClientConfig::disable_transient_retry`]).
    transient_retry: bool,
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
    sha256: Option<Sha256>,
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
    /// bytes it will never receive have to be folded into the same hash as
    /// the ones it does. Feeding the wrong bytes fails the download at its
    /// end, which is right: the grant's reference is the authority on what
    /// the object holds, not the partial copy on the caller's disk.
    pub fn fold_resumed_prefix(&mut self, bytes: &[u8]) {
        if let Some(sha256) = self.sha256.as_mut() {
            sha256.update(bytes);
        }
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
                    return Err(ClientError::Http(format!(
                        "direct download of `{}` sent more than the {} bytes the grant named",
                        self.path, self.expected.size_bytes
                    )));
                }
                if let Some(sha256) = self.sha256.as_mut() {
                    sha256.update(&chunk);
                }
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
                    return Err(ClientError::Http(format!(
                        "direct download of `{}` ended after {} bytes, not the {} the grant named",
                        self.path, self.size_bytes, self.expected.size_bytes
                    )));
                }
                if let (Some(sha256), Some(expected_sha256)) = (
                    self.sha256.take(),
                    self.expected.whole_file_sha256.as_deref(),
                ) {
                    let observed = sha256.finish().value;
                    if observed != expected_sha256 {
                        return Err(ClientError::Http(format!(
                            "direct download of `{}` hashed to {observed}, not the \
                             {expected_sha256} the grant named",
                            self.path
                        )));
                    }
                }
                Ok(None)
            }
        }
    }
}

/// What a caller keeps so an interrupted direct multipart upload can pick
/// up rather than start over.
///
/// The parts are the caller's own bookkeeping, deliberately: an upload
/// session records the geometry it was opened with and nothing per part, so
/// the only account of which parts landed is the one the uploading client
/// kept. Resuming with fewer parts than actually landed re-sends them,
/// which is harmless; resuming with parts that did not land fails the
/// completion, which is the assembly refusing to be wrong.
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
                .map_err(|error| ClientError::Http(format!("reading the payload failed: {error}"))),
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
            ClientError::Http(format!(
                "server authorized no upload for part {part_number}"
            ))
        })
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
        let mut builder = reqwest::Client::builder()
            // Bounds a stalled connection without cutting off a slow but
            // progressing transfer, which a whole-request deadline would.
            .read_timeout(IO_INACTIVITY_TIMEOUT)
            .connect_timeout(IO_INACTIVITY_TIMEOUT);
        if let Some(timeout_ms) = config.request_timeout_ms {
            builder = builder.timeout(std::time::Duration::from_millis(timeout_ms));
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
                .map_err(|err| ClientError::Http(err.to_string()))?,
            transient_retry: !config.disable_transient_retry,
            capabilities: Arc::new(OnceLock::new()),
        })
    }

    /// Returns the server's capability document, fetched once and cached for
    /// the life of this client and its clones (API spec, "Capability
    /// discovery").
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
    /// [`Self::list_path_entries_page`] for page-level control.
    pub async fn list_path_entries_all(
        &self,
        spec: &NamespacePath,
    ) -> Result<ListPathEntriesResponse> {
        let mut entries = Vec::new();
        let mut envelope = None;
        let mut cursor = None;
        loop {
            let page = self
                .list_path_entries_page(spec, None, cursor.as_deref())
                .await?;
            let envelope_ref = envelope.get_or_insert_with(|| ListPathEntriesResponse {
                namespace_id: page.namespace_id.clone(),
                absolute_path: page.absolute_path.clone(),
                head_seq: page.head_seq,
                entries: Vec::new(),
                next_cursor: None,
            });
            envelope_ref.head_seq = envelope_ref.head_seq.max(page.head_seq);
            entries.extend(page.entries);
            cursor = page.next_cursor;
            if cursor.is_none() {
                // Pages arrive in canonical name-key order; concatenation
                // preserves it, so aggregation must not re-sort.
                envelope_ref.entries = entries;
                return Ok(envelope.expect("first page initializes response envelope"));
            }
        }
    }

    pub async fn list_path_entries_page(
        &self,
        spec: &NamespacePath,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<ListPathEntriesResponse> {
        let mut url = format!(
            "{}/v0/namespaces/{}/filesystem/list?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        let has_query = true;
        append_optional_pagination_query(&mut url, has_query, limit, cursor);
        self.request_json::<(), ListPathEntriesResponse>(self.get(&url), None)
            .await
    }

    pub async fn stat_path(&self, spec: &NamespacePath) -> Result<AuthoritativePathEntry> {
        let url = format!(
            "{}/v0/namespaces/{}/filesystem/stat?path={}",
            self.base_url,
            spec.namespace().as_str(),
            urlencoding::encode(spec.absolute_path().as_str())
        );
        self.request_json::<(), AuthoritativePathEntry>(self.get(&url), None)
            .await
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
    pub async fn offers_direct_download(&self, size_bytes: u64) -> bool {
        let Ok(capabilities) = self.capabilities().await else {
            return false;
        };
        capabilities.supports(FEATURE_DOWNLOADS_DIRECT_GET)
            && capabilities
                .limits
                .get(LIMIT_DOWNLOAD_MAX_CONTENT_BYTES)
                .is_some_and(|proxy_cap| size_bytes > *proxy_cap)
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
            return Err(ClientError::Http(format!(
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
            sha256: download
                .content_ref
                .whole_file_sha256
                .as_ref()
                .map(|_| Sha256::new()),
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
        let has_query = true;
        append_optional_pagination_query(&mut url, has_query, limit, cursor);
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
        let has_query = false;
        append_optional_pagination_query(&mut url, has_query, limit, cursor);
        self.request_json::<(), ListTrashResponse>(self.get(&url), None)
            .await
    }

    pub async fn health(&self) -> Result<()> {
        let url = format!("{}/health", self.base_url);
        self.call_with_transient_retry(&self.get(&url), None)
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
            &BeginUploadRequest {
                mode: Some(UploadMode::DirectPut),
                content: Some(claim),
                multipart: None,
            },
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
            &BeginUploadRequest {
                mode: Some(UploadMode::DirectMultipart),
                content: None,
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
            return Err(ClientError::Http(format!(
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
            .call_with_transient_retry_headers(&request, Some(&bytes))
            .await?;
        let etag = response
            .get(http::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| {
                ClientError::Http(format!("part {part_number} upload returned no etag"))
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
        let (method, url, headers) = match access {
            ObjectTransferAccess::PresignedUrl {
                method,
                url,
                headers,
                ..
            } => (method, url, headers),
        };
        if method != "PUT" {
            return Err(ClientError::Http(format!(
                "unsupported presigned upload method `{method}`"
            )));
        }
        let mut request = WireRequest::presigned(reqwest::Method::PUT, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        // A successful create-only PUT may replay as a provider precondition error, not success.
        self.call_once(&request, Some(&Bytes::copy_from_slice(bytes)))
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
            .call_with_transient_retry(&request, Some(&Bytes::copy_from_slice(bytes)))
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
    /// Absent request fields use the server's defaults; garbage collection
    /// runs only when the request opts in.
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
        if bytes.len() as u64 >= STREAMING_PUT_MIN_BYTES && self.offers_direct_multipart().await {
            return self
                .stage_via_multipart(
                    namespace_id,
                    MultipartPayload::Held(bytes),
                    UploadContinuity::default(),
                )
                .await;
        }
        self.stage_bytes_via_server(namespace_id, bytes).await
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
        // A source that knows it is small has nothing to gain from parts,
        // exactly as a held payload of that size does not. A source that
        // does not know its length cannot make that judgement, so it takes
        // the transport that can carry any length.
        let known_small = source
            .size_bytes()
            .is_some_and(|size_bytes| size_bytes < STREAMING_PUT_MIN_BYTES);
        if !known_small && self.offers_direct_multipart().await {
            let (stream, _) = source.into_stream();
            return self
                .stage_via_multipart(namespace_id, MultipartPayload::Streamed(stream), continuity)
                .await;
        }
        // Nothing to resume off this path: a proxied upload is one request
        // with no session behind it, so there are no parts to have landed.
        self.stage_source_via_server(namespace_id, source).await
    }

    /// Whether this deployment authorizes direct part uploads.
    async fn offers_direct_multipart(&self) -> bool {
        self.capabilities()
            .await
            .is_ok_and(|capabilities| capabilities.supports(FEATURE_UPLOADS_DIRECT_MULTIPART))
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
                let Some(multipart) = begin.direct_multipart else {
                    return Err(ClientError::Http(
                        "server accepted direct_multipart without part geometry".to_owned(),
                    ));
                };
                if let Some(journal) = continuity.journal {
                    journal.began(&begin.upload_id, multipart.part_size_bytes);
                }
                (begin.upload_id, multipart.part_size_bytes)
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
        let part_size = usize::try_from(part_size_bytes)
            .map_err(|_| ClientError::Http("part size does not fit this platform".to_owned()))?;
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
        let mut in_flight = tokio::task::JoinSet::new();
        for part in wave {
            let access = signed_access(&signed.parts, part.claim.part_number)?;
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
            match joined.map_err(|err| ClientError::Http(format!("part upload task failed: {err}")))
            {
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
        for attempt in 1..=DIRECT_MULTIPART_PART_ATTEMPTS {
            let result = self
                .upload_part_via_presigned_url(
                    part_number,
                    &access,
                    part.claim.crc64nvme.clone(),
                    part.bytes.clone(),
                )
                .await;
            match result {
                Ok(uploaded) => return Ok(uploaded),
                Err(error) if attempt == DIRECT_MULTIPART_PART_ATTEMPTS => return Err(error),
                Err(_) => {
                    let signed = self
                        .sign_upload_parts(namespace_id, upload_id, vec![part.claim.clone()])
                        .await?;
                    access = signed_access(&signed.parts, part_number)?;
                }
            }
        }
        // The loop above returns on its last attempt.
        Err(ClientError::Http(format!(
            "part {part_number} upload made no attempt"
        )))
    }

    async fn stage_bytes_via_server(
        &self,
        namespace_id: &NamespaceId,
        bytes: &[u8],
    ) -> Result<StagedContent> {
        let upload = self
            .begin_upload(namespace_id, &BeginUploadRequest::default())
            .await?;
        let staged = self
            .upload_content(namespace_id, &upload.upload_id, bytes)
            .await?;
        self.complete_staged(namespace_id, &upload.upload_id, staged)
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
            .begin_upload(namespace_id, &BeginUploadRequest::default())
            .await?;
        let staged = self
            .upload_streamed_content(namespace_id, &upload.upload_id, source)
            .await;
        let staged = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = self.abort_upload(namespace_id, &upload.upload_id).await;
                return Err(error);
            }
        };
        self.complete_staged(namespace_id, &upload.upload_id, staged)
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
    /// original answer is returned; anything else — different bytes, a
    /// different message, or content this build cannot compare — surfaces
    /// the conflict.
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
        self.commit_staged_file(spec, staged, options, UploadedContent::Bytes(bytes))
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
        self.commit_staged_file(spec, staged, options, UploadedContent::Streamed(&uploaded))
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
        self.commit_staged_file(spec, staged, options, UploadedContent::Streamed(&uploaded))
            .await
    }

    /// Commits one staged payload at a path, reconciling a reused commit id
    /// against what was just uploaded.
    async fn commit_staged_file(
        &self,
        spec: &NamespacePath,
        staged: StagedContent,
        options: &PutFileOptions,
        uploaded: UploadedContent<'_>,
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
                self.reconcile_commit_id_reuse(
                    spec.namespace(),
                    &commit_id,
                    options.message.as_deref(),
                    uploaded,
                    error,
                )
                .await
            }
            Err(error) => Err(error),
        }
    }

    /// Decides whether a reused commit id already did this exact work.
    ///
    /// The evidence is the committed change itself, read back from the
    /// change feed at the sequence the conflict reported: its message
    /// against the one this request asked for, and its content against the
    /// bytes that were just uploaded. Nothing weaker counts: every way of
    /// failing to prove the two requests are the same — including a
    /// comparison this client cannot make — leaves the original conflict
    /// standing, never agreement.
    async fn reconcile_commit_id_reuse(
        &self,
        namespace_id: &NamespaceId,
        commit_id: &CommitId,
        message: Option<&str>,
        uploaded: UploadedContent<'_>,
        conflict: ClientError,
    ) -> Result<ApiCommitResponse> {
        let Some(committed_seq) = reported_committed_seq(&conflict) else {
            return Err(conflict);
        };
        let Some(committed) = self
            .read_committed_change(namespace_id, commit_id, committed_seq)
            .await?
        else {
            return Err(conflict);
        };
        // The message is part of a commit's identity, so a rerun that
        // changed it asked for a different mutation and the server's
        // conflict is the honest answer. Equality here is the server
        // fingerprint's: the message is taken as given — absent serializes
        // as `null`, an empty message as `""` — so no message and an empty
        // message are different commits, and this comparison must not fold
        // them together.
        if committed.message.as_deref() != message {
            return Err(conflict);
        }
        let Some(content_ref) = committed_content_ref(&committed) else {
            return Err(conflict);
        };
        if content_ref.size_bytes != uploaded.size_bytes() {
            return Err(conflict);
        }
        if !uploaded_matches_committed(&uploaded, content_ref) {
            return Err(conflict);
        }
        Ok(ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: committed.commit_id,
            committed_seq: committed.seq,
        })
    }

    /// Reads the one change a reuse conflict said the commit id landed at.
    ///
    /// There is no by-commit-id read on the wire, but the conflict already
    /// named the sequence, so this is one feed page positioned on it rather
    /// than a search. `None` means the evidence is not there to compare —
    /// the sequence has fallen below the retention floor, or the row at it
    /// belongs to some other commit — and the caller turns that into the
    /// conflict rather than into a guess.
    async fn read_committed_change(
        &self,
        namespace_id: &NamespaceId,
        commit_id: &CommitId,
        committed_seq: ChangeSeq,
    ) -> Result<Option<CommittedChange>> {
        let after_seq = ChangeSeq(committed_seq.0.saturating_sub(1));
        let page = match self.list_changes(namespace_id, after_seq, Some(1)).await {
            Ok(page) => page,
            // Retention gave up the replay promise below the floor: the
            // commit landed, but what it wrote can no longer be read back.
            Err(error) if error.code() == Some(ErrorCode::RebootstrapRequired) => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        Ok(page
            .changes
            .into_iter()
            .find(|change| change.seq == committed_seq && &change.commit_id == commit_id))
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
        spec: &NamespacePath,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        options: &UndeleteOptions,
    ) -> Result<ApiCommitResponse> {
        let commit_id = options.commit_id.clone().unwrap_or_else(CommitId::generate);
        let response = self
            .commit(
                spec.namespace(),
                &CommitRequest::single(
                    commit_id,
                    options.message.clone(),
                    FilesystemOperation::Undelete {
                        inode_id,
                        deleted_at_seq,
                        path: spec.absolute_path().clone(),
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
    has_query: bool,
    limit: Option<u32>,
    cursor: Option<&str>,
) {
    let mut has_query = has_query;
    if let Some(limit) = limit {
        append_query_param(url, &mut has_query, "limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        append_query_param(url, &mut has_query, "cursor", cursor);
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
    use crate::transport::{transient_failure, MAX_TRANSIENT_ATTEMPTS};
    use loonfs_api::{ContentId, ErrorCode, ErrorKind};
    use std::fs;
    use tempfile::tempdir;

    fn test_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef::blob_v1(ContentId::generate(), bytes)
    }

    fn direct_put_claim(bytes: &[u8]) -> DirectPutContentClaim {
        let content_ref = test_content_ref(bytes);
        DirectPutContentClaim {
            size_bytes: content_ref.size_bytes,
            sha256: content_ref.storage_checksum.value,
        }
    }

    /// A reference whose only full-object evidence is a digest this client
    /// has no implementation for. No server mints one today, which is
    /// exactly why the reconciliation has to say what it does when one
    /// arrives.
    fn crc32c_content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef {
            kind: loonfs_api::ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum {
                algorithm: ChecksumAlgorithm::Crc32c,
                value: "0f5c0a1e".to_owned(),
            },
            whole_file_sha256: None,
        }
    }

    #[test]
    fn a_digest_this_client_cannot_compare_leaves_the_reuse_conflict_standing() {
        let bytes = b"retried payload";
        let committed = crc32c_content_ref(bytes);
        let uploaded = UploadedContent::Bytes(bytes);

        assert_eq!(
            uploaded.matches(&committed.storage_checksum),
            None,
            "the fixture must reach the refusal, not a comparison"
        );
        assert!(!uploaded_matches_committed(&uploaded, &committed));
    }

    #[test]
    fn a_whole_file_digest_over_the_same_bytes_proves_the_retry_did_this_work() {
        let bytes = b"retried payload";
        let committed = test_content_ref(bytes);

        assert!(uploaded_matches_committed(
            &UploadedContent::Bytes(bytes),
            &committed
        ));
        assert!(!uploaded_matches_committed(
            &UploadedContent::Bytes(b"some other payload"),
            &committed
        ));
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
    fn transient_failure_covers_transport_and_retryable_unavailability_only() {
        let api = |code: &str| ClientError::Api {
            status: 503,
            code: code.to_owned(),
            feature: None,
            message: String::new(),
            request_id: None,
            details: None,
        };
        assert!(transient_failure(
            true,
            &ClientError::Http("reset".to_owned())
        ));
        assert!(transient_failure(false, &api("server_busy")));
        assert!(transient_failure(false, &api("commit_queue_full")));
        assert!(transient_failure(false, &api("shutting_down")));
        assert!(!transient_failure(false, &api("server_error")));
        assert!(!transient_failure(false, &api("maintenance_required")));
        assert!(!transient_failure(
            false,
            &ClientError::Http("http status 502 with a non-envelope body".to_owned())
        ));
    }

    /// A network-level transport failure resends up to the attempt cap when
    /// retry is enabled; with it disabled the first failure surfaces.
    #[tokio::test]
    async fn transport_failures_resend_up_to_the_attempt_cap() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let transport = crate::transport::test_transport::failures(MAX_TRANSIENT_ATTEMPTS as usize);
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
        assert_eq!(transport.attempts(), MAX_TRANSIENT_ATTEMPTS as usize);
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
                .begin_upload(&namespace_id, &BeginUploadRequest::default())
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
    fn api_errors_with_known_codes_classify_through_the_registry() {
        let error = api_error(409, "stale_revision");
        assert_eq!(error.code(), Some(ErrorCode::StaleRevision));
        assert_eq!(error.kind(), Some(ErrorKind::Conflict));

        let error = api_error(409, "content_not_prepared");
        assert_eq!(error.code(), Some(ErrorCode::ContentNotPrepared));
        assert_eq!(error.kind(), Some(ErrorKind::Conflict));

        let error = api_error(410, "namespace_deleted");
        assert_eq!(error.code(), Some(ErrorCode::NamespaceDeleted));
        assert_eq!(error.kind(), Some(ErrorKind::Gone));

        let error = api_error(503, "commit_outcome_unknown");
        assert_eq!(error.code(), Some(ErrorCode::CommitOutcomeUnknown));
        assert_eq!(error.kind(), Some(ErrorKind::OutcomeUnknown));

        let error = api_error(500, "index_corrupt");
        assert_eq!(error.code(), Some(ErrorCode::IndexCorrupt));
        assert_eq!(error.kind(), Some(ErrorKind::DataCorruption));
    }

    #[test]
    fn api_errors_with_unknown_codes_fall_back_to_the_status_class() {
        for (status, kind) in [
            (400, ErrorKind::InvalidRequest),
            (404, ErrorKind::InvalidRequest),
            (500, ErrorKind::Internal),
            (503, ErrorKind::Unavailable),
        ] {
            let error = api_error(status, "code_from_a_newer_server");
            assert_eq!(error.code(), None);
            assert_eq!(error.kind(), Some(kind), "status {status}");
        }
    }

    #[test]
    fn non_api_errors_have_no_code_or_kind() {
        let error = ClientError::Http("connection refused".to_owned());
        assert_eq!(error.code(), None);
        assert_eq!(error.kind(), None);
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
