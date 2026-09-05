//! Async HTTP client for a LoonFS server.
//!
//! Use [`Client`] with [`ClientConfig`] and [`NamespacePath`] to access a
//! hosted LoonFS server. Applications that embed the runtime directly should
//! use the `loonfs` crate. Both crates share operation options from
//! `loonfs-api`.

#![warn(missing_docs)]
#![allow(
    clippy::result_large_err,
    reason = "ClientError::Api exposes all structured server error fields"
)]

mod config;
mod downloads;
mod error;
mod maintenance;
mod mutations;
mod namespace_path;
mod payload;
mod query;
mod reads;
mod transport;
mod uploads;

use bytes::Bytes;
use futures::StreamExt as _;
use loonfs_api::{
    v0::{
        BeginDownloadByInodeRequest, BeginDownloadByInodeResponse, BeginDownloadRequest,
        BeginDownloadResponse, BeginUploadRequest, BeginUploadResponse,
        CommitResponse as ApiCommitResponse, CompleteUploadRequest, CompletedUploadPart,
        ContentToken, CreateSnapshotRequest, ExtendSnapshotRequest, GrepGcRequest, GrepGcResponse,
        GrepIndex, ListChangesResponse, ListSnapshotsResponse, ObjectTransferAccess,
        ReleaseSnapshotResponse, SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart,
        SnapshotSummary, StoreProbeRequest, StoreProbeResponse, UploadContentClaim,
        UploadContentResponse, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
    },
    AbsolutePath, CapabilityDocument, ChangeSeq, Checkpoint, CheckpointId, Checksum,
    ChecksumAlgorithm, CommitId, CommitRequest, ContentRef, CreateCheckpointRequest,
    CreateNamespaceRequest, DeleteNamespaceResponse, FilesystemOperation, ForkNamespaceRequest,
    GrepRequest, GrepResponse, InodeId, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListInodeChildrenResponse, ListPathEntriesResponse, ListTrashResponse, Namespace,
    NamespaceDiagnostics, NamespaceId, PathEntry, ReleaseCheckpointResponse, RevisionNo,
    RunMaintenanceRequest, RunMaintenanceResponse, SecretString, StreamingChecksum, UploadId,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_DOWNLOAD_MAX_CONTENT_BYTES, LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_MAX_CONTENT_BYTES,
};
use payload::PartReader;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub use config::ClientConfig;
pub use error::ClientError;
pub use maintenance::CheckpointsPager;
pub use payload::{PayloadSource, PayloadStream};
pub use reads::{
    ChangesPager, FileRevisionsPager, InodeChildrenPager, ListChangesOptions, PathEntriesPager,
    ReadFileOptions, SnapshotsPager, TrashPager,
};
use transport::{
    SendPolicy, StdMonotonicTimer, TransportRetryPolicy, WireRequest, DEFAULT,
    IO_INACTIVITY_TIMEOUT,
};
pub use ClientError as Error;

/// Per-operation options, defined once in `loonfs-api` and shared with the
/// embedded `loonfs` runtime so the two surfaces cannot drift a field apart.
pub use loonfs_api::options::{
    CommitOptions, CopyOptions, CreateDirectoryOptions, DeleteOptions,
    DirectMultipartUploadOptions, ListInodeChildrenOptions, ListPathEntriesOptions, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, StatPathOptions, UndeleteOptions,
    UpdateAttributesOptions,
};

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, ClientError>;

pub use downloads::{DirectDownloadStream, DownloadOptions};
pub use namespace_path::NamespacePath;
pub use uploads::staging::{
    MultipartUploadResume, PreparedContent, PutFileJournal, STREAMING_PUT_MIN_BYTES,
};

/// Async HTTP client for LoonFS.
///
/// Cloning is cheap: clones share one connection pool and one capability
/// cache.
#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    auth_token: Option<SecretString>,
    http: reqwest::Client,
    /// Optional timeout configured for each HTTP attempt.
    ///
    /// Replay-safe requests use the smaller of this value and the time left in
    /// the total retry budget.
    request_timeout: Option<Duration>,
    /// Whether the client retries eligible network and server errors (see
    /// [`ClientConfig::disable_transient_retry`]).
    transport_retry_enabled: bool,
    /// Attempt count, delay, and total duration limits for replay-safe requests.
    transport_retry: TransportRetryPolicy,
    /// Monotonic clock used to enforce the total retry limit.
    timer: Arc<dyn transport::MonotonicTimer>,
    /// Capability document cache, shared by clones and filled on first use.
    capabilities: Arc<OnceLock<CapabilityDocument>>,
}

impl Client {
    /// The configured server URL, without a trailing slash.
    pub fn server_url(&self) -> &str {
        &self.base_url
    }

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
            transport_retry: DEFAULT,
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
    /// Feature keys that are not parented by an advertised API group are
    /// dropped rather than trusted, per the spec's client guidance for
    /// malformed documents.
    pub async fn get_capabilities(&self) -> Result<CapabilityDocument> {
        if let Some(document) = self.capabilities.get() {
            return Ok(document.clone());
        }
        let url = format!("{}/v0/capabilities", self.base_url);
        let mut document: CapabilityDocument = self
            .request_json::<(), _>(self.get(&url), None, SendPolicy::Retry)
            .await?;
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

    /// Checks the server's health endpoint.
    pub async fn get_health(&self) -> Result<()> {
        let url = format!("{}/health", self.base_url);
        self.call(&self.get(&url), None, SendPolicy::Retry).await?;
        Ok(())
    }
}
