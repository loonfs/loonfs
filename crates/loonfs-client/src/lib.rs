//! Async HTTP client for a LoonFS server.
//!
//! Use [`Client`] with [`ClientConfig`] and [`NamespacePath`] to access a
//! hosted LoonFS server. Applications that embed the runtime directly should
//! use the `loonfs` crate. Both crates share operation options from
//! `loonfs-types`.
//!
//! A client is built with the [`Context`](loonfs_types::Context) that its
//! requests act in: the service context, or the principals of the user that
//! the requests act for. There's no default context, so each application
//! chooses one when it builds the client.
//!
//! The config's `actor` is sent as `Loonfs-Actor` on commits, uploads, and
//! namespace creation and forks. A client without an actor can read, delete
//! namespaces, and manage snapshots, but an operation that records an actor
//! fails before it is sent.
//! [`Client::with_actor`] clones a client with another actor.
//!
//! Namespace logic doesn't belong here: the client sends HTTP API requests
//! and decodes their responses, and the server does the work.

#![warn(missing_docs)]

mod body;
mod config;
mod downloads;
mod error;
mod maintenance;
mod mutations;
mod mutations_by_inode;
mod namespace_path;
mod namespaces;
mod operations;
mod payload;
mod query;
mod ranged_reads;
mod read_at_fields;
mod reads;
mod reqwest_transport;
mod transport;
mod transport_body;
mod transport_error;

#[cfg(test)]
mod scripted_transport;
mod snapshot_reader;
mod snapshots;
mod uploads;

use loonfs_types::config::ConfigFieldError;
use loonfs_types::{ActorId, CapabilityDocument, SecretString, SnapshotId};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tower::{util::BoxCloneSyncService, Service};
use transport::{StdMonotonicTimer, TransportRetryPolicy, DEFAULT};

pub use body::Body;
pub use config::ClientConfig;
pub use error::ClientError;
pub use maintenance::SnapshotRecordsPager;
pub use namespaces::NamespacesPager;
pub use payload::{PayloadSource, PayloadStream};
pub use query::GrepPager;
pub use ranged_reads::FileRangeStream;
pub use reads::{
    ChangesPager, FileRevisionsPager, InodeChildrenPager, PathEntriesPager, TrashPager,
};
pub use snapshot_reader::SnapshotReader;
pub use snapshots::SnapshotsPager;
pub use transport_error::TransportError;

/// Per-operation options, defined once in `loonfs-types` and shared with the
/// embedded `loonfs` runtime so the two surfaces cannot drift a field apart.
pub use loonfs_types::options::{
    AccessState, AppendFileByInodeOptions, AppendFileOptions, AttributeChanges, CommitOptions,
    Consistency, CopyOptions, CreateDirectoryOptions, CreateNamespaceOptions,
    CreateSnapshotOptions, DeleteByInodeOptions, DeleteNamespaceOptions, DeleteOptions,
    DirectMultipartUploadOptions, DownloadOptions, ForkNamespaceOptions, ListChangesOptions,
    ListOptions, MergeForkOptions, MoveOptions, PutFileOptions, ReadRange,
    RestoreDeletedDestination, RestoreDeletedOptions, StatOptions, UpdateAccessByInodeOptions,
    UpdateAccessOptions, UpdateAttributesByInodeOptions, UpdateAttributesOptions,
};

/// Result type returned by the client.
pub type Result<T> = std::result::Result<T, ClientError>;

pub use downloads::DirectDownloadStream;
pub use namespace_path::NamespacePath;
pub use uploads::staging::{
    MultipartUploadResume, PreparedContent, PutFileJournal, MIN_STREAMING_PUT_BYTES,
};

/// Async HTTP client for LoonFS.
///
/// Cloning is cheap: clones share one connection pool and one capability
/// cache.
///
/// Every read reads the live head at the client's consistency, which is
/// `eventual` on a new client. [`Self::strong`] returns a copy of the client
/// whose reads are `strong`, and [`Self::snapshot_reader`] returns a reader
/// of a snapshot.
#[derive(Debug, Clone)]
pub struct Client {
    context: loonfs_types::Context,
    /// How recently each read confirms the live head.
    consistency: Consistency,
    /// The actor sent as `Loonfs-Actor` on every mutation.
    actor: Option<ActorId>,
    base_url: String,
    auth_token: Option<SecretString>,
    transport: transport::Transport,
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
    /// Monotonic clock used for inactivity and retry limits.
    timer: Arc<dyn transport::MonotonicTimer>,
    /// Capability document cache, shared by clones and filled on first use.
    capabilities: Arc<OnceLock<CapabilityDocument>>,
}

impl Client {
    /// Returns the context that every request acts in: the service context,
    /// sent as `Loonfs-Context: service`, or a subject's principals, sent as
    /// `Loonfs-Principals`.
    pub fn context(&self) -> &loonfs_types::Context {
        &self.context
    }

    /// Clones this client with the context that every request acts in.
    pub fn with_context(&self, context: loonfs_types::Context) -> Self {
        Self {
            context,
            ..self.clone()
        }
    }

    /// Returns a copy of this client whose reads confirm the live head
    /// before they read, as a `strong` read does. Each read sends
    /// `consistency=strong`.
    pub fn strong(&self) -> Self {
        Self {
            consistency: Consistency::Strong,
            ..self.clone()
        }
    }

    /// Returns a reader of the snapshot `snapshot_id`, or of one of a
    /// namespace's fork snapshots. Each read through the reader sends
    /// `snapshot_id`.
    pub fn snapshot_reader(&self, snapshot_id: SnapshotId) -> SnapshotReader {
        SnapshotReader::new(self.clone(), snapshot_id)
    }

    /// Returns the fields of a read of the live head at this client's
    /// consistency.
    pub(crate) fn head_fields(&self) -> read_at_fields::ReadAtFields {
        read_at_fields::ReadAtFields::head(self.consistency)
    }

    /// Returns the actor sent as `Loonfs-Actor` on commits, uploads, and
    /// namespace creation and forks.
    pub fn actor(&self) -> Option<&ActorId> {
        self.actor.as_ref()
    }

    /// Clones this client with the actor that its commits, uploads, and
    /// namespace creation and forks record.
    pub fn with_actor(&self, actor: ActorId) -> Self {
        Self {
            actor: Some(actor),
            ..self.clone()
        }
    }

    /// Returns the actor for an operation that records one, or an error when
    /// the client has none.
    fn mutation_actor(&self) -> Result<&ActorId> {
        self.actor
            .as_ref()
            .ok_or(ClientError::ConfigField(ConfigFieldError::Missing {
                field: "actor",
            }))
    }

    /// The configured server URL, without a trailing slash.
    pub fn server_url(&self) -> &str {
        &self.base_url
    }

    /// Creates a client, validating the config exactly as
    /// [`ClientConfig::load`] does, so direct Rust construction cannot bypass
    /// validation.
    ///
    /// `context` is the context that every request acts in. There's no
    /// default, so a client that acts for a user can't send requests in the
    /// service context by mistake.
    pub fn new(config: ClientConfig, context: loonfs_types::Context) -> Result<Self> {
        config.validate()?;
        let service = reqwest_transport::service(&config)?;
        Self::with_transport(config, service, context)
    }

    /// Creates a client for in-process hosts and tests using an HTTP service.
    /// Retries, timeouts, and identity headers apply the same way as with
    /// [`Client::new`].
    pub fn with_transport<S>(
        config: ClientConfig,
        service: S,
        context: loonfs_types::Context,
    ) -> Result<Self>
    where
        S: Service<http::Request<Body>, Response = http::Response<Body>, Error = TransportError>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        config.validate()?;
        let request_timeout = config
            .request_timeout_ms
            .map(|timeout_ms| Duration::from_millis(timeout_ms.get()));
        Ok(Self {
            context,
            consistency: Consistency::Eventual,
            actor: config.actor,
            base_url: config.server_url.trim().trim_end_matches('/').to_owned(),
            auth_token: config.auth_token,
            transport: BoxCloneSyncService::new(service),
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
    /// Malformed feature keys, including keys outside an advertised API
    /// group, are dropped rather than trusted, per the spec's client
    /// guidance for malformed documents.
    pub async fn get_capabilities(&self) -> Result<CapabilityDocument> {
        if let Some(document) = self.capabilities.get() {
            return Ok(document.clone());
        }
        let url = format!("{}/v0/capabilities", self.base_url);
        let mut document: CapabilityDocument = self
            .send_json(self.request_without_context(operations::GET_CAPABILITIES, &url))
            .await?;
        document.retain_well_formed();
        // If a racing clone fetched first, keep its copy; both came from the
        // same server.
        let _ = self.capabilities.set(document);
        Ok(self
            .capabilities
            .get()
            .expect("the capability cache should be filled after a fetch")
            .clone())
    }

    /// Checks the server's health endpoint.
    pub async fn get_health(&self) -> Result<()> {
        let url = format!("{}/health", self.base_url);
        self.call(&self.request_without_context(operations::GET_HEALTH, &url))
            .await?;
        Ok(())
    }
}
