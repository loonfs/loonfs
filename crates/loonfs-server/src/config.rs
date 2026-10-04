//! Server configuration: strict TOML decoding of the listen address,
//! store, metadata cache limits, and runtime overrides.

use crate::local_cache::{DISK_BLOCK_BYTES, MIN_DISK_BYTES};
use loonfs::metrics::MetricsRecorder;
use loonfs::{ExecutionBudget, MetadataCache};
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::{ConfiguredObjectStore, StoreConfigError};
use loonfs_types::env::{AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV};
use loonfs_types::SecretString;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

pub use loonfs_objectstore::StoreConfig;

/// Overrides the embedded writer's inline content policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InlineContentOverrides {
    #[serde(
        serialize_with = "serialize_inline_content_threshold",
        deserialize_with = "deserialize_inline_content_threshold"
    )]
    pub inline_content_threshold_bytes: Option<usize>,
    pub inline_content_wal_object_budget_bytes: Option<usize>,
    pub inline_content_fold_at_bytes: Option<usize>,
    pub inline_content_tail_limit_bytes: Option<usize>,
}

impl Default for InlineContentOverrides {
    fn default() -> Self {
        Self {
            inline_content_threshold_bytes: loonfs::InlineContentPolicy::default()
                .inline_content_threshold_bytes,
            inline_content_wal_object_budget_bytes: None,
            inline_content_fold_at_bytes: None,
            inline_content_tail_limit_bytes: None,
        }
    }
}

fn serialize_inline_content_threshold<S>(
    threshold: &Option<usize>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match threshold {
        Some(bytes) => bytes.serialize(serializer),
        None => serializer.serialize_bool(false),
    }
}

fn deserialize_inline_content_threshold<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Threshold {
        Bytes(usize),
        Enabled(bool),
    }

    match Threshold::deserialize(deserializer)? {
        Threshold::Bytes(bytes) => Ok(Some(bytes)),
        Threshold::Enabled(false) => Ok(None),
        Threshold::Enabled(true) => Err(serde::de::Error::custom(
            "`inline_content_threshold_bytes` must be a byte count or false",
        )),
    }
}

impl InlineContentOverrides {
    pub(crate) fn resolve(&self) -> loonfs::InlineContentPolicy {
        let defaults = loonfs::InlineContentPolicy::default();
        loonfs::InlineContentPolicy {
            inline_content_threshold_bytes: self.inline_content_threshold_bytes,
            inline_content_wal_object_budget_bytes: self
                .inline_content_wal_object_budget_bytes
                .unwrap_or(defaults.inline_content_wal_object_budget_bytes),
            inline_content_fold_at_bytes: self
                .inline_content_fold_at_bytes
                .unwrap_or(defaults.inline_content_fold_at_bytes),
            inline_content_tail_limit_bytes: self
                .inline_content_tail_limit_bytes
                .unwrap_or(defaults.inline_content_tail_limit_bytes),
        }
    }
}

/// The server's `[publication]` table. Total bytes are derived when sized.
///
/// `max_requests`, `max_estimated_bytes`, and `max_concurrent_publications`
/// are limits of the server's execution budget. The two per-namespace fields
/// are the runtime's publication limits.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationLimitsOverrides {
    pub max_requests: Option<std::num::NonZeroUsize>,
    pub max_requests_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_concurrent_publications: Option<std::num::NonZeroUsize>,
}

impl PublicationLimitsOverrides {
    /// The per-namespace limits the server's runtime applies.
    pub(crate) fn resolve(&self) -> loonfs::PublicationLimits {
        let defaults = loonfs::PublicationLimits::default();
        loonfs::PublicationLimits {
            max_requests_per_namespace: self
                .max_requests_per_namespace
                .unwrap_or(defaults.max_requests_per_namespace),
            max_estimated_bytes_per_namespace: self
                .max_estimated_bytes_per_namespace
                .unwrap_or(defaults.max_estimated_bytes_per_namespace),
        }
    }
}

/// The server config file.
///
/// # Secret precedence
///
/// `auth_token` and `content_token_secret` may be supplied through the
/// `LOONFS_AUTH_TOKEN` and `LOONFS_CONTENT_TOKEN_SECRET` environment
/// variables instead of the file. A non-blank value in the file takes
/// precedence; blank environment values are ignored. Object-store credentials
/// follow the source explicitly selected by the nested `credentials` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub memory_limit_bytes: Option<usize>,
    pub auth_token: Option<SecretString>,
    /// Signs content tokens; unset or empty here falls back to
    /// `LOONFS_CONTENT_TOKEN_SECRET`.
    #[serde(default)]
    pub content_token_secret: SecretString,
    pub writer_id: String,
    /// Maximum public reads running at once. Reads past this limit wait.
    #[serde(default = "default_max_concurrent_reads")]
    pub max_concurrent_reads: usize,
    #[serde(default = "default_max_in_flight_requests")]
    pub max_in_flight_requests: usize,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Maximum WAL folds this server runs concurrently. A sustained
    /// `loonfs.execution_budget.folds_waiting` gauge means this cap is too
    /// low.
    #[serde(default = "default_max_concurrent_folds")]
    pub max_concurrent_folds: usize,
    /// Maximum metadata merges this server runs at once, bounded compaction
    /// steps and streaming compactions alike, whether writer sessions, the
    /// maintenance sweep, or maintenance requests start them. A merge never
    /// holds a fold permit.
    #[serde(default = "default_max_concurrent_compactions")]
    pub max_concurrent_compactions: usize,
    /// Shared request and concurrency limits for namespace publications.
    #[serde(default)]
    pub publication: PublicationLimitsOverrides,
    /// Defaults to a 64 KiB threshold; `false` disables inline writes in TOML.
    #[serde(default)]
    pub inline_content: InlineContentOverrides,
    /// Limits of the one metadata cache the server's runtime reads through.
    #[serde(default)]
    pub metadata_cache: MetadataCacheOverrides,
    /// The node-local cache of encoded metadata blocks, if this deployment
    /// keeps one. Absent means no local cache: every metadata block read
    /// that misses the decoded cache goes to object storage, which is the
    /// behavior of a server that never had this table.
    #[serde(default)]
    pub local_cache: Option<LocalCacheConfig>,
    /// What this server does about grep, plus the bounded-step budgets its
    /// index maintenance runs under. A config with no `[grep]` table
    /// composes no grep at all; a present table must name its `mode`.
    #[serde(default)]
    pub grep: GrepConfig,
    /// Whether this server serves maintenance requests, runs the maintenance
    /// sweep, both, or neither.
    #[serde(default)]
    pub maintenance: MaintenanceMode,
    /// Minimum interval between publication starts per namespace, in
    /// milliseconds. A request to an idle namespace publishes immediately;
    /// the interval paces requests that queued behind a publish, so hot
    /// namespaces amortize into fewer, larger WAL objects. The server default favors batch economy over
    /// the embedded default's latency bias.
    #[serde(default = "default_min_publish_interval_ms")]
    pub min_publish_interval_ms: u64,
    /// Maximum time a metadata or query request may run, in milliseconds.
    /// Streamed content and long-running operator work are exempt.
    #[serde(default = "default_request_deadline_ms")]
    pub request_deadline_ms: u64,
    /// Maximum time graceful shutdown waits for accepted requests to drain
    /// and then for the maintenance sweep to stop, in milliseconds, counted
    /// from the shutdown signal. Requests and sweep visits still running at
    /// the deadline are dropped. Writer and cache settlement continue
    /// afterward.
    #[serde(default = "default_shutdown_deadline_ms")]
    pub shutdown_deadline_ms: u64,
    /// Largest request body accepted for service-proxied upload content
    /// requests (`PUT .../uploads/{upload_id}/content`). Enforced
    /// incrementally while the body streams to the store, so it bounds the
    /// accepted transfer size, not per-request memory (streamed writes hold
    /// at most one internal part). Clients may use `direct_put` or direct
    /// multipart for larger transfers when the capability is advertised.
    /// Advertised as the `upload.service_proxied.max_content_bytes` capability limit.
    #[serde(default = "default_max_upload_bytes")]
    pub max_upload_bytes: u64,
    /// Largest file content a service-proxied read (`GET .../filesystem/
    /// content` and inode revision content) will stream and return. Checked
    /// against resolved metadata before fetching content bytes; over-limit reads
    /// answer `content_too_large`. Advertised to clients as the
    /// `download.service_proxied.max_content_bytes` capability limit.
    #[serde(default = "default_max_download_bytes")]
    pub max_download_bytes: u64,
    /// Largest `ttl_ms` accepted by snapshot create and extend requests.
    /// Advertised as the `snapshot.max_ttl_ms` capability limit.
    #[serde(default = "default_snapshot_max_ttl_ms")]
    pub snapshot_max_ttl_ms: u64,
    /// Largest snapshot lifetime measured from its creation time. Extensions
    /// cannot pass this ceiling. Advertised as the
    /// `snapshot.max_lifetime_ms` capability limit.
    #[serde(default = "default_snapshot_max_lifetime_ms")]
    pub snapshot_max_lifetime_ms: u64,
    /// Maximum live snapshots per namespace. Advertised as
    /// the `snapshot.max_live_per_namespace` capability limit.
    #[serde(default = "default_snapshot_max_live_per_namespace")]
    pub snapshot_max_live_per_namespace: usize,
    /// How many proxied upload bodies the server will stream at once;
    /// requests past the cap answer `server_busy` before any transfer.
    /// Worst-case upload memory is this times one streamed part, since
    /// bodies forward to the store incrementally instead of buffering.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// How many proxied content streams the server will serve at once;
    /// requests past the cap answer `server_busy` before any fetch.
    /// Content memory follows this count times the internal read chunk size,
    /// independent of `max_download_bytes`. Admission lasts until the body
    /// finishes or is dropped.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,
    /// How many namespaces the maintenance sweep visits at once. Folds and
    /// merges also wait for the fold and compaction permits. Defaults to 8.
    #[serde(default = "default_max_concurrent_maintenance")]
    pub max_concurrent_maintenance: usize,
    /// Milliseconds between ticks of held sessions. Defaults to 5000.
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    /// Milliseconds before retrying unfinished metadata. Defaults to 300000.
    #[serde(default = "default_maintenance_interval_ms")]
    pub maintenance_interval_ms: u64,
    /// Least milliseconds between collections for sessions whose seq moved. Defaults to 3600000 (1 hour).
    #[serde(default = "default_gc_interval_ms")]
    pub gc_interval_ms: u64,
    /// Milliseconds between full passes over every namespace, with collection.
    /// The first runs at start. Defaults to 86400000 (24 hours).
    #[serde(default = "default_full_sweep_interval_ms")]
    pub full_sweep_interval_ms: u64,
    /// Idle milliseconds before a caught-up session with no request handle
    /// closes. A later write opens a new session. Defaults to 30 minutes.
    #[serde(default = "default_idle_session_close_after_ms")]
    pub idle_session_close_after_ms: u64,
    /// Decoded metadata bytes one maintenance step may merge. A step merges
    /// inline only the runs that fit; a larger window runs as a streaming
    /// compaction that holds at most this much at once. Derived when sized.
    pub max_merge_input_bytes: Option<usize>,
    /// Minimum interval between checks for a successor to a cached manifest,
    /// in milliseconds. Zero checks on every read. Unset keeps the runtime
    /// default of 1000.
    pub manifest_revalidation_interval_ms: Option<u64>,
    /// Bytes retained by all read, publication, and fold memos. Zero keeps
    /// none. Derived when sized; otherwise defaults to 256 MiB.
    pub max_read_working_bytes: Option<usize>,
    /// How old a WAL tail's newest commit must be before maintenance folds
    /// a tail that is below the fold thresholds, in milliseconds. The
    /// maintenance sweep and explicit `metadata` requests use the same
    /// period. Zero turns the rule off. Defaults to 15 minutes.
    #[serde(default = "default_idle_fold_after_ms")]
    pub idle_fold_after_ms: u64,
    /// Allows serving on a non-loopback address with `auth_token` unset.
    /// Off by default: exposing every endpoint unauthenticated is almost
    /// always a misconfiguration, so validation rejects it unless this is
    /// explicitly set.
    #[serde(default)]
    pub allow_unauthenticated_remote: bool,
    /// Allows serving on a non-loopback address in plaintext. Off by
    /// default for the same reason as `allow_unauthenticated_remote`: the
    /// wire carries the bearer token and the presigned object-store URLs
    /// the upload routes hand back, so plaintext beyond localhost is almost
    /// always a misconfiguration rather than a choice. Set it where TLS
    /// terminates in front of this process.
    #[serde(default)]
    pub allow_remote_without_tls: bool,
    /// Terminates TLS in this process when present. Absent means plaintext
    /// HTTP, which validation only accepts on a loopback bind or with
    /// `allow_remote_without_tls`.
    #[serde(default)]
    pub tls: Option<TlsServerConfig>,
    pub store: StoreConfig,
}

/// The server's TLS identity: one certificate chain and its private key,
/// both read at startup. A file that is missing, unreadable, or not the PEM
/// it claims to be fails the process rather than degrading to plaintext.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsServerConfig {
    /// PEM certificate chain, leaf first.
    pub cert_path: String,
    /// PEM private key: PKCS#8, PKCS#1 (RSA), or SEC1.
    pub key_path: String,
}

fn default_min_publish_interval_ms() -> u64 {
    1_000
}

fn default_request_deadline_ms() -> u64 {
    loonfs_http::DEFAULT_REQUEST_DEADLINE_MS
}

fn default_shutdown_deadline_ms() -> u64 {
    // Clears loonfs_objectstore::PROVIDER_OPERATION_DEADLINE so an accepted
    // request can finish the provider operation it already started.
    600_000
}

fn default_max_upload_bytes() -> u64 {
    256 * 1024 * 1024
}

fn default_max_download_bytes() -> u64 {
    // Mirrors the upload default so anything the proxy accepted, the proxy
    // will serve back. Content ingested past this through `direct_put`
    // needs a raised limit to be read through the server.
    256 * 1024 * 1024
}

fn default_snapshot_max_ttl_ms() -> u64 {
    loonfs::SnapshotPolicy::default().max_ttl_ms
}

fn default_snapshot_max_lifetime_ms() -> u64 {
    loonfs::SnapshotPolicy::default().max_lifetime_ms
}

fn default_snapshot_max_live_per_namespace() -> usize {
    loonfs::SnapshotPolicy::default().max_live_per_namespace
}

fn default_max_in_flight_requests() -> usize {
    256
}

fn default_max_connections() -> usize {
    1024
}

fn default_max_concurrent_uploads() -> usize {
    loonfs_http::DEFAULT_MAX_CONCURRENT_UPLOADS
}

fn default_max_concurrent_reads() -> usize {
    loonfs::DEFAULT_MAX_CONCURRENT_READS
}

fn default_max_concurrent_folds() -> usize {
    loonfs::DEFAULT_MAX_CONCURRENT_FOLDS
}

fn default_max_concurrent_compactions() -> usize {
    loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS
}

fn default_max_concurrent_downloads() -> usize {
    loonfs_http::DEFAULT_MAX_CONCURRENT_DOWNLOADS
}

fn default_max_concurrent_maintenance() -> usize {
    8
}

fn default_tick_interval_ms() -> u64 {
    5_000
}

fn default_maintenance_interval_ms() -> u64 {
    300_000
}

fn default_gc_interval_ms() -> u64 {
    3_600_000
}

fn default_full_sweep_interval_ms() -> u64 {
    86_400_000
}

fn default_idle_session_close_after_ms() -> u64 {
    1_800_000
}

fn default_max_merge_input_bytes() -> usize {
    loonfs_types::format::sst_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES
}

fn default_idle_fold_after_ms() -> u64 {
    loonfs::MetadataMaintenanceOptions::default().idle_fold_after_ms
}

/// The server's `[local_cache]` table: where the node-local cache of encoded
/// metadata blocks lives, and how much memory and disk it may use.
///
/// The directory is this process's alone while it runs, and it holds nothing
/// durable. Object storage remains the authority for every block the cache
/// answers, so the directory can be deleted whenever the process is not
/// running and the only cost is a cold cache.
///
/// Everything else about the tier — its on-disk layout, how many flushers
/// write it, how large its write buffers are — is fixed in the
/// implementation. Those are engine-tuning numbers, not deployment
/// decisions, and they stay out of configuration until measurement says
/// otherwise.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCacheConfig {
    /// Directory the cache owns. Created if missing; locked while this
    /// process runs, so two servers cannot share one.
    pub path: String,
    /// Bytes of memory the cache's in-memory tier may hold. The disk tier's
    /// write buffers and queued inserts are not counted here.
    pub memory_bytes: u64,
    /// Bytes of disk the cache's disk tier may hold. The tier claims this
    /// much space up front, in whole blocks, one file per block under
    /// `path`; six blocks is the smallest tier that may be configured.
    /// Raising this across a restart keeps what the directory holds, and
    /// lowering it starts the directory empty rather than leaving the
    /// blocks it no longer claims behind.
    pub disk_bytes: u64,
}

/// The server's `[metadata_cache]` table: the limits of the one metadata
/// cache the server builds at startup. Omitted fields are derived when sized.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheOverrides {
    pub max_segment_bytes: Option<usize>,
    pub max_head_state_bytes: Option<usize>,
}

/// What this server does about maintenance: serve the API group, run the
/// maintenance sweep, both, or neither.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceMode {
    /// Neither serve the maintenance API group nor run the sweep.
    Disabled,
    /// Serve explicit maintenance requests without running the sweep.
    ServeOnly,
    /// Run the sweep without serving the maintenance API group.
    MaintainOnly,
    /// Serve explicit maintenance requests and run the sweep.
    #[default]
    ServeAndMaintain,
}

impl MaintenanceMode {
    /// Whether this server serves the maintenance API group.
    pub fn serves(self) -> bool {
        matches!(self, Self::ServeOnly | Self::ServeAndMaintain)
    }

    /// Whether this server runs the maintenance sweep.
    pub fn maintains(self) -> bool {
        matches!(self, Self::MaintainOnly | Self::ServeAndMaintain)
    }
}

/// What this server does about grep: answer queries, keep the index built,
/// both, or neither.
///
/// The two jobs are independent. A read replica can serve searches over an
/// index another process maintains; a write node can maintain the index for
/// namespaces it never answers searches about; the reference deployment
/// does both. Every combination is named here, so none has to be validated
/// away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrepMode {
    /// Neither answer grep queries nor maintain the index.
    Disabled,
    /// Answer queries over an index some other process maintains.
    ServeOnly,
    /// Maintain the index without answering queries about it.
    MaintainOnly,
    /// Answer queries and maintain the index in this process.
    ServeAndMaintain,
}

impl GrepMode {
    /// Whether the grep query endpoint is supported.
    pub fn serves_grep(self) -> bool {
        matches!(self, Self::ServeOnly | Self::ServeAndMaintain)
    }

    /// Whether this server serves the index-maintenance endpoints, and
    /// whether its maintenance sweep builds and collects the index.
    pub fn maintains_index(self) -> bool {
        matches!(self, Self::MaintainOnly | Self::ServeAndMaintain)
    }
}

/// The server's `[grep]` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepConfig {
    pub mode: GrepMode,
    pub max_cache_bytes: Option<usize>,
    /// Flattening preserves the existing `[grep]` keys while leaving the
    /// worker policy itself as their one in-memory owner.
    #[serde(flatten)]
    pub worker: GrepWorkerConfig,
}

impl Default for GrepConfig {
    fn default() -> Self {
        Self {
            mode: GrepMode::Disabled,
            max_cache_bytes: None,
            worker: GrepWorkerConfig::default(),
        }
    }
}

impl GrepConfig {
    /// Returns the shared bounded-step configuration represented by this table.
    pub fn worker_config(self) -> GrepWorkerConfig {
        self.worker
    }
}

#[derive(Debug, Error)]
pub enum ServerConfigError {
    #[error("failed to read config: {0}")]
    Io(String),
    #[error("failed to decode config: {0}")]
    Decode(String),
    #[error("missing `{field}`")]
    MissingField { field: &'static str },
    #[error("invalid `{field}`: {reason}")]
    InvalidField { field: &'static str, reason: String },
}

impl ServerConfig {
    /// Fills `auth_token` and `content_token_secret` from the environment
    /// when the file left them unset or blank. Non-blank file values win;
    /// blank environment values are ignored.
    fn apply_env_fallbacks(
        &mut self,
        auth_token_env: Option<String>,
        content_token_secret_env: Option<String>,
    ) {
        if self
            .auth_token
            .as_ref()
            .is_none_or(|token| token.expose().trim().is_empty())
        {
            if let Some(token) = non_blank(auth_token_env) {
                self.auth_token = Some(SecretString::new(token));
            }
        }
        if self.content_token_secret.expose().trim().is_empty() {
            if let Some(secret) = non_blank(content_token_secret_env) {
                self.content_token_secret = SecretString::new(secret);
            }
        }
    }

    /// Builds the object store selected by this server configuration.
    pub fn object_store(&self) -> Result<ConfiguredObjectStore, ServerConfigError> {
        self.store
            .configured_object_store()
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store",
                reason: err.public_message().into_owned(),
            })
    }

    /// The line `loonfs-server --check-config` prints when the config loads.
    ///
    /// It names the address, provider, and maintenance settings.
    pub fn check_summary(&self) -> String {
        let maintenance = match self.maintenance {
            MaintenanceMode::Disabled => "disabled",
            MaintenanceMode::ServeOnly => "serve_only",
            MaintenanceMode::MaintainOnly => "maintain_only",
            MaintenanceMode::ServeAndMaintain => "serve_and_maintain",
        };
        format!(
            "config ok: bind {}, store {}, maintenance {}",
            self.bind.trim(),
            self.store.kind().as_str(),
            maintenance
        )
    }

    /// Parses the bind address; the one authority for that conversion, used
    /// by validation and by serving.
    pub(crate) fn bind_addr(&self) -> Result<SocketAddr, ServerConfigError> {
        validate_socket_addr("bind", &self.bind)
    }

    pub(crate) fn validate(&self) -> Result<MemorySizing, ServerConfigError> {
        let bind = self.bind_addr()?;
        require_non_empty("writer_id", &self.writer_id)?;

        if let Some(token) = &self.auth_token {
            if token.expose().trim().is_empty() {
                return Err(ServerConfigError::InvalidField {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
        } else if bind_serves_beyond_localhost(&bind) && !self.allow_unauthenticated_remote {
            return Err(ServerConfigError::InvalidField {
                field: "auth_token",
                reason: format!(
                    "bind `{bind}` serves every endpoint to the network without \
                     authentication; set `auth_token` (or `LOONFS_AUTH_TOKEN`), \
                     or set `allow_unauthenticated_remote = true` to serve open \
                     on purpose"
                ),
            });
        }
        if let Some(tls) = &self.tls {
            require_non_empty("tls.cert_path", &tls.cert_path)?;
            require_non_empty("tls.key_path", &tls.key_path)?;
        } else if bind_serves_beyond_localhost(&bind) && !self.allow_remote_without_tls {
            return Err(ServerConfigError::InvalidField {
                field: "tls",
                reason: format!(
                    "bind `{bind}` serves the network in plaintext, exposing the \
                     bearer token and the presigned object-store URLs in upload \
                     responses; configure `[tls]` with `cert_path` and `key_path`, \
                     or set `allow_remote_without_tls = true` when TLS terminates \
                     in front of this process"
                ),
            });
        }
        for (field, value) in [
            ("max_upload_bytes", self.max_upload_bytes),
            ("request_deadline_ms", self.request_deadline_ms),
            ("shutdown_deadline_ms", self.shutdown_deadline_ms),
            ("max_download_bytes", self.max_download_bytes),
            ("snapshot_max_ttl_ms", self.snapshot_max_ttl_ms),
            ("snapshot_max_lifetime_ms", self.snapshot_max_lifetime_ms),
            (
                "snapshot_max_live_per_namespace",
                self.snapshot_max_live_per_namespace as u64,
            ),
            ("max_concurrent_reads", self.max_concurrent_reads as u64),
            ("max_concurrent_folds", self.max_concurrent_folds as u64),
            (
                "max_concurrent_compactions",
                self.max_concurrent_compactions as u64,
            ),
            ("max_concurrent_uploads", self.max_concurrent_uploads as u64),
            (
                "max_concurrent_downloads",
                self.max_concurrent_downloads as u64,
            ),
            (
                "max_merge_input_bytes",
                self.max_merge_input_bytes
                    .unwrap_or(default_max_merge_input_bytes()) as u64,
            ),
        ] {
            require_positive(field, value, None)?;
        }
        for (field, value) in [
            ("max_in_flight_requests", self.max_in_flight_requests),
            ("max_connections", self.max_connections),
        ] {
            require_positive(field, value as u64, None)?;
            if value > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(ServerConfigError::InvalidField {
                    field,
                    reason: format!("must not exceed {}", tokio::sync::Semaphore::MAX_PERMITS),
                });
            }
        }
        if self.snapshot_max_ttl_ms > self.snapshot_max_lifetime_ms {
            return Err(ServerConfigError::InvalidField {
                field: "snapshot_max_ttl_ms",
                reason: "must not exceed `snapshot_max_lifetime_ms`".to_owned(),
            });
        }
        for (field, value) in [
            (
                "max_concurrent_maintenance",
                self.max_concurrent_maintenance as u64,
            ),
            ("tick_interval_ms", self.tick_interval_ms),
            ("maintenance_interval_ms", self.maintenance_interval_ms),
            ("gc_interval_ms", self.gc_interval_ms),
            ("full_sweep_interval_ms", self.full_sweep_interval_ms),
            (
                "idle_session_close_after_ms",
                self.idle_session_close_after_ms,
            ),
        ] {
            require_positive(
                field,
                value,
                Some("set `maintenance = \"serve_only\"` to turn the maintenance sweep off"),
            )?;
        }
        if let Some(local_cache) = &self.local_cache {
            require_non_empty("local_cache.path", &local_cache.path)?;
            require_positive(
                "local_cache.memory_bytes",
                local_cache.memory_bytes,
                Some("omit the `[local_cache]` table to run without a local cache"),
            )?;
            // The disk tier allocates whole blocks and never a partial one,
            // so a capacity under the floor is a cache that starts, holds
            // nothing on disk, and says nothing about it. Refuse it here
            // instead.
            if local_cache.disk_bytes < MIN_DISK_BYTES {
                return Err(ServerConfigError::InvalidField {
                    field: "local_cache.disk_bytes",
                    reason: format!(
                        "must be at least {MIN_DISK_BYTES}; the disk tier allocates whole \
                         blocks of {DISK_BLOCK_BYTES} bytes, and six blocks is the floor \
                         for stable operation. Omit the `[local_cache]` table to run \
                         without a local cache"
                    ),
                });
            }
        }
        if let Err(error) = self.grep.worker_config().validate() {
            return Err(ServerConfigError::InvalidField {
                field: "grep",
                reason: error.to_string(),
            });
        }
        require_non_empty("content_token_secret", self.content_token_secret.expose())?;
        self.store.validate()?;

        let cgroup_limit = if self.memory_limit_bytes.is_none() && cfg!(target_os = "linux") {
            read_cgroup_memory_limit(&[
                Path::new("/sys/fs/cgroup/memory.max"),
                Path::new("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            ])
        } else {
            None
        };
        self.memory_sizing(cgroup_limit)
    }

    /// Builds the execution budget of the server's one runtime from its fold,
    /// compaction, and merge input limits and the totals of its
    /// `[publication]` table, reporting to `recorder`.
    pub(crate) fn execution_budget(
        &self,
        memory: &MemorySizing,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> ExecutionBudget {
        let positive = |value: usize| {
            std::num::NonZeroUsize::new(value).expect("validated budget limits should be nonzero")
        };
        let mut builder = ExecutionBudget::builder()
            .max_concurrent_reads(positive(self.max_concurrent_reads))
            .max_concurrent_folds(positive(self.max_concurrent_folds))
            .max_concurrent_compactions(positive(self.max_concurrent_compactions))
            .max_merge_input_bytes(positive(memory.merge_input.bytes))
            .max_read_working_bytes(memory.read_working.bytes)
            .max_admitted_bytes(positive(memory.publication.bytes))
            .metrics_recorder(recorder);
        if let Some(limit) = self.publication.max_requests {
            builder = builder.max_admitted_requests(limit);
        }
        if let Some(limit) = self.publication.max_concurrent_publications {
            builder = builder.max_concurrent_publications(limit);
        }
        builder.build()
    }
}

const MIB: usize = 1024 * 1024;

#[derive(Debug)]
pub(crate) struct MemoryTerm {
    name: &'static str,
    pub(crate) bytes: usize,
    source: &'static str,
}

impl MemoryTerm {
    fn new(
        name: &'static str,
        explicit: Option<usize>,
        derived: Option<usize>,
        default: usize,
    ) -> Self {
        let (bytes, source) = match (explicit, derived) {
            (Some(bytes), _) => (bytes, "set"),
            (None, Some(bytes)) => (bytes, "derived"),
            (None, None) => (default, "default"),
        };
        Self {
            name,
            bytes,
            source,
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemorySizing {
    limit: Option<MemoryTerm>,
    fixed: [(&'static str, u128); 3],
    headroom: usize,
    managed: usize,
    segment: MemoryTerm,
    head_state: MemoryTerm,
    pub(crate) grep: MemoryTerm,
    read_working: MemoryTerm,
    publication: MemoryTerm,
    merge_input: MemoryTerm,
    compactions: usize,
    serves_grep: bool,
}

impl MemorySizing {
    fn terms(&self) -> [(&MemoryTerm, usize); 6] {
        [
            (&self.segment, 1),
            (&self.head_state, 1),
            (&self.grep, usize::from(self.serves_grep)),
            (&self.read_working, 1),
            (&self.publication, 1),
            (&self.merge_input, self.compactions),
        ]
    }

    pub(crate) fn metadata_cache(&self, recorder: Arc<dyn MetricsRecorder>) -> MetadataCache {
        MetadataCache::builder()
            .max_segment_bytes(self.segment.bytes)
            .max_head_state_bytes(self.head_state.bytes)
            .metrics_recorder(recorder)
            .build()
    }

    pub(crate) fn log(&self) {
        if let Some(limit) = &self.limit {
            tracing::info!(
                term = limit.name,
                bytes = limit.bytes,
                source = limit.source,
                "server memory"
            );
            for (term, bytes) in self.fixed.into_iter().chain([
                ("headroom", self.headroom as u128),
                ("managed", self.managed as u128),
            ]) {
                tracing::info!(term, bytes = %bytes, source = "derived", "server memory");
            }
        } else {
            tracing::info!("server memory is unsized; no memory limit was configured or found");
        }
        for (term, count) in self.terms() {
            tracing::info!(
                term = term.name,
                bytes = term.bytes,
                source = term.source,
                count,
                "server memory"
            );
        }
    }

    fn validate(&self) -> Result<(), ServerConfigError> {
        let Some(limit) = &self.limit else {
            return Ok(());
        };
        let fixed: u128 = self.fixed.iter().map(|(_, bytes)| bytes).sum();
        let pools = self.terms().iter().fold(0u128, |total, (term, count)| {
            total.saturating_add(term.bytes as u128 * *count as u128)
        });
        let total = fixed
            .saturating_add(self.headroom as u128)
            .saturating_add(pools);
        if self.managed >= 256 * MIB && total <= limit.bytes as u128 {
            return Ok(());
        }
        let mut terms: Vec<String> = self
            .fixed
            .iter()
            .map(|(name, bytes)| format!("{name} = {bytes}"))
            .collect();
        terms.extend(
            self.terms()
                .map(|(term, count)| format!("`{}` = {} x {count}", term.name, term.bytes)),
        );
        Err(ServerConfigError::InvalidField {
            field: "memory_limit_bytes",
            reason: format!(
                "memory terms must fit the limit and managed memory must be at least {} bytes; limit = {}, headroom = {}, managed = {}, total = {}; {}",
                256 * MIB, limit.bytes, self.headroom, self.managed, total, terms.join(", ")
            ),
        })
    }
}

impl ServerConfig {
    fn memory_sizing(
        &self,
        cgroup_limit: Option<usize>,
    ) -> Result<MemorySizing, ServerConfigError> {
        let limit = self.memory_limit_bytes.or(cgroup_limit);
        let fixed = [
            ("process", (128 * MIB) as u128),
            (
                "connections (`max_connections` x 472 KiB)",
                self.max_connections as u128 * 472 * 1024,
            ),
            (
                "transfers ((`max_concurrent_uploads` + `max_concurrent_downloads`) x 8 MiB)",
                (self.max_concurrent_uploads as u128 + self.max_concurrent_downloads as u128)
                    * (8 * MIB) as u128,
            ),
        ];
        let headroom = limit.unwrap_or(0) / 10;
        let fixed_bytes: u128 = fixed.iter().map(|(_, bytes)| bytes).sum();
        let managed =
            (limit.unwrap_or(0) as u128).saturating_sub(fixed_bytes + headroom as u128) as usize;
        let share =
            |percent: usize| limit.map(|_| (managed as u128 * percent as u128 / 100) as usize);
        let serves_grep = self.grep.mode.serves_grep();
        require_positive(
            "max_concurrent_compactions",
            self.max_concurrent_compactions as u64,
            None,
        )?;
        let memory = MemorySizing {
            limit: limit.map(|bytes| {
                MemoryTerm::new(
                    "memory_limit_bytes",
                    self.memory_limit_bytes,
                    Some(bytes),
                    0,
                )
            }),
            fixed,
            headroom,
            managed,
            segment: MemoryTerm::new(
                "metadata_cache.max_segment_bytes",
                self.metadata_cache.max_segment_bytes,
                share(if serves_grep { 40 } else { 50 }),
                loonfs::DEFAULT_MAX_SEGMENT_BYTES,
            ),
            head_state: MemoryTerm::new(
                "metadata_cache.max_head_state_bytes",
                self.metadata_cache.max_head_state_bytes,
                share(10),
                loonfs::DEFAULT_MAX_HEAD_STATE_BYTES,
            ),
            grep: MemoryTerm::new(
                "grep.max_cache_bytes",
                self.grep.max_cache_bytes,
                share(if serves_grep { 10 } else { 0 }),
                loonfs_grep::DEFAULT_GREP_BLOCK_CACHE_DECODED_BYTES,
            ),
            read_working: MemoryTerm::new(
                "max_read_working_bytes",
                self.max_read_working_bytes,
                share(20),
                loonfs::DEFAULT_MAX_READ_WORKING_BYTES,
            ),
            publication: MemoryTerm::new(
                "publication.max_estimated_bytes",
                self.publication
                    .max_estimated_bytes
                    .map(std::num::NonZeroUsize::get),
                share(10),
                64 * MIB,
            ),
            merge_input: MemoryTerm::new(
                "max_merge_input_bytes",
                self.max_merge_input_bytes,
                share(10).map(|bytes| bytes / self.max_concurrent_compactions),
                default_max_merge_input_bytes(),
            ),
            compactions: self.max_concurrent_compactions,
            serves_grep,
        };
        memory.validate()?;
        require_positive(
            "max_merge_input_bytes",
            memory.merge_input.bytes as u64,
            None,
        )?;
        Ok(memory)
    }
}

fn read_cgroup_memory_limit(paths: &[&Path]) -> Option<usize> {
    paths.iter().find_map(|path| {
        let bytes = fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()?;
        (bytes < 1 << 60)
            .then(|| usize::try_from(bytes).ok())
            .flatten()
    })
}

impl From<StoreConfigError> for ServerConfigError {
    fn from(error: StoreConfigError) -> Self {
        match error {
            StoreConfigError::MissingField { field } => ServerConfigError::MissingField { field },
            StoreConfigError::InvalidField { field, reason } => {
                ServerConfigError::InvalidField { field, reason }
            }
            error => ServerConfigError::InvalidField {
                field: "store",
                reason: error.to_string(),
            },
        }
    }
}

/// Loads and validates a server configuration from TOML.
pub fn load_server_config(path: impl AsRef<Path>) -> Result<ServerConfig, ServerConfigError> {
    let bytes = fs::read(path.as_ref()).map_err(|err| ServerConfigError::Io(err.to_string()))?;
    let source =
        std::str::from_utf8(&bytes).map_err(|err| ServerConfigError::Decode(err.to_string()))?;
    parse_server_config(source)
}

/// Parses and validates a server configuration supplied directly as TOML.
///
/// Container hosts that cannot mount a configuration file use this entry
/// point. Provider and server secrets should still come from their dedicated
/// environment variables instead of being included in `source`.
pub fn parse_server_config(source: &str) -> Result<ServerConfig, ServerConfigError> {
    let mut config: ServerConfig =
        toml::from_str(source).map_err(|err| ServerConfigError::Decode(err.to_string()))?;
    config.apply_env_fallbacks(
        env::var(AUTH_TOKEN_ENV).ok(),
        env::var(CONTENT_TOKEN_SECRET_ENV).ok(),
    );
    config.validate()?;
    config.object_store()?;
    Ok(config)
}

fn non_blank(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ServerConfigError> {
    if value.trim().is_empty() {
        Err(ServerConfigError::MissingField { field })
    } else {
        Ok(())
    }
}

fn require_positive(
    field: &'static str,
    value: u64,
    hint: Option<&str>,
) -> Result<(), ServerConfigError> {
    if value > 0 {
        return Ok(());
    }
    let mut reason = "must be greater than zero".to_owned();
    if let Some(hint) = hint {
        reason.push_str("; ");
        reason.push_str(hint);
    }
    Err(ServerConfigError::InvalidField { field, reason })
}

fn validate_socket_addr(field: &'static str, value: &str) -> Result<SocketAddr, ServerConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerConfigError::MissingField { field });
    }
    trimmed
        .parse::<SocketAddr>()
        .map_err(|err| ServerConfigError::InvalidField {
            field,
            reason: err.to_string(),
        })
}

/// Whether a bind address accepts connections from other hosts: any
/// non-loopback ip, including the unspecified addresses (`0.0.0.0`, `[::]`)
/// that bind every interface.
fn bind_serves_beyond_localhost(addr: &SocketAddr) -> bool {
    !addr.ip().is_loopback()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Config tests use panic in unexpected match arms for precise diagnostics.

    use super::{
        load_server_config, InlineContentOverrides, PublicationLimitsOverrides, ServerConfigError,
        AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV, DISK_BLOCK_BYTES, MIN_DISK_BYTES,
    };
    use loonfs_test_support::EnvGuard;
    use std::fs;
    use tempfile::tempdir;

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    fn memory_config(settings: &str) -> super::ServerConfig {
        toml::from_str(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "memory-test"
content_token_secret = "test-secret"
{settings}
[store]
kind = "local-fs"
root = "unused"
"#
        ))
        .expect("parse memory config")
    }

    #[test]
    fn four_gib_derives_each_pool_and_folds_unused_grep_into_segments() {
        let mut config = memory_config("memory_limit_bytes = 4294967296");
        for mode in [
            super::GrepMode::ServeOnly,
            super::GrepMode::Disabled,
            super::GrepMode::MaintainOnly,
        ] {
            config.grep.mode = mode;
            let memory = config.memory_sizing(Some(1)).expect("explicit limit wins");
            assert_eq!(
                memory.fixed.map(|(_, bytes)| bytes),
                [134_217_728, 494_927_872, 201_326_592]
            );
            assert_eq!(memory.headroom, 429_496_729);
            assert_eq!(memory.managed, 3_034_998_375);
            assert_eq!(
                memory.segment.bytes,
                if mode.serves_grep() {
                    1_213_999_350
                } else {
                    1_517_499_187
                }
            );
            assert_eq!(memory.head_state.bytes, 303_499_837);
            assert_eq!(
                memory.grep.bytes,
                if mode.serves_grep() { 303_499_837 } else { 0 }
            );
            assert_eq!(memory.read_working.bytes, 606_999_675);
            assert_eq!(memory.publication.bytes, 303_499_837);
            assert_eq!(memory.merge_input.bytes, 151_749_918);
            for (term, _) in memory.terms() {
                assert_eq!(term.source, "derived");
            }
        }
        assert_eq!(config.max_concurrent_reads, 64);
        assert_eq!(config.max_concurrent_folds, 2);
        assert_eq!(config.max_concurrent_compactions, 2);
        assert_eq!(config.max_in_flight_requests, 256);
        assert_eq!(config.max_connections, 1024);
        assert_eq!(config.max_concurrent_uploads, 8);
        assert_eq!(config.max_concurrent_downloads, 16);
        assert_eq!(config.publication.max_concurrent_publications, None);
    }

    #[test]
    fn explicit_memory_limits_survive_derivation_and_still_must_fit() {
        let mut config = memory_config(
            r#"
memory_limit_bytes = 4294967296
[metadata_cache]
max_segment_bytes = 67108864
"#,
        );
        let memory = config.validate().expect("smaller segment cache fits");
        assert_eq!(memory.segment.bytes, 67_108_864);
        assert_eq!(memory.segment.source, "set");
        assert_eq!(memory.head_state.bytes, 303_499_837);
        config.metadata_cache.max_segment_bytes = Some(4 * 1024 * super::MIB);
        let error = config.validate().expect_err("overrides must fit");
        assert!(error
            .to_string()
            .contains("`metadata_cache.max_segment_bytes` = 4294967296"));
        assert_invalid_field(error, "memory_limit_bytes");

        let mut config = memory_config(
            r#"
memory_limit_bytes = 4294967296
max_connections = 512
max_concurrent_uploads = 4
max_concurrent_downloads = 4
max_concurrent_compactions = 4
max_read_working_bytes = 0
max_merge_input_bytes = 67108864
[metadata_cache]
max_segment_bytes = 67108864
max_head_state_bytes = 0
[publication]
max_estimated_bytes = 8388608
[grep]
mode = "serve_only"
max_cache_bytes = 0
"#,
        );
        let memory = config.validate().expect("explicit pools fit");
        assert_eq!(
            memory.fixed.map(|(_, bytes)| bytes),
            [134_217_728, 247_463_936, 67_108_864]
        );
        assert_eq!(
            memory.terms().map(|(term, count)| (term.bytes, count)),
            [
                (67_108_864, 1),
                (0, 1),
                (0, 1),
                (0, 1),
                (8_388_608, 1),
                (67_108_864, 4)
            ]
        );
        assert!(memory.terms().iter().all(|(term, _)| term.source == "set"));
        config.max_merge_input_bytes = Some(1024 * super::MIB);
        assert_invalid_field(
            config.validate().expect_err("all four merges must fit"),
            "memory_limit_bytes",
        );
    }

    #[test]
    fn insufficient_memory_names_every_term_before_startup() {
        for limit in [512 * super::MIB, 1024 * super::MIB] {
            let config = memory_config(&format!("memory_limit_bytes = {limit}"));
            let error = config
                .validate()
                .expect_err("managed memory is below 256 MiB");
            let message = error.to_string();
            for term in [
                "memory_limit_bytes",
                "process",
                "max_connections",
                "max_concurrent_uploads",
                "max_concurrent_downloads",
                "headroom",
                "managed",
                "268435456",
                "metadata_cache.max_segment_bytes",
                "metadata_cache.max_head_state_bytes",
                "grep.max_cache_bytes",
                "max_read_working_bytes",
                "publication.max_estimated_bytes",
                "max_merge_input_bytes",
            ] {
                assert!(message.contains(term), "missing {term}: {message}");
            }
            assert_invalid_field(error, "memory_limit_bytes");
        }
    }

    #[test]
    fn no_limit_or_cgroup_file_keeps_the_unsized_defaults() {
        let directory = tempdir().expect("tempdir");
        let missing = directory.path().join("memory.max");
        let config = memory_config("");
        let memory = config
            .memory_sizing(super::read_cgroup_memory_limit(&[&missing]))
            .expect("unsized config");
        assert!(memory.limit.is_none());
        assert_eq!(memory.segment.bytes, 268_435_456);
        assert_eq!(memory.head_state.bytes, 67_108_864);
        assert_eq!(memory.grep.bytes, 268_435_456);
        assert_eq!(memory.read_working.bytes, 268_435_456);
        assert_eq!(memory.publication.bytes, 67_108_864);
        assert_eq!(memory.merge_input.bytes, 67_108_864);
        assert!(memory
            .terms()
            .iter()
            .all(|(term, _)| term.source == "default"));
    }

    #[test]
    fn cgroup_limits_accept_finite_numbers_and_skip_absent_or_unlimited_files() {
        let directory = tempdir().expect("tempdir");
        let v2 = directory.path().join("memory.max");
        let v1 = directory.path().join("memory.limit_in_bytes");
        assert_eq!(super::read_cgroup_memory_limit(&[&v2, &v1]), None);
        fs::write(&v1, "2147483648\n").expect("write v1 limit");
        for value in [
            "max\n",
            "9223372036854771712",
            "1152921504606846976",
            "invalid",
        ] {
            fs::write(&v2, value).expect("write v2 limit");
            assert_eq!(super::read_cgroup_memory_limit(&[&v2]), None);
            assert_eq!(
                super::read_cgroup_memory_limit(&[&v2, &v1]),
                Some(2_147_483_648)
            );
        }
        fs::write(&v2, "4294967296\n").expect("write finite v2 limit");
        let limit = super::read_cgroup_memory_limit(&[&v2, &v1]);
        assert_eq!(limit, Some(4_294_967_296));
        let memory = memory_config("")
            .memory_sizing(limit)
            .expect("cgroup sizes config");
        assert_eq!(memory.managed, 3_034_998_375);
        assert_eq!(memory.limit.expect("cgroup limit").source, "derived");
    }

    #[test]
    fn ambient_credential_sources_survive_loading_with_environment_credentials_set() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"

[store.credentials]
kind = "ambient"
"#,
        );
        let access_key = EnvGuard::set("AWS_ACCESS_KEY_ID", "parity-access");
        let secret_key = EnvGuard::set("AWS_SECRET_ACCESS_KEY", "parity-secret");
        let config = load_server_config(&path).expect("load server config");
        drop((access_key, secret_key));
        assert_eq!(config.store.credentials_kind(), Some("ambient"));
    }

    #[test]
    fn every_maintenance_mode_names_its_serving_and_scheduling_behavior() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.maintenance, super::MaintenanceMode::ServeAndMaintain);
        assert!(config.maintenance.serves());
        assert!(config.maintenance.maintains());

        for (spelling, mode, serves, maintains) in [
            ("disabled", super::MaintenanceMode::Disabled, false, false),
            ("serve_only", super::MaintenanceMode::ServeOnly, true, false),
            (
                "maintain_only",
                super::MaintenanceMode::MaintainOnly,
                false,
                true,
            ),
            (
                "serve_and_maintain",
                super::MaintenanceMode::ServeAndMaintain,
                true,
                true,
            ),
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
maintenance = "{spelling}"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let config = load_server_config(&path).expect("valid config");
            assert_eq!(config.maintenance, mode);
            assert_eq!(config.maintenance.serves(), serves);
            assert_eq!(config.maintenance.maintains(), maintains);
        }
    }

    #[test]
    fn the_retired_background_maintenance_key_is_no_longer_a_key() {
        // One field decides maintenance behavior now. The boolean it
        // replaced fails through strict decoding
        // like any other unknown key.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
background_maintenance = false

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("retired key must not load");
        assert!(
            error.to_string().contains("background_maintenance"),
            "{error}"
        );
    }

    #[test]
    fn an_unknown_maintenance_word_is_rejected() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
maintenance = "sometimes"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("unknown mode must not load");
        match error {
            ServerConfigError::Decode(message) => {
                assert!(message.contains("serve_and_maintain"), "{message}");
                assert!(message.contains("serve_only"), "{message}");
                assert!(message.contains("maintain_only"), "{message}");
                assert!(message.contains("disabled"), "{message}");
            }
            other => panic!("expected decode error naming the modes, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_invalid_bind() {
        let path = write_config(
            r#"
bind = "bad-bind"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("invalid bind");

        assert_invalid_field(error, "bind");
    }

    #[test]
    fn load_rejects_blank_writer_id() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "   "

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("blank writer fields");

        assert_missing_field(error, "writer_id");
    }

    #[test]
    fn load_rejects_blank_provider_required_fields() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "cloudflare-r2"
bucket = " "
account_id = "account"
endpoint_url = "https://example.com"

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );

        let error = load_server_config(&path).expect_err("blank bucket");

        assert_missing_field(error, "store.bucket");
    }

    #[test]
    fn load_rejects_invalid_endpoint_urls() {
        let aws_path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "ftp://example.com"
key_prefix = "demo"
force_path_style = false

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let r2_path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "not a url"
key_prefix = "demo"

[store.credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        );
        let azure_path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "azure-abs"
account_name = "devstoreaccount1"
container_name = "container"
endpoint_url = "not a url"
key_prefix = "demo"

[store.credentials]
kind = "access-key"
access_key = "{AZURITE_ACCOUNT_KEY}"
"#
        ));

        let aws_error = load_server_config(&aws_path).expect_err("invalid aws endpoint");
        let r2_error = load_server_config(&r2_path).expect_err("invalid r2 endpoint");
        let azure_error = load_server_config(&azure_path).expect_err("invalid azure endpoint");

        assert_invalid_field(aws_error, "store.endpoint_url");
        assert_invalid_field(r2_error, "store.endpoint_url");
        assert_invalid_field(azure_error, "store.endpoint_url");
    }

    #[test]
    fn load_rejects_blank_gcs_bucket() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "gcp-gcs"
bucket = " "
key_prefix = "demo"

[store.credentials]
kind = "service-account-file"
path = "/tmp/service-account.json"
"#,
        );

        let error = load_server_config(&path).expect_err("blank gcs bucket");

        assert_missing_field(error, "store.bucket");
    }

    #[test]
    fn load_accepts_azure_abs_store() {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "azure-abs"
account_name = "devstoreaccount1"
container_name = "container"
endpoint_url = "http://127.0.0.1:10000/devstoreaccount1"
key_prefix = "demo"

[store.credentials]
kind = "access-key"
access_key = "{AZURITE_ACCOUNT_KEY}"
"#
        ));

        load_server_config(&path).expect("load azure config");
    }

    #[test]
    fn load_rejects_blank_azure_account_name() {
        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "azure-abs"
account_name = " "
container_name = "container"

[store.credentials]
kind = "access-key"
access_key = "{AZURITE_ACCOUNT_KEY}"
"#
        ));

        let error = load_server_config(&path).expect_err("blank azure account name");

        assert_missing_field(error, "store.account_name");
    }

    #[test]
    fn load_rejects_blank_auth_token_when_present() {
        // Prevent an ambient token from filling the blank value.
        let _auth_token = EnvGuard::unset(AUTH_TOKEN_ENV);
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "   "
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("blank auth token");

        assert_invalid_field(error, "auth_token");
    }

    #[test]
    fn load_rejects_non_loopback_bind_without_auth_token() {
        // Remove any ambient token so these configs remain invalid.
        let _auth_token = EnvGuard::unset(AUTH_TOKEN_ENV);
        for bind in ["0.0.0.0:9400", "[::]:9400", "10.1.2.3:9400"] {
            let path = write_config(&format!(
                r#"
bind = "{bind}"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));

            let error = load_server_config(&path).expect_err("open network bind");

            assert_invalid_field(error, "auth_token");
        }
    }

    #[test]
    fn allow_unauthenticated_remote_permits_an_open_bind() {
        let path = write_config(
            r#"
bind = "0.0.0.0:9400"
allow_unauthenticated_remote = true
allow_remote_without_tls = true
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        load_server_config(&path).expect("explicitly-open config loads");
    }

    #[test]
    fn loopback_bind_without_auth_token_is_allowed() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        load_server_config(&path).expect("loopback-only config loads");
    }

    #[test]
    fn load_rejects_non_loopback_bind_without_tls() {
        for bind in ["0.0.0.0:9400", "[::]:9400", "10.1.2.3:9400"] {
            let path = write_config(&format!(
                r#"
bind = "{bind}"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));

            let error = load_server_config(&path).expect_err("plaintext network bind");

            assert_invalid_field(error, "tls");
        }
    }

    #[test]
    fn allow_remote_without_tls_permits_a_plaintext_network_bind() {
        let path = write_config(
            r#"
bind = "0.0.0.0:9400"
auth_token = "dev-token"
allow_remote_without_tls = true
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        load_server_config(&path).expect("proxy-terminated config loads");
    }

    #[test]
    fn tls_satisfies_the_network_bind_requirement() {
        let path = write_config(
            r#"
bind = "0.0.0.0:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[tls]
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("tls-terminating config loads");

        let tls = config.tls.expect("tls table decodes");
        assert_eq!(tls.cert_path, "/etc/loonfs/tls/server.crt");
        assert_eq!(tls.key_path, "/etc/loonfs/tls/server.key");
    }

    #[test]
    fn load_rejects_blank_tls_paths() {
        for (cert_path, key_path, field) in [
            (" ", "/etc/loonfs/tls/server.key", "tls.cert_path"),
            ("/etc/loonfs/tls/server.crt", "", "tls.key_path"),
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[tls]
cert_path = "{cert_path}"
key_path = "{key_path}"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));

            let error = load_server_config(&path).expect_err("blank tls path");

            assert_missing_field(error, field);
        }
    }

    #[test]
    fn load_rejects_unknown_tls_keys() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[tls]
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"
client_ca_path = "/etc/loonfs/tls/clients.crt"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        match load_server_config(&path).expect_err("unknown tls key") {
            ServerConfigError::Decode(message) => assert!(
                message.contains("client_ca_path"),
                "decode error must name the unknown key, got: {message}"
            ),
            other => panic!("expected a decode error, got {other:?}"),
        }
    }

    #[test]
    fn max_upload_bytes_defaults_and_rejects_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.max_upload_bytes, 256 * 1024 * 1024);
        assert!(!config.allow_unauthenticated_remote);

        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
max_upload_bytes = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let error = load_server_config(&path).expect_err("zero upload limit");
        assert_invalid_field(error, "max_upload_bytes");
    }

    #[test]
    fn request_and_shutdown_deadlines_default_and_reject_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.request_deadline_ms, 60_000);
        assert_eq!(config.shutdown_deadline_ms, 600_000);

        for field in ["request_deadline_ms", "shutdown_deadline_ms"] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let error = load_server_config(&path).expect_err("zero deadline must be rejected");
            assert_invalid_field(error, field);
        }
    }

    #[test]
    fn inline_content_defaults_and_overrides_preserve_explicit_disablement() {
        let defaults = loonfs::InlineContentPolicy::default();
        assert_eq!(defaults.inline_content_threshold_bytes, Some(64 * 1024));
        assert_eq!(defaults.inline_content_fold_at_bytes, 2 * 1024 * 1024);
        assert_eq!(defaults.inline_content_wal_object_budget_bytes, 1024 * 1024);
        assert_eq!(defaults.inline_content_tail_limit_bytes, 32 * 1024 * 1024);
        assert_eq!(
            loonfs::MetadataMaintenanceOptions::default()
                .inline_content_fold_at_bytes
                .get(),
            defaults.inline_content_fold_at_bytes
        );
        assert_eq!(InlineContentOverrides::default().resolve(), defaults);
        for (source, threshold) in [
            ("", Some(64 * 1024)),
            (
                "inline_content_wal_object_budget_bytes = 1024",
                Some(64 * 1024),
            ),
            ("inline_content_threshold_bytes = 4096", Some(4096)),
            ("inline_content_threshold_bytes = 0", Some(0)),
            ("inline_content_threshold_bytes = false", None),
        ] {
            let overrides: InlineContentOverrides = toml::from_str(source).expect("overrides");
            assert_eq!(
                overrides.resolve().inline_content_threshold_bytes,
                threshold
            );
        }
        assert!(
            toml::from_str::<InlineContentOverrides>("inline_content_threshold_bytes = true")
                .is_err()
        );
    }

    #[test]
    fn publication_overrides_are_strict_and_positive() {
        let defaults: PublicationLimitsOverrides = toml::from_str("").expect("empty overrides");
        assert_eq!(defaults.resolve(), loonfs::PublicationLimits::default());
        for field in [
            "max_requests",
            "max_requests_per_namespace",
            "max_estimated_bytes",
            "max_estimated_bytes_per_namespace",
            "max_concurrent_publications",
        ] {
            assert!(toml::from_str::<PublicationLimitsOverrides>(&format!("{field} = 0")).is_err());
        }
        assert!(toml::from_str::<PublicationLimitsOverrides>("max_requsets = 10").is_err());
        let overrides: PublicationLimitsOverrides =
            toml::from_str("max_requests = 12\nmax_requests_per_namespace = 3").expect("overrides");
        assert_eq!(overrides.max_requests, std::num::NonZeroUsize::new(12));
        assert_eq!(overrides.resolve().max_requests_per_namespace.get(), 3);
        assert_eq!(
            overrides.resolve().max_estimated_bytes_per_namespace,
            loonfs::PublicationLimits::default().max_estimated_bytes_per_namespace
        );
    }

    #[test]
    fn transfer_bounds_default_and_reject_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.max_download_bytes, 256 * 1024 * 1024);
        assert_eq!(
            config.max_concurrent_reads,
            loonfs::DEFAULT_MAX_CONCURRENT_READS
        );
        assert_eq!(
            config.max_concurrent_folds,
            loonfs::DEFAULT_MAX_CONCURRENT_FOLDS
        );
        assert_eq!(
            config.max_concurrent_compactions,
            loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS
        );
        for field in ["max_in_flight_requests", "max_connections"] {
            let mut oversized = config.clone();
            let value = tokio::sync::Semaphore::MAX_PERMITS + 1;
            match field {
                "max_in_flight_requests" => oversized.max_in_flight_requests = value,
                _ => oversized.max_connections = value,
            }
            assert_invalid_field(oversized.validate().expect_err("oversized bound"), field);
        }
        assert_eq!(config.max_in_flight_requests, 256);
        assert_eq!(config.max_connections, 1024);
        assert_eq!(config.max_concurrent_uploads, 8);
        assert_eq!(config.max_concurrent_downloads, 16);
        assert_eq!(config.max_concurrent_maintenance, 8);
        assert_eq!(config.tick_interval_ms, 5_000);
        assert_eq!(config.maintenance_interval_ms, 300_000);
        assert_eq!(config.gc_interval_ms, 3_600_000);
        assert_eq!(config.full_sweep_interval_ms, 86_400_000);
        assert_eq!(config.idle_session_close_after_ms, 1_800_000);

        for field in [
            "max_download_bytes",
            "max_concurrent_reads",
            "max_in_flight_requests",
            "max_connections",
            "max_concurrent_folds",
            "max_concurrent_compactions",
            "max_concurrent_uploads",
            "max_concurrent_downloads",
            "max_concurrent_maintenance",
            "tick_interval_ms",
            "maintenance_interval_ms",
            "gc_interval_ms",
            "full_sweep_interval_ms",
            "idle_session_close_after_ms",
            "max_merge_input_bytes",
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let error = load_server_config(&path).expect_err("zero bound must be rejected");
            assert_invalid_field(error, field);
        }

        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
max_content_bytes_per_step = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let error = load_server_config(&path).expect_err("zero grep bound must be rejected");
        assert_invalid_field(error, "grep");
    }

    #[test]
    fn idle_fold_period_defaults_to_fifteen_minutes_and_accepts_zero() {
        for (setting, expected) in [
            ("", 900_000),
            ("idle_fold_after_ms = 60000", 60_000),
            ("idle_fold_after_ms = 0", 0),
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{setting}

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let config = load_server_config(&path).expect("valid config");
            assert_eq!(config.idle_fold_after_ms, expected, "{setting:?}");
        }
    }

    #[test]
    fn server_config_debug_redacts_secrets() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "debug-auth-token"
content_token_secret = "debug-content-token-secret"
writer_id = "loonfs-server"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
key_prefix = "demo"
force_path_style = false

[store.credentials]
kind = "static"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"
"#,
        );
        let config = load_server_config(&path).expect("load config");

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("debug-auth-token"));
        assert!(!rendered.contains("debug-content-token-secret"));
        assert!(!rendered.contains("debug-access-key-id"));
        assert!(!rendered.contains("debug-secret-access-key"));
        assert!(!rendered.contains("debug-session-token"));
    }

    #[test]
    fn env_fallbacks_fill_blank_or_unset_secrets() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "file-auth-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let mut config = load_server_config(&path).expect("load config");

        // File values win over the environment.
        config.apply_env_fallbacks(
            Some("env-auth-token".to_owned()),
            Some("env-content-token-secret".to_owned()),
        );
        assert_eq!(
            config.auth_token.as_ref().map(|token| token.expose()),
            Some("file-auth-token")
        );
        assert_eq!(
            config.content_token_secret.expose(),
            "dev-content-token-secret"
        );

        // The environment fills fields the file left unset.
        config.auth_token = None;
        config.content_token_secret = loonfs_types::SecretString::default();
        config.apply_env_fallbacks(
            Some("env-auth-token".to_owned()),
            Some("env-content-token-secret".to_owned()),
        );
        assert_eq!(
            config.auth_token.as_ref().map(|token| token.expose()),
            Some("env-auth-token")
        );
        assert_eq!(
            config.content_token_secret.expose(),
            "env-content-token-secret"
        );

        config.auth_token = Some(loonfs_types::SecretString::new("   ".to_owned()));
        config.content_token_secret = loonfs_types::SecretString::new("   ".to_owned());
        config.apply_env_fallbacks(
            Some("env-auth-token".to_owned()),
            Some("env-content-token-secret".to_owned()),
        );
        assert_eq!(
            config.auth_token.as_ref().map(|token| token.expose()),
            Some("env-auth-token")
        );
        assert_eq!(
            config.content_token_secret.expose(),
            "env-content-token-secret"
        );

        // Blank environment values are ignored.
        config.auth_token = None;
        config.content_token_secret = loonfs_types::SecretString::default();
        config.apply_env_fallbacks(Some("   ".to_owned()), Some(String::new()));
        assert!(config.auth_token.is_none());
        assert!(config.content_token_secret.expose().is_empty());
    }

    #[test]
    fn static_store_credentials_are_preserved_as_static() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"

[store.credentials]
kind = "static"
access_key_id = "file-access"
secret_access_key = "file-secret"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        match config.store {
            super::StoreConfig::AwsS3 { credentials, .. } => {
                let loonfs_objectstore::AwsS3Credentials::Static {
                    access_key_id,
                    secret_access_key,
                    ..
                } = credentials
                else {
                    panic!("expected static credentials")
                };
                assert_eq!(access_key_id.expose(), "file-access");
                assert_eq!(secret_access_key.expose(), "file-secret");
            }
            other => panic!("expected an aws-s3 store, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_unknown_keys_at_every_level() {
        let top_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
lease_duration = 60000

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let store_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
key_prefiks = "typo"
"#,
        );
        let metadata_cache_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[metadata_cache]
max_head_state_byte = 2

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let grep_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
max_files_per_stepp = 3

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        for (path, typo) in [
            (top_level, "lease_duration"),
            (store_level, "key_prefiks"),
            (metadata_cache_level, "max_head_state_byte"),
            (grep_level, "max_files_per_stepp"),
        ] {
            let error = load_server_config(&path).expect_err("typo'd key must be rejected");
            match error {
                ServerConfigError::Decode(message) => {
                    assert!(
                        message.contains(typo),
                        "decode error must name `{typo}`, got: {message}"
                    );
                }
                other => panic!("expected decode error naming {typo}, got {other:?}"),
            }
        }
    }

    #[test]
    fn load_accepts_config_without_content_token_secret_field() {
        // `content_token_secret` may come from LOONFS_CONTENT_TOKEN_SECRET
        // instead of the file; omitting both must still fail validation.
        let path = write_config_verbatim(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let mut config: super::ServerConfig =
            toml::from_str(&std::fs::read_to_string(&path).expect("read config"))
                .expect("config without content_token_secret parses");
        assert!(config.content_token_secret.expose().is_empty());

        config.apply_env_fallbacks(None, Some("env-content-token-secret".to_owned()));
        assert_eq!(
            config.content_token_secret.expose(),
            "env-content-token-secret"
        );

        // Without the env fallback the load path reports the missing field.
        let _content_token_secret = EnvGuard::unset(CONTENT_TOKEN_SECRET_ENV);
        let error = load_server_config(&path).expect_err("missing content token secret");
        assert_missing_field(error, "content_token_secret");
    }

    #[test]
    fn load_uses_default_metadata_cache_when_omitted() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(
            config.metadata_cache,
            super::MetadataCacheOverrides::default()
        );
        assert_eq!(config.manifest_revalidation_interval_ms, None);
        assert_eq!(config.max_read_working_bytes, None);
    }

    #[test]
    fn load_applies_metadata_cache_and_read_settings() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
manifest_revalidation_interval_ms = 250
max_read_working_bytes = 8192

[metadata_cache]
max_segment_bytes = 16384
max_head_state_bytes = 4096

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.manifest_revalidation_interval_ms, Some(250));
        assert_eq!(config.max_read_working_bytes, Some(8192));
        assert_eq!(
            config.metadata_cache,
            super::MetadataCacheOverrides {
                max_segment_bytes: Some(16384),
                max_head_state_bytes: Some(4096),
            }
        );
    }

    #[test]
    fn load_accepts_zero_metadata_cache_limits() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[metadata_cache]
max_segment_bytes = 0
max_head_state_bytes = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(
            config.metadata_cache,
            super::MetadataCacheOverrides {
                max_segment_bytes: Some(0),
                max_head_state_bytes: Some(0),
            }
        );
    }

    #[test]
    fn an_omitted_local_cache_table_asks_for_no_local_cache() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("valid config");
        assert!(
            config.local_cache.is_none(),
            "a server without the table reads every metadata block from object storage"
        );
    }

    #[test]
    fn load_reads_the_local_cache_table() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = 107374182400

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let local_cache = load_server_config(&path)
            .expect("valid config")
            .local_cache
            .expect("a local cache table");
        assert_eq!(local_cache.path, "/var/lib/loonfs/cache");
        assert_eq!(local_cache.memory_bytes, 67_108_864);
        assert_eq!(local_cache.disk_bytes, 107_374_182_400);
    }

    #[test]
    fn a_local_cache_table_needs_a_path_and_two_sizes() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[local_cache]
path = "   "
memory_bytes = 67108864
disk_bytes = 107374182400

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let error = load_server_config(&path).expect_err("a blank path must be rejected");
        assert_missing_field(error, "local_cache.path");

        for (field, memory_bytes, disk_bytes) in [
            ("local_cache.memory_bytes", 0, 107_374_182_400_u64),
            ("local_cache.disk_bytes", 67_108_864, 0),
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = {memory_bytes}
disk_bytes = {disk_bytes}

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let error = load_server_config(&path).expect_err("a zero size must be rejected");
            assert_invalid_field(error, field);
        }
    }

    #[test]
    fn a_local_cache_disk_tier_has_a_floor() {
        let with_disk_bytes = |disk_bytes: u64| {
            write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = {disk_bytes}

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ))
        };

        // Eight megabytes is a plausible value that would have started a
        // server whose disk tier holds nothing at all.
        let error = load_server_config(with_disk_bytes(8 * 1024 * 1024))
            .expect_err("a disk tier under one block must be rejected");
        let message = error.to_string();
        assert!(message.contains(&MIN_DISK_BYTES.to_string()), "{message}");
        assert!(message.contains(&DISK_BLOCK_BYTES.to_string()), "{message}");
        assert_invalid_field(error, "local_cache.disk_bytes");

        let error = load_server_config(with_disk_bytes(MIN_DISK_BYTES - 1))
            .expect_err("one byte under the floor is still under it");
        assert_invalid_field(error, "local_cache.disk_bytes");

        let local_cache = load_server_config(with_disk_bytes(MIN_DISK_BYTES))
            .expect("the floor itself is a valid disk tier")
            .local_cache
            .expect("a local cache table");
        assert_eq!(local_cache.disk_bytes, MIN_DISK_BYTES);
    }

    #[test]
    fn the_local_cache_table_takes_no_engine_settings() {
        // The disk engine's geometry is fixed in the implementation. A
        // deployment reaching for it fails through strict decoding like any
        // other unknown key.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[local_cache]
path = "/var/lib/loonfs/cache"
memory_bytes = 67108864
disk_bytes = 107374182400
block_bytes = 65536

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("an engine key must not load");
        assert!(error.to_string().contains("block_bytes"), "{error}");
    }

    #[test]
    fn an_omitted_grep_table_composes_no_grep() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.grep.mode, super::GrepMode::Disabled);
    }

    #[test]
    fn a_grep_table_without_a_mode_is_rejected() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("mode is required");
        assert!(error.to_string().contains("mode"), "{error}");
    }

    #[test]
    fn every_grep_mode_names_the_two_jobs_it_does() {
        for (spelling, mode, serves, maintains) in [
            ("disabled", super::GrepMode::Disabled, false, false),
            ("serve_only", super::GrepMode::ServeOnly, true, false),
            ("maintain_only", super::GrepMode::MaintainOnly, false, true),
            (
                "serve_and_maintain",
                super::GrepMode::ServeAndMaintain,
                true,
                true,
            ),
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "{spelling}"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));

            let config = load_server_config(&path).expect("load config");
            assert_eq!(config.grep.mode, mode);
            assert_eq!(config.grep.mode.serves_grep(), serves);
            assert_eq!(config.grep.mode.maintains_index(), maintains);
        }
    }

    #[test]
    fn the_grep_step_limit_defaults_to_two_and_refuses_zero() {
        let load = |setting: &str| {
            load_server_config(write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
{setting}

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            )))
        };

        let default = load("").expect("load the default step limit");
        assert_eq!(default.grep.worker.max_concurrent_steps, 2);
        let configured = load("max_concurrent_steps = 5").expect("load a step limit");
        assert_eq!(configured.grep.worker.max_concurrent_steps, 5);
        match load("max_concurrent_steps = 0") {
            Err(ServerConfigError::InvalidField {
                field: "grep",
                reason,
            }) => assert!(reason.contains("max_concurrent_steps"), "{reason}"),
            other => panic!("expected a zero step limit to be refused, got {other:?}"),
        }
    }

    #[test]
    fn load_applies_grep_mode_and_policy() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "serve_only"
max_files_per_step = 4096
max_content_bytes_per_step = 536870912

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let grep = load_server_config(&path).expect("load config").grep;
        assert_eq!(grep.mode, super::GrepMode::ServeOnly);
        let policy = grep
            .worker_config()
            .build_policy()
            .expect("valid configured grep policy");
        assert_eq!(policy.max_files_per_step.get(), 4096);
        assert_eq!(policy.max_content_bytes_per_step.get(), 536_870_912);
        let defaults = loonfs_grep::GramIndexBuildPolicy::default();
        assert_eq!(policy.max_rows_per_segment, defaults.max_rows_per_segment);
        assert_eq!(policy.max_delta_runs, defaults.max_delta_runs);
        assert_eq!(policy.max_mid_runs, defaults.max_mid_runs);
        assert_eq!(
            policy.max_decoded_input_rows_per_step,
            defaults.max_decoded_input_rows_per_step
        );
    }

    #[test]
    fn grep_work_budget_overrides_preserve_engine_defaults() {
        // Operators control input work; the engine controls segment and merge
        // shape. A work override must not change the merge policy.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
mode = "serve_and_maintain"
max_files_per_step = 1024

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let policy = load_server_config(&path)
            .expect("load config")
            .grep
            .worker_config()
            .build_policy()
            .expect("valid configured grep policy");
        assert_eq!(
            policy.max_mid_runs,
            loonfs_grep::GramIndexBuildPolicy::default().max_mid_runs,
            "merge policy remains an engine default"
        );
    }

    #[test]
    fn grep_rejects_retired_engine_tuning_keys() {
        for key in [
            "max_rows_per_segment",
            "max_delta_runs",
            "max_mid_runs",
            "max_decoded_input_rows_per_step",
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
[grep]
mode = "serve_and_maintain"
{key} = 1
[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#
            ));
            let error = load_server_config(&path).expect_err("engine tuning keys must be rejected");
            assert!(error.to_string().contains(key), "{error}");
        }
    }

    #[test]
    fn unknown_config_tables_fail_decode() {
        // Unknown tables fail through the config's strict parsing.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[gram_index_build]
max_files_per_step = 4

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("unknown table must fail");
        match error {
            ServerConfigError::Decode(message) => {
                assert!(message.contains("gram_index_build"), "{message}");
            }
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    #[test]
    fn load_rejects_negative_metadata_cache_limits_as_decode_error() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[metadata_cache]
max_head_state_bytes = -1

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("negative byte limit");
        match error {
            ServerConfigError::Decode(_) => {}
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    #[test]
    fn server_config_toml_round_trip_preserves_values() {
        let mut config: super::ServerConfig =
            toml::from_str(include_str!("../config/local-fs.example.toml"))
                .expect("example config should parse");

        for threshold in [
            config.inline_content.inline_content_threshold_bytes,
            Some(0),
            None,
        ] {
            config.inline_content.inline_content_threshold_bytes = threshold;
            let serialized = toml::to_string(&config).expect("server config should serialize");
            let decoded: super::ServerConfig =
                toml::from_str(&serialized).expect("serialized server config should parse");
            assert_eq!(decoded, config);
        }
    }

    #[test]
    fn server_example_configs_parse_and_validate() {
        let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config");
        let mut examples = 0usize;
        for entry in fs::read_dir(configs_dir).expect("read config directory") {
            let path = entry.expect("read config entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.ends_with(".example.toml") {
                continue;
            }
            let contents = fs::read_to_string(&path).expect("read example config");
            let config: super::ServerConfig =
                toml::from_str(&contents).unwrap_or_else(|err| panic!("{name} must parse: {err}"));
            config
                .validate()
                .unwrap_or_else(|err| panic!("{name} must validate: {err}"));
            examples += 1;
        }
        assert!(
            examples >= 5,
            "expected at least 5 server example configs, found {examples}"
        );
    }

    fn write_config(contents: &str) -> std::path::PathBuf {
        let contents = if contents.contains("content_token_secret") {
            contents.to_owned()
        } else {
            contents.replacen(
                "writer_id",
                "content_token_secret = \"dev-content-token-secret\"\nwriter_id",
                1,
            )
        };
        write_config_verbatim(&contents)
    }

    fn write_config_verbatim(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("server.toml");
        fs::write(&path, contents).expect("write config");
        let _ = temp_dir.keep();
        path
    }

    fn assert_invalid_field(error: ServerConfigError, field: &'static str) {
        match error {
            ServerConfigError::InvalidField { field: actual, .. } => assert_eq!(actual, field),
            other => panic!("expected invalid field error for {field}, got {other:?}"),
        }
    }

    fn assert_missing_field(error: ServerConfigError, field: &'static str) {
        match error {
            ServerConfigError::MissingField { field: actual } => assert_eq!(actual, field),
            other => panic!("expected missing field error for {field}, got {other:?}"),
        }
    }
}
