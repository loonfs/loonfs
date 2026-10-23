//! Server configuration: strict TOML decoding of the listen address,
//! store, metadata cache limits, and runtime overrides.

use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::metrics::MetricsRecorder;
use loonfs::{ExecutionBudget, MetadataCache, SsdSegmentBlocks};
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::ConfiguredObjectStore;
use loonfs_types::config::{non_blank, validate_non_empty, ConfigFieldError};
use loonfs_types::env::{AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV};
use loonfs_types::{SecretString, WriterId};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

pub use loonfs::ReadConsistency;
pub use loonfs_http::JobMode;
pub use loonfs_objectstore::StoreConfig;

/// Overrides the embedded writer's inline content policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct InlineContentConfig {
    #[serde(
        serialize_with = "serialize_inline_content_threshold",
        deserialize_with = "deserialize_inline_content_threshold"
    )]
    pub inline_content_threshold_bytes: Option<usize>,
    pub inline_content_wal_object_budget_bytes: Option<usize>,
    pub inline_content_fold_at_bytes: Option<usize>,
    pub inline_content_tail_limit_bytes: Option<usize>,
}

impl Default for InlineContentConfig {
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

impl InlineContentConfig {
    pub(crate) fn effective_policy(&self) -> loonfs::InlineContentPolicy {
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
pub struct PublicationLimitsConfig {
    pub max_requests: Option<std::num::NonZeroUsize>,
    pub max_requests_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes: Option<std::num::NonZeroUsize>,
    pub max_estimated_bytes_per_namespace: Option<std::num::NonZeroUsize>,
    pub max_concurrent_publications: Option<std::num::NonZeroUsize>,
}

impl PublicationLimitsConfig {
    /// The per-namespace limits the server's runtime applies.
    pub(crate) fn effective_policy(&self) -> loonfs::PublicationPolicy {
        let defaults = loonfs::PublicationPolicy::default();
        loonfs::PublicationPolicy {
            max_requests_per_namespace: self
                .max_requests_per_namespace
                .unwrap_or(defaults.max_requests_per_namespace),
            max_estimated_bytes_per_namespace: self
                .max_estimated_bytes_per_namespace
                .unwrap_or(defaults.max_estimated_bytes_per_namespace),
            ..defaults
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
    pub writer_id: WriterId,
    /// Maximum public reads running at once. Reads past this limit wait.
    #[serde(default = "default_max_concurrent_reads")]
    pub max_concurrent_reads: NonZeroUsize,
    #[serde(default = "default_max_in_flight_requests")]
    pub max_in_flight_requests: NonZeroUsize,
    #[serde(default = "default_max_connections")]
    pub max_connections: NonZeroUsize,
    /// Maximum WAL folds this server runs concurrently. A sustained
    /// `loonfs.execution_budget.folds_waiting` gauge means this cap is too
    /// low.
    #[serde(default = "default_max_concurrent_folds")]
    pub max_concurrent_folds: NonZeroUsize,
    /// Maximum metadata compaction units this server runs at once, whether
    /// writer sessions, the maintenance sweep, or maintenance requests start
    /// them. A merge never takes a fold permit.
    #[serde(default = "default_max_concurrent_compactions")]
    pub max_concurrent_compactions: NonZeroUsize,
    /// Maximum content object writes this server runs at once, including the
    /// content that folds write. A sustained
    /// `loonfs.execution_budget.content_writes_waiting` gauge means this cap
    /// is too low.
    #[serde(default = "default_max_concurrent_content_writes")]
    pub max_concurrent_content_writes: NonZeroUsize,
    /// Shared request and concurrency limits for namespace publications.
    #[serde(default)]
    pub publication: PublicationLimitsConfig,
    /// Defaults to a 64 KiB threshold; `false` disables inline writes in TOML.
    #[serde(default)]
    pub inline_content: InlineContentConfig,
    /// Limits of the one metadata cache the server's runtime reads through.
    #[serde(default)]
    pub metadata_cache: MetadataCacheConfig,
    /// What this server does about grep, plus the bounded-step budgets its
    /// index maintenance runs under. A config with no `[grep]` table
    /// composes no grep at all; a present table must name its `mode`.
    #[serde(default)]
    pub grep: GrepConfig,
    /// Whether this server serves maintenance requests, runs the maintenance
    /// sweep, both, or neither.
    #[serde(default = "default_maintenance")]
    pub maintenance: JobMode,
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
    /// How many proxied upload bodies the server will stream at once;
    /// requests past the cap answer `server_busy` before any transfer.
    /// Worst-case upload memory is this times one streamed part, since
    /// bodies forward to the store incrementally instead of buffering.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: NonZeroUsize,
    /// How many proxied content streams the server will serve at once;
    /// requests past the cap answer `server_busy` before any fetch.
    /// Content memory follows this count times the internal read chunk size,
    /// independent of `max_download_bytes`. Admission lasts until the body
    /// finishes or is dropped.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: NonZeroUsize,
    /// How many namespaces the maintenance sweep visits at once. Folds and
    /// merges also wait for the fold and compaction permits. Defaults to 8.
    #[serde(default = "default_max_concurrent_maintenance")]
    pub max_concurrent_maintenance: NonZeroUsize,
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
    /// How long the server keeps a namespace's history before it advances
    /// the namespace's retention floor past that history, in milliseconds.
    /// With it set, each full pass advances every active namespace's floor
    /// to the last commit at or before the current time minus this window.
    /// Absent, the server advances no floor.
    pub retention_window_ms: Option<NonZeroU64>,
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
    /// default of 1000. At most 1155000, the revalidation bound.
    pub manifest_revalidation_interval_ms: Option<u64>,
    /// The maximum staleness, in milliseconds, of an eventual read that doesn't
    /// set its own `max_staleness_ms`. Zero confirms the namespace head on
    /// every read. Unset keeps the runtime default of 1000. At most 1155000,
    /// the revalidation bound.
    pub max_staleness_ms: Option<u64>,
    /// The runtime's default consistency, which every read that doesn't set
    /// its own takes.
    #[serde(default)]
    pub consistency: ReadConsistency,
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

/// The server's TLS identity: one certificate chain and its private key, both
/// read at startup.
///
/// A file that is missing, unreadable, or not the PEM it claims to be fails the
/// process rather than degrading to plaintext.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsServerConfig {
    /// PEM certificate chain, leaf first.
    pub cert_path: String,
    /// PEM private key: PKCS#8, PKCS#1 (RSA), or SEC1.
    pub key_path: String,
}

fn default_maintenance() -> JobMode {
    JobMode::ServeAndMaintain
}

fn default_min_publish_interval_ms() -> u64 {
    1_000
}

fn default_request_deadline_ms() -> u64 {
    loonfs_http::DEFAULT_REQUEST_DEADLINE_MS
}

/// Default for `shutdown_deadline_ms`, so a graceful shutdown waits at most
/// 10 minutes for accepted requests and the maintenance sweep. That is long
/// enough for an accepted request to finish the provider operation it already
/// started. The assertion below checks this default only, because an operator
/// can configure any positive value.
const DEFAULT_SHUTDOWN_DEADLINE_MS: u64 = 600_000;

const _: () = assert!(
    DEFAULT_SHUTDOWN_DEADLINE_MS as u128
        > loonfs_objectstore::PROVIDER_OPERATION_DEADLINE.as_millis()
);

fn default_shutdown_deadline_ms() -> u64 {
    DEFAULT_SHUTDOWN_DEADLINE_MS
}

fn default_max_upload_bytes() -> u64 {
    // Mirrors the download default so anything the proxy accepted, the proxy
    // will serve back. Content ingested past this through `direct_put`
    // needs a raised limit to be read through the server.
    default_max_download_bytes()
}

fn default_max_download_bytes() -> u64 {
    loonfs::DEFAULT_MAX_READ_CONTENT_BYTES
}

fn default_max_in_flight_requests() -> NonZeroUsize {
    const { NonZeroUsize::new(256).unwrap() }
}

fn default_max_connections() -> NonZeroUsize {
    const { NonZeroUsize::new(1024).unwrap() }
}

fn default_max_concurrent_uploads() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs_http::DEFAULT_MAX_CONCURRENT_UPLOADS).unwrap() }
}

fn default_max_concurrent_reads() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs::DEFAULT_MAX_CONCURRENT_READS).unwrap() }
}

fn default_max_concurrent_folds() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs::DEFAULT_MAX_CONCURRENT_FOLDS).unwrap() }
}

fn default_max_concurrent_compactions() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS).unwrap() }
}

fn default_max_concurrent_content_writes() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs::DEFAULT_MAX_CONCURRENT_CONTENT_WRITES).unwrap() }
}

fn default_max_concurrent_downloads() -> NonZeroUsize {
    const { NonZeroUsize::new(loonfs_http::DEFAULT_MAX_CONCURRENT_DOWNLOADS).unwrap() }
}

fn default_max_concurrent_maintenance() -> NonZeroUsize {
    const { NonZeroUsize::new(8).unwrap() }
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
    loonfs_types::format::segment_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES
}

fn default_idle_fold_after_ms() -> u64 {
    loonfs::DEFAULT_IDLE_FOLD_AFTER_MS
}

/// The server's `[metadata_cache]` table: the limits of the one metadata
/// cache the server builds at startup.
///
/// Omitted fields are derived when sized, except `max_segment_index_bytes`
/// and `max_head_bytes`, which default to 32 MiB and 16 MiB with or without
/// sizing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheConfig {
    pub max_segment_bytes: Option<usize>,
    pub max_segment_index_bytes: Option<usize>,
    pub max_head_bytes: Option<usize>,
    pub max_wal_tail_bytes: Option<usize>,
    pub max_content_object_bytes: Option<usize>,
    /// The SSD tier of the metadata segment cache. The server has no SSD
    /// tier when the table is absent.
    pub ssd: Option<MetadataCacheSsdConfig>,
}

/// The server's `[metadata_cache.ssd]` table: where the SSD tier of the
/// metadata segment cache keeps its blocks, and how many bytes of blocks it
/// keeps. Both settings are required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheSsdConfig {
    /// The directory on the server's local SSD that the tier writes its
    /// blocks to. The server creates it if needed.
    pub directory: String,
    /// The most bytes of blocks that the tier keeps on disk, for every
    /// namespace that the server serves.
    pub max_segment_bytes: NonZeroUsize,
}

impl MetadataCacheSsdConfig {
    /// Opens the SSD tier in the configured directory, which reads the
    /// names of the files that the directory already has.
    pub(crate) async fn open(&self) -> Result<SsdSegmentBlocks, ServerConfigError> {
        SsdSegmentBlocks::open(&self.directory, self.max_segment_bytes)
            .await
            .map_err(|error| {
                self.invalid_directory(format!(
                    "the ssd tier can't open {:?}: {error}",
                    self.directory
                ))
            })
    }

    /// Creates the directory if it's missing, and checks that the server can
    /// write a file in it, by writing and removing a probe file. This is the
    /// SSD tier's part of `--check-config`.
    pub(crate) fn check_directory(&self) -> Result<(), ServerConfigError> {
        let directory = Path::new(&self.directory);
        fs::create_dir_all(directory).map_err(|error| {
            self.invalid_directory(format!(
                "the ssd tier can't create {:?}: {error}",
                self.directory
            ))
        })?;
        let probe = directory.join("check-config.probe");
        fs::write(&probe, b"probe")
            .and_then(|()| fs::remove_file(&probe))
            .map_err(|error| {
                self.invalid_directory(format!(
                    "the ssd tier can't write a file in {:?}: {error}",
                    self.directory
                ))
            })
    }

    fn invalid_directory(&self, reason: String) -> ServerConfigError {
        ServerConfigError::Field(ConfigFieldError::Invalid {
            field: "metadata_cache.ssd.directory",
            reason,
        })
    }
}

/// The server's `[grep]` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrepConfig {
    /// Whether this server answers grep queries, and whether it serves the
    /// index-maintenance endpoints and its sweep builds and collects the
    /// index.
    pub mode: JobMode,
    pub max_cache_bytes: Option<usize>,
    /// Flattening preserves the existing `[grep]` keys while leaving the
    /// worker policy itself as their one in-memory owner.
    #[serde(flatten)]
    pub worker: GrepWorkerConfig,
}

impl Default for GrepConfig {
    fn default() -> Self {
        Self {
            mode: JobMode::Disabled,
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
    #[error(transparent)]
    Field(#[from] ConfigFieldError),
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
        if self.auth_token.as_ref().is_none_or(SecretString::is_blank) {
            if let Some(token) = non_blank(auth_token_env) {
                self.auth_token = Some(SecretString::new(token));
            }
        }
        if self.content_token_secret.is_blank() {
            if let Some(secret) = non_blank(content_token_secret_env) {
                self.content_token_secret = SecretString::new(secret);
            }
        }
    }

    /// Builds the object store selected by this server configuration.
    pub fn object_store(&self) -> Result<ConfiguredObjectStore, ServerConfigError> {
        self.store.configured_object_store().map_err(|err| {
            ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "store",
                reason: err.public_message().into_owned(),
            })
        })
    }

    /// The line `loonfs-server --check-config` prints when the config loads.
    ///
    /// It names the address, provider, and maintenance settings.
    pub fn check_summary(&self) -> String {
        let maintenance = match self.maintenance {
            JobMode::Disabled => "disabled",
            JobMode::ServeOnly => "serve_only",
            JobMode::MaintainOnly => "maintain_only",
            JobMode::ServeAndMaintain => "serve_and_maintain",
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

        if let Some(token) = &self.auth_token {
            if token.is_blank() {
                return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                }));
            }
        } else if bind_serves_beyond_localhost(&bind) && !self.allow_unauthenticated_remote {
            return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "auth_token",
                reason: format!(
                    "bind `{bind}` serves every endpoint to the network without \
                     authentication; set `auth_token` (or `LOONFS_AUTH_TOKEN`), \
                     or set `allow_unauthenticated_remote = true` to serve open \
                     on purpose"
                ),
            }));
        }
        if let Some(tls) = &self.tls {
            validate_non_empty("tls.cert_path", &tls.cert_path)?;
            validate_non_empty("tls.key_path", &tls.key_path)?;
        } else if bind_serves_beyond_localhost(&bind) && !self.allow_remote_without_tls {
            return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "tls",
                reason: format!(
                    "bind `{bind}` serves the network in plaintext, exposing the \
                     bearer token and the presigned object-store URLs in upload \
                     responses; configure `[tls]` with `cert_path` and `key_path`, \
                     or set `allow_remote_without_tls = true` when TLS terminates \
                     in front of this process"
                ),
            }));
        }
        for (field, value) in [
            ("max_upload_bytes", self.max_upload_bytes),
            ("request_deadline_ms", self.request_deadline_ms),
            ("shutdown_deadline_ms", self.shutdown_deadline_ms),
            ("max_download_bytes", self.max_download_bytes),
            (
                "max_merge_input_bytes",
                self.max_merge_input_bytes
                    .unwrap_or(default_max_merge_input_bytes()) as u64,
            ),
        ] {
            validate_positive(field, value, None)?;
        }
        for (field, value) in [
            ("max_in_flight_requests", self.max_in_flight_requests),
            ("max_connections", self.max_connections),
        ] {
            if value.get() > tokio::sync::Semaphore::MAX_PERMITS {
                return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                    field,
                    reason: format!("must not exceed {}", tokio::sync::Semaphore::MAX_PERMITS),
                }));
            }
        }
        for (field, interval_ms) in [
            (
                "manifest_revalidation_interval_ms",
                self.manifest_revalidation_interval_ms,
            ),
            ("max_staleness_ms", self.max_staleness_ms),
        ] {
            if interval_ms.is_some_and(|interval_ms| interval_ms > READ_REVALIDATION_BOUND_MS) {
                return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                    field,
                    reason: format!(
                        "must not exceed {READ_REVALIDATION_BOUND_MS}, the revalidation bound"
                    ),
                }));
            }
        }
        for (field, value) in [
            ("tick_interval_ms", self.tick_interval_ms),
            ("maintenance_interval_ms", self.maintenance_interval_ms),
            ("gc_interval_ms", self.gc_interval_ms),
            ("full_sweep_interval_ms", self.full_sweep_interval_ms),
            (
                "idle_session_close_after_ms",
                self.idle_session_close_after_ms,
            ),
        ] {
            validate_positive(
                field,
                value,
                Some("set `maintenance = \"serve_only\"` to turn the maintenance sweep off"),
            )?;
        }
        if let Err(error) = self.grep.worker_config().validate() {
            return Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "grep",
                reason: error.to_string(),
            }));
        }
        validate_non_empty("content_token_secret", self.content_token_secret.expose())?;
        if let Some(ssd) = &self.metadata_cache.ssd {
            validate_non_empty("metadata_cache.ssd.directory", &ssd.directory)?;
        }
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

    /// Builds the publication policy of the server's one runtime from its
    /// `[publication]` table and `request_deadline_ms`.
    ///
    /// The write-stop wait is half the request deadline, so a commit that
    /// waits for the whole write-stop wait returns `maintenance_required`
    /// before its request returns `deadline_exceeded`.
    pub(crate) fn publication_policy(&self) -> loonfs::PublicationPolicy {
        loonfs::PublicationPolicy {
            write_stop_wait_budget_ms: self.request_deadline_ms / 2,
            ..self.publication.effective_policy()
        }
    }

    /// Builds the execution budget of the server's one runtime from its fold,
    /// compaction, content write, and merge input limits and the totals of
    /// its `[publication]` table, reporting to `recorder`.
    pub(crate) fn execution_budget(
        &self,
        memory: &MemorySizing,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> ExecutionBudget {
        let positive = |value: usize| {
            NonZeroUsize::new(value).expect("validated budget limits should be nonzero")
        };
        let mut builder = ExecutionBudget::builder()
            .max_concurrent_reads(self.max_concurrent_reads)
            .max_concurrent_folds(self.max_concurrent_folds)
            .max_concurrent_compactions(self.max_concurrent_compactions)
            .max_concurrent_content_writes(self.max_concurrent_content_writes)
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

/// The memory that glibc keeps on Linux in a quiet phase beyond what the same
/// workload uses on macOS.
///
/// The value is for `MALLOC_ARENA_MAX=2`, `MALLOC_TRIM_THRESHOLD_=131072`, and
/// `MALLOC_MMAP_THRESHOLD_=131072`, which the server image sets. In the lab's
/// `local-workloads-10k` run with those settings (`20261020T052717Z`), the app
/// drive's quiet phase stayed at 266 MiB against 76 MiB on macOS, and the
/// peaks were 10% to 32% above macOS (122 MiB against 111 MiB for sessions
/// turns, 323 MiB against 256 MiB for the app drive, and 298 MiB against
/// 226 MiB for the sandbox). With glibc's defaults (`20261020T024314Z`), those
/// four phases reached 307, 153, 769, and 715 MiB. The headroom is this floor
/// plus a quarter of the pools, which covers each of those peaks.
const ALLOCATOR_HEADROOM_FLOOR: usize = 200 * MIB;

// The label of the SSD tier's term in `fixed` states these three values.
const _: () = assert!(
    loonfs::SSD_WRITE_QUEUE_BYTES == 64 * MIB
        && loonfs::ASSUMED_STORED_BLOCK_BYTES == 4 * 1024
        && loonfs::SSD_INDEX_ENTRY_BYTES == 168
);

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
    fixed: [(&'static str, u128); 7],
    headroom: usize,
    managed: usize,
    segment: MemoryTerm,
    segment_index: MemoryTerm,
    head: MemoryTerm,
    wal_tail: MemoryTerm,
    content_object: MemoryTerm,
    pub(crate) grep: MemoryTerm,
    read_working: MemoryTerm,
    publication: MemoryTerm,
    merge_input: MemoryTerm,
    compactions: usize,
    serves_grep: bool,
}

impl MemorySizing {
    fn terms(&self) -> [(&MemoryTerm, usize); 9] {
        [
            (&self.segment, 1),
            (&self.segment_index, 1),
            (&self.head, 1),
            (&self.wal_tail, 1),
            (&self.content_object, 1),
            (&self.grep, usize::from(self.serves_grep)),
            (&self.read_working, 1),
            (&self.publication, 1),
            (&self.merge_input, self.compactions),
        ]
    }

    /// Builds the metadata cache with the sized limits, and with `ssd` as
    /// the SSD tier of its segment blocks if the config has one.
    pub(crate) fn metadata_cache(&self, ssd: Option<SsdSegmentBlocks>) -> MetadataCache {
        let builder = MetadataCache::builder()
            .max_segment_bytes(self.segment.bytes)
            .max_segment_index_bytes(self.segment_index.bytes)
            .max_head_bytes(self.head.bytes)
            .max_wal_tail_bytes(self.wal_tail.bytes)
            .max_content_object_bytes(self.content_object.bytes);
        match ssd {
            Some(ssd) => builder.ssd_segment_blocks(ssd),
            None => builder,
        }
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
        Err(ServerConfigError::Field(ConfigFieldError::Invalid {
            field: "memory_limit_bytes",
            reason: format!(
                "memory terms must fit the limit and managed memory must be at least {} bytes; limit = {}, headroom = {}, managed = {}, total = {}; {}",
                256 * MIB, limit.bytes, self.headroom, self.managed, total, terms.join(", ")
            ),
        }))
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
                self.max_connections.get() as u128 * 472 * 1024,
            ),
            (
                "transfers ((`max_concurrent_uploads` + `max_concurrent_downloads`) x 8 MiB)",
                (self.max_concurrent_uploads.get() as u128
                    + self.max_concurrent_downloads.get() as u128)
                    * (8 * MIB) as u128,
            ),
            (
                "segment output ((`max_concurrent_folds` + `max_concurrent_compactions`) x 32 MiB)",
                (self.max_concurrent_folds.get() as u128
                    + self.max_concurrent_compactions.get() as u128)
                    * loonfs::MAX_PENDING_SEGMENT_BYTES as u128,
            ),
            (
                "content writes (`max_concurrent_content_writes` x 16 MiB, or 72 MiB on R2)",
                self.max_concurrent_content_writes.get() as u128
                    * u128::from(self.store.kind().max_assembly_buffer_bytes()),
            ),
            (
                "completed upload sessions",
                loonfs::DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES as u128,
            ),
            (
                "ssd tier (64 MiB write queue + `metadata_cache.ssd.max_segment_bytes` / 4 KiB x 168 B)",
                self.metadata_cache.ssd.as_ref().map_or(0, |ssd| {
                    SsdSegmentBlocks::memory_bytes(ssd.max_segment_bytes) as u128
                }),
            ),
        ];
        let fixed_bytes: u128 = fixed.iter().map(|(_, bytes)| bytes).sum();
        // The pools share four fifths of what the limit has left after
        // `fixed` and the floor, so the fifth that the headroom takes is a
        // quarter of the pools.
        let headroom = limit.map_or(0, |limit| {
            let rest =
                (limit as u128).saturating_sub(fixed_bytes + ALLOCATOR_HEADROOM_FLOOR as u128);
            ALLOCATOR_HEADROOM_FLOOR + (rest / 5) as usize
        });
        // The budgets of heads and of segment index blocks, filter blocks,
        // and manifests aren't shares of managed memory, because the memory
        // that those entries need depends on the namespaces, not on the host.
        let segment_index = MemoryTerm::new(
            "metadata_cache.max_segment_index_bytes",
            self.metadata_cache.max_segment_index_bytes,
            None,
            loonfs::DEFAULT_MAX_SEGMENT_INDEX_BYTES,
        );
        let head = MemoryTerm::new(
            "metadata_cache.max_head_bytes",
            self.metadata_cache.max_head_bytes,
            None,
            loonfs::DEFAULT_MAX_HEAD_BYTES,
        );
        let managed = (limit.unwrap_or(0) as u128).saturating_sub(
            fixed_bytes + headroom as u128 + segment_index.bytes as u128 + head.bytes as u128,
        ) as usize;
        let share =
            |percent: usize| limit.map(|_| (managed as u128 * percent as u128 / 100) as usize);
        let serves_grep = self.grep.mode.serves();
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
                share(if serves_grep { 30 } else { 40 }),
                loonfs::DEFAULT_MAX_SEGMENT_BYTES,
            ),
            segment_index,
            head,
            wal_tail: MemoryTerm::new(
                "metadata_cache.max_wal_tail_bytes",
                self.metadata_cache.max_wal_tail_bytes,
                share(10),
                loonfs::DEFAULT_MAX_WAL_TAIL_BYTES,
            ),
            content_object: MemoryTerm::new(
                "metadata_cache.max_content_object_bytes",
                self.metadata_cache.max_content_object_bytes,
                share(10),
                loonfs::DEFAULT_MAX_CONTENT_OBJECT_BYTES,
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
                share(10).map(|bytes| bytes / self.max_concurrent_compactions.get()),
                default_max_merge_input_bytes(),
            ),
            compactions: self.max_concurrent_compactions.get(),
            serves_grep,
        };
        memory.validate()?;
        validate_positive(
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

fn validate_positive(
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
    Err(ServerConfigError::Field(ConfigFieldError::Invalid {
        field,
        reason,
    }))
}

fn validate_socket_addr(field: &'static str, value: &str) -> Result<SocketAddr, ServerConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerConfigError::Field(ConfigFieldError::Missing {
            field,
        }));
    }
    trimmed.parse::<SocketAddr>().map_err(|err| {
        ServerConfigError::Field(ConfigFieldError::Invalid {
            field,
            reason: err.to_string(),
        })
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

    use super::{
        load_server_config, ConfigFieldError, InlineContentConfig, PublicationLimitsConfig,
        ServerConfigError, AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV, READ_REVALIDATION_BOUND_MS,
    };
    use loonfs_test_support::EnvGuard;
    use std::fs;
    use std::num::NonZeroUsize;
    use tempfile::tempdir;

    fn memory_config(settings: &str) -> super::ServerConfig {
        toml::from_str(&format!(
            r#"
bind = "127.0.0.1:9400"
writer_id = "memory-test"
content_token_secret = "test-secret"
{settings}
[store]
kind = "local_fs"
root = "unused"
"#
        ))
        .expect("parse memory config")
    }

    #[test]
    fn four_gib_derives_each_pool_and_folds_unused_grep_into_segments() {
        let mut config = memory_config("memory_limit_bytes = 4294967296");
        for mode in [
            super::JobMode::ServeOnly,
            super::JobMode::Disabled,
            super::JobMode::MaintainOnly,
        ] {
            config.grep.mode = mode;
            let memory = config.memory_sizing(Some(1)).expect("explicit limit wins");
            assert_eq!(
                memory.fixed.map(|(_, bytes)| bytes),
                [
                    134_217_728,
                    494_927_872,
                    201_326_592,
                    134_217_728,
                    536_870_912,
                    33_554_432,
                    0
                ]
            );
            assert_eq!(memory.headroom, 719_742_566);
            assert_eq!(
                (memory.head.bytes, memory.head.source),
                (16_777_216, "default"),
                "the head budget isn't derived"
            );
            assert_eq!(
                (memory.segment_index.bytes, memory.segment_index.source),
                (33_554_432, "default"),
                "the segment index budget isn't derived"
            );
            assert_eq!(memory.managed, 1_989_777_818);
            assert_eq!(
                memory.segment.bytes,
                if mode.serves() {
                    596_933_345
                } else {
                    795_911_127
                }
            );
            assert_eq!(memory.wal_tail.bytes, 198_977_781);
            assert_eq!(memory.content_object.bytes, 198_977_781);
            assert_eq!(
                memory.grep.bytes,
                if mode.serves() { 198_977_781 } else { 0 }
            );
            assert_eq!(memory.read_working.bytes, 397_955_563);
            assert_eq!(memory.publication.bytes, 198_977_781);
            assert_eq!(memory.merge_input.bytes, 99_488_890);
            for (term, _) in memory.terms() {
                if ![memory.head.name, memory.segment_index.name].contains(&term.name) {
                    assert_eq!(term.source, "derived");
                }
            }
        }
        assert_eq!(config.max_concurrent_reads.get(), 64);
        assert_eq!(config.max_concurrent_folds.get(), 2);
        assert_eq!(config.max_concurrent_compactions.get(), 2);
        assert_eq!(config.max_concurrent_content_writes.get(), 32);
        assert_eq!(config.max_in_flight_requests.get(), 256);
        assert_eq!(config.max_connections.get(), 1024);
        assert_eq!(config.max_concurrent_uploads.get(), 8);
        assert_eq!(config.max_concurrent_downloads.get(), 16);
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
        assert_eq!(memory.wal_tail.bytes, 198_977_781);
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
max_concurrent_content_writes = 8
max_read_working_bytes = 0
max_merge_input_bytes = 67108864
[metadata_cache]
max_segment_bytes = 67108864
max_segment_index_bytes = 4194304
max_head_bytes = 8388608
max_wal_tail_bytes = 0
max_content_object_bytes = 12582912
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
            [
                134_217_728,
                247_463_936,
                67_108_864,
                201_326_592,
                134_217_728,
                33_554_432,
                0
            ]
        );
        assert_eq!(
            memory.terms().map(|(term, count)| (term.bytes, count)),
            [
                (67_108_864, 1),
                (4_194_304, 1),
                (8_388_608, 1),
                (0, 1),
                (12_582_912, 1),
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

    /// The terms that the error for a failed memory check lists.
    const EVERY_MEMORY_TERM: [&str; 23] = [
        "memory_limit_bytes",
        "process",
        "max_connections",
        "max_concurrent_uploads",
        "max_concurrent_downloads",
        "max_concurrent_folds",
        "max_concurrent_compactions",
        "max_concurrent_content_writes",
        "completed upload sessions",
        "metadata_cache.ssd.max_segment_bytes",
        "headroom",
        "managed",
        "268435456",
        "metadata_cache.max_segment_bytes",
        "metadata_cache.max_segment_index_bytes",
        "metadata_cache.max_head_bytes",
        "metadata_cache.max_wal_tail_bytes",
        "metadata_cache.max_content_object_bytes",
        "grep.max_cache_bytes",
        "max_read_working_bytes",
        "publication.max_estimated_bytes",
        "max_merge_input_bytes",
        "total",
    ];

    #[test]
    fn insufficient_memory_names_every_term_before_startup() {
        for limit in [512 * super::MIB, 1024 * super::MIB] {
            let config = memory_config(&format!("memory_limit_bytes = {limit}"));
            let error = config
                .validate()
                .expect_err("managed memory is below 256 MiB");
            let message = error.to_string();
            for term in EVERY_MEMORY_TERM {
                assert!(message.contains(term), "missing {term}: {message}");
            }
            assert_invalid_field(error, "memory_limit_bytes");
        }
    }

    #[test]
    fn two_gib_fits_the_default_counts() {
        let memory = memory_config("memory_limit_bytes = 2147483648")
            .validate()
            .expect("2 GiB fits the default counts");
        assert_eq!(memory.headroom, 290_245_836);
        assert_eq!(memory.managed, 271_790_900);
    }

    #[test]
    fn content_writes_reserve_one_part_and_one_read_each_and_72_mib_on_r2() {
        let local = memory_config("max_concurrent_content_writes = 4");
        let mut r2 = local.clone();
        r2.store = toml::from_str(
            r#"
kind = "cloudflare_r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"

[credentials]
kind = "static"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .expect("parse an R2 store");
        for (config, write_bytes) in [(local, 16 * super::MIB), (r2, 72 * super::MIB)] {
            let memory = config.memory_sizing(None).expect("unsized config");
            let (name, bytes) = memory.fixed[4];
            assert!(name.contains("max_concurrent_content_writes"), "{name}");
            assert_eq!(bytes, 4 * write_bytes as u128);
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
        assert_eq!(memory.segment_index.bytes, 33_554_432);
        assert_eq!(memory.head.bytes, 16_777_216);
        assert_eq!(memory.wal_tail.bytes, 67_108_864);
        assert_eq!(memory.content_object.bytes, 100_663_296);
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
        assert_eq!(memory.managed, 1_989_777_818);
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
kind = "aws_s3"
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.maintenance, super::JobMode::ServeAndMaintain);
        assert!(config.maintenance.serves());
        assert!(config.maintenance.maintains());

        for (spelling, mode, serves, maintains) in [
            ("disabled", super::JobMode::Disabled, false, false),
            ("serve_only", super::JobMode::ServeOnly, true, false),
            ("maintain_only", super::JobMode::MaintainOnly, false, true),
            (
                "serve_and_maintain",
                super::JobMode::ServeAndMaintain,
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

[grep]
mode = "{spelling}"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ));
            let config = load_server_config(&path).expect("valid config");
            assert_eq!(config.maintenance, mode);
            assert_eq!(config.grep.mode, mode);
            assert_eq!(config.maintenance.serves(), serves);
            assert_eq!(config.maintenance.maintains(), maintains);
        }
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("blank writer fields");

        let ServerConfigError::Decode(message) = &error else {
            panic!("expected a decode error for the blank writer id, got {error:?}");
        };
        assert!(message.contains("must not be blank"), "{message}");
    }

    #[test]
    fn load_rejects_blank_provider_required_fields() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "cloudflare_r2"
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
kind = "aws_s3"
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
kind = "cloudflare_r2"
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
        let aws_error = load_server_config(&aws_path).expect_err("invalid aws endpoint");
        let r2_error = load_server_config(&r2_path).expect_err("invalid r2 endpoint");

        assert_invalid_field(aws_error, "store.endpoint_url");
        assert_invalid_field(r2_error, "store.endpoint_url");
    }

    #[test]
    fn load_rejects_blank_gcs_bucket() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "gcp_gcs"
bucket = " "
key_prefix = "demo"

[store.credentials]
kind = "service_account_file"
path = "/tmp/service-account.json"
"#,
        );

        let error = load_server_config(&path).expect_err("blank gcs bucket");

        assert_missing_field(error, "store.bucket");
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
kind = "local_fs"
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
        assert_eq!(InlineContentConfig::default().effective_policy(), defaults);
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
            let overrides: InlineContentConfig = toml::from_str(source).expect("overrides");
            assert_eq!(
                overrides.effective_policy().inline_content_threshold_bytes,
                threshold
            );
        }
        toml::from_str::<InlineContentConfig>("inline_content_threshold_bytes = true")
            .expect_err("a boolean threshold should be rejected");
    }

    #[test]
    fn publication_overrides_are_strict_and_positive() {
        let defaults: PublicationLimitsConfig = toml::from_str("").expect("empty overrides");
        assert_eq!(
            defaults.effective_policy(),
            loonfs::PublicationPolicy::default()
        );
        for field in [
            "max_requests",
            "max_requests_per_namespace",
            "max_estimated_bytes",
            "max_estimated_bytes_per_namespace",
            "max_concurrent_publications",
        ] {
            toml::from_str::<PublicationLimitsConfig>(&format!("{field} = 0"))
                .expect_err(&format!("`{field} = 0` should be rejected"));
        }
        toml::from_str::<PublicationLimitsConfig>("max_requsets = 10")
            .expect_err("a misspelled field should be rejected");
        let overrides: PublicationLimitsConfig =
            toml::from_str("max_requests = 12\nmax_requests_per_namespace = 3").expect("overrides");
        assert_eq!(overrides.max_requests, std::num::NonZeroUsize::new(12));
        assert_eq!(
            overrides
                .effective_policy()
                .max_requests_per_namespace
                .get(),
            3
        );
        assert_eq!(
            overrides
                .effective_policy()
                .max_estimated_bytes_per_namespace,
            loonfs::PublicationPolicy::default().max_estimated_bytes_per_namespace
        );
    }

    /// Checks that the server's write-stop wait is half its request deadline,
    /// so a commit that waits for the whole write-stop wait returns
    /// `maintenance_required` before its request returns `deadline_exceeded`.
    #[test]
    fn the_write_stop_wait_is_half_the_request_deadline() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
request_deadline_ms = 10000

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.publication_policy().write_stop_wait_budget_ms, 5_000);
    }

    #[test]
    fn transfer_bounds_default_and_reject_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.max_download_bytes, 256 * 1024 * 1024);
        assert_eq!(
            config.max_concurrent_reads.get(),
            loonfs::DEFAULT_MAX_CONCURRENT_READS
        );
        assert_eq!(
            config.max_concurrent_folds.get(),
            loonfs::DEFAULT_MAX_CONCURRENT_FOLDS
        );
        assert_eq!(
            config.max_concurrent_compactions.get(),
            loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS
        );
        assert_eq!(
            config.max_concurrent_content_writes.get(),
            loonfs::DEFAULT_MAX_CONCURRENT_CONTENT_WRITES
        );
        for field in ["max_in_flight_requests", "max_connections"] {
            let mut oversized = config.clone();
            let value = NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1)
                .expect("a bound past the permit limit is nonzero");
            match field {
                "max_in_flight_requests" => oversized.max_in_flight_requests = value,
                _ => oversized.max_connections = value,
            }
            assert_invalid_field(oversized.validate().expect_err("oversized bound"), field);
        }
        assert_eq!(config.max_in_flight_requests.get(), 256);
        assert_eq!(config.max_connections.get(), 1024);
        assert_eq!(config.max_concurrent_uploads.get(), 8);
        assert_eq!(config.max_concurrent_downloads.get(), 16);
        assert_eq!(config.max_concurrent_maintenance.get(), 8);
        assert_eq!(config.tick_interval_ms, 5_000);
        assert_eq!(config.maintenance_interval_ms, 300_000);
        assert_eq!(config.gc_interval_ms, 3_600_000);
        assert_eq!(config.full_sweep_interval_ms, 86_400_000);
        assert_eq!(config.idle_session_close_after_ms, 1_800_000);

        for field in [
            "max_download_bytes",
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ));
            let error = load_server_config(&path).expect_err("zero bound must be rejected");
            assert_invalid_field(error, field);
        }
        for field in [
            "max_concurrent_reads",
            "max_in_flight_requests",
            "max_connections",
            "max_concurrent_folds",
            "max_concurrent_compactions",
            "max_concurrent_content_writes",
            "max_concurrent_uploads",
            "max_concurrent_downloads",
            "max_concurrent_maintenance",
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{field} = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ));
            match load_server_config(&path).expect_err("a zero count must be rejected") {
                ServerConfigError::Decode(message) => assert!(
                    message.contains(field) && message.contains("expected a nonzero usize"),
                    "the decode error should identify `{field}` and say why: {message}"
                ),
                other => panic!("expected a decode error for `{field}`, got {other:?}"),
            }
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
kind = "local_fs"
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ));
            let config = load_server_config(&path).expect("valid config");
            assert_eq!(config.idle_fold_after_ms, expected, "{setting:?}");
        }
    }

    #[test]
    fn a_retention_window_is_optional_and_must_be_positive() {
        let config = |setting: &str| {
            write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{setting}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ))
        };
        for (setting, expected) in [
            ("", None),
            (
                "retention_window_ms = 2592000000",
                std::num::NonZeroU64::new(2_592_000_000),
            ),
        ] {
            let loaded = load_server_config(config(setting)).expect("valid config");
            assert_eq!(loaded.retention_window_ms, expected, "{setting:?}");
        }
        match load_server_config(config("retention_window_ms = 0"))
            .expect_err("a zero window must be rejected")
        {
            ServerConfigError::Decode(message) => assert!(
                message.contains("retention_window_ms")
                    && message.contains("expected a nonzero u64"),
                "the decode error should identify `retention_window_ms` and say why: {message}"
            ),
            other => panic!("expected a decode error, got {other:?}"),
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
kind = "aws_s3"
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
kind = "local_fs"
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
kind = "aws_s3"
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
            super::StoreConfig::AwsS3(store) => {
                let loonfs_objectstore::AwsS3Credentials::Static {
                    access_key_id,
                    secret_access_key,
                    ..
                } = store.credentials
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );
        let store_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
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
max_wal_tail_byte = 2

[store]
kind = "local_fs"
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        for (path, typo) in [
            (top_level, "lease_duration"),
            (store_level, "key_prefiks"),
            (metadata_cache_level, "max_wal_tail_byte"),
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
kind = "local_fs"
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

    /// Checks that a manifest check interval or a maximum staleness above the
    /// revalidation bound fails to load and fails `validate`, and that the
    /// bound itself loads.
    #[test]
    fn read_intervals_above_the_revalidation_bound_are_rejected() {
        let bound = READ_REVALIDATION_BOUND_MS;
        for field in ["manifest_revalidation_interval_ms", "max_staleness_ms"] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
{field} = {}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
                bound + 1
            ));
            let error = load_server_config(&path).expect_err("a setting above the bound");
            assert_invalid_field(error, field);
        }

        let path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
manifest_revalidation_interval_ms = {bound}
max_staleness_ms = {bound}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
        ));
        let config = load_server_config(&path).expect("the bound itself loads");
        assert_eq!(config.manifest_revalidation_interval_ms, Some(bound));
        assert_eq!(config.max_staleness_ms, Some(bound));
        for field in ["manifest_revalidation_interval_ms", "max_staleness_ms"] {
            let mut oversized = config.clone();
            match field {
                "manifest_revalidation_interval_ms" => {
                    oversized.manifest_revalidation_interval_ms = Some(bound + 1)
                }
                _ => oversized.max_staleness_ms = Some(bound + 1),
            }
            assert_invalid_field(
                oversized.validate().expect_err("a setting above the bound"),
                field,
            );
        }
    }

    #[test]
    fn load_uses_default_metadata_cache_when_omitted() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.metadata_cache, super::MetadataCacheConfig::default());
        assert_eq!(config.manifest_revalidation_interval_ms, None);
        assert_eq!(config.max_staleness_ms, None);
        assert_eq!(config.consistency, super::ReadConsistency::Eventual);
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
max_staleness_ms = 5000
consistency = "strong"
max_read_working_bytes = 8192

[metadata_cache]
max_segment_bytes = 16384
max_segment_index_bytes = 2048
max_head_bytes = 1024
max_wal_tail_bytes = 4096
max_content_object_bytes = 3072

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.manifest_revalidation_interval_ms, Some(250));
        assert_eq!(config.max_staleness_ms, Some(5000));
        assert_eq!(config.consistency, super::ReadConsistency::Strong);
        assert_eq!(config.max_read_working_bytes, Some(8192));
        assert_eq!(
            config.metadata_cache,
            super::MetadataCacheConfig {
                max_segment_bytes: Some(16384),
                max_segment_index_bytes: Some(2048),
                max_head_bytes: Some(1024),
                max_wal_tail_bytes: Some(4096),
                max_content_object_bytes: Some(3072),
                ssd: None,
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
max_segment_index_bytes = 0
max_head_bytes = 0
max_wal_tail_bytes = 0
max_content_object_bytes = 0

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(
            config.metadata_cache,
            super::MetadataCacheConfig {
                max_segment_bytes: Some(0),
                max_segment_index_bytes: Some(0),
                max_head_bytes: Some(0),
                max_wal_tail_bytes: Some(0),
                max_content_object_bytes: Some(0),
                ssd: None,
            }
        );
    }

    /// Checks that a config without `[metadata_cache.ssd]` has no SSD tier,
    /// that the table needs both of its settings, and that the server
    /// rejects a zero budget and an empty directory.
    #[test]
    fn the_ssd_table_is_optional_and_needs_both_settings() {
        let config = |ssd: &str| {
            write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

{ssd}

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            ))
        };
        let absent = load_server_config(config("")).expect("no ssd table");
        assert_eq!(absent.metadata_cache.ssd, None);

        let present = load_server_config(config(
            "[metadata_cache.ssd]\ndirectory = \"/var/lib/loonfs/ssd\"\nmax_segment_bytes = 1073741824",
        ))
        .expect("an ssd table with both settings");
        assert_eq!(
            present.metadata_cache.ssd,
            Some(super::MetadataCacheSsdConfig {
                directory: "/var/lib/loonfs/ssd".to_owned(),
                max_segment_bytes: NonZeroUsize::new(1 << 30).expect("nonzero"),
            })
        );

        for (ssd, field, reason) in [
            (
                "max_segment_bytes = 1073741824",
                "directory",
                "missing field",
            ),
            ("directory = \"/ssd\"", "max_segment_bytes", "missing field"),
            (
                "directory = \"/ssd\"\nmax_segment_bytes = 0",
                "max_segment_bytes",
                "expected a nonzero usize",
            ),
        ] {
            match load_server_config(config(&format!("[metadata_cache.ssd]\n{ssd}"))) {
                Err(ServerConfigError::Decode(message)) => assert!(
                    message.contains(field) && message.contains(reason),
                    "the decode error should identify `{field}` and say why: {message}"
                ),
                other => panic!("expected a decode error for `{field}`, got {other:?}"),
            }
        }
        let empty = config("[metadata_cache.ssd]\ndirectory = \" \"\nmax_segment_bytes = 1");
        assert_missing_field(
            load_server_config(empty).expect_err("an empty directory"),
            "metadata_cache.ssd.directory",
        );
    }

    /// Checks that the SSD tier adds its write queue and the estimated
    /// memory of its index to `fixed`, which the pools are sized after.
    #[test]
    fn the_ssd_tier_adds_its_write_queue_and_index_to_fixed() {
        let without = memory_config("memory_limit_bytes = 8589934592");
        let with = memory_config(
            "memory_limit_bytes = 8589934592\n[metadata_cache.ssd]\ndirectory = \"/ssd\"\nmax_segment_bytes = 10737418240",
        );
        let without = without.memory_sizing(None).expect("sized without a tier");
        let with = with.memory_sizing(None).expect("sized with a tier");
        let (name, bytes) = with.fixed[6];
        assert!(
            name.contains("metadata_cache.ssd.max_segment_bytes"),
            "{name}"
        );
        // 64 MiB of queue, and 10 GiB / 4 KiB = 2,621,440 entries of 168 B.
        assert_eq!(bytes, 67_108_864 + 2_621_440 * 168);
        assert_eq!(without.fixed[6].1, 0);
        assert!(with.managed < without.managed);
    }

    #[test]
    fn an_omitted_grep_table_composes_no_grep() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[store]
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.grep.mode, super::JobMode::Disabled);
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("mode is required");
        assert!(error.to_string().contains("mode"), "{error}");
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#
            )))
        };

        let default = load("").expect("load the default step limit");
        assert_eq!(default.grep.worker.max_concurrent_steps, 2);
        let configured = load("max_concurrent_steps = 5").expect("load a step limit");
        assert_eq!(configured.grep.worker.max_concurrent_steps, 5);
        match load("max_concurrent_steps = 0") {
            Err(ServerConfigError::Field(ConfigFieldError::Invalid {
                field: "grep",
                reason,
            })) => assert!(reason.contains("max_concurrent_steps"), "{reason}"),
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
kind = "local_fs"
root = "/tmp/loonfs-server"
"#,
        );

        let grep = load_server_config(&path).expect("load config").grep;
        assert_eq!(grep.mode, super::JobMode::ServeOnly);
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
kind = "local_fs"
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
kind = "local_fs"
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
max_wal_tail_bytes = -1

[store]
kind = "local_fs"
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
            examples >= 4,
            "expected at least 4 server example configs, found {examples}"
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
            ServerConfigError::Field(ConfigFieldError::Invalid { field: actual, .. }) => {
                assert_eq!(actual, field)
            }
            other => panic!("expected invalid field error for {field}, got {other:?}"),
        }
    }

    fn assert_missing_field(error: ServerConfigError, field: &'static str) {
        match error {
            ServerConfigError::Field(ConfigFieldError::Missing { field: actual }) => {
                assert_eq!(actual, field)
            }
            other => panic!("expected missing field error for {field}, got {other:?}"),
        }
    }
}
