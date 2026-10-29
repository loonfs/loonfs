//! The server config file's top-level settings, its `[auth]` and
//! `[transport]` tables, and their defaults.

use super::{GrepConfig, JobMode, MemorySizing, MetadataCacheConfig, ReadConsistency, StoreConfig};
use crate::{Result, ServerError};
use loonfs::metrics::MetricsRecorder;
use loonfs::ExecutionBudget;
use loonfs_objectstore::ConfiguredObjectStore;
use loonfs_types::config::ConfigFieldError;
use loonfs_types::{SecretString, WriterId};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

/// The server config file.
///
/// # Secret precedence
///
/// The `[auth]` table's `token` and `content_token_secret` may be supplied
/// through the `LOONFS_AUTH_TOKEN` and `LOONFS_CONTENT_TOKEN_SECRET`
/// environment variables instead of the file. A non-blank value in the file
/// takes precedence; blank environment values are ignored. Object-store
/// credentials follow the source explicitly selected by the nested
/// `credentials` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub memory_limit_bytes: Option<usize>,
    /// How the server authenticates requests. Without an `[auth]` table,
    /// the server takes a bearer token from `LOONFS_AUTH_TOKEN` when that is
    /// set, and otherwise serves without authentication, which validation
    /// accepts only on a loopback bind.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// Signs content tokens; unset or empty here falls back to
    /// `LOONFS_CONTENT_TOKEN_SECRET`.
    #[serde(default)]
    pub content_token_secret: SecretString,
    pub writer_id: WriterId,
    /// Maximum public reads running at once. Reads past this limit wait.
    /// Defaults to the runtime's
    /// [`DEFAULT_MAX_CONCURRENT_READS`](loonfs::DEFAULT_MAX_CONCURRENT_READS),
    /// 64.
    pub max_concurrent_reads: Option<NonZeroUsize>,
    #[serde(default = "default_max_in_flight_requests")]
    pub max_in_flight_requests: NonZeroUsize,
    #[serde(default = "default_max_connections")]
    pub max_connections: NonZeroUsize,
    /// Maximum WAL folds this server runs concurrently. A sustained
    /// `loonfs.execution_budget.folds_waiting` gauge means this cap is too
    /// low. Defaults to the runtime's
    /// [`DEFAULT_MAX_CONCURRENT_FOLDS`](loonfs::DEFAULT_MAX_CONCURRENT_FOLDS),
    /// 2.
    pub max_concurrent_folds: Option<NonZeroUsize>,
    /// Maximum metadata compaction units this server runs at once, whether
    /// writer sessions, the maintenance sweep, or maintenance requests start
    /// them. A merge never takes a fold permit. Defaults to the runtime's
    /// [`DEFAULT_MAX_CONCURRENT_COMPACTIONS`](loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS),
    /// 2.
    pub max_concurrent_compactions: Option<NonZeroUsize>,
    /// Maximum content object writes this server runs at once, including the
    /// content that folds write. A sustained
    /// `loonfs.execution_budget.content_writes_waiting` gauge means this cap
    /// is too low. Defaults to the runtime's
    /// [`DEFAULT_MAX_CONCURRENT_CONTENT_WRITES`](loonfs::DEFAULT_MAX_CONCURRENT_CONTENT_WRITES),
    /// 32.
    pub max_concurrent_content_writes: Option<NonZeroUsize>,
    /// Maximum publication requests admitted and not yet settled, across
    /// every namespace. Defaults to 8,192.
    pub max_admitted_requests: Option<NonZeroUsize>,
    /// Approximate retained bytes of the publication requests admitted and
    /// not yet settled, across every namespace. Derived when sized, and
    /// otherwise 64 MiB.
    pub max_admitted_bytes: Option<NonZeroUsize>,
    /// Maximum publication batches running at once. Defaults to 8.
    pub max_concurrent_publications: Option<NonZeroUsize>,
    /// The per-namespace admission limits of the server's runtime. The
    /// write-stop wait isn't a key of this table: the server sets it to half
    /// of `request_deadline_ms`, so a commit that waits for the whole wait
    /// returns `maintenance_required` before its request returns
    /// `deadline_exceeded`.
    #[serde(default)]
    pub publication: loonfs::PublicationPolicy,
    /// The inline content policy of the server's runtime. Inline writes are
    /// on with a 64 KiB threshold by default, and
    /// `inline_content_threshold_bytes = false` turns them off.
    #[serde(default)]
    pub inline_content: loonfs::InlineContentPolicy,
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
    /// Minimum time between publication starts in one namespace, in
    /// milliseconds.
    ///
    /// A request to an idle namespace publishes immediately. A request that
    /// arrives less than the interval after the last publication started
    /// waits until the interval has passed, and the next WAL object includes
    /// the commits queued in that time. A longer interval writes fewer,
    /// larger WAL objects and keeps the namespace's fold rate low. (The
    /// interval bounds how many WAL objects the namespace writes each second,
    /// and so how many folds it needs, and each fold costs segment and
    /// manifest puts and reads.) A shorter interval
    /// lowers commit latency and raises throughput. Measured on S3 with 256
    /// clients that each wait for their commits, one namespace published 244
    /// commits per second with a median commit latency of 1,049 ms at an
    /// interval of 1,000 ms. At 100 ms, it published 1,330 commits per second
    /// with a median latency of 183 ms, and at 15 ms (the embedded default),
    /// 1,930 per second with a median latency of 127 ms. The default of
    /// 200 ms writes at most five WAL objects a second per namespace and
    /// avoids most of the latency and throughput cost of a 1,000 ms interval.
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
    /// Defaults to the runtime's
    /// [`DEFAULT_MAX_READ_CONTENT_BYTES`](loonfs::DEFAULT_MAX_READ_CONTENT_BYTES),
    /// 256 MiB.
    pub max_download_bytes: Option<u64>,
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
    /// the namespace's retention floor past that history, in milliseconds,
    /// for each namespace without a retention window of its own. Each full
    /// pass advances every active namespace's floor to the last commit at or
    /// before the current time minus the namespace's window, or minus this
    /// setting for a namespace that has no window of its own. Absent, the
    /// server advances only the floors of namespaces that have their own
    /// window.
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
    /// period. Zero turns the rule off. Defaults to the runtime's
    /// [`DEFAULT_IDLE_FOLD_AFTER_MS`](loonfs::DEFAULT_IDLE_FOLD_AFTER_MS), 15
    /// minutes.
    pub idle_fold_after_ms: Option<u64>,
    /// How the server's listener carries requests. Without a `[transport]`
    /// table, the server serves plaintext HTTP, which validation accepts
    /// only on a loopback bind, because the wire carries the bearer token
    /// and the presigned object-store URLs that the upload routes return.
    #[serde(default)]
    pub transport: Option<TransportConfig>,
    pub store: StoreConfig,
}

/// The server's `[auth]` table: how the server authenticates requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthConfig {
    /// Every request except the health and readiness probes sends `token`
    /// as `Authorization: Bearer <token>`. A blank or omitted `token` falls
    /// back to `LOONFS_AUTH_TOKEN`.
    BearerToken {
        #[serde(default)]
        token: SecretString,
    },
    /// The server serves every request without authentication, on any
    /// bind.
    Open,
}

/// The server's `[transport]` table: how the server's listener carries
/// requests.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TransportConfig {
    /// The server terminates TLS with this identity.
    Tls(TlsServerConfig),
    /// The server serves plaintext HTTP on any bind, because TLS terminates
    /// in a proxy in front of it.
    PlaintextBehindProxy,
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
    200
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
    // The download default, so anything the proxy accepted, the proxy will
    // serve back. Content ingested past this through `direct_put` needs a
    // raised limit to be read through the server.
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

impl ServerConfig {
    /// Builds the object store selected by this server configuration.
    pub fn object_store(&self) -> Result<ConfiguredObjectStore> {
        self.store.configured_object_store().map_err(|err| {
            ServerError::Config(ConfigFieldError::Invalid {
                field: "store",
                reason: err.public_message().into_owned(),
            })
        })
    }

    /// Returns the settings of the server's one runtime.
    pub(crate) fn runtime_config(&self) -> loonfs::RuntimeConfig {
        let defaults = loonfs::RuntimeConfig::default();
        loonfs::RuntimeConfig {
            // Every read the server serves goes through this runtime, so the
            // read cap covers every proxied content read.
            max_read_content_bytes: self
                .max_download_bytes
                .unwrap_or(defaults.max_read_content_bytes),
            manifest_revalidation_interval_ms: self
                .manifest_revalidation_interval_ms
                .unwrap_or(defaults.manifest_revalidation_interval_ms),
            max_staleness_ms: self.max_staleness_ms.unwrap_or(defaults.max_staleness_ms),
            consistency: self.consistency,
            trace_mode: loonfs::TraceMode::Remote,
            min_publish_interval_ms: self.min_publish_interval_ms,
            publication: loonfs::PublicationPolicy {
                write_stop_wait_budget_ms: self.request_deadline_ms / 2,
                ..self.publication.clone()
            },
            inline_content: self.inline_content.clone(),
            idle_fold_after_ms: match self.idle_fold_after_ms {
                Some(idle_fold_after_ms) => NonZeroU64::new(idle_fold_after_ms),
                None => defaults.idle_fold_after_ms,
            },
        }
    }

    /// Returns the cadence and limits of the maintenance sweep.
    pub(crate) fn sweep_config(&self) -> crate::sweep::SweepConfig {
        crate::sweep::SweepConfig {
            max_concurrent_maintenance: self.max_concurrent_maintenance,
            tick_interval_ms: self.tick_interval_ms,
            maintenance_interval_ms: self.maintenance_interval_ms,
            gc_interval_ms: self.gc_interval_ms,
            full_sweep_interval_ms: self.full_sweep_interval_ms,
            retention_window_ms: self.retention_window_ms,
            idle_session_close_after_ms: self.idle_session_close_after_ms,
        }
    }

    /// Builds the execution budget of the server's one runtime from its read,
    /// fold, compaction, content write, and merge input limits and its
    /// admission totals, reporting to `recorder`.
    pub(crate) fn execution_budget(
        &self,
        memory: &MemorySizing,
        recorder: Arc<dyn MetricsRecorder>,
    ) -> ExecutionBudget {
        let positive = |value: usize| {
            NonZeroUsize::new(value).expect("validated budget limits should be nonzero")
        };
        let mut builder = ExecutionBudget::builder()
            .max_merge_input_bytes(positive(memory.merge_input.bytes))
            .max_read_working_bytes(memory.read_working.bytes)
            .max_admitted_bytes(positive(memory.admitted.bytes))
            .metrics_recorder(recorder);
        if let Some(limit) = self.max_concurrent_reads {
            builder = builder.max_concurrent_reads(limit);
        }
        if let Some(limit) = self.max_concurrent_folds {
            builder = builder.max_concurrent_folds(limit);
        }
        if let Some(limit) = self.max_concurrent_compactions {
            builder = builder.max_concurrent_compactions(limit);
        }
        if let Some(limit) = self.max_concurrent_content_writes {
            builder = builder.max_concurrent_content_writes(limit);
        }
        if let Some(limit) = self.max_admitted_requests {
            builder = builder.max_admitted_requests(limit);
        }
        if let Some(limit) = self.max_concurrent_publications {
            builder = builder.max_concurrent_publications(limit);
        }
        builder.build()
    }
}
