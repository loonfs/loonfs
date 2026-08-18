//! Server configuration: strict TOML decoding of the listen address,
//! store, and runtime cache overrides.

use crate::local_cache::{DISK_BLOCK_BYTES, MIN_DISK_BYTES};
use loonfs::RuntimeCacheConfig;
use loonfs_api::env::{AUTH_TOKEN_ENV, CONTENT_TOKEN_SECRET_ENV};
use loonfs_api::SecretString;
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::{ConfiguredObjectStore, StoreConfigError};
use serde::Deserialize;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;

pub use loonfs_objectstore::StoreConfig;

/// The only request-authentication decisions exposed to HTTP helpers.
#[derive(Debug, Clone, Copy)]
pub(crate) enum AuthPolicy<'a> {
    Unauthenticated,
    BearerToken(&'a str),
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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub auth_token: Option<SecretString>,
    /// Signs content tokens; unset or empty here falls back to
    /// `LOONFS_CONTENT_TOKEN_SECRET`.
    #[serde(default)]
    pub content_token_secret: SecretString,
    pub writer_id: String,
    #[serde(default)]
    pub runtime_cache: RuntimeCacheConfigOverrides,
    /// The node-local cache of encoded metadata blocks, if this deployment
    /// keeps one. Absent means no local cache: every metadata block read
    /// that misses the decoded cache goes to object storage, which is the
    /// behavior of a server that never had this table.
    #[serde(default)]
    pub local_cache: Option<LocalCacheConfig>,
    /// What this server does about grep, plus the bounded-step budgets its
    /// index maintenance runs under. A config with no `[grep]` table
    /// composes no grep at all; a table that names no `mode` both serves
    /// queries and maintains the index.
    #[serde(default = "grep_absent")]
    pub grep: GrepConfig,
    /// Whether this server maintains the namespaces it touches by itself.
    /// Automatic by default; see [`MaintenanceMode`].
    #[serde(default)]
    pub maintenance: MaintenanceMode,
    /// Minimum interval between publication starts per namespace, in
    /// milliseconds. A cold namespace publishes immediately; the interval
    /// paces follow-up batches so hot namespaces amortize into fewer,
    /// larger WAL segments. The server default favors batch economy over
    /// the embedded default's latency bias.
    #[serde(default = "default_min_publish_interval_ms")]
    pub min_publish_interval_ms: u64,
    /// Maximum time a metadata or query request may run, in milliseconds.
    /// Streamed content and long-running operator work are exempt.
    #[serde(default = "default_request_deadline_ms")]
    pub request_deadline_ms: u64,
    /// Maximum time graceful shutdown waits for accepted requests to drain,
    /// in milliseconds. Writer and cache settlement continue afterward.
    #[serde(default = "default_shutdown_deadline_ms")]
    pub shutdown_deadline_ms: u64,
    /// Largest request body accepted for service-proxied upload content
    /// requests (`PUT .../uploads/{upload_id}/content`). Enforced
    /// incrementally while the body streams to the store, so it bounds the
    /// accepted transfer size, not per-request memory (streamed writes hold
    /// at most one internal part). Clients may use `direct_put` or direct
    /// multipart for larger transfers when the capability is advertised.
    /// Advertised as the `upload.max_content_bytes` capability limit.
    #[serde(default = "default_max_upload_bytes")]
    pub max_upload_bytes: u64,
    /// Largest file content a service-proxied read (`GET .../filesystem/
    /// content` and inode revision content) will buffer and return. Checked
    /// against resolved metadata before any content fetch; over-limit reads
    /// answer `content_too_large`. Advertised to clients as the
    /// `download.max_content_bytes` capability limit.
    #[serde(default = "default_max_download_bytes")]
    pub max_download_bytes: u64,
    /// How many proxied upload bodies the server will stream at once;
    /// requests past the cap answer `server_busy` before any transfer.
    /// Worst-case upload memory is this times one streamed part, since
    /// bodies forward to the store incrementally instead of buffering.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// How many proxied content reads the server will materialize at once;
    /// requests past the cap answer `server_busy` before any fetch.
    /// Worst-case download memory is this times `max_download_bytes`.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,
    /// How many writer-scheduled maintenance steps may run at once across
    /// every job and namespace. Each job runs at most one step per
    /// namespace at a time; this bounds the fan-out when a write burst
    /// crosses thresholds in many namespaces together. A step that waits
    /// for a permit takes the next one that frees.
    #[serde(default = "default_max_concurrent_maintenance")]
    pub max_concurrent_maintenance: usize,
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
#[derive(Debug, Clone, Deserialize)]
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
    60_000
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

fn default_max_concurrent_uploads() -> usize {
    8
}

fn default_max_concurrent_downloads() -> usize {
    16
}

fn default_max_concurrent_maintenance() -> usize {
    loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE
}

/// Default used when no `[grep]` table is present.
///
/// This disables grep routes and maintenance until the deployment explicitly
/// configures grep.
fn grep_absent() -> GrepConfig {
    GrepConfig {
        mode: GrepMode::Disabled,
        ..GrepConfig::default()
    }
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
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCacheConfig {
    /// Directory the cache owns. Created if missing; locked while this
    /// process runs, so two servers cannot share one.
    pub path: String,
    /// Bytes of memory the cache's in-memory tier may hold.
    pub memory_bytes: u64,
    /// Bytes of disk the cache's disk tier may hold. The tier claims this
    /// much space up front, in whole blocks, one file per block under
    /// `path`; six blocks is the smallest tier that may be configured.
    /// Raising this across a restart keeps what the directory holds, and
    /// lowering it starts the directory empty rather than leaving the
    /// blocks it no longer claims behind.
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCacheConfigOverrides {
    pub max_cached_namespaces: Option<usize>,
    pub max_cached_wal_tail_projection_rows: Option<usize>,
    pub max_cached_wal_tail_projection_decoded_bytes: Option<usize>,
    pub metadata_table_cache_max_decoded_bytes: Option<usize>,
}

/// Whether this server maintains the namespaces it touches.
///
/// One word decides it, and it is the only switch: `automatic` registers the
/// runtime's own jobs — metadata upkeep and collection — and the grep index
/// job when `[grep]`'s mode maintains, and lets the writer's runner schedule
/// all of them; `manual` registers nothing automatic and schedules nothing.
/// Explicit admin operations work identically either way, and the retention
/// floor is never advanced automatically under either.
///
/// Set `manual` on a write-serving node when a dedicated maintenance
/// process — `loonfs admin run --namespace ...`, or another server — owns
/// upkeep for these namespaces. Automatic maintenance covers namespaces
/// touched by the running process and namespaces explicitly assigned to a
/// maintenance host, so a deployment that switches this off has to assign
/// its namespaces somewhere.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceMode {
    /// The writer schedules its own metadata and collection steps, and the
    /// grep index job when the grep mode maintains.
    #[default]
    Automatic,
    /// Nothing is scheduled and no automatic job is registered. Explicit
    /// admin operations remain available.
    Manual,
}

impl MaintenanceMode {
    /// Whether this server registers automatic maintenance jobs at all.
    pub fn registers_automatic_jobs(self) -> bool {
        matches!(self, Self::Automatic)
    }

    /// The writer policy this mode selects.
    pub fn background_work(self) -> loonfs::FsBackgroundWork {
        match self {
            Self::Automatic => loonfs::FsBackgroundWork::Enabled,
            Self::Manual => loonfs::FsBackgroundWork::ManualOnly,
        }
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrepMode {
    /// Neither answer grep queries nor maintain the index.
    Disabled,
    /// Answer queries over an index some other process maintains.
    ServeOnly,
    /// Maintain the index without answering queries about it.
    MaintainOnly,
    /// Answer queries and maintain the index in this process.
    #[default]
    ServeAndMaintain,
}

impl GrepMode {
    /// Whether the grep query endpoint is supported.
    pub fn serves_grep(self) -> bool {
        matches!(self, Self::ServeOnly | Self::ServeAndMaintain)
    }

    /// Whether this server's writer registers the grep index job — which is
    /// also what the index-administration endpoints act through.
    pub fn maintains_index(self) -> bool {
        matches!(self, Self::MaintainOnly | Self::ServeAndMaintain)
    }
}

/// The server's `[grep]` table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GrepConfig {
    pub mode: GrepMode,
    /// Flattening preserves the existing `[grep]` keys while leaving the
    /// worker policy itself as their one in-memory owner.
    #[serde(flatten)]
    pub worker: GrepWorkerConfig,
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
    pub(crate) fn auth_policy(&self) -> AuthPolicy<'_> {
        match &self.auth_token {
            Some(token) => AuthPolicy::BearerToken(token.expose()),
            None => AuthPolicy::Unauthenticated,
        }
    }

    pub(crate) fn content_token_secret(&self) -> &str {
        self.content_token_secret.expose()
    }

    /// Fills `auth_token` and `content_token_secret` from the environment
    /// when the file left them unset. Non-blank file values win; blank
    /// environment values are ignored.
    ///
    fn apply_env_fallbacks(
        &mut self,
        auth_token_env: Option<String>,
        content_token_secret_env: Option<String>,
    ) {
        if self.auth_token.is_none() {
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

    /// Resolves runtime cache settings by applying server overrides to the defaults.
    pub fn runtime_cache_config(&self) -> RuntimeCacheConfig {
        let mut config = RuntimeCacheConfig::default();
        if let Some(value) = self.runtime_cache.max_cached_namespaces {
            config.max_cached_namespaces = value;
        }
        if let Some(value) = self.runtime_cache.max_cached_wal_tail_projection_rows {
            config.max_cached_wal_tail_projection_rows = value;
        }
        if let Some(value) = self
            .runtime_cache
            .max_cached_wal_tail_projection_decoded_bytes
        {
            config.max_cached_wal_tail_projection_decoded_bytes = value;
        }
        if let Some(value) = self.runtime_cache.metadata_table_cache_max_decoded_bytes {
            config.metadata_table_cache.max_decoded_bytes = value;
        }
        config
    }

    /// Builds the object store selected by this server configuration.
    pub fn object_store(&self) -> Result<ConfiguredObjectStore, ServerConfigError> {
        self.store
            .configured_object_store()
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store",
                reason: err.to_string(),
            })
    }

    /// The line `loonfs-server --check-config` prints when the config loads.
    ///
    /// It names the two things an operator checks first: the address the
    /// server would bind and the provider it would talk to.
    pub fn check_summary(&self) -> String {
        format!(
            "config ok: bind {}, store {}",
            self.bind.trim(),
            self.store.kind().as_str()
        )
    }

    /// Parses the bind address; the one authority for that conversion, used
    /// by validation and by serving.
    pub(crate) fn bind_addr(&self) -> Result<SocketAddr, ServerConfigError> {
        validate_socket_addr("bind", &self.bind)
    }

    pub(crate) fn validate(&self) -> Result<(), ServerConfigError> {
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
        if self.max_upload_bytes == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_upload_bytes",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.request_deadline_ms == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "request_deadline_ms",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.shutdown_deadline_ms == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "shutdown_deadline_ms",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_download_bytes == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_download_bytes",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_concurrent_uploads == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_concurrent_uploads",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_concurrent_downloads == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_concurrent_downloads",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_concurrent_maintenance == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_concurrent_maintenance",
                reason: "must be greater than zero; \
                         set `maintenance = \"manual\"` to disable scheduling"
                    .to_owned(),
            });
        }
        if let Some(local_cache) = &self.local_cache {
            require_non_empty("local_cache.path", &local_cache.path)?;
            if local_cache.memory_bytes == 0 {
                return Err(ServerConfigError::InvalidField {
                    field: "local_cache.memory_bytes",
                    reason: "must be greater than zero; \
                             omit the `[local_cache]` table to run without a local cache"
                        .to_owned(),
                });
            }
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

        Ok(())
    }
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

    use super::{load_server_config, ServerConfigError, DISK_BLOCK_BYTES, MIN_DISK_BYTES};
    use std::fs;
    use tempfile::tempdir;

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    struct EnvGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            std::env::set_var(name, value);
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match self.previous.take() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    /// Loading a config preserves its credential source. Environment
    /// credentials are read only when the object store is constructed.
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
    fn maintenance_defaults_to_automatic_and_accepts_manual() {
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
        assert_eq!(config.maintenance, super::MaintenanceMode::Automatic);
        assert!(config.maintenance.registers_automatic_jobs());
        assert_eq!(
            config.maintenance.background_work(),
            loonfs::FsBackgroundWork::Enabled
        );

        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
maintenance = "manual"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(
            config.maintenance,
            super::MaintenanceMode::Manual,
            "write-serving nodes can hand maintenance to a dedicated process"
        );
        assert!(!config.maintenance.registers_automatic_jobs());
        assert_eq!(
            config.maintenance.background_work(),
            loonfs::FsBackgroundWork::ManualOnly
        );
    }

    #[test]
    fn the_retired_background_maintenance_key_is_no_longer_a_key() {
        // One word decides automatic maintenance now, and `maintenance` is
        // the word. The boolean it replaced fails through strict decoding
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
                assert!(message.contains("automatic"), "{message}");
                assert!(message.contains("manual"), "{message}");
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
        // LOONFS_AUTH_TOKEN in the environment would legitimately fill the
        // token and make this config valid; only assert when it is unset.
        if std::env::var("LOONFS_AUTH_TOKEN").is_ok() {
            return;
        }
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
    fn loopback_bind_accepts_tls() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
writer_id = "loonfs-server"

[tls]
cert_path = "/etc/loonfs/tls/server.crt"
key_path = "/etc/loonfs/tls/server.key"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        load_server_config(&path).expect("loopback tls config loads");
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
        assert_eq!(config.max_concurrent_uploads, 8);
        assert_eq!(config.max_concurrent_downloads, 16);
        assert_eq!(
            config.max_concurrent_maintenance,
            loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE
        );

        for field in [
            "max_download_bytes",
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
max_decoded_input_rows_per_step = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let error = load_server_config(&path).expect_err("zero grep bound must be rejected");
        assert_invalid_field(error, "grep");
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
    fn env_fallbacks_fill_only_unset_secrets() {
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
        assert_eq!(config.content_token_secret(), "dev-content-token-secret");

        // The environment fills fields the file left unset.
        config.auth_token = None;
        config.content_token_secret = loonfs_api::SecretString::default();
        config.apply_env_fallbacks(
            Some("env-auth-token".to_owned()),
            Some("env-content-token-secret".to_owned()),
        );
        assert_eq!(
            config.auth_token.as_ref().map(|token| token.expose()),
            Some("env-auth-token")
        );
        assert_eq!(config.content_token_secret(), "env-content-token-secret");

        // Blank environment values are ignored.
        config.auth_token = None;
        config.content_token_secret = loonfs_api::SecretString::default();
        config.apply_env_fallbacks(Some("   ".to_owned()), Some(String::new()));
        assert!(config.auth_token.is_none());
        assert!(config.content_token_secret().is_empty());
    }

    #[test]
    fn an_ambient_store_table_preserves_its_credential_source() {
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
        let config =
            toml::from_str::<super::ServerConfig>(&fs::read_to_string(&path).expect("read config"))
                .expect("ambient store parses");
        assert_eq!(config.store.credentials_kind(), Some("ambient"));
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
        let runtime_cache_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[runtime_cache]
max_cached_namespacs = 2

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
max_files_per_stepp = 3

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        for (path, typo) in [
            (top_level, "lease_duration"),
            (store_level, "key_prefiks"),
            (runtime_cache_level, "max_cached_namespacs"),
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
        assert!(config.content_token_secret().is_empty());

        config.apply_env_fallbacks(None, Some("env-content-token-secret".to_owned()));
        assert_eq!(config.content_token_secret(), "env-content-token-secret");

        // Without the env fallback the load path reports the missing field.
        if std::env::var("LOONFS_CONTENT_TOKEN_SECRET").is_err() {
            let error = load_server_config(&path).expect_err("missing content token secret");
            assert_missing_field(error, "content_token_secret");
        }
    }

    #[test]
    fn load_uses_default_runtime_cache_when_omitted() {
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
            config.runtime_cache_config(),
            loonfs::RuntimeCacheConfig::default()
        );
    }

    #[test]
    fn load_applies_runtime_cache_overrides() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[runtime_cache]
max_cached_namespaces = 2
max_cached_wal_tail_projection_rows = 10
max_cached_wal_tail_projection_decoded_bytes = 4096

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path)
            .expect("load config")
            .runtime_cache_config();
        assert_eq!(config.max_cached_namespaces, 2);
        assert_eq!(config.max_cached_wal_tail_projection_rows, 10);
        assert_eq!(config.max_cached_wal_tail_projection_decoded_bytes, 4096);
    }

    #[test]
    fn load_accepts_disabled_runtime_cache_overrides() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[runtime_cache]
max_cached_namespaces = 0
max_cached_wal_tail_projection_rows = 0
max_cached_wal_tail_projection_decoded_bytes = 0
metadata_table_cache_max_decoded_bytes = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path)
            .expect("load config")
            .runtime_cache_config();
        assert_eq!(config, loonfs::RuntimeCacheConfig::disabled());
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

    /// A disk tier smaller than one block holds nothing on disk, and foyer
    /// says so in a warning rather than an error. The config check is what
    /// makes it a startup failure with a number in it.
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
        assert!(!config.grep.mode.serves_grep());
        assert!(!config.grep.mode.maintains_index());
    }

    #[test]
    fn a_grep_table_without_a_mode_serves_and_maintains() {
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

        let config = load_server_config(&path).expect("load config");
        assert_eq!(config.grep, super::GrepConfig::default());
        assert_eq!(config.grep.mode, super::GrepMode::ServeAndMaintain);
        assert_eq!(config.grep.worker, loonfs_grep::GrepWorkerConfig::default());
        assert_eq!(
            config
                .grep
                .worker_config()
                .build_policy()
                .expect("valid default grep policy"),
            loonfs_grep::GramIndexBuildPolicy::default(),
        );
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
    fn the_retired_step_concurrency_key_is_no_longer_a_key() {
        // One permit pool bounds every maintenance family now, and it is
        // configured by `max_concurrent_maintenance`.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
max_concurrent_steps = 7

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("retired key must not load");
        assert!(
            error.to_string().contains("max_concurrent_steps"),
            "{error}"
        );
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
max_rows_per_segment = 131072
max_l0_runs = 4
max_mid_runs = 6
max_decoded_input_rows_per_step = 262144

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
        assert_eq!(policy.max_rows_per_segment.get(), 131_072);
        assert_eq!(policy.max_l0_runs.get(), 4);
        assert_eq!(policy.max_mid_runs.get(), 6);
        assert_eq!(policy.max_decoded_input_rows_per_step.get(), 262_144);
    }

    #[test]
    fn grep_policy_overrides_apply_verbatim() {
        // The policy handed to the worker is exactly the configured one:
        // zero budgets are rejected by validation, never rewritten.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[grep]
max_files_per_step = 1024
max_l0_runs = 3

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
        assert_eq!(policy.max_files_per_step.get(), 1024);
        assert_eq!(policy.max_l0_runs.get(), 3);
        assert_eq!(
            policy.max_mid_runs,
            loonfs_grep::GramIndexBuildPolicy::default().max_mid_runs,
            "untouched budgets keep their defaults"
        );
    }

    #[test]
    fn unknown_config_tables_fail_decode() {
        // Pre-release rule: no compatibility courtesies. An unrecognized
        // table — including any removed one — fails through the config's
        // own strict parsing, with no special-cased guidance.
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
    fn load_rejects_negative_runtime_cache_limits_as_decode_error() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"

[runtime_cache]
max_cached_wal_tail_projection_rows = -1

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("negative row limit");
        match error {
            ServerConfigError::Decode(_) => {}
            other => panic!("expected decode error, got {other:?}"),
        }
    }

    /// Every server example config must keep parsing into [`ServerConfig`]
    /// (including under `deny_unknown_fields`) and passing field validation.
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
