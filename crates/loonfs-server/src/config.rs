//! Server configuration: strict TOML decoding of the listen address,
//! store, and runtime cache overrides.

use loonfs::{GramIndexBuildPolicy, RuntimeCacheConfig};
use loonfs_objectstore::{ConfiguredObjectStore, SecretString, StoreConfigError};
use serde::Deserialize;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;

pub use loonfs_objectstore::StoreConfig;

/// Environment fallback for [`ServerConfig::auth_token`].
const AUTH_TOKEN_ENV: &str = "LOONFS_AUTH_TOKEN";
/// Environment fallback for [`ServerConfig::content_token_secret`].
const CONTENT_TOKEN_SECRET_ENV: &str = "LOONFS_CONTENT_TOKEN_SECRET";

/// The server config file.
///
/// # Secret precedence
///
/// `auth_token` and `content_token_secret` may be supplied through the
/// `LOONFS_AUTH_TOKEN` and `LOONFS_CONTENT_TOKEN_SECRET` environment
/// variables instead of the file. A non-blank value in the file always takes
/// precedence; the environment variable fills the field only when the file
/// leaves it unset (blank environment values are ignored).
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
    pub writer_version: String,
    #[serde(default)]
    pub runtime_cache: RuntimeCacheConfigOverrides,
    /// Budgets for the gram index build and fold steps run by this
    /// server's maintenance (background catch-up after writes and explicit
    /// ticks). Omitted fields keep the runtime defaults; bulk backfills
    /// typically raise the per-step budgets so each tick indexes more
    /// files per manifest publish. Zero budgets are rejected at startup.
    #[serde(default)]
    pub gram_index_build: GramIndexBuildPolicyOverrides,
    /// Whether the server writer schedules maintenance (checkpoints and
    /// reorganization folds) after writes that cross the WAL-tail
    /// threshold. On by default; set `false` on write-serving nodes when a
    /// dedicated maintenance process owns ticks for these namespaces.
    #[serde(default = "default_background_maintenance")]
    pub background_maintenance: bool,
    /// Minimum interval between publication starts per namespace, in
    /// milliseconds. A cold namespace publishes immediately; the interval
    /// paces follow-up batches so hot namespaces amortize into fewer,
    /// larger WAL segments. The server default favors batch economy over
    /// the embedded default's latency bias.
    #[serde(default = "default_min_publish_interval_ms")]
    pub min_publish_interval_ms: u64,
    /// Largest request body accepted for service-proxied upload content
    /// requests (`PUT .../uploads/{upload_id}/content`). The server buffers
    /// each upload body in memory, so this bounds per-request memory;
    /// larger transfers should use `direct_put` uploads. Advertised to
    /// clients as the `upload.max_content_bytes` capability limit.
    #[serde(default = "default_max_upload_bytes")]
    pub max_upload_bytes: u64,
    /// Largest file content a service-proxied read (`GET .../filesystem/
    /// content` and inode revision content) will buffer and return. Checked
    /// against resolved metadata before any content fetch; over-limit reads
    /// answer `content_too_large`. Advertised to clients as the
    /// `download.max_content_bytes` capability limit.
    #[serde(default = "default_max_download_bytes")]
    pub max_download_bytes: u64,
    /// Largest JSON body the commits endpoint accepts
    /// (`POST .../namespaces/{ns}/commits`). Commit bodies carry metadata
    /// only — file bytes ride uploads — so this bounds per-request parse
    /// memory for bulk commits. Advertised to clients as the
    /// `commit.max_body_bytes` capability limit.
    #[serde(default = "default_max_commit_body_bytes")]
    pub max_commit_body_bytes: u64,
    /// How many proxied upload bodies the server will buffer at once;
    /// requests past the cap answer `server_busy` before any buffering.
    /// Worst-case upload memory is this times `max_upload_bytes`.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// How many proxied content reads the server will materialize at once;
    /// requests past the cap answer `server_busy` before any fetch.
    /// Worst-case download memory is this times `max_download_bytes`.
    #[serde(default = "default_max_concurrent_downloads")]
    pub max_concurrent_downloads: usize,
    /// How many writer-scheduled maintenance ticks may run at once across
    /// all namespaces. Each namespace runs at most one tick at a time; this
    /// bounds the fan-out when a write burst crosses thresholds in many
    /// namespaces together. Skipped ticks are rescheduled by the next
    /// over-threshold publish.
    #[serde(default = "default_max_concurrent_maintenance")]
    pub max_concurrent_maintenance: usize,
    /// Allows serving on a non-loopback address with `auth_token` unset.
    /// Off by default: exposing every endpoint unauthenticated is almost
    /// always a misconfiguration, so validation rejects it unless this is
    /// explicitly set.
    #[serde(default)]
    pub allow_unauthenticated_remote: bool,
    pub store: StoreConfig,
}

fn default_background_maintenance() -> bool {
    true
}

fn default_min_publish_interval_ms() -> u64 {
    1_000
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

fn default_max_commit_body_bytes() -> u64 {
    // A 5,000-op commit serializes to roughly 1.5 MB; 8 MiB leaves bulk
    // headroom while bounding what one request can make the parser buffer.
    8 * 1024 * 1024
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

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCacheConfigOverrides {
    pub max_cached_namespaces: Option<usize>,
    pub max_cached_wal_tail_projection_rows: Option<usize>,
    pub max_cached_wal_tail_projection_decoded_bytes: Option<usize>,
    pub metadata_table_cache_max_decoded_bytes: Option<usize>,
}

/// Optional `[gram_index_build]` overrides, field-for-field the budgets of
/// [`GramIndexBuildPolicy`]; omitted fields keep that policy's defaults.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GramIndexBuildPolicyOverrides {
    pub max_files_per_step: Option<usize>,
    pub max_content_bytes_per_step: Option<u64>,
    pub max_rows_per_segment: Option<usize>,
    pub max_l0_runs: Option<usize>,
    pub max_mid_runs: Option<usize>,
    pub max_fold_rows_per_step: Option<usize>,
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
    pub(crate) fn content_token_secret(&self) -> &str {
        self.content_token_secret.expose()
    }

    /// Fills `auth_token` and `content_token_secret` from the environment
    /// when the file left them unset. Non-blank file values win; blank
    /// environment values are ignored.
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

    pub fn gram_index_build_policy(&self) -> GramIndexBuildPolicy {
        let mut policy = GramIndexBuildPolicy::default();
        if let Some(value) = self.gram_index_build.max_files_per_step {
            policy.max_files_per_step = value;
        }
        if let Some(value) = self.gram_index_build.max_content_bytes_per_step {
            policy.max_content_bytes_per_step = value;
        }
        if let Some(value) = self.gram_index_build.max_rows_per_segment {
            policy.max_rows_per_segment = value;
        }
        if let Some(value) = self.gram_index_build.max_l0_runs {
            policy.max_l0_runs = value;
        }
        if let Some(value) = self.gram_index_build.max_mid_runs {
            policy.max_mid_runs = value;
        }
        if let Some(value) = self.gram_index_build.max_fold_rows_per_step {
            policy.max_fold_rows_per_step = value;
        }
        policy
    }

    pub fn object_store(&self) -> Result<ConfiguredObjectStore, ServerConfigError> {
        self.store
            .configured_object_store()
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            })
    }

    /// Parses the bind address; the one authority for that conversion, used
    /// by validation and by serving.
    pub(crate) fn bind_addr(&self) -> Result<SocketAddr, ServerConfigError> {
        validate_socket_addr("bind", &self.bind)
    }

    pub(crate) fn validate(&self) -> Result<(), ServerConfigError> {
        let bind = self.bind_addr()?;
        require_non_empty("writer_id", &self.writer_id)?;
        require_non_empty("writer_version", &self.writer_version)?;

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
        if self.max_upload_bytes == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_upload_bytes",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_download_bytes == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_download_bytes",
                reason: "must be greater than zero".to_owned(),
            });
        }
        if self.max_commit_body_bytes == 0 {
            return Err(ServerConfigError::InvalidField {
                field: "max_commit_body_bytes",
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
                         set `background_maintenance = false` to disable scheduling"
                    .to_owned(),
            });
        }
        if let Some(budget) = self.gram_index_build_policy().zero_budget_field() {
            return Err(ServerConfigError::InvalidField {
                field: "gram_index_build",
                reason: format!("`{budget}` must be greater than zero"),
            });
        }
        require_non_empty("content_token_secret", self.content_token_secret.expose())?;
        self.store.validate().map_err(ServerConfigError::from)?;

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
        }
    }
}

pub fn load_server_config(path: impl AsRef<Path>) -> Result<ServerConfig, ServerConfigError> {
    let bytes = fs::read(path.as_ref()).map_err(|err| ServerConfigError::Io(err.to_string()))?;
    let mut config: ServerConfig = toml::from_str(
        std::str::from_utf8(&bytes).map_err(|err| ServerConfigError::Decode(err.to_string()))?,
    )
    .map_err(|err| ServerConfigError::Decode(err.to_string()))?;
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

    use super::{load_server_config, ServerConfigError};
    use std::fs;
    use tempfile::tempdir;

    const AZURITE_ACCOUNT_KEY: &str =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    #[test]
    fn background_maintenance_defaults_on_and_accepts_false() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert!(
            config.background_maintenance,
            "background maintenance defaults on"
        );

        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"
background_maintenance = false

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert!(
            !config.background_maintenance,
            "write-serving nodes can hand maintenance to a dedicated process"
        );
    }

    #[test]
    fn load_rejects_invalid_bind() {
        let path = write_config(
            r#"
bind = "bad-bind"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("blank writer fields");

        assert_missing_field(error, "writer_id");
    }

    #[test]
    fn load_rejects_blank_writer_version() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "   "

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let error = load_server_config(&path).expect_err("blank writer version");

        assert_missing_field(error, "writer_version");
    }

    #[test]
    fn load_rejects_blank_provider_required_fields() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "cloudflare-r2"
bucket = " "
account_id = "account"
endpoint_url = "https://example.com"
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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
endpoint_url = "ftp://example.com"
access_key_id = "access"
secret_access_key = "secret"
key_prefix = "demo"
force_path_style = false
"#,
        );
        let r2_path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "not a url"
access_key_id = "access"
secret_access_key = "secret"
key_prefix = "demo"
"#,
        );
        let azure_path = write_config(&format!(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "azure-abs"
account_name = "devstoreaccount1"
container_name = "container"
access_key = "{AZURITE_ACCOUNT_KEY}"
endpoint_url = "not a url"
key_prefix = "demo"
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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "gcp-gcs"
bucket = " "
service_account_key_path = "/tmp/service-account.json"
key_prefix = "demo"
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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "azure-abs"
account_name = "devstoreaccount1"
container_name = "container"
access_key = "{AZURITE_ACCOUNT_KEY}"
endpoint_url = "http://127.0.0.1:10000/devstoreaccount1"
key_prefix = "demo"
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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "azure-abs"
account_name = " "
container_name = "container"
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
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

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
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        load_server_config(&path).expect("loopback-only config loads");
    }

    #[test]
    fn max_upload_bytes_defaults_and_rejects_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"
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
    fn transfer_bounds_default_and_reject_zero() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let config = load_server_config(&path).expect("valid config");
        assert_eq!(config.max_download_bytes, 256 * 1024 * 1024);
        assert_eq!(config.max_commit_body_bytes, 8 * 1024 * 1024);
        assert_eq!(config.max_concurrent_uploads, 8);
        assert_eq!(config.max_concurrent_downloads, 16);
        assert_eq!(
            config.max_concurrent_maintenance,
            loonfs::DEFAULT_MAX_CONCURRENT_MAINTENANCE
        );

        for field in [
            "max_download_bytes",
            "max_commit_body_bytes",
            "max_concurrent_uploads",
            "max_concurrent_downloads",
            "max_concurrent_maintenance",
        ] {
            let path = write_config(&format!(
                r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"
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
writer_version = "loonfs-server/0.1.0"

[gram_index_build]
max_fold_rows_per_step = 0

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let error = load_server_config(&path).expect_err("zero gram budget must be rejected");
        assert_invalid_field(error, "gram_index_build");
    }

    #[test]
    fn server_config_debug_redacts_secrets() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "debug-auth-token"
content_token_secret = "debug-content-token-secret"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"
key_prefix = "demo"
force_path_style = false
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
writer_version = "loonfs-server/0.1.0"

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
        config.content_token_secret = loonfs_objectstore::SecretString::default();
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
        config.content_token_secret = loonfs_objectstore::SecretString::default();
        config.apply_env_fallbacks(Some("   ".to_owned()), Some(String::new()));
        assert!(config.auth_token.is_none());
        assert!(config.content_token_secret().is_empty());
    }

    #[test]
    fn load_rejects_unknown_keys_at_every_level() {
        let top_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"
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
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

[runtime_cache]
max_cached_namespacs = 2

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );
        let gram_index_build_level = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[gram_index_build]
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
            (gram_index_build_level, "max_files_per_stepp"),
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
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

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
writer_version = "loonfs-server/0.1.0"

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
    fn load_uses_default_gram_index_build_policy_when_omitted() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let config = load_server_config(&path).expect("load config");
        assert_eq!(
            config.gram_index_build_policy(),
            loonfs::GramIndexBuildPolicy::default()
        );
    }

    #[test]
    fn load_applies_gram_index_build_overrides() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[gram_index_build]
max_files_per_step = 4096
max_content_bytes_per_step = 536870912
max_rows_per_segment = 131072
max_l0_runs = 4
max_mid_runs = 6
max_fold_rows_per_step = 262144

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let policy = load_server_config(&path)
            .expect("load config")
            .gram_index_build_policy();
        assert_eq!(policy.max_files_per_step, 4096);
        assert_eq!(policy.max_content_bytes_per_step, 536_870_912);
        assert_eq!(policy.max_rows_per_segment, 131_072);
        assert_eq!(policy.max_l0_runs, 4);
        assert_eq!(policy.max_mid_runs, 6);
        assert_eq!(policy.max_fold_rows_per_step, 262_144);
    }

    #[test]
    fn gram_index_build_overrides_apply_verbatim() {
        // The policy handed to the runtime is exactly the configured one:
        // zero budgets are rejected by validation, never rewritten.
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

[gram_index_build]
max_files_per_step = 1024
max_l0_runs = 3

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        let policy = load_server_config(&path)
            .expect("load config")
            .gram_index_build_policy();
        assert_eq!(policy.max_files_per_step, 1024);
        assert_eq!(policy.max_l0_runs, 3);
        assert_eq!(
            policy.max_mid_runs,
            loonfs::GramIndexBuildPolicy::default().max_mid_runs,
            "untouched budgets keep their defaults"
        );
    }

    #[test]
    fn load_rejects_negative_runtime_cache_limits_as_decode_error() {
        let path = write_config(
            r#"
bind = "127.0.0.1:9400"
auth_token = "dev-token"
writer_id = "loonfs-server"
writer_version = "loonfs-server/0.1.0"

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
        let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs");
        let mut examples = 0usize;
        for entry in fs::read_dir(configs_dir).expect("read configs directory") {
            let path = entry.expect("read configs entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with("loonfs-server.") || !name.ends_with(".example.toml") {
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
