use http::Uri;
use loonfs::RuntimeCacheConfig;
use loonfs_objectstore::abs::AzureAbsStoreConfig;
use loonfs_objectstore::gcs::GcpGcsStoreConfig;
use loonfs_objectstore::r2::CloudflareR2StoreConfig;
use loonfs_objectstore::s3::AwsS3StoreConfig;
use loonfs_objectstore::ConfiguredObjectStore;
use serde::Deserialize;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use thiserror::Error;

#[derive(Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub auth_token: Option<String>,
    pub content_token_secret: String,
    pub writer_id: String,
    pub writer_version: String,
    #[serde(default)]
    pub runtime_cache: RuntimeCacheConfigOverrides,
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RuntimeCacheConfigOverrides {
    pub wal_tail_projection_cache_enabled: Option<bool>,
    pub control_cache_enabled: Option<bool>,
    pub max_cached_namespaces: Option<usize>,
    pub max_cached_wal_tail_projection_rows: Option<usize>,
    pub max_cached_wal_tail_projection_decoded_bytes: Option<usize>,
    pub metadata_table_cache_enabled: Option<bool>,
    pub metadata_table_cache_max_blocks: Option<usize>,
    pub metadata_table_cache_max_decoded_bytes: Option<usize>,
}

#[derive(Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum StoreConfig {
    LocalFs {
        root: String,
        key_prefix: Option<String>,
    },
    AwsS3 {
        bucket: String,
        region: String,
        endpoint_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        key_prefix: Option<String>,
        force_path_style: Option<bool>,
    },
    CloudflareR2 {
        bucket: String,
        account_id: String,
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
        key_prefix: Option<String>,
    },
    GcpGcs {
        bucket: String,
        service_account_key_path: String,
        key_prefix: Option<String>,
    },
    AzureAbs {
        account_name: String,
        container_name: String,
        access_key: String,
        endpoint_url: Option<String>,
        key_prefix: Option<String>,
    },
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConfig")
            .field("bind", &self.bind)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field("content_token_secret", &"<redacted>")
            .field("writer_id", &self.writer_id)
            .field("writer_version", &self.writer_version)
            .field("runtime_cache", &self.runtime_cache)
            .field("store", &self.store)
            .finish()
    }
}

impl fmt::Debug for StoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreConfig::LocalFs { root, key_prefix } => f
                .debug_struct("LocalFs")
                .field("root", root)
                .field("key_prefix", key_prefix)
                .finish(),
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id: _,
                secret_access_key: _,
                session_token,
                key_prefix,
                force_path_style,
            } => f
                .debug_struct("AwsS3")
                .field("bucket", bucket)
                .field("region", region)
                .field("endpoint_url", endpoint_url)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field(
                    "session_token",
                    &session_token.as_ref().map(|_| "<redacted>"),
                )
                .field("key_prefix", key_prefix)
                .field("force_path_style", force_path_style)
                .finish(),
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id: _,
                secret_access_key: _,
                key_prefix,
            } => f
                .debug_struct("CloudflareR2")
                .field("bucket", bucket)
                .field("account_id", account_id)
                .field("endpoint_url", endpoint_url)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field("key_prefix", key_prefix)
                .finish(),
            StoreConfig::GcpGcs {
                bucket,
                service_account_key_path,
                key_prefix,
            } => f
                .debug_struct("GcpGcs")
                .field("bucket", bucket)
                .field("service_account_key_path", service_account_key_path)
                .field("key_prefix", key_prefix)
                .finish(),
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                access_key: _,
                endpoint_url,
                key_prefix,
            } => f
                .debug_struct("AzureAbs")
                .field("account_name", account_name)
                .field("container_name", container_name)
                .field("access_key", &"<redacted>")
                .field("endpoint_url", endpoint_url)
                .field("key_prefix", key_prefix)
                .finish(),
        }
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
    pub(crate) fn content_token_secret(&self) -> &str {
        &self.content_token_secret
    }

    pub fn runtime_cache_config(&self) -> RuntimeCacheConfig {
        let mut config = RuntimeCacheConfig::default();
        if let Some(value) = self.runtime_cache.wal_tail_projection_cache_enabled {
            config.wal_tail_projection_cache_enabled = value;
        }
        if let Some(value) = self.runtime_cache.control_cache_enabled {
            config.control_cache_enabled = value;
        }
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
            config.max_cached_wal_tail_projection_decoded_bytes = Some(value);
        }
        if let Some(value) = self.runtime_cache.metadata_table_cache_enabled {
            config.metadata_table_cache.enabled = value;
        }
        if let Some(value) = self.runtime_cache.metadata_table_cache_max_blocks {
            config.metadata_table_cache.max_blocks = value;
        }
        if let Some(value) = self.runtime_cache.metadata_table_cache_max_decoded_bytes {
            config.metadata_table_cache.max_decoded_bytes = Some(value);
        }
        config
    }

    pub fn object_store(&self) -> Result<ConfiguredObjectStore, ServerConfigError> {
        match &self.store {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref()).map_err(|err| {
                    ServerConfigError::InvalidField {
                        field: "store.key_prefix",
                        reason: err.to_string(),
                    }
                })
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
                session_token,
                key_prefix,
                force_path_style,
            } => ConfiguredObjectStore::aws_s3(AwsS3StoreConfig {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: session_token.clone(),
                key_prefix: key_prefix.clone(),
                force_path_style: force_path_style.unwrap_or(false),
            })
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            }),
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix,
            } => ConfiguredObjectStore::cloudflare_r2(CloudflareR2StoreConfig {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            }),
            StoreConfig::GcpGcs {
                bucket,
                service_account_key_path,
                key_prefix,
            } => ConfiguredObjectStore::gcp_gcs(GcpGcsStoreConfig {
                bucket: bucket.clone(),
                service_account_key_path: service_account_key_path.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            }),
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                access_key,
                endpoint_url,
                key_prefix,
            } => ConfiguredObjectStore::azure_abs(AzureAbsStoreConfig {
                account_name: account_name.clone(),
                container_name: container_name.clone(),
                access_key: access_key.clone(),
                endpoint_url: endpoint_url.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            }),
        }
    }

    fn validate(&self) -> Result<(), ServerConfigError> {
        validate_socket_addr("bind", &self.bind)?;
        require_non_empty("writer_id", &self.writer_id)?;
        require_non_empty("writer_version", &self.writer_version)?;

        if let Some(token) = &self.auth_token {
            if token.trim().is_empty() {
                return Err(ServerConfigError::InvalidField {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
        }
        require_non_empty("content_token_secret", &self.content_token_secret)?;
        match &self.store {
            StoreConfig::LocalFs { root, .. } => {
                require_non_empty("store.root", root)?;
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.region", region)?;
                require_non_empty("store.access_key_id", access_key_id)?;
                require_non_empty("store.secret_access_key", secret_access_key)?;
                if let Some(url) = endpoint_url {
                    validate_optional_absolute_http_url("store.endpoint_url", url)?;
                }
            }
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.account_id", account_id)?;
                require_non_empty("store.access_key_id", access_key_id)?;
                require_non_empty("store.secret_access_key", secret_access_key)?;
                validate_absolute_http_url("store.endpoint_url", endpoint_url)?;
            }
            StoreConfig::GcpGcs {
                bucket,
                service_account_key_path,
                ..
            } => {
                require_non_empty("store.bucket", bucket)?;
                require_non_empty("store.service_account_key_path", service_account_key_path)?;
            }
            StoreConfig::AzureAbs {
                account_name,
                container_name,
                access_key,
                endpoint_url,
                ..
            } => {
                require_non_empty("store.account_name", account_name)?;
                require_non_empty("store.container_name", container_name)?;
                require_non_empty("store.access_key", access_key)?;
                if let Some(url) = endpoint_url {
                    validate_optional_absolute_http_url("store.endpoint_url", url)?;
                }
            }
        }

        Ok(())
    }
}

pub fn load_server_config(path: impl AsRef<Path>) -> Result<ServerConfig, ServerConfigError> {
    let bytes = fs::read(path.as_ref()).map_err(|err| ServerConfigError::Io(err.to_string()))?;
    let config: ServerConfig = toml::from_str(
        std::str::from_utf8(&bytes).map_err(|err| ServerConfigError::Decode(err.to_string()))?,
    )
    .map_err(|err| ServerConfigError::Decode(err.to_string()))?;
    config.validate()?;
    config.object_store()?;
    Ok(config)
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), ServerConfigError> {
    if value.trim().is_empty() {
        Err(ServerConfigError::MissingField { field })
    } else {
        Ok(())
    }
}

fn validate_socket_addr(field: &'static str, value: &str) -> Result<(), ServerConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerConfigError::MissingField { field });
    }
    trimmed
        .parse::<SocketAddr>()
        .map(|_| ())
        .map_err(|err| ServerConfigError::InvalidField {
            field,
            reason: err.to_string(),
        })
}

fn validate_optional_absolute_http_url(
    field: &'static str,
    value: &str,
) -> Result<(), ServerConfigError> {
    if value.trim().is_empty() {
        return Err(ServerConfigError::MissingField { field });
    }
    validate_absolute_http_url(field, value)
}

fn validate_absolute_http_url(field: &'static str, value: &str) -> Result<(), ServerConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ServerConfigError::MissingField { field });
    }

    let uri: Uri =
        trimmed.parse().map_err(
            |err: http::uri::InvalidUri| ServerConfigError::InvalidField {
                field,
                reason: err.to_string(),
            },
        )?;

    match uri.scheme_str() {
        Some("http" | "https") => {}
        Some(other) => {
            return Err(ServerConfigError::InvalidField {
                field,
                reason: format!("scheme must be http or https, got `{other}`"),
            });
        }
        None => {
            return Err(ServerConfigError::InvalidField {
                field,
                reason: "must be an absolute http or https URL".to_owned(),
            });
        }
    }

    if uri.authority().is_none() {
        return Err(ServerConfigError::InvalidField {
            field,
            reason: "must be an absolute http or https URL".to_owned(),
        });
    }

    Ok(())
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
wal_tail_projection_cache_enabled = true
control_cache_enabled = false
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
        assert!(config.wal_tail_projection_cache_enabled);
        assert!(!config.control_cache_enabled);
        assert_eq!(config.max_cached_namespaces, 2);
        assert_eq!(config.max_cached_wal_tail_projection_rows, 10);
        assert_eq!(
            config.max_cached_wal_tail_projection_decoded_bytes,
            Some(4096)
        );
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
wal_tail_projection_cache_enabled = false
control_cache_enabled = false
max_cached_namespaces = 0
max_cached_wal_tail_projection_rows = 0
max_cached_wal_tail_projection_decoded_bytes = 0
metadata_table_cache_enabled = false
metadata_table_cache_max_blocks = 0
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

    fn write_config(contents: &str) -> std::path::PathBuf {
        let temp_dir = tempdir().expect("tempdir");
        let path = temp_dir.path().join("server.toml");
        let contents = if contents.contains("content_token_secret") {
            contents.to_owned()
        } else {
            contents.replacen(
                "writer_id",
                "content_token_secret = \"dev-content-token-secret\"\nwriter_id",
                1,
            )
        };
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
