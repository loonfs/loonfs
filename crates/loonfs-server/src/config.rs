use loonfs::RuntimeCacheConfig;
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
/// File-provided secrets win over environment variables; blank environment
/// values are ignored.
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
    pub store: StoreConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
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
        self.store
            .configured_object_store()
            .map_err(|err| ServerConfigError::InvalidField {
                field: "store.key_prefix",
                reason: err.to_string(),
            })
    }

    fn validate(&self) -> Result<(), ServerConfigError> {
        validate_socket_addr("bind", &self.bind)?;
        require_non_empty("writer_id", &self.writer_id)?;
        require_non_empty("writer_version", &self.writer_version)?;

        if let Some(token) = &self.auth_token {
            if token.expose().trim().is_empty() {
                return Err(ServerConfigError::InvalidField {
                    field: "auth_token",
                    reason: "must not be empty".to_owned(),
                });
            }
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
control_cache_enable = true

[store]
kind = "local-fs"
root = "/tmp/loonfs-server"
"#,
        );

        for (path, typo) in [
            (top_level, "lease_duration"),
            (store_level, "key_prefiks"),
            (runtime_cache_level, "control_cache_enable"),
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
