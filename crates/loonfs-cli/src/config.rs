use crate::error::CliError;
use http::Uri;
use loonfs_api::NamespaceId;
use loonfs_objectstore::{SecretString, StoreConfigError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) use loonfs_objectstore::StoreConfig;

pub(crate) const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CliConfig {
    pub config_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    /// Profiles live in their own `[profiles.<name>]` tables so profile
    /// names can never collide with top-level settings.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub(crate) enum ProfileConfig {
    Embedded {
        store: StoreConfig,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_version: Option<String>,
    },
    Remote {
        server_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<SecretString>,
    },
}

impl CliConfig {
    pub(crate) fn new() -> Self {
        Self {
            config_version: CONFIG_VERSION,
            default_profile: None,
            profiles: BTreeMap::new(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CliError> {
        if self.config_version != CONFIG_VERSION {
            return Err(CliError::invalid_config(format!(
                "unsupported `config_version`: expected `{CONFIG_VERSION}`, got `{}`",
                self.config_version
            )));
        }
        if let Some(default_profile) = &self.default_profile {
            require_non_empty("default_profile", default_profile)?;
            if !self.profiles.contains_key(default_profile) {
                return Err(CliError::invalid_config(format!(
                    "`default_profile` points to missing profile `{default_profile}`"
                )));
            }
        }
        for (name, profile) in &self.profiles {
            validate_profile_name(name).map_err(|error| CliError::invalid_config(error.message))?;
            profile.validate(name)?;
        }
        Ok(())
    }

    pub(crate) fn redacted(&self) -> Self {
        CliConfig {
            config_version: self.config_version,
            default_profile: self.default_profile.clone(),
            profiles: self
                .profiles
                .iter()
                .map(|(name, profile)| (name.clone(), profile.redacted()))
                .collect(),
        }
    }
}

impl Default for CliConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileConfig {
    pub(crate) fn mode_str(&self) -> &'static str {
        match self {
            ProfileConfig::Embedded { .. } => "embedded",
            ProfileConfig::Remote { .. } => "remote",
        }
    }

    pub(crate) fn store_kind_str(&self) -> Option<&'static str> {
        match self {
            ProfileConfig::Embedded { store, .. } => Some(store.kind().as_str()),
            ProfileConfig::Remote { .. } => None,
        }
    }

    pub(crate) fn validate(&self, name: &str) -> Result<(), CliError> {
        match self {
            ProfileConfig::Embedded {
                store,
                default_namespace,
                ..
            } => {
                if let Some(namespace) = default_namespace {
                    validate_default_namespace(
                        &profile_field(name, "default_namespace"),
                        namespace,
                    )?;
                }
                store
                    .validate()
                    .map_err(|error| profile_store_error(name, &error))
            }
            ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
                ..
            } => {
                if let Some(namespace) = default_namespace {
                    validate_default_namespace(
                        &profile_field(name, "default_namespace"),
                        namespace,
                    )?;
                }
                validate_http_url(&profile_field(name, "server_url"), server_url)?;
                if let Some(token) = auth_token {
                    require_non_empty(&profile_field(name, "auth_token"), token.expose())?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn redacted(&self) -> Self {
        match self {
            ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
                writer_version,
            } => ProfileConfig::Embedded {
                store: store.redacted(),
                default_namespace: default_namespace.clone(),
                writer_id: writer_id.clone(),
                writer_version: writer_version.clone(),
            },
            ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
            } => ProfileConfig::Remote {
                server_url: server_url.clone(),
                default_namespace: default_namespace.clone(),
                auth_token: auth_token.as_ref().map(SecretString::masked),
            },
        }
    }
}

/// Prefixes a shared store-validation error (`store.<field>`-rooted) with the
/// profile name, preserving the CLI's `missing `<profile>.store.<field>``
/// message shape.
fn profile_store_error(profile_name: &str, error: &StoreConfigError) -> CliError {
    match error {
        StoreConfigError::MissingField { field } => {
            CliError::invalid_config(format!("missing `{profile_name}.{field}`"))
        }
        StoreConfigError::InvalidField { field, reason } => {
            CliError::invalid_config(format!("invalid `{profile_name}.{field}`: {reason}"))
        }
    }
}

pub(crate) fn default_config_path() -> Result<PathBuf, CliError> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| CliError::invalid_config("unable to determine the home directory"))?;
    Ok(home.join(".loonfs").join("config.toml"))
}

pub(crate) fn validate_profile_name(name: &str) -> Result<(), CliError> {
    require_non_empty("profile name", name)
}

fn profile_field(name: &str, field: &str) -> String {
    format!("{name}.{field}")
}

pub(crate) fn load_config(path: &Path) -> Result<CliConfig, CliError> {
    let bytes = fs::read(path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            CliError::invalid_config(format!("config file does not exist: {}", path.display()))
        } else {
            CliError::invalid_config(format!("failed to read config: {err}"))
        }
    })?;
    let contents = std::str::from_utf8(&bytes)
        .map_err(|err| CliError::invalid_config(format!("failed to decode config: {err}")))?;
    let config: CliConfig = toml::from_str(contents)
        .map_err(|err| CliError::invalid_config(format!("failed to decode config: {err}")))?;
    config.validate()?;
    Ok(config)
}

pub(crate) fn load_config_if_exists(path: &Path) -> Result<Option<CliConfig>, CliError> {
    if !path.exists() {
        return Ok(None);
    }
    load_config(path).map(Some)
}

pub(crate) fn load_or_default_config(path: &Path) -> Result<CliConfig, CliError> {
    Ok(load_config_if_exists(path)?.unwrap_or_default())
}

pub(crate) fn save_config(path: &Path, config: &CliConfig) -> Result<(), CliError> {
    config.validate()?;
    let parent = path.parent().ok_or_else(|| {
        CliError::invalid_config(format!(
            "config path has no parent directory: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|err| {
        CliError::invalid_config(format!("failed to create config directory: {err}"))
    })?;
    let contents = toml::to_string_pretty(config)
        .map_err(|err| CliError::invalid_config(format!("failed to encode config: {err}")))?;
    let tmp_path = path.with_extension("tmp");
    write_owner_only(&tmp_path, contents.as_bytes())?;
    fs::rename(&tmp_path, path).map_err(|err| {
        CliError::invalid_config(format!(
            "failed to persist config {}: {err}",
            path.display()
        ))
    })?;
    Ok(())
}

fn write_owner_only(path: &Path, bytes: &[u8]) -> Result<(), CliError> {
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|err| {
                CliError::invalid_config(format!(
                    "failed to create config file {}: {err}",
                    path.display()
                ))
            })?
    };
    #[cfg(not(unix))]
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|err| {
            CliError::invalid_config(format!(
                "failed to create config file {}: {err}",
                path.display()
            ))
        })?;

    file.write_all(bytes)
        .map_err(|err| CliError::invalid_config(format!("failed to write config: {err}")))?;
    file.sync_all()
        .map_err(|err| CliError::invalid_config(format!("failed to flush config: {err}")))?;
    Ok(())
}

fn require_non_empty(field: &str, value: &str) -> Result<(), CliError> {
    if value.trim().is_empty() {
        return Err(CliError::invalid_config(format!("missing `{field}`")));
    }
    Ok(())
}

fn validate_default_namespace(field: &str, value: &str) -> Result<(), CliError> {
    NamespaceId::parse(value)
        .map(|_| ())
        .map_err(|err| CliError::invalid_config(format!("invalid `{field}`: {err}")))
}

fn validate_http_url(field: &str, value: &str) -> Result<(), CliError> {
    require_non_empty(field, value)?;
    let uri = value
        .trim()
        .parse::<Uri>()
        .map_err(|err| CliError::invalid_config(format!("invalid `{field}`: {err}")))?;
    let scheme = uri
        .scheme_str()
        .ok_or_else(|| CliError::invalid_config(format!("invalid `{field}`: missing scheme")))?;
    if !matches!(scheme, "http" | "https") {
        return Err(CliError::invalid_config(format!(
            "invalid `{field}`: scheme must be http or https, got `{scheme}`"
        )));
    }
    if uri.host().is_none() {
        return Err(CliError::invalid_config(format!(
            "invalid `{field}`: missing host"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]
    // Config tests use panic in unexpected match arms for precise diagnostics.

    use super::{CliConfig, ProfileConfig, StoreConfig};

    fn parse(contents: &str) -> Result<CliConfig, toml::de::Error> {
        toml::from_str(contents)
    }

    #[test]
    fn cli_config_debug_redacts_secrets() {
        let config = parse(
            r#"
config_version = 1
default_profile = "cloud"

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "aws-s3"
bucket = "bucket"
region = "us-east-1"
access_key_id = "debug-access-key-id"
secret_access_key = "debug-secret-access-key"
session_token = "debug-session-token"

[profiles.prod]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "debug-auth-token"
"#,
        )
        .expect("parse config");
        config.validate().expect("valid config");

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("debug-access-key-id"));
        assert!(!rendered.contains("debug-secret-access-key"));
        assert!(!rendered.contains("debug-session-token"));
        assert!(!rendered.contains("debug-auth-token"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn redacted_config_serializes_without_secrets() {
        let config = parse(
            r#"
config_version = 1

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "cloudflare-r2"
bucket = "bucket"
account_id = "account"
endpoint_url = "https://account.r2.cloudflarestorage.com"
access_key_id = "plain-access-key-id"
secret_access_key = "plain-secret-access-key"

[profiles.prod]
mode = "remote"
server_url = "https://loonfs.example.com"
auth_token = "plain-auth-token"
"#,
        )
        .expect("parse config");

        let rendered =
            toml::to_string_pretty(&config.redacted()).expect("serialize redacted config");

        assert!(!rendered.contains("plain-access-key-id"));
        assert!(!rendered.contains("plain-secret-access-key"));
        assert!(!rendered.contains("plain-auth-token"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn unknown_keys_are_rejected_at_every_level() {
        let top_level = parse(
            r#"
config_version = 1
default_profil = "typo"
"#,
        )
        .expect_err("typo'd top-level key");
        assert!(top_level.to_string().contains("default_profil"));

        let profile_level = parse(
            r#"
config_version = 1

[profiles.local]
mode = "embedded"
default_namespac = "typo"

[profiles.local.store]
kind = "local-fs"
root = "/tmp/store"
"#,
        )
        .expect_err("typo'd profile key");
        assert!(profile_level.to_string().contains("default_namespac"));

        let store_level = parse(
            r#"
config_version = 1

[profiles.local]
mode = "embedded"

[profiles.local.store]
kind = "local-fs"
root = "/tmp/store"
key_prefiks = "typo"
"#,
        )
        .expect_err("typo'd store key");
        assert!(store_level.to_string().contains("key_prefiks"));
    }

    #[test]
    fn validation_reports_profile_prefixed_store_fields() {
        let config = parse(
            r#"
config_version = 1

[profiles.cloud]
mode = "embedded"

[profiles.cloud.store]
kind = "aws-s3"
bucket = " "
region = "us-east-1"
access_key_id = "access"
secret_access_key = "secret"
"#,
        )
        .expect("parse config");

        let error = config.validate().expect_err("blank bucket");
        assert_eq!(error.message, "missing `cloud.store.bucket`");
    }

    /// Every CLI example config must keep parsing into [`CliConfig`]
    /// (including under `deny_unknown_fields`) and passing validation.
    #[test]
    fn cli_example_configs_parse_and_validate() {
        let configs_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../configs");
        let mut examples = 0usize;
        for entry in std::fs::read_dir(configs_dir).expect("read configs directory") {
            let path = entry.expect("read configs entry").path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.starts_with("loon.") || !name.ends_with(".example.toml") {
                continue;
            }
            let contents = std::fs::read_to_string(&path).expect("read example config");
            let config: CliConfig =
                toml::from_str(&contents).unwrap_or_else(|err| panic!("{name} must parse: {err}"));
            config
                .validate()
                .unwrap_or_else(|err| panic!("{name} must validate: {}", err.message));
            examples += 1;
        }
        assert!(
            examples >= 2,
            "expected at least 2 CLI example configs, found {examples}"
        );
    }

    #[test]
    fn store_kind_str_matches_config_tags() {
        let profile = ProfileConfig::Embedded {
            store: StoreConfig::LocalFs {
                root: "/tmp/store".to_owned(),
                key_prefix: None,
            },
            default_namespace: None,
            writer_id: None,
            writer_version: None,
        };
        assert_eq!(profile.store_kind_str(), Some("local-fs"));
    }
}
