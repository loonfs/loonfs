use crate::error::CliError;
use http::Uri;
use loon_api::NamespaceId;
use loon_objectstore::r2::R2StoreConfig;
use loon_objectstore::s3::AwsS3StoreConfig;
use loon_objectstore::ConfiguredObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliConfig {
    pub config_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_profile: Option<String>,
    #[serde(flatten)]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum ProfileConfig {
    Embedded {
        store: StoreConfig,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        writer_version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_duration_ms: Option<u64>,
    },
    Remote {
        server_url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default_namespace: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_token: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum StoreConfig {
    LocalFs {
        root: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
    AwsS3 {
        bucket: String,
        region: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_token: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        force_path_style: Option<bool>,
    },
    CloudflareR2 {
        bucket: String,
        account_id: String,
        endpoint_url: String,
        access_key_id: String,
        secret_access_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
}

impl CliConfig {
    pub fn new() -> Self {
        Self {
            config_version: CONFIG_VERSION,
            default_profile: None,
            profiles: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), CliError> {
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

    pub fn redacted(&self) -> Self {
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
    pub fn mode_str(&self) -> &'static str {
        match self {
            ProfileConfig::Embedded { .. } => "embedded",
            ProfileConfig::Remote { .. } => "remote",
        }
    }

    pub fn store_kind_str(&self) -> Option<&'static str> {
        match self {
            ProfileConfig::Embedded { store, .. } => Some(store.kind_str()),
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
                store.validate(name)
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
                    require_non_empty(&profile_field(name, "auth_token"), token)?;
                }
                Ok(())
            }
        }
    }

    pub fn redacted(&self) -> Self {
        match self {
            ProfileConfig::Embedded {
                store,
                default_namespace,
                writer_id,
                writer_version,
                lease_duration_ms,
            } => ProfileConfig::Embedded {
                store: store.redacted(),
                default_namespace: default_namespace.clone(),
                writer_id: writer_id.clone(),
                writer_version: writer_version.clone(),
                lease_duration_ms: *lease_duration_ms,
            },
            ProfileConfig::Remote {
                server_url,
                default_namespace,
                auth_token,
            } => ProfileConfig::Remote {
                server_url: server_url.clone(),
                default_namespace: default_namespace.clone(),
                auth_token: auth_token.as_ref().map(|_| "REDACTED".to_owned()),
            },
        }
    }
}

impl StoreConfig {
    pub fn kind_str(&self) -> &'static str {
        match self {
            StoreConfig::LocalFs { .. } => "local-fs",
            StoreConfig::AwsS3 { .. } => "aws-s3",
            StoreConfig::CloudflareR2 { .. } => "cloudflare-r2",
        }
    }

    pub fn object_store(&self) -> Result<ConfiguredObjectStore, CliError> {
        match self {
            StoreConfig::LocalFs { root, key_prefix } => {
                ConfiguredObjectStore::local_fs(root, key_prefix.as_deref())
                    .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}")))
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
            .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}"))),
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                key_prefix,
            } => ConfiguredObjectStore::cloudflare_r2(R2StoreConfig {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                key_prefix: key_prefix.clone(),
            })
            .map_err(|err| CliError::invalid_config(format!("invalid store config: {err}"))),
        }
    }

    fn validate(&self, profile_name: &str) -> Result<(), CliError> {
        match self {
            StoreConfig::LocalFs { root, .. } => {
                require_non_empty(&store_field(profile_name, "root"), root)
            }
            StoreConfig::AwsS3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty(&store_field(profile_name, "bucket"), bucket)?;
                require_non_empty(&store_field(profile_name, "region"), region)?;
                require_non_empty(&store_field(profile_name, "access_key_id"), access_key_id)?;
                require_non_empty(
                    &store_field(profile_name, "secret_access_key"),
                    secret_access_key,
                )
            }
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                access_key_id,
                secret_access_key,
                ..
            } => {
                require_non_empty(&store_field(profile_name, "bucket"), bucket)?;
                require_non_empty(&store_field(profile_name, "account_id"), account_id)?;
                require_non_empty(&store_field(profile_name, "endpoint_url"), endpoint_url)?;
                require_non_empty(&store_field(profile_name, "access_key_id"), access_key_id)?;
                require_non_empty(
                    &store_field(profile_name, "secret_access_key"),
                    secret_access_key,
                )
            }
        }
    }

    fn redacted(&self) -> Self {
        match self {
            StoreConfig::LocalFs { .. } => self.clone(),
            StoreConfig::AwsS3 {
                bucket,
                region,
                endpoint_url,
                key_prefix,
                force_path_style,
                ..
            } => StoreConfig::AwsS3 {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: "REDACTED".to_owned(),
                secret_access_key: "REDACTED".to_owned(),
                session_token: None,
                key_prefix: key_prefix.clone(),
                force_path_style: *force_path_style,
            },
            StoreConfig::CloudflareR2 {
                bucket,
                account_id,
                endpoint_url,
                key_prefix,
                ..
            } => StoreConfig::CloudflareR2 {
                bucket: bucket.clone(),
                account_id: account_id.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: "REDACTED".to_owned(),
                secret_access_key: "REDACTED".to_owned(),
                key_prefix: key_prefix.clone(),
            },
        }
    }
}

pub fn default_config_path() -> Result<PathBuf, CliError> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| CliError::invalid_config("unable to determine the home directory"))?;
    Ok(home.join(".loonfs").join("config.toml"))
}

pub(crate) fn validate_profile_name(name: &str) -> Result<(), CliError> {
    require_non_empty("profile name", name)?;
    if is_reserved_profile_name(name) {
        return Err(CliError::invalid_input(format!(
            "profile name `{name}` is reserved"
        )));
    }
    Ok(())
}

fn is_reserved_profile_name(name: &str) -> bool {
    matches!(name, "config_version" | "default_profile")
}

fn profile_field(name: &str, field: &str) -> String {
    format!("{name}.{field}")
}

fn store_field(profile_name: &str, field: &str) -> String {
    format!("{profile_name}.store.{field}")
}

pub fn load_config(path: &Path) -> Result<CliConfig, CliError> {
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

pub fn load_config_if_exists(path: &Path) -> Result<Option<CliConfig>, CliError> {
    if !path.exists() {
        return Ok(None);
    }
    load_config(path).map(Some)
}

pub fn load_or_default_config(path: &Path) -> Result<CliConfig, CliError> {
    Ok(load_config_if_exists(path)?.unwrap_or_default())
}

pub fn save_config(path: &Path, config: &CliConfig) -> Result<(), CliError> {
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
