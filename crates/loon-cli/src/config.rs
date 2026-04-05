use crate::error::CliError;
use http::Uri;
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
    pub active_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, ProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileConfig {
    pub mode: ProfileMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalProfileConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteProfileConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileMode {
    Local,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalProfileConfig {
    pub server_config_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteProfileConfig {
    pub server_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactedCliConfig {
    pub config_version: u32,
    pub active_profile: Option<String>,
    pub profiles: BTreeMap<String, RedactedProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactedProfileConfig {
    pub mode: ProfileMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalProfileConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RedactedRemoteProfileConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedactedRemoteProfileConfig {
    pub server_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

impl CliConfig {
    pub fn new() -> Self {
        Self {
            config_version: CONFIG_VERSION,
            active_profile: None,
            profiles: BTreeMap::new(),
        }
    }

    pub fn redacted(&self) -> RedactedCliConfig {
        RedactedCliConfig {
            config_version: self.config_version,
            active_profile: self.active_profile.clone(),
            profiles: self
                .profiles
                .iter()
                .map(|(name, profile)| (name.clone(), profile.redacted()))
                .collect(),
        }
    }

    pub fn validate(&self) -> Result<(), CliError> {
        if self.config_version != CONFIG_VERSION {
            return Err(CliError::invalid_config(format!(
                "unsupported `config_version`: expected `{CONFIG_VERSION}`, got `{}`",
                self.config_version
            )));
        }
        if let Some(active_profile) = &self.active_profile {
            require_non_empty("active_profile", active_profile)?;
            if !self.profiles.contains_key(active_profile) {
                return Err(CliError::invalid_config(format!(
                    "`active_profile` points to missing profile `{active_profile}`"
                )));
            }
        }
        for (name, profile) in &self.profiles {
            require_non_empty("profiles.<name>", name)?;
            profile.validate(name)?;
        }
        Ok(())
    }
}

impl Default for CliConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ProfileConfig {
    pub fn local(local: LocalProfileConfig) -> Self {
        Self {
            mode: ProfileMode::Local,
            local: Some(local),
            remote: None,
        }
    }

    pub fn remote(remote: RemoteProfileConfig) -> Self {
        Self {
            mode: ProfileMode::Remote,
            local: None,
            remote: Some(remote),
        }
    }

    pub(crate) fn validate(&self, name: &str) -> Result<(), CliError> {
        match self.mode {
            ProfileMode::Local => {
                let local = self.local.as_ref().ok_or_else(|| {
                    CliError::invalid_config(format!(
                        "profile `{name}` is missing `[profiles.{name}.local]`"
                    ))
                })?;
                if self.remote.is_some() {
                    return Err(CliError::invalid_config(format!(
                        "profile `{name}` has unexpected `[profiles.{name}.remote]` section"
                    )));
                }
                local.validate(name)
            }
            ProfileMode::Remote => {
                let remote = self.remote.as_ref().ok_or_else(|| {
                    CliError::invalid_config(format!(
                        "profile `{name}` is missing `[profiles.{name}.remote]`"
                    ))
                })?;
                if self.local.is_some() {
                    return Err(CliError::invalid_config(format!(
                        "profile `{name}` has unexpected `[profiles.{name}.local]` section"
                    )));
                }
                remote.validate(name)
            }
        }
    }

    pub fn redacted(&self) -> RedactedProfileConfig {
        RedactedProfileConfig {
            mode: self.mode,
            local: self.local.clone(),
            remote: self.remote.as_ref().map(RemoteProfileConfig::redacted),
        }
    }
}

impl LocalProfileConfig {
    pub(crate) fn validate(&self, name: &str) -> Result<(), CliError> {
        require_non_empty(
            &format!("profiles.{name}.local.server_config_path"),
            &self.server_config_path,
        )
    }
}

impl RemoteProfileConfig {
    pub(crate) fn validate(&self, name: &str) -> Result<(), CliError> {
        validate_http_url(
            &format!("profiles.{name}.remote.server_url"),
            &self.server_url,
        )?;
        if let Some(token) = &self.auth_token {
            require_non_empty(&format!("profiles.{name}.remote.auth_token"), token)?;
        }
        Ok(())
    }

    fn redacted(&self) -> RedactedRemoteProfileConfig {
        RedactedRemoteProfileConfig {
            server_url: self.server_url.clone(),
            auth_token: self.auth_token.as_ref().map(|_| "REDACTED".to_owned()),
        }
    }
}

pub fn default_config_path() -> Result<PathBuf, CliError> {
    #[cfg(target_os = "macos")]
    let path = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| CliError::invalid_config("unable to determine the home directory"))
        .map(|home| macos_default_config_path(&home))?;
    #[cfg(all(unix, not(target_os = "macos")))]
    let path = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| CliError::invalid_config("unable to determine the home directory"))?
        .join(".config");
    #[cfg(target_os = "windows")]
    let path = std::env::var_os("APPDATA")
        .map(PathBuf::from)
        .ok_or_else(|| CliError::invalid_config("unable to determine the config directory"))?;

    #[cfg(not(target_os = "macos"))]
    let path = path.join("loon").join("config.toml");

    Ok(path)
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

pub fn resolve_profile_path(config_path: &Path, input: &Path) -> PathBuf {
    if input.is_absolute() {
        return input.to_path_buf();
    }
    config_path
        .parent()
        .map(|parent| parent.join(input))
        .unwrap_or_else(|| input.to_path_buf())
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

#[cfg(target_os = "macos")]
fn macos_default_config_path(home: &Path) -> PathBuf {
    home.join(".config")
        .join("loonfs")
        .join("loon")
        .join("config.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_default_config_path_uses_loonfs_config_root() {
        let path = macos_default_config_path(Path::new("/Users/tester"));
        assert_eq!(
            path,
            PathBuf::from("/Users/tester/.config/loonfs/loon/config.toml")
        );
    }
}
