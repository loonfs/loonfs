use crate::config::{LocalProfileConfig, ProfileConfig, ProfileMode, RemoteProfileConfig};
use crate::error::CliError;
use crate::local_runtime::server_url_from_bind;
use loon_client::{Client, ClientConfig};
use std::path::{Path, PathBuf};

pub enum ResolvedTarget {
    Local(LocalTarget),
    Remote(RemoteTarget),
}

pub struct LocalTarget {
    pub server_config_path: PathBuf,
    pub server_url: String,
    client: Client,
}

pub struct RemoteTarget {
    client: Client,
}

impl ResolvedTarget {
    pub fn resolve(
        profile_name: &str,
        profile: &ProfileConfig,
        config_path: &Path,
    ) -> Result<Self, CliError> {
        match profile.mode {
            ProfileMode::Local => {
                let local = profile.local.as_ref().ok_or_else(|| {
                    CliError::invalid_config(format!(
                        "profile `{profile_name}` is missing local settings"
                    ))
                })?;
                Ok(Self::Local(LocalTarget::new(local, config_path)?))
            }
            ProfileMode::Remote => {
                let remote = profile.remote.as_ref().ok_or_else(|| {
                    CliError::invalid_config(format!(
                        "profile `{profile_name}` is missing remote settings"
                    ))
                })?;
                Ok(Self::Remote(RemoteTarget::new(remote)?))
            }
        }
    }

    pub fn mode(&self) -> ProfileMode {
        match self {
            ResolvedTarget::Local(_) => ProfileMode::Local,
            ResolvedTarget::Remote(_) => ProfileMode::Remote,
        }
    }

    pub fn client(&self) -> &Client {
        match self {
            ResolvedTarget::Local(target) => &target.client,
            ResolvedTarget::Remote(target) => &target.client,
        }
    }

    pub fn as_local(&self) -> Option<&LocalTarget> {
        match self {
            ResolvedTarget::Local(target) => Some(target),
            ResolvedTarget::Remote(_) => None,
        }
    }
}

impl LocalTarget {
    fn new(profile: &LocalProfileConfig, config_path: &Path) -> Result<Self, CliError> {
        let server_config_path = crate::config::resolve_profile_path(
            config_path,
            Path::new(&profile.server_config_path),
        );
        let server_config =
            loon_server::load_server_config(&server_config_path).map_err(|err| {
                CliError::invalid_config(format!(
                    "failed to load local server config `{}`: {err}",
                    server_config_path.display()
                ))
            })?;
        let server_url = server_url_from_bind(&server_config.bind)?;
        let client = Client::new(ClientConfig {
            server_url: server_url.clone(),
            auth_token: server_config.auth_token.clone(),
        });
        Ok(Self {
            server_config_path,
            server_url,
            client,
        })
    }
}

impl RemoteTarget {
    fn new(profile: &RemoteProfileConfig) -> Result<Self, CliError> {
        profile.validate("remote")?;
        let client = Client::new(ClientConfig {
            server_url: profile.server_url.clone(),
            auth_token: profile.auth_token.clone(),
        });
        Ok(Self { client })
    }
}
