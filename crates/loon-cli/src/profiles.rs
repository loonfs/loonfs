use crate::config::{
    CliConfig, LocalProfileConfig, ProfileConfig, ProfileMode, RedactedProfileConfig,
    RedactedRemoteProfileConfig, RemoteProfileConfig,
};
use crate::error::CliError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub name: String,
    pub mode: ProfileMode,
    pub active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileView {
    pub name: String,
    pub mode: ProfileMode,
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<LocalProfileConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RedactedRemoteProfileConfig>,
}

pub fn list_profiles(config: Option<&CliConfig>) -> Vec<ProfileSummary> {
    let Some(config) = config else {
        return Vec::new();
    };
    config
        .profiles
        .iter()
        .map(|(name, profile)| ProfileSummary {
            name: name.clone(),
            mode: profile.mode,
            active: config.active_profile.as_deref() == Some(name.as_str()),
        })
        .collect()
}

pub fn show_profile(
    config: &CliConfig,
    explicit_name: Option<&str>,
) -> Result<ProfileView, CliError> {
    let name = match explicit_name {
        Some(name) => name.to_owned(),
        None => config
            .active_profile
            .clone()
            .ok_or_else(CliError::no_active_profile)?,
    };
    let profile = config
        .profiles
        .get(&name)
        .ok_or_else(|| CliError::profile_not_found(&name))?;
    Ok(profile_view(
        &name,
        profile.redacted(),
        config.active_profile.as_deref() == Some(name.as_str()),
    ))
}

pub fn add_local_profile(
    config: &mut CliConfig,
    name: &str,
    local: LocalProfileConfig,
) -> Result<ProfileView, CliError> {
    add_profile(config, name, ProfileConfig::local(local))
}

pub fn add_remote_profile(
    config: &mut CliConfig,
    name: &str,
    remote: RemoteProfileConfig,
) -> Result<ProfileView, CliError> {
    add_profile(config, name, ProfileConfig::remote(remote))
}

fn add_profile(
    config: &mut CliConfig,
    name: &str,
    profile: ProfileConfig,
) -> Result<ProfileView, CliError> {
    if config.profiles.contains_key(name) {
        return Err(CliError::profile_already_exists(name));
    }
    let should_activate = config.active_profile.is_none();
    config.profiles.insert(name.to_owned(), profile.clone());
    if should_activate {
        config.active_profile = Some(name.to_owned());
    }
    Ok(profile_view(name, profile.redacted(), should_activate))
}

pub fn use_profile(config: &mut CliConfig, name: &str) -> Result<ProfileView, CliError> {
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    config.active_profile = Some(name.to_owned());
    Ok(profile_view(name, profile.redacted(), true))
}

pub fn remove_profile(config: &mut CliConfig, name: &str) -> Result<ProfileSummary, CliError> {
    let removed = config
        .profiles
        .remove(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    if config.active_profile.as_deref() == Some(name) {
        config.active_profile = None;
    }
    Ok(ProfileSummary {
        name: name.to_owned(),
        mode: removed.mode,
        active: false,
    })
}

pub fn resolve_profile<'a>(
    config: &'a CliConfig,
    explicit_name: Option<&'a str>,
) -> Result<(&'a str, &'a ProfileConfig), CliError> {
    let name = match explicit_name {
        Some(name) => name,
        None => config
            .active_profile
            .as_deref()
            .ok_or_else(CliError::no_active_profile)?,
    };
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    Ok((name, profile))
}

fn profile_view(name: &str, redacted: RedactedProfileConfig, active: bool) -> ProfileView {
    ProfileView {
        name: name.to_owned(),
        mode: redacted.mode,
        active,
        local: redacted.local,
        remote: redacted.remote,
    }
}
