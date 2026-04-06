use crate::config::{CliConfig, ProfileConfig};
use crate::error::CliError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileSummary {
    pub name: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_kind: Option<String>,
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
            mode: profile.mode_str().to_owned(),
            store_kind: profile.store_kind_str().map(ToOwned::to_owned),
        })
        .collect()
}

pub fn show_profile(
    config: &CliConfig,
    explicit_name: Option<&str>,
) -> Result<(String, ProfileConfig), CliError> {
    let name = explicit_name.unwrap_or("default");
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    Ok((name.to_owned(), profile.redacted()))
}

pub fn add_profile(
    config: &mut CliConfig,
    name: &str,
    profile: ProfileConfig,
) -> Result<(String, ProfileConfig), CliError> {
    if config.profiles.contains_key(name) {
        return Err(CliError::profile_already_exists(name));
    }
    let redacted = profile.redacted();
    config.profiles.insert(name.to_owned(), profile);
    Ok((name.to_owned(), redacted))
}

pub fn remove_profile(config: &mut CliConfig, name: &str) -> Result<ProfileSummary, CliError> {
    let removed = config
        .profiles
        .remove(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    Ok(ProfileSummary {
        name: name.to_owned(),
        mode: removed.mode_str().to_owned(),
        store_kind: removed.store_kind_str().map(ToOwned::to_owned),
    })
}

pub fn resolve_profile<'a>(
    config: &'a CliConfig,
    explicit_name: Option<&'a str>,
) -> Result<(&'a str, &'a ProfileConfig), CliError> {
    let name = explicit_name.unwrap_or("default");
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    Ok((name, profile))
}
