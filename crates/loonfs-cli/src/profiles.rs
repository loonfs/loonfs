//! Profile listing, inspection, and mutation over the loaded CLI config.

use crate::config::{validate_profile_name, CliConfig, ProfileConfig};
use crate::error::CliError;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ProfileSummary {
    pub name: String,
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_kind: Option<String>,
}

pub(crate) fn list_profiles(config: Option<&CliConfig>) -> Vec<ProfileSummary> {
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

pub(crate) fn show_profile(
    config: &CliConfig,
    explicit_name: Option<&str>,
) -> Result<(String, ProfileConfig), CliError> {
    let (name, profile) = resolve_profile(config, explicit_name)?;
    Ok((name.to_owned(), profile.redacted()))
}

pub(crate) fn add_profile(
    config: &mut CliConfig,
    name: &str,
    profile: ProfileConfig,
) -> Result<(String, ProfileConfig), CliError> {
    validate_profile_name(name)?;
    if config.profiles.contains_key(name) {
        return Err(CliError::profile_already_exists(name));
    }
    let is_first = config.profiles.is_empty();
    let redacted = profile.redacted();
    config.profiles.insert(name.to_owned(), profile);
    if is_first {
        config.default_profile = Some(name.to_owned());
    }
    Ok((name.to_owned(), redacted))
}

pub(crate) fn update_profile(
    config: &mut CliConfig,
    name: &str,
    profile: ProfileConfig,
) -> Result<(String, ProfileConfig), CliError> {
    if !config.profiles.contains_key(name) {
        return Err(CliError::profile_not_found(name));
    }
    let redacted = profile.redacted();
    config.profiles.insert(name.to_owned(), profile);
    Ok((name.to_owned(), redacted))
}

pub(crate) fn delete_profile(
    config: &mut CliConfig,
    name: &str,
) -> Result<ProfileSummary, CliError> {
    let removed = config
        .profiles
        .remove(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    if config.default_profile.as_deref() == Some(name) {
        config.default_profile = None;
    }
    Ok(ProfileSummary {
        name: name.to_owned(),
        mode: removed.mode_str().to_owned(),
        store_kind: removed.store_kind_str().map(ToOwned::to_owned),
    })
}

/// [`delete_profile`] over the loose table of a degraded config, so a
/// profile the strict decoder rejects can still be deleted.
pub(crate) fn delete_profile_in_table(
    table: &mut toml::Table,
    name: &str,
) -> Result<ProfileSummary, CliError> {
    let removed = table
        .get_mut("profiles")
        .and_then(toml::Value::as_table_mut)
        .and_then(|profiles| profiles.remove(name))
        .ok_or_else(|| CliError::profile_not_found(name))?;
    if table.get("default_profile").and_then(toml::Value::as_str) == Some(name) {
        table.remove("default_profile");
    }
    Ok(ProfileSummary {
        name: name.to_owned(),
        mode: removed
            .get("mode")
            .and_then(toml::Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        store_kind: removed
            .get("store")
            .and_then(|store| store.get("kind"))
            .and_then(toml::Value::as_str)
            .map(ToOwned::to_owned),
    })
}

pub(crate) fn make_default_profile(config: &mut CliConfig, name: &str) -> Result<(), CliError> {
    if !config.profiles.contains_key(name) {
        return Err(CliError::profile_not_found(name));
    }
    config.default_profile = Some(name.to_owned());
    Ok(())
}

/// [`make_default_profile`] over the loose table of a degraded config.
pub(crate) fn make_default_profile_in_table(
    table: &mut toml::Table,
    name: &str,
) -> Result<(), CliError> {
    let exists = table
        .get("profiles")
        .and_then(toml::Value::as_table)
        .is_some_and(|profiles| profiles.contains_key(name));
    if !exists {
        return Err(CliError::profile_not_found(name));
    }
    table.insert(
        "default_profile".to_owned(),
        toml::Value::String(name.to_owned()),
    );
    Ok(())
}

pub(crate) fn resolve_profile<'a>(
    config: &'a CliConfig,
    explicit_name: Option<&'a str>,
) -> Result<(&'a str, &'a ProfileConfig), CliError> {
    let name = match explicit_name {
        Some(name) => name,
        None => config
            .default_profile
            .as_deref()
            .ok_or_else(CliError::no_default_profile)?,
    };
    let profile = config
        .profiles
        .get(name)
        .ok_or_else(|| CliError::profile_not_found(name))?;
    Ok((name, profile))
}

pub(crate) fn default_namespace(profile: &ProfileConfig) -> Option<&str> {
    match profile {
        ProfileConfig::Embedded {
            default_namespace, ..
        }
        | ProfileConfig::Remote {
            default_namespace, ..
        } => default_namespace.as_deref(),
    }
}

pub(crate) fn set_default_namespace(
    config: &mut CliConfig,
    profile_name: &str,
    namespace: &str,
) -> Result<(), CliError> {
    let profile = config
        .profiles
        .get_mut(profile_name)
        .ok_or_else(|| CliError::profile_not_found(profile_name))?;
    match profile {
        ProfileConfig::Embedded {
            default_namespace, ..
        }
        | ProfileConfig::Remote {
            default_namespace, ..
        } => {
            *default_namespace = Some(namespace.to_owned());
        }
    }
    Ok(())
}
