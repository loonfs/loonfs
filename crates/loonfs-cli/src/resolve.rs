use crate::backend::ResolvedTarget;
use crate::config::{default_config_path, load_config, CliConfig};
use crate::error::CliError;
use crate::profiles::{default_namespace, resolve_profile};
use loonfs_api::NamespaceId;

pub(crate) struct LoadedConfig {
    pub path: std::path::PathBuf,
    pub config: CliConfig,
}

pub(crate) struct ResolvedProfile {
    pub profile_name: String,
    pub target: ResolvedTarget,
}

pub(crate) struct ResolvedNamespace {
    pub namespace: String,
}

pub(crate) fn load_cli_config() -> Result<LoadedConfig, CliError> {
    let path = default_config_path()?;
    let config = load_config(&path)?;
    Ok(LoadedConfig { path, config })
}

pub(crate) fn resolve_target_profile(
    explicit_profile: Option<&str>,
) -> Result<ResolvedProfile, CliError> {
    let loaded = load_cli_config()?;
    resolve_target_profile_from_config(&loaded.config, explicit_profile)
}

pub(crate) fn resolve_target_profile_from_config(
    config: &CliConfig,
    explicit_profile: Option<&str>,
) -> Result<ResolvedProfile, CliError> {
    let (profile_name, profile) = resolve_profile(config, explicit_profile)?;
    let target = ResolvedTarget::resolve(profile_name, profile)?;
    Ok(ResolvedProfile {
        profile_name: profile_name.to_owned(),
        target,
    })
}

pub(crate) fn resolve_namespace(
    config: &CliConfig,
    explicit_profile: Option<&str>,
    explicit_namespace: Option<&str>,
) -> Result<ResolvedNamespace, CliError> {
    if let Some(namespace) = explicit_namespace {
        return validate_namespace(namespace);
    }
    let (profile_name, profile) = resolve_profile(config, explicit_profile)?;
    let namespace =
        default_namespace(profile).ok_or_else(|| CliError::no_default_namespace(profile_name))?;
    validate_namespace(namespace)
}

fn validate_namespace(namespace: &str) -> Result<ResolvedNamespace, CliError> {
    NamespaceId::parse(namespace).map_err(|error| CliError::invalid_input(error.to_string()))?;
    Ok(ResolvedNamespace {
        namespace: namespace.to_owned(),
    })
}
