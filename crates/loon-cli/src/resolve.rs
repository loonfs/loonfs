use crate::backend::ResolvedTarget;
use crate::config::{default_config_path, load_config, CliConfig};
use crate::error::CliError;
use crate::profiles::{default_namespace, resolve_profile};

pub struct LoadedConfig {
    pub path: std::path::PathBuf,
    pub config: CliConfig,
}

pub struct ResolvedProfile {
    pub profile_name: String,
    pub target: ResolvedTarget,
}

pub struct ResolvedNamespace {
    pub namespace: String,
}

pub fn load_cli_config() -> Result<LoadedConfig, CliError> {
    let path = default_config_path()?;
    let config = load_config(&path)?;
    Ok(LoadedConfig { path, config })
}

pub fn resolve_target_profile(explicit_profile: Option<&str>) -> Result<ResolvedProfile, CliError> {
    let loaded = load_cli_config()?;
    resolve_target_profile_from_config(&loaded.config, explicit_profile)
}

pub fn resolve_target_profile_from_config(
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

pub fn resolve_namespace(
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
    if namespace.trim().is_empty() {
        return Err(CliError::invalid_input("namespace_id must not be empty"));
    }
    Ok(ResolvedNamespace {
        namespace: namespace.to_owned(),
    })
}
