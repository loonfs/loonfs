use crate::backend::ResolvedTarget;
use crate::config::{default_config_path, load_config};
use crate::error::CliError;
use crate::profiles::resolve_profile;
use std::path::{Path, PathBuf};

pub struct ResolvedProfile {
    pub profile_name: String,
    pub target: ResolvedTarget,
}

pub fn resolved_config_path(explicit: Option<&Path>) -> Result<PathBuf, CliError> {
    match explicit {
        Some(path) => Ok(path.to_path_buf()),
        None => default_config_path(),
    }
}

pub fn resolve_target_profile(
    explicit_config: Option<&Path>,
    explicit_profile: Option<&str>,
) -> Result<ResolvedProfile, CliError> {
    let config_path = resolved_config_path(explicit_config)?;
    let config = load_config(&config_path)?;
    let (profile_name, profile) = resolve_profile(&config, explicit_profile)?;
    let target = ResolvedTarget::resolve(profile_name, profile)?;
    Ok(ResolvedProfile {
        profile_name: profile_name.to_owned(),
        target,
    })
}
