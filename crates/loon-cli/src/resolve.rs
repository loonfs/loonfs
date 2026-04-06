use crate::backend::ResolvedTarget;
use crate::config::{default_config_path, load_config};
use crate::error::CliError;
use crate::profiles::resolve_profile;

pub struct ResolvedProfile {
    pub profile_name: String,
    pub target: ResolvedTarget,
}

pub fn resolve_target_profile(explicit_profile: Option<&str>) -> Result<ResolvedProfile, CliError> {
    let config_path = default_config_path()?;
    let config = load_config(&config_path)?;
    let (profile_name, profile) = resolve_profile(&config, explicit_profile)?;
    let target = ResolvedTarget::resolve(profile_name, profile)?;
    Ok(ResolvedProfile {
        profile_name: profile_name.to_owned(),
        target,
    })
}
