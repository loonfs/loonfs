//! Shared per-command context: target resolution and common helpers.

use super::output::CommandFailure;
use crate::args::{CommandKind, TargetSelectorArgs};
use crate::error::CliError;
use crate::resolve::{load_cli_config, resolve_namespace, resolve_target_profile_from_config};
use loonfs_api::{AbsolutePath, NamespaceId};
use loonfs_client::NamespacePath;
use std::path::{Path, PathBuf};

pub(crate) struct CommandContext {
    pub(crate) profile_name: String,
    pub(crate) mode: String,
    pub(crate) namespace: NamespaceId,
    pub(crate) target: crate::backend::ResolvedTarget,
}

/// Attributes a failure to a resolved profile and mode, for command paths
/// that run before (or without) a full [`CommandContext`].
pub(crate) fn fail_for(
    kind: CommandKind,
    profile_name: &str,
    mode: &str,
    error: impl Into<CliError>,
) -> CommandFailure {
    fail(
        kind,
        Some(profile_name.to_owned()),
        Some(mode.to_owned()),
        error,
    )
}

impl CommandContext {
    /// Attributes a failure to this resolved context: the profile and mode
    /// that produced it ride with the error.
    pub(crate) fn fail(&self, kind: CommandKind, error: impl Into<CliError>) -> CommandFailure {
        fail(
            kind,
            Some(self.profile_name.clone()),
            Some(self.mode.clone()),
            error,
        )
    }
}

pub(crate) async fn resolve_command_context(
    kind: CommandKind,
    target: &TargetSelectorArgs,
) -> Result<CommandContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(
        &loaded.config,
        explicit_profile,
        target.profile.no_retry,
    )
    .await
    .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let namespace = resolve_namespace(
        &loaded.config,
        explicit_profile,
        target.namespace.as_deref(),
    )
    .map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?
    .namespace;

    Ok(CommandContext {
        profile_name: resolved.profile_name,
        mode,
        namespace,
        target: resolved.target,
    })
}

// --- general helpers ---

pub(crate) fn namespace_path(
    namespace_id: &NamespaceId,
    path: &str,
    allow_root: bool,
) -> Result<NamespacePath, CliError> {
    Ok(NamespacePath::new(
        namespace_id.clone(),
        normalize_absolute_path(path, allow_root)?,
    ))
}

/// Parses a CLI path argument through the API's absolute-path grammar, so
/// the CLI rejects exactly what the server would. `allow_root` is the only
/// CLI-local policy on top.
pub(crate) fn normalize_absolute_path(
    path: &str,
    allow_root: bool,
) -> Result<AbsolutePath, CliError> {
    let path =
        AbsolutePath::parse(path).map_err(|error| CliError::invalid_input(error.to_string()))?;
    if !allow_root && path.is_root() {
        return Err(CliError::invalid_input(
            "root path is not allowed for this command",
        ));
    }
    Ok(path)
}

pub(crate) fn default_remote_put_path(local_path: &Path) -> Result<AbsolutePath, CliError> {
    let file_name = local_path.file_name().ok_or_else(|| {
        CliError::invalid_input(format!(
            "unable to derive remote target from `{}`",
            local_path.display()
        ))
    })?;
    AbsolutePath::parse(format!("/{}", file_name.to_string_lossy()))
        .map_err(|error| CliError::invalid_input(error.to_string()))
}

pub(crate) fn destination_path_for_get(
    remote_path: &str,
    explicit_destination: Option<&str>,
) -> Result<PathBuf, CliError> {
    match explicit_destination {
        Some(path) => Ok(PathBuf::from(path)),
        None => {
            let file_name = Path::new(remote_path).file_name().ok_or_else(|| {
                CliError::invalid_input(format!(
                    "unable to derive local destination from `{remote_path}`"
                ))
            })?;
            Ok(PathBuf::from(file_name))
        }
    }
}

pub(crate) fn render_target(namespace_id: &NamespaceId, absolute_path: &AbsolutePath) -> String {
    format!("{namespace_id}:{absolute_path}")
}

pub(crate) fn fail(
    kind: CommandKind,
    profile: Option<String>,
    mode: Option<String>,
    error: impl Into<CliError>,
) -> CommandFailure {
    CommandFailure {
        kind,
        profile,
        mode,
        error: Box::new(error.into()),
    }
}
