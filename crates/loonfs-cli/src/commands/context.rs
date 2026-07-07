use super::output::CommandFailure;
use crate::args::{CommandKind, TargetSelectorArgs};
use crate::error::CliError;
use crate::resolve::{load_cli_config, resolve_namespace, resolve_target_profile_from_config};
use loonfs_api::{ErrorCode, NamespaceId};
use loonfs_client::NamespacePath;
use std::path::{Path, PathBuf};

pub(crate) struct CommandContext {
    pub(crate) profile_name: String,
    pub(crate) mode: String,
    pub(crate) namespace: String,
    pub(crate) target: crate::backend::ResolvedTarget,
}

pub(crate) async fn resolve_command_context(
    kind: CommandKind,
    target: &TargetSelectorArgs,
) -> Result<CommandContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let loaded = load_cli_config()
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(&loaded.config, explicit_profile)
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

    // Keep the loaded config alive long enough for the backend borrow to remain valid within the caller.
    Ok(CommandContext {
        profile_name: resolved.profile_name,
        mode,
        namespace,
        target: resolved.target,
    })
}

// --- general helpers ---

pub(crate) fn validate_namespace_id(namespace: &str) -> Result<(), CliError> {
    // A malformed namespace id surfaces its registry code so both profile
    // modes report the same code the server would serve for it.
    NamespaceId::parse(namespace)
        .map(|_| ())
        .map_err(|error| CliError::new(ErrorCode::InvalidRequest.as_str(), error.to_string()))
}

pub(crate) fn namespace_path(
    namespace: &str,
    path: &str,
    allow_root: bool,
) -> Result<NamespacePath, CliError> {
    validate_namespace_id(namespace)?;
    Ok(NamespacePath {
        namespace: namespace.to_owned(),
        absolute_path: normalize_absolute_path(path, allow_root)?,
    })
}

pub(crate) fn normalize_absolute_path(path: &str, allow_root: bool) -> Result<String, CliError> {
    if !path.starts_with('/') {
        return Err(CliError::invalid_input(format!(
            "filesystem paths must be absolute: `{path}`"
        )));
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        if component.is_empty() {
            continue;
        }
        if component == "." || component == ".." {
            return Err(CliError::invalid_input(format!(
                "invalid filesystem path `{path}`"
            )));
        }
        components.push(component);
    }
    if components.is_empty() {
        if allow_root {
            return Ok("/".to_owned());
        }
        return Err(CliError::invalid_input(
            "root path is not allowed for this command",
        ));
    }
    Ok(format!("/{}", components.join("/")))
}

pub(crate) fn default_remote_put_path(local_path: &Path) -> Result<String, CliError> {
    let file_name = local_path.file_name().ok_or_else(|| {
        CliError::invalid_input(format!(
            "unable to derive remote target from `{}`",
            local_path.display()
        ))
    })?;
    Ok(format!("/{}", file_name.to_string_lossy()))
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

pub(crate) fn render_target(namespace: &str, path: &str) -> String {
    format!("{namespace}:{path}")
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
        error: error.into(),
    }
}
