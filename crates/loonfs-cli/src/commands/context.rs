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
    pub(crate) target: crate::resolve::ResolvedTarget,
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
        parse_user_path(path, allow_root)?,
    ))
}

/// Whether a human-entered path spelled directory intent: one trailing
/// slash after a non-root path. `put`, `cp`, and `mv` destinations read it
/// as "into this directory"; everywhere else the slash is simply accepted.
pub(crate) fn directory_intent(path: &str) -> bool {
    path.len() > 1 && path.ends_with('/') && !path.ends_with("//")
}

/// Parses a human-entered CLI path with the wire's strictness, plus exactly
/// one concession: a single trailing slash (directory intent) is accepted
/// and dropped. Repeated separators, `.`/`..`, and relative spellings fail
/// here exactly as the wire rejects them — the CLI never silently rewrites
/// a path into something the caller did not type.
pub(crate) fn parse_user_path(path: &str, allow_root: bool) -> Result<AbsolutePath, CliError> {
    let trimmed = if directory_intent(path) {
        &path[..path.len() - 1]
    } else {
        path
    };
    let parsed =
        AbsolutePath::parse(trimmed).map_err(|error| CliError::invalid_input(error.to_string()))?;
    if !allow_root && parsed.is_root() {
        return Err(CliError::invalid_input(
            "root path is not allowed for this command",
        ));
    }
    Ok(parsed)
}

/// Resolves a mutation destination: with directory intent the source's leaf
/// name lands inside the named directory; otherwise the path is the full
/// destination.
pub(crate) fn destination_user_path(
    raw: &str,
    source_leaf: &str,
    allow_root_directory: bool,
) -> Result<AbsolutePath, CliError> {
    if directory_intent(raw) || raw == "/" {
        let directory = parse_user_path(raw, true)?;
        if !allow_root_directory && directory.is_root() && raw == "/" {
            return Err(CliError::invalid_input(
                "root path is not allowed for this command",
            ));
        }
        let leaf = loonfs_api::DisplayName::parse(source_leaf)
            .map_err(|error| CliError::invalid_input(error.to_string()))?;
        return Ok(directory.join(&leaf));
    }
    parse_user_path(raw, false)
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
    let remote_leaf = || {
        Path::new(remote_path)
            .file_name()
            .map(PathBuf::from)
            .ok_or_else(|| {
                CliError::invalid_input(format!(
                    "unable to derive local destination from `{remote_path}`"
                ))
            })
    };
    match explicit_destination {
        // The cp habit: a trailing separator or an existing directory means
        // the file lands inside it under its remote name, never as a file
        // named like the directory.
        Some(path) if path.ends_with('/') || Path::new(path).is_dir() => {
            Ok(Path::new(path).join(remote_leaf()?))
        }
        Some(path) => Ok(PathBuf::from(path)),
        None => remote_leaf(),
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

#[cfg(test)]
mod tests {
    use super::{destination_user_path, parse_user_path};

    #[test]
    fn user_paths_keep_wire_strictness_except_directory_intent() {
        // One trailing slash is directory intent; everything else the wire
        // rejects, the CLI rejects too, instead of silently rewriting.
        assert_eq!(
            parse_user_path("/docs/", false)
                .expect("dir intent")
                .as_str(),
            "/docs"
        );
        assert!(parse_user_path("//docs///A.txt/", false).is_err());
        assert!(parse_user_path("//x.txt", false).is_err());
        assert!(parse_user_path("docs/A.txt", false).is_err());
        assert!(parse_user_path("", false).is_err());
        assert_eq!(parse_user_path("/", true).expect("root").as_str(), "/");

        assert_eq!(
            destination_user_path("/docs/", "report.pdf", false)
                .expect("into directory")
                .as_str(),
            "/docs/report.pdf"
        );
        assert_eq!(
            destination_user_path("/docs/renamed.pdf", "report.pdf", false)
                .expect("full destination")
                .as_str(),
            "/docs/renamed.pdf"
        );
        assert_eq!(
            destination_user_path("/", "report.pdf", true)
                .expect("into root")
                .as_str(),
            "/report.pdf"
        );
    }
}
