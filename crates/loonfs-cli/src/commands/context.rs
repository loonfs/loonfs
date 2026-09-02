//! Shared per-command context: target resolution and common helpers.

use super::output::CommandFailure;
use crate::args::{ActorSelectorArgs, CommandKind, TargetSelectorArgs};
use crate::config::{ConfigLocation, ConfigSource};
use crate::error::CliError;
use crate::resolve::{
    load_cli_config, resolve_actor, resolve_namespace, resolve_target_profile_from_config,
};
use loonfs_api::{
    AbsolutePath, ActorRef, ChangeSeq, ErrorCode, InodeId, NamespaceId, PublicOrdinalRangeError,
};
use loonfs_client::NamespacePath;
use std::path::{Path, PathBuf};

pub(crate) struct CommandContext {
    pub(crate) profile_name: String,
    pub(crate) mode: String,
    pub(crate) namespace: NamespaceId,
    pub(crate) actor: ActorRef,
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

/// Validates an ordinal argument before dispatching the command.
pub(crate) fn parse_public_ordinal_arg<T>(
    argument: &str,
    value: u64,
    constructor: impl FnOnce(u64) -> Result<T, PublicOrdinalRangeError>,
) -> Result<T, CliError> {
    constructor(value).map_err(|error| {
        CliError::new(
            ErrorCode::InvalidRequest.as_str(),
            format!("invalid {argument} `{value}`: {error}"),
        )
        .with_param(argument)
    })
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
    config_path: &Path,
    target: &TargetSelectorArgs,
) -> Result<CommandContext, CommandFailure> {
    resolve_command_context_with_actor(kind, config_path, target, None).await
}

pub(crate) async fn resolve_mutation_context(
    kind: CommandKind,
    config_path: &Path,
    target: &TargetSelectorArgs,
    actor: &ActorSelectorArgs,
) -> Result<CommandContext, CommandFailure> {
    resolve_command_context_with_actor(kind, config_path, target, Some(actor)).await
}

async fn resolve_command_context_with_actor(
    kind: CommandKind,
    config_path: &Path,
    target: &TargetSelectorArgs,
    actor: Option<&ActorSelectorArgs>,
) -> Result<CommandContext, CommandFailure> {
    let explicit_profile = target.profile.profile.as_deref();
    let loaded = load_cli_config(config_path)
        .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let resolved = resolve_target_profile_from_config(
        &loaded.config,
        explicit_profile,
        target.request.no_retry,
    )
    .await
    .map_err(|error| fail(kind, explicit_profile.map(ToOwned::to_owned), None, error))?;
    let mode = resolved.target.mode_str().to_owned();
    let (_, profile) =
        crate::profiles::resolve_profile(&loaded.config, explicit_profile).map_err(|error| {
            fail(
                kind,
                Some(resolved.profile_name.clone()),
                Some(mode.clone()),
                error,
            )
        })?;
    let actor = resolve_actor(
        profile,
        actor.and_then(|actor| actor.actor_kind).map(Into::into),
        actor.and_then(|actor| actor.actor_id.as_deref()),
    )
    .map_err(|error| {
        fail(
            kind,
            Some(resolved.profile_name.clone()),
            Some(mode.clone()),
            error,
        )
    })?;
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
        actor,
        target: resolved.target,
    })
}

// --- general helpers ---

pub(crate) fn namespace_path(
    namespace_id: &NamespaceId,
    param: &str,
    path: &str,
    allow_root: bool,
) -> Result<NamespacePath, CliError> {
    Ok(NamespacePath::new(
        namespace_id.clone(),
        parse_user_path_arg(param, path, allow_root)?,
    ))
}

/// Whether a human-entered path spelled directory intent: one trailing
/// slash after a non-root path. `put`, `cp`, and `mv` destinations read it
/// as "into this directory"; everywhere else the slash is simply accepted.
pub(crate) fn directory_intent(path: &str) -> bool {
    path.len() > 1 && path.ends_with('/') && !path.ends_with("//")
}

/// Removes one trailing slash used to mark a directory. Repeated separators,
/// `.` and `..` components, and relative paths remain invalid.
pub(crate) fn parse_user_path_arg(
    param: &str,
    path: &str,
    allow_root: bool,
) -> Result<AbsolutePath, CliError> {
    let trimmed = if directory_intent(path) {
        &path[..path.len() - 1]
    } else {
        path
    };
    let parsed = AbsolutePath::parse(trimmed)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param(param))?;
    if !allow_root && parsed.is_root() {
        return Err(
            CliError::invalid_request("root path is not allowed for this command")
                .with_param(param),
        );
    }
    Ok(parsed)
}

/// Appends the source name when the destination ends in `/`. Otherwise, the
/// destination is treated as the full path.
pub(crate) fn destination_user_path(
    path_param: &str,
    source_leaf_param: &str,
    raw: &str,
    source_leaf: &str,
    allow_root_directory: bool,
) -> Result<AbsolutePath, CliError> {
    if directory_intent(raw) || raw == "/" {
        let directory = parse_user_path_arg(path_param, raw, true)?;
        if !allow_root_directory && directory.is_root() && raw == "/" {
            return Err(
                CliError::invalid_request("root path is not allowed for this command")
                    .with_param(path_param),
            );
        }
        let leaf = loonfs_api::DisplayName::parse(source_leaf).map_err(|error| {
            CliError::invalid_request(error.to_string()).with_param(source_leaf_param)
        })?;
        return Ok(directory.join(&leaf));
    }
    parse_user_path_arg(path_param, raw, false)
}

pub(crate) fn default_remote_put_path(local_path: &Path) -> Result<AbsolutePath, CliError> {
    let file_name = local_path.file_name().ok_or_else(|| {
        CliError::invalid_request(format!(
            "unable to derive remote target from `{}`",
            local_path.display()
        ))
        .with_param("local_path")
    })?;
    AbsolutePath::parse(format!("/{}", file_name.to_string_lossy()))
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("local_path"))
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
                CliError::invalid_request(format!(
                    "unable to derive local destination from `{remote_path}`"
                ))
                .with_param("remote_path")
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

// --- printed recovery commands ---

/// The flags a printed `loonfs undelete` has to spell so that pasting it into
/// a later shell still reaches the filesystem this command ran against.
///
/// Every one of them names something the CLI would otherwise take from
/// ambient state that drifts between the print and the paste: the namespace
/// comes from a config value `loonfs use` rewrites, the profile from another
/// the `profile default` command rewrites, and the config file itself from a
/// flag or an environment variable that a later shell need not repeat. The
/// hint is only worth printing if it survives all three, so the flags are
/// built once per invocation and shared by every entry it prints.
pub(crate) struct UndeleteHint {
    /// Rendered and already shell-quoted, for example
    /// `--namespace demo --profile prod`. Never empty: `--namespace` is
    /// unconditional.
    context_flags: String,
}

impl UndeleteHint {
    pub(crate) fn new(
        context: &CommandContext,
        location: &ConfigLocation,
        explicit_profile: bool,
    ) -> Self {
        let mut context_flags = format!(" --namespace {}", shell_quote(context.namespace.as_str()));
        // A bare invocation resolves the config's default profile, which is
        // exactly the profile this run used whenever `--profile` went
        // unspelled; spelling it means the default may be some other profile,
        // and then the hint has to say which one it meant.
        if explicit_profile {
            context_flags.push_str(&format!(
                " --profile {}",
                shell_quote(&context.profile_name)
            ));
        }
        // The flag and the environment variable both name a file that a bare
        // invocation would not find on its own — the flag because it is not
        // spelled again, the variable because a later shell need not still
        // export it. The default locations need no flag: they are what a bare
        // invocation looks at.
        if matches!(location.source, ConfigSource::Flag | ConfigSource::Env) {
            context_flags.push_str(&format!(
                " --config {}",
                shell_quote(&location.path.to_string_lossy())
            ));
        }
        Self { context_flags }
    }

    /// The complete `loonfs undelete` invocation that recovers one deletion.
    pub(crate) fn command(&self, inode_id: InodeId, deletion_seq: ChangeSeq) -> String {
        let inode_id = loonfs_api::public_inode_id::encode(inode_id);
        format!(
            "loonfs undelete --inode {inode_id} --deletion-seq {}{}",
            deletion_seq.0, self.context_flags
        )
    }
}

/// Quotes an argument for a POSIX shell, leaving it alone when it needs no
/// quoting.
///
/// Names may hold spaces and quotes (they are display strings, not
/// identifiers), and so may profile names and config paths, so a printed
/// command that is meant to be pasted has to survive them. Single quotes take
/// everything literally, which is why the one character they cannot hold — a
/// single quote — is spliced in as an escaped one outside them.
fn shell_quote(argument: &str) -> String {
    const SAFE: &str = "@%+=:,./-_";
    if !argument.is_empty()
        && argument
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || SAFE.contains(character))
    {
        return argument.to_owned();
    }
    format!("'{}'", argument.replace('\'', r#"'\''"#))
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
    use super::{destination_user_path, parse_user_path_arg, shell_quote};

    #[test]
    fn quoting_leaves_ordinary_paths_alone_and_survives_a_quote() {
        // Quoting everything would make the common command harder to read,
        // so an argument a shell would pass through untouched stays bare.
        assert_eq!(shell_quote("/docs/report.txt"), "/docs/report.txt");
        assert_eq!(shell_quote("prod-2"), "prod-2");

        assert_eq!(
            shell_quote("/docs/Quarterly Report.PDF"),
            "'/docs/Quarterly Report.PDF'"
        );
        // A single quote is the one character single quotes cannot hold, so
        // it closes them, contributes an escaped quote, and reopens them.
        assert_eq!(shell_quote("/tom's notes"), r#"'/tom'\''s notes'"#);
        // Empty is not "nothing to quote": bare, it would vanish as an
        // argument entirely.
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn user_paths_keep_wire_strictness_except_directory_intent() {
        // One trailing slash is directory intent; everything else the wire
        // rejects, the CLI rejects too, instead of silently rewriting.
        assert_eq!(
            parse_user_path_arg("path", "/docs/", false)
                .expect("dir intent")
                .as_str(),
            "/docs"
        );
        assert!(parse_user_path_arg("path", "//docs///A.txt/", false).is_err());
        assert!(parse_user_path_arg("path", "//x.txt", false).is_err());
        assert!(parse_user_path_arg("path", "docs/A.txt", false).is_err());
        assert!(parse_user_path_arg("path", "", false).is_err());
        assert_eq!(
            parse_user_path_arg("path", "/", true)
                .expect("root")
                .as_str(),
            "/"
        );

        assert_eq!(
            destination_user_path(
                "destination_path",
                "source_path",
                "/docs/",
                "report.pdf",
                false,
            )
            .expect("into directory")
            .as_str(),
            "/docs/report.pdf"
        );
        assert_eq!(
            destination_user_path(
                "destination_path",
                "source_path",
                "/docs/renamed.pdf",
                "report.pdf",
                false,
            )
            .expect("full destination")
            .as_str(),
            "/docs/renamed.pdf"
        );
        assert_eq!(
            destination_user_path("destination_path", "source_path", "/", "report.pdf", true,)
                .expect("into root")
                .as_str(),
            "/report.pdf"
        );
    }
}
