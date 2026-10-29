//! Shared per-command context: target resolution and common helpers.

use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{CommandKind, RuntimeBehavior, Selectors};
use crate::config::{ProfileConfig, ProfileMode};
use crate::config_file::{load_config, ConfigLocation, ConfigSource};
use crate::error::{CliError, Result};
use crate::profiles::{resolve_profile, NamedProfile};
use crate::resolve::{resolve_actor, resolve_namespace, resolve_subject, ResolvedTarget};
use loonfs_client::{CreateDirectoryOptions, NamespacePath};
use loonfs_types::{
    AbsolutePath, ActorId, ChangeSeq, Commit, EntryInodeKind, ErrorCode, InodeId, NamespaceId,
};
use std::path::{Path, PathBuf};

/// What one invocation computes before its command runs: the command, the
/// config file, how it reports and retries, and the selector flags.
pub(crate) struct Invocation {
    pub(crate) kind: CommandKind,
    pub(crate) config: ConfigLocation,
    pub(crate) behavior: RuntimeBehavior,
    pub(crate) selectors: Selectors,
}

impl Invocation {
    /// Reports a failure that no resolved profile produced.
    pub(crate) fn fail(&self, error: impl Into<CliError>) -> CommandFailure {
        fail(self.kind, None, None, error)
    }

    /// Reports a failure of the profile `profile_name`, which may not exist
    /// or may not have been opened.
    pub(crate) fn fail_for_profile(
        &self,
        profile_name: Option<&str>,
        error: impl Into<CliError>,
    ) -> CommandFailure {
        fail(self.kind, profile_name.map(ToOwned::to_owned), None, error)
    }

    /// Returns `data` as the output of a command that resolved no profile.
    pub(crate) fn output(&self, data: CommandData) -> CommandOutput {
        CommandOutput {
            kind: self.kind,
            profile: None,
            mode: None,
            data,
        }
    }
}

/// The profile a command resolved, and the backend the command opened for
/// it.
pub(crate) struct ProfileContext {
    pub(crate) kind: CommandKind,
    pub(crate) profile_name: String,
    pub(crate) profile: ProfileConfig,
    pub(crate) target: ResolvedTarget,
}

impl ProfileContext {
    /// Resolves the selected profile and opens its backend, with a client
    /// that acts as the token holder.
    pub(crate) async fn resolve(invocation: &Invocation) -> Result<Self, CommandFailure> {
        let explicit_profile = invocation.selectors.profile.as_deref();
        let config = load_config(&invocation.config.path)
            .map_err(|error| invocation.fail_for_profile(explicit_profile, error))?;
        let NamedProfile {
            name: profile_name,
            profile,
        } = resolve_profile(&config, explicit_profile)
            .map_err(|error| invocation.fail_for_profile(explicit_profile, error))?;
        let target =
            ResolvedTarget::resolve(&profile_name, profile, invocation.behavior.transient_retry)
                .await
                .map_err(|error| invocation.fail_for_profile(Some(&profile_name), error))?;
        Ok(Self {
            kind: invocation.kind,
            profile: profile.clone(),
            profile_name,
            target,
        })
    }

    /// Resolves the selected profile with a client that acts as the selected
    /// subject, whose ID defaults to the profile's actor.
    pub(crate) async fn resolve_as_subject(
        invocation: &Invocation,
    ) -> Result<Self, CommandFailure> {
        let context = Self::resolve(invocation).await?;
        let actor_id = context.actor(None)?;
        context.act_as_subject(&invocation.selectors, &actor_id)
    }

    pub(crate) fn mode(&self) -> ProfileMode {
        self.target.mode()
    }

    /// Resolves the actor of a mutation, from `explicit` or the environment
    /// or the profile.
    pub(crate) fn actor(&self, explicit: Option<&ActorId>) -> Result<ActorId, CommandFailure> {
        resolve_actor(&self.profile, explicit).map_err(|error| self.fail(error))
    }

    /// Scopes the client to the subject that the selectors, the environment,
    /// and the profile select, whose ID defaults to `actor_id`.
    pub(crate) fn act_as_subject(
        mut self,
        selectors: &Selectors,
        actor_id: &ActorId,
    ) -> Result<Self, CommandFailure> {
        let subject = resolve_subject(&self.profile, actor_id, &selectors.subject)
            .map_err(|error| self.fail(error))?;
        if let Some(subject) = &subject {
            self.target.scope_to_subject(subject);
        }
        Ok(self)
    }

    /// Resolves the namespace a command runs against, from `explicit`, the
    /// environment, or the profile's default.
    pub(crate) fn namespace(
        &self,
        explicit: Option<&NamespaceId>,
    ) -> Result<NamespaceId, CommandFailure> {
        resolve_namespace(&self.profile_name, &self.profile, explicit)
            .map_err(|error| self.fail(error))
    }

    pub(crate) fn for_namespace(
        self,
        namespace: NamespaceId,
        actor_id: ActorId,
    ) -> NamespaceContext {
        NamespaceContext {
            profile: self,
            namespace,
            actor_id,
        }
    }

    /// Attributes a failure to this resolved context: the profile and mode
    /// that produced it ride with the error.
    pub(crate) fn fail(&self, error: impl Into<CliError>) -> CommandFailure {
        fail(
            self.kind,
            Some(self.profile_name.clone()),
            Some(self.mode()),
            error,
        )
    }

    pub(crate) fn output(&self, data: CommandData) -> CommandOutput {
        CommandOutput {
            kind: self.kind,
            profile: Some(self.profile_name.clone()),
            mode: Some(self.mode()),
            data,
        }
    }
}

/// A resolved profile, and the namespace and the actor of a command that
/// runs against one namespace.
pub(crate) struct NamespaceContext {
    pub(crate) profile: ProfileContext,
    pub(crate) namespace: NamespaceId,
    pub(crate) actor_id: ActorId,
}

impl NamespaceContext {
    /// Resolves the selected profile, the selected namespace, and the actor,
    /// with a client that acts as the selected subject.
    pub(crate) async fn resolve(
        invocation: &Invocation,
        explicit_actor: Option<&ActorId>,
    ) -> Result<Self, CommandFailure> {
        let profile = ProfileContext::resolve(invocation).await?;
        let actor_id = profile.actor(explicit_actor)?;
        let profile = profile.act_as_subject(&invocation.selectors, &actor_id)?;
        let namespace = profile.namespace(invocation.selectors.namespace.as_ref())?;
        Ok(profile.for_namespace(namespace, actor_id))
    }

    pub(crate) fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub(crate) fn actor(&self) -> &ActorId {
        &self.actor_id
    }

    pub(crate) fn fail(&self, error: impl Into<CliError>) -> CommandFailure {
        self.profile.fail(error)
    }

    pub(crate) fn output(&self, data: CommandData) -> CommandOutput {
        self.profile.output(data)
    }
}

pub(crate) enum RemoteDirectoryOutcome {
    Created(Commit),
    AlreadyExists {
        inode_id: InodeId,
        head_seq: ChangeSeq,
    },
}

pub(crate) async fn create_directory_tolerating_existing(
    context: &NamespaceContext,
    spec: &NamespacePath,
    options: &CreateDirectoryOptions,
) -> Result<RemoteDirectoryOutcome> {
    match context
        .profile
        .target
        .client
        .create_directory_with_options(spec, context.actor(), options)
        .await
        .map_err(CliError::from)
    {
        Ok(result) => Ok(RemoteDirectoryOutcome::Created(result)),
        Err(error) if error.code == ErrorCode::PathConflict.as_str() => {
            let existing = context.profile.target.stat_without_attributes(spec).await?;
            if existing.inode_kind() != EntryInodeKind::Directory {
                return Err(error);
            }
            Ok(RemoteDirectoryOutcome::AlreadyExists {
                inode_id: existing.inode_id,
                head_seq: existing.head_seq,
            })
        }
        Err(error) => Err(error),
    }
}

/// Whether a command accepts the root path `/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootPath {
    /// The command accepts `/`.
    Allowed,
    /// The command refuses `/` with `invalid_request`.
    Refused,
}

pub(crate) fn namespace_path(
    namespace_id: &NamespaceId,
    param: &str,
    path: &str,
    root: RootPath,
) -> Result<NamespacePath> {
    Ok(NamespacePath::new(
        namespace_id.clone(),
        parse_user_path_arg(param, path, root)?,
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
pub(crate) fn parse_user_path_arg(param: &str, path: &str, root: RootPath) -> Result<AbsolutePath> {
    let trimmed = if directory_intent(path) {
        &path[..path.len() - 1]
    } else {
        path
    };
    let parsed = AbsolutePath::parse(trimmed)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param(param))?;
    if root == RootPath::Refused && parsed.is_root() {
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
    root: RootPath,
) -> Result<AbsolutePath> {
    if directory_intent(raw) || raw == "/" {
        let directory = parse_user_path_arg(path_param, raw, RootPath::Allowed)?;
        if root == RootPath::Refused && directory.is_root() && raw == "/" {
            return Err(
                CliError::invalid_request("root path is not allowed for this command")
                    .with_param(path_param),
            );
        }
        let leaf = loonfs_types::DisplayName::parse(source_leaf).map_err(|error| {
            CliError::invalid_request(error.to_string()).with_param(source_leaf_param)
        })?;
        return Ok(directory.join(&leaf));
    }
    parse_user_path_arg(path_param, raw, RootPath::Refused)
}

pub(crate) fn utf8_local_name(name: &std::ffi::OsStr) -> Result<&str> {
    name.to_str().ok_or_else(|| {
        CliError::invalid_request("local path is not valid UTF-8").with_param("local_path")
    })
}

pub(crate) fn default_remote_put_path(local_path: &Path) -> Result<AbsolutePath> {
    let file_name = local_path.file_name().ok_or_else(|| {
        CliError::invalid_request(format!(
            "unable to derive remote target from `{}`",
            local_path.display()
        ))
        .with_param("local_path")
    })?;
    AbsolutePath::parse(format!("/{}", utf8_local_name(file_name)?))
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("local_path"))
}

pub(crate) fn destination_path_for_get(
    remote_path: &AbsolutePath,
    explicit_destination: Option<&str>,
) -> Result<PathBuf> {
    let remote_leaf = || {
        remote_path
            .final_component()
            .map(|component| PathBuf::from(component.as_str()))
            .ok_or_else(|| {
                CliError::invalid_request(format!(
                    "unable to derive local destination from `{remote_path}`"
                ))
                .with_param("remote_path")
            })
    };
    match explicit_destination {
        // The cp habit: a trailing separator or an existing directory means
        // the file is written inside it under its remote name, never as a file
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

/// Whether the command line named the profile a command ran against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProfileSource {
    /// `--profile` named the profile.
    Flag,
    /// The command line named no profile.
    Default,
}

impl ProfileSource {
    /// Returns `Flag` when the selectors have a `--profile` value.
    pub(crate) fn from_selectors(selectors: &Selectors) -> Self {
        if selectors.profile.is_some() {
            Self::Flag
        } else {
            Self::Default
        }
    }
}

impl UndeleteHint {
    pub(crate) fn new(
        context: &NamespaceContext,
        location: &ConfigLocation,
        profile_source: ProfileSource,
    ) -> Self {
        let mut context_flags =
            format!(" --namespace {}", shell_quote(context.namespace().as_str()));
        // A bare invocation resolves the config's default profile, which is
        // exactly the profile this run used whenever `--profile` went
        // unspelled; spelling it means the default may be some other profile,
        // and then the hint has to say which one it meant.
        if profile_source == ProfileSource::Flag {
            context_flags.push_str(&format!(
                " --profile {}",
                shell_quote(&context.profile.profile_name)
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
        let inode_id = loonfs_types::public_inode_id::encode(inode_id);
        format!(
            "loonfs undelete --inode {inode_id} --deletion-seq {}{}",
            deletion_seq.0, self.context_flags
        )
    }
}

/// Quotes an argument for a POSIX shell, leaving it alone when it needs no
/// quoting.
///
/// Names may contain spaces and quotes (they are display strings, not
/// identifiers), and so may profile names and config paths, so a printed
/// command that is meant to be pasted has to survive them. Single quotes take
/// everything literally, which is why the one character they cannot contain — a
/// single quote — is spliced in as an escaped one outside them.
pub(crate) fn shell_quote(argument: &str) -> String {
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
    mode: Option<ProfileMode>,
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
    use super::{destination_user_path, parse_user_path_arg, shell_quote, RootPath};

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
            parse_user_path_arg("path", "/docs/", RootPath::Refused)
                .expect("dir intent")
                .as_str(),
            "/docs"
        );
        assert_eq!(
            parse_user_path_arg("path", "//docs///A.txt/", RootPath::Refused)
                .expect_err("a path with repeated slashes should be rejected")
                .code,
            loonfs_types::ErrorCode::InvalidRequest.as_str()
        );
        assert_eq!(
            parse_user_path_arg("path", "//x.txt", RootPath::Refused)
                .expect_err("a path with a leading double slash should be rejected")
                .code,
            loonfs_types::ErrorCode::InvalidRequest.as_str()
        );
        assert_eq!(
            parse_user_path_arg("path", "docs/A.txt", RootPath::Refused)
                .expect_err("a relative path should be rejected")
                .code,
            loonfs_types::ErrorCode::InvalidRequest.as_str()
        );
        assert_eq!(
            parse_user_path_arg("path", "", RootPath::Refused)
                .expect_err("an empty path should be rejected")
                .code,
            loonfs_types::ErrorCode::InvalidRequest.as_str()
        );
        assert_eq!(
            parse_user_path_arg("path", "/", RootPath::Allowed)
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
                RootPath::Refused,
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
                RootPath::Refused,
            )
            .expect("full destination")
            .as_str(),
            "/docs/renamed.pdf"
        );
        assert_eq!(
            destination_user_path(
                "destination_path",
                "source_path",
                "/",
                "report.pdf",
                RootPath::Allowed,
            )
            .expect("into root")
            .as_str(),
            "/report.pdf"
        );
    }
}
