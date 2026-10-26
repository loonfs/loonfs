//! `loonfs mount`, `unmount`, `status`, and `refresh`, which talk to the
//! daemon that serves this user's mounts, and the hidden `loonfs daemon`,
//! which is that daemon.
//!
//! Mounts are available on Linux only. Elsewhere every one of these commands
//! fails at once with that reason.

use super::context::{fail, fail_for, render_target, RootPath};
use super::output::{CommandData, CommandFailure, CommandOutput};
use crate::args::{
    CommandKind, DaemonArgs, MountArgs, RefreshArgs, RuntimeBehavior, StatusArgs, UnmountArgs,
};
use crate::config::{remote_client_config, ProfileConfig};
use crate::error::{CliError, CliErrorCode};
use crate::progress::{ProgressOp, ProgressReporter, Step};
use crate::resolve::{load_cli_config, resolve_actor, resolve_subject};
use loonfs_mount::daemon::protocol::{
    DaemonErrorCode, DaemonFailure, DaemonProfile, DaemonRequest, DaemonResponse, MountOptions,
    MountRequest, MountStep, RefreshRequest, StatusRequest, UnmountRequest, UnmountedReport,
};
use loonfs_mount::daemon::{DaemonClient, DaemonError, DaemonPaths, Kernel};
use loonfs_mount::{MountAccess, MountProgress};
use loonfs_types::{AbsolutePath, ErrorCode, NamespaceId};
use std::os::unix::fs::DirBuilderExt as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

/// How long `loonfs mount` waits for a daemon that it started to listen.
const DAEMON_START_BUDGET_MS: u64 = 10_000;

/// How often `loonfs mount` checks whether a daemon that it started exited.
const DAEMON_START_CHECK_INTERVAL_MS: u64 = 200;

/// The profile mode that every mount uses, because the daemon is a client of
/// a server.
const REMOTE_MODE: &str = "remote";

pub(crate) async fn run_mount(
    kind: CommandKind,
    config_path: &Path,
    args: MountArgs,
    runtime: RuntimeBehavior,
) -> Result<CommandOutput, CommandFailure> {
    let failure = |error| fail(kind, None, None, error);
    kernel().map_err(failure)?;
    if !args.ro {
        return Err(failure(
            CliError::new(
                ErrorCode::NotSupported.as_str(),
                "writable mounts aren't available yet; pass `--ro` to mount read-only",
            )
            .with_param("--ro"),
        ));
    }
    let (namespace_id, path) = parse_target(&args.target).map_err(failure)?;
    let directory = mounted_directory(&args.directory).map_err(failure)?;
    let paths = daemon_paths(args.state.state_dir.as_deref()).map_err(failure)?;
    let (profile_name, profile) = daemon_profile(config_path, &args).map_err(failure)?;
    let failure = |error| fail_for(kind, &profile_name, REMOTE_MODE, error);
    let target = render_target(&namespace_id, &path);
    let request = DaemonRequest::Mount(Box::new(MountRequest {
        directory: directory.clone(),
        namespace_id,
        path,
        access: MountAccess::ReadOnly,
        state_directory: paths.state_directory().to_path_buf(),
        options: MountOptions {
            max_entries: args.max_entries,
            poll_interval_ms: args.poll_interval_ms,
            max_cache_bytes: args.cache_max_bytes,
            min_free_bytes: args.cache_min_free,
            max_pinned_bytes: args.pinned_max_bytes,
            uid: args.uid,
            gid: args.gid,
            sync_timeout_ms: args.sync_timeout_ms,
        },
        profile,
    }));
    let client = connect_or_start(&paths).await.map_err(failure)?;
    let reporter = ProgressReporter::new(runtime, ProgressOp::Mount, target);
    let response = client
        .request(&request, |step| {
            reporter.step(&progress_step(step, &directory))
        })
        .await;
    reporter.clear();
    let response = response.map_err(|error| failure(daemon_error(&error)))?;
    match response {
        DaemonResponse::Mounted(report) => Ok(CommandOutput {
            kind,
            profile: Some(profile_name.clone()),
            mode: Some(REMOTE_MODE.to_owned()),
            data: CommandData::Mounted(report),
        }),
        other => Err(failure(refused(other))),
    }
}

pub(crate) async fn run_unmount(
    kind: CommandKind,
    args: UnmountArgs,
) -> Result<CommandOutput, CommandFailure> {
    let failure = |error| fail(kind, None, None, error);
    let kernel = kernel().map_err(failure)?;
    let directory = mounted_directory(&args.directory).map_err(failure)?;
    let paths = daemon_paths(args.state.state_dir.as_deref()).map_err(failure)?;
    let request = DaemonRequest::Unmount(UnmountRequest {
        directory: directory.clone(),
        force: args.force,
    });
    let response = match connect(&paths).await.map_err(failure)? {
        Some(client) => client
            .request(&request, |_| {})
            .await
            .map_err(|error| failure(daemon_error(&error)))?,
        None => {
            // No daemon runs, so a mount at the directory is one that a
            // daemon which stopped left, and nothing serves it.
            let outcome = kernel.detach_stale(&directory).map_err(|error| {
                failure(CliError::io_error(format!(
                    "detaching `{}` failed: {error}",
                    directory.display()
                )))
            })?;
            DaemonResponse::Unmounted(UnmountedReport {
                state_directory: paths.mount_state_directory(&directory),
                directory,
                outcome: outcome.into(),
            })
        }
    };
    match response {
        DaemonResponse::Unmounted(report) => Ok(output(kind, CommandData::Unmounted(report))),
        other => Err(failure(refused(other))),
    }
}

pub(crate) async fn run_status(
    kind: CommandKind,
    args: StatusArgs,
) -> Result<CommandOutput, CommandFailure> {
    let failure = |error| fail(kind, None, None, error);
    kernel().map_err(failure)?;
    let directory = args
        .directory
        .as_deref()
        .map(mounted_directory)
        .transpose()
        .map_err(failure)?;
    let paths = daemon_paths(args.state.state_dir.as_deref()).map_err(failure)?;
    let Some(client) = connect(&paths).await.map_err(failure)? else {
        return match directory {
            Some(directory) => Err(failure(not_mounted(&directory))),
            None => Ok(output(
                kind,
                CommandData::MountStatuses { mounts: Vec::new() },
            )),
        };
    };
    let request = DaemonRequest::Status(StatusRequest { directory });
    match client
        .request(&request, |_| {})
        .await
        .map_err(|error| failure(daemon_error(&error)))?
    {
        DaemonResponse::Statuses { mounts } => {
            Ok(output(kind, CommandData::MountStatuses { mounts }))
        }
        other => Err(failure(refused(other))),
    }
}

pub(crate) async fn run_refresh(
    kind: CommandKind,
    args: RefreshArgs,
) -> Result<CommandOutput, CommandFailure> {
    let failure = |error| fail(kind, None, None, error);
    kernel().map_err(failure)?;
    let directory = mounted_directory(&args.directory).map_err(failure)?;
    let paths = daemon_paths(args.state.state_dir.as_deref()).map_err(failure)?;
    let Some(client) = connect(&paths).await.map_err(failure)? else {
        return Err(failure(not_mounted(&directory)));
    };
    let request = DaemonRequest::Refresh(RefreshRequest {
        directory: directory.clone(),
        at_least: args.at_least,
    });
    match client
        .request(&request, |_| {})
        .await
        .map_err(|error| failure(daemon_error(&error)))?
    {
        DaemonResponse::Refreshed { applied_seq } => Ok(output(
            kind,
            CommandData::Refreshed {
                directory,
                applied_seq,
            },
        )),
        other => Err(failure(refused(other))),
    }
}

pub(crate) async fn run_daemon(
    kind: CommandKind,
    args: DaemonArgs,
) -> Result<CommandOutput, CommandFailure> {
    let failure = |error| fail(kind, None, None, error);
    let kernel = kernel().map_err(failure)?;
    let paths = daemon_paths(args.state.state_dir.as_deref()).map_err(failure)?;
    if args.stop {
        return stop_daemon(&paths)
            .await
            .map(|()| output(kind, CommandData::DaemonStopped))
            .map_err(failure);
    }
    start_daemon_log();
    let daemon = loonfs_mount::daemon::Daemon::bind(paths, kernel)
        .map_err(|error| failure(daemon_error(&error)))?;
    daemon
        .run()
        .await
        .map_err(|error| failure(daemon_error(&error)))?;
    Ok(output(kind, CommandData::DaemonStopped))
}

/// Sends the running daemon a `shutdown` request, and does nothing when no
/// daemon runs.
async fn stop_daemon(paths: &DaemonPaths) -> Result<(), CliError> {
    let Some(client) = connect(paths).await? else {
        return Ok(());
    };
    match client
        .request(&DaemonRequest::Shutdown, |_| {})
        .await
        .map_err(|error| daemon_error(&error))?
    {
        DaemonResponse::ShuttingDown => Ok(()),
        other => Err(refused(other)),
    }
}

#[cfg(target_os = "linux")]
fn kernel() -> Result<Arc<dyn Kernel>, CliError> {
    Ok(Arc::new(loonfs_fuse::FuseKernel::new()))
}

#[cfg(not(target_os = "linux"))]
fn kernel() -> Result<Arc<dyn Kernel>, CliError> {
    Err(CliError::mounts_unavailable())
}

fn output(kind: CommandKind, data: CommandData) -> CommandOutput {
    CommandOutput {
        kind,
        profile: None,
        mode: None,
        data,
    }
}

/// Parses `<namespace>[/<path>]`.
fn parse_target(target: &str) -> Result<(NamespaceId, AbsolutePath), CliError> {
    let (namespace, path) = match target.split_once('/') {
        Some((namespace, path)) => (namespace, format!("/{path}")),
        None => (target, "/".to_owned()),
    };
    let namespace_id = NamespaceId::parse(namespace)
        .map_err(|error| CliError::invalid_request(error.to_string()).with_param("target"))?;
    let path = super::context::parse_user_path_arg("target", &path, RootPath::Allowed)?;
    Ok((namespace_id, path))
}

/// Returns the absolute, canonical form of a mounted directory.
///
/// A directory whose mount's daemon stopped can't be read (`ENOTCONN`), so
/// its parent is resolved instead, and the directory's name is kept.
fn mounted_directory(directory: &Path) -> Result<PathBuf, CliError> {
    match std::fs::canonicalize(directory) {
        Ok(canonical) if canonical.is_dir() => Ok(canonical),
        Ok(_) => Err(CliError::invalid_request(format!(
            "`{}` is not a directory",
            directory.display()
        ))
        .with_param("directory")),
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {
            let absolute = std::path::absolute(directory)
                .map_err(|error| CliError::io_for_path(directory, error))?;
            let (Some(parent), Some(name)) = (absolute.parent(), absolute.file_name()) else {
                return Err(CliError::io_for_path(directory, error));
            };
            let parent = std::fs::canonicalize(parent)
                .map_err(|error| CliError::io_for_path(parent, error))?;
            Ok(parent.join(name))
        }
        Err(error) => Err(CliError::io_for_path(directory, error)),
    }
}

fn daemon_paths(state_directory: Option<&Path>) -> Result<DaemonPaths, CliError> {
    DaemonPaths::resolve(state_directory).map_err(|error| daemon_error(&error))
}

/// Resolves the profile that the mount acts with: a remote profile's server,
/// token, and certificate authorities, and the actor and subject that the
/// flags, the environment, and the profile set.
fn daemon_profile(
    config_path: &Path,
    args: &MountArgs,
) -> Result<(String, DaemonProfile), CliError> {
    let loaded = load_cli_config(config_path)?;
    let (profile_name, profile) =
        crate::profiles::resolve_profile(&loaded.config, args.profile.profile.as_deref())?;
    let ProfileConfig::Remote {
        server_url,
        auth_token,
        ca_cert_path,
        ..
    } = profile
    else {
        return Err(CliError::invalid_request(format!(
            "profile `{profile_name}` is embedded, and a mount needs a remote profile, because \
             the mount's daemon is a client of a LoonFS server"
        ))
        .with_param("--profile"));
    };
    let client_config = remote_client_config(
        server_url,
        auth_token.as_ref(),
        ca_cert_path.as_deref(),
        false,
    );
    client_config.validate()?;
    let actor_id = resolve_actor(profile, args.actor.actor_id.as_ref())?;
    let subject = resolve_subject(
        profile,
        &actor_id,
        args.subject.subject_id.as_ref(),
        args.subject.principal_scope.as_deref(),
        args.subject.principals.as_deref(),
    )?;
    Ok((
        profile_name.to_owned(),
        DaemonProfile {
            server_url: client_config.server_url,
            auth_token: client_config.auth_token,
            ca_cert_path: client_config.ca_cert_path,
            actor_id,
            subject,
        },
    ))
}

async fn connect(paths: &DaemonPaths) -> Result<Option<DaemonClient>, CliError> {
    DaemonClient::connect(paths.socket())
        .await
        .map_err(|error| daemon_error(&error))
}

/// Connects to the running daemon, or starts one and waits until it listens.
async fn connect_or_start(paths: &DaemonPaths) -> Result<DaemonClient, CliError> {
    if let Some(client) = connect(paths).await? {
        return Ok(client);
    }
    let mut daemon = start_daemon(paths)?;
    for _ in 0..DAEMON_START_BUDGET_MS / DAEMON_START_CHECK_INTERVAL_MS {
        let connected =
            DaemonClient::connect_within(paths.socket(), DAEMON_START_CHECK_INTERVAL_MS)
                .await
                .map_err(|error| daemon_error(&error))?;
        if let Some(client) = connected {
            return Ok(client);
        }
        if let Some(status) = daemon.try_wait().map_err(CliError::io)? {
            // Another `loonfs mount` may have started a daemon first, and
            // this one stopped because that one has the lock.
            return connect(paths).await?.ok_or_else(|| {
                CliError::io_error(format!(
                    "the mount daemon stopped ({status}) before it listened; its log is `{}`",
                    paths.log_file().display()
                ))
            });
        }
    }
    Err(CliError::io_error(format!(
        "the mount daemon didn't listen on `{}` within {} seconds; its log is `{}`",
        paths.socket().display(),
        DAEMON_START_BUDGET_MS / 1_000,
        paths.log_file().display()
    )))
}

/// Starts `loonfs daemon` in a process group of its own, so the daemon
/// outlives this command and a Ctrl-C in the terminal, with its log appended
/// to the state directory's log file.
fn start_daemon(paths: &DaemonPaths) -> Result<Child, CliError> {
    let state_directory = paths.state_directory();
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(state_directory)
        .map_err(|error| CliError::io_for_path(state_directory, error))?;
    let log_path = paths.log_file();
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|error| CliError::io_for_path(&log_path, error))?;
    let executable = std::env::current_exe().map_err(CliError::io)?;
    Command::new(&executable)
        .arg("daemon")
        .arg("--state-dir")
        .arg(state_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .process_group(0)
        .spawn()
        .map_err(|error| CliError::io_for_path(&executable, error))
}

/// Installs the daemon's log, which goes to standard error, and which
/// `loonfs mount` points at the state directory's log file.
fn start_daemon_log() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // A subscriber that's already installed keeps logging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}

/// Returns how a terminal and a `step` event show a step of mounting.
fn progress_step(step: MountStep, directory: &Path) -> Step<'static> {
    let (name, description, entries) = match step {
        MountStep::CheckingKernel => (
            "checking_kernel",
            "checking that the kernel can mount".to_owned(),
            None,
        ),
        MountStep::Engine(MountProgress::CheckingHost) => (
            "checking_host",
            "checking the state directory".to_owned(),
            None,
        ),
        MountStep::Engine(MountProgress::ReadingCapabilities) => (
            "reading_capabilities",
            "reading the deployment's capabilities and the namespace".to_owned(),
            None,
        ),
        MountStep::Engine(MountProgress::CreatingSnapshot) => (
            "creating_snapshot",
            "creating the snapshot that the scan lists".to_owned(),
            None,
        ),
        MountStep::Engine(MountProgress::Scanning {
            listed_entries,
            max_entries,
        }) => (
            "scanning",
            format!("listing the tree: {listed_entries} of at most {max_entries} entries"),
            Some((listed_entries, max_entries)),
        ),
        MountStep::Engine(MountProgress::StartingFeed) => (
            "starting_feed",
            "starting to follow the change feed".to_owned(),
            None,
        ),
        MountStep::Mounting => (
            "mounting",
            format!("mounting at {}", directory.display()),
            None,
        ),
    };
    Step {
        name,
        description,
        entries,
    }
}

fn not_mounted(directory: &Path) -> CliError {
    CliError::new(
        DaemonErrorCode::NotMounted.as_str(),
        format!("`{}` has no mount", directory.display()),
    )
}

/// Reports a failure to reach the daemon, or a response that doesn't decode.
fn daemon_error(error: &DaemonError) -> CliError {
    CliError::new(CliErrorCode::DaemonFailed.as_str(), error.to_string())
}

/// Reports a response other than the request's result: the daemon's
/// refusal, or a response that belongs to another request.
fn refused(response: DaemonResponse) -> CliError {
    match response {
        DaemonResponse::Error(DaemonFailure { code, message }) => {
            CliError::new(code.as_str(), message)
        }
        other => daemon_error(&DaemonError::Protocol {
            message: format!("the daemon sent an unexpected response: {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Checks that a target identifies a namespace and, after its first slash, a
    /// directory in it.
    #[test]
    fn mount_targets_name_a_namespace_and_an_optional_path() {
        let parsed = |target| {
            parse_target(target).map(|(namespace, path)| (namespace.to_string(), path.to_string()))
        };
        assert_eq!(
            parsed("demo").expect("a target"),
            ("demo".to_owned(), "/".to_owned())
        );
        assert_eq!(
            parsed("demo/").expect("a target"),
            ("demo".to_owned(), "/".to_owned())
        );
        assert_eq!(
            parsed("demo/work/project").expect("a target"),
            ("demo".to_owned(), "/work/project".to_owned())
        );
        for invalid in ["", "/work", "demo//work", "Demo Space/work"] {
            let error = parsed(invalid).expect_err("an invalid target");
            assert_eq!(
                error.code,
                ErrorCode::InvalidRequest.as_str(),
                "{invalid:?}"
            );
            assert_eq!(error.param.as_deref(), Some("target"), "{invalid:?}");
        }
    }
}
