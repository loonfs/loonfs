//! The arguments of `loonfs mount`, `unmount`, `status`, `refresh`, and the
//! hidden `daemon` command.

#![cfg_attr(
    not(unix),
    allow(
        dead_code,
        reason = "mounts need Unix, so elsewhere the commands fail before reading their arguments"
    )
)]

use super::{parse_change_seq, ActorSelectorArgs, ProfileSelectorArgs, SubjectSelectorArgs};
use clap::{Args, ValueHint};
use loonfs_types::ChangeSeq;
use std::path::PathBuf;

#[derive(Debug, Args)]
#[command(
    override_usage = "loonfs mount <NAMESPACE[/PATH]> <DIRECTORY> --ro [OPTIONS]",
    after_help = "Mounts are available on Linux only, and this version mounts read-only."
)]
pub(crate) struct MountArgs {
    #[command(flatten)]
    pub profile: ProfileSelectorArgs,
    #[command(flatten)]
    pub subject: SubjectSelectorArgs,
    #[command(flatten)]
    pub actor: ActorSelectorArgs,
    /// The namespace to mount, and optionally a directory in it, such as
    /// `demo` or `demo/work/project`.
    #[arg(value_name = "NAMESPACE[/PATH]", value_hint = ValueHint::Other)]
    pub target: String,
    /// The local directory to mount it on.
    #[arg(value_hint = ValueHint::DirPath)]
    pub directory: PathBuf,
    /// Mount read-only. Writable mounts aren't available yet, so this is
    /// required.
    #[arg(long)]
    pub ro: bool,
    /// The most entries that the mounted tree can have when it's mounted
    /// [default: 100000].
    #[arg(long, value_name = "N")]
    pub max_entries: Option<u64>,
    /// How often the mount reads the namespace's change feed, in
    /// milliseconds [default: 500].
    #[arg(long, value_name = "MS")]
    pub poll_interval_ms: Option<u64>,
    /// The bytes of cached content that nothing uses that the mount keeps
    /// before it evicts the least recently used [default: 8 GiB].
    #[arg(long, value_name = "BYTES")]
    pub cache_max_bytes: Option<u64>,
    /// The free bytes that the mount keeps on the state directory's
    /// filesystem [default: 2 GiB].
    #[arg(long, value_name = "BYTES")]
    pub cache_min_free: Option<u64>,
    /// The most local bytes that the mount can't evict, such as files that
    /// are open [default: 16 GiB].
    #[arg(long, value_name = "BYTES")]
    pub pinned_max_bytes: Option<u64>,
    /// The owner that every item shows [default: the user that runs the
    /// daemon].
    #[arg(long, value_name = "UID")]
    pub uid: Option<u32>,
    /// The group that every item shows [default: the group of the user that
    /// runs the daemon].
    #[arg(long, value_name = "GID")]
    pub gid: Option<u32>,
    /// How long a `loonfs refresh` of the mount waits for the sequence it
    /// asks for, in milliseconds [default: 60000].
    #[arg(long, value_name = "MS")]
    pub sync_timeout_ms: Option<u64>,
    #[command(flatten)]
    pub state: StateDirectoryArgs,
}

#[derive(Debug, Args)]
pub(crate) struct UnmountArgs {
    /// The mounted directory.
    #[arg(value_hint = ValueHint::DirPath)]
    pub directory: PathBuf,
    /// Keep the mount's state on disk, so a later `loonfs mount` of the
    /// directory resumes it.
    #[arg(long)]
    pub force: bool,
    #[command(flatten)]
    pub state: StateDirectoryArgs,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// The mounted directory to report. Every mount is reported when it's
    /// left out.
    #[arg(value_hint = ValueHint::DirPath)]
    pub directory: Option<PathBuf>,
    #[command(flatten)]
    pub state: StateDirectoryArgs,
}

#[derive(Debug, Args)]
pub(crate) struct RefreshArgs {
    /// The mounted directory.
    #[arg(value_hint = ValueHint::DirPath)]
    pub directory: PathBuf,
    /// Wait until the mount has applied the commit at this sequence, such as
    /// one that another host printed.
    #[arg(long, value_name = "SEQ", value_parser = parse_change_seq)]
    pub at_least: Option<ChangeSeq>,
    #[command(flatten)]
    pub state: StateDirectoryArgs,
}

#[derive(Debug, Args)]
pub(crate) struct DaemonArgs {
    /// Stop the running daemon instead: detach every mount, keeping its
    /// state for a later `loonfs mount`, and wait until the daemon replies.
    #[arg(long)]
    pub stop: bool,
    #[command(flatten)]
    pub state: StateDirectoryArgs,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StateDirectoryArgs {
    /// Where the daemon keeps each mount's state and cache. The daemon's
    /// socket is in it too when `XDG_RUNTIME_DIR` isn't set [default:
    /// `$XDG_STATE_HOME/loonfs`, or `~/.local/state/loonfs`].
    #[arg(long, value_name = "PATH", value_hint = ValueHint::DirPath)]
    pub state_dir: Option<PathBuf>,
}
