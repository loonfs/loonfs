//! The messages between `loonfs` commands and the daemon, one JSON object per
//! line on the daemon's socket.
//!
//! A command connects, sends one request, and reads response lines until a
//! final one. A `mount` request's progress lines come before its result.

use super::kernel::StaleMountOutcome;
use crate::config::{MountAccess, MountConfig};
use crate::progress::MountProgress;
use crate::status::Status;
use loonfs_types::{AbsolutePath, ActorId, ChangeSeq, NamespaceId, SecretString, Subject};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// The longest request line that the daemon reads. A `mount` request, the
/// largest, is a few hundred bytes plus the profile's token.
pub(crate) const MAX_REQUEST_BYTES: usize = 64 << 10;

/// The longest response line that a command reads. A status report is a few
/// kilobytes for each mount.
pub(crate) const MAX_RESPONSE_BYTES: usize = 16 << 20;

/// One request to the daemon.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DaemonRequest {
    /// Admits a mount and mounts its directory, or confirms that the
    /// directory already has the same mount. Boxed, because it's much larger
    /// than the other requests.
    Mount(Box<MountRequest>),
    /// Unmounts a directory.
    Unmount(UnmountRequest),
    /// Waits until every change in a mount is published.
    Sync(DirectoryRequest),
    /// Reads a mount's change feed now, and waits for a sequence.
    Refresh(RefreshRequest),
    /// Reports the state of one mount, or of every mount.
    Status(StatusRequest),
    /// Lists the daemon's mounts.
    List,
    /// Detaches every mount, keeping their state, and stops the daemon.
    Shutdown,
}

impl DaemonRequest {
    /// The `op` of every request, which tells an unknown operation from a
    /// request that doesn't decode.
    const OPERATIONS: [&'static str; 7] = [
        "mount", "unmount", "sync", "refresh", "status", "list", "shutdown",
    ];
}

/// A request to mount `namespace_id` at `path` on `directory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountRequest {
    /// The absolute host directory to mount on.
    pub directory: PathBuf,
    /// The namespace to mount.
    pub namespace_id: NamespaceId,
    /// The directory in the namespace that the mount shows.
    pub path: AbsolutePath,
    /// Whether the mount accepts changes.
    pub access: MountAccess,
    /// The state directory that the command resolved, which has to be the
    /// daemon's.
    pub state_directory: PathBuf,
    /// The settings that the command line set.
    pub options: MountOptions,
    /// The credentials and identity that the mount acts with.
    pub profile: DaemonProfile,
}

/// The settings of a mount that a command can set. A setting left out keeps
/// the default of [`MountConfig::new`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MountOptions {
    /// `--max-entries`.
    pub max_entries: Option<u64>,
    /// `--poll-interval-ms`.
    pub poll_interval_ms: Option<u64>,
    /// `--cache-max-bytes`.
    pub max_cache_bytes: Option<u64>,
    /// `--cache-min-free`.
    pub min_free_bytes: Option<u64>,
    /// `--pinned-max-bytes`.
    pub max_pinned_bytes: Option<u64>,
    /// `--uid`.
    pub uid: Option<u32>,
    /// `--gid`.
    pub gid: Option<u32>,
    /// `--sync-timeout-ms`, which also bounds how long `refresh` waits.
    pub sync_timeout_ms: Option<u64>,
}

impl MountOptions {
    /// Writes the settings that are set into `config`.
    pub(crate) fn apply(&self, config: &mut MountConfig) {
        let Self {
            max_entries,
            poll_interval_ms,
            max_cache_bytes,
            min_free_bytes,
            max_pinned_bytes,
            uid,
            gid,
            sync_timeout_ms,
        } = self.clone();
        config.max_entries = max_entries.unwrap_or(config.max_entries);
        config.poll_interval_ms = poll_interval_ms.unwrap_or(config.poll_interval_ms);
        config.max_cache_bytes = max_cache_bytes.unwrap_or(config.max_cache_bytes);
        config.min_free_bytes = min_free_bytes.unwrap_or(config.min_free_bytes);
        config.max_pinned_bytes = max_pinned_bytes.unwrap_or(config.max_pinned_bytes);
        config.uid = uid.or(config.uid);
        config.gid = gid.or(config.gid);
        config.sync_timeout_ms = sync_timeout_ms.unwrap_or(config.sync_timeout_ms);
    }
}

/// The resolved profile that a mount acts with, which the daemon keeps in
/// memory only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonProfile {
    /// The server's base URL.
    pub server_url: String,
    /// The bearer token, which `Debug` redacts.
    pub auth_token: Option<SecretString>,
    /// A PEM bundle of extra certificate authorities to trust.
    pub ca_cert_path: Option<String>,
    /// The actor that the mount's commits record.
    pub actor_id: ActorId,
    /// The subject that the mount's requests act as, or `None` for the token
    /// holder.
    pub subject: Option<Subject>,
}

/// A request to unmount `directory`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnmountRequest {
    /// The mounted directory.
    pub directory: PathBuf,
    /// Whether to keep the mount's state on disk for a later `loonfs mount`,
    /// as `--force` does.
    pub force: bool,
}

/// A request about one mounted directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryRequest {
    /// The mounted directory.
    pub directory: PathBuf,
}

/// A request to read a mount's change feed now.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshRequest {
    /// The mounted directory.
    pub directory: PathBuf,
    /// The sequence to wait for, or `None` to wait for one poll.
    pub at_least: Option<ChangeSeq>,
}

/// A request for the state of one mount, or of every mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatusRequest {
    /// The mounted directory, or `None` for every mount.
    pub directory: Option<PathBuf>,
}

/// One line that the daemon sends in response to a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DaemonResponse {
    /// A step of a mount's admission, sent before the mount's result.
    Progress {
        /// The step that started.
        step: MountStep,
    },
    /// The result of a `mount` request. Boxed, because it's much larger than
    /// the other responses.
    Mounted(Box<MountedReport>),
    /// The result of an `unmount` request.
    Unmounted(UnmountedReport),
    /// The result of a `refresh` request.
    Refreshed {
        /// The mount's applied sequence after the refresh.
        applied_seq: ChangeSeq,
    },
    /// The result of a `status` request.
    Statuses {
        /// One report for each ready mount.
        mounts: Vec<MountStatusReport>,
    },
    /// The result of a `list` request.
    Mounts {
        /// One listing for each mount that the daemon has.
        mounts: Vec<MountListing>,
    },
    /// The result of a `shutdown` request.
    ShuttingDown,
    /// The request failed.
    Error(DaemonFailure),
}

/// A step of admitting and mounting.
///
/// A command skips a progress line that doesn't decode, so a newer daemon
/// can add steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountStep {
    /// Checking that the kernel can mount on this host, and detaching a
    /// mount that a stopped daemon left at the directory.
    CheckingKernel,
    /// A step of the engine's admission, or of resuming the state that an
    /// earlier mount left, such as the entries that the scan has listed.
    Engine(MountProgress),
    /// Mounting the directory.
    Mounting,
}

/// How a `mount` request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountOutcome {
    /// The daemon mounted the directory.
    Mounted,
    /// The directory already had the same mount.
    AlreadyMounted,
}

/// The result of a `mount` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedReport {
    /// How the request ended.
    pub outcome: MountOutcome,
    /// The mount's directory and state.
    pub mount: MountStatusReport,
}

/// What happened to a mount's state on disk when it was unmounted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateDisposal {
    /// The state was removed.
    Removed,
    /// The state stays for a later `loonfs mount`, as `--force` asked.
    Kept,
    /// The directory is detached, but programs still have files open in it,
    /// so the state is removed once they close them.
    RemovedAfterRelease,
}

/// How an `unmount` request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UnmountOutcome {
    /// The daemon unmounted the directory.
    Unmounted {
        /// What happened to the mount's state.
        state: StateDisposal,
    },
    /// The daemon had no mount at the directory, but the directory had a
    /// mount that a stopped daemon left, which is detached now.
    DetachedStale,
    /// Neither the daemon nor a stopped daemon had a mount at the
    /// directory, so nothing changed.
    NotMounted,
}

impl From<StaleMountOutcome> for UnmountOutcome {
    /// Returns how an `unmount` of a directory without a mount in the
    /// daemon ended.
    fn from(outcome: StaleMountOutcome) -> Self {
        match outcome {
            StaleMountOutcome::Detached => Self::DetachedStale,
            StaleMountOutcome::NotMounted => Self::NotMounted,
        }
    }
}

/// The result of an `unmount` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnmountedReport {
    /// The directory.
    pub directory: PathBuf,
    /// How the request ended.
    pub outcome: UnmountOutcome,
    /// The mount's state directory.
    pub state_directory: PathBuf,
}

/// The state of one mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountStatusReport {
    /// The mounted directory.
    pub directory: PathBuf,
    /// The engine's report.
    pub status: Status,
}

/// One mount in a `list` response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountListing {
    /// The mounted directory.
    pub directory: PathBuf,
    /// The mounted namespace.
    pub namespace_id: NamespaceId,
    /// The mounted directory in the namespace.
    pub path: AbsolutePath,
    /// Where the mount is in its lifecycle.
    pub phase: MountPhase,
}

/// Where a mount is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountPhase {
    /// A `mount` request is admitting it.
    Starting,
    /// The directory is mounted.
    Ready,
    /// The directory is detached, and the daemon waits for programs to close
    /// their files in it.
    Releasing,
}

/// Why the daemon refused a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonFailure {
    /// What kind of failure it is.
    pub code: DaemonErrorCode,
    /// The reason, in lowercase.
    pub message: String,
}

impl DaemonFailure {
    pub(crate) fn new(code: DaemonErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// The kinds of failure that the daemon reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonErrorCode {
    /// The request's `op` isn't one that this daemon knows.
    UnknownOperation,
    /// The request didn't decode, or its values are invalid.
    InvalidRequest,
    /// The daemon has no mount at the directory.
    NotMounted,
    /// The directory has another mount, or one that is starting or
    /// releasing.
    DirectoryBusy,
    /// The request refers to another state directory than the daemon's.
    StateDirectoryMismatch,
    /// The host, the namespace, or the kernel refused the mount.
    MountFailed,
    /// The kernel refused to detach the mount.
    UnmountFailed,
    /// The engine failed the request.
    EngineFailed,
    /// A refresh didn't see the mount apply the sequence within the mount's
    /// sync timeout.
    RefreshTimedOut,
    /// A refresh waited for a sequence past the namespace's head.
    SequenceNotCommitted,
    /// The operation isn't available yet.
    NotSupported,
    /// The daemon is stopping.
    ShuttingDown,
    /// A failure that a newer daemon reports.
    #[serde(other)]
    Unknown,
}

impl DaemonErrorCode {
    /// Returns the code's wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnknownOperation => "unknown_operation",
            Self::InvalidRequest => "invalid_request",
            Self::NotMounted => "not_mounted",
            Self::DirectoryBusy => "directory_busy",
            Self::StateDirectoryMismatch => "state_directory_mismatch",
            Self::MountFailed => "mount_failed",
            Self::UnmountFailed => "unmount_failed",
            Self::EngineFailed => "engine_failed",
            Self::RefreshTimedOut => "refresh_timed_out",
            Self::SequenceNotCommitted => "sequence_not_committed",
            Self::NotSupported => "not_supported",
            Self::ShuttingDown => "shutting_down",
            Self::Unknown => "unknown",
        }
    }
}

/// Encodes a message as one line, ending in a newline.
pub fn encode_line<T: Serialize>(message: &T) -> Vec<u8> {
    let mut line = serde_json::to_vec(message).expect("a daemon message should always encode");
    line.push(b'\n');
    line
}

/// Decodes a request line, and reports an `op` that no request has as an
/// unknown operation.
pub fn decode_request(line: &[u8]) -> Result<DaemonRequest, DaemonFailure> {
    serde_json::from_slice(line).map_err(|error| {
        let operation = serde_json::from_slice::<serde_json::Value>(line)
            .ok()
            .and_then(|value| value.get("op")?.as_str().map(ToOwned::to_owned));
        match operation {
            Some(operation) if !DaemonRequest::OPERATIONS.contains(&operation.as_str()) => {
                DaemonFailure::new(
                    DaemonErrorCode::UnknownOperation,
                    format!("unknown daemon operation {operation:?}"),
                )
            }
            _ => DaemonFailure::new(
                DaemonErrorCode::InvalidRequest,
                format!("invalid daemon request: {error}"),
            ),
        }
    })
}

/// Decodes a response line.
pub fn decode_response(line: &[u8]) -> serde_json::Result<DaemonResponse> {
    serde_json::from_slice(line)
}

/// Returns whether a response line is a progress line, whether or not its
/// step decodes.
pub(crate) fn is_progress_line(line: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(line)
        .is_ok_and(|value| value.get("kind").and_then(|kind| kind.as_str()) == Some("progress"))
}

#[cfg(test)]
mod tests;
