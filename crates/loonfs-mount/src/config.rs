//! The settings of one mount, with the defaults that `loonfs mount` uses.

use crate::error::{MountError, Result};
use crate::fetch::MAX_FETCH_BYTES_IN_FLIGHT;
use loonfs_types::{AbsolutePath, NamespaceId};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Whether a mount accepts changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountAccess {
    /// The mount shows the namespace and accepts no changes (`--ro`).
    ReadOnly,
    /// The mount publishes local changes to the namespace.
    Writable,
}

/// How the engine resolves a local change that conflicts with a remote one
/// (`--conflict`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicy {
    /// The remote change stays, and the local version is published beside it.
    #[default]
    KeepBoth,
    /// The local change is published on top of the remote one.
    Overwrite,
    /// The local change isn't published, and its bytes go to the control
    /// directory.
    Refuse,
}

/// Construction-time configuration of one mount.
///
/// The database records every field, so a mount that resumes after the
/// daemon restarts keeps the settings it was admitted with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountConfig {
    /// The namespace to mount.
    pub namespace_id: NamespaceId,
    /// The directory in the namespace that the mount shows.
    pub path: AbsolutePath,
    /// Whether the mount accepts changes.
    pub access: MountAccess,
    /// The mount's own state directory, which contains `tree.db` and the
    /// `cache`, `staging`, and `captures` directories.
    pub state_directory: PathBuf,
    /// The entry budget that admission checks (`--max-entries`).
    pub max_entries: u64,
    /// How often the planner publishes changes (`--publish-interval-ms`).
    pub publish_interval_ms: u64,
    /// How often the planner publishes files whose only change is an append
    /// (`--append-interval-ms`).
    pub append_interval_ms: u64,
    /// How often the engine reads the change feed (`--poll-interval-ms`).
    pub poll_interval_ms: u64,
    /// The conflict policy (`--conflict`).
    pub conflict: ConflictPolicy,
    /// The eviction target for cached content that nothing uses
    /// (`--cache-max-bytes`).
    pub max_cache_bytes: u64,
    /// The free space that the mount keeps on the state directory's
    /// filesystem (`--cache-min-free`).
    pub min_free_bytes: u64,
    /// The hard limit on local bytes that the engine can't evict
    /// (`--pinned-max-bytes`).
    pub max_pinned_bytes: u64,
    /// The owner that every item shows (`--uid`), or `None` for the user that
    /// runs the daemon.
    pub uid: Option<u32>,
    /// The group that every item shows (`--gid`), or `None` for the group of
    /// the user that runs the daemon.
    pub gid: Option<u32>,
    /// How long a barrier waits for publication before it fails
    /// (`--sync-timeout-ms`).
    pub sync_timeout_ms: u64,
    /// The largest file that the first read fetches whole.
    pub max_whole_file_bytes: u64,
    /// The first read window of a larger file, and the window after a seek.
    pub first_window_bytes: u64,
    /// The largest window that sequential reads grow to.
    pub max_window_bytes: u64,
    /// The largest fetch that's a ranged read through the server. A larger
    /// fetch uses a download grant when the deployment advertises grants.
    pub max_ranged_read_bytes: u64,
}

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

/// The name of the mount's control directory at its root, which the mount
/// reserves.
pub(crate) const CONTROL_DIRECTORY_NAME: &str = ".loonfs";

impl MountConfig {
    /// Returns the configuration of a mount of `path` in `namespace_id`, with
    /// the default of every option.
    pub fn new(
        namespace_id: NamespaceId,
        path: AbsolutePath,
        access: MountAccess,
        state_directory: PathBuf,
    ) -> Self {
        Self {
            namespace_id,
            path,
            access,
            state_directory,
            max_entries: 100_000,
            publish_interval_ms: 1_000,
            append_interval_ms: 1_000,
            poll_interval_ms: 500,
            conflict: ConflictPolicy::KeepBoth,
            max_cache_bytes: 8 * GIB,
            min_free_bytes: 2 * GIB,
            max_pinned_bytes: 16 * GIB,
            uid: None,
            gid: None,
            sync_timeout_ms: 60_000,
            max_whole_file_bytes: 8 * MIB,
            first_window_bytes: MIB,
            max_window_bytes: 64 * MIB,
            max_ranged_read_bytes: 8 * MIB,
        }
    }

    /// Checks the fetch sizes: each one fetch fits the shared limit on
    /// fetches in flight, so a fetch never waits for room that can't exist,
    /// and the first window is at most the largest one.
    pub(crate) fn validate(&self) -> Result<()> {
        let max_fetch_bytes = MAX_FETCH_BYTES_IN_FLIGHT;
        for (setting, value) in [
            ("max_whole_file_bytes", self.max_whole_file_bytes),
            ("first_window_bytes", self.first_window_bytes),
            ("max_window_bytes", self.max_window_bytes),
            ("max_ranged_read_bytes", self.max_ranged_read_bytes),
        ] {
            if value == 0 || value > max_fetch_bytes {
                return Err(MountError::InvalidSetting {
                    setting,
                    reason: format!("{value} isn't between 1 and {max_fetch_bytes}"),
                });
            }
        }
        if self.first_window_bytes > self.max_window_bytes {
            return Err(MountError::InvalidSetting {
                setting: "first_window_bytes",
                reason: format!(
                    "{} is larger than `max_window_bytes` ({})",
                    self.first_window_bytes, self.max_window_bytes
                ),
            });
        }
        Ok(())
    }

    pub(crate) fn database_path(&self) -> PathBuf {
        self.state_directory.join("tree.db")
    }

    pub(crate) fn cache_directory(&self) -> PathBuf {
        self.state_directory.join("cache")
    }
}
