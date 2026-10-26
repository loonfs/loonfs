//! The steps of mounting that [`Engine::mount`](crate::Engine::mount)
//! reports, which the daemon forwards to `loonfs mount`.

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

/// A step of mounting, reported as the engine reaches it.
///
/// Admitting a new mount reports every step in order, and `Scanning` once
/// when the scan starts and again after each listing page that fits the
/// budget. Resuming the state that an earlier mount left reports
/// `CheckingHost` and `StartingFeed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum MountProgress {
    /// Checking the state directory's filesystem: its free space, and
    /// whether it supports reflinks.
    CheckingHost,
    /// Reading the deployment's capabilities and the namespace's settings.
    ReadingCapabilities,
    /// Creating the snapshot that the scan lists the tree at.
    CreatingSnapshot,
    /// Listing the mounted tree at the snapshot.
    Scanning {
        /// The entries listed so far.
        listed_entries: u64,
        /// The entry budget, past which admission refuses the mount.
        max_entries: u64,
    },
    /// Starting to follow the namespace's change feed.
    StartingFeed,
}

/// Where a mount reports its progress, if anywhere.
#[derive(Debug)]
pub(crate) struct Progress(Option<mpsc::UnboundedSender<MountProgress>>);

impl Progress {
    pub(crate) fn new(sender: Option<mpsc::UnboundedSender<MountProgress>>) -> Self {
        Self(sender)
    }

    pub(crate) fn report(&self, step: MountProgress) {
        if let Some(sender) = &self.0 {
            // A caller that stopped listening doesn't stop the mount.
            let _ = sender.send(step);
        }
    }
}
