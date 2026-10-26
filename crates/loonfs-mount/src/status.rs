//! The report that `loonfs status` prints for a mount.

use crate::config::MountAccess;
use loonfs_types::{AbsolutePath, ChangeSeq, CommitId, InodeId, NamespaceId};
use serde::{Deserialize, Serialize};

/// The state of one mount, as `loonfs status` and `.loonfs/status.json`
/// report it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    /// The mounted namespace.
    pub namespace_id: NamespaceId,
    /// The mounted directory in the namespace.
    pub path: AbsolutePath,
    /// Whether the mount accepts changes.
    pub access: MountAccess,
    /// Whether the engine serves its trees, or is listing the namespace again.
    pub readiness: Readiness,
    /// How the adapter mounted the directory, once it has.
    pub mount_method: Option<MountMethod>,
    /// The sequence of the last commit whose changes are in the mount's trees.
    pub applied_seq: ChangeSeq,
    /// The number of items with unpublished changes.
    pub dirty_items: u64,
    /// How long ago the oldest unpublished change was made, in milliseconds.
    pub oldest_unpublished_change_age_ms: Option<u64>,
    /// The commit that the mount has in flight.
    pub pending_batch: Option<PendingBatchStatus>,
    /// The number of capture uploads in flight.
    pub uploads_in_flight: u64,
    /// The content cache's bytes and limits.
    pub cache: CacheStatus,
    /// The number of items in the mount's trees.
    pub entries: u64,
    /// The entry budget that admission checked.
    pub max_entries: u64,
    /// Whether the state directory's filesystem supports reflinks.
    pub reflinks: ReflinkSupport,
    /// The largest file that a commit sends inline, or zero when the
    /// deployment doesn't advertise inline content.
    pub inline_limit_bytes: u64,
    /// The transfers that the deployment advertises.
    pub transports: Transports,
    /// Conflicts that the conflict policy resolved.
    pub conflicts: Vec<ConflictReport>,
    /// Changes that the mount didn't publish, whose bytes are in the control
    /// directory.
    pub refused_publications: Vec<ConflictReport>,
    /// Why the mount stopped publishing, when it has.
    pub publication_stop: Option<PublicationStop>,
    /// Conditions that the mount works through, such as a tree that grew past
    /// twice the budget.
    pub warnings: Vec<Warning>,
    /// The last error that a feed poll returned, until a poll succeeds.
    pub feed_error: Option<String>,
    /// The kernel invalidations that the engine dropped because the adapter
    /// read them too slowly.
    pub dropped_invalidations: u64,
}

/// Whether the engine serves its trees.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    /// The trees are current up to the applied sequence.
    Ready,
    /// The feed passed the retention floor, and the engine is listing the
    /// tree again at a new snapshot.
    Rebuilding,
}

/// How the adapter mounted the directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountMethod {
    /// `mount(2)`, as root.
    MountSyscall,
    /// The `fusermount3` helper.
    Fusermount3,
}

/// Whether the state directory's filesystem can clone a file's extents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReflinkSupport {
    /// `FICLONE` works, so a capture or a working file is a reflink.
    Supported,
    /// `FICLONE` doesn't work, so the engine copies.
    Unsupported,
}

/// The transfers that a deployment advertises.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transports {
    /// `filesystem.uploads.direct_put`.
    pub direct_put: bool,
    /// `filesystem.uploads.direct_multipart`.
    pub direct_multipart: bool,
    /// `filesystem.downloads.direct_get`.
    pub direct_get: bool,
}

/// The content cache's bytes and limits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStatus {
    /// Complete cache files that no descriptor uses.
    pub evictable_bytes: u64,
    /// Partial cache files and cache files with open descriptors.
    pub pinned_bytes: u64,
    /// The eviction target for evictable bytes.
    pub max_cache_bytes: u64,
    /// The hard limit on pinned bytes.
    pub max_pinned_bytes: u64,
    /// Cache files that didn't match their checksum.
    pub mismatched_files: u64,
}

/// The commit that a mount has in flight.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingBatchStatus {
    /// The commit's ID.
    pub commit_id: CommitId,
    /// How many times the engine sent the commit.
    pub attempts: u64,
}

/// One conflict or refused publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictReport {
    /// The item's path in the namespace.
    pub path: String,
    /// The remote commit that the local change conflicted with.
    pub remote_seq: ChangeSeq,
    /// Why the change conflicted.
    pub reason: String,
}

/// Why a mount stopped publishing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PublicationStop {
    /// The feed deleted the mount's root, or a directory above it.
    RootDeleted {
        /// The deleted directory: the root, or one of its ancestors.
        inode_id: InodeId,
        /// The commit that deleted it.
        deleted_seq: ChangeSeq,
    },
    /// The namespace was deleted.
    NamespaceDeleted,
}

/// A condition that a mount reports and works through.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Warning {
    /// The tree has more than twice the budget's entries.
    EntriesOverBudget {
        /// The items in the tree.
        entries: u64,
        /// The entry budget.
        max_entries: u64,
    },
    /// Another client created `.loonfs` at the mount's root, which the mount
    /// doesn't show.
    ReservedName {
        /// The hidden entry's inode.
        inode_id: InodeId,
    },
}
