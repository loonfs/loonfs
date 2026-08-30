//! Per-operation options shared by the embedded runtime and HTTP client.
//!
//! There is one type per operation, even where two of them currently hold the
//! same fields: options follow the operation they parameterize, so a guard
//! added to one is not silently offered on the others.
//!
//! These are plain in-process argument structs, not wire shapes: nothing here
//! serializes. The request bodies that do cross the wire live in
//! [`crate::v0`], and each surface resolves these options into one. A read's
//! options reach the wire as query parameters the surface builds from them.

use crate::{
    ActorRef, AttributeKey, AttributeRevisionNo, AttributeValue, CheckpointId, CommitId,
    DeleteDirectoryBehavior, DestinationBehavior, InodeId, RevisionNo,
};
use std::collections::BTreeMap;

/// Commit settings shared by every filesystem mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOptions {
    /// Actor responsible for the commit, as supplied by the application.
    pub actor: ActorRef,
    /// Optional idempotency key. LoonFS generates one when this is `None`.
    pub commit_id: Option<CommitId>,
    /// Optional commit message. Changing it changes the commit identity.
    pub message: Option<String>,
}

impl CommitOptions {
    /// Creates settings with no commit ID or message.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            actor,
            commit_id: None,
            message: None,
        }
    }
}

/// Options for stating one path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatPathOptions {
    /// Project the inode's attribute map and its revision onto the answer.
    ///
    /// Defaults to on. A stat answers for one path, and an attribute map is
    /// capped at 64 KiB, so the cost of including it is bounded by the
    /// request.
    pub include_attributes: bool,
    /// Read a path from this snapshot. Inode lookups do not support snapshots.
    pub snapshot_id: Option<CheckpointId>,
}

impl Default for StatPathOptions {
    fn default() -> Self {
        Self {
            include_attributes: true,
            snapshot_id: None,
        }
    }
}

/// Options for listing a directory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListPathEntriesOptions {
    /// Project each entry's attribute map and its revision onto the answer.
    ///
    /// Defaults to off, and that default is what bounds a listing: a page
    /// holds up to 1,000 entries and each attribute map may be 64 KiB, so an
    /// always-on projection would put a 64 MiB response behind a request that
    /// declares no byte budget anywhere. A caller that wants attributes for a
    /// whole directory asks for them, and pages accordingly.
    pub include_attributes: bool,
    /// Read the directory from this snapshot.
    pub snapshot_id: Option<CheckpointId>,
}

/// Options for listing a directory's children by parent inode.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListInodeChildrenOptions {
    /// Project each entry's attribute map and its revision onto the answer.
    ///
    /// Defaults to off for the same reason as
    /// [`ListPathEntriesOptions::include_attributes`].
    pub include_attributes: bool,
}

/// Options for writing and removing an inode's attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAttributesOptions {
    /// Attributes to write. Each key replaces whatever the inode holds under
    /// it; keys the inode holds and this map does not name are left alone.
    pub set: BTreeMap<AttributeKey, AttributeValue>,
    /// Keys to remove.
    pub remove: Vec<AttributeKey>,
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// When set, the update applies only while the path still resolves to
    /// this inode, so a raced rebinding fails instead of writing attributes
    /// onto the wrong inode.
    pub expected_inode_id: Option<InodeId>,
    /// When set, the update applies only while the inode's attribute revision
    /// is still this one. Every update carries its own revision guard either
    /// way, so a concurrent update never merges silently.
    pub expected_attributes_revision_no: Option<AttributeRevisionNo>,
}

impl UpdateAttributesOptions {
    /// Creates an empty attribute update for this actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            set: BTreeMap::new(),
            remove: Vec::new(),
            commit: CommitOptions::new(actor),
            expected_inode_id: None,
            expected_attributes_revision_no: None,
        }
    }
}

/// Options for writing a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    /// Create-only or replace-existing behavior.
    pub behavior: DestinationBehavior,
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// Replace only if the path still points to this inode.
    /// Requires `Replace` behavior.
    pub expected_inode_id: Option<InodeId>,
    /// Replace only if the file still has this revision.
    /// Requires `Replace` behavior.
    pub expected_revision_no: Option<RevisionNo>,
}

impl PutFileOptions {
    /// Creates options that refuse to replace an existing file.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
            expected_inode_id: None,
            expected_revision_no: None,
        }
    }
}

/// Options for creating a directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDirectoryOptions {
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// Also create missing ancestor directories, like `put_file` does.
    pub parents: bool,
}

impl CreateDirectoryOptions {
    /// Creates options that do not create missing parent directories.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            commit: CommitOptions::new(actor),
            parents: false,
        }
    }
}

/// Options for deleting a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Directory delete behavior.
    pub behavior: DeleteDirectoryBehavior,
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// When set, the delete applies only while the path still resolves to
    /// this inode, so a raced rebinding fails instead of deleting the wrong
    /// inode.
    pub expected_inode_id: Option<InodeId>,
}

impl DeleteOptions {
    /// Creates options for a non-recursive delete.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DeleteDirectoryBehavior::NonRecursive,
            commit: CommitOptions::new(actor),
            expected_inode_id: None,
        }
    }
}

/// Options for moving a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveOptions {
    /// Create-only or replace-existing behavior for the destination.
    pub behavior: DestinationBehavior,
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// Replace only if the destination still points to this inode.
    /// Requires `Replace` behavior.
    pub destination_expected_inode_id: Option<InodeId>,
    /// Replace only if the destination still has this revision.
    /// Requires `Replace` behavior.
    pub destination_expected_revision_no: Option<RevisionNo>,
}

impl MoveOptions {
    /// Creates options that refuse to replace the destination.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        }
    }
}

/// Options for copying a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    /// Create-only or replace-existing behavior for the destination.
    pub behavior: DestinationBehavior,
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
    /// Replace only if the destination still points to this inode.
    /// Requires `Replace` behavior.
    pub destination_expected_inode_id: Option<InodeId>,
    /// Replace only if the destination still has this revision.
    /// Requires `Replace` behavior.
    pub destination_expected_revision_no: Option<RevisionNo>,
}

impl CopyOptions {
    /// Creates options that refuse to replace the destination.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
            destination_expected_inode_id: None,
            destination_expected_revision_no: None,
        }
    }
}

/// Options for restoring a file revision by path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRevisionOptions {
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
}

impl RestoreRevisionOptions {
    /// Creates restore options for this actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            commit: CommitOptions::new(actor),
        }
    }
}

/// Options for recovering a deleted file or subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndeleteOptions {
    /// Actor, commit ID, and message.
    pub commit: CommitOptions,
}

impl UndeleteOptions {
    /// Creates undelete options for this actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            commit: CommitOptions::new(actor),
        }
    }
}

/// Options for starting a direct multipart upload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectMultipartUploadOptions {
    /// Byte length of every part except the last. `None` uses the server
    /// default. Providers accept at most 10,000 parts.
    pub part_size_bytes: Option<u64>,
}
