//! Per-operation option shapes shared by the runtime and client surfaces.
//!
//! `loonfs` (embedded runtime) and `loonfs-client` (HTTP client) expose the
//! same semantic filesystem operations, so the options that parameterize them
//! are defined once here and re-exported by both under their existing names.
//! Keeping one definition is what stops the two surfaces from drifting a
//! field apart.
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
    ActorRef, AttributeKey, AttributeRevisionNo, AttributeValue, CommitId, DeleteDirectoryBehavior,
    DestinationBehavior, InodeId, RevisionNo,
};
use std::collections::BTreeMap;

/// Attribution and idempotency settings shared by every filesystem mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOptions {
    /// Application-asserted identity that caused the logical commit.
    pub actor: ActorRef,
    /// Optional idempotency key; a fresh id is generated when absent.
    pub commit_id: Option<CommitId>,
    /// Annotation recorded on the commit and included in its identity.
    pub message: Option<String>,
}

impl CommitOptions {
    /// Creates commit settings for the required actor.
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
}

impl Default for StatPathOptions {
    fn default() -> Self {
        Self {
            include_attributes: true,
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
}

/// Options for writing and removing an inode's attributes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAttributesOptions {
    /// Attributes to write. Each key replaces whatever the inode holds under
    /// it; keys the inode holds and this map does not name are left alone.
    pub set: BTreeMap<AttributeKey, AttributeValue>,
    /// Keys to remove.
    pub remove: Vec<AttributeKey>,
    /// Attribution, idempotency, and annotation for the commit.
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
    /// Creates an empty attribute update for the required actor.
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
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
    /// Replace only while the file's current revision is still this one.
    /// Requires `Replace` behavior; a raced write fails instead of stacking a
    /// revision on state the caller never saw.
    pub expected_revision_no: Option<RevisionNo>,
}

impl PutFileOptions {
    /// Creates put options with create-only behavior for the required actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
            expected_revision_no: None,
        }
    }
}

/// Options for creating a directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDirectoryOptions {
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
    /// Also create missing ancestor directories, like `put_file` does.
    pub parents: bool,
}

impl CreateDirectoryOptions {
    /// Creates directory options without parent creation for the required actor.
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
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
    /// When set, the delete applies only while the path still resolves to
    /// this inode, so a raced rebinding fails instead of deleting the wrong
    /// inode.
    pub expected_inode_id: Option<InodeId>,
}

impl DeleteOptions {
    /// Creates non-recursive delete options for the required actor.
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
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
}

impl MoveOptions {
    /// Creates create-only move options for the required actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
        }
    }
}

/// Options for copying a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    /// Create-only or replace-existing behavior for the destination.
    pub behavior: DestinationBehavior,
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
}

impl CopyOptions {
    /// Creates create-only copy options for the required actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit: CommitOptions::new(actor),
        }
    }
}

/// Options for restoring a file revision by path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreRevisionOptions {
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
}

impl RestoreRevisionOptions {
    /// Creates restore options for the required actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            commit: CommitOptions::new(actor),
        }
    }
}

/// Options for recovering a deleted file or subtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndeleteOptions {
    /// Attribution, idempotency, and annotation for the commit.
    pub commit: CommitOptions,
}

impl UndeleteOptions {
    /// Creates undelete options for the required actor.
    pub fn new(actor: ActorRef) -> Self {
        Self {
            commit: CommitOptions::new(actor),
        }
    }
}
