//! Per-operation option shapes shared by the runtime and client surfaces.
//!
//! `loonfs` (embedded runtime) and `loonfs-client` (HTTP client) expose the
//! same semantic filesystem operations, so the options that parameterize them
//! are defined once here and re-exported by both under their existing names.
//! Keeping one definition is what stops the two surfaces from drifting a
//! field apart.
//!
//! These are plain in-process argument structs, not wire shapes: nothing here
//! serializes. The request bodies that do cross the wire live in
//! [`crate::v0`], and each surface resolves these options into one.

use crate::{CommitId, DeleteDirectoryBehavior, DestinationBehavior, InodeId, RevisionNo};

/// Options for writing a file path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutFileOptions {
    /// Create-only or replace-existing behavior.
    pub behavior: DestinationBehavior,
    /// Idempotency key for the commit; retrying with the same id replays the
    /// committed mutation instead of double-committing. A fresh id is
    /// generated when absent.
    pub commit_id: Option<CommitId>,
    /// Annotation recorded on the commit; part of the commit's identity, so
    /// the same `commit_id` with a different message is a
    /// `commit_id_reuse_conflict`.
    pub message: Option<String>,
    /// Replace only while the file's current revision is still this one.
    /// Requires `Replace` behavior; a raced write fails instead of stacking a
    /// revision on state the caller never saw.
    pub expected_revision_no: Option<RevisionNo>,
}

impl Default for PutFileOptions {
    fn default() -> Self {
        Self {
            behavior: DestinationBehavior::NoReplace,
            commit_id: None,
            message: None,
            expected_revision_no: None,
        }
    }
}

/// Options for creating a directory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateDirectoryOptions {
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
    /// Annotation recorded on the commit; part of the commit's identity.
    pub message: Option<String>,
    /// Also create missing ancestor directories, like `put_file` does.
    pub parents: bool,
}

/// Options for deleting a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Directory delete behavior.
    pub behavior: DeleteDirectoryBehavior,
    /// Optional idempotency key.
    pub commit_id: Option<CommitId>,
    /// Annotation recorded on the commit; part of the commit's identity.
    pub message: Option<String>,
    /// When set, the delete applies only while the path still resolves to
    /// this inode, so a raced rebinding fails instead of deleting the wrong
    /// inode.
    pub expected_inode_id: Option<InodeId>,
}

impl Default for DeleteOptions {
    fn default() -> Self {
        Self {
            behavior: DeleteDirectoryBehavior::NonRecursive,
            commit_id: None,
            message: None,
            expected_inode_id: None,
        }
    }
}
