//! [`MutationRequest`]: the one filesystem mutation language, before planning.

use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
    InodeId, RevisionNo,
};

/// One logical filesystem mutation: an idempotency key, an optional caller
/// annotation, and an ordered, non-empty list of operations.
///
/// The whole request is one commit. Operations resolve in order, each seeing
/// the effects of the ones before it, and either all of them commit or none
/// of them do. A convenience one-operation request is this same type with a
/// single-element list, so it plans, fingerprints, and replays identically.
///
/// Paths are parsed once at the surface that accepts the raw string
/// ([`parse_mutation_path`]); a request always carries validated, normalized
/// paths.
///
/// [`parse_mutation_path`]: crate::path::parse_mutation_path
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRequest {
    /// Client idempotency key for the whole request.
    pub commit_id: CommitId,
    /// Caller annotation recorded on the commit. Part of the request's
    /// identity: reusing a commit id with a different message conflicts.
    pub message: Option<String>,
    /// Ordered operations. Must be non-empty.
    pub operations: Vec<FilesystemOperation>,
}

impl MutationRequest {
    /// A request carrying exactly one operation.
    pub fn single(
        commit_id: CommitId,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            message,
            operations: vec![operation],
        }
    }
}

/// One path-oriented operation inside a [`MutationRequest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilesystemOperation {
    /// Create one directory.
    CreateDir {
        absolute_path: AbsolutePath,
        /// Also create missing ancestor directories, like `PutFile` does.
        parents: bool,
    },
    /// Put one file at a path.
    PutFile {
        absolute_path: AbsolutePath,
        content_ref: ContentRef,
        behavior: DestinationBehavior,
        /// When set, replacing applies only while the file's current
        /// revision is still this one; a raced write fails the plan
        /// instead of stacking a revision on state the caller never saw.
        expected_revision_no: Option<RevisionNo>,
    },
    /// Delete one path.
    DeletePath {
        absolute_path: AbsolutePath,
        behavior: DeleteDirectoryBehavior,
        /// When set, the delete applies only while the path still resolves
        /// to this inode; a raced rebinding fails the plan instead of
        /// deleting the wrong inode.
        expected_inode_id: Option<InodeId>,
    },
    /// Move one path to another path.
    MovePath {
        from_path: AbsolutePath,
        to_path: AbsolutePath,
        behavior: DestinationBehavior,
    },
    /// Copy one file path to another path.
    CopyFilePath {
        from_path: AbsolutePath,
        to_path: AbsolutePath,
        behavior: DestinationBehavior,
    },
    /// Restore a file revision at a path.
    RestoreRevision {
        absolute_path: AbsolutePath,
        source_revision_no: RevisionNo,
    },
    /// Recover a deleted subtree to a destination path.
    Undelete {
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: AbsolutePath,
    },
}
