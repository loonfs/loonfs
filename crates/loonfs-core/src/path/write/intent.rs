//! [`PathMutationIntent`]: a user-facing path mutation before planning.

use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, ContentRef, DeleteDirectoryBehavior, DestinationBehavior,
    InodeId, RevisionNo,
};

/// User-facing path mutation before it is planned against namespace state.
///
/// Server and embedded publishers accept these intents, then resolve paths and
/// preconditions immediately before publishing. Paths are parsed once at the
/// surface that accepts the raw string ([`parse_mutation_path`]); an intent
/// always carries validated, normalized paths.
///
/// [`parse_mutation_path`]: crate::path::parse_mutation_path
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMutationIntent {
    /// Create one directory.
    CreateDir {
        commit_id: CommitId,
        absolute_path: AbsolutePath,
        /// Also create missing ancestor directories, like `PutFile` does.
        parents: bool,
    },
    /// Put one file at a path.
    PutFile {
        commit_id: CommitId,
        absolute_path: AbsolutePath,
        content_ref: ContentRef,
        behavior: DestinationBehavior,
    },
    /// Delete one path.
    DeletePath {
        commit_id: CommitId,
        absolute_path: AbsolutePath,
        behavior: DeleteDirectoryBehavior,
        /// When set, the delete applies only while the path still resolves
        /// to this inode; a raced rebinding fails the plan instead of
        /// deleting the wrong inode.
        expected_inode_id: Option<InodeId>,
    },
    /// Move one path to another path.
    MovePath {
        commit_id: CommitId,
        from_path: AbsolutePath,
        to_path: AbsolutePath,
        behavior: DestinationBehavior,
    },
    /// Copy one file path to another path.
    CopyFilePath {
        commit_id: CommitId,
        from_path: AbsolutePath,
        to_path: AbsolutePath,
        behavior: DestinationBehavior,
    },
    /// Restore a file revision at a path.
    RestoreRevision {
        commit_id: CommitId,
        absolute_path: AbsolutePath,
        source_revision_no: RevisionNo,
    },
    /// Recover a deleted subtree to a destination path.
    Undelete {
        commit_id: CommitId,
        inode_id: InodeId,
        deleted_at_seq: ChangeSeq,
        absolute_path: AbsolutePath,
    },
}

impl PathMutationIntent {
    /// Returns the idempotency key carried by this intent.
    pub fn commit_id(&self) -> &CommitId {
        match self {
            Self::CreateDir { commit_id, .. }
            | Self::PutFile { commit_id, .. }
            | Self::DeletePath { commit_id, .. }
            | Self::MovePath { commit_id, .. }
            | Self::CopyFilePath { commit_id, .. }
            | Self::RestoreRevision { commit_id, .. }
            | Self::Undelete { commit_id, .. } => commit_id,
        }
    }
}
