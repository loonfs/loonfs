use loon_api::{v0::RenameMode, CommitId, ContentRef, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Behavior for path-based file put operations.
pub enum PutFileBehavior {
    /// Create the file only if the target path is absent.
    CreateOnly,
    /// Replace an existing file if the target path already exists.
    ReplaceExisting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// User-facing path mutation before it is planned against namespace state.
///
/// Server and embedded publishers accept these intents, then resolve paths and
/// preconditions immediately before publishing.
pub enum PathMutationIntent {
    /// Create one directory.
    CreateDir {
        commit_id: CommitId,
        absolute_path: String,
    },
    /// Put one file at a path.
    PutFile {
        commit_id: CommitId,
        absolute_path: String,
        content_ref: ContentRef,
        behavior: PutFileBehavior,
    },
    /// Delete one path.
    DeletePath {
        commit_id: CommitId,
        absolute_path: String,
        recursive: bool,
    },
    /// Move one path to another path.
    MovePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
        mode: RenameMode,
    },
    /// Copy one file path to another path.
    CopyFilePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
    },
    /// Restore a file revision at a path.
    RestoreRevision {
        commit_id: CommitId,
        absolute_path: String,
        source_revision_no: RevisionNo,
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
            | Self::RestoreRevision { commit_id, .. } => commit_id,
        }
    }
}
