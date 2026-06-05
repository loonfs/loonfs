use loon_api::{v0::RenameMode, CommitId, ContentRef, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PutFileBehavior {
    CreateOnly,
    ReplaceExisting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMutationIntent {
    CreateDir {
        commit_id: CommitId,
        absolute_path: String,
    },
    PutFile {
        commit_id: CommitId,
        absolute_path: String,
        content_ref: ContentRef,
        behavior: PutFileBehavior,
    },
    DeletePath {
        commit_id: CommitId,
        absolute_path: String,
        recursive: bool,
    },
    MovePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
        mode: RenameMode,
    },
    CopyFilePath {
        commit_id: CommitId,
        from_path: String,
        to_path: String,
    },
    RestoreRevision {
        commit_id: CommitId,
        absolute_path: String,
        source_revision_no: RevisionNo,
    },
}

impl PathMutationIntent {
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
