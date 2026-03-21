#![forbid(unsafe_code)]

mod conflict;
mod local_apply;

pub use conflict::{
    discover_conflict_artifacts_for_namespace, load_conflict_artifact,
    load_or_discover_conflict_artifact, load_subtree_conflict_artifact, prepare_conflict_artifact,
    prepare_subtree_conflict_artifact, restore_conflict_artifact_to_path,
    restore_file_conflict_artifact_to_path, restore_subtree_conflict_artifact_to_path,
    upload_loser_content_from_path, write_conflict_artifact_if_absent,
    write_subtree_conflict_artifact_if_absent, ConflictArtifactError, PreparedConflictArtifact,
    PreparedSubtreeConflictArtifact, RestoredConflictArtifact, RestoredFileConflictArtifact,
    RestoredSubtreeConflictArtifact,
};
pub use local_apply::LocalApplyError;

pub mod download;
pub mod executor;
pub mod planner;
pub mod state_db;
pub mod upload;
