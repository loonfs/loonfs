//! Path-oriented writes: the mutation request language, the planner that
//! compiles it into one commit's operations, and the publish planning
//! session.

mod intent;
mod plan_access;
mod plan_append;
mod plan_attributes;
mod plan_by_inode;
mod plan_create;
mod plan_delete;
mod plan_restore;
mod plan_transfer;
mod planned_pieces;
pub(crate) mod planner;
mod preconditions;
mod publish_path_planning;
mod session;

use crate::error::CoreError;

pub use intent::{CommitIntent, FilesystemOperation};
pub(crate) use planner::commit_fingerprint;
pub(crate) use session::PublishPlanningSession;

impl From<loonfs_types::DestinationPreconditionError> for CoreError {
    fn from(error: loonfs_types::DestinationPreconditionError) -> Self {
        let field = match error {
            loonfs_types::DestinationPreconditionError::PreconditionsRequireReplace { field } => {
                field
            }
            loonfs_types::DestinationPreconditionError::RevisionRequiresInode {
                revision_field,
                ..
            } => revision_field,
            _ => return Self::InvalidCommitRequest(error.to_string()),
        };
        Self::InvalidCommitField {
            field,
            message: error.to_string(),
            precondition_index: None,
        }
    }
}
