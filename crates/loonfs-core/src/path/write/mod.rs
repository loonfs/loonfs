//! Path-oriented writes: the mutation request language, the planner that
//! compiles it into one commit's operations, and the publish planning
//! session.

mod intent;
mod plan_attributes;
mod plan_by_inode;
mod plan_create;
mod plan_delete;
mod plan_restore;
mod plan_transfer;
pub(crate) mod planner;
mod preconditions;
mod publish_path_planning;
mod session;

use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_api::{DisplayName, InodeId, NameKey, ROOT_INODE_ID};

pub use intent::{CommitRequest, FilesystemOperation};
pub(crate) use planner::commit_fingerprint;
pub(crate) use session::PublishPlanningSession;

pub(super) fn ensure_expected_inode(
    resolved: &ResolvedVisiblePath,
    expected: Option<InodeId>,
    name: &DisplayName,
) -> Result<()> {
    if let Some(expected) = expected {
        if resolved.inode_id != expected {
            return Err(
                crate::commit::CommitValidationError::BindingPreconditionMismatch {
                    target: format!(
                        "name `{}` under parent inode `{}`",
                        NameKey::for_display_name(name),
                        resolved.parent_inode_id.unwrap_or(ROOT_INODE_ID)
                    ),
                    expected_inode_id: Some(expected),
                    actual_inode_id: Some(resolved.inode_id),
                    precondition_index: None,
                }
                .into(),
            );
        }
    }
    Ok(())
}

impl From<loonfs_api::DestinationPreconditionError> for CoreError {
    fn from(error: loonfs_api::DestinationPreconditionError) -> Self {
        let field = match error {
            loonfs_api::DestinationPreconditionError::PreconditionsRequireReplace { field } => {
                field
            }
            loonfs_api::DestinationPreconditionError::RevisionRequiresInode {
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
