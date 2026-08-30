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
mod publish_path_planning;
mod session;

use crate::error::{CoreError, Result};
use loonfs_api::{DestinationBehavior, InodeId, RevisionNo};

pub use intent::{CommitRequest, FilesystemOperation};
pub(crate) use planner::commit_fingerprint;
pub(crate) use session::PublishPlanningSession;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExpectedFileState {
    pub(super) inode_id: InodeId,
    pub(super) revision_no: Option<RevisionNo>,
}

pub(super) fn validate_expected_file_state(
    behavior: DestinationBehavior,
    expected_inode_id: Option<InodeId>,
    expected_revision_no: Option<RevisionNo>,
    revision_field: &str,
    inode_field: &str,
) -> Result<Option<ExpectedFileState>> {
    if behavior == DestinationBehavior::NoReplace
        && !matches!((expected_inode_id, expected_revision_no), (None, None))
    {
        return Err(CoreError::InvalidCommitRequest(
            "write guards require replace behavior".to_owned(),
        ));
    }
    let Some(inode_id) = expected_inode_id else {
        if expected_revision_no.is_some() {
            return Err(CoreError::InvalidCommitRequest(format!(
                "{revision_field} asserts a revision of a specific file; pair it with {inode_field} so the assertion names which file"
            )));
        }
        return Ok(None);
    };
    Ok(Some(ExpectedFileState {
        inode_id,
        revision_no: expected_revision_no,
    }))
}
