//! The publish plan for one access update.

use super::planner::OperationPlanningSession;
use super::planning_authorization::AccessStateRef;
use super::preconditions::ensure_expected_access_revision;
use super::publish_path_planning::{AccessUpdateTarget, Target};
use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessGrants, AccessRevisionNo, AccessRight, InodeKind, ROOT_INODE_ID};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    pub(super) async fn plan_update_access(
        &mut self,
        target: Target<'_>,
        boundary: bool,
        grants: &AccessGrants,
        expected_access_revision_no: Option<AccessRevisionNo>,
    ) -> Result<()> {
        let AccessUpdateTarget { target, current } = self
            .view()
            .resolve_access_update_target(target, AccessStateRef { boundary, grants })
            .await?;
        validate_access_target(target.resolved(), boundary, grants)?;
        target.ensure_expected_inode()?;
        if let Some(expected) = expected_access_revision_no {
            let actual =
                current.map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
            ensure_expected_access_revision(target.inode_id(), expected, Some(actual))?;
        }
        self.commit.update_access(&target, boundary, grants).await
    }
}

fn validate_access_target(
    target: &ResolvedVisiblePath,
    boundary: bool,
    grants: &AccessGrants,
) -> Result<()> {
    if target.inode_id != ROOT_INODE_ID
        && grants
            .iter()
            .any(|(_, rights)| rights.contains(AccessRight::Admin))
    {
        return Err(CoreError::InvalidCommitRequest(
            "admin is valid only on the root inode".to_owned(),
        ));
    }
    if boundary && target.inode_kind == InodeKind::File {
        return Err(CoreError::InvalidCommitRequest(
            "a boundary applies only to a directory".to_owned(),
        ));
    }
    Ok(())
}
