//! The publish plan for one access update.

use super::planner::OperationPlanningSession;
use super::planning_authorization::AccessStateRef;
use super::preconditions::ensure_expected_access_revision;
use super::publish_path_planning::{ResolvedTarget, Target};
use crate::error::{CoreError, Result};
use crate::metadata::{ResolvedVisiblePath, VisiblePathError};
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
        let (authorized_inode_id, resolved) = match target {
            // A file in the middle of the path is authorized like a target,
            // so the error that names it reaches only a subject allowed to
            // learn of it.
            Target::Path { path, .. } => {
                match self.view().metadata.resolve_visible_path(path).await {
                    Ok(resolved) => (resolved.inode_id, Ok(resolved)),
                    Err(
                        error @ CoreError::VisiblePath(
                            VisiblePathError::PathComponentNotDirectory { inode_id, .. },
                        ),
                    ) => (inode_id, Err(error)),
                    Err(error) => return Err(error),
                }
            }
            Target::Inode(inode_id) => (
                inode_id,
                Ok(self.view().resolve_visible_inode(inode_id).await?),
            ),
        };
        let current = self
            .view()
            .metadata
            .latest_access_revision(authorized_inode_id)
            .await?;
        self.view()
            .authorize_access_update(
                authorized_inode_id,
                AccessStateRef::from_row(current.as_ref()),
                AccessStateRef { boundary, grants },
                target.absence(),
            )
            .await?;
        let target = ResolvedTarget {
            resolved: resolved?,
            target,
        };
        validate_access_target(&target.resolved, boundary, grants)?;
        target.ensure_expected_inode()?;
        if let Some(expected) = expected_access_revision_no {
            let actual =
                current.map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
            ensure_expected_access_revision(target.inode_id(), expected, Some(actual))?;
        }
        self.validation
            .update_access(target.inode_id(), boundary, grants)
            .await
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
