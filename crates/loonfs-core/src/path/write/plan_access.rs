//! The publish plan for one access update.

use super::authorize::Absence;
use super::ensure_expected_inode;
use super::publish_path_planning::{CompiledFilesystemOperation, PublishPathPlanningView};
use crate::commit::{CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::final_component;
use loonfs_api::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, InodeId, InodeKind, ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_update_access<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    boundary: bool,
    grants: &AccessGrants,
    expected_inode_id: Option<InodeId>,
    expected_access_revision_no: Option<AccessRevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    loonfs_api::v0::validate_access_precondition(expected_inode_id, expected_access_revision_no)?;
    if view.access.is_unrestricted() {
        return Err(CoreError::NamespaceUnrestricted {
            namespace_id: view.namespace_id.clone(),
        });
    }

    let target = view.view.resolve_visible_path(absolute_path).await?;
    if absolute_path.is_root() {
        if let Some(expected) = expected_inode_id {
            if target.inode_id != expected {
                return Err(CommitValidationError::BindingPreconditionMismatch {
                    target: format!("path `{absolute_path}`"),
                    expected_inode_id: Some(expected),
                    actual_inode_id: Some(target.inode_id),
                    precondition_index: None,
                }
                .into());
            }
        }
    } else {
        ensure_expected_inode(&target, expected_inode_id, &final_component(absolute_path)?)?;
    }
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

    let current = view.view.latest_access_revision(target.inode_id).await?;
    let empty = AccessGrants::default();
    view.authorize_access_update(
        target.inode_id,
        current
            .as_ref()
            .map_or((false, &empty), |row| (row.boundary, &row.grants)),
        (boundary, grants),
        Absence::Path(absolute_path.as_str()),
    )
    .await?;
    let current_revision_no =
        current.map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
    let base_access_revision_no = expected_access_revision_no.unwrap_or(current_revision_no);
    Ok(CompiledFilesystemOperation::new(vec![
        CommitOp::UpdateAccess {
            inode_id: target.inode_id,
            base_access_revision_no,
            boundary,
            grants: grants.clone(),
        },
    ]))
}
