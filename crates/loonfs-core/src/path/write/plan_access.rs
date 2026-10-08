//! The publish plan for one access update.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    resolve_visible_inode, CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::authorize::Absence;
use crate::commit::{CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::metadata::{AccessRevisionRecord, ResolvedVisiblePath, VisiblePathError};
use crate::path::mutation_path::final_component;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AbsolutePath, AccessGrants, AccessRevisionNo, AccessRight, InodeId, InodeKind, ROOT_INODE_ID,
};

pub(super) async fn plan_update_access<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    boundary: bool,
    grants: &AccessGrants,
    expected_inode_id: Option<InodeId>,
    expected_access_revision_no: Option<AccessRevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    loonfs_types::api::v0::validate_access_precondition(
        expected_inode_id,
        expected_access_revision_no,
    )?;
    if view.access.is_unrestricted() {
        return Err(CoreError::NamespaceUnrestricted {
            namespace_id: view.namespace_id.clone(),
        });
    }

    // A file in the middle of the path is authorized like a target, so the
    // error that names it reaches only a subject allowed to learn of it.
    let (authorized_inode_id, resolved) = match view.view.resolve_visible_path(absolute_path).await
    {
        Ok(target) => (target.inode_id, Ok(target)),
        Err(
            error @ CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                inode_id,
                ..
            }),
        ) => (inode_id, Err(error)),
        Err(error) => return Err(error),
    };
    let current = view
        .view
        .latest_access_revision(authorized_inode_id)
        .await?;
    let empty = AccessGrants::default();
    view.authorize_access_update(
        authorized_inode_id,
        current
            .as_ref()
            .map_or((false, &empty), |row| (row.boundary, &row.grants)),
        (boundary, grants),
        Absence::Path(absolute_path.as_str()),
    )
    .await?;
    let target = resolved?;
    validate_access_target(&target, boundary, grants)?;
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
        ensure_expected_inode(
            view.view.naming(),
            &target,
            expected_inode_id,
            &final_component(absolute_path)?,
        )?;
    }
    Ok(update_access(
        target.inode_id,
        current,
        boundary,
        grants,
        expected_access_revision_no,
    ))
}

pub(super) async fn plan_update_access_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    boundary: bool,
    grants: &AccessGrants,
    expected_access_revision_no: Option<AccessRevisionNo>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    if view.access.is_unrestricted() {
        return Err(CoreError::NamespaceUnrestricted {
            namespace_id: view.namespace_id.clone(),
        });
    }
    let target = resolve_visible_inode(view, inode_id).await?;
    let current = view.view.latest_access_revision(inode_id).await?;
    let empty = AccessGrants::default();
    view.authorize_access_update(
        inode_id,
        current
            .as_ref()
            .map_or((false, &empty), |row| (row.boundary, &row.grants)),
        (boundary, grants),
        Absence::Inode,
    )
    .await?;
    validate_access_target(&target, boundary, grants)?;
    Ok(update_access(
        inode_id,
        current,
        boundary,
        grants,
        expected_access_revision_no,
    ))
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

fn update_access(
    inode_id: InodeId,
    current: Option<AccessRevisionRecord>,
    boundary: bool,
    grants: &AccessGrants,
    expected_access_revision_no: Option<AccessRevisionNo>,
) -> CompiledFilesystemOperation {
    let current_revision_no =
        current.map_or(AccessRevisionNo(0), |revision| revision.access_revision_no);
    let base_access_revision_no = expected_access_revision_no.unwrap_or(current_revision_no);
    CompiledFilesystemOperation::new(vec![CommitOp::UpdateAccess {
        inode_id,
        base_access_revision_no,
        boundary,
        grants: grants.clone(),
    }])
}
