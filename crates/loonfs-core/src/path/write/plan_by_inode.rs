//! Plans inode-addressed mutations.

use super::plan_delete::plan_delete;
use super::plan_transfer::plan_move;
use super::publish_path_planning::{
    check_binding_version, child_display_path, classify_replace_destination, resolve_visible_child,
    resolve_visible_inode, CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::authorize::{Absence, Replacement};
use crate::commit::{CandidateAllocation, CommitOp};
use crate::error::{CoreError, Result};
use loonfs_api::{
    AccessRight, AccessRights, BindingVersion, ContentRef, DeleteDirectoryBehavior,
    DestinationBehavior, DisplayName, ExpectedFileState, InodeId, InodeKind, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

pub(super) enum NewChild {
    Directory,
    File(ContentRef),
}

pub(super) async fn plan_create_by_inode<S: ObjectStore + ?Sized>(
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    child: NewChild,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    let parent = resolve_visible_inode(view, parent_inode_id).await?;
    view.authorize(
        parent_inode_id,
        AccessRights::from_iter([AccessRight::Create]),
        Absence::Inode,
    )
    .await?;
    if parent.inode_kind != InodeKind::Directory {
        return Err(CoreError::ExpectedDirectory {
            target: parent.absolute_path,
            kind: parent.inode_kind,
        });
    }
    if let Some(existing) = resolve_visible_child(view, parent_inode_id, display_name).await? {
        return Err(CoreError::DestinationExists {
            path: child_display_path(&parent.absolute_path, display_name),
            existing_display_name: Some(existing.display_name),
        });
    }
    let child_inode_id = allocation.allocate()?;
    let op = match child {
        NewChild::Directory => CommitOp::CreateDirectory {
            child_inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
        },
        NewChild::File(content_ref) => CommitOp::CreateFile {
            child_inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
            content_ref,
        },
    };
    Ok(CompiledFilesystemOperation::new(vec![op]))
}

pub(super) async fn plan_put_file_revision_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    content_ref: ContentRef,
    expected_revision_no: RevisionNo,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = resolve_visible_inode(view, inode_id).await?;
    view.authorize(
        inode_id,
        AccessRights::from_iter([AccessRight::Write]),
        Absence::Inode,
    )
    .await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            target: target.absolute_path,
            kind: target.inode_kind,
        });
    }
    Ok(CompiledFilesystemOperation::new(vec![
        CommitOp::ReplaceFile {
            inode_id,
            base_revision_no: expected_revision_no,
            content_ref,
        },
    ]))
}

pub(super) async fn plan_move_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    expected_binding_version: &BindingVersion,
    to_parent_inode_id: InodeId,
    to_display_name: &DisplayName,
    behavior: DestinationBehavior,
    expected_destination: Option<ExpectedFileState>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let source = resolve_visible_inode(view, inode_id).await?;
    view.authorize(
        source
            .parent_inode_id
            .ok_or(CoreError::RootMutationForbidden)?,
        AccessRights::from_iter([AccessRight::Remove]),
        Absence::Inode,
    )
    .await?;
    let target_parent = resolve_visible_inode(view, to_parent_inode_id).await?;
    if target_parent.inode_kind != InodeKind::Directory {
        view.authorize(
            to_parent_inode_id,
            AccessRights::from_iter([AccessRight::Create]),
            Absence::Inode,
        )
        .await?;
        return Err(CoreError::ExpectedDirectory {
            target: target_parent.absolute_path,
            kind: target_parent.inode_kind,
        });
    }
    let destination_path = child_display_path(&target_parent.absolute_path, to_display_name);
    let occupant = resolve_visible_child(view, to_parent_inode_id, to_display_name).await?;
    view.authorize_destination(
        occupant.as_ref(),
        behavior,
        Some(inode_id),
        to_parent_inode_id,
        Replacement::RemovesEntry,
        Absence::Inode,
    )
    .await?;
    check_binding_version(view, &source, expected_binding_version)?;
    let replaced = classify_replace_destination(occupant, behavior, inode_id, &destination_path)?;
    plan_move(
        view,
        &source,
        to_parent_inode_id,
        to_display_name,
        replaced,
        &destination_path,
        expected_destination,
    )
    .await
}

pub(super) async fn plan_delete_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    expected_binding_version: &BindingVersion,
    behavior: DeleteDirectoryBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = resolve_visible_inode(view, inode_id).await?;
    plan_delete(view, &target, behavior, Absence::Inode, || {
        check_binding_version(view, &target, expected_binding_version)
    })
    .await
}
