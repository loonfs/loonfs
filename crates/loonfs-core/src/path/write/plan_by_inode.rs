//! Plans inode-addressed mutations.

use super::plan_delete::plan_delete;
use super::plan_transfer::plan_move;
use super::publish_path_planning::{
    check_binding_generation, child_display_path, classify_replace_destination,
    resolve_visible_child, resolve_visible_directory, resolve_visible_inode,
    CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::commit::{CandidateAllocation, CommitOp};
use crate::error::{CoreError, Result};
use loonfs_api::{
    BindingGeneration, ContentRef, DeleteDirectoryBehavior, DestinationBehavior, DisplayName,
    ExpectedFileState, InodeId, InodeKind, RevisionNo,
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
    let parent = resolve_visible_directory(view, parent_inode_id).await?;
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
    expected_binding_generation: &BindingGeneration,
    to_parent_inode_id: InodeId,
    to_display_name: &DisplayName,
    behavior: DestinationBehavior,
    expected_destination: Option<ExpectedFileState>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let source = resolve_visible_inode(view, inode_id).await?;
    check_binding_generation(view, &source, expected_binding_generation)?;
    let target_parent = resolve_visible_directory(view, to_parent_inode_id).await?;
    let destination_path = child_display_path(&target_parent.absolute_path, to_display_name);
    let occupant = resolve_visible_child(view, to_parent_inode_id, to_display_name).await?;
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
    expected_binding_generation: &BindingGeneration,
    behavior: DeleteDirectoryBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = resolve_visible_inode(view, inode_id).await?;
    check_binding_generation(view, &target, expected_binding_generation)?;
    plan_delete(view, &target, behavior).await
}
