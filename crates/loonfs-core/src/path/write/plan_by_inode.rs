//! Publish plans for inode-addressed mutations: the operations a client
//! writes when it holds inode identity rather than a path.
//!
//! Each plan resolves its targets by inode and then compiles the same
//! commit operations its path twin compiles, so the durable deltas and the
//! change feed cannot tell the two spellings apart.

use super::plan_delete::publish_plan_delete;
use super::plan_transfer::publish_plan_move;
use super::publish_path_planning::{
    publish_check_binding_generation, publish_child_display_path,
    publish_child_name_absent_precondition, publish_classify_replace_destination,
    publish_resolve_visible_child, publish_resolve_visible_directory,
    publish_resolve_visible_inode, CompiledFilesystemOperation, PublishPathPlanningView,
};
use crate::commit::{
    CandidateAllocation, CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
};
use crate::error::{CoreError, Result};
use loonfs_api::{
    ContentRef, DeleteDirectoryBehavior, DestinationBehavior, DisplayName, InodeId, InodeKind,
    RevisionNo,
};
use loonfs_objectstore::ObjectStore;

/// What an inode-addressed create binds under its parent.
pub(super) enum NewChild {
    Directory,
    File(ContentRef),
}

/// Creates one entry under a parent addressed by inode.
///
/// Create-only for both kinds: an already bound name is a conflict, never a
/// replacement.
pub(super) async fn plan_publish_create_by_inode<S: ObjectStore + ?Sized>(
    parent_inode_id: InodeId,
    display_name: &DisplayName,
    child: NewChild,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    let parent = publish_resolve_visible_directory(view, parent_inode_id).await?;
    if let Some(existing) =
        publish_resolve_visible_child(view, parent_inode_id, display_name).await?
    {
        return Err(CoreError::DestinationExists {
            path: publish_child_display_path(&parent.absolute_path, display_name),
            existing_display_name: Some(existing.display_name),
        });
    }
    let child_inode_id = allocation.allocate()?;
    let op = match child {
        NewChild::Directory => ApiCommitOp::CreateDirectory {
            child_inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
        },
        NewChild::File(content_ref) => ApiCommitOp::CreateFile {
            child_inode_id,
            parent_inode_id,
            display_name: display_name.clone(),
            content_ref,
        },
    };
    Ok(CompiledFilesystemOperation::new(
        vec![op],
        vec![
            publish_child_name_absent_precondition(parent_inode_id, display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode_id,
            },
        ],
    ))
}

/// Appends a revision to a file addressed by inode.
///
/// The plan carries no binding precondition: the operation names the file
/// itself, so where the file is bound is not part of what it asserts.
pub(super) async fn plan_publish_put_file_revision_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    content_ref: ContentRef,
    expected_revision_no: RevisionNo,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = publish_resolve_visible_inode(view, inode_id).await?;
    if target.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: target.absolute_path,
            kind: target.inode_kind,
        });
    }
    // The caller's revision is the base the write applies over, so a raced
    // write reaches the same base-revision mismatch a guarded path put
    // reaches, with the same expected and actual details.
    Ok(CompiledFilesystemOperation::new(
        vec![ApiCommitOp::ReplaceFile {
            inode_id,
            base_revision_no: expected_revision_no,
            content_ref,
        }],
        vec![
            ApiCommitPrecondition::InodeRevisionIs {
                inode_id,
                revision_no: expected_revision_no,
            },
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted { inode_id },
        ],
    ))
}

/// Moves the inode a client holds under a new parent and name.
pub(super) async fn plan_publish_move_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    expected_binding_generation: &str,
    to_parent_inode_id: InodeId,
    to_display_name: &DisplayName,
    behavior: DestinationBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let source = publish_resolve_visible_inode(view, inode_id).await?;
    publish_check_binding_generation(view, &source, expected_binding_generation)?;
    let target_parent = publish_resolve_visible_directory(view, to_parent_inode_id).await?;
    let destination_path =
        publish_child_display_path(&target_parent.absolute_path, to_display_name);
    let occupant = publish_resolve_visible_child(view, to_parent_inode_id, to_display_name).await?;
    let replaced =
        publish_classify_replace_destination(occupant, behavior, inode_id, &destination_path)?;
    publish_plan_move(
        view,
        &source,
        to_parent_inode_id,
        to_display_name,
        replaced,
        &destination_path,
    )
    .await
}

/// Deletes the inode a client holds.
pub(super) async fn plan_publish_delete_by_inode<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    expected_binding_generation: &str,
    behavior: DeleteDirectoryBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let target = publish_resolve_visible_inode(view, inode_id).await?;
    publish_check_binding_generation(view, &target, expected_binding_generation)?;
    let target_path = target.absolute_path.clone();
    publish_plan_delete(view, &target, behavior, &target_path).await
}
