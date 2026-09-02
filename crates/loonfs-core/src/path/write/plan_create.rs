//! Publish plans that create, recover, or replace path content.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    ensure_parent_directories, is_missing_visible_path, reject_tombstoned_path_ancestor,
    require_vacant_path, resolve_parent_directory, CompiledFilesystemOperation,
    PublishPathPlanningView,
};
use crate::commit::{CandidateAllocation, CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_api::{
    AbsolutePath, ChangeSeq, ContentRef, DestinationBehavior, ExpectedFileState, InodeId, InodeKind,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_create_directory<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    parents: bool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(view, absolute_path).await?;
    require_vacant_path(view, absolute_path).await?;
    let mut ops = Vec::new();
    let parent_inode_id = if parents {
        ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?
    } else {
        resolve_parent_directory(view, absolute_path).await?
    };
    let display_name = final_component(absolute_path)?;
    let child_inode_id = allocation.allocate()?;
    ops.push(CommitOp::CreateDirectory {
        child_inode_id,
        parent_inode_id,
        display_name,
    });
    Ok(CompiledFilesystemOperation::new(ops))
}

pub(super) async fn plan_undelete<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
    absolute_path: Option<&AbsolutePath>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let (parent_inode_id, display_name) = match absolute_path {
        Some(absolute_path) => {
            ensure_mutation_path(absolute_path)?;
            reject_tombstoned_path_ancestor(view, absolute_path).await?;
            require_vacant_path(view, absolute_path).await?;
            // The destination parent must already exist: recovery targets a
            // place the caller can see, and commit validation re-checks the
            // tombstone root, the parent, and the name under the publish
            // lock.
            let parent_inode_id = resolve_parent_directory(view, absolute_path).await?;
            (parent_inode_id, final_component(absolute_path)?.clone())
        }
        // In place: re-bind under the parent and name the deletion recorded,
        // anchored on the parent's identity rather than a remembered
        // spelling, so recovery lands correctly even when ancestors were
        // renamed since.
        None => {
            let Some(deletion) = view
                .view
                .recoverable_deletion(deletion_seq, inode_id)
                .await?
            else {
                return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
            };
            let direntry = deletion.deleted_direntry;
            (direntry.parent_inode_id, direntry.display_name)
        }
    };
    Ok(CompiledFilesystemOperation::new(vec![CommitOp::Undelete {
        inode_id,
        deletion_seq,
        parent_inode_id,
        display_name,
    }]))
}

pub(super) async fn plan_put_file_content_ref<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    content_ref: ContentRef,
    behavior: DestinationBehavior,
    expected_file_state: Option<ExpectedFileState>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view.view.resolve_visible_path(absolute_path).await;

    let mut ops = Vec::new();
    let final_parent_inode =
        ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?;
    let final_name = final_component(absolute_path)?;

    match target {
        Ok(existing) => {
            if behavior == DestinationBehavior::NoReplace {
                return Err(CoreError::DestinationExists {
                    path: absolute_path.as_str().to_owned(),
                    existing_display_name: Some(existing.display_name.clone()),
                });
            }
            ensure_expected_inode(
                &existing,
                expected_file_state.map(|expected| expected.inode_id),
                &final_name,
            )?;
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    target: absolute_path.as_str().to_owned(),
                    kind: existing.inode_kind,
                });
            }
            let revision = view
                .view
                .latest_revision_head(existing.inode_id)
                .await?
                .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
            let base_revision_no = expected_file_state
                .and_then(|expected| expected.revision_no)
                .unwrap_or(revision.revision_no);
            ops.push(CommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no,
                content_ref: content_ref.clone(),
            });
        }
        Err(error) if is_missing_visible_path(&error) => {
            if expected_file_state.is_some() {
                return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
            }
            let child_inode_id = allocation.allocate()?;
            ops.push(CommitOp::CreateFile {
                child_inode_id,
                parent_inode_id: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
        }
        Err(other) => return Err(other),
    }

    Ok(CompiledFilesystemOperation::new(ops))
}
