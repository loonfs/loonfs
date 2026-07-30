//! Publish plans that delete visible paths.

use super::planning_helpers::{publish_binding_is_precondition, PublishPathPlanningView};
use crate::error::{CoreError, Result};
use crate::path::helpers::ensure_mutation_path;
use loonfs_api::{
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest,
    },
    AbsolutePath, CommitId, DeleteDirectoryBehavior, InodeId, InodeKind, NameKey, ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_publish_delete_path<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    behavior: DeleteDirectoryBehavior,
    expected_inode_id: Option<InodeId>,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest> {
    ensure_mutation_path(absolute_path)?;
    let resolved = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await?;
    // Planning happens under the publish lock, so this check is race-free:
    // a caller that resolved the path earlier (a stat) either deletes that
    // exact inode or fails, never a raced rebinding.
    if let Some(expected) = expected_inode_id {
        if resolved.inode_id != expected {
            return Err(
                crate::commit::CommitValidationError::BindingPreconditionMismatch {
                    // Root cannot be deleted, so a resolved delete target
                    // always has a parent.
                    parent_inode_id: resolved.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                    name_key: NameKey::for_display_name(
                        &absolute_path
                            .final_component()
                            .expect("non-root mutation path should have a final component")
                            .to_display_name(),
                    ),
                    expected_child_inode_id: expected,
                    actual_child_inode_id: resolved.inode_id,
                }
                .into(),
            );
        }
    }
    let recursive = behavior == DeleteDirectoryBehavior::Recursive;
    let op = match resolved.inode_kind {
        InodeKind::File => ApiCommitOp::DeleteFile {
            inode_id: resolved.inode_id,
        },
        InodeKind::Directory if recursive => ApiCommitOp::DeleteSubtree {
            root_inode_id: resolved.inode_id,
        },
        InodeKind::Directory => {
            if view
                .metadata_state
                .has_visible_children(resolved.inode_id)
                .await?
            {
                return Err(CoreError::DirectoryNotEmpty(
                    absolute_path.as_str().to_owned(),
                ));
            }
            ApiCommitOp::DeleteSubtree {
                root_inode_id: resolved.inode_id,
            }
        }
    };
    let mut preconditions = vec![publish_binding_is_precondition(view, &resolved).await?];
    if !recursive && resolved.inode_kind == InodeKind::Directory {
        preconditions.push(ApiCommitPrecondition::DirectoryEmpty {
            inode_id: resolved.inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![op],
        preconditions,
        message: None,
    })
}
