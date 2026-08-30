//! Publish plans that create, recover, or replace path content.

use super::publish_path_planning::{
    is_missing_visible_path, publish_binding_is_precondition,
    publish_child_name_absent_precondition, publish_ensure_parent_directories,
    publish_reject_tombstoned_path_ancestor, publish_resolve_parent_directory,
    CompiledFilesystemOperation, PublishPathPlanningView,
};
use super::ExpectedFileState;
use crate::commit::{
    CandidateAllocation, CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
    CommitValidationError,
};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_api::{
    AbsolutePath, ChangeSeq, ContentRef, DestinationBehavior, InodeId, InodeKind, NameKey,
    ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_publish_create_directory<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    parents: bool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    match view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await
    {
        Ok(existing) => {
            return Err(CoreError::DestinationExists {
                path: absolute_path.as_str().to_owned(),
                existing_display_name: Some(existing.display_name),
            });
        }
        Err(error) if is_missing_visible_path(&error) => {}
        Err(error) => return Err(error),
    }
    let mut ops = Vec::new();
    let parent_inode_id = if parents {
        // The same ancestor auto-create the put-file plan performs.
        publish_ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?
    } else {
        publish_resolve_parent_directory(view, absolute_path).await?
    };
    let display_name = final_component(absolute_path)?;
    let child_inode_id = allocation.allocate()?;
    ops.push(ApiCommitOp::CreateDirectory {
        child_inode_id,
        parent_inode_id,
        display_name: display_name.clone(),
    });
    // A parent allocated by this same commit cannot have conflicting
    // children yet, so the name and ancestor preconditions only apply when
    // the parent already exists — mirroring the put-file plan.
    let mut preconditions = Vec::new();
    if view
        .metadata_state
        .visible_inode(parent_inode_id)
        .await?
        .is_some()
    {
        preconditions.push(publish_child_name_absent_precondition(
            parent_inode_id,
            &display_name,
        ));
        preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: parent_inode_id,
        });
    }
    Ok(CompiledFilesystemOperation::new(ops, preconditions))
}

pub(super) async fn plan_publish_undelete<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
    absolute_path: Option<&AbsolutePath>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let (parent_inode_id, display_name) = match absolute_path {
        Some(absolute_path) => {
            ensure_mutation_path(absolute_path)?;
            publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
            match view
                .metadata_state
                .resolve_visible_path(absolute_path)
                .await
            {
                Ok(existing) => {
                    return Err(CoreError::DestinationExists {
                        path: absolute_path.as_str().to_owned(),
                        existing_display_name: Some(existing.display_name),
                    });
                }
                Err(error) if is_missing_visible_path(&error) => {}
                Err(error) => return Err(error),
            }
            // The destination parent must already exist: recovery targets a
            // place the caller can see, and commit validation re-checks the
            // tombstone root, the parent, and the name under the publish
            // lock.
            let parent_inode_id = publish_resolve_parent_directory(view, absolute_path).await?;
            (parent_inode_id, final_component(absolute_path)?.clone())
        }
        // In place: re-bind under the parent and name the deletion recorded,
        // anchored on the parent's identity rather than a remembered
        // spelling, so recovery lands correctly even when ancestors were
        // renamed since. Planning takes no courtesy looks at the parent or
        // the name — the preconditions below decide both under the publish
        // lock, and their rejections already speak the right vocabulary.
        None => {
            let Some(deletion) = view
                .metadata_state
                .recoverable_deletion(deletion_seq, inode_id)
                .await?
            else {
                return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
            };
            // The name key is re-derived at validation from the spelling,
            // so only the parent and the spelling are read here.
            match deletion.deleted_direntry {
                Some(direntry) => (direntry.parent_inode_id, direntry.display_name),
                None => {
                    return Err(CoreError::InvalidCommitRequest(
                        "the deletion recorded no binding to restore into; \
                         pass a destination path"
                            .to_owned(),
                    ));
                }
            }
        }
    };
    Ok(CompiledFilesystemOperation::new(
        vec![ApiCommitOp::Undelete {
            inode_id,
            deletion_seq,
            parent_inode_id,
            display_name: display_name.clone(),
        }],
        vec![
            publish_child_name_absent_precondition(parent_inode_id, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode_id,
            },
        ],
    ))
}

pub(super) async fn plan_publish_put_file_content_ref<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    content_ref: ContentRef,
    behavior: DestinationBehavior,
    expected_file_state: Option<ExpectedFileState>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await;

    let mut ops = Vec::new();
    let final_parent_inode =
        publish_ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?;
    let final_name = final_component(absolute_path)?;
    let mut preconditions = Vec::new();

    match target {
        Ok(existing) => {
            if behavior == DestinationBehavior::NoReplace {
                return Err(CoreError::DestinationExists {
                    path: absolute_path.as_str().to_owned(),
                    existing_display_name: Some(existing.display_name.clone()),
                });
            }
            if let Some(expected) = expected_file_state {
                if existing.inode_id != expected.inode_id {
                    return Err(CommitValidationError::BindingPreconditionMismatch {
                        parent_inode_id: existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                        name_key: NameKey::for_display_name(&final_name),
                        expected_child_inode_id: expected.inode_id,
                        actual_child_inode_id: existing.inode_id,
                    }
                    .into());
                }
            }
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    path: absolute_path.as_str().to_owned(),
                    kind: existing.inode_kind,
                });
            }
            let revision = view
                .metadata_state
                .latest_revision_head(existing.inode_id)
                .await?
                .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
            let base_revision_no = expected_file_state
                .and_then(|expected| expected.revision_no)
                .unwrap_or(revision.revision_no);
            preconditions.push(publish_binding_is_precondition(view, &existing).await?);
            ops.push(ApiCommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no,
                content_ref: content_ref.clone(),
            });
            preconditions.push(ApiCommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: base_revision_no,
            });
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        Err(error) if is_missing_visible_path(&error) => {
            if expected_file_state.is_some() {
                return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
            }
            let child_inode_id = allocation.allocate()?;
            ops.push(ApiCommitOp::CreateFile {
                child_inode_id,
                parent_inode_id: final_parent_inode,
                display_name: final_name.clone(),
                content_ref,
            });
            if view
                .metadata_state
                .visible_inode(final_parent_inode)
                .await?
                .is_some()
            {
                preconditions.push(publish_child_name_absent_precondition(
                    final_parent_inode,
                    &final_name,
                ));
                preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                    inode_id: final_parent_inode,
                });
            }
        }
        Err(other) => return Err(other),
    }

    Ok(CompiledFilesystemOperation::new(ops, preconditions))
}
