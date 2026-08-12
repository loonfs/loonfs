//! Publish plans that move or copy visible paths.

use super::planning_helpers::ReplaceDestination;
use super::planning_helpers::{
    publish_binding_is_precondition, publish_child_name_absent_precondition,
    publish_reject_tombstoned_path_ancestor, publish_resolve_parent_directory,
    publish_resolve_replace_destination, PlannedOperation, PublishPathPlanningView,
};
use crate::commit::{
    CandidateAllocation, CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
};
use crate::error::{CoreError, Result};
use crate::path::helpers::{ensure_mutation_path, final_component};
use loonfs_api::{AbsolutePath, AttributeRevisionNo, DestinationBehavior, InodeKind};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_publish_move_path<S: ObjectStore + ?Sized>(
    from_path: &AbsolutePath,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<PlannedOperation> {
    ensure_mutation_path(from_path)?;
    ensure_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, to_path).await?;
    let source = view.metadata_state.resolve_visible_path(from_path).await?;
    let target_parent = publish_resolve_parent_directory(view, to_path).await?;
    let target_name = final_component(to_path)?;
    // Replace compiles to an atomic delete-plus-rename: the destination
    // file's delete and the source's rebind land in one commit, and the
    // rename's target-name check observes the in-commit unbind. Mirrors
    // put: only a file destination can be replaced, and a path never
    // replaces itself.
    let replaced =
        publish_resolve_replace_destination(view, to_path, behavior, source.inode_id).await?;
    let mut ops = Vec::new();
    let mut preconditions = vec![publish_binding_is_precondition(view, &source).await?];
    match &replaced {
        ReplaceDestination::Replaced(existing) => {
            ops.push(ApiCommitOp::DeleteFile {
                inode_id: existing.inode_id,
            });
            preconditions.push(publish_binding_is_precondition(view, existing).await?);
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        ReplaceDestination::Vacant => {
            preconditions.push(publish_child_name_absent_precondition(
                target_parent,
                &target_name,
            ));
        }
        // The destination is the source's own binding: a same-slot
        // respelling (case-only rename, normalization-equal spelling). The
        // name is legitimately present — bound to the source — so no
        // absence precondition and nothing to delete; the rename alone
        // rebinds the new spelling. An unchanged spelling stays a
        // conflict: there is nothing to rename.
        ReplaceDestination::SameInode => {
            if target_name.as_str() == source.display_name {
                return Err(CoreError::DestinationExists {
                    path: to_path.as_str().to_owned(),
                    existing_display_name: Some(source.display_name.clone()),
                });
            }
        }
    }
    ops.push(ApiCommitOp::Rename {
        inode_id: source.inode_id,
        new_parent_inode_id: target_parent,
        new_display_name: target_name.clone(),
    });
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: source.inode_id,
    });
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: target_parent,
    });
    Ok(PlannedOperation::new(ops, preconditions))
}

pub(super) async fn plan_publish_copy_file_path<S: ObjectStore + ?Sized>(
    from_path: &AbsolutePath,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<PlannedOperation> {
    ensure_mutation_path(from_path)?;
    ensure_mutation_path(to_path)?;
    publish_reject_tombstoned_path_ancestor(view, from_path).await?;
    publish_reject_tombstoned_path_ancestor(view, to_path).await?;

    let source = view.metadata_state.resolve_visible_path(from_path).await?;
    if source.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            path: from_path.as_str().to_owned(),
            kind: source.inode_kind,
        });
    }

    // Replace mirrors put onto an existing file: the copy appends a new
    // revision to the destination inode, keeping its identity and revision
    // history. Only a file destination can be replaced, and a path never
    // replaces itself.
    let replaced =
        publish_resolve_replace_destination(view, to_path, behavior, source.inode_id).await?;

    let revision = view
        .metadata_state
        .latest_revision_head(source.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(from_path.as_str().to_owned()))?;

    let target_parent = publish_resolve_parent_directory(view, to_path).await?;
    let target_name = final_component(to_path)?;
    let mut ops = Vec::new();
    let mut preconditions = vec![
        publish_binding_is_precondition(view, &source).await?,
        ApiCommitPrecondition::InodeRevisionIs {
            inode_id: source.inode_id,
            revision_no: revision.revision_no,
        },
        ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: source.inode_id,
        },
    ];
    match &replaced {
        // Copying a file onto its own binding would delete the source to
        // make room for its copy; that is a conflict, with or without
        // `Replace` — the same-file rule every copy tool applies.
        ReplaceDestination::SameInode => {
            return Err(CoreError::DestinationExists {
                path: to_path.as_str().to_owned(),
                existing_display_name: Some(source.display_name.clone()),
            })
        }
        ReplaceDestination::Replaced(existing) => {
            let existing_revision = view
                .metadata_state
                .latest_revision_head(existing.inode_id)
                .await?
                .ok_or_else(|| CoreError::PathNotFound(to_path.as_str().to_owned()))?;
            ops.push(ApiCommitOp::ReplaceFile {
                inode_id: existing.inode_id,
                base_revision_no: existing_revision.revision_no,
                content_ref: revision.content_ref,
            });
            preconditions.push(publish_binding_is_precondition(view, existing).await?);
            preconditions.push(ApiCommitPrecondition::InodeRevisionIs {
                inode_id: existing.inode_id,
                revision_no: existing_revision.revision_no,
            });
            preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: existing.inode_id,
            });
        }
        ReplaceDestination::Vacant => {
            let child_inode_id = allocation.allocate()?;
            ops.push(ApiCommitOp::CreateFile {
                child_inode_id,
                parent_inode_id: target_parent,
                display_name: target_name.clone(),
                content_ref: revision.content_ref,
            });
            preconditions.push(publish_child_name_absent_precondition(
                target_parent,
                &target_name,
            ));
            // A copy to a vacant destination is a new resource that stands
            // for the source, so it starts with the source's attributes. The
            // new inode is at revision 0 with an empty map, so carrying them
            // over is a second internal operation with its own feed event.
            // Copying over an existing file changes nothing about that
            // file's attributes: the destination is a resource that already
            // exists and keeps what it holds.
            let (_, source_attributes) = view
                .metadata_state
                .attributes_at_visible_seq(source.inode_id)
                .await?;
            if !source_attributes.is_empty() {
                // Both operations carry the one identity assigned above, so
                // validation cannot allocate a different inode for either.
                ops.push(ApiCommitOp::UpdateAttributes {
                    inode_id: child_inode_id,
                    base_attributes_revision_no: AttributeRevisionNo(0),
                    attributes: source_attributes,
                });
            }
        }
    }
    preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
        inode_id: target_parent,
    });
    Ok(PlannedOperation::new(ops, preconditions))
}
