//! Publish plans that create, recover, or replace path content.

use super::planning_helpers::{
    is_missing_visible_path, publish_binding_is_precondition,
    publish_child_name_absent_precondition, publish_ensure_parent_directories,
    publish_reject_tombstoned_path_ancestor, publish_resolve_parent_directory,
    PublishPathPlanningView,
};
use crate::error::{CoreError, Result};
use crate::path::helpers::{ensure_mutation_path, final_component};
use loonfs_api::{
    v0::{
        CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition,
        CommitRequest as ApiCommitRequest,
    },
    AbsolutePath, ChangeSeq, CommitId, ContentRef, DestinationBehavior, InodeId, InodeKind,
    RevisionNo,
};
use loonfs_objectstore::ObjectStore;

pub(super) async fn plan_publish_create_directory<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    parents: bool,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest> {
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
        let mut next_inode_id = view.head.next_inode_id;
        publish_ensure_parent_directories(absolute_path, view, &mut ops, &mut next_inode_id).await?
    } else {
        publish_resolve_parent_directory(view, absolute_path).await?
    };
    let display_name = final_component(absolute_path)?;
    ops.push(ApiCommitOp::CreateDirectory {
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
            view,
            parent_inode_id,
            &display_name,
        ));
        preconditions.push(ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
            inode_id: parent_inode_id,
        });
    }
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}

pub(super) async fn plan_publish_undelete<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    deleted_at_seq: ChangeSeq,
    absolute_path: &AbsolutePath,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest> {
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
    // The destination parent must already exist: recovery targets a place
    // the caller can see, and commit validation re-checks the tombstone
    // root, the parent, and the name under the publish lock.
    let parent_inode_id = publish_resolve_parent_directory(view, absolute_path).await?;
    let display_name = final_component(absolute_path)?;
    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops: vec![ApiCommitOp::Undelete {
            inode_id,
            deleted_at_seq,
            parent_inode_id,
            display_name: display_name.clone(),
        }],
        preconditions: vec![
            publish_child_name_absent_precondition(view, parent_inode_id, &display_name),
            ApiCommitPrecondition::AncestorsNotSubtreeDeleted {
                inode_id: parent_inode_id,
            },
        ],
        message: None,
    })
}

pub(super) async fn plan_publish_put_file_content_ref<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    content_ref: ContentRef,
    behavior: DestinationBehavior,
    expected_revision_no: Option<RevisionNo>,
    commit_id: &CommitId,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<ApiCommitRequest> {
    ensure_mutation_path(absolute_path)?;
    if expected_revision_no.is_some() && behavior == DestinationBehavior::NoReplace {
        return Err(CoreError::InvalidCommitRequest(
            "expected_revision_no asserts an existing file revision, which \
             contradicts no_replace; use replace behavior with the guard"
                .to_owned(),
        ));
    }
    publish_reject_tombstoned_path_ancestor(view, absolute_path).await?;
    let target = view
        .metadata_state
        .resolve_visible_path(absolute_path)
        .await;

    let mut ops = Vec::new();
    let mut next_inode_id = view.head.next_inode_id;
    let final_parent_inode =
        publish_ensure_parent_directories(absolute_path, view, &mut ops, &mut next_inode_id)
            .await?;
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
            // A caller-supplied guard replaces the freshly-read revision in
            // both the op and the precondition, so commit validation rejects
            // a raced write with the existing base-revision-mismatch error
            // and its expected/actual details.
            let base_revision_no = expected_revision_no.unwrap_or(revision.revision_no);
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
            // The guard asserts an existing file at a revision; an absent
            // path fails that assertion rather than silently creating.
            if expected_revision_no.is_some() {
                return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
            }
            ops.push(ApiCommitOp::CreateFile {
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
                    view,
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

    Ok(ApiCommitRequest {
        commit_id: commit_id.to_owned(),
        ops,
        preconditions,
        message: None,
    })
}
