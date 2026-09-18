//! Publish plans that create, recover, or replace path content.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    ensure_parent_directories, is_missing_visible_path, reject_tombstoned_path_ancestor,
    require_vacant_path, resolve_parent_directory, resolve_visible_path_for_authorization,
    CompiledFilesystemOperation, PublishPathPlanningView, ResolvedParents,
};
use crate::authorize::Absence;
use crate::commit::{CandidateAllocation, CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_api::wire::manifest::TombstoneRowAction;
use loonfs_api::{
    AbsolutePath, AccessRight, AccessRights, ChangeSeq, ContentRef, DestinationBehavior,
    ExpectedFileState, InodeId, InodeKind, ROOT_INODE_ID,
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
    let mut ops = Vec::new();
    let parents = if parents {
        ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?
    } else {
        let parent_inode_id =
            resolve_parent_directory(view, absolute_path, Absence::Path(absolute_path.as_str()))
                .await?;
        ResolvedParents {
            parent_inode_id,
            deepest_existing: parent_inode_id,
        }
    };
    view.authorize(
        parents.deepest_existing,
        AccessRights::from_iter([AccessRight::Create]),
        Absence::Path(absolute_path.as_str()),
    )
    .await?;
    require_vacant_path(view, absolute_path).await?;
    let display_name = final_component(absolute_path)?;
    let child_inode_id = allocation.allocate()?;
    ops.push(CommitOp::CreateDirectory {
        child_inode_id,
        parent_inode_id: parents.parent_inode_id,
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
    let active = view.view.active_subtree_tombstone(inode_id).await?;
    let deleted_direntry = match active.map(|record| record.action) {
        Some(TombstoneRowAction::Set { deleted_direntry }) => deleted_direntry,
        _ => {
            let authorization_target = view
                .view
                .current_parent_binding_for_child(inode_id)
                .await?
                .map_or(inode_id, |binding| binding.parent_inode_id);
            view.authorize(
                authorization_target,
                AccessRights::from_iter([AccessRight::Remove]),
                Absence::Inode,
            )
            .await?;
            return Err(CommitValidationError::UndeleteTargetNotDeleted { inode_id }.into());
        }
    };
    let saved_parent = deleted_direntry.parent_inode_id;
    view.authorize(
        saved_parent,
        AccessRights::from_iter([AccessRight::Remove]),
        Absence::Inode,
    )
    .await?;
    let (parent_inode_id, display_name) = match absolute_path {
        Some(absolute_path) => {
            ensure_mutation_path(absolute_path)?;
            reject_tombstoned_path_ancestor(view, absolute_path).await?;
            let parent_inode_id = resolve_parent_directory(
                view,
                absolute_path,
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
            view.authorize(
                parent_inode_id,
                AccessRights::from_iter([AccessRight::Create]),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
            require_vacant_path(view, absolute_path).await?;
            view.authorize_relocation(inode_id, saved_parent, parent_inode_id)
                .await?;
            (parent_inode_id, final_component(absolute_path)?.clone())
        }
        None => {
            view.authorize(
                saved_parent,
                AccessRights::from_iter([AccessRight::Create]),
                Absence::Inode,
            )
            .await?;
            (saved_parent, deleted_direntry.display_name)
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
    let target = resolve_visible_path_for_authorization(
        view,
        absolute_path,
        AccessRights::from_iter([AccessRight::Create]),
        Absence::Path(absolute_path.as_str()),
    )
    .await;

    let mut ops = Vec::new();
    let final_name = final_component(absolute_path)?;

    match target {
        Ok(existing) => {
            if behavior == DestinationBehavior::NoReplace {
                view.authorize(
                    existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                    AccessRights::from_iter([AccessRight::Create]),
                    Absence::Path(absolute_path.as_str()),
                )
                .await?;
                return Err(CoreError::DestinationExists {
                    path: absolute_path.as_str().to_owned(),
                    existing_display_name: Some(existing.display_name.clone()),
                });
            }
            view.authorize(
                existing.inode_id,
                AccessRights::from_iter([AccessRight::Write]),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
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
            let parents =
                ensure_parent_directories(absolute_path, view, &mut ops, allocation).await?;
            view.authorize(
                parents.deepest_existing,
                AccessRights::from_iter([AccessRight::Create]),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
            if expected_file_state.is_some() {
                return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
            }
            let child_inode_id = allocation.allocate()?;
            ops.push(CommitOp::CreateFile {
                child_inode_id,
                parent_inode_id: parents.parent_inode_id,
                display_name: final_name.clone(),
                content_ref,
            });
        }
        Err(other) => return Err(other),
    }

    Ok(CompiledFilesystemOperation::new(ops))
}
