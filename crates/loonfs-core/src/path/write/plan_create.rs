//! Publish plans that create, recover, or replace path content.

use super::ensure_expected_inode;
use super::publish_path_planning::{
    ensure_parent_directories, is_missing_visible_path, require_vacant_path,
    resolve_parent_directory, resolve_visible_child, resolve_visible_inode,
    resolve_visible_path_for_authorization, CompiledFilesystemOperation, PublishPathPlanningView,
    ResolvedParents,
};
use crate::authorize::Absence;
use crate::commit::{CandidateAllocation, CommitOp, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::TombstoneRowAction;
use loonfs_types::{
    AbsolutePath, AccessRight, AccessRights, ChangeSeq, ContentRef, DestinationBehavior,
    DisplayName, ExpectedFileState, InodeId, InodeKind, ROOT_INODE_ID,
};

pub(super) async fn plan_create_directory<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    parents: bool,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    allocation: &mut CandidateAllocation,
) -> Result<CompiledFilesystemOperation> {
    ensure_mutation_path(absolute_path)?;
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

/// Where an undelete binds the entry it restores.
pub(super) enum UndeleteDestination<'a> {
    /// The parent and name the deletion recorded.
    Recorded,
    Path(&'a AbsolutePath),
    Child {
        parent_inode_id: InodeId,
        display_name: &'a DisplayName,
    },
}

impl<'a> UndeleteDestination<'a> {
    /// Reads the request's destination fields, which name at most one
    /// destination.
    pub(super) fn from_request(
        path: Option<&'a AbsolutePath>,
        parent_inode_id: Option<InodeId>,
        display_name: Option<&'a DisplayName>,
    ) -> Result<Self> {
        let (field, message) = match (path, parent_inode_id, display_name) {
            (None, None, None) => return Ok(Self::Recorded),
            (Some(path), None, None) => return Ok(Self::Path(path)),
            (None, Some(parent_inode_id), Some(display_name)) => {
                return Ok(Self::Child {
                    parent_inode_id,
                    display_name,
                })
            }
            (Some(_), _, _) => (
                "destination_path",
                "`destination_path` cannot be combined with `destination_parent_inode_id` \
                 or `destination_display_name`",
            ),
            (None, Some(_), None) => (
                "destination_display_name",
                "`destination_parent_inode_id` requires `destination_display_name`",
            ),
            (None, None, Some(_)) => (
                "destination_parent_inode_id",
                "`destination_display_name` requires `destination_parent_inode_id`",
            ),
        };
        Err(CoreError::InvalidCommitField {
            field,
            message: message.to_owned(),
            precondition_index: None,
        })
    }
}

pub(super) async fn plan_undelete<S: ObjectStore + ?Sized>(
    inode_id: InodeId,
    deletion_seq: ChangeSeq,
    destination: UndeleteDestination<'_>,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
) -> Result<CompiledFilesystemOperation> {
    let active = view.view.active_subtree_tombstone(inode_id).await?;
    let deleted_binding = match active.map(|record| record.action) {
        Some(TombstoneRowAction::Set { deleted_binding }) => deleted_binding,
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
    let saved_parent = deleted_binding.parent_inode_id;
    view.authorize(
        saved_parent,
        AccessRights::from_iter([AccessRight::Remove]),
        Absence::Inode,
    )
    .await?;
    let (parent_inode_id, display_name) = match destination {
        UndeleteDestination::Path(absolute_path) => {
            ensure_mutation_path(absolute_path)?;
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
        UndeleteDestination::Child {
            parent_inode_id,
            display_name,
        } => {
            let parent = resolve_visible_inode(view, parent_inode_id).await?;
            view.authorize(
                parent_inode_id,
                AccessRights::from_iter([AccessRight::Create]),
                Absence::Inode,
            )
            .await?;
            if parent.inode_kind != InodeKind::Directory {
                return Err(CoreError::ExpectedDirectory {
                    target: parent.absolute_path.to_string(),
                    kind: parent.inode_kind,
                });
            }
            if let Some(existing) =
                resolve_visible_child(view, parent_inode_id, display_name).await?
            {
                return Err(CoreError::DestinationExists {
                    path: parent.absolute_path.join(display_name).to_string(),
                    existing_display_name: Some(existing.display_name),
                });
            }
            view.authorize_relocation(inode_id, saved_parent, parent_inode_id)
                .await?;
            (parent_inode_id, display_name.clone())
        }
        UndeleteDestination::Recorded => {
            view.authorize(
                saved_parent,
                AccessRights::from_iter([AccessRight::Create]),
                Absence::Inode,
            )
            .await?;
            (saved_parent, deleted_binding.display_name)
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
