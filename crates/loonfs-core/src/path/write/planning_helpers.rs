//! Shared publish-planning preconditions and visible-ancestor walks.

use crate::error::{CoreError, Result};
use crate::metadata::{MetadataView, ResolvedVisiblePath, VisiblePathError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{
    v0::{CommitOp as ApiCommitOp, CommitPrecondition as ApiCommitPrecondition},
    AbsolutePath, DestinationBehavior, DisplayName, InodeId, InodeKind, NameKey, ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;

pub(super) fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::PathNotFound(_) | CoreError::VisiblePath(VisiblePathError::PathNotFound { .. })
    )
}

pub(super) struct PublishPathPlanningView<'a, 'b, 'store, S: ObjectStore + ?Sized> {
    pub(super) head: &'a HeadState,
    pub(super) metadata_state: &'a MetadataView<'b, 'store, S>,
}

pub(super) async fn publish_binding_is_precondition<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
) -> Result<ApiCommitPrecondition> {
    let parent_inode_id = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .metadata_state
        .current_parent_binding_for_child(resolved.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode_id {
        return Err(CoreError::PathNotFound(resolved.absolute_path.clone()));
    }
    Ok(ApiCommitPrecondition::BindingIs {
        parent_inode_id,
        name_key: binding.name_key.clone(),
        child_inode_id: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

pub(super) fn publish_child_name_absent_precondition(
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> ApiCommitPrecondition {
    let name_key = NameKey::for_display_name(display_name);
    ApiCommitPrecondition::ChildNameAbsent {
        parent_inode_id,
        name_key,
    }
}

/// Rejects planning through a *visible* path component covered by a subtree
/// tombstone. The walk observes only visible bindings, so its answer cannot
/// change when compaction drops rows no retained sequence observes: a deleted
/// (unbound) name simply ends the walk, and recreating it plans as a fresh
/// subtree. A visible-but-covered component cannot arise from legal writer
/// histories; hitting one means corrupt state, and failing the plan is the
/// safe answer.
pub(super) async fn publish_reject_tombstoned_path_ancestor<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<()> {
    let mut current_inode = ROOT_INODE_ID;
    let mut current_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(&display_name);
        let Some(bound_child) = view
            .metadata_state
            .visible_child(current_inode, &name_key)
            .await?
        else {
            return Ok(());
        };
        let visible_component = bound_child.display_name.clone();
        let visible_path = current_path.join(&visible_component);
        if let Some(tombstone) = view
            .metadata_state
            .covering_subtree_tombstone(bound_child.child_inode_id)
            .await?
        {
            return Err(CoreError::TombstoneConflict {
                path: visible_path.as_str().to_owned(),
                root_inode_id: tombstone.root_inode_id,
                tombstone_seq: tombstone.tombstone_seq,
            });
        }
        current_inode = bound_child.child_inode_id;
        current_path = visible_path;
    }
    Ok(())
}

/// How the shared move/copy destination rule resolved.
pub(super) enum ReplaceDestination {
    /// Nothing visible occupies the destination.
    Vacant,
    /// A distinct file occupies it and `Replace` accepted it.
    Replaced(ResolvedVisiblePath),
    /// The destination resolves to the moving inode itself: a same-slot
    /// respelling, such as a case-only rename, whose name key already
    /// belongs to the source.
    SameInode,
}

/// Resolves the shared move/copy destination rule: replacement accepts a
/// distinct file, a move may respell its own binding, and everything else
/// visible at the destination is a conflict.
pub(super) async fn publish_resolve_replace_destination<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    source_inode_id: InodeId,
) -> Result<ReplaceDestination> {
    Ok(
        match view.metadata_state.resolve_visible_path(to_path).await {
            Ok(existing) if existing.inode_id == source_inode_id => ReplaceDestination::SameInode,
            Ok(existing) if behavior == DestinationBehavior::Replace => {
                if existing.inode_kind != InodeKind::File {
                    return Err(CoreError::ExpectedFile {
                        path: to_path.as_str().to_owned(),
                        kind: existing.inode_kind,
                    });
                }
                ReplaceDestination::Replaced(existing)
            }
            Ok(existing) => {
                return Err(CoreError::DestinationExists {
                    path: to_path.as_str().to_owned(),
                    existing_display_name: Some(existing.display_name),
                })
            }
            Err(error) if is_missing_visible_path(&error) => ReplaceDestination::Vacant,
            Err(error) => return Err(error),
        },
    )
}

pub(super) async fn publish_ensure_parent_directories<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    ops: &mut Vec<ApiCommitOp>,
    next_inode_id: &mut InodeId,
) -> Result<InodeId> {
    let components = absolute_path.components();
    if components.len() <= 1 {
        return Ok(ROOT_INODE_ID);
    }

    let mut current_inode = ROOT_INODE_ID;
    let mut creating_missing_ancestors = false;
    for component in &components[..components.len() - 1] {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(&display_name);
        if !creating_missing_ancestors {
            if let Some(child) = view
                .metadata_state
                .visible_child(current_inode, &name_key)
                .await?
            {
                let inode = view
                    .metadata_state
                    .visible_inode(child.child_inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(component.as_str().to_owned()))?;
                if inode.inode_kind != InodeKind::Directory {
                    return Err(CoreError::NonDirectoryPathComponent(
                        component.as_str().to_owned(),
                    ));
                }
                current_inode = child.child_inode_id;
                continue;
            }
            creating_missing_ancestors = true;
        }

        ops.push(ApiCommitOp::CreateDirectory {
            parent_inode_id: current_inode,
            display_name,
        });
        let allocated = *next_inode_id;
        *next_inode_id = InodeId(next_inode_id.0.saturating_add(1));
        current_inode = allocated;
    }
    Ok(current_inode)
}

pub(super) async fn publish_resolve_parent_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<InodeId> {
    let Some(parent_path) = absolute_path.parent() else {
        return Ok(ROOT_INODE_ID);
    };
    if parent_path.is_root() {
        return Ok(ROOT_INODE_ID);
    }
    let resolved = view
        .metadata_state
        .resolve_visible_path(&parent_path)
        .await?;
    if resolved.inode_kind != InodeKind::Directory {
        return Err(CoreError::ExpectedDirectory {
            path: parent_path.as_str().to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}
