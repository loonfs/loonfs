//! Shared path-planning checks and visible-ancestor walks.

use crate::binding_generation::BindingGeneration;
use crate::commit::{CandidateAllocation, CommitOp, ResolvedBinding};
use crate::error::{CoreError, Result};
use crate::metadata::{MetadataView, ResolvedVisiblePath, VisiblePathError};
use crate::path::read;
use loonfs_api::{
    AbsolutePath, BindingGeneration as BindingGenerationToken, DestinationBehavior, DisplayName,
    InodeId, InodeKind, NameKey, NamespaceId, ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

pub(super) fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(
        error,
        CoreError::PathNotFound(_) | CoreError::VisiblePath(VisiblePathError::PathNotFound { .. })
    )
}

pub(super) async fn require_vacant_path<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    path: &AbsolutePath,
) -> Result<()> {
    match view.view.resolve_visible_path(path).await {
        Ok(existing) => Err(CoreError::DestinationExists {
            path: path.as_str().to_owned(),
            existing_display_name: Some(existing.display_name),
        }),
        Err(error) if is_missing_visible_path(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

/// One filesystem operation compiled into the commit operations it needs.
pub(super) struct CompiledFilesystemOperation {
    pub(super) ops: Vec<CommitOp>,
}

impl CompiledFilesystemOperation {
    pub(super) fn new(ops: Vec<CommitOp>) -> Self {
        Self { ops }
    }
}

pub(super) struct PublishPathPlanningView<'a, 'view, 'store, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) view: &'a MetadataView<'view, 'store, S>,
}

pub(super) async fn resolve_visible_inode<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    inode_id: InodeId,
) -> Result<ResolvedVisiblePath> {
    let mut session = view.view.session();
    let mut ancestor_paths = HashMap::new();
    read::resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
        .await?
        .ok_or(CoreError::InodeNotFound(inode_id))
}

pub(super) async fn resolve_visible_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    inode_id: InodeId,
) -> Result<ResolvedVisiblePath> {
    let resolved = resolve_visible_inode(view, inode_id).await?;
    if resolved.inode_kind != InodeKind::Directory {
        return Err(CoreError::ExpectedDirectory {
            target: resolved.absolute_path,
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved)
}

pub(super) async fn resolve_visible_child<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    parent_inode_id: InodeId,
    display_name: &DisplayName,
) -> Result<Option<ResolvedVisiblePath>> {
    let name_key = NameKey::for_display_name(display_name);
    let Some(binding) = view.view.visible_child(parent_inode_id, &name_key).await? else {
        return Ok(None);
    };
    resolve_visible_inode(view, binding.child_inode_id)
        .await
        .map(Some)
}

pub(super) fn child_display_path(parent_path: &str, display_name: &DisplayName) -> String {
    AbsolutePath::parse(parent_path)
        .expect("resolved parent path should be absolute")
        .join(display_name)
        .as_str()
        .to_owned()
}

/// Requires the binding generation supplied by the caller to still be current.
pub(super) fn check_binding_generation<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
    expected_binding_generation: &BindingGenerationToken,
) -> Result<()> {
    let expected = BindingGeneration::decode(expected_binding_generation, view.namespace_id)
        .map_err(|error| {
            CoreError::InvalidCommitRequest(format!("invalid expected binding generation: {error}"))
        })?;
    let Some(current) = resolved.binding_generation else {
        return Err(CoreError::RootMutationForbidden);
    };
    if current != expected {
        return Err(CoreError::BindingGenerationMismatch {
            inode_id: resolved.inode_id,
        });
    }
    Ok(())
}

pub(super) async fn source_binding<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
) -> Result<ResolvedBinding> {
    let parent_inode_id = resolved
        .parent_inode_id
        .ok_or(CoreError::RootMutationForbidden)?;
    let binding = view
        .view
        .current_parent_binding_for_child(resolved.inode_id)
        .await?
        .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.clone()))?;
    if binding.parent_inode_id != parent_inode_id {
        return Err(CoreError::PathNotFound(resolved.absolute_path.clone()));
    }
    Ok(ResolvedBinding {
        parent_inode_id,
        name_key: binding.name_key.clone(),
        display_name: binding.display_name.clone(),
        child_inode_id: binding.child_inode_id,
        bind_seq: binding.bind_seq,
        bind_delta_index: binding.bind_delta_index,
    })
}

/// Rejects planning through a *visible* path component covered by a subtree
/// tombstone. The walk observes only visible bindings, so its answer cannot
/// change when compaction drops rows no retained sequence observes: a deleted
/// (unbound) name simply ends the walk, and recreating it plans as a fresh
/// subtree.
///
/// A visible-but-covered component cannot arise from legal writer histories:
/// a delete unbinds and tombstones in one commit, and visibility already
/// excludes a covered inode (`metadata::visibility`). Hitting one means the
/// stored rows contradict themselves, so this reports corruption rather than
/// a conflict a caller could resolve.
pub(super) async fn reject_tombstoned_path_ancestor<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<()> {
    let mut current_inode = ROOT_INODE_ID;
    let mut current_path = AbsolutePath::root();

    for component in absolute_path.components() {
        let display_name = component.to_display_name();
        let name_key = NameKey::for_display_name(&display_name);
        let Some(bound_child) = view.view.visible_child(current_inode, &name_key).await? else {
            return Ok(());
        };
        let visible_component = bound_child.display_name.clone();
        let visible_path = current_path.join(&visible_component);
        if let Some(tombstone) = view
            .view
            .covering_subtree_tombstone(bound_child.child_inode_id)
            .await?
        {
            return Err(CoreError::NamespaceCorrupt(format!(
                "path `{}` is visible but covered by the subtree tombstone rooted at inode \
                 `{}` from seq `{}`",
                visible_path.as_str(),
                tombstone.root_inode_id,
                tombstone.generation.seq,
            )));
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
pub(super) async fn resolve_replace_destination<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    to_path: &AbsolutePath,
    behavior: DestinationBehavior,
    source_inode_id: InodeId,
) -> Result<ReplaceDestination> {
    let occupant = match view.view.resolve_visible_path(to_path).await {
        Ok(existing) => Some(existing),
        Err(error) if is_missing_visible_path(&error) => None,
        Err(error) => return Err(error),
    };
    classify_replace_destination(occupant, behavior, source_inode_id, to_path.as_str())
}

pub(super) fn classify_replace_destination(
    occupant: Option<ResolvedVisiblePath>,
    behavior: DestinationBehavior,
    source_inode_id: InodeId,
    destination_path: &str,
) -> Result<ReplaceDestination> {
    Ok(match occupant {
        Some(existing) if existing.inode_id == source_inode_id => ReplaceDestination::SameInode,
        Some(existing) if behavior == DestinationBehavior::Replace => {
            if existing.inode_kind != InodeKind::File {
                return Err(CoreError::ExpectedFile {
                    target: destination_path.to_owned(),
                    kind: existing.inode_kind,
                });
            }
            ReplaceDestination::Replaced(existing)
        }
        Some(existing) => {
            return Err(CoreError::DestinationExists {
                path: destination_path.to_owned(),
                existing_display_name: Some(existing.display_name),
            })
        }
        None => ReplaceDestination::Vacant,
    })
}

pub(super) async fn ensure_parent_directories<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    ops: &mut Vec<CommitOp>,
    allocation: &mut CandidateAllocation,
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
            if let Some(child) = view.view.visible_child(current_inode, &name_key).await? {
                let inode = view
                    .view
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

        let child_inode_id = allocation.allocate()?;
        ops.push(CommitOp::CreateDirectory {
            child_inode_id,
            parent_inode_id: current_inode,
            display_name,
        });
        current_inode = child_inode_id;
    }
    Ok(current_inode)
}

pub(super) async fn resolve_parent_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
) -> Result<InodeId> {
    let Some(parent_path) = absolute_path.parent() else {
        return Ok(ROOT_INODE_ID);
    };
    if parent_path.is_root() {
        return Ok(ROOT_INODE_ID);
    }
    let resolved = view.view.resolve_visible_path(&parent_path).await?;
    if resolved.inode_kind != InodeKind::Directory {
        return Err(CoreError::ExpectedDirectory {
            target: parent_path.as_str().to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}
