//! Shared path-planning checks and visible-ancestor walks.

use crate::authorize::{Absence, Authorizer, Replacement};
use crate::binding_version;
use crate::commit::{CandidateAllocation, CommitOp, ResolvedBinding};
use crate::error::{CoreError, Result};
use crate::metadata::access::{access_chain, effective_rights, is_administrator};
use crate::metadata::MetadataVisibilityReads;
use crate::metadata::{MetadataView, ResolvedVisiblePath, VisiblePathError};
use crate::path::read;
use loonfs_api::{
    AbsolutePath, BindingVersion as BindingVersionToken, DestinationBehavior, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceAccess, NamespaceId, ROOT_INODE_ID,
};
use loonfs_api::{AccessGrants, AccessRight, AccessRights, PrincipalId};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
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
    pub(super) access: &'a NamespaceAccess,
    pub(super) authorizer: &'a Authorizer<'a>,
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

pub(super) async fn resolve_visible_path_for_authorization<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
    rights: AccessRights,
    absence: Absence<'_>,
) -> Result<ResolvedVisiblePath> {
    match view.view.resolve_visible_path(absolute_path).await {
        Err(
            error @ CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                inode_id,
                ..
            }),
        ) => {
            view.authorize(inode_id, rights, absence).await?;
            Err(error)
        }
        result => result,
    }
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

/// Requires the binding version supplied by the caller to still be current.
pub(super) fn check_binding_version<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    resolved: &ResolvedVisiblePath,
    expected_binding_version: &BindingVersionToken,
) -> Result<()> {
    let expected =
        binding_version::decode(expected_binding_version, view.namespace_id).map_err(|error| {
            CoreError::InvalidCommitField {
                field: "expected_binding_version",
                message: format!("invalid expected binding version: {error}"),
                precondition_index: None,
            }
        })?;
    let Some(current) = resolved.binding_version else {
        return Err(CoreError::RootMutationForbidden);
    };
    if current != expected {
        return Err(CoreError::BindingVersionMismatch {
            inode_id: resolved.inode_id,
            expected_binding_version: expected_binding_version.clone(),
            actual_binding_version: Some(binding_version::encode(current, view.namespace_id)),
            precondition_index: None,
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
        display_name: binding
            .display_name()
            .expect("visible binding should be bound")
            .clone(),
        child_inode_id: binding.child_inode_id,
        position: binding.position(),
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
        let visible_component = bound_child
            .display_name()
            .expect("visible binding should be bound")
            .clone();
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
                tombstone.committed_seq,
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

pub(super) struct ResolvedParents {
    /// The directory the new entry is bound under, possibly allocated here.
    pub(super) parent_inode_id: InodeId,
    /// The deepest directory that already existed, whose `create` right
    /// covers every directory allocated below it.
    pub(super) deepest_existing: InodeId,
}

pub(super) async fn ensure_parent_directories<S: ObjectStore + ?Sized>(
    absolute_path: &AbsolutePath,
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    ops: &mut Vec<CommitOp>,
    allocation: &mut CandidateAllocation,
) -> Result<ResolvedParents> {
    let components = absolute_path.components();
    if components.len() <= 1 {
        return Ok(ResolvedParents {
            parent_inode_id: ROOT_INODE_ID,
            deepest_existing: ROOT_INODE_ID,
        });
    }

    let mut current_inode = ROOT_INODE_ID;
    let mut creating_missing_ancestors = false;
    let mut deepest_existing = current_inode;
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
                    view.authorize(
                        child.child_inode_id,
                        AccessRights::from_iter([AccessRight::Create]),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                    return Err(CoreError::NonDirectoryPathComponent(
                        component.as_str().to_owned(),
                    ));
                }
                current_inode = child.child_inode_id;
                continue;
            }
            deepest_existing = current_inode;
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
    if !creating_missing_ancestors {
        deepest_existing = current_inode;
    }
    Ok(ResolvedParents {
        parent_inode_id: current_inode,
        deepest_existing,
    })
}

pub(super) async fn resolve_parent_directory<S: ObjectStore + ?Sized>(
    view: &PublishPathPlanningView<'_, '_, '_, S>,
    absolute_path: &AbsolutePath,
    absence: Absence<'_>,
) -> Result<InodeId> {
    let Some(parent_path) = absolute_path.parent() else {
        return Ok(ROOT_INODE_ID);
    };
    if parent_path.is_root() {
        return Ok(ROOT_INODE_ID);
    }
    let resolved = resolve_visible_path_for_authorization(
        view,
        &parent_path,
        AccessRights::from_iter([AccessRight::Create]),
        absence,
    )
    .await?;
    if resolved.inode_kind != InodeKind::Directory {
        view.authorize(
            resolved.inode_id,
            AccessRights::from_iter([AccessRight::Create]),
            absence,
        )
        .await?;
        return Err(CoreError::ExpectedDirectory {
            target: parent_path.as_str().to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(resolved.inode_id)
}

impl<S: ObjectStore + ?Sized> PublishPathPlanningView<'_, '_, '_, S> {
    /// Requires `rights` on `inode_id`.
    pub(super) async fn authorize(
        &self,
        inode_id: InodeId,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<()> {
        crate::authorize::require(
            self.authorizer,
            &mut self.view.reads(),
            inode_id,
            rights,
            absence,
        )
        .await
    }

    /// Authorizes the entry right an occupied or vacant destination needs,
    /// before the conflict rule can reveal whether the name exists.
    pub(super) async fn authorize_destination(
        &self,
        occupant: Option<&ResolvedVisiblePath>,
        behavior: DestinationBehavior,
        source_inode_id: Option<InodeId>,
        destination_parent: InodeId,
        replacement: Replacement,
        absence: Absence<'_>,
    ) -> Result<()> {
        if matches!(self.authorizer, Authorizer::Unrestricted) {
            return Ok(());
        }
        let (inode_id, rights) = match occupant {
            None => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
            Some(existing) if Some(existing.inode_id) == source_inode_id => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
            Some(existing) if behavior == DestinationBehavior::Replace => match replacement {
                Replacement::RemovesEntry => (
                    destination_parent,
                    AccessRights::from_iter([AccessRight::Create, AccessRight::Remove]),
                ),
                Replacement::WritesFile => (
                    existing.inode_id,
                    AccessRights::from_iter([AccessRight::Write]),
                ),
            },
            Some(_) => (
                destination_parent,
                AccessRights::from_iter([AccessRight::Create]),
            ),
        };
        self.authorize(inode_id, rights, absence).await
    }

    /// Authorizes moving `moved` from `source_parent` to `destination_parent`
    /// as if the mover had granted every right the move confers.
    pub(super) async fn authorize_relocation(
        &self,
        moved: InodeId,
        source_parent: InodeId,
        destination_parent: InodeId,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        if source_parent == destination_parent {
            return Ok(());
        }
        let mut reads = self.view.reads();
        let own = reads.find_access_row(moved).await?;
        if own.as_ref().is_some_and(|row| row.boundary) {
            return Ok(());
        }
        let before_chain = access_chain(&mut reads, source_parent).await?;
        let after_chain = access_chain(&mut reads, destination_parent).await?;
        let before_rows: Vec<_> = own.iter().chain(before_chain.rows()).collect();
        let after_rows: Vec<_> = own.iter().chain(after_chain.rows()).collect();
        let root = reads.find_access_row(ROOT_INODE_ID).await?;
        let named: BTreeSet<&PrincipalId> = before_rows
            .iter()
            .chain(&after_rows)
            .flat_map(|row| row.grants.iter().map(|(principal, _)| principal))
            .collect();
        let mut gain = AccessRights::EMPTY;
        for principal in named {
            if root
                .as_ref()
                .is_some_and(|row| row.grants.get(principal).contains(AccessRight::Admin))
            {
                continue;
            }
            let before = before_rows
                .iter()
                .map(|row| row.grants.get(principal))
                .fold(AccessRights::EMPTY, AccessRights::union)
                .difference(AccessRights::ADMIN);
            let after = after_rows
                .iter()
                .map(|row| row.grants.get(principal))
                .fold(AccessRights::EMPTY, AccessRights::union)
                .difference(AccessRights::ADMIN);
            gain = gain.union(after.difference(before));
        }
        if gain.is_empty() || is_administrator(&mut reads, principals).await? {
            return Ok(());
        }
        let mover = effective_rights(&mut reads, principals, moved).await?;
        if mover.contains(AccessRight::Manage)
            || (mover.contains(AccessRight::Share) && gain.is_subset_of(mover))
        {
            return Ok(());
        }
        Err(CoreError::Forbidden { inode_id: moved })
    }

    /// Authorizes replacing `target`'s access row with `next`.
    pub(super) async fn authorize_access_update(
        &self,
        target: InodeId,
        current: (bool, &AccessGrants),
        next: (bool, &AccessGrants),
        absence: Absence<'_>,
    ) -> Result<()> {
        let Authorizer::Subject { principals } = self.authorizer else {
            return Ok(());
        };
        let mut reads = self.view.reads();
        let effective = effective_rights(&mut reads, principals, target).await?;
        if effective.is_empty() {
            return Err(absence.not_found(target));
        }
        if target == ROOT_INODE_ID && administrators(current.1) != administrators(next.1) {
            return if is_administrator(&mut reads, principals).await? {
                Ok(())
            } else {
                Err(CoreError::Forbidden { inode_id: target })
            };
        }
        if effective.contains(AccessRight::Manage) {
            return Ok(());
        }
        if current.0 != next.0 {
            return Err(CoreError::Forbidden { inode_id: target });
        }
        let named: BTreeSet<&PrincipalId> = current
            .1
            .iter()
            .chain(next.1.iter())
            .map(|(principal, _)| principal)
            .collect();
        for principal in named {
            let added = next.1.get(principal).difference(current.1.get(principal));
            let removed = current.1.get(principal).difference(next.1.get(principal));
            if !added.union(removed).is_subset_of(effective) {
                return Err(CoreError::Forbidden { inode_id: target });
            }
        }
        if effective.contains(AccessRight::Share) {
            Ok(())
        } else {
            Err(CoreError::Forbidden { inode_id: target })
        }
    }
}

fn administrators(grants: &AccessGrants) -> BTreeSet<&PrincipalId> {
    grants
        .iter()
        .filter_map(|(principal, rights)| rights.contains(AccessRight::Admin).then_some(principal))
        .collect()
}
