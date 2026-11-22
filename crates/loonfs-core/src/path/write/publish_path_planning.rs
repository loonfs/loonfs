//! [`PlanningView`]: the read-only part of planning one operation, with
//! the target and destination resolution that the planners share. The
//! directories that a put or `mkdir -p` creates above its path are created
//! here too, because only this module makes a [`ResolvedDestination`]. The
//! authorization checks are in [`super::planning_authorization`].

use super::planner::OperationPlanningSession;
use super::planning_authorization::AccessStateRef;
use super::preconditions::ensure_expected_inode;
use crate::authorize::{Absence, Authorizer};
use crate::binding_version;
use crate::commit::NewChild;
use crate::error::{CoreError, Result};

use crate::metadata::{
    AccessRevisionRecord, MetadataView, MissingPath, ResolvedVisiblePath, RevisionRecord,
    VisiblePathError,
};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use crate::path::read;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::{
    AbsolutePath, ActorId, BindingVersion as BindingVersionToken, DestinationBehavior, DisplayName,
    InodeId, InodeKind, NameKey, NamespaceId, RevisionNo, ROOT_INODE_ID,
};
use loonfs_types::{AccessRight, AccessRights};

use std::collections::HashMap;

pub(super) fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(error, CoreError::PathNotFound(_))
}

/// What one operation plans against: the namespace, the request's
/// authorizer, and the metadata that the operation resolves
/// against, which includes the commit's earlier operations.
pub(super) struct PlanningView<'a, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) authorizer: Authorizer<'a>,
    pub(super) metadata: MetadataView<'a, 'a, S>,
}

/// The inode an operation changes, as the request names it.
#[derive(Clone, Copy)]
pub(super) enum Target<'a> {
    /// A path, with the inode the request expects to find there.
    Path {
        path: &'a AbsolutePath,
        expected_inode_id: Option<InodeId>,
    },
    Inode(InodeId),
}

impl<'a> Target<'a> {
    pub(super) fn path(path: &'a AbsolutePath, expected_inode_id: Option<InodeId>) -> Self {
        Self::Path {
            path,
            expected_inode_id,
        }
    }

    /// Returns how an inode the subject can't see is reported: as the
    /// request's path, or as the inode ID it named.
    pub(super) fn absence(&self) -> Absence<'a> {
        match self {
            Self::Path { path, .. } => Absence::Path(path.as_str()),
            Self::Inode(_) => Absence::Inode,
        }
    }
}

/// A target resolved to its visible inode. Only the resolutions in this
/// module make one, so a write that takes one changes a visible inode.
pub(crate) struct ResolvedTarget<'a> {
    resolved: ResolvedVisiblePath,
    target: Target<'a>,
}

impl<'a> ResolvedTarget<'a> {
    pub(crate) fn inode_id(&self) -> InodeId {
        self.resolved.inode_id
    }

    pub(super) fn resolved(&self) -> &ResolvedVisiblePath {
        &self.resolved
    }

    pub(super) fn absence(&self) -> Absence<'a> {
        self.target.absence()
    }

    /// Returns the target as error messages name it: the request's path, or
    /// the path that the inode resolved to.
    pub(super) fn label(&self) -> String {
        match self.target {
            Target::Path { path, .. } => path.as_str().to_owned(),
            Target::Inode(_) => self.resolved.absolute_path.to_string(),
        }
    }

    pub(super) fn not_found(&self) -> CoreError {
        self.absence().not_found(self.resolved.inode_id)
    }

    pub(super) fn parent_inode_id(&self) -> Result<InodeId> {
        self.resolved
            .parent_inode_id
            .ok_or(CoreError::RootMutationForbidden)
    }

    pub(super) fn ensure_file(&self) -> Result<()> {
        ensure_file(&self.resolved, &self.label())
    }

    /// Requires the inode that the request expects at a path target.
    pub(super) fn ensure_expected_inode(&self) -> Result<()> {
        let Target::Path {
            path,
            expected_inode_id,
        } = self.target
        else {
            return Ok(());
        };
        if !path.is_root() {
            return ensure_expected_inode(
                &self.resolved,
                expected_inode_id,
                &final_component(path)?,
            );
        }
        // The root has no name under a parent, so its mismatch names the path.
        match expected_inode_id {
            Some(expected) if expected != self.resolved.inode_id => Err(
                crate::commit::CommitValidationError::BindingPreconditionMismatch {
                    target: format!("path `{path}`"),
                    expected_inode_id: Some(expected),
                    actual_inode_id: Some(self.resolved.inode_id),
                }
                .into(),
            ),
            _ => Ok(()),
        }
    }
}

/// The target of an access update, with the access row that the update
/// replaces.
pub(super) struct AccessUpdateTarget<'a> {
    pub(super) target: ResolvedTarget<'a>,
    pub(super) current: Option<AccessRevisionRecord>,
}

/// The current binding of a visible inode other than the root, as
/// [`PlanningView::source_binding`] read it. Only that function makes one,
/// so a write that takes one unbinds a current binding or writes a revision
/// under it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ResolvedBinding {
    parent_inode_id: InodeId,
    name_key: NameKey,
    display_name: DisplayName,
    child_inode_id: InodeId,
    /// The child's kind, as the binding records it.
    child_kind: InodeKind,
    /// Position of the bind event that established the binding.
    bind_position: DeltaPosition,
    /// Actor of the child's creating commit, as the binding records it.
    child_created_by: ActorId,
    /// Unix milliseconds of the child's creating commit, as the binding
    /// records it.
    child_created_at_ms: u64,
}

impl ResolvedBinding {
    pub(crate) fn parent_inode_id(&self) -> InodeId {
        self.parent_inode_id
    }

    pub(crate) fn name_key(&self) -> &NameKey {
        &self.name_key
    }

    pub(crate) fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub(crate) fn child_inode_id(&self) -> InodeId {
        self.child_inode_id
    }

    pub(crate) fn child_kind(&self) -> InodeKind {
        self.child_kind
    }

    pub(crate) fn bind_position(&self) -> DeltaPosition {
        self.bind_position
    }

    pub(crate) fn child_created_by(&self) -> &ActorId {
        &self.child_created_by
    }

    pub(crate) fn child_created_at_ms(&self) -> u64 {
        self.child_created_at_ms
    }
}

/// Where a create, move, copy, or restore binds its inode.
#[derive(Clone, Copy)]
pub(super) enum Destination<'a> {
    Path(&'a AbsolutePath),
    Name {
        parent_inode_id: InodeId,
        display_name: &'a DisplayName,
    },
}

impl<'a> Destination<'a> {
    pub(super) fn name(parent_inode_id: InodeId, display_name: &'a DisplayName) -> Self {
        Self::Name {
            parent_inode_id,
            display_name,
        }
    }

    /// Rejects a destination path at the root, which no operation binds.
    pub(super) fn ensure_mutation(&self) -> Result<()> {
        match self {
            Self::Path(path) => ensure_mutation_path(path),
            Self::Name { .. } => Ok(()),
        }
    }
}

/// A destination resolved to its parent directory and its current occupant.
/// [`PlanningView::resolve_destination`] makes one from a [`Destination`],
/// and [`ResolvedDestination::found`] and
/// [`OperationPlanningSession::create_missing_directories`] make one from
/// what a walk of the request's path returned.
pub(super) struct ResolvedDestination<'a> {
    parent_inode_id: InodeId,
    name: DisplayName,
    /// The destination as error messages name it: the request's path, or
    /// the parent's path joined with the name.
    path: String,
    occupant: Option<ResolvedVisiblePath>,
    absence: Absence<'a>,
}

impl<'a> ResolvedDestination<'a> {
    /// Returns the destination `name` under the directory `parent_inode_id`,
    /// with `occupant`, the inode that occupies it now, for an operation at
    /// the request's path `absolute_path`, which its errors report.
    fn at_path(
        absolute_path: &'a AbsolutePath,
        parent_inode_id: InodeId,
        name: DisplayName,
        occupant: Option<ResolvedVisiblePath>,
    ) -> Self {
        Self {
            parent_inode_id,
            name,
            path: absolute_path.as_str().to_owned(),
            occupant,
            absence: Absence::Path(absolute_path.as_str()),
        }
    }

    /// Returns the destination at `absolute_path`, which a walk of the path
    /// found occupied by `existing`.
    pub(super) fn found(
        absolute_path: &'a AbsolutePath,
        existing: ResolvedVisiblePath,
    ) -> Result<Self> {
        Ok(Self::at_path(
            absolute_path,
            existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
            final_component(absolute_path)?,
            Some(existing),
        ))
    }

    pub(super) fn parent_inode_id(&self) -> InodeId {
        self.parent_inode_id
    }

    pub(super) fn name(&self) -> &DisplayName {
        &self.name
    }

    pub(super) fn path(&self) -> &str {
        &self.path
    }

    /// Returns the occupant that a move or copy with `behavior` replaces: an
    /// inode other than the source `source_inode_id`, when the behavior is
    /// `Replace`.
    pub(super) fn replaced_occupant(
        &self,
        behavior: DestinationBehavior,
        source_inode_id: InodeId,
    ) -> Option<&ResolvedVisiblePath> {
        self.occupant.as_ref().filter(|existing| {
            existing.inode_id != source_inode_id && behavior == DestinationBehavior::Replace
        })
    }

    pub(super) fn absence(&self) -> Absence<'a> {
        self.absence
    }

    /// Returns the destination as a name that a bind can take, which is a
    /// name that nothing visible occupies, or that `rebinding`, the inode
    /// that the bind moves, occupies already.
    pub(super) fn available_name(&self, rebinding: Option<InodeId>) -> Result<AvailableName> {
        match &self.occupant {
            Some(existing) if Some(existing.inode_id) != rebinding => {
                Err(CoreError::DestinationExists {
                    path: self.path.clone(),
                    existing_display_name: Some(existing.display_name.clone()),
                })
            }
            _ => Ok(AvailableName {
                parent_inode_id: self.parent_inode_id,
                display_name: self.name.clone(),
            }),
        }
    }
}

/// A name under a visible directory that a bind can take. Only
/// [`ResolvedDestination::available_name`] makes one, so a write that takes
/// one binds under a visible directory, at a name that no other visible
/// inode occupies.
pub(crate) struct AvailableName {
    parent_inode_id: InodeId,
    display_name: DisplayName,
}

impl AvailableName {
    pub(crate) fn parent_inode_id(&self) -> InodeId {
        self.parent_inode_id
    }

    pub(crate) fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub(crate) fn name_key(&self) -> NameKey {
        NameKey::for_display_name(&self.display_name)
    }
}

/// Requires `resolved` to be a file, the only kind of inode whose content an
/// operation replaces or copies. `target` is the target as the error message
/// shows it.
pub(super) fn ensure_file(resolved: &ResolvedVisiblePath, target: &str) -> Result<()> {
    if resolved.inode_kind != InodeKind::File {
        return Err(CoreError::ExpectedFile {
            target: target.to_owned(),
            kind: resolved.inode_kind,
        });
    }
    Ok(())
}

/// Rejects the root, which only an access update may name as its target.
pub(super) fn ensure_mutation_inode(inode_id: InodeId) -> Result<()> {
    if inode_id == ROOT_INODE_ID {
        return Err(CoreError::RootMutationForbidden);
    }
    Ok(())
}

impl<S: ObjectStore + ?Sized> PlanningView<'_, S> {
    /// Resolves `target` to its visible inode, after rejecting the root.
    ///
    /// A path whose middle component is not a directory is reported only
    /// after `rights` is authorized on that component.
    pub(super) async fn resolve_target<'t>(
        &self,
        target: Target<'t>,
        rights: AccessRights,
    ) -> Result<ResolvedTarget<'t>> {
        let resolved = match target {
            Target::Path { path, .. } => {
                ensure_mutation_path(path)?;
                self.authorize_non_directory_component(
                    self.metadata.resolve_visible_path(path).await,
                    rights,
                    Absence::Path(path.as_str()),
                )
                .await?
            }
            Target::Inode(inode_id) => {
                ensure_mutation_inode(inode_id)?;
                self.resolve_visible_inode(inode_id).await?
            }
        };
        Ok(ResolvedTarget { resolved, target })
    }

    /// Resolves `target`, requires `rights` on it, and then requires the
    /// inode that the request expects.
    pub(super) async fn resolve_authorized_target<'t>(
        &self,
        target: Target<'t>,
        rights: AccessRights,
    ) -> Result<ResolvedTarget<'t>> {
        let target = self.resolve_target(target, rights).await?;
        self.authorize(target.inode_id(), rights, target.absence())
            .await?;
        target.ensure_expected_inode()?;
        Ok(target)
    }

    /// Resolves the target of an access update to its visible inode, after
    /// authorizing the update from the inode's current access to `next`.
    ///
    /// A path whose middle component is not a directory is reported only
    /// after the update is authorized on that component, so the error that
    /// identifies it reaches only a subject allowed to learn of it.
    pub(super) async fn resolve_access_update_target<'t>(
        &self,
        target: Target<'t>,
        next: AccessStateRef<'_>,
    ) -> Result<AccessUpdateTarget<'t>> {
        let (authorized_inode_id, resolved) = match target {
            Target::Path { path, .. } => match self.metadata.resolve_visible_path(path).await {
                Ok(resolved) => (resolved.inode_id, Ok(resolved)),
                Err(
                    error @ CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                        inode_id,
                        ..
                    }),
                ) => (inode_id, Err(error)),
                Err(error) => return Err(error),
            },
            Target::Inode(inode_id) => (inode_id, Ok(self.resolve_visible_inode(inode_id).await?)),
        };
        let current = self
            .metadata
            .latest_access_revision(authorized_inode_id)
            .await?;
        self.authorize_access_update(
            authorized_inode_id,
            AccessStateRef::from_row(current.as_ref()),
            next,
            target.absence(),
        )
        .await?;
        Ok(AccessUpdateTarget {
            target: ResolvedTarget {
                resolved: resolved?,
                target,
            },
            current,
        })
    }

    /// Returns the latest revision of a resolved file target.
    pub(super) async fn latest_file_revision(
        &self,
        target: &ResolvedTarget<'_>,
    ) -> Result<RevisionRecord> {
        self.metadata
            .latest_revision_head(target.inode_id())
            .await?
            .ok_or_else(|| target.not_found())
    }

    /// Returns the number of a file's current revision, or `None` when the
    /// file isn't visible.
    pub(super) async fn current_revision_no(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionNo>> {
        let revision = self.metadata.latest_revision_head(inode_id).await?;
        Ok(revision.map(|revision| revision.revision_no))
    }

    /// Resolves `destination` to its parent directory, its name, and the
    /// entry that occupies it now.
    ///
    /// A parent that is not a directory is reported only after `create` is
    /// authorized on it.
    pub(super) async fn resolve_destination<'d>(
        &self,
        destination: Destination<'d>,
    ) -> Result<ResolvedDestination<'d>> {
        match destination {
            Destination::Path(path) => {
                let parent_inode_id = self
                    .resolve_parent_directory(path, Absence::Path(path.as_str()))
                    .await?;
                let name = final_component(path)?;
                let occupant = match self.metadata.resolve_visible_path(path).await {
                    Ok(existing) => Some(existing),
                    Err(error) if is_missing_visible_path(&error) => None,
                    Err(error) => return Err(error),
                };
                Ok(ResolvedDestination::at_path(
                    path,
                    parent_inode_id,
                    name,
                    occupant,
                ))
            }
            Destination::Name {
                parent_inode_id,
                display_name,
            } => {
                let parent = self.resolve_visible_inode(parent_inode_id).await?;
                if parent.inode_kind != InodeKind::Directory {
                    self.authorize(parent_inode_id, AccessRight::Create.into(), Absence::Inode)
                        .await?;
                    return Err(CoreError::ExpectedDirectory {
                        target: parent.absolute_path.to_string(),
                        kind: parent.inode_kind,
                    });
                }
                let occupant = self
                    .resolve_visible_child(parent_inode_id, display_name)
                    .await?;
                Ok(ResolvedDestination {
                    parent_inode_id,
                    name: display_name.clone(),
                    path: parent.absolute_path.join(display_name).to_string(),
                    occupant,
                    absence: Absence::Inode,
                })
            }
        }
    }

    /// Authorizes `create` on the destination's parent and then returns the
    /// destination as a vacant name.
    pub(super) async fn ensure_vacant_destination(
        &self,
        destination: &ResolvedDestination<'_>,
    ) -> Result<AvailableName> {
        self.authorize(
            destination.parent_inode_id,
            AccessRight::Create.into(),
            destination.absence,
        )
        .await?;
        destination.available_name(None)
    }

    pub(super) async fn resolve_visible_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<ResolvedVisiblePath> {
        let mut session = self.metadata.session();
        let mut ancestor_paths = HashMap::new();
        read::resolve_visible_inode(&mut session, &mut ancestor_paths, inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))
    }

    /// Returns `resolution`, the result of resolving or walking a path, after
    /// authorizing `rights` on the path's component that isn't a directory
    /// when the resolution stopped there, so the error that identifies the
    /// component reaches only a subject allowed to learn of it.
    pub(super) async fn authorize_non_directory_component<T>(
        &self,
        resolution: Result<T>,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<T> {
        if let Err(CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
            inode_id,
            ..
        })) = &resolution
        {
            self.authorize(*inode_id, rights, absence).await?;
        }
        resolution
    }

    pub(super) async fn resolve_visible_child(
        &self,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
    ) -> Result<Option<ResolvedVisiblePath>> {
        let name_key = NameKey::for_display_name(display_name);
        let Some(binding) = self
            .metadata
            .visible_child(parent_inode_id, &name_key)
            .await?
        else {
            return Ok(None);
        };
        self.resolve_visible_inode(binding.child_inode_id)
            .await
            .map(Some)
    }

    /// Requires the binding version supplied by the caller to still be current.
    pub(super) fn ensure_binding_version(
        &self,
        resolved: &ResolvedVisiblePath,
        expected_binding_version: &BindingVersionToken,
    ) -> Result<()> {
        let expected = self.decode_expected_binding_version(expected_binding_version)?;
        let Some(current) = resolved.binding_version else {
            return Err(CoreError::RootMutationForbidden);
        };
        if current != expected {
            return Err(CoreError::BindingVersionMismatch {
                inode_id: resolved.inode_id,
                expected_binding_version: expected_binding_version.clone(),
                actual_binding_version: Some(binding_version::encode(current, self.namespace_id)),
            });
        }
        Ok(())
    }

    /// The error for a binding version expected of an inode that is not visible.
    pub(super) fn hidden_inode_binding_error(
        &self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersionToken,
    ) -> CoreError {
        match self.decode_expected_binding_version(expected_binding_version) {
            Ok(_) => CoreError::BindingVersionMismatch {
                inode_id,
                expected_binding_version: expected_binding_version.clone(),
                actual_binding_version: None,
            },
            Err(error) => error,
        }
    }

    fn decode_expected_binding_version(
        &self,
        expected_binding_version: &BindingVersionToken,
    ) -> Result<DeltaPosition> {
        binding_version::decode(expected_binding_version, self.namespace_id).map_err(|error| {
            CoreError::InvalidCommitField {
                field: "expected_binding_version",
                message: format!("invalid expected binding version: {error}"),
            }
        })
    }

    /// Returns the current binding of `resolved`, a visible inode other than
    /// the root. The root has no binding.
    pub(super) async fn source_binding(
        &self,
        resolved: &ResolvedVisiblePath,
    ) -> Result<ResolvedBinding> {
        let binding = self
            .metadata
            .current_parent_binding_for_child(resolved.inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.to_string()))?;
        if Some(binding.parent_inode_id) != resolved.parent_inode_id {
            return Err(CoreError::PathNotFound(resolved.absolute_path.to_string()));
        }
        Ok(ResolvedBinding {
            parent_inode_id: binding.parent_inode_id,
            name_key: binding.name_key.clone(),
            display_name: binding
                .display_name()
                .expect("visible binding should be bound")
                .clone(),
            child_inode_id: binding.child_inode_id,
            child_kind: binding.child_kind,
            bind_position: binding.bind_position,
            child_created_by: binding.child_created_by.clone(),
            child_created_at_ms: binding.child_created_at_ms,
        })
    }

    pub(super) async fn resolve_parent_directory(
        &self,
        absolute_path: &AbsolutePath,
        absence: Absence<'_>,
    ) -> Result<InodeId> {
        let Some(parent_path) = absolute_path.parent() else {
            return Ok(ROOT_INODE_ID);
        };
        if parent_path.is_root() {
            return Ok(ROOT_INODE_ID);
        }
        let resolved = self
            .authorize_non_directory_component(
                self.metadata.resolve_visible_path(&parent_path).await,
                AccessRight::Create.into(),
                absence,
            )
            .await?;
        if resolved.inode_kind != InodeKind::Directory {
            self.authorize(resolved.inode_id, AccessRight::Create.into(), absence)
                .await?;
            return Err(CoreError::ExpectedDirectory {
                target: parent_path.as_str().to_owned(),
                kind: resolved.inode_kind,
            });
        }
        Ok(resolved.inode_id)
    }
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    /// Creates a directory at each missing component of `absolute_path`
    /// except the last, each under the one before it and the first under
    /// the deepest visible directory, and returns the last component, which
    /// is the final name of `absolute_path`, as a vacant name under the last
    /// directory.
    ///
    /// A walk of `absolute_path` returned `missing`, so the first component
    /// is vacant under the deepest visible directory, and each later
    /// component is under a directory that this operation created. The
    /// caller must authorize `create` on the deepest visible directory, which
    /// covers every name below it.
    pub(super) async fn create_missing_directories(
        &mut self,
        absolute_path: &AbsolutePath,
        missing: MissingPath,
    ) -> Result<AvailableName> {
        let mut parent_inode_id = missing.deepest_existing();
        let mut missing = missing.into_missing();
        let final_name = missing
            .pop()
            .expect("a walk that stops at a missing component should report it");
        for display_name in missing {
            let name =
                ResolvedDestination::at_path(absolute_path, parent_inode_id, display_name, None)
                    .available_name(None)?;
            let new_inode = self.next_inode_id()?;
            parent_inode_id = new_inode.inode_id();
            self.commit
                .create(new_inode, name, NewChild::Directory)
                .await?;
        }
        ResolvedDestination::at_path(absolute_path, parent_inode_id, final_name, None)
            .available_name(None)
    }
}

/// How the shared move/copy destination rule resolved.
pub(super) enum ReplaceDestination<'d> {
    /// Nothing visible occupies the destination.
    Vacant(AvailableName),
    /// A distinct file occupies it and `Replace` accepted it.
    Replaced(&'d ResolvedVisiblePath),
    /// The destination resolves to the moving inode itself: a same-slot
    /// respelling, such as a case-only rename, whose name key already
    /// belongs to the source.
    SameInode(AvailableName),
}

pub(super) fn classify_replace_destination<'d>(
    destination: &'d ResolvedDestination<'_>,
    behavior: DestinationBehavior,
    source_inode_id: InodeId,
) -> Result<ReplaceDestination<'d>> {
    if let Some(existing) = destination.replaced_occupant(behavior, source_inode_id) {
        ensure_file(existing, &destination.path)?;
        return Ok(ReplaceDestination::Replaced(existing));
    }
    // Any other occupant is the source itself, or a conflict that
    // `available_name` reports.
    let available = destination.available_name(Some(source_inode_id))?;
    Ok(match destination.occupant {
        Some(_) => ReplaceDestination::SameInode(available),
        None => ReplaceDestination::Vacant(available),
    })
}
