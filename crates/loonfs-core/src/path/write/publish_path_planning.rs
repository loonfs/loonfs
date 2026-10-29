//! [`PlanningView`]: the read-only part of planning one operation, with
//! the target and destination resolution that the planners share. The
//! authorization checks are in [`super::planning_authorization`].

use super::preconditions::ensure_expected_inode;
use crate::authorize::{Absence, Authorizer};
use crate::binding_version;
use crate::commit::{AppendedContent, CommitOperation, ResolvedBinding};
use crate::error::{CoreError, Result};

use crate::metadata::{MetadataView, ResolvedVisiblePath, RevisionRecord, VisiblePathError};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use crate::path::read;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::DeltaPosition;
use loonfs_types::{
    AbsolutePath, BindingVersion as BindingVersionToken, DestinationBehavior, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceAccess, NamespaceId, ROOT_INODE_ID,
};
use loonfs_types::{AccessRight, AccessRights};

use std::collections::HashMap;

pub(super) fn is_missing_visible_path(error: &CoreError) -> bool {
    matches!(error, CoreError::PathNotFound(_))
}

/// One filesystem operation compiled into the commit operations it needs,
/// with the bytes it appends when it is an append.
pub(super) struct CompiledFilesystemOperation {
    pub(super) ops: Vec<CommitOperation>,
    pub(super) appended: Option<AppendedContent>,
}

impl CompiledFilesystemOperation {
    pub(super) fn new(ops: Vec<CommitOperation>) -> Self {
        Self {
            ops,
            appended: None,
        }
    }
}

/// What one operation plans against: the namespace, its access mode, the
/// request's authorizer, and the metadata that the operation resolves
/// against, which includes the commit's earlier operations.
pub(super) struct PlanningView<'a, S: ObjectStore + ?Sized> {
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) access: &'a NamespaceAccess,
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

/// A target resolved to its visible inode.
pub(super) struct ResolvedTarget<'a> {
    pub(super) resolved: ResolvedVisiblePath,
    pub(super) target: Target<'a>,
}

impl<'a> ResolvedTarget<'a> {
    pub(super) fn inode_id(&self) -> InodeId {
        self.resolved.inode_id
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
        if self.resolved.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                target: self.label(),
                kind: self.resolved.inode_kind,
            });
        }
        Ok(())
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

/// Where a create, move, copy, or undelete binds its inode.
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
pub(super) struct ResolvedDestination<'a> {
    pub(super) parent_inode_id: InodeId,
    pub(super) name: DisplayName,
    /// The destination as error messages name it: the request's path, or
    /// the parent's path joined with the name.
    pub(super) path: String,
    pub(super) occupant: Option<ResolvedVisiblePath>,
    pub(super) absence: Absence<'a>,
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
                self.resolve_visible_path_for_authorization(
                    path,
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
                let absence = Absence::Path(path.as_str());
                let parent_inode_id = self.resolve_parent_directory(path, absence).await?;
                let name = final_component(path)?;
                let occupant = match self.metadata.resolve_visible_path(path).await {
                    Ok(existing) => Some(existing),
                    Err(error) if is_missing_visible_path(&error) => None,
                    Err(error) => return Err(error),
                };
                Ok(ResolvedDestination {
                    parent_inode_id,
                    name,
                    path: path.as_str().to_owned(),
                    occupant,
                    absence,
                })
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

    /// Authorizes `create` on the destination's parent and then requires the
    /// destination to be vacant.
    pub(super) async fn ensure_vacant_destination(
        &self,
        destination: &ResolvedDestination<'_>,
    ) -> Result<()> {
        self.authorize(
            destination.parent_inode_id,
            AccessRight::Create.into(),
            destination.absence,
        )
        .await?;
        match &destination.occupant {
            Some(existing) => Err(CoreError::DestinationExists {
                path: destination.path.clone(),
                existing_display_name: Some(existing.display_name.clone()),
            }),
            None => Ok(()),
        }
    }

    pub(super) async fn ensure_vacant_path(&self, path: &AbsolutePath) -> Result<()> {
        match self.metadata.resolve_visible_path(path).await {
            Ok(existing) => Err(CoreError::DestinationExists {
                path: path.as_str().to_owned(),
                existing_display_name: Some(existing.display_name),
            }),
            Err(error) if is_missing_visible_path(&error) => Ok(()),
            Err(error) => Err(error),
        }
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

    pub(super) async fn resolve_visible_path_for_authorization(
        &self,
        absolute_path: &AbsolutePath,
        rights: AccessRights,
        absence: Absence<'_>,
    ) -> Result<ResolvedVisiblePath> {
        match self.metadata.resolve_visible_path(absolute_path).await {
            Err(
                error @ CoreError::VisiblePath(VisiblePathError::PathComponentNotDirectory {
                    inode_id,
                    ..
                }),
            ) => {
                self.authorize(inode_id, rights, absence).await?;
                Err(error)
            }
            result => result,
        }
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

    pub(super) async fn source_binding(
        &self,
        resolved: &ResolvedVisiblePath,
    ) -> Result<ResolvedBinding> {
        let parent_inode_id = resolved
            .parent_inode_id
            .ok_or(CoreError::RootMutationForbidden)?;
        let binding = self
            .metadata
            .current_parent_binding_for_child(resolved.inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(resolved.absolute_path.to_string()))?;
        if binding.parent_inode_id != parent_inode_id {
            return Err(CoreError::PathNotFound(resolved.absolute_path.to_string()));
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
            .resolve_visible_path_for_authorization(
                &parent_path,
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
