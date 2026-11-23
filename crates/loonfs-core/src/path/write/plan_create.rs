//! Publish plans that create, recover, or replace path content.

use super::planner::OperationPlanningSession;
use super::preconditions::{ensure_expected_inode, ExpectedFileState};
use super::publish_path_planning::{AvailableName, Destination, ResolvedDestination, Target};
use crate::authorize::{check_rights, Absence};
use crate::commit::{CommitValidationError, NewChild};
use crate::error::{CoreError, Result};
use crate::metadata::VisiblePathWalk;
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{DeletedBinding, DeltaPosition, TombstoneRowAction};
use loonfs_types::options::RestoreDeletedDestination;
use loonfs_types::{
    AbsolutePath, AccessRight, CommitSeq, ContentRef, DestinationBehavior, DisplayName, InodeId,
    InodeKind, ROOT_INODE_ID,
};

/// A deleted item's deletion in the trash: the latest tombstone event of the
/// item's root, which is a `set` above the retention floor. Only
/// [`OperationPlanningSession::resolve_deletion_in_trash`] makes one, so a
/// restore that takes one revokes a deletion that is in the trash.
pub(crate) struct DeletionInTrash {
    root_inode_id: InodeId,
    position: DeltaPosition,
    deleted_binding: DeletedBinding,
}

impl DeletionInTrash {
    pub(crate) fn root_inode_id(&self) -> InodeId {
        self.root_inode_id
    }

    /// The position of the deletion's `set` event, which a revoke refers to.
    pub(crate) fn position(&self) -> DeltaPosition {
        self.position
    }
}

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    /// Plans a new directory or file at a vacant `destination`.
    pub(super) async fn plan_create(
        &mut self,
        destination: Destination<'_>,
        child: NewChild,
    ) -> Result<()> {
        destination.ensure_mutation()?;
        let destination = self.view().resolve_destination(destination).await?;
        let name = self.view().ensure_vacant_destination(&destination).await?;
        let new_inode = self.next_inode_id()?;
        self.commit.create(new_inode, name, child).await
    }

    /// Plans a new directory at `absolute_path`, and with `parents`, the
    /// missing directories above it.
    pub(super) async fn plan_create_directory(
        &mut self,
        absolute_path: &AbsolutePath,
        parents: bool,
    ) -> Result<()> {
        if !parents {
            return self
                .plan_create(Destination::Path(absolute_path), NewChild::Directory)
                .await;
        }
        ensure_mutation_path(absolute_path)?;
        let view = self.view();
        let walk = view
            .authorize_non_directory_component(
                view.metadata.walk_visible_path(absolute_path).await,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
        let name = match walk {
            VisiblePathWalk::Found(existing) => {
                let destination = ResolvedDestination::at_path(
                    absolute_path,
                    existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                    final_component(absolute_path)?,
                    Some(existing),
                );
                self.view().ensure_vacant_destination(&destination).await?
            }
            VisiblePathWalk::Missing {
                deepest_existing,
                missing,
                ..
            } => {
                self.view()
                    .authorize(
                        deepest_existing,
                        AccessRight::Create.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                self.create_missing_directories(absolute_path, deepest_existing, missing)
                    .await?
            }
        };
        let new_inode = self.next_inode_id()?;
        self.commit
            .create(new_inode, name, NewChild::Directory)
            .await
    }

    pub(super) async fn plan_restore_deleted(
        &mut self,
        inode_id: InodeId,
        deletion_seq: CommitSeq,
        destination: RestoreDeletedDestination,
    ) -> Result<()> {
        let deletion = self.resolve_deletion_in_trash(inode_id).await?;
        let saved_parent = deletion.deleted_binding.parent_inode_id;
        self.view()
            .authorize(saved_parent, AccessRight::Remove.into(), Absence::Inode)
            .await?;
        let destination = match &destination {
            RestoreDeletedDestination::Path(absolute_path) => Destination::Path(absolute_path),
            RestoreDeletedDestination::Name {
                parent_inode_id,
                display_name,
            } => Destination::name(*parent_inode_id, display_name),
            RestoreDeletedDestination::Recorded => {
                Destination::name(saved_parent, &deletion.deleted_binding.display_name)
            }
        };
        let name = self
            .resolve_restore_deleted_destination(inode_id, saved_parent, destination)
            .await?;
        self.commit
            .restore_deleted(deletion, deletion_seq, name)
            .await
    }

    /// Returns the deletion of `inode_id` that is in the trash.
    ///
    /// When the inode's latest tombstone event isn't a `set` in the trash,
    /// the restore fails with `RestoreDeletedTargetNotDeleted`, after the
    /// `remove` right is authorized on the inode's current parent, or on the
    /// inode when it has no parent.
    async fn resolve_deletion_in_trash(&self, inode_id: InodeId) -> Result<DeletionInTrash> {
        if let Some(deletion) = self.view().metadata.tombstone_in_trash(inode_id).await? {
            let position = deletion.position();
            if let TombstoneRowAction::Set {
                deleted_binding, ..
            } = deletion.action
            {
                return Ok(DeletionInTrash {
                    root_inode_id: deletion.root_inode_id,
                    position,
                    deleted_binding,
                });
            }
        }
        let authorization_target = self
            .view()
            .metadata
            .current_parent_binding_for_child(inode_id)
            .await?
            .map_or(inode_id, |binding| binding.parent_inode_id);
        self.view()
            .authorize(
                authorization_target,
                AccessRight::Remove.into(),
                Absence::Inode,
            )
            .await?;
        Err(CommitValidationError::RestoreDeletedTargetNotDeleted { inode_id }.into())
    }

    /// Resolves the destination of a restore, which has to be vacant, and
    /// authorizes moving the deleted inode there from its saved parent.
    async fn resolve_restore_deleted_destination(
        &self,
        inode_id: InodeId,
        saved_parent: InodeId,
        destination: Destination<'_>,
    ) -> Result<AvailableName> {
        destination.ensure_mutation()?;
        let view = self.view();
        let destination = view.resolve_destination(destination).await?;
        // A destination in the saved parent is a restore in place, which
        // needs no read right on the inode.
        let mover = view.mover_rights(inode_id).await?;
        if destination.parent_inode_id() != saved_parent {
            check_rights(mover, AccessRight::Read.into(), inode_id, Absence::Inode)?;
        }
        let name = view.ensure_vacant_destination(&destination).await?;
        view.authorize_relocation(inode_id, saved_parent, destination.parent_inode_id(), mover)
            .await?;
        Ok(name)
    }

    pub(super) async fn plan_put_file_revision_by_inode(
        &mut self,
        inode_id: InodeId,
        content_ref: ContentRef,
        expected: ExpectedFileState,
    ) -> Result<()> {
        let target = self
            .view()
            .resolve_authorized_target(Target::Inode(inode_id), AccessRight::Write.into())
            .await?;
        target.ensure_file()?;
        let actual = self.view().current_revision_no(inode_id).await?;
        expected.ensure_revision(actual)?;
        let binding = self.view().source_binding(target.resolved()).await?;
        self.commit.replace_file(binding, &content_ref, None).await
    }

    pub(super) async fn plan_put_file_content_ref(
        &mut self,
        absolute_path: &AbsolutePath,
        content_ref: ContentRef,
        behavior: DestinationBehavior,
        expected_file_state: Option<ExpectedFileState>,
    ) -> Result<()> {
        ensure_mutation_path(absolute_path)?;
        let view = self.view();
        let walk = view
            .authorize_non_directory_component(
                view.metadata.walk_visible_path(absolute_path).await,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;

        let final_name = final_component(absolute_path)?;

        match walk {
            VisiblePathWalk::Found(existing) => {
                if behavior == DestinationBehavior::NoReplace {
                    self.view()
                        .authorize(
                            existing.parent_inode_id.unwrap_or(ROOT_INODE_ID),
                            AccessRight::Create.into(),
                            Absence::Path(absolute_path.as_str()),
                        )
                        .await?;
                    return Err(CoreError::DestinationExists {
                        path: absolute_path.as_str().to_owned(),
                        existing_display_name: Some(existing.display_name.clone()),
                    });
                }
                self.view()
                    .authorize(
                        existing.inode_id,
                        AccessRight::Write.into(),
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
                let revision = self
                    .view()
                    .metadata
                    .latest_revision_head(existing.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(absolute_path.as_str().to_owned()))?;
                if let Some(expected) = expected_file_state {
                    expected.ensure_revision(Some(revision.revision_no))?;
                }
                let binding = self.view().source_binding(&existing).await?;
                self.commit.replace_file(binding, &content_ref, None).await
            }
            VisiblePathWalk::Missing {
                deepest_existing,
                missing,
                ..
            } => {
                self.view()
                    .authorize(
                        deepest_existing,
                        AccessRight::Create.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                if expected_file_state.is_some() {
                    return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
                }
                let name = self
                    .create_missing_directories(absolute_path, deepest_existing, missing)
                    .await?;
                let new_inode = self.next_inode_id()?;
                self.commit
                    .create(new_inode, name, NewChild::File(content_ref))
                    .await
            }
        }
    }

    /// Creates a directory at each name of `missing` except the last, each
    /// under the one before it and the first under `deepest_existing`, and
    /// returns the last name of `missing`, which is the final name of
    /// `absolute_path`, as a vacant name under the last directory.
    ///
    /// A walk of `absolute_path` returned `deepest_existing` and `missing`,
    /// so the first name is vacant under `deepest_existing`, and each later
    /// name is under a directory that this operation created. The caller
    /// authorized `create` on `deepest_existing`, which covers every name
    /// below it.
    async fn create_missing_directories(
        &mut self,
        absolute_path: &AbsolutePath,
        deepest_existing: InodeId,
        mut missing: Vec<DisplayName>,
    ) -> Result<AvailableName> {
        let final_name = missing
            .pop()
            .expect("a walk that stops at a missing component should report it");
        let mut parent_inode_id = deepest_existing;
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
