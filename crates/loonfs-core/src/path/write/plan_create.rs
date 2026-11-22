//! Publish plans that create, recover, or replace path content.

use super::planner::OperationPlanningSession;
use super::preconditions::{ensure_expected_inode, ExpectedFileState};
use super::publish_path_planning::{is_missing_visible_path, AvailableName, Destination, Target};
use crate::authorize::{check_rights, Absence};
use crate::commit::{CommitValidationError, NewChild};
use crate::error::{CoreError, Result};
use crate::metadata::{SubtreeTombstoneRecord, VisiblePathError};
use crate::path::mutation_path::{ensure_mutation_path, final_component};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::TombstoneRowAction;
use loonfs_types::options::RestoreDeletedDestination;
use loonfs_types::{
    AbsolutePath, AccessRight, CommitSeq, ContentRef, DestinationBehavior, DisplayName, InodeId,
    InodeKind, NameKey, ROOT_INODE_ID,
};

/// The directories above a path that a create resolved: the deepest one
/// that exists, and the names of the missing ones below it, from the top.
pub(super) struct ParentDirectories {
    /// The deepest directory that exists, whose `create` right covers every
    /// directory created below it.
    deepest_existing: InodeId,
    missing: Vec<DisplayName>,
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
        let child_inode_id = self.next_inode_id()?;
        self.validation.create(child_inode_id, &name, child).await
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
        let parents = self.resolve_parent_directories(absolute_path).await?;
        self.view()
            .authorize(
                parents.deepest_existing,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await?;
        self.create_parent_directories(parents).await?;
        let name = self.available_path(absolute_path).await?;
        let child_inode_id = self.next_inode_id()?;
        self.validation
            .create(child_inode_id, &name, NewChild::Directory)
            .await
    }

    pub(super) async fn plan_restore_deleted(
        &mut self,
        inode_id: InodeId,
        deletion_seq: CommitSeq,
        destination: RestoreDeletedDestination,
    ) -> Result<()> {
        let deletion = self.view().metadata.tombstone_in_trash(inode_id).await?;
        let (deletion, deleted_binding) = match &deletion {
            Some(
                deletion @ SubtreeTombstoneRecord {
                    action:
                        TombstoneRowAction::Set {
                            deleted_binding, ..
                        },
                    ..
                },
            ) => (deletion, deleted_binding),
            _ => {
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
                return Err(
                    CommitValidationError::RestoreDeletedTargetNotDeleted { inode_id }.into(),
                );
            }
        };
        let saved_parent = deleted_binding.parent_inode_id;
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
                Destination::name(saved_parent, &deleted_binding.display_name)
            }
        };
        let name = self
            .resolve_restore_deleted_destination(inode_id, saved_parent, destination)
            .await?;
        self.validation
            .restore_deleted(deletion, deletion_seq, &name)
            .await
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
        if destination.parent_inode_id != saved_parent {
            check_rights(mover, AccessRight::Read.into(), inode_id, Absence::Inode)?;
        }
        let name = view.ensure_vacant_destination(&destination).await?;
        view.authorize_relocation(inode_id, saved_parent, destination.parent_inode_id, mover)
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
        expected.ensure_revision(inode_id, actual)?;
        let binding = self.view().source_binding(target.resolved()).await?;
        self.validation
            .replace_file(&binding, &content_ref, None)
            .await
    }

    pub(super) async fn plan_put_file_content_ref(
        &mut self,
        absolute_path: &AbsolutePath,
        content_ref: ContentRef,
        behavior: DestinationBehavior,
        expected_file_state: Option<ExpectedFileState>,
    ) -> Result<()> {
        ensure_mutation_path(absolute_path)?;
        let target = self
            .view()
            .resolve_visible_path_for_authorization(
                absolute_path,
                AccessRight::Create.into(),
                Absence::Path(absolute_path.as_str()),
            )
            .await;

        let final_name = final_component(absolute_path)?;

        match target {
            Ok(existing) => {
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
                    expected.ensure_revision(existing.inode_id, Some(revision.revision_no))?;
                }
                let binding = self.view().source_binding(&existing).await?;
                self.validation
                    .replace_file(&binding, &content_ref, None)
                    .await
            }
            Err(error) if is_missing_visible_path(&error) => {
                let parents = self.resolve_parent_directories(absolute_path).await?;
                self.view()
                    .authorize(
                        parents.deepest_existing,
                        AccessRight::Create.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                if expected_file_state.is_some() {
                    return Err(CoreError::PathNotFound(absolute_path.as_str().to_owned()));
                }
                self.create_parent_directories(parents).await?;
                let name = self.available_path(absolute_path).await?;
                let child_inode_id = self.next_inode_id()?;
                self.validation
                    .create(child_inode_id, &name, NewChild::File(content_ref))
                    .await
            }
            Err(other) => Err(other),
        }
    }

    /// Resolves the directories above `absolute_path`, down to the deepest
    /// one that exists.
    ///
    /// A component that exists and isn't a directory is reported only after
    /// `create` is authorized on it.
    async fn resolve_parent_directories(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ParentDirectories> {
        let components = absolute_path.components();
        let above = components.split_last().map_or(&[][..], |(_, above)| above);
        let mut current_inode = ROOT_INODE_ID;
        let mut current_path = AbsolutePath::root();
        for (depth, component) in above.iter().enumerate() {
            let name_key = NameKey::for_display_name(&component.to_display_name());
            let Some(child) = self
                .view()
                .metadata
                .visible_child(current_inode, &name_key)
                .await?
            else {
                return Ok(ParentDirectories {
                    deepest_existing: current_inode,
                    missing: above[depth..]
                        .iter()
                        .map(|component| component.to_display_name())
                        .collect(),
                });
            };
            current_path = current_path.join(
                child
                    .display_name()
                    .expect("visible binding should be bound"),
            );
            if child.child_kind != InodeKind::Directory {
                self.view()
                    .authorize(
                        child.child_inode_id,
                        AccessRight::Create.into(),
                        Absence::Path(absolute_path.as_str()),
                    )
                    .await?;
                return Err(VisiblePathError::PathComponentNotDirectory {
                    absolute_path: current_path.to_string(),
                    inode_id: child.child_inode_id,
                    inode_kind: child.child_kind,
                }
                .into());
            }
            current_inode = child.child_inode_id;
        }
        Ok(ParentDirectories {
            deepest_existing: current_inode,
            missing: Vec::new(),
        })
    }

    /// Creates the missing directories of `parents`, each under the one
    /// before it.
    ///
    /// Each name is checked for vacancy after the directory above it is
    /// created, against the state that the create left. The caller
    /// authorizes the `create` right that covers every new directory once,
    /// on the deepest directory that exists.
    async fn create_parent_directories(&mut self, parents: ParentDirectories) -> Result<()> {
        let mut parent_inode_id = parents.deepest_existing;
        for display_name in &parents.missing {
            let destination = Destination::name(parent_inode_id, display_name);
            let name = self
                .view()
                .resolve_destination(destination)
                .await?
                .available_name(None)?;
            let child_inode_id = self.next_inode_id()?;
            self.validation
                .create(child_inode_id, &name, NewChild::Directory)
                .await?;
            parent_inode_id = child_inode_id;
        }
        Ok(())
    }

    /// Returns `absolute_path` as a vacant name, checked against the state
    /// that the operation's earlier creates left. The caller authorized
    /// `create` on the deepest directory above the path that existed before
    /// those creates.
    async fn available_path(&self, absolute_path: &AbsolutePath) -> Result<AvailableName> {
        self.view()
            .resolve_destination(Destination::Path(absolute_path))
            .await?
            .available_name(None)
    }
}
