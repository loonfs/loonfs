//! Publish plans that move or copy visible paths.

use super::planner::OperationPlanningSession;
use super::preconditions::{
    ensure_expected_inode, validate_expected_file_state, ExpectedFileState, PreconditionFields,
};
use super::publish_path_planning::{
    classify_replace_destination, Destination, ReplaceDestination, ResolvedDestination, Target,
};
use crate::authorize::{check_rights, Replacement};
use crate::commit::engine::TemporaryMove;
use crate::commit::NewChild;
use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{AccessRight, AccessRights, BindingVersion, DestinationPrecondition};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, '_, S> {
    /// Plans a move of `source` to `destination`, after checking the
    /// source's binding version when the request names one.
    pub(super) async fn plan_move(
        &mut self,
        source: Target<'_>,
        expected_binding_version: Option<&BindingVersion>,
        destination: Destination<'_>,
        precondition: &DestinationPrecondition,
    ) -> Result<()> {
        let expected_destination = validate_expected_file_state(
            precondition.behavior,
            precondition.expected_inode_id,
            precondition.expected_revision_no,
            PreconditionFields::Destination,
        )?;
        destination.ensure_mutation()?;
        let remove = AccessRights::from(AccessRight::Remove);
        let source = self.view().resolve_target(source, remove).await?;
        // The second leg of a merge's move through a temporary name leaves
        // the temporary directory, which the subject needs no right on,
        // because the temporary binding doesn't survive the commit. The first
        // leg checked the remove right on the inode's directory.
        // The second leg also skips the read check on the inode, which the
        // first leg made where the move starts. Both checks come before any
        // conflict check, so a subject with no right on the inode gets the
        // not-found error.
        let parent_inode_id = source.parent_inode_id()?;
        let mover = self.view().mover_rights(source.inode_id()).await?;
        if !matches!(self.temporary_move, Some(TemporaryMove::Back { .. })) {
            self.view()
                .authorize(parent_inode_id, remove, source.absence())
                .await?;
            check_rights(
                mover,
                AccessRight::Read.into(),
                source.inode_id(),
                source.absence(),
            )?;
        }
        // Replace is an atomic delete-plus-rename: the destination file's
        // delete and the source's rebind are written in one commit, and the
        // destination is checked for vacancy again after the delete. Mirrors
        // put: only a file destination can be replaced, and a path never
        // replaces itself.
        let destination = self.view().resolve_destination(destination).await?;
        // The first leg of a merge's move through a temporary name enters
        // the temporary directory, which needs no create right for the same
        // reason. The second leg checks the create right on the inode's new
        // directory.
        if self.temporary_move != Some(TemporaryMove::Away) {
            self.view()
                .authorize_destination(
                    &destination,
                    precondition.behavior,
                    source.inode_id(),
                    Replacement::RemovesEntry,
                )
                .await?;
        }
        if let Some(expected) = expected_binding_version {
            self.view()
                .ensure_binding_version(source.resolved(), expected)?;
        }
        self.plan_rename(
            source.resolved(),
            mover,
            destination,
            precondition,
            expected_destination,
        )
        .await
    }

    async fn plan_rename(
        &mut self,
        source: &ResolvedVisiblePath,
        mover: AccessRights,
        destination: ResolvedDestination<'_>,
        precondition: &DestinationPrecondition,
        expected_destination: Option<ExpectedFileState>,
    ) -> Result<()> {
        let replaced =
            classify_replace_destination(&destination, precondition.behavior, source.inode_id)?;
        let parent_inode_id = destination.parent_inode_id();
        let name = destination.name();
        let path = destination.path();
        let current_source_binding = self.view().source_binding(source).await?;
        // A merge's move through a temporary name changes the inode's rights
        // once, from the directory that it leaves in the first leg to the
        // directory that it enters in the second.
        let relocated_from = match self.temporary_move {
            None => Some(current_source_binding.parent_inode_id()),
            Some(TemporaryMove::Away) => None,
            Some(TemporaryMove::Back { from_parent }) => Some(from_parent),
        };
        if let Some(from_parent) = relocated_from {
            self.view()
                .authorize_relocation(source.inode_id, from_parent, parent_inode_id, mover)
                .await?;
        }
        let available = match replaced {
            ReplaceDestination::Replaced(existing) => {
                ensure_expected_inode(
                    existing,
                    expected_destination.map(|expected| expected.inode_id),
                    name,
                )?;
                self.ensure_expected_destination_revision(existing, expected_destination)
                    .await?;
                let existing_binding = self.view().source_binding(existing).await?;
                self.commit.delete(existing_binding).await?;
                // The delete unbinds the file, so the destination is vacant
                // in the state that the delete left.
                self.view()
                    .resolve_destination(Destination::name(parent_inode_id, name))
                    .await?
                    .available_name(Some(source.inode_id))?
            }
            ReplaceDestination::Vacant(available) => {
                if expected_destination.is_some() {
                    return Err(CoreError::PathNotFound(path.to_owned()));
                }
                available
            }
            // The destination is the source's own binding: a same-slot
            // respelling (case-only rename, normalization-equal spelling).
            // The name is legitimately present, bound to the source, so
            // there is nothing to delete; the rename alone rebinds the new
            // spelling. An unchanged spelling stays a conflict: there is
            // nothing to rename.
            ReplaceDestination::SameInode(available) => {
                ensure_expected_inode(
                    source,
                    expected_destination.map(|expected| expected.inode_id),
                    name,
                )?;
                self.ensure_expected_destination_revision(source, expected_destination)
                    .await?;
                if name.as_str() == source.display_name {
                    return Err(CoreError::DestinationExists {
                        path: path.to_owned(),
                        existing_display_name: Some(source.display_name.clone()),
                    });
                }
                available
            }
        };
        self.commit.rename(current_source_binding, available).await
    }

    /// Plans a copy of the file `source` to `destination`.
    pub(super) async fn plan_copy(
        &mut self,
        source: Target<'_>,
        destination: Destination<'_>,
        precondition: &DestinationPrecondition,
    ) -> Result<()> {
        let expected_destination = validate_expected_file_state(
            precondition.behavior,
            precondition.expected_inode_id,
            precondition.expected_revision_no,
            PreconditionFields::Destination,
        )?;
        destination.ensure_mutation()?;
        let source = self
            .view()
            .resolve_authorized_target(source, AccessRight::Read.into())
            .await?;
        source.ensure_file()?;
        // Replace mirrors put onto an existing file: the copy appends a new
        // revision to the destination inode, keeping its identity and revision
        // history. Only a file destination can be replaced, and a path never
        // replaces itself.
        let destination = self.view().resolve_destination(destination).await?;
        self.view()
            .authorize_destination(
                &destination,
                precondition.behavior,
                source.inode_id(),
                Replacement::WritesFile,
            )
            .await?;
        self.plan_copy_into(
            source.resolved(),
            destination,
            precondition,
            expected_destination,
        )
        .await
    }

    async fn plan_copy_into(
        &mut self,
        source: &ResolvedVisiblePath,
        destination: ResolvedDestination<'_>,
        precondition: &DestinationPrecondition,
        expected_destination: Option<ExpectedFileState>,
    ) -> Result<()> {
        let replaced =
            classify_replace_destination(&destination, precondition.behavior, source.inode_id)?;
        let name = destination.name();
        let path = destination.path();
        let revision = self
            .view()
            .metadata
            .latest_revision_head(source.inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(source.absolute_path.to_string()))?;
        match replaced {
            // Copying a file onto its own binding would delete the source to
            // make room for its copy; that is a conflict, with or without
            // `Replace`, the same-file rule every copy tool applies.
            ReplaceDestination::SameInode(_) => Err(CoreError::DestinationExists {
                path: path.to_owned(),
                existing_display_name: Some(source.display_name.clone()),
            }),
            ReplaceDestination::Replaced(existing) => {
                ensure_expected_inode(
                    existing,
                    expected_destination.map(|expected| expected.inode_id),
                    name,
                )?;
                let existing_revision = self
                    .view()
                    .metadata
                    .latest_revision_head(existing.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(path.to_owned()))?;
                if let Some(expected) = expected_destination {
                    expected
                        .ensure_revision(existing.inode_id, Some(existing_revision.revision_no))?;
                }
                let binding = self.view().source_binding(existing).await?;
                self.commit
                    .replace_file(binding, &revision.content_ref, None)
                    .await
            }
            ReplaceDestination::Vacant(available) => {
                if expected_destination.is_some() {
                    return Err(CoreError::PathNotFound(path.to_owned()));
                }
                let child_inode_id = self.next_inode_id()?;
                self.commit
                    .create(
                        child_inode_id,
                        available,
                        NewChild::File(revision.content_ref),
                    )
                    .await?;
                // A copy to a vacant destination is a new resource that
                // stands for the source, so it starts with the source's
                // attributes. The new inode is at revision 0 with an empty
                // map, so copying them over is a second internal operation
                // with its own feed event. Copying over an existing file
                // changes nothing about that file's attributes: the
                // destination is a resource that already exists and keeps
                // what it contains.
                let source_attributes = self
                    .view()
                    .metadata
                    .attributes(source.inode_id)
                    .await?
                    .attributes;
                if source_attributes.is_empty() {
                    return Ok(());
                }
                // The attribute update resolves the inode that the create
                // wrote, after the create, so both write to the same new
                // inode.
                let child = self
                    .view()
                    .resolve_target(Target::Inode(child_inode_id), AccessRight::Write.into())
                    .await?;
                self.commit
                    .update_attributes(child, &source_attributes)
                    .await
            }
        }
    }

    async fn ensure_expected_destination_revision(
        &self,
        existing: &ResolvedVisiblePath,
        expected_destination: Option<ExpectedFileState>,
    ) -> Result<()> {
        let Some(expected) = expected_destination.filter(ExpectedFileState::expects_revision)
        else {
            return Ok(());
        };
        let actual = self.view().current_revision_no(existing.inode_id).await?;
        expected.ensure_revision(existing.inode_id, actual)
    }
}
