//! Publish plans that move or copy visible paths.

use super::planner::OperationPlanningSession;
use super::preconditions::{
    ensure_expected_inode, validate_expected_file_state, ExpectedFileState, PreconditionFields,
};
use super::publish_path_planning::{
    classify_replace_destination, CompiledFilesystemOperation, Destination, ReplaceDestination,
    ResolvedDestination, Target,
};
use crate::authorize::Replacement;
use crate::commit::{CommitOperation, CommitValidationError};
use crate::error::{CoreError, Result};
use crate::metadata::ResolvedVisiblePath;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AccessRight, AccessRights, AttributesRevisionNo, BindingVersion, DestinationPrecondition,
};

impl<S: ObjectStore + ?Sized> OperationPlanningSession<'_, S> {
    /// Plans a move of `source` to `destination`, after checking the
    /// source's binding version when the request names one.
    pub(super) async fn plan_move(
        &self,
        source: Target<'_>,
        expected_binding_version: Option<&BindingVersion>,
        destination: Destination<'_>,
        precondition: &DestinationPrecondition,
    ) -> Result<CompiledFilesystemOperation> {
        let expected_destination = validate_expected_file_state(
            precondition.behavior,
            precondition.expected_inode_id,
            precondition.expected_revision_no,
            PreconditionFields::Destination,
        )?;
        destination.ensure_mutation()?;
        let remove = AccessRights::from(AccessRight::Remove);
        let source = self.view.resolve_target(source, remove).await?;
        self.view
            .authorize(source.parent_inode_id()?, remove, source.absence())
            .await?;
        // Replace compiles to an atomic delete-plus-rename: the destination
        // file's delete and the source's rebind are written in one commit, and the
        // rename's target-name check observes the in-commit unbind. Mirrors
        // put: only a file destination can be replaced, and a path never
        // replaces itself.
        let destination = self.view.resolve_destination(destination).await?;
        self.view
            .authorize_destination(
                &destination,
                precondition.behavior,
                source.inode_id(),
                Replacement::RemovesEntry,
            )
            .await?;
        if let Some(expected) = expected_binding_version {
            self.view
                .ensure_binding_version(&source.resolved, expected)?;
        }
        self.plan_rename(
            &source.resolved,
            destination,
            precondition,
            expected_destination,
        )
        .await
    }

    async fn plan_rename(
        &self,
        source: &ResolvedVisiblePath,
        destination: ResolvedDestination<'_>,
        precondition: &DestinationPrecondition,
        expected_destination: Option<ExpectedFileState>,
    ) -> Result<CompiledFilesystemOperation> {
        let ResolvedDestination {
            parent_inode_id,
            name,
            path,
            occupant,
            ..
        } = destination;
        let replaced =
            classify_replace_destination(occupant, precondition.behavior, source.inode_id, &path)?;
        let mut ops = Vec::new();
        let current_source_binding = self.view.source_binding(source).await?;
        self.view
            .authorize_relocation(
                source.inode_id,
                current_source_binding.parent_inode_id,
                parent_inode_id,
            )
            .await?;
        match &replaced {
            ReplaceDestination::Replaced(existing) => {
                ensure_expected_inode(
                    existing,
                    expected_destination.map(|expected| expected.inode_id),
                    &name,
                )?;
                self.ensure_expected_destination_revision(existing, expected_destination)
                    .await?;
                ops.push(CommitOperation::DeleteFile {
                    inode_id: existing.inode_id,
                    source_binding: self.view.source_binding(existing).await?,
                });
            }
            ReplaceDestination::Vacant => {
                if expected_destination.is_some() {
                    return Err(CoreError::PathNotFound(path));
                }
            }
            // The destination is the source's own binding: a same-slot
            // respelling (case-only rename, normalization-equal spelling).
            // The name is legitimately present, bound to the source, so
            // there is nothing to delete; the rename alone rebinds the new
            // spelling. An unchanged spelling stays a conflict: there is
            // nothing to rename.
            ReplaceDestination::SameInode => {
                ensure_expected_inode(
                    source,
                    expected_destination.map(|expected| expected.inode_id),
                    &name,
                )?;
                self.ensure_expected_destination_revision(source, expected_destination)
                    .await?;
                if name.as_str() == source.display_name {
                    return Err(CoreError::DestinationExists {
                        path,
                        existing_display_name: Some(source.display_name.clone()),
                    });
                }
            }
        }
        ops.push(CommitOperation::Rename {
            inode_id: source.inode_id,
            source_binding: current_source_binding,
            new_parent_inode_id: parent_inode_id,
            new_display_name: name,
        });
        Ok(CompiledFilesystemOperation::new(ops))
    }

    /// Plans a copy of the file `source` to `destination`.
    pub(super) async fn plan_copy(
        &mut self,
        source: Target<'_>,
        destination: Destination<'_>,
        precondition: &DestinationPrecondition,
    ) -> Result<CompiledFilesystemOperation> {
        let expected_destination = validate_expected_file_state(
            precondition.behavior,
            precondition.expected_inode_id,
            precondition.expected_revision_no,
            PreconditionFields::Destination,
        )?;
        destination.ensure_mutation()?;
        let source = self
            .view
            .resolve_authorized_target(source, AccessRight::Read.into())
            .await?;
        source.ensure_file()?;
        // Replace mirrors put onto an existing file: the copy appends a new
        // revision to the destination inode, keeping its identity and revision
        // history. Only a file destination can be replaced, and a path never
        // replaces itself.
        let destination = self.view.resolve_destination(destination).await?;
        self.view
            .authorize_destination(
                &destination,
                precondition.behavior,
                source.inode_id(),
                Replacement::WritesFile,
            )
            .await?;
        self.plan_copy_into(
            &source.resolved,
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
    ) -> Result<CompiledFilesystemOperation> {
        let ResolvedDestination {
            parent_inode_id,
            name,
            path,
            occupant,
            ..
        } = destination;
        let replaced =
            classify_replace_destination(occupant, precondition.behavior, source.inode_id, &path)?;
        let revision = self
            .view
            .metadata
            .latest_revision_head(source.inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(source.absolute_path.to_string()))?;
        let mut ops = Vec::new();
        match &replaced {
            // Copying a file onto its own binding would delete the source to
            // make room for its copy; that is a conflict, with or without
            // `Replace`, the same-file rule every copy tool applies.
            ReplaceDestination::SameInode => {
                return Err(CoreError::DestinationExists {
                    path,
                    existing_display_name: Some(source.display_name.clone()),
                })
            }
            ReplaceDestination::Replaced(existing) => {
                ensure_expected_inode(
                    existing,
                    expected_destination.map(|expected| expected.inode_id),
                    &name,
                )?;
                let existing_revision = self
                    .view
                    .metadata
                    .latest_revision_head(existing.inode_id)
                    .await?
                    .ok_or_else(|| CoreError::PathNotFound(path.clone()))?;
                let base_revision_no = expected_destination
                    .and_then(|state| state.revision_no)
                    .unwrap_or(existing_revision.revision_no);
                ops.push(CommitOperation::ReplaceFile {
                    inode_id: existing.inode_id,
                    base_revision_no,
                    content_ref: revision.content_ref.clone(),
                    append_offset: None,
                });
            }
            ReplaceDestination::Vacant => {
                if expected_destination.is_some() {
                    return Err(CoreError::PathNotFound(path));
                }
                let child_inode_id = self.next_inode_id();
                ops.push(CommitOperation::CreateFile {
                    child_inode_id,
                    parent_inode_id,
                    display_name: name,
                    content_ref: revision.content_ref.clone(),
                });
                // A copy to a vacant destination is a new resource that
                // stands for the source, so it starts with the source's
                // attributes. The new inode is at revision 0 with an empty
                // map, so copying them over is a second internal operation
                // with its own feed event. Copying over an existing file
                // changes nothing about that file's attributes: the
                // destination is a resource that already exists and keeps
                // what it contains.
                let source_attributes = self
                    .view
                    .metadata
                    .attributes(source.inode_id)
                    .await?
                    .attributes;
                if !source_attributes.is_empty() {
                    // Both operations include the one ID generated above, so
                    // they refer to the same new inode.
                    ops.push(CommitOperation::UpdateAttributes {
                        inode_id: child_inode_id,
                        base_attributes_revision_no: AttributesRevisionNo(0),
                        attributes: source_attributes,
                    });
                }
            }
        }
        Ok(CompiledFilesystemOperation::new(ops))
    }

    async fn ensure_expected_destination_revision(
        &self,
        existing: &ResolvedVisiblePath,
        expected_destination: Option<ExpectedFileState>,
    ) -> Result<()> {
        let Some(expected) = expected_destination.and_then(|state| state.revision_no) else {
            return Ok(());
        };
        let actual = self
            .view
            .metadata
            .latest_revision_head(existing.inode_id)
            .await?
            .map(|revision| revision.revision_no);
        if actual != Some(expected) {
            return Err(CommitValidationError::BaseRevisionMismatch {
                inode_id: existing.inode_id,
                expected,
                actual,
            }
            .into());
        }
        Ok(())
    }
}
