//! Planning moves and copies: `move_path`, `move_by_inode`, and `copy_by_inode`.

use super::files::{file, final_name};
use super::planner::{
    ensure_expected_inode, ensure_expected_revision, path_conflict, root_mutation, Planner,
};
use crate::entries::path_lookup_error;
use crate::error::{MountSimError, Result};
use crate::tokens::decode_binding_version;
use loonfs_model::metadata::{DeltaPosition, InodeRecord};
use loonfs_model::visibility::PathLookup;
use loonfs_types::{
    AbsolutePath, AttributesRevisionNo, BindingVersion, DestinationBehavior,
    DestinationPrecondition, DisplayName, ErrorCode, ExpectedFileState, InodeId, InodeKind,
    PreconditionFields, ROOT_INODE_ID,
};

/// What occupies the destination of a move or copy.
enum Destination {
    /// Nothing visible.
    Vacant,
    /// A file that the operation replaces.
    Replaced(InodeRecord),
    /// The moving inode itself, as in a rename that changes only the case of a name.
    SameInode,
}

impl Planner<'_> {
    pub(super) fn move_path(
        &mut self,
        source_path: &AbsolutePath,
        destination_path: &AbsolutePath,
        precondition: &DestinationPrecondition,
    ) -> Result<()> {
        let expected = resolve_precondition(precondition)?;
        final_name(source_path)?;
        let display_name = final_name(destination_path)?;
        let source = match self.view().resolve_path(source_path) {
            PathLookup::Found { inode, .. } => inode.inode_id,
            lookup => return Err(path_lookup_error(&lookup)),
        };
        let parent_inode_id = self.parent_directory(destination_path)?;
        let occupant = match self.view().resolve_path(destination_path) {
            PathLookup::Found { inode, .. } => Some(inode.clone()),
            PathLookup::NotFound { .. } | PathLookup::NotADirectory { .. } => None,
        };
        let destination = classify(occupant, precondition.behavior, source)?;
        self.finish_move(
            source,
            parent_inode_id,
            &display_name,
            destination,
            expected,
        )
    }

    pub(super) fn move_by_inode(
        &mut self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        precondition: &DestinationPrecondition,
    ) -> Result<()> {
        let expected = resolve_precondition(precondition)?;
        if inode_id == ROOT_INODE_ID {
            return Err(root_mutation());
        }
        self.visible(inode_id)?;
        self.directory(parent_inode_id)?;
        let occupant = self
            .view()
            .visible_child(parent_inode_id, display_name)
            .map(|entry| entry.child.clone());
        self.ensure_binding_version(inode_id, expected_binding_version)?;
        let destination = classify(occupant, precondition.behavior, inode_id)?;
        self.finish_move(
            inode_id,
            parent_inode_id,
            display_name,
            destination,
            expected,
        )
    }

    pub(super) fn copy_by_inode(
        &mut self,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        precondition: &DestinationPrecondition,
    ) -> Result<()> {
        let expected = resolve_precondition(precondition)?;
        if inode_id == ROOT_INODE_ID {
            return Err(root_mutation());
        }
        file(&self.visible(inode_id)?)?;
        self.directory(parent_inode_id)?;
        let occupant = self
            .view()
            .visible_child(parent_inode_id, display_name)
            .map(|entry| entry.child.clone());
        let source_content = self
            .view()
            .current_revision(inode_id)
            .expect("a visible file should have a revision")
            .content_ref
            .clone();
        match classify(occupant, precondition.behavior, inode_id)? {
            // Copying a file onto its own name would delete the source to make room for the copy.
            Destination::SameInode => Err(path_conflict("a file can't be copied onto itself")),
            // A replacing copy appends a revision to the destination, which keeps its identity,
            // its history, and its attributes.
            Destination::Replaced(existing) => {
                ensure_expected_inode(expected.map(|state| state.inode_id), existing.inode_id)?;
                ensure_expected_revision(
                    &self.view(),
                    existing.inode_id,
                    expected.and_then(|state| state.revision_no),
                )?;
                self.add_revision(existing.inode_id, source_content, None);
                Ok(())
            }
            // A copy to a vacant name is a new file that starts with the source's attributes,
            // as its own event.
            Destination::Vacant => {
                if expected.is_some() {
                    return Err(no_destination(display_name));
                }
                let attributes = self.view().attributes(inode_id).attributes;
                let copy = self.create_file_at(parent_inode_id, display_name, source_content);
                if !attributes.is_empty() {
                    self.set_attributes(copy, AttributesRevisionNo(0), attributes);
                }
                Ok(())
            }
        }
    }

    /// Returns a visible directory, or the error for a destination parent that isn't one.
    fn directory(&self, inode_id: InodeId) -> Result<()> {
        match self.visible(inode_id)?.inode_kind {
            InodeKind::Directory => Ok(()),
            InodeKind::File => Err(path_conflict("the destination parent isn't a directory")),
        }
    }

    /// Checks that an inode's current binding has the version that the caller read.
    pub(super) fn ensure_binding_version(
        &self,
        inode_id: InodeId,
        expected: &BindingVersion,
    ) -> Result<()> {
        let Some(expected_position) = decode_binding_version(self.namespace_id, expected) else {
            return Err(MountSimError::invalid_request(
                "`expected_binding_version` is malformed or belongs to another namespace",
            )
            .with_param("expected_binding_version"));
        };
        let view = self.view();
        let binding = view.current_binding(inode_id).ok_or_else(root_mutation)?;
        let actual = DeltaPosition {
            seq: binding.committed_seq,
            delta_index: binding.delta_index,
        };
        if actual == expected_position {
            return Ok(());
        }
        Err(MountSimError::new(
            ErrorCode::BindingVersionMismatch,
            "the entry's binding changed since the caller read it",
        )
        .with_details(|details| {
            details.inode_id = Some(inode_id);
            details.expected_binding_version = Some(expected.clone());
            details.actual_binding_version = Some(view.binding_version(binding));
        }))
    }

    fn finish_move(
        &mut self,
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: &DisplayName,
        destination: Destination,
        expected: Option<ExpectedFileState>,
    ) -> Result<()> {
        let expected_inode_id = expected.map(|state| state.inode_id);
        let expected_revision_no = expected.and_then(|state| state.revision_no);
        match destination {
            // A replacing move deletes the destination file and binds the source in its place.
            Destination::Replaced(existing) => {
                ensure_expected_inode(expected_inode_id, existing.inode_id)?;
                ensure_expected_revision(&self.view(), existing.inode_id, expected_revision_no)?;
                self.delete(existing.inode_id);
            }
            Destination::Vacant => {
                if expected.is_some() {
                    return Err(no_destination(display_name));
                }
            }
            // A move onto the inode's own name changes the name's spelling. An unchanged spelling
            // has nothing to rename.
            Destination::SameInode => {
                ensure_expected_inode(expected_inode_id, inode_id)?;
                ensure_expected_revision(&self.view(), inode_id, expected_revision_no)?;
                let current = self
                    .view()
                    .current_binding(inode_id)
                    .map(crate::model_view::display_name);
                if current.as_ref() == Some(display_name) {
                    return Err(path_conflict(format!(
                        "`{display_name}` already has this name"
                    )));
                }
            }
        }
        if self.view().would_create_cycle(inode_id, parent_inode_id) {
            return Err(MountSimError::new(
                ErrorCode::WouldCycle,
                "the move would place a directory inside itself",
            ));
        }
        self.rebind(inode_id, parent_inode_id, display_name);
        Ok(())
    }
}

fn resolve_precondition(
    precondition: &DestinationPrecondition,
) -> Result<Option<ExpectedFileState>> {
    precondition
        .resolve(PreconditionFields::Destination)
        .map_err(|error| MountSimError::invalid_request(error.to_string()))
}

fn classify(
    occupant: Option<InodeRecord>,
    behavior: DestinationBehavior,
    source_inode_id: InodeId,
) -> Result<Destination> {
    match occupant {
        None => Ok(Destination::Vacant),
        Some(existing) if existing.inode_id == source_inode_id => Ok(Destination::SameInode),
        Some(_) if behavior == DestinationBehavior::NoReplace => {
            Err(path_conflict("the destination already exists"))
        }
        Some(existing) => match existing.inode_kind {
            InodeKind::File => Ok(Destination::Replaced(existing)),
            InodeKind::Directory => Err(path_conflict("only a file destination can be replaced")),
        },
    }
}

fn no_destination(display_name: &DisplayName) -> MountSimError {
    MountSimError::new(
        ErrorCode::PathNotFound,
        format!("no file named `{display_name}` to replace"),
    )
}
