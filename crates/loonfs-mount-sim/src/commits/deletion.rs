//! Planning deletes and undeletes: `delete_path`, `delete_by_inode`, and `undelete`.

use super::files::final_name;
use super::planner::{ensure_expected_inode, path_conflict, root_mutation, Planner};
use crate::entries::path_lookup_error;
use crate::error::{MountSimError, Result};
use loonfs_model::metadata::{DeltaPosition, SubtreeTombstoneAction};
use loonfs_model::visibility::PathLookup;
use loonfs_types::{
    AbsolutePath, BindingVersion, ChangeSeq, DeleteDirectoryBehavior, DisplayName, ErrorCode,
    InodeId, InodeKind, ROOT_INODE_ID,
};

impl Planner<'_> {
    pub(super) fn delete_path(
        &mut self,
        path: &AbsolutePath,
        behavior: DeleteDirectoryBehavior,
        expected_inode_id: Option<InodeId>,
    ) -> Result<()> {
        final_name(path)?;
        let inode_id = match self.view().resolve_path(path) {
            PathLookup::Found { inode, .. } => inode.inode_id,
            lookup => return Err(path_lookup_error(&lookup)),
        };
        ensure_expected_inode(expected_inode_id, inode_id)?;
        self.delete_inode(inode_id, behavior)
    }

    pub(super) fn delete_by_inode(
        &mut self,
        inode_id: InodeId,
        expected_binding_version: &BindingVersion,
        behavior: DeleteDirectoryBehavior,
    ) -> Result<()> {
        self.visible(inode_id)?;
        if inode_id == ROOT_INODE_ID {
            return Err(root_mutation());
        }
        self.ensure_binding_version(inode_id, expected_binding_version)?;
        self.delete_inode(inode_id, behavior)
    }

    /// Deletes a visible inode. A non-recursive delete of a directory with visible children fails,
    /// and a recursive delete records one tombstone, for the directory, which hides its subtree.
    fn delete_inode(&mut self, inode_id: InodeId, behavior: DeleteDirectoryBehavior) -> Result<()> {
        let view = self.view();
        let is_directory = view
            .visible_inode(inode_id)
            .is_some_and(|inode| inode.inode_kind == InodeKind::Directory);
        if is_directory
            && behavior == DeleteDirectoryBehavior::NonRecursive
            && view.has_visible_children(inode_id)
        {
            return Err(MountSimError::new(
                ErrorCode::DirectoryNotEmpty,
                "the directory has children, and the delete isn't recursive",
            )
            .with_details(|details| details.inode_id = Some(inode_id)));
        }
        self.delete(inode_id);
        Ok(())
    }

    /// Revokes the deletion of `inode_id` that was committed at `deletion_seq`, and binds the
    /// inode where the request says, or where the deletion recorded it.
    pub(super) fn undelete(
        &mut self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
        destination_path: Option<&AbsolutePath>,
        destination_parent_inode_id: Option<InodeId>,
        destination_display_name: Option<&DisplayName>,
    ) -> Result<()> {
        validate_destination(
            destination_path,
            destination_parent_inode_id,
            destination_display_name,
        )?;
        let (deletion, recorded_parent, recorded_name) =
            self.recoverable(inode_id, deletion_seq)?;
        let (parent_inode_id, display_name) = match (
            destination_path,
            destination_parent_inode_id,
            destination_display_name,
        ) {
            (Some(path), _, _) => (self.parent_directory(path)?, final_name(path)?),
            (None, Some(parent_inode_id), Some(display_name)) => {
                (parent_inode_id, display_name.clone())
            }
            _ => (recorded_parent, recorded_name),
        };
        let parent = self.visible(parent_inode_id)?;
        if parent.inode_kind != InodeKind::Directory {
            return Err(path_conflict("the destination parent isn't a directory"));
        }
        if self
            .view()
            .visible_child(parent_inode_id, &display_name)
            .is_some()
        {
            return Err(path_conflict(format!(
                "the destination name `{display_name}` is taken"
            )));
        }
        self.restore(inode_id, deletion, parent_inode_id, &display_name);
        Ok(())
    }

    /// Returns the position of the recoverable deletion of `inode_id` at `deletion_seq`, and the
    /// parent and name that the deletion recorded.
    fn recoverable(
        &self,
        inode_id: InodeId,
        deletion_seq: ChangeSeq,
    ) -> Result<(DeltaPosition, InodeId, DisplayName)> {
        let view = self.view();
        let not_deleted = |actual: Option<ChangeSeq>| {
            MountSimError::new(
                ErrorCode::NotDeleted,
                format!(
                    "inode `{}` isn't the root of a recoverable deletion at sequence {deletion_seq}",
                    loonfs_types::public_inode_id::encode(inode_id)
                ),
            )
            .with_details(|details| {
                details.inode_id = Some(inode_id);
                details.expected_deletion_seq = actual.map(|_| deletion_seq);
                details.actual_deletion_seq = actual;
            })
        };
        // A deletion made earlier in this request has no committed sequence yet, so it isn't
        // recoverable here.
        let Some(event) = view
            .recoverable_deletion(inode_id)
            .filter(|event| event.committed_seq < self.seq)
        else {
            return Err(not_deleted(None));
        };
        if event.committed_seq != deletion_seq {
            return Err(not_deleted(Some(event.committed_seq)));
        }
        let SubtreeTombstoneAction::Set { deleted_binding } = &event.action else {
            return Err(not_deleted(None));
        };
        let recorded_name = DisplayName::parse(&deleted_binding.display_name)
            .expect("a recorded display name should be valid");
        Ok((
            DeltaPosition {
                seq: event.committed_seq,
                delta_index: event.delta_index,
            },
            deleted_binding.parent_inode_id,
            recorded_name,
        ))
    }
}

/// Rejects an undelete that sets more than one destination, or half of the parent-and-name
/// destination.
fn validate_destination(
    path: Option<&AbsolutePath>,
    parent_inode_id: Option<InodeId>,
    display_name: Option<&DisplayName>,
) -> Result<()> {
    let (field, message) = match (path, parent_inode_id, display_name) {
        (None, None, None) | (Some(_), None, None) | (None, Some(_), Some(_)) => return Ok(()),
        (Some(_), _, _) => (
            "destination_path",
            "`destination_path` can't be combined with `destination_parent_inode_id` or \
             `destination_display_name`",
        ),
        (None, Some(_), None) => (
            "destination_display_name",
            "`destination_parent_inode_id` requires `destination_display_name`",
        ),
        (None, None, Some(_)) => (
            "destination_parent_inode_id",
            "`destination_display_name` requires `destination_parent_inode_id`",
        ),
    };
    Err(MountSimError::invalid_request(message).with_param(field))
}
