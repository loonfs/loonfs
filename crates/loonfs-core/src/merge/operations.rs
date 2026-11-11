//! What a merge's commit does to each inode, and the operation that makes
//! each change.

use super::state::InodeState;
use crate::error::{CoreError, Result};
use crate::metadata::RevisionRecord;
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, CommitPrecondition, CommitSeq, ContentRef,
    DeleteDirectoryBehavior, DisplayName, FilesystemOperation, InodeId, InodeKind, NamespaceId,
    RevisionNo,
};

/// What a merge's commit does to one inode.
pub(super) struct InodePlan {
    pub(super) inode_id: InodeId,
    pub(super) kind: InodeKind,
    /// The create, move, or delete, if the merge changes the inode's
    /// binding.
    pub(super) binding: Option<BindingChange>,
    /// The revision, attribute, and access updates, in that order.
    pub(super) updates: Vec<FilesystemOperation>,
}

impl InodePlan {
    /// Returns the plan's operations: the binding change, then the updates.
    pub(super) fn operations(&self) -> Vec<FilesystemOperation> {
        self.binding
            .as_ref()
            .map(|binding| binding.operation(self.inode_id, self.kind))
            .into_iter()
            .chain(self.updates.iter().cloned())
            .collect()
    }
}

/// Returns the plan of the create of `inode_id`, with its parent, name,
/// content, attributes, and access at `ours`, its state in the fork, for a
/// merge into a namespace that doesn't have the inode, or `None` when the
/// inode isn't visible in the fork.
pub(super) fn create_plan(inode_id: InodeId, ours: &InodeState) -> Result<Option<InodePlan>> {
    let Some(edge) = ours.edge() else {
        return Ok(None);
    };
    let binding = BindingChange::Create {
        parent_inode_id: edge.parent_inode_id,
        display_name: display_name(&edge.display_name)?,
        content_ref: ours.revision.as_ref().map(|row| row.content_ref.clone()),
    };
    let updates = ours
        .attributes
        .is_some()
        .then(|| attributes_update(inode_id, ours, None))
        .into_iter()
        .chain(
            ours.access
                .is_some()
                .then(|| access_update(inode_id, ours, None)),
        )
        .collect();
    Ok(Some(InodePlan {
        inode_id,
        kind: ours.kind,
        binding: Some(binding),
        updates,
    }))
}

/// Returns how a merge changes the binding of an inode that the
/// merged-into namespace `namespace_id` has, from `theirs`, its state there,
/// to `ours`, its state in the fork, or `None` when the binding stays.
///
/// The merge moves an inode that both states have visible at different
/// places, and deletes one that only `theirs` has visible. When only `ours`
/// has the inode visible, the merge undoes the deletion at `deletion_seq`,
/// which the caller passes when a deletion of the inode itself hides it in
/// `theirs` and the merge can undo that deletion. Without one, as when only
/// an ancestor's deletion hides the inode, the binding stays, and undoing
/// the ancestor's deletion shows the inode again.
pub(super) fn binding_change(
    namespace_id: &NamespaceId,
    ours: &InodeState,
    theirs: &InodeState,
    deletion_seq: Option<CommitSeq>,
) -> Result<Option<BindingChange>> {
    if ours.same_binding(theirs) {
        return Ok(None);
    }
    let change = match (theirs.edge(), ours.edge(), deletion_seq) {
        (Some(from), Some(to), _) => BindingChange::Move {
            expected_binding_version: crate::binding_version::encode(from.binding, namespace_id),
            parent_inode_id: to.parent_inode_id,
            display_name: display_name(&to.display_name)?,
        },
        (Some(from), None, _) => BindingChange::Delete {
            expected_binding_version: crate::binding_version::encode(from.binding, namespace_id),
            expected_revision_no: theirs.revision.as_ref().map(|row| row.revision_no),
        },
        (None, Some(to), Some(deletion_seq)) => BindingChange::RestoreDeleted {
            deletion_seq,
            parent_inode_id: to.parent_inode_id,
            display_name: display_name(&to.display_name)?,
        },
        (None, _, _) => return Ok(None),
    };
    Ok(Some(change))
}

/// How a merge changes an inode's binding.
pub(super) enum BindingChange {
    /// The inode doesn't exist in the merged-into namespace, so the merge
    /// creates it under its ID in the fork.
    Create {
        parent_inode_id: InodeId,
        display_name: DisplayName,
        /// A file's content. A directory has none.
        content_ref: Option<ContentRef>,
    },
    Move {
        expected_binding_version: BindingVersion,
        parent_inode_id: InodeId,
        display_name: DisplayName,
    },
    Delete {
        expected_binding_version: BindingVersion,
        /// A file's revision in the merged-into namespace, which the commit
        /// expects with a precondition, because another writer can change
        /// the file's content without changing its binding version. A
        /// directory has none, and the subtree check and the head
        /// precondition cover its delete.
        expected_revision_no: Option<RevisionNo>,
    },
    /// The merged-into namespace deleted the inode in an earlier merge of
    /// the fork, and the fork undid the deletion, so the merge undoes it too
    /// and gives the inode its parent and name in the fork. The deletion's
    /// sequence identifies the deletion that the restore reverses.
    RestoreDeleted {
        deletion_seq: CommitSeq,
        parent_inode_id: InodeId,
        display_name: DisplayName,
    },
}

impl BindingChange {
    /// Returns the precondition that the change to `inode_id` needs beyond
    /// its operation, which only a file's delete has.
    pub(super) fn precondition(&self, inode_id: InodeId) -> Option<CommitPrecondition> {
        match self {
            Self::Delete {
                expected_revision_no: Some(expected_revision_no),
                ..
            } => Some(CommitPrecondition::FileRevision {
                inode_id,
                expected_revision_no: *expected_revision_no,
            }),
            _ => None,
        }
    }

    /// Returns the operation that makes the change to `inode_id`.
    pub(super) fn operation(&self, inode_id: InodeId, kind: InodeKind) -> FilesystemOperation {
        match self {
            Self::Create {
                parent_inode_id,
                display_name,
                content_ref,
                ..
            } => match content_ref {
                Some(content_ref) => FilesystemOperation::CreateFileByInode {
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                    content: loonfs_types::FileContent::Ref(content_ref.clone()),
                },
                None => FilesystemOperation::CreateDirectoryByInode {
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                },
            },
            Self::Move {
                expected_binding_version,
                parent_inode_id,
                display_name,
            } => FilesystemOperation::MoveByInode {
                inode_id,
                expected_binding_version: expected_binding_version.clone(),
                destination_parent_inode_id: *parent_inode_id,
                destination_display_name: display_name.clone(),
                precondition: loonfs_types::DestinationPrecondition::default(),
            },
            Self::RestoreDeleted {
                deletion_seq,
                parent_inode_id,
                display_name,
            } => FilesystemOperation::RestoreDeleted {
                inode_id,
                deletion_seq: *deletion_seq,
                destination: loonfs_types::options::RestoreDeletedDestination::Name {
                    parent_inode_id: *parent_inode_id,
                    display_name: display_name.clone(),
                },
            },
            Self::Delete {
                expected_binding_version,
                ..
            } => FilesystemOperation::DeleteByInode {
                inode_id,
                expected_binding_version: expected_binding_version.clone(),
                behavior: match kind {
                    InodeKind::Directory => DeleteDirectoryBehavior::Recursive,
                    InodeKind::File => DeleteDirectoryBehavior::NonRecursive,
                },
            },
        }
    }
}

/// Builds the revision update that sets a file's content to the fork's
/// revision `ours`, against the revision `theirs`.
pub(super) fn revision_update(
    inode_id: InodeId,
    ours: &RevisionRecord,
    theirs: &RevisionRecord,
) -> FilesystemOperation {
    FilesystemOperation::PutFileRevisionByInode {
        inode_id,
        content: loonfs_types::FileContent::Ref(ours.content_ref.clone()),
        expected_revision_no: theirs.revision_no,
    }
}

/// Builds the attribute update that sets an inode's attributes to the
/// fork's, against `theirs`, or against no attributes for an inode that the
/// merge creates.
pub(super) fn attributes_update(
    inode_id: InodeId,
    ours: &InodeState,
    theirs: Option<&InodeState>,
) -> FilesystemOperation {
    let ours_attributes = ours.attributes();
    let their_attributes = theirs.map(InodeState::attributes).unwrap_or_default();
    FilesystemOperation::UpdateAttributesByInode {
        inode_id,
        set: ours_attributes
            .iter()
            .filter(|(key, value)| their_attributes.get(key) != Some(value))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        remove: their_attributes
            .iter()
            .filter(|(key, _)| ours_attributes.get(key).is_none())
            .map(|(key, _)| key.clone())
            .collect(),
        expected_attributes_revision_no: theirs.map(|theirs| {
            theirs
                .attributes
                .as_ref()
                .map_or(AttributesRevisionNo(0), |row| row.attributes_revision_no)
        }),
    }
}

/// Builds the access update that sets an inode's boundary flag and direct
/// grants to the fork's, against `theirs`, or against no access row for an
/// inode that the merge creates.
pub(super) fn access_update(
    inode_id: InodeId,
    ours: &InodeState,
    theirs: Option<&InodeState>,
) -> FilesystemOperation {
    let (boundary, grants) = ours.access();
    FilesystemOperation::UpdateAccessByInode {
        inode_id,
        boundary,
        grants,
        expected_access_revision_no: theirs.map(|theirs| {
            theirs
                .access
                .as_ref()
                .map_or(loonfs_types::AccessRevisionNo(0), |row| {
                    row.access_revision_no
                })
        }),
    }
}

/// Parses a display name that a binding row stores.
pub(super) fn display_name(stored: &str) -> Result<DisplayName> {
    DisplayName::parse(stored).map_err(|error| {
        CoreError::NamespaceCorrupt(format!(
            "a binding row stores the display name {stored:?}, which isn't valid: {error}"
        ))
    })
}
