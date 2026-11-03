//! What a merge's commit does to each inode, and the operation that makes
//! each change.

use super::state::InodeState;
use crate::error::{CoreError, Result};
use loonfs_types::{
    AttributesRevisionNo, BindingVersion, ChangeSeq, CommitPrecondition, ContentRef,
    DeleteDirectoryBehavior, DisplayName, FilesystemOperation, InodeId, InodeKind, RevisionNo,
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
    /// sequence identifies the deletion that the undelete reverses.
    Undelete {
        deletion_seq: ChangeSeq,
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
                    content: loonfs_types::FileContent::Ref {
                        content_ref: content_ref.clone(),
                        source_namespace_id: None,
                    },
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
            Self::Undelete {
                deletion_seq,
                parent_inode_id,
                display_name,
            } => FilesystemOperation::Undelete {
                inode_id,
                deletion_seq: *deletion_seq,
                destination_path: None,
                destination_parent_inode_id: Some(*parent_inode_id),
                destination_display_name: Some(display_name.clone()),
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
