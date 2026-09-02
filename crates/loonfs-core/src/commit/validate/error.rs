//! [`CommitValidationError`]: every way a commit request can fail
//! validation.

use loonfs_api::{
    AttributeRevisionNo, ChangeSeq, ErrorCode, ErrorDetails, InodeId, InodeKind, NameKey,
    RevisionNo,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOperand {
    CreateParent,
    RenameSource,
    RenameTargetParent,
    ReplaceTarget,
    RestoreTarget,
    DeleteTarget,
    SubtreeRoot,
    AttributeTarget,
    UndeleteTarget,
    EmptyDirectoryTarget,
}

impl fmt::Display for CommitOperand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CreateParent => "create parent",
            Self::RenameSource => "rename source",
            Self::RenameTargetParent => "rename target parent",
            Self::ReplaceTarget => "replace target",
            Self::RestoreTarget => "restore target",
            Self::DeleteTarget => "delete target",
            Self::SubtreeRoot => "delete subtree root",
            Self::AttributeTarget => "attribute update target",
            Self::UndeleteTarget => "undelete target",
            Self::EmptyDirectoryTarget => "empty directory target",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum CommitValidationError {
    #[error("{operand} inode `{inode_id}` is missing")]
    InodeMissing {
        operand: CommitOperand,
        inode_id: InodeId,
    },
    #[error(
        "{operand} inode `{inode_id}` has the wrong kind: expected `{expected}`, found `{actual}`"
    )]
    InodeWrongKind {
        operand: CommitOperand,
        inode_id: InodeId,
        expected: InodeKind,
        actual: InodeKind,
    },
    #[error(
        "{operand} name `{name_key}` under parent inode `{parent_inode_id}` is taken by inode `{child_inode_id}`"
    )]
    NameTaken {
        operand: CommitOperand,
        parent_inode_id: InodeId,
        name_key: NameKey,
        child_inode_id: InodeId,
    },
    #[error(
        "{operand} inode `{inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    TargetUnderSubtreeTombstone {
        operand: CommitOperand,
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error(
        "base revision mismatch for inode `{inode_id}`: {}",
        revision_mismatch(.expected, .actual)
    )]
    BaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    #[error(
        "binding precondition failed: name `{name_key}` is not bound under parent inode `{parent_inode_id}`"
    )]
    BindingPreconditionMissing {
        parent_inode_id: InodeId,
        name_key: NameKey,
    },
    #[error(
        "binding precondition failed: name `{name_key}` under parent inode `{parent_inode_id}` expected child inode `{expected_child_inode_id}` but found `{actual_child_inode_id}`"
    )]
    BindingPreconditionMismatch {
        parent_inode_id: InodeId,
        name_key: NameKey,
        expected_child_inode_id: InodeId,
        actual_child_inode_id: InodeId,
    },
    #[error("directory inode `{inode_id}` is not empty")]
    DirectoryNotEmpty { inode_id: InodeId },
    #[error("restore source revision `{source_revision_no}` not found for inode `{inode_id}`")]
    RestoreRevisionSourceRevisionMissing {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    },
    #[error(
        "rename of directory inode `{inode_id}` into inode `{new_parent_inode_id}` would create a cycle"
    )]
    RenameWouldCycleDirectory {
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    },
    #[error("undelete target inode `{inode_id}` is not the root of a live deletion")]
    UndeleteTargetNotDeleted { inode_id: InodeId },
    #[error(
        "undelete of inode `{inode_id}` targets a deletion at seq `{requested_seq}`, which is not from an earlier commit"
    )]
    UndeleteTargetsCurrentCommit {
        inode_id: InodeId,
        requested_seq: ChangeSeq,
    },
    #[error(
        "undelete of inode `{inode_id}` targets the deletion at seq `{requested_seq}`, but the active deletion is at seq `{active_seq}`"
    )]
    UndeleteGenerationMismatch {
        inode_id: InodeId,
        requested_seq: ChangeSeq,
        active_seq: ChangeSeq,
    },
    #[error(
        "cannot restore inode `{inode_id}` because revision `{base_revision_no}` is already at the maximum 9007199254740991"
    )]
    RestoreRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error(
        "cannot replace inode `{inode_id}` because revision `{base_revision_no}` is already at the maximum 9007199254740991"
    )]
    ReplaceFileRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error(
        "attribute base revision mismatch for inode `{inode_id}`: expected revision {expected}, found revision {actual}"
    )]
    UpdateAttributesBaseRevisionMismatch {
        inode_id: InodeId,
        expected: AttributeRevisionNo,
        actual: AttributeRevisionNo,
    },
    #[error(
        "cannot update attributes for inode `{inode_id}` because revision `{base_attributes_revision_no}` is already at the maximum 9007199254740991"
    )]
    UpdateAttributesRevisionOverflow {
        inode_id: InodeId,
        base_attributes_revision_no: AttributeRevisionNo,
    },
    #[error("op index overflow")]
    OpIndexOverflow,
    #[error("delta index overflow")]
    DeltaIndexOverflow,
}

impl CommitValidationError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::BaseRevisionMismatch { .. } => ErrorCode::StaleRevision,
            Self::RestoreRevisionSourceRevisionMissing { .. } => ErrorCode::RevisionNotFound,
            Self::TargetUnderSubtreeTombstone { .. } => ErrorCode::NamespaceCorrupt,
            Self::UpdateAttributesBaseRevisionMismatch { .. } => ErrorCode::StaleAttributes,
            Self::UpdateAttributesRevisionOverflow { .. } => ErrorCode::ServerError,
            Self::NameTaken { .. }
            | Self::InodeWrongKind { .. }
            | Self::BindingPreconditionMissing { .. }
            | Self::BindingPreconditionMismatch { .. } => ErrorCode::PathConflict,
            Self::DirectoryNotEmpty { .. } => ErrorCode::DirectoryNotEmpty,
            Self::InodeMissing { .. } => ErrorCode::PathNotFound,
            Self::UndeleteTargetNotDeleted { .. }
            | Self::UndeleteTargetsCurrentCommit { .. }
            | Self::UndeleteGenerationMismatch { .. } => ErrorCode::NotDeleted,
            Self::RenameWouldCycleDirectory { .. } => ErrorCode::WouldCycle,
            Self::RestoreRevisionOverflow { .. }
            | Self::ReplaceFileRevisionOverflow { .. }
            | Self::OpIndexOverflow
            | Self::DeltaIndexOverflow => ErrorCode::ServerError,
        }
    }

    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            Self::BindingPreconditionMismatch {
                expected_child_inode_id,
                actual_child_inode_id,
                ..
            } => Some(ErrorDetails {
                expected_inode_id: Some(*expected_child_inode_id),
                actual_inode_id: Some(*actual_child_inode_id),
                ..ErrorDetails::default()
            }),
            Self::BaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_revision_no: Some(*expected),
                actual_revision_no: *actual,
                ..ErrorDetails::default()
            }),
            Self::InodeMissing {
                operand: CommitOperand::UndeleteTarget,
                inode_id,
            }
            | Self::UndeleteTargetNotDeleted { inode_id } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                ..ErrorDetails::default()
            }),
            Self::UndeleteTargetsCurrentCommit {
                inode_id,
                requested_seq,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_deletion_seq: Some(*requested_seq),
                ..ErrorDetails::default()
            }),
            Self::UndeleteGenerationMismatch {
                inode_id,
                requested_seq,
                active_seq,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_deletion_seq: Some(*requested_seq),
                actual_deletion_seq: Some(*active_seq),
                ..ErrorDetails::default()
            }),
            Self::UpdateAttributesBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_attributes_revision_no: Some(*expected),
                actual_attributes_revision_no: Some(*actual),
                ..ErrorDetails::default()
            }),
            Self::InodeMissing {
                operand: CommitOperand::AttributeTarget,
                inode_id,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                ..ErrorDetails::default()
            }),
            _ => None,
        }
    }
}

fn revision_mismatch(expected: &RevisionNo, actual: &Option<RevisionNo>) -> String {
    match actual {
        Some(actual) => format!("expected revision {expected}, found revision {actual}"),
        None => format!("expected revision {expected}, found no revision"),
    }
}
