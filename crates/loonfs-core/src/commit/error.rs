//! [`CommitValidationError`]: every way a commit request can fail
//! validation.

use loonfs_types::{
    AccessRevisionNo, AttributesRevisionNo, CommitSeq, ErrorCode, ErrorDetails, InodeId, RevisionNo,
};
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CommitValidationError {
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
        "binding precondition failed: {target} {}", binding_mismatch(.expected_inode_id, .actual_inode_id)
    )]
    BindingPreconditionMismatch {
        target: String,
        expected_inode_id: Option<InodeId>,
        actual_inode_id: Option<InodeId>,
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
    #[error("restore target inode `{inode_id}` is not the root of a deletion in the trash")]
    RestoreDeletedTargetNotDeleted { inode_id: InodeId },
    #[error(
        "restore of inode `{inode_id}` targets a deletion made in the same commit, not an earlier one"
    )]
    RestoreDeletedTargetsCurrentCommit { inode_id: InodeId },
    #[error(
        "restore of inode `{inode_id}` targets the deletion at seq `{requested_seq}`, but the deletion in the trash is at seq `{actual_deletion_seq}`"
    )]
    RestoreDeletedSequenceMismatch {
        inode_id: InodeId,
        requested_seq: CommitSeq,
        actual_deletion_seq: CommitSeq,
    },
    #[error(
        "cannot add a revision to inode `{inode_id}` because revision `{base_revision_no}` is already at the maximum 9007199254740991"
    )]
    RevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error(
        "attribute base revision mismatch for inode `{inode_id}`: {}", revision_mismatch(.expected, .actual)
    )]
    UpdateAttributesBaseRevisionMismatch {
        inode_id: InodeId,
        expected: AttributesRevisionNo,
        actual: Option<AttributesRevisionNo>,
    },
    #[error(
        "cannot update attributes for inode `{inode_id}` because revision `{base_attributes_revision_no}` is already at the maximum 9007199254740991"
    )]
    UpdateAttributesRevisionOverflow {
        inode_id: InodeId,
        base_attributes_revision_no: AttributesRevisionNo,
    },
    #[error(
        "access base revision mismatch for inode `{inode_id}`: {}", revision_mismatch(.expected, .actual)
    )]
    UpdateAccessBaseRevisionMismatch {
        inode_id: InodeId,
        expected: AccessRevisionNo,
        actual: Option<AccessRevisionNo>,
    },
    #[error(
        "cannot update access for inode `{inode_id}` because revision `{base_access_revision_no}` is already at the maximum 9007199254740991"
    )]
    UpdateAccessRevisionOverflow {
        inode_id: InodeId,
        base_access_revision_no: AccessRevisionNo,
    },
    #[error("op index overflow")]
    OperationIndexOverflow,
    #[error("delta index overflow")]
    DeltaIndexOverflow,
}

impl CommitValidationError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::BaseRevisionMismatch { .. } => ErrorCode::StaleRevision,
            Self::RestoreRevisionSourceRevisionMissing { .. } => ErrorCode::RevisionNotFound,
            Self::UpdateAttributesBaseRevisionMismatch { .. } => ErrorCode::StaleAttributes,
            Self::UpdateAttributesRevisionOverflow { .. } => ErrorCode::ServerError,
            Self::UpdateAccessBaseRevisionMismatch { .. } => ErrorCode::StaleAccess,
            Self::UpdateAccessRevisionOverflow { .. } => ErrorCode::ServerError,
            Self::BindingPreconditionMismatch { .. } => ErrorCode::PathConflict,
            Self::DirectoryNotEmpty { .. } => ErrorCode::DirectoryNotEmpty,
            Self::RestoreDeletedTargetNotDeleted { .. }
            | Self::RestoreDeletedTargetsCurrentCommit { .. }
            | Self::RestoreDeletedSequenceMismatch { .. } => ErrorCode::NotDeleted,
            Self::RenameWouldCycleDirectory { .. } => ErrorCode::WouldCycle,
            Self::RevisionOverflow { .. }
            | Self::OperationIndexOverflow
            | Self::DeltaIndexOverflow => ErrorCode::ServerError,
        }
    }

    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            Self::BindingPreconditionMismatch {
                expected_inode_id,
                actual_inode_id,
                ..
            } => Some(ErrorDetails {
                expected_inode_id: *expected_inode_id,
                actual_inode_id: *actual_inode_id,
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
            Self::RestoreDeletedTargetNotDeleted { inode_id }
            | Self::RestoreDeletedTargetsCurrentCommit { inode_id } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                ..ErrorDetails::default()
            }),
            Self::RestoreDeletedSequenceMismatch {
                inode_id,
                requested_seq,
                actual_deletion_seq,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_deletion_seq: Some(*requested_seq),
                actual_deletion_seq: Some(*actual_deletion_seq),
                ..ErrorDetails::default()
            }),
            Self::UpdateAttributesBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_attributes_revision_no: Some(*expected),
                actual_attributes_revision_no: *actual,
                ..ErrorDetails::default()
            }),
            Self::UpdateAccessBaseRevisionMismatch {
                inode_id,
                expected,
                actual,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_access_revision_no: Some(*expected),
                actual_access_revision_no: *actual,
                ..ErrorDetails::default()
            }),
            _ => None,
        }
    }
}

fn revision_mismatch<T: fmt::Display>(expected: &T, actual: &Option<T>) -> String {
    match actual {
        Some(actual) => format!("expected revision {expected}, found revision {actual}"),
        None => format!("expected revision {expected}, found no revision"),
    }
}

fn binding_mismatch(expected: &Option<InodeId>, actual: &Option<InodeId>) -> String {
    let expected = expected.map_or_else(
        || "no inode".to_owned(),
        |inode_id| format!("child inode `{inode_id}`"),
    );
    let actual = actual.map_or_else(|| "no inode".to_owned(), |inode_id| format!("`{inode_id}`"));
    format!("expected {expected} but found {actual}")
}
