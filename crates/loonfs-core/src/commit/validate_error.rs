//! [`CommitValidationError`]: every way a commit request can fail
//! validation.

use loonfs_api::{
    AttributeRevisionNo, ChangeSeq, InodeId, InodeKind, NameKey, RevisionNo, WriterEpoch,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum CommitValidationError {
    #[error("commit contains no operations")]
    EmptyCommit,
    #[error("commit namespace does not match the namespace head")]
    NamespaceMismatch,
    #[error("name precondition parent inode `{parent_inode_id}` is missing")]
    NamePreconditionParentMissing { parent_inode_id: InodeId },
    #[error(
        "name precondition parent inode `{parent_inode_id}` is not a directory (found `{actual_kind}`)"
    )]
    NamePreconditionParentNotDirectory {
        parent_inode_id: InodeId,
        actual_kind: InodeKind,
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
    #[error("directory-empty precondition inode `{inode_id}` is missing")]
    DirectoryEmptyPreconditionInodeMissing { inode_id: InodeId },
    #[error(
        "directory-empty precondition inode `{inode_id}` is not a directory (found `{actual_kind}`)"
    )]
    DirectoryEmptyPreconditionInodeNotDirectory {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error("directory-empty precondition failed: directory inode `{inode_id}` is not empty")]
    DirectoryEmptyPreconditionNotEmpty { inode_id: InodeId },
    #[error("create parent inode `{parent_inode_id}` is missing")]
    CreateParentMissing { parent_inode_id: InodeId },
    #[error("create parent inode `{parent_inode_id}` is not a directory (found `{actual_kind}`)")]
    CreateParentNotDirectory {
        parent_inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "create collides with existing name `{name_key}` under parent inode `{parent_inode_id}` (bound to inode `{child_inode_id}`)"
    )]
    CreateChildNameCollision {
        parent_inode_id: InodeId,
        name_key: NameKey,
        child_inode_id: InodeId,
    },
    #[error("invalid display name: {reason}")]
    InvalidDisplayName {
        display_name: String,
        reason: String,
    },
    #[error(
        "create under parent inode `{parent_inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    CreateUnderSubtreeTombstone {
        parent_inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("replace target file inode `{inode_id}` is missing")]
    ReplaceFileInodeMissing { inode_id: InodeId },
    #[error("replace target inode `{inode_id}` is not a file (found `{actual_kind}`)")]
    ReplaceFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "replace base revision mismatch for inode `{inode_id}`: {}",
        revision_mismatch(.expected, .actual)
    )]
    ReplaceFileBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    #[error("restore target file inode `{inode_id}` is missing")]
    RestoreRevisionInodeMissing { inode_id: InodeId },
    #[error("restore target inode `{inode_id}` is not a file (found `{actual_kind}`)")]
    RestoreRevisionInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "restore base revision mismatch for inode `{inode_id}`: {}",
        revision_mismatch(.expected, .actual)
    )]
    RestoreRevisionBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    #[error("restore source revision `{source_revision_no}` not found for inode `{inode_id}`")]
    RestoreRevisionSourceRevisionMissing {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    },
    #[error(
        "restore of inode `{inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    RestoreRevisionUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error(
        "replace of inode `{inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    ReplaceFileUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("delete target file inode `{inode_id}` is missing")]
    DeleteFileInodeMissing { inode_id: InodeId },
    #[error("delete target inode `{inode_id}` is not a file (found `{actual_kind}`)")]
    DeleteFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "delete of file inode `{inode_id}` is already covered by subtree tombstone rooted at inode `{covering_root_inode_id}` from seq `{tombstone_seq}`"
    )]
    DeleteFileCoveredByTombstone {
        inode_id: InodeId,
        covering_root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("rename source inode `{inode_id}` is missing")]
    RenameInodeMissing { inode_id: InodeId },
    #[error("rename source inode `{inode_id}` has no current binding")]
    RenameSourceBindingMissing { inode_id: InodeId },
    #[error("source inode `{inode_id}` has no current binding")]
    SourceBindingMissing { inode_id: InodeId },
    #[error("rename target parent inode `{parent_inode_id}` is missing")]
    RenameTargetParentMissing { parent_inode_id: InodeId },
    #[error(
        "rename target parent inode `{parent_inode_id}` is not a directory (found `{actual_kind}`)"
    )]
    RenameTargetParentNotDirectory {
        parent_inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "rename collides with existing name `{name_key}` under parent inode `{parent_inode_id}` (bound to inode `{child_inode_id}`)"
    )]
    RenameTargetNameCollision {
        parent_inode_id: InodeId,
        name_key: NameKey,
        child_inode_id: InodeId,
    },
    #[error(
        "rename of directory inode `{inode_id}` into inode `{new_parent_inode_id}` would create a cycle"
    )]
    RenameWouldCycleDirectory {
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    },
    #[error(
        "rename of inode `{inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    RenameInodeUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error(
        "rename target parent inode `{parent_inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    RenameTargetParentUnderSubtreeTombstone {
        parent_inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("delete subtree root inode `{root_inode_id}` is missing")]
    DeleteSubtreeRootMissing { root_inode_id: InodeId },
    #[error(
        "delete subtree root inode `{root_inode_id}` is not a directory (found `{actual_kind}`)"
    )]
    DeleteSubtreeRootNotDirectory {
        root_inode_id: InodeId,
        actual_kind: InodeKind,
    },
    #[error(
        "delete subtree root inode `{root_inode_id}` is already covered by subtree tombstone rooted at inode `{covering_root_inode_id}` from seq `{tombstone_seq}`"
    )]
    DeleteSubtreeRootCoveredByTombstone {
        root_inode_id: InodeId,
        covering_root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("undelete target inode `{inode_id}` is missing")]
    UndeleteInodeMissing { inode_id: InodeId },
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
        "revision counter overflow restoring inode `{inode_id}` at base revision `{base_revision_no}`"
    )]
    RestoreRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error(
        "revision counter overflow replacing inode `{inode_id}` at base revision `{base_revision_no}`"
    )]
    ReplaceFileRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    #[error("attribute update target inode `{inode_id}` is missing")]
    UpdateAttributesInodeMissing { inode_id: InodeId },
    #[error(
        "attribute base revision mismatch for inode `{inode_id}`: expected revision {expected}, found revision {actual}"
    )]
    UpdateAttributesBaseRevisionMismatch {
        inode_id: InodeId,
        expected: AttributeRevisionNo,
        actual: AttributeRevisionNo,
    },
    #[error(
        "attribute update of inode `{inode_id}` conflicts with subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    UpdateAttributesUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error(
        "attribute revision counter overflow updating inode `{inode_id}` at base revision `{base_attributes_revision_no}`"
    )]
    UpdateAttributesRevisionOverflow {
        inode_id: InodeId,
        base_attributes_revision_no: AttributeRevisionNo,
    },
    #[error("stale writer epoch: requested `{requested}` but active is `{active}`")]
    StaleWriterEpoch {
        active: WriterEpoch,
        requested: WriterEpoch,
    },
    #[error("validated preview apply failed: {0}")]
    ValidatedPreviewApplyFailed(String),
    #[error("sequence counter overflow")]
    SeqOverflow,
    #[error("next inode id counter overflow")]
    NextInodeOverflow,
    #[error("op index overflow")]
    OpIndexOverflow,
    #[error("delta index overflow")]
    DeltaIndexOverflow,
}

/// What a base-revision guard asked for against what it found, in words.
///
/// The revision it found is absent when the file carries no revision at all,
/// and that case reads as a sentence rather than printing the `Option` — a
/// message is for a person, while the same pair rides the wire as typed
/// `expected_revision` and `actual_revision` details for a program.
fn revision_mismatch(expected: &RevisionNo, actual: &Option<RevisionNo>) -> String {
    match actual {
        Some(actual) => format!("expected revision {expected}, found revision {actual}"),
        None => format!("expected revision {expected}, found no revision"),
    }
}
