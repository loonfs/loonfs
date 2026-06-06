use loon_api::v0::RenameMode;
use loon_api::{ChangeSeq, FenceToken, InodeId, InodeKind, RevisionNo};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitValidationError {
    EmptyCommit,
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    NamePreconditionParentMissing {
        parent_inode: InodeId,
    },
    NamePreconditionParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    BindingPreconditionMissing {
        parent_inode: InodeId,
        name_key: String,
    },
    BindingPreconditionMismatch {
        parent_inode: InodeId,
        name_key: String,
        expected_child_inode: InodeId,
        actual_child_inode: Option<InodeId>,
    },
    DirectoryEmptyPreconditionInodeMissing {
        inode_id: InodeId,
    },
    DirectoryEmptyPreconditionInodeNotDirectory {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    DirectoryEmptyPreconditionNotEmpty {
        inode_id: InodeId,
    },
    CreateParentMissing {
        parent_inode: InodeId,
    },
    CreateParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    CreateChildNameCollision {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
    },
    InvalidDisplayName {
        display_name: String,
    },
    CreateUnderSubtreeTombstone {
        parent_inode: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    ReplaceFileInodeMissing {
        inode_id: InodeId,
    },
    ReplaceFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    ReplaceFileBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    RestoreRevisionInodeMissing {
        inode_id: InodeId,
    },
    RestoreRevisionInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    RestoreRevisionBaseRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    RestoreRevisionSourceRevisionMissing {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
    },
    RestoreRevisionUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    ReplaceFileUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    DeleteFileInodeMissing {
        inode_id: InodeId,
    },
    DeleteFileInodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    DeleteFileCoveredByTombstone {
        inode_id: InodeId,
        covering_root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RenameInodeMissing {
        inode_id: InodeId,
    },
    RenameSourceBindingMissing {
        inode_id: InodeId,
    },
    SourceBindingMissing {
        inode_id: InodeId,
    },
    RenameTargetParentMissing {
        parent_inode: InodeId,
    },
    RenameTargetParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    RenameTargetNameCollision {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
    },
    RenameWouldCycleDirectory {
        inode_id: InodeId,
        new_parent_inode: InodeId,
    },
    RenameInodeUnderSubtreeTombstone {
        inode_id: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RenameTargetParentUnderSubtreeTombstone {
        parent_inode: InodeId,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    UnsupportedRenameMode {
        mode: RenameMode,
    },
    DeleteSubtreeRootMissing {
        root_inode: InodeId,
    },
    DeleteSubtreeRootNotDirectory {
        root_inode: InodeId,
        actual_kind: InodeKind,
    },
    DeleteSubtreeRootCoveredByTombstone {
        root_inode: InodeId,
        covering_root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    RestoreRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    ReplaceFileRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    StaleWriterFenceToken {
        active: FenceToken,
        requested: FenceToken,
    },
    LeaseHolderMismatch {
        expected: String,
        actual: String,
    },
    LeaseExpired {
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    SeqOverflow,
    NextInodeOverflow,
    OpIndexOverflow,
    DeltaIndexOverflow,
}
