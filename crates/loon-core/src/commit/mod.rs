use crate::metadata::MetadataState;
use loon_api::{
    v0::CommitAnnotations, ChangeSeq, CommitId, ContentRef, FenceToken, HeadState,
    HeadStateEnvelope, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

mod api_adapter;
mod durable_adapter;
mod frame;
mod identity;
mod ordered;
mod prepared;
mod publish;

pub use self::api_adapter::{commit_request_from_v0, CommitConversionError};
pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::{
    semantic_commit_fingerprint, semantic_commit_fingerprint_for_v0_request,
    CommitFingerprintError, SemanticCommitFingerprint,
};
pub use self::ordered::build_commit_plan;
pub(crate) use self::ordered::resolve_restore_content_refs;
pub(crate) use self::prepared::CommitFingerprintSource;
pub use self::prepared::{
    materialize_commit, CommitExecutionContext, CommitMaterializationError, CommitPrepareError,
    MaterializedCommit, MaterializedCommitOp, PreparedCommit,
};
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub ops: Vec<CommitOp>,
    pub preconditions: Vec<Precondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitOp {
    CreateDir {
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        parent_inode: InodeId,
        display_name: String,
        content_ref: ContentRef,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    RestoreRevision {
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision_no: RevisionNo,
    },
    DeleteFile {
        inode_id: InodeId,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precondition {
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
    ChildNameIs {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
    },
    DirectoryEmpty {
        inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub apply_after_seq: ChangeSeq,
    pub assigned_seq: ChangeSeq,
    pub allocated_inode_ids: Vec<InodeId>,
    pub resolved_restore_content_refs: Vec<Option<ContentRef>>,
    pub resulting_next_inode_id: InodeId,
    pub metadata_preconditions: Vec<Precondition>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitValidationContext {
    pub head: HeadState,
    pub lease: LeaseState,
    pub now_ms: u64,
    #[serde(default)]
    pub metadata_state: MetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedCommitHeadPublish {
    pub object_key: String,
    pub resulting_head: HeadState,
    pub envelope: HeadStateEnvelope,
    pub encoded_bytes: Vec<u8>,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitValidationError {
    EmptyCommit,
    NamespaceMismatch,
    HeadLeaseNamespaceMismatch,
    HeadLeaseFenceMismatch {
        head: FenceToken,
        lease: FenceToken,
    },
    ChildNamePreconditionParentMissing {
        parent_inode: InodeId,
    },
    ChildNamePreconditionParentNotDirectory {
        parent_inode: InodeId,
        actual_kind: InodeKind,
    },
    ChildNamePreconditionMissing {
        parent_inode: InodeId,
        name_key: String,
    },
    ChildNamePreconditionMismatch {
        parent_inode: InodeId,
        name_key: String,
        expected_child_inode: InodeId,
        actual_child_inode: InodeId,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitHeadPublishError {
    EmptyWriterVersion,
    EmptyExpectedHeadEtag,
    NamespaceMismatch {
        head: NamespaceId,
        plan: NamespaceId,
    },
    WalSegmentNamespaceMismatch {
        head: NamespaceId,
        wal: NamespaceId,
    },
    WalSegmentBaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    WalSegmentStartSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    WalSegmentEndSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    EmptyWalSegment,
    SeqOverflow,
    StaleHead,
    Codec(String),
    Store(String),
}

pub(crate) fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}
