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

pub use self::api_adapter::{
    commit_request_from_v0, commit_request_from_v0_with_semantic_fingerprint, CommitConversionError,
};
pub(crate) use self::durable_adapter::wal_payload_from_materialized_commit;
pub use self::identity::{
    commit_identity, durable_commit_checksum_sha256, semantic_commit_fingerprint_sha256,
    source_api_commit_checksum_sha256, CommitIdentityError,
};
pub use self::ordered::build_commit_plan;
pub(crate) use self::ordered::resolve_restore_content_refs;
pub use self::prepared::{
    CommitExecutionContext, CommitIdentity, CommitPrepareError, MaterializedCommit, PreparedCommit,
};
pub use self::publish::{prepare_commit_head_publish, publish_commit_head};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRequest {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub planned_head_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_commit_fingerprint_sha256: Option<String>,
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
        base_revision: RevisionNo,
        content_ref: ContentRef,
    },
    RestoreRevision {
        inode_id: InodeId,
        source_revision: RevisionNo,
        base_revision: RevisionNo,
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
    HeadSeqIs(ChangeSeq),
    InodeRevisionIs {
        inode_id: InodeId,
        revision: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitPlan {
    pub namespace_id: NamespaceId,
    pub commit_id: CommitId,
    pub base_head_seq: ChangeSeq,
    pub next_seq: ChangeSeq,
    pub allocated_inode_ids: Vec<InodeId>,
    pub resolved_restore_content_refs: Vec<Option<ContentRef>>,
    pub resulting_next_inode_id: InodeId,
    pub durable_content_required: bool,
    pub wal_object_must_be_written: bool,
    pub head_cas_must_succeed: bool,
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
    PlannedHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    MissingHeadSeqPrecondition {
        expected: ChangeSeq,
    },
    ConflictingHeadSeqPrecondition {
        expected: ChangeSeq,
        actual: ChangeSeq,
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
        source_revision: RevisionNo,
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
        base_revision: RevisionNo,
    },
    ReplaceFileRevisionOverflow {
        inode_id: InodeId,
        base_revision: RevisionNo,
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
    PlanBaseHeadSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    PlanNextSeqMismatch {
        head: ChangeSeq,
        plan: ChangeSeq,
    },
    StaleHead,
    Codec(String),
    Store(String),
}

pub(crate) fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}
