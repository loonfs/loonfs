#![forbid(unsafe_code)]

use loon_types::{
    ChangeSeq, ContentManifestEnvelope, FenceToken, InodeId, InodeKind, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelNamespace {
    pub namespace_id: NamespaceId,
    pub head_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub snapshot_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
    #[serde(default)]
    pub metadata_state: ModelMetadataState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelWalCommit {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub base_head_seq: ChangeSeq,
    pub commit_id: String,
    pub writer_fence_token: FenceToken,
    #[serde(default)]
    pub ops: Vec<ModelMetadataMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpoint {
    pub namespace_id: NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub retention_floor_seq: ChangeSeq,
    pub verified: bool,
    pub tables: Vec<ModelCheckpointTable>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCheckpointFamily {
    Inodes,
    Direntries,
    Revisions,
    Tombstones,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointSegment {
    pub object_key: String,
    pub segment_index: u32,
    pub row_count: u64,
    #[serde(default)]
    pub pages: Vec<ModelCheckpointPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointTable {
    pub family: ModelCheckpointFamily,
    pub segments: Vec<ModelCheckpointSegment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointPage {
    pub page_index: u32,
    pub min_key: String,
    pub max_key: String,
    #[serde(default)]
    pub row_keys: Vec<String>,
    #[serde(default)]
    pub rows: Vec<ModelCheckpointRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "row_kind", rename_all = "snake_case")]
pub enum ModelCheckpointRow {
    Inode {
        inode_id: InodeId,
        inode_kind: InodeKind,
        created_seq: ChangeSeq,
    },
    Direntry {
        parent_inode_id: InodeId,
        name_key: String,
        display_name: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
    },
    Revision {
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        content_manifest_digest: String,
    },
    Tombstone {
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ModelMetadataState {
    #[serde(default)]
    pub inodes: Vec<ModelInodeRecord>,
    #[serde(default)]
    pub direntries: Vec<ModelDirentryRecord>,
    #[serde(default)]
    pub revisions: Vec<ModelRevisionRecord>,
    #[serde(default)]
    pub subtree_tombstones: Vec<ModelSubtreeTombstoneRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInodeRecord {
    pub inode_id: InodeId,
    pub inode_kind: InodeKind,
    pub created_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDirentryRecord {
    pub parent_inode_id: InodeId,
    pub name_key: String,
    pub display_name: String,
    pub child_inode_id: InodeId,
    pub bind_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRevisionRecord {
    pub inode_id: InodeId,
    pub revision_no: RevisionNo,
    pub committed_seq: ChangeSeq,
    pub content_manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSubtreeTombstoneRecord {
    pub root_inode_id: InodeId,
    pub tombstone_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProgressObject {
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUploadedContent {
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_envelope: ContentManifestEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelLocalOnlyUploadRecord {
    pub namespace_id: NamespaceId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInodeUploadRecord {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelLocalOnlyUploadValidationError {
    MissingLocalContentDigest,
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    FileDigestMismatch {
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelLocalOnlyUploadDecision {
    ReuseExisting { content_manifest_digest: String },
    UploadFresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelInodeUploadValidationError {
    MissingLocalContentDigest,
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    FileDigestMismatch {
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelInodeUploadDecision {
    ReuseExisting { content_manifest_digest: String },
    UploadFresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPlannedLocalOnlyAction {
    pub client_file_id: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelPlannedInodeAction {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelObservedRemoteInode {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub inode_kind: loon_types::InodeKind,
    pub observed_seq: ChangeSeq,
    pub revision_no: loon_types::RevisionNo,
    pub content_digest: Option<String>,
    pub content_manifest_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub is_deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelLocalOnlyObservationCandidate {
    pub client_file_id: String,
    pub namespace_id: NamespaceId,
    pub inode_kind: loon_types::InodeKind,
    pub content_digest: Option<String>,
    pub parent_inode_id: Option<InodeId>,
    pub display_name: String,
    pub exists_on_disk: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelClientIssue {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub kind: String,
    pub summary: String,
    pub detail_json: Value,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelRemoteObservationSelectionError {
    AmbiguousLocalOnlyBind { matches: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelMetadataPreconditionError {
    ParentMissing {
        parent_inode_id: InodeId,
    },
    ParentNotDirectory {
        parent_inode_id: InodeId,
        actual_kind: InodeKind,
    },
    ChildNameCollision {
        parent_inode_id: InodeId,
        name_key: String,
        child_inode_id: InodeId,
    },
    InodeMissing {
        inode_id: InodeId,
    },
    InodeNotDirectory {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    InodeNotFile {
        inode_id: InodeId,
        actual_kind: InodeKind,
    },
    InodeRevisionMismatch {
        inode_id: InodeId,
        expected: RevisionNo,
        actual: Option<RevisionNo>,
    },
    SourceRevisionMissing {
        inode_id: InodeId,
        restore_from_revision: RevisionNo,
    },
    SourceRevisionNotHistorical {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        restore_from_revision: RevisionNo,
    },
    SourceBindingMissing {
        inode_id: InodeId,
    },
    RenameWouldCycle {
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    },
    AncestorCoveredBySubtreeTombstone {
        inode_id: InodeId,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelMetadataMutation {
    CreateDir {
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: String,
    },
    CreateFile {
        inode_id: InodeId,
        parent_inode_id: InodeId,
        display_name: String,
        content_manifest_digest: String,
    },
    ReplaceFile {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        content_manifest_digest: String,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        new_display_name: String,
    },
    RestoreRevision {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        restore_from_revision_no: RevisionNo,
    },
    DeleteSubtree {
        root_inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedModelMetadataState {
    pub metadata_state: ModelMetadataState,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelMetadataApplyError {
    RevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    RestoreSourceRevisionMissing {
        inode_id: InodeId,
        restore_from_revision_no: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelScheduledClientAction {
    LocalOnlyCreate(ModelPlannedLocalOnlyAction),
    PlannedInodeAction(ModelPlannedInodeAction),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelValidatedContent {
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub block_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelMaterializedContent {
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelContentValidationError {
    ManifestDigestMismatch {
        expected: String,
        actual: String,
    },
    ManifestNamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    MissingBlock {
        digest: String,
    },
    BlockLengthMismatch {
        digest: String,
        expected: u64,
        actual: u64,
    },
    BlockDigestMismatch {
        expected: String,
        actual: String,
    },
    FileSizeMismatch {
        expected: u64,
        actual: u64,
    },
    FileDigestMismatch {
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCheckpointPublishAuthorizers {
    pub required_progress: Vec<ModelProgressObject>,
    pub retention_policy: ModelProgressObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCommitValidationRequest {
    pub namespace_id: NamespaceId,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub planned_head_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCommitValidationOutcome {
    pub next_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelCommitValidationError {
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
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelQueueWorkClass {
    BuildSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelQueueJobState {
    Ready,
    Claimed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueBroker {
    pub broker_id: String,
    pub epoch: u64,
    pub lease_expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueClaim {
    pub worker_id: String,
    pub claim_token: String,
    pub heartbeat_at_ms: u64,
    pub timeout_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueSeqPayload {
    pub namespace_id: NamespaceId,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueJob {
    pub job_id: String,
    pub dedupe_key: String,
    pub state: ModelQueueJobState,
    pub payload: ModelQueueSeqPayload,
    pub follow_up: Option<ModelQueueSeqPayload>,
    pub claim: Option<ModelQueueClaim>,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelQueueShard {
    pub work_class: ModelQueueWorkClass,
    pub shard_id: u32,
    pub broker: Option<ModelQueueBroker>,
    pub jobs: Vec<ModelQueueJob>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelQueueRepairOutcome {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelBrokerLeaseOutcome {
    Acquired { epoch: u64 },
    Renewed { epoch: u64 },
    TakenOver { epoch: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelJobClaimOutcome {
    Claimed { claim_token: String },
    Stolen { claim_token: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelJobCompleteOutcome {
    Removed,
    PromotedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelAction {
    CreateDir {
        inode_id: InodeId,
        writer_fence_token: FenceToken,
    },
    CreateFile {
        inode_id: InodeId,
        writer_fence_token: FenceToken,
    },
    DeleteSubtree {
        root_inode: InodeId,
        writer_fence_token: FenceToken,
    },
    BumpSeq {
        writer_fence_token: FenceToken,
    },
    RotateFence {
        new_fence_token: FenceToken,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelError {
    StaleWriterFenceToken {
        expected: FenceToken,
        actual: FenceToken,
    },
    NamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    BaseHeadSeqMismatch {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    NonContiguousSeq {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    UnverifiedCheckpoint {
        checkpoint_seq: ChangeSeq,
    },
    MissingCheckpointSegment {
        object_key: String,
    },
    CheckpointAheadOfHead {
        checkpoint_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    RetentionFloorRegression {
        current: ChangeSeq,
        requested: ChangeSeq,
    },
    RetentionFloorBeyondCheckpoint {
        checkpoint_seq: ChangeSeq,
        requested: ChangeSeq,
    },
    MissingRetentionAuthorizers {
        requested: ChangeSeq,
    },
    ProgressNamespaceMismatch {
        work_class: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    ProgressWorkClassMismatch {
        expected: String,
        actual: String,
    },
    RequiredProgressLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    RetentionPolicyLag {
        work_class: String,
        requested: ChangeSeq,
        available: ChangeSeq,
    },
    QueueWorkClassMismatch {
        expected: ModelQueueWorkClass,
        actual: ModelQueueWorkClass,
    },
    MissingBrokerLease,
    BrokerLeaseHeldByOther {
        active_broker_id: String,
        active_epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    BrokerLeaseMismatch {
        expected_broker_id: String,
        expected_epoch: u64,
        actual_broker_id: String,
        actual_epoch: u64,
    },
    BrokerLeaseExpired {
        broker_id: String,
        epoch: u64,
        lease_expires_at_ms: u64,
        now_ms: u64,
    },
    JobNotFound {
        job_id: String,
    },
    JobBusy {
        job_id: String,
        worker_id: String,
        timeout_at_ms: u64,
        now_ms: u64,
    },
    JobNotClaimed {
        job_id: String,
    },
    ClaimTokenMismatch {
        expected: String,
        actual: String,
    },
    MetadataRevisionOverflow {
        inode_id: InodeId,
        base_revision_no: RevisionNo,
    },
    MetadataRestoreSourceRevisionMissing {
        inode_id: InodeId,
        restore_from_revision_no: RevisionNo,
    },
}

mod checkpoint;
mod client;
mod content;
mod metadata;
mod namespace;
mod queue;

pub use client::{
    allocate_client_request_id, bound_local_matches_remote_observation, download_transfer_id,
    expected_download_staged_size, local_apply_failed_issue, local_only_matches_remote_observation,
    local_only_upload_transfer_id, reconcile_download_resume_block_index,
    reconcile_upload_resume_block_index, remote_observation_bind_ambiguous_issue,
    remote_observation_is_stale, remote_only_discovery_supported,
    remote_only_placeholder_matches_remote_observation, reuse_or_allocate_client_request_id,
    select_local_only_observation_bind_candidate, select_next_client_action,
    select_next_local_only_action, upload_failed_issue, upload_transfer_id, upsert_client_issue,
};
pub use content::{
    build_uploaded_content, decide_inode_upload_action, decide_local_only_upload_action,
    materialize_uploaded_content_reference, validate_inode_upload_record,
    validate_local_only_upload_record, validate_uploaded_content_reference,
};

#[cfg(test)]
pub(crate) use queue::ensure_active_broker_lease;

#[cfg(test)]
mod tests;
