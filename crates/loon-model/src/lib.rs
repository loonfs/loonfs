#![forbid(unsafe_code)]

use loon_types::{
    content_manifest_digest_sha256, sha256_digest, ChangeSeq, ContentBlockDescriptor,
    ContentManifestCodecError, ContentManifestEnvelope, ContentManifestPayload, FenceToken,
    InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo, CONTENT_BLOCK_SIZE_BYTES,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

impl ModelQueueShard {
    pub fn renew_broker_lease(
        &mut self,
        broker_id: &str,
        now_ms: u64,
        lease_duration_ms: u64,
    ) -> Result<ModelBrokerLeaseOutcome, ModelError> {
        match &mut self.broker {
            None => {
                self.broker = Some(ModelQueueBroker {
                    broker_id: broker_id.to_owned(),
                    epoch: 1,
                    lease_expires_at_ms: now_ms.saturating_add(lease_duration_ms),
                });

                Ok(ModelBrokerLeaseOutcome::Acquired { epoch: 1 })
            }
            Some(current)
                if current.broker_id == broker_id && current.lease_expires_at_ms > now_ms =>
            {
                current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
                Ok(ModelBrokerLeaseOutcome::Renewed {
                    epoch: current.epoch,
                })
            }
            Some(current) if current.lease_expires_at_ms <= now_ms => {
                current.broker_id = broker_id.to_owned();
                current.epoch = current.epoch.saturating_add(1);
                current.lease_expires_at_ms = now_ms.saturating_add(lease_duration_ms);
                Ok(ModelBrokerLeaseOutcome::TakenOver {
                    epoch: current.epoch,
                })
            }
            Some(current) => Err(ModelError::BrokerLeaseHeldByOther {
                active_broker_id: current.broker_id.clone(),
                active_epoch: current.epoch,
                lease_expires_at_ms: current.lease_expires_at_ms,
                now_ms,
            }),
        }
    }

    pub fn claim_job(
        &mut self,
        broker_id: &str,
        broker_epoch: u64,
        worker_id: &str,
        claim_token: &str,
        job_id: &str,
        now_ms: u64,
        claim_timeout_ms: u64,
    ) -> Result<ModelJobClaimOutcome, ModelError> {
        ensure_active_broker_lease(self, broker_id, broker_epoch, now_ms)?;

        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;

        let new_claim = ModelQueueClaim {
            worker_id: worker_id.to_owned(),
            claim_token: claim_token.to_owned(),
            heartbeat_at_ms: now_ms,
            timeout_at_ms: now_ms.saturating_add(claim_timeout_ms),
        };

        match job.state {
            ModelQueueJobState::Ready => {
                job.state = ModelQueueJobState::Claimed;
                job.claim = Some(new_claim.clone());
                job.attempts = job.attempts.saturating_add(1);
                Ok(ModelJobClaimOutcome::Claimed {
                    claim_token: new_claim.claim_token,
                })
            }
            ModelQueueJobState::Claimed => {
                let current = job
                    .claim
                    .as_ref()
                    .ok_or_else(|| ModelError::JobNotClaimed {
                        job_id: job_id.to_owned(),
                    })?;
                if current.timeout_at_ms > now_ms {
                    return Err(ModelError::JobBusy {
                        job_id: job_id.to_owned(),
                        worker_id: current.worker_id.clone(),
                        timeout_at_ms: current.timeout_at_ms,
                        now_ms,
                    });
                }

                job.claim = Some(new_claim.clone());
                job.attempts = job.attempts.saturating_add(1);
                Ok(ModelJobClaimOutcome::Stolen {
                    claim_token: new_claim.claim_token,
                })
            }
        }
    }

    pub fn heartbeat_job(
        &mut self,
        broker_id: &str,
        broker_epoch: u64,
        job_id: &str,
        claim_token: &str,
        now_ms: u64,
        claim_timeout_ms: u64,
    ) -> Result<(), ModelError> {
        ensure_active_broker_lease(self, broker_id, broker_epoch, now_ms)?;

        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;
        let claim = job
            .claim
            .as_mut()
            .ok_or_else(|| ModelError::JobNotClaimed {
                job_id: job_id.to_owned(),
            })?;
        if claim.claim_token != claim_token {
            return Err(ModelError::ClaimTokenMismatch {
                expected: claim.claim_token.clone(),
                actual: claim_token.to_owned(),
            });
        }

        claim.heartbeat_at_ms = now_ms;
        claim.timeout_at_ms = now_ms.saturating_add(claim_timeout_ms);
        Ok(())
    }

    pub fn complete_job(
        &mut self,
        broker_id: &str,
        broker_epoch: u64,
        job_id: &str,
        claim_token: &str,
        now_ms: u64,
    ) -> Result<ModelJobCompleteOutcome, ModelError> {
        ensure_active_broker_lease(self, broker_id, broker_epoch, now_ms)?;

        let job_index = self
            .jobs
            .iter()
            .position(|job| job.job_id == job_id)
            .ok_or_else(|| ModelError::JobNotFound {
                job_id: job_id.to_owned(),
            })?;

        {
            let claim =
                self.jobs[job_index]
                    .claim
                    .as_ref()
                    .ok_or_else(|| ModelError::JobNotClaimed {
                        job_id: job_id.to_owned(),
                    })?;
            if claim.claim_token != claim_token {
                return Err(ModelError::ClaimTokenMismatch {
                    expected: claim.claim_token.clone(),
                    actual: claim_token.to_owned(),
                });
            }
        }

        if let Some(follow_up) = self.jobs[job_index].follow_up.take() {
            let job = &mut self.jobs[job_index];
            job.state = ModelQueueJobState::Ready;
            job.payload = follow_up.clone();
            job.claim = None;
            return Ok(ModelJobCompleteOutcome::PromotedFollowUp {
                through_seq: follow_up.through_seq,
            });
        }

        self.jobs.remove(job_index);
        Ok(ModelJobCompleteOutcome::Removed)
    }
}

impl ModelNamespace {
    pub fn new(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            head_seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(1),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
            metadata_state: ModelMetadataState::default(),
        }
    }

    pub fn apply(&mut self, action: ModelAction) -> Result<(), ModelError> {
        match action {
            ModelAction::CreateDir {
                inode_id,
                writer_fence_token,
            }
            | ModelAction::CreateFile {
                inode_id,
                writer_fence_token,
            } => {
                if writer_fence_token != self.active_fence_token {
                    return Err(ModelError::StaleWriterFenceToken {
                        expected: self.active_fence_token,
                        actual: writer_fence_token,
                    });
                }
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
                self.next_inode_id =
                    InodeId(self.next_inode_id.0.max(inode_id.0.saturating_add(1)));
                Ok(())
            }
            ModelAction::DeleteSubtree {
                writer_fence_token, ..
            }
            | ModelAction::BumpSeq { writer_fence_token } => {
                if writer_fence_token != self.active_fence_token {
                    return Err(ModelError::StaleWriterFenceToken {
                        expected: self.active_fence_token,
                        actual: writer_fence_token,
                    });
                }
                self.head_seq = ChangeSeq(self.head_seq.0 + 1);
                Ok(())
            }
            ModelAction::RotateFence { new_fence_token } => {
                self.active_fence_token = new_fence_token;
                Ok(())
            }
        }
    }

    pub fn prepare_wal_commit(
        &self,
        commit_id: impl Into<String>,
        writer_fence_token: FenceToken,
    ) -> Result<ModelWalCommit, ModelError> {
        if writer_fence_token != self.active_fence_token {
            return Err(ModelError::StaleWriterFenceToken {
                expected: self.active_fence_token,
                actual: writer_fence_token,
            });
        }

        Ok(ModelWalCommit {
            namespace_id: self.namespace_id.clone(),
            seq: ChangeSeq(self.head_seq.0 + 1),
            base_head_seq: self.head_seq,
            commit_id: commit_id.into(),
            writer_fence_token,
            ops: Vec::new(),
        })
    }

    pub fn validate_commit_attempt(
        &self,
        request: &ModelCommitValidationRequest,
        lease: &LeaseState,
        now_ms: u64,
    ) -> Result<ModelCommitValidationOutcome, ModelCommitValidationError> {
        if request.namespace_id != self.namespace_id || request.namespace_id != lease.namespace_id {
            return Err(ModelCommitValidationError::NamespaceMismatch);
        }

        if self.namespace_id != lease.namespace_id {
            return Err(ModelCommitValidationError::HeadLeaseNamespaceMismatch);
        }

        if self.active_fence_token != lease.fence_token {
            return Err(ModelCommitValidationError::HeadLeaseFenceMismatch {
                head: self.active_fence_token,
                lease: lease.fence_token,
            });
        }

        if request.planned_head_seq != self.head_seq {
            return Err(ModelCommitValidationError::PlannedHeadSeqMismatch {
                expected: self.head_seq,
                actual: request.planned_head_seq,
            });
        }

        if request.writer_fence_token != self.active_fence_token {
            return Err(ModelCommitValidationError::StaleWriterFenceToken {
                expected: self.active_fence_token,
                actual: request.writer_fence_token,
            });
        }

        if request.writer_id != lease.holder_id {
            return Err(ModelCommitValidationError::LeaseHolderMismatch {
                expected: lease.holder_id.clone(),
                actual: request.writer_id.clone(),
            });
        }

        if !lease.is_valid_at(now_ms) {
            return Err(ModelCommitValidationError::LeaseExpired {
                lease_expires_at_ms: lease.lease_expires_at_ms,
                now_ms,
            });
        }

        let next_seq = self
            .head_seq
            .0
            .checked_add(1)
            .map(ChangeSeq)
            .ok_or(ModelCommitValidationError::SeqOverflow)?;

        Ok(ModelCommitValidationOutcome { next_seq })
    }

    pub fn replay_wal_commit(&mut self, wal: &ModelWalCommit) -> Result<(), ModelError> {
        if wal.namespace_id != self.namespace_id {
            return Err(ModelError::NamespaceMismatch {
                expected: self.namespace_id.clone(),
                actual: wal.namespace_id.clone(),
            });
        }

        if wal.base_head_seq != self.head_seq {
            return Err(ModelError::BaseHeadSeqMismatch {
                expected: self.head_seq,
                actual: wal.base_head_seq,
            });
        }

        let expected_next_seq = ChangeSeq(self.head_seq.0 + 1);
        if wal.seq != expected_next_seq {
            return Err(ModelError::NonContiguousSeq {
                expected: expected_next_seq,
                actual: wal.seq,
            });
        }

        let applied_metadata = self
            .metadata_state
            .apply_committed_mutations(wal.seq, &wal.ops)
            .map_err(|err| match err {
                ModelMetadataApplyError::RevisionOverflow {
                    inode_id,
                    base_revision_no,
                } => ModelError::MetadataRevisionOverflow {
                    inode_id,
                    base_revision_no,
                },
                ModelMetadataApplyError::RestoreSourceRevisionMissing {
                    inode_id,
                    restore_from_revision_no,
                } => ModelError::MetadataRestoreSourceRevisionMissing {
                    inode_id,
                    restore_from_revision_no,
                },
            })?;

        self.head_seq = wal.seq;
        self.active_fence_token = wal.writer_fence_token;
        self.metadata_state = applied_metadata.metadata_state;
        let replay_next_inode_id =
            wal.ops
                .iter()
                .fold(self.next_inode_id, |current, op| match op {
                    ModelMetadataMutation::CreateDir { inode_id, .. }
                    | ModelMetadataMutation::CreateFile { inode_id, .. } => {
                        InodeId(current.0.max(inode_id.0.saturating_add(1)))
                    }
                    ModelMetadataMutation::ReplaceFile { .. }
                    | ModelMetadataMutation::Rename { .. }
                    | ModelMetadataMutation::RestoreRevision { .. }
                    | ModelMetadataMutation::DeleteSubtree { .. } => current,
                });
        self.next_inode_id = replay_next_inode_id;
        Ok(())
    }

    pub fn checkpoint(&self) -> ModelCheckpoint {
        ModelCheckpoint {
            namespace_id: self.namespace_id.clone(),
            checkpoint_seq: self.head_seq,
            active_fence_token: self.active_fence_token,
            next_inode_id: self.next_inode_id,
            retention_floor_seq: self.retention_floor_seq,
            verified: true,
            tables: vec![
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Inodes,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Inodes,
                            0,
                        ),
                        segment_index: 0,
                        row_count: self.metadata_state.inodes.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Inodes,
                            &self.metadata_state,
                        )],
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Direntries,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Direntries,
                            0,
                        ),
                        segment_index: 0,
                        row_count: self.metadata_state.direntries.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Direntries,
                            &self.metadata_state,
                        )],
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Revisions,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Revisions,
                            0,
                        ),
                        segment_index: 0,
                        row_count: self.metadata_state.revisions.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Revisions,
                            &self.metadata_state,
                        )],
                    }],
                },
                ModelCheckpointTable {
                    family: ModelCheckpointFamily::Tombstones,
                    segments: vec![ModelCheckpointSegment {
                        object_key: checkpoint_segment_object_key(
                            &self.namespace_id,
                            self.head_seq,
                            ModelCheckpointFamily::Tombstones,
                            0,
                        ),
                        segment_index: 0,
                        row_count: self.metadata_state.subtree_tombstones.len() as u64,
                        pages: vec![build_model_checkpoint_page(
                            ModelCheckpointFamily::Tombstones,
                            &self.metadata_state,
                        )],
                    }],
                },
            ],
        }
    }

    pub fn publish_progress(
        &self,
        current: Option<&ModelProgressObject>,
        work_class: &str,
        requested_through_seq: ChangeSeq,
    ) -> Result<ModelProgressObject, ModelError> {
        if let Some(current) = current {
            if current.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: current.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: current.namespace_id.clone(),
                });
            }

            if current.work_class != work_class {
                return Err(ModelError::ProgressWorkClassMismatch {
                    expected: work_class.to_owned(),
                    actual: current.work_class.clone(),
                });
            }

            if current.through_seq >= requested_through_seq {
                return Ok(current.clone());
            }
        }

        Ok(ModelProgressObject {
            namespace_id: self.namespace_id.clone(),
            work_class: work_class.to_owned(),
            through_seq: requested_through_seq,
        })
    }

    pub fn repair_lost_snapshot_enqueue(
        &self,
        queue: &mut ModelQueueShard,
        progress: Option<&ModelProgressObject>,
    ) -> Result<ModelQueueRepairOutcome, ModelError> {
        if queue.work_class != ModelQueueWorkClass::BuildSnapshot {
            return Err(ModelError::QueueWorkClassMismatch {
                expected: ModelQueueWorkClass::BuildSnapshot,
                actual: queue.work_class,
            });
        }

        if self.head_seq == ChangeSeq(0) {
            return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
        }

        if let Some(progress) = progress {
            if progress.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: progress.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: progress.namespace_id.clone(),
                });
            }

            if progress.work_class != build_snapshot_work_class() {
                return Err(ModelError::ProgressWorkClassMismatch {
                    expected: build_snapshot_work_class().to_owned(),
                    actual: progress.work_class.clone(),
                });
            }

            if progress.through_seq >= self.head_seq {
                return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
            }
        }

        let desired_payload = ModelQueueSeqPayload {
            namespace_id: self.namespace_id.clone(),
            through_seq: self.head_seq,
        };
        let dedupe_key = build_snapshot_dedupe_key(&self.namespace_id);

        if let Some(job) = queue
            .jobs
            .iter_mut()
            .find(|job| job.dedupe_key == dedupe_key)
        {
            match job.state {
                ModelQueueJobState::Ready => {
                    if desired_payload.through_seq > job.payload.through_seq {
                        job.payload.through_seq = desired_payload.through_seq;
                        return Ok(ModelQueueRepairOutcome::RaisedReadyJob {
                            through_seq: job.payload.through_seq,
                        });
                    }
                }
                ModelQueueJobState::Claimed => match &mut job.follow_up {
                    Some(existing) => {
                        if desired_payload.through_seq > existing.through_seq {
                            existing.through_seq = desired_payload.through_seq;
                            return Ok(ModelQueueRepairOutcome::AttachedFollowUp {
                                through_seq: existing.through_seq,
                            });
                        }
                    }
                    None => {
                        job.follow_up = Some(desired_payload.clone());
                        return Ok(ModelQueueRepairOutcome::AttachedFollowUp {
                            through_seq: desired_payload.through_seq,
                        });
                    }
                },
            }

            return Ok(ModelQueueRepairOutcome::NoRepairNeeded);
        }

        queue.jobs.push(ModelQueueJob {
            job_id: build_snapshot_repair_job_id(&self.namespace_id),
            dedupe_key,
            state: ModelQueueJobState::Ready,
            payload: desired_payload.clone(),
            follow_up: None,
            claim: None,
            attempts: 0,
        });

        Ok(ModelQueueRepairOutcome::Enqueued {
            through_seq: desired_payload.through_seq,
        })
    }

    pub fn publish_checkpoint(
        &mut self,
        checkpoint: &ModelCheckpoint,
        available_segment_keys: &[String],
        requested_retention_floor_seq: Option<ChangeSeq>,
        authorizers: Option<&ModelCheckpointPublishAuthorizers>,
    ) -> Result<(), ModelError> {
        ensure_checkpoint_is_restorable(checkpoint, available_segment_keys)?;

        if checkpoint.checkpoint_seq > self.head_seq {
            return Err(ModelError::CheckpointAheadOfHead {
                checkpoint_seq: checkpoint.checkpoint_seq,
                head_seq: self.head_seq,
            });
        }

        self.snapshot_hint_seq = Some(
            self.snapshot_hint_seq
                .unwrap_or(checkpoint.checkpoint_seq)
                .max(checkpoint.checkpoint_seq),
        );

        if let Some(requested) = requested_retention_floor_seq {
            if requested < self.retention_floor_seq {
                return Err(ModelError::RetentionFloorRegression {
                    current: self.retention_floor_seq,
                    requested,
                });
            }

            if requested > checkpoint.checkpoint_seq {
                return Err(ModelError::RetentionFloorBeyondCheckpoint {
                    checkpoint_seq: checkpoint.checkpoint_seq,
                    requested,
                });
            }

            let authorizers =
                authorizers.ok_or(ModelError::MissingRetentionAuthorizers { requested })?;

            for progress in &authorizers.required_progress {
                if progress.namespace_id != self.namespace_id {
                    return Err(ModelError::ProgressNamespaceMismatch {
                        work_class: progress.work_class.clone(),
                        expected: self.namespace_id.clone(),
                        actual: progress.namespace_id.clone(),
                    });
                }

                if progress.through_seq < requested {
                    return Err(ModelError::RequiredProgressLag {
                        work_class: progress.work_class.clone(),
                        requested,
                        available: progress.through_seq,
                    });
                }
            }

            if authorizers.retention_policy.namespace_id != self.namespace_id {
                return Err(ModelError::ProgressNamespaceMismatch {
                    work_class: authorizers.retention_policy.work_class.clone(),
                    expected: self.namespace_id.clone(),
                    actual: authorizers.retention_policy.namespace_id.clone(),
                });
            }

            if authorizers.retention_policy.through_seq < requested {
                return Err(ModelError::RetentionPolicyLag {
                    work_class: authorizers.retention_policy.work_class.clone(),
                    requested,
                    available: authorizers.retention_policy.through_seq,
                });
            }

            self.retention_floor_seq = requested;
        }

        Ok(())
    }

    pub fn restore_from_checkpoint(
        checkpoint: &ModelCheckpoint,
        available_segment_keys: &[String],
    ) -> Result<Self, ModelError> {
        ensure_checkpoint_is_restorable(checkpoint, available_segment_keys)?;

        Ok(Self {
            namespace_id: checkpoint.namespace_id.clone(),
            head_seq: checkpoint.checkpoint_seq,
            active_fence_token: checkpoint.active_fence_token,
            next_inode_id: checkpoint.next_inode_id,
            snapshot_hint_seq: Some(checkpoint.checkpoint_seq),
            retention_floor_seq: checkpoint.retention_floor_seq,
            metadata_state: metadata_state_from_checkpoint(checkpoint)?,
        })
    }
}

pub fn build_uploaded_content(
    namespace_id: NamespaceId,
    bytes: &[u8],
) -> Result<ModelUploadedContent, ContentManifestCodecError> {
    let blocks = bytes
        .chunks(CONTENT_BLOCK_SIZE_BYTES as usize)
        .map(|block_bytes| ContentBlockDescriptor {
            content_digest_sha256: sha256_digest(block_bytes),
            plaintext_size_bytes: block_bytes.len() as u64,
        })
        .collect();
    let payload = ContentManifestPayload {
        namespace_id,
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256: sha256_digest(bytes),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks,
    };
    let manifest_envelope = ContentManifestEnvelope::from_payload(payload)?;
    let content_manifest_digest = content_manifest_digest_sha256(&manifest_envelope)?;

    Ok(ModelUploadedContent {
        file_size_bytes: manifest_envelope.payload.file_size_bytes,
        file_digest_sha256: manifest_envelope.payload.file_digest_sha256.clone(),
        content_manifest_digest,
        manifest_envelope,
    })
}

pub fn validate_local_only_upload_record(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    upload: &ModelLocalOnlyUploadRecord,
) -> Result<String, ModelLocalOnlyUploadValidationError> {
    let local_content_digest = local_content_digest
        .ok_or(ModelLocalOnlyUploadValidationError::MissingLocalContentDigest)?;

    if &upload.namespace_id != namespace_id {
        return Err(ModelLocalOnlyUploadValidationError::NamespaceMismatch {
            expected: namespace_id.clone(),
            actual: upload.namespace_id.clone(),
        });
    }

    if upload.file_digest_sha256 != local_content_digest {
        return Err(ModelLocalOnlyUploadValidationError::FileDigestMismatch {
            expected: local_content_digest.to_owned(),
            actual: upload.file_digest_sha256.clone(),
        });
    }

    Ok(upload.content_manifest_digest.clone())
}

pub fn validate_inode_upload_record(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    upload: &ModelInodeUploadRecord,
) -> Result<String, ModelInodeUploadValidationError> {
    let local_content_digest =
        local_content_digest.ok_or(ModelInodeUploadValidationError::MissingLocalContentDigest)?;

    if &upload.namespace_id != namespace_id {
        return Err(ModelInodeUploadValidationError::NamespaceMismatch {
            expected: namespace_id.clone(),
            actual: upload.namespace_id.clone(),
        });
    }

    if upload.file_digest_sha256 != local_content_digest {
        return Err(ModelInodeUploadValidationError::FileDigestMismatch {
            expected: local_content_digest.to_owned(),
            actual: upload.file_digest_sha256.clone(),
        });
    }

    Ok(upload.content_manifest_digest.clone())
}

pub fn decide_local_only_upload_action(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    existing_upload: Option<&ModelLocalOnlyUploadRecord>,
) -> Result<ModelLocalOnlyUploadDecision, ModelLocalOnlyUploadValidationError> {
    match existing_upload {
        Some(upload) => {
            match validate_local_only_upload_record(namespace_id, local_content_digest, upload) {
                Ok(content_manifest_digest) => Ok(ModelLocalOnlyUploadDecision::ReuseExisting {
                    content_manifest_digest,
                }),
                Err(ModelLocalOnlyUploadValidationError::NamespaceMismatch { .. })
                | Err(ModelLocalOnlyUploadValidationError::FileDigestMismatch { .. }) => {
                    Ok(ModelLocalOnlyUploadDecision::UploadFresh)
                }
                Err(other) => Err(other),
            }
        }
        None => Ok(ModelLocalOnlyUploadDecision::UploadFresh),
    }
}

pub fn decide_inode_upload_action(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    existing_upload: Option<&ModelInodeUploadRecord>,
) -> Result<ModelInodeUploadDecision, ModelInodeUploadValidationError> {
    match existing_upload {
        Some(upload) => {
            match validate_inode_upload_record(namespace_id, local_content_digest, upload) {
                Ok(content_manifest_digest) => Ok(ModelInodeUploadDecision::ReuseExisting {
                    content_manifest_digest,
                }),
                Err(ModelInodeUploadValidationError::NamespaceMismatch { .. })
                | Err(ModelInodeUploadValidationError::FileDigestMismatch { .. }) => {
                    Ok(ModelInodeUploadDecision::UploadFresh)
                }
                Err(other) => Err(other),
            }
        }
        None => Ok(ModelInodeUploadDecision::UploadFresh),
    }
}

pub fn allocate_client_request_id(next_counter: u64) -> String {
    format!("client-req-{next_counter:020}")
}

pub fn reuse_or_allocate_client_request_id(
    existing_request_id: Option<&str>,
    next_counter: u64,
) -> (String, bool) {
    match existing_request_id {
        Some(existing) => (existing.to_owned(), false),
        None => (allocate_client_request_id(next_counter), true),
    }
}

pub fn select_next_local_only_action(
    actions: &[ModelPlannedLocalOnlyAction],
) -> Option<ModelPlannedLocalOnlyAction> {
    actions
        .iter()
        .min_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.client_file_id.cmp(&right.client_file_id))
        })
        .cloned()
}

pub fn select_next_client_action(
    next_local_only: Option<&ModelPlannedLocalOnlyAction>,
    next_inode_action: Option<&ModelPlannedInodeAction>,
) -> Option<ModelScheduledClientAction> {
    match (next_local_only, next_inode_action) {
        (Some(local_only), Some(inode_action)) => {
            if local_only.created_at_ms <= inode_action.created_at_ms {
                Some(ModelScheduledClientAction::LocalOnlyCreate(
                    local_only.clone(),
                ))
            } else {
                Some(ModelScheduledClientAction::PlannedInodeAction(
                    inode_action.clone(),
                ))
            }
        }
        (Some(local_only), None) => Some(ModelScheduledClientAction::LocalOnlyCreate(
            local_only.clone(),
        )),
        (None, Some(inode_action)) => Some(ModelScheduledClientAction::PlannedInodeAction(
            inode_action.clone(),
        )),
        (None, None) => None,
    }
}

pub fn remote_observation_is_stale(
    current_observed_seq: Option<ChangeSeq>,
    incoming_observed_seq: ChangeSeq,
) -> bool {
    matches!(current_observed_seq, Some(current) if incoming_observed_seq <= current)
}

pub fn bound_local_matches_remote_observation(
    inode_kind: &loon_types::InodeKind,
    content_digest: Option<&str>,
    parent_inode_id: Option<InodeId>,
    display_name: &str,
    exists_on_disk: bool,
    observed: &ModelObservedRemoteInode,
) -> bool {
    exists_on_disk
        && !observed.is_deleted
        && observed.inode_kind == *inode_kind
        && observed.content_digest.as_deref() == content_digest
        && observed.parent_inode_id == parent_inode_id
        && observed.display_name == display_name
}

pub fn local_only_matches_remote_observation(
    candidate: &ModelLocalOnlyObservationCandidate,
    observed: &ModelObservedRemoteInode,
) -> bool {
    candidate.exists_on_disk
        && !observed.is_deleted
        && candidate.namespace_id == observed.namespace_id
        && candidate.inode_kind == observed.inode_kind
        && candidate.content_digest == observed.content_digest
        && candidate.parent_inode_id == observed.parent_inode_id
        && candidate.display_name == observed.display_name
}

pub fn select_local_only_observation_bind_candidate(
    candidates: &[ModelLocalOnlyObservationCandidate],
    observed: &ModelObservedRemoteInode,
) -> Result<Option<String>, ModelRemoteObservationSelectionError> {
    let mut matches = candidates
        .iter()
        .filter(|candidate| local_only_matches_remote_observation(candidate, observed))
        .map(|candidate| candidate.client_file_id.clone());
    let first = matches.next();
    let second = matches.next();
    match (first, second) {
        (None, _) => Ok(None),
        (Some(client_file_id), None) => Ok(Some(client_file_id)),
        (Some(_), Some(_)) => {
            let total_matches = candidates
                .iter()
                .filter(|candidate| local_only_matches_remote_observation(candidate, observed))
                .count();
            Err(
                ModelRemoteObservationSelectionError::AmbiguousLocalOnlyBind {
                    matches: total_matches,
                },
            )
        }
    }
}

impl ModelMetadataState {
    pub fn apply_committed_mutations(
        &self,
        committed_seq: ChangeSeq,
        mutations: &[ModelMetadataMutation],
    ) -> Result<AppliedModelMetadataState, ModelMetadataApplyError> {
        let mut metadata_state = self.clone();
        let mut checked_invariants = Vec::new();

        for mutation in mutations {
            match mutation {
                ModelMetadataMutation::CreateDir {
                    inode_id,
                    parent_inode_id,
                    display_name,
                } => {
                    metadata_state.inodes.push(ModelInodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::Dir,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_dir_writes_inode_and_direntry_rows",
                    );
                }
                ModelMetadataMutation::CreateFile {
                    inode_id,
                    parent_inode_id,
                    display_name,
                    content_manifest_digest,
                } => {
                    metadata_state.inodes.push(ModelInodeRecord {
                        inode_id: *inode_id,
                        inode_kind: InodeKind::File,
                        created_seq: committed_seq,
                    });
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *parent_inode_id,
                        name_key: display_name.clone(),
                        display_name: display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                    });
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: RevisionNo(1),
                        committed_seq,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "create_file_writes_inode_direntry_and_initial_revision",
                    );
                }
                ModelMetadataMutation::ReplaceFile {
                    inode_id,
                    base_revision_no,
                    content_manifest_digest,
                } => {
                    let next_revision_no =
                        base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                            ModelMetadataApplyError::RevisionOverflow {
                                inode_id: *inode_id,
                                base_revision_no: *base_revision_no,
                            },
                        )?;
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision_no,
                        committed_seq,
                        content_manifest_digest: content_manifest_digest.clone(),
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "replace_file_appends_new_revision_head",
                    );
                }
                ModelMetadataMutation::Rename {
                    inode_id,
                    new_parent_inode_id,
                    new_display_name,
                } => {
                    metadata_state.direntries.push(ModelDirentryRecord {
                        parent_inode_id: *new_parent_inode_id,
                        name_key: new_display_name.clone(),
                        display_name: new_display_name.clone(),
                        child_inode_id: *inode_id,
                        bind_seq: committed_seq,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "rename_appends_new_direntry_binding",
                    );
                }
                ModelMetadataMutation::RestoreRevision {
                    inode_id,
                    base_revision_no,
                    restore_from_revision_no,
                } => {
                    let next_revision_no =
                        base_revision_no.0.checked_add(1).map(RevisionNo).ok_or(
                            ModelMetadataApplyError::RevisionOverflow {
                                inode_id: *inode_id,
                                base_revision_no: *base_revision_no,
                            },
                        )?;
                    let source_revision = metadata_state
                        .revision_at_seq(*inode_id, *restore_from_revision_no, committed_seq)
                        .ok_or(ModelMetadataApplyError::RestoreSourceRevisionMissing {
                            inode_id: *inode_id,
                            restore_from_revision_no: *restore_from_revision_no,
                        })?;
                    metadata_state.revisions.push(ModelRevisionRecord {
                        inode_id: *inode_id,
                        revision_no: next_revision_no,
                        committed_seq,
                        content_manifest_digest: source_revision.content_manifest_digest,
                    });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "restore_creates_new_revision_head",
                    );
                }
                ModelMetadataMutation::DeleteSubtree { root_inode_id } => {
                    metadata_state
                        .subtree_tombstones
                        .push(ModelSubtreeTombstoneRecord {
                            root_inode_id: *root_inode_id,
                            tombstone_seq: committed_seq,
                        });
                    push_unique_invariant(
                        &mut checked_invariants,
                        "delete_subtree_writes_tombstone_row",
                    );
                }
            }
        }

        Ok(AppliedModelMetadataState {
            metadata_state,
            checked_invariants,
        })
    }

    pub fn inode_at_seq(&self, inode_id: InodeId, base_seq: ChangeSeq) -> Option<ModelInodeRecord> {
        self.inodes
            .iter()
            .find(|inode| inode.inode_id == inode_id && inode.created_seq <= base_seq)
            .cloned()
    }

    pub fn latest_revision_head_at_seq(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelRevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| revision.inode_id == inode_id && revision.committed_seq <= base_seq)
            .max_by_key(|revision| revision.revision_no)
            .cloned()
    }

    pub fn revision_at_seq(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Option<ModelRevisionRecord> {
        self.revisions
            .iter()
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= base_seq
            })
            .max_by_key(|revision| revision.committed_seq)
            .cloned()
    }

    pub fn bound_child_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && direntry.name_key == name_key
                    && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| direntry.bind_seq)
            .cloned()
    }

    pub fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        self.latest_parent_binding_for_child_at_seq(child_inode_id, base_seq)
    }

    pub fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelSubtreeTombstoneRecord> {
        self.subtree_tombstones
            .iter()
            .filter(|tombstone| {
                tombstone.root_inode_id == root_inode_id && tombstone.tombstone_seq <= base_seq
            })
            .max_by_key(|tombstone| tombstone.tombstone_seq)
            .cloned()
    }

    pub fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelSubtreeTombstoneRecord> {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }

            if let Some(tombstone) = self.active_subtree_tombstone(candidate_inode_id, base_seq) {
                return Some(tombstone);
            }

            current = self
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        None
    }

    pub fn visible_inode(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelInodeRecord> {
        let inode = self.inode_at_seq(inode_id, base_seq)?;
        if self
            .covering_subtree_tombstone(inode_id, base_seq)
            .is_some()
        {
            return None;
        }

        Some(inode)
    }

    pub fn current_revision_head(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelRevisionRecord> {
        self.visible_inode(inode_id, base_seq)?;
        self.latest_revision_head_at_seq(inode_id, base_seq)
    }

    pub fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        let parent = self.visible_inode(parent_inode_id, base_seq)?;
        if parent.inode_kind != InodeKind::Dir {
            return None;
        }

        let direntry = self.active_child_binding_at_seq(parent_inode_id, name_key, base_seq)?;
        self.visible_inode(direntry.child_inode_id, base_seq)?;
        Some(direntry)
    }

    pub fn ensure_child_name_absent(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let parent = self
            .inode_at_seq(parent_inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::ParentMissing { parent_inode_id })?;
        if parent.inode_kind != InodeKind::Dir {
            return Err(ModelMetadataPreconditionError::ParentNotDirectory {
                parent_inode_id,
                actual_kind: parent.inode_kind,
            });
        }

        if let Some(existing) = self.visible_child(parent_inode_id, name_key, base_seq) {
            return Err(ModelMetadataPreconditionError::ChildNameCollision {
                parent_inode_id,
                name_key: name_key.to_owned(),
                child_inode_id: existing.child_inode_id,
            });
        }

        Ok(())
    }

    pub fn ensure_inode_revision_is(
        &self,
        inode_id: InodeId,
        expected_revision_no: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::File {
            return Err(ModelMetadataPreconditionError::InodeNotFile {
                inode_id,
                actual_kind: inode.inode_kind,
            });
        }

        let actual_revision_no = self
            .latest_revision_head_at_seq(inode_id, base_seq)
            .map(|revision| revision.revision_no);
        if actual_revision_no != Some(expected_revision_no) {
            return Err(ModelMetadataPreconditionError::InodeRevisionMismatch {
                inode_id,
                expected: expected_revision_no,
                actual: actual_revision_no,
            });
        }

        Ok(())
    }

    pub fn ensure_inode_is_directory(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::Dir {
            return Err(ModelMetadataPreconditionError::InodeNotDirectory {
                inode_id,
                actual_kind: inode.inode_kind,
            });
        }

        Ok(())
    }

    pub fn ensure_restore_source_revision_exists(
        &self,
        inode_id: InodeId,
        base_revision_no: RevisionNo,
        restore_from_revision: RevisionNo,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        if self
            .revision_at_seq(inode_id, restore_from_revision, base_seq)
            .is_none()
        {
            return Err(ModelMetadataPreconditionError::SourceRevisionMissing {
                inode_id,
                restore_from_revision,
            });
        }
        if restore_from_revision >= base_revision_no {
            return Err(
                ModelMetadataPreconditionError::SourceRevisionNotHistorical {
                    inode_id,
                    base_revision_no,
                    restore_from_revision,
                },
            );
        }

        Ok(())
    }

    pub fn ensure_rename_source_binding_exists(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        self.inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if self
            .current_parent_binding_for_child(inode_id, base_seq)
            .is_none()
        {
            return Err(ModelMetadataPreconditionError::SourceBindingMissing { inode_id });
        }

        Ok(())
    }

    pub fn ensure_rename_does_not_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        let inode = self
            .inode_at_seq(inode_id, base_seq)
            .ok_or(ModelMetadataPreconditionError::InodeMissing { inode_id })?;
        if inode.inode_kind != InodeKind::Dir {
            return Ok(());
        }
        if self.is_ancestor_of(inode_id, new_parent_inode_id, base_seq) {
            return Err(ModelMetadataPreconditionError::RenameWouldCycle {
                inode_id,
                new_parent_inode_id,
            });
        }

        Ok(())
    }

    pub fn ensure_ancestors_not_subtree_deleted(
        &self,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Result<(), ModelMetadataPreconditionError> {
        if let Some(tombstone) = self.covering_subtree_tombstone(inode_id, base_seq) {
            return Err(
                ModelMetadataPreconditionError::AncestorCoveredBySubtreeTombstone {
                    inode_id,
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.tombstone_seq,
                },
            );
        }

        Ok(())
    }

    fn active_child_binding_at_seq(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        let direntry = self.bound_child_at_seq(parent_inode_id, name_key, base_seq)?;
        let latest_binding =
            self.latest_parent_binding_for_child_at_seq(direntry.child_inode_id, base_seq)?;
        if latest_binding.parent_inode_id != direntry.parent_inode_id
            || latest_binding.name_key != direntry.name_key
            || latest_binding.bind_seq != direntry.bind_seq
        {
            return None;
        }

        Some(direntry)
    }

    fn latest_parent_binding_for_child_at_seq(
        &self,
        child_inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> Option<ModelDirentryRecord> {
        self.direntries
            .iter()
            .filter(|direntry| {
                direntry.child_inode_id == child_inode_id && direntry.bind_seq <= base_seq
            })
            .max_by_key(|direntry| direntry.bind_seq)
            .cloned()
    }

    fn is_ancestor_of(
        &self,
        ancestor_inode_id: InodeId,
        inode_id: InodeId,
        base_seq: ChangeSeq,
    ) -> bool {
        let mut current = Some(inode_id);
        let mut visited = BTreeSet::new();

        while let Some(candidate_inode_id) = current {
            if !visited.insert(candidate_inode_id.0) {
                break;
            }
            if candidate_inode_id == ancestor_inode_id {
                return true;
            }
            current = self
                .latest_parent_binding_for_child_at_seq(candidate_inode_id, base_seq)
                .map(|direntry| direntry.parent_inode_id);
        }

        false
    }
}

fn push_unique_invariant(invariants: &mut Vec<String>, name: &str) {
    if !invariants.iter().any(|existing| existing == name) {
        invariants.push(name.to_owned());
    }
}

pub fn validate_uploaded_content_reference(
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
    manifest_envelope: &ContentManifestEnvelope,
    available_blocks: &BTreeMap<String, Vec<u8>>,
) -> Result<ModelValidatedContent, ModelContentValidationError> {
    let materialized = materialize_uploaded_content_reference(
        namespace_id,
        content_manifest_digest,
        manifest_envelope,
        available_blocks,
    )?;

    Ok(ModelValidatedContent {
        file_size_bytes: materialized.file_size_bytes,
        file_digest_sha256: materialized.file_digest_sha256,
        block_count: manifest_envelope.payload.blocks.len(),
    })
}

pub fn materialize_uploaded_content_reference(
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
    manifest_envelope: &ContentManifestEnvelope,
    available_blocks: &BTreeMap<String, Vec<u8>>,
) -> Result<ModelMaterializedContent, ModelContentValidationError> {
    let actual_manifest_digest = content_manifest_digest_sha256(manifest_envelope)
        .expect("content manifest envelope should always re-encode");
    if actual_manifest_digest != content_manifest_digest {
        return Err(ModelContentValidationError::ManifestDigestMismatch {
            expected: content_manifest_digest.to_owned(),
            actual: actual_manifest_digest,
        });
    }

    if &manifest_envelope.payload.namespace_id != namespace_id {
        return Err(ModelContentValidationError::ManifestNamespaceMismatch {
            expected: namespace_id.clone(),
            actual: manifest_envelope.payload.namespace_id.clone(),
        });
    }

    let mut reconstructed = Vec::new();
    for block in &manifest_envelope.payload.blocks {
        let bytes = available_blocks
            .get(&block.content_digest_sha256)
            .ok_or_else(|| ModelContentValidationError::MissingBlock {
                digest: block.content_digest_sha256.clone(),
            })?;
        let actual_size = bytes.len() as u64;
        if actual_size != block.plaintext_size_bytes {
            return Err(ModelContentValidationError::BlockLengthMismatch {
                digest: block.content_digest_sha256.clone(),
                expected: block.plaintext_size_bytes,
                actual: actual_size,
            });
        }

        let actual_digest = sha256_digest(bytes);
        if actual_digest != block.content_digest_sha256 {
            return Err(ModelContentValidationError::BlockDigestMismatch {
                expected: block.content_digest_sha256.clone(),
                actual: actual_digest,
            });
        }

        reconstructed.extend_from_slice(bytes);
    }

    let actual_file_size = reconstructed.len() as u64;
    if actual_file_size != manifest_envelope.payload.file_size_bytes {
        return Err(ModelContentValidationError::FileSizeMismatch {
            expected: manifest_envelope.payload.file_size_bytes,
            actual: actual_file_size,
        });
    }

    let actual_file_digest = sha256_digest(&reconstructed);
    if actual_file_digest != manifest_envelope.payload.file_digest_sha256 {
        return Err(ModelContentValidationError::FileDigestMismatch {
            expected: manifest_envelope.payload.file_digest_sha256.clone(),
            actual: actual_file_digest,
        });
    }

    Ok(ModelMaterializedContent {
        file_size_bytes: actual_file_size,
        file_digest_sha256: actual_file_digest,
        bytes: reconstructed,
    })
}

fn ensure_checkpoint_is_restorable(
    checkpoint: &ModelCheckpoint,
    available_segment_keys: &[String],
) -> Result<(), ModelError> {
    if !checkpoint.verified {
        return Err(ModelError::UnverifiedCheckpoint {
            checkpoint_seq: checkpoint.checkpoint_seq,
        });
    }

    let available: BTreeSet<&str> = available_segment_keys.iter().map(String::as_str).collect();
    for table in &checkpoint.tables {
        for segment in &table.segments {
            if !available.contains(segment.object_key.as_str()) {
                return Err(ModelError::MissingCheckpointSegment {
                    object_key: segment.object_key.clone(),
                });
            }
        }
    }

    Ok(())
}

fn metadata_state_from_checkpoint(
    checkpoint: &ModelCheckpoint,
) -> Result<ModelMetadataState, ModelError> {
    let mut metadata_state = ModelMetadataState::default();

    for table in &checkpoint.tables {
        for segment in &table.segments {
            for page in &segment.pages {
                for row in &page.rows {
                    match row {
                        ModelCheckpointRow::Inode {
                            inode_id,
                            inode_kind,
                            created_seq,
                        } => metadata_state.inodes.push(ModelInodeRecord {
                            inode_id: *inode_id,
                            inode_kind: inode_kind.clone(),
                            created_seq: *created_seq,
                        }),
                        ModelCheckpointRow::Direntry {
                            parent_inode_id,
                            name_key,
                            display_name,
                            child_inode_id,
                            bind_seq,
                        } => metadata_state.direntries.push(ModelDirentryRecord {
                            parent_inode_id: *parent_inode_id,
                            name_key: name_key.clone(),
                            display_name: display_name.clone(),
                            child_inode_id: *child_inode_id,
                            bind_seq: *bind_seq,
                        }),
                        ModelCheckpointRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            content_manifest_digest,
                        } => metadata_state.revisions.push(ModelRevisionRecord {
                            inode_id: *inode_id,
                            revision_no: *revision_no,
                            committed_seq: *committed_seq,
                            content_manifest_digest: content_manifest_digest.clone(),
                        }),
                        ModelCheckpointRow::Tombstone {
                            root_inode_id,
                            tombstone_seq,
                        } => metadata_state
                            .subtree_tombstones
                            .push(ModelSubtreeTombstoneRecord {
                                root_inode_id: *root_inode_id,
                                tombstone_seq: *tombstone_seq,
                            }),
                    }
                }
            }
        }
    }

    Ok(metadata_state)
}

fn build_model_checkpoint_page(
    family: ModelCheckpointFamily,
    metadata_state: &ModelMetadataState,
) -> ModelCheckpointPage {
    let rows = checkpoint_rows_for_family(family, metadata_state);
    let row_keys = rows
        .iter()
        .map(ModelCheckpointRow::row_key)
        .collect::<Vec<_>>();
    let min_key = row_keys.first().cloned().unwrap_or_default();
    let max_key = row_keys.last().cloned().unwrap_or_default();

    ModelCheckpointPage {
        page_index: 0,
        min_key,
        max_key,
        row_keys,
        rows,
    }
}

fn checkpoint_rows_for_family(
    family: ModelCheckpointFamily,
    metadata_state: &ModelMetadataState,
) -> Vec<ModelCheckpointRow> {
    match family {
        ModelCheckpointFamily::Inodes => {
            let mut rows = metadata_state
                .inodes
                .iter()
                .map(|inode| ModelCheckpointRow::Inode {
                    inode_id: inode.inode_id,
                    inode_kind: inode.inode_kind.clone(),
                    created_seq: inode.created_seq,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Direntries => {
            let mut rows = metadata_state
                .direntries
                .iter()
                .map(|direntry| ModelCheckpointRow::Direntry {
                    parent_inode_id: direntry.parent_inode_id,
                    name_key: direntry.name_key.clone(),
                    display_name: direntry.display_name.clone(),
                    child_inode_id: direntry.child_inode_id,
                    bind_seq: direntry.bind_seq,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Revisions => {
            let mut rows = metadata_state
                .revisions
                .iter()
                .map(|revision| ModelCheckpointRow::Revision {
                    inode_id: revision.inode_id,
                    revision_no: revision.revision_no,
                    committed_seq: revision.committed_seq,
                    content_manifest_digest: revision.content_manifest_digest.clone(),
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
        ModelCheckpointFamily::Tombstones => {
            let mut rows = metadata_state
                .subtree_tombstones
                .iter()
                .map(|tombstone| ModelCheckpointRow::Tombstone {
                    root_inode_id: tombstone.root_inode_id,
                    tombstone_seq: tombstone.tombstone_seq,
                })
                .collect::<Vec<_>>();
            rows.sort_by_key(ModelCheckpointRow::row_key);
            rows
        }
    }
}

fn checkpoint_segment_object_key(
    namespace_id: &NamespaceId,
    checkpoint_seq: ChangeSeq,
    family: ModelCheckpointFamily,
    segment_index: u32,
) -> String {
    format!(
        "namespaces/{}/snapshots/{:020}/tables/{}-{segment_index:05}.sst.zst",
        namespace_id.as_str(),
        checkpoint_seq.0,
        family.as_str()
    )
}

impl ModelCheckpointRow {
    fn row_key(&self) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::Direntry {
                parent_inode_id,
                name_key,
                bind_seq,
                ..
            } => format!(
                "direntry-{:020}-{name_key}-{:020}",
                parent_inode_id.0, bind_seq.0
            ),
            Self::Revision {
                inode_id,
                revision_no,
                ..
            } => format!("revision-{:020}-{:020}", inode_id.0, revision_no.0),
            Self::Tombstone {
                root_inode_id,
                tombstone_seq,
            } => format!("tombstone-{:020}-{:020}", root_inode_id.0, tombstone_seq.0),
        }
    }
}

fn ensure_active_broker_lease(
    queue: &ModelQueueShard,
    broker_id: &str,
    broker_epoch: u64,
    now_ms: u64,
) -> Result<(), ModelError> {
    let broker = queue
        .broker
        .as_ref()
        .ok_or(ModelError::MissingBrokerLease)?;

    if broker.broker_id != broker_id || broker.epoch != broker_epoch {
        return Err(ModelError::BrokerLeaseMismatch {
            expected_broker_id: broker.broker_id.clone(),
            expected_epoch: broker.epoch,
            actual_broker_id: broker_id.to_owned(),
            actual_epoch: broker_epoch,
        });
    }

    if broker.lease_expires_at_ms <= now_ms {
        return Err(ModelError::BrokerLeaseExpired {
            broker_id: broker.broker_id.clone(),
            epoch: broker.epoch,
            lease_expires_at_ms: broker.lease_expires_at_ms,
            now_ms,
        });
    }

    Ok(())
}

fn build_snapshot_work_class() -> &'static str {
    "BuildSnapshot"
}

fn build_snapshot_dedupe_key(namespace_id: &NamespaceId) -> String {
    format!("{}:{namespace_id}", build_snapshot_work_class())
}

fn build_snapshot_repair_job_id(namespace_id: &NamespaceId) -> String {
    format!("repair-{}-{namespace_id}", build_snapshot_work_class())
}

impl ModelCheckpointFamily {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::Direntries => "direntries",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loon_types::{InodeKind, RevisionNo};

    fn seeded_metadata_state() -> ModelMetadataState {
        ModelMetadataState {
            inodes: vec![
                ModelInodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                ModelInodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(12),
                },
                ModelInodeRecord {
                    inode_id: InodeId(88),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(21),
                },
            ],
            direntries: vec![
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(41),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(88),
                    bind_seq: ChangeSeq(21),
                },
            ],
            revisions: vec![
                ModelRevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(17),
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                },
                ModelRevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(2),
                    committed_seq: ChangeSeq(41),
                    content_manifest_digest: "sha256:note-v2".to_owned(),
                },
                ModelRevisionRecord {
                    inode_id: InodeId(88),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(21),
                    content_manifest_digest: "sha256:report-v1".to_owned(),
                },
            ],
            subtree_tombstones: vec![ModelSubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(40),
            }],
        }
    }

    #[test]
    fn model_advances_seq() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        assert_eq!(ns.head_seq.0, 1);
    }

    #[test]
    fn model_create_dir_advances_next_inode_id() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(7),
            writer_fence_token: FenceToken(0),
        })
        .expect("create dir should advance next inode id");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.next_inode_id, InodeId(8));
    }

    #[test]
    fn model_create_file_advances_next_inode_id() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::CreateFile {
            inode_id: InodeId(7),
            writer_fence_token: FenceToken(0),
        })
        .expect("create file should advance next inode id");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.next_inode_id, InodeId(8));
    }

    #[test]
    fn model_builds_uploaded_content_for_small_file() {
        let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
            .expect("build uploaded content");

        assert_eq!(uploaded.file_size_bytes, 16);
        assert_eq!(
            uploaded.file_digest_sha256,
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
        );
        assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 1);
        assert_eq!(
            uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
            16
        );
    }

    #[test]
    fn model_splits_content_at_fixed_block_boundary() {
        let mut bytes = vec![b'a'; CONTENT_BLOCK_SIZE_BYTES as usize];
        bytes.push(b'b');

        let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), &bytes)
            .expect("build uploaded content");

        assert_eq!(uploaded.manifest_envelope.payload.blocks.len(), 2);
        assert_eq!(
            uploaded.manifest_envelope.payload.blocks[0].plaintext_size_bytes,
            CONTENT_BLOCK_SIZE_BYTES
        );
        assert_eq!(
            uploaded.manifest_envelope.payload.blocks[1].plaintext_size_bytes,
            1
        );
    }

    #[test]
    fn model_validates_uploaded_content_reference() {
        let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
            .expect("build uploaded content");
        let mut blocks = BTreeMap::new();
        blocks.insert(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
            b"hello from loon\n".to_vec(),
        );

        let validated = validate_uploaded_content_reference(
            &NamespaceId::from("ns-1"),
            &uploaded.content_manifest_digest,
            &uploaded.manifest_envelope,
            &blocks,
        )
        .expect("validate uploaded content reference");

        assert_eq!(validated.file_size_bytes, 16);
        assert_eq!(
            validated.file_digest_sha256,
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
        );
        assert_eq!(validated.block_count, 1);
    }

    #[test]
    fn model_materializes_uploaded_content_reference() {
        let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
            .expect("build uploaded content");
        let mut blocks = BTreeMap::new();
        blocks.insert(
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9".to_owned(),
            b"hello from loon\n".to_vec(),
        );

        let materialized = materialize_uploaded_content_reference(
            &NamespaceId::from("ns-1"),
            &uploaded.content_manifest_digest,
            &uploaded.manifest_envelope,
            &blocks,
        )
        .expect("materialize uploaded content reference");

        assert_eq!(materialized.file_size_bytes, 16);
        assert_eq!(
            materialized.file_digest_sha256,
            "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
        );
        assert_eq!(materialized.bytes, b"hello from loon\n");
    }

    #[test]
    fn model_validates_local_only_upload_record() {
        let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let resolved = validate_local_only_upload_record(
            &NamespaceId::from("ns-1"),
            Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
            &upload,
        )
        .expect("validate local-only upload");

        assert_eq!(
            resolved,
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
        );
    }

    #[test]
    fn model_validates_inode_upload_record() {
        let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let resolved = validate_inode_upload_record(
            &NamespaceId::from("ns-1"),
            Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
            &upload,
        )
        .expect("validate inode upload");

        assert_eq!(
            resolved,
            "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
        );
    }

    #[test]
    fn model_reuses_matching_local_only_upload_record() {
        let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let decision = decide_local_only_upload_action(
            &NamespaceId::from("ns-1"),
            Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
            Some(&upload),
        )
        .expect("decide upload action");

        assert_eq!(
            decision,
            ModelLocalOnlyUploadDecision::ReuseExisting {
                content_manifest_digest:
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn model_reuses_matching_inode_upload_record() {
        let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let decision = decide_inode_upload_action(
            &NamespaceId::from("ns-1"),
            Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
            Some(&upload),
        )
        .expect("decide inode upload action");

        assert_eq!(
            decision,
            ModelInodeUploadDecision::ReuseExisting {
                content_manifest_digest:
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn model_reuploads_when_existing_local_only_upload_is_stale() {
        let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let decision = decide_local_only_upload_action(
            &NamespaceId::from("ns-1"),
            Some("sha256:edited-after-upload"),
            Some(&upload),
        )
        .expect("stale upload should trigger reupload");

        assert_eq!(decision, ModelLocalOnlyUploadDecision::UploadFresh);
    }

    #[test]
    fn model_reuploads_when_existing_inode_upload_is_stale() {
        let upload = ModelInodeUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            inode_id: InodeId(42),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let decision = decide_inode_upload_action(
            &NamespaceId::from("ns-1"),
            Some("sha256:edited-after-upload"),
            Some(&upload),
        )
        .expect("stale inode upload should trigger reupload");

        assert_eq!(decision, ModelInodeUploadDecision::UploadFresh);
    }

    #[test]
    fn model_allocates_client_request_ids_monotonically() {
        assert_eq!(
            allocate_client_request_id(1),
            "client-req-00000000000000000001"
        );
        assert_eq!(
            allocate_client_request_id(2),
            "client-req-00000000000000000002"
        );
    }

    #[test]
    fn model_reuses_existing_client_request_id_for_retry() {
        let (request_id, allocated_new) =
            reuse_or_allocate_client_request_id(Some("client-req-00000000000000000007"), 8);

        assert_eq!(request_id, "client-req-00000000000000000007");
        assert!(!allocated_new);
    }

    #[test]
    fn model_selects_next_local_only_action_deterministically() {
        let selected = select_next_local_only_action(&[
            ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000003".to_owned(),
                created_at_ms: 1_700_000_300_000,
            },
            ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                created_at_ms: 1_700_000_200_000,
            },
            ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                created_at_ms: 1_700_000_200_000,
            },
        ])
        .expect("one action should be selected");

        assert_eq!(
            selected,
            ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                created_at_ms: 1_700_000_200_000,
            }
        );
    }

    #[test]
    fn model_selects_next_client_action_preferring_local_only_on_tie() {
        let selected = select_next_client_action(
            Some(&ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                created_at_ms: 1_700_000_200_000,
            }),
            Some(&ModelPlannedInodeAction {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                created_at_ms: 1_700_000_200_000,
            }),
        )
        .expect("one action should be selected");

        assert_eq!(
            selected,
            ModelScheduledClientAction::LocalOnlyCreate(ModelPlannedLocalOnlyAction {
                client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                created_at_ms: 1_700_000_200_000,
            })
        );
    }

    #[test]
    fn model_selects_unique_local_only_bind_candidate_for_remote_observation() {
        let selected = select_local_only_observation_bind_candidate(
            &[
                ModelLocalOnlyObservationCandidate {
                    client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_kind: InodeKind::File,
                    content_digest: Some(
                        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                            .to_owned(),
                    ),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    exists_on_disk: true,
                },
                ModelLocalOnlyObservationCandidate {
                    client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_kind: InodeKind::File,
                    content_digest: Some("sha256:different".to_owned()),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "other.txt".to_owned(),
                    exists_on_disk: true,
                },
            ],
            &ModelObservedRemoteInode {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                content_manifest_digest: Some(
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                is_deleted: false,
            },
        )
        .expect("select unique bind candidate");

        assert_eq!(selected, Some("tmp:ns-1:00000000000000000001".to_owned()));
    }

    #[test]
    fn model_rejects_ambiguous_local_only_bind_candidate_for_remote_observation() {
        let error = select_local_only_observation_bind_candidate(
            &[
                ModelLocalOnlyObservationCandidate {
                    client_file_id: "tmp:ns-1:00000000000000000001".to_owned(),
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_kind: InodeKind::File,
                    content_digest: Some(
                        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                            .to_owned(),
                    ),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    exists_on_disk: true,
                },
                ModelLocalOnlyObservationCandidate {
                    client_file_id: "tmp:ns-1:00000000000000000002".to_owned(),
                    namespace_id: NamespaceId::from("ns-1"),
                    inode_kind: InodeKind::File,
                    content_digest: Some(
                        "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                            .to_owned(),
                    ),
                    parent_inode_id: Some(InodeId(2)),
                    display_name: "draft.txt".to_owned(),
                    exists_on_disk: true,
                },
            ],
            &ModelObservedRemoteInode {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(1),
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                content_manifest_digest: Some(
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "draft.txt".to_owned(),
                is_deleted: false,
            },
        )
        .expect_err("ambiguous local-only bind should be rejected");

        assert_eq!(
            error,
            ModelRemoteObservationSelectionError::AmbiguousLocalOnlyBind { matches: 2 }
        );
    }

    #[test]
    fn model_detects_bound_local_match_for_remote_observation() {
        let matches = bound_local_matches_remote_observation(
            &InodeKind::File,
            Some("sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"),
            Some(InodeId(2)),
            "report.txt",
            true,
            &ModelObservedRemoteInode {
                namespace_id: NamespaceId::from("ns-1"),
                inode_id: InodeId(42),
                inode_kind: InodeKind::File,
                observed_seq: ChangeSeq(42),
                revision_no: RevisionNo(18),
                content_digest: Some(
                    "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                        .to_owned(),
                ),
                content_manifest_digest: Some(
                    "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                        .to_owned(),
                ),
                parent_inode_id: Some(InodeId(2)),
                display_name: "report.txt".to_owned(),
                is_deleted: false,
            },
        );

        assert!(matches);
        assert!(remote_observation_is_stale(
            Some(ChangeSeq(42)),
            ChangeSeq(42)
        ));
        assert!(!remote_observation_is_stale(
            Some(ChangeSeq(41)),
            ChangeSeq(42)
        ));
    }

    #[test]
    fn model_child_name_absent_rejects_existing_bound_name() {
        let metadata = seeded_metadata_state();

        let error = metadata
            .ensure_child_name_absent(InodeId(2), "note.txt", ChangeSeq(41))
            .expect_err("existing child name should collide");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::ChildNameCollision {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                child_inode_id: InodeId(42),
            }
        );
    }

    #[test]
    fn model_inode_revision_is_rejects_stale_revision() {
        let metadata = seeded_metadata_state();

        let error = metadata
            .ensure_inode_revision_is(InodeId(42), RevisionNo(1), ChangeSeq(41))
            .expect_err("stale base revision should be rejected");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::InodeRevisionMismatch {
                inode_id: InodeId(42),
                expected: RevisionNo(1),
                actual: Some(RevisionNo(2)),
            }
        );
    }

    #[test]
    fn model_inode_is_directory_rejects_file_inode() {
        let metadata = seeded_metadata_state();

        let error = metadata
            .ensure_inode_is_directory(InodeId(42), ChangeSeq(41))
            .expect_err("file inode should be rejected");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::InodeNotDirectory {
                inode_id: InodeId(42),
                actual_kind: InodeKind::File,
            }
        );
    }

    #[test]
    fn model_ancestors_not_subtree_deleted_rejects_covered_inode() {
        let metadata = seeded_metadata_state();

        let error = metadata
            .ensure_ancestors_not_subtree_deleted(InodeId(88), ChangeSeq(41))
            .expect_err("covered descendant should be rejected");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::AncestorCoveredBySubtreeTombstone {
                inode_id: InodeId(88),
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(40),
            }
        );
    }

    #[test]
    fn model_distinguishes_raw_and_visible_metadata_queries() {
        let metadata = seeded_metadata_state();

        assert_eq!(
            metadata.inode_at_seq(InodeId(7), ChangeSeq(41)),
            Some(ModelInodeRecord {
                inode_id: InodeId(7),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(5),
            })
        );
        assert_eq!(metadata.visible_inode(InodeId(7), ChangeSeq(41)), None);
        assert_eq!(
            metadata.bound_child_at_seq(InodeId(2), "docs", ChangeSeq(41)),
            Some(ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "docs".to_owned(),
                display_name: "docs".to_owned(),
                child_inode_id: InodeId(7),
                bind_seq: ChangeSeq(5),
            })
        );
        assert_eq!(
            metadata.visible_child(InodeId(2), "docs", ChangeSeq(41)),
            None
        );
        assert_eq!(
            metadata.current_revision_head(InodeId(42), ChangeSeq(41)),
            Some(ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(2),
                committed_seq: ChangeSeq(41),
                content_manifest_digest: "sha256:note-v2".to_owned(),
            })
        );
    }

    #[test]
    fn model_apply_create_dir_appends_inode_and_direntry_rows() {
        let applied = ModelMetadataState::default()
            .apply_committed_mutations(
                ChangeSeq(42),
                &[ModelMetadataMutation::CreateDir {
                    inode_id: InodeId(501),
                    parent_inode_id: InodeId(2),
                    display_name: "drafts".to_owned(),
                }],
            )
            .expect("apply create_dir metadata");

        assert_eq!(
            applied.metadata_state.inodes,
            vec![ModelInodeRecord {
                inode_id: InodeId(501),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied.metadata_state.direntries,
            vec![ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "drafts".to_owned(),
                display_name: "drafts".to_owned(),
                child_inode_id: InodeId(501),
                bind_seq: ChangeSeq(42),
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"create_dir_writes_inode_and_direntry_rows".to_owned()));
    }

    #[test]
    fn model_apply_create_file_appends_initial_revision_row() {
        let applied = ModelMetadataState::default()
            .apply_committed_mutations(
                ChangeSeq(42),
                &[ModelMetadataMutation::CreateFile {
                    inode_id: InodeId(501),
                    parent_inode_id: InodeId(2),
                    display_name: "note.txt".to_owned(),
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                }],
            )
            .expect("apply create_file metadata");

        assert_eq!(
            applied.metadata_state.inodes,
            vec![ModelInodeRecord {
                inode_id: InodeId(501),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied.metadata_state.direntries,
            vec![ModelDirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "note.txt".to_owned(),
                display_name: "note.txt".to_owned(),
                child_inode_id: InodeId(501),
                bind_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied.metadata_state.revisions,
            vec![ModelRevisionRecord {
                inode_id: InodeId(501),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(42),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            }]
        );
        assert!(applied
            .checked_invariants
            .contains(&"create_file_writes_inode_direntry_and_initial_revision".to_owned()));
    }

    #[test]
    fn model_apply_replace_file_appends_next_revision_row() {
        let applied = seeded_metadata_state()
            .apply_committed_mutations(
                ChangeSeq(42),
                &[ModelMetadataMutation::ReplaceFile {
                    inode_id: InodeId(42),
                    base_revision_no: RevisionNo(2),
                    content_manifest_digest: "sha256:note-v3".to_owned(),
                }],
            )
            .expect("apply replace_file metadata");

        assert_eq!(
            applied
                .metadata_state
                .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
                .expect("revision head after replace"),
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(3),
                committed_seq: ChangeSeq(42),
                content_manifest_digest: "sha256:note-v3".to_owned(),
            }
        );
        assert!(applied
            .checked_invariants
            .contains(&"replace_file_appends_new_revision_head".to_owned()));
    }

    #[test]
    fn model_apply_restore_revision_appends_new_head_from_historical_content() {
        let applied = seeded_metadata_state()
            .apply_committed_mutations(
                ChangeSeq(42),
                &[ModelMetadataMutation::RestoreRevision {
                    inode_id: InodeId(42),
                    base_revision_no: RevisionNo(2),
                    restore_from_revision_no: RevisionNo(1),
                }],
            )
            .expect("apply restore_revision metadata");

        assert_eq!(
            applied
                .metadata_state
                .latest_revision_head_at_seq(InodeId(42), ChangeSeq(42))
                .expect("revision head after restore"),
            ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(3),
                committed_seq: ChangeSeq(42),
                content_manifest_digest: "sha256:note-v1".to_owned(),
            }
        );
        assert!(applied
            .checked_invariants
            .contains(&"restore_creates_new_revision_head".to_owned()));
    }

    #[test]
    fn model_apply_rename_appends_new_binding_and_hides_old_visible_name() {
        let applied = seeded_metadata_state()
            .apply_committed_mutations(
                ChangeSeq(42),
                &[ModelMetadataMutation::Rename {
                    inode_id: InodeId(42),
                    new_parent_inode_id: InodeId(2),
                    new_display_name: "renamed.txt".to_owned(),
                }],
            )
            .expect("apply rename metadata");

        assert_eq!(
            applied
                .metadata_state
                .visible_child(InodeId(2), "note.txt", ChangeSeq(42)),
            None
        );
        assert_eq!(
            applied
                .metadata_state
                .visible_child(InodeId(2), "renamed.txt", ChangeSeq(42))
                .expect("renamed visible child")
                .child_inode_id,
            InodeId(42)
        );
        assert!(applied
            .checked_invariants
            .contains(&"rename_appends_new_direntry_binding".to_owned()));
    }

    #[test]
    fn model_apply_delete_subtree_appends_tombstone_row_and_hides_descendants() {
        let applied = ModelMetadataState {
            inodes: vec![
                ModelInodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                ModelInodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(17),
                },
            ],
            direntries: vec![
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "report.txt".to_owned(),
                    display_name: "report.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                },
            ],
            revisions: vec![ModelRevisionRecord {
                inode_id: InodeId(42),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(17),
                content_manifest_digest: "sha256:report-v1".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        }
        .apply_committed_mutations(
            ChangeSeq(42),
            &[ModelMetadataMutation::DeleteSubtree {
                root_inode_id: InodeId(7),
            }],
        )
        .expect("apply delete_subtree metadata");

        assert_eq!(
            applied.metadata_state.subtree_tombstones,
            vec![ModelSubtreeTombstoneRecord {
                root_inode_id: InodeId(7),
                tombstone_seq: ChangeSeq(42),
            }]
        );
        assert_eq!(
            applied
                .metadata_state
                .visible_inode(InodeId(7), ChangeSeq(42)),
            None
        );
        assert_eq!(
            applied
                .metadata_state
                .visible_inode(InodeId(42), ChangeSeq(42)),
            None
        );
        assert!(applied
            .checked_invariants
            .contains(&"delete_subtree_writes_tombstone_row".to_owned()));
    }

    #[test]
    fn model_restore_source_must_be_historical() {
        let error = seeded_metadata_state()
            .ensure_restore_source_revision_exists(
                InodeId(42),
                RevisionNo(2),
                RevisionNo(2),
                ChangeSeq(41),
            )
            .expect_err("current head cannot be restore source");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::SourceRevisionNotHistorical {
                inode_id: InodeId(42),
                base_revision_no: RevisionNo(2),
                restore_from_revision: RevisionNo(2),
            }
        );
    }

    #[test]
    fn model_rejects_directory_rename_cycle() {
        let metadata_state = ModelMetadataState {
            inodes: vec![
                ModelInodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(7),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(5),
                },
                ModelInodeRecord {
                    inode_id: InodeId(9),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(8),
                },
            ],
            direntries: vec![
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "docs".to_owned(),
                    display_name: "docs".to_owned(),
                    child_inode_id: InodeId(7),
                    bind_seq: ChangeSeq(5),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(7),
                    name_key: "archive".to_owned(),
                    display_name: "archive".to_owned(),
                    child_inode_id: InodeId(9),
                    bind_seq: ChangeSeq(8),
                },
            ],
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };

        let error = metadata_state
            .ensure_rename_does_not_cycle(InodeId(7), InodeId(9), ChangeSeq(52))
            .expect_err("directory cycle should be rejected");

        assert_eq!(
            error,
            ModelMetadataPreconditionError::RenameWouldCycle {
                inode_id: InodeId(7),
                new_parent_inode_id: InodeId(9),
            }
        );
    }

    #[test]
    fn model_visible_child_prefers_latest_slot_binding_when_name_is_reused() {
        let metadata_state = ModelMetadataState {
            inodes: vec![
                ModelInodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                ModelInodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(10),
                },
                ModelInodeRecord {
                    inode_id: InodeId(77),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(30),
                },
            ],
            direntries: vec![
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(10),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "archive.txt".to_owned(),
                    display_name: "archive.txt".to_owned(),
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(20),
                },
                ModelDirentryRecord {
                    parent_inode_id: InodeId(2),
                    name_key: "note.txt".to_owned(),
                    display_name: "note.txt".to_owned(),
                    child_inode_id: InodeId(77),
                    bind_seq: ChangeSeq(30),
                },
            ],
            revisions: vec![
                ModelRevisionRecord {
                    inode_id: InodeId(42),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(10),
                    content_manifest_digest: "sha256:note-v1".to_owned(),
                },
                ModelRevisionRecord {
                    inode_id: InodeId(77),
                    revision_no: RevisionNo(1),
                    committed_seq: ChangeSeq(30),
                    content_manifest_digest: "sha256:note-v2".to_owned(),
                },
            ],
            subtree_tombstones: Vec::new(),
        };

        assert_eq!(
            metadata_state
                .visible_child(InodeId(2), "note.txt", ChangeSeq(30))
                .expect("latest visible note.txt binding")
                .child_inode_id,
            InodeId(77)
        );
        let old_child_binding = metadata_state
            .current_parent_binding_for_child(InodeId(42), ChangeSeq(30))
            .expect("latest binding for renamed-away inode");
        assert_eq!(old_child_binding.parent_inode_id, InodeId(2));
        assert_eq!(old_child_binding.name_key, "archive.txt");
    }

    #[test]
    fn model_rejects_local_only_upload_record_when_digest_mismatches() {
        let upload = ModelLocalOnlyUploadRecord {
            namespace_id: NamespaceId::from("ns-1"),
            file_digest_sha256:
                "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            content_manifest_digest:
                "sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6"
                    .to_owned(),
            manifest_object_key:
                "namespaces/ns-1/manifests/sha256:a7dd295b99876396927803c988ea9e657b53fd62d295a8483a013fd31b5660f6.json"
                    .to_owned(),
            file_size_bytes: 16,
        };

        let error = validate_local_only_upload_record(
            &NamespaceId::from("ns-1"),
            Some("sha256:different"),
            &upload,
        )
        .expect_err("mismatched digest should be rejected");

        assert_eq!(
            error,
            ModelLocalOnlyUploadValidationError::FileDigestMismatch {
                expected: "sha256:different".to_owned(),
                actual: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            }
        );
    }

    #[test]
    fn model_rejects_uploaded_content_reference_when_block_is_missing() {
        let uploaded = build_uploaded_content(NamespaceId::from("ns-1"), b"hello from loon\n")
            .expect("build uploaded content");
        let error = validate_uploaded_content_reference(
            &NamespaceId::from("ns-1"),
            &uploaded.content_manifest_digest,
            &uploaded.manifest_envelope,
            &BTreeMap::new(),
        )
        .expect_err("missing block should be rejected");

        assert_eq!(
            error,
            ModelContentValidationError::MissingBlock {
                digest: "sha256:9c5a4fd8b568931d08d0cde5b7980661c74239df0454b4c2f177ce8518aab2c9"
                    .to_owned(),
            }
        );
    }

    #[test]
    fn model_rejects_stale_writer_after_fence_rotation() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");

        let error = ns
            .apply(ModelAction::BumpSeq {
                writer_fence_token: FenceToken(8),
            })
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            ModelError::StaleWriterFenceToken {
                expected: FenceToken(9),
                actual: FenceToken(8),
            }
        );
    }

    #[test]
    fn model_accepts_active_commit_attempt() {
        let ns = ModelNamespace {
            namespace_id: NamespaceId::from("ns-1"),
            head_seq: ChangeSeq(41),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
            metadata_state: ModelMetadataState::default(),
        };
        let lease = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };
        let request = ModelCommitValidationRequest {
            namespace_id: NamespaceId::from("ns-1"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
        };

        let outcome = ns
            .validate_commit_attempt(&request, &lease, 999)
            .expect("active writer should validate");

        assert_eq!(
            outcome,
            ModelCommitValidationOutcome {
                next_seq: ChangeSeq(42),
            }
        );
    }

    #[test]
    fn model_stale_commit_attempt_hits_planned_head_seq_mismatch_after_handover_publish() {
        let ns = ModelNamespace {
            namespace_id: NamespaceId::from("ns-1"),
            head_seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(504),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
            metadata_state: ModelMetadataState::default(),
        };
        let lease = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-b".to_owned(),
            fence_token: FenceToken(9),
            lease_expires_at_ms: 2_000,
        };
        let request = ModelCommitValidationRequest {
            namespace_id: NamespaceId::from("ns-1"),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            planned_head_seq: ChangeSeq(41),
        };

        let error = ns
            .validate_commit_attempt(&request, &lease, 1_500)
            .expect_err("stale writer should be rejected");

        assert_eq!(
            error,
            ModelCommitValidationError::PlannedHeadSeqMismatch {
                expected: ChangeSeq(42),
                actual: ChangeSeq(41),
            }
        );
    }

    #[test]
    fn model_prepares_next_wal_commit_seq_for_active_writer() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ns
            .prepare_wal_commit("req-20260311-0001", FenceToken(0))
            .expect("active writer should prepare WAL");

        assert_eq!(wal.seq, ChangeSeq(1));
        assert_eq!(wal.base_head_seq, ChangeSeq(0));
        assert_eq!(wal.commit_id, "req-20260311-0001");
    }

    #[test]
    fn model_replays_contiguous_wal_commit() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ns
            .prepare_wal_commit("req-20260311-0001", FenceToken(0))
            .expect("active writer should prepare WAL");

        ns.replay_wal_commit(&wal)
            .expect("contiguous WAL should replay");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.active_fence_token, FenceToken(0));
    }

    #[test]
    fn model_rejects_non_contiguous_wal_commit() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let wal = ModelWalCommit {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(2),
            base_head_seq: ChangeSeq(0),
            commit_id: "req-20260311-0001".to_owned(),
            writer_fence_token: FenceToken(0),
            ops: Vec::new(),
        };

        let error = ns
            .replay_wal_commit(&wal)
            .expect_err("gap should be rejected");

        assert_eq!(
            error,
            ModelError::NonContiguousSeq {
                expected: ChangeSeq(1),
                actual: ChangeSeq(2),
            }
        );
    }

    #[test]
    fn model_restores_from_verified_checkpoint() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(41),
            writer_fence_token: FenceToken(9),
        })
        .expect("active writer should advance seq");

        let checkpoint = ns.checkpoint();
        let available_segment_keys = checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect::<Vec<_>>();
        let restored =
            ModelNamespace::restore_from_checkpoint(&checkpoint, &available_segment_keys)
                .expect("checkpoint restore");

        assert_eq!(restored.namespace_id, NamespaceId::from("ns-1"));
        assert_eq!(restored.head_seq, ChangeSeq(1));
        assert_eq!(restored.active_fence_token, FenceToken(9));
        assert_eq!(restored.next_inode_id, InodeId(42));
        assert_eq!(restored.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(restored.retention_floor_seq, ChangeSeq(0));
    }

    #[test]
    fn model_publishes_verified_checkpoint_into_head_summary() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::RotateFence {
            new_fence_token: FenceToken(9),
        })
        .expect("fence rotation should succeed");
        ns.apply(ModelAction::CreateDir {
            inode_id: InodeId(41),
            writer_fence_token: FenceToken(9),
        })
        .expect("active writer should advance seq");

        let checkpoint = ns.checkpoint();
        ns.publish_checkpoint(
            &checkpoint,
            &available_segment_keys(&checkpoint),
            Some(ChangeSeq(1)),
            Some(&sample_publish_authorizers(ChangeSeq(1))),
        )
        .expect("checkpoint publication should succeed");

        assert_eq!(ns.head_seq, ChangeSeq(1));
        assert_eq!(ns.active_fence_token, FenceToken(9));
        assert_eq!(ns.next_inode_id, InodeId(42));
        assert_eq!(ns.snapshot_hint_seq, Some(ChangeSeq(1)));
        assert_eq!(ns.retention_floor_seq, ChangeSeq(1));
    }

    #[test]
    fn model_progress_publish_is_monotonic() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let current = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(42),
        };

        let published = ns
            .publish_progress(Some(&current), "BuildSnapshot", ChangeSeq(41))
            .expect("stale progress publish should no-op");

        assert_eq!(published, current);
    }

    #[test]
    fn model_repair_enqueues_snapshot_job_when_progress_lags() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let progress = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(0),
        };
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![],
        };

        let outcome = ns
            .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
            .expect("repair should enqueue missing snapshot job");

        assert_eq!(
            outcome,
            ModelQueueRepairOutcome::Enqueued {
                through_seq: ChangeSeq(1),
            }
        );
        assert_eq!(queue.jobs.len(), 1);
        assert_eq!(queue.jobs[0].dedupe_key, "BuildSnapshot:ns-1");
        assert_eq!(queue.jobs[0].payload.through_seq, ChangeSeq(1));
    }

    #[test]
    fn model_repair_attaches_follow_up_for_claimed_snapshot_job() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq again");
        let progress = ModelProgressObject {
            namespace_id: NamespaceId::from("ns-1"),
            work_class: "BuildSnapshot".to_owned(),
            through_seq: ChangeSeq(0),
        };
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![ModelQueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: ModelQueueJobState::Claimed,
                payload: ModelQueueSeqPayload {
                    namespace_id: NamespaceId::from("ns-1"),
                    through_seq: ChangeSeq(1),
                },
                follow_up: None,
                claim: Some(ModelQueueClaim {
                    worker_id: "worker-a".to_owned(),
                    claim_token: "claim-a".to_owned(),
                    heartbeat_at_ms: 0,
                    timeout_at_ms: 10_000,
                }),
                attempts: 1,
            }],
        };

        let outcome = ns
            .repair_lost_snapshot_enqueue(&mut queue, Some(&progress))
            .expect("repair should attach follow-up to claimed job");

        assert_eq!(
            outcome,
            ModelQueueRepairOutcome::AttachedFollowUp {
                through_seq: ChangeSeq(2),
            }
        );
        assert_eq!(
            queue.jobs[0].follow_up,
            Some(ModelQueueSeqPayload {
                namespace_id: NamespaceId::from("ns-1"),
                through_seq: ChangeSeq(2),
            })
        );
    }

    #[test]
    fn model_broker_lease_takeover_fences_old_generation() {
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![],
        };

        assert_eq!(
            queue
                .renew_broker_lease("broker-a", 0, 10_000)
                .expect("first lease should be acquired"),
            ModelBrokerLeaseOutcome::Acquired { epoch: 1 }
        );
        assert_eq!(
            queue
                .renew_broker_lease("broker-b", 30_000, 10_000)
                .expect("expired lease should be takeable"),
            ModelBrokerLeaseOutcome::TakenOver { epoch: 2 }
        );
        assert_eq!(
            ensure_active_broker_lease(&queue, "broker-a", 1, 30_001)
                .expect_err("old broker generation should be fenced"),
            ModelError::BrokerLeaseMismatch {
                expected_broker_id: "broker-b".to_owned(),
                expected_epoch: 2,
                actual_broker_id: "broker-a".to_owned(),
                actual_epoch: 1,
            }
        );
    }

    #[test]
    fn model_claim_timeout_then_steal_rejects_stale_complete() {
        let mut queue = ModelQueueShard {
            work_class: ModelQueueWorkClass::BuildSnapshot,
            shard_id: 17,
            broker: None,
            jobs: vec![ModelQueueJob {
                job_id: "job-1".to_owned(),
                dedupe_key: "BuildSnapshot:ns-1".to_owned(),
                state: ModelQueueJobState::Ready,
                payload: ModelQueueSeqPayload {
                    namespace_id: NamespaceId::from("ns-1"),
                    through_seq: ChangeSeq(420),
                },
                follow_up: None,
                claim: None,
                attempts: 0,
            }],
        };
        queue
            .renew_broker_lease("broker-a", 0, 10_000)
            .expect("broker-a should acquire lease");
        assert_eq!(
            queue
                .claim_job("broker-a", 1, "worker-a", "claim-a", "job-1", 0, 10_000)
                .expect("worker-a should claim job"),
            ModelJobClaimOutcome::Claimed {
                claim_token: "claim-a".to_owned(),
            }
        );

        queue
            .renew_broker_lease("broker-b", 30_000, 10_000)
            .expect("broker-b should take over after expiry");
        assert_eq!(
            queue
                .claim_job("broker-b", 2, "worker-b", "claim-b", "job-1", 30_000, 10_000,)
                .expect("worker-b should steal expired job"),
            ModelJobClaimOutcome::Stolen {
                claim_token: "claim-b".to_owned(),
            }
        );

        assert_eq!(
            queue
                .complete_job("broker-b", 2, "job-1", "claim-a", 30_001)
                .expect_err("stale claim token should be rejected"),
            ModelError::ClaimTokenMismatch {
                expected: "claim-b".to_owned(),
                actual: "claim-a".to_owned(),
            }
        );
        assert_eq!(
            queue
                .complete_job("broker-b", 2, "job-1", "claim-b", 30_001)
                .expect("fresh claim should complete"),
            ModelJobCompleteOutcome::Removed
        );
        assert!(queue.jobs.is_empty());
    }

    #[test]
    fn model_rejects_retention_floor_without_authorizers() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(1)),
                None,
            )
            .expect_err("missing authorizers should fail");

        assert_eq!(
            error,
            ModelError::MissingRetentionAuthorizers {
                requested: ChangeSeq(1),
            }
        );
    }

    #[test]
    fn model_rejects_retention_floor_when_required_progress_lags() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();
        let authorizers = sample_publish_authorizers(ChangeSeq(0));

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(1)),
                Some(&authorizers),
            )
            .expect_err("lagging required progress should fail");

        assert_eq!(
            error,
            ModelError::RequiredProgressLag {
                work_class: "BuildListingIndex".to_owned(),
                requested: ChangeSeq(1),
                available: ChangeSeq(0),
            }
        );
    }

    #[test]
    fn model_rejects_retention_floor_above_checkpoint() {
        let mut ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        ns.apply(ModelAction::BumpSeq {
            writer_fence_token: FenceToken(0),
        })
        .expect("active writer should advance seq");
        let checkpoint = ns.checkpoint();

        let error = ns
            .publish_checkpoint(
                &checkpoint,
                &available_segment_keys(&checkpoint),
                Some(ChangeSeq(2)),
                Some(&sample_publish_authorizers(ChangeSeq(2))),
            )
            .expect_err("retention floor beyond checkpoint should fail");

        assert_eq!(
            error,
            ModelError::RetentionFloorBeyondCheckpoint {
                checkpoint_seq: ChangeSeq(1),
                requested: ChangeSeq(2),
            }
        );
    }

    #[test]
    fn model_checkpoint_includes_one_empty_segment_per_family() {
        let ns = ModelNamespace::new(NamespaceId::from("ns-1"));
        let checkpoint = ns.checkpoint();

        assert_eq!(checkpoint.tables.len(), 4);
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments.len() == 1));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].segment_index == 0));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].row_count == 0));
        assert!(checkpoint
            .tables
            .iter()
            .all(|table| table.segments[0].object_key.contains("/tables/")));
    }

    #[test]
    fn model_rejects_restore_when_checkpoint_segment_is_missing() {
        let checkpoint = ModelNamespace::new(NamespaceId::from("ns-1")).checkpoint();
        let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
            .expect_err("missing checkpoint segment should fail");

        assert_eq!(
            error,
            ModelError::MissingCheckpointSegment {
                object_key:
                    "namespaces/ns-1/snapshots/00000000000000000000/tables/inodes-00000.sst.zst"
                        .to_owned(),
            }
        );
    }

    #[test]
    fn model_rejects_unverified_checkpoint() {
        let checkpoint = ModelCheckpoint {
            namespace_id: NamespaceId::from("ns-1"),
            checkpoint_seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            retention_floor_seq: ChangeSeq(40),
            verified: false,
            tables: vec![],
        };

        let error = ModelNamespace::restore_from_checkpoint(&checkpoint, &[])
            .expect_err("unverified checkpoint should fail");

        assert_eq!(
            error,
            ModelError::UnverifiedCheckpoint {
                checkpoint_seq: ChangeSeq(40),
            }
        );
    }

    fn available_segment_keys(checkpoint: &ModelCheckpoint) -> Vec<String> {
        checkpoint
            .tables
            .iter()
            .flat_map(|table| {
                table
                    .segments
                    .iter()
                    .map(|segment| segment.object_key.clone())
            })
            .collect()
    }

    fn sample_publish_authorizers(through_seq: ChangeSeq) -> ModelCheckpointPublishAuthorizers {
        ModelCheckpointPublishAuthorizers {
            required_progress: vec![ModelProgressObject {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: "BuildListingIndex".to_owned(),
                through_seq,
            }],
            retention_policy: ModelProgressObject {
                namespace_id: NamespaceId::from("ns-1"),
                work_class: "RetentionPolicy".to_owned(),
                through_seq,
            },
        }
    }
}
