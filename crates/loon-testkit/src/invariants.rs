use loon_core::checkpoint::{StoredCheckpointManifest, StoredCheckpointSegment};
use loon_core::commit::CommitRequest;
use loon_core::metadata::{
    DirentryRecord, InodeRecord, MetadataState, RevisionRecord, SubtreeTombstoneRecord,
};
use loon_core::wal::PreparedWalCommit;
use loon_core::wal::StoredWalObject;
use loon_objectstore::keys::{
    blob, derived_progress, queue_shard, snapshot_manifest, snapshot_table, wal_commit,
    SnapshotTableFamily,
};
use loon_types::{
    checkpoint_page_checksum_sha256, content_manifest_payload_checksum_sha256,
    decode_checkpoint_manifest_json, decode_checkpoint_segment_envelope_zstd,
    decode_wal_commit_envelope_zstd, sha256_digest, ChangeSeq, CheckpointManifestEnvelope,
    CheckpointRow, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope, CheckpointTableFamily,
    ContentManifestEnvelope, HeadState, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
    WalCommitEnvelope, WalOp,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantCheck {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NamespaceCoreInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl NamespaceCoreInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackgroundWorkInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl BackgroundWorkInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContentObjectInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl ContentObjectInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointObjectInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl CheckpointObjectInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientTransferInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl ClientTransferInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClientReconciliationInvariantReport {
    pub checks: Vec<InvariantCheck>,
}

impl ClientReconciliationInvariantReport {
    pub fn check(&self, name: &str) -> Option<&InvariantCheck> {
        self.checks.iter().find(|check| check.name == name)
    }

    pub fn passed_names(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|check| check.passed)
            .map(|check| check.name.clone())
            .collect()
    }

    pub fn render_trace_lines(&self, label: &str) -> Vec<String> {
        self.checks
            .iter()
            .map(|check| {
                format!(
                    "invariants[{label}] {}={} detail={}",
                    check.name,
                    if check.passed { "pass" } else { "fail" },
                    check.detail
                )
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContentBlockSnapshot {
    pub object_key: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentObjectInvariantSnapshot {
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub manifest_bytes: Vec<u8>,
    pub available_blocks: BTreeMap<String, StoredContentBlockSnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub struct ContentObjectInvariantInputs<'a> {
    pub expected_namespace: &'a NamespaceId,
    pub content: &'a ContentObjectInvariantSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCheckpointSegmentSnapshot {
    pub object_key: String,
    pub encoded_bytes: Vec<u8>,
    pub envelope: CheckpointSegmentEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointObjectInvariantSnapshot {
    pub source_head: HeadState,
    pub source_basis_metadata: MetadataState,
    pub manifest_object_key: String,
    pub manifest_bytes: Vec<u8>,
    pub manifest_envelope: CheckpointManifestEnvelope,
    pub segments: Vec<StoredCheckpointSegmentSnapshot>,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointObjectInvariantInputs<'a> {
    pub checkpoint: &'a CheckpointObjectInvariantSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadTransferOutcomeKind {
    Progressed,
    ResetProgressed,
    Completed,
}

#[derive(Debug, Clone)]
pub struct DownloadTransferInvariantInputs<'a> {
    pub before_block_index: Option<u64>,
    pub after_transfer_block_index: Option<u64>,
    pub block_count: u64,
    pub reset_issue_kind: Option<&'a str>,
    pub reset_issue_reason: Option<&'a str>,
    pub remote_synced_seq: ChangeSeq,
    pub remote_revision_no: RevisionNo,
    pub remote_content_digest: Option<&'a str>,
    pub remote_content_manifest_digest: Option<&'a str>,
    pub local_exists_on_disk: bool,
    pub local_dirty: bool,
    pub local_content_digest: Option<&'a str>,
    pub sync_anchor_seq: Option<ChangeSeq>,
    pub sync_anchor_revision_no: Option<RevisionNo>,
    pub sync_anchor_content_digest: Option<&'a str>,
    pub sync_anchor_content_manifest_digest: Option<&'a str>,
    pub outcome: DownloadTransferOutcomeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeUploadTransferOutcomeKind {
    Progressed,
    ResetProgressed,
    Completed,
    RetryReusedPending,
}

#[derive(Debug, Clone)]
pub struct InodeUploadTransferInvariantInputs<'a> {
    pub before_block_index: Option<u64>,
    pub after_transfer_block_index: Option<u64>,
    pub block_count: u64,
    pub ensured_upload_present: bool,
    pub upload_reused: bool,
    pub before_pending_request_id: Option<&'a str>,
    pub after_pending_request_id: Option<&'a str>,
    pub reset_issue_kind: Option<&'a str>,
    pub reset_issue_reason: Option<&'a str>,
    pub outcome: InodeUploadTransferOutcomeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOnlyUploadTransferOutcomeKind {
    Progressed,
    ResetProgressed,
    Completed,
    RetryReusedPending,
}

#[derive(Debug, Clone)]
pub struct LocalOnlyUploadTransferInvariantInputs<'a> {
    pub before_block_index: Option<u64>,
    pub after_transfer_block_index: Option<u64>,
    pub block_count: u64,
    pub ensured_upload_present: bool,
    pub upload_reused: bool,
    pub before_pending_request_id: Option<&'a str>,
    pub after_pending_request_id: Option<&'a str>,
    pub reset_issue_kind: Option<&'a str>,
    pub reset_issue_reason: Option<&'a str>,
    pub local_only_file_present_after: bool,
    pub local_only_issue_count_after: usize,
    pub outcome: LocalOnlyUploadTransferOutcomeKind,
}

#[derive(Debug, Clone)]
pub struct RemoteObservationLateBindInvariantInputs<'a> {
    pub remote_present_after: bool,
    pub local_present_after: bool,
    pub sync_anchor_present_after: bool,
    pub local_dirty_after: bool,
    pub remote_content_digest_after: Option<&'a str>,
    pub local_content_digest_after: Option<&'a str>,
    pub sync_anchor_content_digest_after: Option<&'a str>,
    pub local_only_file_present_after: bool,
    pub planned_local_only_action_present_after: bool,
    pub local_only_upload_present_after: bool,
    pub local_only_transfer_present_after: bool,
    pub local_only_issue_count_after: usize,
    pub pending_client_mutation_present_after: bool,
}

#[derive(Debug, Clone)]
pub struct RemoteObservationConvergenceInvariantInputs<'a> {
    pub planned_action_present_after: bool,
    pub pending_inode_mutation_present_after: bool,
    pub local_dirty_after: bool,
    pub local_content_digest_after: Option<&'a str>,
    pub remote_synced_seq_after: ChangeSeq,
    pub remote_revision_no_after: RevisionNo,
    pub remote_content_digest_after: Option<&'a str>,
    pub sync_anchor_seq_after: Option<ChangeSeq>,
    pub sync_anchor_revision_no_after: Option<RevisionNo>,
    pub sync_anchor_content_digest_after: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct RemoteObservationAmbiguousBindInvariantInputs<'a> {
    pub issue_kind_after: Option<&'a str>,
    pub issue_matches_after: Option<usize>,
    pub remote_present_after: bool,
    pub local_present_after: bool,
    pub sync_anchor_present_after: bool,
    pub surviving_local_only_count_after: usize,
    pub initial_local_only_count: usize,
}

#[derive(Debug, Clone)]
pub struct RemoteObservationActiveUploadInvariantInputs {
    pub transfer_present_after: bool,
    pub pending_inode_mutation_present_after: bool,
    pub remote_synced_seq_after: ChangeSeq,
    pub expected_remote_synced_seq: ChangeSeq,
}

#[derive(Debug, Clone)]
pub struct RemoteObservationActiveDownloadInvariantInputs {
    pub transfer_present_after: bool,
    pub remote_synced_seq_after: ChangeSeq,
    pub expected_remote_synced_seq: ChangeSeq,
}

#[derive(Debug, Clone)]
pub struct RemoteOnlyDiscoveryInvariantInputs<'a> {
    pub inode_kind: InodeKind,
    pub local_exists_on_disk_after: bool,
    pub local_dirty_after: bool,
    pub sync_anchor_present_after: bool,
    pub planned_action_decision_after: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteOnlyDirectoryMaterializationOutcomeKind {
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct RemoteOnlyDirectoryMaterializationInvariantInputs<'a> {
    pub outcome: RemoteOnlyDirectoryMaterializationOutcomeKind,
    pub local_exists_on_disk_after: bool,
    pub local_dirty_after: bool,
    pub sync_anchor_present_after: bool,
    pub planned_action_present_after: bool,
    pub issue_kind_after: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct RemotePathChangePlanningInvariantInputs<'a> {
    pub planned_action_decision_after: Option<&'a str>,
    pub planned_action_reason_after: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct RemoteDeletePlanningInvariantInputs<'a> {
    pub planned_action_decision_after: Option<&'a str>,
    pub planned_action_reason_after: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct ApplyRemoteDeleteInvariantInputs<'a> {
    pub remote_present_after: bool,
    pub remote_is_deleted_after: bool,
    pub local_present_after: bool,
    pub sync_anchor_present_after: bool,
    pub planned_action_present_after: bool,
    pub issue_kind_after: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct ApplyRemoteRenameInvariantInputs<'a> {
    pub local_exists_on_disk_after: bool,
    pub local_dirty_after: bool,
    pub local_parent_inode_after: Option<InodeId>,
    pub local_display_name_after: &'a str,
    pub remote_synced_seq_after: ChangeSeq,
    pub remote_revision_no_after: RevisionNo,
    pub remote_content_digest_after: Option<&'a str>,
    pub remote_parent_inode_after: Option<InodeId>,
    pub remote_display_name_after: &'a str,
    pub sync_anchor_seq_after: Option<ChangeSeq>,
    pub sync_anchor_revision_no_after: Option<RevisionNo>,
    pub sync_anchor_content_digest_after: Option<&'a str>,
    pub sync_anchor_parent_inode_after: Option<InodeId>,
    pub sync_anchor_display_name_after: Option<&'a str>,
    pub planned_action_present_after: bool,
    pub issue_kind_after: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressPublishOutcomeKind {
    Created,
    Advanced,
    NoChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressInvariantSnapshot {
    pub object_key: String,
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
    pub payload_checksum_valid: bool,
}

#[derive(Debug, Clone)]
pub struct ProgressPublishInvariantInputs<'a> {
    pub expected_namespace: &'a NamespaceId,
    pub expected_work_class: &'a str,
    pub before_through_seq: Option<ChangeSeq>,
    pub requested_through_seq: ChangeSeq,
    pub outcome: ProgressPublishOutcomeKind,
    pub after_progress: &'a ProgressInvariantSnapshot,
}

#[derive(Debug, Clone)]
pub struct QueueShardObjectInvariantInputs<'a> {
    pub shard_index: u32,
    pub payload_checksum_valid: bool,
    pub object_key: &'a str,
    pub actual_shard_id: u32,
    pub cas_protected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueRepairOutcomeKind {
    NoRepairNeeded,
    Enqueued { through_seq: ChangeSeq },
    RaisedReadyJob { through_seq: ChangeSeq },
    AttachedFollowUp { through_seq: ChangeSeq },
}

#[derive(Debug, Clone)]
pub struct QueueRepairInvariantInputs<'a> {
    pub namespace_id: &'a NamespaceId,
    pub head_seq: ChangeSeq,
    pub progress_through_seq: Option<ChangeSeq>,
    pub outcome: QueueRepairOutcomeKind,
    pub has_namespace_scoped_job_after: bool,
    pub ready_job_through_seq_after: Option<ChangeSeq>,
    pub follow_up_through_seq_after: Option<ChangeSeq>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueBrokerLeaseOutcomeKind {
    Acquired { epoch: u64 },
    Renewed { epoch: u64 },
    TakenOver { epoch: u64 },
}

#[derive(Debug, Clone)]
pub struct QueueBrokerLeaseInvariantInputs<'a> {
    pub broker_id: &'a str,
    pub before_broker_id: Option<&'a str>,
    pub before_epoch: Option<u64>,
    pub after_broker_id: Option<&'a str>,
    pub after_epoch: Option<u64>,
    pub outcome: QueueBrokerLeaseOutcomeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueClaimOutcomeKind<'a> {
    Claimed { claim_token: &'a str },
    Stolen { claim_token: &'a str },
    Error,
}

#[derive(Debug, Clone)]
pub struct QueueClaimInvariantInputs<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub now_ms: u64,
    pub claim_token: &'a str,
    pub before_timeout_at_ms: Option<u64>,
    pub after_broker_id: Option<&'a str>,
    pub after_broker_epoch: Option<u64>,
    pub after_broker_lease_expires_at_ms: Option<u64>,
    pub after_claim_token: Option<&'a str>,
    pub outcome: QueueClaimOutcomeKind<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueHeartbeatOutcomeKind<'a> {
    Heartbeat {
        claim_token: &'a str,
        timeout_at_ms: u64,
    },
    Error,
}

#[derive(Debug, Clone)]
pub struct QueueHeartbeatInvariantInputs<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub now_ms: u64,
    pub claim_token: &'a str,
    pub after_broker_id: Option<&'a str>,
    pub after_broker_epoch: Option<u64>,
    pub after_broker_lease_expires_at_ms: Option<u64>,
    pub after_claim_token: Option<&'a str>,
    pub after_timeout_at_ms: Option<u64>,
    pub outcome: QueueHeartbeatOutcomeKind<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueCompleteOutcomeKind {
    Removed,
    PromotedFollowUp { through_seq: ChangeSeq },
    ClaimTokenMismatch,
    Error,
}

#[derive(Debug, Clone)]
pub struct QueueCompleteInvariantInputs<'a> {
    pub broker_id: &'a str,
    pub broker_epoch: u64,
    pub now_ms: u64,
    pub provided_claim_token: &'a str,
    pub before_claim_token: Option<&'a str>,
    pub after_broker_id: Option<&'a str>,
    pub after_broker_epoch: Option<u64>,
    pub after_broker_lease_expires_at_ms: Option<u64>,
    pub after_job_present: bool,
    pub prior_stolen_claim_seen: bool,
    pub outcome: QueueCompleteOutcomeKind,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointProgressAuthorizer<'a> {
    pub namespace_id: &'a NamespaceId,
    pub work_class: &'a str,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone)]
pub struct CheckpointHeadPublishInvariantInputs<'a> {
    pub current_head: &'a HeadState,
    pub checkpoint_namespace: &'a NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub checkpoint_verified: bool,
    pub checkpoint_segments_verified: bool,
    pub requested_retention_floor_seq: Option<ChangeSeq>,
    pub required_progress: &'a [CheckpointProgressAuthorizer<'a>],
    pub retention_policy: Option<CheckpointProgressAuthorizer<'a>>,
    pub resulting_head: &'a HeadState,
}

#[derive(Debug, Clone, Copy)]
pub struct CommitInvariantInputs<'a> {
    pub request: &'a CommitRequest,
    pub before_head: &'a HeadState,
    pub before_lease: &'a LeaseState,
    pub before_metadata: &'a MetadataState,
    pub prepared_wal: &'a PreparedWalCommit,
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
    pub now_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct WalReplayInvariantInputs<'a> {
    pub expected_namespace: &'a str,
    pub basis_head: &'a HeadState,
    pub basis_metadata: &'a MetadataState,
    pub wal_objects: &'a [StoredWalObject],
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckpointReplayInvariantInputs<'a> {
    pub expected_namespace: &'a str,
    pub stored_manifest: &'a StoredCheckpointManifest,
    pub stored_segments: &'a [StoredCheckpointSegment],
    pub basis_head: &'a HeadState,
    pub basis_metadata: &'a MetadataState,
    pub wal_objects: &'a [StoredWalObject],
    pub after_head: &'a HeadState,
    pub after_metadata: &'a MetadataState,
}

pub fn evaluate_namespace_commit_invariants(
    inputs: CommitInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let mut checks = Vec::new();
    let wal_payload = &inputs.prepared_wal.envelope.payload;
    let committed_seq = wal_payload.seq;

    checks.push(InvariantCheck {
        name: "stale_writer_cannot_publish".to_owned(),
        passed: inputs.request.writer_fence_token == inputs.before_head.active_fence_token
            && inputs.request.writer_fence_token == inputs.before_lease.fence_token
            && inputs.request.writer_id == inputs.before_lease.holder_id
            && inputs.before_lease.is_valid_at(inputs.now_ms),
        detail: format!(
            "request_fence={} head_fence={} lease_fence={} holder={} lease_valid_at_now={}",
            inputs.request.writer_fence_token.0,
            inputs.before_head.active_fence_token.0,
            inputs.before_lease.fence_token.0,
            inputs.before_lease.holder_id,
            inputs.before_lease.is_valid_at(inputs.now_ms)
        ),
    });
    checks.push(InvariantCheck {
        name: "head_and_lease_fence_tokens_agree".to_owned(),
        passed: inputs.before_head.namespace_id == inputs.before_lease.namespace_id
            && inputs.before_head.active_fence_token == inputs.before_lease.fence_token,
        detail: format!(
            "head_ns={} lease_ns={} head_fence={} lease_fence={}",
            inputs.before_head.namespace_id,
            inputs.before_lease.namespace_id,
            inputs.before_head.active_fence_token.0,
            inputs.before_lease.fence_token.0
        ),
    });
    checks.push(InvariantCheck {
        name: "next_inode_id_is_monotonic".to_owned(),
        passed: inputs.after_head.next_inode_id.0 >= inputs.before_head.next_inode_id.0,
        detail: format!(
            "before_next_inode={} after_next_inode={}",
            inputs.before_head.next_inode_id.0, inputs.after_head.next_inode_id.0
        ),
    });
    checks.push(check_create_mutation_consumes_next_inode_id(
        inputs.before_head.next_inode_id,
        &wal_payload.ops,
        inputs.after_head.next_inode_id,
    ));
    checks.push(check_requires_durable_content(
        "create_file_requires_durable_content",
        &wal_payload.ops,
        inputs.after_metadata,
        |op| match op {
            WalOp::CreateFile {
                op_index,
                inode_id,
                content_manifest_digest,
                ..
            } => Some(inputs.after_metadata.revisions.iter().any(|revision| {
                revision.inode_id == *inode_id
                    && revision.revision_no == RevisionNo(1)
                    && revision.committed_seq == committed_seq
                    && revision.revision_op_index == *op_index
                    && revision.content_manifest_digest == *content_manifest_digest
            })),
            _ => None,
        },
    ));
    checks.push(check_requires_durable_content(
        "replace_file_requires_durable_content",
        &wal_payload.ops,
        inputs.after_metadata,
        |op| match op {
            WalOp::ReplaceFile {
                op_index,
                inode_id,
                base_revision,
                content_manifest_digest,
            } => Some(inputs.after_metadata.revisions.iter().any(|revision| {
                revision.inode_id == *inode_id
                    && revision.revision_no == RevisionNo(base_revision.0 + 1)
                    && revision.committed_seq == committed_seq
                    && revision.revision_op_index == *op_index
                    && revision.content_manifest_digest == *content_manifest_digest
            })),
            _ => None,
        },
    ));
    checks.push(check_subtree_tombstone_blocks_descendant_mutation(
        inputs.before_metadata,
        committed_seq,
        &wal_payload.ops,
    ));
    checks.extend(evaluate_metadata_apply_invariants(
        inputs.before_metadata,
        &sequenced_ops_for_commit(committed_seq, &wal_payload.ops),
        inputs.after_metadata,
    ));

    NamespaceCoreInvariantReport { checks }
}

pub fn evaluate_namespace_wal_replay_invariants(
    inputs: WalReplayInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let decoded = decode_wal_tail(inputs.wal_objects);
    let sequenced_ops = match decoded.as_ref() {
        Ok(decoded) => flatten_wal_ops(decoded),
        Err(_) => Vec::new(),
    };
    let metadata_apply_checks = match decoded.as_ref() {
        Ok(_) => evaluate_metadata_apply_invariants(
            inputs.basis_metadata,
            &sequenced_ops,
            inputs.after_metadata,
        ),
        Err(error) => metadata_apply_checks_decode_failure(error),
    };

    let mut checks = vec![
        wal_payload_checksum_check(&decoded),
        wal_key_matches_committed_seq_check(&decoded),
        head_publish_requires_durable_wal_check(
            inputs.basis_head,
            inputs.after_head,
            decoded.as_ref().ok().map(Vec::as_slice),
        ),
        wal_replay_requires_matching_namespace_check(inputs.expected_namespace, &decoded),
        wal_replay_requires_matching_base_head_seq_check(inputs.basis_head, &decoded),
        wal_tail_seq_is_contiguous_check(inputs.basis_head, &decoded),
        wal_replay_applies_metadata_rows_check(
            inputs.basis_head,
            inputs.basis_metadata,
            inputs.after_head,
            inputs.after_metadata,
            decoded.as_ref().ok().map(Vec::as_slice),
        ),
    ];
    checks.extend(metadata_apply_checks);

    NamespaceCoreInvariantReport { checks }
}

pub fn evaluate_namespace_checkpoint_replay_invariants(
    inputs: CheckpointReplayInvariantInputs<'_>,
) -> NamespaceCoreInvariantReport {
    let manifest = decode_checkpoint_manifest_json(&inputs.stored_manifest.encoded_bytes)
        .map_err(|err| err.to_string());
    let decoded_segments = decode_checkpoint_segments(inputs.stored_segments);
    let reconstructed_basis =
        reconstruct_checkpoint_metadata(inputs.stored_segments, manifest.as_ref().ok());
    let replay_report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
        expected_namespace: inputs.expected_namespace,
        basis_head: inputs.basis_head,
        basis_metadata: inputs.basis_metadata,
        wal_objects: inputs.wal_objects,
        after_head: inputs.after_head,
        after_metadata: inputs.after_metadata,
    });

    let mut checks = vec![
        InvariantCheck {
            name: "checkpoint_manifest_checksum_matches_payload".to_owned(),
            passed: manifest.is_ok(),
            detail: match &manifest {
                Ok(decoded) => format!(
                    "manifest_checksum_verified checkpoint_seq={}",
                    decoded.payload.checkpoint_seq.0
                ),
                Err(error) => format!("manifest_decode_failed error={error}"),
            },
        },
        checkpoint_manifest_key_matches_seq_check(inputs.stored_manifest, manifest.as_ref().ok()),
        checkpoint_manifest_must_be_verified_check(manifest.as_ref().ok()),
        checkpoint_replay_requires_all_manifest_segments_check(
            inputs.stored_segments,
            manifest.as_ref().ok(),
        ),
        checkpoint_segment_descriptor_matches_payload_check(
            inputs.stored_segments,
            manifest.as_ref().ok(),
            decoded_segments.as_ref().ok().map(Vec::as_slice),
        ),
        checkpoint_segment_rows_restore_basis_metadata_check(
            reconstructed_basis.as_ref().ok(),
            inputs.basis_metadata,
        ),
        checkpoint_plus_wal_tail_reproduces_head_check(
            inputs.basis_head,
            inputs.after_head,
            replay_report.check("head_publish_requires_durable_wal"),
            replay_report.check("wal_tail_seq_is_contiguous"),
            inputs.wal_objects,
        ),
        checkpoint_plus_wal_tail_reproduces_metadata_check(
            replay_report.check("wal_replay_applies_metadata_rows"),
            inputs.after_metadata,
        ),
    ];
    checks.extend(replay_report.checks);

    NamespaceCoreInvariantReport { checks }
}

pub fn evaluate_progress_publish_invariants(
    inputs: ProgressPublishInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let expected_key = derived_progress(
        inputs.expected_namespace.as_str(),
        inputs.expected_work_class,
    );
    let key_matches = inputs.after_progress.object_key == expected_key
        && inputs.after_progress.namespace_id == *inputs.expected_namespace
        && inputs.after_progress.work_class == inputs.expected_work_class;
    let before = inputs.before_through_seq;
    let monotonic = match before {
        None => {
            inputs.outcome == ProgressPublishOutcomeKind::Created
                && inputs.after_progress.through_seq == inputs.requested_through_seq
        }
        Some(before_through_seq) if before_through_seq >= inputs.requested_through_seq => {
            inputs.outcome == ProgressPublishOutcomeKind::NoChange
                && inputs.after_progress.through_seq == before_through_seq
        }
        Some(before_through_seq) => {
            inputs.after_progress.through_seq >= before_through_seq
                && matches!(
                    inputs.outcome,
                    ProgressPublishOutcomeKind::Advanced | ProgressPublishOutcomeKind::Created
                )
                && inputs.after_progress.through_seq == inputs.requested_through_seq
        }
    };

    BackgroundWorkInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "progress_object_checksum_matches_payload".to_owned(),
                passed: inputs.after_progress.payload_checksum_valid,
                detail: format!(
                    "object_key={} through_seq={}",
                    inputs.after_progress.object_key, inputs.after_progress.through_seq.0
                ),
            },
            InvariantCheck {
                name: "progress_object_key_matches_namespace_and_work_class".to_owned(),
                passed: key_matches,
                detail: format!(
                    "expected_key={} actual_key={} expected_namespace={} actual_namespace={} expected_work_class={} actual_work_class={}",
                    expected_key,
                    inputs.after_progress.object_key,
                    inputs.expected_namespace,
                    inputs.after_progress.namespace_id,
                    inputs.expected_work_class,
                    inputs.after_progress.work_class
                ),
            },
            InvariantCheck {
                name: "progress_through_seq_advances_monotonically".to_owned(),
                passed: monotonic,
                detail: format!(
                    "before={:?} requested={} outcome={:?} after={}",
                    before.map(|seq| seq.0),
                    inputs.requested_through_seq.0,
                    inputs.outcome,
                    inputs.after_progress.through_seq.0
                ),
            },
        ],
    }
}

pub fn evaluate_content_object_invariants(
    inputs: ContentObjectInvariantInputs<'_>,
) -> ContentObjectInvariantReport {
    let payload_checksum_valid =
        content_manifest_payload_checksum_sha256(&inputs.content.manifest_envelope.payload)
            .map(|actual| actual == inputs.content.manifest_envelope.payload_checksum_sha256)
            .unwrap_or(false);
    let actual_manifest_digest = sha256_digest(&inputs.content.manifest_bytes);
    let digest_matches = actual_manifest_digest == inputs.content.content_manifest_digest;
    let namespace_matches =
        inputs.content.manifest_envelope.payload.namespace_id == *inputs.expected_namespace;

    let mut reconstructed = Vec::new();
    let mut block_details = Vec::new();
    let mut blocks_match = true;
    for descriptor in &inputs.content.manifest_envelope.payload.blocks {
        match inputs
            .content
            .available_blocks
            .get(&descriptor.content_digest_sha256)
        {
            Some(block) => {
                let actual_size = block.bytes.len() as u64;
                let actual_digest = sha256_digest(&block.bytes);
                let size_matches = actual_size == descriptor.plaintext_size_bytes;
                let digest_ok = actual_digest == descriptor.content_digest_sha256;
                if !(size_matches && digest_ok) {
                    blocks_match = false;
                }
                reconstructed.extend_from_slice(&block.bytes);
                block_details.push(format!(
                    "{} size={} expected_size={} digest_ok={} object_key={}",
                    descriptor.content_digest_sha256,
                    actual_size,
                    descriptor.plaintext_size_bytes,
                    digest_ok,
                    block.object_key
                ));
            }
            None => {
                blocks_match = false;
                block_details.push(format!(
                    "{} missing expected_key={}",
                    descriptor.content_digest_sha256,
                    blob(
                        inputs.expected_namespace.as_str(),
                        &descriptor.content_digest_sha256
                    )
                ));
            }
        }
    }

    let actual_file_size = reconstructed.len() as u64;
    let actual_file_digest = sha256_digest(&reconstructed);
    let file_digest_matches = actual_file_size
        == inputs.content.manifest_envelope.payload.file_size_bytes
        && actual_file_digest == inputs.content.manifest_envelope.payload.file_digest_sha256;

    ContentObjectInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "content_manifest_checksum_matches_payload".to_owned(),
                passed: payload_checksum_valid,
                detail: format!(
                    "manifest_object_key={} payload_checksum={}",
                    inputs.content.manifest_object_key,
                    inputs.content.manifest_envelope.payload_checksum_sha256
                ),
            },
            InvariantCheck {
                name: "content_manifest_digest_matches_object".to_owned(),
                passed: digest_matches,
                detail: format!(
                    "manifest_object_key={} expected_digest={} actual_digest={}",
                    inputs.content.manifest_object_key,
                    inputs.content.content_manifest_digest,
                    actual_manifest_digest
                ),
            },
            InvariantCheck {
                name: "content_manifest_namespace_matches_request".to_owned(),
                passed: namespace_matches,
                detail: format!(
                    "expected_namespace={} actual_namespace={}",
                    inputs.expected_namespace,
                    inputs.content.manifest_envelope.payload.namespace_id
                ),
            },
            InvariantCheck {
                name: "content_manifest_blocks_match_descriptors".to_owned(),
                passed: blocks_match,
                detail: format!("blocks=[{}]", block_details.join("; ")),
            },
            InvariantCheck {
                name: "content_manifest_file_digest_matches_blocks".to_owned(),
                passed: file_digest_matches,
                detail: format!(
                    "expected_size={} actual_size={} expected_digest={} actual_digest={}",
                    inputs.content.manifest_envelope.payload.file_size_bytes,
                    actual_file_size,
                    inputs.content.manifest_envelope.payload.file_digest_sha256,
                    actual_file_digest
                ),
            },
        ],
    }
}

pub fn evaluate_checkpoint_object_invariants(
    inputs: CheckpointObjectInvariantInputs<'_>,
) -> CheckpointObjectInvariantReport {
    let manifest_from_bytes = decode_checkpoint_manifest_json(&inputs.checkpoint.manifest_bytes)
        .map_err(|err| err.to_string());
    let manifest = manifest_from_bytes
        .as_ref()
        .unwrap_or(&inputs.checkpoint.manifest_envelope);
    let stored_segments = inputs
        .checkpoint
        .segments
        .iter()
        .map(|segment| StoredCheckpointSegment {
            object_key: segment.object_key.clone(),
            encoded_bytes: segment.encoded_bytes.clone(),
        })
        .collect::<Vec<_>>();
    let decoded_segments = decode_checkpoint_segments(&stored_segments);
    let reconstructed_basis =
        reconstruct_checkpoint_metadata(&stored_segments, manifest_from_bytes.as_ref().ok());

    let segment_checksum_pass = match decoded_segments.as_ref() {
        Ok(decoded) => decoded.iter().all(|segment| {
            segment
                .envelope
                .has_valid_payload_checksum()
                .unwrap_or(false)
        }),
        Err(_) => false,
    };
    let segment_checksum_detail = match decoded_segments.as_ref() {
        Ok(decoded) => format!("validated_segments={}", decoded.len()),
        Err(error) => format!("segment_decode_failed error={error}"),
    };

    let key_matches = match decoded_segments.as_ref() {
        Ok(decoded) => {
            let mismatches = decoded
                .iter()
                .filter_map(|segment| {
                    let expected = snapshot_table(
                        manifest.payload.namespace_id.as_str(),
                        manifest.payload.checkpoint_seq.0,
                        snapshot_table_family_from_checkpoint(segment.envelope.payload.family),
                        segment.envelope.payload.segment_index,
                    );
                    (segment.object_key != expected)
                        .then(|| format!("expected={} actual={}", expected, segment.object_key))
                })
                .collect::<Vec<_>>();
            (
                mismatches.is_empty(),
                if mismatches.is_empty() {
                    format!("validated_segments={}", decoded.len())
                } else {
                    mismatches.join("; ")
                },
            )
        }
        Err(error) => (false, format!("segment_decode_failed error={error}")),
    };

    let durable_segments = match decoded_segments.as_ref() {
        Ok(decoded) => {
            let actual_descriptors = decoded
                .iter()
                .map(|segment| {
                    checkpoint_segment_descriptor_from_payload(
                        &segment.object_key,
                        &segment.envelope,
                    )
                })
                .collect::<Result<Vec<_>, _>>();
            match actual_descriptors {
                Ok(actual_descriptors) => {
                    let expected = manifest
                        .payload
                        .tables
                        .iter()
                        .flat_map(|table| table.segments.iter().cloned())
                        .collect::<Vec<_>>();
                    (
                        manifest.payload.verified && expected == actual_descriptors,
                        format!(
                            "verified={} expected_segments={} actual_segments={}",
                            manifest.payload.verified,
                            expected.len(),
                            actual_descriptors.len()
                        ),
                    )
                }
                Err(error) => (false, format!("descriptor_build_failed error={error}")),
            }
        }
        Err(error) => (false, format!("segment_decode_failed error={error}")),
    };

    let preserves_head_summary = manifest.payload.namespace_id
        == inputs.checkpoint.source_head.namespace_id
        && manifest.payload.checkpoint_seq == inputs.checkpoint.source_head.seq
        && manifest.payload.active_fence_token == inputs.checkpoint.source_head.active_fence_token
        && manifest.payload.next_inode_id == inputs.checkpoint.source_head.next_inode_id
        && manifest.payload.retention_floor_seq
            == inputs.checkpoint.source_head.retention_floor_seq;

    let preserves_basis_metadata = match reconstructed_basis.as_ref() {
        Ok(metadata) => (
            metadata == &inputs.checkpoint.source_basis_metadata,
            format!(
                "reconstructed_matches_expected={} rows=(inodes={}, direntries={}, revisions={}, tombstones={})",
                metadata == &inputs.checkpoint.source_basis_metadata,
                metadata.inodes.len(),
                metadata.direntries.len(),
                metadata.revisions.len(),
                metadata.subtree_tombstones.len()
            ),
        ),
        Err(error) => (false, format!("basis_reconstruction_failed error={error}")),
    };

    CheckpointObjectInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "checkpoint_segment_payload_checksum_matches_payload".to_owned(),
                passed: segment_checksum_pass,
                detail: segment_checksum_detail,
            },
            InvariantCheck {
                name: "checkpoint_segment_key_matches_family_and_index".to_owned(),
                passed: key_matches.0,
                detail: key_matches.1,
            },
            InvariantCheck {
                name: "verified_checkpoint_manifest_requires_durable_segments".to_owned(),
                passed: durable_segments.0,
                detail: durable_segments.1,
            },
            InvariantCheck {
                name: "checkpoint_manifest_preserves_head_summary".to_owned(),
                passed: preserves_head_summary,
                detail: format!(
                    "source=(namespace={}, seq={}, fence={}, next_inode={}, retention_floor={}) manifest=(namespace={}, seq={}, fence={}, next_inode={}, retention_floor={})",
                    inputs.checkpoint.source_head.namespace_id,
                    inputs.checkpoint.source_head.seq.0,
                    inputs.checkpoint.source_head.active_fence_token.0,
                    inputs.checkpoint.source_head.next_inode_id.0,
                    inputs.checkpoint.source_head.retention_floor_seq.0,
                    manifest.payload.namespace_id,
                    manifest.payload.checkpoint_seq.0,
                    manifest.payload.active_fence_token.0,
                    manifest.payload.next_inode_id.0,
                    manifest.payload.retention_floor_seq.0
                ),
            },
            InvariantCheck {
                name: "checkpoint_manifest_preserves_basis_metadata".to_owned(),
                passed: preserves_basis_metadata.0,
                detail: preserves_basis_metadata.1,
            },
        ],
    }
}

pub fn evaluate_download_transfer_invariants(
    inputs: DownloadTransferInvariantInputs<'_>,
) -> ClientTransferInvariantReport {
    let monotonic = match inputs.outcome {
        DownloadTransferOutcomeKind::Progressed | DownloadTransferOutcomeKind::ResetProgressed => {
            inputs.after_transfer_block_index.is_some_and(|after| {
                after > inputs.before_block_index.unwrap_or(0) && after <= inputs.block_count
            })
        }
        DownloadTransferOutcomeKind::Completed => {
            inputs
                .before_block_index
                .is_some_and(|before| before < inputs.block_count)
                && inputs.after_transfer_block_index.is_none()
        }
    };
    let reset_issue = matches!(inputs.outcome, DownloadTransferOutcomeKind::ResetProgressed)
        && inputs.reset_issue_kind == Some("download_remote_edit_transfer_reset")
        && inputs.reset_issue_reason.is_some();
    let completion_clears = matches!(inputs.outcome, DownloadTransferOutcomeKind::Completed)
        && inputs.after_transfer_block_index.is_none();
    let materialized = matches!(inputs.outcome, DownloadTransferOutcomeKind::Completed)
        && inputs.local_exists_on_disk
        && !inputs.local_dirty
        && inputs.local_content_digest == inputs.remote_content_digest
        && inputs.sync_anchor_seq == Some(inputs.remote_synced_seq)
        && inputs.sync_anchor_revision_no == Some(inputs.remote_revision_no)
        && inputs.sync_anchor_content_digest == inputs.remote_content_digest
        && inputs.sync_anchor_content_manifest_digest == inputs.remote_content_manifest_digest;

    ClientTransferInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "download_transfer_block_index_advances_monotonically".to_owned(),
                passed: monotonic,
                detail: format!(
                    "before_block_index={:?} after_block_index={:?} block_count={} outcome={:?}",
                    inputs.before_block_index,
                    inputs.after_transfer_block_index,
                    inputs.block_count,
                    inputs.outcome
                ),
            },
            InvariantCheck {
                name: "download_transfer_reset_records_durable_issue".to_owned(),
                passed: reset_issue,
                detail: format!(
                    "outcome={:?} issue_kind={:?} issue_reason={:?}",
                    inputs.outcome, inputs.reset_issue_kind, inputs.reset_issue_reason
                ),
            },
            InvariantCheck {
                name: "download_completion_clears_transfer_ledger".to_owned(),
                passed: completion_clears,
                detail: format!(
                    "outcome={:?} after_transfer_block_index={:?}",
                    inputs.outcome, inputs.after_transfer_block_index
                ),
            },
            InvariantCheck {
                name: "download_materialization_updates_local_state_and_sync_anchor".to_owned(),
                passed: materialized,
                detail: format!(
                    "outcome={:?} local_exists={} local_dirty={} local_digest={:?} remote_digest={:?} sync_anchor_seq={:?} sync_anchor_revision={:?}",
                    inputs.outcome,
                    inputs.local_exists_on_disk,
                    inputs.local_dirty,
                    inputs.local_content_digest,
                    inputs.remote_content_digest,
                    inputs.sync_anchor_seq.map(|seq| seq.0),
                    inputs.sync_anchor_revision_no.map(|revision| revision.0)
                ),
            },
        ],
    }
}

pub fn evaluate_inode_upload_transfer_invariants(
    inputs: InodeUploadTransferInvariantInputs<'_>,
) -> ClientTransferInvariantReport {
    let monotonic = match inputs.outcome {
        InodeUploadTransferOutcomeKind::Progressed
        | InodeUploadTransferOutcomeKind::ResetProgressed => {
            inputs.after_transfer_block_index.is_some_and(|after| {
                after > inputs.before_block_index.unwrap_or(0) && after <= inputs.block_count
            })
        }
        InodeUploadTransferOutcomeKind::Completed => {
            inputs
                .before_block_index
                .is_some_and(|before| before < inputs.block_count)
                && inputs.after_transfer_block_index.is_none()
        }
        InodeUploadTransferOutcomeKind::RetryReusedPending => false,
    };
    let dispatch_waits = match inputs.outcome {
        InodeUploadTransferOutcomeKind::Progressed
        | InodeUploadTransferOutcomeKind::ResetProgressed => {
            !inputs.ensured_upload_present && inputs.after_pending_request_id.is_none()
        }
        InodeUploadTransferOutcomeKind::Completed => {
            inputs.ensured_upload_present && inputs.after_transfer_block_index.is_none()
        }
        InodeUploadTransferOutcomeKind::RetryReusedPending => {
            inputs.upload_reused && inputs.after_pending_request_id.is_some()
        }
    };
    let retry_reused = matches!(
        inputs.outcome,
        InodeUploadTransferOutcomeKind::RetryReusedPending
    ) && inputs.upload_reused
        && inputs.before_pending_request_id.is_some()
        && inputs.before_pending_request_id == inputs.after_pending_request_id;
    let completion_clears = matches!(
        inputs.outcome,
        InodeUploadTransferOutcomeKind::Completed
            | InodeUploadTransferOutcomeKind::RetryReusedPending
    ) && inputs.after_transfer_block_index.is_none();
    let reset_issue = matches!(
        inputs.outcome,
        InodeUploadTransferOutcomeKind::ResetProgressed
    ) && inputs.reset_issue_kind == Some("upload_local_edit_transfer_reset")
        && inputs.reset_issue_reason.is_some();

    ClientTransferInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "inode_upload_block_index_advances_monotonically".to_owned(),
                passed: monotonic,
                detail: format!(
                    "before_block_index={:?} after_block_index={:?} block_count={} outcome={:?}",
                    inputs.before_block_index,
                    inputs.after_transfer_block_index,
                    inputs.block_count,
                    inputs.outcome
                ),
            },
            InvariantCheck {
                name: "inode_upload_dispatch_waits_for_terminal_block".to_owned(),
                passed: dispatch_waits,
                detail: format!(
                    "outcome={:?} ensured_upload_present={} after_pending_request_id={:?}",
                    inputs.outcome, inputs.ensured_upload_present, inputs.after_pending_request_id
                ),
            },
            InvariantCheck {
                name: "inode_upload_retry_reuses_pending_inode_mutation".to_owned(),
                passed: retry_reused,
                detail: format!(
                    "outcome={:?} upload_reused={} before_pending={:?} after_pending={:?}",
                    inputs.outcome,
                    inputs.upload_reused,
                    inputs.before_pending_request_id,
                    inputs.after_pending_request_id
                ),
            },
            InvariantCheck {
                name: "inode_upload_completion_clears_transfer_ledger".to_owned(),
                passed: completion_clears,
                detail: format!(
                    "outcome={:?} after_transfer_block_index={:?}",
                    inputs.outcome, inputs.after_transfer_block_index
                ),
            },
            InvariantCheck {
                name: "inode_upload_transfer_reset_records_durable_issue".to_owned(),
                passed: reset_issue,
                detail: format!(
                    "outcome={:?} issue_kind={:?} issue_reason={:?}",
                    inputs.outcome, inputs.reset_issue_kind, inputs.reset_issue_reason
                ),
            },
        ],
    }
}

pub fn evaluate_local_only_upload_transfer_invariants(
    inputs: LocalOnlyUploadTransferInvariantInputs<'_>,
) -> ClientTransferInvariantReport {
    let monotonic = match inputs.outcome {
        LocalOnlyUploadTransferOutcomeKind::Progressed
        | LocalOnlyUploadTransferOutcomeKind::ResetProgressed => {
            inputs.after_transfer_block_index.is_some_and(|after| {
                after > inputs.before_block_index.unwrap_or(0) && after <= inputs.block_count
            })
        }
        LocalOnlyUploadTransferOutcomeKind::Completed => {
            inputs
                .before_block_index
                .is_some_and(|before| before < inputs.block_count)
                && inputs.after_transfer_block_index.is_none()
        }
        LocalOnlyUploadTransferOutcomeKind::RetryReusedPending => false,
    };
    let dispatch_waits = match inputs.outcome {
        LocalOnlyUploadTransferOutcomeKind::Progressed
        | LocalOnlyUploadTransferOutcomeKind::ResetProgressed => {
            !inputs.ensured_upload_present && inputs.after_pending_request_id.is_none()
        }
        LocalOnlyUploadTransferOutcomeKind::Completed => {
            inputs.ensured_upload_present && inputs.after_transfer_block_index.is_none()
        }
        LocalOnlyUploadTransferOutcomeKind::RetryReusedPending => {
            inputs.upload_reused && inputs.after_pending_request_id.is_some()
        }
    };
    let retry_reused = matches!(
        inputs.outcome,
        LocalOnlyUploadTransferOutcomeKind::RetryReusedPending
    ) && inputs.upload_reused
        && inputs.before_pending_request_id.is_some()
        && inputs.before_pending_request_id == inputs.after_pending_request_id;
    let completion_clears = matches!(
        inputs.outcome,
        LocalOnlyUploadTransferOutcomeKind::Completed
            | LocalOnlyUploadTransferOutcomeKind::RetryReusedPending
    ) && inputs.after_transfer_block_index.is_none();
    let bind_clears = matches!(
        inputs.outcome,
        LocalOnlyUploadTransferOutcomeKind::Completed
    ) && inputs.after_transfer_block_index.is_none()
        && !inputs.local_only_file_present_after
        && inputs.local_only_issue_count_after == 0;
    let reset_issue = matches!(
        inputs.outcome,
        LocalOnlyUploadTransferOutcomeKind::ResetProgressed
    ) && inputs.reset_issue_kind == Some("upload_local_create_transfer_reset")
        && inputs.reset_issue_reason.is_some();

    ClientTransferInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "local_only_upload_block_index_advances_monotonically".to_owned(),
                passed: monotonic,
                detail: format!(
                    "before_block_index={:?} after_block_index={:?} block_count={} outcome={:?}",
                    inputs.before_block_index,
                    inputs.after_transfer_block_index,
                    inputs.block_count,
                    inputs.outcome
                ),
            },
            InvariantCheck {
                name: "local_only_upload_dispatch_waits_for_terminal_block".to_owned(),
                passed: dispatch_waits,
                detail: format!(
                    "outcome={:?} ensured_upload_present={} after_pending_request_id={:?}",
                    inputs.outcome,
                    inputs.ensured_upload_present,
                    inputs.after_pending_request_id
                ),
            },
            InvariantCheck {
                name: "local_only_upload_retry_reuses_pending_client_mutation".to_owned(),
                passed: retry_reused,
                detail: format!(
                    "outcome={:?} upload_reused={} before_pending={:?} after_pending={:?}",
                    inputs.outcome,
                    inputs.upload_reused,
                    inputs.before_pending_request_id,
                    inputs.after_pending_request_id
                ),
            },
            InvariantCheck {
                name: "local_only_upload_completion_clears_temp_transfer_ledger".to_owned(),
                passed: completion_clears,
                detail: format!(
                    "outcome={:?} after_transfer_block_index={:?}",
                    inputs.outcome, inputs.after_transfer_block_index
                ),
            },
            InvariantCheck {
                name: "local_only_upload_bind_clears_temp_issue_and_transfer_ledger".to_owned(),
                passed: bind_clears,
                detail: format!(
                    "outcome={:?} after_transfer_block_index={:?} local_only_file_present_after={} issue_count_after={}",
                    inputs.outcome,
                    inputs.after_transfer_block_index,
                    inputs.local_only_file_present_after,
                    inputs.local_only_issue_count_after
                ),
            },
            InvariantCheck {
                name: "local_only_upload_transfer_reset_records_durable_issue".to_owned(),
                passed: reset_issue,
                detail: format!(
                    "outcome={:?} issue_kind={:?} issue_reason={:?}",
                    inputs.outcome, inputs.reset_issue_kind, inputs.reset_issue_reason
                ),
            },
        ],
    }
}

pub fn evaluate_remote_observation_late_bind_invariants(
    inputs: RemoteObservationLateBindInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    let establishes = inputs.remote_present_after
        && inputs.local_present_after
        && inputs.sync_anchor_present_after
        && !inputs.local_dirty_after
        && inputs.local_content_digest_after == inputs.remote_content_digest_after
        && inputs.sync_anchor_content_digest_after == inputs.remote_content_digest_after;
    let clears_temp_state =
        !inputs.local_only_file_present_after && !inputs.planned_local_only_action_present_after;
    let clears_temp_transfer_and_issue_rows = !inputs.local_only_upload_present_after
        && !inputs.local_only_transfer_present_after
        && inputs.local_only_issue_count_after == 0;
    let retains_pending = inputs.pending_client_mutation_present_after;

    ClientReconciliationInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "remote_observation_late_bind_establishes_remote_local_and_anchor"
                    .to_owned(),
                passed: establishes,
                detail: format!(
                    "remote_present={} local_present={} sync_anchor_present={} local_dirty={} remote_digest={:?} local_digest={:?} anchor_digest={:?}",
                    inputs.remote_present_after,
                    inputs.local_present_after,
                    inputs.sync_anchor_present_after,
                    inputs.local_dirty_after,
                    inputs.remote_content_digest_after,
                    inputs.local_content_digest_after,
                    inputs.sync_anchor_content_digest_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_late_bind_clears_temp_local_state".to_owned(),
                passed: clears_temp_state,
                detail: format!(
                    "local_only_file_present_after={} planned_local_only_action_present_after={}",
                    inputs.local_only_file_present_after,
                    inputs.planned_local_only_action_present_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_late_bind_clears_temp_transfer_and_issue_rows"
                    .to_owned(),
                passed: clears_temp_transfer_and_issue_rows,
                detail: format!(
                    "local_only_upload_present_after={} local_only_transfer_present_after={} local_only_issue_count_after={}",
                    inputs.local_only_upload_present_after,
                    inputs.local_only_transfer_present_after,
                    inputs.local_only_issue_count_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_late_bind_retains_pending_client_mutation_until_response"
                    .to_owned(),
                passed: retains_pending,
                detail: format!(
                    "pending_client_mutation_present_after={}",
                    inputs.pending_client_mutation_present_after
                ),
            },
        ],
    }
}

pub fn evaluate_remote_observation_convergence_invariants(
    inputs: RemoteObservationConvergenceInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    let clears_dirty_and_planned_action =
        !inputs.local_dirty_after && !inputs.planned_action_present_after;
    let clears_pending_inode_mutation = !inputs.pending_inode_mutation_present_after;
    let advances_sync_anchor = inputs.sync_anchor_seq_after == Some(inputs.remote_synced_seq_after)
        && inputs.sync_anchor_revision_no_after == Some(inputs.remote_revision_no_after)
        && inputs.sync_anchor_content_digest_after == inputs.remote_content_digest_after
        && inputs.local_content_digest_after == inputs.remote_content_digest_after;

    ClientReconciliationInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "remote_observation_convergence_clears_dirty_and_planned_action"
                    .to_owned(),
                passed: clears_dirty_and_planned_action,
                detail: format!(
                    "local_dirty_after={} planned_action_present_after={}",
                    inputs.local_dirty_after, inputs.planned_action_present_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_convergence_clears_pending_inode_mutation".to_owned(),
                passed: clears_pending_inode_mutation,
                detail: format!(
                    "pending_inode_mutation_present_after={}",
                    inputs.pending_inode_mutation_present_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_convergence_advances_sync_anchor".to_owned(),
                passed: advances_sync_anchor,
                detail: format!(
                    "remote_seq={} remote_revision={} remote_digest={:?} local_digest={:?} sync_anchor_seq={:?} sync_anchor_revision={:?} sync_anchor_digest={:?}",
                    inputs.remote_synced_seq_after.0,
                    inputs.remote_revision_no_after.0,
                    inputs.remote_content_digest_after,
                    inputs.local_content_digest_after,
                    inputs.sync_anchor_seq_after.map(|seq| seq.0),
                    inputs.sync_anchor_revision_no_after.map(|revision| revision.0),
                    inputs.sync_anchor_content_digest_after
                ),
            },
        ],
    }
}

pub fn evaluate_remote_observation_ambiguous_bind_invariants(
    inputs: RemoteObservationAmbiguousBindInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    let records_issue = inputs.issue_kind_after == Some("remote_observation_bind_ambiguous")
        && inputs
            .issue_matches_after
            .is_some_and(|matches| matches > 1);
    let avoids_partial = !inputs.remote_present_after
        && !inputs.local_present_after
        && !inputs.sync_anchor_present_after
        && inputs.surviving_local_only_count_after == inputs.initial_local_only_count;

    ClientReconciliationInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "remote_observation_ambiguous_bind_records_durable_issue".to_owned(),
                passed: records_issue,
                detail: format!(
                    "issue_kind_after={:?} issue_matches_after={:?}",
                    inputs.issue_kind_after, inputs.issue_matches_after
                ),
            },
            InvariantCheck {
                name: "remote_observation_ambiguous_bind_avoids_partial_migration".to_owned(),
                passed: avoids_partial,
                detail: format!(
                    "remote_present_after={} local_present_after={} sync_anchor_present_after={} surviving_local_only_count_after={} initial_local_only_count={}",
                    inputs.remote_present_after,
                    inputs.local_present_after,
                    inputs.sync_anchor_present_after,
                    inputs.surviving_local_only_count_after,
                    inputs.initial_local_only_count
                ),
            },
        ],
    }
}

pub fn evaluate_remote_observation_active_upload_invariants(
    inputs: RemoteObservationActiveUploadInvariantInputs,
) -> ClientReconciliationInvariantReport {
    ClientReconciliationInvariantReport {
        checks: vec![InvariantCheck {
            name: "remote_observation_active_upload_preserves_transfer_and_pending_inode_mutation"
                .to_owned(),
            passed: inputs.transfer_present_after
                && inputs.pending_inode_mutation_present_after
                && inputs.remote_synced_seq_after == inputs.expected_remote_synced_seq,
            detail: format!(
                "transfer_present_after={} pending_inode_mutation_present_after={} remote_seq_after={} expected_remote_seq={}",
                inputs.transfer_present_after,
                inputs.pending_inode_mutation_present_after,
                inputs.remote_synced_seq_after.0,
                inputs.expected_remote_synced_seq.0
            ),
        }],
    }
}

pub fn evaluate_remote_observation_active_download_invariants(
    inputs: RemoteObservationActiveDownloadInvariantInputs,
) -> ClientReconciliationInvariantReport {
    ClientReconciliationInvariantReport {
        checks: vec![InvariantCheck {
            name: "remote_observation_active_download_preserves_transfer_ledger".to_owned(),
            passed: inputs.transfer_present_after
                && inputs.remote_synced_seq_after == inputs.expected_remote_synced_seq,
            detail: format!(
                "transfer_present_after={} remote_seq_after={} expected_remote_seq={}",
                inputs.transfer_present_after,
                inputs.remote_synced_seq_after.0,
                inputs.expected_remote_synced_seq.0
            ),
        }],
    }
}

pub fn evaluate_remote_only_discovery_invariants(
    inputs: RemoteOnlyDiscoveryInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    let (name, expected_decision) = match inputs.inode_kind {
        InodeKind::File => (
            "remote_only_file_discovery_creates_placeholder_without_anchor",
            Some("download_remote_edit"),
        ),
        InodeKind::Dir => (
            "remote_only_directory_discovery_creates_placeholder_without_anchor",
            Some("materialize_remote_dir"),
        ),
        InodeKind::Symlink | InodeKind::Mount => (
            "remote_only_file_discovery_creates_placeholder_without_anchor",
            None,
        ),
    };

    ClientReconciliationInvariantReport {
        checks: vec![InvariantCheck {
            name: name.to_owned(),
            passed: !inputs.local_exists_on_disk_after
                && !inputs.local_dirty_after
                && !inputs.sync_anchor_present_after
                && inputs.planned_action_decision_after == expected_decision,
            detail: format!(
                "inode_kind={:?} local_exists_on_disk_after={} local_dirty_after={} sync_anchor_present_after={} planned_action_decision_after={:?}",
                inputs.inode_kind,
                inputs.local_exists_on_disk_after,
                inputs.local_dirty_after,
                inputs.sync_anchor_present_after,
                inputs.planned_action_decision_after
            ),
        }],
    }
}

pub fn evaluate_remote_only_directory_materialization_invariants(
    inputs: RemoteOnlyDirectoryMaterializationInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    match inputs.outcome {
        RemoteOnlyDirectoryMaterializationOutcomeKind::Completed => {
            ClientReconciliationInvariantReport {
                checks: vec![
                    InvariantCheck {
                        name: "remote_only_directory_materialization_updates_local_state_and_sync_anchor"
                            .to_owned(),
                        passed: inputs.local_exists_on_disk_after
                            && !inputs.local_dirty_after
                            && inputs.sync_anchor_present_after,
                        detail: format!(
                            "local_exists_on_disk_after={} local_dirty_after={} sync_anchor_present_after={}",
                            inputs.local_exists_on_disk_after,
                            inputs.local_dirty_after,
                            inputs.sync_anchor_present_after
                        ),
                    },
                    InvariantCheck {
                        name: "remote_only_directory_materialization_clears_planned_action"
                            .to_owned(),
                        passed: !inputs.planned_action_present_after,
                        detail: format!(
                            "planned_action_present_after={}",
                            inputs.planned_action_present_after
                        ),
                    },
                ],
            }
        }
        RemoteOnlyDirectoryMaterializationOutcomeKind::Failed => {
            ClientReconciliationInvariantReport {
                checks: vec![InvariantCheck {
                    name: "remote_only_directory_materialization_failure_records_durable_issue"
                        .to_owned(),
                    passed: inputs.issue_kind_after
                        == Some("materialize_remote_dir_local_apply_failed"),
                    detail: format!("issue_kind_after={:?}", inputs.issue_kind_after),
                }],
            }
        }
    }
}

pub fn evaluate_remote_path_change_planning_invariants(
    inputs: RemotePathChangePlanningInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    ClientReconciliationInvariantReport {
        checks: vec![InvariantCheck {
            name: "remote_path_change_plans_apply_remote_rename".to_owned(),
            passed: inputs.planned_action_decision_after == Some("apply_remote_rename")
                && inputs.planned_action_reason_after == Some("remote_path_differs_from_anchor"),
            detail: format!(
                "planned_action_decision_after={:?} planned_action_reason_after={:?}",
                inputs.planned_action_decision_after, inputs.planned_action_reason_after
            ),
        }],
    }
}

pub fn evaluate_remote_delete_planning_invariants(
    inputs: RemoteDeletePlanningInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    ClientReconciliationInvariantReport {
        checks: vec![InvariantCheck {
            name: "remote_delete_plans_apply_remote_delete".to_owned(),
            passed: inputs.planned_action_decision_after == Some("apply_remote_delete")
                && inputs.planned_action_reason_after == Some("remote_deleted_from_anchor"),
            detail: format!(
                "decision={:?} reason={:?}",
                inputs.planned_action_decision_after, inputs.planned_action_reason_after
            ),
        }],
    }
}

pub fn evaluate_apply_remote_delete_invariants(
    inputs: ApplyRemoteDeleteInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    ClientReconciliationInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "apply_remote_delete_preserves_remote_tombstone".to_owned(),
                passed: inputs.remote_present_after && inputs.remote_is_deleted_after,
                detail: format!(
                    "remote_present_after={} remote_is_deleted_after={}",
                    inputs.remote_present_after, inputs.remote_is_deleted_after
                ),
            },
            InvariantCheck {
                name: "apply_remote_delete_clears_local_state_and_sync_anchor".to_owned(),
                passed: !inputs.local_present_after && !inputs.sync_anchor_present_after,
                detail: format!(
                    "local_present_after={} sync_anchor_present_after={}",
                    inputs.local_present_after, inputs.sync_anchor_present_after
                ),
            },
            InvariantCheck {
                name: "apply_remote_delete_clears_planned_action".to_owned(),
                passed: !inputs.planned_action_present_after,
                detail: format!(
                    "planned_action_present_after={}",
                    inputs.planned_action_present_after
                ),
            },
            InvariantCheck {
                name: "apply_remote_delete_failure_records_durable_issue".to_owned(),
                passed: inputs.issue_kind_after == Some("apply_remote_delete_local_apply_failed"),
                detail: format!("issue_kind_after={:?}", inputs.issue_kind_after),
            },
        ],
    }
}

pub fn evaluate_apply_remote_rename_invariants(
    inputs: ApplyRemoteRenameInvariantInputs<'_>,
) -> ClientReconciliationInvariantReport {
    let updates_local_state_and_sync_anchor = inputs.local_exists_on_disk_after
        && !inputs.local_dirty_after
        && inputs.local_parent_inode_after == inputs.remote_parent_inode_after
        && inputs.local_display_name_after == inputs.remote_display_name_after
        && inputs.sync_anchor_seq_after == Some(inputs.remote_synced_seq_after)
        && inputs.sync_anchor_revision_no_after == Some(inputs.remote_revision_no_after)
        && inputs.sync_anchor_content_digest_after == inputs.remote_content_digest_after
        && inputs.sync_anchor_parent_inode_after == inputs.remote_parent_inode_after
        && inputs.sync_anchor_display_name_after == Some(inputs.remote_display_name_after);

    ClientReconciliationInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "apply_remote_rename_updates_local_state_and_sync_anchor".to_owned(),
                passed: updates_local_state_and_sync_anchor,
                detail: format!(
                    "local_exists_on_disk_after={} local_dirty_after={} local_parent_inode_after={:?} local_display_name_after={} remote_seq_after={} remote_revision_after={} remote_content_digest_after={:?} remote_parent_inode_after={:?} remote_display_name_after={} sync_anchor_seq_after={:?} sync_anchor_revision_after={:?} sync_anchor_content_digest_after={:?} sync_anchor_parent_inode_after={:?} sync_anchor_display_name_after={:?}",
                    inputs.local_exists_on_disk_after,
                    inputs.local_dirty_after,
                    inputs.local_parent_inode_after,
                    inputs.local_display_name_after,
                    inputs.remote_synced_seq_after.0,
                    inputs.remote_revision_no_after.0,
                    inputs.remote_content_digest_after,
                    inputs.remote_parent_inode_after,
                    inputs.remote_display_name_after,
                    inputs.sync_anchor_seq_after.map(|seq| seq.0),
                    inputs.sync_anchor_revision_no_after.map(|revision| revision.0),
                    inputs.sync_anchor_content_digest_after,
                    inputs.sync_anchor_parent_inode_after,
                    inputs.sync_anchor_display_name_after
                ),
            },
            InvariantCheck {
                name: "apply_remote_rename_clears_planned_action".to_owned(),
                passed: !inputs.planned_action_present_after,
                detail: format!(
                    "planned_action_present_after={}",
                    inputs.planned_action_present_after
                ),
            },
            InvariantCheck {
                name: "apply_remote_rename_failure_records_durable_issue".to_owned(),
                passed: inputs.issue_kind_after == Some("apply_remote_rename_local_apply_failed"),
                detail: format!("issue_kind_after={:?}", inputs.issue_kind_after),
            },
        ],
    }
}

pub fn evaluate_queue_shard_object_invariants(
    inputs: QueueShardObjectInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    BackgroundWorkInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "queue_shard_checksum_matches_payload".to_owned(),
                passed: inputs.payload_checksum_valid,
                detail: format!(
                    "object_key={} shard_id={}",
                    inputs.object_key, inputs.actual_shard_id
                ),
            },
            InvariantCheck {
                name: "queue_shard_key_matches_shard_id".to_owned(),
                passed: inputs.object_key == queue_shard(inputs.shard_index)
                    && inputs.actual_shard_id == inputs.shard_index,
                detail: format!(
                    "expected_key={} actual_key={} expected_shard_id={} actual_shard_id={}",
                    queue_shard(inputs.shard_index),
                    inputs.object_key,
                    inputs.shard_index,
                    inputs.actual_shard_id
                ),
            },
            InvariantCheck {
                name: "queue_shard_cas_protects_updates".to_owned(),
                passed: inputs.cas_protected,
                detail: format!(
                    "object_key={} cas_protected={}",
                    inputs.object_key, inputs.cas_protected
                ),
            },
        ],
    }
}

pub fn evaluate_queue_repair_invariants(
    inputs: QueueRepairInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let progress_covers_head = inputs
        .progress_through_seq
        .is_some_and(|through_seq| through_seq >= inputs.head_seq);
    let through_seq_after = inputs
        .ready_job_through_seq_after
        .or(inputs.follow_up_through_seq_after);
    let mut checks = Vec::new();

    if !matches!(inputs.outcome, QueueRepairOutcomeKind::NoRepairNeeded) {
        checks.push(InvariantCheck {
            name: "lost_enqueue_repair_enqueues_when_head_outpaces_progress".to_owned(),
            passed: !progress_covers_head
                && inputs.head_seq > ChangeSeq(0)
                && inputs.has_namespace_scoped_job_after
                && through_seq_after == Some(inputs.head_seq),
            detail: format!(
                "namespace={} head_seq={} progress_through_seq={:?} outcome={:?} through_seq_after={:?}",
                inputs.namespace_id,
                inputs.head_seq.0,
                inputs.progress_through_seq.map(|seq| seq.0),
                inputs.outcome,
                through_seq_after.map(|seq| seq.0)
            ),
        });
        checks.push(InvariantCheck {
            name: "snapshot_repair_dedupe_key_is_namespace_scoped".to_owned(),
            passed: inputs.has_namespace_scoped_job_after,
            detail: format!(
                "namespace={} has_namespace_scoped_job_after={}",
                inputs.namespace_id, inputs.has_namespace_scoped_job_after
            ),
        });
    }

    if matches!(
        inputs.outcome,
        QueueRepairOutcomeKind::AttachedFollowUp { .. }
    ) {
        checks.push(InvariantCheck {
            name: "snapshot_repair_claimed_job_gets_follow_up".to_owned(),
            passed: inputs.follow_up_through_seq_after == Some(inputs.head_seq),
            detail: format!(
                "namespace={} expected_follow_up={} actual_follow_up={:?}",
                inputs.namespace_id,
                inputs.head_seq.0,
                inputs.follow_up_through_seq_after.map(|seq| seq.0)
            ),
        });
    }

    BackgroundWorkInvariantReport { checks }
}

pub fn evaluate_queue_broker_lease_invariants(
    inputs: QueueBrokerLeaseInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let mut checks = Vec::new();
    if let QueueBrokerLeaseOutcomeKind::TakenOver { epoch } = inputs.outcome {
        checks.push(InvariantCheck {
            name: "broker_lease_takeover_increments_epoch".to_owned(),
            passed: inputs.before_epoch.is_some()
                && inputs.after_epoch == Some(epoch)
                && inputs.before_epoch.map(|value| value.saturating_add(1)) == Some(epoch)
                && inputs.after_broker_id == Some(inputs.broker_id)
                && inputs.before_broker_id != inputs.after_broker_id,
            detail: format!(
                "broker={} before_broker={:?} after_broker={:?} before_epoch={:?} after_epoch={:?}",
                inputs.broker_id,
                inputs.before_broker_id,
                inputs.after_broker_id,
                inputs.before_epoch,
                inputs.after_epoch
            ),
        });
    }

    BackgroundWorkInvariantReport { checks }
}

pub fn evaluate_queue_claim_invariants(
    inputs: QueueClaimInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let mut checks = Vec::new();
    if matches!(
        inputs.outcome,
        QueueClaimOutcomeKind::Claimed { .. } | QueueClaimOutcomeKind::Stolen { .. }
    ) {
        checks.push(InvariantCheck {
            name: "active_broker_lease_required_for_shard_mutation".to_owned(),
            passed: inputs.after_broker_id == Some(inputs.broker_id)
                && inputs.after_broker_epoch == Some(inputs.broker_epoch)
                && inputs
                    .after_broker_lease_expires_at_ms
                    .is_some_and(|lease_expires_at_ms| lease_expires_at_ms > inputs.now_ms),
            detail: format!(
                "broker={} epoch={} now_ms={} after_broker={:?} after_epoch={:?} lease_expires_at={:?}",
                inputs.broker_id,
                inputs.broker_epoch,
                inputs.now_ms,
                inputs.after_broker_id,
                inputs.after_broker_epoch,
                inputs.after_broker_lease_expires_at_ms
            ),
        });
    }

    if let QueueClaimOutcomeKind::Stolen { claim_token } = inputs.outcome {
        checks.push(InvariantCheck {
            name: "claim_timeout_allows_steal".to_owned(),
            passed: inputs
                .before_timeout_at_ms
                .is_some_and(|timeout_at_ms| timeout_at_ms <= inputs.now_ms)
                && inputs.after_claim_token == Some(claim_token),
            detail: format!(
                "claim_token={} before_timeout_at={:?} now_ms={} after_claim_token={:?}",
                claim_token, inputs.before_timeout_at_ms, inputs.now_ms, inputs.after_claim_token
            ),
        });
    }

    BackgroundWorkInvariantReport { checks }
}

pub fn evaluate_queue_heartbeat_invariants(
    inputs: QueueHeartbeatInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let mut checks = Vec::new();
    if let QueueHeartbeatOutcomeKind::Heartbeat {
        claim_token,
        timeout_at_ms,
    } = inputs.outcome
    {
        checks.push(InvariantCheck {
            name: "active_broker_lease_required_for_shard_mutation".to_owned(),
            passed: inputs.after_broker_id == Some(inputs.broker_id)
                && inputs.after_broker_epoch == Some(inputs.broker_epoch)
                && inputs
                    .after_broker_lease_expires_at_ms
                    .is_some_and(|lease_expires_at_ms| lease_expires_at_ms > inputs.now_ms),
            detail: format!(
                "broker={} epoch={} now_ms={} after_broker={:?} after_epoch={:?} lease_expires_at={:?}",
                inputs.broker_id,
                inputs.broker_epoch,
                inputs.now_ms,
                inputs.after_broker_id,
                inputs.after_broker_epoch,
                inputs.after_broker_lease_expires_at_ms
            ),
        });
        checks.push(InvariantCheck {
            name: "worker_heartbeat_requires_matching_claim_token".to_owned(),
            passed: inputs.after_claim_token == Some(claim_token)
                && inputs.after_timeout_at_ms == Some(timeout_at_ms)
                && inputs.claim_token == claim_token,
            detail: format!(
                "provided_claim_token={} outcome_claim_token={} after_claim_token={:?} after_timeout_at={:?} expected_timeout_at={}",
                inputs.claim_token,
                claim_token,
                inputs.after_claim_token,
                inputs.after_timeout_at_ms,
                timeout_at_ms
            ),
        });
    }

    BackgroundWorkInvariantReport { checks }
}

pub fn evaluate_queue_complete_invariants(
    inputs: QueueCompleteInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let mut checks = Vec::new();
    if matches!(
        inputs.outcome,
        QueueCompleteOutcomeKind::Removed | QueueCompleteOutcomeKind::PromotedFollowUp { .. }
    ) {
        checks.push(InvariantCheck {
            name: "active_broker_lease_required_for_shard_mutation".to_owned(),
            passed: inputs.after_broker_id == Some(inputs.broker_id)
                && inputs.after_broker_epoch == Some(inputs.broker_epoch)
                && inputs
                    .after_broker_lease_expires_at_ms
                    .is_some_and(|lease_expires_at_ms| lease_expires_at_ms > inputs.now_ms),
            detail: format!(
                "broker={} epoch={} now_ms={} after_broker={:?} after_epoch={:?} lease_expires_at={:?}",
                inputs.broker_id,
                inputs.broker_epoch,
                inputs.now_ms,
                inputs.after_broker_id,
                inputs.after_broker_epoch,
                inputs.after_broker_lease_expires_at_ms
            ),
        });
    }
    if matches!(inputs.outcome, QueueCompleteOutcomeKind::ClaimTokenMismatch) {
        checks.push(InvariantCheck {
            name: "stale_claim_token_cannot_complete".to_owned(),
            passed: inputs.before_claim_token.is_some()
                && inputs.before_claim_token != Some(inputs.provided_claim_token),
            detail: format!(
                "before_claim_token={:?} provided_claim_token={}",
                inputs.before_claim_token, inputs.provided_claim_token
            ),
        });
    }
    if inputs.prior_stolen_claim_seen && matches!(inputs.outcome, QueueCompleteOutcomeKind::Removed)
    {
        checks.push(InvariantCheck {
            name: "stolen_job_completes_once".to_owned(),
            passed: !inputs.after_job_present,
            detail: format!(
                "prior_stolen_claim_seen={} after_job_present={}",
                inputs.prior_stolen_claim_seen, inputs.after_job_present
            ),
        });
    }

    BackgroundWorkInvariantReport { checks }
}

pub fn evaluate_checkpoint_head_publish_invariants(
    inputs: CheckpointHeadPublishInvariantInputs<'_>,
) -> BackgroundWorkInvariantReport {
    let requested_retention_floor_seq = inputs.requested_retention_floor_seq;
    let required_progress_satisfied = requested_retention_floor_seq.is_none_or(|requested| {
        inputs.required_progress.iter().all(|progress| {
            progress.namespace_id == &inputs.current_head.namespace_id
                && progress.through_seq >= requested
        })
    });
    let retention_policy_satisfied = requested_retention_floor_seq.is_none_or(|requested| {
        inputs.retention_policy.is_some_and(|progress| {
            progress.namespace_id == &inputs.current_head.namespace_id
                && progress.through_seq >= requested
        })
    });

    BackgroundWorkInvariantReport {
        checks: vec![
            InvariantCheck {
                name: "checkpoint_publish_requires_verified_checkpoint".to_owned(),
                passed: inputs.checkpoint_verified
                    && inputs.checkpoint_segments_verified
                    && inputs.current_head.namespace_id == *inputs.checkpoint_namespace
                    && inputs.checkpoint_seq <= inputs.current_head.seq,
                detail: format!(
                    "head_namespace={} checkpoint_namespace={} checkpoint_seq={} head_seq={} checkpoint_verified={} checkpoint_segments_verified={}",
                    inputs.current_head.namespace_id,
                    inputs.checkpoint_namespace,
                    inputs.checkpoint_seq.0,
                    inputs.current_head.seq.0,
                    inputs.checkpoint_verified,
                    inputs.checkpoint_segments_verified
                ),
            },
            InvariantCheck {
                name: "snapshot_hint_seq_advances_monotonically".to_owned(),
                passed: inputs.resulting_head.snapshot_hint_seq.unwrap_or(ChangeSeq(0))
                    >= inputs.current_head.snapshot_hint_seq.unwrap_or(ChangeSeq(0))
                    && inputs.resulting_head.snapshot_hint_seq.unwrap_or(ChangeSeq(0))
                        >= inputs.checkpoint_seq,
                detail: format!(
                    "before_snapshot_hint={:?} checkpoint_seq={} after_snapshot_hint={:?}",
                    inputs.current_head.snapshot_hint_seq.map(|seq| seq.0),
                    inputs.checkpoint_seq.0,
                    inputs.resulting_head.snapshot_hint_seq.map(|seq| seq.0)
                ),
            },
            InvariantCheck {
                name: "retention_floor_seq_advances_monotonically".to_owned(),
                passed: requested_retention_floor_seq.is_none_or(|requested| {
                    inputs.resulting_head.retention_floor_seq >= inputs.current_head.retention_floor_seq
                        && inputs.resulting_head.retention_floor_seq == requested
                }),
                detail: format!(
                    "before_retention_floor={} requested_retention_floor={:?} after_retention_floor={}",
                    inputs.current_head.retention_floor_seq.0,
                    requested_retention_floor_seq.map(|seq| seq.0),
                    inputs.resulting_head.retention_floor_seq.0
                ),
            },
            InvariantCheck {
                name: "retention_floor_seq_requires_checkpoint_coverage".to_owned(),
                passed: requested_retention_floor_seq
                    .is_none_or(|requested| requested <= inputs.checkpoint_seq),
                detail: format!(
                    "requested_retention_floor={:?} checkpoint_seq={}",
                    requested_retention_floor_seq.map(|seq| seq.0),
                    inputs.checkpoint_seq.0
                ),
            },
            InvariantCheck {
                name: "retention_floor_seq_requires_derived_progress".to_owned(),
                passed: required_progress_satisfied,
                detail: format!(
                    "requested_retention_floor={:?} required_progress={:?}",
                    requested_retention_floor_seq.map(|seq| seq.0),
                    inputs
                        .required_progress
                        .iter()
                        .map(|progress| (
                            progress.namespace_id.as_str().to_owned(),
                            progress.work_class.to_owned(),
                            progress.through_seq.0
                        ))
                        .collect::<Vec<_>>()
                ),
            },
            InvariantCheck {
                name: "retention_floor_seq_respects_policy_gate".to_owned(),
                passed: retention_policy_satisfied,
                detail: format!(
                    "requested_retention_floor={:?} retention_policy={:?}",
                    requested_retention_floor_seq.map(|seq| seq.0),
                    inputs.retention_policy.map(|progress| (
                        progress.namespace_id.as_str().to_owned(),
                        progress.work_class.to_owned(),
                        progress.through_seq.0
                    ))
                ),
            },
        ],
    }
}

fn metadata_apply_checks_decode_failure(error: &str) -> Vec<InvariantCheck> {
    [
        "create_dir_writes_inode_and_direntry_rows",
        "create_file_writes_inode_direntry_and_initial_revision",
        "replace_file_appends_new_revision_head",
        "rename_appends_new_direntry_binding",
        "delete_subtree_writes_tombstone_row",
        "restore_creates_new_revision_head",
    ]
    .into_iter()
    .map(|name| InvariantCheck {
        name: name.to_owned(),
        passed: false,
        detail: format!("wal_decode_failed error={error}"),
    })
    .collect()
}

fn sequenced_ops_for_commit<'a>(seq: ChangeSeq, ops: &'a [WalOp]) -> Vec<SequencedWalOp<'a>> {
    ops.iter().map(|op| SequencedWalOp { seq, op }).collect()
}

fn flatten_wal_ops<'a>(decoded: &'a [DecodedWalObject]) -> Vec<SequencedWalOp<'a>> {
    let mut out = Vec::new();
    for object in decoded {
        out.extend(object.envelope.payload.ops.iter().map(|op| SequencedWalOp {
            seq: object.envelope.payload.seq,
            op,
        }));
    }
    out
}

fn evaluate_metadata_apply_invariants(
    before_metadata: &MetadataState,
    ops: &[SequencedWalOp<'_>],
    after_metadata: &MetadataState,
) -> Vec<InvariantCheck> {
    let mut states = BTreeMap::<&'static str, AggregateCheck>::from([
        (
            "create_dir_writes_inode_and_direntry_rows",
            AggregateCheck::not_applicable("no create_dir wal op"),
        ),
        (
            "create_file_writes_inode_direntry_and_initial_revision",
            AggregateCheck::not_applicable("no create_file wal op"),
        ),
        (
            "replace_file_appends_new_revision_head",
            AggregateCheck::not_applicable("no replace_file wal op"),
        ),
        (
            "rename_appends_new_direntry_binding",
            AggregateCheck::not_applicable("no rename wal op"),
        ),
        (
            "delete_subtree_writes_tombstone_row",
            AggregateCheck::not_applicable("no delete_subtree wal op"),
        ),
        (
            "restore_creates_new_revision_head",
            AggregateCheck::not_applicable("no restore_revision wal op"),
        ),
    ]);

    let mut metadata_state = before_metadata.clone();
    for sequenced in ops {
        match sequenced.op {
            WalOp::CreateDir {
                op_index,
                inode_id,
                parent_inode,
                display_name,
            } => {
                let passed = after_metadata.inodes.iter().any(|inode| {
                    inode.inode_id == *inode_id
                        && inode.inode_kind == InodeKind::Dir
                        && inode.created_seq == sequenced.seq
                }) && after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *parent_inode
                        && direntry.name_key == *display_name
                        && direntry.display_name == *display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                });
                states
                    .get_mut("create_dir_writes_inode_and_direntry_rows")
                    .expect("create_dir invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} parent={} name={}",
                            sequenced.seq.0, op_index, inode_id.0, parent_inode.0, display_name
                        ),
                    );
            }
            WalOp::CreateFile {
                op_index,
                inode_id,
                parent_inode,
                display_name,
                content_manifest_digest,
            } => {
                let passed = after_metadata.inodes.iter().any(|inode| {
                    inode.inode_id == *inode_id
                        && inode.inode_kind == InodeKind::File
                        && inode.created_seq == sequenced.seq
                }) && after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *parent_inode
                        && direntry.name_key == *display_name
                        && direntry.display_name == *display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                }) && after_metadata.revisions.iter().any(|revision| {
                    revision.inode_id == *inode_id
                        && revision.revision_no == RevisionNo(1)
                        && revision.committed_seq == sequenced.seq
                        && revision.revision_op_index == *op_index
                        && revision.content_manifest_digest == *content_manifest_digest
                });
                states
                    .get_mut("create_file_writes_inode_direntry_and_initial_revision")
                    .expect("create_file invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} parent={} name={} digest={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            parent_inode.0,
                            display_name,
                            content_manifest_digest
                        ),
                    );
            }
            WalOp::ReplaceFile {
                op_index,
                inode_id,
                base_revision,
                content_manifest_digest,
            } => {
                let next_revision = RevisionNo(base_revision.0.saturating_add(1));
                let passed = after_metadata.revisions.iter().any(|revision| {
                    revision.inode_id == *inode_id
                        && revision.revision_no == next_revision
                        && revision.committed_seq == sequenced.seq
                        && revision.revision_op_index == *op_index
                        && revision.content_manifest_digest == *content_manifest_digest
                });
                states
                    .get_mut("replace_file_appends_new_revision_head")
                    .expect("replace_file invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} base_revision={} next_revision={} digest={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            base_revision.0,
                            next_revision.0,
                            content_manifest_digest
                        ),
                    );
            }
            WalOp::Rename {
                op_index,
                inode_id,
                new_parent_inode,
                new_display_name,
            } => {
                let passed = after_metadata.direntries.iter().any(|direntry| {
                    direntry.parent_inode_id == *new_parent_inode
                        && direntry.name_key == *new_display_name
                        && direntry.display_name == *new_display_name
                        && direntry.child_inode_id == *inode_id
                        && direntry.bind_seq == sequenced.seq
                        && direntry.bind_op_index == *op_index
                });
                states
                    .get_mut("rename_appends_new_direntry_binding")
                    .expect("rename invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} new_parent={} new_name={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            new_parent_inode.0,
                            new_display_name
                        ),
                    );
            }
            WalOp::DeleteSubtree {
                op_index,
                root_inode,
            } => {
                let passed = after_metadata.subtree_tombstones.iter().any(|tombstone| {
                    tombstone.root_inode_id == *root_inode
                        && tombstone.tombstone_seq == sequenced.seq
                        && tombstone.tombstone_op_index == *op_index
                });
                states
                    .get_mut("delete_subtree_writes_tombstone_row")
                    .expect("delete invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} root_inode={}",
                            sequenced.seq.0, op_index, root_inode.0
                        ),
                    );
            }
            WalOp::RestoreRevision {
                op_index,
                inode_id,
                base_revision,
                restore_from_revision,
            } => {
                let next_revision = RevisionNo(base_revision.0.saturating_add(1));
                let source_digest = metadata_state
                    .revision_at_seq(*inode_id, *restore_from_revision, sequenced.seq)
                    .map(|revision| revision.content_manifest_digest);
                let passed = source_digest.as_ref().is_some_and(|digest| {
                    after_metadata.revisions.iter().any(|revision| {
                        revision.inode_id == *inode_id
                            && revision.revision_no == next_revision
                            && revision.committed_seq == sequenced.seq
                            && revision.revision_op_index == *op_index
                            && revision.content_manifest_digest == *digest
                    })
                });
                states
                    .get_mut("restore_creates_new_revision_head")
                    .expect("restore invariant")
                    .record(
                        passed,
                        format!(
                            "seq={} op_index={} inode={} base_revision={} restore_from={} source_found={}",
                            sequenced.seq.0,
                            op_index,
                            inode_id.0,
                            base_revision.0,
                            restore_from_revision.0,
                            source_digest.is_some()
                        ),
                    );
            }
        }

        if let Ok(applied) = metadata_state
            .apply_committed_wal_ops(sequenced.seq, std::slice::from_ref(sequenced.op))
        {
            metadata_state = applied.metadata_state;
        }
    }

    states
        .into_iter()
        .map(|(name, aggregate)| aggregate.finish(name))
        .collect()
}

fn check_create_mutation_consumes_next_inode_id(
    initial_next_inode_id: InodeId,
    ops: &[WalOp],
    resulting_next_inode_id: InodeId,
) -> InvariantCheck {
    let create_ids = ops
        .iter()
        .filter_map(|op| match op {
            WalOp::CreateDir { inode_id, .. } | WalOp::CreateFile { inode_id, .. } => {
                Some(*inode_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if create_ids.is_empty() {
        return InvariantCheck {
            name: "create_mutation_consumes_next_inode_id".to_owned(),
            passed: false,
            detail: "no create ops".to_owned(),
        };
    }

    let contiguous = create_ids.iter().enumerate().all(|(offset, inode_id)| {
        initial_next_inode_id
            .0
            .checked_add(offset as u64)
            .is_some_and(|expected| expected == inode_id.0)
    });
    let expected_next = initial_next_inode_id
        .0
        .checked_add(create_ids.len() as u64)
        .unwrap_or(u64::MAX);

    InvariantCheck {
        name: "create_mutation_consumes_next_inode_id".to_owned(),
        passed: contiguous && resulting_next_inode_id.0 == expected_next,
        detail: format!(
            "initial_next_inode={} allocated={:?} resulting_next_inode={} expected_next_inode={}",
            initial_next_inode_id.0,
            create_ids.iter().map(|inode| inode.0).collect::<Vec<_>>(),
            resulting_next_inode_id.0,
            expected_next
        ),
    }
}

fn check_requires_durable_content<F>(
    name: &'static str,
    ops: &[WalOp],
    _after_metadata: &MetadataState,
    mut evaluator: F,
) -> InvariantCheck
where
    F: FnMut(&WalOp) -> Option<bool>,
{
    let mut applicable = false;
    let mut passed = true;
    let mut details = Vec::new();

    for op in ops {
        if let Some(op_passed) = evaluator(op) {
            applicable = true;
            passed &= op_passed;
            details.push(format!("op={op:?} passed={op_passed}"));
        }
    }

    InvariantCheck {
        name: name.to_owned(),
        passed: applicable && passed,
        detail: if applicable {
            details.join("; ")
        } else {
            "no matching op".to_owned()
        },
    }
}

fn check_subtree_tombstone_blocks_descendant_mutation(
    before_metadata: &MetadataState,
    committed_seq: ChangeSeq,
    ops: &[WalOp],
) -> InvariantCheck {
    let mut metadata_state = before_metadata.clone();
    let mut applicable = false;
    let mut passed = true;
    let mut details = Vec::new();

    for op in ops {
        let result = match op {
            WalOp::CreateDir { parent_inode, .. } | WalOp::CreateFile { parent_inode, .. } => {
                applicable = true;
                let covering =
                    metadata_state.covering_subtree_tombstone(*parent_inode, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} parent_inode={} covering_root={:?}",
                        parent_inode.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::ReplaceFile { inode_id, .. } | WalOp::RestoreRevision { inode_id, .. } => {
                applicable = true;
                let covering = metadata_state.covering_subtree_tombstone(*inode_id, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} inode={} covering_root={:?}",
                        inode_id.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::Rename {
                inode_id,
                new_parent_inode,
                ..
            } => {
                applicable = true;
                let inode_covering =
                    metadata_state.covering_subtree_tombstone(*inode_id, committed_seq);
                let parent_covering =
                    metadata_state.covering_subtree_tombstone(*new_parent_inode, committed_seq);
                (
                    inode_covering.is_none() && parent_covering.is_none(),
                    format!(
                        "op={op:?} inode_covering={:?} parent_covering={:?}",
                        inode_covering.as_ref().map(|record| record.root_inode_id.0),
                        parent_covering
                            .as_ref()
                            .map(|record| record.root_inode_id.0)
                    ),
                )
            }
            WalOp::DeleteSubtree { root_inode, .. } => {
                applicable = true;
                let covering =
                    metadata_state.covering_subtree_tombstone(*root_inode, committed_seq);
                (
                    covering.is_none(),
                    format!(
                        "op={op:?} root_inode={} covering_root={:?}",
                        root_inode.0,
                        covering.as_ref().map(|record| record.root_inode_id.0)
                    ),
                )
            }
        };
        passed &= result.0;
        details.push(result.1);

        if let Ok(applied) =
            metadata_state.apply_committed_wal_ops(committed_seq, std::slice::from_ref(op))
        {
            metadata_state = applied.metadata_state;
        }
    }

    InvariantCheck {
        name: "subtree_tombstone_blocks_descendant_mutation".to_owned(),
        passed: applicable && passed,
        detail: if applicable {
            details.join("; ")
        } else {
            "no mutation with ancestor/tombstone coverage rules".to_owned()
        },
    }
}

fn wal_payload_checksum_check(decoded: &Result<Vec<DecodedWalObject>, String>) -> InvariantCheck {
    match decoded {
        Ok(objects) => InvariantCheck {
            name: "wal_payload_checksum_matches_payload".to_owned(),
            passed: true,
            detail: format!("decoded_wal_objects={}", objects.len()),
        },
        Err(error) => InvariantCheck {
            name: "wal_payload_checksum_matches_payload".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_key_matches_committed_seq_check(
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mismatches = objects
                .iter()
                .filter_map(|object| {
                    let expected = wal_commit(
                        object.envelope.payload.namespace_id.as_str(),
                        object.envelope.payload.seq.0,
                        &object.envelope.payload.commit_id,
                    );
                    (expected != object.object_key)
                        .then(|| format!("expected={expected} actual={}", object.object_key))
                })
                .collect::<Vec<_>>();
            InvariantCheck {
                name: "wal_key_matches_committed_seq".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!("validated_object_keys={}", objects.len())
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_key_matches_committed_seq".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn head_publish_requires_durable_wal_check(
    basis_head: &HeadState,
    after_head: &HeadState,
    decoded: Option<&[DecodedWalObject]>,
) -> InvariantCheck {
    let advanced = after_head.seq > basis_head.seq;
    let covered = match decoded {
        Some(objects) if !objects.is_empty() => objects
            .last()
            .map(|object| object.envelope.payload.seq == after_head.seq)
            .unwrap_or(false),
        Some(_) => after_head.seq == basis_head.seq,
        None => false,
    };

    InvariantCheck {
        name: "head_publish_requires_durable_wal".to_owned(),
        passed: (!advanced && decoded.is_some()) || (advanced && covered),
        detail: format!(
            "basis_seq={} after_seq={} wal_tail_len={} covered={covered}",
            basis_head.seq.0,
            after_head.seq.0,
            decoded.map_or(0, |objects| objects.len())
        ),
    }
}

fn wal_replay_requires_matching_namespace_check(
    expected_namespace: &str,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mismatches = objects
                .iter()
                .filter(|object| {
                    object.envelope.payload.namespace_id.as_str() != expected_namespace
                })
                .map(|object| object.envelope.payload.namespace_id.to_string())
                .collect::<Vec<_>>();
            InvariantCheck {
                name: "wal_replay_requires_matching_namespace".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!("expected_namespace={expected_namespace}")
                } else {
                    format!("unexpected_namespaces={mismatches:?}")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_replay_requires_matching_namespace".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_replay_requires_matching_base_head_seq_check(
    basis_head: &HeadState,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mut current_seq = basis_head.seq;
            let mut mismatches = Vec::new();
            for object in objects {
                if object.envelope.payload.base_head_seq != current_seq {
                    mismatches.push(format!(
                        "seq={} expected_base={} actual_base={}",
                        object.envelope.payload.seq.0,
                        current_seq.0,
                        object.envelope.payload.base_head_seq.0
                    ));
                }
                current_seq = object.envelope.payload.seq;
            }
            InvariantCheck {
                name: "wal_replay_requires_matching_base_head_seq".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!(
                        "basis_seq={} wal_objects={}",
                        basis_head.seq.0,
                        objects.len()
                    )
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_replay_requires_matching_base_head_seq".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_tail_seq_is_contiguous_check(
    basis_head: &HeadState,
    decoded: &Result<Vec<DecodedWalObject>, String>,
) -> InvariantCheck {
    match decoded {
        Ok(objects) => {
            let mut expected_seq = ChangeSeq(basis_head.seq.0.saturating_add(1));
            let mut mismatches = Vec::new();
            for object in objects {
                if object.envelope.payload.seq != expected_seq {
                    mismatches.push(format!(
                        "expected_seq={} actual_seq={}",
                        expected_seq.0, object.envelope.payload.seq.0
                    ));
                }
                expected_seq = ChangeSeq(object.envelope.payload.seq.0.saturating_add(1));
            }
            InvariantCheck {
                name: "wal_tail_seq_is_contiguous".to_owned(),
                passed: mismatches.is_empty(),
                detail: if mismatches.is_empty() {
                    format!(
                        "basis_seq={} wal_objects={}",
                        basis_head.seq.0,
                        objects.len()
                    )
                } else {
                    mismatches.join("; ")
                },
            }
        }
        Err(error) => InvariantCheck {
            name: "wal_tail_seq_is_contiguous".to_owned(),
            passed: false,
            detail: format!("wal_decode_failed error={error}"),
        },
    }
}

fn wal_replay_applies_metadata_rows_check(
    basis_head: &HeadState,
    basis_metadata: &MetadataState,
    after_head: &HeadState,
    after_metadata: &MetadataState,
    decoded: Option<&[DecodedWalObject]>,
) -> InvariantCheck {
    match decoded {
        Some(objects) => match replay_wal_tail_locally(basis_head, basis_metadata, objects) {
            Ok((replayed_head, replayed_metadata)) => InvariantCheck {
                name: "wal_replay_applies_metadata_rows".to_owned(),
                passed: replayed_head == *after_head && replayed_metadata == *after_metadata,
                detail: format!(
                    "replayed_head_seq={} after_head_seq={} replayed_metadata_matches={}",
                    replayed_head.seq.0,
                    after_head.seq.0,
                    replayed_metadata == *after_metadata
                ),
            },
            Err(error) => InvariantCheck {
                name: "wal_replay_applies_metadata_rows".to_owned(),
                passed: false,
                detail: error,
            },
        },
        None => InvariantCheck {
            name: "wal_replay_applies_metadata_rows".to_owned(),
            passed: false,
            detail: "wal_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_manifest_key_matches_seq_check(
    stored_manifest: &StoredCheckpointManifest,
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => {
            let expected = snapshot_manifest(
                manifest.payload.namespace_id.as_str(),
                manifest.payload.checkpoint_seq.0,
            );
            InvariantCheck {
                name: "checkpoint_manifest_key_matches_seq".to_owned(),
                passed: stored_manifest.object_key == expected,
                detail: format!(
                    "expected={} actual={}",
                    expected, stored_manifest.object_key
                ),
            }
        }
        None => InvariantCheck {
            name: "checkpoint_manifest_key_matches_seq".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_manifest_must_be_verified_check(
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => InvariantCheck {
            name: "checkpoint_manifest_must_be_verified".to_owned(),
            passed: manifest.payload.verified,
            detail: format!("verified={}", manifest.payload.verified),
        },
        None => InvariantCheck {
            name: "checkpoint_manifest_must_be_verified".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_replay_requires_all_manifest_segments_check(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> InvariantCheck {
    match manifest {
        Some(manifest) => {
            let expected = manifest
                .payload
                .tables
                .iter()
                .flat_map(|table| {
                    table
                        .segments
                        .iter()
                        .map(|segment| segment.object_key.clone())
                })
                .collect::<BTreeSet<_>>();
            let actual = stored_segments
                .iter()
                .map(|segment| segment.object_key.clone())
                .collect::<BTreeSet<_>>();
            InvariantCheck {
                name: "checkpoint_replay_requires_all_manifest_segments".to_owned(),
                passed: expected == actual,
                detail: format!("expected={expected:?} actual={actual:?}"),
            }
        }
        None => InvariantCheck {
            name: "checkpoint_replay_requires_all_manifest_segments".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        },
    }
}

fn checkpoint_segment_descriptor_matches_payload_check(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
    decoded_segments: Option<&[DecodedCheckpointSegment]>,
) -> InvariantCheck {
    let Some(manifest) = manifest else {
        return InvariantCheck {
            name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
            passed: false,
            detail: "manifest_decode_failed".to_owned(),
        };
    };
    let Some(decoded_segments) = decoded_segments else {
        return InvariantCheck {
            name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
            passed: false,
            detail: "segment_decode_failed".to_owned(),
        };
    };

    let by_key = decoded_segments
        .iter()
        .map(|segment| (segment.object_key.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    let mut mismatches = Vec::new();

    for table in &manifest.payload.tables {
        for expected in &table.segments {
            let Some(decoded) = by_key.get(expected.object_key.as_str()) else {
                mismatches.push(format!("missing_segment={}", expected.object_key));
                continue;
            };
            let actual = match checkpoint_segment_descriptor_from_payload(
                &decoded.object_key,
                &decoded.envelope,
            ) {
                Ok(actual) => actual,
                Err(error) => {
                    mismatches.push(format!(
                        "descriptor_build_failed key={} error={error}",
                        expected.object_key
                    ));
                    continue;
                }
            };
            if &actual != expected {
                mismatches.push(format!(
                    "descriptor_mismatch key={} expected={expected:?} actual={actual:?}",
                    expected.object_key
                ));
            }
        }
    }

    let unexpected = stored_segments
        .iter()
        .filter(|segment| {
            !manifest
                .payload
                .tables
                .iter()
                .flat_map(|table| table.segments.iter())
                .any(|expected| expected.object_key == segment.object_key)
        })
        .map(|segment| segment.object_key.clone())
        .collect::<Vec<_>>();
    if !unexpected.is_empty() {
        mismatches.push(format!("unexpected_segments={unexpected:?}"));
    }

    InvariantCheck {
        name: "checkpoint_segment_descriptor_matches_payload".to_owned(),
        passed: mismatches.is_empty(),
        detail: if mismatches.is_empty() {
            format!("validated_segments={}", decoded_segments.len())
        } else {
            mismatches.join("; ")
        },
    }
}

fn checkpoint_segment_rows_restore_basis_metadata_check(
    reconstructed_basis: Option<&MetadataState>,
    expected_basis: &MetadataState,
) -> InvariantCheck {
    match reconstructed_basis {
        Some(reconstructed_basis) => InvariantCheck {
            name: "checkpoint_segment_rows_restore_basis_metadata".to_owned(),
            passed: reconstructed_basis == expected_basis,
            detail: format!(
                "reconstructed_matches_expected={}",
                reconstructed_basis == expected_basis
            ),
        },
        None => InvariantCheck {
            name: "checkpoint_segment_rows_restore_basis_metadata".to_owned(),
            passed: false,
            detail: "basis_reconstruction_failed".to_owned(),
        },
    }
}

fn checkpoint_plus_wal_tail_reproduces_head_check(
    basis_head: &HeadState,
    after_head: &HeadState,
    durable_wal: Option<&InvariantCheck>,
    contiguous: Option<&InvariantCheck>,
    wal_objects: &[StoredWalObject],
) -> InvariantCheck {
    let pass = durable_wal.is_some_and(|check| check.passed)
        && contiguous.is_some_and(|check| check.passed)
        && (!wal_objects.is_empty() || basis_head.seq == after_head.seq);
    InvariantCheck {
        name: "checkpoint_plus_wal_tail_reproduces_head".to_owned(),
        passed: pass,
        detail: format!(
            "basis_seq={} after_seq={} wal_objects={} durable_wal_passed={} contiguous_passed={}",
            basis_head.seq.0,
            after_head.seq.0,
            wal_objects.len(),
            durable_wal.is_some_and(|check| check.passed),
            contiguous.is_some_and(|check| check.passed)
        ),
    }
}

fn checkpoint_plus_wal_tail_reproduces_metadata_check(
    wal_replay_applies: Option<&InvariantCheck>,
    after_metadata: &MetadataState,
) -> InvariantCheck {
    InvariantCheck {
        name: "checkpoint_plus_wal_tail_reproduces_metadata".to_owned(),
        passed: wal_replay_applies.is_some_and(|check| check.passed),
        detail: format!(
            "metadata_rows_replayed={} final_row_counts=(inodes={}, direntries={}, revisions={}, tombstones={})",
            wal_replay_applies.is_some_and(|check| check.passed),
            after_metadata.inodes.len(),
            after_metadata.direntries.len(),
            after_metadata.revisions.len(),
            after_metadata.subtree_tombstones.len()
        ),
    }
}

fn decode_wal_tail(wal_objects: &[StoredWalObject]) -> Result<Vec<DecodedWalObject>, String> {
    wal_objects
        .iter()
        .map(|object| {
            decode_wal_commit_envelope_zstd(&object.encoded_bytes)
                .map(|envelope| DecodedWalObject {
                    object_key: object.object_key.clone(),
                    envelope,
                })
                .map_err(|err| err.to_string())
        })
        .collect()
}

fn decode_checkpoint_segments(
    stored_segments: &[StoredCheckpointSegment],
) -> Result<Vec<DecodedCheckpointSegment>, String> {
    stored_segments
        .iter()
        .map(|segment| {
            decode_checkpoint_segment_envelope_zstd(&segment.encoded_bytes)
                .map(|envelope| DecodedCheckpointSegment {
                    object_key: segment.object_key.clone(),
                    envelope,
                })
                .map_err(|err| err.to_string())
        })
        .collect()
}

fn reconstruct_checkpoint_metadata(
    stored_segments: &[StoredCheckpointSegment],
    manifest: Option<&loon_types::CheckpointManifestEnvelope>,
) -> Result<MetadataState, String> {
    let Some(manifest) = manifest else {
        return Err("manifest_decode_failed".to_owned());
    };
    let decoded_segments = decode_checkpoint_segments(stored_segments)?;
    let by_key = decoded_segments
        .iter()
        .map(|segment| (segment.object_key.as_str(), &segment.envelope))
        .collect::<BTreeMap<_, _>>();
    let mut metadata = MetadataState::default();

    for table in &manifest.payload.tables {
        for descriptor in &table.segments {
            let envelope = by_key
                .get(descriptor.object_key.as_str())
                .ok_or_else(|| format!("missing_segment={}", descriptor.object_key))?;
            for page in &envelope.payload.pages {
                if page.row_keys.len() != page.rows.len() {
                    return Err(format!(
                        "page_row_key_mismatch key={} page_index={}",
                        descriptor.object_key, page.page_index
                    ));
                }
                let actual_row_keys = page
                    .rows
                    .iter()
                    .map(CheckpointRow::row_key)
                    .collect::<Vec<_>>();
                if page.row_keys != actual_row_keys {
                    return Err(format!(
                        "page_row_key_mismatch key={} page_index={}",
                        descriptor.object_key, page.page_index
                    ));
                }

                for row in &page.rows {
                    match row {
                        CheckpointRow::Inode {
                            inode_id,
                            inode_kind,
                            created_seq,
                        } => {
                            if table.family != CheckpointTableFamily::Inodes {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.inodes.push(InodeRecord {
                                inode_id: *inode_id,
                                inode_kind: inode_kind.clone(),
                                created_seq: *created_seq,
                            });
                        }
                        CheckpointRow::Direntry {
                            parent_inode_id,
                            name_key,
                            display_name,
                            child_inode_id,
                            bind_seq,
                            bind_op_index,
                        } => {
                            if table.family != CheckpointTableFamily::Direntries {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.direntries.push(DirentryRecord {
                                parent_inode_id: *parent_inode_id,
                                name_key: name_key.clone(),
                                display_name: display_name.clone(),
                                child_inode_id: *child_inode_id,
                                bind_seq: *bind_seq,
                                bind_op_index: *bind_op_index,
                            });
                        }
                        CheckpointRow::Revision {
                            inode_id,
                            revision_no,
                            committed_seq,
                            revision_op_index,
                            content_manifest_digest,
                        } => {
                            if table.family != CheckpointTableFamily::Revisions {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.revisions.push(RevisionRecord {
                                inode_id: *inode_id,
                                revision_no: *revision_no,
                                committed_seq: *committed_seq,
                                revision_op_index: *revision_op_index,
                                content_manifest_digest: content_manifest_digest.clone(),
                            });
                        }
                        CheckpointRow::Tombstone {
                            root_inode_id,
                            tombstone_seq,
                            tombstone_op_index,
                        } => {
                            if table.family != CheckpointTableFamily::Tombstones {
                                return Err(format!(
                                    "row_family_mismatch key={} expected_family={:?} row={row:?}",
                                    descriptor.object_key, table.family
                                ));
                            }
                            metadata.subtree_tombstones.push(SubtreeTombstoneRecord {
                                root_inode_id: *root_inode_id,
                                tombstone_seq: *tombstone_seq,
                                tombstone_op_index: *tombstone_op_index,
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(metadata)
}

fn replay_wal_tail_locally(
    basis_head: &HeadState,
    basis_metadata: &MetadataState,
    decoded: &[DecodedWalObject],
) -> Result<(HeadState, MetadataState), String> {
    let mut head = basis_head.clone();
    let mut metadata = basis_metadata.clone();

    for object in decoded {
        if object.envelope.payload.namespace_id != head.namespace_id {
            return Err(format!(
                "namespace_mismatch expected={} actual={}",
                head.namespace_id, object.envelope.payload.namespace_id
            ));
        }
        if object.envelope.payload.base_head_seq != head.seq {
            return Err(format!(
                "base_head_seq_mismatch expected={} actual={}",
                head.seq.0, object.envelope.payload.base_head_seq.0
            ));
        }
        if object.envelope.payload.seq != ChangeSeq(head.seq.0.saturating_add(1)) {
            return Err(format!(
                "non_contiguous_seq expected={} actual={}",
                head.seq.0.saturating_add(1),
                object.envelope.payload.seq.0
            ));
        }
        let applied = metadata
            .apply_committed_wal_ops(object.envelope.payload.seq, &object.envelope.payload.ops)
            .map_err(|err| format!("metadata_apply_failed error={err:?}"))?;
        metadata = applied.metadata_state;
        head = HeadState {
            namespace_id: head.namespace_id.clone(),
            seq: object.envelope.payload.seq,
            active_fence_token: object.envelope.payload.writer_fence_token,
            next_inode_id: next_inode_after_ops(head.next_inode_id, &object.envelope.payload.ops),
            snapshot_hint_seq: head.snapshot_hint_seq,
            retention_floor_seq: head.retention_floor_seq,
        };
    }

    Ok((head, metadata))
}

fn next_inode_after_ops(current: InodeId, ops: &[WalOp]) -> InodeId {
    let create_count = ops
        .iter()
        .filter(|op| matches!(op, WalOp::CreateDir { .. } | WalOp::CreateFile { .. }))
        .count() as u64;
    InodeId(current.0.saturating_add(create_count))
}

fn checkpoint_segment_descriptor_from_payload(
    object_key: &str,
    envelope: &CheckpointSegmentEnvelope,
) -> Result<CheckpointSegmentDescriptor, String> {
    Ok(CheckpointSegmentDescriptor {
        object_key: object_key.to_owned(),
        segment_index: envelope.payload.segment_index,
        row_count: envelope.payload.row_count,
        min_key: envelope.payload.min_key.clone(),
        max_key: envelope.payload.max_key.clone(),
        payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
        page_checksums_sha256: envelope
            .payload
            .pages
            .iter()
            .map(checkpoint_page_checksum_sha256)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| err.to_string())?,
    })
}

fn snapshot_table_family_from_checkpoint(family: CheckpointTableFamily) -> SnapshotTableFamily {
    match family {
        CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
        CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
        CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
        CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
    }
}

#[derive(Debug, Clone)]
struct SequencedWalOp<'a> {
    seq: ChangeSeq,
    op: &'a WalOp,
}

#[derive(Debug, Clone)]
struct DecodedWalObject {
    object_key: String,
    envelope: WalCommitEnvelope,
}

#[derive(Debug, Clone)]
struct DecodedCheckpointSegment {
    object_key: String,
    envelope: CheckpointSegmentEnvelope,
}

#[derive(Debug, Clone)]
struct AggregateCheck {
    applicable: bool,
    passed: bool,
    details: Vec<String>,
}

impl AggregateCheck {
    fn not_applicable(detail: &str) -> Self {
        Self {
            applicable: false,
            passed: false,
            details: vec![detail.to_owned()],
        }
    }

    fn record(&mut self, passed: bool, detail: String) {
        if !self.applicable {
            self.passed = true;
        }
        self.applicable = true;
        if self.details.len() == 1 && self.details[0].starts_with("no ") {
            self.details.clear();
        }
        self.passed &= passed;
        self.details.push(detail);
    }

    fn finish(self, name: &str) -> InvariantCheck {
        InvariantCheck {
            name: name.to_owned(),
            passed: self.applicable && self.passed,
            detail: self.details.join("; "),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        evaluate_apply_remote_delete_invariants, evaluate_checkpoint_head_publish_invariants,
        evaluate_checkpoint_object_invariants, evaluate_content_object_invariants,
        evaluate_download_transfer_invariants, evaluate_inode_upload_transfer_invariants,
        evaluate_local_only_upload_transfer_invariants,
        evaluate_namespace_checkpoint_replay_invariants, evaluate_namespace_commit_invariants,
        evaluate_namespace_wal_replay_invariants, evaluate_progress_publish_invariants,
        evaluate_queue_complete_invariants, evaluate_remote_delete_planning_invariants,
        evaluate_remote_observation_ambiguous_bind_invariants,
        evaluate_remote_observation_convergence_invariants,
        evaluate_remote_only_directory_materialization_invariants,
        ApplyRemoteDeleteInvariantInputs, CheckpointHeadPublishInvariantInputs,
        CheckpointObjectInvariantInputs, CheckpointObjectInvariantSnapshot,
        CheckpointProgressAuthorizer, CheckpointReplayInvariantInputs, CommitInvariantInputs,
        ContentObjectInvariantInputs, ContentObjectInvariantSnapshot,
        DownloadTransferInvariantInputs, DownloadTransferOutcomeKind,
        InodeUploadTransferInvariantInputs, InodeUploadTransferOutcomeKind,
        LocalOnlyUploadTransferInvariantInputs, LocalOnlyUploadTransferOutcomeKind,
        ProgressInvariantSnapshot, ProgressPublishInvariantInputs, ProgressPublishOutcomeKind,
        QueueCompleteInvariantInputs, QueueCompleteOutcomeKind,
        RemoteDeletePlanningInvariantInputs, RemoteObservationAmbiguousBindInvariantInputs,
        RemoteObservationConvergenceInvariantInputs,
        RemoteOnlyDirectoryMaterializationInvariantInputs,
        RemoteOnlyDirectoryMaterializationOutcomeKind, StoredCheckpointSegmentSnapshot,
        StoredContentBlockSnapshot, WalReplayInvariantInputs,
    };
    use loon_core::checkpoint::{StoredCheckpointManifest, StoredCheckpointSegment};
    use loon_core::commit::{
        build_commit_plan, prepare_commit_head_publish, CommitOp, CommitRequest,
        CommitValidationContext, Precondition,
    };
    use loon_core::metadata::{DirentryRecord, InodeRecord, MetadataState, RevisionRecord};
    use loon_core::wal::{prepare_wal_commit, StoredWalObject};
    use loon_objectstore::keys::{
        blob, content_manifest, derived_progress, snapshot_manifest, snapshot_table,
        SnapshotTableFamily,
    };
    use loon_types::{
        checkpoint_page_checksum_sha256, decode_checkpoint_manifest_json,
        decode_checkpoint_segment_envelope_zstd, encode_checkpoint_manifest_json,
        encode_checkpoint_segment_envelope_zstd, encode_content_manifest_json, sha256_digest,
        ChangeSeq, CheckpointManifestEnvelope, CheckpointManifestPayload, CheckpointPage,
        CheckpointRow, CheckpointSegmentDescriptor, CheckpointSegmentEnvelope,
        CheckpointSegmentPayload, CheckpointTableFamily, CheckpointTableManifest,
        ContentBlockDescriptor, ContentManifestEnvelope, ContentManifestPayload, FenceToken,
        HeadState, InodeId, InodeKind, LeaseState, NamespaceId, RevisionNo,
        CONTENT_BLOCK_SIZE_BYTES,
    };
    use std::collections::BTreeMap;

    const NOW_MS: u64 = 1_500;
    const TEST_WRITER_VERSION: &str = "loon-testkit-invariants";

    #[test]
    fn commit_invariant_report_marks_create_file_row_write_as_passed() {
        let namespace_id = NamespaceId::new("ns-test");
        let before_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(5),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(7),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let before_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(9),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(9),
            planned_head_seq: before_head.seq,
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:abc".to_owned(),
            }],
            preconditions: vec![
                Precondition::HeadSeqIs(before_head.seq),
                Precondition::ChildNameAbsent {
                    parent_inode: InodeId(2),
                    name_key: "note.txt".to_owned(),
                },
            ],
        };
        let before_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: before_head.clone(),
                lease: before_lease.clone(),
                now_ms: NOW_MS,
                metadata_state: before_metadata.clone(),
            },
        )
        .expect("plan");
        let prepared_wal = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        let after_metadata = before_metadata
            .apply_committed_wal_ops(plan.next_seq, &prepared_wal.envelope.payload.ops)
            .expect("apply metadata")
            .metadata_state;
        let after_head = prepare_commit_head_publish(&before_head, &plan, TEST_WRITER_VERSION)
            .expect("head publish")
            .resulting_head;

        let report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
            request: &request,
            before_head: &before_head,
            before_lease: &before_lease,
            before_metadata: &before_metadata,
            prepared_wal: &prepared_wal,
            after_head: &after_head,
            after_metadata: &after_metadata,
            now_ms: NOW_MS,
        });

        assert!(
            report
                .check("create_file_writes_inode_direntry_and_initial_revision")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn commit_invariant_report_marks_create_file_row_write_as_failed_when_revision_missing() {
        let namespace_id = NamespaceId::new("ns-test");
        let before_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(5),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(7),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let before_lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(9),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "req-1".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(9),
            planned_head_seq: before_head.seq,
            ops: vec![CommitOp::CreateFile {
                parent_inode: InodeId(2),
                display_name: "note.txt".to_owned(),
                content_manifest_digest: "sha256:abc".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(before_head.seq)],
        };
        let before_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: before_head.clone(),
                lease: before_lease.clone(),
                now_ms: NOW_MS,
                metadata_state: before_metadata.clone(),
            },
        )
        .expect("plan");
        let prepared_wal = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        let after_head = prepare_commit_head_publish(&before_head, &plan, TEST_WRITER_VERSION)
            .expect("head publish")
            .resulting_head;
        let mut after_metadata = before_metadata
            .apply_committed_wal_ops(plan.next_seq, &prepared_wal.envelope.payload.ops)
            .expect("apply metadata")
            .metadata_state;
        after_metadata.revisions.clear();

        let report = evaluate_namespace_commit_invariants(CommitInvariantInputs {
            request: &request,
            before_head: &before_head,
            before_lease: &before_lease,
            before_metadata: &before_metadata,
            prepared_wal: &prepared_wal,
            after_head: &after_head,
            after_metadata: &after_metadata,
            now_ms: NOW_MS,
        });

        assert!(
            !report
                .check("create_file_writes_inode_direntry_and_initial_revision")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn wal_replay_invariant_report_marks_metadata_apply_as_passed() {
        let namespace_id = NamespaceId::new("ns-wal");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(10),
            active_fence_token: FenceToken(4),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let basis_metadata = MetadataState::default();
        let wal = simple_replace_wal_object(&namespace_id, basis_head.seq, RevisionNo(1));
        let decoded = super::decode_wal_tail(std::slice::from_ref(&wal)).expect("decode wal");
        let (after_head, after_metadata) =
            super::replay_wal_tail_locally(&basis_head, &basis_metadata, &decoded).expect("replay");

        let report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
            expected_namespace: namespace_id.as_str(),
            basis_head: &basis_head,
            basis_metadata: &basis_metadata,
            wal_objects: &[StoredWalObject {
                object_key: wal.object_key.clone(),
                encoded_bytes: wal.encoded_bytes.clone(),
            }],
            after_head: &after_head,
            after_metadata: &after_metadata,
        });

        assert!(
            report
                .check("wal_replay_applies_metadata_rows")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn wal_replay_invariant_report_marks_metadata_apply_as_failed_when_final_state_diverges() {
        let namespace_id = NamespaceId::new("ns-wal");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(10),
            active_fence_token: FenceToken(4),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let basis_metadata = MetadataState::default();
        let wal = simple_replace_wal_object(&namespace_id, basis_head.seq, RevisionNo(1));
        let decoded = super::decode_wal_tail(std::slice::from_ref(&wal)).expect("decode wal");
        let (after_head, mut after_metadata) =
            super::replay_wal_tail_locally(&basis_head, &basis_metadata, &decoded).expect("replay");
        after_metadata.revisions.clear();

        let report = evaluate_namespace_wal_replay_invariants(WalReplayInvariantInputs {
            expected_namespace: namespace_id.as_str(),
            basis_head: &basis_head,
            basis_metadata: &basis_metadata,
            wal_objects: &[StoredWalObject {
                object_key: wal.object_key.clone(),
                encoded_bytes: wal.encoded_bytes.clone(),
            }],
            after_head: &after_head,
            after_metadata: &after_metadata,
        });

        assert!(
            !report
                .check("wal_replay_applies_metadata_rows")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_replay_invariant_report_marks_basis_restore_as_passed() {
        let namespace_id = NamespaceId::new("ns-checkpoint");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(30),
            active_fence_token: FenceToken(11),
            next_inode_id: InodeId(50),
            snapshot_hint_seq: Some(ChangeSeq(30)),
            retention_floor_seq: ChangeSeq(10),
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(9),
                bind_seq: ChangeSeq(30),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(9),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(30),
                revision_op_index: 0,
                content_manifest_digest: "sha256:checkpoint".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let fixture = checkpoint_fixture(&namespace_id, &basis_head, &basis_metadata);

        let report =
            evaluate_namespace_checkpoint_replay_invariants(CheckpointReplayInvariantInputs {
                expected_namespace: namespace_id.as_str(),
                stored_manifest: &fixture.0,
                stored_segments: &fixture.1,
                basis_head: &basis_head,
                basis_metadata: &basis_metadata,
                wal_objects: &[],
                after_head: &basis_head,
                after_metadata: &basis_metadata,
            });

        assert!(
            report
                .check("checkpoint_segment_rows_restore_basis_metadata")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_replay_invariant_report_marks_basis_restore_as_failed_when_basis_diverges() {
        let namespace_id = NamespaceId::new("ns-checkpoint");
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(30),
            active_fence_token: FenceToken(11),
            next_inode_id: InodeId(50),
            snapshot_hint_seq: Some(ChangeSeq(30)),
            retention_floor_seq: ChangeSeq(10),
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(2),
                inode_kind: InodeKind::Dir,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: Vec::new(),
            subtree_tombstones: Vec::new(),
        };
        let fixture = checkpoint_fixture(&namespace_id, &basis_head, &basis_metadata);
        let mismatched_basis = MetadataState::default();

        let report =
            evaluate_namespace_checkpoint_replay_invariants(CheckpointReplayInvariantInputs {
                expected_namespace: namespace_id.as_str(),
                stored_manifest: &fixture.0,
                stored_segments: &fixture.1,
                basis_head: &basis_head,
                basis_metadata: &mismatched_basis,
                wal_objects: &[],
                after_head: &basis_head,
                after_metadata: &mismatched_basis,
            });

        assert!(
            !report
                .check("checkpoint_segment_rows_restore_basis_metadata")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn progress_invariant_report_marks_monotonic_advance_as_passed() {
        let namespace_id = NamespaceId::new("ns-progress");
        let report = evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
            expected_namespace: &namespace_id,
            expected_work_class: "BuildSnapshot",
            before_through_seq: Some(ChangeSeq(40)),
            requested_through_seq: ChangeSeq(42),
            outcome: ProgressPublishOutcomeKind::Advanced,
            after_progress: &ProgressInvariantSnapshot {
                object_key: derived_progress(namespace_id.as_str(), "BuildSnapshot"),
                namespace_id: namespace_id.clone(),
                work_class: "BuildSnapshot".to_owned(),
                through_seq: ChangeSeq(42),
                payload_checksum_valid: true,
            },
        });

        assert!(
            report
                .check("progress_through_seq_advances_monotonically")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn progress_invariant_report_marks_monotonic_advance_as_failed_when_seq_regresses() {
        let namespace_id = NamespaceId::new("ns-progress");
        let report = evaluate_progress_publish_invariants(ProgressPublishInvariantInputs {
            expected_namespace: &namespace_id,
            expected_work_class: "BuildSnapshot",
            before_through_seq: Some(ChangeSeq(40)),
            requested_through_seq: ChangeSeq(42),
            outcome: ProgressPublishOutcomeKind::Advanced,
            after_progress: &ProgressInvariantSnapshot {
                object_key: derived_progress(namespace_id.as_str(), "BuildSnapshot"),
                namespace_id: namespace_id.clone(),
                work_class: "BuildSnapshot".to_owned(),
                through_seq: ChangeSeq(39),
                payload_checksum_valid: true,
            },
        });

        assert!(
            !report
                .check("progress_through_seq_advances_monotonically")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn queue_complete_invariant_report_marks_stale_claim_rejection_as_passed() {
        let report = evaluate_queue_complete_invariants(QueueCompleteInvariantInputs {
            broker_id: "broker-b",
            broker_epoch: 2,
            now_ms: 30_000,
            provided_claim_token: "claim-a",
            before_claim_token: Some("claim-b"),
            after_broker_id: Some("broker-b"),
            after_broker_epoch: Some(2),
            after_broker_lease_expires_at_ms: Some(40_000),
            after_job_present: true,
            prior_stolen_claim_seen: false,
            outcome: QueueCompleteOutcomeKind::ClaimTokenMismatch,
        });

        assert!(
            report
                .check("stale_claim_token_cannot_complete")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn queue_complete_invariant_report_marks_stale_claim_rejection_as_failed_when_token_matches() {
        let report = evaluate_queue_complete_invariants(QueueCompleteInvariantInputs {
            broker_id: "broker-b",
            broker_epoch: 2,
            now_ms: 30_000,
            provided_claim_token: "claim-b",
            before_claim_token: Some("claim-b"),
            after_broker_id: Some("broker-b"),
            after_broker_epoch: Some(2),
            after_broker_lease_expires_at_ms: Some(40_000),
            after_job_present: true,
            prior_stolen_claim_seen: false,
            outcome: QueueCompleteOutcomeKind::ClaimTokenMismatch,
        });

        assert!(
            !report
                .check("stale_claim_token_cannot_complete")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_publish_invariant_report_marks_required_progress_gate_as_passed() {
        let namespace_id = NamespaceId::new("ns-checkpoint-publish");
        let current_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let resulting_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(42)),
            retention_floor_seq: ChangeSeq(42),
        };
        let required_progress = [CheckpointProgressAuthorizer {
            namespace_id: &namespace_id,
            work_class: "BuildListingIndex",
            through_seq: ChangeSeq(42),
        }];
        let retention_policy = CheckpointProgressAuthorizer {
            namespace_id: &namespace_id,
            work_class: "RetentionPolicy",
            through_seq: ChangeSeq(42),
        };

        let report =
            evaluate_checkpoint_head_publish_invariants(CheckpointHeadPublishInvariantInputs {
                current_head: &current_head,
                checkpoint_namespace: &namespace_id,
                checkpoint_seq: ChangeSeq(42),
                checkpoint_verified: true,
                checkpoint_segments_verified: true,
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                required_progress: &required_progress,
                retention_policy: Some(retention_policy),
                resulting_head: &resulting_head,
            });

        assert!(
            report
                .check("retention_floor_seq_requires_derived_progress")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_publish_invariant_report_marks_required_progress_gate_as_failed_when_progress_lags(
    ) {
        let namespace_id = NamespaceId::new("ns-checkpoint-publish");
        let current_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let resulting_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(42)),
            retention_floor_seq: ChangeSeq(42),
        };
        let required_progress = [CheckpointProgressAuthorizer {
            namespace_id: &namespace_id,
            work_class: "BuildListingIndex",
            through_seq: ChangeSeq(41),
        }];
        let retention_policy = CheckpointProgressAuthorizer {
            namespace_id: &namespace_id,
            work_class: "RetentionPolicy",
            through_seq: ChangeSeq(42),
        };

        let report =
            evaluate_checkpoint_head_publish_invariants(CheckpointHeadPublishInvariantInputs {
                current_head: &current_head,
                checkpoint_namespace: &namespace_id,
                checkpoint_seq: ChangeSeq(42),
                checkpoint_verified: true,
                checkpoint_segments_verified: true,
                requested_retention_floor_seq: Some(ChangeSeq(42)),
                required_progress: &required_progress,
                retention_policy: Some(retention_policy),
                resulting_head: &resulting_head,
            });

        assert!(
            !report
                .check("retention_floor_seq_requires_derived_progress")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_object_invariant_report_marks_basis_preservation_as_passed() {
        let namespace_id = NamespaceId::new("ns-checkpoint-object");
        let source_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let source_metadata = MetadataState {
            inodes: vec![
                InodeRecord {
                    inode_id: InodeId(2),
                    inode_kind: InodeKind::Dir,
                    created_seq: ChangeSeq(1),
                },
                InodeRecord {
                    inode_id: InodeId(9),
                    inode_kind: InodeKind::File,
                    created_seq: ChangeSeq(42),
                },
            ],
            direntries: vec![DirentryRecord {
                parent_inode_id: InodeId(2),
                name_key: "report.txt".to_owned(),
                display_name: "report.txt".to_owned(),
                child_inode_id: InodeId(9),
                bind_seq: ChangeSeq(42),
                bind_op_index: 0,
            }],
            revisions: vec![RevisionRecord {
                inode_id: InodeId(9),
                revision_no: RevisionNo(1),
                committed_seq: ChangeSeq(42),
                revision_op_index: 0,
                content_manifest_digest: "sha256:checkpoint".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let snapshot = checkpoint_object_snapshot(&source_head, &source_metadata);

        let report = evaluate_checkpoint_object_invariants(CheckpointObjectInvariantInputs {
            checkpoint: &snapshot,
        });

        assert!(
            report
                .check("checkpoint_manifest_preserves_basis_metadata")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn checkpoint_object_invariant_report_marks_head_summary_drift_as_failed() {
        let namespace_id = NamespaceId::new("ns-checkpoint-object");
        let source_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: ChangeSeq(42),
            active_fence_token: FenceToken(9),
            next_inode_id: InodeId(777),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };
        let source_metadata = MetadataState::default();
        let mut snapshot = checkpoint_object_snapshot(&source_head, &source_metadata);
        snapshot.manifest_envelope.payload.next_inode_id = InodeId(999);
        snapshot.manifest_bytes = encode_checkpoint_manifest_json(&snapshot.manifest_envelope)
            .expect("re-encode drifted manifest");

        let report = evaluate_checkpoint_object_invariants(CheckpointObjectInvariantInputs {
            checkpoint: &snapshot,
        });

        assert!(
            !report
                .check("checkpoint_manifest_preserves_head_summary")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn download_transfer_invariant_report_marks_completion_as_passed() {
        let report = evaluate_download_transfer_invariants(DownloadTransferInvariantInputs {
            before_block_index: Some(1),
            after_transfer_block_index: None,
            block_count: 2,
            reset_issue_kind: None,
            reset_issue_reason: None,
            remote_synced_seq: ChangeSeq(42),
            remote_revision_no: RevisionNo(1),
            remote_content_digest: Some("sha256:file"),
            remote_content_manifest_digest: Some("sha256:manifest"),
            local_exists_on_disk: true,
            local_dirty: false,
            local_content_digest: Some("sha256:file"),
            sync_anchor_seq: Some(ChangeSeq(42)),
            sync_anchor_revision_no: Some(RevisionNo(1)),
            sync_anchor_content_digest: Some("sha256:file"),
            sync_anchor_content_manifest_digest: Some("sha256:manifest"),
            outcome: DownloadTransferOutcomeKind::Completed,
        });

        assert!(
            report
                .check("download_materialization_updates_local_state_and_sync_anchor")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn download_transfer_invariant_report_marks_missing_reset_issue_as_failed() {
        let report = evaluate_download_transfer_invariants(DownloadTransferInvariantInputs {
            before_block_index: Some(1),
            after_transfer_block_index: Some(1),
            block_count: 2,
            reset_issue_kind: None,
            reset_issue_reason: None,
            remote_synced_seq: ChangeSeq(42),
            remote_revision_no: RevisionNo(1),
            remote_content_digest: Some("sha256:file"),
            remote_content_manifest_digest: Some("sha256:manifest"),
            local_exists_on_disk: false,
            local_dirty: false,
            local_content_digest: None,
            sync_anchor_seq: None,
            sync_anchor_revision_no: None,
            sync_anchor_content_digest: None,
            sync_anchor_content_manifest_digest: None,
            outcome: DownloadTransferOutcomeKind::ResetProgressed,
        });

        assert!(
            !report
                .check("download_transfer_reset_records_durable_issue")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn inode_upload_transfer_invariant_report_marks_retry_reuse_as_passed() {
        let report =
            evaluate_inode_upload_transfer_invariants(InodeUploadTransferInvariantInputs {
                before_block_index: None,
                after_transfer_block_index: None,
                block_count: 2,
                ensured_upload_present: false,
                upload_reused: true,
                before_pending_request_id: Some("client-req-1"),
                after_pending_request_id: Some("client-req-1"),
                reset_issue_kind: None,
                reset_issue_reason: None,
                outcome: InodeUploadTransferOutcomeKind::RetryReusedPending,
            });

        assert!(
            report
                .check("inode_upload_retry_reuses_pending_inode_mutation")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn inode_upload_transfer_invariant_report_marks_missing_reset_issue_as_failed() {
        let report =
            evaluate_inode_upload_transfer_invariants(InodeUploadTransferInvariantInputs {
                before_block_index: Some(1),
                after_transfer_block_index: Some(1),
                block_count: 2,
                ensured_upload_present: false,
                upload_reused: false,
                before_pending_request_id: None,
                after_pending_request_id: None,
                reset_issue_kind: None,
                reset_issue_reason: None,
                outcome: InodeUploadTransferOutcomeKind::ResetProgressed,
            });

        assert!(
            !report
                .check("inode_upload_transfer_reset_records_durable_issue")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn local_only_upload_transfer_invariant_report_marks_bind_cleanup_as_passed() {
        let report = evaluate_local_only_upload_transfer_invariants(
            LocalOnlyUploadTransferInvariantInputs {
                before_block_index: Some(1),
                after_transfer_block_index: None,
                block_count: 2,
                ensured_upload_present: true,
                upload_reused: false,
                before_pending_request_id: None,
                after_pending_request_id: None,
                reset_issue_kind: None,
                reset_issue_reason: None,
                local_only_file_present_after: false,
                local_only_issue_count_after: 0,
                outcome: LocalOnlyUploadTransferOutcomeKind::Completed,
            },
        );

        assert!(
            report
                .check("local_only_upload_bind_clears_temp_issue_and_transfer_ledger")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn local_only_upload_transfer_invariant_report_marks_missing_retry_reuse_as_failed() {
        let report = evaluate_local_only_upload_transfer_invariants(
            LocalOnlyUploadTransferInvariantInputs {
                before_block_index: None,
                after_transfer_block_index: None,
                block_count: 2,
                ensured_upload_present: false,
                upload_reused: false,
                before_pending_request_id: Some("client-req-1"),
                after_pending_request_id: Some("client-req-2"),
                reset_issue_kind: None,
                reset_issue_reason: None,
                local_only_file_present_after: true,
                local_only_issue_count_after: 1,
                outcome: LocalOnlyUploadTransferOutcomeKind::RetryReusedPending,
            },
        );

        assert!(
            !report
                .check("local_only_upload_retry_reuses_pending_client_mutation")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_bound_convergence_as_passed() {
        let report = evaluate_remote_observation_convergence_invariants(
            RemoteObservationConvergenceInvariantInputs {
                planned_action_present_after: false,
                pending_inode_mutation_present_after: false,
                local_dirty_after: false,
                local_content_digest_after: Some("sha256:file"),
                remote_synced_seq_after: ChangeSeq(42),
                remote_revision_no_after: RevisionNo(18),
                remote_content_digest_after: Some("sha256:file"),
                sync_anchor_seq_after: Some(ChangeSeq(42)),
                sync_anchor_revision_no_after: Some(RevisionNo(18)),
                sync_anchor_content_digest_after: Some("sha256:file"),
            },
        );

        assert!(
            report
                .check("remote_observation_convergence_clears_pending_inode_mutation")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_bound_convergence_as_failed_when_pending_survives() {
        let report = evaluate_remote_observation_convergence_invariants(
            RemoteObservationConvergenceInvariantInputs {
                planned_action_present_after: false,
                pending_inode_mutation_present_after: true,
                local_dirty_after: false,
                local_content_digest_after: Some("sha256:file"),
                remote_synced_seq_after: ChangeSeq(42),
                remote_revision_no_after: RevisionNo(18),
                remote_content_digest_after: Some("sha256:file"),
                sync_anchor_seq_after: Some(ChangeSeq(42)),
                sync_anchor_revision_no_after: Some(RevisionNo(18)),
                sync_anchor_content_digest_after: Some("sha256:file"),
            },
        );

        assert!(
            !report
                .check("remote_observation_convergence_clears_pending_inode_mutation")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_ambiguous_bind_as_passed() {
        let report = evaluate_remote_observation_ambiguous_bind_invariants(
            RemoteObservationAmbiguousBindInvariantInputs {
                issue_kind_after: Some("remote_observation_bind_ambiguous"),
                issue_matches_after: Some(2),
                remote_present_after: false,
                local_present_after: false,
                sync_anchor_present_after: false,
                surviving_local_only_count_after: 2,
                initial_local_only_count: 2,
            },
        );

        assert!(
            report
                .check("remote_observation_ambiguous_bind_records_durable_issue")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_ambiguous_bind_as_failed_when_partial_rows_exist() {
        let report = evaluate_remote_observation_ambiguous_bind_invariants(
            RemoteObservationAmbiguousBindInvariantInputs {
                issue_kind_after: Some("remote_observation_bind_ambiguous"),
                issue_matches_after: Some(2),
                remote_present_after: true,
                local_present_after: false,
                sync_anchor_present_after: false,
                surviving_local_only_count_after: 1,
                initial_local_only_count: 2,
            },
        );

        assert!(
            !report
                .check("remote_observation_ambiguous_bind_avoids_partial_migration")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_remote_only_directory_materialization_as_passed() {
        let report = evaluate_remote_only_directory_materialization_invariants(
            RemoteOnlyDirectoryMaterializationInvariantInputs {
                outcome: RemoteOnlyDirectoryMaterializationOutcomeKind::Completed,
                local_exists_on_disk_after: true,
                local_dirty_after: false,
                sync_anchor_present_after: true,
                planned_action_present_after: false,
                issue_kind_after: None,
            },
        );

        assert!(
            report
                .check("remote_only_directory_materialization_clears_planned_action")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_remote_only_directory_failure_as_failed_without_issue()
    {
        let report = evaluate_remote_only_directory_materialization_invariants(
            RemoteOnlyDirectoryMaterializationInvariantInputs {
                outcome: RemoteOnlyDirectoryMaterializationOutcomeKind::Failed,
                local_exists_on_disk_after: false,
                local_dirty_after: false,
                sync_anchor_present_after: false,
                planned_action_present_after: true,
                issue_kind_after: None,
            },
        );

        assert!(
            !report
                .check("remote_only_directory_materialization_failure_records_durable_issue")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_remote_delete_as_passed() {
        let planning_report =
            evaluate_remote_delete_planning_invariants(RemoteDeletePlanningInvariantInputs {
                planned_action_decision_after: Some("apply_remote_delete"),
                planned_action_reason_after: Some("remote_deleted_from_anchor"),
            });
        let apply_report =
            evaluate_apply_remote_delete_invariants(ApplyRemoteDeleteInvariantInputs {
                remote_present_after: true,
                remote_is_deleted_after: true,
                local_present_after: false,
                sync_anchor_present_after: false,
                planned_action_present_after: false,
                issue_kind_after: None,
            });

        assert!(
            planning_report
                .check("remote_delete_plans_apply_remote_delete")
                .expect("planning check")
                .passed
        );
        assert!(
            apply_report
                .check("apply_remote_delete_clears_local_state_and_sync_anchor")
                .expect("apply check")
                .passed
        );
    }

    #[test]
    fn reconciliation_invariant_report_marks_remote_delete_failure_without_issue_as_failed() {
        let report = evaluate_apply_remote_delete_invariants(ApplyRemoteDeleteInvariantInputs {
            remote_present_after: true,
            remote_is_deleted_after: true,
            local_present_after: true,
            sync_anchor_present_after: true,
            planned_action_present_after: true,
            issue_kind_after: None,
        });

        assert!(
            !report
                .check("apply_remote_delete_failure_records_durable_issue")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn content_invariant_report_marks_all_file_content_checks_as_passed() {
        let namespace_id = NamespaceId::new("ns-content");
        let content_bytes = b"hello from loon\n";
        let snapshot = content_snapshot(&namespace_id, content_bytes, content_bytes);

        let report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
            expected_namespace: &namespace_id,
            content: &snapshot,
        });

        assert!(
            report
                .check("content_manifest_file_digest_matches_blocks")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn content_invariant_report_marks_namespace_mismatch_as_failed() {
        let namespace_id = NamespaceId::new("ns-content");
        let other_namespace = NamespaceId::new("ns-other");
        let content_bytes = b"hello from loon\n";
        let snapshot = content_snapshot(&namespace_id, content_bytes, content_bytes);

        let report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
            expected_namespace: &other_namespace,
            content: &snapshot,
        });

        assert!(
            !report
                .check("content_manifest_namespace_matches_request")
                .expect("check")
                .passed
        );
    }

    #[test]
    fn content_invariant_report_marks_file_digest_mismatch_as_failed_when_block_bytes_drift() {
        let namespace_id = NamespaceId::new("ns-content");
        let content_bytes = b"hello from loon\n";
        let drifted_bytes = b"hello from moon\n";
        let snapshot = content_snapshot(&namespace_id, content_bytes, drifted_bytes);

        let report = evaluate_content_object_invariants(ContentObjectInvariantInputs {
            expected_namespace: &namespace_id,
            content: &snapshot,
        });

        assert!(
            !report
                .check("content_manifest_file_digest_matches_blocks")
                .expect("check")
                .passed
        );
    }

    fn simple_replace_wal_object(
        namespace_id: &NamespaceId,
        base_head_seq: ChangeSeq,
        base_revision: RevisionNo,
    ) -> StoredWalObject {
        let request = CommitRequest {
            namespace_id: namespace_id.clone(),
            request_id: "wal-req".to_owned(),
            writer_id: "writer".to_owned(),
            writer_fence_token: FenceToken(5),
            planned_head_seq: base_head_seq,
            ops: vec![CommitOp::ReplaceFile {
                inode_id: InodeId(9),
                base_revision,
                content_manifest_digest: "sha256:new".to_owned(),
            }],
            preconditions: vec![Precondition::HeadSeqIs(base_head_seq)],
        };
        let basis_head = HeadState {
            namespace_id: namespace_id.clone(),
            seq: base_head_seq,
            active_fence_token: FenceToken(5),
            next_inode_id: InodeId(20),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
        };
        let lease = LeaseState {
            namespace_id: namespace_id.clone(),
            holder_id: "writer".to_owned(),
            fence_token: FenceToken(5),
            lease_expires_at_ms: NOW_MS + 100,
        };
        let basis_metadata = MetadataState {
            inodes: vec![InodeRecord {
                inode_id: InodeId(9),
                inode_kind: InodeKind::File,
                created_seq: ChangeSeq(1),
            }],
            direntries: Vec::new(),
            revisions: vec![RevisionRecord {
                inode_id: InodeId(9),
                revision_no: base_revision,
                committed_seq: base_head_seq,
                revision_op_index: 0,
                content_manifest_digest: "sha256:old".to_owned(),
            }],
            subtree_tombstones: Vec::new(),
        };
        let plan = build_commit_plan(
            &request,
            &CommitValidationContext {
                head: basis_head,
                lease,
                now_ms: NOW_MS,
                metadata_state: basis_metadata,
            },
        )
        .expect("plan");
        let prepared = prepare_wal_commit(&request, &plan, TEST_WRITER_VERSION).expect("wal");
        StoredWalObject {
            object_key: prepared.object_key,
            encoded_bytes: prepared.encoded_bytes,
        }
    }

    fn checkpoint_fixture(
        namespace_id: &NamespaceId,
        basis_head: &HeadState,
        basis_metadata: &MetadataState,
    ) -> (StoredCheckpointManifest, Vec<StoredCheckpointSegment>) {
        let mut segments = Vec::new();
        let mut descriptors = Vec::new();
        for (family, rows) in [
            (
                CheckpointTableFamily::Inodes,
                basis_metadata
                    .inodes
                    .iter()
                    .map(|inode| CheckpointRow::Inode {
                        inode_id: inode.inode_id,
                        inode_kind: inode.inode_kind.clone(),
                        created_seq: inode.created_seq,
                    })
                    .collect::<Vec<_>>(),
            ),
            (
                CheckpointTableFamily::Direntries,
                basis_metadata
                    .direntries
                    .iter()
                    .map(|direntry| CheckpointRow::Direntry {
                        parent_inode_id: direntry.parent_inode_id,
                        name_key: direntry.name_key.clone(),
                        display_name: direntry.display_name.clone(),
                        child_inode_id: direntry.child_inode_id,
                        bind_seq: direntry.bind_seq,
                        bind_op_index: direntry.bind_op_index,
                    })
                    .collect::<Vec<_>>(),
            ),
            (
                CheckpointTableFamily::Revisions,
                basis_metadata
                    .revisions
                    .iter()
                    .map(|revision| CheckpointRow::Revision {
                        inode_id: revision.inode_id,
                        revision_no: revision.revision_no,
                        committed_seq: revision.committed_seq,
                        revision_op_index: revision.revision_op_index,
                        content_manifest_digest: revision.content_manifest_digest.clone(),
                    })
                    .collect::<Vec<_>>(),
            ),
            (
                CheckpointTableFamily::Tombstones,
                basis_metadata
                    .subtree_tombstones
                    .iter()
                    .map(|tombstone| CheckpointRow::Tombstone {
                        root_inode_id: tombstone.root_inode_id,
                        tombstone_seq: tombstone.tombstone_seq,
                        tombstone_op_index: tombstone.tombstone_op_index,
                    })
                    .collect::<Vec<_>>(),
            ),
        ] {
            let key = snapshot_table(
                namespace_id.as_str(),
                basis_head.seq.0,
                match family {
                    CheckpointTableFamily::Inodes => SnapshotTableFamily::Inodes,
                    CheckpointTableFamily::Direntries => SnapshotTableFamily::Direntries,
                    CheckpointTableFamily::Revisions => SnapshotTableFamily::Revisions,
                    CheckpointTableFamily::Tombstones => SnapshotTableFamily::Tombstones,
                },
                0,
            );
            let min_key = rows.first().map(CheckpointRow::row_key).unwrap_or_default();
            let max_key = rows.last().map(CheckpointRow::row_key).unwrap_or_default();
            let page = CheckpointPage {
                page_index: 0,
                min_key: min_key.clone(),
                max_key: max_key.clone(),
                row_keys: rows.iter().map(CheckpointRow::row_key).collect(),
                rows,
            };
            let payload = CheckpointSegmentPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_head.seq,
                family,
                segment_index: 0,
                row_count: page.rows.len() as u64,
                min_key: page.min_key.clone(),
                max_key: page.max_key.clone(),
                pages: vec![page],
            };
            let envelope =
                CheckpointSegmentEnvelope::from_payload(TEST_WRITER_VERSION, payload.clone())
                    .expect("segment envelope");
            let descriptor = CheckpointSegmentDescriptor {
                object_key: key.clone(),
                segment_index: 0,
                row_count: 1,
                min_key: payload.min_key.clone(),
                max_key: payload.max_key.clone(),
                payload_checksum_sha256: envelope.payload_checksum_sha256.clone(),
                page_checksums_sha256: envelope
                    .payload
                    .pages
                    .iter()
                    .map(checkpoint_page_checksum_sha256)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("page checksums"),
            };
            segments.push(StoredCheckpointSegment {
                object_key: key.clone(),
                encoded_bytes: encode_checkpoint_segment_envelope_zstd(&envelope)
                    .expect("encode segment"),
            });
            descriptors.push(CheckpointTableManifest {
                family,
                segments: vec![descriptor],
            });
        }

        let manifest = CheckpointManifestEnvelope::from_payload(
            TEST_WRITER_VERSION,
            CheckpointManifestPayload {
                namespace_id: namespace_id.clone(),
                checkpoint_seq: basis_head.seq,
                active_fence_token: basis_head.active_fence_token,
                next_inode_id: basis_head.next_inode_id,
                retention_floor_seq: basis_head.retention_floor_seq,
                verified: true,
                tables: descriptors,
            },
        )
        .expect("manifest envelope");

        let stored_manifest = StoredCheckpointManifest {
            object_key: snapshot_manifest(namespace_id.as_str(), basis_head.seq.0),
            encoded_bytes: encode_checkpoint_manifest_json(&manifest).expect("encode manifest"),
        };
        (stored_manifest, segments)
    }

    fn checkpoint_object_snapshot(
        source_head: &HeadState,
        source_metadata: &MetadataState,
    ) -> CheckpointObjectInvariantSnapshot {
        let (stored_manifest, stored_segments) =
            checkpoint_fixture(&source_head.namespace_id, source_head, source_metadata);
        let manifest_envelope = decode_checkpoint_manifest_json(&stored_manifest.encoded_bytes)
            .expect("decode manifest");
        let segments = stored_segments
            .iter()
            .map(|segment| StoredCheckpointSegmentSnapshot {
                object_key: segment.object_key.clone(),
                encoded_bytes: segment.encoded_bytes.clone(),
                envelope: decode_checkpoint_segment_envelope_zstd(&segment.encoded_bytes)
                    .expect("decode segment"),
            })
            .collect();

        CheckpointObjectInvariantSnapshot {
            source_head: source_head.clone(),
            source_basis_metadata: source_metadata.clone(),
            manifest_object_key: stored_manifest.object_key,
            manifest_bytes: stored_manifest.encoded_bytes,
            manifest_envelope,
            segments,
        }
    }

    fn content_snapshot(
        namespace_id: &NamespaceId,
        content_bytes: &[u8],
        stored_block_bytes: &[u8],
    ) -> ContentObjectInvariantSnapshot {
        let block_digest = sha256_digest(content_bytes);
        let manifest_envelope = ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: content_bytes.len() as u64,
            file_digest_sha256: sha256_digest(content_bytes),
            block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: block_digest.clone(),
                plaintext_size_bytes: content_bytes.len() as u64,
            }],
        })
        .expect("build content manifest envelope");
        let manifest_bytes =
            encode_content_manifest_json(&manifest_envelope).expect("encode manifest");
        let content_manifest_digest = sha256_digest(&manifest_bytes);

        ContentObjectInvariantSnapshot {
            content_manifest_digest: content_manifest_digest.clone(),
            manifest_object_key: content_manifest(namespace_id.as_str(), &content_manifest_digest),
            manifest_envelope,
            manifest_bytes,
            available_blocks: BTreeMap::from([(
                block_digest.clone(),
                StoredContentBlockSnapshot {
                    object_key: blob(namespace_id.as_str(), &block_digest),
                    bytes: stored_block_bytes.to_vec(),
                },
            )]),
        }
    }
}
