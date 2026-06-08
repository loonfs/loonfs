use crate::digest::sha256_hex;
use crate::{
    ChangeSeq, CommitId, ContentRef, ContentStoreId, FenceToken, InodeId, ManifestId, NamePolicy,
    NamespaceId,
};
use serde::{Deserialize, Serialize};

pub const CONTROL_OBJECT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceDescriptor,
    ContentStoreDescriptor,
    NamespaceHead,
    NamespaceLease,
    NamespaceForkState,
    NamespaceGcPinState,
    NamespaceProgress,
    UploadSession,
    QueueShard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDescriptorState {
    pub namespace_id: NamespaceId,
    pub content_store_id: ContentStoreId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreDescriptorState {
    pub content_store_id: ContentStoreId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceForkState {
    pub namespace_id: NamespaceId,
    pub source_namespace_id: NamespaceId,
    pub fork_seq: ChangeSeq,
    pub source_checkpoint_id: String,
    pub source_manifest_id: ManifestId,
    pub source_head_seq: ChangeSeq,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceGcPinState {
    pub pin_id: String,
    pub source_namespace_id: NamespaceId,
    pub target_namespace_id: NamespaceId,
    pub source_checkpoint_id: String,
    pub source_manifest_id: ManifestId,
    pub source_head_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub referenced_metadata_files_debug: Vec<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPointer {
    pub object_key: String,
    pub segment_id: String,
    pub start_seq: ChangeSeq,
    pub end_seq: ChangeSeq,
    pub payload_checksum_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadState {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    #[serde(default)]
    pub name_policy: NamePolicy,
    /// Live read/recovery pointer. If present, basis reconstruction loads this
    /// manifest before replaying the visible WAL tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_manifest_id: Option<ManifestId>,
    /// Admin/provenance convenience pointer to the latest checkpoint record.
    /// Reads do not use this as authority; checkpoints pin manifests for
    /// retention, forks, stable reads, and restore workflows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_id: Option<String>,
    pub retention_floor_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_wal_tip: Option<WalSegmentPointer>,
}

impl HeadState {
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            seq: ChangeSeq(0),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000000")
                .expect("genesis commit id is valid"),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(2),
            name_policy: NamePolicy::default(),
            current_manifest_id: None,
            latest_checkpoint_id: None,
            retention_floor_seq: ChangeSeq(0),
            visible_wal_tip: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseState {
    pub namespace_id: NamespaceId,
    pub holder_id: String,
    pub fence_token: FenceToken,
    pub lease_expires_at_ms: u64,
}

impl LeaseState {
    pub fn is_valid_at(&self, now_ms: u64) -> bool {
        self.lease_expires_at_ms > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressState {
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedUpload {
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSessionState {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_ref: Option<ContentRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<CompletedUpload>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectEnvelope<T> {
    pub kind: ControlObjectKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub state: T,
}

impl<T> ControlObjectEnvelope<T>
where
    T: Serialize,
{
    pub fn from_state(
        kind: ControlObjectKind,
        writer_version: impl Into<String>,
        state: T,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self {
            kind,
            format_version: CONTROL_OBJECT_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: payload_checksum_sha256(&state)?,
            state,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, serde_json::Error> {
        Ok(self.payload_checksum_sha256 == payload_checksum_sha256(&self.state)?)
    }
}

pub type HeadStateEnvelope = ControlObjectEnvelope<HeadState>;
pub type LeaseStateEnvelope = ControlObjectEnvelope<LeaseState>;
pub type ProgressStateEnvelope = ControlObjectEnvelope<ProgressState>;
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
pub type NamespaceDescriptorEnvelope = ControlObjectEnvelope<NamespaceDescriptorState>;
pub type ContentStoreDescriptorEnvelope = ControlObjectEnvelope<ContentStoreDescriptorState>;
pub type NamespaceForkStateEnvelope = ControlObjectEnvelope<NamespaceForkState>;
pub type NamespaceGcPinStateEnvelope = ControlObjectEnvelope<NamespaceGcPinState>;

pub fn payload_checksum_sha256<T>(value: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    Ok(sha256_hex(&bytes))
}
