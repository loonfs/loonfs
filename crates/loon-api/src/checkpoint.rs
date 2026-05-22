use crate::digest::sha256_hex;
use crate::v0::CommitOpResult;
use crate::{
    ChangeSeq, CommitId, ContentRef, FenceToken, InodeId, InodeKind, NamespaceId, RevisionNo,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CHECKPOINT_MANIFEST_FORMAT_VERSION: u32 = 1;
pub const CHECKPOINT_SEGMENT_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointManifestKind {
    NamespaceCheckpointManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointSegmentKind {
    NamespaceCheckpointSegment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointTableFamily {
    Inodes,
    DirentryBinds,
    DirentryUnbinds,
    Revisions,
    Tombstones,
    CommitReceipts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSegmentDescriptor {
    pub object_key: String,
    pub segment_index: u32,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    pub payload_checksum_sha256: String,
    pub page_checksums_sha256: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTableManifest {
    pub family: CheckpointTableFamily,
    pub segments: Vec<CheckpointSegmentDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointPage {
    pub page_index: u32,
    pub min_key: String,
    pub max_key: String,
    #[serde(default)]
    pub row_keys: Vec<String>,
    #[serde(default)]
    pub rows: Vec<CheckpointRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "row_kind", rename_all = "snake_case")]
pub enum CheckpointRow {
    Inode {
        inode_id: InodeId,
        inode_kind: InodeKind,
        created_seq: ChangeSeq,
    },
    DirentryBind {
        parent_inode_id: InodeId,
        name_key: String,
        display_name: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    DirentryUnbind {
        parent_inode_id: InodeId,
        name_key: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
        unbind_seq: ChangeSeq,
        unbind_delta_index: u32,
    },
    Revision {
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        revision_delta_index: u32,
        content_ref: ContentRef,
    },
    Tombstone {
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
        tombstone_delta_index: u32,
    },
    CommitReceipt {
        commit_id: CommitId,
        semantic_commit_fingerprint_sha256: String,
        committed_seq: ChangeSeq,
        results: Vec<CommitOpResult>,
    },
}

impl CheckpointRow {
    pub fn row_key(&self) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::DirentryBind {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_delta_index,
                ..
            } => {
                format!(
                    "direntry-{:020}-{name_key}-{:020}-{:010}",
                    parent_inode_id.0, bind_seq.0, bind_delta_index
                )
            }
            Self::DirentryUnbind {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_delta_index,
                unbind_seq,
                unbind_delta_index,
                ..
            } => {
                format!(
                    "direntry-unbind-{:020}-{name_key}-{:020}-{:010}-{:020}-{:010}",
                    parent_inode_id.0,
                    bind_seq.0,
                    bind_delta_index,
                    unbind_seq.0,
                    unbind_delta_index
                )
            }
            Self::Revision {
                inode_id,
                revision_no,
                revision_delta_index,
                ..
            } => {
                format!(
                    "revision-{:020}-{:020}-{:010}",
                    inode_id.0, revision_no.0, revision_delta_index
                )
            }
            Self::Tombstone {
                root_inode_id,
                tombstone_seq,
                tombstone_delta_index,
            } => {
                format!(
                    "tombstone-{:020}-{:020}-{:010}",
                    root_inode_id.0, tombstone_seq.0, tombstone_delta_index
                )
            }
            Self::CommitReceipt {
                committed_seq,
                commit_id,
                ..
            } => format!("commit-receipt-{commit_id}-{:020}", committed_seq.0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSegmentPayload {
    pub namespace_id: NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub family: CheckpointTableFamily,
    pub segment_index: u32,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    pub pages: Vec<CheckpointPage>,
}

impl CheckpointSegmentPayload {
    pub fn page_checksums_sha256(&self) -> Result<Vec<String>, CheckpointSegmentCodecError> {
        self.pages
            .iter()
            .map(checkpoint_page_checksum_sha256)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSegmentEnvelope {
    pub kind: CheckpointSegmentKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub payload: CheckpointSegmentPayload,
}

impl CheckpointSegmentEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: CheckpointSegmentPayload,
    ) -> Result<Self, CheckpointSegmentCodecError> {
        Ok(Self {
            kind: CheckpointSegmentKind::NamespaceCheckpointSegment,
            format_version: CHECKPOINT_SEGMENT_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: checkpoint_segment_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, CheckpointSegmentCodecError> {
        Ok(self.payload_checksum_sha256
            == checkpoint_segment_payload_checksum_sha256(&self.payload)?)
    }

    pub fn page_checksums_sha256(&self) -> Result<Vec<String>, CheckpointSegmentCodecError> {
        self.payload.page_checksums_sha256()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifestPayload {
    pub namespace_id: NamespaceId,
    pub checkpoint_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub retention_floor_seq: ChangeSeq,
    pub verified: bool,
    pub tables: Vec<CheckpointTableManifest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifestEnvelope {
    pub kind: CheckpointManifestKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub payload: CheckpointManifestPayload,
}

impl CheckpointManifestEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: CheckpointManifestPayload,
    ) -> Result<Self, CheckpointManifestCodecError> {
        Ok(Self {
            kind: CheckpointManifestKind::NamespaceCheckpointManifest,
            format_version: CHECKPOINT_MANIFEST_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: checkpoint_manifest_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, CheckpointManifestCodecError> {
        Ok(self.payload_checksum_sha256
            == checkpoint_manifest_payload_checksum_sha256(&self.payload)?)
    }
}

#[derive(Debug, Error)]
pub enum CheckpointManifestCodecError {
    #[error("failed to encode checkpoint manifest payload to JSON: {0}")]
    PayloadEncode(String),
    #[error("failed to encode checkpoint manifest envelope to JSON: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode checkpoint manifest envelope from JSON: {0}")]
    EnvelopeDecode(String),
    #[error("checkpoint manifest payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

#[derive(Debug, Error)]
pub enum CheckpointSegmentCodecError {
    #[error("failed to encode checkpoint page to CBOR: {0}")]
    PageEncode(String),
    #[error("failed to encode checkpoint segment payload to CBOR: {0}")]
    PayloadEncode(String),
    #[error("failed to encode checkpoint segment envelope to CBOR: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode checkpoint segment envelope from CBOR: {0}")]
    EnvelopeDecode(String),
    #[error("failed to compress checkpoint segment envelope: {0}")]
    Compress(String),
    #[error("failed to decompress checkpoint segment envelope: {0}")]
    Decompress(String),
    #[error("checkpoint segment payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub fn checkpoint_manifest_payload_checksum_sha256(
    payload: &CheckpointManifestPayload,
) -> Result<String, CheckpointManifestCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| CheckpointManifestCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_hex(&bytes))
}

pub fn encode_checkpoint_manifest_json(
    envelope: &CheckpointManifestEnvelope,
) -> Result<Vec<u8>, CheckpointManifestCodecError> {
    serde_json::to_vec(envelope)
        .map_err(|err| CheckpointManifestCodecError::EnvelopeEncode(err.to_string()))
}

pub fn checkpoint_page_checksum_sha256(
    page: &CheckpointPage,
) -> Result<String, CheckpointSegmentCodecError> {
    let mut encoded = Vec::new();
    into_writer(page, &mut encoded)
        .map_err(|err| CheckpointSegmentCodecError::PageEncode(err.to_string()))?;
    Ok(sha256_hex(&encoded))
}

pub fn checkpoint_segment_payload_checksum_sha256(
    payload: &CheckpointSegmentPayload,
) -> Result<String, CheckpointSegmentCodecError> {
    Ok(sha256_hex(&encode_checkpoint_segment_payload_cbor(
        payload,
    )?))
}

pub fn encode_checkpoint_segment_payload_cbor(
    payload: &CheckpointSegmentPayload,
) -> Result<Vec<u8>, CheckpointSegmentCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| CheckpointSegmentCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

pub fn decode_checkpoint_manifest_json(
    bytes: &[u8],
) -> Result<CheckpointManifestEnvelope, CheckpointManifestCodecError> {
    let envelope: CheckpointManifestEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| CheckpointManifestCodecError::EnvelopeDecode(err.to_string()))?;
    if envelope.format_version != CHECKPOINT_MANIFEST_FORMAT_VERSION {
        return Err(CheckpointManifestCodecError::EnvelopeDecode(format!(
            "unsupported checkpoint manifest format version `{}`",
            envelope.format_version
        )));
    }
    let actual = checkpoint_manifest_payload_checksum_sha256(&envelope.payload)?;

    if actual != envelope.payload_checksum_sha256 {
        return Err(CheckpointManifestCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}

pub fn encode_checkpoint_segment_envelope_zstd(
    envelope: &CheckpointSegmentEnvelope,
) -> Result<Vec<u8>, CheckpointSegmentCodecError> {
    let mut encoded = Vec::new();
    into_writer(envelope, &mut encoded)
        .map_err(|err| CheckpointSegmentCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| CheckpointSegmentCodecError::Compress(err.to_string()))
}

pub fn decode_checkpoint_segment_envelope_zstd(
    bytes: &[u8],
) -> Result<CheckpointSegmentEnvelope, CheckpointSegmentCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| CheckpointSegmentCodecError::Decompress(err.to_string()))?;
    let envelope: CheckpointSegmentEnvelope = from_reader(decompressed.as_slice())
        .map_err(|err| CheckpointSegmentCodecError::EnvelopeDecode(err.to_string()))?;
    if envelope.format_version != CHECKPOINT_SEGMENT_FORMAT_VERSION {
        return Err(CheckpointSegmentCodecError::EnvelopeDecode(format!(
            "unsupported checkpoint segment format version `{}`",
            envelope.format_version
        )));
    }

    let actual = checkpoint_segment_payload_checksum_sha256(&envelope.payload)?;
    if actual != envelope.payload_checksum_sha256 {
        return Err(CheckpointSegmentCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}
