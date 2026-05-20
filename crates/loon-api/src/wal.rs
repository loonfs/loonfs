use crate::digest::sha256_hex;
use crate::v0::{CommitAnnotations, CommitOpResult};
use crate::{ChangeSeq, CommitId, ContentRef, FenceToken, InodeId, NamespaceId, RevisionNo};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const WAL_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    NamespaceWalSegment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOp {
    CreateDir {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        parent_inode: InodeId,
        display_name: String,
    },
    CreateFile {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        parent_inode: InodeId,
        display_name: String,
        content_ref: ContentRef,
    },
    ReplaceFile {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_ref: ContentRef,
    },
    RestoreRevision {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        source_revision_no: RevisionNo,
        base_revision: RevisionNo,
        content_ref: ContentRef,
    },
    DeleteFile {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
    },
    Rename {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        #[serde(default)]
        op_index: u32,
        root_inode: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalPrecondition {
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
pub struct WalCommitPayload {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub apply_after_seq: ChangeSeq,
    pub commit_id: CommitId,
    pub semantic_commit_fingerprint_sha256: String,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<CommitAnnotations>,
    pub ops: Vec<WalOp>,
    pub preconditions: Vec<WalPrecondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPayload {
    pub namespace_id: NamespaceId,
    pub segment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_visible_segment: Option<crate::WalSegmentPointer>,
    pub base_head_seq: ChangeSeq,
    pub start_seq: ChangeSeq,
    pub end_seq: ChangeSeq,
    pub records: Vec<WalCommitPayload>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentEnvelope {
    pub kind: WalEnvelopeKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub payload: WalSegmentPayload,
}

impl WalSegmentEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: WalSegmentPayload,
    ) -> Result<Self, WalCodecError> {
        Ok(Self {
            kind: WalEnvelopeKind::NamespaceWalSegment,
            format_version: WAL_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: wal_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, WalCodecError> {
        Ok(self.payload_checksum_sha256 == wal_payload_checksum_sha256(&self.payload)?)
    }

    pub fn pointer(&self, object_key: String) -> crate::WalSegmentPointer {
        crate::WalSegmentPointer {
            object_key,
            segment_id: self.payload.segment_id.clone(),
            start_seq: self.payload.start_seq,
            end_seq: self.payload.end_seq,
            payload_checksum_sha256: self.payload_checksum_sha256.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum WalCodecError {
    #[error("failed to encode WAL payload to CBOR: {0}")]
    PayloadEncode(String),
    #[error("failed to encode WAL envelope to CBOR: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode WAL envelope from CBOR: {0}")]
    EnvelopeDecode(String),
    #[error("failed to compress WAL envelope: {0}")]
    Compress(String),
    #[error("failed to decompress WAL envelope: {0}")]
    Decompress(String),
    #[error("unsupported WAL format version `{0}`")]
    UnsupportedFormatVersion(u32),
    #[error("WAL payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub fn wal_payload_checksum_sha256(payload: &WalSegmentPayload) -> Result<String, WalCodecError> {
    Ok(sha256_hex(&encode_wal_payload_cbor(payload)?))
}

pub fn encode_wal_payload_cbor(payload: &WalSegmentPayload) -> Result<Vec<u8>, WalCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| WalCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

pub fn encode_wal_segment_envelope_zstd(
    envelope: &WalSegmentEnvelope,
) -> Result<Vec<u8>, WalCodecError> {
    let mut encoded = Vec::new();
    into_writer(envelope, &mut encoded)
        .map_err(|err| WalCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| WalCodecError::Compress(err.to_string()))
}

pub fn decode_wal_segment_envelope_zstd(bytes: &[u8]) -> Result<WalSegmentEnvelope, WalCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| WalCodecError::Decompress(err.to_string()))?;
    let envelope: WalSegmentEnvelope = from_reader(decompressed.as_slice())
        .map_err(|err| WalCodecError::EnvelopeDecode(err.to_string()))?;

    if envelope.format_version != WAL_FORMAT_VERSION {
        return Err(WalCodecError::UnsupportedFormatVersion(
            envelope.format_version,
        ));
    }

    let actual = wal_payload_checksum_sha256(&envelope.payload)?;
    if actual != envelope.payload_checksum_sha256 {
        return Err(WalCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}
