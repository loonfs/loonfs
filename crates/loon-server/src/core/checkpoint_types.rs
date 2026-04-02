use crate::digest::sha256_hex;
use ciborium::{de::from_reader, ser::into_writer};
use loon_types::{ChangeSeq, FenceToken, InodeId, InodeKind, NamespaceId, RevisionNo};
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
    Direntries,
    Revisions,
    Tombstones,
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
    Direntry {
        parent_inode_id: InodeId,
        name_key: String,
        display_name: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        #[serde(default)]
        bind_op_index: u32,
    },
    Revision {
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        #[serde(default)]
        revision_op_index: u32,
        content_manifest_digest: String,
    },
    Tombstone {
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
        #[serde(default)]
        tombstone_op_index: u32,
    },
}

impl CheckpointRow {
    pub fn row_key(&self) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::Direntry {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_op_index,
                ..
            } => {
                if *bind_op_index == 0 {
                    format!(
                        "direntry-{:020}-{name_key}-{:020}",
                        parent_inode_id.0, bind_seq.0
                    )
                } else {
                    format!(
                        "direntry-{:020}-{name_key}-{:020}-{:010}",
                        parent_inode_id.0, bind_seq.0, bind_op_index
                    )
                }
            }
            Self::Revision {
                inode_id,
                revision_no,
                revision_op_index,
                ..
            } => {
                if *revision_op_index == 0 {
                    format!("revision-{:020}-{:020}", inode_id.0, revision_no.0)
                } else {
                    format!(
                        "revision-{:020}-{:020}-{:010}",
                        inode_id.0, revision_no.0, revision_op_index
                    )
                }
            }
            Self::Tombstone {
                root_inode_id,
                tombstone_seq,
                tombstone_op_index,
            } => {
                if *tombstone_op_index == 0 {
                    format!("tombstone-{:020}-{:020}", root_inode_id.0, tombstone_seq.0)
                } else {
                    format!(
                        "tombstone-{:020}-{:020}-{:010}",
                        root_inode_id.0, tombstone_seq.0, tombstone_op_index
                    )
                }
            }
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

    let actual = checkpoint_segment_payload_checksum_sha256(&envelope.payload)?;
    if actual != envelope.payload_checksum_sha256 {
        return Err(CheckpointSegmentCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_manifest_round_trips_through_json() {
        let payload = sample_checkpoint_manifest_payload();
        let envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload.clone())
            .expect("build manifest envelope");

        let encoded = encode_checkpoint_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_checkpoint_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(
            decoded.kind,
            CheckpointManifestKind::NamespaceCheckpointManifest
        );
        assert_eq!(decoded.format_version, CHECKPOINT_MANIFEST_FORMAT_VERSION);
        assert_eq!(decoded.payload, payload);
        assert!(decoded
            .has_valid_payload_checksum()
            .expect("recompute manifest checksum"));
    }

    #[test]
    fn checkpoint_manifest_checksum_detects_tampering() {
        let payload = sample_checkpoint_manifest_payload();
        let mut envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload)
            .expect("build manifest envelope");
        envelope.payload.checkpoint_seq = ChangeSeq(41);

        let encoded = encode_checkpoint_manifest_json(&envelope).expect("encode manifest");
        let error = decode_checkpoint_manifest_json(&encoded).expect_err("tampering should fail");

        assert!(matches!(
            error,
            CheckpointManifestCodecError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn checkpoint_manifest_checksum_helper_matches_envelope_value() {
        let payload = sample_checkpoint_manifest_payload();
        let envelope = CheckpointManifestEnvelope::from_payload("test-writer", payload)
            .expect("build manifest envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            checkpoint_manifest_payload_checksum_sha256(&envelope.payload)
                .expect("recompute manifest checksum")
        );
    }

    #[test]
    fn checkpoint_segment_round_trips_through_cbor_zstd() {
        let payload = sample_checkpoint_segment_payload();
        let envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload.clone())
            .expect("build checkpoint segment envelope");

        let encoded =
            encode_checkpoint_segment_envelope_zstd(&envelope).expect("encode checkpoint segment");
        let decoded =
            decode_checkpoint_segment_envelope_zstd(&encoded).expect("decode checkpoint segment");

        assert_eq!(
            decoded.kind,
            CheckpointSegmentKind::NamespaceCheckpointSegment
        );
        assert_eq!(decoded.format_version, CHECKPOINT_SEGMENT_FORMAT_VERSION);
        assert_eq!(decoded.payload, payload);
        assert!(decoded
            .has_valid_payload_checksum()
            .expect("recompute checkpoint segment checksum"));
        assert_eq!(
            decoded
                .page_checksums_sha256()
                .expect("compute page checksums from payload"),
            vec![checkpoint_page_checksum_sha256(&decoded.payload.pages[0])
                .expect("compute page checksum")]
        );
    }

    #[test]
    fn checkpoint_segment_checksum_detects_tampering() {
        let payload = sample_checkpoint_segment_payload();
        let mut envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload)
            .expect("build checkpoint segment envelope");
        envelope.payload.row_count = 3;

        let encoded =
            encode_checkpoint_segment_envelope_zstd(&envelope).expect("encode checkpoint segment");
        let error = decode_checkpoint_segment_envelope_zstd(&encoded)
            .expect_err("tampered checkpoint segment should fail");

        assert!(matches!(
            error,
            CheckpointSegmentCodecError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn checkpoint_segment_checksum_helper_matches_envelope_value() {
        let payload = sample_checkpoint_segment_payload();
        let envelope = CheckpointSegmentEnvelope::from_payload("test-writer", payload)
            .expect("build checkpoint segment envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            checkpoint_segment_payload_checksum_sha256(&envelope.payload)
                .expect("recompute checkpoint segment checksum")
        );
    }

    fn sample_checkpoint_manifest_payload() -> CheckpointManifestPayload {
        CheckpointManifestPayload {
            namespace_id: NamespaceId::from("ns-1"),
            checkpoint_seq: ChangeSeq(40),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            retention_floor_seq: ChangeSeq(40),
            verified: true,
            tables: vec![CheckpointTableManifest {
                family: CheckpointTableFamily::Inodes,
                segments: vec![CheckpointSegmentDescriptor {
                    object_key:
                        "namespaces/ns-1/snapshots/00000000000000000040/tables/inodes-00000.sst.zst"
                            .to_owned(),
                    segment_index: 0,
                    row_count: 500,
                    min_key: "inode-1".to_owned(),
                    max_key: "inode-500".to_owned(),
                    payload_checksum_sha256: "seg-checksum-1".to_owned(),
                    page_checksums_sha256: vec!["page-checksum-1".to_owned()],
                }],
            }],
        }
    }

    fn sample_checkpoint_segment_payload() -> CheckpointSegmentPayload {
        CheckpointSegmentPayload {
            namespace_id: NamespaceId::from("ns-1"),
            checkpoint_seq: ChangeSeq(40),
            family: CheckpointTableFamily::Inodes,
            segment_index: 0,
            row_count: 2,
            min_key: "inode-1".to_owned(),
            max_key: "inode-2".to_owned(),
            pages: vec![CheckpointPage {
                page_index: 0,
                min_key: "inode-1".to_owned(),
                max_key: "inode-2".to_owned(),
                row_keys: vec!["inode-1".to_owned(), "inode-2".to_owned()],
                rows: vec![
                    CheckpointRow::Inode {
                        inode_id: InodeId(1),
                        inode_kind: InodeKind::Dir,
                        created_seq: ChangeSeq(1),
                    },
                    CheckpointRow::Inode {
                        inode_id: InodeId(2),
                        inode_kind: InodeKind::File,
                        created_seq: ChangeSeq(2),
                    },
                ],
            }],
        }
    }
}
