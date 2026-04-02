use crate::digest::sha256_hex;
use ciborium::{de::from_reader, ser::into_writer};
use loon_types::{ChangeSeq, FenceToken, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const WAL_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    NamespaceWalCommit,
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
        content_manifest_digest: String,
    },
    ReplaceFile {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
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
    RestoreRevision {
        #[serde(default)]
        op_index: u32,
        inode_id: InodeId,
        base_revision: RevisionNo,
        restore_from_revision: RevisionNo,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalPrecondition {
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
pub struct WalCommitPayload {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub base_head_seq: ChangeSeq,
    pub commit_id: String,
    pub request_id: String,
    pub writer_id: String,
    pub writer_fence_token: FenceToken,
    pub ops: Vec<WalOp>,
    pub preconditions: Vec<WalPrecondition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCommitEnvelope {
    pub kind: WalEnvelopeKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
    pub payload: WalCommitPayload,
}

impl WalCommitEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: WalCommitPayload,
    ) -> Result<Self, WalCodecError> {
        Ok(Self {
            kind: WalEnvelopeKind::NamespaceWalCommit,
            format_version: WAL_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum_sha256: wal_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, WalCodecError> {
        Ok(self.payload_checksum_sha256 == wal_payload_checksum_sha256(&self.payload)?)
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
    #[error("WAL payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub fn wal_payload_checksum_sha256(payload: &WalCommitPayload) -> Result<String, WalCodecError> {
    Ok(sha256_hex(&encode_wal_payload_cbor(payload)?))
}

pub fn encode_wal_payload_cbor(payload: &WalCommitPayload) -> Result<Vec<u8>, WalCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| WalCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

pub fn encode_wal_commit_envelope_zstd(
    envelope: &WalCommitEnvelope,
) -> Result<Vec<u8>, WalCodecError> {
    let mut encoded = Vec::new();
    into_writer(envelope, &mut encoded)
        .map_err(|err| WalCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| WalCodecError::Compress(err.to_string()))
}

pub fn decode_wal_commit_envelope_zstd(bytes: &[u8]) -> Result<WalCommitEnvelope, WalCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| WalCodecError::Decompress(err.to_string()))?;
    let envelope: WalCommitEnvelope = from_reader(decompressed.as_slice())
        .map_err(|err| WalCodecError::EnvelopeDecode(err.to_string()))?;

    let actual = wal_payload_checksum_sha256(&envelope.payload)?;
    if actual != envelope.payload_checksum_sha256 {
        return Err(WalCodecError::ChecksumMismatch {
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
    fn wal_commit_envelope_round_trips_through_cbor_zstd() {
        let payload = sample_wal_payload();
        let envelope = WalCommitEnvelope::from_payload("test-writer", payload.clone())
            .expect("build WAL envelope");

        let encoded = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");
        let decoded = decode_wal_commit_envelope_zstd(&encoded).expect("decode WAL envelope");

        assert_eq!(decoded.kind, envelope.kind);
        assert_eq!(decoded.format_version, WAL_FORMAT_VERSION);
        assert_eq!(decoded.payload, payload);
        assert!(decoded
            .has_valid_payload_checksum()
            .expect("recompute payload checksum"));
    }

    #[test]
    fn wal_payload_checksum_detects_tampering() {
        let payload = sample_wal_payload();
        let mut envelope =
            WalCommitEnvelope::from_payload("test-writer", payload).expect("build WAL envelope");

        envelope.payload.seq = ChangeSeq(43);
        let encoded = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");
        let error =
            decode_wal_commit_envelope_zstd(&encoded).expect_err("tampering should fail");

        assert!(matches!(error, WalCodecError::ChecksumMismatch { .. }));
    }

    #[test]
    fn wal_checksum_helper_matches_envelope_value() {
        let payload = sample_wal_payload();
        let envelope =
            WalCommitEnvelope::from_payload("test-writer", payload).expect("build WAL envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            wal_payload_checksum_sha256(&envelope.payload).expect("recompute checksum")
        );
    }

    fn sample_wal_payload() -> WalCommitPayload {
        WalCommitPayload {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(42),
            base_head_seq: ChangeSeq(41),
            commit_id: "req-20260311-0001".to_owned(),
            request_id: "req-20260311-0001".to_owned(),
            writer_id: "writer-a".to_owned(),
            writer_fence_token: FenceToken(8),
            ops: vec![WalOp::ReplaceFile {
                op_index: 0,
                inode_id: InodeId(42),
                base_revision: RevisionNo(7),
                content_manifest_digest: "sha256:manifest".to_owned(),
            }],
            preconditions: vec![
                WalPrecondition::HeadSeqIs(ChangeSeq(41)),
                WalPrecondition::InodeRevisionIs {
                    inode_id: InodeId(42),
                    revision: RevisionNo(7),
                },
            ],
        }
    }
}
