#![forbid(unsafe_code)]

use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt::{self, Write as _};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NamespaceId(pub String);

impl NamespaceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for NamespaceId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for NamespaceId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl Serialize for NamespaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for NamespaceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(String::deserialize(deserializer)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct InodeId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RevisionNo(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChangeSeq(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FenceToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NameKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InodeKind {
    File,
    Dir,
    Symlink,
    Mount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictDisposition {
    KeepRequestedName,
    RenameLoser { deterministic_suffix: String },
    ConflictCopy { deterministic_suffix: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

pub const CONTROL_OBJECT_FORMAT_VERSION: u32 = 1;
pub const WAL_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceHead,
    NamespaceLease,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadState {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    pub snapshot_hint_seq: Option<ChangeSeq>,
    pub retention_floor_seq: ChangeSeq,
}

impl HeadState {
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            seq: ChangeSeq(0),
            active_fence_token: FenceToken(0),
            next_inode_id: InodeId(1),
            snapshot_hint_seq: None,
            retention_floor_seq: ChangeSeq(0),
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

pub fn payload_checksum_sha256<T>(value: &T) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    Ok(sha256_hex(&bytes))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    NamespaceWalCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOp {
    ReplaceFile {
        inode_id: InodeId,
        base_revision: RevisionNo,
        content_manifest_digest: String,
    },
    Rename {
        inode_id: InodeId,
        new_parent_inode: InodeId,
        new_display_name: String,
    },
    DeleteSubtree {
        root_inode: InodeId,
    },
    RestoreRevision {
        inode_id: InodeId,
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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);

    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String should not fail");
    }

    encoded
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "inode-{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_wal_commit_envelope_zstd, encode_wal_commit_envelope_zstd, payload_checksum_sha256,
        wal_payload_checksum_sha256, ChangeSeq, ControlObjectEnvelope, ControlObjectKind,
        FenceToken, HeadState, InodeId, LeaseState, NamespaceId, RevisionNo, WalCodecError,
        WalCommitEnvelope, WalCommitPayload, WalOp, WalPrecondition, CONTROL_OBJECT_FORMAT_VERSION,
        WAL_FORMAT_VERSION,
    };

    #[test]
    fn namespace_id_serializes_as_string() {
        let namespace_id = NamespaceId::from("ns-home");
        let json = serde_json::to_string(&namespace_id).expect("serialize namespace id");
        let round_trip: NamespaceId =
            serde_json::from_str(&json).expect("deserialize namespace id");

        assert_eq!(json, "\"ns-home\"");
        assert_eq!(round_trip, namespace_id);
    }

    #[test]
    fn control_object_envelope_computes_payload_checksum() {
        let state = HeadState {
            namespace_id: NamespaceId::from("ns-1"),
            seq: ChangeSeq(41),
            active_fence_token: FenceToken(8),
            next_inode_id: InodeId(501),
            snapshot_hint_seq: Some(ChangeSeq(40)),
            retention_floor_seq: ChangeSeq(40),
        };

        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
            "test-writer",
            state,
        )
        .expect("build envelope");

        assert_eq!(envelope.kind, ControlObjectKind::NamespaceHead);
        assert_eq!(envelope.format_version, CONTROL_OBJECT_FORMAT_VERSION);
        assert!(envelope
            .has_valid_payload_checksum()
            .expect("recompute payload checksum"));
    }

    #[test]
    fn lease_state_expiration_is_explicit() {
        let lease = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        assert!(lease.is_valid_at(999));
        assert!(!lease.is_valid_at(1_000));
    }

    #[test]
    fn checksum_helper_matches_envelope_value() {
        let state = LeaseState {
            namespace_id: NamespaceId::from("ns-1"),
            holder_id: "writer-a".to_owned(),
            fence_token: FenceToken(8),
            lease_expires_at_ms: 1_000,
        };

        let envelope = ControlObjectEnvelope::from_state(
            ControlObjectKind::NamespaceLease,
            "test-writer",
            state,
        )
        .expect("build envelope");

        assert_eq!(
            envelope.payload_checksum_sha256,
            payload_checksum_sha256(&envelope.state).expect("recompute checksum")
        );
    }

    #[test]
    fn wal_commit_envelope_round_trips_through_cbor_zstd() {
        let payload = sample_wal_payload();
        let envelope = WalCommitEnvelope::from_payload("test-writer", payload.clone())
            .expect("build envelope");

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
            WalCommitEnvelope::from_payload("test-writer", payload).expect("build envelope");

        envelope.payload.seq = ChangeSeq(43);
        let encoded = encode_wal_commit_envelope_zstd(&envelope).expect("encode WAL envelope");
        let error = decode_wal_commit_envelope_zstd(&encoded).expect_err("tampering should fail");

        assert!(matches!(error, WalCodecError::ChecksumMismatch { .. }));
    }

    #[test]
    fn wal_checksum_helper_matches_envelope_value() {
        let payload = sample_wal_payload();
        let envelope =
            WalCommitEnvelope::from_payload("test-writer", payload).expect("build envelope");

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
