use crate::control::WalSegmentPointer;
use crate::digest::sha256_digest;
use crate::envelope::EnvelopeProbe;
use crate::v0::{CommitAnnotations, CommitOpResult};
use crate::{
    ChangeSeq, CommitId, ContentRef, FenceToken, InodeId, InodeKind, NamespaceId, RevisionNo,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Version 2: the durable document carries the payload as an opaque CBOR byte
/// string checksummed over those exact bytes, and delta/precondition tags use
/// the snake_case names from spec 050 §5–6. (Version 1 verified checksums by
/// re-encoding the decoded payload and stored Rust enum identifiers; v1
/// objects are rejected with `UnsupportedFormatVersion`.)
pub const WAL_FORMAT_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    NamespaceWalSegment,
}

impl WalEnvelopeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceWalSegment => "namespace_wal_segment",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "delta", rename_all = "snake_case")]
pub enum WalDelta {
    CreateInode {
        delta_index: u32,
        inode_id: InodeId,
        inode_kind: InodeKind,
    },
    BindDirentry {
        delta_index: u32,
        parent_inode: InodeId,
        name_key: String,
        display_name: String,
        child_inode: InodeId,
    },
    UnbindDirentry {
        delta_index: u32,
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    AppendFileRevision {
        delta_index: u32,
        inode_id: InodeId,
        revision_no: RevisionNo,
        content_ref: ContentRef,
    },
    TombstoneSubtree {
        delta_index: u32,
        root_inode: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalPrecondition {
    InodeRevisionIs {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    AncestorsNotSubtreeDeleted {
        inode_id: InodeId,
    },
    ChildNameAbsent {
        parent_inode: InodeId,
        name_key: String,
    },
    BindingIs {
        parent_inode: InodeId,
        name_key: String,
        child_inode: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    DirectoryEmpty {
        inode_id: InodeId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCommitDelta {
    pub semantic_op_index: u32,
    pub delta: WalDelta,
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
    pub deltas: Vec<WalCommitDelta>,
    pub preconditions: Vec<WalPrecondition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<CommitOpResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPayload {
    pub namespace_id: NamespaceId,
    pub segment_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_visible_segment: Option<WalSegmentPointer>,
    pub base_head_seq: ChangeSeq,
    pub start_seq: ChangeSeq,
    pub end_seq: ChangeSeq,
    pub records: Vec<WalCommitPayload>,
}

/// In-memory view of a WAL segment envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_wal_segment_envelope_zstd`] and validated only by
/// [`decode_wal_segment_envelope_zstd`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentEnvelope {
    pub kind: WalEnvelopeKind,
    pub format_version: u32,
    pub writer_version: String,
    /// Digest of the encoded payload bytes exactly as stored in the durable
    /// document, in `sha256:<hex>` form.
    pub payload_checksum: String,
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
            payload_checksum: wal_payload_checksum(&payload)?,
            payload,
        })
    }

    pub fn pointer(&self, object_key: String) -> WalSegmentPointer {
        WalSegmentPointer {
            object_key,
            segment_id: self.payload.segment_id.clone(),
            start_seq: self.payload.start_seq,
            end_seq: self.payload.end_seq,
            payload_checksum: self.payload_checksum.clone(),
        }
    }
}

/// Durable layout of a WAL segment object (before zstd compression): the
/// envelope fields plus the payload as an opaque CBOR byte string.
/// `payload_checksum` covers exactly those bytes, so integrity verification
/// never depends on re-encoding the payload with this build's schema and a
/// payload with unknown additive fields still verifies.
#[derive(Serialize, Deserialize)]
struct WalSegmentDocument {
    kind: String,
    format_version: u32,
    writer_version: String,
    payload_checksum: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum WalCodecError {
    #[error("failed to encode WAL payload to CBOR: {0}")]
    PayloadEncode(String),
    #[error("failed to encode WAL envelope to CBOR: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode WAL envelope from CBOR: {0}")]
    EnvelopeDecode(String),
    #[error("failed to decode WAL payload from CBOR: {0}")]
    PayloadDecode(String),
    #[error("failed to compress WAL envelope: {0}")]
    Compress(String),
    #[error("failed to decompress WAL envelope: {0}")]
    Decompress(String),
    #[error("unexpected WAL envelope kind `{found}`: expected `{expected}`")]
    UnexpectedKind { expected: String, found: String },
    #[error("unsupported WAL format version `{found}`: this build supports `{supported}`")]
    UnsupportedFormatVersion { found: u32, supported: u32 },
    #[error("WAL payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error(
        "WAL envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope with `WalSegmentEnvelope::from_payload`"
    )]
    StalePayloadChecksum { checksum: String, actual: String },
}

pub fn wal_payload_checksum(payload: &WalSegmentPayload) -> Result<String, WalCodecError> {
    Ok(sha256_digest(&encode_wal_payload_cbor(payload)?))
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
    if envelope.format_version != WAL_FORMAT_VERSION {
        return Err(WalCodecError::UnsupportedFormatVersion {
            found: envelope.format_version,
            supported: WAL_FORMAT_VERSION,
        });
    }
    let payload_bytes = encode_wal_payload_cbor(&envelope.payload)?;
    let actual = sha256_digest(&payload_bytes);
    if actual != envelope.payload_checksum {
        return Err(WalCodecError::StalePayloadChecksum {
            checksum: envelope.payload_checksum.clone(),
            actual,
        });
    }

    let document = WalSegmentDocument {
        kind: envelope.kind.as_str().to_owned(),
        format_version: envelope.format_version,
        writer_version: envelope.writer_version.clone(),
        payload_checksum: envelope.payload_checksum.clone(),
        payload: payload_bytes,
    };
    let mut encoded = Vec::new();
    into_writer(&document, &mut encoded)
        .map_err(|err| WalCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| WalCodecError::Compress(err.to_string()))
}

pub fn decode_wal_segment_envelope_zstd(bytes: &[u8]) -> Result<WalSegmentEnvelope, WalCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| WalCodecError::Decompress(err.to_string()))?;
    let probe: EnvelopeProbe = from_reader(decompressed.as_slice())
        .map_err(|err| WalCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = WalEnvelopeKind::NamespaceWalSegment;
    if probe.kind != expected_kind.as_str() {
        return Err(WalCodecError::UnexpectedKind {
            expected: expected_kind.as_str().to_owned(),
            found: probe.kind,
        });
    }
    if probe.format_version != WAL_FORMAT_VERSION {
        return Err(WalCodecError::UnsupportedFormatVersion {
            found: probe.format_version,
            supported: WAL_FORMAT_VERSION,
        });
    }

    let document: WalSegmentDocument = from_reader(decompressed.as_slice())
        .map_err(|err| WalCodecError::EnvelopeDecode(err.to_string()))?;
    let actual = sha256_digest(&document.payload);
    if actual != document.payload_checksum {
        return Err(WalCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual,
        });
    }
    let payload: WalSegmentPayload = from_reader(document.payload.as_slice())
        .map_err(|err| WalCodecError::PayloadDecode(err.to_string()))?;

    Ok(WalSegmentEnvelope {
        kind: expected_kind,
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}
