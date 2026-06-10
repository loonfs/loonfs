use crate::digest::sha256_digest;
use crate::envelope::EnvelopeProbe;
use crate::v0::CommitOpResult;
use crate::{
    ChangeSeq, CommitId, ContentRef, FenceToken, InodeId, InodeKind, ManifestId, NamePolicy,
    NamespaceId, RevisionNo,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use thiserror::Error;

/// Version 1: an uncompressed JSON envelope document carrying the payload as
/// a raw JSON fragment. `payload_checksum` covers the fragment's exact bytes.
pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 1;
/// Version 1: a zstd-compressed CBOR envelope document carrying the payload
/// as an opaque CBOR byte string. `payload_checksum` covers exactly those
/// bytes; pages are not independently stored, so the payload checksum is the
/// unit of integrity.
pub const METADATA_SST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    NamespaceManifest,
}

impl NamespaceManifestKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceManifest => "namespace_manifest",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataSstKind {
    MetadataSst,
}

impl MetadataSstKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataSst => "metadata_sst",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTableFamily {
    Inodes,
    DirentryBinds,
    DirentryChildBinds,
    DirentryUnbinds,
    Revisions,
    Tombstones,
    CommitReceipts,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataSegmentKey {
    Full,
    DirentryParent { parent_inode_id: InodeId },
    RowKeyRange { shard: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFileRef {
    pub owner_namespace_id: NamespaceId,
    pub table_id: String,
    pub object_key: String,
    pub run_seq: ChangeSeq,
    pub level: u32,
    pub family: MetadataTableFamily,
    pub segment_index: u32,
    pub segment_key: MetadataSegmentKey,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    /// Checksum of the referenced SST's payload bytes, in `sha256:<hex>`
    /// form. Must equal the `payload_checksum` in the referenced envelope.
    pub payload_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestFork {
    pub source_namespace_id: NamespaceId,
    pub fork_seq: ChangeSeq,
    pub source_checkpoint_id: String,
    pub source_manifest_id: ManifestId,
    pub source_head_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceCheckpointRecord {
    pub checkpoint_id: String,
    pub manifest_id: ManifestId,
    pub head_seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataPage {
    pub page_index: u32,
    pub min_key: String,
    pub max_key: String,
    #[serde(default)]
    pub row_keys: Vec<String>,
    #[serde(default)]
    pub rows: Vec<MetadataRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "row_kind", rename_all = "snake_case")]
pub enum MetadataRow {
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
        semantic_commit_fingerprint: String,
        committed_seq: ChangeSeq,
        results: Vec<CommitOpResult>,
    },
}

impl MetadataRow {
    pub fn row_key(&self) -> String {
        self.row_key_for_family(match self {
            Self::Inode { .. } => MetadataTableFamily::Inodes,
            Self::DirentryBind { .. } => MetadataTableFamily::DirentryBinds,
            Self::DirentryUnbind { .. } => MetadataTableFamily::DirentryUnbinds,
            Self::Revision { .. } => MetadataTableFamily::Revisions,
            Self::Tombstone { .. } => MetadataTableFamily::Tombstones,
            Self::CommitReceipt { .. } => MetadataTableFamily::CommitReceipts,
        })
    }

    pub fn row_key_for_family(&self, family: MetadataTableFamily) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::DirentryBind {
                parent_inode_id,
                name_key,
                child_inode_id,
                bind_seq,
                bind_delta_index,
                ..
            } => match family {
                MetadataTableFamily::DirentryChildBinds => {
                    let name_key = hex_encode_row_key_component(name_key);
                    format!(
                        "direntry-child-{:020}-{:020}-{:010}-{:020}-{name_key}",
                        child_inode_id.0, bind_seq.0, bind_delta_index, parent_inode_id.0
                    )
                }
                _ => {
                    let name_key = hex_encode_row_key_component(name_key);
                    format!(
                        "direntry-{:020}-{name_key}-{:020}-{:010}",
                        parent_inode_id.0, bind_seq.0, bind_delta_index
                    )
                }
            },
            Self::DirentryUnbind {
                parent_inode_id,
                name_key,
                bind_seq,
                bind_delta_index,
                unbind_seq,
                unbind_delta_index,
                ..
            } => {
                let name_key = hex_encode_row_key_component(name_key);
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
            } => {
                let commit_id = hex_encode_row_key_component(commit_id.as_str());
                format!("commit-receipt-{commit_id}-{:020}", committed_seq.0)
            }
        }
    }
}

pub fn hex_encode_row_key_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSstPayload {
    pub namespace_id: NamespaceId,
    pub table_id: String,
    pub run_seq: ChangeSeq,
    pub level: u32,
    pub family: MetadataTableFamily,
    pub segment_index: u32,
    pub segment_key: MetadataSegmentKey,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    pub pages: Vec<MetadataPage>,
}

/// In-memory view of a metadata SST envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_metadata_sst_envelope_zstd`] and validated only by
/// [`decode_metadata_sst_envelope_zstd`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSstEnvelope {
    pub kind: MetadataSstKind,
    pub format_version: u32,
    pub writer_version: String,
    /// Digest of the encoded payload bytes exactly as stored in the durable
    /// document, in `sha256:<hex>` form.
    pub payload_checksum: String,
    pub payload: MetadataSstPayload,
}

impl MetadataSstEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: MetadataSstPayload,
    ) -> Result<Self, MetadataSstCodecError> {
        Ok(Self {
            kind: MetadataSstKind::MetadataSst,
            format_version: METADATA_SST_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum: metadata_sst_payload_checksum(&payload)?,
            payload,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestPayload {
    pub namespace_id: NamespaceId,
    pub manifest_id: ManifestId,
    pub head_seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub base_seq: ChangeSeq,
    pub active_fence_token: FenceToken,
    pub next_inode_id: InodeId,
    #[serde(default)]
    pub name_policy: NamePolicy,
    pub retention_floor_seq: ChangeSeq,
    pub initialized: bool,
    pub verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<NamespaceManifestFork>,
    #[serde(default)]
    pub checkpoints: Vec<NamespaceCheckpointRecord>,
    /// Per-namespace capabilities materialized on this file-set version,
    /// such as derived indexes (format spec, "Namespace features map").
    /// Values are feature-owned JSON objects; readers ignore unknown keys.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub features: BTreeMap<String, serde_json::Value>,
    // TODO: split this flat list into structured run/table/index roots when
    // compaction and GC need richer reachability decisions.
    pub metadata_files: Vec<MetadataFileRef>,
}

/// In-memory view of a namespace manifest envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_namespace_manifest_json`] and validated only by
/// [`decode_namespace_manifest_json`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestEnvelope {
    pub kind: NamespaceManifestKind,
    pub format_version: u32,
    pub writer_version: String,
    /// Digest of the payload JSON exactly as stored in the durable document,
    /// in `sha256:<hex>` form.
    pub payload_checksum: String,
    pub payload: NamespaceManifestPayload,
}

impl NamespaceManifestEnvelope {
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: NamespaceManifestPayload,
    ) -> Result<Self, NamespaceManifestCodecError> {
        Ok(Self {
            kind: NamespaceManifestKind::NamespaceManifest,
            format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum: namespace_manifest_payload_checksum(&payload)?,
            payload,
        })
    }
}

/// Durable layout of a namespace manifest object: the envelope fields plus
/// the payload as a raw JSON fragment. Keeping the payload inline (rather
/// than as encoded bytes) preserves the operational property that manifests
/// are directly readable JSON, while `payload_checksum` still covers the
/// exact fragment bytes as stored.
#[derive(Serialize, Deserialize)]
struct NamespaceManifestDocument {
    kind: String,
    format_version: u32,
    writer_version: String,
    payload_checksum: String,
    payload: Box<RawValue>,
}

/// Durable layout of a metadata SST object (before zstd compression), with
/// the payload as an opaque CBOR byte string. See [`WAL_FORMAT_VERSION`] for
/// the shared envelope rationale.
///
/// [`WAL_FORMAT_VERSION`]: crate::wal::WAL_FORMAT_VERSION
#[derive(Serialize, Deserialize)]
struct MetadataSstDocument {
    kind: String,
    format_version: u32,
    writer_version: String,
    payload_checksum: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum NamespaceManifestCodecError {
    #[error("failed to encode namespace manifest payload to JSON: {0}")]
    PayloadEncode(String),
    #[error("failed to encode namespace manifest envelope to JSON: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode namespace manifest envelope from JSON: {0}")]
    EnvelopeDecode(String),
    #[error("failed to decode namespace manifest payload from JSON: {0}")]
    PayloadDecode(String),
    #[error("unexpected namespace manifest kind `{found}`: expected `{expected}`")]
    UnexpectedKind { expected: String, found: String },
    #[error("unsupported namespace manifest format version `{found}`: this build supports `{supported}`")]
    UnsupportedFormatVersion { found: u32, supported: u32 },
    #[error("namespace manifest payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error(
        "namespace manifest envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope with `NamespaceManifestEnvelope::from_payload`"
    )]
    StalePayloadChecksum { checksum: String, actual: String },
}

#[derive(Debug, Error)]
pub enum MetadataSstCodecError {
    #[error("failed to encode metadata SST payload to CBOR: {0}")]
    PayloadEncode(String),
    #[error("failed to encode metadata SST envelope to CBOR: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode metadata SST envelope from CBOR: {0}")]
    EnvelopeDecode(String),
    #[error("failed to decode metadata SST payload from CBOR: {0}")]
    PayloadDecode(String),
    #[error("failed to compress metadata SST envelope: {0}")]
    Compress(String),
    #[error("failed to decompress metadata SST envelope: {0}")]
    Decompress(String),
    #[error("unexpected metadata SST kind `{found}`: expected `{expected}`")]
    UnexpectedKind { expected: String, found: String },
    #[error(
        "unsupported metadata SST format version `{found}`: this build supports `{supported}`"
    )]
    UnsupportedFormatVersion { found: u32, supported: u32 },
    #[error("metadata SST payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error(
        "metadata SST envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope with `MetadataSstEnvelope::from_payload`"
    )]
    StalePayloadChecksum { checksum: String, actual: String },
}

pub fn namespace_manifest_payload_checksum(
    payload: &NamespaceManifestPayload,
) -> Result<String, NamespaceManifestCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| NamespaceManifestCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_digest(&bytes))
}

pub fn encode_namespace_manifest_json(
    envelope: &NamespaceManifestEnvelope,
) -> Result<Vec<u8>, NamespaceManifestCodecError> {
    if envelope.format_version != NAMESPACE_MANIFEST_FORMAT_VERSION {
        return Err(NamespaceManifestCodecError::UnsupportedFormatVersion {
            found: envelope.format_version,
            supported: NAMESPACE_MANIFEST_FORMAT_VERSION,
        });
    }
    let payload_json = serde_json::to_string(&envelope.payload)
        .map_err(|err| NamespaceManifestCodecError::PayloadEncode(err.to_string()))?;
    let actual = sha256_digest(payload_json.as_bytes());
    if actual != envelope.payload_checksum {
        return Err(NamespaceManifestCodecError::StalePayloadChecksum {
            checksum: envelope.payload_checksum.clone(),
            actual,
        });
    }

    let document = NamespaceManifestDocument {
        kind: envelope.kind.as_str().to_owned(),
        format_version: envelope.format_version,
        writer_version: envelope.writer_version.clone(),
        payload_checksum: envelope.payload_checksum.clone(),
        payload: RawValue::from_string(payload_json)
            .map_err(|err| NamespaceManifestCodecError::PayloadEncode(err.to_string()))?,
    };
    serde_json::to_vec(&document)
        .map_err(|err| NamespaceManifestCodecError::EnvelopeEncode(err.to_string()))
}

pub fn decode_namespace_manifest_json(
    bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, NamespaceManifestCodecError> {
    let probe: EnvelopeProbe = serde_json::from_slice(bytes)
        .map_err(|err| NamespaceManifestCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = NamespaceManifestKind::NamespaceManifest;
    if probe.kind != expected_kind.as_str() {
        return Err(NamespaceManifestCodecError::UnexpectedKind {
            expected: expected_kind.as_str().to_owned(),
            found: probe.kind,
        });
    }
    if probe.format_version != NAMESPACE_MANIFEST_FORMAT_VERSION {
        return Err(NamespaceManifestCodecError::UnsupportedFormatVersion {
            found: probe.format_version,
            supported: NAMESPACE_MANIFEST_FORMAT_VERSION,
        });
    }

    let document: NamespaceManifestDocument = serde_json::from_slice(bytes)
        .map_err(|err| NamespaceManifestCodecError::EnvelopeDecode(err.to_string()))?;
    let actual = sha256_digest(document.payload.get().as_bytes());
    if actual != document.payload_checksum {
        return Err(NamespaceManifestCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual,
        });
    }
    let payload: NamespaceManifestPayload = serde_json::from_str(document.payload.get())
        .map_err(|err| NamespaceManifestCodecError::PayloadDecode(err.to_string()))?;

    Ok(NamespaceManifestEnvelope {
        kind: expected_kind,
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}

pub fn metadata_sst_payload_checksum(
    payload: &MetadataSstPayload,
) -> Result<String, MetadataSstCodecError> {
    Ok(sha256_digest(&encode_metadata_sst_payload_cbor(payload)?))
}

pub fn encode_metadata_sst_payload_cbor(
    payload: &MetadataSstPayload,
) -> Result<Vec<u8>, MetadataSstCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| MetadataSstCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

pub fn encode_metadata_sst_envelope_zstd(
    envelope: &MetadataSstEnvelope,
) -> Result<Vec<u8>, MetadataSstCodecError> {
    if envelope.format_version != METADATA_SST_FORMAT_VERSION {
        return Err(MetadataSstCodecError::UnsupportedFormatVersion {
            found: envelope.format_version,
            supported: METADATA_SST_FORMAT_VERSION,
        });
    }
    let payload_bytes = encode_metadata_sst_payload_cbor(&envelope.payload)?;
    let actual = sha256_digest(&payload_bytes);
    if actual != envelope.payload_checksum {
        return Err(MetadataSstCodecError::StalePayloadChecksum {
            checksum: envelope.payload_checksum.clone(),
            actual,
        });
    }

    let document = MetadataSstDocument {
        kind: envelope.kind.as_str().to_owned(),
        format_version: envelope.format_version,
        writer_version: envelope.writer_version.clone(),
        payload_checksum: envelope.payload_checksum.clone(),
        payload: payload_bytes,
    };
    let mut encoded = Vec::new();
    into_writer(&document, &mut encoded)
        .map_err(|err| MetadataSstCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| MetadataSstCodecError::Compress(err.to_string()))
}

pub fn decode_metadata_sst_envelope_zstd(
    bytes: &[u8],
) -> Result<MetadataSstEnvelope, MetadataSstCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| MetadataSstCodecError::Decompress(err.to_string()))?;
    let probe: EnvelopeProbe = from_reader(decompressed.as_slice())
        .map_err(|err| MetadataSstCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = MetadataSstKind::MetadataSst;
    if probe.kind != expected_kind.as_str() {
        return Err(MetadataSstCodecError::UnexpectedKind {
            expected: expected_kind.as_str().to_owned(),
            found: probe.kind,
        });
    }
    if probe.format_version != METADATA_SST_FORMAT_VERSION {
        return Err(MetadataSstCodecError::UnsupportedFormatVersion {
            found: probe.format_version,
            supported: METADATA_SST_FORMAT_VERSION,
        });
    }

    let document: MetadataSstDocument = from_reader(decompressed.as_slice())
        .map_err(|err| MetadataSstCodecError::EnvelopeDecode(err.to_string()))?;
    let actual = sha256_digest(&document.payload);
    if actual != document.payload_checksum {
        return Err(MetadataSstCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual,
        });
    }
    let payload: MetadataSstPayload = from_reader(document.payload.as_slice())
        .map_err(|err| MetadataSstCodecError::PayloadDecode(err.to_string()))?;

    Ok(MetadataSstEnvelope {
        kind: expected_kind,
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataFileRef,
        MetadataSegmentKey, MetadataTableFamily, NamespaceCheckpointRecord,
        NamespaceManifestEnvelope, NamespaceManifestFork, NamespaceManifestPayload,
    };
    use crate::{ChangeSeq, CommitId, FenceToken, InodeId, ManifestId, NamePolicy, NamespaceId};
    use std::collections::BTreeMap;

    #[test]
    fn namespace_manifest_codec_round_trips_base_only_materialization() {
        let envelope = NamespaceManifestEnvelope::from_payload(
            "test-writer",
            NamespaceManifestPayload {
                namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
                manifest_id: ManifestId(10),
                head_seq: ChangeSeq(10),
                head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                    .expect("commit id"),
                base_seq: ChangeSeq(10),
                active_fence_token: FenceToken(2),
                next_inode_id: InodeId(42),
                name_policy: NamePolicy::default(),
                retention_floor_seq: ChangeSeq(0),
                initialized: true,
                verified: true,
                fork: None,
                checkpoints: vec![NamespaceCheckpointRecord {
                    checkpoint_id: "chk_00000000000000000000000000000001".to_owned(),
                    manifest_id: ManifestId(10),
                    head_seq: ChangeSeq(10),
                    head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                        .expect("commit id"),
                    created_at_ms: 1_000,
                    expires_at_ms: None,
                    name: None,
                }],
                features: BTreeMap::new(),
                metadata_files: vec![metadata_file_ref(
                    "demo",
                    "tbl_00000000000000000000000000000001",
                    ChangeSeq(10),
                    1,
                    "namespaces/demo/compacted/metadata/tbl_00000000000000000000000000000001.sst",
                )],
            },
        )
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.base_seq, ChangeSeq(10));
        assert_eq!(decoded.payload.checkpoints.len(), 1);
        assert_eq!(
            decoded.payload.checkpoints[0].checkpoint_id,
            "chk_00000000000000000000000000000001"
        );
        assert_eq!(decoded.payload.metadata_files.len(), 1);
        assert_eq!(decoded.payload.metadata_files[0].run_seq, ChangeSeq(10));
    }

    #[test]
    fn namespace_manifest_codec_round_trips_fork_materialization() {
        let envelope = NamespaceManifestEnvelope::from_payload(
            "test-writer",
            NamespaceManifestPayload {
                namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
                manifest_id: ManifestId(12),
                head_seq: ChangeSeq(12),
                head_commit_id: CommitId::parse("c_00000000000000000000000000000002")
                    .expect("commit id"),
                base_seq: ChangeSeq(10),
                active_fence_token: FenceToken(2),
                next_inode_id: InodeId(42),
                name_policy: NamePolicy::default(),
                retention_floor_seq: ChangeSeq(0),
                initialized: true,
                verified: true,
                fork: Some(NamespaceManifestFork {
                    source_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                    fork_seq: ChangeSeq(12),
                    source_checkpoint_id: "chk_00000000000000000000000000000002".to_owned(),
                    source_manifest_id: ManifestId(10),
                    source_head_seq: ChangeSeq(12),
                }),
                checkpoints: Vec::new(),
                features: BTreeMap::new(),
                metadata_files: vec![
                    metadata_file_ref(
                        "source",
                        "tbl_00000000000000000000000000000001",
                        ChangeSeq(10),
                        1,
                        "namespaces/source/compacted/metadata/tbl_00000000000000000000000000000001.sst",
                    ),
                    metadata_file_ref(
                        "demo",
                        "tbl_00000000000000000000000000000002",
                        ChangeSeq(12),
                        0,
                        "namespaces/demo/compacted/metadata/tbl_00000000000000000000000000000002.sst",
                    ),
                ],
            },
        )
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.metadata_files[0].level, 1);
        assert_eq!(decoded.payload.metadata_files[1].level, 0);
        assert_eq!(decoded.payload.metadata_files[1].run_seq, ChangeSeq(12));
        assert_eq!(
            decoded
                .payload
                .fork
                .as_ref()
                .expect("fork")
                .source_manifest_id,
            ManifestId(10)
        );
    }

    #[test]
    fn direntry_bind_row_key_supports_parent_and_child_indexes() {
        let row = super::MetadataRow::DirentryBind {
            parent_inode_id: InodeId(9),
            name_key: "report.txt".to_owned(),
            display_name: "Report.txt".to_owned(),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        };

        assert_eq!(
            row.row_key_for_family(MetadataTableFamily::DirentryBinds),
            "direntry-00000000000000000009-7265706f72742e747874-00000000000000000017-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataTableFamily::DirentryChildBinds),
            "direntry-child-00000000000000000042-00000000000000000017-0000000003-00000000000000000009-7265706f72742e747874"
        );
    }

    #[test]
    fn row_keys_hex_encode_dash_containing_variable_components() {
        let row = super::MetadataRow::DirentryBind {
            parent_inode_id: InodeId(9),
            name_key: "report-2024".to_owned(),
            display_name: "report-2024".to_owned(),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        };

        assert_eq!(
            row.row_key_for_family(MetadataTableFamily::DirentryBinds),
            "direntry-00000000000000000009-7265706f72742d32303234-00000000000000000017-0000000003"
        );
    }

    fn metadata_file_ref(
        owner_namespace_id: &str,
        table_id: &str,
        run_seq: ChangeSeq,
        level: u32,
        object_key: &str,
    ) -> MetadataFileRef {
        MetadataFileRef {
            owner_namespace_id: NamespaceId::parse(owner_namespace_id).expect("valid namespace id"),
            table_id: table_id.to_owned(),
            object_key: object_key.to_owned(),
            run_seq,
            level,
            family: MetadataTableFamily::Inodes,
            segment_index: 0,
            segment_key: MetadataSegmentKey::Full,
            row_count: 0,
            min_key: String::new(),
            max_key: String::new(),
            payload_checksum: "sha256:unused".to_owned(),
        }
    }
}
