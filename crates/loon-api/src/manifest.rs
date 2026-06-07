use crate::digest::sha256_hex;
use crate::v0::CommitOpResult;
use crate::{
    ChangeSeq, CommitId, ContentRef, FenceToken, InodeId, InodeKind, ManifestId, NamePolicy,
    NamespaceId, RevisionNo,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 5;
pub const METADATA_SST_FORMAT_VERSION: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    NamespaceManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataSstKind {
    MetadataSst,
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
    pub payload_checksum_sha256: String,
    pub page_checksums_sha256: Vec<String>,
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
        semantic_commit_fingerprint_sha256: String,
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

impl MetadataSstPayload {
    pub fn page_checksums_sha256(&self) -> Result<Vec<String>, MetadataSstCodecError> {
        self.pages
            .iter()
            .map(metadata_page_checksum_sha256)
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSstEnvelope {
    pub kind: MetadataSstKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
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
            payload_checksum_sha256: metadata_sst_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, MetadataSstCodecError> {
        Ok(self.payload_checksum_sha256 == metadata_sst_payload_checksum_sha256(&self.payload)?)
    }

    pub fn page_checksums_sha256(&self) -> Result<Vec<String>, MetadataSstCodecError> {
        self.payload.page_checksums_sha256()
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
    pub metadata_files: Vec<MetadataFileRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestEnvelope {
    pub kind: NamespaceManifestKind,
    pub format_version: u32,
    pub writer_version: String,
    pub payload_checksum_sha256: String,
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
            payload_checksum_sha256: namespace_manifest_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, NamespaceManifestCodecError> {
        Ok(self.payload_checksum_sha256
            == namespace_manifest_payload_checksum_sha256(&self.payload)?)
    }
}

#[derive(Debug, Error)]
pub enum NamespaceManifestCodecError {
    #[error("failed to encode namespace manifest payload to JSON: {0}")]
    PayloadEncode(String),
    #[error("failed to encode namespace manifest envelope to JSON: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode namespace manifest envelope from JSON: {0}")]
    EnvelopeDecode(String),
    #[error("namespace manifest payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

#[derive(Debug, Error)]
pub enum MetadataSstCodecError {
    #[error("failed to encode manifest page to CBOR: {0}")]
    PageEncode(String),
    #[error("failed to encode metadata SST payload to CBOR: {0}")]
    PayloadEncode(String),
    #[error("failed to encode metadata SST envelope to CBOR: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode metadata SST envelope from CBOR: {0}")]
    EnvelopeDecode(String),
    #[error("failed to compress metadata SST envelope: {0}")]
    Compress(String),
    #[error("failed to decompress metadata SST envelope: {0}")]
    Decompress(String),
    #[error("metadata SST payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub fn namespace_manifest_payload_checksum_sha256(
    payload: &NamespaceManifestPayload,
) -> Result<String, NamespaceManifestCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| NamespaceManifestCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_hex(&bytes))
}

pub fn encode_namespace_manifest_json(
    envelope: &NamespaceManifestEnvelope,
) -> Result<Vec<u8>, NamespaceManifestCodecError> {
    serde_json::to_vec(envelope)
        .map_err(|err| NamespaceManifestCodecError::EnvelopeEncode(err.to_string()))
}

pub fn metadata_page_checksum_sha256(page: &MetadataPage) -> Result<String, MetadataSstCodecError> {
    let mut encoded = Vec::new();
    into_writer(page, &mut encoded)
        .map_err(|err| MetadataSstCodecError::PageEncode(err.to_string()))?;
    Ok(sha256_hex(&encoded))
}

pub fn metadata_sst_payload_checksum_sha256(
    payload: &MetadataSstPayload,
) -> Result<String, MetadataSstCodecError> {
    Ok(sha256_hex(&encode_metadata_sst_payload_cbor(payload)?))
}

pub fn encode_metadata_sst_payload_cbor(
    payload: &MetadataSstPayload,
) -> Result<Vec<u8>, MetadataSstCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| MetadataSstCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

pub fn decode_namespace_manifest_json(
    bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, NamespaceManifestCodecError> {
    let envelope: NamespaceManifestEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| NamespaceManifestCodecError::EnvelopeDecode(err.to_string()))?;
    if envelope.format_version != NAMESPACE_MANIFEST_FORMAT_VERSION {
        return Err(NamespaceManifestCodecError::EnvelopeDecode(format!(
            "unsupported namespace manifest format version `{}`",
            envelope.format_version
        )));
    }
    let actual = namespace_manifest_payload_checksum_sha256(&envelope.payload)?;

    if actual != envelope.payload_checksum_sha256 {
        return Err(NamespaceManifestCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}

pub fn encode_metadata_sst_envelope_zstd(
    envelope: &MetadataSstEnvelope,
) -> Result<Vec<u8>, MetadataSstCodecError> {
    let mut encoded = Vec::new();
    into_writer(envelope, &mut encoded)
        .map_err(|err| MetadataSstCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), 0)
        .map_err(|err| MetadataSstCodecError::Compress(err.to_string()))
}

pub fn decode_metadata_sst_envelope_zstd(
    bytes: &[u8],
) -> Result<MetadataSstEnvelope, MetadataSstCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| MetadataSstCodecError::Decompress(err.to_string()))?;
    let envelope: MetadataSstEnvelope = from_reader(decompressed.as_slice())
        .map_err(|err| MetadataSstCodecError::EnvelopeDecode(err.to_string()))?;
    if envelope.format_version != METADATA_SST_FORMAT_VERSION {
        return Err(MetadataSstCodecError::EnvelopeDecode(format!(
            "unsupported metadata SST format version `{}`",
            envelope.format_version
        )));
    }

    let actual = metadata_sst_payload_checksum_sha256(&envelope.payload)?;
    if actual != envelope.payload_checksum_sha256 {
        return Err(MetadataSstCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, MetadataFileRef,
        MetadataSegmentKey, MetadataTableFamily, NamespaceCheckpointRecord,
        NamespaceManifestEnvelope, NamespaceManifestFork, NamespaceManifestPayload,
    };
    use crate::{ChangeSeq, CommitId, FenceToken, InodeId, ManifestId, NamePolicy, NamespaceId};

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
            payload_checksum_sha256: "sha256:unused".to_owned(),
            page_checksums_sha256: Vec::new(),
        }
    }
}
