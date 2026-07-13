use crate::digest::sha256_digest;
use crate::envelope::EnvelopeProbe;
use crate::sst_blocks::BlockHandle;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, CommitId, ContentRef, IndexSegmentId, InodeId, InodeKind, ManifestId,
    ManifestObjectId, MetadataTableId, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::BTreeMap;
use thiserror::Error;

/// Version 1: an uncompressed JSON envelope document carrying the payload as
/// a raw JSON fragment. `payload_checksum` covers the fragment's exact bytes.
pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    NamespaceManifest,
}

impl NamespaceManifestKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceManifest => "metadata_manifest",
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
    RevisionsByInodeDesc,
    Tombstones,
    CommitReceipts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFileRef {
    pub owner_namespace_id: NamespaceId,
    pub table_id: MetadataTableId,
    pub object_key: String,
    pub run_seq: ChangeSeq,
    pub level: u32,
    pub family: MetadataTableFamily,
    pub segment_index: u32,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    /// Where the segment's index block lives and how to verify it. The
    /// descriptor is the only entry point into a segment object — there is
    /// no footer — so a reader starts here.
    pub index_block: BlockHandle,
    /// Where the segment's bloom filter block lives and how to verify it.
    pub filter_block: BlockHandle,
    /// The filter block's stored bytes inlined as hex, present when the
    /// filter is small (small delta runs). Point lookups consult it to skip
    /// the segment without any object fetch; `filter_block` still names and
    /// verifies the same bytes, so the inline copy must decode byte-for-byte
    /// identical (same length and CRC32C) or the manifest is corrupt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_inline: Option<String>,
    /// Checksum of the segment object's full bytes, in `sha256:<hex>` form.
    /// The ranged read path verifies per-block CRCs instead; this digest is
    /// the segment's identity in the decoded-block cache.
    pub payload_checksum: String,
}

/// One derived-index segment referenced by the manifest.
///
/// Index segments are derived work (format spec, "Derived work"): the same
/// block grammar as metadata segments with a feature-owned row payload,
/// listed separately from `metadata_files` so index-unaware readers can
/// ignore them. `family` is an open vocabulary string — not a closed enum
/// like [`MetadataTableFamily`] — because an unknown derived index must
/// never make a manifest unreadable; readers use the entries whose family
/// they understand and preserve the rest verbatim. Garbage collection
/// protects every listed `object_key` regardless of family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFileRef {
    pub owner_namespace_id: NamespaceId,
    pub segment_id: IndexSegmentId,
    pub object_key: String,
    /// Which derived index owns this segment and how to read its rows,
    /// e.g. `grams` (`index_grams::INDEX_FAMILY_GRAMS`). The feature's
    /// entry in the manifest `features` map version-gates the bytes.
    pub family: String,
    pub run_seq: ChangeSeq,
    pub level: u32,
    pub segment_index: u32,
    pub row_count: u64,
    pub min_key: String,
    pub max_key: String,
    /// Where the segment's index block lives and how to verify it. The
    /// descriptor is the only entry point into a segment object — there is
    /// no footer — so a reader starts here.
    pub index_block: BlockHandle,
    /// Where the segment's bloom filter block lives and how to verify it.
    pub filter_block: BlockHandle,
    /// The filter block's stored bytes inlined as hex, present when the
    /// filter is small, under the same byte-identical rule as
    /// [`MetadataFileRef::filter_inline`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_inline: Option<String>,
    /// Checksum of the segment object's full bytes, in `sha256:<hex>` form.
    pub payload_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestFork {
    pub source_namespace_id: NamespaceId,
    pub fork_seq: ChangeSeq,
    pub source_checkpoint_id: CheckpointId,
    pub source_manifest_id: ManifestId,
    pub source_manifest_object_id: ManifestObjectId,
    pub source_head_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataRow {
    Inode {
        inode_id: InodeId,
        inode_kind: InodeKind,
        created_seq: ChangeSeq,
    },
    DirentryBind {
        parent_inode_id: InodeId,
        name_key: NameKey,
        display_name: String,
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    },
    DirentryUnbind {
        parent_inode_id: InodeId,
        name_key: NameKey,
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
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
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
                    let name_key = hex_encode_row_key_component(name_key.as_str());
                    format!(
                        "direntry-child-{:020}-{:020}-{:010}-{:020}-{name_key}",
                        child_inode_id.0, bind_seq.0, bind_delta_index, parent_inode_id.0
                    )
                }
                _ => {
                    let name_key = hex_encode_row_key_component(name_key.as_str());
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
                let name_key = hex_encode_row_key_component(name_key.as_str());
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
                committed_seq,
                revision_delta_index,
                ..
            } => match family {
                MetadataTableFamily::RevisionsByInodeDesc => {
                    let reverse_revision_no = u64::MAX - revision_no.0;
                    let reverse_committed_seq = u64::MAX - committed_seq.0;
                    let reverse_delta_index = u32::MAX - revision_delta_index;
                    format!(
                        "revision-by-inode-desc-{:020}-{:020}-{:020}-{:010}",
                        inode_id.0, reverse_revision_no, reverse_committed_seq, reverse_delta_index
                    )
                }
                _ => {
                    format!(
                        "revision-{:020}-{:020}-{:010}",
                        inode_id.0, revision_no.0, revision_delta_index
                    )
                }
            },
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

    /// The exact lookup prefix a point read probes for this row in `family`,
    /// and therefore the key inserted into the segment's bloom filter. The
    /// two sides must agree byte-for-byte — a filter is an exact-match
    /// structure — so both are defined here, next to the row keys they
    /// shorten. Range scans at coarser granularity (a whole directory, a
    /// wave of names) do not consult filters.
    pub fn filter_key_for_family(&self, family: MetadataTableFamily) -> String {
        match self {
            Self::Inode { .. } => self.row_key_for_family(family),
            Self::DirentryBind {
                parent_inode_id,
                name_key,
                child_inode_id,
                ..
            } => match family {
                MetadataTableFamily::DirentryChildBinds => {
                    format!("direntry-child-{:020}", child_inode_id.0)
                }
                _ => {
                    let name_key = hex_encode_row_key_component(name_key.as_str());
                    format!("direntry-{:020}-{name_key}", parent_inode_id.0)
                }
            },
            Self::DirentryUnbind {
                parent_inode_id,
                name_key,
                ..
            } => {
                let name_key = hex_encode_row_key_component(name_key.as_str());
                format!("direntry-unbind-{:020}-{name_key}", parent_inode_id.0)
            }
            Self::Revision { inode_id, .. } => match family {
                MetadataTableFamily::RevisionsByInodeDesc => {
                    format!("revision-by-inode-desc-{:020}", inode_id.0)
                }
                _ => format!("revision-{:020}", inode_id.0),
            },
            Self::Tombstone { root_inode_id, .. } => {
                format!("tombstone-{:020}", root_inode_id.0)
            }
            Self::CommitReceipt { commit_id, .. } => {
                let commit_id = hex_encode_row_key_component(commit_id.as_str());
                format!("commit-receipt-{commit_id}")
            }
        }
    }
}

pub fn hex_encode_row_key_component(value: &str) -> String {
    hex_encode_bytes(value.as_bytes())
}

pub fn hex_encode_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[(byte >> 4) as usize]));
        encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    encoded
}

/// Decodes the lowercase hex this module's encoders produce. Errors carry no
/// input bytes; callers name the field they were decoding.
pub fn hex_decode_bytes(encoded: &str) -> Result<Vec<u8>, String> {
    fn nibble(byte: u8) -> Result<u8, String> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(format!("invalid hex byte {byte:#04x}")),
        }
    }
    let bytes = encoded.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(format!("odd hex length {}", bytes.len()));
    }
    let mut decoded = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        decoded.push((nibble(pair[0])? << 4) | nibble(pair[1])?);
    }
    Ok(decoded)
}

/// Reader-side lookup grammar: the probes, prefixes, and resume keys that
/// point lookups and scans build per family. Defined beside
/// `row_key_for_family` and `filter_key_for_family` because the pairing is
/// byte-for-byte — a probe must equal the filter key the writer stored, and
/// a prefix must be a prefix of the row keys it selects. Change a key
/// format and its lookup grammar together, here.
pub mod lookup_keys {
    use super::hex_encode_row_key_component;
    use crate::{ChangeSeq, InodeId, RevisionNo};

    pub fn inode_key(inode_id: InodeId) -> String {
        format!("inode-{:020}", inode_id.0)
    }

    pub fn direntry_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("direntry-{:020}-", parent_inode_id.0)
    }

    pub fn direntry_bind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "direntry-{:020}-{}",
            parent_inode_id.0,
            hex_encode_row_key_component(name_key)
        )
    }

    pub fn direntry_bind_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!("{}-", direntry_bind_probe(parent_inode_id, name_key))
    }

    pub fn direntry_child_probe(child_inode_id: InodeId) -> String {
        format!("direntry-child-{:020}", child_inode_id.0)
    }

    pub fn direntry_child_prefix(child_inode_id: InodeId) -> String {
        format!("{}-", direntry_child_probe(child_inode_id))
    }

    pub fn direntry_unbind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "direntry-unbind-{:020}-{}",
            parent_inode_id.0,
            hex_encode_row_key_component(name_key)
        )
    }

    /// Rows for one specific binding generation under the unbind probe.
    pub fn direntry_unbind_binding_prefix(
        parent_inode_id: InodeId,
        name_key: &str,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) -> String {
        format!(
            "{}-{:020}-{:010}-",
            direntry_unbind_probe(parent_inode_id, name_key),
            bind_seq.0,
            bind_delta_index
        )
    }

    pub fn direntry_unbind_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("direntry-unbind-{:020}-", parent_inode_id.0)
    }

    pub fn direntry_unbind_name_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "{}{}-",
            direntry_unbind_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key)
        )
    }

    pub fn tombstone_probe(root_inode_id: InodeId) -> String {
        format!("tombstone-{:020}", root_inode_id.0)
    }

    pub fn tombstone_prefix(root_inode_id: InodeId) -> String {
        format!("{}-", tombstone_probe(root_inode_id))
    }

    pub fn commit_receipt_probe(commit_id: &str) -> String {
        format!("commit-receipt-{}", hex_encode_row_key_component(commit_id))
    }

    pub fn commit_receipt_prefix(commit_id: &str) -> String {
        format!("{}-", commit_receipt_probe(commit_id))
    }

    pub fn revision_by_inode_desc_probe(inode_id: InodeId) -> String {
        format!("revision-by-inode-desc-{:020}", inode_id.0)
    }

    pub fn revision_by_inode_desc_prefix(inode_id: InodeId) -> String {
        format!("{}-", revision_by_inode_desc_probe(inode_id))
    }

    /// Revision numbers are stored inverted so newest sorts first.
    pub fn revision_by_inode_desc_revision_prefix(
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> String {
        format!(
            "{}{:020}-",
            revision_by_inode_desc_prefix(inode_id),
            u64::MAX - revision_no.0
        )
    }

    /// The full descending-index row key: revision number, commit seq, and
    /// delta index all inverted so newest sorts first.
    pub fn revision_by_inode_desc_row_key(
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        revision_delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            revision_by_inode_desc_prefix(inode_id),
            u64::MAX - revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - revision_delta_index
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestPayload {
    pub namespace_id: NamespaceId,
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub head_seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub base_seq: ChangeSeq,
    pub writer_epoch: WriterEpoch,
    pub next_inode_id: InodeId,
    pub retention_floor_seq: ChangeSeq,
    pub initialized: bool,
    pub verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<NamespaceManifestFork>,
    /// Per-namespace capabilities materialized on this file-set version,
    /// such as derived indexes (format spec, "Namespace features map").
    /// Values are feature-owned JSON objects; readers ignore unknown keys.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub features: BTreeMap<String, serde_json::Value>,
    // TODO: split this flat list into structured run/table/index roots when
    // compaction and GC need richer reachability decisions.
    pub metadata_files: Vec<MetadataFileRef>,
    /// Derived-index segments materialized on this file-set version, empty
    /// (and omitted) when no derived index exists. Additive: readers that
    /// predate derived indexes ignore the field, and the paired `features`
    /// entry — not this list — says what the segments mean.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub index_files: Vec<IndexFileRef>,
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

fn namespace_manifest_payload_checksum(
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

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, BlockHandle,
        MetadataFileRef, MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestFork,
        NamespaceManifestPayload,
    };
    use crate::{
        ChangeSeq, CheckpointId, CommitId, InodeId, ManifestId, ManifestObjectId, MetadataTableId,
        NameKey, NamespaceId, WriterEpoch,
    };
    use std::collections::BTreeMap;

    #[test]
    fn namespace_manifest_codec_round_trips_base_only_materialization() {
        let envelope = NamespaceManifestEnvelope::from_payload(
            "test-writer",
            NamespaceManifestPayload {
                namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
                manifest_id: ManifestId(10),
                manifest_object_id: ManifestObjectId::parse(
                    "00000000000000000010-0123456789abcdef",
                )
                .expect("valid manifest object id"),
                head_seq: ChangeSeq(10),
                head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                    .expect("commit id"),
                base_seq: ChangeSeq(10),
                writer_epoch: WriterEpoch(2),
                next_inode_id: InodeId(42),
                retention_floor_seq: ChangeSeq(0),
                initialized: true,
                verified: true,
                fork: None,
                features: BTreeMap::new(),
                metadata_files: vec![metadata_file_ref(
                    "demo",
                    "tbl_00000000000000000000000000000001",
                    ChangeSeq(10),
                    1,
                    "namespaces/demo/metadata/tables/tbl_00000000000000000000000000000001.sst.zst",
                )],
                index_files: Vec::new(),
            },
        )
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.base_seq, ChangeSeq(10));
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
                manifest_object_id: ManifestObjectId::parse(
                    "00000000000000000012-0123456789abcdef",
                )
                .expect("valid manifest object id"),
                head_seq: ChangeSeq(12),
                head_commit_id: CommitId::parse("c_00000000000000000000000000000002")
                    .expect("commit id"),
                base_seq: ChangeSeq(10),
                writer_epoch: WriterEpoch(2),
                next_inode_id: InodeId(42),
                retention_floor_seq: ChangeSeq(0),
                initialized: true,
                verified: true,
                fork: Some(NamespaceManifestFork {
                    source_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                    fork_seq: ChangeSeq(12),
                    source_checkpoint_id: CheckpointId::parse(
                        "chk_00000000000000000000000000000002",
                    )
                    .expect("checkpoint id"),
                    source_manifest_id: ManifestId(10),
                    source_manifest_object_id: ManifestObjectId::parse(
                        "00000000000000000010-0123456789abcdef",
                    )
                    .expect("valid manifest object id"),
                    source_head_seq: ChangeSeq(12),
                }),
                features: BTreeMap::new(),
                metadata_files: vec![
                    metadata_file_ref(
                        "source",
                        "tbl_00000000000000000000000000000001",
                        ChangeSeq(10),
                        1,
                        "namespaces/source/tables/metadata/tbl_00000000000000000000000000000001.sst.zst",
                    ),
                    metadata_file_ref(
                        "demo",
                        "tbl_00000000000000000000000000000002",
                        ChangeSeq(12),
                        0,
                        "namespaces/demo/metadata/tables/tbl_00000000000000000000000000000002.sst.zst",
                    ),
                ],
                index_files: Vec::new(),
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
            name_key: NameKey::parse("report.txt").expect("valid name key"),
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
            name_key: NameKey::parse("report-2024").expect("valid name key"),
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

    #[test]
    fn revision_row_key_supports_newest_first_inode_index() {
        let row = super::MetadataRow::Revision {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            revision_delta_index: 3,
            content_ref: crate::ContentRef {
                kind: crate::ContentRefKind::WholeFileV0,
                digest: "sha256:abc".to_owned(),
                size_bytes: 123,
            },
        };

        assert_eq!(
            row.row_key_for_family(MetadataTableFamily::Revisions),
            "revision-00000000000000000042-00000000000000000007-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataTableFamily::RevisionsByInodeDesc),
            "revision-by-inode-desc-00000000000000000042-18446744073709551608-18446744073709551603-4294967292"
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
            table_id: MetadataTableId::parse(table_id).expect("valid table id"),
            object_key: object_key.to_owned(),
            run_seq,
            level,
            family: MetadataTableFamily::Inodes,
            segment_index: 0,
            row_count: 0,
            min_key: String::new(),
            max_key: String::new(),
            index_block: BlockHandle {
                offset: 0,
                stored_len: 0,
                decoded_len: 0,
                crc32c: 0,
            },
            filter_block: BlockHandle {
                offset: 0,
                stored_len: 0,
                decoded_len: 0,
                crc32c: 0,
            },
            filter_inline: None,
            payload_checksum: "sha256:unused".to_owned(),
        }
    }
}
