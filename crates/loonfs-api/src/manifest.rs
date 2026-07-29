//! The namespace manifest format: the durable document naming the
//! metadata SST runs that materialize one namespace file-set version
//! (format spec, "Namespace manifests").

use crate::envelope::EnvelopeCodecError;
use crate::sst_blocks::BlockHandle;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CommitId, ContentRef, DisplayName, InodeId, InodeKind, ManifestId, ManifestObjectId,
    MetadataTableId, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Version 1: an uncompressed JSON envelope document carrying the payload as
/// a raw JSON fragment. `payload_checksum` covers the fragment's exact bytes.
pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 1;

/// Identifies the durable payload family carried by a namespace-manifest envelope.
///
/// See [durable object families](../../../docs/specs/format.md#12-durable-object-families).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    /// Marks the file-set descriptor used to materialize a namespace snapshot.
    NamespaceManifest,
}

impl NamespaceManifestKind {
    /// Returns the frozen envelope discriminator written to durable storage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceManifest => "namespace_manifest",
        }
    }
}

/// Selects a metadata row family and its durable lookup ordering.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataTableFamily {
    /// Stores inode identity, kind, and creation position.
    Inodes,
    /// Orders directory bindings for parent-and-name visibility lookups.
    DirentryBinds,
    /// Re-indexes directory bindings by child for parent discovery.
    DirentryChildBinds,
    /// Stores immutable events that retire exact historical bindings.
    DirentryUnbinds,
    /// Stores file revisions in their canonical durable ordering.
    Revisions,
    /// Re-indexes file revisions for newest-first per-inode reads.
    RevisionsByInodeDesc,
    /// Stores set and revoke events used to determine active subtree tombstones.
    Tombstones,
    /// Preserves commit idempotency evidence independently of retained WAL history.
    CommitReceipts,
}

/// Describes one immutable metadata SST object referenced by a namespace manifest.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataFileRef {
    /// Namespace whose keyspace owns the object, which may be a fork source rather than the reader.
    pub owner_namespace_id: NamespaceId,
    /// Immutable table identity incorporated into the object's durable key.
    pub table_id: MetadataTableId,
    /// Fully resolved object-store key trusted only after descriptor validation.
    pub object_key: String,
    /// Namespace sequence at which this run was produced.
    pub run_seq: ChangeSeq,
    /// Compaction tier used to order overlapping runs during reads and reorganization.
    pub level: u32,
    /// Row schema and lookup ordering encoded in this segment.
    pub family: MetadataTableFamily,
    /// Zero-based shard position among segments emitted for the same family and run.
    pub segment_index: u32,
    /// Number of row payloads in the segment, used for validation and planning.
    pub row_count: u64,
    /// Inclusive least durable row key; the segment is corrupt if decoded rows disagree.
    pub min_key: String,
    /// Inclusive greatest durable row key; range planning skips disjoint segments.
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

/// Stores one materialized metadata event in an SST segment.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataRow {
    /// Establishes one inode's immutable identity and kind.
    Inode {
        /// Namespace-scoped inode identity allocated by the publishing writer.
        inode_id: InodeId,
        /// Classification fixed when the inode was created.
        inode_kind: InodeKind,
        /// Commit sequence from which the inode can become visible.
        created_seq: ChangeSeq,
    },
    /// Records one generation of a directory name binding.
    DirentryBind {
        /// Directory in which the name was bound.
        parent_inode_id: InodeId,
        /// Policy-derived key used for uniqueness and lookup.
        name_key: NameKey,
        /// User-facing component spelling retained for directory responses.
        display_name: DisplayName,
        /// Inode reached while this binding generation remains active.
        child_inode_id: InodeId,
        /// Commit sequence that created this binding generation.
        bind_seq: ChangeSeq,
        /// Position that disambiguates the binding within `bind_seq`.
        bind_delta_index: u32,
    },
    /// Retires one exact directory-binding generation.
    DirentryUnbind {
        /// Directory that held the targeted binding.
        parent_inode_id: InodeId,
        /// Canonical name key of the targeted binding.
        name_key: NameKey,
        /// User-facing spelling the retired binding carried.
        display_name: DisplayName,
        /// Child identity recorded by the targeted binding.
        child_inode_id: InodeId,
        /// Commit sequence that created the binding being retired.
        bind_seq: ChangeSeq,
        /// Delta position of the binding being retired.
        bind_delta_index: u32,
        /// Commit sequence from which this unbind takes effect.
        unbind_seq: ChangeSeq,
        /// Position that disambiguates the unbind within `unbind_seq`.
        unbind_delta_index: u32,
    },
    /// Publishes one immutable content revision for a file inode.
    Revision {
        /// File inode whose history contains the revision.
        inode_id: InodeId,
        /// Monotonic revision number within that file's history.
        revision_no: RevisionNo,
        /// Namespace sequence that published the revision.
        committed_seq: ChangeSeq,
        /// The owning commit's observational wall-clock stamp, denormalized
        /// onto the row so revision reads answer times without a receipt
        /// join. Never a validity input; `committed_seq` is the order.
        committed_at_ms: u64,
        /// Delta position that disambiguates the revision within `committed_seq`.
        revision_delta_index: u32,
        /// Immutable bytes published by the revision.
        content_ref: ContentRef,
    },
    /// Changes whether one root inode has an active subtree tombstone.
    Tombstone {
        /// Inode whose rooted subtree the event governs.
        root_inode_id: InodeId,
        /// Commit sequence that published this tombstone event.
        tombstone_seq: ChangeSeq,
        /// Position that disambiguates the event within `tombstone_seq`.
        tombstone_delta_index: u32,
        /// What this event did; readers take the newest row per root and
        /// treat a `revoke` newest row as "no active tombstone".
        action: TombstoneRowAction,
        /// Wall-clock stamp of the recording commit. Observational, like
        /// every `committed_at_ms`.
        deleted_at_ms: u64,
        /// Directory that held the deleted binding, for `set` rows from
        /// path deletes; tombstone rows are immortal, so this is where a
        /// deleted name survives after unbind rows age out.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_inode_id: Option<InodeId>,
        /// Canonical key of the deleted binding, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name_key: Option<NameKey>,
        /// User-facing spelling of the deleted binding, when known.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        display_name: Option<DisplayName>,
    },
    /// Preserves the evidence needed to answer a retried logical commit.
    CommitReceipt {
        /// Caller idempotency key whose later reuse is checked against this row.
        commit_id: CommitId,
        /// Digest used to distinguish a safe retry from conflicting id reuse.
        semantic_commit_fingerprint: String,
        /// Namespace sequence assigned to the accepted commit.
        committed_seq: ChangeSeq,
        /// The commit's observational wall-clock stamp. Receipts are the
        /// durable per-commit record once WAL history drops below the
        /// retention floor, so the stamp lives here for every commit,
        /// revision-bearing or not.
        committed_at_ms: u64,
        /// Caller annotation preserved for idempotent response reconstruction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
}

/// Tombstone-row event vocabulary (format spec, "Tombstones and deletion").
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TombstoneRowAction {
    /// The subtree rooted at the row's inode is deleted.
    Set,
    /// The deletion recorded at `(target_seq, target_delta_index)` is
    /// revoked.
    Revoke {
        /// Commit sequence of the exact `Set` event being compensated.
        target_seq: ChangeSeq,
        /// Delta position of the exact `Set` event within `target_seq`.
        target_delta_index: u32,
    },
}

impl MetadataRow {
    /// Builds this row's canonical durable key in its primary table family.
    ///
    /// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
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

    /// Builds this row's durable key using the selected primary or secondary ordering.
    ///
    /// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
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
                // The action and binding context live in the value: revoke
                // rows sort exactly like the deletions they cancel, so
                // newest-per-root scans see them in one prefix pass.
                ..
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

/// Encodes an arbitrary string so it can occupy one component of a durable row key.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
pub fn hex_encode_row_key_component(value: &str) -> String {
    crate::hex::hex_encode_bytes(value.as_bytes())
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

    /// Builds the exact point-lookup key for an inode row.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn inode_key(inode_id: InodeId) -> String {
        format!("inode-{:020}", inode_id.0)
    }

    /// Builds the range prefix selecting directory bindings under one parent.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("direntry-{:020}-", parent_inode_id.0)
    }

    /// Builds the bloom-filter probe shared by all generations of one parent/name binding.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_bind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "direntry-{:020}-{}",
            parent_inode_id.0,
            hex_encode_row_key_component(name_key)
        )
    }

    /// Builds the range prefix selecting every generation of one parent/name binding.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_bind_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!("{}-", direntry_bind_probe(parent_inode_id, name_key))
    }

    /// Builds the bloom-filter probe shared by bindings that target one child inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_child_probe(child_inode_id: InodeId) -> String {
        format!("direntry-child-{:020}", child_inode_id.0)
    }

    /// Builds the reverse-index range prefix selecting bindings to one child inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_child_prefix(child_inode_id: InodeId) -> String {
        format!("{}-", direntry_child_probe(child_inode_id))
    }

    /// Builds the bloom-filter probe shared by unbinds for one parent/name pair.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_unbind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "direntry-unbind-{:020}-{}",
            parent_inode_id.0,
            hex_encode_row_key_component(name_key)
        )
    }

    /// Rows for one specific binding generation under the unbind probe.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
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

    /// Builds the range prefix selecting every unbind below one parent directory.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_unbind_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("direntry-unbind-{:020}-", parent_inode_id.0)
    }

    /// Builds the range prefix selecting unbinds for one parent/name pair.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_unbind_name_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "{}{}-",
            direntry_unbind_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key)
        )
    }

    /// Builds the bloom-filter probe shared by tombstone events for one root inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn tombstone_probe(root_inode_id: InodeId) -> String {
        format!("tombstone-{:020}", root_inode_id.0)
    }

    /// Builds the range prefix selecting the tombstone history of one root inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn tombstone_prefix(root_inode_id: InodeId) -> String {
        format!("{}-", tombstone_probe(root_inode_id))
    }

    /// Builds the bloom-filter probe shared by receipts for one commit id.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn commit_receipt_probe(commit_id: &str) -> String {
        format!("commit-receipt-{}", hex_encode_row_key_component(commit_id))
    }

    /// Builds the range prefix selecting durable receipts for one commit id.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn commit_receipt_prefix(commit_id: &str) -> String {
        format!("{}-", commit_receipt_probe(commit_id))
    }

    /// Builds the bloom-filter probe shared by newest-first revisions of one inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn revision_by_inode_desc_probe(inode_id: InodeId) -> String {
        format!("revision-by-inode-desc-{:020}", inode_id.0)
    }

    /// Builds the range prefix selecting newest-first revisions of one inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn revision_by_inode_desc_prefix(inode_id: InodeId) -> String {
        format!("{}-", revision_by_inode_desc_probe(inode_id))
    }

    /// Revision numbers are stored inverted so newest sorts first.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
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
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
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

/// Carries one complete namespace file-set description inside a manifest envelope.
///
/// See [manifest publication](../../../docs/specs/format.md#61-manifest-publication-and-checkpoint-verification).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestPayload {
    /// Namespace whose materialized state this manifest describes.
    pub namespace_id: NamespaceId,
    /// Monotonic logical manifest position selected by the namespace root.
    pub manifest_id: ManifestId,
    /// Immutable object identity that distinguishes speculative candidates at `manifest_id`.
    pub manifest_object_id: ManifestObjectId,
    /// Greatest namespace sequence materialized by the referenced file set.
    pub head_seq: ChangeSeq,
    /// Commit id assigned to `head_seq`, used to validate agreement with the head.
    pub head_commit_id: CommitId,
    /// Oldest run sequence still represented by `metadata_files`.
    pub base_seq: ChangeSeq,
    /// Fencing epoch of the writer that produced this candidate.
    pub writer_epoch: WriterEpoch,
    /// First inode identity available after replaying the manifest snapshot.
    pub next_inode_id: InodeId,
    /// Earliest sequence for which retained history remains readable.
    pub retention_floor_seq: ChangeSeq,
    /// Complete ordered set of metadata segments required to reconstruct the snapshot.
    pub metadata_files: Vec<MetadataFileRef>,
}

/// In-memory view of a namespace manifest envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_namespace_manifest_json`] and validated only by
/// [`decode_namespace_manifest_json`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceManifestEnvelope {
    /// Durable-family discriminator checked before payload decoding.
    pub kind: NamespaceManifestKind,
    /// Family-local format version, which must equal [`NAMESPACE_MANIFEST_FORMAT_VERSION`].
    pub format_version: u32,
    /// Informational software version of the writer that encoded this object.
    pub writer_version: String,
    /// Digest of the payload JSON exactly as stored in the durable document,
    /// in `sha256:<hex>` form.
    pub payload_checksum: String,
    /// Decoded file-set description protected by `payload_checksum`.
    pub payload: NamespaceManifestPayload,
}

impl NamespaceManifestEnvelope {
    /// Builds a versioned envelope and computes its checksum from canonical payload JSON.
    ///
    /// Construction fails when the payload cannot be encoded.
    pub fn from_payload(
        writer_version: impl Into<String>,
        payload: NamespaceManifestPayload,
    ) -> Result<Self, EnvelopeCodecError> {
        Ok(Self {
            kind: NamespaceManifestKind::NamespaceManifest,
            format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
            writer_version: writer_version.into(),
            payload_checksum: namespace_manifest_payload_checksum(&payload)?,
            payload,
        })
    }
}

fn namespace_manifest_payload_checksum(
    payload: &NamespaceManifestPayload,
) -> Result<String, EnvelopeCodecError> {
    crate::envelope::json_payload_checksum(payload)
}

/// Encodes a namespace-manifest envelope as its durable JSON representation.
///
/// Encoding fails when the version is unsupported, the in-memory checksum is
/// stale, or JSON serialization fails. See
/// [manifest publication](../../../docs/specs/format.md#61-manifest-publication-and-checkpoint-verification).
pub fn encode_namespace_manifest_json(
    envelope: &NamespaceManifestEnvelope,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    crate::envelope::encode_json_envelope(
        envelope.kind.as_str(),
        envelope.format_version,
        NAMESPACE_MANIFEST_FORMAT_VERSION,
        &envelope.writer_version,
        &envelope.payload_checksum,
        &envelope.payload,
    )
}

/// Decodes and verifies a durable namespace-manifest JSON envelope.
///
/// Decoding fails for invalid JSON, the wrong kind or version, a checksum
/// mismatch, or an invalid payload. See
/// [manifest publication](../../../docs/specs/format.md#61-manifest-publication-and-checkpoint-verification).
pub fn decode_namespace_manifest_json(
    bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, EnvelopeCodecError> {
    let expected_kind = NamespaceManifestKind::NamespaceManifest;
    let decoded =
        crate::envelope::decode_json_envelope(bytes, NAMESPACE_MANIFEST_FORMAT_VERSION, |found| {
            crate::envelope::verify_kind(expected_kind.as_str(), found)
        })?;

    Ok(NamespaceManifestEnvelope {
        kind: expected_kind,
        format_version: decoded.format_version,
        writer_version: decoded.writer_version,
        payload_checksum: decoded.payload_checksum,
        payload: decoded.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, BlockHandle,
        MetadataFileRef, MetadataTableFamily, NamespaceManifestEnvelope, NamespaceManifestPayload,
    };
    use crate::{
        ChangeSeq, CommitId, InodeId, ManifestId, ManifestObjectId, MetadataTableId, NameKey,
        NamespaceId, WriterEpoch,
    };

    #[test]
    fn namespace_manifest_kind_string_matches_serde() {
        let kind = super::NamespaceManifestKind::NamespaceManifest;
        let serialized = serde_json::to_value(kind).expect("serialize kind");
        assert_eq!(serialized, serde_json::Value::from(kind.as_str()));
    }

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
                metadata_files: vec![metadata_file_ref(
                    "demo",
                    "tbl_00000000000000000000000000000001",
                    ChangeSeq(10),
                    1,
                    "namespaces/demo/metadata/tables/tbl_00000000000000000000000000000001.sst.zst",
                )],
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

    /// A fork target's first own manifest keeps referencing the source's
    /// metadata objects: ownership travels with each file reference, not
    /// with the manifest.
    #[test]
    fn namespace_manifest_codec_round_trips_inherited_source_tables() {
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
            decoded.payload.metadata_files[0].owner_namespace_id,
            NamespaceId::parse("source").expect("valid namespace id")
        );
    }

    #[test]
    fn direntry_bind_row_key_supports_parent_and_child_indexes() {
        let row = super::MetadataRow::DirentryBind {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: crate::DisplayName::parse("Report.txt").expect("valid display name"),
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
            display_name: crate::DisplayName::parse("report-2024").expect("valid display name"),
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
            committed_at_ms: 12_000,
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
