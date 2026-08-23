//! The namespace manifest format: the durable document naming the
//! metadata segment runs that materialize one namespace file-set version
//! (format spec, "Namespace manifests").

use crate::envelope::EnvelopeCodecError;
use crate::sst_blocks::BlockHandle;
use crate::WriterEpoch;
use crate::{
    AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId,
    InodeKind, ManifestNo, ManifestObjectId, MetadataCompactionId, MetadataSegmentId, NameKey,
    NamespaceId, RevisionNo, RunNo,
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
pub enum MetadataRowFamily {
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
    /// Names the deletions that are recoverable right now, derived from the
    /// tombstone family and ordered by deletion time.
    ActiveDeletions,
    /// Preserves commit idempotency evidence independently of retained WAL history.
    CommitReceipts,
    /// Stores inode attribute revisions newest-first.
    ///
    /// Attributes are read only in this order, so the family has no secondary
    /// index and requires no cross-family parity check.
    Attributes,
}

/// Reference to one immutable metadata segment in a namespace manifest.
///
/// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataSegmentRef {
    /// Namespace that stores the segment. This may be a fork source.
    pub owner_namespace_id: NamespaceId,
    /// Immutable segment id used in the durable object key.
    pub segment_id: MetadataSegmentId,
    /// Compaction job id when the segment is stored under a compaction
    /// prefix. Flushed segments omit this field and use `metadata/segments/`.
    /// The owner, segment id, and optional job id determine the object key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_job_id: Option<MetadataCompactionId>,
    /// Run this segment belongs to. Every segment one producer wrote
    /// together carries the same run number, and no two runs share one.
    pub run_no: RunNo,
    /// Namespace sequence at which this run was produced.
    pub run_seq: ChangeSeq,
    /// Compaction tier used to order overlapping runs during reads and reorganization.
    pub level: u32,
    /// Row schema and lookup ordering encoded in this segment.
    pub family: MetadataRowFamily,
    /// Zero-based shard position among segments emitted for the same family and run.
    pub segment_index: u32,
    /// Number of row payloads in the segment, used for validation and planning.
    pub row_count: u64,
    /// Inclusive least durable row key; the segment is corrupt if decoded rows disagree.
    pub min_row_key: String,
    /// Inclusive greatest durable row key; range planning skips disjoint segments.
    pub max_row_key: String,
    /// Location and verification data for the segment index block.
    ///
    /// Segments have no footer, so readers begin with this handle.
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
    /// SHA-256 of the complete stored segment, formatted as
    /// `sha256:<64 lowercase hex>`. Caches and offline verification use this
    /// value. Ranged reads verify each block with its CRC32C instead.
    pub object_checksum: String,
}

/// One materialized metadata row stored in a segment.
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
        /// Commit ID associated with this row.
        commit_id: CommitId,
        /// Actor that created the inode, as supplied by the application.
        created_by: crate::ActorRef,
        /// Time the inode was created, in Unix milliseconds.
        created_at_ms: u64,
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
    FileRevision {
        /// File inode whose history contains the revision.
        inode_id: InodeId,
        /// Monotonic revision number within that file's history.
        revision_no: RevisionNo,
        /// Namespace sequence that published the revision.
        committed_seq: ChangeSeq,
        /// Commit ID associated with this row.
        commit_id: CommitId,
        /// The owning commit's observational wall-clock stamp, denormalized
        /// onto the row so revision reads answer times without a receipt
        /// join. Never a validity input; `committed_seq` is the order.
        committed_at_ms: u64,
        /// Actor that committed this revision, as supplied by the application.
        committed_by: crate::ActorRef,
        /// Delta position that disambiguates the revision within `committed_seq`.
        delta_index: u32,
        /// Immutable bytes published by the revision.
        content_ref: ContentRef,
    },
    /// Changes whether one root inode has an active subtree tombstone.
    Tombstone {
        /// Inode whose rooted subtree the event governs.
        root_inode_id: InodeId,
        /// Where this event sits in the namespace's history, and the
        /// generation a later `revoke` names.
        generation: TombstoneGeneration,
        /// Commit ID associated with this row.
        commit_id: CommitId,
        /// What this event did; readers take the newest row per root and
        /// treat a `revoke` newest row as "no active tombstone".
        action: TombstoneRowAction,
        /// Wall-clock stamp of the recording commit. Observational, like
        /// every `committed_at_ms`.
        deleted_at_ms: u64,
        /// Actor that recorded this tombstone event.
        deleted_by: crate::ActorRef,
    },
    /// Derived row used to list currently recoverable deletions.
    ///
    /// Materialization writes `listed` for each tombstone set and `removed` for
    /// each revoke. This lets trash listing use an ordered range scan instead of
    /// replaying all historical deletion events.
    ActiveDeletion {
        /// Subtree root the deletion covers. With `deletion_seq` this is
        /// exactly the handle `undelete` addresses.
        root_inode_id: InodeId,
        /// Commit sequence of the deletion. A `removed` row repeats the
        /// original deletion sequence so both rows sort together.
        deletion_seq: ChangeSeq,
        /// Whether the deletion is still recoverable, and the listing detail
        /// it carries while it is.
        action: ActiveDeletionRowAction,
    },
    /// Preserves the evidence needed to answer a retried logical commit.
    CommitReceipt {
        /// Caller idempotency key whose later reuse is checked against this row.
        commit_id: CommitId,
        /// Actor that committed the change, as supplied by the application.
        committed_by: crate::ActorRef,
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
    /// Publishes one inode's complete attribute map at one revision.
    ///
    /// The row is whole state, not a change: a reader takes the newest row
    /// for an inode and needs nothing older. An inode with no row anywhere is
    /// at revision 0 with an empty map, so nothing is written until a caller
    /// writes an attribute.
    AttributesRevision {
        /// Inode whose attributes this revision states.
        inode_id: InodeId,
        /// Monotonic per-inode attribute revision.
        attributes_revision_no: AttributeRevisionNo,
        /// Namespace sequence that published the revision.
        committed_seq: ChangeSeq,
        /// Commit ID associated with this row.
        commit_id: CommitId,
        /// Delta position that disambiguates the revision within `committed_seq`.
        delta_index: u32,
        /// Actor that updated the attributes.
        updated_by: crate::ActorRef,
        /// Time of the attribute update, in Unix milliseconds.
        updated_at_ms: u64,
        /// The inode's complete attribute map at this revision. An empty map
        /// is the cleared state.
        attributes: Attributes,
    },
}

/// Names one deletion generation: the commit that recorded a tombstone
/// event and the position that disambiguates it inside that commit.
///
/// Shared by the tombstone row and the WAL delta that revokes one, so a
/// revoke names its target in the same spelling everywhere.
///
/// This type appears only in immutable data, so it accepts unknown fields.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TombstoneGeneration {
    /// Commit sequence that published the event.
    pub seq: ChangeSeq,
    /// Position that disambiguates the event within `seq`.
    pub delta_index: u32,
}

/// Directory binding removed by a path deletion.
///
/// Tombstones retain this binding after the corresponding unbind row may be
/// collected. Undelete uses it to restore the original parent and name.
///
/// This type appears only in immutable data, so it accepts unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeletedDirentry {
    /// Directory that held the binding.
    pub parent_inode_id: InodeId,
    /// Canonical key the binding was reachable under.
    pub name_key: NameKey,
    /// User-facing spelling the binding carried.
    pub display_name: DisplayName,
}

/// Reads an optional field that must still be written.
///
/// Serde reads a missing `Option` field as `None`, which would make an
/// encoding that never had the field indistinguishable from one that stated
/// its absence. A durable optional that distinguishes those two reads
/// through here instead.
pub(crate) fn required_option<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

/// Tombstone-row event vocabulary (format spec, "Tombstones and deletion").
///
/// This type appears only in immutable rows, so it accepts unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TombstoneRowAction {
    /// The subtree rooted at the row's inode is deleted.
    Set {
        /// The binding the delete removed, or `null` for a delete addressed
        /// by inode, which had no name to record. Stated either way and
        /// never defaulted, so bytes without the field are the pre-grouping
        /// layout — which spelled the binding as three optional row fields —
        /// rather than a deletion that recorded no name.
        #[serde(deserialize_with = "required_option")]
        deleted_direntry: Option<DeletedDirentry>,
    },
    /// The deletion recorded at `target` is revoked. Only a `set` carries a
    /// binding, so the revoke has no place to put one.
    Revoke {
        /// The exact `set` event being compensated.
        target: TombstoneGeneration,
    },
}

/// Current-state rows for recoverable deletions.
///
/// `Listed` exposes a deletion in trash; `Removed` hides it after undelete.
/// Both rows share a key prefix, with `Removed` sorting first, so scans can
/// suppress restored entries. Reorganization later removes the cancelled
/// pair.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ActiveDeletionRowAction {
    /// The deletion is recoverable; these are the fields the trash entry
    /// renders, denormalized so a page needs no per-entry join.
    Listed {
        /// Wall-clock stamp of the deleting commit. Observational, like every
        /// `committed_at_ms`.
        deleted_at_ms: u64,
        /// Actor responsible for the deletion.
        deleted_by: crate::ActorRef,
        /// The binding the deletion removed, copied from the tombstone event
        /// this row derives from, or `null` when it recorded none. Stated
        /// either way, like the event's own.
        #[serde(deserialize_with = "required_option")]
        deleted_direntry: Option<DeletedDirentry>,
    },
    /// The deletion was cancelled by an undelete at `revocation_seq`.
    Removed {
        /// Commit sequence of the undelete that cancelled the deletion.
        revocation_seq: ChangeSeq,
    },
}

impl ActiveDeletionRowAction {
    /// The row-key component that orders a removal ahead of the row it
    /// removes.
    fn sort_rank(&self) -> u32 {
        match self {
            Self::Removed { .. } => lookup_keys::ACTIVE_DELETION_RANK_REMOVED,
            Self::Listed { .. } => lookup_keys::ACTIVE_DELETION_RANK_LISTED,
        }
    }
}

impl MetadataRowFamily {
    /// Fixed prefix before the first variable component in this family's row
    /// keys. Compaction uses the remaining components to group rows for
    /// retention, so this must match [`MetadataRow::row_key_for_family`].
    pub const fn row_key_prefix(self) -> &'static str {
        match self {
            Self::Inodes => "inode-",
            Self::DirentryBinds => "direntry-bind-",
            Self::DirentryChildBinds => "direntry-child-bind-",
            Self::DirentryUnbinds => "direntry-unbind-",
            Self::Revisions => "revision-",
            Self::RevisionsByInodeDesc => "revision-by-inode-desc-",
            Self::Tombstones => "tombstone-",
            Self::ActiveDeletions => "active-deletion-",
            Self::CommitReceipts => "commit-receipt-",
            Self::Attributes => "attribute-",
        }
    }
}

impl MetadataRow {
    /// Builds this row's canonical durable key in its primary row family.
    ///
    /// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
    pub fn row_key(&self) -> String {
        self.row_key_for_family(match self {
            Self::Inode { .. } => MetadataRowFamily::Inodes,
            Self::DirentryBind { .. } => MetadataRowFamily::DirentryBinds,
            Self::DirentryUnbind { .. } => MetadataRowFamily::DirentryUnbinds,
            Self::FileRevision { .. } => MetadataRowFamily::Revisions,
            Self::Tombstone { .. } => MetadataRowFamily::Tombstones,
            Self::ActiveDeletion { .. } => MetadataRowFamily::ActiveDeletions,
            Self::CommitReceipt { .. } => MetadataRowFamily::CommitReceipts,
            Self::AttributesRevision { .. } => MetadataRowFamily::Attributes,
        })
    }

    /// Builds this row's durable key using the selected primary or secondary ordering.
    ///
    /// See [metadata segments](../../../docs/specs/format.md#421-metadata-segments).
    pub fn row_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode { inode_id, .. } => format!("inode-{:020}", inode_id.0),
            Self::DirentryBind {
                parent_inode_id,
                name_key,
                child_inode_id,
                bind_seq,
                bind_delta_index,
                ..
            } => {
                let name_key = hex_encode_row_key_component(name_key.as_str());
                match family {
                    MetadataRowFamily::DirentryChildBinds => {
                        format!(
                            "direntry-child-bind-{:020}-{:020}-{:010}-{:020}-{name_key}",
                            child_inode_id.0, bind_seq.0, bind_delta_index, parent_inode_id.0
                        )
                    }
                    _ => {
                        format!(
                            "direntry-bind-{:020}-{name_key}-{:020}-{:010}",
                            parent_inode_id.0, bind_seq.0, bind_delta_index
                        )
                    }
                }
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
            Self::FileRevision {
                inode_id,
                revision_no,
                committed_seq,
                delta_index,
                ..
            } => match family {
                MetadataRowFamily::RevisionsByInodeDesc => {
                    let reverse_revision_no = u64::MAX - revision_no.0;
                    let reverse_committed_seq = u64::MAX - committed_seq.0;
                    let reverse_delta_index = u32::MAX - delta_index;
                    format!(
                        "revision-by-inode-desc-{:020}-{:020}-{:020}-{:010}",
                        inode_id.0, reverse_revision_no, reverse_committed_seq, reverse_delta_index
                    )
                }
                _ => {
                    format!(
                        "revision-{:020}-{:020}-{:010}",
                        inode_id.0, revision_no.0, delta_index
                    )
                }
            },
            Self::Tombstone {
                root_inode_id,
                generation,
                // The action and binding context live in the value: revoke
                // rows sort exactly like the deletions they cancel, so
                // newest-per-root scans see them in one prefix pass.
                ..
            } => {
                format!(
                    "tombstone-{:020}-{:020}-{:010}",
                    root_inode_id.0, generation.seq.0, generation.delta_index
                )
            }
            Self::ActiveDeletion {
                root_inode_id,
                deletion_seq,
                action,
            } => lookup_keys::active_deletion_row_key(
                *deletion_seq,
                *root_inode_id,
                action.sort_rank(),
            ),
            Self::CommitReceipt {
                committed_seq,
                commit_id,
                ..
            } => {
                let commit_id = hex_encode_row_key_component(commit_id.as_str());
                format!("commit-receipt-{commit_id}-{:020}", committed_seq.0)
            }
            Self::AttributesRevision {
                inode_id,
                attributes_revision_no,
                committed_seq,
                delta_index,
                ..
            } => lookup_keys::attributes_row_key(
                *inode_id,
                *attributes_revision_no,
                *committed_seq,
                *delta_index,
            ),
        }
    }

    /// The exact lookup prefix a point read probes for this row in `family`,
    /// and therefore the key inserted into the segment's bloom filter. The
    /// two sides must agree byte-for-byte — a filter is an exact-match
    /// structure — so both are defined here, next to the row keys they
    /// shorten. Range scans at coarser granularity (a whole directory, a
    /// wave of names) do not consult filters.
    pub fn filter_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode { .. } => self.row_key_for_family(family),
            Self::DirentryBind {
                parent_inode_id,
                name_key,
                child_inode_id,
                ..
            } => match family {
                MetadataRowFamily::DirentryChildBinds => {
                    format!("direntry-child-bind-{:020}", child_inode_id.0)
                }
                _ => {
                    let name_key = hex_encode_row_key_component(name_key.as_str());
                    format!("direntry-bind-{:020}-{name_key}", parent_inode_id.0)
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
            Self::FileRevision { inode_id, .. } => match family {
                MetadataRowFamily::RevisionsByInodeDesc => {
                    format!("revision-by-inode-desc-{:020}", inode_id.0)
                }
                _ => format!("revision-{:020}", inode_id.0),
            },
            Self::Tombstone { root_inode_id, .. } => {
                format!("tombstone-{:020}", root_inode_id.0)
            }
            // The family is only ever range-scanned in key order, never
            // probed for one deletion, so the filter key is the row key.
            Self::ActiveDeletion { .. } => self.row_key_for_family(family),
            Self::CommitReceipt { commit_id, .. } => {
                let commit_id = hex_encode_row_key_component(commit_id.as_str());
                format!("commit-receipt-{commit_id}")
            }
            Self::AttributesRevision { inode_id, .. } => lookup_keys::attributes_probe(*inode_id),
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
    use crate::{AttributeRevisionNo, ChangeSeq, InodeId, RevisionNo};

    /// The prefix every inode row key starts with. Inode ids are
    /// zero-padded to a fixed width after it, so a range scan over this
    /// prefix walks the inode family in ascending inode-id order.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub const INODE_ROW_PREFIX: &str = "inode-";

    /// The prefix every canonical revision row key starts with. A range scan
    /// over it walks every revision the manifest records, superseded ones
    /// included, which is what a reachability question about content needs.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub const REVISION_ROW_PREFIX: &str = "revision-";

    /// Builds the exact point-lookup key for an inode row.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn inode_key(inode_id: InodeId) -> String {
        format!("{INODE_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the resume bound for a scan that must continue strictly after
    /// `inode_id`'s row.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn inode_key_after(inode_id: InodeId) -> String {
        format!("{}\0", inode_key(inode_id))
    }

    /// Builds the range prefix selecting directory bindings under one parent.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("direntry-bind-{:020}-", parent_inode_id.0)
    }

    /// Builds the bloom-filter probe shared by all generations of one parent/name binding.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn direntry_bind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "direntry-bind-{:020}-{}",
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
        format!("direntry-child-bind-{:020}", child_inode_id.0)
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

    /// The prefix every active-deletion row key starts with. Deletion
    /// sequence and root inode are zero-padded to fixed widths after it, so a
    /// range scan over this prefix walks the namespace's recoverable
    /// deletions oldest deletion first — the trash listing's whole read.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub const ACTIVE_DELETION_ROW_PREFIX: &str = "active-deletion-";

    /// Rank of an undelete's removal marker within one deletion generation.
    /// It is the lowest rank on purpose: an ascending scan sees the removal
    /// before the row it removes, so a page never lists a deletion whose
    /// marker was going to arrive one page later.
    pub const ACTIVE_DELETION_RANK_REMOVED: u32 = 0;

    /// Rank of the listed row within one deletion generation, and the highest
    /// rank the family defines.
    pub const ACTIVE_DELETION_RANK_LISTED: u32 = 1;

    /// Builds an active-deletion row key from its generation and rank.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn active_deletion_row_key(
        deletion_seq: ChangeSeq,
        root_inode_id: InodeId,
        sort_rank: u32,
    ) -> String {
        format!(
            "{ACTIVE_DELETION_ROW_PREFIX}{:020}-{:020}-{sort_rank:010}",
            deletion_seq.0, root_inode_id.0
        )
    }

    /// Builds the resume bound for a trash page that must continue strictly
    /// after the deletion generation it last returned. The listed row is the
    /// generation's highest-ranked row, so resuming past it skips the whole
    /// generation and nothing else.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn active_deletion_key_after(deletion_seq: ChangeSeq, root_inode_id: InodeId) -> String {
        format!(
            "{}\0",
            active_deletion_row_key(deletion_seq, root_inode_id, ACTIVE_DELETION_RANK_LISTED)
        )
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
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            revision_by_inode_desc_prefix(inode_id),
            u64::MAX - revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }

    /// Builds the bloom-filter probe shared by every attribute revision of one
    /// inode.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn attributes_probe(inode_id: InodeId) -> String {
        format!("attribute-{:020}", inode_id.0)
    }

    /// Builds the range prefix selecting one inode's attribute revisions,
    /// newest first.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn attributes_prefix(inode_id: InodeId) -> String {
        format!("{}-", attributes_probe(inode_id))
    }

    /// The full attribute row key: revision number, commit seq, and delta
    /// index all inverted so an ascending scan of one inode's prefix reads
    /// its newest attribute state first.
    ///
    /// See [metadata segments](../../../../docs/specs/format.md#421-metadata-segments).
    pub fn attributes_row_key(
        inode_id: InodeId,
        attributes_revision_no: AttributeRevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            attributes_prefix(inode_id),
            u64::MAX - attributes_revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
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
    pub manifest_no: ManifestNo,
    /// Unique id for this candidate at `manifest_no`.
    pub manifest_object_id: ManifestObjectId,
    /// Greatest namespace sequence materialized by the referenced file set.
    pub head_seq: ChangeSeq,
    /// Commit id assigned to `head_seq`, used to validate agreement with the head.
    pub head_commit_id: CommitId,
    /// Oldest run sequence still represented by `segments`.
    pub base_seq: ChangeSeq,
    /// Fencing epoch of the writer that produced this candidate.
    pub writer_epoch: WriterEpoch,
    /// First inode identity available after replaying the manifest snapshot.
    pub next_inode_id: InodeId,
    /// Run number the next producer allocates. Every segment's `run_no` is
    /// below it.
    pub next_run_no: RunNo,
    /// Earliest sequence for which retained history remains readable.
    pub retention_floor_seq: ChangeSeq,
    /// Complete ordered set of metadata segments required to reconstruct the snapshot.
    pub segments: Vec<MetadataSegmentRef>,
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
    pub fn from_payload(payload: NamespaceManifestPayload) -> Result<Self, EnvelopeCodecError> {
        Ok(Self {
            kind: NamespaceManifestKind::NamespaceManifest,
            format_version: NAMESPACE_MANIFEST_FORMAT_VERSION,
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
        payload_checksum: decoded.payload_checksum,
        payload: decoded.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, BlockHandle,
        MetadataRowFamily, MetadataSegmentRef, NamespaceManifestEnvelope, NamespaceManifestPayload,
    };
    use crate::{
        ChangeSeq, CommitId, InodeId, ManifestNo, ManifestObjectId, MetadataCompactionId,
        MetadataSegmentId, NameKey, NamespaceId, RunNo, WriterEpoch,
    };

    fn row_commit_id() -> CommitId {
        CommitId::parse("c_metadata_row").expect("commit id")
    }

    #[test]
    fn inode_row_keys_sort_by_ascending_inode_id() {
        // The inode family's durable order IS ascending inode id, which is
        // what lets a whole-namespace file walk resume from one bound.
        let ids = [9_u64, 1, 100, 10, 2];
        let key_of = |id: u64| super::lookup_keys::inode_key(InodeId(id));
        let mut keys: Vec<String> = ids.iter().copied().map(key_of).collect();
        keys.sort();

        let mut ascending_ids = ids;
        ascending_ids.sort_unstable();
        assert_eq!(
            keys,
            ascending_ids
                .iter()
                .copied()
                .map(key_of)
                .collect::<Vec<_>>(),
            "row-key order must agree with inode-id order"
        );
        assert!(keys
            .iter()
            .all(|key| key.starts_with(super::lookup_keys::INODE_ROW_PREFIX)));
    }

    #[test]
    fn the_inode_resume_bound_skips_its_own_row_and_nothing_after_it() {
        let resume = super::lookup_keys::inode_key_after(InodeId(7));
        assert!(resume > super::lookup_keys::inode_key(InodeId(7)));
        assert!(resume < super::lookup_keys::inode_key(InodeId(8)));
    }

    #[test]
    fn namespace_manifest_kind_string_matches_serde() {
        let kind = super::NamespaceManifestKind::NamespaceManifest;
        let serialized = serde_json::to_value(kind).expect("serialize kind");
        assert_eq!(serialized, serde_json::Value::from(kind.as_str()));
    }

    #[test]
    fn namespace_manifest_codec_round_trips_base_only_materialization() {
        let envelope = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(10),
            manifest_object_id: ManifestObjectId::parse("00000000000000000010-0123456789abcdef")
                .expect("valid manifest object id"),
            head_seq: ChangeSeq(10),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000001")
                .expect("commit id"),
            base_seq: ChangeSeq(10),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(1),
            retention_floor_seq: ChangeSeq(0),
            segments: vec![metadata_segment_ref(
                "demo",
                "seg_00000000000000000000000000000001",
                RunNo(0),
                ChangeSeq(10),
                1,
            )],
        })
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.base_seq, ChangeSeq(10));
        assert_eq!(decoded.payload.segments.len(), 1);
        assert_eq!(decoded.payload.segments[0].run_seq, ChangeSeq(10));
    }

    /// A fork target's first own manifest keeps referencing the source's
    /// metadata objects: ownership travels with each file reference, not
    /// with the manifest.
    #[test]
    fn namespace_manifest_codec_round_trips_inherited_source_segments() {
        let envelope = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(12),
            manifest_object_id: ManifestObjectId::parse("00000000000000000012-0123456789abcdef")
                .expect("valid manifest object id"),
            head_seq: ChangeSeq(12),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000002")
                .expect("commit id"),
            base_seq: ChangeSeq(10),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(2),
            retention_floor_seq: ChangeSeq(0),
            segments: vec![
                metadata_segment_ref(
                    "source",
                    "seg_00000000000000000000000000000001",
                    RunNo(0),
                    ChangeSeq(10),
                    1,
                ),
                metadata_segment_ref(
                    "demo",
                    "seg_00000000000000000000000000000002",
                    RunNo(1),
                    ChangeSeq(12),
                    0,
                ),
            ],
        })
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.segments[0].level, 1);
        assert_eq!(decoded.payload.segments[1].level, 0);
        assert_eq!(decoded.payload.segments[1].run_seq, ChangeSeq(12));
        assert_eq!(
            decoded.payload.segments[0].owner_namespace_id,
            NamespaceId::parse("source").expect("valid namespace id")
        );
    }

    /// Compaction segments include a job id; flushed segments omit it.
    #[test]
    fn namespace_manifest_codec_round_trips_a_compaction_job_segment() {
        let compaction_job_id = MetadataCompactionId::parse("cmp_0123456789abcdef0123456789abcdef")
            .expect("valid compaction job id");
        let mut staged = metadata_segment_ref(
            "demo",
            "seg_00000000000000000000000000000001",
            RunNo(0),
            ChangeSeq(14),
            1,
        );
        staged.compaction_job_id = Some(compaction_job_id.clone());
        let flushed = metadata_segment_ref(
            "demo",
            "seg_00000000000000000000000000000002",
            RunNo(1),
            ChangeSeq(14),
            0,
        );
        let envelope = NamespaceManifestEnvelope::from_payload(NamespaceManifestPayload {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(14),
            manifest_object_id: ManifestObjectId::parse("00000000000000000014-0123456789abcdef")
                .expect("valid manifest object id"),
            head_seq: ChangeSeq(14),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000003")
                .expect("commit id"),
            base_seq: ChangeSeq(14),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(2),
            retention_floor_seq: ChangeSeq(0),
            segments: vec![staged, flushed],
        })
        .expect("manifest");

        let encoded = encode_namespace_manifest_json(&envelope).expect("encode manifest");
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(
            decoded.payload.segments[0].compaction_job_id,
            Some(compaction_job_id)
        );
        assert_eq!(decoded.payload.segments[1].compaction_job_id, None);
        let text = String::from_utf8(encoded).expect("manifest json is utf-8");
        assert_eq!(
            text.matches("\"compaction_job_id\"").count(),
            1,
            "only the compaction job's segment writes the field, got {text}"
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
            row.row_key_for_family(MetadataRowFamily::DirentryBinds),
            "direntry-bind-00000000000000000009-7265706f72742e747874-00000000000000000017-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryChildBinds),
            "direntry-child-bind-00000000000000000042-00000000000000000017-0000000003-00000000000000000009-7265706f72742e747874"
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
            row.row_key_for_family(MetadataRowFamily::DirentryBinds),
            "direntry-bind-00000000000000000009-7265706f72742d32303234-00000000000000000017-0000000003"
        );
    }

    #[test]
    fn revision_row_key_supports_newest_first_inode_index() {
        let row = super::MetadataRow::FileRevision {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorRef::loonfs_system(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                    .expect("valid content id"),
                b"row key sample",
            ),
        };

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::Revisions),
            "revision-00000000000000000042-00000000000000000007-0000000003"
        );
        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::RevisionsByInodeDesc),
            "revision-by-inode-desc-00000000000000000042-18446744073709551608-18446744073709551603-4294967292"
        );
    }

    /// The attribute family's durable order is newest revision first within
    /// one inode, and the row key a writer stores must be the exact key a
    /// reader's prefix selects.
    #[test]
    fn attributes_row_keys_sort_newest_revision_first_under_the_inode_prefix() {
        let row_of =
            |revision: u64, seq: u64, delta_index: u32| super::MetadataRow::AttributesRevision {
                inode_id: InodeId(42),
                attributes_revision_no: crate::AttributeRevisionNo(revision),
                committed_seq: ChangeSeq(seq),
                commit_id: row_commit_id(),
                delta_index,
                updated_by: crate::ActorRef::loonfs_system(),
                updated_at_ms: 12_000 + seq,
                attributes: crate::Attributes::default(),
            };
        let newest = row_of(3, 12, 1);
        let older = row_of(2, 11, 0);

        assert_eq!(
            newest.row_key_for_family(MetadataRowFamily::Attributes),
            "attribute-00000000000000000042-18446744073709551612-18446744073709551603-4294967294"
        );
        assert_eq!(
            newest.row_key(),
            newest.row_key_for_family(MetadataRowFamily::Attributes)
        );
        assert!(
            newest.row_key() < older.row_key(),
            "an ascending scan must reach the newest revision first"
        );
        let prefix = super::lookup_keys::attributes_prefix(InodeId(42));
        assert!(newest.row_key().starts_with(&prefix));
        assert!(older.row_key().starts_with(&prefix));
        // A point lookup probes the filter with the inode's shared key, and
        // the writer stores exactly that key.
        assert_eq!(
            newest.filter_key_for_family(MetadataRowFamily::Attributes),
            super::lookup_keys::attributes_probe(InodeId(42))
        );
        // Another inode's rows sort outside the prefix.
        assert!(!row_of(3, 12, 1)
            .row_key()
            .starts_with(&super::lookup_keys::attributes_prefix(InodeId(43))));
    }

    /// Compaction groups rows using the components after each family prefix.
    /// This test keeps those prefixes consistent with the generated row keys.
    #[test]
    fn row_key_prefixes_match_the_row_keys_they_front() {
        let name_key = NameKey::parse("report.txt").expect("valid name key");
        let display_name = crate::DisplayName::parse("report.txt").expect("valid display name");
        let bind = super::MetadataRow::DirentryBind {
            parent_inode_id: InodeId(9),
            name_key: name_key.clone(),
            display_name: display_name.clone(),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        };
        let revision = super::MetadataRow::FileRevision {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorRef::loonfs_system(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                    .expect("valid content id"),
                b"row key prefix sample",
            ),
        };
        let rows: [(MetadataRowFamily, super::MetadataRow); 10] = [
            (
                MetadataRowFamily::Inodes,
                super::MetadataRow::Inode {
                    inode_id: InodeId(42),
                    inode_kind: crate::InodeKind::File,
                    created_seq: ChangeSeq(3),
                    commit_id: row_commit_id(),
                    created_by: crate::ActorRef::loonfs_system(),
                    created_at_ms: 3_000,
                },
            ),
            (MetadataRowFamily::DirentryBinds, bind.clone()),
            (MetadataRowFamily::DirentryChildBinds, bind),
            (
                MetadataRowFamily::DirentryUnbinds,
                super::MetadataRow::DirentryUnbind {
                    parent_inode_id: InodeId(9),
                    name_key,
                    display_name,
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_delta_index: 3,
                    unbind_seq: ChangeSeq(19),
                    unbind_delta_index: 0,
                },
            ),
            (MetadataRowFamily::Revisions, revision.clone()),
            (MetadataRowFamily::RevisionsByInodeDesc, revision),
            (
                MetadataRowFamily::Tombstones,
                super::MetadataRow::Tombstone {
                    root_inode_id: InodeId(42),
                    generation: super::TombstoneGeneration {
                        seq: ChangeSeq(12),
                        delta_index: 0,
                    },
                    commit_id: row_commit_id(),
                    action: super::TombstoneRowAction::Set {
                        deleted_direntry: None,
                    },
                    deleted_at_ms: 12_000,
                    deleted_by: crate::ActorRef::loonfs_system(),
                },
            ),
            (
                MetadataRowFamily::ActiveDeletions,
                super::MetadataRow::ActiveDeletion {
                    root_inode_id: InodeId(42),
                    deletion_seq: ChangeSeq(12),
                    action: super::ActiveDeletionRowAction::Removed {
                        revocation_seq: ChangeSeq(15),
                    },
                },
            ),
            (
                MetadataRowFamily::CommitReceipts,
                super::MetadataRow::CommitReceipt {
                    commit_id: CommitId::parse("c_00000000000000000000000000000001")
                        .expect("commit id"),
                    committed_by: crate::ActorRef::loonfs_system(),
                    semantic_commit_fingerprint: "sha256:unused".to_owned(),
                    committed_seq: ChangeSeq(12),
                    committed_at_ms: 12_000,
                    message: None,
                },
            ),
            (
                MetadataRowFamily::Attributes,
                super::MetadataRow::AttributesRevision {
                    inode_id: InodeId(42),
                    attributes_revision_no: crate::AttributeRevisionNo(3),
                    committed_seq: ChangeSeq(12),
                    commit_id: row_commit_id(),
                    delta_index: 0,
                    updated_by: crate::ActorRef::loonfs_system(),
                    updated_at_ms: 12_000,
                    attributes: crate::Attributes::default(),
                },
            ),
        ];

        for (family, row) in rows {
            let row_key = row.row_key_for_family(family);
            let prefix = family.row_key_prefix();
            assert!(
                !prefix.is_empty(),
                "`{family:?}` declares no row-key prefix"
            );
            assert!(
                row_key.starts_with(prefix),
                "row key `{row_key}` for `{family:?}` does not start with `{prefix}`"
            );
        }
    }

    #[test]
    fn attribution_values_never_change_row_or_index_keys() {
        fn rows(actor: crate::ActorRef) -> Vec<(MetadataRowFamily, super::MetadataRow)> {
            vec![
                (
                    MetadataRowFamily::Inodes,
                    super::MetadataRow::Inode {
                        inode_id: InodeId(42),
                        inode_kind: crate::InodeKind::File,
                        created_seq: ChangeSeq(3),
                        commit_id: row_commit_id(),
                        created_by: actor.clone(),
                        created_at_ms: 3_000,
                    },
                ),
                (
                    MetadataRowFamily::RevisionsByInodeDesc,
                    super::MetadataRow::FileRevision {
                        inode_id: InodeId(42),
                        revision_no: crate::RevisionNo(7),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        committed_at_ms: 12_000,
                        committed_by: actor.clone(),
                        delta_index: 3,
                        content_ref: crate::ContentRef::blob_v1(
                            crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                                .expect("content id"),
                            b"attribution key test",
                        ),
                    },
                ),
                (
                    MetadataRowFamily::Tombstones,
                    super::MetadataRow::Tombstone {
                        root_inode_id: InodeId(42),
                        generation: super::TombstoneGeneration {
                            seq: ChangeSeq(12),
                            delta_index: 3,
                        },
                        commit_id: row_commit_id(),
                        action: super::TombstoneRowAction::Set {
                            deleted_direntry: None,
                        },
                        deleted_at_ms: 12_000,
                        deleted_by: actor.clone(),
                    },
                ),
                (
                    MetadataRowFamily::ActiveDeletions,
                    super::MetadataRow::ActiveDeletion {
                        root_inode_id: InodeId(42),
                        deletion_seq: ChangeSeq(12),
                        action: super::ActiveDeletionRowAction::Listed {
                            deleted_at_ms: 12_000,
                            deleted_by: actor.clone(),
                            deleted_direntry: None,
                        },
                    },
                ),
                (
                    MetadataRowFamily::Attributes,
                    super::MetadataRow::AttributesRevision {
                        inode_id: InodeId(42),
                        attributes_revision_no: crate::AttributeRevisionNo(2),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        delta_index: 3,
                        updated_by: actor,
                        updated_at_ms: 12_000,
                        attributes: crate::Attributes::default(),
                    },
                ),
            ]
        }

        let actors = [
            crate::ActorRef::user(crate::ActorId::parse("auth0|x").expect("actor id")),
            crate::ActorRef::service(
                crate::ActorId::parse("x".repeat(256)).expect("256-byte actor id"),
            ),
            crate::ActorRef::system(crate::ActorId::parse("雪-actor").expect("unicode actor id")),
        ];
        let baseline = rows(actors[0].clone());
        for actor in actors.into_iter().skip(1) {
            let changed = rows(actor);
            for ((family, baseline), (changed_family, changed)) in baseline.iter().zip(&changed) {
                assert_eq!(family, changed_family);
                assert_eq!(
                    baseline.row_key_for_family(*family),
                    changed.row_key_for_family(*family)
                );
                assert_eq!(
                    baseline.filter_key_for_family(*family),
                    changed.filter_key_for_family(*family)
                );
            }
        }
    }

    fn metadata_segment_ref(
        owner_namespace_id: &str,
        segment_id: &str,
        run_no: RunNo,
        run_seq: ChangeSeq,
        level: u32,
    ) -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: NamespaceId::parse(owner_namespace_id).expect("valid namespace id"),
            segment_id: MetadataSegmentId::parse(segment_id).expect("valid segment id"),
            compaction_job_id: None,
            run_no,
            run_seq,
            level,
            family: MetadataRowFamily::Inodes,
            segment_index: 0,
            row_count: 0,
            min_row_key: String::new(),
            max_row_key: String::new(),
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
            object_checksum: "sha256:unused".to_owned(),
        }
    }
}
