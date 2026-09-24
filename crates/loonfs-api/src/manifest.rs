//! The namespace manifest format: the durable document naming the
//! metadata segment runs that materialize one namespace file-set version
//! (format spec, "Namespace manifests").

use crate::control::{ForkBasis, NamespaceStatus, WriterBlock};
use crate::envelope::EnvelopeCodecError;
use crate::sst_blocks::BlockHandle;
use crate::wal::WalCommitPayload;
use crate::{
    AccessGrants, AccessRevisionNo, ActorId, Attributes, AttributesRevisionNo, ChangeSeq, CommitId,
    ContentId, ContentRef, DisplayName, InodeId, InodeKind, ManifestNo, MetadataSegmentId, NameKey,
    NamespaceId, RevisionNo, RunNo,
};
use crate::{PrincipalScope, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Version 1 is an uncompressed JSON envelope document carrying the payload as
/// a raw JSON fragment. `payload_checksum` covers the fragment's exact bytes.
pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 1;

/// Identifies the durable payload family carried by a namespace-manifest envelope.
///
/// See [durable object families](../../../docs/specs/format.md#a8-object-keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    /// Marks the file-set descriptor used to materialize a namespace snapshot.
    Manifest,
}

impl NamespaceManifestKind {
    /// Returns the frozen envelope discriminator written to durable storage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manifest => "manifest",
        }
    }
}

/// Selects a metadata row family and its durable lookup ordering.
///
/// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
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
    /// Stores file revisions newest-first within each inode.
    Revisions,
    /// Stores set and revoke events used to determine active subtree tombstones.
    Tombstones,
    /// Names the deletions that are recoverable right now, derived from the
    /// tombstone family and ordered by deletion time.
    ActiveDeletions,
    /// Preserves commit idempotency evidence independently of retained WAL history.
    CommitReceipts,
    /// Stores each retained commit's record in sequence order.
    Commits,
    /// Preserves evidence that content was published.
    ContentPublications,
    /// Stores inode attribute revisions newest-first.
    ///
    /// Attributes are read only in this order, so the family has no secondary
    /// index and requires no cross-family parity check.
    Attributes,
    /// Stores inode access revisions newest-first.
    ///
    /// Access rows are read only in this order, so the family has no
    /// secondary index and requires no cross-family parity check.
    Access,
}

/// Metadata families merged together as one consistency unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataFamilyGroup {
    /// Directory bindings, their child index, and unbinds.
    Bindings,
    /// File revisions.
    Revisions,
    /// Inodes.
    Inodes,
    /// Tombstones.
    Tombstones,
    /// Active deletions.
    ActiveDeletions,
    /// Commit records and their idempotency receipts, one row each per commit.
    Commits,
    /// Preserves evidence that content was published.
    ContentPublications,
    /// Attributes.
    Attributes,
    /// Access rows.
    Access,
}

impl MetadataFamilyGroup {
    /// Every family group in serialized declaration order.
    pub const ALL: [Self; 9] = [
        Self::Bindings,
        Self::Revisions,
        Self::Inodes,
        Self::Tombstones,
        Self::ActiveDeletions,
        Self::Commits,
        Self::ContentPublications,
        Self::Attributes,
        Self::Access,
    ];

    /// Returns the snake-case name used in durable keys and serialized values.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bindings => "bindings",
            Self::Revisions => "revisions",
            Self::Inodes => "inodes",
            Self::Tombstones => "tombstones",
            Self::ActiveDeletions => "active_deletions",
            Self::Commits => "commits",
            Self::ContentPublications => "content_publications",
            Self::Attributes => "attributes",
            Self::Access => "access",
        }
    }

    /// Returns the families this group merges together.
    pub const fn families(self) -> &'static [MetadataRowFamily] {
        match self {
            Self::Bindings => &[
                MetadataRowFamily::DirentryBinds,
                MetadataRowFamily::DirentryChildBinds,
                MetadataRowFamily::DirentryUnbinds,
            ],
            Self::Revisions => &[MetadataRowFamily::Revisions],
            Self::Inodes => &[MetadataRowFamily::Inodes],
            Self::Tombstones => &[MetadataRowFamily::Tombstones],
            Self::ActiveDeletions => &[MetadataRowFamily::ActiveDeletions],
            Self::Commits => &[
                MetadataRowFamily::Commits,
                MetadataRowFamily::CommitReceipts,
            ],
            Self::ContentPublications => &[MetadataRowFamily::ContentPublications],
            Self::Attributes => &[MetadataRowFamily::Attributes],
            Self::Access => &[MetadataRowFamily::Access],
        }
    }
}

/// Identifies the compaction tier that holds a metadata run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTier {
    /// Holds rows that no compaction has dropped.
    Delta,
    /// Holds rows produced by a compaction over the oldest run.
    Base,
}

/// Reference to one immutable metadata run in a namespace manifest.
///
/// See [control and manifest payloads](../../../docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRunRef {
    /// Run identity allocated by the manifest.
    pub run_no: RunNo,
    /// Namespace sequence at which this run was produced.
    pub run_seq: ChangeSeq,
    /// Compaction tier used to order overlapping runs.
    pub tier: RunTier,
    /// Segments written as part of this run.
    pub segments: Vec<MetadataSegmentRef>,
}

/// Reference to one immutable metadata segment in a namespace manifest.
///
/// See [control and manifest payloads](../../../docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSegmentRef {
    /// Namespace that stores the segment. This may be a fork source.
    pub owner_namespace_id: NamespaceId,
    /// Immutable segment id used in the durable object key.
    pub segment_id: MetadataSegmentId,
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
/// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MetadataRow {
    /// Establishes one inode's immutable identity and kind.
    Inode(InodeRecord),
    /// Records one generation of a directory name binding.
    DirentryBind(DirentryBindRecord),
    /// Retires one exact directory-binding generation.
    DirentryUnbind(DirentryUnbindRecord),
    /// Publishes one immutable content revision for a file inode.
    FileRevision(RevisionRecord),
    /// Changes whether one root inode has an active subtree tombstone.
    Tombstone(SubtreeTombstoneRecord),
    /// Derived row used to list currently recoverable deletions.
    ///
    /// Materialization writes `listed` for each tombstone set and `removed` for
    /// each revoke. This lets trash listing use an ordered range scan instead of
    /// replaying all historical deletion events.
    ActiveDeletion(ActiveDeletionRecord),
    /// Preserves the evidence needed to answer a retried logical commit.
    CommitReceipt(CommitReceiptRecord),
    /// One committed logical commit as its WAL record carries it, without inline bytes.
    /// The change feed and commit replay read this row.
    Commit(WalCommitPayload),
    /// Records a published content identity independently of its revisions.
    ContentPublication(ContentPublicationRecord),
    /// Publishes one inode's complete attribute map at one revision.
    ///
    /// The row is whole state, not a change: a reader takes the newest row
    /// for an inode and needs nothing older. An inode with no row anywhere is
    /// at revision 0 with an empty map, so nothing is written until a caller
    /// writes an attribute.
    AttributesRevision(AttributesRevisionRecord),
    /// Publishes one inode's complete access state at one revision.
    ///
    /// Whole state, like an attribute revision: a reader takes the newest
    /// row for an inode. An inode with no row anywhere is at revision 0
    /// with no boundary and no grants.
    AccessRevision(AccessRevisionRecord),
}

/// One inode's immutable identity and creation metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InodeRecord {
    /// Namespace-scoped inode identity allocated by the publishing writer.
    pub inode_id: InodeId,
    /// Classification fixed when the inode was created.
    pub inode_kind: InodeKind,
    /// Commit sequence that created the inode.
    pub committed_seq: ChangeSeq,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// Actor of the creating commit, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// Wall-clock stamp of the creating commit, in Unix milliseconds.
    pub committed_at_ms: u64,
}

/// One generation of a directory name binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirentryBindRecord {
    /// Directory in which the name was bound.
    pub parent_inode_id: InodeId,
    /// Policy-derived key used for uniqueness and lookup.
    pub name_key: NameKey,
    /// User-facing component spelling retained for directory responses.
    pub display_name: DisplayName,
    /// Inode reached while this binding generation remains active.
    pub child_inode_id: InodeId,
    /// Commit sequence that created this binding generation.
    pub bind_seq: ChangeSeq,
    /// Position that disambiguates the binding within `bind_seq`.
    pub bind_delta_index: u32,
}

/// One event that retires an exact directory-binding generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirentryUnbindRecord {
    /// Directory that held the targeted binding.
    pub parent_inode_id: InodeId,
    /// Canonical name key of the targeted binding.
    pub name_key: NameKey,
    /// User-facing spelling the retired binding carried.
    pub display_name: DisplayName,
    /// Child identity recorded by the targeted binding.
    pub child_inode_id: InodeId,
    /// Commit sequence that created the binding being retired.
    pub bind_seq: ChangeSeq,
    /// Delta position of the binding being retired.
    pub bind_delta_index: u32,
    /// Commit sequence from which this unbind takes effect.
    pub unbind_seq: ChangeSeq,
    /// Position that disambiguates the unbind within `unbind_seq`.
    pub unbind_delta_index: u32,
}

/// One immutable content revision for a file inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionRecord {
    /// File inode whose history contains the revision.
    pub inode_id: InodeId,
    /// Monotonic revision number within that file's history.
    pub revision_no: RevisionNo,
    /// Namespace sequence that published the revision.
    pub committed_seq: ChangeSeq,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// Actor that committed this revision, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// The owning commit's observational wall-clock stamp.
    pub committed_at_ms: u64,
    /// Delta position that disambiguates the revision within `committed_seq`.
    pub delta_index: u32,
    /// Immutable bytes published by the revision.
    pub content_ref: ContentRef,
}

/// One event that changes whether a root inode has an active subtree tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubtreeTombstoneRecord {
    /// Inode whose rooted subtree the event governs.
    pub root_inode_id: InodeId,
    /// Position of the event in namespace history.
    pub generation: TombstoneGeneration,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// What this event did.
    pub action: TombstoneRowAction,
    /// Actor of the commit that recorded this event.
    pub committed_by: crate::ActorId,
    /// Wall-clock stamp of the commit that recorded this event.
    pub committed_at_ms: u64,
}

/// One current-state row for a recoverable deletion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveDeletionRecord {
    /// Subtree root the deletion covers.
    pub root_inode_id: InodeId,
    /// Commit sequence of the deletion.
    pub deletion_seq: ChangeSeq,
    /// Current listing state for the deletion.
    pub action: ActiveDeletionRowAction,
}

impl ActiveDeletionRecord {
    /// Builds this row's durable key in trash-listing order.
    pub fn row_key(&self) -> String {
        lookup_keys::active_deletion_row_key(
            self.deletion_seq,
            self.root_inode_id,
            self.action.sort_rank(),
        )
    }
}

/// Evidence retained at every retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentPublicationRecord {
    /// Stored directly in the row key and Bloom filter key.
    pub content_id: ContentId,
    /// Distinguishes later publications of the same content.
    pub committed_seq: ChangeSeq,
    /// First publishing delta when a commit uses this content more than once.
    pub delta_index: u32,
}

/// Indexes one retained commit by the caller's idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitReceiptRecord {
    /// Caller idempotency key whose later reuse is checked against this row.
    pub commit_id: CommitId,
    /// Namespace sequence assigned to the accepted commit.
    pub committed_seq: ChangeSeq,
    /// Digest used to distinguish a safe retry from conflicting ID reuse.
    pub semantic_commit_fingerprint: crate::CommitFingerprint,
}

/// One inode's complete attribute map at one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributesRevisionRecord {
    /// Inode whose attributes this revision states.
    pub inode_id: InodeId,
    /// Monotonic per-inode attribute revision.
    pub attributes_revision_no: AttributesRevisionNo,
    /// Namespace sequence that published the revision.
    pub committed_seq: ChangeSeq,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// Delta position that disambiguates the revision within `committed_seq`.
    pub delta_index: u32,
    /// Actor that updated the attributes.
    pub committed_by: crate::ActorId,
    /// Time of the attribute update, in Unix milliseconds.
    pub committed_at_ms: u64,
    /// The inode's complete attribute map at this revision.
    pub attributes: Attributes,
}

/// One inode's complete access state at one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessRevisionRecord {
    /// Inode whose access this revision states.
    pub inode_id: InodeId,
    /// Monotonic per-inode access revision.
    pub access_revision_no: AccessRevisionNo,
    /// Namespace sequence that published the revision.
    pub committed_seq: ChangeSeq,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// Delta position that disambiguates the revision within `committed_seq`.
    pub delta_index: u32,
    /// Actor that updated the access state.
    pub committed_by: crate::ActorId,
    /// Time of the update, in Unix milliseconds.
    pub committed_at_ms: u64,
    /// Whether this directory stops inheritance from its ancestors.
    pub boundary: bool,
    /// The inode's complete direct grants at this revision.
    pub grants: AccessGrants,
}

/// Names one deletion generation: the commit that recorded a tombstone
/// event and the position that disambiguates it inside that commit.
///
/// Shared by the tombstone row and the WAL delta that revokes one, so a
/// revoke names its target in the same spelling everywhere.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletedBinding {
    /// Directory that held the binding.
    pub parent_inode_id: InodeId,
    /// Canonical key the binding was reachable under.
    pub name_key: NameKey,
    /// User-facing spelling the binding carried.
    pub display_name: DisplayName,
}

/// Tombstone-row event vocabulary (format spec, "Tombstones and deletion").
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TombstoneRowAction {
    /// The subtree rooted at the row's inode is deleted.
    Set {
        /// The binding the delete removed.
        deleted_binding: DeletedBinding,
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
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveDeletionRowAction {
    /// The deletion is recoverable; these are the fields the trash entry
    /// renders, denormalized so a page needs no per-entry join.
    Listed {
        /// Whether the deleted root is a file or a directory.
        inode_kind: InodeKind,
        /// Wall-clock stamp of the deleting commit. Observational, like every
        /// `committed_at_ms`.
        deleted_at_ms: u64,
        /// Actor responsible for the deletion.
        deleted_by: crate::ActorId,
        /// The binding the deletion removed, copied from the tombstone event
        /// this row derives from.
        deleted_binding: DeletedBinding,
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
    /// Returns the snake-case name used in durable keys and serialized values.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::DirentryBinds => "direntry_binds",
            Self::DirentryChildBinds => "direntry_child_binds",
            Self::DirentryUnbinds => "direntry_unbinds",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
            Self::ActiveDeletions => "active_deletions",
            Self::CommitReceipts => "commit_receipts",
            Self::Commits => "commits",
            Self::ContentPublications => "content_publications",
            Self::Attributes => "attributes",
            Self::Access => "access",
        }
    }

    /// Fixed prefix before the first variable component in this family's row
    /// keys. Compaction uses the remaining components to group rows for
    /// retention.
    pub const fn row_key_prefix(self) -> &'static str {
        match self {
            Self::Inodes => lookup_keys::INODE_ROW_PREFIX,
            Self::DirentryBinds => lookup_keys::DIRENTRY_BIND_ROW_PREFIX,
            Self::DirentryChildBinds => lookup_keys::DIRENTRY_CHILD_BIND_ROW_PREFIX,
            Self::DirentryUnbinds => lookup_keys::DIRENTRY_UNBIND_ROW_PREFIX,
            Self::Revisions => lookup_keys::REVISION_ROW_PREFIX,
            Self::Tombstones => lookup_keys::TOMBSTONE_ROW_PREFIX,
            Self::ActiveDeletions => lookup_keys::ACTIVE_DELETION_ROW_PREFIX,
            Self::CommitReceipts => lookup_keys::COMMIT_RECEIPT_ROW_PREFIX,
            Self::Commits => lookup_keys::COMMIT_ROW_PREFIX,
            Self::ContentPublications => lookup_keys::CONTENT_PUBLICATION_ROW_PREFIX,
            Self::Attributes => lookup_keys::ATTRIBUTE_ROW_PREFIX,
            Self::Access => lookup_keys::ACCESS_ROW_PREFIX,
        }
    }
}

impl MetadataRow {
    /// Builds this row's canonical durable key in its primary row family.
    ///
    /// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
    pub fn row_key(&self) -> String {
        self.row_key_for_family(match self {
            Self::Inode(_) => MetadataRowFamily::Inodes,
            Self::DirentryBind(_) => MetadataRowFamily::DirentryBinds,
            Self::DirentryUnbind(_) => MetadataRowFamily::DirentryUnbinds,
            Self::FileRevision(_) => MetadataRowFamily::Revisions,
            Self::Tombstone(_) => MetadataRowFamily::Tombstones,
            Self::ActiveDeletion(_) => MetadataRowFamily::ActiveDeletions,
            Self::CommitReceipt(_) => MetadataRowFamily::CommitReceipts,
            Self::Commit(_) => MetadataRowFamily::Commits,
            Self::ContentPublication(_) => MetadataRowFamily::ContentPublications,
            Self::AttributesRevision(_) => MetadataRowFamily::Attributes,
            Self::AccessRevision(_) => MetadataRowFamily::Access,
        })
    }

    /// Builds this row's durable key using the selected primary or secondary ordering.
    ///
    /// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
    pub fn row_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode(record) => lookup_keys::inode_key(record.inode_id),
            Self::DirentryBind(record) => match family {
                MetadataRowFamily::DirentryBinds => Some(lookup_keys::direntry_bind_row_key(
                    record.parent_inode_id,
                    record.name_key.as_str(),
                    record.bind_seq,
                    record.bind_delta_index,
                )),
                MetadataRowFamily::DirentryChildBinds => {
                    Some(lookup_keys::direntry_child_bind_row_key(
                        record.child_inode_id,
                        record.bind_seq,
                        record.bind_delta_index,
                        record.parent_inode_id,
                        record.name_key.as_str(),
                    ))
                }
                MetadataRowFamily::Inodes
                | MetadataRowFamily::DirentryUnbinds
                | MetadataRowFamily::Revisions
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::CommitReceipts
                | MetadataRowFamily::Commits
                | MetadataRowFamily::ContentPublications
                | MetadataRowFamily::Attributes
                | MetadataRowFamily::Access => None,
            }
            .expect("a direntry bind row should use a direntry bind family"),
            Self::DirentryUnbind(record) => lookup_keys::direntry_unbind_row_key(
                record.parent_inode_id,
                record.name_key.as_str(),
                record.bind_seq,
                record.bind_delta_index,
                record.unbind_seq,
                record.unbind_delta_index,
            ),
            Self::FileRevision(record) => lookup_keys::revision_row_key(
                record.inode_id,
                record.revision_no,
                record.committed_seq,
                record.delta_index,
            ),
            Self::Tombstone(record) => {
                lookup_keys::tombstone_row_key(record.root_inode_id, record.generation)
            }
            Self::ActiveDeletion(record) => lookup_keys::active_deletion_row_key(
                record.deletion_seq,
                record.root_inode_id,
                record.action.sort_rank(),
            ),
            Self::CommitReceipt(record) => {
                lookup_keys::commit_receipt_row_key(record.commit_id.as_str(), record.committed_seq)
            }
            Self::Commit(record) => lookup_keys::commit_row_key(record.seq),
            Self::ContentPublication(record) => {
                lookup_keys::content_publication_row_key(&record.content_id, record.committed_seq)
            }
            Self::AttributesRevision(record) => lookup_keys::attributes_row_key(
                record.inode_id,
                record.attributes_revision_no,
                record.committed_seq,
                record.delta_index,
            ),
            Self::AccessRevision(record) => lookup_keys::access_row_key(
                record.inode_id,
                record.access_revision_no,
                record.committed_seq,
                record.delta_index,
            ),
        }
    }

    /// Returns the Bloom filter key for this row in `family`.
    pub fn filter_key_for_family(&self, family: MetadataRowFamily) -> String {
        match self {
            Self::Inode(_) => self.row_key_for_family(family),
            Self::DirentryBind(record) => match family {
                MetadataRowFamily::DirentryBinds => Some(lookup_keys::direntry_bind_probe(
                    record.parent_inode_id,
                    record.name_key.as_str(),
                )),
                MetadataRowFamily::DirentryChildBinds => {
                    Some(lookup_keys::direntry_child_probe(record.child_inode_id))
                }
                MetadataRowFamily::Inodes
                | MetadataRowFamily::DirentryUnbinds
                | MetadataRowFamily::Revisions
                | MetadataRowFamily::Tombstones
                | MetadataRowFamily::ActiveDeletions
                | MetadataRowFamily::CommitReceipts
                | MetadataRowFamily::Commits
                | MetadataRowFamily::ContentPublications
                | MetadataRowFamily::Attributes
                | MetadataRowFamily::Access => None,
            }
            .expect("a direntry bind row should use a direntry bind family"),
            Self::DirentryUnbind(record) => {
                lookup_keys::direntry_unbind_probe(record.parent_inode_id, record.name_key.as_str())
            }
            Self::FileRevision(record) => lookup_keys::revision_probe(record.inode_id),
            Self::Tombstone(record) => lookup_keys::tombstone_probe(record.root_inode_id),
            // The family is only ever range-scanned in key order, never
            // probed for one deletion, so the filter key is the row key.
            Self::ActiveDeletion(_) => self.row_key_for_family(family),
            Self::CommitReceipt(record) => {
                lookup_keys::commit_receipt_probe(record.commit_id.as_str())
            }
            Self::Commit(_) => self.row_key_for_family(family),
            Self::ContentPublication(record) => {
                lookup_keys::content_publication_probe(&record.content_id)
            }
            Self::AttributesRevision(record) => lookup_keys::attributes_probe(record.inode_id),
            Self::AccessRevision(record) => lookup_keys::access_probe(record.inode_id),
        }
    }
}

/// Encodes an arbitrary string so it can occupy one component of a durable row key.
///
/// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
pub fn hex_encode_row_key_component(value: &str) -> String {
    crate::hex::hex_encode_bytes(value.as_bytes())
}

/// Builders for metadata row keys, lookup prefixes, and Bloom filter probes.
///
/// See [metadata rows and row keys](../../../docs/specs/format.md#a6-metadata-rows-and-row-keys).
pub mod lookup_keys {
    use super::{hex_encode_row_key_component, TombstoneGeneration};
    use crate::{
        AccessRevisionNo, AttributesRevisionNo, ChangeSeq, ContentId, InodeId, RevisionNo,
    };

    /// Prefix for inode row keys.
    pub const INODE_ROW_PREFIX: &str = "inode-";

    /// Prefix for revision row keys.
    pub const REVISION_ROW_PREFIX: &str = "revision-";

    /// Prefix for commit row keys.
    pub const COMMIT_ROW_PREFIX: &str = "commit-";

    pub(super) const DIRENTRY_BIND_ROW_PREFIX: &str = "direntry-bind-";
    pub(super) const DIRENTRY_CHILD_BIND_ROW_PREFIX: &str = "direntry-child-bind-";
    pub(super) const DIRENTRY_UNBIND_ROW_PREFIX: &str = "direntry-unbind-";
    pub(super) const TOMBSTONE_ROW_PREFIX: &str = "tombstone-";
    pub(super) const CONTENT_PUBLICATION_ROW_PREFIX: &str = "content-publication-";
    pub(super) const COMMIT_RECEIPT_ROW_PREFIX: &str = "commit-receipt-";
    pub(super) const ATTRIBUTE_ROW_PREFIX: &str = "attribute-";
    pub(super) const ACCESS_ROW_PREFIX: &str = "access-";

    /// Builds the exclusive lower bound after `row_key`.
    pub fn after_row_key(row_key: &str) -> String {
        format!("{row_key}\0")
    }

    /// Builds a commit row key.
    pub fn commit_row_key(seq: ChangeSeq) -> String {
        format!("{COMMIT_ROW_PREFIX}{:020}", seq.0)
    }

    /// Builds an inode row key.
    pub fn inode_key(inode_id: InodeId) -> String {
        format!("{INODE_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds a scan bound immediately after an inode row.
    pub fn inode_key_after(inode_id: InodeId) -> String {
        after_row_key(&inode_key(inode_id))
    }

    /// Builds the prefix for directory bindings under one parent.
    pub fn direntry_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("{DIRENTRY_BIND_ROW_PREFIX}{:020}-", parent_inode_id.0)
    }

    /// Builds the Bloom filter probe for a parent/name binding.
    pub fn direntry_bind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "{}{}",
            direntry_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key)
        )
    }

    /// Builds the prefix for every generation of a parent/name binding.
    pub fn direntry_bind_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!("{}-", direntry_bind_probe(parent_inode_id, name_key))
    }

    /// Builds a row key for one generation of a parent/name binding.
    pub fn direntry_bind_row_key(
        parent_inode_id: InodeId,
        name_key: &str,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{bind_delta_index:010}",
            direntry_bind_prefix(parent_inode_id, name_key),
            bind_seq.0
        )
    }

    /// Builds the Bloom filter probe for bindings to one child inode.
    pub fn direntry_child_probe(child_inode_id: InodeId) -> String {
        format!("{DIRENTRY_CHILD_BIND_ROW_PREFIX}{:020}", child_inode_id.0)
    }

    /// Builds the reverse-index prefix for bindings to one child inode.
    pub fn direntry_child_prefix(child_inode_id: InodeId) -> String {
        format!("{}-", direntry_child_probe(child_inode_id))
    }

    /// Builds a reverse-index row key for one binding generation.
    pub(super) fn direntry_child_bind_row_key(
        child_inode_id: InodeId,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> String {
        format!(
            "{}{:020}-{bind_delta_index:010}-{:020}-{}",
            direntry_child_prefix(child_inode_id),
            bind_seq.0,
            parent_inode_id.0,
            hex_encode_row_key_component(name_key)
        )
    }

    /// Builds the Bloom filter probe for unbinds of one parent/name pair.
    pub fn direntry_unbind_probe(parent_inode_id: InodeId, name_key: &str) -> String {
        format!(
            "{}{}",
            direntry_unbind_parent_prefix(parent_inode_id),
            hex_encode_row_key_component(name_key)
        )
    }

    /// Builds the prefix for unbinds of one binding generation.
    pub fn direntry_unbind_binding_prefix(
        parent_inode_id: InodeId,
        name_key: &str,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{bind_delta_index:010}-",
            direntry_unbind_name_prefix(parent_inode_id, name_key),
            bind_seq.0
        )
    }

    /// Builds a row key for one unbind event.
    pub(super) fn direntry_unbind_row_key(
        parent_inode_id: InodeId,
        name_key: &str,
        bind_seq: ChangeSeq,
        bind_delta_index: u32,
        unbind_seq: ChangeSeq,
        unbind_delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{unbind_delta_index:010}",
            direntry_unbind_binding_prefix(parent_inode_id, name_key, bind_seq, bind_delta_index),
            unbind_seq.0
        )
    }

    /// Builds the prefix for unbinds below one parent directory.
    pub(super) fn direntry_unbind_parent_prefix(parent_inode_id: InodeId) -> String {
        format!("{DIRENTRY_UNBIND_ROW_PREFIX}{:020}-", parent_inode_id.0)
    }

    /// Builds the prefix for unbinds of one parent/name pair.
    pub fn direntry_unbind_name_prefix(parent_inode_id: InodeId, name_key: &str) -> String {
        format!("{}-", direntry_unbind_probe(parent_inode_id, name_key))
    }

    /// Builds the Bloom filter probe for one tombstone root.
    pub fn tombstone_probe(root_inode_id: InodeId) -> String {
        format!("{TOMBSTONE_ROW_PREFIX}{:020}", root_inode_id.0)
    }

    /// Builds the prefix for a root inode's tombstone history.
    pub fn tombstone_prefix(root_inode_id: InodeId) -> String {
        format!("{}-", tombstone_probe(root_inode_id))
    }

    /// Builds a row key for one tombstone event.
    ///
    /// The action is stored in the value, so delete and revoke rows for one
    /// generation share a key.
    pub(super) fn tombstone_row_key(
        root_inode_id: InodeId,
        generation: TombstoneGeneration,
    ) -> String {
        format!(
            "{}{:020}-{:010}",
            tombstone_prefix(root_inode_id),
            generation.seq.0,
            generation.delta_index
        )
    }

    /// Prefix for active-deletion row keys.
    pub const ACTIVE_DELETION_ROW_PREFIX: &str = "active-deletion-";

    /// Rank of an undelete's removal marker within one deletion generation.
    /// It is the lowest rank on purpose: an ascending scan sees the removal
    /// before the row it removes, so a page never lists a deletion whose
    /// marker was going to arrive one page later.
    pub(super) const ACTIVE_DELETION_RANK_REMOVED: u32 = 0;

    /// Rank of the listed row within one deletion generation, and the highest
    /// rank the family defines.
    pub(super) const ACTIVE_DELETION_RANK_LISTED: u32 = 1;

    /// Builds an active-deletion row key.
    pub(super) fn active_deletion_row_key(
        deletion_seq: ChangeSeq,
        root_inode_id: InodeId,
        sort_rank: u32,
    ) -> String {
        format!(
            "{ACTIVE_DELETION_ROW_PREFIX}{:020}-{:020}-{sort_rank:010}",
            deletion_seq.0, root_inode_id.0
        )
    }

    /// Builds a trash scan bound after one deletion generation.
    pub fn active_deletion_key_after(deletion_seq: ChangeSeq, root_inode_id: InodeId) -> String {
        after_row_key(&active_deletion_row_key(
            deletion_seq,
            root_inode_id,
            ACTIVE_DELETION_RANK_LISTED,
        ))
    }

    /// Selects all publications of one content identity in the Bloom filter.
    pub fn content_publication_probe(content_id: &ContentId) -> String {
        format!("{CONTENT_PUBLICATION_ROW_PREFIX}{content_id}")
    }

    /// Selects the rows for one content identity.
    pub fn content_publication_prefix(content_id: &ContentId) -> String {
        format!("{}-", content_publication_probe(content_id))
    }

    /// Orders publications by content identity and commit sequence.
    pub(super) fn content_publication_row_key(
        content_id: &ContentId,
        committed_seq: ChangeSeq,
    ) -> String {
        format!(
            "{}{:020}",
            content_publication_prefix(content_id),
            committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for one commit ID.
    pub fn commit_receipt_probe(commit_id: &str) -> String {
        format!(
            "{COMMIT_RECEIPT_ROW_PREFIX}{}",
            hex_encode_row_key_component(commit_id)
        )
    }

    /// Builds the prefix for receipts with one commit ID.
    pub fn commit_receipt_prefix(commit_id: &str) -> String {
        format!("{}-", commit_receipt_probe(commit_id))
    }

    /// Builds a commit receipt row key.
    pub(super) fn commit_receipt_row_key(commit_id: &str, committed_seq: ChangeSeq) -> String {
        format!(
            "{}{:020}",
            commit_receipt_prefix(commit_id),
            committed_seq.0
        )
    }

    /// Builds the Bloom filter probe for an inode's revisions.
    pub fn revision_probe(inode_id: InodeId) -> String {
        format!("{REVISION_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first revisions.
    pub fn revision_prefix(inode_id: InodeId) -> String {
        format!("{}-", revision_probe(inode_id))
    }

    /// Builds a prefix for one revision number within an inode.
    pub fn revision_number_prefix(inode_id: InodeId, revision_no: RevisionNo) -> String {
        format!(
            "{}{:020}-",
            revision_prefix(inode_id),
            u64::MAX - revision_no.0
        )
    }

    /// Builds a newest-first revision row key.
    pub fn revision_row_key(
        inode_id: InodeId,
        revision_no: RevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:010}",
            revision_number_prefix(inode_id, revision_no),
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }

    /// Builds the Bloom filter probe for an inode's attribute revisions.
    pub fn attributes_probe(inode_id: InodeId) -> String {
        format!("{ATTRIBUTE_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first attribute revisions.
    pub fn attributes_prefix(inode_id: InodeId) -> String {
        format!("{}-", attributes_probe(inode_id))
    }

    /// Builds a row key for an attribute revision.
    pub(super) fn attributes_row_key(
        inode_id: InodeId,
        attributes_revision_no: AttributesRevisionNo,
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

    /// Builds the Bloom filter probe for an inode's access revisions.
    pub fn access_probe(inode_id: InodeId) -> String {
        format!("{ACCESS_ROW_PREFIX}{:020}", inode_id.0)
    }

    /// Builds the prefix for an inode's newest-first access revisions.
    pub fn access_prefix(inode_id: InodeId) -> String {
        format!("{}-", access_probe(inode_id))
    }

    /// Builds a row key for an access revision.
    pub(super) fn access_row_key(
        inode_id: InodeId,
        access_revision_no: AccessRevisionNo,
        committed_seq: ChangeSeq,
        delta_index: u32,
    ) -> String {
        format!(
            "{}{:020}-{:020}-{:010}",
            access_prefix(inode_id),
            u64::MAX - access_revision_no.0,
            u64::MAX - committed_seq.0,
            u32::MAX - delta_index
        )
    }
}

/// A namespace's access mode, fixed at creation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceAccess {
    /// Every caller holding the deployment credential may do everything.
    Unrestricted {},
    /// Access rows govern every operation.
    Acl {
        /// Identity domain the namespace's principal ids belong to.
        principal_scope: PrincipalScope,
        /// The root inode's grants at genesis, normally `admin` for each
        /// initial administrator.
        root_grants: AccessGrants,
    },
}

impl NamespaceAccess {
    /// The unrestricted access mode.
    pub fn unrestricted() -> Self {
        Self::Unrestricted {}
    }

    /// Whether every caller holding the deployment credential may do everything.
    pub const fn is_unrestricted(&self) -> bool {
        matches!(self, Self::Unrestricted {})
    }
}

/// Stores a counter within the public integer bound.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct ActivityCounter(u64);

impl ActivityCounter {
    /// Rejects values above [`crate::MAX_PUBLIC_INTEGER`].
    pub fn parse(value: u64) -> Result<Self, crate::PublicOrdinalRangeError> {
        if value > crate::MAX_PUBLIC_INTEGER {
            return Err(crate::PublicOrdinalRangeError);
        }
        Ok(Self(value))
    }

    /// Returns the validated integer.
    pub fn get(self) -> u64 {
        self.0
    }

    /// Returns `None` if the sum exceeds the public integer bound.
    pub fn checked_add(self, value: u64) -> Option<Self> {
        Self::parse(self.0.checked_add(value)?).ok()
    }
}

impl<'de> Deserialize<'de> for ActivityCounter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::parse(u64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// Activity committed in this namespace through a manifest's folded position.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestActivity {
    /// Full content length of every committed file revision, including reused content.
    pub content_bytes: ActivityCounter,
    /// Number of committed file revisions, including empty revisions.
    pub file_revisions: ActivityCounter,
    /// Number of durable semantic operations, rather than requests or raw deltas.
    pub mutations: ActivityCounter,
}

impl ManifestActivity {
    /// Adds activity, returning `None` if any counter overflows.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            content_bytes: self.content_bytes.checked_add(other.content_bytes.get())?,
            file_revisions: self
                .file_revisions
                .checked_add(other.file_revisions.get())?,
            mutations: self.mutations.checked_add(other.mutations.get())?,
        })
    }
}

/// Carries one complete namespace file-set description inside a manifest envelope.
///
/// See [manifest publication](../../../docs/specs/format.md#72-publishing-a-materialized-file-set).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceManifestPayload {
    /// Namespace whose materialized state this manifest describes.
    pub namespace_id: NamespaceId,
    /// Namespace creation stamp in Unix milliseconds.
    pub created_at_ms: u64,
    /// Actor that created the namespace, as supplied by the application.
    pub created_by: ActorId,
    /// Access mode, fixed for the namespace.
    pub access: NamespaceAccess,
    /// Fork provenance and source checkpoint identity for this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_basis: Option<ForkBasis>,
    /// Lifecycle state of this namespace.
    pub status: NamespaceStatus,
    /// Writer that acquired the current epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<WriterBlock>,
    /// Highest WAL number represented by the runs.
    pub folded_wal_no: WalNo,
    /// Positive publication number matching the manifest object key.
    pub manifest_no: ManifestNo,
    /// Stops a stale streaming compactor at its next check when a newer runtime claims it.
    /// Streaming compaction rebuilds a whole family group and publishes once at the end.
    /// Grep publishes each bounded step, so a lost race costs only one step.
    pub compactor_epoch: u64,
    /// Materialized head sequence, or the final namespace sequence on deletion.
    pub head_seq: ChangeSeq,
    /// Activity committed in this namespace through `head_seq`.
    pub activity: ManifestActivity,
    /// Oldest run sequence still represented by `runs`.
    pub base_seq: ChangeSeq,
    /// Current writer fencing epoch.
    pub writer_epoch: WriterEpoch,
    /// First inode identity available after replaying the manifest snapshot.
    pub next_inode_id: InodeId,
    /// Run number the next producer allocates. Every run's `run_no` is below it.
    pub next_run_no: RunNo,
    /// Earliest sequence for which retained history remains readable.
    pub retention_floor_seq: ChangeSeq,
    /// Complete set of metadata runs required to reconstruct the snapshot.
    pub runs: Vec<MetadataRunRef>,
}

/// A manifest breaks a rule of the namespace's manifest chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestChainError {
    /// Which field breaks the rule.
    pub field: String,
}

impl fmt::Display for ManifestChainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "manifest `{}` does not follow the namespace's manifest chain",
            self.field
        )
    }
}

impl std::error::Error for ManifestChainError {}

impl NamespaceManifestPayload {
    /// Constructs manifest 1 with the root inode reserved.
    pub fn initial(
        namespace_id: NamespaceId,
        created_at_ms: u64,
        created_by: ActorId,
        access: NamespaceAccess,
    ) -> Self {
        Self {
            namespace_id,
            created_at_ms,
            created_by,
            access,
            fork_basis: None,
            status: NamespaceStatus::Active {},
            writer: None,
            manifest_no: ManifestNo(1),
            compactor_epoch: 0,
            head_seq: ChangeSeq(0),
            activity: ManifestActivity::default(),
            base_seq: ChangeSeq(0),
            writer_epoch: WriterEpoch(0),
            next_inode_id: crate::FIRST_ALLOCATABLE_INODE_ID,
            next_run_no: RunNo(0),
            folded_wal_no: WalNo(0),
            retention_floor_seq: ChangeSeq(0),
            runs: Vec::new(),
        }
    }

    /// Rejects an initial manifest with writer authority or prior activity.
    pub fn ensure_first_manifest(&self) -> Result<(), ManifestChainError> {
        let invalid = |field: &str| {
            Err(ManifestChainError {
                field: field.to_owned(),
            })
        };
        if self.manifest_no != ManifestNo(1) {
            return invalid("manifest_no");
        }
        if self.status.is_deleted() {
            return invalid("status");
        }
        if self.writer.is_some() {
            return invalid("writer");
        }
        if self.activity != ManifestActivity::default() {
            return invalid("activity");
        }
        match &self.fork_basis {
            Some(basis) => {
                if basis.manifest.head_seq != self.head_seq
                    || self.retention_floor_seq != self.head_seq
                {
                    return invalid("head_seq");
                }
            }
            None => {
                if !self.runs.is_empty() {
                    return invalid("runs");
                }
                if self.head_seq != ChangeSeq(0)
                    || self.base_seq != ChangeSeq(0)
                    || self.retention_floor_seq != ChangeSeq(0)
                {
                    return invalid("head_seq");
                }
                if self.next_inode_id != crate::FIRST_ALLOCATABLE_INODE_ID {
                    return invalid("next_inode_id");
                }
                if self.next_run_no != RunNo(0) {
                    return invalid("next_run_no");
                }
            }
        }
        Ok(())
    }

    /// Rejects changed identity, decreasing counters, and publication after deletion.
    pub fn ensure_successor(&self, successor: &Self) -> Result<(), ManifestChainError> {
        let invalid = |field: &str| {
            Err(ManifestChainError {
                field: field.to_owned(),
            })
        };
        if successor.namespace_id != self.namespace_id {
            return invalid("namespace_id");
        }
        if self.manifest_no.successor().ok() != Some(successor.manifest_no) {
            return invalid("manifest_no");
        }
        if successor.folded_wal_no < self.folded_wal_no {
            return invalid("folded_wal_no");
        }
        if successor.writer_epoch < self.writer_epoch {
            return invalid("writer_epoch");
        }
        if successor.compactor_epoch < self.compactor_epoch {
            return invalid("compactor_epoch");
        }
        if successor.head_seq < self.head_seq {
            return invalid("head_seq");
        }
        if successor.retention_floor_seq < self.retention_floor_seq {
            return invalid("retention_floor_seq");
        }
        if successor.next_inode_id < self.next_inode_id {
            return invalid("next_inode_id");
        }
        if successor.next_run_no < self.next_run_no {
            return invalid("next_run_no");
        }
        if successor.created_at_ms != self.created_at_ms {
            return invalid("created_at_ms");
        }
        if successor.created_by != self.created_by {
            return invalid("created_by");
        }
        if successor.access != self.access {
            return invalid("access");
        }
        if successor.fork_basis != self.fork_basis {
            return invalid("fork_basis");
        }
        if self.status.is_deleted() {
            return invalid("status");
        }
        // Activity only grows, and only when the head folds new commits.
        let (before, after) = (&self.activity, &successor.activity);
        if after.content_bytes < before.content_bytes
            || after.file_revisions < before.file_revisions
            || after.mutations < before.mutations
            || (successor.head_seq == self.head_seq && after != before)
        {
            return invalid("activity");
        }
        Ok(())
    }
}

/// A manifest decoded through its checked durable codec.
pub type NamespaceManifestEnvelope = crate::envelope::VerifiedEnvelope<NamespaceManifestPayload>;

/// Encodes a manifest once and returns its immutable framing and durable bytes.
pub fn encode_namespace_manifest_json(
    payload: NamespaceManifestPayload,
) -> Result<crate::envelope::EncodedEnvelope<NamespaceManifestPayload>, EnvelopeCodecError> {
    crate::envelope::encode_json_envelope(
        NamespaceManifestKind::Manifest.as_str(),
        NAMESPACE_MANIFEST_FORMAT_VERSION,
        payload,
    )
}

/// Decodes and verifies a durable namespace-manifest JSON envelope.
///
/// Decoding fails for invalid JSON, the wrong kind or version, a checksum
/// mismatch, or an invalid payload. See
/// [manifest publication](../../../docs/specs/format.md#72-publishing-a-materialized-file-set).
pub fn decode_namespace_manifest_json(
    bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, EnvelopeCodecError> {
    let expected_kind = NamespaceManifestKind::Manifest;
    let decoded =
        crate::envelope::decode_json_envelope(bytes, NAMESPACE_MANIFEST_FORMAT_VERSION, |found| {
            crate::envelope::verify_kind(expected_kind.as_str(), found)
        })?;

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, encode_namespace_manifest_json, BlockHandle,
        MetadataRowFamily, MetadataRunRef, MetadataSegmentRef, NamespaceManifestPayload, RunTier,
    };
    use crate::{
        ChangeSeq, CommitId, InodeId, ManifestNo, MetadataSegmentId, NameKey, NamespaceId, RunNo,
        WriterEpoch,
    };

    fn row_commit_id() -> CommitId {
        CommitId::parse("c_metadata_row").expect("commit id")
    }

    fn deleted_binding() -> super::DeletedBinding {
        super::DeletedBinding {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: crate::DisplayName::parse("report.txt").expect("valid display name"),
        }
    }

    #[test]
    fn activity_arithmetic_and_successors_reject_invalid_totals() {
        let counter = super::ActivityCounter::parse(crate::MAX_PUBLIC_INTEGER).expect("maximum");
        let maximum = super::ManifestActivity {
            content_bytes: counter,
            file_revisions: counter,
            mutations: counter,
        };
        let initial = NamespaceManifestPayload::initial(
            NamespaceId::parse("activity").expect("namespace"),
            0,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::unrestricted(),
        );
        assert_eq!(maximum.checked_add(Default::default()), Some(maximum));
        let mut successor = initial.clone();
        successor.manifest_no = ManifestNo(2);
        initial
            .ensure_successor(&successor)
            .expect("unchanged activity");
        successor.activity = maximum;
        assert_eq!(
            initial
                .ensure_successor(&successor)
                .expect_err("activity without a head advance")
                .field,
            "activity"
        );
        successor.head_seq = ChangeSeq(1);
        initial
            .ensure_successor(&successor)
            .expect("folded activity");
        let (envelope, encoded) = encode_namespace_manifest_json(successor.clone())
            .expect("encode")
            .into_parts();
        assert_eq!(
            decode_namespace_manifest_json(&encoded).expect("decode"),
            envelope
        );

        for field in ["content_bytes", "file_revisions", "mutations"] {
            let mut increment =
                serde_json::to_value(super::ManifestActivity::default()).expect("value");
            increment[field] = 1.into();
            let increment = serde_json::from_value(increment).expect("increment");
            assert_eq!(maximum.checked_add(increment), None);

            let mut payload = serde_json::to_value(&successor).expect("value");
            payload["activity"][field] = (crate::MAX_PUBLIC_INTEGER - 1).into();
            let mut regression: NamespaceManifestPayload =
                serde_json::from_value(payload.clone()).expect("regression");
            regression.manifest_no = ManifestNo(3);
            regression.head_seq = ChangeSeq(2);
            assert!(successor.ensure_successor(&regression).is_err());
            assert_eq!(regression.activity.checked_add(increment), Some(maximum));

            payload["activity"][field] = (crate::MAX_PUBLIC_INTEGER + 1).into();
            let (_, encoded) = crate::envelope::encode_json_envelope(
                super::NamespaceManifestKind::Manifest.as_str(),
                super::NAMESPACE_MANIFEST_FORMAT_VERSION,
                payload,
            )
            .expect("encode invalid payload")
            .into_parts();
            assert!(matches!(
                decode_namespace_manifest_json(&encoded),
                Err(crate::envelope::EnvelopeCodecError::PayloadDecode(message))
                    if message.contains(&crate::PublicOrdinalRangeError.to_string())
            ));
        }
    }

    #[test]
    fn successor_preserves_identity() {
        let initial = NamespaceManifestPayload::initial(
            NamespaceId::parse("original").expect("namespace"),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        );
        let mut next = initial.clone();
        next.manifest_no = ManifestNo(2);
        for (field, change) in [
            ("namespace_id", 0),
            ("created_at_ms", 1),
            ("fork_basis", 2),
            ("access", 3),
        ] {
            let mut successor = next.clone();
            match change {
                0 => successor.namespace_id = NamespaceId::parse("changed").expect("namespace"),
                1 => successor.created_at_ms += 1,
                2 => {
                    successor.fork_basis = Some(crate::control::ForkBasis {
                        manifest: crate::control::ManifestRef {
                            owner_namespace_id: NamespaceId::parse("source").expect("namespace"),
                            manifest_no: ManifestNo(1),
                            head_seq: ChangeSeq(0),
                            payload_checksum: "sha256:source".to_owned(),
                        },
                        source_pin_id: crate::PinId::parse(
                            "pin_00000000000000000001-0000000000000001",
                        )
                        .expect("checkpoint"),
                    })
                }
                _ => {
                    successor.access = super::NamespaceAccess::Acl {
                        principal_scope: crate::PrincipalScope::parse("org_test").expect("scope"),
                        root_grants: crate::AccessGrants::default(),
                    }
                }
            }
            assert_eq!(
                initial
                    .ensure_successor(&successor)
                    .expect_err("identity drift")
                    .field,
                field
            );
        }
        let mut deleted = next.clone();
        deleted.status = crate::control::NamespaceStatus::Deleted {
            deleted_at_ms: 1_500,
        };
        initial.ensure_successor(&deleted).expect("delete");
        let mut after_tombstone = deleted.clone();
        after_tombstone.manifest_no = ManifestNo(3);
        assert_eq!(
            deleted
                .ensure_successor(&after_tombstone)
                .expect_err("a tombstone ends its namespace")
                .field,
            "status"
        );
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
        let kind = super::NamespaceManifestKind::Manifest;
        let serialized = serde_json::to_value(kind).expect("serialize kind");
        assert_eq!(serialized, serde_json::Value::from(kind.as_str()));
    }

    #[test]
    fn namespace_manifest_codec_round_trips_base_only_materialization() {
        let (envelope, encoded) = encode_namespace_manifest_json(NamespaceManifestPayload {
            activity: Default::default(),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            access: super::NamespaceAccess::Unrestricted {},
            fork_basis: None,
            status: crate::control::NamespaceStatus::Active {},
            writer: None,
            folded_wal_no: crate::WalNo(0),
            compactor_epoch: 0,
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(10),

            head_seq: ChangeSeq(10),
            base_seq: ChangeSeq(10),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(1),
            retention_floor_seq: ChangeSeq(0),
            runs: vec![metadata_run_ref(
                "demo",
                "seg_00000000000000000000000000000001",
                RunNo(0),
                ChangeSeq(10),
                RunTier::Base,
            )],
        })
        .expect("manifest")
        .into_parts();
        let document: serde_json::Value =
            serde_json::from_slice(&encoded).expect("decode manifest document");
        assert!(document["payload"]
            .get("frozen_base_delta_merges")
            .is_none());
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.base_seq, ChangeSeq(10));
        assert_eq!(decoded.payload.runs.len(), 1);
        assert_eq!(decoded.payload.runs[0].run_seq, ChangeSeq(10));
    }

    #[test]
    fn namespace_manifest_codec_round_trips_inherited_source_segments() {
        let (envelope, encoded) = encode_namespace_manifest_json(NamespaceManifestPayload {
            activity: Default::default(),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            access: super::NamespaceAccess::Unrestricted {},
            fork_basis: None,
            status: crate::control::NamespaceStatus::Active {},
            writer: None,
            folded_wal_no: crate::WalNo(0),
            compactor_epoch: 0,
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(12),

            head_seq: ChangeSeq(12),
            base_seq: ChangeSeq(10),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(2),
            retention_floor_seq: ChangeSeq(0),
            runs: vec![
                metadata_run_ref(
                    "source",
                    "seg_00000000000000000000000000000001",
                    RunNo(0),
                    ChangeSeq(10),
                    RunTier::Base,
                ),
                metadata_run_ref(
                    "demo",
                    "seg_00000000000000000000000000000002",
                    RunNo(1),
                    ChangeSeq(12),
                    RunTier::Delta,
                ),
            ],
        })
        .expect("manifest")
        .into_parts();
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.runs[0].tier, RunTier::Base);
        assert_eq!(decoded.payload.runs[1].tier, RunTier::Delta);
        assert_eq!(decoded.payload.runs[1].run_seq, ChangeSeq(12));
        assert_eq!(
            decoded.payload.runs[0].segments[0].owner_namespace_id,
            NamespaceId::parse("source").expect("valid namespace id")
        );
    }

    #[test]
    fn direntry_bind_row_key_supports_parent_and_child_indexes() {
        let row = super::MetadataRow::DirentryBind(super::DirentryBindRecord {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report.txt").expect("valid name key"),
            display_name: crate::DisplayName::parse("Report.txt").expect("valid display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        });

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
        let row = super::MetadataRow::DirentryBind(super::DirentryBindRecord {
            parent_inode_id: InodeId(9),
            name_key: NameKey::parse("report-2024").expect("valid name key"),
            display_name: crate::DisplayName::parse("report-2024").expect("valid display name"),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        });

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::DirentryBinds),
            "direntry-bind-00000000000000000009-7265706f72742d32303234-00000000000000000017-0000000003"
        );
    }

    #[test]
    fn revision_row_key_orders_newest_first_within_each_inode() {
        let row = super::MetadataRow::FileRevision(super::RevisionRecord {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorId::loonfs(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                    .expect("valid content id"),
                b"row key sample",
            ),
        });

        assert_eq!(
            row.row_key_for_family(MetadataRowFamily::Revisions),
            "revision-00000000000000000042-18446744073709551608-18446744073709551603-4294967292"
        );
    }

    #[test]
    fn whole_state_row_keys_sort_newest_revision_first_under_the_inode_prefix() {
        let row_of = |revision: u64, seq: u64, delta_index: u32| {
            super::MetadataRow::AttributesRevision(super::AttributesRevisionRecord {
                inode_id: InodeId(42),
                attributes_revision_no: crate::AttributesRevisionNo(revision),
                committed_seq: ChangeSeq(seq),
                commit_id: row_commit_id(),
                delta_index,
                committed_by: crate::ActorId::loonfs(),
                committed_at_ms: 12_000 + seq,
                attributes: crate::Attributes::default(),
            })
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

        let access_row = |revision, seq, delta_index| {
            super::MetadataRow::AccessRevision(super::AccessRevisionRecord {
                inode_id: InodeId(42),
                access_revision_no: crate::AccessRevisionNo(revision),
                committed_seq: ChangeSeq(seq),
                commit_id: crate::CommitId::parse("c_access").expect("commit"),
                delta_index,
                committed_by: crate::ActorId::loonfs(),
                committed_at_ms: 1_000,
                boundary: false,
                grants: crate::AccessGrants::default(),
            })
        };
        let newest = access_row(3, 12, 1);
        let older = access_row(2, 11, 0);
        assert_eq!(
            newest.row_key(),
            "access-00000000000000000042-18446744073709551612-18446744073709551603-4294967294"
        );
        assert!(newest.row_key() < older.row_key());
        assert_eq!(
            newest.filter_key_for_family(MetadataRowFamily::Access),
            super::lookup_keys::access_probe(InodeId(42))
        );
    }

    #[test]
    fn row_key_prefixes_match_the_row_keys_they_front() {
        let name_key = NameKey::parse("report.txt").expect("valid name key");
        let display_name = crate::DisplayName::parse("report.txt").expect("valid display name");
        let bind = super::MetadataRow::DirentryBind(super::DirentryBindRecord {
            parent_inode_id: InodeId(9),
            name_key: name_key.clone(),
            display_name: display_name.clone(),
            child_inode_id: InodeId(42),
            bind_seq: ChangeSeq(17),
            bind_delta_index: 3,
        });
        let revision = super::MetadataRow::FileRevision(super::RevisionRecord {
            inode_id: InodeId(42),
            revision_no: crate::RevisionNo(7),
            committed_seq: ChangeSeq(12),
            commit_id: row_commit_id(),
            committed_at_ms: 12_000,
            committed_by: crate::ActorId::loonfs(),
            delta_index: 3,
            content_ref: crate::ContentRef::blob_v1(
                crate::NamespaceId::parse("demo").expect("namespace id"),
                crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                    .expect("valid content id"),
                b"row key prefix sample",
            ),
        });
        let rows: [(MetadataRowFamily, super::MetadataRow); 10] = [
            (
                MetadataRowFamily::Inodes,
                super::MetadataRow::Inode(super::InodeRecord {
                    inode_id: InodeId(42),
                    inode_kind: crate::InodeKind::File,
                    committed_seq: ChangeSeq(3),
                    commit_id: row_commit_id(),
                    committed_by: crate::ActorId::loonfs(),
                    committed_at_ms: 3_000,
                }),
            ),
            (MetadataRowFamily::DirentryBinds, bind.clone()),
            (MetadataRowFamily::DirentryChildBinds, bind),
            (
                MetadataRowFamily::DirentryUnbinds,
                super::MetadataRow::DirentryUnbind(super::DirentryUnbindRecord {
                    parent_inode_id: InodeId(9),
                    name_key,
                    display_name,
                    child_inode_id: InodeId(42),
                    bind_seq: ChangeSeq(17),
                    bind_delta_index: 3,
                    unbind_seq: ChangeSeq(19),
                    unbind_delta_index: 0,
                }),
            ),
            (MetadataRowFamily::Revisions, revision),
            (
                MetadataRowFamily::ContentPublications,
                super::MetadataRow::ContentPublication(super::ContentPublicationRecord {
                    content_id: crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                        .expect("valid content id"),
                    committed_seq: ChangeSeq(12),
                    delta_index: 3,
                }),
            ),
            (
                MetadataRowFamily::Tombstones,
                super::MetadataRow::Tombstone(super::SubtreeTombstoneRecord {
                    root_inode_id: InodeId(42),
                    generation: super::TombstoneGeneration {
                        seq: ChangeSeq(12),
                        delta_index: 0,
                    },
                    commit_id: row_commit_id(),
                    action: super::TombstoneRowAction::Set {
                        deleted_binding: deleted_binding(),
                    },
                    committed_at_ms: 12_000,
                    committed_by: crate::ActorId::loonfs(),
                }),
            ),
            (
                MetadataRowFamily::ActiveDeletions,
                super::MetadataRow::ActiveDeletion(super::ActiveDeletionRecord {
                    root_inode_id: InodeId(42),
                    deletion_seq: ChangeSeq(12),
                    action: super::ActiveDeletionRowAction::Removed {
                        revocation_seq: ChangeSeq(15),
                    },
                }),
            ),
            (
                MetadataRowFamily::CommitReceipts,
                super::MetadataRow::CommitReceipt(super::CommitReceiptRecord {
                    commit_id: CommitId::parse("c_00000000000000000000000000000001")
                        .expect("commit id"),
                    committed_seq: ChangeSeq(12),
                    semantic_commit_fingerprint: serde_json::from_str(r#""sha256:unused""#)
                        .expect("fingerprint"),
                }),
            ),
            (
                MetadataRowFamily::Attributes,
                super::MetadataRow::AttributesRevision(super::AttributesRevisionRecord {
                    inode_id: InodeId(42),
                    attributes_revision_no: crate::AttributesRevisionNo(3),
                    committed_seq: ChangeSeq(12),
                    commit_id: row_commit_id(),
                    delta_index: 0,
                    committed_by: crate::ActorId::loonfs(),
                    committed_at_ms: 12_000,
                    attributes: crate::Attributes::default(),
                }),
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
        fn rows(actor: crate::ActorId) -> Vec<(MetadataRowFamily, super::MetadataRow)> {
            vec![
                (
                    MetadataRowFamily::Inodes,
                    super::MetadataRow::Inode(super::InodeRecord {
                        inode_id: InodeId(42),
                        inode_kind: crate::InodeKind::File,
                        committed_seq: ChangeSeq(3),
                        commit_id: row_commit_id(),
                        committed_by: actor.clone(),
                        committed_at_ms: 3_000,
                    }),
                ),
                (
                    MetadataRowFamily::Revisions,
                    super::MetadataRow::FileRevision(super::RevisionRecord {
                        inode_id: InodeId(42),
                        revision_no: crate::RevisionNo(7),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        committed_at_ms: 12_000,
                        committed_by: actor.clone(),
                        delta_index: 3,
                        content_ref: crate::ContentRef::blob_v1(
                            crate::NamespaceId::parse("demo").expect("namespace id"),
                            crate::ContentId::parse("con_0123456789abcdef0123456789abcdef")
                                .expect("content id"),
                            b"attribution key test",
                        ),
                    }),
                ),
                (
                    MetadataRowFamily::Tombstones,
                    super::MetadataRow::Tombstone(super::SubtreeTombstoneRecord {
                        root_inode_id: InodeId(42),
                        generation: super::TombstoneGeneration {
                            seq: ChangeSeq(12),
                            delta_index: 3,
                        },
                        commit_id: row_commit_id(),
                        action: super::TombstoneRowAction::Set {
                            deleted_binding: deleted_binding(),
                        },
                        committed_at_ms: 12_000,
                        committed_by: actor.clone(),
                    }),
                ),
                (
                    MetadataRowFamily::ActiveDeletions,
                    super::MetadataRow::ActiveDeletion(super::ActiveDeletionRecord {
                        root_inode_id: InodeId(42),
                        deletion_seq: ChangeSeq(12),
                        action: super::ActiveDeletionRowAction::Listed {
                            inode_kind: crate::InodeKind::File,
                            deleted_at_ms: 12_000,
                            deleted_by: actor.clone(),
                            deleted_binding: deleted_binding(),
                        },
                    }),
                ),
                (
                    MetadataRowFamily::Attributes,
                    super::MetadataRow::AttributesRevision(super::AttributesRevisionRecord {
                        inode_id: InodeId(42),
                        attributes_revision_no: crate::AttributesRevisionNo(2),
                        committed_seq: ChangeSeq(12),
                        commit_id: row_commit_id(),
                        delta_index: 3,
                        committed_by: actor,
                        committed_at_ms: 12_000,
                        attributes: crate::Attributes::default(),
                    }),
                ),
            ]
        }

        let actors = [
            crate::ActorId::parse("auth0|x").expect("actor id"),
            crate::ActorId::parse("x".repeat(256)).expect("256-byte actor id"),
            crate::ActorId::parse("external|actor").expect("external actor id"),
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

    fn metadata_run_ref(
        owner_namespace_id: &str,
        segment_id: &str,
        run_no: RunNo,
        run_seq: ChangeSeq,
        tier: RunTier,
    ) -> MetadataRunRef {
        MetadataRunRef {
            run_no,
            run_seq,
            tier,
            segments: vec![metadata_segment_ref(owner_namespace_id, segment_id)],
        }
    }

    fn metadata_segment_ref(owner_namespace_id: &str, segment_id: &str) -> MetadataSegmentRef {
        MetadataSegmentRef {
            owner_namespace_id: NamespaceId::parse(owner_namespace_id).expect("valid namespace id"),
            segment_id: MetadataSegmentId::parse(segment_id).expect("valid segment id"),
            family: MetadataRowFamily::Inodes,
            segment_index: 0,
            row_count: 0,
            min_row_key: String::new(),
            max_row_key: String::new(),
            index_block: BlockHandle {
                offset: 0,
                stored_bytes: 0,
                decoded_bytes: 0,
                crc32c: 0,
            },
            filter_block: BlockHandle {
                offset: 0,
                stored_bytes: 0,
                decoded_bytes: 0,
                crc32c: 0,
            },
            filter_inline: None,
            object_checksum: "sha256:unused".to_owned(),
        }
    }
}
