//! Metadata row families, the row union, and the record that each family
//! stores ([metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys)).

use crate::wal::WalCommitPayload;
use crate::{
    AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo, ChangeSeq, CommitId,
    ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo,
};
use serde::{Deserialize, Serialize};

/// Selects a metadata row family and its durable lookup ordering.
///
/// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataRowFamily {
    /// Stores inode identity, kind, and creation position.
    Inodes,
    /// Orders directory bindings by parent, name, and child, newest first
    /// within each edge, for path resolution and directory listings. Besides
    /// the binding events, each bound file edge has a summary row for every
    /// revision of the file.
    DirentryBinds,
    /// Re-indexes the binding events by child, newest first within each
    /// edge, for parent discovery.
    DirentryChildBinds,
    /// Stores file revisions newest-first within each inode.
    Revisions,
    /// Stores set and revoke events used to determine active subtree
    /// tombstones, newest first within each root.
    Tombstones,
    /// Lists the deletions that no undelete has cancelled, derived from the
    /// tombstone family and ordered by deletion sequence. The trash listing
    /// reads the rows above the retention floor.
    ActiveDeletions,
    /// Preserves commit idempotency evidence independently of retained WAL history.
    CommitReceipts,
    /// Stores each retained commit's record in sequence order.
    Commits,
    /// Lists the objects that store each chain.
    ContentLayouts,
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

impl MetadataRowFamily {
    /// Every row family in serialized declaration order.
    pub const ALL: [Self; 11] = [
        Self::Inodes,
        Self::DirentryBinds,
        Self::DirentryChildBinds,
        Self::Revisions,
        Self::Tombstones,
        Self::ActiveDeletions,
        Self::CommitReceipts,
        Self::Commits,
        Self::ContentLayouts,
        Self::Attributes,
        Self::Access,
    ];

    /// Returns the snake-case name used in durable keys and serialized values.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inodes => "inodes",
            Self::DirentryBinds => "direntry_binds",
            Self::DirentryChildBinds => "direntry_child_binds",
            Self::Revisions => "revisions",
            Self::Tombstones => "tombstones",
            Self::ActiveDeletions => "active_deletions",
            Self::CommitReceipts => "commit_receipts",
            Self::Commits => "commits",
            Self::ContentLayouts => "content_layouts",
            Self::Attributes => "attributes",
            Self::Access => "access",
        }
    }
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
    /// Lists the objects that store each chain.
    ContentLayouts,
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
        Self::ContentLayouts,
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
            Self::ContentLayouts => "content_layouts",
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
            ],
            Self::Revisions => &[MetadataRowFamily::Revisions],
            Self::Inodes => &[MetadataRowFamily::Inodes],
            Self::Tombstones => &[MetadataRowFamily::Tombstones],
            Self::ActiveDeletions => &[MetadataRowFamily::ActiveDeletions],
            Self::Commits => &[
                MetadataRowFamily::Commits,
                MetadataRowFamily::CommitReceipts,
            ],
            Self::ContentLayouts => &[MetadataRowFamily::ContentLayouts],
            Self::Attributes => &[MetadataRowFamily::Attributes],
            Self::Access => &[MetadataRowFamily::Access],
        }
    }
}

/// One materialized metadata row stored in a segment.
///
/// See [metadata rows and row keys](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a6-metadata-rows-and-row-keys).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MetadataRow {
    /// Establishes one inode's immutable identity and kind.
    Inode(InodeRecord),
    /// Stores the value of a directory edge at one delta position.
    DirentryBinding(DirentryBindingRecord),
    /// Publishes one immutable content revision for a file inode.
    FileRevision(RevisionRecord),
    /// Changes whether one root inode has an active subtree tombstone.
    Tombstone(SubtreeTombstoneRecord),
    /// Derived row used to list recoverable deletions.
    ///
    /// Materialization writes `listed` for each tombstone set and `removed` for
    /// each revoke. This lets trash listing use an ordered range scan
    /// instead of replaying all historical deletion events.
    ActiveDeletion(ActiveDeletionRecord),
    /// Preserves the evidence needed to respond to a retried logical commit.
    CommitReceipt(CommitReceiptRecord),
    /// One committed logical commit as its WAL record describes it, without inline bytes.
    /// The change feed and commit replay read this row.
    Commit(WalCommitPayload),
    /// Lists the objects that store one chain as of a fold or commit.
    ContentLayout(ContentLayoutRecord),
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
    /// Namespace-scoped inode identity generated by the publishing writer.
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

/// Stores one value of a directory edge: a binding event in both binding
/// indexes, or a revision summary in the slot index.
///
/// See [directory bindings](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirentryBindingRecord {
    /// Directory containing the slot.
    pub parent_inode_id: InodeId,
    /// Canonical component used for lookup.
    pub name_key: NameKey,
    /// Child affected by this change.
    pub child_inode_id: InodeId,
    /// Classification copied from the child's inode.
    pub child_kind: InodeKind,
    /// Actor of the child's creating commit.
    pub child_created_by: crate::ActorId,
    /// Unix milliseconds of the child's creating commit.
    pub child_created_at_ms: u64,
    /// Commit that wrote this row.
    pub committed_seq: ChangeSeq,
    /// Order within the commit.
    pub delta_index: u32,
    /// Commit ID of the commit that wrote this row.
    pub commit_id: CommitId,
    /// Actor of the commit that wrote this row, as supplied by the
    /// application.
    pub committed_by: crate::ActorId,
    /// Wall-clock stamp of the commit that wrote this row, in Unix
    /// milliseconds.
    pub committed_at_ms: u64,
    /// Position of the bind event that established the edge's binding. An
    /// unbound row stores the position of the bind that it removes.
    pub bind_position: DeltaPosition,
    /// Value after this change.
    pub state: DirentryBindingState,
}

impl DirentryBindingRecord {
    /// Returns the position used to order an edge's values.
    pub fn position(&self) -> DeltaPosition {
        DeltaPosition {
            seq: self.committed_seq,
            delta_index: self.delta_index,
        }
    }

    /// Returns the spelling while the slot is bound.
    pub fn display_name(&self) -> Option<&DisplayName> {
        match &self.state {
            DirentryBindingState::Bound { display_name, .. } => Some(display_name),
            DirentryBindingState::Unbound => None,
        }
    }

    /// Returns the bound file's revision summary, which a directory, an
    /// unbound row, and a corrupt file row don't have.
    pub fn revision(&self) -> Option<&RevisionSummary> {
        match &self.state {
            DirentryBindingState::Bound { revision, .. } => revision.as_ref(),
            DirentryBindingState::Unbound => None,
        }
    }

    /// Returns whether the slot has a child at this position.
    pub fn is_bound(&self) -> bool {
        matches!(self.state, DirentryBindingState::Bound { .. })
    }

    /// Returns whether a bind or unbind delta wrote this row, which both
    /// binding indexes store, rather than a revision delta, which writes a
    /// summary row to the slot index alone. A summary row is bound, and its
    /// position isn't its bind position.
    pub fn is_binding_event(&self) -> bool {
        !self.is_bound() || self.position() == self.bind_position
    }
}

/// An unbound value hides older values of the edge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DirentryBindingState {
    /// The slot names its child.
    Bound {
        /// Caller spelling preserved for reads.
        display_name: DisplayName,
        /// The file's current revision as of this row. A directory has none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision: Option<RevisionSummary>,
    },
    /// The slot is available.
    Unbound,
}

/// The fields of a file's current revision that a directory listing reports,
/// copied into the bound rows of the file's edge.
///
/// See [directory bindings](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionSummary {
    /// The file's current revision number.
    pub revision_no: RevisionNo,
    /// The current revision's length in bytes.
    pub size_bytes: u64,
    /// Actor of the commit that published the current revision.
    pub revision_committed_by: crate::ActorId,
    /// Unix milliseconds of the commit that published the current revision.
    pub revision_committed_at_ms: u64,
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
    /// Namespace sequence that published the event.
    pub committed_seq: ChangeSeq,
    /// Position within the owning commit.
    pub delta_index: u32,
    /// Commit ID associated with this row.
    pub commit_id: CommitId,
    /// What this event did.
    pub action: TombstoneRowAction,
    /// Actor of the commit that recorded this event.
    pub committed_by: crate::ActorId,
    /// Wall-clock stamp of the commit that recorded this event.
    pub committed_at_ms: u64,
}

impl SubtreeTombstoneRecord {
    /// Identifies this event when another event refers to it.
    pub fn position(&self) -> DeltaPosition {
        DeltaPosition {
            seq: self.committed_seq,
            delta_index: self.delta_index,
        }
    }
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

/// The objects that store a chain as of one fold or commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentLayoutRecord {
    /// Namespace that owns the chain.
    pub owner_namespace_id: NamespaceId,
    /// Chain whose bytes the layout lists.
    pub content_id: ContentId,
    /// Newest commit whose bytes the layout covers.
    pub committed_seq: ChangeSeq,
    /// Total length covered by the extents.
    pub size_bytes: u64,
    /// Objects in chain order.
    pub layout: crate::ContentLayout,
}

/// Indexes one retained commit by the caller's idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitReceiptRecord {
    /// Caller idempotency key whose later reuse is checked against this row.
    pub commit_id: CommitId,
    /// Namespace sequence assigned to the accepted commit.
    pub committed_seq: ChangeSeq,
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

/// Identifies one delta within committed namespace history.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct DeltaPosition {
    /// Commit sequence that published the event.
    pub seq: ChangeSeq,
    /// Position that disambiguates the event within `seq`.
    pub delta_index: u32,
}

/// Directory binding removed by a path deletion.
///
/// Tombstones retain this binding after old slot versions are collected. Undelete uses it to restore the original parent and name.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletedBinding {
    /// Directory that contained the binding.
    pub parent_inode_id: InodeId,
    /// Canonical key the binding was reachable under.
    pub name_key: NameKey,
    /// User-facing spelling of the binding.
    pub display_name: DisplayName,
}

/// Events retained for recoverable subtree deletion.
///
/// The latest event for a root at a sequence determines the root's state there
/// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TombstoneRowAction {
    /// The subtree rooted at the row's inode is deleted.
    Set {
        /// The binding the delete removed.
        deleted_binding: DeletedBinding,
    },
    /// The deletion recorded at `target` is revoked. Only a `set` has a
    /// binding, so the revoke has no place to put one.
    Revoke {
        /// The exact `set` event being compensated.
        target: DeltaPosition,
    },
}

impl TombstoneRowAction {
    /// Whether a root whose latest event is this one hides its subtree.
    pub const fn hides_subtree(&self) -> bool {
        match self {
            Self::Set { .. } => true,
            Self::Revoke { .. } => false,
        }
    }
}

/// Current-state rows for deletions.
///
/// `Listed` exposes a deletion in trash; `Removed` hides it after an undelete.
/// Both rows share a key prefix, with `Removed` sorting first, so scans can
/// suppress cancelled entries. Compaction later removes the cancelled pair,
/// and the `Listed` row of a deletion at or below the acknowledged floor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActiveDeletionRowAction {
    /// The deletion is in the trash while it's above the retention floor;
    /// these are the fields the trash entry renders, denormalized so a page
    /// needs no per-entry join.
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
    /// The deletion was cancelled by an undelete at `removed_seq`.
    Removed {
        /// Commit sequence of the undelete that cancelled the deletion.
        removed_seq: ChangeSeq,
    },
}
