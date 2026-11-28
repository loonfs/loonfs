//! The WAL object format: envelopes, commit payloads, and the delta
//! records replay applies ([Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records)).

use crate::digest::{sha256_digest, SHA256_DIGEST_BYTES};
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::metadata_rows::{DeletedBinding, DeltaPosition, RevisionSummary};
use crate::{
    AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo, CommitFingerprint, CommitId,
    CommitSeq, ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo, WalNo, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use thiserror::Error;

/// Why a WAL object can't be encoded or decoded.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WalCodecError {
    /// Reports a failure that the shared envelope codec found.
    #[error(transparent)]
    Envelope(#[from] EnvelopeCodecError),
    /// Reports inline content that violates the WAL format rules.
    #[error(
        "invalid WAL inline content in commit `{seq}` for `content_id` `{content_id}`: {reason}"
    )]
    InvalidWalInlineContent {
        /// Commit containing the rejected entry.
        seq: CommitSeq,
        /// Content whose entry violates a rule or exceeds the WAL object total.
        content_id: ContentId,
        /// Format rule violated by the entry.
        reason: &'static str,
    },
    /// Reports a bind whose revision summary doesn't match its child's kind.
    #[error("invalid WAL bind in commit `{seq}` for child inode `{child_inode_id}`: {reason}")]
    InvalidWalBind {
        /// Commit containing the rejected delta.
        seq: CommitSeq,
        /// Child the rejected bind places.
        child_inode_id: InodeId,
        /// Format rule violated by the bind.
        reason: &'static str,
    },
    /// Reports a layout that does not cover its revision's reference.
    #[error(
        "invalid WAL revision layout in commit `{seq}` for `content_id` `{content_id}`: {reason}"
    )]
    InvalidWalRevisionLayout {
        /// Commit containing the rejected delta.
        seq: CommitSeq,
        /// Chain the rejected layout names.
        content_id: ContentId,
        /// Format rule violated by the layout.
        reason: &'static str,
    },
    /// Reports a merge record whose range is empty or inverted, or whose source
    /// is the WAL object's own namespace.
    #[error(
        "invalid WAL merge record in commit `{seq}` from namespace `{source_namespace_id}`: {reason}"
    )]
    InvalidWalMerge {
        /// Commit containing the rejected merge record.
        seq: CommitSeq,
        /// The fork that the rejected merge record identifies.
        source_namespace_id: NamespaceId,
        /// Format rule violated by the merge record.
        reason: &'static str,
    },
}

/// Version 1: a zstd-compressed CBOR envelope document that contains the payload as
/// an opaque CBOR byte string.
///
/// `payload_checksum` covers exactly those bytes, and delta and precondition
/// tags use the snake_case names in
/// [Operations and WAL deltas](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#66-operations-and-wal-deltas)
/// and [Preconditions](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#b3-preconditions).
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Largest decompressed WAL document allowed by [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
pub const MAX_WAL_OBJECT_BYTES: usize = 512 * 1024 * 1024;

/// Reader limit per inline entry in
/// [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records);
/// writer thresholds are policy at or below this limit.
pub const MAX_WAL_INLINE_CONTENT_BYTES: usize = 256 * 1024;

/// Reader limit for total inline bytes in one WAL object in
/// [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records);
/// writer thresholds are policy at or below this limit.
pub const MAX_WAL_OBJECT_INLINE_CONTENT_BYTES: usize = 4 * 1024 * 1024;

/// Upper bound for the document of a WAL object with no records: the envelope
/// fields and a payload with the longest namespace ID, four integers and an
/// empty record list.
const RECORDLESS_WAL_DOCUMENT_BYTES: usize = cbor_map_bytes(&[
    (
        "kind",
        cbor_string_bytes(WalEnvelopeKind::WalObject.as_str().len()),
    ),
    ("format_version", 5),
    ("payload_checksum", cbor_string_bytes(SHA256_DIGEST_BYTES)),
    ("payload", 9),
]) + cbor_map_bytes(&[
    ("namespace_id", cbor_string_bytes(crate::ids::MAX_ID_BYTES)),
    ("wal_no", 9),
    ("writer_epoch", 9),
    ("head_seq", 9),
    ("records", 9),
]);

/// Largest stored size of a fence, which is zstd's compression bound for
/// [`RECORDLESS_WAL_DOCUMENT_BYTES`].
///
/// A WAL object with records is stored in more bytes than this, because the
/// encoder pads one that would otherwise be stored in this many bytes or
/// fewer. A collection tells a fence from a commit object by its listed size
/// alone
/// ([Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records)).
pub const MAX_FENCE_BYTES: usize = zstd_compress_bound(RECORDLESS_WAL_DOCUMENT_BYTES);

/// Upper bound for the document and payload fields outside commit records,
/// the longest padding included.
pub const WAL_OBJECT_OVERHEAD_BYTES: usize = RECORDLESS_WAL_DOCUMENT_BYTES
    + cbor_string_bytes("padding".len())
    + cbor_string_bytes(MAX_FENCE_BYTES + 1);

/// Returns zstd's bound on the compressed size of `length` input bytes, as
/// the `ZSTD_COMPRESSBOUND` macro in `zstd.h` defines it.
const fn zstd_compress_bound(length: usize) -> usize {
    let small_input_margin = if length < 128 * 1024 {
        (128 * 1024 - length) >> 11
    } else {
        0
    };
    length + (length >> 8) + small_input_margin
}

// CBOR strings, collections, and u64 values need at most nine header bytes.
/// Returns the most bytes CBOR uses for a string of `length` bytes.
pub const fn cbor_string_bytes(length: usize) -> usize {
    9 + length
}

/// Returns the most bytes CBOR uses for a map with these field names and
/// encoded value sizes.
pub const fn cbor_map_bytes(fields: &[(&str, usize)]) -> usize {
    let mut bytes = 9;
    let mut index = 0;
    while index < fields.len() {
        bytes += cbor_string_bytes(fields[index].0.len()) + fields[index].1;
        index += 1;
    }
    bytes
}

/// Identifies the durable payload family in a WAL envelope.
///
/// See [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    /// Marks an immutable numbered object in one namespace's WAL.
    WalObject,
}

impl WalEnvelopeKind {
    /// Returns the frozen envelope discriminator written to durable storage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalObject => "wal_object",
        }
    }
}

/// Records one replayable metadata mutation derived from a semantic commit operation.
///
/// See [standard mutation operations](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#66-operations-and-wal-deltas).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WalDelta {
    /// Introduces an inode whose identity and kind remain fixed for its lifetime.
    CreateInode {
        /// Stable position of this delta within its commit, used in row ordering and identity.
        delta_index: u32,
        /// The new inode's generated ID.
        inode_id: InodeId,
        /// File-or-directory classification established at creation.
        inode_kind: InodeKind,
    },
    /// Makes a child reachable under one canonical name in a directory.
    BindDirentry {
        /// Stable position of this delta within its commit, used to identify the binding.
        delta_index: u32,
        /// Directory receiving the new name binding.
        parent_inode_id: InodeId,
        /// Policy-derived lookup key on which directory uniqueness is enforced.
        name_key: NameKey,
        /// User-facing spelling preserved independently of `name_key`.
        display_name: DisplayName,
        /// Inode made reachable by the binding.
        child_inode_id: InodeId,
        /// Classification copied from the child's inode.
        child_kind: InodeKind,
        /// Actor of the child's creating commit.
        child_created_by: crate::ActorId,
        /// Unix milliseconds of the child's creating commit.
        child_created_at_ms: u64,
        /// The file's current revision, which replay copies into the new
        /// edge's rows. Present exactly when the child is a file
        /// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        revision: Option<RevisionSummary>,
    },
    /// Removes one exact historical directory binding without affecting a later rebind.
    UnbindDirentry {
        /// Stable position of this unbind within its commit.
        delta_index: u32,
        /// Directory from which the binding is removed.
        parent_inode_id: InodeId,
        /// Canonical lookup key of the binding being removed.
        name_key: NameKey,
        /// User-facing spelling of the removed binding, so feed
        /// consumers see the name a person typed without a second lookup.
        display_name: DisplayName,
        /// Child identity expected on the targeted binding.
        child_inode_id: InodeId,
        /// Classification copied from the child's inode.
        child_kind: InodeKind,
        /// Actor of the child's creating commit.
        child_created_by: crate::ActorId,
        /// Unix milliseconds of the child's creating commit.
        child_created_at_ms: u64,
        /// The exact bind event this delta retires.
        target: DeltaPosition,
    },
    /// Publishes the next immutable content revision of a file inode.
    AppendFileRevision {
        /// Stable position of this revision delta within its commit.
        delta_index: u32,
        /// File inode receiving the revision.
        inode_id: InodeId,
        /// Directory of the file's current edge, under which replay writes
        /// the new revision's summary row
        /// ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).
        parent_inode_id: InodeId,
        /// Canonical component of the file's current edge.
        name_key: NameKey,
        /// Spelling of the file's current edge, copied into the summary row.
        display_name: DisplayName,
        /// Position of the bind event that established the file's current
        /// edge.
        bind_position: DeltaPosition,
        /// Actor of the file's creating commit, copied into the summary row.
        child_created_by: crate::ActorId,
        /// Unix milliseconds of the file's creating commit, copied into the
        /// summary row.
        child_created_at_ms: u64,
        /// Monotonic per-file revision number validated against visible history.
        revision_no: RevisionNo,
        /// Content of the new revision. Uploaded bytes are durable before
        /// publication; inline bytes travel in this commit's `inline_content`
        /// ([format section 1.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#15-file-contents-and-ownership)).
        content_ref: ContentRef,
        /// Objects that store the bytes before this commit's pieces.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        layout: Option<crate::ContentLayout>,
        /// The offset where an append's bytes begin, which is the base
        /// revision's `size_bytes`.
        ///
        /// The field is absent from the delta of every other revision, such
        /// as one from a put, a copy, or a restore
        /// ([format section 6.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#66-operations-and-wal-deltas)).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        append_offset: Option<u64>,
    },
    /// Hides a rooted subtree from snapshots at this delta's sequence or later.
    TombstoneSubtree {
        /// Stable position that identifies this tombstone within its commit.
        delta_index: u32,
        /// Inode at the root of the newly hidden subtree.
        root_inode_id: InodeId,
        /// Classification copied from the root's inode, which the
        /// deletion's `listed` trash row stores.
        root_kind: InodeKind,
        /// The binding the delete removed, included so the deleted name
        /// survives on the immortal tombstone row after unbind rows age out.
        deleted_binding: DeletedBinding,
    },
    /// Revokes exactly one subtree tombstone (the one recorded at `target`),
    /// making the subtree eligible for visibility again once re-bound. An
    /// immutable compensating event, not an in-place row deletion: a later
    /// `TombstoneSubtree` for the same root supersedes the revoke.
    RevokeSubtreeTombstone {
        /// Stable position of this compensating delta within its commit.
        delta_index: u32,
        /// Root inode whose selected tombstone is being revoked.
        root_inode_id: InodeId,
        /// The exact tombstone position this delta compensates.
        target: DeltaPosition,
    },
    /// Publishes the next attribute revision of one inode, as complete state.
    ///
    /// The delta contains the whole resulting map rather than the changes that
    /// produced it, so replay never needs an earlier revision to determine what
    /// attributes an inode has. An empty map is a real revision: it is the cleared
    /// state, and it hides every earlier map.
    AppendAttributesRevision {
        /// Stable position of this attribute delta within its commit.
        delta_index: u32,
        /// Inode whose attributes this revision replaces.
        inode_id: InodeId,
        /// Monotonic per-inode attribute revision, exactly one past the
        /// revision the update was validated against.
        attributes_revision_no: AttributesRevisionNo,
        /// The inode's complete attribute map after this update.
        attributes: Attributes,
    },
    /// Publishes the next access revision of one inode, as complete state.
    ///
    /// Like an attribute revision, the delta contains the whole resulting
    /// state. A row with no boundary and no grants is a real revision: it is
    /// the cleared state, and it hides every earlier row.
    AppendAccessRevision {
        /// Stable position of this access delta within its commit.
        delta_index: u32,
        /// Inode whose access state this revision replaces.
        inode_id: InodeId,
        /// Monotonic per-inode access revision, exactly one past the
        /// revision the update was validated against.
        access_revision_no: AccessRevisionNo,
        /// Whether the directory stops inheritance after this update.
        boundary: bool,
        /// The inode's complete direct grants after this update.
        grants: AccessGrants,
    },
}

/// Associates a WAL delta with the semantic operation that produced it.
///
/// See [logical commits](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#12-commits-and-revisions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitDelta {
    /// Internal operation position within this commit. Convenience requests can
    /// expand into several operations; each operation's deltas are contiguous.
    pub semantic_operation_index: u32,
    /// Replay mutation produced for that semantic operation.
    pub delta: WalDelta,
}

/// Groups a commit's ordered deltas by their durable semantic operation.
///
/// Indices are local to the commit and need not be consecutive.
pub fn semantic_operation_groups(
    deltas: &[WalCommitDelta],
) -> impl Iterator<Item = &[WalCommitDelta]> {
    deltas.chunk_by(|left, right| left.semantic_operation_index == right.semantic_operation_index)
}

/// Counts activity represented by one committed record.
///
/// Returns `None` if a counter overflows.
///
/// A revision counts its full length, except that bytes the record's pieces
/// add to a content id count once: the first revision that reaches past
/// what earlier revisions counted for that id counts only the bytes past
/// it. An append therefore counts the bytes it appends.
pub fn committed_activity(record: &WalCommitPayload) -> Option<crate::manifest::ManifestActivity> {
    let mut counted_ends: BTreeMap<&ContentId, u64> = BTreeMap::new();
    for entry in &record.inline_content {
        let end = counted_ends
            .entry(&entry.content_id)
            .or_insert(entry.offset);
        *end = (*end).min(entry.offset);
    }
    let mut activity = crate::manifest::ManifestActivity::default();
    for group in semantic_operation_groups(&record.deltas) {
        activity.mutations = activity.mutations.checked_add(1)?;
        for delta in group {
            if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                let size_bytes = content_ref.size_bytes;
                let counted = match counted_ends.get_mut(&content_ref.content_id) {
                    Some(end) if size_bytes > *end => {
                        size_bytes - std::mem::replace(end, size_bytes)
                    }
                    _ => size_bytes,
                };
                activity.content_bytes = activity.content_bytes.checked_add(counted)?;
                activity.file_revisions = activity.file_revisions.checked_add(1)?;
            }
        }
    }
    Some(activity)
}

/// Contains bytes of a content object that a revision delta references in the same
/// commit, starting at `offset`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalInlineContent {
    /// Identity shared with the accompanying `blob_v1` reference.
    pub content_id: ContentId,
    /// Position of the first byte in the content object.
    pub offset: u64,
    /// The bytes, encoded as a CBOR byte string.
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

/// Identifies the ranges of fork commits that a merge applied or covered.
///
/// The record identifies the fork and the range of its commits that the merge
/// applied to the committing namespace, the ranges of other forks that the
/// merge also covers, and for a rebase's replay commit, the changes that the
/// replay commit left out.
///
/// The range follows the change feed's convention: the fork's commits above
/// `source_after_seq`, through `source_through_seq`. See
/// [WAL records](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeRecord {
    /// The fork whose commits the merge applied.
    pub source_namespace_id: NamespaceId,
    /// The fork's commits above this sequence were applied. For a fork's
    /// first merge, this is the fork point.
    pub source_after_seq: CommitSeq,
    /// The fork's commits at or below this sequence were applied.
    pub source_through_seq: CommitSeq,
    /// The ranges of other forks' commits that the merge also covers, one
    /// for each fork: the forks whose commits the replay commits in the
    /// merge's range applied to the source. Empty unless the source is a
    /// rebase's new fork and the range has a replay commit.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub covered_ranges: Vec<ForkRange>,
    /// The changes that a replay commit left out, as its conflict report. A
    /// merge's record has none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<crate::MergeConflict>,
}

impl MergeRecord {
    /// Returns every range of fork commits that the merge covers: its own
    /// source's range, and then each of `covered_ranges`.
    pub fn ranges(&self) -> impl Iterator<Item = ForkRange> + '_ {
        std::iter::once(ForkRange {
            source_namespace_id: self.source_namespace_id.clone(),
            source_after_seq: self.source_after_seq,
            source_through_seq: self.source_through_seq,
        })
        .chain(self.covered_ranges.iter().cloned())
    }
}

/// A range of one fork's commits, above `source_after_seq` through
/// `source_through_seq`, that a merge covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkRange {
    /// The fork whose commits the range contains.
    pub source_namespace_id: NamespaceId,
    /// The fork's commits above this sequence are in the range.
    pub source_after_seq: CommitSeq,
    /// The fork's commits at or below this sequence are in the range.
    pub source_through_seq: CommitSeq,
}

/// Contains one accepted logical commit inside a WAL object.
///
/// See [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitPayload {
    /// Namespace-wide commit position; the records in one WAL object must cover their range contiguously.
    pub committed_seq: CommitSeq,
    /// Caller idempotency key whose reuse must retain the same semantic fingerprint.
    pub commit_id: CommitId,
    /// Actor that committed the change, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// Digest of semantic request content used to reject conflicting `commit_id` reuse.
    pub semantic_commit_fingerprint: CommitFingerprint,
    /// Wall-clock stamp from the publishing writer's request context, in
    /// Unix milliseconds. Observational only: never a validity or ordering
    /// input (`committed_seq` defines the order), and excluded from the semantic
    /// commit fingerprint, so replay identity is untouched by clocks.
    pub committed_at_ms: u64,
    /// Caller-supplied annotation, omitted when absent and excluded from filesystem semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The fork and the range of its commits that this commit applied, when
    /// the commit is a merge, and omitted for every other commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge: Option<MergeRecord>,
    /// Metadata mutations in their authoritative `delta_index` order.
    pub deltas: Vec<WalCommitDelta>,
    /// Content pieces governed by [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inline_content: Vec<WalInlineContent>,
}

/// Contains the namespace identity, numbered range, and body of one WAL object.
///
/// See [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectPayload {
    /// Namespace this WAL object belongs to; recovery rejects cross-namespace content.
    pub namespace_id: NamespaceId,
    /// Contiguous object number checked against the key.
    pub wal_no: WalNo,
    /// Fencing epoch of the writer that proposed this WAL object.
    pub writer_epoch: WriterEpoch,
    /// Visible sequence after this WAL object.
    pub head_seq: CommitSeq,
    /// The kind of WAL object and its contents.
    pub body: WalObjectBody,
}

impl WalObjectPayload {
    /// Returns the WAL object's commits. A fence has none.
    pub fn records(&self) -> &[WalCommitPayload] {
        match &self.body {
            WalObjectBody::Commits(commits) => commits,
            WalObjectBody::Fence => &[],
        }
    }
}

/// Lists the two kinds of WAL object and what each one contains.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalObjectBody {
    /// A data WAL object, which contains one or more commits.
    Commits(WalCommits),
    /// A fence, which contains no commits and leaves the head unchanged.
    Fence,
}

/// The commits of a data WAL object: at least one, in contiguous ascending
/// sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCommits(Vec<WalCommitPayload>);

impl WalCommits {
    /// Returns `records` as the commits of a data WAL object, or `None` when
    /// `records` is empty.
    pub fn new(records: Vec<WalCommitPayload>) -> Option<Self> {
        (!records.is_empty()).then_some(Self(records))
    }

    /// Returns the commit with the lowest sequence.
    pub fn oldest(&self) -> &WalCommitPayload {
        self.0
            .first()
            .expect("a data WAL object should contain at least one commit")
    }

    /// Returns the commit with the highest sequence.
    pub fn newest(&self) -> &WalCommitPayload {
        self.0
            .last()
            .expect("a data WAL object should contain at least one commit")
    }
}

impl std::ops::Deref for WalCommits {
    type Target = [WalCommitPayload];

    fn deref(&self) -> &[WalCommitPayload] {
        &self.0
    }
}

impl std::ops::DerefMut for WalCommits {
    fn deref_mut(&mut self) -> &mut [WalCommitPayload] {
        &mut self.0
    }
}

/// Lists the CBOR fields of a stored WAL object's payload. `records` is
/// empty in a fence.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalPayloadDocument<'a> {
    namespace_id: Cow<'a, NamespaceId>,
    wal_no: WalNo,
    writer_epoch: WriterEpoch,
    head_seq: CommitSeq,
    records: Cow<'a, [WalCommitPayload]>,
}

impl<'a> From<&'a WalObjectPayload> for WalPayloadDocument<'a> {
    fn from(payload: &'a WalObjectPayload) -> Self {
        Self {
            namespace_id: Cow::Borrowed(&payload.namespace_id),
            wal_no: payload.wal_no,
            writer_epoch: payload.writer_epoch,
            head_seq: payload.head_seq,
            records: Cow::Borrowed(payload.records()),
        }
    }
}

impl From<WalPayloadDocument<'_>> for WalObjectPayload {
    fn from(document: WalPayloadDocument<'_>) -> Self {
        let body = match WalCommits::new(document.records.into_owned()) {
            Some(commits) => WalObjectBody::Commits(commits),
            None => WalObjectBody::Fence,
        };
        Self {
            namespace_id: document.namespace_id.into_owned(),
            wal_no: document.wal_no,
            writer_epoch: document.writer_epoch,
            head_seq: document.head_seq,
            body,
        }
    }
}

/// A WAL object decoded through its checked durable codec.
pub type WalObjectEnvelope = crate::envelope::VerifiedEnvelope<WalObjectPayload>;

/// Durable layout of a WAL object (before zstd compression): the
/// envelope fields plus the payload as an opaque CBOR byte string.
/// `payload_checksum` covers exactly those bytes, so integrity verification
/// never depends on re-encoding the payload with this build's schema. Unknown
/// payload fields are rejected after checksum verification.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalObjectDocument {
    kind: String,
    format_version: u32,
    payload_checksum: crate::PayloadChecksum,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
    /// Bytes that keep a commit object's stored size above
    /// [`MAX_FENCE_BYTES`], left out when the object is
    /// larger without them. `payload_checksum` doesn't cover them, and
    /// readers ignore their content.
    #[serde(default, with = "serde_bytes", skip_serializing_if = "Option::is_none")]
    padding: Option<Vec<u8>>,
}

/// Returns the number of bytes that a conflict report takes in a WAL
/// record's CBOR encoding, without the field name.
pub fn conflict_report_cbor_bytes(conflicts: &[crate::MergeConflict]) -> usize {
    let mut encoded = Vec::new();
    into_writer(conflicts, &mut encoded)
        .expect("a conflict report of paths, IDs, and integers should encode as CBOR");
    encoded.len()
}

pub(crate) fn encode_wal_payload_cbor(
    payload: &WalObjectPayload,
) -> Result<Vec<u8>, WalCodecError> {
    validate_wal_payload(payload)?;
    let mut encoded = Vec::new();
    into_writer(&WalPayloadDocument::from(payload), &mut encoded)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

/// Encodes a WAL payload once, then checksums and compresses its durable document.
///
/// A commit object that would be stored in [`MAX_FENCE_BYTES`]
/// or fewer gets padding until its stored size is above that bound.
pub fn encode_wal_object_envelope_zstd(
    payload: WalObjectPayload,
) -> Result<crate::envelope::EncodedEnvelope<WalObjectPayload>, WalCodecError> {
    let payload_bytes = encode_wal_payload_cbor(&payload)?;
    let payload_checksum = sha256_digest(&payload_bytes);
    let mut document = WalObjectDocument {
        kind: WalEnvelopeKind::WalObject.as_str().to_owned(),
        format_version: WAL_FORMAT_VERSION,
        payload_checksum: payload_checksum.clone(),
        payload: payload_bytes,
        padding: None,
    };
    // Each pass adds the missing bytes, up to one more than the bound.
    // Padding doesn't compress, so one extra pass is usual, and a pass with
    // the longest padding is the last.
    loop {
        let mut encoded = Vec::new();
        into_writer(&document, &mut encoded)
            .map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))?;
        let bytes = envelope::compress_document(&encoded)?;
        let padding_bytes = document.padding.as_ref().map_or(0, Vec::len);
        if !matches!(payload.body, WalObjectBody::Commits(_))
            || bytes.len() > MAX_FENCE_BYTES
            || padding_bytes > MAX_FENCE_BYTES
        {
            return Ok(crate::envelope::EncodedEnvelope {
                envelope: crate::envelope::VerifiedEnvelope {
                    payload_checksum,
                    payload,
                },
                bytes,
                document_bytes: encoded.len(),
            });
        }
        let padding_bytes =
            (padding_bytes + MAX_FENCE_BYTES + 1 - bytes.len()).min(MAX_FENCE_BYTES + 1);
        document.padding = Some(wal_object_padding(&payload_checksum, padding_bytes));
    }
}

/// Returns `length` bytes of padding: successive SHA-256 digests of the
/// payload checksum and a counter, so the same payload gets the same padding.
fn wal_object_padding(payload_checksum: &crate::PayloadChecksum, length: usize) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let mut padding = Vec::with_capacity(length);
    let mut counter: u32 = 0;
    while padding.len() < length {
        let digest = Sha256::new()
            .chain_update(payload_checksum.as_str().as_bytes())
            .chain_update(counter.to_be_bytes())
            .finalize();
        padding.extend_from_slice(&digest);
        counter += 1;
    }
    padding.truncate(length);
    padding
}

/// Decodes and verifies a durable zstd-compressed WAL object envelope.
///
/// Decoding fails for invalid compression or CBOR, the wrong kind or version,
/// a checksum mismatch, or an invalid payload. See
/// [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
pub fn decode_wal_object_envelope_zstd(bytes: &[u8]) -> Result<WalObjectEnvelope, WalCodecError> {
    decode_wal_object_envelope_zstd_with_limit(bytes, MAX_WAL_OBJECT_BYTES)
}

fn decode_wal_object_envelope_zstd_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<WalObjectEnvelope, WalCodecError> {
    let Some(decompressed) = envelope::decompress_document(bytes, limit)? else {
        return Err(EnvelopeCodecError::DocumentTooLarge {
            kind: WalEnvelopeKind::WalObject.as_str(),
            max_bytes: limit,
        }
        .into());
    };
    let probe: EnvelopeProbe = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = WalEnvelopeKind::WalObject;
    envelope::ensure_kind(expected_kind.as_str(), &probe.kind)?;
    envelope::ensure_version(&probe.kind, probe.format_version, WAL_FORMAT_VERSION)?;

    let document: WalObjectDocument = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    envelope::verify_payload_checksum(&document.payload_checksum, &document.payload)?;
    let payload_document: WalPayloadDocument = from_reader(document.payload.as_slice())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;
    let payload = WalObjectPayload::from(payload_document);
    validate_wal_payload(&payload)?;

    Ok(WalObjectEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

fn validate_wal_payload(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    validate_wal_inline_content(payload)?;
    validate_wal_revision_layouts(payload)?;
    validate_wal_bind_revisions(payload)?;
    validate_wal_merges(payload)
}

fn validate_wal_inline_content(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    let mut total_bytes = 0;
    for record in payload.records() {
        if record.inline_content.is_empty() {
            continue;
        }
        let invalid = |content_id: &ContentId, reason| WalCodecError::InvalidWalInlineContent {
            seq: record.committed_seq,
            content_id: content_id.clone(),
            reason,
        };
        let mut reference_ends: BTreeMap<&ContentId, u64> = BTreeMap::new();
        for delta in &record.deltas {
            if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                let end = reference_ends.entry(&content_ref.content_id).or_default();
                *end = (*end).max(content_ref.size_bytes);
            }
        }
        let mut entries: BTreeMap<&ContentId, BTreeMap<u64, &WalInlineContent>> = BTreeMap::new();
        for entry in &record.inline_content {
            if entry.bytes.len() > MAX_WAL_INLINE_CONTENT_BYTES {
                return Err(invalid(
                    &entry.content_id,
                    "value exceeds `MAX_WAL_INLINE_CONTENT_BYTES`",
                ));
            }
            total_bytes += entry.bytes.len();
            if total_bytes > MAX_WAL_OBJECT_INLINE_CONTENT_BYTES {
                return Err(invalid(
                    &entry.content_id,
                    "WAL object inline total exceeds `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES`",
                ));
            }
            if !reference_ends.contains_key(&entry.content_id) {
                return Err(invalid(
                    &entry.content_id,
                    "no `append_file_revision` reference in the same commit",
                ));
            }
            if entries
                .entry(&entry.content_id)
                .or_default()
                .insert(entry.offset, entry)
                .is_some()
            {
                return Err(invalid(
                    &entry.content_id,
                    "duplicate `content_id` and `offset` in commit",
                ));
            }
        }
        // A namespace's WAL includes pieces only for chains that the namespace
        // created, so every layout of a chain with pieces records it.
        for delta in &record.deltas {
            if let WalDelta::AppendFileRevision {
                content_ref,
                layout: Some(layout),
                ..
            } = &delta.delta
            {
                if entries.contains_key(&content_ref.content_id)
                    && layout.creator_namespace_id != payload.namespace_id
                {
                    return Err(invalid(
                        &content_ref.content_id,
                        "a layout of a chain with pieces records another namespace as its creator",
                    ));
                }
            }
        }
        for (content_id, by_offset) in entries {
            let mut end = None;
            for (offset, entry) in by_offset {
                if end.is_some_and(|end| end != offset) {
                    return Err(invalid(content_id, "entries are not contiguous"));
                }
                end = offset.checked_add(entry.bytes.len() as u64);
                if end.is_none() {
                    break;
                }
            }
            if end != reference_ends.get(content_id).copied() {
                return Err(invalid(
                    content_id,
                    "entries do not end at the reference `size_bytes`",
                ));
            }
        }
    }
    Ok(())
}

fn validate_wal_revision_layouts(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    for record in payload.records() {
        for delta in &record.deltas {
            let WalDelta::AppendFileRevision {
                content_ref,
                layout,
                ..
            } = &delta.delta
            else {
                continue;
            };
            let invalid = |reason| WalCodecError::InvalidWalRevisionLayout {
                seq: record.committed_seq,
                content_id: content_ref.content_id.clone(),
                reason,
            };
            let first_offset = record
                .inline_content
                .iter()
                .filter(|piece| piece.content_id == content_ref.content_id)
                .map(|piece| piece.offset)
                .min();
            if first_offset == Some(0) {
                if layout.is_some() {
                    return Err(invalid("a whole value carries no layout"));
                }
                continue;
            }
            let Some(layout) = layout else {
                if first_offset.is_some() {
                    continue;
                }
                return Err(invalid("a reference without pieces requires a layout"));
            };
            let size_bytes = layout
                .extents
                .iter()
                .try_fold(0_u64, |size, extent| size.checked_add(extent.length))
                .ok_or_else(|| invalid("layout length overflows"))?;
            layout
                .validate(size_bytes)
                .map_err(|error| invalid(error.reason))?;
            if size_bytes != first_offset.unwrap_or(content_ref.size_bytes) {
                return Err(invalid(
                    "layout size must equal the first piece offset or the reference size",
                ));
            }
        }
    }
    Ok(())
}

/// Requires a revision summary on every file bind and on no directory bind.
fn validate_wal_bind_revisions(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    for record in payload.records() {
        for delta in &record.deltas {
            let WalDelta::BindDirentry {
                child_inode_id,
                child_kind,
                revision,
                ..
            } = &delta.delta
            else {
                continue;
            };
            let reason = match (child_kind, revision) {
                (InodeKind::File, Some(_)) | (InodeKind::Directory, None) => continue,
                (InodeKind::File, None) => "a file bind requires the file's revision summary",
                (InodeKind::Directory, Some(_)) => {
                    "a directory bind can't include a revision summary"
                }
            };
            return Err(WalCodecError::InvalidWalBind {
                seq: record.committed_seq,
                child_inode_id: *child_inode_id,
                reason,
            });
        }
    }
    Ok(())
}

/// Requires each range that a merge record covers to contain at least one
/// sequence, and to be of a namespace other than the committing namespace
/// and the record's other ranges.
fn validate_wal_merges(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    for record in payload.records() {
        let Some(merge) = &record.merge else {
            continue;
        };
        for (index, range) in merge.ranges().enumerate() {
            let reason = if range.source_after_seq >= range.source_through_seq {
                "`source_after_seq` must be below `source_through_seq`"
            } else if range.source_namespace_id == payload.namespace_id {
                "the source is the committing namespace"
            } else if merge
                .ranges()
                .take(index)
                .any(|earlier| earlier.source_namespace_id == range.source_namespace_id)
            {
                "the record covers the source twice"
            } else {
                continue;
            };
            return Err(WalCodecError::InvalidWalMerge {
                seq: record.committed_seq,
                source_namespace_id: range.source_namespace_id,
                reason,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
