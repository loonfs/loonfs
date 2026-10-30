//! The WAL object format: envelopes, commit payloads, and the delta
//! records replay applies ([Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records)).

use crate::digest::{sha256_digest, SHA256_DIGEST_BYTES};
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::metadata_rows::{DeletedBinding, DeltaPosition, RevisionSummary};
use crate::{
    AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo, ChangeSeq, CommitFingerprint,
    CommitId, ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo, WalNo, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
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
        seq: ChangeSeq,
        /// Content whose entry violates a rule or exceeds the WAL object total.
        content_id: ContentId,
        /// Format rule violated by the entry.
        reason: &'static str,
    },
    /// Reports a bind whose revision summary doesn't match its child's kind.
    #[error("invalid WAL bind in commit `{seq}` for child inode `{child_inode_id}`: {reason}")]
    InvalidWalBind {
        /// Commit containing the rejected delta.
        seq: ChangeSeq,
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
        seq: ChangeSeq,
        /// Chain the rejected layout names.
        content_id: ContentId,
        /// Format rule violated by the layout.
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

/// Upper bound for the document and payload fields outside commit records.
pub const WAL_OBJECT_OVERHEAD_BYTES: usize = cbor_map_bytes(&[
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
    ("basis_floor_seq", 9),
    ("records", 9),
]);

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

/// Records one replayable metadata mutation materialized from a semantic commit operation.
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
        /// The binding the delete removed, included so the deleted name
        /// survives on the immortal tombstone row after unbind rows age out.
        deleted_binding: DeletedBinding,
    },
    /// Revokes exactly one subtree tombstone — the one recorded at `target`
    /// — making the subtree eligible for visibility again once re-bound. An
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

/// Associates a materialized WAL delta with the semantic operation that produced it.
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

/// Contains one accepted logical commit inside a WAL object.
///
/// See [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitPayload {
    /// Namespace-wide commit position; the records in one WAL object must cover their range contiguously.
    pub committed_seq: ChangeSeq,
    /// Caller idempotency key whose reuse must retain the same semantic fingerprint.
    pub commit_id: CommitId,
    /// Actor that committed the change, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// Digest of semantic request content used to reject conflicting `commit_id` reuse.
    pub semantic_commit_fingerprint: CommitFingerprint,
    /// Wall-clock stamp from the publishing writer's request context, in
    /// Unix milliseconds. Observational only: never a validity or ordering
    /// input — `committed_seq` is the order — and excluded from the semantic commit
    /// fingerprint, so replay identity is untouched by clocks.
    pub committed_at_ms: u64,
    /// Caller-supplied annotation, omitted when absent and excluded from filesystem semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Materialized mutations in their authoritative `delta_index` order.
    pub deltas: Vec<WalCommitDelta>,
    /// Content pieces governed by [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inline_content: Vec<WalInlineContent>,
}

/// Contains the namespace identity, numbered range, and commits stored in one WAL object.
///
/// See [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalObjectPayload {
    /// Namespace this WAL object belongs to; recovery rejects cross-namespace content.
    pub namespace_id: NamespaceId,
    /// Contiguous object number checked against the key.
    pub wal_no: WalNo,
    /// Fencing epoch of the writer that proposed this WAL object.
    pub writer_epoch: WriterEpoch,
    /// Visible sequence after this WAL object.
    pub head_seq: ChangeSeq,
    /// Retention floor of the manifest that the writer validated this
    /// object's commits against, or that a fence was planned against (see
    /// [Advancing the retention floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    pub basis_floor_seq: ChangeSeq,
    /// Logical commits in contiguous ascending sequence order.
    pub records: Vec<WalCommitPayload>,
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
}

pub(crate) fn encode_wal_payload_cbor(
    payload: &WalObjectPayload,
) -> Result<Vec<u8>, WalCodecError> {
    validate_wal_inline_content(payload)?;
    validate_wal_revision_layouts(payload)?;
    validate_wal_bind_revisions(payload)?;
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

/// Encodes a WAL payload once, then checksums and compresses its durable document.
pub fn encode_wal_object_envelope_zstd(
    payload: WalObjectPayload,
) -> Result<crate::envelope::EncodedEnvelope<WalObjectPayload>, WalCodecError> {
    let payload_bytes = encode_wal_payload_cbor(&payload)?;
    let payload_checksum = sha256_digest(&payload_bytes);
    let document = WalObjectDocument {
        kind: WalEnvelopeKind::WalObject.as_str().to_owned(),
        format_version: WAL_FORMAT_VERSION,
        payload_checksum: payload_checksum.clone(),
        payload: payload_bytes,
    };
    let mut encoded = Vec::new();
    into_writer(&document, &mut encoded)
        .map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))?;
    let bytes = envelope::compress_document(&encoded)?;
    Ok(crate::envelope::EncodedEnvelope {
        envelope: crate::envelope::VerifiedEnvelope {
            payload_checksum,
            payload,
        },
        bytes,
        document_bytes: encoded.len(),
    })
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
    let payload: WalObjectPayload = from_reader(document.payload.as_slice())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;
    validate_wal_inline_content(&payload)?;
    validate_wal_revision_layouts(&payload)?;
    validate_wal_bind_revisions(&payload)?;

    Ok(WalObjectEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

fn validate_wal_inline_content(payload: &WalObjectPayload) -> Result<(), WalCodecError> {
    let mut total_bytes = 0;
    for record in &payload.records {
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
                if content_ref.owner_namespace_id == payload.namespace_id {
                    let end = reference_ends.entry(&content_ref.content_id).or_default();
                    *end = (*end).max(content_ref.size_bytes);
                }
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
                return Err(invalid(&entry.content_id, "no `append_file_revision` reference in the same commit owned by the WAL object's `namespace_id`"));
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
    for record in &payload.records {
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
    for record in &payload.records {
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

#[cfg(test)]
mod tests {

    use super::*;
    use std::assert_matches;

    fn activity(
        content_bytes: u64,
        file_revisions: u64,
        mutations: u64,
    ) -> Option<crate::manifest::ManifestActivity> {
        let counter =
            |value| crate::manifest::ActivityCounter::parse(value).expect("activity counter");
        Some(crate::manifest::ManifestActivity {
            content_bytes: counter(content_bytes),
            file_revisions: counter(file_revisions),
            mutations: counter(mutations),
        })
    }

    #[test]
    fn committed_activity_counts_full_revisions_and_semantic_groups() {
        let payload = inline_wal_object(&[5, 5, 0]);
        let mut record = payload.records[0].clone();
        record.inline_content.clear();
        record.deltas = payload
            .records
            .into_iter()
            .flat_map(|record| record.deltas)
            .collect();
        // Several deltas in one group count once, and indices can have gaps.
        record.deltas[0].semantic_operation_index = 2;
        record.deltas[1].semantic_operation_index = 2;
        record.deltas[2].semantic_operation_index = 9;
        assert_eq!(committed_activity(&record), activity(10, 3, 2));
        let mut empty = record.clone();
        empty.deltas.clear();
        assert_eq!(committed_activity(&empty), Some(Default::default()));
        if let WalDelta::AppendFileRevision { content_ref, .. } = &mut record.deltas[0].delta {
            content_ref.size_bytes = crate::MAX_PUBLIC_INTEGER;
        }
        assert_eq!(committed_activity(&record), None);
    }

    #[test]
    fn committed_activity_counts_appended_bytes_once() {
        let mut record = appended_wal_object().records.remove(0);
        assert_eq!(committed_activity(&record), activity(5, 1, 1));
        let repeated = record.deltas[0].clone();
        record.deltas.push(WalCommitDelta {
            semantic_operation_index: 1,
            ..repeated
        });
        assert_eq!(committed_activity(&record), activity(13, 2, 2));
    }

    fn inline_wal_object(lengths: &[usize]) -> WalObjectPayload {
        let namespace_id = NamespaceId::parse("bounded").expect("namespace");
        let records: Vec<WalCommitPayload> = lengths
            .iter()
            .enumerate()
            .map(|(index, &length)| {
                let bytes = vec![42; length];
                let content_id =
                    ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id");
                WalCommitPayload {
                    committed_seq: ChangeSeq(index as u64 + 1),
                    commit_id: CommitId::parse(format!("c_{index:032x}")).expect("commit id"),
                    committed_by: crate::ActorId::parse("test").expect("actor"),
                    semantic_commit_fingerprint: CommitFingerprint::parse(format!(
                        "v1:sha256:{}",
                        crate::sha256_hex(b"test")
                    ))
                    .expect("fingerprint"),
                    committed_at_ms: 0,
                    message: None,
                    deltas: vec![WalCommitDelta {
                        semantic_operation_index: 0,
                        delta: WalDelta::AppendFileRevision {
                            delta_index: 0,
                            inode_id: InodeId(2),
                            parent_inode_id: crate::ROOT_INODE_ID,
                            name_key: NameKey::parse("file.txt").expect("name key"),
                            display_name: DisplayName::parse("file.txt").expect("display name"),
                            bind_position: DeltaPosition {
                                seq: ChangeSeq(1),
                                delta_index: 0,
                            },
                            child_created_by: crate::ActorId::parse("test").expect("actor"),
                            child_created_at_ms: 0,
                            revision_no: RevisionNo(index as u64 + 1),
                            content_ref: ContentRef::blob_v1(
                                namespace_id.clone(),
                                content_id.clone(),
                                &bytes,
                                crate::ChecksumAlgorithm::Crc64nvme,
                            ),
                            layout: None,
                            append_offset: None,
                        },
                    }],
                    inline_content: vec![WalInlineContent {
                        content_id,
                        offset: 0,
                        bytes,
                    }],
                }
            })
            .collect();

        WalObjectPayload {
            namespace_id,
            wal_no: WalNo(1),
            writer_epoch: WriterEpoch(1),
            head_seq: ChangeSeq(lengths.len() as u64),
            basis_floor_seq: ChangeSeq(0),
            records,
        }
    }

    fn unchecked_wal_object_bytes(payload: &WalObjectPayload) -> Vec<u8> {
        let mut payload_bytes = Vec::new();
        into_writer(payload, &mut payload_bytes).expect("encode payload directly");
        let document = WalObjectDocument {
            kind: WalEnvelopeKind::WalObject.as_str().to_owned(),
            format_version: WAL_FORMAT_VERSION,
            payload_checksum: sha256_digest(&payload_bytes),
            payload: payload_bytes,
        };
        let mut document_bytes = Vec::new();
        into_writer(&document, &mut document_bytes).expect("encode document directly");
        zstd::stream::encode_all(document_bytes.as_slice(), 0).expect("compress document")
    }

    fn assert_inline_content_rejected(
        payload: WalObjectPayload,
        record_index: usize,
        expected_reason: &str,
    ) {
        let expected_seq = payload.records[record_index].committed_seq;
        let expected_content_id = payload.records[record_index].inline_content[0]
            .content_id
            .clone();
        let decoded_error = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload))
            .expect_err("invalid inline content should not decode");
        let encoded_error = encode_wal_object_envelope_zstd(payload)
            .expect_err("invalid inline content should not encode");
        for error in [decoded_error, encoded_error] {
            assert_eq!(
                error.to_string(),
                format!(
                    "invalid WAL inline content in commit `{expected_seq}` for `content_id` `{expected_content_id}`: {expected_reason}"
                ),
            );
            match error {
                WalCodecError::InvalidWalInlineContent {
                    seq,
                    content_id,
                    reason,
                } => {
                    assert_eq!(seq, expected_seq);
                    assert_eq!(content_id, expected_content_id);
                    assert_eq!(reason, expected_reason);
                }
                other => panic!("expected invalid inline content, got {other:?}"),
            }
        }
    }

    fn assert_inline_content_accepted(payload: WalObjectPayload) {
        let encoded = encode_wal_object_envelope_zstd(payload.clone()).expect("encode WAL object");
        let decoded =
            decode_wal_object_envelope_zstd(encoded.as_bytes()).expect("decode WAL object");
        assert_eq!(decoded.into_payload(), payload);
    }

    #[test]
    fn inline_content_requires_a_local_revision_reference_in_the_same_commit() {
        let expected_reason = "no `append_file_revision` reference in the same commit owned by the WAL object's `namespace_id`";
        let mut missing = inline_wal_object(&[3, 3]);
        missing.records[0].deltas.clear();
        assert_inline_content_rejected(missing, 0, expected_reason);

        let mut wrong_id = inline_wal_object(&[3]);
        wrong_id.records[0].inline_content[0].content_id =
            ContentId::parse("con_7yvjx9gxjm688fxq5tk1v58cgg").expect("content id");
        assert_inline_content_rejected(wrong_id, 0, expected_reason);

        let mut foreign = inline_wal_object(&[3]);
        foreign.namespace_id = NamespaceId::parse("other").expect("namespace");
        assert_inline_content_rejected(foreign, 0, expected_reason);
    }

    #[test]
    fn inline_content_must_end_at_the_longest_reference() {
        let expected_reason = "entries do not end at the reference `size_bytes`";
        let mut payload = inline_wal_object(&[3]);
        payload.records[0].inline_content[0].bytes.push(42);
        assert_inline_content_rejected(payload, 0, expected_reason);

        let mut payload = inline_wal_object(&[3]);
        let mut other_reference = payload.records[0].deltas[0].clone();
        other_reference.semantic_operation_index = 1;
        match &mut other_reference.delta {
            WalDelta::AppendFileRevision {
                delta_index,
                revision_no,
                content_ref,
                ..
            } => {
                *delta_index = 1;
                *revision_no = RevisionNo(2);
                content_ref.size_bytes += 1;
            }
            other => panic!("expected file revision, got {other:?}"),
        }
        payload.records[0].deltas.push(other_reference);
        assert_inline_content_rejected(payload, 0, expected_reason);
    }

    #[test]
    fn inline_content_offsets_must_be_unique_within_each_commit() {
        let mut payload = inline_wal_object(&[0]);
        let entry = payload.records[0].inline_content[0].clone();
        payload.records[0].inline_content.push(entry);
        assert_inline_content_rejected(payload, 0, "duplicate `content_id` and `offset` in commit");
    }

    /// One commit appending `[3, 8)` to a content id, as two entries.
    fn appended_wal_object() -> WalObjectPayload {
        let mut payload = inline_wal_object(&[8]);
        let record = &mut payload.records[0];
        let bytes = std::mem::take(&mut record.inline_content[0].bytes);
        let entry = record.inline_content[0].clone();
        if let WalDelta::AppendFileRevision {
            content_ref,
            layout,
            ..
        } = &mut record.deltas[0].delta
        {
            *layout = Some(crate::ContentLayout {
                extents: vec![crate::ContentExtent {
                    owner_namespace_id: content_ref.owner_namespace_id.clone(),
                    content_id: content_ref.content_id.clone(),
                    object: crate::ExtentObject::Whole,
                    offset: 0,
                    length: 3,
                }],
            });
        }
        record.inline_content = vec![
            WalInlineContent {
                offset: 3,
                bytes: bytes[3..5].to_vec(),
                ..entry.clone()
            },
            WalInlineContent {
                offset: 5,
                bytes: bytes[5..].to_vec(),
                ..entry
            },
        ];
        payload
    }

    #[test]
    fn inline_entries_extend_contiguously() {
        assert_inline_content_accepted(appended_wal_object());
        let mut gap = appended_wal_object();
        gap.records[0].inline_content[1].offset = 6;
        gap.records[0].inline_content[1].bytes.pop();
        assert_inline_content_rejected(gap, 0, "entries are not contiguous");
    }

    #[test]
    fn inline_content_accepts_the_value_limit_and_rejects_one_byte_more() {
        assert_eq!(MAX_WAL_INLINE_CONTENT_BYTES, 262_144);
        assert_inline_content_accepted(inline_wal_object(&[MAX_WAL_INLINE_CONTENT_BYTES]));
        assert_inline_content_rejected(
            inline_wal_object(&[MAX_WAL_INLINE_CONTENT_BYTES + 1]),
            0,
            "value exceeds `MAX_WAL_INLINE_CONTENT_BYTES`",
        );
    }

    #[test]
    fn inline_content_accepts_the_wal_object_limit_and_rejects_one_byte_more() {
        assert_eq!(MAX_WAL_OBJECT_INLINE_CONTENT_BYTES, 4_194_304);
        let mut lengths = vec![
            MAX_WAL_INLINE_CONTENT_BYTES;
            MAX_WAL_OBJECT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES
        ];
        assert_inline_content_accepted(inline_wal_object(&lengths));
        lengths.push(1);
        assert_inline_content_rejected(
            inline_wal_object(&lengths),
            lengths.len() - 1,
            "WAL object inline total exceeds `MAX_WAL_OBJECT_INLINE_CONTENT_BYTES`",
        );
    }

    #[test]
    fn inline_content_is_not_hashed_against_the_reference_checksum() {
        let mut payload = inline_wal_object(&[3]);
        payload.records[0].inline_content[0].bytes[0] = 43;
        assert_inline_content_accepted(payload);
    }

    #[test]
    fn decoder_accepts_the_limit_and_rejects_the_next_byte_before_decoding() {
        let encoded = encode_wal_object_envelope_zstd(WalObjectPayload {
            namespace_id: NamespaceId::parse("n".repeat(crate::MAX_ID_BYTES)).expect("namespace"),
            wal_no: WalNo(crate::MAX_PUBLIC_INTEGER),
            writer_epoch: WriterEpoch(crate::MAX_PUBLIC_INTEGER),
            head_seq: ChangeSeq(crate::MAX_PUBLIC_INTEGER),
            basis_floor_seq: ChangeSeq(crate::MAX_PUBLIC_INTEGER),
            records: Vec::new(),
        })
        .expect("encode");
        let document = zstd::stream::decode_all(encoded.as_bytes()).expect("decompress");
        assert_eq!(document.len(), encoded.document_bytes());
        assert!(document.len() <= WAL_OBJECT_OVERHEAD_BYTES);
        assert_eq!(
            &decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len())
                .expect("at limit"),
            encoded.envelope(),
        );
        assert_matches!(
            decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len() - 1),
            Err(WalCodecError::Envelope(EnvelopeCodecError::DocumentTooLarge { max_bytes, .. }))
                if max_bytes == document.len() - 1
        );
        let invalid = zstd::stream::encode_all(&[0xff; 64][..], 0).expect("compress");
        assert_matches!(
            decode_wal_object_envelope_zstd_with_limit(&invalid, 8),
            Err(WalCodecError::Envelope(
                EnvelopeCodecError::DocumentTooLarge { max_bytes: 8, .. }
            ))
        );
        assert_matches!(
            decode_wal_object_envelope_zstd_with_limit(&invalid, 64),
            Err(WalCodecError::Envelope(EnvelopeCodecError::EnvelopeDecode(
                _
            )))
        );
    }

    #[test]
    fn revision_layouts_equal_the_reference_size_or_first_piece_offset() {
        for (piece_offset, layout_size, valid) in [
            (None, Some(3), true),
            (None, Some(5), false),
            (None, Some(2), false),
            (None, None, false),
            (Some(2), Some(2), true),
            (Some(2), Some(1), false),
            (Some(2), Some(3), false),
            (Some(2), None, true),
            (Some(0), Some(1), false),
            (Some(0), None, true),
        ] {
            let mut payload = inline_wal_object(&[3]);
            if let Some(offset) = piece_offset {
                let piece = &mut payload.records[0].inline_content[0];
                piece.offset = offset;
                piece.bytes.drain(..crate::usize_from_u64(offset));
            } else {
                payload.records[0].inline_content.clear();
            }
            let WalDelta::AppendFileRevision { layout, .. } =
                &mut payload.records[0].deltas[0].delta
            else {
                panic!("revision");
            };
            *layout = layout_size.map(|length| crate::ContentLayout {
                extents: vec![crate::ContentExtent {
                    owner_namespace_id: NamespaceId::parse("other").expect("namespace"),
                    content_id: ContentId::parse("con_7yvjx9gxjm688fxq5tk1v58cgg")
                        .expect("content"),
                    object: crate::ExtentObject::Whole,
                    offset: 0,
                    length,
                }],
            });
            let decoded = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload));
            let encoded = encode_wal_object_envelope_zstd(payload);
            if valid {
                assert!(
                    decoded.is_ok(),
                    "{piece_offset:?}, {layout_size:?}: {decoded:?}"
                );
                assert!(encoded.is_ok());
            } else {
                if piece_offset == Some(0) {
                    for error in [decoded.as_ref().err(), encoded.as_ref().err()] {
                        assert_matches!(
                            error,
                            Some(WalCodecError::InvalidWalRevisionLayout { reason, .. })
                            if *reason == "a whole value carries no layout"
                        );
                    }
                }
                assert_matches!(decoded, Err(WalCodecError::InvalidWalRevisionLayout { .. }));
                assert_matches!(encoded, Err(WalCodecError::InvalidWalRevisionLayout { .. }));
            }
        }
    }

    /// Checks that a bind includes a revision summary exactly when its child
    /// is a file, on both the write and the read side.
    #[test]
    fn a_bind_includes_a_revision_summary_exactly_when_its_child_is_a_file() {
        let summary = RevisionSummary {
            revision_no: RevisionNo(1),
            size_bytes: 5,
            revision_committed_by: crate::ActorId::parse("test").expect("actor"),
            revision_committed_at_ms: 0,
        };
        for (child_kind, revision, rejected) in [
            (InodeKind::File, Some(summary.clone()), None),
            (InodeKind::Directory, None, None),
            (
                InodeKind::File,
                None,
                Some("a file bind requires the file's revision summary"),
            ),
            (
                InodeKind::Directory,
                Some(summary),
                Some("a directory bind can't include a revision summary"),
            ),
        ] {
            let mut payload = inline_wal_object(&[0]);
            let record = &mut payload.records[0];
            record.inline_content.clear();
            record.deltas = vec![WalCommitDelta {
                semantic_operation_index: 0,
                delta: WalDelta::BindDirentry {
                    delta_index: 0,
                    parent_inode_id: crate::ROOT_INODE_ID,
                    name_key: NameKey::parse("child").expect("name key"),
                    display_name: DisplayName::parse("child").expect("display name"),
                    child_inode_id: InodeId(2),
                    child_kind,
                    child_created_by: crate::ActorId::parse("test").expect("actor"),
                    child_created_at_ms: 0,
                    revision,
                },
            }];
            let decoded = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload));
            let encoded = encode_wal_object_envelope_zstd(payload);
            match rejected {
                None => {
                    assert!(decoded.is_ok(), "{child_kind:?}: {decoded:?}");
                    assert!(encoded.is_ok(), "{child_kind:?}");
                }
                Some(expected) => {
                    for error in [decoded.err(), encoded.err()] {
                        assert_matches!(
                            error,
                            Some(WalCodecError::InvalidWalBind { reason, child_inode_id, .. })
                            if reason == expected && child_inode_id == InodeId(2)
                        );
                    }
                }
            }
        }
    }
}
