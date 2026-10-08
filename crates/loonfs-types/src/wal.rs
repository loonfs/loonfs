//! The WAL object format: envelopes, commit payloads, and the delta
//! records replay applies (format spec, "WAL records").

use crate::digest::sha256_digest;
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::manifest::{DeletedBinding, DeltaPosition};
use crate::{
    AccessGrants, AccessRevisionNo, Attributes, AttributesRevisionNo, ChangeSeq, Checksum,
    ChecksumAlgorithm, CommitFingerprint, CommitId, ContentId, ContentRef, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceId, RevisionNo, Sha256State, WalNo, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;

/// Version 1: a zstd-compressed CBOR envelope document carrying the payload
/// as an opaque CBOR byte string. `payload_checksum` covers exactly those
/// bytes, and delta/precondition tags use the snake_case names the format
/// spec fixes ("Standard mutation operations" and "Preconditions").
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Largest decompressed WAL document allowed by [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
pub const MAX_WAL_OBJECT_BYTES: usize = 512 * 1024 * 1024;

/// Reader limit per inline value in
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
    ("payload_checksum", cbor_string_bytes(64)),
    ("payload", 9),
]) + cbor_map_bytes(&[
    ("namespace_id", cbor_string_bytes(crate::ids::MAX_ID_BYTES)),
    ("wal_no", 9),
    ("writer_epoch", 9),
    ("head_seq", 9),
    ("next_inode_id", 9),
    ("records", 9),
]);

// CBOR strings, collections, and u64 values need at most nine header bytes.
const fn cbor_string_bytes(length: usize) -> usize {
    9 + length
}

const fn cbor_map_bytes(fields: &[(&str, usize)]) -> usize {
    let mut bytes = 9;
    let mut index = 0;
    while index < fields.len() {
        bytes += cbor_string_bytes(fields[index].0.len()) + fields[index].1;
        index += 1;
    }
    bytes
}

/// Identifies the durable payload family carried by a WAL envelope.
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
        /// Newly allocated durable inode identity.
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
    },
    /// Removes one exact historical directory binding without affecting a later rebind.
    UnbindDirentry {
        /// Stable position of this unbind within its commit.
        delta_index: u32,
        /// Directory from which the binding is removed.
        parent_inode_id: InodeId,
        /// Canonical lookup key of the binding being removed.
        name_key: NameKey,
        /// User-facing spelling the removed binding carried, so feed
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
        /// Monotonic per-file revision number validated against visible history.
        revision_no: RevisionNo,
        /// Content of the new revision. Uploaded bytes are durable before
        /// publication; inline bytes travel in this commit's `inline_content`
        /// ([format section 1.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#15-file-contents-and-ownership)).
        content_ref: ContentRef,
        /// SHA-256 state after the reference's bytes, recorded when the
        /// reference is a SHA-256 and the writer had the bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hash_state: Option<Sha256State>,
        /// CRC-64/NVME of the reference's bytes, recorded when the writer had
        /// the bytes or the reference's own checksum is that CRC.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        crc64nvme: Option<Checksum>,
    },
    /// Hides a rooted subtree from snapshots at this delta's sequence or later.
    TombstoneSubtree {
        /// Stable position that identifies this tombstone within its commit.
        delta_index: u32,
        /// Inode at the root of the newly hidden subtree.
        root_inode_id: InodeId,
        /// The binding the delete removed, carried so the deleted name
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
    /// The delta carries the whole resulting map rather than the changes that
    /// produced it, so replay never needs an earlier revision to answer what
    /// an inode holds. An empty map is a real revision: it is the cleared
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
    /// Like an attribute revision, the delta carries the whole resulting
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
/// Indices are local to the commit and need not be consecutive.
pub fn semantic_operation_groups(
    deltas: &[WalCommitDelta],
) -> impl Iterator<Item = &[WalCommitDelta]> {
    deltas.chunk_by(|left, right| left.semantic_operation_index == right.semantic_operation_index)
}

/// Counts activity represented by one committed record.
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

/// Carries bytes of a content object named by a revision delta in the same
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
    /// Content whose first `offset` bytes precede `bytes`, when this entry
    /// starts a chain under a new content id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base: Option<ContentBase>,
}

/// Names the content object a chain copies its first bytes from.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentBase {
    /// Namespace that owns the base content.
    pub owner_namespace_id: NamespaceId,
    /// Identity of the base content.
    pub content_id: ContentId,
}

/// Carries one accepted logical commit inside a WAL object.
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
    /// Content values governed by [Appendix A.5](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inline_content: Vec<WalInlineContent>,
}

/// Carries the namespace identity, numbered range, and commits stored in one WAL object.
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
    /// Allocation high-water mark after this WAL object.
    pub next_inode_id: InodeId,
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
    payload_checksum: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

pub(crate) fn encode_wal_payload_cbor(
    payload: &WalObjectPayload,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    validate_wal_inline_content(payload)?;
    validate_wal_revision_digests(payload)?;
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

/// Encodes a WAL payload once, then checksums and compresses its durable document.
pub fn encode_wal_object_envelope_zstd(
    payload: WalObjectPayload,
) -> Result<crate::envelope::EncodedEnvelope<WalObjectPayload>, EnvelopeCodecError> {
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
    let bytes = zstd::stream::encode_all(encoded.as_slice(), crate::sst_blocks::ZSTD_LEVEL)
        .map_err(|err| EnvelopeCodecError::Compress(err.to_string()))?;
    Ok(crate::envelope::EncodedEnvelope {
        envelope: crate::envelope::VerifiedEnvelope {
            payload_checksum,
            payload,
        },
        bytes,
        document_len: encoded.len(),
    })
}

/// Decodes and verifies a durable zstd-compressed WAL object envelope.
///
/// Decoding fails for invalid compression or CBOR, the wrong kind or version,
/// a checksum mismatch, or an invalid payload. See
/// [WAL object rules](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a5-wal-records).
pub fn decode_wal_object_envelope_zstd(
    bytes: &[u8],
) -> Result<WalObjectEnvelope, EnvelopeCodecError> {
    decode_wal_object_envelope_zstd_with_limit(bytes, MAX_WAL_OBJECT_BYTES)
}

fn decode_wal_object_envelope_zstd_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<WalObjectEnvelope, EnvelopeCodecError> {
    let decoder = zstd::stream::read::Decoder::new(bytes)
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
    let mut decompressed = Vec::new();
    decoder
        .take(limit as u64 + 1)
        .read_to_end(&mut decompressed)
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
    if decompressed.len() > limit {
        return Err(EnvelopeCodecError::WalObjectTooLarge { max_bytes: limit });
    }
    let probe: EnvelopeProbe = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = WalEnvelopeKind::WalObject;
    envelope::verify_kind(expected_kind.as_str(), &probe.kind)?;
    envelope::verify_version(&probe.kind, probe.format_version, WAL_FORMAT_VERSION)?;

    let document: WalObjectDocument = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    envelope::verify_payload_checksum(&document.payload_checksum, &document.payload)?;
    let payload: WalObjectPayload = from_reader(document.payload.as_slice())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;
    validate_wal_inline_content(&payload)?;
    validate_wal_revision_digests(&payload)?;

    Ok(WalObjectEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

/// Checks what a revision delta records about its reference's bytes against
/// the reference (A.4). A `hash_state` is the SHA-256 state after exactly
/// the bytes a SHA-256 reference names, so its length and digest are the
/// reference's. A `crc64nvme` is a CRC-64/NVME, and the reference's own
/// checksum when that is one.
fn validate_wal_revision_digests(payload: &WalObjectPayload) -> Result<(), EnvelopeCodecError> {
    for record in &payload.records {
        for delta in &record.deltas {
            let WalDelta::AppendFileRevision {
                content_ref,
                hash_state,
                crc64nvme,
                ..
            } = &delta.delta
            else {
                continue;
            };
            let invalid = |reason| EnvelopeCodecError::InvalidWalRevisionDigest {
                seq: record.committed_seq,
                content_id: content_ref.content_id.clone(),
                reason,
            };
            let checksum = &content_ref.checksum;
            if let Some(state) = hash_state {
                if checksum.algorithm != ChecksumAlgorithm::Sha256 {
                    return Err(invalid(
                        "`hash_state` on a reference whose checksum is not a SHA-256",
                    ));
                }
                if state.length() != content_ref.size_bytes {
                    return Err(invalid(
                        "`hash_state` length differs from the reference `size_bytes`",
                    ));
                }
                if state.finish() != *checksum {
                    return Err(invalid(
                        "`hash_state` digest differs from the reference checksum",
                    ));
                }
            }
            if let Some(crc) = crc64nvme {
                if crc.algorithm != ChecksumAlgorithm::Crc64nvme || crc.validate().is_err() {
                    return Err(invalid("`crc64nvme` is not a CRC-64/NVME"));
                }
                if checksum.algorithm == ChecksumAlgorithm::Crc64nvme && crc != checksum {
                    return Err(invalid("`crc64nvme` differs from the reference checksum"));
                }
            }
        }
    }
    Ok(())
}

fn validate_wal_inline_content(payload: &WalObjectPayload) -> Result<(), EnvelopeCodecError> {
    let mut total_bytes = 0;
    for record in &payload.records {
        if record.inline_content.is_empty() {
            continue;
        }
        let invalid =
            |content_id: &ContentId, reason| EnvelopeCodecError::InvalidWalInlineContent {
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
                match &entry.base {
                    Some(_) if offset == 0 => {
                        return Err(invalid(content_id, "an entry at offset 0 names a `base`"))
                    }
                    Some(_) if end.is_some() => {
                        return Err(invalid(
                            content_id,
                            "an entry after the first names a `base`",
                        ))
                    }
                    Some(base) if &base.content_id == content_id => {
                        return Err(invalid(
                            content_id,
                            "an entry names its own content as its `base`",
                        ))
                    }
                    _ => {}
                }
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

#[cfg(test)]
mod tests {
    #![allow(clippy::panic)]

    use super::*;

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
                    ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id");
                WalCommitPayload {
                    committed_seq: ChangeSeq(index as u64 + 1),
                    commit_id: CommitId::parse(format!("c_{index:032x}")).expect("commit id"),
                    committed_by: crate::ActorId::parse("test").expect("actor"),
                    semantic_commit_fingerprint: serde_json::from_str(r#""v1:sha256:test""#)
                        .expect("fingerprint"),
                    committed_at_ms: 0,
                    message: None,
                    deltas: vec![WalCommitDelta {
                        semantic_operation_index: 0,
                        delta: WalDelta::AppendFileRevision {
                            delta_index: 0,
                            inode_id: InodeId(2),
                            revision_no: RevisionNo(index as u64 + 1),
                            content_ref: ContentRef::blob_v1(
                                namespace_id.clone(),
                                content_id.clone(),
                                &bytes,
                            ),
                            hash_state: None,
                            crc64nvme: None,
                        },
                    }],
                    inline_content: vec![WalInlineContent {
                        content_id,
                        offset: 0,
                        bytes,
                        base: None,
                    }],
                }
            })
            .collect();

        WalObjectPayload {
            namespace_id,
            wal_no: WalNo(1),
            writer_epoch: WriterEpoch(1),
            head_seq: ChangeSeq(lengths.len() as u64),
            next_inode_id: InodeId(3),
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
                    "invalid wal inline content in commit `{expected_seq}` for `content_id` `{expected_content_id}`: {expected_reason}"
                ),
            );
            match error {
                EnvelopeCodecError::InvalidWalInlineContent {
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
            ContentId::parse("con_fedcba9876543210fedcba9876543210").expect("content id");
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
    fn inline_entries_extend_contiguously_and_only_the_first_names_a_base() {
        assert_inline_content_accepted(appended_wal_object());
        let base = ContentBase {
            owner_namespace_id: NamespaceId::parse("source").expect("namespace"),
            content_id: ContentId::parse("con_fedcba9876543210fedcba9876543210")
                .expect("content id"),
        };
        let mut chain = appended_wal_object();
        chain.records[0].inline_content[0].base = Some(base.clone());
        assert_inline_content_accepted(chain);

        let mut gap = appended_wal_object();
        gap.records[0].inline_content[1].offset = 6;
        gap.records[0].inline_content[1].bytes.pop();
        assert_inline_content_rejected(gap, 0, "entries are not contiguous");

        let mut later_base = appended_wal_object();
        later_base.records[0].inline_content[1].base = Some(base.clone());
        assert_inline_content_rejected(later_base, 0, "an entry after the first names a `base`");

        let mut own_base = appended_wal_object();
        own_base.records[0].inline_content[0].base = Some(ContentBase {
            content_id: own_base.records[0].inline_content[0].content_id.clone(),
            ..base.clone()
        });
        assert_inline_content_rejected(own_base, 0, "an entry names its own content as its `base`");

        let mut first = inline_wal_object(&[3]);
        first.records[0].inline_content[0].base = Some(base);
        assert_inline_content_rejected(first, 0, "an entry at offset 0 names a `base`");
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
            namespace_id: NamespaceId::parse("bounded").expect("namespace"),
            wal_no: WalNo(1),
            writer_epoch: WriterEpoch(1),
            head_seq: ChangeSeq(0),
            next_inode_id: InodeId(2),
            records: Vec::new(),
        })
        .expect("encode");
        let document = zstd::stream::decode_all(encoded.as_bytes()).expect("decompress");
        assert_eq!(document.len(), encoded.document_len());
        assert!(document.len() <= WAL_OBJECT_OVERHEAD_BYTES);
        assert_eq!(
            &decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len())
                .expect("at limit"),
            encoded.envelope(),
        );
        assert!(matches!(
            decode_wal_object_envelope_zstd_with_limit(encoded.as_bytes(), document.len() - 1),
            Err(EnvelopeCodecError::WalObjectTooLarge { max_bytes }) if max_bytes == document.len() - 1
        ));
        let invalid = zstd::stream::encode_all(&[0xff; 64][..], 0).expect("compress");
        assert!(matches!(
            decode_wal_object_envelope_zstd_with_limit(&invalid, 8),
            Err(EnvelopeCodecError::WalObjectTooLarge { max_bytes: 8 })
        ));
        assert!(matches!(
            decode_wal_object_envelope_zstd_with_limit(&invalid, 64),
            Err(EnvelopeCodecError::EnvelopeDecode(_))
        ));
    }

    fn assert_revision_digests_rejected(payload: WalObjectPayload, expected_reason: &str) {
        let expected_seq = payload.records[0].committed_seq;
        let expected_content_id = payload.records[0].inline_content[0].content_id.clone();
        let decoded_error = decode_wal_object_envelope_zstd(&unchecked_wal_object_bytes(&payload))
            .expect_err("invalid revision digests should not decode");
        let encoded_error = encode_wal_object_envelope_zstd(payload)
            .expect_err("invalid revision digests should not encode");
        for error in [decoded_error, encoded_error] {
            assert_eq!(
                error.to_string(),
                format!(
                    "invalid wal revision digest in commit `{expected_seq}` for `content_id` `{expected_content_id}`: {expected_reason}"
                ),
            );
            assert!(matches!(
                error,
                EnvelopeCodecError::InvalidWalRevisionDigest { seq, content_id, reason }
                    if seq == expected_seq && content_id == expected_content_id && reason == expected_reason
            ));
        }
    }

    #[test]
    fn revision_digests_must_describe_the_reference() {
        let digests = |bytes: &[u8]| {
            let mut state = Sha256State::new();
            state.update(bytes);
            (Some(state), Some(Checksum::crc64nvme(bytes)))
        };
        let with_digests =
            |checksum: Option<Checksum>,
             (hash_state, crc64nvme): (Option<Sha256State>, Option<Checksum>)| {
                let mut payload = inline_wal_object(&[3]);
                let WalDelta::AppendFileRevision {
                    content_ref,
                    hash_state: state,
                    crc64nvme: crc,
                    ..
                } = &mut payload.records[0].deltas[0].delta
                else {
                    panic!("the fixture commits one revision");
                };
                if let Some(checksum) = checksum {
                    content_ref.checksum = checksum;
                }
                *state = hash_state;
                *crc = crc64nvme;
                payload
            };
        let crc_reference = Some(Checksum::crc64nvme(&[42; 3]));

        assert_inline_content_accepted(with_digests(None, digests(&[42; 3])));
        assert_inline_content_accepted(with_digests(
            crc_reference.clone(),
            (None, Some(Checksum::crc64nvme(&[42; 3]))),
        ));

        let (state, _) = digests(&[42; 3]);
        assert_revision_digests_rejected(
            with_digests(crc_reference.clone(), (state, None)),
            "`hash_state` on a reference whose checksum is not a SHA-256",
        );
        assert_revision_digests_rejected(
            with_digests(None, digests(&[42; 4])),
            "`hash_state` length differs from the reference `size_bytes`",
        );
        assert_revision_digests_rejected(
            with_digests(None, digests(&[43; 3])),
            "`hash_state` digest differs from the reference checksum",
        );
        assert_revision_digests_rejected(
            with_digests(None, (None, Some(Checksum::sha256(&[42; 3])))),
            "`crc64nvme` is not a CRC-64/NVME",
        );
        assert_revision_digests_rejected(
            with_digests(crc_reference, (None, Some(Checksum::crc64nvme(&[43; 3])))),
            "`crc64nvme` differs from the reference checksum",
        );
    }
}
