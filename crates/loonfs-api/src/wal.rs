//! The WAL segment format: envelopes, commit payloads, and the delta
//! records replay applies (format spec, "WAL segments").

use crate::digest::sha256_digest;
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::manifest::{DeletedDirentry, TombstoneGeneration};
use crate::{
    AccessGrants, AccessRevisionNo, AttributeRevisionNo, Attributes, ChangeSeq, CommitFingerprint,
    CommitId, ContentId, ContentRef, DisplayName, InodeId, InodeKind, NameKey, NamespaceId,
    RevisionNo, WalNo, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

/// Version 1: a zstd-compressed CBOR envelope document carrying the payload
/// as an opaque CBOR byte string. `payload_checksum` covers exactly those
/// bytes, and delta/precondition tags use the snake_case names the format
/// spec fixes ("Standard mutation operations" and "Preconditions").
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Largest decompressed WAL document allowed by [Appendix A.5](../../../docs/specs/format.md#a5-wal-records).
pub const MAX_WAL_SEGMENT_BYTES: usize = 512 * 1024 * 1024;

/// Reader limit per inline value in
/// [Appendix A.5](../../../docs/specs/format.md#a5-wal-records);
/// writer thresholds are policy at or below this limit.
pub const MAX_WAL_INLINE_CONTENT_BYTES: usize = 256 * 1024;

/// Reader limit for total inline bytes in one WAL segment in
/// [Appendix A.5](../../../docs/specs/format.md#a5-wal-records);
/// writer thresholds are policy at or below this limit.
pub const MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES: usize = 4 * 1024 * 1024;

/// Upper bound for the document and payload fields outside commit records.
pub const WAL_SEGMENT_OVERHEAD_BYTES: usize = cbor_map_bytes(&[
    ("kind", cbor_string_bytes("namespace_wal_segment".len())),
    ("format_version", 5),
    ("payload_checksum", cbor_string_bytes(64)),
    ("payload", 9),
]) + cbor_map_bytes(&[
    ("namespace_id", cbor_string_bytes(crate::ids::MAX_ID_BYTES)),
    ("wal_no", 9),
    ("next_inode_id", 9),
    ("writer_epoch", 9),
    ("base_head_seq", 9),
    ("start_seq", 9),
    ("end_seq", 9),
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
/// See [WAL segment rules](../../../docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    /// Marks an immutable segment in one namespace's numbered WAL.
    NamespaceWalSegment,
}

impl WalEnvelopeKind {
    /// Returns the frozen envelope discriminator written to durable storage.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceWalSegment => "namespace_wal_segment",
        }
    }
}

/// Records one replayable metadata mutation materialized from a semantic commit operation.
///
/// See [standard mutation operations](../../../docs/specs/format.md#66-operations-and-wal-deltas).
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
        /// Commit sequence that created the exact binding being removed.
        bind_seq: ChangeSeq,
        /// Delta position that disambiguates the binding within `bind_seq`.
        bind_delta_index: u32,
    },
    /// Publishes the next immutable content revision of a file inode.
    AppendFileRevision {
        /// Stable position of this revision delta within its commit.
        delta_index: u32,
        /// File inode receiving the revision.
        inode_id: InodeId,
        /// Monotonic per-file revision number validated against visible history.
        revision_no: RevisionNo,
        /// Immutable content that must already be durable before publication.
        content_ref: ContentRef,
    },
    /// Hides a rooted subtree from snapshots at this delta's sequence or later.
    TombstoneSubtree {
        /// Stable position that identifies this tombstone within its commit.
        delta_index: u32,
        /// Inode at the root of the newly hidden subtree.
        root_inode_id: InodeId,
        /// The binding the delete removed, carried so the deleted name
        /// survives on the immortal tombstone row after unbind rows age out.
        deleted_direntry: DeletedDirentry,
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
        /// The exact tombstone generation this delta compensates.
        target: TombstoneGeneration,
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
        attributes_revision_no: AttributeRevisionNo,
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
/// See [logical commits](../../../docs/specs/format.md#12-commits-and-revisions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitDelta {
    /// Internal operation position within this commit. Convenience requests can
    /// expand into several operations; each operation's deltas are contiguous.
    pub semantic_op_index: u32,
    /// Replay mutation produced for that semantic operation.
    pub delta: WalDelta,
}

/// Groups a commit's ordered deltas by their durable semantic operation.
/// Indices are local to the commit and need not be consecutive.
pub fn semantic_operation_groups(
    deltas: &[WalCommitDelta],
) -> impl Iterator<Item = &[WalCommitDelta]> {
    deltas.chunk_by(|left, right| left.semantic_op_index == right.semantic_op_index)
}

/// Counts activity represented by one committed delta vector.
/// Returns `None` if a counter overflows.
pub fn committed_activity(deltas: &[WalCommitDelta]) -> Option<crate::manifest::ManifestActivity> {
    let mut activity = crate::manifest::ManifestActivity::default();
    for group in semantic_operation_groups(deltas) {
        activity.mutations = activity.mutations.checked_add(1)?;
        for delta in group {
            if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                activity.content_bytes =
                    activity.content_bytes.checked_add(content_ref.size_bytes)?;
                activity.file_revisions = activity.file_revisions.checked_add(1)?;
            }
        }
    }
    Some(activity)
}

/// Carries bytes named by a revision delta in the same commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalInlineContent {
    /// Identity shared with the accompanying `blob_v1` reference.
    pub content_id: ContentId,
    /// Complete content encoded as a CBOR byte string.
    #[serde(with = "serde_bytes")]
    pub bytes: Vec<u8>,
}

/// Carries one accepted logical commit inside a WAL segment.
///
/// See [WAL segment rules](../../../docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitPayload {
    /// Namespace-wide commit position; segment records must cover their range contiguously.
    pub seq: ChangeSeq,
    /// Caller idempotency key whose reuse must retain the same semantic fingerprint.
    pub commit_id: CommitId,
    /// Actor that committed the change, as supplied by the application.
    pub committed_by: crate::ActorId,
    /// Digest of semantic request content used to reject conflicting `commit_id` reuse.
    pub semantic_commit_fingerprint: CommitFingerprint,
    /// Wall-clock stamp from the publishing writer's request context, in
    /// Unix milliseconds. Observational only: never a validity or ordering
    /// input — `seq` is the order — and excluded from the semantic commit
    /// fingerprint, so replay identity is untouched by clocks.
    pub committed_at_ms: u64,
    /// Caller-supplied annotation, omitted when absent and excluded from filesystem semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Materialized mutations in their authoritative `delta_index` order.
    pub deltas: Vec<WalCommitDelta>,
    /// Content values governed by [Appendix A.5](../../../docs/specs/format.md#a5-wal-records).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inline_content: Vec<WalInlineContent>,
}

/// Carries the namespace identity, numbered range, and commits stored in one WAL object.
///
/// See [WAL segment rules](../../../docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalSegmentPayload {
    /// Namespace this segment belongs to; recovery rejects cross-namespace content.
    pub namespace_id: NamespaceId,
    /// Contiguous object number checked against the key.
    pub wal_no: WalNo,
    /// Allocation high-water mark after this segment.
    pub next_inode_id: InodeId,
    /// Fencing epoch of the writer that proposed this segment.
    pub writer_epoch: WriterEpoch,
    /// Head sequence the writer materialized against before adding these records.
    pub base_head_seq: ChangeSeq,
    /// Sequence of the first record, or the unchanged head sequence for a fence.
    pub start_seq: ChangeSeq,
    /// Visible sequence after this segment, unchanged for a fence.
    pub end_seq: ChangeSeq,
    /// Logical commits in contiguous ascending sequence order.
    pub records: Vec<WalCommitPayload>,
}

/// A WAL segment decoded through its checked durable codec.
pub type WalSegmentEnvelope = crate::envelope::VerifiedEnvelope<WalSegmentPayload>;

/// Durable layout of a WAL segment object (before zstd compression): the
/// envelope fields plus the payload as an opaque CBOR byte string.
/// `payload_checksum` covers exactly those bytes, so integrity verification
/// never depends on re-encoding the payload with this build's schema. Unknown
/// payload fields are rejected after checksum verification.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WalSegmentDocument {
    kind: String,
    format_version: u32,
    payload_checksum: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

pub(crate) fn encode_wal_payload_cbor(
    payload: &WalSegmentPayload,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    validate_wal_inline_content(payload)?;
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

/// Encodes a WAL payload once, then checksums and compresses its durable document.
pub fn encode_wal_segment_envelope_zstd(
    payload: WalSegmentPayload,
) -> Result<crate::envelope::EncodedEnvelope<WalSegmentPayload>, EnvelopeCodecError> {
    let payload_bytes = encode_wal_payload_cbor(&payload)?;
    let payload_checksum = sha256_digest(&payload_bytes);
    let document = WalSegmentDocument {
        kind: WalEnvelopeKind::NamespaceWalSegment.as_str().to_owned(),
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

/// Decodes and verifies a durable zstd-compressed WAL segment envelope.
///
/// Decoding fails for invalid compression or CBOR, the wrong kind or version,
/// a checksum mismatch, or an invalid payload. See
/// [WAL segment rules](../../../docs/specs/format.md#a5-wal-records).
pub fn decode_wal_segment_envelope_zstd(
    bytes: &[u8],
) -> Result<WalSegmentEnvelope, EnvelopeCodecError> {
    decode_wal_segment_envelope_zstd_with_limit(bytes, MAX_WAL_SEGMENT_BYTES)
}

fn decode_wal_segment_envelope_zstd_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<WalSegmentEnvelope, EnvelopeCodecError> {
    let decoder = zstd::stream::read::Decoder::new(bytes)
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
    let mut decompressed = Vec::new();
    decoder
        .take(limit as u64 + 1)
        .read_to_end(&mut decompressed)
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
    if decompressed.len() > limit {
        return Err(EnvelopeCodecError::WalSegmentTooLarge { max_bytes: limit });
    }
    let probe: EnvelopeProbe = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    let expected_kind = WalEnvelopeKind::NamespaceWalSegment;
    envelope::verify_kind(expected_kind.as_str(), &probe.kind)?;
    envelope::verify_version(&probe.kind, probe.format_version, WAL_FORMAT_VERSION)?;

    let document: WalSegmentDocument = from_reader(decompressed.as_slice())
        .map_err(|err| EnvelopeCodecError::EnvelopeDecode(err.to_string()))?;
    envelope::verify_payload_checksum(&document.payload_checksum, &document.payload)?;
    let payload: WalSegmentPayload = from_reader(document.payload.as_slice())
        .map_err(|err| EnvelopeCodecError::PayloadDecode(err.to_string()))?;
    validate_wal_inline_content(&payload)?;

    Ok(WalSegmentEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

fn validate_wal_inline_content(payload: &WalSegmentPayload) -> Result<(), EnvelopeCodecError> {
    let mut total_bytes = 0;
    for record in &payload.records {
        if record.inline_content.is_empty() {
            continue;
        }
        let mut reference_sizes: BTreeMap<&ContentId, Vec<u64>> = BTreeMap::new();
        for delta in &record.deltas {
            if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                if content_ref.owner_namespace_id == payload.namespace_id {
                    reference_sizes
                        .entry(&content_ref.content_id)
                        .or_default()
                        .push(content_ref.size_bytes);
                }
            }
        }
        let mut content_ids = BTreeSet::new();
        for entry in &record.inline_content {
            let invalid = |reason| EnvelopeCodecError::InvalidWalInlineContent {
                seq: record.seq,
                content_id: entry.content_id.clone(),
                reason,
            };
            if !content_ids.insert(&entry.content_id) {
                return Err(invalid("duplicate `content_id` in commit"));
            }
            if entry.bytes.len() > MAX_WAL_INLINE_CONTENT_BYTES {
                return Err(invalid("value exceeds `MAX_WAL_INLINE_CONTENT_BYTES`"));
            }
            let sizes = reference_sizes.get(&entry.content_id).ok_or_else(|| {
                invalid("no `append_file_revision` reference in the same commit owned by the segment's `namespace_id`")
            })?;
            if !sizes.iter().all(|&size| size == entry.bytes.len() as u64) {
                return Err(invalid("length does not match reference `size_bytes`"));
            }
            total_bytes += entry.bytes.len();
            if total_bytes > MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES {
                return Err(invalid(
                    "segment inline total exceeds `MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES`",
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

    #[test]
    fn committed_activity_counts_full_revisions_and_semantic_groups() {
        let mut deltas: Vec<_> = inline_segment(&[5, 5, 0])
            .records
            .into_iter()
            .flat_map(|record| record.deltas)
            .collect();
        // Several deltas in one group count once, and indices can have gaps.
        deltas[0].semantic_op_index = 2;
        deltas[1].semantic_op_index = 2;
        deltas[2].semantic_op_index = 9;
        assert_eq!(
            committed_activity(&deltas),
            Some(crate::manifest::ManifestActivity {
                content_bytes: crate::manifest::ActivityCounter::parse(10).expect("activity"),
                file_revisions: crate::manifest::ActivityCounter::parse(3).expect("activity"),
                mutations: crate::manifest::ActivityCounter::parse(2).expect("activity"),
            })
        );
        assert_eq!(committed_activity(&[]), Some(Default::default()));
        if let WalDelta::AppendFileRevision { content_ref, .. } = &mut deltas[0].delta {
            content_ref.size_bytes = crate::MAX_PUBLIC_INTEGER;
        }
        assert_eq!(committed_activity(&deltas), None);
    }

    fn inline_segment(lengths: &[usize]) -> WalSegmentPayload {
        let namespace_id = NamespaceId::parse("bounded").expect("namespace");
        let records = lengths
            .iter()
            .enumerate()
            .map(|(index, &length)| {
                let bytes = vec![42; length];
                let content_id =
                    ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id");
                WalCommitPayload {
                    seq: ChangeSeq(index as u64 + 1),
                    commit_id: CommitId::parse(format!("c_{index:032x}")).expect("commit id"),
                    committed_by: crate::ActorId::parse("test").expect("actor"),
                    semantic_commit_fingerprint: serde_json::from_str(r#""v1:sha256:test""#)
                        .expect("fingerprint"),
                    committed_at_ms: 0,
                    message: None,
                    deltas: vec![WalCommitDelta {
                        semantic_op_index: 0,
                        delta: WalDelta::AppendFileRevision {
                            delta_index: 0,
                            inode_id: InodeId(2),
                            revision_no: RevisionNo(index as u64 + 1),
                            content_ref: ContentRef::blob_v1(
                                namespace_id.clone(),
                                content_id.clone(),
                                &bytes,
                            ),
                        },
                    }],
                    inline_content: vec![WalInlineContent { content_id, bytes }],
                }
            })
            .collect();
        WalSegmentPayload {
            namespace_id,
            wal_no: WalNo(1),
            next_inode_id: InodeId(3),
            writer_epoch: WriterEpoch(1),
            base_head_seq: ChangeSeq(0),
            start_seq: ChangeSeq(1),
            end_seq: ChangeSeq(lengths.len() as u64),
            records,
        }
    }

    fn unchecked_segment_bytes(payload: &WalSegmentPayload) -> Vec<u8> {
        let mut payload_bytes = Vec::new();
        into_writer(payload, &mut payload_bytes).expect("encode payload directly");
        let document = WalSegmentDocument {
            kind: WalEnvelopeKind::NamespaceWalSegment.as_str().to_owned(),
            format_version: WAL_FORMAT_VERSION,
            payload_checksum: sha256_digest(&payload_bytes),
            payload: payload_bytes,
        };
        let mut document_bytes = Vec::new();
        into_writer(&document, &mut document_bytes).expect("encode document directly");
        zstd::stream::encode_all(document_bytes.as_slice(), 0).expect("compress document")
    }

    fn assert_inline_content_rejected(
        payload: WalSegmentPayload,
        record_index: usize,
        expected_reason: &str,
    ) {
        let expected_seq = payload.records[record_index].seq;
        let expected_content_id = payload.records[record_index].inline_content[0]
            .content_id
            .clone();
        let decoded_error = decode_wal_segment_envelope_zstd(&unchecked_segment_bytes(&payload))
            .expect_err("invalid inline content should not decode");
        let encoded_error = encode_wal_segment_envelope_zstd(payload)
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

    fn assert_inline_content_accepted(payload: WalSegmentPayload) {
        let encoded = encode_wal_segment_envelope_zstd(payload.clone()).expect("encode segment");
        let decoded = decode_wal_segment_envelope_zstd(encoded.as_bytes()).expect("decode segment");
        assert_eq!(decoded.into_payload(), payload);
    }

    #[test]
    fn inline_content_requires_a_local_revision_reference_in_the_same_commit() {
        let expected_reason = "no `append_file_revision` reference in the same commit owned by the segment's `namespace_id`";
        let mut missing = inline_segment(&[3, 3]);
        missing.records[0].deltas.clear();
        assert_inline_content_rejected(missing, 0, expected_reason);

        let mut wrong_id = inline_segment(&[3]);
        wrong_id.records[0].inline_content[0].content_id =
            ContentId::parse("con_fedcba9876543210fedcba9876543210").expect("content id");
        assert_inline_content_rejected(wrong_id, 0, expected_reason);

        let mut foreign = inline_segment(&[3]);
        foreign.namespace_id = NamespaceId::parse("other").expect("namespace");
        assert_inline_content_rejected(foreign, 0, expected_reason);
    }

    #[test]
    fn inline_content_length_must_match_the_reference() {
        let mut payload = inline_segment(&[3]);
        payload.records[0].inline_content[0].bytes.push(42);
        assert_inline_content_rejected(payload, 0, "length does not match reference `size_bytes`");

        let mut payload = inline_segment(&[3]);
        let mut other_reference = payload.records[0].deltas[0].clone();
        other_reference.semantic_op_index = 1;
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
        assert_inline_content_rejected(payload, 0, "length does not match reference `size_bytes`");
    }

    #[test]
    fn inline_content_ids_must_be_unique_within_each_commit() {
        let mut payload = inline_segment(&[0]);
        let entry = payload.records[0].inline_content[0].clone();
        payload.records[0].inline_content.push(entry);
        assert_inline_content_rejected(payload, 0, "duplicate `content_id` in commit");
    }

    #[test]
    fn inline_content_accepts_the_value_limit_and_rejects_one_byte_more() {
        assert_eq!(MAX_WAL_INLINE_CONTENT_BYTES, 262_144);
        assert_inline_content_accepted(inline_segment(&[MAX_WAL_INLINE_CONTENT_BYTES]));
        assert_inline_content_rejected(
            inline_segment(&[MAX_WAL_INLINE_CONTENT_BYTES + 1]),
            0,
            "value exceeds `MAX_WAL_INLINE_CONTENT_BYTES`",
        );
    }

    #[test]
    fn inline_content_accepts_the_segment_limit_and_rejects_one_byte_more() {
        assert_eq!(MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES, 4_194_304);
        let mut lengths = vec![
            MAX_WAL_INLINE_CONTENT_BYTES;
            MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES / MAX_WAL_INLINE_CONTENT_BYTES
        ];
        assert_inline_content_accepted(inline_segment(&lengths));
        lengths.push(1);
        assert_inline_content_rejected(
            inline_segment(&lengths),
            lengths.len() - 1,
            "segment inline total exceeds `MAX_WAL_SEGMENT_INLINE_CONTENT_BYTES`",
        );
    }

    #[test]
    fn inline_content_is_not_hashed_against_the_reference_checksum() {
        let mut payload = inline_segment(&[3]);
        payload.records[0].inline_content[0].bytes[0] = 43;
        assert_inline_content_accepted(payload);
    }

    #[test]
    fn decoder_accepts_the_limit_and_rejects_the_next_byte_before_decoding() {
        let encoded = encode_wal_segment_envelope_zstd(WalSegmentPayload {
            namespace_id: NamespaceId::parse("bounded").expect("namespace"),
            wal_no: WalNo(1),
            next_inode_id: InodeId(2),
            writer_epoch: WriterEpoch(1),
            base_head_seq: ChangeSeq(0),
            start_seq: ChangeSeq(0),
            end_seq: ChangeSeq(0),
            records: Vec::new(),
        })
        .expect("encode");
        let document = zstd::stream::decode_all(encoded.as_bytes()).expect("decompress");
        assert_eq!(document.len(), encoded.document_len());
        assert!(document.len() <= WAL_SEGMENT_OVERHEAD_BYTES);
        assert_eq!(
            &decode_wal_segment_envelope_zstd_with_limit(encoded.as_bytes(), document.len())
                .expect("at limit"),
            encoded.envelope(),
        );
        assert!(matches!(
            decode_wal_segment_envelope_zstd_with_limit(encoded.as_bytes(), document.len() - 1),
            Err(EnvelopeCodecError::WalSegmentTooLarge { max_bytes }) if max_bytes == document.len() - 1
        ));
        let invalid = zstd::stream::encode_all(&[0xff; 64][..], 0).expect("compress");
        assert!(matches!(
            decode_wal_segment_envelope_zstd_with_limit(&invalid, 8),
            Err(EnvelopeCodecError::WalSegmentTooLarge { max_bytes: 8 })
        ));
        assert!(matches!(
            decode_wal_segment_envelope_zstd_with_limit(&invalid, 64),
            Err(EnvelopeCodecError::EnvelopeDecode(_))
        ));
    }
}
