//! The WAL segment format: envelopes, commit payloads, and the delta
//! records replay applies (format spec, "WAL segments").

use crate::digest::sha256_digest;
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::manifest::{DeletedDirentry, TombstoneGeneration};
use crate::{
    AttributeRevisionNo, Attributes, ChangeSeq, CommitFingerprint, CommitId, ContentRef,
    DisplayName, InodeId, InodeKind, NameKey, NamespaceId, RevisionNo, WalNo, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};
use std::io::Read;

/// Reader limit on a decompressed WAL segment. A valid segment holds one
/// publish batch, which admission keeps far below this; the limit stops a
/// corrupt object from expanding without end.
pub const MAX_WAL_SEGMENT_DECODED_BYTES: u64 = 256 * 1024 * 1024;

/// Version 1: a zstd-compressed CBOR envelope document carrying the payload
/// as an opaque CBOR byte string. `payload_checksum` covers exactly those
/// bytes, and delta/precondition tags use the snake_case names the format
/// spec fixes ("Standard mutation operations" and "Preconditions").
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Identifies the durable payload family carried by a WAL envelope.
///
/// See [WAL segment rules](../../../docs/specs/format.md#a5-wal-records).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEnvelopeKind {
    /// Marks an immutable segment in one namespace's authoritative WAL chain.
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
}

/// Associates a materialized WAL delta with the semantic operation that produced it.
///
/// See [logical commits](../../../docs/specs/format.md#12-commits-and-revisions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalCommitDelta {
    /// Zero-based request-operation position used to attribute one or more resulting deltas.
    pub semantic_op_index: u32,
    /// Replay mutation produced for that semantic operation.
    pub delta: WalDelta,
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
}

/// Carries the namespace-specific chain metadata and commits stored in one WAL object.
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
    decode_wal_segment_envelope_zstd_bounded(bytes, MAX_WAL_SEGMENT_DECODED_BYTES)
}

fn decode_wal_segment_envelope_zstd_bounded(
    bytes: &[u8],
    limit_bytes: u64,
) -> Result<WalSegmentEnvelope, EnvelopeCodecError> {
    let mut decompressed = Vec::new();
    zstd::Decoder::new(bytes)
        .and_then(|decoder| {
            decoder
                .take(limit_bytes.saturating_add(1))
                .read_to_end(&mut decompressed)
        })
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
    if decompressed.len() as u64 > limit_bytes {
        return Err(EnvelopeCodecError::DecompressedSizeExceeded { limit_bytes });
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

    Ok(WalSegmentEnvelope {
        payload_checksum: document.payload_checksum,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wal_segment_expanding_past_the_reader_limit_is_refused() {
        let bytes = zstd::stream::encode_all(&[0u8; 1024][..], crate::sst_blocks::ZSTD_LEVEL)
            .expect("compress");
        assert!(matches!(
            decode_wal_segment_envelope_zstd_bounded(&bytes, 16),
            Err(EnvelopeCodecError::DecompressedSizeExceeded { limit_bytes: 16 })
        ));
    }
}
