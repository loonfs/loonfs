//! The WAL segment format: envelopes, commit payloads, and the delta
//! records replay applies (format spec, "WAL segments").

use crate::control::WalSegmentPointer;
use crate::digest::sha256_digest;
use crate::envelope::{self, EnvelopeCodecError, EnvelopeProbe};
use crate::manifest::{required_option, DeletedDirentry, TombstoneGeneration};
use crate::{
    AttributeRevisionNo, Attributes, ChangeSeq, CommitId, ContentRef, DisplayName, InodeId,
    InodeKind, NameKey, NamespaceId, RevisionNo, WalSegmentId, WriterEpoch,
};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{Deserialize, Serialize};

/// Version 1: a zstd-compressed CBOR envelope document carrying the payload
/// as an opaque CBOR byte string. `payload_checksum` covers exactly those
/// bytes, and delta/precondition tags use the snake_case names the format
/// spec fixes ("Standard mutation operations" and "Preconditions").
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Identifies the durable payload family carried by a WAL envelope.
///
/// See [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
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
/// See [standard mutation operations](../../../docs/specs/format.md#35-standard-mutation-operations).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
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
        /// The binding a path delete removed, carried so the deleted name
        /// survives on the immortal tombstone row after unbind rows age
        /// out; `null` for a delete addressed by inode. Stated either way
        /// and never defaulted, so the pre-grouping layout — which spelled
        /// the binding as three optional delta fields — does not decode.
        #[serde(deserialize_with = "required_option")]
        deleted_direntry: Option<DeletedDirentry>,
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
/// See [logical commits](../../../docs/specs/format.md#33-logical-commits-sequence-numbers-and-visibility).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCommitDelta {
    /// Zero-based request-operation position used to attribute one or more resulting deltas.
    pub semantic_op_index: u32,
    /// Replay mutation produced for that semantic operation.
    pub delta: WalDelta,
}

/// Carries one accepted logical commit inside a WAL segment.
///
/// See [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalCommitPayload {
    /// Namespace-wide commit position; segment records must cover their range contiguously.
    pub seq: ChangeSeq,
    /// Caller idempotency key whose reuse must retain the same semantic fingerprint.
    pub commit_id: CommitId,
    /// Application-asserted identity that caused this logical commit.
    pub actor: crate::ActorRef,
    /// Digest of semantic request content used to reject conflicting `commit_id` reuse.
    pub semantic_commit_fingerprint: String,
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
/// See [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPayload {
    /// Namespace this segment belongs to; recovery rejects cross-namespace content.
    pub namespace_id: NamespaceId,
    /// Immutable object identity expected to agree with the head pointer and object key.
    pub segment_id: WalSegmentId,
    /// Fencing epoch of the writer that proposed this segment.
    pub writer_epoch: WriterEpoch,
    /// Previous accepted chain member, or `None` only when no visible segment precedes this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_visible_segment: Option<WalSegmentPointer>,
    /// Head sequence the writer materialized against before adding these records.
    pub base_head_seq: ChangeSeq,
    /// Sequence of the first record, and the position encoded into `segment_id`.
    pub start_seq: ChangeSeq,
    /// Sequence of the final record, checked against both `records` and the head pointer.
    pub end_seq: ChangeSeq,
    /// Logical commits in contiguous ascending sequence order.
    pub records: Vec<WalCommitPayload>,
}

/// In-memory view of a WAL segment envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_wal_segment_envelope_zstd`] and validated only by
/// [`decode_wal_segment_envelope_zstd`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentEnvelope {
    /// Durable-family discriminator checked before payload decoding.
    pub kind: WalEnvelopeKind,
    /// Family-local format version, which must equal [`WAL_FORMAT_VERSION`].
    pub format_version: u32,
    /// Digest of the encoded payload bytes exactly as stored in the durable
    /// document, in `sha256:<hex>` form.
    pub payload_checksum: String,
    /// Decoded namespace segment content protected by `payload_checksum`.
    pub payload: WalSegmentPayload,
}

impl WalSegmentEnvelope {
    /// Builds a versioned envelope and computes its checksum from canonical CBOR payload bytes.
    ///
    /// Construction fails when the payload cannot be encoded.
    pub fn from_payload(payload: WalSegmentPayload) -> Result<Self, EnvelopeCodecError> {
        Ok(Self {
            kind: WalEnvelopeKind::NamespaceWalSegment,
            format_version: WAL_FORMAT_VERSION,
            payload_checksum: wal_payload_checksum(&payload)?,
            payload,
        })
    }

    /// Projects the integrity and sequence metadata needed to link this stored segment from a head.
    pub fn pointer(&self, object_key: String) -> WalSegmentPointer {
        WalSegmentPointer {
            object_key,
            segment_id: self.payload.segment_id.clone(),
            start_seq: self.payload.start_seq,
            end_seq: self.payload.end_seq,
            payload_checksum: self.payload_checksum.clone(),
        }
    }
}

/// Durable layout of a WAL segment object (before zstd compression): the
/// envelope fields plus the payload as an opaque CBOR byte string.
/// `payload_checksum` covers exactly those bytes, so integrity verification
/// never depends on re-encoding the payload with this build's schema and a
/// payload with unknown additive fields still verifies.
#[derive(Serialize, Deserialize)]
struct WalSegmentDocument {
    kind: String,
    format_version: u32,
    payload_checksum: String,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

pub(crate) fn wal_payload_checksum(
    payload: &WalSegmentPayload,
) -> Result<String, EnvelopeCodecError> {
    Ok(sha256_digest(&encode_wal_payload_cbor(payload)?))
}

pub(crate) fn encode_wal_payload_cbor(
    payload: &WalSegmentPayload,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    let mut encoded = Vec::new();
    into_writer(payload, &mut encoded)
        .map_err(|err| EnvelopeCodecError::PayloadEncode(err.to_string()))?;
    Ok(encoded)
}

/// Encodes a WAL envelope as its durable zstd-compressed CBOR representation.
///
/// Encoding fails when the version is unsupported, the in-memory checksum is
/// stale, CBOR serialization fails, or zstd cannot compress the document. See
/// [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
pub fn encode_wal_segment_envelope_zstd(
    envelope: &WalSegmentEnvelope,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    envelope::verify_version(
        envelope.kind.as_str(),
        envelope.format_version,
        WAL_FORMAT_VERSION,
    )?;
    let payload_bytes = encode_wal_payload_cbor(&envelope.payload)?;
    envelope::verify_checksum_fresh(&envelope.payload_checksum, &payload_bytes)?;

    let document = WalSegmentDocument {
        kind: envelope.kind.as_str().to_owned(),
        format_version: envelope.format_version,
        payload_checksum: envelope.payload_checksum.clone(),
        payload: payload_bytes,
    };
    let mut encoded = Vec::new();
    into_writer(&document, &mut encoded)
        .map_err(|err| EnvelopeCodecError::EnvelopeEncode(err.to_string()))?;
    zstd::stream::encode_all(encoded.as_slice(), crate::sst_blocks::ZSTD_LEVEL)
        .map_err(|err| EnvelopeCodecError::Compress(err.to_string()))
}

/// Decodes and verifies a durable zstd-compressed WAL segment envelope.
///
/// Decoding fails for invalid compression or CBOR, the wrong kind or version,
/// a checksum mismatch, or an invalid payload. See
/// [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
pub fn decode_wal_segment_envelope_zstd(
    bytes: &[u8],
) -> Result<WalSegmentEnvelope, EnvelopeCodecError> {
    let decompressed = zstd::stream::decode_all(bytes)
        .map_err(|err| EnvelopeCodecError::Decompress(err.to_string()))?;
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
        kind: expected_kind,
        format_version: document.format_version,
        payload_checksum: document.payload_checksum,
        payload,
    })
}
