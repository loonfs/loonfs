//! The namespace manifest format: the durable document naming the
//! metadata segment runs that materialize one namespace file-set version
//! ([manifest format](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#71-manifests-runs-and-segments)).

use crate::control::{ForkBasis, NamespaceStatus, WriterBlock};
use crate::envelope::EnvelopeCodecError;
use crate::metadata_rows::MetadataRowFamily;
use crate::segment_blocks::BlockHandle;
use crate::CompactorEpoch;
use crate::{
    AccessGrants, ActorId, ChangeSeq, InodeId, ManifestNo, MetadataSegmentId, NamespaceId, RunNo,
};
use crate::{PrincipalScope, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::num::NonZeroU64;

/// Version 1 is a zstd-compressed JSON envelope document that contains the
/// payload as a raw JSON fragment.
///
/// `payload_checksum` covers the fragment's exact bytes.
pub const NAMESPACE_MANIFEST_FORMAT_VERSION: u32 = 1;

/// Largest decompressed manifest document allowed by [Control and manifest
/// payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
///
/// A manifest lists every segment of its namespace, and no other format limit
/// bounds how many segments that is, so this limit also bounds how much
/// metadata a namespace can have. A namespace of one million files has a
/// manifest document of about 193 KiB, so the limit is reached at about 2.7
/// billion files. The value is the same as
/// [`MAX_WAL_OBJECT_BYTES`](crate::wal::MAX_WAL_OBJECT_BYTES), so a reader
/// needs at most the same memory to decompress either object. It's a separate
/// constant, so a change to one limit doesn't change which objects of the
/// other family a reader accepts.
pub const MAX_NAMESPACE_MANIFEST_BYTES: usize = 512 * 1024 * 1024;

/// Version 1 uses the [block layout](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a7-block-segment-encoding).
pub const METADATA_SEGMENT_ENCODING: u32 = 1;

/// Identifies the durable payload family carried by a namespace-manifest envelope.
///
/// See [durable object families](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys).
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
/// See [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
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
/// See [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataSegmentRef {
    /// Namespace that stores the segment. This may be a fork source.
    pub owner_namespace_id: NamespaceId,
    /// Immutable segment id used in the durable object key.
    pub segment_id: MetadataSegmentId,
    /// Row schema and lookup ordering encoded in this segment.
    pub family: MetadataRowFamily,
    /// Version of the segment's block layout.
    pub encoding: u32,
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
}

/// A namespace's access mode, which can't be changed after the namespace is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceAccess {
    /// Every caller with the deployment credential can perform every operation.
    Unrestricted {},
    /// Access rows control every operation.
    Acl {
        /// The identity domain that the namespace's principal IDs belong to.
        principal_scope: PrincipalScope,
        /// The root inode's grants when the namespace is created. Usually each initial
        /// administrator has `admin`.
        root_grants: AccessGrants,
    },
}

impl NamespaceAccess {
    /// Returns the unrestricted access mode.
    pub fn unrestricted() -> Self {
        Self::Unrestricted {}
    }

    /// Reports whether every caller holding the deployment credential may do
    /// everything.
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
    /// Full content length of every committed file revision, including reused
    /// content. A revision whose bytes its commit's pieces add counts only
    /// those bytes, so an append counts the bytes it appends.
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
/// See [manifest publication](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#72-publishing-a-materialized-file-set).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceManifestPayload {
    /// Namespace whose materialized state this manifest describes.
    pub namespace_id: NamespaceId,
    /// Namespace creation stamp in Unix milliseconds.
    pub created_at_ms: u64,
    /// Actor that created the namespace, as supplied by the application.
    pub created_by: ActorId,
    /// The namespace's access mode, which can't be changed.
    pub access: NamespaceAccess,
    /// The namespace's retention window in milliseconds, which a scheduled
    /// retention advance uses instead of its host's setting. Unlike the
    /// access mode, a successor can change it (see [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_window_ms: Option<NonZeroU64>,
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
    pub compactor_epoch: CompactorEpoch,
    /// Materialized head sequence, or the final namespace sequence on deletion.
    pub head_seq: ChangeSeq,
    /// Activity committed in this namespace through `head_seq`.
    pub activity: ManifestActivity,
    /// Current writer fencing epoch.
    pub writer_epoch: WriterEpoch,
    /// First inode identity available after replaying the manifest snapshot.
    pub next_inode_id: InodeId,
    /// Run number the next producer allocates. Every run's `run_no` is below it.
    pub next_run_no: RunNo,
    /// Earliest sequence for which retained history remains readable.
    pub retention_floor_seq: ChangeSeq,
    /// Highest `basis_floor_seq` of the WAL objects folded into this
    /// manifest, at most `retention_floor_seq` (see [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    pub acknowledged_floor_seq: ChangeSeq,
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
    /// Returns the oldest non-empty run sequence, or the head when there are none.
    pub fn base_seq(&self) -> ChangeSeq {
        self.runs
            .iter()
            .filter(|run| !run.segments.is_empty())
            .map(|run| run.run_seq)
            .min()
            .unwrap_or(self.head_seq)
    }

    /// Constructs manifest 1 with the root inode reserved and no retention
    /// window.
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
            retention_window_ms: None,
            fork_basis: None,
            status: NamespaceStatus::Active {},
            writer: None,
            manifest_no: ManifestNo(1),
            compactor_epoch: CompactorEpoch(0),
            head_seq: ChangeSeq(0),
            activity: ManifestActivity::default(),
            writer_epoch: WriterEpoch(0),
            next_inode_id: crate::FIRST_ALLOCATABLE_INODE_ID,
            next_run_no: RunNo(0),
            folded_wal_no: WalNo(0),
            retention_floor_seq: ChangeSeq(0),
            acknowledged_floor_seq: ChangeSeq(0),
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
                if self.acknowledged_floor_seq != self.head_seq {
                    return invalid("acknowledged_floor_seq");
                }
            }
            None => {
                if !self.runs.is_empty() {
                    return invalid("runs");
                }
                if self.head_seq != ChangeSeq(0) || self.retention_floor_seq != ChangeSeq(0) {
                    return invalid("head_seq");
                }
                if self.acknowledged_floor_seq != ChangeSeq(0) {
                    return invalid("acknowledged_floor_seq");
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

    /// Rejects changed identity, decreasing counters, and publication after
    /// deletion in `successor`, and a `successor` without the next manifest
    /// number.
    pub fn ensure_successor(&self, successor: &Self) -> Result<(), ManifestChainError> {
        self.ensure_followed_by(successor, true)
    }

    /// Rejects the changes that [`Self::ensure_successor`] rejects in `later`,
    /// a manifest with any higher number.
    ///
    /// Each rule that a successor keeps also holds between any two manifests
    /// of the chain, so a reader that skips the manifests between two numbers
    /// checks the two manifests that it loaded with this.
    pub fn ensure_later(&self, later: &Self) -> Result<(), ManifestChainError> {
        self.ensure_followed_by(later, false)
    }

    fn ensure_followed_by(&self, successor: &Self, next: bool) -> Result<(), ManifestChainError> {
        let invalid = |field: &str| {
            Err(ManifestChainError {
                field: field.to_owned(),
            })
        };
        if successor.namespace_id != self.namespace_id {
            return invalid("namespace_id");
        }
        let numbered = if next {
            self.manifest_no.successor().ok() == Some(successor.manifest_no)
        } else {
            successor.manifest_no > self.manifest_no
        };
        if !numbered {
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
        if successor.acknowledged_floor_seq < self.acknowledged_floor_seq {
            return invalid("acknowledged_floor_seq");
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

/// Encodes a manifest once into a JSON envelope document, with the payload's
/// checksum, and compresses the document.
///
/// [`EncodedEnvelope::document_bytes`](crate::envelope::EncodedEnvelope::document_bytes)
/// is the JSON document's length, which [`MAX_NAMESPACE_MANIFEST_BYTES`] limits.
pub fn encode_namespace_manifest_json(
    payload: NamespaceManifestPayload,
) -> Result<crate::envelope::EncodedEnvelope<NamespaceManifestPayload>, EnvelopeCodecError> {
    let encoded = crate::envelope::encode_json_envelope(
        NamespaceManifestKind::Manifest.as_str(),
        NAMESPACE_MANIFEST_FORMAT_VERSION,
        payload,
    )?;
    let bytes = crate::envelope::compress_document(&encoded.bytes)?;
    Ok(crate::envelope::EncodedEnvelope { bytes, ..encoded })
}

/// Decodes and verifies a durable zstd-compressed namespace-manifest envelope.
///
/// Decoding fails for invalid compression or JSON, a document longer than
/// [`MAX_NAMESPACE_MANIFEST_BYTES`], the wrong kind or version, a checksum
/// mismatch, or an invalid payload. See
/// [manifest publication](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#72-publishing-a-materialized-file-set).
pub fn decode_namespace_manifest_json(
    bytes: &[u8],
) -> Result<NamespaceManifestEnvelope, EnvelopeCodecError> {
    decode_namespace_manifest_json_with_limit(bytes, MAX_NAMESPACE_MANIFEST_BYTES)
}

fn decode_namespace_manifest_json_with_limit(
    bytes: &[u8],
    limit: usize,
) -> Result<NamespaceManifestEnvelope, EnvelopeCodecError> {
    let Some(document) = crate::envelope::decompress_document(bytes, limit)? else {
        return Err(EnvelopeCodecError::DocumentTooLarge {
            kind: NamespaceManifestKind::Manifest.as_str(),
            max_bytes: limit,
        });
    };
    let expected_kind = NamespaceManifestKind::Manifest;
    crate::envelope::decode_json_envelope(&document, NAMESPACE_MANIFEST_FORMAT_VERSION, |found| {
        crate::envelope::ensure_kind(expected_kind.as_str(), found)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        decode_namespace_manifest_json, decode_namespace_manifest_json_with_limit,
        encode_namespace_manifest_json, BlockHandle, MetadataRowFamily, MetadataRunRef,
        MetadataSegmentRef, NamespaceManifestPayload, RunTier,
    };
    use crate::CompactorEpoch;
    use crate::{
        ChangeSeq, InodeId, ManifestNo, MetadataSegmentId, NamespaceId, RunNo, WriterEpoch,
    };
    use std::assert_matches;

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
            successor
                .ensure_successor(&regression)
                .expect_err("a successor whose activity totals go backward should be rejected");
            assert_eq!(regression.activity.checked_add(increment), Some(maximum));

            payload["activity"][field] = (crate::MAX_PUBLIC_INTEGER + 1).into();
            let (_, encoded) = crate::envelope::encode_json_envelope(
                super::NamespaceManifestKind::Manifest.as_str(),
                super::NAMESPACE_MANIFEST_FORMAT_VERSION,
                payload,
            )
            .expect("encode invalid payload")
            .into_parts();
            let stored = crate::envelope::compress_document(&encoded).expect("compress");
            assert_matches!(
                decode_namespace_manifest_json(&stored),
                Err(crate::envelope::EnvelopeCodecError::PayloadDecode(message))
                    if message.contains(&crate::PublicOrdinalRangeError.to_string())
            );
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
                            payload_checksum: crate::sha256_digest(b"source"),
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
        // The retention window is a setting, not identity, so a successor
        // can set it, change it, and clear it.
        let mut predecessor = initial.clone();
        for window_ms in [Some(60_000), Some(1), None] {
            let mut successor = predecessor.clone();
            successor.manifest_no = ManifestNo(predecessor.manifest_no.0 + 1);
            successor.retention_window_ms = window_ms.and_then(std::num::NonZeroU64::new);
            predecessor
                .ensure_successor(&successor)
                .expect("a successor can change the retention window");
            predecessor = successor;
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
    fn a_later_manifest_keeps_the_chain_rules_without_the_next_number() {
        let initial = NamespaceManifestPayload::initial(
            NamespaceId::parse("later").expect("namespace"),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        );
        let mut later = initial.clone();
        later.manifest_no = ManifestNo(4);
        later.head_seq = ChangeSeq(3);
        initial
            .ensure_later(&later)
            .expect("a later manifest may skip numbers");
        assert_eq!(
            initial
                .ensure_successor(&later)
                .expect_err("a successor has the next number")
                .field,
            "manifest_no"
        );
        assert_eq!(
            initial
                .ensure_later(&initial)
                .expect_err("a later manifest has a higher number")
                .field,
            "manifest_no"
        );
        let mut regressed = later.clone();
        regressed.head_seq = ChangeSeq(0);
        assert_eq!(
            later
                .ensure_later(&NamespaceManifestPayload {
                    manifest_no: ManifestNo(9),
                    ..regressed
                })
                .expect_err("a later manifest keeps every counter")
                .field,
            "head_seq"
        );
        let deleted = NamespaceManifestPayload {
            status: crate::control::NamespaceStatus::Deleted {
                deleted_at_ms: 1_500,
            },
            ..later.clone()
        };
        assert_eq!(
            deleted
                .ensure_later(&NamespaceManifestPayload {
                    manifest_no: ManifestNo(7),
                    ..later
                })
                .expect_err("a tombstone ends its namespace")
                .field,
            "status"
        );
    }

    #[test]
    fn first_manifests_and_successors_keep_the_acknowledged_floor() {
        let created = NamespaceManifestPayload::initial(
            NamespaceId::parse("acknowledged").expect("namespace"),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        );
        let fork = NamespaceManifestPayload {
            fork_basis: Some(crate::control::ForkBasis {
                manifest: crate::control::ManifestRef {
                    owner_namespace_id: NamespaceId::parse("source").expect("namespace"),
                    manifest_no: ManifestNo(2),
                    head_seq: ChangeSeq(3),
                    payload_checksum: crate::sha256_digest(b"source"),
                },
                source_pin_id: crate::PinId::parse("pin_00000000000000000002-0000000000000003")
                    .expect("pin"),
            }),
            head_seq: ChangeSeq(3),
            retention_floor_seq: ChangeSeq(3),
            acknowledged_floor_seq: ChangeSeq(3),
            ..created.clone()
        };
        created
            .ensure_first_manifest()
            .expect("a created namespace acknowledges floor 0");
        fork.ensure_first_manifest()
            .expect("a fork acknowledges its fork point");
        for (first, acknowledged_floor_seq) in [(&created, 1), (&fork, 2)] {
            let changed = NamespaceManifestPayload {
                acknowledged_floor_seq: ChangeSeq(acknowledged_floor_seq),
                ..first.clone()
            };
            assert_eq!(
                changed
                    .ensure_first_manifest()
                    .expect_err("a first manifest with another acknowledged floor")
                    .field,
                "acknowledged_floor_seq"
            );
        }
        let mut successor = fork.clone();
        successor.manifest_no = ManifestNo(2);
        fork.ensure_successor(&successor)
            .expect("a successor that copies the acknowledged floor");
        successor.acknowledged_floor_seq = ChangeSeq(2);
        assert_eq!(
            fork.ensure_successor(&successor)
                .expect_err("a successor that lowers the acknowledged floor")
                .field,
            "acknowledged_floor_seq"
        );
    }

    #[test]
    fn namespace_manifest_codec_round_trips_base_only_materialization() {
        let (envelope, encoded) = encode_namespace_manifest_json(NamespaceManifestPayload {
            activity: Default::default(),
            created_at_ms: 1_000,
            created_by: crate::ActorId::parse("test").expect("actor"),
            access: super::NamespaceAccess::Unrestricted {},
            retention_window_ms: None,
            fork_basis: None,
            status: crate::control::NamespaceStatus::Active {},
            writer: None,
            folded_wal_no: crate::WalNo(0),
            compactor_epoch: CompactorEpoch(0),
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(10),

            head_seq: ChangeSeq(10),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(1),
            retention_floor_seq: ChangeSeq(0),
            acknowledged_floor_seq: ChangeSeq(0),
            runs: vec![metadata_run_ref(
                "demo",
                "seg_00000000000000000000000001",
                RunNo(0),
                ChangeSeq(10),
                RunTier::Base,
            )],
        })
        .expect("manifest")
        .into_parts();
        let document: serde_json::Value =
            serde_json::from_slice(&zstd::stream::decode_all(encoded.as_slice()).expect("unzstd"))
                .expect("decode manifest document");
        assert!(document["payload"]
            .get("frozen_base_delta_merges")
            .is_none());
        let decoded = decode_namespace_manifest_json(&encoded).expect("decode manifest");

        assert_eq!(decoded, envelope);
        assert_eq!(decoded.payload.base_seq(), ChangeSeq(10));
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
            retention_window_ms: None,
            fork_basis: None,
            status: crate::control::NamespaceStatus::Active {},
            writer: None,
            folded_wal_no: crate::WalNo(0),
            compactor_epoch: CompactorEpoch(0),
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
            manifest_no: ManifestNo(12),

            head_seq: ChangeSeq(12),
            writer_epoch: WriterEpoch(2),
            next_inode_id: InodeId(42),
            next_run_no: RunNo(2),
            retention_floor_seq: ChangeSeq(0),
            acknowledged_floor_seq: ChangeSeq(0),
            runs: vec![
                metadata_run_ref(
                    "source",
                    "seg_00000000000000000000000001",
                    RunNo(0),
                    ChangeSeq(10),
                    RunTier::Base,
                ),
                metadata_run_ref(
                    "demo",
                    "seg_00000000000000000000000002",
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
    fn decoder_accepts_the_manifest_limit_and_rejects_the_next_byte_before_decoding() {
        let encoded = encode_namespace_manifest_json(NamespaceManifestPayload::initial(
            NamespaceId::parse("bounded").expect("namespace"),
            0,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::unrestricted(),
        ))
        .expect("encode");
        let document = zstd::stream::decode_all(encoded.as_bytes()).expect("decompress");
        assert_eq!(document.len(), encoded.document_bytes());
        assert_eq!(
            &decode_namespace_manifest_json_with_limit(encoded.as_bytes(), document.len())
                .expect("at limit"),
            encoded.envelope(),
        );
        assert_matches!(
            decode_namespace_manifest_json_with_limit(encoded.as_bytes(), document.len() - 1),
            Err(crate::envelope::EnvelopeCodecError::DocumentTooLarge { max_bytes, .. })
                if max_bytes == document.len() - 1
        );
        assert_matches!(
            decode_namespace_manifest_json(&document),
            Err(crate::envelope::EnvelopeCodecError::Decompress(_))
        );
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
            encoding: super::METADATA_SEGMENT_ENCODING,
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
        }
    }
}
