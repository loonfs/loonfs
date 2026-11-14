//! The namespace manifest format: the durable document naming the
//! metadata segment runs that store one version of a namespace's metadata
//! ([manifest format](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#71-manifests-runs-and-segments)).

use crate::control::{ForkBasis, NamespaceStatus, WriterBlock};
use crate::envelope::EnvelopeCodecError;
use crate::metadata_rows::MetadataRowFamily;
use crate::segment_blocks::BlockHandle;
use crate::CompactorEpoch;
use crate::PoolId;
use crate::{AccessGrants, ActorId, CommitSeq, ManifestNo, MetadataSegmentId, NamespaceId, RunNo};
use crate::{ExtensionName, IndexSegmentId, PrincipalScope, WalNo, WriterEpoch};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
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

/// Identifies the durable payload family in a namespace-manifest envelope.
///
/// See [durable object families](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a8-object-keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceManifestKind {
    /// Marks the document that lists the manifest's runs.
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

/// Identifies the compaction tier that contains a metadata run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTier {
    /// Contains rows that no compaction has dropped.
    Delta,
    /// Contains rows produced by a compaction over the oldest run.
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
    pub run_seq: CommitSeq,
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
    /// Where the segment's extent filter lives and how to verify it. Present
    /// exactly when `family` is `content_layouts`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extent_filter_block: Option<BlockHandle>,
}

/// One extension's entry in a namespace manifest
/// ([Extensions](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#124-extensions)).
///
/// Core reads the entry's `version` and `runs`, and it copies the entry's
/// other members forward unchanged, as JSON values that it doesn't interpret.
/// The extension decodes those members itself, and it rejects a member that
/// it doesn't define, so core keeps unknown members instead of rejecting them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtensionEntry {
    /// The version of the extension's format, which governs the entry's other
    /// members and the rows of its segments.
    pub version: u32,
    /// The extension's runs, in the extension's list order.
    pub runs: Vec<ExtensionRunRef>,
    /// The entry's other members, which only the extension interprets.
    #[serde(flatten)]
    state: serde_json::Map<String, serde_json::Value>,
}

impl ExtensionEntry {
    /// Builds an entry from its version, its segments, and the extension's
    /// other members.
    ///
    /// # Panics
    ///
    /// Panics if `state` has a `version` or `runs` member, because the entry's
    /// own fields have those names.
    pub fn new(
        version: u32,
        runs: Vec<ExtensionRunRef>,
        state: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        assert!(
            !state.contains_key("version") && !state.contains_key("runs"),
            "an extension entry's state should not repeat the entry's own members"
        );
        Self {
            version,
            runs,
            state,
        }
    }

    /// Returns the entry's members other than `version` and `runs`.
    pub fn state(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.state
    }

    /// Returns the segments of the entry's runs, in list order.
    pub fn segments(&self) -> impl Iterator<Item = &ExtensionSegmentRef> {
        self.runs.iter().flat_map(|run| &run.segments)
    }
}

/// One run of an extension: the segments that the extension wrote together,
/// under one run number.
///
/// See [Control and manifest
/// payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRunRef {
    /// The run's number, which only the extension interprets.
    pub run_no: RunNo,
    /// The run's level, which only the extension interprets.
    pub level: u32,
    /// The run's segments, in the extension's list order.
    pub segments: Vec<ExtensionSegmentRef>,
}

/// Reference to one immutable segment of an extension's run.
///
/// The fields have the meanings of the [`MetadataSegmentRef`] fields of the
/// same names. See [Control and manifest
/// payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionSegmentRef {
    /// The namespace that stores the segment, which can be a fork's source.
    pub owner_namespace_id: NamespaceId,
    /// The immutable segment ID used in the durable object key.
    pub segment_id: IndexSegmentId,
    /// The number of rows in the segment.
    pub row_count: u64,
    /// The inclusive least row key.
    pub min_row_key: String,
    /// The inclusive greatest row key.
    pub max_row_key: String,
    /// The location and verification data for the segment's index block.
    pub index_block: BlockHandle,
    /// The location and verification data for the segment's filter block.
    pub filter_block: BlockHandle,
    /// The filter block's stored bytes inlined as hex, present when the
    /// filter is small.
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

    /// Reports whether every caller with the deployment credential may do
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

/// Contains one complete namespace file-set description inside a manifest envelope.
///
/// See [manifest publication](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#72-publishing-a-manifest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceManifestPayload {
    /// Namespace whose metadata this manifest describes.
    pub namespace_id: NamespaceId,
    /// Namespace creation stamp in Unix milliseconds.
    pub created_at_ms: u64,
    /// Actor that created the namespace, as supplied by the application.
    pub created_by: ActorId,
    /// The namespace's access mode, which can't be changed.
    pub access: NamespaceAccess,
    /// The namespace's retention window in milliseconds, which a scheduled
    /// retention advance uses. A namespace without a window isn't advanced
    /// on a schedule. Unlike the
    /// access mode, a successor can change it (see [Advancing the retention
    /// floor](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#101-advancing-the-retention-floor)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_window_ms: Option<NonZeroU64>,
    /// Fork provenance and source snapshot identity for this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_basis: Option<ForkBasis>,
    /// The pool whose prefix stores the namespace's content objects and
    /// upload records. A namespace created without a source records a pool ID
    /// that its create call generates, a fork copies its source's pool, and
    /// the value can't change.
    pub pool_id: PoolId,
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
    /// Grep publishes the steps of one maintenance interval together, so a lost race costs
    /// that interval's steps.
    pub compactor_epoch: CompactorEpoch,
    /// Folded head, or the final namespace sequence on deletion.
    pub head_seq: CommitSeq,
    /// Activity committed in this namespace through `head_seq`.
    pub activity: ManifestActivity,
    /// Current writer fencing epoch.
    pub writer_epoch: WriterEpoch,
    /// Run number the next producer allocates. Every metadata run's `run_no` is below it.
    pub next_run_no: RunNo,
    /// Earliest sequence for which retained history remains readable.
    pub retention_floor_seq: CommitSeq,
    /// Complete set of metadata runs required to reconstruct the snapshot.
    pub runs: Vec<MetadataRunRef>,
    /// Each extension's entry, under the extension's name. Every publication
    /// copies each entry forward, except a namespace tombstone, which copies
    /// each entry with its runs emptied.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<ExtensionName, ExtensionEntry>,
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
    pub fn base_seq(&self) -> CommitSeq {
        self.runs
            .iter()
            .filter(|run| !run.segments.is_empty())
            .map(|run| run.run_seq)
            .min()
            .unwrap_or(self.head_seq)
    }

    /// Constructs manifest 1 of a namespace without a source, with the status
    /// `initializing`, no retention window and no fork basis, which records
    /// `pool_id`.
    pub fn initial(
        namespace_id: NamespaceId,
        pool_id: PoolId,
        created_at_ms: u64,
        created_by: ActorId,
        access: NamespaceAccess,
    ) -> Self {
        Self {
            namespace_id,
            pool_id,
            created_at_ms,
            created_by,
            access,
            retention_window_ms: None,
            fork_basis: None,
            status: NamespaceStatus::Initializing {},
            writer: None,
            manifest_no: ManifestNo(1),
            compactor_epoch: CompactorEpoch(0),
            head_seq: CommitSeq(0),
            activity: ManifestActivity::default(),
            writer_epoch: WriterEpoch(0),
            next_run_no: RunNo(0),
            folded_wal_no: WalNo(0),
            retention_floor_seq: CommitSeq(0),
            runs: Vec::new(),
            extensions: BTreeMap::new(),
        }
    }

    /// Constructs manifest 1 of a fork of `source` at `fork_basis`, with the
    /// status `initializing`, which lists the runs and copies the extension
    /// entries of `source`, records the pool of `source`, and starts its
    /// retention floor at the fork's head.
    pub fn initial_fork(
        namespace_id: NamespaceId,
        created_at_ms: u64,
        created_by: ActorId,
        fork_basis: ForkBasis,
        source: &Self,
    ) -> Self {
        let fork_seq = fork_basis.manifest.head_seq;
        Self {
            retention_window_ms: source.retention_window_ms,
            fork_basis: Some(fork_basis),
            head_seq: source.head_seq,
            next_run_no: source.next_run_no,
            retention_floor_seq: fork_seq,
            runs: source.runs.clone(),
            extensions: source.extensions.clone(),
            status: NamespaceStatus::Initializing {},
            ..Self::initial(
                namespace_id,
                source.pool_id.clone(),
                created_at_ms,
                created_by,
                source.access.clone(),
            )
        }
    }

    /// Returns manifest 2 of an initializing namespace: this manifest with the
    /// status `active`. The publication assigns the number.
    pub fn activated(&self) -> Self {
        Self {
            status: NamespaceStatus::Active {},
            ..self.clone()
        }
    }

    /// Returns this manifest as the namespace's tombstone, deleted at
    /// `deleted_at_ms`, which lists no extension segments. The publication
    /// assigns the number.
    pub fn tombstone(&self, deleted_at_ms: u64) -> Self {
        let mut tombstone = Self {
            status: NamespaceStatus::Deleted { deleted_at_ms },
            ..self.clone()
        };
        // A tombstone lists no extension segments, so they're collected once
        // no snapshot keeps a manifest that lists them (format spec,
        // "Extensions").
        for entry in tombstone.extensions.values_mut() {
            entry.runs.clear();
        }
        tombstone
    }

    /// Returns the manifest after this one, which has the next manifest
    /// number, lists `runs`, and copies every other field of this manifest,
    /// including each extension entry.
    pub fn successor(
        &self,
        runs: Vec<MetadataRunRef>,
    ) -> Result<Self, crate::PublicOrdinalRangeError> {
        Ok(Self {
            namespace_id: self.namespace_id.clone(),
            created_at_ms: self.created_at_ms,
            created_by: self.created_by.clone(),
            access: self.access.clone(),
            retention_window_ms: self.retention_window_ms,
            fork_basis: self.fork_basis.clone(),
            pool_id: self.pool_id.clone(),
            status: self.status,
            writer: self.writer.clone(),
            folded_wal_no: self.folded_wal_no,
            manifest_no: self.manifest_no.successor()?,
            compactor_epoch: self.compactor_epoch,
            head_seq: self.head_seq,
            activity: self.activity,
            writer_epoch: self.writer_epoch,
            next_run_no: self.next_run_no,
            retention_floor_seq: self.retention_floor_seq,
            runs,
            extensions: self.extensions.clone(),
        })
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
        if self.status != (NamespaceStatus::Initializing {}) {
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
                if !self.extensions.is_empty() {
                    return invalid("extensions");
                }
                if self.head_seq != CommitSeq(0) || self.retention_floor_seq != CommitSeq(0) {
                    return invalid("head_seq");
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
    /// Each rule that a successor keeps is also true between any two manifests
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
        if successor.pool_id != self.pool_id {
            return invalid("pool_id");
        }
        if self.status.is_deleted() {
            return invalid("status");
        }
        match (self.status, successor.status) {
            (_, NamespaceStatus::Initializing {}) => return invalid("status"),
            // Manifest 2 of an initializing namespace changes only its status,
            // so it's manifest 1 activated or manifest 1's tombstone.
            (NamespaceStatus::Initializing {}, status) if next => {
                let expected = match status {
                    NamespaceStatus::Deleted { deleted_at_ms } => self.tombstone(deleted_at_ms),
                    _ => self.activated(),
                };
                let expected = Self {
                    manifest_no: successor.manifest_no,
                    ..expected
                };
                if *successor != expected {
                    return invalid("status");
                }
            }
            _ => {}
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
/// [manifest publication](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#72-publishing-a-manifest).
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
        encode_namespace_manifest_json, NamespaceManifestPayload,
    };
    use crate::{CommitSeq, ManifestNo, NamespaceId};
    use std::assert_matches;

    fn pool_id() -> crate::PoolId {
        crate::PoolId::parse("pool_3s0drmj8em84yb4mnetqkz2j0x").expect("valid pool ID")
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
            pool_id(),
            0,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::unrestricted(),
        )
        .activated();
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
        successor.head_seq = CommitSeq(1);
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
            regression.head_seq = CommitSeq(2);
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
            pool_id(),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        )
        .activated();
        let mut next = initial.clone();
        next.manifest_no = ManifestNo(2);
        for (field, change) in [
            ("namespace_id", 0),
            ("created_at_ms", 1),
            ("fork_basis", 2),
            ("access", 3),
            ("pool_id", 4),
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
                            head_seq: CommitSeq(0),
                            payload_checksum: crate::sha256_digest(b"source"),
                        },
                        source_snapshot_id: crate::SnapshotId::parse(
                            "snp_00000000000000000001-00000000000000000000000001",
                        )
                        .expect("snapshot"),
                        forked_from_snapshot_id: crate::SnapshotId::parse(
                            "snp_00000000000000000001-00000000000000000000000002",
                        )
                        .expect("snapshot"),
                        rebased_from: None,
                    })
                }
                4 => successor.pool_id = crate::PoolId::generate(),
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
    fn an_initializing_manifest_one_is_followed_only_by_its_activation_or_tombstone() {
        let source = NamespaceManifestPayload::initial(
            NamespaceId::parse("source").expect("namespace"),
            pool_id(),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        )
        .activated();
        let snapshot_id = |random: u8| {
            crate::SnapshotId::parse(format!(
                "snp_00000000000000000001-0000000000000000000000000{random}"
            ))
            .expect("snapshot")
        };
        let fork_basis = crate::control::ForkBasis {
            manifest: crate::control::ManifestRef {
                owner_namespace_id: source.namespace_id.clone(),
                manifest_no: ManifestNo(1),
                head_seq: CommitSeq(0),
                payload_checksum: crate::sha256_digest(b"source"),
            },
            source_snapshot_id: snapshot_id(1),
            forked_from_snapshot_id: snapshot_id(2),
            rebased_from: None,
        };
        let initializing = NamespaceManifestPayload::initial_fork(
            NamespaceId::parse("fork").expect("namespace"),
            2_000,
            crate::ActorId::parse("test").expect("actor"),
            fork_basis,
            &source,
        );
        initializing
            .ensure_first_manifest()
            .expect("a fork's manifest 1 is initializing");
        let numbered = |payload: NamespaceManifestPayload| NamespaceManifestPayload {
            manifest_no: ManifestNo(2),
            ..payload
        };
        let active = numbered(initializing.activated());
        initializing
            .ensure_successor(&active)
            .expect("manifest 2 activates manifest 1");
        initializing
            .ensure_successor(&numbered(initializing.tombstone(3_000)))
            .expect("manifest 2 deletes manifest 1");
        let status_error = |predecessor: &NamespaceManifestPayload, successor| {
            predecessor
                .ensure_successor(&successor)
                .expect_err("the successor's status should be refused")
                .field
        };
        assert_eq!(
            status_error(&initializing, numbered(initializing.clone())),
            "status"
        );
        let mut advanced = active.clone();
        advanced.head_seq = CommitSeq(1);
        assert_eq!(status_error(&initializing, advanced), "status");
        let mut initializing_after_active = active.clone();
        initializing_after_active.manifest_no = ManifestNo(3);
        initializing_after_active.status = crate::control::NamespaceStatus::Initializing {};
        assert_eq!(status_error(&active, initializing_after_active), "status");

        let mut active_fork = initializing.clone();
        active_fork.status = crate::control::NamespaceStatus::Active {};
        for first in [active_fork, source.clone()] {
            assert_eq!(
                first
                    .ensure_first_manifest()
                    .expect_err("manifest 1 should be initializing")
                    .field,
                "status"
            );
        }
    }

    #[test]
    fn a_later_manifest_keeps_the_chain_rules_without_the_next_number() {
        let initial = NamespaceManifestPayload::initial(
            NamespaceId::parse("later").expect("namespace"),
            pool_id(),
            1_000,
            crate::ActorId::parse("test").expect("actor"),
            super::NamespaceAccess::Unrestricted {},
        )
        .activated();
        let mut later = initial.clone();
        later.manifest_no = ManifestNo(4);
        later.head_seq = CommitSeq(3);
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
        regressed.head_seq = CommitSeq(0);
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
    fn decoder_accepts_the_manifest_limit_and_rejects_the_next_byte_before_decoding() {
        let encoded = encode_namespace_manifest_json(NamespaceManifestPayload::initial(
            NamespaceId::parse("bounded").expect("namespace"),
            pool_id(),
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
}
