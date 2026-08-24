//! Durable control-object shapes: the head, metadata root, WAL floor,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::WriterEpoch;
use crate::{
    wal_segment_id_start_seq, ChangeSeq, CheckpointId, Checksum, ChecksumAlgorithm, CommitId,
    ContentId, ContentRef, ContentRefKind, ContentStoreId, InodeId, ManifestNo, ManifestObjectId,
    MetadataCompactionId, NamespaceId, UploadId, WalSegmentId,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::num::NonZeroU64;

/// Selects one independently versioned mutable control-object family.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    /// Carries the sole live-visibility and writer-fencing authority.
    WalHead,
    /// Records the earliest sequence for which incremental history is retained.
    WalFloor,
    /// Points to the best known materialized metadata manifest.
    MetadataRoot,
    /// Pins a manifest basis for a user or fork lifecycle.
    CheckpointRecord,
    /// Tracks staged content through upload completion or cleanup.
    UploadSession,
    /// Marks one streaming metadata compaction's output as owned by a job
    /// that is still running.
    CompactionLease,
}

impl ControlObjectKind {
    /// Lists every registered control-object family in stable registry order.
    pub const ALL: [Self; 6] = [
        Self::WalHead,
        Self::WalFloor,
        Self::MetadataRoot,
        Self::CheckpointRecord,
        Self::UploadSession,
        Self::CompactionLease,
    ];

    /// Durable format version for this control object kind.
    ///
    /// Versions are tracked per kind so one kind's payload schema can make a
    /// breaking change without invalidating every other control object.
    /// Version 1 is a JSON envelope document carrying the current payload as
    /// a raw JSON fragment whose checksum covers its exact bytes.
    pub const fn format_version(self) -> u32 {
        match self {
            Self::WalHead => 1,
            Self::WalFloor => 1,
            Self::MetadataRoot => 1,
            Self::CheckpointRecord => 1,
            Self::UploadSession => 1,
            Self::CompactionLease => 1,
        }
    }

    /// Returns the frozen envelope discriminator for this control-object family.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalHead => "wal_head",
            Self::WalFloor => "wal_floor",
            Self::MetadataRoot => "metadata_root",
            Self::CheckpointRecord => "checkpoint_record",
            Self::UploadSession => "upload_session",
            Self::CompactionLease => "compaction_lease",
        }
    }

    /// Parses a registered envelope discriminator, returning `None` for future families.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// Earliest sequence for which incremental WAL history is retained.
///
/// The floor advances monotonically by compare-and-swap and does not control
/// live visibility. Missing or unverifiable floor state must retain more
/// history. Objects below the floor are only deletion candidates; garbage
/// collection still revalidates them before removal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WalFloorState {
    /// Namespace whose retained history this floor bounds.
    pub namespace_id: NamespaceId,
    /// Earliest sequence at which incremental replay remains promised.
    pub floor_seq: ChangeSeq,
    /// Unix-millisecond stamp of the successful floor update, for
    /// observability only and never an ordering or validity input.
    pub updated_at_ms: u64,
}

/// One reference to a namespace manifest.
///
/// Durable objects embed this shape under `manifest`. It identifies the
/// manifest and provides the checksum required to verify it.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRef {
    /// Namespace under whose prefix the manifest and its segments live.
    pub owner_namespace_id: NamespaceId,
    /// Monotonic logical position of the referenced manifest.
    pub manifest_no: ManifestNo,
    /// Immutable object selected at `manifest_no`.
    pub manifest_object_id: ManifestObjectId,
    /// Greatest owner-namespace sequence the referenced manifest materializes.
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
}

/// Cold pointer to the best known materialized metadata root.
///
/// Manifest publication compare-and-swaps this object, never the WAL head,
/// so head watchers see only commits. Updates are monotonic in
/// `manifest.manifest_head_seq`; a same-seq replacement may reference a
/// different manifest (pure compaction), and a lower-seq replacement no-ops.
/// This object never defines live visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRootState {
    /// Namespace whose materialized file set this root selects.
    pub namespace_id: NamespaceId,
    /// Manifest selected by this root. Its owner must be `namespace_id`.
    pub manifest: ManifestRef,
    /// Unix-millisecond wall-clock stamp for observability and GC grace policy, not ordering.
    pub updated_at_ms: u64,
}

/// Status of a compaction lease.
///
/// A job creates an `active` lease. Garbage collection may change an expired
/// lease to `reaping` by compare-and-swap. That update fences the job and is
/// permanent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompactionLeaseStatus {
    /// The job owns its output prefix. `heartbeat_at_ms` determines whether
    /// the lease has expired.
    ///
    /// The braces make serde reject a stray field; a unit variant would
    /// silently accept and discard one.
    Active {},
    /// Garbage collection owns the prefix and the job is fenced.
    Reaping {},
}

impl fmt::Display for CompactionLeaseStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Active {} => "active",
            Self::Reaping {} => "reaping",
        })
    }
}

/// Records ownership of a streaming compaction's output prefix.
///
/// The lease contains no cursor, output descriptor, or progress. The job
/// refreshes it while running. Garbage collection claims an expired lease by
/// compare-and-swap before reclaiming the prefix (format spec, "Garbage
/// collection", rule 12).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCompactionLeaseState {
    /// Job this lease belongs to, which is also the prefix its output sits
    /// under.
    pub job_id: MetadataCompactionId,
    /// Namespace whose family group the job is rebuilding.
    pub namespace_id: NamespaceId,
    /// Writer identity the job runs under, for an operator reading the
    /// object. This is the same label the namespace head records.
    pub writer_id: String,
    /// Who owns the prefix: the job that wrote the lease, or the collector
    /// that claimed it.
    pub status: CompactionLeaseStatus,
    /// Unix-millisecond stamp of the job's first lease write.
    pub started_at_ms: u64,
    /// Unix-millisecond stamp of the most recent lease write, and the only
    /// input to whether an `active` lease has expired.
    pub heartbeat_at_ms: u64,
}

/// Monotonic status of a durable checkpoint record.
///
/// A new record starts active and pins its basis. Explicit release or expiry
/// moves it to the terminal released status by compare-and-swap. Released
/// records serve no reads and are deleted after the release grace period.
/// Creating another pin always creates a new record id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointStatus {
    /// Protects the checkpoint basis and permits reads.
    ///
    /// The braces make serde reject a stray `released_at_ms`; a unit variant
    /// would silently accept and discard that field.
    Active {},
    /// Terminal: the pin is gone and the record is waiting to be deleted.
    Released {
        /// Unix-millisecond stamp written by the release compare-and-swap,
        /// and the only input to when the record may be deleted.
        released_at_ms: u64,
    },
}

impl std::fmt::Display for CheckpointStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Self::Active {} => "active",
            Self::Released { .. } => "released",
        };
        formatter.write_str(status)
    }
}

/// Durable owner and expiry policy of a checkpoint record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointOwner {
    /// An operator-created pin, released explicitly by checkpoint id or by
    /// its declared expiry. The name is a label, not a key: several records
    /// may carry the same name over different bases.
    User {
        /// Operator-facing label that need not be unique.
        name: String,
        /// When garbage collection may release the pin without an explicit request.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at_ms: Option<u64>,
    },
    /// A fork target keeping its source basis alive. Released once the
    /// target namespace is terminally deleted, or once the attempt's lease
    /// expires with no target head to show for it. A live target keeps the
    /// record whatever the lease says.
    Fork {
        /// Fork namespace whose continued existence keeps the source basis pinned.
        target_namespace_id: NamespaceId,
        /// Lease bounding the fork attempt before its target head is installed.
        expires_at_ms: u64,
    },
}

impl CheckpointOwner {
    /// When garbage collection may release this record without asking its owner.
    pub fn expires_at_ms(&self) -> Option<u64> {
        match self {
            Self::User { expires_at_ms, .. } => *expires_at_ms,
            Self::Fork { expires_at_ms, .. } => Some(*expires_at_ms),
        }
    }
}

/// A checkpoint record: pins one metadata manifest (its basis) so garbage
/// collection keeps everything the manifest references.
///
/// Stored as its own object under `checkpoints/`; never part of a manifest
/// and never an input to latest visibility. Created write-then-verify: the
/// record is written `active`, then the basis manifest is re-verified
/// against the floor, and a failed verification flips the record to
/// `released`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRecordState {
    /// Freshly generated record identity, one per logical pin. Nothing
    /// derives it, and no caller supplies it, so a new pin can never land on
    /// a released record's key.
    pub checkpoint_id: CheckpointId,
    /// Source namespace whose manifest and metadata remain pinned.
    pub namespace_id: NamespaceId,
    /// Manifest pinned by this record. Its owner must be `namespace_id`.
    pub manifest: ManifestRef,
    /// Commit identity at the pinned manifest head, verified against its payload.
    pub head_commit_id: CommitId,
    /// Unix-millisecond creation stamp used by GC grace policy, never validity ordering.
    pub created_at_ms: u64,
    /// Party and expiry policy that determine when this pin can be released.
    pub owner: CheckpointOwner,
    /// Current status, advanced only by the one-way release compare-and-swap.
    pub status: CheckpointStatus,
}

/// Links one accepted WAL segment identity to its verified sequence range.
///
/// Pointers in immutable WAL segments accept unknown fields. The mutable head
/// uses a strict decoder for the same shape so a rewrite cannot discard data.
/// Both decoders reject a pointer whose `segment_id` does not encode its
/// `start_seq`.
///
/// See [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WalSegmentPointer {
    /// Segment identity used to derive the immutable object key and expected
    /// to agree with the decoded payload.
    pub segment_id: WalSegmentId,
    /// First logical commit sequence carried by the segment.
    pub start_seq: ChangeSeq,
    /// Final logical commit sequence carried by the segment.
    pub end_seq: ChangeSeq,
    /// Checksum of the referenced segment's payload bytes, in `sha256:<hex>`
    /// form. Must equal the `payload_checksum` in the referenced envelope.
    pub payload_checksum: String,
}

impl<'de> Deserialize<'de> for WalSegmentPointer {
    /// Decodes a pointer and verifies that its id matches `start_seq`.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Stored fields before validating the segment position.
        #[derive(Deserialize)]
        struct StoredWalSegmentPointer {
            segment_id: WalSegmentId,
            start_seq: ChangeSeq,
            end_seq: ChangeSeq,
            payload_checksum: String,
        }

        let stored = StoredWalSegmentPointer::deserialize(deserializer)?;
        validated_wal_segment_pointer(Self {
            segment_id: stored.segment_id,
            start_seq: stored.start_seq,
            end_seq: stored.end_seq,
            payload_checksum: stored.payload_checksum,
        })
    }
}

/// Verifies that a WAL segment id encodes the supplied start sequence.
/// Reclamation derives the sequence from the object key, so a mismatch could
/// cause a live segment to be collected.
pub(crate) fn validate_wal_segment_start_seq(
    segment_id: &WalSegmentId,
    start_seq: ChangeSeq,
) -> Result<(), String> {
    if wal_segment_id_start_seq(segment_id.as_str()) == Some(start_seq) {
        return Ok(());
    }
    Err(format!(
        "wal segment id `{segment_id}` does not encode start seq `{start_seq}`"
    ))
}

/// Strict WAL pointer shape used only while decoding the mutable head.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictWalSegmentPointer {
    segment_id: WalSegmentId,
    start_seq: ChangeSeq,
    end_seq: ChangeSeq,
    payload_checksum: String,
}

impl From<StrictWalSegmentPointer> for WalSegmentPointer {
    fn from(pointer: StrictWalSegmentPointer) -> Self {
        Self {
            segment_id: pointer.segment_id,
            start_seq: pointer.start_seq,
            end_seq: pointer.end_seq,
            payload_checksum: pointer.payload_checksum,
        }
    }
}

/// Applies the shared position check after strict decoding.
fn validated_wal_segment_pointer<E>(pointer: WalSegmentPointer) -> Result<WalSegmentPointer, E>
where
    E: serde::de::Error,
{
    validate_wal_segment_start_seq(&pointer.segment_id, pointer.start_seq).map_err(E::custom)?;
    Ok(pointer)
}

/// Decodes the head's visible WAL tip without accepting unknown fields.
fn strict_wal_segment_pointer<'de, D>(
    deserializer: D,
) -> Result<Option<WalSegmentPointer>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<StrictWalSegmentPointer>::deserialize(deserializer)?
        .map(|pointer| validated_wal_segment_pointer(pointer.into()))
        .transpose()
}

/// Decodes the head's predecessor hints without accepting unknown fields.
fn strict_wal_segment_pointers<'de, D>(deserializer: D) -> Result<Vec<WalSegmentPointer>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<StrictWalSegmentPointer>::deserialize(deserializer)?
        .into_iter()
        .map(|pointer| validated_wal_segment_pointer(pointer.into()))
        .collect()
}

/// Who most recently acquired the writer epoch, and when.
///
/// Observability only, written during the epoch-acquisition CAS. Fencing
/// authority is `writer_epoch` + CAS; nothing may consult this block for
/// commit validity, takeover permission, or expiry, and no wall-clock
/// comparison may gate a publish.
///
/// There is no session identity here: two runs of the same writer are told
/// apart by `acquired_at_ms`, not by an id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterBlock {
    /// Stable writer label supplied by the embedding process for diagnostics.
    pub writer_id: String,
    /// Unix-millisecond stamp of the successful epoch-acquisition CAS.
    pub acquired_at_ms: u64,
}

/// Captures the writer identity and fencing epoch a session must retain while publishing.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredWriter {
    /// Stable writer label copied into the head's observability block.
    pub writer_id: String,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// Status recorded in every namespace head.
///
/// A namespace is either active or permanently deleted. Missing and unknown
/// status values fail decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceStatus {
    /// The namespace serves reads and accepts commits.
    ///
    /// The braces make serde reject a stray field; a unit variant would
    /// silently accept and discard one.
    Active {},
    /// Terminal: the namespace's history has ended. Reads, commits, forks,
    /// and re-creation of the same id are all refused.
    Deleted {},
}

/// Where a fork target's metadata basis lives before the target publishes
/// its own manifest, and the permanent record of what it was forked from.
///
/// Present in every successor head of a fork target, absent in every head of
/// a created namespace. The basis is head-authorized: a reader that resolves
/// through it must verify the loaded manifest against both the namespace id
/// and the checksum recorded here, and report corruption on any mismatch —
/// there is no fallback (format spec, "Resolving the metadata basis").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkBasis {
    /// Source manifest used as the target's initial state. Its owner must
    /// differ from the target namespace. `manifest_head_seq` is the target's
    /// initial sequence.
    pub manifest: ManifestRef,
    /// Source checkpoint record pinning the basis for as long as the target lives.
    pub source_checkpoint_id: CheckpointId,
}

/// Carries the authoritative visibility, allocation, and fencing state of a namespace.
///
/// See [head update authority](../../../docs/specs/format.md#14-head-update-authority).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeadState {
    /// Namespace whose live history this head governs.
    pub namespace_id: NamespaceId,
    /// Immutable content store in which the namespace publishes file bytes.
    /// Minted at creation; a fork target carries its source's, sharing the
    /// content keyspace copy-on-write.
    pub content_store_id: ContentStoreId,
    /// Time the namespace was created, in Unix milliseconds. Sequence numbers
    /// determine order; this value is for display.
    pub created_at_ms: u64,
    /// Provenance and pre-first-flush basis of a fork target; absent for a
    /// created namespace. Immutable for the namespace's life.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_basis: Option<ForkBasis>,
    /// Greatest visible logical commit sequence.
    pub seq: ChangeSeq,
    /// Commit id assigned to `seq`, or the fixed genesis id at sequence zero.
    pub head_commit_id: CommitId,
    /// Current fencing generation; a publisher holding any other epoch is rejected.
    pub writer_epoch: WriterEpoch,
    /// Non-authoritative record of the most recent epoch acquisition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<WriterBlock>,
    /// First namespace-scoped inode identity available for allocation.
    pub next_inode_id: InodeId,
    /// Accepted tip of the visible WAL chain, or `None` before the first commit.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "strict_wal_segment_pointer"
    )]
    pub visible_wal_tip: Option<WalSegmentPointer>,
    /// Bounded newest-first predecessor accelerator below `visible_wal_tip`.
    /// Chain links remain the only history authority — any disagreement
    /// resolves in favor of the chain, and this array never protects anything
    /// from GC.
    /// An empty list is written as `[]`. A head that omits the field fails
    /// to decode.
    #[serde(deserialize_with = "strict_wal_segment_pointers")]
    pub recent_segments: Vec<WalSegmentPointer>,
    /// Whether the namespace is active or terminally deleted. Every head
    /// writes it, and a head that omits it fails to decode.
    pub status: NamespaceStatus,
}

const GENESIS_COMMIT_ID: &str = "c_00000000000000000000000000000000";

/// The commit id every namespace's sequence zero carries, before any commit
/// has landed.
pub fn genesis_commit_id() -> CommitId {
    CommitId::parse(GENESIS_COMMIT_ID).expect("genesis commit id is valid")
}

/// A successor head changed one of the namespace's immutable identity
/// fields. Every head a namespace ever publishes carries them forward
/// verbatim from the head that created it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadIdentityDrift {
    /// Which field the successor changed.
    pub field: String,
}

impl fmt::Display for HeadIdentityDrift {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "successor head changes the namespace's immutable `{}`",
            self.field
        )
    }
}

impl HeadState {
    /// Constructs the active sequence-zero head with the root inode already reserved.
    pub fn initial(
        namespace_id: NamespaceId,
        content_store_id: ContentStoreId,
        created_at_ms: u64,
    ) -> Self {
        Self {
            namespace_id,
            content_store_id,
            created_at_ms,
            fork_basis: None,
            seq: ChangeSeq(0),
            head_commit_id: CommitId::parse(GENESIS_COMMIT_ID).expect("genesis commit id is valid"),
            writer_epoch: WriterEpoch(0),
            writer: None,
            // Inode 1 is the root directory; inode 2 is the first assignable id.
            next_inode_id: crate::FIRST_ALLOCATABLE_INODE_ID,
            visible_wal_tip: None,
            recent_segments: Vec::new(),
            status: NamespaceStatus::Active {},
        }
    }

    /// Checks that `successor` carries this head's immutable identity
    /// forward verbatim.
    ///
    /// The head is the only durable home of the namespace's content store
    /// and fork provenance, so every publication that rewrites
    /// the head must copy them unchanged. Publishers call this before the
    /// compare-and-swap: a drifting successor is a construction bug, not a
    /// state to persist.
    pub fn ensure_successor_identity(
        &self,
        successor: &HeadState,
    ) -> Result<(), HeadIdentityDrift> {
        let drift = |field: &str| {
            Err(HeadIdentityDrift {
                field: field.to_owned(),
            })
        };
        if successor.namespace_id != self.namespace_id {
            return drift("namespace_id");
        }
        if successor.content_store_id != self.content_store_id {
            return drift("content_store_id");
        }
        if successor.created_at_ms != self.created_at_ms {
            return drift("created_at_ms");
        }
        if successor.fork_basis != self.fork_basis {
            return drift("fork_basis");
        }
        Ok(())
    }
}

/// Staging progress for a service-proxied upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxiedStaging {
    /// No request owns the staging slot and no content has been staged.
    Idle,
    /// One request owns the staging slot.
    Claimed,
    /// Content that passed validation and was recorded by the session.
    Staged(ContentRef),
}

impl Serialize for ProxiedStaging {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        #[serde(tag = "kind", rename_all = "snake_case")]
        enum Shape<'a> {
            Idle {},
            Claimed {},
            Staged { content_ref: &'a ContentRef },
        }

        match self {
            Self::Idle => Shape::Idle {}.serialize(serializer),
            Self::Claimed => Shape::Claimed {}.serialize(serializer),
            Self::Staged(content_ref) => Shape::Staged { content_ref }.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ProxiedStaging {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        StrictProxiedStaging::deserialize(deserializer).map(Into::into)
    }
}

/// Upload mode and its mode-specific state. The mode never changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadSessionMode {
    /// The service receives the bytes and writes the content object itself,
    /// so it learns size and digest from the bytes as they pass.
    ServiceProxied {
        /// Exclusive staging progress, which applies only to this mode.
        staging: ProxiedStaging,
    },
    /// The client writes the whole object through one presigned request.
    DirectPut {
        /// Checksum algorithm chosen when the session began.
        checksum_algorithm: ChecksumAlgorithm,
    },
    /// The client uploads parts and the provider assembles the object.
    ///
    /// Multipart sessions do not store a content reference at creation because
    /// one-pass and streaming clients may not know the final size or checksum.
    /// The client supplies those values at completion, when LoonFS verifies the
    /// assembled object.
    DirectMultipart {
        /// The provider-side upload the parts assemble through, and the
        /// only provider handle LoonFS keeps: parts are the client's
        /// bookkeeping, exactly as they are in the provider's own API, so
        /// there is no durable record per part.
        provider_upload_id: String,
        /// Byte length of every part except the last, settled at begin.
        ///
        /// A session resumed after a lost begin response reads its geometry
        /// from here rather than being told a second, possibly different,
        /// one. Zero is not a geometry, so it is not representable.
        part_size_bytes: NonZeroU64,
        /// Checksum algorithm chosen when the session began. Part signing and
        /// completion continue to use it after a restart.
        checksum_algorithm: ChecksumAlgorithm,
    },
}

impl UploadSessionMode {
    /// Returns the content reference stored by this mode, when present.
    fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::ServiceProxied {
                staging: ProxiedStaging::Staged(content_ref),
            } => Some(content_ref),
            Self::ServiceProxied { .. } | Self::DirectPut { .. } | Self::DirectMultipart { .. } => {
                None
            }
        }
    }
}

/// Monotonic status of a durable upload session.
///
/// A session starts open and ends as completed or aborted. The terminal update
/// uses compare-and-swap and cannot be reversed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadSessionRecordStatus {
    /// Accepts staged bytes until its lease expires.
    Open {
        /// Unix-millisecond instant after which the session is abandoned.
        /// The record carries it so no session transition depends on an
        /// object's provider timestamp.
        expires_at_ms: u64,
    },
    /// The content is durable and verified. Only completed sessions can issue
    /// receipts or replay completion.
    Completed {
        /// Unix-millisecond stamp written by the completing compare-and-swap,
        /// and the only input to when the content may be reclaimed.
        completed_at_ms: u64,
        /// Verified immutable content produced by this session.
        content_ref: ContentRef,
    },
    /// The session cannot publish content. Its unreferenced object is deleted.
    Aborted {
        /// Unix-millisecond stamp written by the aborting compare-and-swap,
        /// and the only input to when the record may be deleted.
        aborted_at_ms: u64,
    },
}

impl UploadSessionRecordStatus {
    /// Returns the completed content reference, if present.
    fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::Open { .. } => None,
            Self::Completed { content_ref, .. } => Some(content_ref),
            Self::Aborted { .. } => None,
        }
    }
}

impl std::fmt::Display for UploadSessionRecordStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = match self {
            Self::Open { .. } => "open",
            Self::Completed { .. } => "completed",
            Self::Aborted { .. } => "aborted",
        };
        formatter.write_str(status)
    }
}

/// Tracks one durable content-upload workflow independently of commit publication.
///
/// The tagged mode and status variants permit only valid field
/// combinations.
///
/// See [upload before publish](../../../docs/specs/format.md#242-upload-before-publish).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UploadSessionState {
    /// Namespace authorized to consume the staged content.
    pub namespace_id: NamespaceId,
    /// Durable session identity used by staging and completion requests.
    pub upload_id: UploadId,
    /// Content object this session writes, allocated when the session began.
    ///
    /// The identity exists before any byte is read, so the final object key
    /// is known up front and belongs to exactly this session. Every
    /// reference the record holds names this object; see `validate` below,
    /// which refuses a record that disagrees with itself.
    pub content_id: ContentId,
    /// Unix-millisecond creation stamp.
    pub created_at_ms: u64,
    /// How the bytes reach object storage, settled when the session opened.
    pub mode: UploadSessionMode,
    /// The session's status, and the field every upload operation
    /// compare-and-swaps against.
    pub status: UploadSessionRecordStatus,
}

impl UploadSessionState {
    /// Proves the relationships this record's shape cannot express but every
    /// reader of one depends on.
    ///
    /// Every reference the record holds is about the same content object,
    /// whose identity the session allocated before any byte moved. A record
    /// whose references disagree with its own `content_id` describes two
    /// objects and cannot be acted on — a completion would verify one key
    /// and publish another.
    ///
    fn validate(&self) -> Result<(), String> {
        for content_ref in self
            .mode
            .content_ref()
            .into_iter()
            .chain(self.status.content_ref())
        {
            content_ref.validate().map_err(|error| {
                format!(
                    "upload session `{}` holds an invalid content ref: {error}",
                    self.upload_id
                )
            })?;
            if content_ref.content_id != self.content_id {
                return Err(format!(
                    "upload session `{}` owns content `{}` but holds a reference to `{}`",
                    self.upload_id, self.content_id, content_ref.content_id
                ));
            }
        }
        if let (
            UploadSessionMode::DirectPut { checksum_algorithm },
            UploadSessionRecordStatus::Completed { content_ref, .. },
        ) = (&self.mode, &self.status)
        {
            if content_ref.checksum.algorithm != *checksum_algorithm {
                return Err(format!(
                    "upload session `{}` requires `{checksum_algorithm}` but its completed \
                     content uses `{}`",
                    self.upload_id, content_ref.checksum.algorithm
                ));
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictUploadSessionState {
    namespace_id: NamespaceId,
    upload_id: UploadId,
    content_id: ContentId,
    created_at_ms: u64,
    mode: StrictUploadSessionMode,
    status: StrictUploadSessionRecordStatus,
}

/// Strict upload-mode shape used while decoding a session record.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictUploadSessionMode {
    ServiceProxied {
        staging: StrictProxiedStaging,
    },
    DirectPut {
        checksum_algorithm: ChecksumAlgorithm,
    },
    DirectMultipart {
        provider_upload_id: String,
        part_size_bytes: NonZeroU64,
        checksum_algorithm: ChecksumAlgorithm,
    },
}

impl From<StrictUploadSessionMode> for UploadSessionMode {
    fn from(mode: StrictUploadSessionMode) -> Self {
        match mode {
            StrictUploadSessionMode::ServiceProxied { staging } => Self::ServiceProxied {
                staging: staging.into(),
            },
            StrictUploadSessionMode::DirectPut { checksum_algorithm } => {
                Self::DirectPut { checksum_algorithm }
            }
            StrictUploadSessionMode::DirectMultipart {
                provider_upload_id,
                part_size_bytes,
                checksum_algorithm,
            } => Self::DirectMultipart {
                provider_upload_id,
                part_size_bytes,
                checksum_algorithm,
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictProxiedStaging {
    Idle {},
    Claimed {},
    Staged { content_ref: StrictContentRef },
}

impl From<StrictProxiedStaging> for ProxiedStaging {
    fn from(staging: StrictProxiedStaging) -> Self {
        match staging {
            StrictProxiedStaging::Idle {} => Self::Idle,
            StrictProxiedStaging::Claimed {} => Self::Claimed,
            StrictProxiedStaging::Staged { content_ref } => Self::Staged(content_ref.into()),
        }
    }
}

/// The status read back through the same strict content-ref decoder the
/// rest of the record uses, so a completed session's reference is held to
/// the durable schema rather than the wire one.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictUploadSessionRecordStatus {
    Open {
        expires_at_ms: u64,
    },
    Completed {
        completed_at_ms: u64,
        content_ref: StrictContentRef,
    },
    Aborted {
        aborted_at_ms: u64,
    },
}

impl From<StrictUploadSessionRecordStatus> for UploadSessionRecordStatus {
    fn from(status: StrictUploadSessionRecordStatus) -> Self {
        match status {
            StrictUploadSessionRecordStatus::Open { expires_at_ms } => Self::Open { expires_at_ms },
            StrictUploadSessionRecordStatus::Completed {
                completed_at_ms,
                content_ref,
            } => Self::Completed {
                completed_at_ms,
                content_ref: content_ref.into(),
            },
            StrictUploadSessionRecordStatus::Aborted { aborted_at_ms } => {
                Self::Aborted { aborted_at_ms }
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictContentRef {
    kind: MutableContentRefKind,
    content_id: ContentId,
    size_bytes: u64,
    checksum: Checksum,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MutableContentRefKind {
    BlobV1,
}

impl From<StrictContentRef> for ContentRef {
    fn from(content_ref: StrictContentRef) -> Self {
        let kind = match content_ref.kind {
            MutableContentRefKind::BlobV1 => ContentRefKind::BlobV1,
        };
        Self {
            kind,
            content_id: content_ref.content_id,
            size_bytes: content_ref.size_bytes,
            checksum: content_ref.checksum,
        }
    }
}

impl<'de> Deserialize<'de> for UploadSessionState {
    /// Reads one session record and refuses one that `validate` finds
    /// disagreeing with itself, like any other corruption and with no shim
    /// or salvage.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = StrictUploadSessionState::deserialize(deserializer)?;
        let session = Self {
            namespace_id: record.namespace_id,
            upload_id: record.upload_id,
            content_id: record.content_id,
            created_at_ms: record.created_at_ms,
            mode: record.mode.into(),
            status: record.status.into(),
        };
        session.validate().map_err(serde::de::Error::custom)?;
        Ok(session)
    }
}

/// In-memory view of a control object envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_control_object`] and validated only by [`decode_control_object`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectEnvelope<T> {
    /// Durable-family discriminator that selects `T` and its independent version.
    pub kind: ControlObjectKind,
    /// Family-local format version obtained from `kind`.
    pub format_version: u32,
    /// Digest of the payload JSON exactly as stored in the durable document,
    /// in `sha256:<hex>` form.
    pub payload_checksum: String,
    /// Decoded control state protected by `payload_checksum`.
    pub state: T,
}

impl<T> ControlObjectEnvelope<T>
where
    T: Serialize,
{
    /// Builds a family-versioned envelope and computes its checksum from canonical state JSON.
    ///
    /// Construction fails when `state` cannot be encoded.
    pub fn from_state(kind: ControlObjectKind, state: T) -> Result<Self, EnvelopeCodecError> {
        Ok(Self {
            kind,
            format_version: kind.format_version(),
            payload_checksum: control_payload_checksum(&state)?,
            state,
        })
    }
}

/// Specializes a control envelope for the authoritative namespace head.
pub type HeadStateEnvelope = ControlObjectEnvelope<HeadState>;
/// Specializes a control envelope for a durable upload workflow.
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
/// Specializes a control envelope for the selected materialized manifest.
pub type MetadataRootEnvelope = ControlObjectEnvelope<MetadataRootState>;
/// Specializes a control envelope for the retained-history floor.
pub type WalFloorEnvelope = ControlObjectEnvelope<WalFloorState>;
/// Specializes a control envelope for a durable manifest pin.
pub type CheckpointRecordEnvelope = ControlObjectEnvelope<CheckpointRecordState>;
/// Specializes a control envelope for a running compaction's ownership of
/// its staged output.
pub type MetadataCompactionLeaseEnvelope = ControlObjectEnvelope<MetadataCompactionLeaseState>;

/// Computes the checksum stored beside canonical JSON for a control state.
///
/// Computation fails when `state` cannot be serialized.
pub fn control_payload_checksum<T>(state: &T) -> Result<String, EnvelopeCodecError>
where
    T: Serialize,
{
    crate::envelope::json_payload_checksum(state)
}

/// Encodes a control-object envelope as its durable JSON representation.
///
/// Encoding fails when the family version is unsupported, the in-memory
/// checksum is stale, or JSON serialization fails. See
/// [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
pub fn encode_control_object<T>(
    envelope: &ControlObjectEnvelope<T>,
) -> Result<Vec<u8>, EnvelopeCodecError>
where
    T: Serialize,
{
    crate::envelope::encode_json_envelope(
        envelope.kind.as_str(),
        envelope.format_version,
        envelope.kind.format_version(),
        &envelope.payload_checksum,
        &envelope.state,
    )
}

/// Encodes state in a control-object envelope.
pub fn encode_control_state<T: Serialize>(
    kind: ControlObjectKind,
    state: &T,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    let envelope = ControlObjectEnvelope::from_state(kind, state)?;
    encode_control_object(&envelope)
}

/// Decodes and verifies a durable JSON control object of `expected_kind`.
///
/// Decoding fails for invalid JSON, an unknown or mismatched kind, an
/// unsupported family version, a checksum mismatch, or an invalid `T`. See
/// [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
pub fn decode_control_object<T>(
    bytes: &[u8],
    expected_kind: ControlObjectKind,
) -> Result<ControlObjectEnvelope<T>, EnvelopeCodecError>
where
    T: DeserializeOwned,
{
    let decoded = crate::envelope::decode_strict_json_envelope(
        bytes,
        expected_kind.format_version(),
        // The kind registry reports unknown kinds distinctly from
        // registered-but-mismatched ones.
        |found| match ControlObjectKind::parse(found) {
            None => Err(EnvelopeCodecError::UnknownKind {
                found: found.to_owned(),
            }),
            Some(kind) if kind != expected_kind => Err(EnvelopeCodecError::KindMismatch {
                expected: expected_kind.as_str().to_owned(),
                found: found.to_owned(),
            }),
            Some(_) => Ok(()),
        },
    )?;

    Ok(ControlObjectEnvelope {
        kind: expected_kind,
        format_version: decoded.format_version,
        payload_checksum: decoded.payload_checksum,
        state: decoded.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_object_kind_strings_round_trip_and_match_serde() {
        for kind in ControlObjectKind::ALL {
            assert_eq!(ControlObjectKind::parse(kind.as_str()), Some(kind));
            let serialized = serde_json::to_value(kind).expect("serialize kind");
            assert_eq!(serialized, serde_json::Value::from(kind.as_str()));
        }
        assert_eq!(ControlObjectKind::parse("not_a_kind"), None);
    }

    fn sample_head() -> HeadState {
        HeadState::initial(
            NamespaceId::parse("demo").expect("valid namespace id"),
            ContentStoreId::parse("cs_0123456789abcdef0123456789abcdef")
                .expect("valid content store id"),
            1_000,
        )
    }

    #[test]
    fn head_without_content_store_is_rejected() {
        // The content store is the namespace's addressing-semantics
        // authority; a head that omits it is malformed, never defaulted.
        let missing = serde_json::json!({
            "namespace_id": "demo",
            "seq": 0,
            "head_commit_id": GENESIS_COMMIT_ID,
            "writer_epoch": 0,
            "next_inode_id": 2,
            "status": { "kind": "active" }
        });

        serde_json::from_value::<HeadState>(missing)
            .expect_err("head without its immutable identity must be rejected");
    }

    /// One WAL pointer as it appears inside a durable head payload.
    fn wal_pointer_json(segment_id: &str, start_seq: u64, end_seq: u64) -> serde_json::Value {
        serde_json::json!({
            "segment_id": segment_id,
            "start_seq": start_seq,
            "end_seq": end_seq,
            "payload_checksum": format!("sha256:{}", "b".repeat(64)),
        })
    }

    /// A decodable head payload carrying whatever tip and accelerator the
    /// caller wants to present. Both fields are omitted when empty, exactly
    /// as the encoder writes them.
    fn head_json(
        visible_wal_tip: Option<serde_json::Value>,
        recent_segments: Vec<serde_json::Value>,
    ) -> serde_json::Value {
        let mut head = serde_json::json!({
            "namespace_id": "demo",
            "content_store_id": "cs_0123456789abcdef0123456789abcdef",
            "created_at_ms": 1_000,
            "seq": 2,
            "head_commit_id": GENESIS_COMMIT_ID,
            "writer_epoch": 0,
            "next_inode_id": 2,
            "status": { "kind": "active" }
        });
        if let Some(tip) = visible_wal_tip {
            head["visible_wal_tip"] = tip;
        }
        head["recent_segments"] = serde_json::Value::Array(recent_segments);
        head
    }

    #[test]
    fn head_decodes_with_a_tip_and_no_predecessor_hints() {
        let tip = wal_pointer_json("wal_00000000000000000002-fedcba9876543210", 2, 2);

        let head = serde_json::from_value::<HeadState>(head_json(Some(tip), Vec::new()))
            .expect("the first published segment has no predecessor hints");
        assert!(head.visible_wal_tip.is_some());
        assert!(head.recent_segments.is_empty());
    }

    #[test]
    fn predecessor_hints_do_not_repeat_the_tip() {
        let tip = wal_pointer_json("wal_00000000000000000002-fedcba9876543210", 2, 2);
        let older = wal_pointer_json("wal_00000000000000000001-0123456789abcdef", 1, 1);

        let head = serde_json::from_value::<HeadState>(head_json(Some(tip), vec![older.clone()]))
            .expect("predecessor hints decode independently of the authoritative tip");
        assert_eq!(
            head.recent_segments,
            vec![serde_json::from_value(older).expect("valid predecessor pointer")]
        );
    }

    #[test]
    fn head_rejects_a_pointer_field_it_does_not_define() {
        let mut tip = wal_pointer_json("wal_00000000000000000002-fedcba9876543210", 2, 2);
        tip["object_key"] = serde_json::json!(
            "namespaces/demo/wal/segments/wal_00000000000000000002-fedcba9876543210.wal.zst"
        );

        serde_json::from_value::<HeadState>(head_json(Some(tip.clone()), Vec::new()))
            .expect_err("the head rejects a field its tip pointer does not define");

        let older = wal_pointer_json("wal_00000000000000000001-0123456789abcdef", 1, 1);
        serde_json::from_value::<HeadState>(head_json(Some(older), vec![tip]))
            .expect_err("the head rejects a field a predecessor hint does not define");
    }

    #[test]
    fn wal_pointers_reject_an_id_that_disagrees_with_its_start_seq() {
        let agreeing = wal_pointer_json("wal_00000000000000000002-fedcba9876543210", 2, 2);
        serde_json::from_value::<WalSegmentPointer>(agreeing)
            .expect("a pointer whose id encodes its start seq decodes");

        let disagreeing = wal_pointer_json("wal_00000000000000000003-fedcba9876543210", 2, 2);
        let error = serde_json::from_value::<WalSegmentPointer>(disagreeing)
            .expect_err("a pointer whose id disagrees with its start seq is corruption");
        let message = error.to_string();
        assert!(
            message.contains("`wal_00000000000000000003-fedcba9876543210`")
                && message.contains("start seq `2`"),
            "the rejection should name both values: {message}"
        );
    }

    #[test]
    fn the_head_rejects_a_pointer_whose_id_disagrees_with_its_start_seq() {
        let tip = wal_pointer_json("wal_00000000000000000003-aaaaaaaaaaaaaaaa", 3, 3);
        let older = wal_pointer_json("wal_00000000000000000002-fedcba9876543210", 2, 2);
        serde_json::from_value::<HeadState>(head_json(Some(tip.clone()), vec![older.clone()]))
            .expect("pointers whose ids encode their start seqs decode");

        let drifted_tip = wal_pointer_json("wal_00000000000000000004-aaaaaaaaaaaaaaaa", 3, 3);
        let error = serde_json::from_value::<HeadState>(head_json(Some(drifted_tip), vec![older]))
            .expect_err("the head rejects a tip that disagrees with its start seq");
        let message = error.to_string();
        assert!(
            message.contains("`wal_00000000000000000004-aaaaaaaaaaaaaaaa`")
                && message.contains("start seq `3`"),
            "the rejection should name both values: {message}"
        );

        let drifted_hint = wal_pointer_json("wal_00000000000000000001-fedcba9876543210", 2, 2);
        serde_json::from_value::<HeadState>(head_json(Some(tip), vec![drifted_hint]))
            .expect_err("the head rejects a hint that disagrees with its start seq");
    }

    #[test]
    fn genesis_head_decodes_without_a_tip_or_hints() {
        let genesis = serde_json::from_value::<HeadState>(head_json(None, Vec::new()))
            .expect("a head with no visible tip decodes");
        assert_eq!(genesis.visible_wal_tip, None);
        assert!(genesis.recent_segments.is_empty());
    }

    #[test]
    fn a_head_that_omits_its_predecessor_hints_does_not_decode() {
        let mut head = head_json(None, Vec::new());
        assert_eq!(head["recent_segments"], serde_json::json!([]));
        head.as_object_mut()
            .expect("the head is a JSON object")
            .remove("recent_segments");

        let error = serde_json::from_value::<HeadState>(head)
            .expect_err("a head without `recent_segments` is corruption");
        assert!(
            error.to_string().contains("recent_segments"),
            "the rejection should name the field: {error}"
        );
    }

    #[test]
    fn control_object_codec_round_trips_and_validates() {
        let envelope = HeadStateEnvelope::from_state(ControlObjectKind::WalHead, sample_head())
            .expect("envelope");

        let encoded = encode_control_object(&envelope).expect("encode");
        let decoded: HeadStateEnvelope =
            decode_control_object(&encoded, ControlObjectKind::WalHead).expect("decode");
        assert_eq!(decoded, envelope);

        let mismatch =
            decode_control_object::<MetadataRootState>(&encoded, ControlObjectKind::MetadataRoot)
                .expect_err("kind mismatch");
        assert!(matches!(mismatch, EnvelopeCodecError::KindMismatch { .. }));
    }

    #[test]
    fn successor_head_must_carry_the_namespace_identity_forward() {
        let head = sample_head();
        let mut successor = head.clone();
        successor.seq = ChangeSeq(4);
        head.ensure_successor_identity(&successor)
            .expect("advancing the sequence keeps the identity");

        let mut drifted = head.clone();
        drifted.content_store_id = ContentStoreId::parse("cs_fedcba9876543210fedcba9876543210")
            .expect("valid content store id");
        assert_eq!(
            head.ensure_successor_identity(&drifted)
                .expect_err("content store drift is rejected")
                .field,
            "content_store_id"
        );

        let mut forked = head.clone();
        forked.fork_basis = Some(ForkBasis {
            manifest: ManifestRef {
                owner_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
                manifest_no: ManifestNo(7),
                manifest_object_id: ManifestObjectId::parse(
                    "man_00000000000000000007-0123456789abcdef",
                )
                .expect("valid manifest object id"),
                manifest_head_seq: ChangeSeq(7),
                manifest_payload_checksum: "sha256:test".to_owned(),
            },
            source_checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000002")
                .expect("valid checkpoint id"),
        });
        assert_eq!(
            head.ensure_successor_identity(&forked)
                .expect_err("gaining a fork basis is rejected")
                .field,
            "fork_basis"
        );
    }
}
