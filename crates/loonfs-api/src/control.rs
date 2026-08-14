//! Durable control-object shapes: the head, metadata root, WAL floor,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, Checksum, ChecksumAlgorithm, CommitId, ContentId, ContentRef,
    ContentRefKind, ContentStoreId, InodeId, ManifestId, ManifestObjectId, MetadataCompactionId,
    NamespaceId, UploadId, WalSegmentId,
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
    /// Unix-millisecond wall-clock time when the referenced manifest basis was last verified.
    pub verified_at_ms: u64,
    /// Unix-millisecond wall-clock time stamped by the successful floor update attempt.
    pub updated_at_ms: u64,
}

/// Cold pointer to the best known materialized metadata root.
///
/// Manifest publication compare-and-swaps this object, never the WAL head,
/// so head watchers see only commits. Updates are monotonic in
/// `manifest_head_seq`; a same-seq replacement may reference a different
/// manifest (pure compaction), and a lower-seq replacement no-ops. This
/// object never defines live visibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataRootState {
    /// Namespace whose materialized file set this root selects.
    pub namespace_id: NamespaceId,
    /// Monotonic logical position of the selected manifest.
    pub manifest_id: ManifestId,
    /// Immutable candidate chosen at `manifest_id`.
    pub manifest_object_id: ManifestObjectId,
    /// Greatest namespace sequence represented by the selected manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
    /// Unix-millisecond wall-clock stamp for observability and GC grace policy, not ordering.
    pub updated_at_ms: u64,
}

/// Lifecycle of a compaction lease: two states and one transition.
///
/// A lease is created `active` by the job that owns the prefix. Garbage
/// collection moves it to `reaping` by compare-and-swap once it has expired,
/// and that transition is the fence: the worker's next heartbeat
/// compare-and-swap fails, so it can never publish, and only then may the
/// collector treat the prefix as orphaned. `reaping` is terminal — nothing
/// returns a lease to `active`, and the collector deletes the object once the
/// prefix's unreferenced segments are gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetadataCompactionLeaseStatus {
    /// The job owns its prefix. Whether it is still running is a question
    /// about `heartbeat_at_ms`, not about this field.
    Active,
    /// Terminal: garbage collection claimed the prefix, so the job that
    /// wrote it is fenced and the objects under it are orphans.
    Reaping,
}

impl fmt::Display for MetadataCompactionLeaseStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Active => "active",
            Self::Reaping => "reaping",
        })
    }
}

/// Says who owns the objects under one streaming metadata compaction's job
/// prefix.
///
/// The lease carries lifecycle ownership and nothing else: no cursor, no
/// output descriptor, no offset, no progress. Two parties write it. The job
/// creates it, refreshes it by compare-and-swap while it runs, and stops
/// writing it once its output is published. Garbage collection claims an
/// expired lease by compare-and-swap and reclaims the prefix behind it
/// (format spec, "Garbage collection", rule 12). Whoever wins that
/// compare-and-swap owns the prefix; the loser has lost it for good.
///
/// Nothing about a job's correctness depends on the object surviving — a job
/// that loses its lease loses its output to a later pass and runs again,
/// which is what every other way a job can end already costs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetadataCompactionLeaseState {
    /// Job this lease belongs to, which is also the prefix its output sits
    /// under.
    pub job_id: MetadataCompactionId,
    /// Namespace whose family group the job is rebuilding.
    pub namespace_id: NamespaceId,
    /// Writer identity the job runs under, for an operator reading the
    /// object.
    pub owner_id: String,
    /// Who owns the prefix: the job that wrote the lease, or the collector
    /// that claimed it.
    pub status: MetadataCompactionLeaseStatus,
    /// Unix-millisecond stamp of the job's first lease write.
    pub started_at_ms: u64,
    /// Unix-millisecond stamp of the most recent lease write, and the only
    /// input to whether an `active` lease has expired.
    pub heartbeat_at_ms: u64,
}

/// Monotonic lifecycle of a durable checkpoint record.
///
/// A new record starts active and pins its basis. Explicit release or expiry
/// moves it to the terminal released state by compare-and-swap. Released
/// records serve no reads and are deleted after the release grace period.
/// Creating another pin always creates a new record id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointRecordLifecycle {
    /// Protects the checkpoint basis. The sole state a read may serve from.
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

impl std::fmt::Display for CheckpointRecordLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Active {} => "active",
            Self::Released { .. } => "released",
        };
        formatter.write_str(state)
    }
}

/// Durable owner of a checkpoint record: the party whose lifecycle decides
/// when the pin is released.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointOwner {
    /// An operator-created pin, released explicitly by checkpoint id or by
    /// its declared expiry. The name is a label, not a key: several records
    /// may carry the same name over different bases.
    User {
        /// Operator-facing label that need not be unique.
        name: String,
    },
    /// A fork target keeping its source basis alive. Released once the
    /// target namespace is terminally deleted, or once the attempt's lease
    /// expires with no target head to show for it. A live target keeps the
    /// record whatever the lease says.
    Fork {
        /// Fork namespace whose continued existence keeps the source basis pinned.
        target_namespace_id: NamespaceId,
    },
}

/// A checkpoint record: pins one metadata manifest (its basis) so garbage
/// collection keeps everything the manifest references.
///
/// Stored as its own object under `checkpoints/`; never part of a manifest
/// and never an input to latest visibility. Created write-then-verify: the
/// record is written `active`, then the basis manifest is re-verified
/// against the floor, and a failed verification flips the record to
/// `released`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CheckpointRecordState {
    /// Freshly generated record identity, one per logical pin. Nothing
    /// derives it, and no caller supplies it, so a new pin can never land on
    /// a released record's key.
    pub checkpoint_id: CheckpointId,
    /// Source namespace whose manifest and metadata remain pinned.
    pub namespace_id: NamespaceId,
    /// Logical manifest position of the pinned basis.
    pub manifest_id: ManifestId,
    /// Immutable manifest candidate selected at `manifest_id`.
    pub manifest_object_id: ManifestObjectId,
    /// Greatest source sequence materialized by the pinned manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
    /// Commit identity at the pinned manifest head, verified against its payload.
    pub head_commit_id: CommitId,
    /// Unix-millisecond creation stamp used by GC grace policy, never validity ordering.
    pub created_at_ms: u64,
    /// When garbage collection may release this record without asking anyone.
    ///
    /// A user pin carries the caller's `ttl_ms`, or nothing at all, in which
    /// case it is held until released. A fork-owned record always carries
    /// one: it is the lease covering a single fork attempt, and its expiry
    /// is how an abandoned attempt becomes collectable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Party whose durable lifecycle determines when this pin can be released.
    pub owner: CheckpointOwner,
    /// Current lifecycle, advanced only by the one-way release compare-and-swap.
    pub state: CheckpointRecordLifecycle,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCheckpointRecordState {
    checkpoint_id: CheckpointId,
    namespace_id: NamespaceId,
    manifest_id: ManifestId,
    manifest_object_id: ManifestObjectId,
    manifest_head_seq: ChangeSeq,
    manifest_payload_checksum: String,
    head_commit_id: CommitId,
    created_at_ms: u64,
    #[serde(default)]
    expires_at_ms: Option<u64>,
    owner: CheckpointOwner,
    state: CheckpointRecordLifecycle,
}

impl<'de> Deserialize<'de> for CheckpointRecordState {
    /// Decodes a checkpoint record and validates that every fork-owned record
    /// has an expiry.
    ///
    /// The expiry bounds an abandoned fork attempt whose target head was never
    /// installed. Without it, the source basis could remain pinned forever, so
    /// such a record is rejected as corrupt.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let record = StrictCheckpointRecordState::deserialize(deserializer)?;
        if matches!(record.owner, CheckpointOwner::Fork { .. }) && record.expires_at_ms.is_none() {
            return Err(serde::de::Error::custom(format!(
                "checkpoint record `{}` is fork-owned but has no lease expiry",
                record.checkpoint_id
            )));
        }
        Ok(Self {
            checkpoint_id: record.checkpoint_id,
            namespace_id: record.namespace_id,
            manifest_id: record.manifest_id,
            manifest_object_id: record.manifest_object_id,
            manifest_head_seq: record.manifest_head_seq,
            manifest_payload_checksum: record.manifest_payload_checksum,
            head_commit_id: record.head_commit_id,
            created_at_ms: record.created_at_ms,
            expires_at_ms: record.expires_at_ms,
            owner: record.owner,
            state: record.state,
        })
    }
}

/// Links one accepted WAL segment to its immutable object and verified sequence range.
///
/// See [WAL segment rules](../../../docs/specs/format.md#15-wal-segment-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPointer {
    /// Fully resolved durable key from which readers load the segment.
    pub object_key: String,
    /// Segment identity expected to agree with both the key and decoded payload.
    pub segment_id: WalSegmentId,
    /// First logical commit sequence carried by the segment.
    pub start_seq: ChangeSeq,
    /// Final logical commit sequence carried by the segment.
    pub end_seq: ChangeSeq,
    /// Checksum of the referenced segment's payload bytes, in `sha256:<hex>`
    /// form. Must equal the `payload_checksum` in the referenced envelope.
    pub payload_checksum: String,
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

/// Lifecycle state recorded in the namespace head.
///
/// There is no initialization state: the head is published complete by one
/// conditional write, so a namespace either has a head (active or deleted)
/// or does not exist. The one transition the head must record is deletion,
/// because a deleted namespace keeps its head forever as the id-reuse
/// tombstone.
///
/// Decoding is fail-closed: a reader presented with a state it does not
/// recognize fails with a typed decode error instead of serving the
/// namespace.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceState {
    /// The namespace serves reads and accepts commits.
    #[default]
    Active,
    /// Terminal: the namespace's history has ended. Reads, commits, forks,
    /// and re-creation of the same id are all refused.
    Deleted,
}

impl NamespaceState {
    /// Whether this is the default state, used to keep active heads encoded
    /// exactly as before the field existed.
    pub fn is_active(&self) -> bool {
        matches!(self, NamespaceState::Active)
    }
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
    /// Namespace whose durable tree owns the basis manifest and its tables.
    pub source_namespace_id: NamespaceId,
    /// Immutable manifest the target starts from, under the source's prefix.
    pub source_manifest_object_id: ManifestObjectId,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub source_manifest_checksum: String,
    /// Source checkpoint record pinning the basis for as long as the target lives.
    pub source_checkpoint_id: CheckpointId,
    /// Source sequence the target's history begins at: its birth seq, and
    /// the floor below which the target never had WAL history of its own.
    pub fork_seq: ChangeSeq,
}

/// Carries the authoritative visibility, allocation, and fencing state of a namespace.
///
/// See [head update authority](../../../docs/specs/format.md#14-head-update-authority).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_wal_tip: Option<WalSegmentPointer>,
    /// Bounded newest-first accelerator over the visible chain, whose first
    /// entry is always `visible_wal_tip`; rewritten by the commit CAS and
    /// checked at decode. Chain links remain the only history authority —
    /// any disagreement resolves in favor of the chain, and this array never
    /// protects anything from GC.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_segments: Vec<WalSegmentPointer>,
    /// Lifecycle state. Absent means active, on read and on write, so the
    /// field appears only in deleted heads.
    #[serde(default, skip_serializing_if = "NamespaceState::is_active")]
    pub state: NamespaceState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictHeadState {
    namespace_id: NamespaceId,
    // Required: a head that omits the namespace's content store is
    // malformed. It was a separate durable object before the
    // one-publication protocol; nothing reconstructs it.
    content_store_id: ContentStoreId,
    created_at_ms: u64,
    #[serde(default)]
    fork_basis: Option<ForkBasis>,
    seq: ChangeSeq,
    head_commit_id: CommitId,
    writer_epoch: WriterEpoch,
    #[serde(default)]
    writer: Option<WriterBlock>,
    next_inode_id: InodeId,
    #[serde(default)]
    visible_wal_tip: Option<StrictWalSegmentPointer>,
    #[serde(default)]
    recent_segments: Vec<StrictWalSegmentPointer>,
    #[serde(default)]
    state: NamespaceState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictWalSegmentPointer {
    object_key: String,
    segment_id: WalSegmentId,
    start_seq: ChangeSeq,
    end_seq: ChangeSeq,
    payload_checksum: String,
}

impl From<StrictWalSegmentPointer> for WalSegmentPointer {
    fn from(pointer: StrictWalSegmentPointer) -> Self {
        Self {
            object_key: pointer.object_key,
            segment_id: pointer.segment_id,
            start_seq: pointer.start_seq,
            end_seq: pointer.end_seq,
            payload_checksum: pointer.payload_checksum,
        }
    }
}

impl<'de> Deserialize<'de> for HeadState {
    /// Decodes a namespace head and validates the WAL hint list.
    ///
    /// Before the first commit, both `visible_wal_tip` and `recent_segments` are
    /// empty. Afterward, the first recent segment must equal the visible tip
    /// because both are written in the same compare-and-swap. A mismatch is
    /// rejected as corrupt state.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let state = StrictHeadState::deserialize(deserializer)?;
        let visible_wal_tip = state.visible_wal_tip.map(WalSegmentPointer::from);
        let recent_segments: Vec<WalSegmentPointer> =
            state.recent_segments.into_iter().map(Into::into).collect();
        if recent_segments.first() != visible_wal_tip.as_ref() {
            let tip = match &visible_wal_tip {
                Some(pointer) => format!("is `{}`", pointer.segment_id),
                None => "is absent".to_owned(),
            };
            let accelerator = match recent_segments.first() {
                Some(pointer) => format!("begins at `{}`", pointer.segment_id),
                None => "is empty".to_owned(),
            };
            return Err(serde::de::Error::custom(format!(
                "head for namespace `{}` at seq `{}` must carry its visible WAL tip as the \
                 first `recent_segments` entry: the tip {tip}, and the accelerator {accelerator}",
                state.namespace_id, state.seq,
            )));
        }
        Ok(Self {
            namespace_id: state.namespace_id,
            content_store_id: state.content_store_id,
            created_at_ms: state.created_at_ms,
            fork_basis: state.fork_basis,
            seq: state.seq,
            head_commit_id: state.head_commit_id,
            writer_epoch: state.writer_epoch,
            writer: state.writer,
            next_inode_id: state.next_inode_id,
            visible_wal_tip,
            recent_segments,
            state: state.state,
        })
    }
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
            state: NamespaceState::Active,
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
    Claimed {
        /// Time at which the request claimed exclusive access.
        at_ms: u64,
    },
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
            Claimed { at_ms: u64 },
            Staged { content_ref: &'a ContentRef },
        }

        match self {
            Self::Idle => Shape::Idle {}.serialize(serializer),
            Self::Claimed { at_ms } => Shape::Claimed { at_ms: *at_ms }.serialize(serializer),
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

/// The transport never changes, and each variant stores only the state
/// required by that upload path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadSessionTransport {
    /// The service receives the bytes and writes the content object itself,
    /// so it learns size and digest from the bytes as they pass.
    ServiceProxied {
        /// Exclusive staging progress, which applies only to this transport.
        staging: ProxiedStaging,
    },
    /// The client writes the whole object through one presigned request.
    DirectPut {
        /// The reference that signed write is minted for.
        ///
        /// A direct-put client declares its byte length and SHA-256 before
        /// the write is authorized, because both are signed into the
        /// request and the provider refuses any body that does not match.
        /// Completion reads the stored object back against this same
        /// reference rather than believing it.
        promised_content: ContentRef,
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
        /// Checksum algorithm frozen when the session began. Part signing
        /// and completion must use this decision after any process restart.
        checksum_algorithm: ChecksumAlgorithm,
    },
}

impl UploadSessionTransport {
    /// The content reference this transport names, when it names one.
    fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::DirectPut { promised_content } => Some(promised_content),
            Self::ServiceProxied {
                staging: ProxiedStaging::Staged(content_ref),
            } => Some(content_ref),
            Self::ServiceProxied { .. } | Self::DirectMultipart { .. } => None,
        }
    }
}

/// Monotonic lifecycle of a durable upload session.
///
/// A session starts open and ends either completed or aborted. The
/// compare-and-swap that writes the terminal state decides which transition
/// won; provider cleanup happens afterward. Terminal sessions never reopen,
/// so another attempt requires a new session and content id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadSessionLifecycle {
    /// The one state that may stage bytes and complete. Live until its lease
    /// passes, after which garbage collection aborts it.
    Open {
        /// Unix-millisecond instant after which the session is abandoned.
        /// The record carries it so no session transition depends on an
        /// object's provider timestamp.
        expires_at_ms: u64,
    },
    /// Terminal: the content is durable and verified. This is the only state
    /// a receipt may be minted from, and the content reference it carries is
    /// what every re-mint and idempotent completion retry answers with.
    Completed {
        /// Unix-millisecond stamp written by the completing compare-and-swap,
        /// and the only input to when the content may be reclaimed.
        completed_at_ms: u64,
        /// Verified immutable content this session settled on, and the one
        /// place a completed session's reference exists.
        content_ref: ContentRef,
    },
    /// Terminal: the session will never select content. Its content
    /// identity was never published — a receipt exists only for a completed
    /// session — so the object it named belongs to nobody and is deleted.
    Aborted {
        /// Unix-millisecond stamp written by the aborting compare-and-swap,
        /// and the only input to when the record may be deleted.
        aborted_at_ms: u64,
    },
}

impl UploadSessionLifecycle {
    /// The content reference this state names, whichever state names one.
    fn content_ref(&self) -> Option<&ContentRef> {
        match self {
            Self::Open { .. } => None,
            Self::Completed { content_ref, .. } => Some(content_ref),
            Self::Aborted { .. } => None,
        }
    }
}

impl std::fmt::Display for UploadSessionLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Open { .. } => "open",
            Self::Completed { .. } => "completed",
            Self::Aborted { .. } => "aborted",
        };
        formatter.write_str(state)
    }
}

/// Tracks one durable content-upload workflow independently of commit publication.
///
/// The record is an identity, a transport, and a state. Everything a
/// transport needs lives in its own variant and everything a state needs
/// lives in its own variant, so the combinations that used to be spelled
/// with independent optional fields — a proxied session holding a provider
/// upload or a multipart session promising content — are not shapes this
/// type has.
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
    pub transport: UploadSessionTransport,
    /// The session's lifecycle, and the field every upload operation
    /// compare-and-swaps against.
    pub state: UploadSessionLifecycle,
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
    /// A direct-put session must also settle on exactly the reference its
    /// write was signed against, because that reference is what the provider
    /// enforced and what completion read back. A record that says otherwise
    /// describes an upload that did not happen.
    fn validate(&self) -> Result<(), String> {
        for content_ref in self
            .transport
            .content_ref()
            .into_iter()
            .chain(self.state.content_ref())
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
        match (&self.transport, &self.state) {
            (
                UploadSessionTransport::DirectPut { promised_content },
                UploadSessionLifecycle::Completed { content_ref, .. },
            ) if content_ref != promised_content => Err(format!(
                "upload session `{}` completed on content its direct write never promised",
                self.upload_id
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictUploadSessionState {
    namespace_id: NamespaceId,
    upload_id: UploadId,
    content_id: ContentId,
    created_at_ms: u64,
    transport: StrictUploadSessionTransport,
    state: StrictUploadSessionLifecycle,
}

/// The transport read back through the same strict content-ref decoder the
/// rest of the record uses.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictUploadSessionTransport {
    ServiceProxied {
        staging: StrictProxiedStaging,
    },
    DirectPut {
        promised_content: StrictContentRef,
    },
    DirectMultipart {
        provider_upload_id: String,
        part_size_bytes: NonZeroU64,
        checksum_algorithm: ChecksumAlgorithm,
    },
}

impl From<StrictUploadSessionTransport> for UploadSessionTransport {
    fn from(transport: StrictUploadSessionTransport) -> Self {
        match transport {
            StrictUploadSessionTransport::ServiceProxied { staging } => Self::ServiceProxied {
                staging: staging.into(),
            },
            StrictUploadSessionTransport::DirectPut { promised_content } => Self::DirectPut {
                promised_content: promised_content.into(),
            },
            StrictUploadSessionTransport::DirectMultipart {
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
    Claimed { at_ms: u64 },
    Staged { content_ref: StrictContentRef },
}

impl From<StrictProxiedStaging> for ProxiedStaging {
    fn from(staging: StrictProxiedStaging) -> Self {
        match staging {
            StrictProxiedStaging::Idle {} => Self::Idle,
            StrictProxiedStaging::Claimed { at_ms } => Self::Claimed { at_ms },
            StrictProxiedStaging::Staged { content_ref } => Self::Staged(content_ref.into()),
        }
    }
}

/// The lifecycle read back through the same strict content-ref decoder the
/// rest of the record uses, so a completed session's reference is held to
/// the durable schema rather than the wire one.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictUploadSessionLifecycle {
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

impl From<StrictUploadSessionLifecycle> for UploadSessionLifecycle {
    fn from(state: StrictUploadSessionLifecycle) -> Self {
        match state {
            StrictUploadSessionLifecycle::Open { expires_at_ms } => Self::Open { expires_at_ms },
            StrictUploadSessionLifecycle::Completed {
                completed_at_ms,
                content_ref,
            } => Self::Completed {
                completed_at_ms,
                content_ref: content_ref.into(),
            },
            StrictUploadSessionLifecycle::Aborted { aborted_at_ms } => {
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
        let state = StrictUploadSessionState::deserialize(deserializer)?;
        let session = Self {
            namespace_id: state.namespace_id,
            upload_id: state.upload_id,
            content_id: state.content_id,
            created_at_ms: state.created_at_ms,
            transport: state.transport.into(),
            state: state.state.into(),
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
            "next_inode_id": 2
        });

        serde_json::from_value::<HeadState>(missing)
            .expect_err("head without its immutable identity must be rejected");
    }

    /// One WAL pointer as it appears inside a durable head payload.
    fn wal_pointer_json(segment_id: &str, start_seq: u64, end_seq: u64) -> serde_json::Value {
        serde_json::json!({
            "object_key": format!("namespaces/demo/wal/{segment_id}.wal.zst"),
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
            "next_inode_id": 2
        });
        if let Some(tip) = visible_wal_tip {
            head["visible_wal_tip"] = tip;
        }
        if !recent_segments.is_empty() {
            head["recent_segments"] = serde_json::Value::Array(recent_segments);
        }
        head
    }

    #[test]
    fn head_with_a_tip_and_no_accelerator_is_rejected() {
        // The commit that installs a tip writes it into the accelerator in
        // the same compare-and-swap, so a head holding one without the other
        // never landed as written.
        let tip = wal_pointer_json("00000000000000000002-fedcba9876543210", 2, 2);

        let error = serde_json::from_value::<HeadState>(head_json(Some(tip), Vec::new()))
            .expect_err("a tip with no accelerator entry must be rejected");
        assert!(
            error.to_string().contains("recent_segments"),
            "unexpected message: {error}"
        );
    }

    #[test]
    fn head_whose_accelerator_does_not_begin_at_the_tip_is_rejected() {
        let tip = wal_pointer_json("00000000000000000002-fedcba9876543210", 2, 2);
        let older = wal_pointer_json("00000000000000000001-0123456789abcdef", 1, 1);

        serde_json::from_value::<HeadState>(head_json(Some(tip.clone()), vec![older, tip]))
            .expect_err("an accelerator that starts below the tip must be rejected");
    }

    #[test]
    fn head_with_an_accelerator_and_no_tip_is_rejected() {
        let tip = wal_pointer_json("00000000000000000002-fedcba9876543210", 2, 2);
        let older = wal_pointer_json("00000000000000000001-0123456789abcdef", 1, 1);

        serde_json::from_value::<HeadState>(head_json(None, vec![tip, older]))
            .expect_err("an accelerator naming segments no tip points at must be rejected");
    }

    #[test]
    fn head_decodes_with_no_tip_at_all_or_with_the_tip_leading_its_accelerator() {
        // A namespace before its first commit carries neither.
        let genesis = serde_json::from_value::<HeadState>(head_json(None, Vec::new()))
            .expect("a head with no visible tip decodes");
        assert_eq!(genesis.visible_wal_tip, None);
        assert!(genesis.recent_segments.is_empty());

        let tip = wal_pointer_json("00000000000000000002-fedcba9876543210", 2, 2);
        let older = wal_pointer_json("00000000000000000001-0123456789abcdef", 1, 1);
        let committed =
            serde_json::from_value::<HeadState>(head_json(Some(tip.clone()), vec![tip, older]))
                .expect("a head whose accelerator begins at its tip decodes");
        assert_eq!(
            committed.recent_segments.first(),
            committed.visible_wal_tip.as_ref()
        );
        assert_eq!(committed.recent_segments.len(), 2);
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
            source_namespace_id: NamespaceId::parse("source").expect("valid namespace id"),
            source_manifest_object_id: ManifestObjectId::parse(
                "00000000000000000007-0123456789abcdef",
            )
            .expect("valid manifest object id"),
            source_manifest_checksum: "sha256:test".to_owned(),
            source_checkpoint_id: CheckpointId::parse("chk_00000000000000000000000000000002")
                .expect("valid checkpoint id"),
            fork_seq: ChangeSeq(7),
        });
        assert_eq!(
            head.ensure_successor_identity(&forked)
                .expect_err("gaining a fork basis is rejected")
                .field,
            "fork_basis"
        );
    }
}
