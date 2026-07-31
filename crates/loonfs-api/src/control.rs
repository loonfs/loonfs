//! Durable control-object shapes: the head, metadata root, WAL floor,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::v0::UploadMode;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, ChecksumAlgorithm, CommitId, ContentId, ContentRef, ContentRefKind,
    ContentStoreId, InodeId, ManifestId, ManifestObjectId, NamespaceId, StorageChecksum, UploadId,
    WalSegmentId, ROOT_INODE_ID,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

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
}

impl ControlObjectKind {
    /// Lists every registered control-object family in stable registry order.
    pub const ALL: [Self; 5] = [
        Self::WalHead,
        Self::WalFloor,
        Self::MetadataRoot,
        Self::CheckpointRecord,
        Self::UploadSession,
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
        }
    }

    /// Parses a registered envelope discriminator, returning `None` for future families.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// Lower bound of retained WAL/change history: the symmetrical pair to
/// `wal/head.json`.
///
/// Updated only by monotonic compare-and-swap on its own etag; never
/// consulted for live commit visibility. Missing, stale, or unverifiable
/// floors mean "retain more history", never less. The floor is necessary
/// but not sufficient for deletion: below-floor objects are candidates,
/// and actual deletion additionally requires delete-time re-verification
/// (format spec, "Garbage collection").
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

/// Lifecycle of a durable checkpoint record: monotonic, with exactly two
/// states and one transition.
///
/// A record is born `active` under a freshly generated id and pins its basis
/// until something moves it to `released` by compare-and-swap — the owner
/// asking for it, or garbage collection observing that its `expires_at_ms`
/// passed. `released` is terminal: nothing returns a record to `active`, so
/// a released record protects nothing and answers no read. Garbage
/// collection deletes it once `released_at_ms` is a grace window old. A new
/// pin is a new record under a new id, never a revival of this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CheckpointRecordLifecycle {
    /// Protects the checkpoint basis. The sole state a read may serve from.
    Active,
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
            Self::Active => "active",
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Bounded newest-first accelerator over the visible chain, always
    /// including the tip; rewritten by the commit CAS. Chain links remain
    /// the only history authority — any disagreement resolves in favor of
    /// the chain, and this array never protects anything from GC.
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
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let state = StrictHeadState::deserialize(deserializer)?;
        Ok(Self {
            namespace_id: state.namespace_id,
            content_store_id: state.content_store_id,
            fork_basis: state.fork_basis,
            seq: state.seq,
            head_commit_id: state.head_commit_id,
            writer_epoch: state.writer_epoch,
            writer: state.writer,
            next_inode_id: state.next_inode_id,
            visible_wal_tip: state.visible_wal_tip.map(Into::into),
            recent_segments: state.recent_segments.into_iter().map(Into::into).collect(),
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
    pub fn initial(namespace_id: NamespaceId, content_store_id: ContentStoreId) -> Self {
        Self {
            namespace_id,
            content_store_id,
            fork_basis: None,
            seq: ChangeSeq(0),
            head_commit_id: CommitId::parse(GENESIS_COMMIT_ID).expect("genesis commit id is valid"),
            writer_epoch: WriterEpoch(0),
            writer: None,
            // Inode 1 is the root directory; inode 2 is the first assignable id.
            next_inode_id: InodeId(ROOT_INODE_ID.0 + 1),
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
        if successor.fork_basis != self.fork_basis {
            return drift("fork_basis");
        }
        Ok(())
    }
}

/// Lifecycle of a durable upload session: one live state and two terminal
/// ones, with no way back.
///
/// A session opens with a lease and either completes or is aborted. The
/// compare-and-swap that makes one of those two land is the serialization
/// point for the whole upload — provider state follows the durable
/// transition, never the other way around — so whichever transition wins is
/// simply what happened, and the loser reports a terminal error rather than
/// undoing anything. Nothing returns a session to `open`: a client that
/// wants another try begins another session, which mints its own content
/// identity.
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
        /// Verified immutable content this session settled on.
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
/// See [upload before publish](../../../docs/specs/format.md#242-upload-before-publish).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UploadSessionState {
    /// Namespace authorized to consume the staged content.
    pub namespace_id: NamespaceId,
    /// Durable session identity used by staging and completion requests.
    pub upload_id: UploadId,
    /// Transport path selected when the session was created.
    #[serde(default, skip_serializing_if = "UploadMode::is_service_proxied")]
    pub mode: UploadMode,
    /// Content object this session writes, allocated when the session began.
    ///
    /// The identity exists before any byte is read, so the final object key
    /// is known up front and belongs to exactly this session.
    pub content_id: ContentId,
    /// What the client promised about the bytes, for sessions that make a
    /// promise up front. `direct_put` claims a size and a whole-file
    /// SHA-256; a service-proxied session claims nothing and learns both
    /// from the bytes it receives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_checksum: Option<StorageChecksum>,
    /// For direct_put and direct_multipart sessions, the content ref the
    /// signed writes were minted for. It becomes staged only after
    /// completion verifies the durable object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put_content_ref: Option<ContentRef>,
    /// The provider-side multipart upload this session opened, for the one
    /// mode that opens one.
    ///
    /// It is the only provider handle LoonFS keeps: parts are the client's
    /// bookkeeping, exactly as they are in the provider's own API, so there
    /// is no durable record per part. Cleanup reads this to abort what the
    /// session left open; nothing else reads it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_multipart_upload_id: Option<String>,
    /// Content already verified and staged, or `None` before bytes have passed validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_ref: Option<ContentRef>,
    /// Unix-millisecond creation stamp.
    pub created_at_ms: u64,
    /// The session's lifecycle, and the field every upload operation
    /// compare-and-swaps against.
    pub state: UploadSessionLifecycle,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictUploadSessionState {
    namespace_id: NamespaceId,
    upload_id: UploadId,
    #[serde(default)]
    mode: UploadMode,
    content_id: ContentId,
    #[serde(default)]
    claimed_checksum: Option<StrictStorageChecksum>,
    #[serde(default)]
    direct_put_content_ref: Option<StrictContentRef>,
    #[serde(default)]
    provider_multipart_upload_id: Option<String>,
    #[serde(default)]
    staged_content_ref: Option<StrictContentRef>,
    created_at_ms: u64,
    state: StrictUploadSessionLifecycle,
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
    storage_checksum: StrictStorageChecksum,
    #[serde(default)]
    whole_file_sha256: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictStorageChecksum {
    algorithm: ChecksumAlgorithm,
    value: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MutableContentRefKind {
    BlobV1,
}

impl From<StrictStorageChecksum> for StorageChecksum {
    fn from(checksum: StrictStorageChecksum) -> Self {
        Self {
            algorithm: checksum.algorithm,
            value: checksum.value,
        }
    }
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
            storage_checksum: content_ref.storage_checksum.into(),
            whole_file_sha256: content_ref.whole_file_sha256,
        }
    }
}

impl<'de> Deserialize<'de> for UploadSessionState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let state = StrictUploadSessionState::deserialize(deserializer)?;
        Ok(Self {
            namespace_id: state.namespace_id,
            upload_id: state.upload_id,
            mode: state.mode,
            content_id: state.content_id,
            claimed_checksum: state.claimed_checksum.map(Into::into),
            direct_put_content_ref: state.direct_put_content_ref.map(Into::into),
            provider_multipart_upload_id: state.provider_multipart_upload_id,
            staged_content_ref: state.staged_content_ref.map(Into::into),
            created_at_ms: state.created_at_ms,
            state: state.state.into(),
        })
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
