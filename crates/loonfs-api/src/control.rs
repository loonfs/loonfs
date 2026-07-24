//! Durable control-object shapes: the head, metadata root, WAL floor,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::v0::UploadMode;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentRefKind, ContentStoreId, InodeId,
    ManifestId, ManifestObjectId, NamePolicy, NamespaceId, UploadId, WalSegmentId, ROOT_INODE_ID,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};

/// Selects one independently versioned mutable control-object family.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    /// Stores immutable namespace-wide configuration.
    NamespaceConfig,
    /// Identifies one immutable-content keyspace.
    ContentStoreDescriptor,
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
    pub const ALL: [Self; 7] = [
        Self::NamespaceConfig,
        Self::ContentStoreDescriptor,
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
            Self::NamespaceConfig => 1,
            Self::ContentStoreDescriptor => 1,
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
            Self::NamespaceConfig => "namespace_config",
            Self::ContentStoreDescriptor => "content_store_descriptor",
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

/// Stores immutable configuration consulted by every read and write in a namespace.
///
/// See [namespaces and identity](../../../docs/specs/format.md#21-namespaces-and-identity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfigState {
    /// Namespace whose durable tree owns this configuration object.
    pub namespace_id: NamespaceId,
    /// Immutable content store in which the namespace publishes file bytes.
    pub content_store_id: ContentStoreId,
    /// Immutable per namespace, chosen at creation: the single authority for
    /// name-key computation on both the write and read paths. Required — a
    /// config without a policy is malformed, never guessed.
    pub name_policy: NamePolicy,
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

/// Identifies the immutable-content keyspace described by one control object.
///
/// See [immutable content rules](../../../docs/specs/format.md#16-immutable-content-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentStoreDescriptorState {
    /// Content-store identity that must agree with the descriptor's durable key.
    pub content_store_id: ContentStoreId,
}

/// Lifecycle of a durable checkpoint record.
///
/// Only `active`, non-expired checkpoints are long-term GC roots. `released`
/// records may be revived after basis verification; garbage collection first
/// compare-and-swaps one into absorbing `condemned`, which no owner operation
/// may leave, before deleting it. Any non-condemned record younger than the GC
/// grace window remains protected regardless of state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointRecordLifecycle {
    /// Protects the checkpoint basis while the owner remains live and the record unexpired.
    #[default]
    Active,
    /// Relinquishes the pin while allowing basis-verified revival by its owner.
    Released,
    /// Irreversibly transfers the record to garbage-collection cleanup.
    Condemned,
}

impl std::fmt::Display for CheckpointRecordLifecycle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = match self {
            Self::Active => "active",
            Self::Released => "released",
            Self::Condemned => "condemned",
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
    /// A fork target keeping its source basis alive. Released only once the
    /// target namespace is terminally deleted or its installation tree is
    /// proven absent.
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
    /// Deterministic record identity derived from basis and owner identity.
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
    /// Unix-millisecond creation stamp used by expiry and GC grace policy, never validity ordering.
    pub created_at_ms: u64,
    /// Expiry for user-owned records; fork-owned records never expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Party whose durable lifecycle determines when this pin can be released.
    pub owner: CheckpointOwner,
    /// Current pin or cleanup lifecycle, changed only by guarded rewrites.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterBlock {
    /// Stable writer label supplied by the embedding process for diagnostics.
    pub writer_id: String,
    /// Per-session generated identity that distinguishes restarts of the same writer.
    pub writer_session_id: String,
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
    /// Per-session identity used to recognize an already-acquired head.
    pub writer_session_id: String,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// Lifecycle state recorded in the namespace head.
///
/// Initialization progress is not recorded here: it stays derived from
/// object presence (the config is the completion marker). This field
/// records the one transition object presence cannot express, because a
/// deleted namespace keeps its head and config forever as the id-reuse
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
    /// Temporary absorbing gate installed only by explicit repair before it
    /// reaps a partial namespace tree. Create/fork attempts see a partial
    /// namespace while this head exists; repair deletes this head last.
    Condemned,
}

impl NamespaceState {
    /// Whether this is the default state, used to keep active heads encoded
    /// exactly as before the field existed.
    pub fn is_active(&self) -> bool {
        matches!(self, NamespaceState::Active)
    }
}

/// Carries the authoritative visibility, allocation, and fencing state of a namespace.
///
/// See [head update authority](../../../docs/specs/format.md#14-head-update-authority).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HeadState {
    /// Namespace whose live history this head governs.
    pub namespace_id: NamespaceId,
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
    /// field appears only in deleted or repair-condemned heads.
    #[serde(default, skip_serializing_if = "NamespaceState::is_active")]
    pub state: NamespaceState,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictHeadState {
    namespace_id: NamespaceId,
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

impl HeadState {
    /// Constructs the active sequence-zero head with the root inode already reserved.
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
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
}

/// Records the immutable content selected when an upload session completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletedUpload {
    /// Verified content reference returned by every idempotent completion retry.
    pub content_ref: ContentRef,
}

/// Lifecycle of a durable upload session.
///
/// `active` sessions may stage or complete content. Garbage collection
/// compare-and-swaps an abandoned session to absorbing `condemned` before
/// physical deletion; upload operations treat that state as not found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadSessionLifecycle {
    /// Allows staging and idempotent completion of content.
    #[default]
    Active,
    /// Irreversibly transfers an abandoned session to garbage-collection cleanup.
    Condemned,
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
    /// For direct_put sessions, the content ref the presigned URL was minted for.
    /// It becomes staged only after completion validates the durable object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put_content_ref: Option<ContentRef>,
    /// Content already verified and staged, or `None` before bytes have passed validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_ref: Option<ContentRef>,
    /// Frozen completion result, or `None` while the session can still select content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<CompletedUpload>,
    /// Unix-millisecond creation stamp used for abandoned-session cleanup policy.
    pub created_at_ms: u64,
    /// Guarded lifecycle that prevents upload operations racing with cleanup.
    pub state: UploadSessionLifecycle,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictUploadSessionState {
    namespace_id: NamespaceId,
    upload_id: UploadId,
    #[serde(default)]
    mode: UploadMode,
    #[serde(default)]
    direct_put_content_ref: Option<StrictContentRef>,
    #[serde(default)]
    staged_content_ref: Option<StrictContentRef>,
    #[serde(default)]
    completed: Option<StrictCompletedUpload>,
    created_at_ms: u64,
    state: UploadSessionLifecycle,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictCompletedUpload {
    content_ref: StrictContentRef,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictContentRef {
    kind: MutableContentRefKind,
    digest: String,
    size_bytes: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum MutableContentRefKind {
    WholeFileV0,
}

impl From<StrictContentRef> for ContentRef {
    fn from(content_ref: StrictContentRef) -> Self {
        let kind = match content_ref.kind {
            MutableContentRefKind::WholeFileV0 => ContentRefKind::WholeFileV0,
        };
        Self {
            kind,
            digest: content_ref.digest,
            size_bytes: content_ref.size_bytes,
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
            direct_put_content_ref: state.direct_put_content_ref.map(Into::into),
            staged_content_ref: state.staged_content_ref.map(Into::into),
            completed: state.completed.map(|completed| CompletedUpload {
                content_ref: completed.content_ref.into(),
            }),
            created_at_ms: state.created_at_ms,
            state: state.state,
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
    /// Informational software version of the writer that encoded this object.
    pub writer_version: String,
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
    pub fn from_state(
        kind: ControlObjectKind,
        writer_version: impl Into<String>,
        state: T,
    ) -> Result<Self, EnvelopeCodecError> {
        Ok(Self {
            kind,
            format_version: kind.format_version(),
            writer_version: writer_version.into(),
            payload_checksum: control_payload_checksum(&state)?,
            state,
        })
    }
}

/// Specializes a control envelope for the authoritative namespace head.
pub type HeadStateEnvelope = ControlObjectEnvelope<HeadState>;
/// Specializes a control envelope for a durable upload workflow.
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
/// Specializes a control envelope for immutable namespace configuration.
pub type NamespaceConfigEnvelope = ControlObjectEnvelope<NamespaceConfigState>;
/// Specializes a control envelope for the selected materialized manifest.
pub type MetadataRootEnvelope = ControlObjectEnvelope<MetadataRootState>;
/// Specializes a control envelope for the retained-history floor.
pub type WalFloorEnvelope = ControlObjectEnvelope<WalFloorState>;
/// Specializes a control envelope for a durable manifest pin.
pub type CheckpointRecordEnvelope = ControlObjectEnvelope<CheckpointRecordState>;
/// Specializes a control envelope for an immutable-content keyspace descriptor.
pub type ContentStoreDescriptorEnvelope = ControlObjectEnvelope<ContentStoreDescriptorState>;

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
        &envelope.writer_version,
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
        writer_version: decoded.writer_version,
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

    #[test]
    fn namespace_config_without_name_policy_is_rejected() {
        // The name policy is the collision-semantics authority; a
        // config that omits it is malformed, never defaulted.
        let missing = serde_json::json!({
            "namespace_id": "demo",
            "content_store_id": "cs_0123456789abcdef"
        });
        serde_json::from_value::<NamespaceConfigState>(missing)
            .expect_err("config without name_policy must be rejected");
    }

    #[test]
    fn control_object_codec_round_trips_and_validates() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            "test-writer",
            HeadState::initial(namespace_id),
        )
        .expect("envelope");

        let encoded = encode_control_object(&envelope).expect("encode");
        let decoded: HeadStateEnvelope =
            decode_control_object(&encoded, ControlObjectKind::WalHead).expect("decode");
        assert_eq!(decoded, envelope);

        let mismatch = decode_control_object::<NamespaceConfigState>(
            &encoded,
            ControlObjectKind::NamespaceConfig,
        )
        .expect_err("kind mismatch");
        assert!(matches!(mismatch, EnvelopeCodecError::KindMismatch { .. }));
    }
}
