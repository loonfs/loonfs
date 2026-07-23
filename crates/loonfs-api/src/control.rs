//! Durable control-object shapes: the head, metadata root, WAL floor,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::v0::UploadMode;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentStoreId, InodeId, ManifestId,
    ManifestObjectId, NamePolicy, NamespaceId, UploadId, WalSegmentId, ROOT_INODE_ID,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceConfig,
    ContentStoreDescriptor,
    WalHead,
    WalFloor,
    MetadataRoot,
    CheckpointRecord,
    UploadSession,
}

impl ControlObjectKind {
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

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceConfigState {
    pub namespace_id: NamespaceId,
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
pub struct WalFloorState {
    pub namespace_id: NamespaceId,
    pub floor_seq: ChangeSeq,
    pub verified_at_ms: u64,
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
pub struct MetadataRootState {
    pub namespace_id: NamespaceId,
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreDescriptorState {
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
    #[default]
    Active,
    Released,
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
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwner {
    /// An operator-created pin, released explicitly by checkpoint id or by
    /// its declared expiry. The name is a label, not a key: several records
    /// may carry the same name over different bases.
    User { name: String },
    /// A fork target keeping its source basis alive. Released only once the
    /// target namespace is terminally deleted or its installation tree is
    /// proven absent.
    Fork { target_namespace_id: NamespaceId },
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
pub struct CheckpointRecordState {
    pub checkpoint_id: CheckpointId,
    pub namespace_id: NamespaceId,
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
    pub head_commit_id: CommitId,
    pub created_at_ms: u64,
    /// Expiry for user-owned records; fork-owned records never expire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    pub owner: CheckpointOwner,
    pub state: CheckpointRecordLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPointer {
    pub object_key: String,
    pub segment_id: WalSegmentId,
    pub start_seq: ChangeSeq,
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
pub struct WriterBlock {
    pub writer_id: String,
    pub writer_session_id: String,
    pub acquired_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredWriter {
    pub writer_id: String,
    pub writer_session_id: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeadState {
    pub namespace_id: NamespaceId,
    pub seq: ChangeSeq,
    pub head_commit_id: CommitId,
    pub writer_epoch: WriterEpoch,
    /// Non-authoritative record of the most recent epoch acquisition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<WriterBlock>,
    pub next_inode_id: InodeId,
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

const GENESIS_COMMIT_ID: &str = "c_00000000000000000000000000000000";

impl HeadState {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedUpload {
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
    #[default]
    Active,
    Condemned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSessionState {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    #[serde(default, skip_serializing_if = "UploadMode::is_service_proxied")]
    pub mode: UploadMode,
    /// For direct_put sessions, the content ref the presigned URL was minted for.
    /// It becomes staged only after completion validates the durable object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put_content_ref: Option<ContentRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_content_ref: Option<ContentRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed: Option<CompletedUpload>,
    pub created_at_ms: u64,
    pub state: UploadSessionLifecycle,
}

/// In-memory view of a control object envelope.
///
/// This struct is not the durable layout; durable bytes are produced only by
/// [`encode_control_object`] and validated only by [`decode_control_object`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlObjectEnvelope<T> {
    pub kind: ControlObjectKind,
    pub format_version: u32,
    pub writer_version: String,
    /// Digest of the payload JSON exactly as stored in the durable document,
    /// in `sha256:<hex>` form.
    pub payload_checksum: String,
    pub state: T,
}

impl<T> ControlObjectEnvelope<T>
where
    T: Serialize,
{
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

pub type HeadStateEnvelope = ControlObjectEnvelope<HeadState>;
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
pub type NamespaceConfigEnvelope = ControlObjectEnvelope<NamespaceConfigState>;
pub type MetadataRootEnvelope = ControlObjectEnvelope<MetadataRootState>;
pub type WalFloorEnvelope = ControlObjectEnvelope<WalFloorState>;
pub type CheckpointRecordEnvelope = ControlObjectEnvelope<CheckpointRecordState>;
pub type ContentStoreDescriptorEnvelope = ControlObjectEnvelope<ContentStoreDescriptorState>;

pub fn control_payload_checksum<T>(state: &T) -> Result<String, EnvelopeCodecError>
where
    T: Serialize,
{
    crate::envelope::json_payload_checksum(state)
}

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

pub fn decode_control_object<T>(
    bytes: &[u8],
    expected_kind: ControlObjectKind,
) -> Result<ControlObjectEnvelope<T>, EnvelopeCodecError>
where
    T: DeserializeOwned,
{
    let decoded = crate::envelope::decode_json_envelope(
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
    use crate::digest::sha256_digest;

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

    #[test]
    fn control_object_decode_tolerates_unknown_payload_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::WalHead,
            "test-writer",
            HeadState::initial(namespace_id),
        )
        .expect("envelope");
        let encoded = encode_control_object(&envelope).expect("encode");

        // Simulate a newer writer adding a payload field: splice it into the
        // raw payload fragment and re-checksum, the way a v-next writer would.
        let text = String::from_utf8(encoded).expect("utf8");
        let future_payload = serde_json::to_string(&envelope.state)
            .expect("payload json")
            .replacen('{', "{\"field_from_the_future\":true,", 1);
        let future_checksum = sha256_digest(future_payload.as_bytes());
        let future_text = text
            .replace(
                &serde_json::to_string(&envelope.state).expect("payload json"),
                &future_payload,
            )
            .replace(&envelope.payload_checksum, &future_checksum);

        let decoded: HeadStateEnvelope =
            decode_control_object(future_text.as_bytes(), ControlObjectKind::WalHead)
                .expect("additive payload fields must remain readable");
        assert_eq!(decoded.state, envelope.state);
        assert_eq!(decoded.payload_checksum, future_checksum);
    }
}
