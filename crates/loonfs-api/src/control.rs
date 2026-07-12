use crate::digest::sha256_digest;
use crate::envelope::EnvelopeProbe;
use crate::v0::UploadMode;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentStoreId, GcPinId, InodeId, ManifestId,
    ManifestObjectId, NamePolicy, NamespaceId, UploadId, WalSegmentId,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceConfig,
    ContentStoreDescriptor,
    WalHead,
    WalFloor,
    MetadataRoot,
    CheckpointRecord,
    NamespaceGcPinState,
    UploadSession,
}

impl ControlObjectKind {
    pub const ALL: [Self; 8] = [
        Self::NamespaceConfig,
        Self::ContentStoreDescriptor,
        Self::WalHead,
        Self::WalFloor,
        Self::MetadataRoot,
        Self::CheckpointRecord,
        Self::NamespaceGcPinState,
        Self::UploadSession,
    ];

    /// Durable format version for this control object kind.
    ///
    /// Versions are tracked per kind so one kind's payload schema can make a
    /// breaking change without invalidating every other control object.
    /// Version 1 (all kinds): a JSON envelope document carrying the payload
    /// as a raw JSON fragment whose checksum covers its exact bytes.
    pub const fn format_version(self) -> u32 {
        match self {
            Self::NamespaceConfig => 1,
            Self::ContentStoreDescriptor => 1,
            Self::WalHead => 1,
            Self::WalFloor => 1,
            Self::MetadataRoot => 1,
            Self::CheckpointRecord => 1,
            Self::NamespaceGcPinState => 1,
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
            Self::NamespaceGcPinState => "namespace_gc_pin_state",
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
    /// name-key computation on both the write and read paths.
    #[serde(default)]
    pub name_policy: NamePolicy,
}

/// Manifest basis whose material was verified when the floor advanced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFloorBasis {
    pub manifest_id: ManifestId,
    pub manifest_object_id: ManifestObjectId,
    pub manifest_head_seq: ChangeSeq,
    pub manifest_payload_checksum: String,
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
    pub basis: WalFloorBasis,
    pub verified_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Cold pointer to the best known materialized metadata root.
///
/// Replaces the head's `current_manifest_id`: manifest publication CASes
/// this object, never the WAL head, so head watchers see only commits.
/// Updates are monotonic in `manifest_head_seq`; a same-seq replacement may
/// reference a different manifest (pure compaction), and a lower-seq
/// replacement no-ops. This object never defines live visibility.
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
/// Only `active`, non-expired checkpoints are long-term GC roots; `released`
/// records are collectable tombstones — failed verification, an explicit
/// owner release, or a fork owner proven gone all end here. Any record
/// younger than the GC grace window is a root regardless of state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointRecordLifecycle {
    #[default]
    Active,
    Released,
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
    /// target namespace is terminally deleted or its abandoned bootstrap is
    /// proven dead.
    Fork { target_namespace_id: NamespaceId },
}

/// Durable stable-view pin to a metadata manifest.
///
/// First-class file under `checkpoints/`; never part of a manifest and never
/// an input to latest visibility. Created write-then-verify: the record is
/// written `active`, then the basis manifest is re-verified against the
/// floor; a failed verification flips the record to `released`.
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

/// Pin that prevents the source namespace's GC from collecting the metadata
/// files a fork still shares. A pin references its source checkpoint only;
/// reachability resolves through it (pin -> checkpoint -> manifest ->
/// tables), so manifest facts are never duplicated here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceGcPinState {
    pub pin_id: GcPinId,
    pub source_namespace_id: NamespaceId,
    pub target_namespace_id: NamespaceId,
    pub source_checkpoint_id: CheckpointId,
    pub created_at_ms: u64,
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
/// object presence (the descriptor is the completion marker). This field
/// records the one transition object presence cannot express, because a
/// deleted namespace keeps its head and descriptor forever as the id-reuse
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
    /// field appears only in deleted heads.
    #[serde(default, skip_serializing_if = "NamespaceState::is_active")]
    pub state: NamespaceState,
}

impl HeadState {
    pub fn initial(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            seq: ChangeSeq(0),
            head_commit_id: CommitId::parse("c_00000000000000000000000000000000")
                .expect("genesis commit id is valid"),
            writer_epoch: WriterEpoch(0),
            writer: None,
            next_inode_id: InodeId(2),
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
    ) -> Result<Self, ControlCodecError> {
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
pub type NamespaceGcPinStateEnvelope = ControlObjectEnvelope<NamespaceGcPinState>;

/// Durable layout of a control object: the envelope fields plus the payload
/// as a raw JSON fragment. The payload stays inline (rather than as encoded
/// bytes) so control objects remain directly readable JSON, while
/// `payload_checksum` covers the exact fragment bytes as stored.
#[derive(Serialize, Deserialize)]
struct ControlObjectDocument {
    kind: String,
    format_version: u32,
    writer_version: String,
    payload_checksum: String,
    payload: Box<RawValue>,
}

#[derive(Debug, Error)]
pub enum ControlCodecError {
    #[error("failed to encode control object payload to JSON: {0}")]
    PayloadEncode(String),
    #[error("failed to encode control object envelope to JSON: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode control object envelope from JSON: {0}")]
    EnvelopeDecode(String),
    #[error("failed to decode control object payload from JSON: {0}")]
    PayloadDecode(String),
    #[error("unknown control object kind `{found}`")]
    UnknownKind { found: String },
    #[error("control object kind mismatch: expected `{expected}`, found `{found}`")]
    KindMismatch { expected: String, found: String },
    #[error(
        "unsupported `{kind}` control object format version `{found}`: \
         this build supports `{supported}`"
    )]
    UnsupportedFormatVersion {
        kind: String,
        found: u32,
        supported: u32,
    },
    #[error("control object payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error(
        "control object envelope checksum `{checksum}` does not match its payload `{actual}`: \
         rebuild the envelope with `ControlObjectEnvelope::from_state`"
    )]
    StalePayloadChecksum { checksum: String, actual: String },
}

pub fn control_payload_checksum<T>(state: &T) -> Result<String, ControlCodecError>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(state)
        .map_err(|err| ControlCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_digest(&bytes))
}

pub fn encode_control_object<T>(
    envelope: &ControlObjectEnvelope<T>,
) -> Result<Vec<u8>, ControlCodecError>
where
    T: Serialize,
{
    if envelope.format_version != envelope.kind.format_version() {
        return Err(ControlCodecError::UnsupportedFormatVersion {
            kind: envelope.kind.as_str().to_owned(),
            found: envelope.format_version,
            supported: envelope.kind.format_version(),
        });
    }
    let payload_json = serde_json::to_string(&envelope.state)
        .map_err(|err| ControlCodecError::PayloadEncode(err.to_string()))?;
    let actual = sha256_digest(payload_json.as_bytes());
    if actual != envelope.payload_checksum {
        return Err(ControlCodecError::StalePayloadChecksum {
            checksum: envelope.payload_checksum.clone(),
            actual,
        });
    }

    let document = ControlObjectDocument {
        kind: envelope.kind.as_str().to_owned(),
        format_version: envelope.format_version,
        writer_version: envelope.writer_version.clone(),
        payload_checksum: envelope.payload_checksum.clone(),
        payload: RawValue::from_string(payload_json)
            .map_err(|err| ControlCodecError::PayloadEncode(err.to_string()))?,
    };
    serde_json::to_vec(&document).map_err(|err| ControlCodecError::EnvelopeEncode(err.to_string()))
}

pub fn decode_control_object<T>(
    bytes: &[u8],
    expected_kind: ControlObjectKind,
) -> Result<ControlObjectEnvelope<T>, ControlCodecError>
where
    T: DeserializeOwned,
{
    let probe: EnvelopeProbe = serde_json::from_slice(bytes)
        .map_err(|err| ControlCodecError::EnvelopeDecode(err.to_string()))?;
    let Some(kind) = ControlObjectKind::parse(&probe.kind) else {
        return Err(ControlCodecError::UnknownKind { found: probe.kind });
    };
    if kind != expected_kind {
        return Err(ControlCodecError::KindMismatch {
            expected: expected_kind.as_str().to_owned(),
            found: probe.kind,
        });
    }
    if probe.format_version != kind.format_version() {
        return Err(ControlCodecError::UnsupportedFormatVersion {
            kind: kind.as_str().to_owned(),
            found: probe.format_version,
            supported: kind.format_version(),
        });
    }

    let document: ControlObjectDocument = serde_json::from_slice(bytes)
        .map_err(|err| ControlCodecError::EnvelopeDecode(err.to_string()))?;
    let actual = sha256_digest(document.payload.get().as_bytes());
    if actual != document.payload_checksum {
        return Err(ControlCodecError::ChecksumMismatch {
            expected: document.payload_checksum,
            actual,
        });
    }
    let state: T = serde_json::from_str(document.payload.get())
        .map_err(|err| ControlCodecError::PayloadDecode(err.to_string()))?;

    Ok(ControlObjectEnvelope {
        kind,
        format_version: document.format_version,
        writer_version: document.writer_version,
        payload_checksum: document.payload_checksum,
        state,
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
        assert!(matches!(mismatch, ControlCodecError::KindMismatch { .. }));
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
