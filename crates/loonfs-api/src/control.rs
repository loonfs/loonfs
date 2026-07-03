use crate::digest::sha256_digest;
use crate::envelope::EnvelopeProbe;
use crate::v0::UploadMode;
use crate::WriterEpoch;
use crate::{
    ChangeSeq, CommitId, ContentRef, ContentStoreId, InodeId, ManifestId, NamePolicy, NamespaceId,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    NamespaceDescriptor,
    ContentStoreDescriptor,
    NamespaceHead,
    NamespaceForkState,
    NamespaceGcPinState,
    NamespaceProgress,
    UploadSession,
}

impl ControlObjectKind {
    pub const ALL: [Self; 7] = [
        Self::NamespaceDescriptor,
        Self::ContentStoreDescriptor,
        Self::NamespaceHead,
        Self::NamespaceForkState,
        Self::NamespaceGcPinState,
        Self::NamespaceProgress,
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
            Self::NamespaceDescriptor => 1,
            Self::ContentStoreDescriptor => 1,
            Self::NamespaceHead => 1,
            Self::NamespaceForkState => 1,
            Self::NamespaceGcPinState => 1,
            Self::NamespaceProgress => 1,
            Self::UploadSession => 1,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NamespaceDescriptor => "namespace_descriptor",
            Self::ContentStoreDescriptor => "content_store_descriptor",
            Self::NamespaceHead => "namespace_head",
            Self::NamespaceForkState => "namespace_fork_state",
            Self::NamespaceGcPinState => "namespace_gc_pin_state",
            Self::NamespaceProgress => "namespace_progress",
            Self::UploadSession => "upload_session",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceDescriptorState {
    pub namespace_id: NamespaceId,
    pub content_store_id: ContentStoreId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreDescriptorState {
    pub content_store_id: ContentStoreId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceForkState {
    pub namespace_id: NamespaceId,
    pub source_namespace_id: NamespaceId,
    pub fork_seq: ChangeSeq,
    pub source_checkpoint_id: String,
    pub source_manifest_id: ManifestId,
    pub source_head_seq: ChangeSeq,
    pub created_at_ms: u64,
}

/// Pin that prevents the source namespace's GC from collecting the metadata
/// files a fork still shares. Reachability is derived from the pinned
/// manifest (`source_manifest_id`), not stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceGcPinState {
    pub pin_id: String,
    pub source_namespace_id: NamespaceId,
    pub target_namespace_id: NamespaceId,
    pub source_checkpoint_id: String,
    pub source_manifest_id: ManifestId,
    pub source_head_seq: ChangeSeq,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalSegmentPointer {
    pub object_key: String,
    pub segment_id: String,
    pub start_seq: ChangeSeq,
    pub end_seq: ChangeSeq,
    /// Checksum of the referenced segment's payload bytes, in `sha256:<hex>`
    /// form. Must equal the `payload_checksum` in the referenced envelope.
    pub payload_checksum: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriterLease {
    pub writer_id: String,
    pub writer_session_id: String,
    pub lease_expires_at_ms: u64,
}

impl WriterLease {
    pub fn is_valid_at(&self, now_ms: u64) -> bool {
        self.lease_expires_at_ms > now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredWriter {
    pub writer_id: String,
    pub writer_session_id: String,
    pub writer_epoch: WriterEpoch,
    pub lease_expires_at_ms: u64,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_lease: Option<WriterLease>,
    pub next_inode_id: InodeId,
    #[serde(default)]
    pub name_policy: NamePolicy,
    /// Live read/recovery pointer. If present, basis reconstruction loads this
    /// manifest before replaying the visible WAL tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_manifest_id: Option<ManifestId>,
    /// Admin/provenance convenience pointer to the latest checkpoint record.
    /// Reads do not use this as authority; checkpoints pin manifests for
    /// retention, forks, stable reads, and restore workflows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_checkpoint_id: Option<String>,
    pub retention_floor_seq: ChangeSeq,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_wal_tip: Option<WalSegmentPointer>,
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
            writer_lease: None,
            next_inode_id: InodeId(2),
            name_policy: NamePolicy::default(),
            current_manifest_id: None,
            latest_checkpoint_id: None,
            retention_floor_seq: ChangeSeq(0),
            visible_wal_tip: None,
            state: NamespaceState::Active,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressState {
    pub namespace_id: NamespaceId,
    pub work_class: String,
    pub through_seq: ChangeSeq,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedUpload {
    pub content_ref: ContentRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadSessionState {
    pub namespace_id: NamespaceId,
    pub upload_id: String,
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
pub type ProgressStateEnvelope = ControlObjectEnvelope<ProgressState>;
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
pub type NamespaceDescriptorEnvelope = ControlObjectEnvelope<NamespaceDescriptorState>;
pub type ContentStoreDescriptorEnvelope = ControlObjectEnvelope<ContentStoreDescriptorState>;
pub type NamespaceForkStateEnvelope = ControlObjectEnvelope<NamespaceForkState>;
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
            ControlObjectKind::NamespaceHead,
            "test-writer",
            HeadState::initial(namespace_id),
        )
        .expect("envelope");

        let encoded = encode_control_object(&envelope).expect("encode");
        let decoded: HeadStateEnvelope =
            decode_control_object(&encoded, ControlObjectKind::NamespaceHead).expect("decode");
        assert_eq!(decoded, envelope);

        let mismatch = decode_control_object::<NamespaceDescriptorState>(
            &encoded,
            ControlObjectKind::NamespaceDescriptor,
        )
        .expect_err("kind mismatch");
        assert!(matches!(mismatch, ControlCodecError::KindMismatch { .. }));
    }

    #[test]
    fn control_object_decode_tolerates_unknown_payload_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let envelope = HeadStateEnvelope::from_state(
            ControlObjectKind::NamespaceHead,
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
            decode_control_object(future_text.as_bytes(), ControlObjectKind::NamespaceHead)
                .expect("additive payload fields must remain readable");
        assert_eq!(decoded.state, envelope.state);
        assert_eq!(decoded.payload_checksum, future_checksum);
    }
}
