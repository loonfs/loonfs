//! Durable control-object shapes: the discovery hint,
//! checkpoint records, upload sessions, and their envelopes (format spec,
//! "Control objects").

use crate::envelope::EnvelopeCodecError;
use crate::{
    ChangeSeq, CheckpointId, ChecksumAlgorithm, CommitId, ContentId, ContentRef, ContentStoreId,
    ManifestNo, NamespaceId, UploadId,
};
use crate::{WriterEpoch, WriterId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::num::NonZeroU64;

/// Selects one independently versioned control-object family.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    /// Starts forward discovery of numbered manifests.
    Hint,
    /// Pins a manifest basis for a user or fork lifecycle.
    CheckpointRecord,
    /// Tracks staged content through upload completion or cleanup.
    UploadSession,
    /// Identifies the content domain held by a backend.
    ContentStore,
}

impl ControlObjectKind {
    /// Lists every registered control-object family in stable registry order.
    pub const ALL: [Self; 4] = [
        Self::Hint,
        Self::CheckpointRecord,
        Self::UploadSession,
        Self::ContentStore,
    ];

    /// Durable format version for this control object kind.
    ///
    /// Versions are tracked per kind so one kind's payload schema can make a
    /// breaking change without invalidating every other control object.
    /// Version 1 is a JSON envelope document carrying the current payload as
    /// a raw JSON fragment whose checksum covers its exact bytes.
    pub const fn format_version(self) -> u32 {
        match self {
            Self::Hint => 1,
            Self::CheckpointRecord => 1,
            Self::UploadSession => 1,
            Self::ContentStore => 1,
        }
    }

    /// Returns the frozen envelope discriminator for this control-object family.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hint => "hint",
            Self::CheckpointRecord => "checkpoint_record",
            Self::UploadSession => "upload_session",
            Self::ContentStore => "content_store",
        }
    }

    /// Parses a registered envelope discriminator, returning `None` for future families.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// Identifies a content domain in its physical backend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContentStoreState {
    /// Domain whose objects share this descriptor's prefix.
    pub content_store_id: ContentStoreId,
    /// Unix-millisecond stamp from the domain's creation context.
    pub created_at_ms: u64,
}

/// Starts manifest discovery without selecting the current version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HintState {
    /// Namespace whose manifest collection is probed.
    pub namespace_id: NamespaceId,
    /// First number to read; zero starts before the first manifest.
    pub manifest_no: ManifestNo,
    /// Highest acknowledged WAL number known to the publisher.
    pub wal_no: crate::WalNo,
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
    /// Greatest owner-namespace sequence the referenced manifest materializes.
    pub manifest_head_seq: ChangeSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub manifest_payload_checksum: String,
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
    /// Keeps the source manifest and its runs readable by the target.
    Fork {
        /// Fork namespace whose continued existence keeps the source basis pinned.
        target_namespace_id: NamespaceId,
    },
    /// An application-created read view with a required expiry.
    Snapshot {
        /// Application-facing label that need not be unique.
        name: String,
        /// When garbage collection may release the pin.
        expires_at_ms: u64,
    },
}

impl CheckpointOwner {
    /// When garbage collection may release this record without asking its owner.
    pub fn expires_at_ms(&self) -> Option<u64> {
        match self {
            Self::User { expires_at_ms, .. } => *expires_at_ms,
            Self::Fork { .. } => None,
            Self::Snapshot { expires_at_ms, .. } => Some(*expires_at_ms),
        }
    }
}

/// A pin stored under `pins/`; see format specification section 1.7.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRecordState {
    /// Namespace containing the pinned manifest.
    pub namespace_id: NamespaceId,
    /// Positions this record at its manifest number.
    pub pin_id: CheckpointId,
    /// Must equal the number in `pin_id`.
    pub manifest_no: ManifestNo,
    /// Greatest sequence in the pinned manifest.
    pub manifest_head_seq: ChangeSeq,
    /// Verifies the referenced manifest payload.
    pub manifest_payload_checksum: String,
    /// Commit at the pinned manifest head.
    pub head_commit_id: CommitId,
    /// Creation time used by collection grace.
    pub created_at_ms: u64,
    /// Determines when collection may delete this record.
    pub owner: CheckpointOwner,
}

impl CheckpointRecordState {
    /// Builds the reference for reads through this pin.
    pub fn manifest(&self) -> ManifestRef {
        ManifestRef {
            owner_namespace_id: self.namespace_id.clone(),
            manifest_no: self.pin_id.manifest_no(),
            manifest_head_seq: self.manifest_head_seq,
            manifest_payload_checksum: self.manifest_payload_checksum.clone(),
        }
    }
}

/// Who most recently acquired the writer epoch, and when.
///
/// Writer label and acquisition time; the epoch determines fencing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriterBlock {
    /// Stable writer label supplied by the embedding process for diagnostics.
    pub writer_id: WriterId,
    /// Unix-millisecond stamp of the epoch acquisition.
    pub acquired_at_ms: u64,
}

/// Captures the writer identity and fencing epoch a session must retain while publishing.
///
/// See [mutable control-object rules](../../../docs/specs/format.md#17-mutable-control-object-rules).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquiredWriter {
    /// Stable writer label copied into the manifest's writer block.
    pub writer_id: WriterId,
    /// Fencing epoch every commit publication from this session must match.
    pub writer_epoch: WriterEpoch,
}

/// Terminal namespace status.
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
    Deleted {
        /// Earliest owner-prefix collection time once dependencies are gone.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reclaim_after_ms: Option<u64>,
    },
}

impl NamespaceStatus {
    /// Returns whether the namespace is permanently deleted.
    pub const fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted { .. })
    }
    /// Returns the irrevocable collection deadline, if retirement is established.
    pub const fn reclaim_after_ms(&self) -> Option<u64> {
        match self {
            Self::Deleted { reclaim_after_ms } => *reclaim_after_ms,
            Self::Active {} => None,
        }
    }
}

/// Immutable fork provenance matched against the source checkpoint by GC.
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

const GENESIS_COMMIT_ID: &str = "c_00000000000000000000000000000000";

/// The commit id every namespace's sequence zero carries, before any commit
/// has landed.
pub fn genesis_commit_id() -> CommitId {
    CommitId::parse(GENESIS_COMMIT_ID).expect("genesis commit id is valid")
}

/// Staging progress for a service-proxied upload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxiedStaging {
    /// No request owns the staging slot and no staged reference is retained.
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
    /// Returns the checksum algorithm fixed by a direct upload mode.
    pub fn checksum_algorithm(&self) -> Option<ChecksumAlgorithm> {
        match self {
            Self::ServiceProxied { .. } => None,
            Self::DirectPut { checksum_algorithm }
            | Self::DirectMultipart {
                checksum_algorithm, ..
            } => Some(*checksum_algorithm),
        }
    }

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
    fn validate(&self) -> Result<(), String> {
        if !matches!(self.status, UploadSessionRecordStatus::Open { .. })
            && self.mode.content_ref().is_some()
        {
            return Err(format!(
                "upload session `{}` is {} but still holds a staged content reference",
                self.upload_id, self.status
            ));
        }
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
            Some(checksum_algorithm),
            UploadSessionRecordStatus::Completed { content_ref, .. },
        ) = (self.mode.checksum_algorithm(), &self.status)
        {
            if content_ref.checksum.algorithm != checksum_algorithm {
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
    Staged { content_ref: ContentRef },
}

impl From<StrictProxiedStaging> for ProxiedStaging {
    fn from(staging: StrictProxiedStaging) -> Self {
        match staging {
            StrictProxiedStaging::Idle {} => Self::Idle,
            StrictProxiedStaging::Claimed {} => Self::Claimed,
            StrictProxiedStaging::Staged { content_ref } => Self::Staged(content_ref),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictUploadSessionRecordStatus {
    Open {
        expires_at_ms: u64,
    },
    Completed {
        completed_at_ms: u64,
        content_ref: ContentRef,
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
                content_ref,
            },
            StrictUploadSessionRecordStatus::Aborted { aborted_at_ms } => {
                Self::Aborted { aborted_at_ms }
            }
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

/// Control state decoded through its checked durable codec.
pub type ControlObjectEnvelope<T> = crate::envelope::VerifiedEnvelope<T>;

/// Specializes a control envelope for a durable upload workflow.
pub type UploadSessionEnvelope = ControlObjectEnvelope<UploadSessionState>;
/// Specializes a control envelope for manifest discovery.
pub type HintEnvelope = ControlObjectEnvelope<HintState>;
/// Specializes a control envelope for a durable manifest pin.
pub type CheckpointRecordEnvelope = ControlObjectEnvelope<CheckpointRecordState>;

/// Encodes control state once, deriving its checksum and family version.
pub fn encode_control_state<T: Serialize>(
    kind: ControlObjectKind,
    state: &T,
) -> Result<Vec<u8>, EnvelopeCodecError> {
    crate::envelope::encode_json_envelope(kind.as_str(), kind.format_version(), state)
        .map(crate::envelope::EncodedEnvelope::into_bytes)
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

    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Checksum, ContentRefKind};

    #[test]
    fn completed_proxied_session_rejects_conflicting_staged_size() {
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id: ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("content id"),
            size_bytes: 5,
            checksum: Checksum::sha256(b"hello"),
        };
        let mut staged = content_ref.clone();
        staged.size_bytes += 1;
        let session = UploadSessionState {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: UploadId::parse("upl_0123456789abcdef0123456789abcdef").expect("upload id"),
            content_id: content_ref.content_id.clone(),
            created_at_ms: 1_000,
            mode: UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Staged(staged),
            },
            status: UploadSessionRecordStatus::Completed {
                completed_at_ms: 2_000,
                content_ref,
            },
        };

        let error = session.validate().expect_err("conflicting staged size");
        assert_eq!(
            error,
            format!(
                "upload session `{}` is completed but still holds a staged content reference",
                session.upload_id
            )
        );
    }

    #[test]
    fn terminal_upload_modes_reject_only_retained_staged_references() {
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: crate::NamespaceId::parse("demo").expect("namespace id"),
            content_id: ContentId::parse("con_0123456789abcdef0123456789abcdef")
                .expect("content id"),
            size_bytes: 5,
            checksum: Checksum::sha256(b"hello"),
        };
        let modes = [
            UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Idle,
            },
            UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Claimed,
            },
            UploadSessionMode::ServiceProxied {
                staging: ProxiedStaging::Staged(content_ref.clone()),
            },
            UploadSessionMode::DirectPut {
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
            UploadSessionMode::DirectMultipart {
                provider_upload_id: "provider-upload".to_owned(),
                part_size_bytes: NonZeroU64::new(8 * 1024 * 1024).expect("part size"),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
        ];
        for mode in modes {
            for status in [
                UploadSessionRecordStatus::Completed {
                    completed_at_ms: 2_000,
                    content_ref: content_ref.clone(),
                },
                UploadSessionRecordStatus::Aborted {
                    aborted_at_ms: 2_000,
                },
            ] {
                let session = UploadSessionState {
                    namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                    upload_id: UploadId::parse("upl_0123456789abcdef0123456789abcdef")
                        .expect("upload id"),
                    content_id: content_ref.content_id.clone(),
                    created_at_ms: 1_000,
                    mode: mode.clone(),
                    status,
                };
                let encoded = serde_json::to_value(&session).expect("encode session");
                let decoded = serde_json::from_value::<UploadSessionState>(encoded);
                if mode.content_ref().is_some() {
                    let error = decoded
                        .expect_err("terminal staging is corrupt")
                        .to_string();
                    assert!(error.contains(session.upload_id.as_str()));
                    assert!(error.contains("still holds a staged content reference"));
                } else {
                    assert_eq!(decoded.expect("valid terminal session"), session);
                }
            }
        }
    }

    #[test]
    fn control_object_kind_strings_round_trip_and_match_serde() {
        for kind in ControlObjectKind::ALL {
            assert_eq!(ControlObjectKind::parse(kind.as_str()), Some(kind));
            let serialized = serde_json::to_value(kind).expect("serialize kind");
            assert_eq!(serialized, serde_json::Value::from(kind.as_str()));
        }
        assert_eq!(ControlObjectKind::parse("not_a_kind"), None);
    }
}
