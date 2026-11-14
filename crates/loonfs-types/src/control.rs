//! Durable control-object shapes: the manifest boundary,
//! snapshots, upload session and outcome records, pool records, and their envelopes
//! ([Appendix A.4](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads)).

use crate::envelope::EnvelopeCodecError;
use crate::WriterId;
use crate::{
    ChecksumAlgorithm, CommitId, CommitSeq, ContentId, ContentRef, ManifestNo, NamespaceId,
    PayloadChecksum, PoolId, SnapshotId, SubjectId, UploadId,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// Selects one independently versioned control-object family.
///
/// See [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlObjectKind {
    /// Keeps a manifest basis for a user or fork lifecycle.
    Snapshot,
    /// Starts one upload session.
    UploadSession,
    /// Records how one upload session ended.
    UploadOutcome,
    /// Records the creation of one pool.
    Pool,
    /// Records the manifest number at or below which every manifest of a
    /// namespace is replaced.
    Boundary,
}

impl ControlObjectKind {
    /// Lists every registered control-object family in stable registry order.
    pub const ALL: [Self; 5] = [
        Self::Snapshot,
        Self::UploadSession,
        Self::UploadOutcome,
        Self::Pool,
        Self::Boundary,
    ];

    /// Returns the durable format version for this control object kind.
    ///
    /// Versions are tracked per kind so one kind's payload schema can make a
    /// breaking change without invalidating every other control object.
    /// Version 1 is a JSON envelope document that contains the current payload as
    /// a raw JSON fragment whose checksum covers its exact bytes.
    pub const fn format_version(self) -> u32 {
        match self {
            Self::Snapshot => 1,
            Self::UploadSession => 1,
            Self::UploadOutcome => 1,
            Self::Pool => 1,
            Self::Boundary => 1,
        }
    }

    /// Returns the frozen envelope discriminator for this control-object family.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::UploadSession => "upload_session",
            Self::UploadOutcome => "upload_outcome",
            Self::Pool => "pool",
            Self::Boundary => "boundary",
        }
    }

    /// Parses a registered envelope discriminator, returning `None` for future families.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// The manifest boundary of one namespace.
///
/// A namespace's collection raises the boundary before it deletes manifests,
/// and no write lowers it. A missing object means that no manifest is at or
/// below the boundary.
///
/// See [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryPayload {
    /// Namespace whose manifests the boundary covers.
    pub namespace_id: NamespaceId,
    /// How far the namespace's collection has raised the boundary.
    pub boundary: ManifestBoundary,
}

/// The value of a manifest boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManifestBoundary {
    /// Every manifest numbered `manifest_no` or lower has been replaced by a
    /// newer manifest.
    Manifest {
        /// Positive number of the highest manifest at or below the boundary.
        manifest_no: ManifestNo,
    },
    /// The namespace is retired, and every manifest number is at or below the
    /// boundary. The boundary never leaves this state.
    ///
    /// The braces make serde reject a stray field, which a unit variant would
    /// accept and discard.
    Retired {},
}

/// One reference to a namespace manifest.
///
/// Durable objects embed this shape under `manifest`. It identifies the
/// manifest and provides the checksum required to verify it.
///
/// See [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestRef {
    /// Namespace under whose prefix the manifest and its segments live.
    pub owner_namespace_id: NamespaceId,
    /// Monotonic logical position of the referenced manifest.
    pub manifest_no: ManifestNo,
    /// The referenced manifest's folded head, a sequence of the owner namespace.
    pub head_seq: CommitSeq,
    /// Must equal `payload_checksum` in the referenced manifest envelope.
    pub payload_checksum: PayloadChecksum,
}

/// Durable owner and expiry policy of a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotOwner {
    /// A snapshot that a caller created through the snapshot API. Garbage
    /// collection deletes the record after its expiry or its deletion. The name
    /// is a label, not a key, so several records can have the same name.
    User {
        /// The label that the creator recorded, if any. Labels don't need to be
        /// unique.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// When the snapshot expires. A snapshot without an expiry doesn't expire.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at_ms: Option<u64>,
        /// When a caller deleted the snapshot. A deleted snapshot can't be used,
        /// whatever its expiry.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deleted_at_ms: Option<u64>,
    },
    /// Keeps the source manifest and its runs readable by the target.
    Fork {
        /// Fork namespace whose continued existence keeps the source basis.
        target_namespace_id: NamespaceId,
    },
}

impl SnapshotOwner {
    /// Returns when this record expires. A fork snapshot and a user snapshot without
    /// an expiry return `None`.
    pub fn expires_at_ms(&self) -> Option<u64> {
        match self {
            Self::User { expires_at_ms, .. } => *expires_at_ms,
            Self::Fork { .. } => None,
        }
    }

    /// Returns when a caller deleted this record. A fork snapshot and a user
    /// snapshot that isn't deleted return `None`.
    pub fn deleted_at_ms(&self) -> Option<u64> {
        match self {
            Self::User { deleted_at_ms, .. } => *deleted_at_ms,
            Self::Fork { .. } => None,
        }
    }
}

/// A snapshot stored under `snapshots/`; see format specification section 8.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotPayload {
    /// Namespace containing the snapshot's manifest.
    pub namespace_id: NamespaceId,
    /// Positions this record at its manifest number.
    pub snapshot_id: SnapshotId,
    /// Greatest sequence in the snapshot's manifest.
    pub head_seq: CommitSeq,
    /// Verifies the referenced manifest payload.
    pub payload_checksum: PayloadChecksum,
    /// Creation time used by collection grace.
    pub created_at_ms: u64,
    /// Determines when collection may delete this record.
    pub owner: SnapshotOwner,
}

impl SnapshotPayload {
    /// Builds the reference for reads through this snapshot.
    pub fn manifest(&self) -> ManifestRef {
        ManifestRef {
            owner_namespace_id: self.namespace_id.clone(),
            manifest_no: self.snapshot_id.manifest_no(),
            head_seq: self.head_seq,
            payload_checksum: self.payload_checksum.clone(),
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

/// Namespace lifecycle status.
///
/// A namespace is either active or deleted. Missing and unknown
/// status values fail decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NamespaceStatus {
    /// The namespace serves reads and accepts commits.
    ///
    /// The braces make serde reject a stray field; a unit variant would
    /// silently accept and discard one.
    Active {},
    /// The namespace has been deleted.
    Deleted {
        /// Unix-millisecond call clock of the deletion.
        deleted_at_ms: u64,
    },
}

impl NamespaceStatus {
    /// Returns whether the current namespace is deleted.
    pub const fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted { .. })
    }
    /// Returns the deletion stamp for a deleted namespace.
    pub const fn deleted_at_ms(&self) -> Option<u64> {
        match self {
            Self::Deleted { deleted_at_ms, .. } => Some(*deleted_at_ms),
            Self::Active {} => None,
        }
    }
}

/// Immutable fork provenance matched against the fork snapshot by GC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkBasis {
    /// Source manifest used as the target's initial state. Its owner must
    /// differ from the target namespace.
    pub manifest: ManifestRef,
    /// Fork snapshot that keeps the basis for as long as the target lives.
    pub source_snapshot_id: SnapshotId,
    /// The fork whose commits a rebase replays into the target, which only a
    /// rebase's new fork records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rebased_from: Option<RebasedFrom>,
}

/// The fork whose unmerged commits a rebase replays into a new fork of that
/// fork's source, recorded in the new fork's fork basis.
///
/// See [Merges](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#merges).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebasedFrom {
    /// The fork whose commits the new fork's replay commits apply.
    pub source_namespace_id: NamespaceId,
    /// The fork sequence that the fork's next merge into its source would
    /// have started after when the rebase made the new fork. The new fork's
    /// first replay commit starts after it.
    pub resume_after_seq: CommitSeq,
}

const GENESIS_COMMIT_ID: &str = "cmt_00000000000000000000000000";

/// Returns the commit id every namespace's sequence zero has, before any
/// commit is published.
pub fn genesis_commit_id() -> CommitId {
    CommitId::parse(GENESIS_COMMIT_ID).expect("the genesis commit ID should parse")
}

/// How an upload session's bytes reach object storage.
///
/// The mode is set when the session starts and can't be changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadSessionMode {
    /// The service receives the bytes and writes the content object itself,
    /// so it learns size and digest from the bytes as they pass.
    ///
    /// The braces make serde reject a stray field; a unit variant would
    /// silently accept and discard one.
    ServiceProxied {},
    /// The client writes the whole object through one presigned request.
    DirectPut {
        /// Checksum algorithm chosen when the session began.
        checksum_algorithm: ChecksumAlgorithm,
    },
    /// The client uploads parts and the provider assembles the object.
    ///
    /// Multipart sessions do not store a content reference at creation because
    /// one-pass and streaming clients may not have the final size or checksum.
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
            Self::ServiceProxied {} => None,
            Self::DirectPut { checksum_algorithm }
            | Self::DirectMultipart {
                checksum_algorithm, ..
            } => Some(*checksum_algorithm),
        }
    }
}

/// The record that starts one upload session.
///
/// LoonFS writes it once, with put-if-absent, when the session starts, and
/// never changes it. How the session ended is a separate record,
/// [`UploadOutcomePayload`], so no upload writes the same object twice. A
/// session without an outcome record is open.
///
/// See [upload sessions](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#51-upload-sessions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadSessionPayload {
    /// The namespace that created the session, and the only namespace whose
    /// commits the session's content admits. The record is stored under the
    /// prefix of that namespace's pool.
    pub namespace_id: NamespaceId,
    /// Durable session identity used by upload and completion requests.
    pub upload_id: UploadId,
    /// Content object this session writes, allocated when the session began.
    ///
    /// The identity exists before any byte is read, so the final object key
    /// is known up front and belongs to exactly this session.
    pub content_id: ContentId,
    /// The subject that opened the session, recorded in an ACL namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_id: Option<SubjectId>,
    /// How the bytes reach object storage, settled when the session opened.
    pub mode: UploadSessionMode,
    /// Unix-millisecond instant at which an open session stops accepting
    /// upload work. The record includes it so no session transition depends
    /// on an object's provider timestamp.
    pub expires_at_ms: u64,
}

/// How an upload session ended.
///
/// A session ends at most once: its outcome record is created with
/// put-if-absent, so completion and abort race on that one create.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UploadOutcome {
    /// The content is durable and verified. Only a completed session can
    /// issue receipts or replay completion.
    Completed {
        /// Unix-millisecond stamp of the completion, and the only input to
        /// when the session's records may be deleted.
        completed_at_ms: u64,
        /// Verified immutable content produced by the session.
        content_ref: ContentRef,
    },
    /// The session can't provide content. Its unpublished object is deleted.
    Aborted {
        /// Unix-millisecond stamp of the abort, and the only input to when
        /// the session's records may be deleted.
        aborted_at_ms: u64,
    },
}

impl UploadOutcome {
    /// Returns the first abort's stamp, for an abort that finds the session
    /// already ended.
    ///
    /// A repeated abort reports the first abort's time. A completed session
    /// refuses, because a commit may already reference its content.
    pub fn repeated_abort(&self) -> Result<u64, UploadAlreadyCompletedError> {
        match self {
            Self::Aborted { aborted_at_ms } => Ok(*aborted_at_ms),
            Self::Completed { .. } => Err(UploadAlreadyCompletedError),
        }
    }
}

impl std::fmt::Display for UploadOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Completed { .. } => "completed",
            Self::Aborted { .. } => "aborted",
        })
    }
}

/// Describes an abort of an upload session that already completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a completed upload session cannot be aborted")]
pub struct UploadAlreadyCompletedError;

/// The record of how one upload session ended.
///
/// LoonFS creates it once, with put-if-absent, beside the session record,
/// and never changes it. It counts only while the session record exists.
///
/// See [upload sessions](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#51-upload-sessions).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadOutcomePayload {
    /// The namespace that created the session.
    pub namespace_id: NamespaceId,
    /// The session that ended.
    pub upload_id: UploadId,
    /// How the session ended.
    pub outcome: UploadOutcome,
}

/// The record that a namespace's create call writes when the call generates
/// a pool.
///
/// LoonFS writes the record once, with put-if-absent, before the namespace's
/// manifest 1, and never changes the record.
///
/// See [pools and pool records](https://github.com/loonfs/loonfs/blob/main/docs/design/pools.md#pools-and-pool-records).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolPayload {
    /// The pool that the record describes.
    pub pool_id: PoolId,
    /// The namespace that was created with the pool, the pool's first member.
    pub first_member_namespace_id: NamespaceId,
    /// The create call's request clock, in unix milliseconds.
    pub created_at_ms: u64,
}

/// Control state decoded through its checked durable codec.
pub type ControlObjectEnvelope<T> = crate::envelope::VerifiedEnvelope<T>;

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
/// [control and manifest payloads](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#a4-control-and-manifest-payloads).
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
