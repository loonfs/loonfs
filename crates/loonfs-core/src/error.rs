//! Core error types and their wire-code classification.
//!
//! Core errors must support `Clone`. Errors from the object store, codecs, and
//! I/O are therefore stored as messages with the relevant object key instead
//! of as `#[source]` chains. The server and CLI use source chains where this
//! constraint does not apply.

use crate::commit::engine::ContentPreparationError;
use crate::commit::CommitValidationError;
use crate::control_object::ControlObjectLoadError;
use crate::manifest::ManifestLoadError;
use crate::metadata::VisiblePathError;
use crate::storage::content::DurableContentValidationError;
use crate::wal::WalPublishError;
use crate::wal::{WalObjectError, WalTailLoadError};
use loonfs_objectstore::ObjectStoreError;
use loonfs_types::format::control::WriterBlock;
pub use loonfs_types::MergeBound;
use loonfs_types::{ActiveWriter, CommittedReceipt, MergeConflict};
use loonfs_types::{
    BindingVersion, CommitId, CommitSeq, ErrorCause, ErrorDetails, InodeId, InodeKind, ManifestNo,
    NamespaceId, PathError, PoolId, RevisionNo, UploadId, WriterEpoch,
};
use thiserror::Error;

/// Result type returned by `loonfs-core`.
pub type Result<T> = std::result::Result<T, CoreError>;

use loonfs_objectstore::ObjectStoreErrorKind;
pub use loonfs_types::{ErrorCode, ErrorKind};

/// Detailed core error.
///
/// Most callers should branch on [`CoreError::kind`] or [`CoreError::code`]
/// instead of matching every internal variant.
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum CoreError {
    #[error(transparent)]
    MetadataProjection(#[from] MetadataProjectionLoadError),
    #[error(transparent)]
    ControlObjectLoad(#[from] ControlObjectLoadError),
    /// The namespace's WAL tail is at the write stop, so the namespace
    /// refuses publishes until maintenance folds the tail. `reason` says how
    /// long the tail is and when publishes resume.
    #[error("writes to namespace `{namespace_id}` are stopped until maintenance runs: {reason}")]
    MaintenanceRequired {
        namespace_id: NamespaceId,
        reason: String,
    },
    #[error(transparent)]
    VisiblePath(VisiblePathError),
    #[error(transparent)]
    DurableContent(#[from] DurableContentValidationError),
    #[error("commit validation failed: {0}")]
    CommitValidation(#[from] CommitValidationError),
    #[error("WAL publication failed: {0}")]
    WalPublish(#[from] WalPublishError),
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error("invalid commit request: {0}")]
    InvalidCommitRequest(String),
    #[error("invalid commit request: {message}")]
    InvalidCommitField {
        field: &'static str,
        message: String,
    },
    #[error("path not found `{0}`")]
    PathNotFound(String),
    #[error("inode not found `{0}`")]
    InodeNotFound(InodeId),
    #[error("revision `{revision_no}` not found for inode `{inode_id}`")]
    RevisionNotFound {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    /// The content exceeds the caller's in-memory limit. No data was fetched.
    #[error(
        "file content is {size_bytes} bytes, over the {max_bytes}-byte limit \
         this deployment buffers for one read"
    )]
    ContentTooLarge { size_bytes: u64, max_bytes: u64 },
    #[error("commit is too large for one WAL object: estimated {estimated_bytes} bytes exceeds the limit of {max_bytes} bytes")]
    CommitTooLarge {
        estimated_bytes: usize,
        max_bytes: usize,
    },
    /// A merge's estimated bytes exceed one WAL object or the publisher's
    /// merge allowance, so no retry can admit it. The caller merges the
    /// fork's commits in smaller ranges.
    #[error(
        "merge is too large for {}: estimated {estimated_bytes} bytes exceeds the limit of {max_bytes} bytes",
        .bound.describe()
    )]
    MergeTooLarge {
        bound: MergeBound,
        estimated_bytes: usize,
        max_bytes: usize,
        /// The split hint: a fork sequence such that a merge of the fork's
        /// commits through it fits, or `None` when no shorter range fits.
        split_at_seq: Option<CommitSeq>,
    },
    #[error("an append carries {size_bytes} bytes; one append carries at most {max_bytes}")]
    AppendTooLarge { size_bytes: u64, max_bytes: u64 },
    /// An inline content value is larger than one WAL piece can contain.
    #[error(
        "inline content `{content_id}` is {size_bytes} bytes; one value is at most {max_bytes}"
    )]
    InlineContentTooLarge {
        content_id: loonfs_types::ContentId,
        size_bytes: u64,
        max_bytes: u64,
    },
    /// The request contains more items than one batch may read. No items were
    /// read; split the request into smaller batches.
    #[error("asked for {requested} items, over the {max} one batch answers")]
    BatchTooLarge { requested: usize, max: usize },
    /// The requested start offset is past the end of the content. This can also
    /// happen when the caller supplies more previously read bytes than the
    /// content contains. No data was read.
    #[error("cannot start a read at offset {start_offset} of {size_bytes}-byte content")]
    ResumeOffsetOutOfRange { start_offset: u64, size_bytes: u64 },
    /// A resumed read cannot continue until the caller supplies all bytes before
    /// the resume offset for whole-object checksum verification. No data was
    /// read.
    #[error(
        "a read resumed at offset {start_offset} was given {folded} bytes of what it skipped; \
         verification covers the whole object, so all of them are needed first"
    )]
    ResumePrefixIncomplete { start_offset: u64, folded: u64 },
    /// A resumed read refused prefix bytes: they ran past the resume offset,
    /// or they arrived after the read fetched content or reached its end.
    /// None of them was folded.
    #[error(transparent)]
    ResumedPrefix(#[from] loonfs_types::ResumedPrefixError),
    /// A ranged read was given resumed prefix bytes, which only a verified
    /// read of the whole file uses.
    #[error("a ranged read doesn't verify the file, so it takes no resumed prefix")]
    PrefixForRangedRead,
    /// An extension's change was given a basis that another namespace's
    /// manifest was loaded from.
    #[error(
        "invalid extension basis: it was loaded from namespace `{basis_namespace_id}`, not \
         `{namespace_id}`"
    )]
    InvalidExtensionBasis {
        namespace_id: loonfs_types::NamespaceId,
        basis_namespace_id: loonfs_types::NamespaceId,
    },
    /// The requested range starts at or past the end of the file. No data
    /// was read.
    #[error(
        "cannot read a range that starts at offset {start_offset} of a {size_bytes}-byte file"
    )]
    RangeNotSatisfiable { start_offset: u64, size_bytes: u64 },
    #[error("expected file at `{target}` but found `{kind}`")]
    ExpectedFile { target: String, kind: InodeKind },
    #[error("expected directory at `{target}` but found `{kind}`")]
    ExpectedDirectory { target: String, kind: InodeKind },
    #[error("the root cannot be mutated and has no binding version")]
    RootMutationForbidden,
    #[error("{}", destination_exists_message(.path, .existing_display_name.as_deref()))]
    DestinationExists {
        path: String,
        /// Stored name of the existing entry, when available. It is included in the
        /// error when normalization or case folding makes it conflict with the name
        /// supplied by the caller.
        existing_display_name: Option<String>,
    },
    /// The requested binding version is no longer current.
    #[error("inode `{inode_id}` is no longer bound at the version the request named")]
    BindingVersionMismatch {
        inode_id: InodeId,
        expected_binding_version: BindingVersion,
        /// Absent when the inode is not visible.
        actual_binding_version: Option<BindingVersion>,
    },
    #[error("commit id conflict for `{commit_id}`")]
    CommitIdReuseConflict {
        commit_id: String,
        /// Sequence and fingerprint of the commit that already used this ID,
        /// when a durable receipt exists. This is `None` when two concurrent
        /// requests claim the same ID before either one commits. The
        /// fingerprint lets a retry verify that it represents the same request.
        committed: Option<CommittedReceipt>,
    },
    /// A merge doesn't start where the previous merge from its fork stopped,
    /// or, for the fork's first merge, at the fork point.
    #[error(
        "merge from `{source_namespace_id}` starts after `{actual_after_seq}`, but it has to continue from `{expected_after_seq}`"
    )]
    MergeRangeConflict {
        source_namespace_id: NamespaceId,
        expected_after_seq: CommitSeq,
        actual_after_seq: CommitSeq,
    },
    /// A fork's change conflicts with the namespace being merged into, under
    /// the conflict rule. `conflict` is the conflicting inode with the
    /// smallest ID.
    #[error(
        "merge conflicts with this namespace on {conflict_count} inodes, the first `{}` ({:?})",
        conflict.inode_id,
        conflict.kind
    )]
    MergeConflict {
        conflict_count: u64,
        conflict: Box<MergeConflict>,
    },
    /// A merge request identifies a source that isn't a fork of the namespace
    /// being merged into, or a sequence to merge through that is outside the
    /// fork's history. `param` identifies the request field.
    #[error("invalid merge `{param}`: {message}")]
    InvalidMerge {
        param: &'static str,
        message: String,
    },
    #[error(transparent)]
    ContentPreparation(#[from] ContentPreparationError),
    #[error("commit queue is full; slow down and retry")]
    CommitQueueFull,
    /// A merge can't reserve its share of the merge allowance, because other
    /// merges' reservations use the allowance.
    #[error("server busy: the merge allowance is in use by other merges")]
    ServerBusy,
    #[error("writer session for namespace `{namespace_id}` is closed; new work is not admitted")]
    WriterSessionClosed { namespace_id: NamespaceId },
    /// The service is shutting down. New requests are rejected, while requests
    /// accepted earlier may finish.
    #[error("shutting down; new work is not admitted")]
    ShuttingDown,
    /// The namespace's head moved while an operation that needs a fixed
    /// basis ran: a snapshot's manifest stopped being current before the
    /// snapshot was verified, or a fork's source kept a WAL tail too old to
    /// fork from after every fold that the fork ran. The runtime's retry
    /// loops take a snapshot again from a fresh basis. The code is
    /// `store_contention`, with the cause `lost_race`, because nothing was
    /// published and the same request can succeed when it's sent again.
    #[error("{0}")]
    HeadMoved(String),
    /// A snapshot record exists, but the manifest that it keeps is gone. The
    /// code is `snapshot_not_found`, with the cause `manifest_missing`,
    /// because the client's remedy is the same: use another snapshot.
    #[error("snapshot `{snapshot_id}` refers to manifest `{object_key}`, which is gone")]
    SnapshotManifestMissing {
        snapshot_id: loonfs_types::SnapshotId,
        object_key: String,
    },
    #[error("content `{content_id}` requires content writes that this deployment cannot perform; read through the proxied route")]
    ContentNotMaterialized { content_id: loonfs_types::ContentId },
    #[error("invalid snapshot request: {0}")]
    InvalidSnapshotRequest(String),
    #[error("snapshot `{snapshot_id}` was not found")]
    SnapshotNotFound {
        snapshot_id: loonfs_types::SnapshotId,
    },
    #[error(
        "metadata publication ran out of its {budget_ms}ms budget after {elapsed_ms}ms \
         and stopped before its next write"
    )]
    MetadataPublicationBudgetExceeded { elapsed_ms: u64, budget_ms: u64 },
    #[error(
        "the garbage collection live set of `{collected_id}` needs {bytes} bytes, \
         above the collection's {limit}-byte limit"
    )]
    LiveSetExceedsCollectionMemory {
        /// The namespace or the pool whose collection needs the memory.
        collected_id: String,
        bytes: usize,
        limit: usize,
    },
    #[error(
        "the next manifest of namespace `{namespace_id}` is a {document_bytes}-byte document, \
         above the {max_bytes}-byte manifest limit"
    )]
    ManifestTooLarge {
        namespace_id: NamespaceId,
        document_bytes: usize,
        max_bytes: usize,
    },
    /// The preparation of a commit in a publish batch panicked, so the
    /// commit failed before its batch's WAL put and nothing of it was
    /// committed. The string is the panic's message, which the runtime logs
    /// and the error's text leaves out, because a panic's message can
    /// include internal state.
    #[error("the server hit a bug while it prepared the commit")]
    CommitPreparationPanicked(String),
    #[error("upload session `{upload_id}` was not found")]
    UploadNotFound { upload_id: UploadId },
    #[error("upload session `{upload_id}` is already completed")]
    UploadAlreadyCompleted { upload_id: UploadId },
    #[error("upload session `{upload_id}` content conflicts with prior content")]
    UploadContentConflict { upload_id: UploadId },
    #[error("invalid upload content: {0}")]
    InvalidUploadContent(String),
    #[error("invalid cursor: {0}")]
    InvalidCursor(String),
    /// A change feed or diff request's `event_index` doesn't name an event
    /// of the commit that the page resumes, or the request doesn't let a
    /// page split a commit.
    #[error("invalid `event_index`: {0}")]
    InvalidEventIndex(String),
    /// A position in a namespace's history that a read needs is below a
    /// retention floor, so a base compaction might have removed the rows or
    /// changes that the read needs. `below` says which position, and the
    /// details report the floor and the fields that identify the position.
    #[error(
        "{below} is older than retention floor `{retention_floor_seq}`{}",
        below.consequence()
    )]
    BelowRetentionFloor {
        below: HistoryPosition,
        retention_floor_seq: CommitSeq,
    },
    /// A diff's `from_seq`, `to_seq`, or `after_seq` is out of order with the
    /// others or with the head. `param` identifies the parameter.
    #[error("invalid diff `{param}`: {message}")]
    InvalidDiffRange {
        param: &'static str,
        message: String,
    },
    #[error("namespace corrupt: {0}")]
    NamespaceCorrupt(String),
    /// This writer session's epoch was superseded. Terminal for the session, so
    /// callers report it without reacquiring.
    #[error("writer session fenced: {0}")]
    WriterFenced(WriterFence),
    #[error("object store error for `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        kind: ObjectStoreErrorKind,
    },
    #[error("the outcome of the write to `{object_key}` is unknown: {message}")]
    OutcomeUnknown { object_key: String, message: String },
    /// An operation on a control object used every attempt it was allowed,
    /// because another publication changed the object first on every
    /// attempt, or a read found a newer manifest on every attempt. The code is
    /// `store_contention`, with the cause `lost_race`. Retrying can succeed.
    #[error("the operation on `{object_key}` used every attempt it was allowed")]
    StoreContention { object_key: String },
    #[error("namespace `{namespace_id}` does not exist")]
    NamespaceNotFound { namespace_id: NamespaceId },
    #[error("pool `{pool_id}` has no object under its prefix")]
    PoolNotFound { pool_id: PoolId },
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error("the subject lacks a right the operation needs on inode `{inode_id}`")]
    Forbidden { inode_id: InodeId },
    /// A caller-supplied `expected_head_seq` did not match the current head.
    ///
    /// This is the one error with the `stale_head` code. It includes both
    /// sequence numbers so the caller can choose whether to retry.
    #[error("expected head sequence {expected}, found {actual}")]
    StaleHeadPrecondition {
        expected: CommitSeq,
        actual: CommitSeq,
    },
    /// A read or a publication on a manifest that a newer manifest replaced
    /// found the manifest, or a metadata segment it lists, missing. Garbage
    /// collection keeps the current manifest and the manifests that
    /// snapshots keep, with their segments, so it can delete a replaced
    /// manifest and that manifest's segments. A read runs again at the
    /// current manifest, and the publisher plans the commit again on it. A
    /// client sees this error when every attempt found its manifest replaced.
    /// The code is `store_contention`, with the cause `lost_race`, for a
    /// read view and an ordinary read alike. `namespace_id` is the
    /// namespace whose manifest was replaced, which can differ from the
    /// namespace that the read started from, such as a merge's fork.
    #[error(
        "manifest `{replaced_manifest_no}` was replaced by manifest `{current_manifest_no}`, and it or a segment it lists is missing"
    )]
    ManifestReplaced {
        namespace_id: NamespaceId,
        replaced_manifest_no: ManifestNo,
        current_manifest_no: ManifestNo,
    },
    // The message is the failed operation's own, and `details()` reports the index.
    #[error("{error}")]
    FailedOperation {
        operation_index: u32,
        error: Box<CoreError>,
    },
    // The message is the failed precondition's own, and `details()` reports the index.
    #[error("{error}")]
    FailedPrecondition {
        precondition_index: u32,
        error: Box<CoreError>,
    },
}

/// Failures while loading a bounded manifest-plus-tail metadata projection.
///
/// These variants name durable/control failure cases without implying that a
/// full namespace state was reconstructed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MetadataProjectionLoadError {
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error(transparent)]
    WalTailLoad(#[from] WalTailLoadError),
    #[error(transparent)]
    ManifestLoad(#[from] ManifestLoadError),
    #[error("WAL object `{object_key}` failed replay: {error}")]
    WalReplay {
        object_key: String,
        error: WalObjectError,
    },
}

impl MetadataProjectionLoadError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::LoadHead(error) => error.code(),
            Self::WalTailLoad(error) => error.code(),
            Self::WalReplay { .. } => ErrorCode::NamespaceCorrupt,
            Self::ManifestLoad(error) => error.code(),
        }
    }
}

impl From<ManifestLoadError> for CoreError {
    fn from(error: ManifestLoadError) -> Self {
        Self::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
    }
}

impl From<VisiblePathError> for CoreError {
    /// Reports a missing path as [`CoreError::PathNotFound`], the one
    /// spelling that every planner and read uses.
    fn from(error: VisiblePathError) -> Self {
        match error {
            VisiblePathError::PathNotFound { absolute_path } => Self::PathNotFound(absolute_path),
            error => Self::VisiblePath(error),
        }
    }
}

impl From<PathError> for CoreError {
    fn from(error: PathError) -> Self {
        Self::InvalidPath(error.invalid_path_input().to_owned())
    }
}

/// Returns the wire code for a failed object-store operation of this kind.
pub fn classify_store_failure(kind: ObjectStoreErrorKind) -> ErrorCode {
    match kind {
        ObjectStoreErrorKind::PermissionDenied => ErrorCode::StoragePermissionDenied,
        ObjectStoreErrorKind::StoredChecksumMissing => ErrorCode::NamespaceCorrupt,
        ObjectStoreErrorKind::NotFound
        | ObjectStoreErrorKind::InvalidRequest
        | ObjectStoreErrorKind::InvalidKey
        | ObjectStoreErrorKind::PreconditionFailed
        | ObjectStoreErrorKind::Unsupported
        | ObjectStoreErrorKind::Configuration
        | ObjectStoreErrorKind::RetryableTransport
        | ObjectStoreErrorKind::Other => ErrorCode::ServerError,
    }
}

impl CoreError {
    /// Builds the core error for a failed object-store operation on
    /// `object_key`.
    ///
    /// An immutable key that contains a different object, or an object
    /// without a stored checksum, breaks a rule of the format, so either
    /// error becomes [`CoreError::NamespaceCorrupt`]. Every other failure
    /// becomes [`CoreError::Store`].
    pub fn store(object_key: impl Into<String>, error: &ObjectStoreError) -> Self {
        match error {
            ObjectStoreError::DifferentObject { object_key: key } => Self::NamespaceCorrupt(
                format!("immutable object `{key}` already exists with different bytes"),
            ),
            ObjectStoreError::StoredChecksumMissing { object_key: key } => Self::NamespaceCorrupt(
                format!("immutable object `{key}` already exists without a stored checksum"),
            ),
            error => Self::Store {
                object_key: object_key.into(),
                message: error.public_message().into_owned(),
                kind: error.kind(),
            },
        }
    }

    pub(crate) fn contention_exhausted(object_key: &str) -> Self {
        Self::StoreContention {
            object_key: object_key.to_owned(),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            CoreError::MetadataProjection(error) => error.code(),
            CoreError::ControlObjectLoad(error) => error.code(),
            CoreError::MaintenanceRequired { .. } => ErrorCode::MaintenanceRequired,
            CoreError::VisiblePath(error) => error.code(),
            CoreError::DurableContent(error) => error.code(),
            CoreError::CommitValidation(error) => error.code(),
            CoreError::Store { kind, .. } => classify_store_failure(*kind),
            CoreError::WalPublish(error) => error.code(),
            CoreError::OutcomeUnknown { .. } => ErrorCode::OutcomeUnknown,
            CoreError::StoreContention { .. } => ErrorCode::StoreContention,
            CoreError::InvalidPath(_)
            | CoreError::RootMutationForbidden
            | CoreError::InvalidCommitRequest(_)
            | CoreError::InvalidCommitField { .. }
            | CoreError::InvalidSnapshotRequest(_)
            | CoreError::InvalidUploadContent(_)
            | CoreError::InvalidCursor(_)
            | CoreError::InvalidEventIndex(_)
            | CoreError::InvalidDiffRange { .. }
            | CoreError::InvalidMerge { .. }
            | CoreError::BatchTooLarge { .. }
            | CoreError::ResumeOffsetOutOfRange { .. }
            | CoreError::ResumePrefixIncomplete { .. }
            | CoreError::ResumedPrefix(_)
            | CoreError::PrefixForRangedRead
            | CoreError::InvalidExtensionBasis { .. } => ErrorCode::InvalidRequest,
            CoreError::RangeNotSatisfiable { .. } => ErrorCode::RangeNotSatisfiable,
            CoreError::SnapshotNotFound { .. } | CoreError::SnapshotManifestMissing { .. } => {
                ErrorCode::SnapshotNotFound
            }
            CoreError::PathNotFound(_) => ErrorCode::PathNotFound,
            CoreError::InodeNotFound(_) => ErrorCode::InodeNotFound,
            CoreError::RevisionNotFound { .. } => ErrorCode::RevisionNotFound,
            CoreError::ContentTooLarge { .. }
            | CoreError::AppendTooLarge { .. }
            | CoreError::InlineContentTooLarge { .. } => ErrorCode::ContentTooLarge,
            CoreError::CommitTooLarge { .. } => ErrorCode::CommitTooLarge,
            CoreError::MergeTooLarge { .. } => ErrorCode::MergeTooLarge,
            CoreError::NamespaceNotFound { .. } => ErrorCode::NamespaceNotFound,
            CoreError::PoolNotFound { .. } => ErrorCode::PoolNotFound,
            CoreError::NamespaceExists { .. } => ErrorCode::NamespaceExists,
            CoreError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            CoreError::Forbidden { .. } => ErrorCode::Forbidden,
            CoreError::StaleHeadPrecondition { .. } => ErrorCode::StaleHead,
            // LoonFS lost its own race on every attempt and published
            // nothing, so the same request can succeed when it's sent again.
            CoreError::ManifestReplaced { .. } | CoreError::HeadMoved(_) => {
                ErrorCode::StoreContention
            }
            CoreError::BindingVersionMismatch { .. } => ErrorCode::BindingVersionMismatch,
            CoreError::CommitIdReuseConflict { .. } => ErrorCode::CommitIdReuseConflict,
            CoreError::MergeRangeConflict { .. } => ErrorCode::MergeRangeConflict,
            CoreError::MergeConflict { .. } => ErrorCode::MergeConflict,
            CoreError::ContentPreparation(_) => ErrorCode::ContentNotPrepared,
            CoreError::CommitQueueFull => ErrorCode::CommitQueueFull,
            CoreError::ServerBusy => ErrorCode::ServerBusy,
            CoreError::WriterSessionClosed { .. } => ErrorCode::WriterSessionClosed,
            CoreError::ShuttingDown => ErrorCode::ShuttingDown,
            CoreError::ContentNotMaterialized { .. } => ErrorCode::ContentNotMaterialized,
            // An over-budget publication stops before its next write, so
            // nothing was published, and the client backs off before it
            // retries, as it does after contention.
            CoreError::MetadataPublicationBudgetExceeded { .. } => ErrorCode::StoreContention,
            // The deployment's read memory cannot fit this namespace's
            // collection live set; an operator raises the limit.
            CoreError::LiveSetExceedsCollectionMemory { .. } => ErrorCode::ServerError,
            // The namespace's segment descriptors don't fit in one manifest
            // document, and a retry doesn't change that.
            CoreError::ManifestTooLarge { .. } => ErrorCode::ServerError,
            // A bug in the server stopped the commit, and the same request
            // would reach the same bug again.
            CoreError::CommitPreparationPanicked(_) => ErrorCode::ServerError,
            CoreError::UploadNotFound { .. } => ErrorCode::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => ErrorCode::UploadAlreadyCompleted,
            CoreError::UploadContentConflict { .. } => ErrorCode::UploadContentConflict,
            CoreError::BelowRetentionFloor { .. } => ErrorCode::BelowRetentionFloor,
            CoreError::ExpectedFile { .. } | CoreError::ExpectedDirectory { .. } => {
                ErrorCode::WrongKind
            }
            CoreError::DestinationExists { .. } => ErrorCode::PathConflict,
            CoreError::WriterFenced(_) => ErrorCode::WriterFenced,
            CoreError::NamespaceCorrupt(_) => ErrorCode::NamespaceCorrupt,
            // Naming which operation or precondition stopped a commit says
            // nothing new about what went wrong, so the code stays the
            // failure's own.
            CoreError::FailedOperation { error, .. }
            | CoreError::FailedPrecondition { error, .. } => error.code(),
        }
    }

    /// Returns the failure that this error reports: the error that a failed
    /// operation or precondition wraps, or this error itself.
    pub fn failure(&self) -> &CoreError {
        match self {
            CoreError::FailedOperation { error, .. }
            | CoreError::FailedPrecondition { error, .. } => error.failure(),
            error => error,
        }
    }

    /// Returns whether the head or the manifest that an operation planned on
    /// is no longer the namespace's current one. Either the caller stated a
    /// head that isn't the head, or another publication changed the namespace
    /// first on every attempt. A runtime that keeps a view of the namespace
    /// drops it after one of these errors, so its next attempt starts from the
    /// store.
    pub fn is_stale_basis(&self) -> bool {
        matches!(
            self.failure(),
            CoreError::StaleHeadPrecondition { .. }
                | CoreError::ManifestReplaced { .. }
                | CoreError::HeadMoved(_)
                | CoreError::WalPublish(WalPublishError::NumberTaken)
        )
    }

    /// Returns whether this error leaves the outcome of a put unknown, so the
    /// put can still be written later at the number it was sent to (format
    /// spec, "Failed and unknown outcomes").
    pub fn is_outcome_unknown(&self) -> bool {
        matches!(
            self.failure(),
            CoreError::OutcomeUnknown { .. }
                | CoreError::WalPublish(WalPublishError::OutcomeUnknown(_))
        )
    }

    /// Returns whether a merge or a replay commit that failed with this error
    /// failed because the namespace that it commits into changed between the
    /// plan and the commit, so that a later plan reads the change.
    ///
    /// The commit's preconditions fail on a stale basis (see
    /// [`Self::is_stale_basis`]), a stale file, attribute, or access revision,
    /// or a stale binding version. The commit fails with
    /// [`CoreError::DestinationExists`] or a binding precondition when another
    /// inode took a name that a create, move, or restore needs, with
    /// [`CoreError::InodeNotFound`] when a deletion hid an inode that the merge
    /// changes or the directory that an inode goes into, and with a restore
    /// error when another writer restored an inode that the merge restores. A
    /// later plan reports a changed row that isn't the fork's own as
    /// `merge_conflict`. The plan checks a subject's rights as the commit
    /// does, so the commit's `InodeNotFound` isn't a subject's missing rights.
    pub fn is_stale_plan(&self) -> bool {
        self.is_stale_basis()
            || matches!(
                self.failure(),
                CoreError::CommitValidation(
                    CommitValidationError::BaseRevisionMismatch { .. }
                        | CommitValidationError::UpdateAttributesBaseRevisionMismatch { .. }
                        | CommitValidationError::UpdateAccessBaseRevisionMismatch { .. }
                        | CommitValidationError::BindingPreconditionMismatch { .. }
                        | CommitValidationError::RestoreDeletedTargetNotDeleted { .. }
                        | CommitValidationError::RestoreDeletedTargetsCurrentCommit { .. }
                        | CommitValidationError::RestoreDeletedSequenceMismatch { .. }
                ) | CoreError::BindingVersionMismatch { .. }
                    | CoreError::DestinationExists { .. }
                    | CoreError::InodeNotFound(_)
            )
    }

    /// Returns this error with the index of the commit operation that failed
    /// with it. `ManifestReplaced` is returned as it is, because it reports
    /// the commit's basis and not the operation, and the commit is published
    /// again on the current manifest.
    pub(crate) fn at_operation(self, operation_index: usize) -> Self {
        let Ok(operation_index) = u32::try_from(operation_index) else {
            return self;
        };
        if matches!(self, Self::ManifestReplaced { .. }) {
            return self;
        }
        Self::FailedOperation {
            operation_index,
            error: Box::new(self),
        }
    }

    /// Returns this error with the index of the commit precondition that
    /// failed with it. `ManifestReplaced` is returned as it is, as
    /// [`Self::at_operation`] returns it.
    pub(crate) fn at_precondition(self, precondition_index: usize) -> Self {
        let Ok(precondition_index) = u32::try_from(precondition_index) else {
            return self;
        };
        if matches!(self, Self::ManifestReplaced { .. }) {
            return self;
        }
        Self::FailedPrecondition {
            precondition_index,
            error: Box::new(self),
        }
    }

    /// Returns a safe message when this error came from the object store.
    pub fn object_store_public_message(&self) -> Option<std::borrow::Cow<'static, str>> {
        match self {
            CoreError::MetadataProjection(error) => metadata_projection_store_message(error),
            CoreError::ControlObjectLoad(error) => control_object_store_message(error),
            CoreError::DurableContent(DurableContentValidationError::Store { message, .. }) => {
                Some(std::borrow::Cow::Owned(message.clone()))
            }
            CoreError::WalPublish(WalPublishError::OutcomeUnknown(message)) => {
                Some(std::borrow::Cow::Owned(message.clone()))
            }
            CoreError::WalPublish(WalPublishError::Write { kind, .. })
            | CoreError::Store { kind, .. } => Some(kind.public_message()),
            CoreError::FailedOperation { error, .. }
            | CoreError::FailedPrecondition { error, .. } => error.object_store_public_message(),
            CoreError::DurableContent(
                DurableContentValidationError::MissingContentObject { .. }
                | DurableContentValidationError::ContentLengthMismatch { .. }
                | DurableContentValidationError::ContentChecksumMismatch { .. }
                | DurableContentValidationError::StoredChecksumMissing { .. },
            )
            | CoreError::WalPublish(WalPublishError::NumberTaken)
            | CoreError::MaintenanceRequired { .. }
            | CoreError::VisiblePath(_)
            | CoreError::CommitValidation(_)
            | CoreError::InvalidPath(_)
            | CoreError::InvalidCommitRequest(_)
            | CoreError::InvalidCommitField { .. }
            | CoreError::PathNotFound(_)
            | CoreError::InodeNotFound(_)
            | CoreError::RevisionNotFound { .. }
            | CoreError::ContentTooLarge { .. }
            | CoreError::CommitTooLarge { .. }
            | CoreError::MergeTooLarge { .. }
            | CoreError::AppendTooLarge { .. }
            | CoreError::InlineContentTooLarge { .. }
            | CoreError::BatchTooLarge { .. }
            | CoreError::ResumeOffsetOutOfRange { .. }
            | CoreError::ResumePrefixIncomplete { .. }
            | CoreError::ResumedPrefix(_)
            | CoreError::PrefixForRangedRead
            | CoreError::InvalidExtensionBasis { .. }
            | CoreError::RangeNotSatisfiable { .. }
            | CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::RootMutationForbidden
            | CoreError::DestinationExists { .. }
            | CoreError::BindingVersionMismatch { .. }
            | CoreError::CommitIdReuseConflict { .. }
            | CoreError::MergeRangeConflict { .. }
            | CoreError::MergeConflict { .. }
            | CoreError::InvalidMerge { .. }
            | CoreError::ContentPreparation(_)
            | CoreError::CommitQueueFull
            | CoreError::ServerBusy
            | CoreError::WriterSessionClosed { .. }
            | CoreError::ShuttingDown
            | CoreError::ContentNotMaterialized { .. }
            | CoreError::HeadMoved(_)
            | CoreError::SnapshotManifestMissing { .. }
            | CoreError::InvalidSnapshotRequest(_)
            | CoreError::SnapshotNotFound { .. }
            | CoreError::MetadataPublicationBudgetExceeded { .. }
            | CoreError::UploadNotFound { .. }
            | CoreError::UploadAlreadyCompleted { .. }
            | CoreError::UploadContentConflict { .. }
            | CoreError::InvalidUploadContent(_)
            | CoreError::InvalidCursor(_)
            | CoreError::InvalidEventIndex(_)
            | CoreError::InvalidDiffRange { .. }
            | CoreError::BelowRetentionFloor { .. }
            | CoreError::NamespaceCorrupt(_)
            | CoreError::WriterFenced(_)
            | CoreError::OutcomeUnknown { .. }
            | CoreError::StoreContention { .. }
            | CoreError::NamespaceNotFound { .. }
            | CoreError::PoolNotFound { .. }
            | CoreError::NamespaceExists { .. }
            | CoreError::NamespaceDeleted { .. }
            | CoreError::Forbidden { .. }
            | CoreError::LiveSetExceedsCollectionMemory { .. }
            | CoreError::ManifestTooLarge { .. }
            | CoreError::CommitPreparationPanicked(_)
            | CoreError::StaleHeadPrecondition { .. }
            | CoreError::ManifestReplaced { .. } => None,
        }
    }

    /// Structured wire details for this error, when the variant has
    /// machine-usable identity (API spec, "Standard error contract"). The
    /// server serializes this beside [`CoreError::code`]; embedded callers
    /// can match the typed variants directly instead.
    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            CoreError::NamespaceNotFound { namespace_id }
            | CoreError::NamespaceDeleted { namespace_id } => Some(ErrorDetails {
                namespace_id: Some(namespace_id.clone()),
                ..ErrorDetails::default()
            }),
            CoreError::Forbidden { inode_id } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                ..ErrorDetails::default()
            }),
            CoreError::WriterFenced(fence) => Some(ErrorDetails {
                fenced_writer_epoch: Some(fence.fenced_epoch),
                active_writer_epoch: Some(fence.active_epoch),
                active_writer: Some(ActiveWriter::from(&fence.active_writer)),
                ..ErrorDetails::default()
            }),
            CoreError::CommitIdReuseConflict {
                commit_id,
                committed,
            } => Some(ErrorDetails {
                commit_id: CommitId::parse(commit_id).ok(),
                committed: committed.clone(),
                ..ErrorDetails::default()
            }),
            CoreError::MergeRangeConflict {
                source_namespace_id,
                expected_after_seq,
                actual_after_seq,
            } => Some(ErrorDetails {
                source_namespace_id: Some(source_namespace_id.clone()),
                expected_after_seq: Some(*expected_after_seq),
                actual_after_seq: Some(*actual_after_seq),
                ..ErrorDetails::default()
            }),
            CoreError::RangeNotSatisfiable { size_bytes, .. } => Some(ErrorDetails {
                size_bytes: Some(*size_bytes),
                ..ErrorDetails::default()
            }),
            CoreError::BelowRetentionFloor {
                below,
                retention_floor_seq,
            } => Some(ErrorDetails {
                retention_floor_seq: Some(*retention_floor_seq),
                ..below.details()
            }),
            CoreError::MergeTooLarge {
                bound,
                split_at_seq,
                ..
            } => Some(ErrorDetails {
                merge_bound: Some(*bound),
                split_at_seq: *split_at_seq,
                ..ErrorDetails::default()
            }),
            CoreError::MergeConflict {
                conflict_count,
                conflict,
            } => Some(ErrorDetails {
                conflict_count: Some(*conflict_count),
                conflict: Some(conflict.as_ref().clone()),
                ..ErrorDetails::default()
            }),
            CoreError::StaleHeadPrecondition { expected, actual } => Some(ErrorDetails {
                expected_head_seq: Some(*expected),
                actual_head_seq: Some(*actual),
                ..ErrorDetails::default()
            }),
            CoreError::BindingVersionMismatch {
                inode_id,
                expected_binding_version,
                actual_binding_version,
            } => Some(ErrorDetails {
                inode_id: Some(*inode_id),
                expected_binding_version: Some(expected_binding_version.clone()),
                actual_binding_version: actual_binding_version.clone(),
                ..ErrorDetails::default()
            }),
            CoreError::SnapshotManifestMissing { .. } => Some(ErrorDetails {
                cause: Some(ErrorCause::ManifestMissing),
                ..ErrorDetails::default()
            }),
            CoreError::MetadataPublicationBudgetExceeded { .. } => Some(ErrorDetails {
                cause: Some(ErrorCause::PublicationBudget),
                ..ErrorDetails::default()
            }),
            CoreError::StoreContention { .. }
            | CoreError::HeadMoved(_)
            | CoreError::ManifestReplaced { .. }
            | CoreError::WalPublish(WalPublishError::NumberTaken) => Some(ErrorDetails {
                cause: Some(ErrorCause::LostRace),
                ..ErrorDetails::default()
            }),
            CoreError::OutcomeUnknown { .. }
            | CoreError::WalPublish(WalPublishError::OutcomeUnknown(_)) => Some(ErrorDetails {
                cause: Some(ErrorCause::Unconfirmed),
                ..ErrorDetails::default()
            }),
            CoreError::CommitValidation(error) => error.details(),
            CoreError::FailedOperation {
                operation_index,
                error,
            } => Some(ErrorDetails {
                operation_index: Some(*operation_index),
                ..error.details().unwrap_or_default()
            }),
            CoreError::FailedPrecondition {
                precondition_index,
                error,
            } => Some(ErrorDetails {
                precondition_index: Some(*precondition_index),
                ..error.details().unwrap_or_default()
            }),
            _ => None,
        }
    }
}

/// The position in a namespace's history that fell below a retention floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryPosition {
    /// The `after_seq` cursor of a change feed read.
    FeedCursor { after_seq: CommitSeq },
    /// The `from_seq` of a diff.
    DiffStart { from_seq: CommitSeq },
    /// The sequence of a read view.
    ReadSeq { read_seq: CommitSeq },
    /// A merge's fork point, below the floor of the namespace being merged
    /// into, so a base compaction might have removed the fork's previous
    /// merge from the merge index.
    ForkPoint { fork_point_seq: CommitSeq },
    /// The start of a merge's range, below the fork's own floor, so the
    /// fork's change over the range can't be computed.
    RangeStart {
        source_namespace_id: NamespaceId,
        source_after_seq: CommitSeq,
    },
}

impl HistoryPosition {
    /// Returns the error details that identify the position. A read view's
    /// sequence is not reported.
    fn details(&self) -> ErrorDetails {
        match self {
            Self::FeedCursor { after_seq } => ErrorDetails {
                after_seq: Some(*after_seq),
                ..ErrorDetails::default()
            },
            Self::DiffStart { from_seq } => ErrorDetails {
                from_seq: Some(*from_seq),
                ..ErrorDetails::default()
            },
            Self::ReadSeq { .. } => ErrorDetails::default(),
            Self::ForkPoint { fork_point_seq } => ErrorDetails {
                fork_point_seq: Some(*fork_point_seq),
                ..ErrorDetails::default()
            },
            Self::RangeStart {
                source_namespace_id,
                source_after_seq,
            } => ErrorDetails {
                source_namespace_id: Some(source_namespace_id.clone()),
                source_after_seq: Some(*source_after_seq),
                ..ErrorDetails::default()
            },
        }
    }

    /// Returns the clause that ends the error message after the floor.
    fn consequence(&self) -> &'static str {
        match self {
            Self::FeedCursor { .. } | Self::DiffStart { .. } | Self::ReadSeq { .. } => "",
            Self::ForkPoint { .. } | Self::RangeStart { .. } => ", so the fork can't be merged",
        }
    }
}

impl std::fmt::Display for HistoryPosition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FeedCursor { after_seq } => {
                write!(formatter, "change feed cursor `{after_seq}`")
            }
            Self::DiffStart { from_seq } => write!(formatter, "diff start `{from_seq}`"),
            Self::ReadSeq { read_seq } => write!(formatter, "read sequence `{read_seq}`"),
            Self::ForkPoint { fork_point_seq } => {
                write!(formatter, "fork point `{fork_point_seq}`")
            }
            Self::RangeStart {
                source_namespace_id,
                source_after_seq,
            } => write!(
                formatter,
                "the merge range's start `{source_after_seq}` in fork `{source_namespace_id}`"
            ),
        }
    }
}

fn metadata_projection_store_message(
    error: &MetadataProjectionLoadError,
) -> Option<std::borrow::Cow<'static, str>> {
    match error {
        MetadataProjectionLoadError::LoadHead(error) => control_object_store_message(error),
        MetadataProjectionLoadError::WalTailLoad(WalTailLoadError::ReadWal { message, .. })
        | MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadManifest { message, .. }
            | ManifestLoadError::ReadSegment { message, .. },
        ) => Some(std::borrow::Cow::Owned(message.clone())),
        _ => None,
    }
}

fn control_object_store_message(
    error: &ControlObjectLoadError,
) -> Option<std::borrow::Cow<'static, str>> {
    match error {
        ControlObjectLoadError::Store { kind, .. } => Some(kind.public_message()),
        _ => None,
    }
}

/// Describes a writer fencing event: the displaced epoch, the active epoch,
/// and the active writer's writer block.
///
/// Epochs identify the competing sessions. Writer labels may be shared by
/// multiple processes, so the acquisition timestamp helps distinguish them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterFence {
    /// Epoch of the fenced session.
    pub fenced_epoch: WriterEpoch,
    /// Epoch that owns the namespace now.
    pub active_epoch: WriterEpoch,
    /// Writer label and acquisition time that the claim of the active epoch
    /// recorded. Every manifest of a claimed epoch includes them, because a
    /// claim writes them and every later manifest copies them.
    pub active_writer: WriterBlock,
}

fn destination_exists_message(path: &str, existing_display_name: Option<&str>) -> String {
    let typed_leaf = path.rsplit('/').next().unwrap_or(path);
    match existing_display_name {
        Some(existing) if existing != typed_leaf => format!(
            "destination already exists at `{path}` (stored as `{existing}`; sibling names \
             collide after Unicode NFC normalization and case folding)"
        ),
        _ => format!("destination already exists at `{path}`"),
    }
}

impl std::fmt::Display for WriterFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "epoch {} was fenced by epoch {} (writer `{}`, acquired at {} ms)",
            self.fenced_epoch,
            self.active_epoch,
            self.active_writer.writer_id,
            self.active_writer.acquired_at_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classify_store_failure, ActiveWriter, CommitValidationError, CommittedReceipt, CoreError,
        ErrorCode, ErrorKind, ManifestLoadError, MetadataProjectionLoadError, ObjectStoreErrorKind,
        WriterBlock, WriterFence,
    };
    use crate::commit::engine::ContentPreparationError;
    use crate::control_object::ControlObjectLoadError;
    use crate::storage::content_admission::ContentTokenError;
    use loonfs_objectstore::ObjectStoreError;
    use loonfs_types::{CommitId, CommitSeq, InodeId, NamespaceId, RevisionNo, WriterEpoch};
    use std::assert_matches;

    #[test]
    fn core_error_exposes_public_kind_and_detailed_code() {
        let error = CoreError::NamespaceExists {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        };
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(error.code(), ErrorCode::NamespaceExists);
        assert_eq!(error.code().as_str(), "namespace_exists");
        assert!(error.to_string().contains("already exists"));

        let content_id = loonfs_types::ContentId::generate();
        let error = CoreError::ContentPreparation(ContentPreparationError::ContentNotPrepared {
            content_id: content_id.clone(),
        });
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
        assert!(error.to_string().contains(content_id.as_str()));
    }

    #[test]
    fn rejected_content_token_maps_to_content_not_prepared() {
        let content_id = loonfs_types::ContentId::generate();
        let error = CoreError::from(ContentPreparationError::ContentToken(vec![(
            content_id.clone(),
            ContentTokenError::Expired,
        )]));

        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
        // The caller has to know which token to mint again, so the ref it
        // was supplied for travels with the reason.
        assert!(error.to_string().contains(content_id.as_str()));
        assert!(error.to_string().contains("expired"));
    }

    #[test]
    fn identity_bearing_errors_expose_structured_wire_details() {
        let fenced = CoreError::WriterFenced(WriterFence {
            fenced_epoch: WriterEpoch(3),
            active_epoch: WriterEpoch(4),
            active_writer: WriterBlock {
                writer_id: loonfs_types::WriterId::parse("writer-b").expect("writer id"),
                acquired_at_ms: 2_000,
            },
        });
        let details = fenced.details().expect("fence details");
        assert_eq!(details.fenced_writer_epoch, Some(WriterEpoch(3)));
        assert_eq!(details.active_writer_epoch, Some(WriterEpoch(4)));
        assert_eq!(
            details.active_writer,
            Some(ActiveWriter {
                active_writer_id: loonfs_types::WriterId::parse("writer-b").expect("writer id"),
                active_acquired_at_ms: 2_000,
            })
        );

        // A conflict decided against a durable receipt names where the
        // commit id was committed and what was committed there, which is what a retry
        // reads back and proves itself against.
        let receipt = CommittedReceipt {
            committed_seq: CommitSeq(9),
            committed_fingerprint: loonfs_test_support::ids::commit_fingerprint("abc"),
        };
        let reuse = CoreError::CommitIdReuseConflict {
            commit_id: "retry-key-1".to_owned(),
            committed: Some(receipt.clone()),
        };
        let details = reuse.details().expect("reuse details");
        assert_eq!(
            details.commit_id,
            Some(CommitId::parse("retry-key-1").expect("valid commit id"))
        );
        assert_eq!(details.committed, Some(receipt));

        // A conflict between two live claims has no commit to name,
        // so it carries neither half of the receipt.
        let contended = CoreError::CommitIdReuseConflict {
            commit_id: "retry-key-1".to_owned(),
            committed: None,
        };
        let details = contended.details().expect("reuse details");
        assert_eq!(details.committed, None);

        let stale = CoreError::CommitValidation(CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(7),
            expected: RevisionNo(2),
            actual: Some(RevisionNo(5)),
        });
        let details = stale.details().expect("stale-revision details");
        assert_eq!(details.inode_id, Some(InodeId(7)));
        assert_eq!(details.expected_revision_no, Some(RevisionNo(2)));
        assert_eq!(details.actual_revision_no, Some(RevisionNo(5)));
        let message = stale.to_string();
        assert!(message.contains("revision 2"), "{message}");
        assert!(message.contains("revision 5"), "{message}");
        assert!(!message.contains("Some("), "{message}");

        // A file with no revision at all reads as a sentence rather than
        // printing the absent value.
        let unversioned =
            CoreError::CommitValidation(CommitValidationError::BaseRevisionMismatch {
                inode_id: InodeId(7),
                expected: RevisionNo(2),
                actual: None,
            });
        let message = unversioned.to_string();
        assert!(message.contains("revision 2"), "{message}");
        assert!(!message.contains("None"), "{message}");
        assert_eq!(
            unversioned
                .details()
                .expect("stale-revision details")
                .actual_revision_no,
            None
        );

        // A refused `expected_head_seq` carries both sequences, so a caller
        // that still means to delete knows what to retry against.
        let precondition = CoreError::StaleHeadPrecondition {
            expected: CommitSeq(41),
            actual: CommitSeq(45),
        };
        assert_eq!(precondition.code(), ErrorCode::StaleHead);
        assert_eq!(
            precondition.to_string(),
            "expected head sequence 41, found 45"
        );
        let details = precondition.details().expect("head-sequence details");
        assert_eq!(details.expected_head_seq, Some(CommitSeq(41)));
        assert_eq!(details.actual_head_seq, Some(CommitSeq(45)));

        // Errors without machine-usable identity stay detail-free.
        assert!(CoreError::NamespaceCorrupt("boom".to_owned())
            .details()
            .is_none());
    }

    /// Checks that each cause that shares a code with another cause is
    /// reported in the details, and that the code is the one whose remedy
    /// fits the cause.
    #[test]
    fn errors_that_share_a_code_report_their_cause() {
        use crate::wal::WalPublishError;
        use loonfs_types::{ErrorCause, SnapshotId};

        let snapshot_id = SnapshotId::parse("snp_0000000000000g000000000002").expect("snapshot id");
        let cases = [
            (
                CoreError::SnapshotManifestMissing {
                    snapshot_id: snapshot_id.clone(),
                    object_key: "namespaces/demo/manifests/1".to_owned(),
                },
                ErrorCode::SnapshotNotFound,
                Some(ErrorCause::ManifestMissing),
            ),
            (
                CoreError::SnapshotNotFound { snapshot_id },
                ErrorCode::SnapshotNotFound,
                None,
            ),
            (
                CoreError::MetadataPublicationBudgetExceeded {
                    elapsed_ms: 31_000,
                    budget_ms: 30_000,
                },
                ErrorCode::StoreContention,
                Some(ErrorCause::PublicationBudget),
            ),
            (
                CoreError::OutcomeUnknown {
                    object_key: "namespaces/demo/manifests/2".to_owned(),
                    message: "timed out".to_owned(),
                },
                ErrorCode::OutcomeUnknown,
                Some(ErrorCause::Unconfirmed),
            ),
            (
                CoreError::WalPublish(WalPublishError::OutcomeUnknown("timed out".to_owned())),
                ErrorCode::OutcomeUnknown,
                Some(ErrorCause::Unconfirmed),
            ),
            (
                CoreError::HeadMoved("the manifest was replaced".to_owned()),
                ErrorCode::StoreContention,
                Some(ErrorCause::LostRace),
            ),
            (
                CoreError::WalPublish(WalPublishError::NumberTaken),
                ErrorCode::StoreContention,
                Some(ErrorCause::LostRace),
            ),
            (
                CoreError::ManifestReplaced {
                    namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                    replaced_manifest_no: loonfs_types::ManifestNo(1),
                    current_manifest_no: loonfs_types::ManifestNo(2),
                },
                ErrorCode::StoreContention,
                Some(ErrorCause::LostRace),
            ),
            (
                CoreError::contention_exhausted("namespaces/demo/manifests/3"),
                ErrorCode::StoreContention,
                Some(ErrorCause::LostRace),
            ),
        ];
        for (error, code, cause) in cases {
            assert_eq!(error.code(), code, "{error}");
            assert_eq!(
                error.details().and_then(|details| details.cause),
                cause,
                "{error}"
            );
            // A failed operation reports the cause of the failure it wraps.
            let wrapped = error.at_operation(1);
            assert_eq!(wrapped.code(), code);
            assert_eq!(wrapped.details().and_then(|details| details.cause), cause);
        }
    }

    /// Checks that a stale basis is decided from the variant, so a lost race
    /// and a stated head that isn't the head both count, through the index of
    /// a failed precondition, while a compare-and-swap that used every
    /// attempt, which shares the lost race's code, doesn't.
    #[test]
    fn a_stale_basis_is_decided_from_the_variant() {
        use crate::wal::WalPublishError;

        let stated = CoreError::StaleHeadPrecondition {
            expected: CommitSeq(41),
            actual: CommitSeq(45),
        }
        .at_precondition(0);
        let lost = CoreError::WalPublish(WalPublishError::NumberTaken);
        let contended = CoreError::contention_exhausted("namespaces/demo/manifests/2");
        assert_eq!(lost.code(), contended.code());
        for (error, stale) in [(stated, true), (lost, true), (contended, false)] {
            assert_eq!(error.is_stale_basis(), stale, "{error:?}");
            assert_eq!(error.is_stale_plan(), stale, "{error:?}");
        }
        let revision = CoreError::CommitValidation(CommitValidationError::BaseRevisionMismatch {
            inode_id: InodeId(7),
            expected: RevisionNo(2),
            actual: Some(RevisionNo(5)),
        })
        .at_operation(0);
        assert!(!revision.is_stale_basis());
        assert!(revision.is_stale_plan());
    }

    #[test]
    fn store_failures_classify_to_their_wire_codes() {
        let denied = ObjectStoreError::PermissionDenied {
            object_key: "namespaces/demo/hint.json".to_owned(),
            message: "AccessDenied: bucket policy".to_owned(),
        };
        let error = CoreError::store("namespaces/demo/hint.json", &denied);
        assert_eq!(error.code(), ErrorCode::StoragePermissionDenied);
        assert_eq!(error.kind(), ErrorKind::StoragePermissionDenied);

        let transport = ObjectStoreError::transport("namespaces/demo/hint.json", "timed out");
        let error = CoreError::store("namespaces/demo/hint.json", &transport);
        assert_eq!(error.code(), ErrorCode::ServerError);

        let invalid_range = ObjectStoreError::InvalidRange {
            object_key: "namespaces/demo/metadata/segment".to_owned(),
        };
        let error = CoreError::store("namespaces/demo/metadata/segment", &invalid_range);
        assert_eq!(error.code(), ErrorCode::ServerError);

        let key = "pools/pool_00000000000000000000000001/content/blob".to_owned();
        for (occupied, message) in [
            (
                ObjectStoreError::DifferentObject {
                    object_key: key.clone(),
                },
                "immutable object `pools/pool_00000000000000000000000001/content/blob` already exists with different bytes",
            ),
            (
                ObjectStoreError::StoredChecksumMissing {
                    object_key: key.clone(),
                },
                "immutable object `pools/pool_00000000000000000000000001/content/blob` already exists without a stored checksum",
            ),
        ] {
            let error = CoreError::store(&key, &occupied);
            assert_eq!(error.code(), ErrorCode::NamespaceCorrupt);
            assert_matches!(error, CoreError::NamespaceCorrupt(text) if text == message);
        }
        assert_eq!(
            classify_store_failure(ObjectStoreErrorKind::StoredChecksumMissing),
            ErrorCode::NamespaceCorrupt
        );

        let segment_read = CoreError::from(MetadataProjectionLoadError::ManifestLoad(
            ManifestLoadError::ReadSegment {
                object_key: "namespaces/demo/metadata/segment".to_owned(),
                message: "permission denied: bucket policy".to_owned(),
                kind: ObjectStoreErrorKind::PermissionDenied,
            },
        ));
        assert_eq!(segment_read.code(), ErrorCode::StoragePermissionDenied);

        let contended = CoreError::contention_exhausted("namespaces/demo/snapshots/p");
        assert_eq!(contended.code(), ErrorCode::StoreContention);
        assert_eq!(contended.kind(), ErrorKind::Unavailable);
        assert!(!contended.code().retryable_without_operator_action());
    }

    #[test]
    fn control_object_permission_failure_survives_every_head_wrapper() {
        let denied = ControlObjectLoadError::Store {
            object_key: "namespaces/demo/hint.json".to_owned(),
            message: "permission denied: bucket policy".to_owned(),
            kind: ObjectStoreErrorKind::PermissionDenied,
        };

        let core_wrappers = [
            CoreError::ControlObjectLoad(denied.clone()),
            CoreError::from(MetadataProjectionLoadError::LoadHead(denied.clone())),
        ];
        for error in core_wrappers {
            assert_eq!(error.code(), ErrorCode::StoragePermissionDenied);
        }
    }
}
