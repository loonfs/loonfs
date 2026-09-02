//! Core error types and their wire-code classification.
//!
//! Core errors must support `Clone` and serialization. Errors from the object
//! store, codecs, and I/O are therefore stored as messages with the relevant
//! object key instead of as `#[source]` chains. The server and CLI use source
//! chains where these constraints do not apply.

use crate::checkpoint::ManifestLoadError;
use crate::commit::{CommitHeadPublishError, CommitValidationError};
use crate::commit_engine::ContentPreparationError;
use crate::control_object::ControlObjectLoadError;
use crate::metadata::VisiblePathError;
use crate::namespace::catalog::NamespaceCatalogLoadError;
use crate::namespace::writer_epoch::WriterEpochAcquireError;
use crate::storage::content::DurableContentValidationError;
use crate::wal::{WalChainLoadError, WalSegmentError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{
    ChangeSeq, CommitId, ErrorDetails, InodeId, InodeKind, NamespaceId, RevisionNo, UploadId,
    WriterEpoch,
};
use loonfs_objectstore::{ImmutableWriteError, ObjectStoreError};
use thiserror::Error;

/// Public error type returned by `loonfs-core`.
///
/// Use [`Error::kind`] for broad caller action and [`Error::code`] for a stable
/// machine-readable reason.
pub use self::CoreError as Error;

/// Internal result alias used by core entry points. Public signatures still
/// expose this as `std::result::Result<T, Error>`.
pub(crate) type Result<T> = std::result::Result<T, Error>;

pub use loonfs_api::{ErrorCode, ErrorKind};
pub use loonfs_objectstore::ObjectStoreErrorClass as StoreFailureClass;

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
    #[error(transparent)]
    MetadataView(#[from] MetadataViewError),
    #[error(transparent)]
    VisiblePath(#[from] VisiblePathError),
    #[error(transparent)]
    DurableContent(#[from] DurableContentValidationError),
    #[error(transparent)]
    WriterEpoch(#[from] WriterEpochAcquireError),
    #[error("commit validation failed: {0}")]
    CommitValidation(#[from] CommitValidationError),
    #[error("WAL build failed: {0}")]
    WalBuild(#[from] WalSegmentError),
    #[error("head publish failed: {0}")]
    HeadPublish(#[from] CommitHeadPublishError),
    #[error("failed to write WAL object `{object_key}`: {message}")]
    WalWrite {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error("invalid commit request: {0}")]
    InvalidCommitRequest(String),
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
    #[error("expected file at `{target}` but found `{kind}`")]
    ExpectedFile { target: String, kind: InodeKind },
    #[error("expected directory at `{target}` but found `{kind}`")]
    ExpectedDirectory { target: String, kind: InodeKind },
    #[error("cannot mutate root path")]
    RootMutationForbidden,
    #[error("{}", destination_exists_message(.path, .existing_display_name.as_deref()))]
    DestinationExists {
        path: String,
        /// Stored name of the existing entry, when available. It is included in the
        /// error when normalization or case folding makes it conflict with the name
        /// supplied by the caller.
        existing_display_name: Option<String>,
    },
    /// The requested binding generation is no longer current.
    #[error("inode `{inode_id}` is no longer bound at the generation the request named")]
    BindingGenerationMismatch { inode_id: InodeId },
    #[error("commit id conflict for `{commit_id}`")]
    CommitIdReuseConflict {
        commit_id: String,
        /// Sequence of the commit that already used this ID, when a durable receipt
        /// exists. This is `None` when two concurrent requests claim the same ID
        /// before either one commits.
        committed_seq: Option<ChangeSeq>,
        /// Fingerprint of the committed mutation. It is present whenever
        /// `committed_seq` is present and lets a retry verify that it represents the
        /// same request.
        committed_fingerprint: Option<String>,
    },
    #[error(transparent)]
    ContentPreparation(#[from] ContentPreparationError),
    #[error("commit queue is full; slow down and retry")]
    CommitQueueFull,
    /// The service is shutting down. New requests are rejected, while requests
    /// accepted earlier may finish.
    #[error("shutting down; new work is not admitted")]
    ShuttingDown,
    #[error("checkpoint unavailable: {0}")]
    CheckpointUnavailable(String),
    #[error("invalid checkpoint request: {0}")]
    InvalidCheckpointRequest(String),
    #[error("snapshot `{snapshot_id}` was not found")]
    SnapshotNotFound {
        snapshot_id: loonfs_api::CheckpointId,
    },
    #[error("snapshot `{snapshot_id}` is gone: {reason}")]
    SnapshotGone {
        snapshot_id: loonfs_api::CheckpointId,
        reason: String,
    },
    #[error(
        "namespace `{namespace_id}` already has its limit of {max_live} live snapshots; \
         release one or wait for a lease to expire"
    )]
    SnapshotQuotaExceeded {
        namespace_id: loonfs_api::NamespaceId,
        max_live: usize,
    },
    #[error(
        "metadata publication budget exceeded after {elapsed_ms}ms (budget {budget_ms}ms); \
         the root was not published"
    )]
    MetadataPublicationBudgetExceeded { elapsed_ms: u64, budget_ms: u64 },
    #[error("invalid gc configuration: {0}")]
    InvalidGcConfig(String),
    #[error("invalid search query: {0}")]
    InvalidQuery(String),
    #[error("the pattern requires no literal bytes and cannot use the index: {0}")]
    QueryUnindexable(String),
    #[error(
        "the grep index trails the head by {behind_commits} commits, past the \
         exhaustive-scan budget; run maintenance or set allow_stale"
    )]
    IndexLagging { behind_commits: u64 },
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
    #[error(
        "change feed cursor `{after_seq}` is older than retention floor `{retention_floor_seq}`"
    )]
    RebootstrapRequired {
        after_seq: ChangeSeq,
        retention_floor_seq: ChangeSeq,
    },
    #[error("path component `{0}` is not a directory")]
    NonDirectoryPathComponent(String),
    #[error("namespace corrupt: {0}")]
    NamespaceCorrupt(String),
    /// This writer session's epoch was superseded. Terminal for the session:
    /// callers surface it without reacquiring.
    #[error("writer session fenced: {0}")]
    WriterFenced(WriterFence),
    #[error("object store error for `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("failed to encode `{object_key}`: {message}")]
    Codec { object_key: String, message: String },
    /// Non-store internal failure (codec, overflow, invariant breach). Same
    /// wire code as [`ErrorCode::ServerError`]; the message is the detail.
    #[error("internal error: {0}")]
    Internal(String),
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    /// A caller-supplied `expected_head_seq` did not match the current head.
    ///
    /// Unlike [`CommitHeadPublishError::StaleHead`], this error reports a failed
    /// explicit precondition. It includes both sequence numbers so the caller can
    /// decide whether to retry. Both errors use the `stale_head` code.
    #[error("expected head sequence {expected}, found {actual}")]
    StaleHeadPrecondition {
        expected: ChangeSeq,
        actual: ChangeSeq,
    },
    /// Identifies the operation that caused a multi-operation request to fail.
    /// The request remains atomic, and the error code is taken from the underlying
    /// failure. Single-operation requests return the underlying error directly.
    #[error("operation {operation_index}: {source}")]
    FailedOperation {
        operation_index: u32,
        source: Box<CoreError>,
    },
}

/// Failures specific to manifest-plus-tail metadata views.
///
/// These are not generic store failures: each variant names the recovery or
/// caller action we expect. Normal reads and publishes must return these
/// errors instead of falling back to a whole-namespace rebuild.
#[derive(Debug, Clone, Error)]
pub enum MetadataViewError {
    #[error("metadata view for namespace `{namespace_id}` requires maintenance: {reason}")]
    MaintenanceRequired {
        namespace_id: NamespaceId,
        reason: String,
    },
    #[error(
        "the cursor was minted at seq `{cursor_seq}`, ahead of the loaded head `{head_seq}`; restart the listing"
    )]
    CursorAheadOfHead {
        cursor_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
}

impl MetadataViewError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::MaintenanceRequired { .. } => ErrorCode::MaintenanceRequired,
            Self::CursorAheadOfHead { .. } => ErrorCode::RebootstrapRequired,
        }
    }
}

/// Failures while loading a bounded manifest-plus-tail metadata projection.
///
/// These variants name durable/control failure cases without implying that a
/// full namespace state was reconstructed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataProjectionLoadError {
    #[error(transparent)]
    LoadHead(#[from] ControlObjectLoadError),
    #[error("missing head etag for `{object_key}`")]
    MissingHeadEtag { object_key: String },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error(
        "namespace head changed during metadata projection load for `{object_key}`: loaded `{loaded_head_etag}`, current `{current_head_etag}`"
    )]
    HeadChangedDuringLoad {
        object_key: String,
        loaded_head_etag: String,
        current_head_etag: String,
    },
    #[error(transparent)]
    WalChainLoad(#[from] WalChainLoadError),
    #[error(transparent)]
    ManifestLoad(#[from] ManifestLoadError),
    #[error("WAL replay failed: {0}")]
    WalReplay(#[from] WalSegmentError),
    #[error(
        "metadata projection head mismatch: expected current head `{expected:?}`, replayed `{actual:?}`"
    )]
    ReplayedHeadMismatch {
        expected: Box<HeadState>,
        actual: Box<HeadState>,
    },
}

impl MetadataProjectionLoadError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            Self::LoadHead(error) => error.code(),
            Self::WalChainLoad(error) => error.code(),
            Self::WalReplay(_) | Self::ReplayedHeadMismatch { .. } => ErrorCode::NamespaceCorrupt,
            Self::ManifestLoad(error) => match error.failure_class() {
                crate::checkpoint::ManifestLoadFailureClass::Corrupt => ErrorCode::NamespaceCorrupt,
                crate::checkpoint::ManifestLoadFailureClass::Store => ErrorCode::ServerError,
            },
            Self::MissingHeadEtag { .. } => ErrorCode::ServerError,
            Self::HeadChangedDuringLoad { .. } => ErrorCode::StaleHead,
        }
    }
}

impl From<NamespaceCatalogLoadError> for MetadataProjectionLoadError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        match value {
            NamespaceCatalogLoadError::LoadHead(error) => Self::LoadHead(error),
        }
    }
}

impl From<NamespaceCatalogLoadError> for CoreError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        Self::MetadataProjection(value.into())
    }
}

impl From<ImmutableWriteError> for CoreError {
    fn from(value: ImmutableWriteError) -> Self {
        let fallback_object_key = value.object_key().to_owned();
        match value {
            ImmutableWriteError::DifferentObject { object_key } => Self::NamespaceCorrupt(format!(
                "immutable object `{object_key}` already exists with different bytes"
            )),
            ImmutableWriteError::Transport { object_key, source } => Self::Store {
                object_key,
                message: source.public_message().into_owned(),
                class: StoreFailureClass::of(&source),
            },
            error => Self::Store {
                object_key: fallback_object_key,
                message: error.to_string(),
                class: StoreFailureClass::Other,
            },
        }
    }
}

pub(crate) fn classify_store_failure(class: StoreFailureClass) -> ErrorCode {
    match class {
        StoreFailureClass::PermissionDenied => ErrorCode::StoragePermissionDenied,
        StoreFailureClass::NotFound
        | StoreFailureClass::InvalidRequest
        | StoreFailureClass::InvalidKey
        | StoreFailureClass::PreconditionFailed
        | StoreFailureClass::StoredChecksumMissing
        | StoreFailureClass::Unsupported
        | StoreFailureClass::Configuration
        | StoreFailureClass::RetryableTransport
        | StoreFailureClass::Other => ErrorCode::ServerError,
    }
}

impl CoreError {
    /// Builds [`CoreError::Store`] for a failed object-store operation on
    /// `object_key`.
    pub(crate) fn store(object_key: impl Into<String>, error: &ObjectStoreError) -> Self {
        Self::Store {
            object_key: object_key.into(),
            message: error.public_message().into_owned(),
            class: StoreFailureClass::of(error),
        }
    }

    pub(crate) fn contention_exhausted(object_key: &str) -> Self {
        Self::Internal(contention_message(object_key))
    }

    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            CoreError::MetadataProjection(error) => error.code(),
            CoreError::ControlObjectLoad(error) => error.code(),
            CoreError::MetadataView(error) => error.code(),
            CoreError::VisiblePath(error) => error.code(),
            CoreError::DurableContent(error) => error.code(),
            CoreError::WriterEpoch(error) => error.code(),
            CoreError::CommitValidation(error) => error.code(),
            CoreError::WalBuild(_) | CoreError::Codec { .. } | CoreError::Internal(_) => {
                ErrorCode::ServerError
            }
            CoreError::WalWrite { class, .. } | CoreError::Store { class, .. } => {
                classify_store_failure(*class)
            }
            CoreError::HeadPublish(error) => error.code(),
            CoreError::InvalidPath(_)
            | CoreError::RootMutationForbidden
            | CoreError::InvalidCommitRequest(_)
            | CoreError::InvalidCheckpointRequest(_)
            | CoreError::InvalidGcConfig(_)
            | CoreError::InvalidQuery(_)
            | CoreError::InvalidUploadContent(_)
            | CoreError::InvalidCursor(_)
            | CoreError::BatchTooLarge { .. }
            | CoreError::ResumeOffsetOutOfRange { .. }
            | CoreError::ResumePrefixIncomplete { .. }
            | CoreError::NonDirectoryPathComponent(_) => ErrorCode::InvalidRequest,
            CoreError::SnapshotNotFound { .. } => ErrorCode::SnapshotNotFound,
            CoreError::SnapshotGone { .. } => ErrorCode::SnapshotGone,
            CoreError::SnapshotQuotaExceeded { .. } => ErrorCode::SnapshotQuotaExceeded,
            CoreError::PathNotFound(_) => ErrorCode::PathNotFound,
            CoreError::InodeNotFound(_) => ErrorCode::InodeNotFound,
            CoreError::RevisionNotFound { .. } => ErrorCode::RevisionNotFound,
            CoreError::ContentTooLarge { .. } => ErrorCode::ContentTooLarge,
            CoreError::NamespaceExists { .. } => ErrorCode::NamespaceExists,
            CoreError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            CoreError::StaleHeadPrecondition { .. } => ErrorCode::StaleHead,
            CoreError::BindingGenerationMismatch { .. } => ErrorCode::BindingGenerationMismatch,
            CoreError::CommitIdReuseConflict { .. } => ErrorCode::CommitIdReuseConflict,
            CoreError::ContentPreparation(_) => ErrorCode::ContentNotPrepared,
            CoreError::CommitQueueFull => ErrorCode::CommitQueueFull,
            CoreError::ShuttingDown => ErrorCode::ShuttingDown,
            // An over-budget publication aborts pre-CAS and is retryable
            // after maintenance, exactly the checkpoint_unavailable contract.
            CoreError::CheckpointUnavailable(_)
            | CoreError::MetadataPublicationBudgetExceeded { .. } => {
                ErrorCode::CheckpointUnavailable
            }
            CoreError::QueryUnindexable(_) => ErrorCode::QueryUnindexable,
            CoreError::IndexLagging { .. } => ErrorCode::IndexLagging,
            CoreError::UploadNotFound { .. } => ErrorCode::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => ErrorCode::UploadAlreadyCompleted,
            CoreError::UploadContentConflict { .. } => ErrorCode::UploadContentConflict,
            CoreError::RebootstrapRequired { .. } => ErrorCode::RebootstrapRequired,
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DestinationExists { .. } => ErrorCode::PathConflict,
            CoreError::WriterFenced(_) => ErrorCode::WriterFenced,
            CoreError::NamespaceCorrupt(_) => ErrorCode::NamespaceCorrupt,
            // Naming which operation stopped a batch says nothing new about
            // what went wrong, so the code stays the failure's own.
            CoreError::FailedOperation { source, .. } => source.code(),
        }
    }

    /// Attributes this failure to the operation at `operation_index` of a
    /// multi-operation request.
    pub(crate) fn at_operation(self, operation_index: usize) -> Self {
        let Ok(operation_index) = u32::try_from(operation_index) else {
            return self;
        };
        Self::FailedOperation {
            operation_index,
            source: Box::new(self),
        }
    }

    pub fn message(&self) -> String {
        self.to_string()
    }

    /// Returns a safe message when this error came from the object store.
    pub fn object_store_public_message(&self) -> Option<std::borrow::Cow<'static, str>> {
        match self {
            CoreError::MetadataProjection(error) => metadata_projection_store_message(error),
            CoreError::ControlObjectLoad(error) => control_object_store_message(error),
            CoreError::DurableContent(DurableContentValidationError::Store { message, .. }) => {
                Some(std::borrow::Cow::Owned(message.clone()))
            }
            CoreError::WriterEpoch(error) => writer_epoch_store_message(error),
            CoreError::HeadPublish(
                CommitHeadPublishError::OutcomeUnknown(message)
                | CommitHeadPublishError::Store { message, .. },
            ) => Some(std::borrow::Cow::Owned(message.clone())),
            CoreError::WalWrite { class, .. } | CoreError::Store { class, .. } => {
                Some(class.public_message())
            }
            CoreError::FailedOperation { source, .. } => source.object_store_public_message(),
            CoreError::DurableContent(
                DurableContentValidationError::InvalidContentRef(_)
                | DurableContentValidationError::MissingContentObject { .. }
                | DurableContentValidationError::ContentLengthMismatch { .. }
                | DurableContentValidationError::ContentChecksumMismatch { .. },
            )
            | CoreError::HeadPublish(
                CommitHeadPublishError::EmptyExpectedHeadEtag
                | CommitHeadPublishError::SegmentDoesNotConnect { .. }
                | CommitHeadPublishError::EmptyWalSegment
                | CommitHeadPublishError::SeqOverflow
                | CommitHeadPublishError::StaleHead
                | CommitHeadPublishError::PublishBudgetExceeded { .. }
                | CommitHeadPublishError::Codec { .. },
            )
            | CoreError::MetadataView(_)
            | CoreError::VisiblePath(_)
            | CoreError::CommitValidation(_)
            | CoreError::WalBuild(_)
            | CoreError::InvalidPath(_)
            | CoreError::InvalidCommitRequest(_)
            | CoreError::PathNotFound(_)
            | CoreError::InodeNotFound(_)
            | CoreError::RevisionNotFound { .. }
            | CoreError::ContentTooLarge { .. }
            | CoreError::BatchTooLarge { .. }
            | CoreError::ResumeOffsetOutOfRange { .. }
            | CoreError::ResumePrefixIncomplete { .. }
            | CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::RootMutationForbidden
            | CoreError::DestinationExists { .. }
            | CoreError::BindingGenerationMismatch { .. }
            | CoreError::CommitIdReuseConflict { .. }
            | CoreError::ContentPreparation(_)
            | CoreError::CommitQueueFull
            | CoreError::ShuttingDown
            | CoreError::CheckpointUnavailable(_)
            | CoreError::InvalidCheckpointRequest(_)
            | CoreError::SnapshotNotFound { .. }
            | CoreError::SnapshotGone { .. }
            | CoreError::SnapshotQuotaExceeded { .. }
            | CoreError::MetadataPublicationBudgetExceeded { .. }
            | CoreError::InvalidGcConfig(_)
            | CoreError::InvalidQuery(_)
            | CoreError::QueryUnindexable(_)
            | CoreError::IndexLagging { .. }
            | CoreError::UploadNotFound { .. }
            | CoreError::UploadAlreadyCompleted { .. }
            | CoreError::UploadContentConflict { .. }
            | CoreError::InvalidUploadContent(_)
            | CoreError::InvalidCursor(_)
            | CoreError::RebootstrapRequired { .. }
            | CoreError::NonDirectoryPathComponent(_)
            | CoreError::NamespaceCorrupt(_)
            | CoreError::WriterFenced(_)
            | CoreError::Codec { .. }
            | CoreError::Internal(_)
            | CoreError::NamespaceExists { .. }
            | CoreError::NamespaceDeleted { .. }
            | CoreError::StaleHeadPrecondition { .. } => None,
            #[cfg(any(test, feature = "test-support"))]
            CoreError::DurableContent(DurableContentValidationError::ContentStoreMismatch {
                ..
            }) => None,
        }
    }

    /// Structured wire details for this error, when the variant carries
    /// machine-usable identity (API spec, "Standard error contract"). The
    /// server serializes this beside [`CoreError::code`]; embedded callers
    /// can match the typed variants directly instead.
    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            CoreError::WriterFenced(fence) => Some(ErrorDetails {
                fenced_writer_epoch: Some(fence.fenced_epoch),
                active_writer_epoch: Some(fence.active_epoch),
                active_writer: fence.active_writer.clone(),
                active_acquired_at_ms: fence.active_acquired_at_ms,
                ..ErrorDetails::default()
            }),
            CoreError::CommitIdReuseConflict {
                commit_id,
                committed_seq,
                committed_fingerprint,
            } => Some(ErrorDetails {
                commit_id: CommitId::parse(commit_id).ok(),
                committed_seq: *committed_seq,
                committed_fingerprint: committed_fingerprint.clone(),
                ..ErrorDetails::default()
            }),
            CoreError::RebootstrapRequired {
                after_seq,
                retention_floor_seq,
            } => Some(ErrorDetails {
                after_seq: Some(*after_seq),
                retention_floor_seq: Some(*retention_floor_seq),
                ..ErrorDetails::default()
            }),
            CoreError::StaleHeadPrecondition { expected, actual } => Some(ErrorDetails {
                expected_head_seq: Some(*expected),
                actual_head_seq: Some(*actual),
                ..ErrorDetails::default()
            }),
            CoreError::CommitValidation(error) => error.details(),
            CoreError::FailedOperation {
                operation_index,
                source,
            } => Some(ErrorDetails {
                operation_index: Some(*operation_index),
                ..source.details().unwrap_or_default()
            }),
            _ => None,
        }
    }
}

pub(crate) fn contention_message(object_key: &str) -> String {
    format!(
        "`{object_key}` lost all {} compare-and-swap attempts to contention",
        crate::limits::CONTENTION_RETRY_LIMIT
    )
}

fn metadata_projection_store_message(
    error: &MetadataProjectionLoadError,
) -> Option<std::borrow::Cow<'static, str>> {
    match error {
        MetadataProjectionLoadError::LoadHead(error) => control_object_store_message(error),
        MetadataProjectionLoadError::WalChainLoad(WalChainLoadError::ReadWal {
            message, ..
        })
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
        ControlObjectLoadError::Store { class, .. } => Some(class.public_message()),
        _ => None,
    }
}

fn writer_epoch_store_message(
    error: &WriterEpochAcquireError,
) -> Option<std::borrow::Cow<'static, str>> {
    match error {
        WriterEpochAcquireError::LoadHead(error) => control_object_store_message(error),
        WriterEpochAcquireError::HeadWrite { class, .. } => Some(class.public_message()),
        _ => None,
    }
}

/// Describes a writer fencing event: the displaced epoch, the active epoch,
/// and any available information about the active writer.
///
/// Epochs identify the competing sessions. Writer labels may be shared by
/// multiple processes, so the acquisition timestamp helps distinguish them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterFence {
    /// Epoch the fenced session held.
    pub fenced_epoch: WriterEpoch,
    /// Epoch that owns the namespace now.
    pub active_epoch: WriterEpoch,
    /// Writer label recorded by the winning acquirer, when known.
    pub active_writer: Option<String>,
    /// When the winning acquirer took the epoch, in Unix milliseconds, when
    /// known.
    pub active_acquired_at_ms: Option<u64>,
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
            "epoch {} was fenced by epoch {}",
            self.fenced_epoch, self.active_epoch
        )?;
        // These fields normally appear together, but format a useful message when
        // only one is available.
        match (self.active_writer.as_deref(), self.active_acquired_at_ms) {
            (Some(writer), Some(acquired_at_ms)) => {
                write!(f, " (writer `{writer}`, acquired at {acquired_at_ms} ms)")
            }
            (Some(writer), None) => write!(f, " (writer `{writer}`)"),
            (None, Some(acquired_at_ms)) => write!(f, " (acquired at {acquired_at_ms} ms)"),
            (None, None) => Ok(()),
        }
    }
}

impl From<crate::control_update::ControlUpdateError> for CoreError {
    fn from(value: crate::control_update::ControlUpdateError) -> Self {
        use crate::control_update::ControlUpdateError;
        match value {
            ControlUpdateError::LoadHead(error) => CoreError::ControlObjectLoad(error),
            ControlUpdateError::Store {
                object_key,
                message,
                class,
            } => CoreError::Store {
                object_key,
                message,
                class,
            },
            // Codec and retry exhaustion are non-store control-plane
            // failures. Preserve their prefixed detail as an internal error.
            other => CoreError::Internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitValidationError, CoreError, ErrorCode, ErrorKind, MetadataViewError,
        StoreFailureClass, WriterFence,
    };
    use crate::commit_engine::ContentPreparationError;
    use crate::control_object::ControlObjectLoadError;
    use crate::control_update::ControlUpdateError;
    use crate::namespace::catalog::NamespaceCatalogLoadError;
    use crate::namespace::writer_epoch::WriterEpochAcquireError;
    use crate::namespace::BootstrapNamespaceError;
    use crate::storage::content_admission::ContentTokenError;
    use loonfs_api::{ChangeSeq, CommitId, InodeId, NamespaceId, RevisionNo, WriterEpoch};
    use loonfs_objectstore::ObjectStoreError;

    #[test]
    fn public_error_kind_groups_detailed_codes() {
        assert_eq!(ErrorCode::InvalidRequest.kind(), ErrorKind::InvalidRequest);
        assert_eq!(ErrorCode::PathNotFound.kind(), ErrorKind::NotFound);
        assert_eq!(ErrorCode::NamespaceDeleted.kind(), ErrorKind::Gone);
        assert_eq!(ErrorCode::NamespaceExists.kind(), ErrorKind::AlreadyExists);
        // Precondition failures are 409 resource-state conflicts in v0
        // (api.md, "Standard error contract"), so the kind is Conflict.
        assert_eq!(ErrorCode::StaleRevision.kind(), ErrorKind::Conflict);
        assert_eq!(ErrorCode::ContentNotPrepared.kind(), ErrorKind::Conflict);
        assert_eq!(ErrorCode::CommitQueueFull.kind(), ErrorKind::Unavailable);
        assert_eq!(
            ErrorCode::MaintenanceRequired.kind(),
            ErrorKind::Unavailable
        );
        assert_eq!(
            ErrorCode::CommitOutcomeUnknown.kind(),
            ErrorKind::OutcomeUnknown
        );
        assert_eq!(
            ErrorCode::NamespaceCorrupt.kind(),
            ErrorKind::DataCorruption
        );
        assert_eq!(ErrorCode::ServerError.kind(), ErrorKind::Internal);
    }

    #[test]
    fn core_error_exposes_public_kind_and_detailed_code() {
        let error = CoreError::NamespaceExists {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        };
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(error.code(), ErrorCode::NamespaceExists);
        assert_eq!(error.code().as_str(), "namespace_exists");
        assert!(error.message().contains("already exists"));

        let content_id = loonfs_api::ContentId::generate();
        let error = CoreError::ContentPreparation(ContentPreparationError::ContentNotPrepared {
            content_id: content_id.clone(),
        });
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
        assert!(error.message().contains(content_id.as_str()));
    }

    #[test]
    fn rejected_content_token_maps_to_content_not_prepared() {
        let content_id = loonfs_api::ContentId::generate();
        let error = CoreError::from(ContentPreparationError::ContentToken(vec![(
            content_id.clone(),
            ContentTokenError::Expired,
        )]));

        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
        // The caller has to know which token to mint again, so the ref it
        // was supplied for travels with the reason.
        assert!(error.message().contains(content_id.as_str()));
        assert!(error.message().contains("expired"));
    }

    #[test]
    fn metadata_view_errors_map_to_actionable_public_codes() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let head_seq = ChangeSeq(3);

        let cases = [
            (
                MetadataViewError::MaintenanceRequired {
                    namespace_id: namespace_id.clone(),
                    reason: "retention progress is missing".to_owned(),
                },
                ErrorCode::MaintenanceRequired,
            ),
            (
                MetadataViewError::CursorAheadOfHead {
                    cursor_seq: ChangeSeq(1),
                    head_seq,
                },
                ErrorCode::RebootstrapRequired,
            ),
        ];

        for (metadata_error, code) in cases {
            let error = CoreError::from(metadata_error);
            assert_eq!(error.code(), code);
        }
    }

    #[test]
    fn identity_bearing_errors_expose_structured_wire_details() {
        let fenced = CoreError::WriterFenced(WriterFence {
            fenced_epoch: WriterEpoch(3),
            active_epoch: WriterEpoch(4),
            active_writer: Some("writer-b".to_owned()),
            active_acquired_at_ms: Some(2_000),
        });
        let details = fenced.details().expect("fence details");
        assert_eq!(details.fenced_writer_epoch, Some(WriterEpoch(3)));
        assert_eq!(details.active_writer_epoch, Some(WriterEpoch(4)));
        assert_eq!(details.active_writer.as_deref(), Some("writer-b"));
        assert_eq!(details.active_acquired_at_ms, Some(2_000));

        let anonymous = CoreError::WriterFenced(WriterFence {
            fenced_epoch: WriterEpoch(3),
            active_epoch: WriterEpoch(4),
            active_writer: None,
            active_acquired_at_ms: None,
        });
        let details = anonymous.details().expect("fence details");
        assert_eq!(details.fenced_writer_epoch, Some(WriterEpoch(3)));
        assert_eq!(details.active_writer_epoch, Some(WriterEpoch(4)));
        assert_eq!(details.active_writer, None);
        assert_eq!(details.active_acquired_at_ms, None);

        // A conflict decided against a durable receipt names where the
        // commit id landed and what landed there, which is what a retry
        // reads back and proves itself against.
        let reuse = CoreError::CommitIdReuseConflict {
            commit_id: "retry-key-1".to_owned(),
            committed_seq: Some(ChangeSeq(9)),
            committed_fingerprint: Some("v1:sha256:abc".to_owned()),
        };
        let details = reuse.details().expect("reuse details");
        assert_eq!(
            details.commit_id,
            Some(CommitId::parse("retry-key-1").expect("valid commit id"))
        );
        assert_eq!(details.committed_seq, Some(ChangeSeq(9)));
        assert_eq!(
            details.committed_fingerprint.as_deref(),
            Some("v1:sha256:abc")
        );

        // A conflict between two live claims has no landed commit to name,
        // so it carries neither half of the receipt.
        let contended = CoreError::CommitIdReuseConflict {
            commit_id: "retry-key-1".to_owned(),
            committed_seq: None,
            committed_fingerprint: None,
        };
        let details = contended.details().expect("reuse details");
        assert_eq!(details.committed_seq, None);
        assert_eq!(details.committed_fingerprint, None);

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
            expected: ChangeSeq(41),
            actual: ChangeSeq(45),
        };
        assert_eq!(precondition.code(), ErrorCode::StaleHead);
        assert_eq!(
            precondition.to_string(),
            "expected head sequence 41, found 45"
        );
        let details = precondition.details().expect("head-sequence details");
        assert_eq!(details.expected_head_seq, Some(ChangeSeq(41)));
        assert_eq!(details.actual_head_seq, Some(ChangeSeq(45)));

        // Errors without machine-usable identity stay detail-free.
        assert!(CoreError::Internal("boom".to_owned()).details().is_none());
    }

    #[test]
    fn store_failures_classify_to_their_wire_codes() {
        let denied = ObjectStoreError::PermissionDenied {
            object_key: "namespaces/demo/wal/head.json".to_owned(),
            message: "AccessDenied: bucket policy".to_owned(),
        };
        let error = CoreError::store("namespaces/demo/wal/head.json", &denied);
        assert_eq!(error.code(), ErrorCode::StoragePermissionDenied);
        assert_eq!(error.kind(), ErrorKind::StoragePermissionDenied);

        let transport = ObjectStoreError::transport("namespaces/demo/wal/head.json", "timed out");
        let error = CoreError::store("namespaces/demo/wal/head.json", &transport);
        assert_eq!(error.code(), ErrorCode::ServerError);

        let invalid_range = ObjectStoreError::InvalidRange {
            object_key: "namespaces/demo/metadata/segment".to_owned(),
        };
        let error = CoreError::store("namespaces/demo/metadata/segment", &invalid_range);
        assert_eq!(error.code(), ErrorCode::ServerError);
    }

    #[test]
    fn control_object_permission_failure_survives_every_head_wrapper() {
        let denied = ControlObjectLoadError::Store {
            object_key: "namespaces/demo/wal/head.json".to_owned(),
            message: "permission denied: bucket policy".to_owned(),
            class: StoreFailureClass::PermissionDenied,
        };

        let core_wrappers = [
            CoreError::ControlObjectLoad(denied.clone()),
            CoreError::WriterEpoch(WriterEpochAcquireError::LoadHead(denied.clone())),
            CoreError::from(NamespaceCatalogLoadError::LoadHead(denied.clone())),
            CoreError::from(ControlUpdateError::LoadHead(denied.clone())),
        ];
        for error in core_wrappers {
            assert_eq!(error.code(), ErrorCode::StoragePermissionDenied);
        }
        assert_eq!(
            BootstrapNamespaceError::Head(denied).code(),
            ErrorCode::StoragePermissionDenied
        );
    }
}
