//! Core error types and their wire-code classification.
//!
//! Stringification rule: core errors are `Clone`/serde-constrained, so foreign
//! causes (object-store, codec, io) are captured as prefixed message strings
//! next to the object key they are about, not as `#[source]` chains. Crates
//! without those constraints (server, CLI) prefer `#[source]` chains instead.

use crate::checkpoint::ManifestLoadError;
use crate::commit::{CommitConversionError, CommitHeadPublishError, CommitValidationError};
use crate::metadata::{MetadataApplyError, VisiblePathError};
use crate::namespace::catalog::NamespaceCatalogLoadError;
use crate::namespace::control::ControlObjectLoadError;
use crate::namespace::writer_epoch::WriterEpochAcquireError;
use crate::storage::content::{DurableContentValidationError, ImmutableObjectWriteError};
use crate::wal::{WalBuildError, WalChainLoadError, WalReplayError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{
    ChangeSeq, CommitIdValidationError, GeneratedIdValidationError, InodeId, InodeKind,
    NamespaceId, NamespaceIdValidationError, UploadId,
};
use loonfs_objectstore::ObjectStoreError;
use thiserror::Error;

/// Public error type returned by `loonfs-core`.
///
/// Use [`Error::kind`] for broad caller action and [`Error::code`] for a stable
/// machine-readable reason.
pub use self::CoreError as Error;

/// Result type used by `loonfs-core` entrypoints.
pub type Result<T> = std::result::Result<T, Error>;

pub use loonfs_api::{ErrorCode, ErrorKind};

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
    MetadataView(#[from] MetadataViewError),
    #[error(transparent)]
    VisiblePath(#[from] VisiblePathError),
    #[error(transparent)]
    DurableContent(#[from] DurableContentValidationError),
    #[error(transparent)]
    WriterEpoch(#[from] WriterEpochAcquireError),
    #[error("commit validation failed: {0}")]
    CommitValidation(#[from] CommitValidationError),
    #[error("wal build failed: {0}")]
    WalBuild(#[from] WalBuildError),
    #[error("metadata apply failed: {0}")]
    MetadataApply(#[from] MetadataApplyError),
    #[error("head publish failed: {0}")]
    HeadPublish(#[from] CommitHeadPublishError),
    #[error("failed to write wal object `{object_key}`: {message}")]
    WalWrite { object_key: String, message: String },
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error(transparent)]
    InvalidNamespaceId(#[from] NamespaceIdValidationError),
    #[error(transparent)]
    InvalidCommitId(#[from] CommitIdValidationError),
    #[error(transparent)]
    InvalidUploadId(#[from] GeneratedIdValidationError),
    #[error("path not found `{0}`")]
    PathNotFound(String),
    #[error("revision `{revision_no}` not found for inode `{inode_id}`")]
    RevisionNotFound {
        inode_id: InodeId,
        revision_no: loonfs_api::RevisionNo,
    },
    #[error("expected file at `{path}` but found `{kind}`")]
    ExpectedFile { path: String, kind: InodeKind },
    #[error("expected directory at `{path}` but found `{kind}`")]
    ExpectedDirectory { path: String, kind: InodeKind },
    #[error("directory not empty `{0}`")]
    DirectoryNotEmpty(String),
    #[error("cannot mutate root path")]
    RootMutationForbidden,
    #[error("destination already exists at `{0}`")]
    DestinationExists(String),
    #[error("commit id conflict for `{0}`")]
    CommitIdReuseConflict(String),
    #[error("commit queue is full; slow down and retry")]
    CommitQueueFull,
    #[error("checkpoint unavailable: {0}")]
    CheckpointUnavailable(String),
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
    #[error(
        "path `{path}` is covered by subtree tombstone rooted at inode `{root_inode_id}` from seq `{tombstone_seq}`"
    )]
    TombstoneConflict {
        path: String,
        root_inode_id: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("path component `{0}` is not a directory")]
    NonDirectoryPathComponent(String),
    #[error("namespace corrupt: {0}")]
    NamespaceCorrupt(String),
    /// This writer session's epoch was superseded. Terminal for the session:
    /// callers surface it without reacquiring.
    #[error("writer session fenced: {0}")]
    WriterFenced(String),
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
    /// Non-store internal failure (codec, overflow, invariant breach). Same
    /// wire code as [`ErrorCode::ServerError`]; the message is the detail.
    #[error("internal error: {0}")]
    Internal(String),
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
}

/// Failures specific to manifest-plus-tail metadata views.
///
/// These are not generic store failures: each variant names the recovery or
/// caller action we expect. Normal reads and publishes must return these
/// errors instead of falling back to a whole-namespace rebuild.
#[derive(Debug, Clone, Error)]
pub enum MetadataViewError {
    #[error("namespace `{namespace_id}` head has no current manifest")]
    MissingManifest { namespace_id: NamespaceId },
    #[error("metadata view for namespace `{namespace_id}` requires maintenance: {reason}")]
    MaintenanceRequired {
        namespace_id: NamespaceId,
        reason: String,
    },
    #[error(
        "the cursor's snapshot (seq `{requested_seq}`) is no longer available (current head `{head_seq}`); restart the listing"
    )]
    SnapshotUnavailable {
        requested_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
    #[error(
        "metadata view only supports the loaded head `{head_seq}`, not historical snapshot `{requested_seq}`"
    )]
    UnsupportedHistoricalRead {
        requested_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
}

/// Failures while loading a bounded manifest-plus-tail metadata projection.
///
/// These variants name durable/control failure cases without implying that a
/// full namespace state was reconstructed.
// Four Load* variants share ControlObjectLoadError; only the head load (the
// dominant path) gets `#[from]`, the others stay explicit conversions.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MetadataProjectionLoadError {
    #[error("failed to load namespace descriptor: {0}")]
    LoadNamespaceDescriptor(ControlObjectLoadError),
    #[error("failed to load content store descriptor: {0}")]
    LoadContentStoreDescriptor(ControlObjectLoadError),
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
    #[error("wal replay failed: {0}")]
    WalReplay(#[from] WalReplayError),
    #[error(
        "metadata projection head mismatch: expected current head `{expected:?}`, replayed `{actual:?}`"
    )]
    ReplayedHeadMismatch {
        expected: Box<HeadState>,
        actual: Box<HeadState>,
    },
}

impl From<NamespaceCatalogLoadError> for MetadataProjectionLoadError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        match value {
            NamespaceCatalogLoadError::LoadNamespaceDescriptor(error) => {
                Self::LoadNamespaceDescriptor(error)
            }
            NamespaceCatalogLoadError::LoadContentStoreDescriptor(error) => {
                Self::LoadContentStoreDescriptor(error)
            }
        }
    }
}

impl From<CommitConversionError> for CoreError {
    fn from(value: CommitConversionError) -> Self {
        match value {
            CommitConversionError::InvalidCommitId(error) => Self::InvalidCommitId(error),
        }
    }
}

impl From<NamespaceCatalogLoadError> for CoreError {
    fn from(value: NamespaceCatalogLoadError) -> Self {
        Self::MetadataProjection(value.into())
    }
}

impl From<ImmutableObjectWriteError> for CoreError {
    fn from(value: ImmutableObjectWriteError) -> Self {
        match value {
            ImmutableObjectWriteError::Store {
                object_key,
                message,
            } => Self::Store {
                object_key,
                message,
            },
        }
    }
}

impl CoreError {
    /// Builds [`CoreError::Store`] for a failed object-store operation on
    /// `object_key`.
    pub(crate) fn store(object_key: impl Into<String>, error: &ObjectStoreError) -> Self {
        Self::Store {
            object_key: object_key.into(),
            message: error.message(),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            CoreError::MetadataProjection(error) => classify_metadata_projection_load_error(error),
            CoreError::MetadataView(error) => classify_metadata_view_error(error),
            CoreError::VisiblePath(error) => classify_visible_path_error(error),
            CoreError::DurableContent(error) => classify_durable_content_error(error),
            CoreError::WriterEpoch(error) => classify_writer_epoch_acquire_error(error),
            CoreError::CommitValidation(error) => classify_commit_validation_error(error),
            CoreError::WalBuild(_)
            | CoreError::MetadataApply(_)
            | CoreError::WalWrite { .. }
            | CoreError::Store { .. }
            | CoreError::Internal(_) => ErrorCode::ServerError,
            CoreError::HeadPublish(error) => classify_head_publish_error(error),
            CoreError::InvalidPath(_) | CoreError::RootMutationForbidden => {
                ErrorCode::InvalidRequest
            }
            CoreError::InvalidNamespaceId(_) => ErrorCode::InvalidRequest,
            CoreError::InvalidCommitId(_) => ErrorCode::InvalidRequest,
            CoreError::InvalidUploadId(_) => ErrorCode::InvalidRequest,
            CoreError::PathNotFound(_) => ErrorCode::PathNotFound,
            CoreError::RevisionNotFound { .. } => ErrorCode::RevisionNotFound,
            CoreError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            CoreError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            CoreError::NamespacePartiallyInitialized { .. } => ErrorCode::NamespacePartial,
            CoreError::CommitIdReuseConflict(_) => ErrorCode::CommitIdReuseConflict,
            CoreError::CommitQueueFull => ErrorCode::CommitQueueFull,
            CoreError::CheckpointUnavailable(_) => ErrorCode::CheckpointUnavailable,
            CoreError::UploadNotFound { .. } => ErrorCode::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => ErrorCode::UploadAlreadyCompleted,
            CoreError::UploadContentConflict { .. } => ErrorCode::UploadContentConflict,
            CoreError::InvalidUploadContent(_) => ErrorCode::InvalidRequest,
            CoreError::InvalidCursor(_) => ErrorCode::InvalidRequest,
            CoreError::RebootstrapRequired { .. } => ErrorCode::RebootstrapRequired,
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DestinationExists(_) => ErrorCode::PathConflict,
            CoreError::DirectoryNotEmpty(_) => ErrorCode::DirectoryNotEmpty,
            CoreError::TombstoneConflict { .. } => ErrorCode::TombstoneConflict,
            CoreError::NonDirectoryPathComponent(_) => ErrorCode::InvalidRequest,
            CoreError::WriterFenced(_) => ErrorCode::WriterFenced,
            CoreError::NamespaceCorrupt(_) => ErrorCode::NamespaceCorrupt,
        }
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

fn classify_metadata_view_error(error: &MetadataViewError) -> ErrorCode {
    match error {
        MetadataViewError::MissingManifest { .. } => ErrorCode::NamespaceCorrupt,
        MetadataViewError::MaintenanceRequired { .. } => ErrorCode::MaintenanceRequired,
        // A well-formed cursor whose snapshot aged out is a state condition,
        // not a malformed request: the client's recovery is to restart the
        // listing, same as a sub-floor change cursor.
        MetadataViewError::SnapshotUnavailable { .. }
        | MetadataViewError::UnsupportedHistoricalRead { .. } => ErrorCode::RebootstrapRequired,
    }
}

fn classify_metadata_projection_load_error(error: &MetadataProjectionLoadError) -> ErrorCode {
    match error {
        MetadataProjectionLoadError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
        MetadataProjectionLoadError::LoadNamespaceDescriptor(error) => {
            classify_control_object_load_error(error)
        }
        MetadataProjectionLoadError::LoadContentStoreDescriptor(error) => match error {
            ControlObjectLoadError::InvalidNamespaceId { .. } => ErrorCode::InvalidRequest,
            ControlObjectLoadError::Store { .. } => ErrorCode::ServerError,
            _ => ErrorCode::NamespaceCorrupt,
        },
        MetadataProjectionLoadError::LoadHead(error) => match error {
            ControlObjectLoadError::MissingObject { .. }
            | ControlObjectLoadError::MissingObjectAfterHead { .. } => ErrorCode::NamespaceCorrupt,
            _ => classify_control_object_load_error(error),
        },
        MetadataProjectionLoadError::WalChainLoad(error) => classify_wal_chain_load_error(error),
        MetadataProjectionLoadError::WalReplay(_)
        | MetadataProjectionLoadError::ReplayedHeadMismatch { .. } => ErrorCode::NamespaceCorrupt,
        MetadataProjectionLoadError::ManifestLoad(error) => match error.failure_class() {
            crate::checkpoint::ManifestLoadFailureClass::Corrupt => ErrorCode::NamespaceCorrupt,
            crate::checkpoint::ManifestLoadFailureClass::Store => ErrorCode::ServerError,
        },
        MetadataProjectionLoadError::MissingHeadEtag { .. } => ErrorCode::ServerError,
        MetadataProjectionLoadError::HeadChangedDuringLoad { .. } => ErrorCode::StaleHead,
    }
}

fn classify_control_object_load_error(error: &ControlObjectLoadError) -> ErrorCode {
    match error {
        ControlObjectLoadError::InvalidNamespaceId { .. } => ErrorCode::InvalidRequest,
        ControlObjectLoadError::MissingObject { .. }
        | ControlObjectLoadError::MissingObjectAfterHead { .. } => ErrorCode::NamespaceNotFound,
        ControlObjectLoadError::RootAheadOfHead { .. } => ErrorCode::StaleHead,
        ControlObjectLoadError::NamespaceMismatch { .. }
        | ControlObjectLoadError::ContentStoreMismatch { .. }
        | ControlObjectLoadError::ChecksumMismatch { .. }
        | ControlObjectLoadError::Codec { .. } => ErrorCode::NamespaceCorrupt,
        ControlObjectLoadError::Store { .. } => ErrorCode::ServerError,
    }
}

fn classify_wal_chain_load_error(error: &WalChainLoadError) -> ErrorCode {
    match error {
        WalChainLoadError::ReadWal { .. } => ErrorCode::ServerError,
        WalChainLoadError::InvalidSeqRange { .. }
        | WalChainLoadError::MissingVisibleTip { .. }
        | WalChainLoadError::TipEndSeqMismatch { .. }
        | WalChainLoadError::MissingWalObject { .. }
        | WalChainLoadError::PointerMismatch { .. }
        | WalChainLoadError::HeadSeqMismatch { .. }
        | WalChainLoadError::CursorNotCovered { .. }
        | WalChainLoadError::Replay(_) => ErrorCode::NamespaceCorrupt,
    }
}

fn classify_visible_path_error(error: &VisiblePathError) -> ErrorCode {
    match error {
        VisiblePathError::InvalidAbsolutePath { .. } => ErrorCode::InvalidRequest,
        VisiblePathError::RootMissing => ErrorCode::NamespaceCorrupt,
        VisiblePathError::PathNotFound { .. } => ErrorCode::PathNotFound,
        VisiblePathError::PathComponentNotDirectory { .. } => ErrorCode::PathConflict,
    }
}

fn classify_durable_content_error(error: &DurableContentValidationError) -> ErrorCode {
    match error {
        DurableContentValidationError::UnsupportedContentRefKind { .. }
        | DurableContentValidationError::InvalidDigest { .. }
        | DurableContentValidationError::MissingContentObject { .. }
        | DurableContentValidationError::ContentLengthMismatch { .. }
        | DurableContentValidationError::ContentDigestMismatch { .. } => {
            ErrorCode::NamespaceCorrupt
        }
        DurableContentValidationError::Store { .. } => ErrorCode::ServerError,
    }
}

impl From<crate::control_update::ControlUpdateError> for CoreError {
    fn from(value: crate::control_update::ControlUpdateError) -> Self {
        use crate::control_update::ControlUpdateError;
        match value {
            ControlUpdateError::LoadHead(error) => {
                CoreError::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
            }
            // The remaining variants (missing etag, codec, control store error,
            // retries exhausted) are control-plane plumbing failures with no
            // single object key in scope at this blanket conversion. They share
            // the ServerError wire code with `Store`; keep the detail as a
            // prefixed message.
            other => CoreError::Internal(other.to_string()),
        }
    }
}

fn classify_writer_epoch_acquire_error(error: &WriterEpochAcquireError) -> ErrorCode {
    match error {
        WriterEpochAcquireError::LoadHead(error) => classify_control_object_load_error(error),
        WriterEpochAcquireError::EmptyWriterId
        | WriterEpochAcquireError::EmptyWriterSessionId
        | WriterEpochAcquireError::MissingHeadEtag { .. }
        | WriterEpochAcquireError::WriterEpochOverflow { .. }
        | WriterEpochAcquireError::HeadWrite(_)
        | WriterEpochAcquireError::RetryExhausted { .. } => ErrorCode::ServerError,
    }
}

fn classify_commit_validation_error(error: &CommitValidationError) -> ErrorCode {
    match error {
        CommitValidationError::ReplaceFileBaseRevisionMismatch { .. }
        | CommitValidationError::RestoreRevisionBaseRevisionMismatch { .. } => {
            ErrorCode::StaleRevision
        }
        CommitValidationError::RestoreRevisionSourceRevisionMissing { .. } => {
            ErrorCode::RevisionNotFound
        }
        CommitValidationError::CreateUnderSubtreeTombstone { .. }
        | CommitValidationError::ReplaceFileUnderSubtreeTombstone { .. }
        | CommitValidationError::RestoreRevisionUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteFileCoveredByTombstone { .. }
        | CommitValidationError::RenameInodeUnderSubtreeTombstone { .. }
        | CommitValidationError::RenameTargetParentUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteSubtreeRootCoveredByTombstone { .. } => {
            ErrorCode::TombstoneConflict
        }
        CommitValidationError::CreateChildNameCollision { .. }
        | CommitValidationError::NamePreconditionParentNotDirectory { .. }
        | CommitValidationError::BindingPreconditionMissing { .. }
        | CommitValidationError::BindingPreconditionMismatch { .. }
        | CommitValidationError::CreateParentNotDirectory { .. }
        | CommitValidationError::ReplaceFileInodeNotFile { .. }
        | CommitValidationError::RestoreRevisionInodeNotFile { .. }
        | CommitValidationError::DeleteFileInodeNotFile { .. }
        | CommitValidationError::RenameTargetParentNotDirectory { .. }
        | CommitValidationError::RenameTargetNameCollision { .. }
        | CommitValidationError::DeleteSubtreeRootNotDirectory { .. }
        | CommitValidationError::DirectoryEmptyPreconditionInodeNotDirectory { .. } => {
            ErrorCode::PathConflict
        }
        CommitValidationError::DirectoryEmptyPreconditionNotEmpty { .. } => {
            ErrorCode::DirectoryNotEmpty
        }
        CommitValidationError::CreateParentMissing { .. }
        | CommitValidationError::NamePreconditionParentMissing { .. }
        | CommitValidationError::ReplaceFileInodeMissing { .. }
        | CommitValidationError::RestoreRevisionInodeMissing { .. }
        | CommitValidationError::DeleteFileInodeMissing { .. }
        | CommitValidationError::RenameInodeMissing { .. }
        | CommitValidationError::RenameSourceBindingMissing { .. }
        | CommitValidationError::SourceBindingMissing { .. }
        | CommitValidationError::RenameTargetParentMissing { .. }
        | CommitValidationError::DeleteSubtreeRootMissing { .. }
        | CommitValidationError::DirectoryEmptyPreconditionInodeMissing { .. } => {
            ErrorCode::PathNotFound
        }
        CommitValidationError::RenameWouldCycleDirectory { .. } => ErrorCode::WouldCycle,
        CommitValidationError::InvalidDisplayName { .. } => ErrorCode::InvalidRequest,
        CommitValidationError::StaleWriterEpoch { .. } => ErrorCode::WriterFenced,
        CommitValidationError::EmptyCommit
        | CommitValidationError::NamespaceMismatch
        | CommitValidationError::ValidatedPreviewApplyFailed(_)
        | CommitValidationError::RestoreRevisionOverflow { .. }
        | CommitValidationError::ReplaceFileRevisionOverflow { .. }
        | CommitValidationError::SeqOverflow
        | CommitValidationError::NextInodeOverflow
        | CommitValidationError::OpIndexOverflow
        | CommitValidationError::DeltaIndexOverflow => ErrorCode::ServerError,
    }
}

fn classify_head_publish_error(error: &CommitHeadPublishError) -> ErrorCode {
    match error {
        CommitHeadPublishError::StaleHead
        | CommitHeadPublishError::PublishBudgetExceeded { .. } => ErrorCode::StaleHead,
        CommitHeadPublishError::OutcomeUnknown(_) => ErrorCode::CommitOutcomeUnknown,
        CommitHeadPublishError::EmptyWriterVersion
        | CommitHeadPublishError::EmptyExpectedHeadEtag
        | CommitHeadPublishError::NamespaceMismatch { .. }
        | CommitHeadPublishError::WalSegmentNamespaceMismatch { .. }
        | CommitHeadPublishError::WalSegmentWriterEpochMismatch { .. }
        | CommitHeadPublishError::WalSegmentBaseHeadSeqMismatch { .. }
        | CommitHeadPublishError::WalSegmentStartSeqMismatch { .. }
        | CommitHeadPublishError::WalSegmentEndSeqMismatch { .. }
        | CommitHeadPublishError::EmptyWalSegment
        | CommitHeadPublishError::SeqOverflow
        | CommitHeadPublishError::Codec(_)
        | CommitHeadPublishError::Store(_) => ErrorCode::ServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreError, ErrorCode, ErrorKind, MetadataViewError};
    use loonfs_api::{ChangeSeq, ManifestId, NamespaceId};

    #[test]
    fn public_error_kind_groups_detailed_codes() {
        assert_eq!(ErrorCode::InvalidRequest.kind(), ErrorKind::InvalidRequest);
        assert_eq!(ErrorCode::PathNotFound.kind(), ErrorKind::NotFound);
        assert_eq!(ErrorCode::NamespaceDeleted.kind(), ErrorKind::Gone);
        assert_eq!(ErrorCode::NamespaceExists.kind(), ErrorKind::AlreadyExists);
        // Precondition failures are 409 resource-state conflicts in v0
        // (api.md, "Standard error contract"), so the kind is Conflict.
        assert_eq!(ErrorCode::StaleRevision.kind(), ErrorKind::Conflict);
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
        let error = CoreError::NamespaceAlreadyExists {
            namespace_id: NamespaceId::parse("demo").expect("valid namespace id"),
        };
        assert_eq!(error.kind(), ErrorKind::AlreadyExists);
        assert_eq!(error.code(), ErrorCode::NamespaceExists);
        assert_eq!(error.code().as_str(), "namespace_exists");
        assert!(error.message().contains("already exists"));
    }

    #[test]
    fn metadata_view_errors_map_to_actionable_public_codes() {
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        let _manifest_id = ManifestId(1);
        let head_seq = ChangeSeq(3);

        let cases = [
            (
                MetadataViewError::MissingManifest {
                    namespace_id: namespace_id.clone(),
                },
                ErrorCode::NamespaceCorrupt,
            ),
            (
                MetadataViewError::MaintenanceRequired {
                    namespace_id: namespace_id.clone(),
                    reason: "retention progress is missing".to_owned(),
                },
                ErrorCode::MaintenanceRequired,
            ),
            (
                MetadataViewError::SnapshotUnavailable {
                    requested_seq: ChangeSeq(1),
                    head_seq,
                },
                ErrorCode::RebootstrapRequired,
            ),
            (
                MetadataViewError::UnsupportedHistoricalRead {
                    requested_seq: ChangeSeq(2),
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
}
