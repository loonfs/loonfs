use crate::commit::{CommitConversionError, CommitHeadPublishError, CommitValidationError};
use crate::metadata::{MetadataApplyError, VisiblePathError};
use crate::namespace::basis::BasisLoadError;
use crate::namespace::catalog::NamespaceCatalogLoadError;
use crate::namespace::control::ControlObjectLoadError;
use crate::namespace::lease::LeaseAcquireError;
use crate::storage::content::{DurableContentValidationError, ImmutableObjectWriteError};
use crate::wal::{WalBuildError, WalChainLoadError};
use loon_api::{
    ChangeSeq, CommitIdValidationError, GeneratedIdValidationError, InodeId, InodeKind,
    NamespaceId, NamespaceIdValidationError,
};
use thiserror::Error;

pub type Error = CoreError;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Caller input is malformed or names an unsupported operation shape.
    InvalidRequest,
    /// The requested namespace, path, inode, revision, or upload session does not exist.
    NotFound,
    /// The caller attempted to create a resource that already exists.
    AlreadyExists,
    /// The request conflicts with current namespace state or another writer.
    Conflict,
    /// A caller-supplied precondition was false at validation time.
    PreconditionFailed,
    /// The operation is temporarily unavailable and may succeed later.
    Unavailable,
    /// Durable namespace, WAL, checkpoint, or content state is malformed.
    DataCorruption,
    /// LoonFS hit an internal invariant or implementation failure.
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidPath,
    InvalidNamespaceId,
    InvalidCommitId,
    InvalidUploadId,
    NamespaceNotFound,
    NamespaceExists,
    NamespacePartial,
    PathNotFound,
    RevisionNotFound,
    PathConflict,
    DirectoryNotEmpty,
    StaleHead,
    StaleRevision,
    TombstoneConflict,
    LeaseConflict,
    WouldCycle,
    UnsupportedRenameMode,
    CommitIdReuseConflict,
    CommitQueueFull,
    CheckpointUnavailable,
    UploadNotFound,
    UploadAlreadyCompleted,
    UploadContentConflict,
    InvalidUploadContent,
    RebootstrapRequired,
    NamespaceCorrupt,
    ServerError,
}

#[derive(Debug, Clone, Error)]
pub enum CoreError {
    #[error(transparent)]
    Basis(#[from] BasisLoadError),
    #[error(transparent)]
    VisiblePath(#[from] VisiblePathError),
    #[error(transparent)]
    DurableContent(#[from] DurableContentValidationError),
    #[error(transparent)]
    Lease(#[from] LeaseAcquireError),
    #[error("commit validation failed: {0:?}")]
    CommitValidation(CommitValidationError),
    #[error("wal build failed: {0:?}")]
    WalBuild(WalBuildError),
    #[error("metadata apply failed: {0:?}")]
    MetadataApply(MetadataApplyError),
    #[error("head publish failed: {0:?}")]
    HeadPublish(CommitHeadPublishError),
    #[error("failed to write wal object: {0}")]
    WalWrite(String),
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error(transparent)]
    InvalidNamespaceId(#[from] NamespaceIdValidationError),
    #[error(transparent)]
    InvalidCommitId(#[from] CommitIdValidationError),
    #[error(transparent)]
    InvalidUploadId(GeneratedIdValidationError),
    #[error("path not found `{0}`")]
    MissingPath(String),
    #[error("revision `{revision_no:?}` not found for inode `{inode_id}`")]
    MissingRevision {
        inode_id: InodeId,
        revision_no: loon_api::RevisionNo,
    },
    #[error("expected file at `{path}` but found `{kind:?}`")]
    ExpectedFile { path: String, kind: InodeKind },
    #[error("expected directory at `{path}` but found `{kind:?}`")]
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
    #[error("{0}")]
    CheckpointUnavailable(String),
    #[error("upload session `{upload_id}` was not found")]
    UploadNotFound { upload_id: String },
    #[error("upload session `{upload_id}` is already completed")]
    UploadAlreadyCompleted { upload_id: String },
    #[error("upload session `{upload_id}` content conflicts with prior content")]
    UploadContentConflict { upload_id: String },
    #[error("invalid upload content: {0}")]
    InvalidUploadContent(String),
    #[error(
        "change feed cursor `{after_seq:?}` is older than retention floor `{retention_floor_seq:?}`"
    )]
    RebootstrapRequired {
        after_seq: ChangeSeq,
        retention_floor_seq: ChangeSeq,
    },
    #[error(
        "path `{path}` is covered by subtree tombstone rooted at inode `{root_inode}` from seq `{tombstone_seq:?}`"
    )]
    TombstoneConflict {
        path: String,
        root_inode: InodeId,
        tombstone_seq: ChangeSeq,
    },
    #[error("path component `{0}` is not a directory")]
    NonDirectoryPathComponent(String),
    #[error("namespace corrupt: {0}")]
    NamespaceCorrupt(String),
    #[error("object store error: {0}")]
    Store(String),
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceAlreadyExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is partially initialized")]
    NamespacePartiallyInitialized { namespace_id: NamespaceId },
}

impl From<CommitValidationError> for CoreError {
    fn from(value: CommitValidationError) -> Self {
        Self::CommitValidation(value)
    }
}

impl From<WalBuildError> for CoreError {
    fn from(value: WalBuildError) -> Self {
        Self::WalBuild(value)
    }
}

impl From<MetadataApplyError> for CoreError {
    fn from(value: MetadataApplyError) -> Self {
        Self::MetadataApply(value)
    }
}

impl From<CommitHeadPublishError> for CoreError {
    fn from(value: CommitHeadPublishError) -> Self {
        Self::HeadPublish(value)
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
        Self::Basis(value.into())
    }
}

impl From<ImmutableObjectWriteError> for CoreError {
    fn from(value: ImmutableObjectWriteError) -> Self {
        Self::Store(value.to_string())
    }
}

impl CoreError {
    pub fn kind(&self) -> ErrorKind {
        self.code().kind()
    }

    pub fn code(&self) -> ErrorCode {
        match self {
            CoreError::Basis(error) => classify_basis_load_error(error),
            CoreError::VisiblePath(error) => classify_visible_path_error(error),
            CoreError::DurableContent(error) => classify_durable_content_error(error),
            CoreError::Lease(error) => classify_lease_acquire_error(error),
            CoreError::CommitValidation(error) => classify_commit_validation_error(error),
            CoreError::WalBuild(_)
            | CoreError::MetadataApply(_)
            | CoreError::WalWrite(_)
            | CoreError::Store(_) => ErrorCode::ServerError,
            CoreError::HeadPublish(error) => classify_head_publish_error(error),
            CoreError::InvalidPath(_) | CoreError::RootMutationForbidden => ErrorCode::InvalidPath,
            CoreError::InvalidNamespaceId(_) => ErrorCode::InvalidNamespaceId,
            CoreError::InvalidCommitId(_) => ErrorCode::InvalidCommitId,
            CoreError::InvalidUploadId(_) => ErrorCode::InvalidUploadId,
            CoreError::MissingPath(_) => ErrorCode::PathNotFound,
            CoreError::MissingRevision { .. } => ErrorCode::RevisionNotFound,
            CoreError::NamespaceAlreadyExists { .. } => ErrorCode::NamespaceExists,
            CoreError::NamespacePartiallyInitialized { .. } => ErrorCode::NamespacePartial,
            CoreError::CommitIdReuseConflict(_) => ErrorCode::CommitIdReuseConflict,
            CoreError::CommitQueueFull => ErrorCode::CommitQueueFull,
            CoreError::CheckpointUnavailable(_) => ErrorCode::CheckpointUnavailable,
            CoreError::UploadNotFound { .. } => ErrorCode::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => ErrorCode::UploadAlreadyCompleted,
            CoreError::UploadContentConflict { .. } => ErrorCode::UploadContentConflict,
            CoreError::InvalidUploadContent(_) => ErrorCode::InvalidUploadContent,
            CoreError::RebootstrapRequired { .. } => ErrorCode::RebootstrapRequired,
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DestinationExists(_) => ErrorCode::PathConflict,
            CoreError::DirectoryNotEmpty(_) => ErrorCode::DirectoryNotEmpty,
            CoreError::TombstoneConflict { .. } => ErrorCode::TombstoneConflict,
            CoreError::NonDirectoryPathComponent(_) => ErrorCode::InvalidPath,
            CoreError::NamespaceCorrupt(_) => ErrorCode::NamespaceCorrupt,
        }
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl ErrorCode {
    pub fn kind(self) -> ErrorKind {
        match self {
            ErrorCode::InvalidPath
            | ErrorCode::InvalidNamespaceId
            | ErrorCode::InvalidCommitId
            | ErrorCode::InvalidUploadId
            | ErrorCode::UnsupportedRenameMode
            | ErrorCode::InvalidUploadContent => ErrorKind::InvalidRequest,
            ErrorCode::NamespaceNotFound
            | ErrorCode::PathNotFound
            | ErrorCode::RevisionNotFound
            | ErrorCode::UploadNotFound => ErrorKind::NotFound,
            ErrorCode::NamespaceExists => ErrorKind::AlreadyExists,
            ErrorCode::StaleRevision => ErrorKind::PreconditionFailed,
            ErrorCode::CommitQueueFull | ErrorCode::CheckpointUnavailable => ErrorKind::Unavailable,
            ErrorCode::NamespaceCorrupt => ErrorKind::DataCorruption,
            ErrorCode::ServerError => ErrorKind::Internal,
            ErrorCode::NamespacePartial
            | ErrorCode::PathConflict
            | ErrorCode::DirectoryNotEmpty
            | ErrorCode::StaleHead
            | ErrorCode::TombstoneConflict
            | ErrorCode::LeaseConflict
            | ErrorCode::WouldCycle
            | ErrorCode::CommitIdReuseConflict
            | ErrorCode::UploadAlreadyCompleted
            | ErrorCode::UploadContentConflict
            | ErrorCode::RebootstrapRequired => ErrorKind::Conflict,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidPath => "invalid_path",
            ErrorCode::InvalidNamespaceId => "invalid_namespace_id",
            ErrorCode::InvalidCommitId => "invalid_commit_id",
            ErrorCode::InvalidUploadId => "invalid_upload_id",
            ErrorCode::NamespaceNotFound => "namespace_not_found",
            ErrorCode::NamespaceExists => "namespace_exists",
            ErrorCode::NamespacePartial => "namespace_partial",
            ErrorCode::PathNotFound => "path_not_found",
            ErrorCode::RevisionNotFound => "revision_not_found",
            ErrorCode::PathConflict => "path_conflict",
            ErrorCode::DirectoryNotEmpty => "directory_not_empty",
            ErrorCode::StaleHead => "stale_head",
            ErrorCode::StaleRevision => "stale_revision",
            ErrorCode::TombstoneConflict => "tombstone_conflict",
            ErrorCode::LeaseConflict => "lease_conflict",
            ErrorCode::WouldCycle => "would_cycle",
            ErrorCode::UnsupportedRenameMode => "unsupported_rename_mode",
            ErrorCode::CommitIdReuseConflict => "commit_id_reuse_conflict",
            ErrorCode::CommitQueueFull => "commit_queue_full",
            ErrorCode::CheckpointUnavailable => "checkpoint_unavailable",
            ErrorCode::UploadNotFound => "upload_not_found",
            ErrorCode::UploadAlreadyCompleted => "upload_already_completed",
            ErrorCode::UploadContentConflict => "upload_content_conflict",
            ErrorCode::InvalidUploadContent => "invalid_upload_content",
            ErrorCode::RebootstrapRequired => "rebootstrap_required",
            ErrorCode::NamespaceCorrupt => "namespace_corrupt",
            ErrorCode::ServerError => "server_error",
        }
    }
}

fn classify_control_object_load_error(error: &ControlObjectLoadError) -> ErrorCode {
    match error {
        ControlObjectLoadError::InvalidNamespaceId { .. } => ErrorCode::InvalidNamespaceId,
        ControlObjectLoadError::MissingObject { .. }
        | ControlObjectLoadError::MissingObjectAfterHead { .. } => ErrorCode::NamespaceNotFound,
        ControlObjectLoadError::KindMismatch { .. }
        | ControlObjectLoadError::NamespaceMismatch { .. }
        | ControlObjectLoadError::ContentStoreMismatch { .. }
        | ControlObjectLoadError::ChecksumMismatch { .. }
        | ControlObjectLoadError::Codec { .. } => ErrorCode::NamespaceCorrupt,
        ControlObjectLoadError::Store(_) => ErrorCode::ServerError,
    }
}

fn classify_basis_load_error(error: &BasisLoadError) -> ErrorCode {
    match error {
        BasisLoadError::LoadNamespaceDescriptor(error) => classify_control_object_load_error(error),
        BasisLoadError::LoadContentStoreDescriptor(error) => match error {
            ControlObjectLoadError::InvalidNamespaceId { .. } => ErrorCode::InvalidNamespaceId,
            ControlObjectLoadError::Store(_) => ErrorCode::ServerError,
            _ => ErrorCode::NamespaceCorrupt,
        },
        BasisLoadError::LoadHead(error) | BasisLoadError::LoadLease(error) => match error {
            ControlObjectLoadError::MissingObject { .. }
            | ControlObjectLoadError::MissingObjectAfterHead { .. } => ErrorCode::NamespaceCorrupt,
            _ => classify_control_object_load_error(error),
        },
        BasisLoadError::WalChainLoad(error) => classify_wal_chain_load_error(error),
        BasisLoadError::WalReplay(_) | BasisLoadError::ReconstructedHeadMismatch { .. } => {
            ErrorCode::NamespaceCorrupt
        }
        BasisLoadError::CheckpointLoad(error) => match error.kind() {
            crate::checkpoint::CheckpointLoadErrorKind::Corrupt => ErrorCode::NamespaceCorrupt,
            crate::checkpoint::CheckpointLoadErrorKind::Store => ErrorCode::ServerError,
        },
        BasisLoadError::MissingHeadEtag { .. } => ErrorCode::ServerError,
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
        VisiblePathError::InvalidAbsolutePath { .. } => ErrorCode::InvalidPath,
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

fn classify_lease_acquire_error(error: &LeaseAcquireError) -> ErrorCode {
    match error {
        LeaseAcquireError::LoadHead(error) | LeaseAcquireError::LoadLease(error) => {
            classify_control_object_load_error(error)
        }
        LeaseAcquireError::HeldByOtherWriter { .. } => ErrorCode::LeaseConflict,
        LeaseAcquireError::UnexpectedControlState { .. } => ErrorCode::NamespaceCorrupt,
        LeaseAcquireError::EmptyWriterId
        | LeaseAcquireError::ZeroLeaseDuration
        | LeaseAcquireError::MissingHeadEtag { .. }
        | LeaseAcquireError::MissingLeaseEtag { .. }
        | LeaseAcquireError::HeadFenceTakeover(_)
        | LeaseAcquireError::HeadWrite(_)
        | LeaseAcquireError::LeaseWrite(_)
        | LeaseAcquireError::RetryExhausted { .. } => ErrorCode::ServerError,
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
        CommitValidationError::UnsupportedRenameMode { .. } => ErrorCode::UnsupportedRenameMode,
        CommitValidationError::InvalidDisplayName { .. } => ErrorCode::InvalidPath,
        CommitValidationError::StaleWriterFenceToken { .. }
        | CommitValidationError::LeaseHolderMismatch { .. }
        | CommitValidationError::LeaseExpired { .. } => ErrorCode::LeaseConflict,
        CommitValidationError::EmptyCommit
        | CommitValidationError::NamespaceMismatch
        | CommitValidationError::HeadLeaseNamespaceMismatch
        | CommitValidationError::HeadLeaseFenceMismatch { .. }
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
        CommitHeadPublishError::StaleHead => ErrorCode::StaleHead,
        CommitHeadPublishError::EmptyWriterVersion
        | CommitHeadPublishError::EmptyExpectedHeadEtag
        | CommitHeadPublishError::NamespaceMismatch { .. }
        | CommitHeadPublishError::WalSegmentNamespaceMismatch { .. }
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
    use super::{CoreError, ErrorCode, ErrorKind};
    use loon_api::NamespaceId;

    #[test]
    fn public_error_kind_groups_detailed_codes() {
        assert_eq!(ErrorCode::InvalidPath.kind(), ErrorKind::InvalidRequest);
        assert_eq!(ErrorCode::PathNotFound.kind(), ErrorKind::NotFound);
        assert_eq!(ErrorCode::NamespaceExists.kind(), ErrorKind::AlreadyExists);
        assert_eq!(
            ErrorCode::StaleRevision.kind(),
            ErrorKind::PreconditionFailed
        );
        assert_eq!(ErrorCode::CommitQueueFull.kind(), ErrorKind::Unavailable);
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
}
