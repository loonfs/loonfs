use crate::basis::BasisLoadError;
use crate::commit::{CommitHeadPublishError, CommitValidationError};
use crate::content::{DurableContentValidationError, ImmutableObjectWriteError};
use crate::lease::LeaseAcquireError;
use crate::loading::ControlObjectLoadError;
use crate::metadata::{MetadataApplyError, VisiblePathError};
use crate::namespace::catalog::NamespaceCatalogLoadError;
use crate::wal::WalBuildError;
use loon_api::{ChangeSeq, InodeId, InodeKind, NamespaceId, NamespaceIdValidationError};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreErrorKind {
    InvalidPath,
    InvalidNamespaceId,
    NamespaceNotFound,
    NamespaceExists,
    NamespacePartial,
    PathNotFound,
    RevisionNotFound,
    PathConflict,
    StaleHead,
    StaleRevision,
    TombstoneConflict,
    LeaseConflict,
    WouldCycle,
    RequestIdConflict,
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
    #[error("path not found `{0}`")]
    MissingPath(String),
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
    #[error("request id conflict for `{0}`")]
    RequestIdConflict(String),
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
    pub fn kind(&self) -> CoreErrorKind {
        match self {
            CoreError::Basis(error) => classify_basis_load_error(error),
            CoreError::VisiblePath(error) => classify_visible_path_error(error),
            CoreError::DurableContent(error) => classify_durable_content_error(error),
            CoreError::Lease(error) => classify_lease_acquire_error(error),
            CoreError::CommitValidation(error) => classify_commit_validation_error(error),
            CoreError::WalBuild(_)
            | CoreError::MetadataApply(_)
            | CoreError::WalWrite(_)
            | CoreError::Store(_) => CoreErrorKind::ServerError,
            CoreError::HeadPublish(error) => classify_head_publish_error(error),
            CoreError::InvalidPath(_) | CoreError::RootMutationForbidden => {
                CoreErrorKind::InvalidPath
            }
            CoreError::InvalidNamespaceId(_) => CoreErrorKind::InvalidNamespaceId,
            CoreError::MissingPath(_) => CoreErrorKind::PathNotFound,
            CoreError::NamespaceAlreadyExists { .. } => CoreErrorKind::NamespaceExists,
            CoreError::NamespacePartiallyInitialized { .. } => CoreErrorKind::NamespacePartial,
            CoreError::RequestIdConflict(_) => CoreErrorKind::RequestIdConflict,
            CoreError::CommitQueueFull => CoreErrorKind::CommitQueueFull,
            CoreError::CheckpointUnavailable(_) => CoreErrorKind::CheckpointUnavailable,
            CoreError::UploadNotFound { .. } => CoreErrorKind::UploadNotFound,
            CoreError::UploadAlreadyCompleted { .. } => CoreErrorKind::UploadAlreadyCompleted,
            CoreError::UploadContentConflict { .. } => CoreErrorKind::UploadContentConflict,
            CoreError::InvalidUploadContent(_) => CoreErrorKind::InvalidUploadContent,
            CoreError::RebootstrapRequired { .. } => CoreErrorKind::RebootstrapRequired,
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DirectoryNotEmpty(_)
            | CoreError::DestinationExists(_) => CoreErrorKind::PathConflict,
            CoreError::TombstoneConflict { .. } => CoreErrorKind::TombstoneConflict,
            CoreError::NonDirectoryPathComponent(_) => CoreErrorKind::InvalidPath,
        }
    }
}

fn classify_control_object_load_error(error: &ControlObjectLoadError) -> CoreErrorKind {
    match error {
        ControlObjectLoadError::InvalidNamespaceId { .. } => CoreErrorKind::InvalidNamespaceId,
        ControlObjectLoadError::MissingObject { .. }
        | ControlObjectLoadError::MissingObjectAfterHead { .. } => CoreErrorKind::NamespaceNotFound,
        ControlObjectLoadError::KindMismatch { .. }
        | ControlObjectLoadError::NamespaceMismatch { .. }
        | ControlObjectLoadError::ContentStoreMismatch { .. }
        | ControlObjectLoadError::ChecksumMismatch { .. }
        | ControlObjectLoadError::Codec { .. } => CoreErrorKind::NamespaceCorrupt,
        ControlObjectLoadError::Store(_) => CoreErrorKind::ServerError,
    }
}

fn classify_basis_load_error(error: &BasisLoadError) -> CoreErrorKind {
    match error {
        BasisLoadError::LoadNamespaceDescriptor(error) => classify_control_object_load_error(error),
        BasisLoadError::LoadContentStoreDescriptor(error) => match error {
            ControlObjectLoadError::InvalidNamespaceId { .. } => CoreErrorKind::InvalidNamespaceId,
            ControlObjectLoadError::Store(_) => CoreErrorKind::ServerError,
            _ => CoreErrorKind::NamespaceCorrupt,
        },
        BasisLoadError::LoadHead(error) | BasisLoadError::LoadLease(error) => match error {
            ControlObjectLoadError::MissingObject { .. }
            | ControlObjectLoadError::MissingObjectAfterHead { .. } => {
                CoreErrorKind::NamespaceCorrupt
            }
            _ => classify_control_object_load_error(error),
        },
        BasisLoadError::InvalidWalObjectKey { .. }
        | BasisLoadError::DuplicateWalSeq { .. }
        | BasisLoadError::MissingWalObject { .. }
        | BasisLoadError::MissingWalObjectAfterList { .. }
        | BasisLoadError::WalReplay(_)
        | BasisLoadError::ReconstructedHeadMismatch { .. } => CoreErrorKind::NamespaceCorrupt,
        BasisLoadError::CheckpointLoad(error) => match error.kind() {
            crate::checkpoint::CheckpointLoadErrorKind::Corrupt => CoreErrorKind::NamespaceCorrupt,
            crate::checkpoint::CheckpointLoadErrorKind::Store => CoreErrorKind::ServerError,
        },
        BasisLoadError::MissingHeadEtag { .. }
        | BasisLoadError::ListWal { .. }
        | BasisLoadError::ReadWal { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_visible_path_error(error: &VisiblePathError) -> CoreErrorKind {
    match error {
        VisiblePathError::InvalidAbsolutePath { .. } => CoreErrorKind::InvalidPath,
        VisiblePathError::RootMissing => CoreErrorKind::NamespaceCorrupt,
        VisiblePathError::PathNotFound { .. } => CoreErrorKind::PathNotFound,
        VisiblePathError::PathComponentNotDirectory { .. } => CoreErrorKind::PathConflict,
    }
}

fn classify_durable_content_error(error: &DurableContentValidationError) -> CoreErrorKind {
    match error {
        DurableContentValidationError::UnsupportedContentRefKind { .. }
        | DurableContentValidationError::InvalidDigest { .. }
        | DurableContentValidationError::MissingContentObject { .. }
        | DurableContentValidationError::ContentLengthMismatch { .. }
        | DurableContentValidationError::ContentDigestMismatch { .. } => {
            CoreErrorKind::NamespaceCorrupt
        }
        DurableContentValidationError::Store { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_lease_acquire_error(error: &LeaseAcquireError) -> CoreErrorKind {
    match error {
        LeaseAcquireError::LoadHead(error) | LeaseAcquireError::LoadLease(error) => {
            classify_control_object_load_error(error)
        }
        LeaseAcquireError::HeldByOtherWriter { .. } => CoreErrorKind::LeaseConflict,
        LeaseAcquireError::UnexpectedControlState { .. } => CoreErrorKind::NamespaceCorrupt,
        LeaseAcquireError::EmptyWriterId
        | LeaseAcquireError::ZeroLeaseDuration
        | LeaseAcquireError::MissingHeadEtag { .. }
        | LeaseAcquireError::MissingLeaseEtag { .. }
        | LeaseAcquireError::HeadFenceTakeover(_)
        | LeaseAcquireError::HeadWrite(_)
        | LeaseAcquireError::LeaseWrite(_)
        | LeaseAcquireError::RetryExhausted { .. } => CoreErrorKind::ServerError,
    }
}

fn classify_commit_validation_error(error: &CommitValidationError) -> CoreErrorKind {
    match error {
        CommitValidationError::PlannedHeadSeqMismatch { .. }
        | CommitValidationError::MissingHeadSeqPrecondition { .. }
        | CommitValidationError::ConflictingHeadSeqPrecondition { .. } => CoreErrorKind::StaleHead,
        CommitValidationError::ReplaceFileBaseRevisionMismatch { .. }
        | CommitValidationError::RestoreRevisionBaseRevisionMismatch { .. } => {
            CoreErrorKind::StaleRevision
        }
        CommitValidationError::RestoreRevisionSourceRevisionMissing { .. } => {
            CoreErrorKind::RevisionNotFound
        }
        CommitValidationError::CreateUnderSubtreeTombstone { .. }
        | CommitValidationError::ReplaceFileUnderSubtreeTombstone { .. }
        | CommitValidationError::RestoreRevisionUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteFileCoveredByTombstone { .. }
        | CommitValidationError::RenameInodeUnderSubtreeTombstone { .. }
        | CommitValidationError::RenameTargetParentUnderSubtreeTombstone { .. }
        | CommitValidationError::DeleteSubtreeRootCoveredByTombstone { .. } => {
            CoreErrorKind::TombstoneConflict
        }
        CommitValidationError::CreateChildNameCollision { .. }
        | CommitValidationError::CreateParentNotDirectory { .. }
        | CommitValidationError::ReplaceFileInodeNotFile { .. }
        | CommitValidationError::RestoreRevisionInodeNotFile { .. }
        | CommitValidationError::DeleteFileInodeNotFile { .. }
        | CommitValidationError::RenameTargetParentNotDirectory { .. }
        | CommitValidationError::RenameTargetNameCollision { .. }
        | CommitValidationError::DeleteSubtreeRootNotDirectory { .. } => {
            CoreErrorKind::PathConflict
        }
        CommitValidationError::CreateParentMissing { .. }
        | CommitValidationError::ReplaceFileInodeMissing { .. }
        | CommitValidationError::RestoreRevisionInodeMissing { .. }
        | CommitValidationError::DeleteFileInodeMissing { .. }
        | CommitValidationError::RenameInodeMissing { .. }
        | CommitValidationError::RenameSourceBindingMissing { .. }
        | CommitValidationError::RenameTargetParentMissing { .. }
        | CommitValidationError::DeleteSubtreeRootMissing { .. } => CoreErrorKind::PathNotFound,
        CommitValidationError::RenameWouldCycleDirectory { .. } => CoreErrorKind::WouldCycle,
        CommitValidationError::StaleWriterFenceToken { .. }
        | CommitValidationError::LeaseHolderMismatch { .. }
        | CommitValidationError::LeaseExpired { .. } => CoreErrorKind::LeaseConflict,
        CommitValidationError::EmptyCommit
        | CommitValidationError::NamespaceMismatch
        | CommitValidationError::HeadLeaseNamespaceMismatch
        | CommitValidationError::HeadLeaseFenceMismatch { .. }
        | CommitValidationError::RestoreRevisionOverflow { .. }
        | CommitValidationError::ReplaceFileRevisionOverflow { .. }
        | CommitValidationError::SeqOverflow
        | CommitValidationError::NextInodeOverflow
        | CommitValidationError::OpIndexOverflow => CoreErrorKind::ServerError,
    }
}

fn classify_head_publish_error(error: &CommitHeadPublishError) -> CoreErrorKind {
    match error {
        CommitHeadPublishError::StaleHead => CoreErrorKind::StaleHead,
        CommitHeadPublishError::EmptyWriterVersion
        | CommitHeadPublishError::EmptyExpectedHeadEtag
        | CommitHeadPublishError::NamespaceMismatch { .. }
        | CommitHeadPublishError::PlanBaseHeadSeqMismatch { .. }
        | CommitHeadPublishError::PlanNextSeqMismatch { .. }
        | CommitHeadPublishError::Codec(_)
        | CommitHeadPublishError::Store(_) => CoreErrorKind::ServerError,
    }
}
