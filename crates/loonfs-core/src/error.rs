//! Core error types and their wire-code classification.
//!
//! Stringification rule: core errors are `Clone`/serde-constrained, so foreign
//! causes (object-store, codec, io) are captured as prefixed message strings
//! next to the object key they are about, not as `#[source]` chains. Crates
//! without those constraints (server, CLI) prefer `#[source]` chains instead.

use crate::checkpoint::ManifestLoadError;
use crate::commit::{CommitHeadPublishError, CommitValidationError};
use crate::commit_engine::ContentPreparationError;
use crate::metadata::VisiblePathError;
use crate::namespace::catalog::NamespaceCatalogLoadError;
use crate::namespace::control::ControlObjectLoadError;
use crate::namespace::writer_epoch::WriterEpochAcquireError;
use crate::storage::content::DurableContentValidationError;
use crate::wal::{WalBuildError, WalChainLoadError, WalReplayError};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{
    ChangeSeq, CommitId, CommitIdValidationError, ErrorDetails, GeneratedIdValidationError,
    InodeId, InodeKind, NamespaceId, NamespaceIdValidationError, RevisionNo, UploadId, WriterEpoch,
};
use loonfs_objectstore::{ImmutableWriteError, ObjectStoreError};
use thiserror::Error;

/// Public error type returned by `loonfs-core`.
///
/// Use [`Error::kind`] for broad caller action and [`Error::code`] for a stable
/// machine-readable reason.
pub use self::CoreError as Error;

/// Result type used by `loonfs-core` entrypoints. Crate-internal: the alias
/// is transparent, so public signatures using it still read as
/// `std::result::Result<T, Error>` from outside.
pub(crate) type Result<T> = std::result::Result<T, Error>;

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
    #[error("head publish failed: {0}")]
    HeadPublish(#[from] CommitHeadPublishError),
    #[error("failed to write wal object `{object_key}`: {message}")]
    WalWrite {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    #[error("invalid absolute path `{0}`")]
    InvalidPath(String),
    #[error(transparent)]
    InvalidNamespaceId(#[from] NamespaceIdValidationError),
    #[error(transparent)]
    InvalidCommitId(#[from] CommitIdValidationError),
    #[error("invalid commit request: {0}")]
    InvalidCommitRequest(String),
    #[error(transparent)]
    InvalidUploadId(#[from] GeneratedIdValidationError),
    #[error("path not found `{0}`")]
    PathNotFound(String),
    #[error("revision `{revision_no}` not found for inode `{inode_id}`")]
    RevisionNotFound {
        inode_id: InodeId,
        revision_no: RevisionNo,
    },
    /// A buffered content read was refused before any fetch: the resolved
    /// content is larger than the caller's read budget allows in memory.
    #[error(
        "file content is {size_bytes} bytes, over the {max_bytes}-byte limit \
         this deployment buffers for one read"
    )]
    ContentTooLarge { size_bytes: u64, max_bytes: u64 },
    /// A batch read asked for more items than one call answers. The caller
    /// splits the batch; nothing was read.
    #[error("asked for {requested} items, over the {max} one batch answers")]
    BatchTooLarge { requested: usize, max: usize },
    #[error("expected file at `{path}` but found `{kind}`")]
    ExpectedFile { path: String, kind: InodeKind },
    #[error("expected directory at `{path}` but found `{kind}`")]
    ExpectedDirectory { path: String, kind: InodeKind },
    #[error("directory not empty `{0}`")]
    DirectoryNotEmpty(String),
    #[error("cannot mutate root path")]
    RootMutationForbidden,
    #[error("{}", destination_exists_message(.path, .existing_display_name.as_deref()))]
    DestinationExists {
        path: String,
        /// Stored spelling of the occupying entry, when the planner had it.
        /// Rendered when it differs from what the caller typed, because
        /// sibling names collide after NFC normalization and case folding
        /// and the two spellings can look identical.
        existing_display_name: Option<String>,
    },
    #[error("commit id conflict for `{0}`")]
    CommitIdReuseConflict(String),
    #[error(transparent)]
    ContentPreparation(#[from] ContentPreparationError),
    #[error("commit queue is full; slow down and retry")]
    CommitQueueFull,
    /// The serving front-end closed admission for shutdown. New work is
    /// refused; work admitted earlier still settles.
    #[error("shutting down; new work is not admitted")]
    ShuttingDown,
    #[error("checkpoint unavailable: {0}")]
    CheckpointUnavailable(String),
    #[error("invalid checkpoint request: {0}")]
    InvalidCheckpointRequest(String),
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
    WriterFenced(WriterFence),
    #[error("object store error for `{object_key}`: {message}")]
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
    /// Non-store internal failure (codec, overflow, invariant breach). Same
    /// wire code as [`ErrorCode::ServerError`]; the message is the detail.
    #[error("internal error: {0}")]
    Internal(String),
    #[error("namespace `{namespace_id}` already exists")]
    NamespaceExists { namespace_id: NamespaceId },
    #[error("namespace `{namespace_id}` is deleted")]
    NamespaceDeleted { namespace_id: NamespaceId },
    /// A batch stopped at one of its operations. A mutation commits all of
    /// its operations or none of them, so this names the operation that
    /// stopped the request and carries the failure it produced. The wire code
    /// stays the inner failure's; the position joins the message and the
    /// structured details.
    ///
    /// Only a request with more than one operation is wrapped: a
    /// single-operation request has one place to fail, so its error stays
    /// exactly what the operation produced.
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
    #[error("namespace `{namespace_id}` head has no current manifest")]
    MissingManifest { namespace_id: NamespaceId },
    #[error("metadata view for namespace `{namespace_id}` requires maintenance: {reason}")]
    MaintenanceRequired {
        namespace_id: NamespaceId,
        reason: String,
    },
    #[error(
        "the cursor was minted at seq `{requested_seq}`, ahead of the loaded head `{head_seq}`; restart the listing"
    )]
    SnapshotUnavailable {
        requested_seq: ChangeSeq,
        head_seq: ChangeSeq,
    },
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
            ImmutableWriteError::DifferentObject { object_key } => Self::Store {
                object_key,
                message: "immutable object already exists with different bytes".to_owned(),
                class: StoreFailureClass::Other,
            },
            ImmutableWriteError::Transport { object_key, source } => Self::Store {
                object_key,
                message: source.message(),
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

/// What a failed provider operation says about who can fix it, preserved
/// across the message-flattening seams so the wire code can distinguish
/// "fix the storage credentials" from "unclassified internal failure".
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StoreFailureClass {
    /// The provider rejected the deployment's credentials: operator work,
    /// never transient. Served as `permission_denied`.
    PermissionDenied,
    /// Everything else is internal from the caller's point of view.
    Other,
}

impl StoreFailureClass {
    /// Classifies a provider error at the seam where its message is
    /// flattened into a carrier's `message` field.
    pub fn of(error: &ObjectStoreError) -> Self {
        match error {
            ObjectStoreError::PermissionDenied { .. } => Self::PermissionDenied,
            _ => Self::Other,
        }
    }
}

fn classify_store_failure(class: StoreFailureClass) -> ErrorCode {
    match class {
        StoreFailureClass::PermissionDenied => ErrorCode::PermissionDenied,
        StoreFailureClass::Other => ErrorCode::ServerError,
    }
}

impl CoreError {
    pub(crate) fn load_head(error: ControlObjectLoadError) -> Self {
        Self::MetadataProjection(MetadataProjectionLoadError::LoadHead(error))
    }

    /// Builds [`CoreError::Store`] for a failed object-store operation on
    /// `object_key`.
    pub(crate) fn store(object_key: impl Into<String>, error: &ObjectStoreError) -> Self {
        Self::Store {
            object_key: object_key.into(),
            message: error.message(),
            class: StoreFailureClass::of(error),
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
            CoreError::WalBuild(_) | CoreError::Internal(_) => ErrorCode::ServerError,
            CoreError::WalWrite { class, .. } | CoreError::Store { class, .. } => {
                classify_store_failure(*class)
            }
            CoreError::HeadPublish(error) => classify_head_publish_error(error),
            CoreError::InvalidPath(_)
            | CoreError::RootMutationForbidden
            | CoreError::InvalidNamespaceId(_)
            | CoreError::InvalidCommitId(_)
            | CoreError::InvalidCommitRequest(_)
            | CoreError::InvalidUploadId(_)
            | CoreError::InvalidCheckpointRequest(_)
            | CoreError::InvalidGcConfig(_)
            | CoreError::InvalidQuery(_)
            | CoreError::InvalidUploadContent(_)
            | CoreError::InvalidCursor(_)
            | CoreError::BatchTooLarge { .. }
            | CoreError::NonDirectoryPathComponent(_) => ErrorCode::InvalidRequest,
            CoreError::PathNotFound(_) => ErrorCode::PathNotFound,
            CoreError::RevisionNotFound { .. } => ErrorCode::RevisionNotFound,
            CoreError::ContentTooLarge { .. } => ErrorCode::ContentTooLarge,
            CoreError::NamespaceExists { .. } => ErrorCode::NamespaceExists,
            CoreError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
            CoreError::CommitIdReuseConflict(_) => ErrorCode::CommitIdReuseConflict,
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
            CoreError::DirectoryNotEmpty(_) => ErrorCode::DirectoryNotEmpty,
            CoreError::TombstoneConflict { .. } => ErrorCode::TombstoneConflict,
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

    /// Structured wire details for this error, when the variant carries
    /// machine-usable identity (API spec, "Standard error contract"). The
    /// server serializes this beside [`CoreError::code`]; embedded callers
    /// can match the typed variants directly instead.
    pub fn details(&self) -> Option<ErrorDetails> {
        match self {
            CoreError::WriterFenced(fence) => Some(ErrorDetails {
                fenced_epoch: Some(fence.fenced_epoch),
                active_writer_epoch: Some(fence.active_epoch),
                active_writer: fence.active_writer.clone(),
                ..ErrorDetails::default()
            }),
            CoreError::CommitIdReuseConflict(commit_id) => Some(ErrorDetails {
                commit_id: CommitId::parse(commit_id).ok(),
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
            CoreError::CommitValidation(error) => commit_validation_details(error),
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

/// The fencing event a writer session observed: the epoch the session held,
/// the epoch that displaced it, and — when the head recorded a writer block —
/// the winner's label and when it acquired.
///
/// The epochs are what identifies the two parties. Writer labels are process
/// names, so two local processes can share one (the CLI defaults `writer_id`
/// to the hostname); the acquisition stamp is what tells those runs apart in
/// a diagnostic.
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
        // Both fields come from the head's writer block, so in practice they
        // are present or absent together; the arms keep the message honest
        // either way.
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

fn commit_validation_details(error: &CommitValidationError) -> Option<ErrorDetails> {
    match error {
        CommitValidationError::ReplaceFileBaseRevisionMismatch {
            inode_id,
            expected,
            actual,
        }
        | CommitValidationError::RestoreRevisionBaseRevisionMismatch {
            inode_id,
            expected,
            actual,
        } => Some(ErrorDetails {
            inode_id: Some(*inode_id),
            expected_revision: Some(*expected),
            actual_revision: *actual,
            ..ErrorDetails::default()
        }),
        CommitValidationError::StaleWriterEpoch { active, requested } => Some(ErrorDetails {
            fenced_epoch: Some(*requested),
            active_writer_epoch: Some(*active),
            ..ErrorDetails::default()
        }),
        CommitValidationError::UndeleteInodeMissing { inode_id }
        | CommitValidationError::UndeleteTargetNotDeleted { inode_id } => Some(ErrorDetails {
            inode_id: Some(*inode_id),
            ..ErrorDetails::default()
        }),
        CommitValidationError::UndeleteTargetsCurrentCommit {
            inode_id,
            requested_seq,
        } => Some(ErrorDetails {
            inode_id: Some(*inode_id),
            requested_deletion_seq: Some(*requested_seq),
            ..ErrorDetails::default()
        }),
        CommitValidationError::UndeleteGenerationMismatch {
            inode_id,
            requested_seq,
            active_seq,
        } => Some(ErrorDetails {
            inode_id: Some(*inode_id),
            requested_deletion_seq: Some(*requested_seq),
            active_deletion_seq: Some(*active_seq),
            ..ErrorDetails::default()
        }),
        _ => None,
    }
}

fn classify_metadata_view_error(error: &MetadataViewError) -> ErrorCode {
    match error {
        MetadataViewError::MissingManifest { .. } => ErrorCode::NamespaceCorrupt,
        MetadataViewError::MaintenanceRequired { .. } => ErrorCode::MaintenanceRequired,
        // A well-formed cursor from ahead of the loaded head is a state
        // condition, not a malformed request: the client's recovery is to
        // restart the listing, same as a sub-floor change cursor.
        MetadataViewError::SnapshotUnavailable { .. } => ErrorCode::RebootstrapRequired,
    }
}

fn classify_metadata_projection_load_error(error: &MetadataProjectionLoadError) -> ErrorCode {
    match error {
        MetadataProjectionLoadError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
        // The head is the namespace: an absent head is the one and only
        // "this namespace does not exist" signal.
        MetadataProjectionLoadError::LoadHead(error) => classify_control_object_load_error(error),
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
        ControlObjectLoadError::MissingObject { .. } => ErrorCode::NamespaceNotFound,
        ControlObjectLoadError::RootAheadOfHead { .. } => ErrorCode::StaleHead,
        ControlObjectLoadError::NamespaceMismatch { .. }
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
        | DurableContentValidationError::ContentDigestMismatch { .. }
        | DurableContentValidationError::ContentStoreMismatch { .. } => ErrorCode::NamespaceCorrupt,
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
        WriterEpochAcquireError::NamespaceDeleted { .. } => ErrorCode::NamespaceDeleted,
        WriterEpochAcquireError::EmptyWriterId
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
        CommitValidationError::UndeleteInodeMissing { .. } => ErrorCode::PathNotFound,
        // Both are "the deletion you named is not the live one": absent
        // entirely, or superseded by a newer generation. One code, with the
        // generations in the structured details.
        CommitValidationError::UndeleteTargetNotDeleted { .. }
        | CommitValidationError::UndeleteTargetsCurrentCommit { .. }
        | CommitValidationError::UndeleteGenerationMismatch { .. } => ErrorCode::NotDeleted,
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
        CommitHeadPublishError::EmptyExpectedHeadEtag
        | CommitHeadPublishError::NamespaceMismatch { .. }
        | CommitHeadPublishError::WalSegmentNamespaceMismatch { .. }
        | CommitHeadPublishError::WalSegmentWriterEpochMismatch { .. }
        | CommitHeadPublishError::WalSegmentBaseHeadSeqMismatch { .. }
        | CommitHeadPublishError::WalSegmentStartSeqMismatch { .. }
        | CommitHeadPublishError::WalSegmentEndSeqMismatch { .. }
        | CommitHeadPublishError::EmptyWalSegment
        | CommitHeadPublishError::HeadIdentityDrift(_)
        | CommitHeadPublishError::SeqOverflow
        | CommitHeadPublishError::Codec { .. }
        | CommitHeadPublishError::Store { .. } => ErrorCode::ServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CommitValidationError, CoreError, ErrorCode, ErrorKind, MetadataViewError, WriterFence,
    };
    use crate::commit_engine::ContentPreparationError;
    use crate::storage::content_admission::ContentTokenError;
    use loonfs_api::{
        ChangeSeq, CommitId, InodeId, ManifestId, NamespaceId, RevisionNo, WriterEpoch,
    };
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

        let error = CoreError::ContentPreparation(ContentPreparationError::ContentNotPrepared {
            content_ref_digest: "abc123".to_owned(),
        });
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
        assert!(error.message().contains("abc123"));
    }

    #[test]
    fn rejected_content_token_maps_to_content_not_prepared() {
        let error = CoreError::from(ContentPreparationError::ContentToken(
            ContentTokenError::Expired,
        ));

        assert_eq!(error.code(), ErrorCode::ContentNotPrepared);
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
        assert_eq!(details.fenced_epoch, Some(WriterEpoch(3)));
        assert_eq!(details.active_writer_epoch, Some(WriterEpoch(4)));
        assert_eq!(details.active_writer.as_deref(), Some("writer-b"));
        assert!(fenced
            .to_string()
            .contains("epoch 3 was fenced by epoch 4 (writer `writer-b`, acquired at 2000 ms)"));

        // A head with no writer block still names both epochs.
        let anonymous = CoreError::WriterFenced(WriterFence {
            fenced_epoch: WriterEpoch(3),
            active_epoch: WriterEpoch(4),
            active_writer: None,
            active_acquired_at_ms: None,
        });
        assert!(anonymous
            .to_string()
            .ends_with("epoch 3 was fenced by epoch 4"));

        let reuse = CoreError::CommitIdReuseConflict("retry-key-1".to_owned());
        let details = reuse.details().expect("reuse details");
        assert_eq!(
            details.commit_id,
            Some(CommitId::parse("retry-key-1").expect("valid commit id"))
        );

        let stale =
            CoreError::CommitValidation(CommitValidationError::ReplaceFileBaseRevisionMismatch {
                inode_id: InodeId(7),
                expected: RevisionNo(2),
                actual: Some(RevisionNo(5)),
            });
        let details = stale.details().expect("stale-revision details");
        assert_eq!(details.inode_id, Some(InodeId(7)));
        assert_eq!(details.expected_revision, Some(RevisionNo(2)));
        assert_eq!(details.actual_revision, Some(RevisionNo(5)));

        // Errors without machine-usable identity stay detail-free.
        assert!(CoreError::Internal("boom".to_owned()).details().is_none());
    }

    /// Provider auth failures keep their class across the message-flattening
    /// seams and reach the wire as `permission_denied`; every other store
    /// failure stays `server_error`.
    #[test]
    fn store_permission_denied_classifies_to_its_wire_code() {
        let denied = ObjectStoreError::PermissionDenied {
            object_key: "namespaces/demo/wal/head.json".to_owned(),
            message: "AccessDenied: bucket policy".to_owned(),
        };
        let error = CoreError::store("namespaces/demo/wal/head.json", &denied);
        assert_eq!(error.code(), ErrorCode::PermissionDenied);
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);

        let transport = ObjectStoreError::transport("namespaces/demo/wal/head.json", "timed out");
        let error = CoreError::store("namespaces/demo/wal/head.json", &transport);
        assert_eq!(error.code(), ErrorCode::ServerError);
    }
}
