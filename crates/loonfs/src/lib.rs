//! Embedded LoonFS runtime.
//!
//! The crate root is the host API: what a host needs to build, configure,
//! run, read through, write through, and maintain a runtime. The parts an
//! extension uses to keep its own index over a namespace are in [`engine`].
//!
//! The crate has two nouns. A [`LoonFs`] is the runtime: it owns the store
//! client and the read budgets, and reads through a [`MetadataCache`] that
//! several runtimes may share. A writable runtime runs its publications,
//! folds, and merges under an [`ExecutionBudget`] that several runtimes may
//! share too. A [`Namespace`] handle acts on one namespace, and its methods
//! take no namespace id. Both have one of two modes, [`ReadOnly`] or
//! [`Writable`]. A writable runtime also creates and forks namespaces, opens
//! the writable handle that is a namespace's writer session, and shuts down.
//! Explicit maintenance is a capability of a writable runtime:
//! [`LoonFs::maintenance`] returns a [`Maintenance`].
//!
//! ```no_run
//! # async fn run(store_config: loonfs::StoreConfig) -> loonfs::Result<()> {
//! use loonfs::{ActorId, CreateNamespaceOptions, LoonFs, NamespaceId, PutFileOptions};
//!
//! let actor_id = ActorId::parse("usr_8f3c").expect("valid actor id");
//! let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
//!
//! let runtime = LoonFs::builder(store_config)
//!     .writer_id("server-a")
//!     .build()
//!     .await?;
//! runtime
//!     .create_namespace(&namespace_id, &actor_id)
//!     .await?;
//!
//! let namespace = runtime.open_namespace(&namespace_id)?;
//! namespace
//!     .put_file("/hello.txt", b"hello", &actor_id)
//!     .await?;
//! let file = namespace.read_file("/hello.txt").await?;
//! assert_eq!(file.bytes, b"hello");
//!
//! runtime.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! A writable session keeps its own namespace's metadata compact: it folds
//! its WAL tail at a threshold, and after each fold it publishes, it runs
//! [`Maintenance::maintain_metadata_while_due`] over its namespace. A runtime
//! starts no other maintenance by itself. A host that wants more, such as
//! folding idle tails, finishing compaction a session left due after its
//! last fold, or collecting garbage, calls the operations on
//! [`LoonFs::maintenance`] on a schedule of its own.

#![warn(missing_docs)]

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
compile_error!("the loonfs runtime needs a monotonic clock that counts host sleep; see StdMonotonicTimer in loonfs-types");

mod cache;
mod config;
mod execution_budget;
mod fs;
mod handle;
mod metadata_cache;
pub mod metrics;
mod options;
mod publisher;
mod trace;

pub use loonfs_core::cache::{
    StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
    StoredMetadataBlockKind,
};
pub use loonfs_core::limits::{
    DIRECT_TRANSFER_URL_TTL_MS, GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS,
    MAX_MULTIPART_PARTS, MAX_SIGNED_PARTS_PER_REQUEST,
};
pub use loonfs_core::time::{current_time_ms, WallClock};
pub use loonfs_core::{
    CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor,
    CreateNamespaceOptions, CurrentFileState, DeleteNamespaceOptions, Error as CoreError,
    ErrorCode, ErrorKind, FileContentStream, GcOptions, ListCheckpointFilesOptions,
    MetadataCompactionJobOutcome, MetadataCompactionPolicy, MetadataViewError, RetentionTarget,
    StoreFailureClass, WriterFence, CONTENT_READ_CHUNK_BYTES, MAX_RESOLVE_CURRENT_FILES,
};
pub use loonfs_types::api::v0::{
    Commit, CompleteMultipartUploadRequest, CompleteUploadBody, CreateUploadBody, FilesystemChange,
    ListChangesResponse, ObjectTransferAccess, UploadContentClaim, UploadMode, UploadSession,
    UploadSessionStatus,
};
pub use loonfs_types::{
    AbsolutePath, ActorId, AdvanceRetentionResponse, AttributeKey, AttributeValue, Attributes,
    AttributesProjection, AttributesRevisionNo, BindingVersion, CapabilityDocument, ChangeSeq,
    Checkpoint, CheckpointOwnerSummary, ChecksumAlgorithm, CommitId, CommitPrecondition,
    CompactionStepOutcome, ContentId, ContentRef, ContentRefKind, DeleteCheckpointResponse,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DeleteSnapshotResponse,
    DeletedCheckpointsByOwner, DeletedObjectCounts, DestinationBehavior, DirectoryPageCursor,
    DisplayName, EffectiveLimit, EntryInodeKind, FileBytes, FileRevision, FileRevisionsPageCursor,
    FoldWalOutcome, FoldWalResponse, GcResponse, InodeId, InodeKind, ListCheckpointsResponse,
    ListFileRevisionsResponse, ListInodeChildrenResponse, ListPathEntriesResponse,
    ListSnapshotsResponse, ManifestNo, MetadataCompactionOutcome, MetadataCompactionRequest,
    MetadataCompactionResponse, MetadataMaintenanceResponse, NameKey, NamespaceDiagnostics,
    NamespaceId, NamespaceMetadata, Page, PageRequest, PaginationPolicy, PathEntry, PathEntryKind,
    PinId, RetainedCandidates, RetainedReason, RevisionNo, RunMaintenanceRequest,
    RunMaintenanceResponse, SnapshotSummary, TrashEntry, UploadId, WalFoldStepOutcome, WriterId,
    API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK,
    FEATURE_SNAPSHOTS, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    PROTOCOL_VERSION,
};

/// The parts an extension uses to keep its own index over a namespace.
///
/// The `loonfs-grep` crate is the example. A host does not need them.
pub mod engine {
    pub use loonfs_core::cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
        DecodedBlockCacheStats, DecodedSegmentBlock, Recency, SegmentBlockKind, SegmentCacheKey,
    };
    pub use loonfs_core::limits::{
        METADATA_PUBLICATION_BUDGET_MS, READ_REVALIDATION_BOUND_MS, UNREFERENCED_SEGMENT_MIN_AGE_MS,
    };
    pub use loonfs_core::time::{Deadline, Observation};
    pub use loonfs_core::{
        grace_age, next_run_no_after, refill_iterators, select_next_iterator,
        write_segments_in_waves, GraceAge, SegmentBlockLoader, SegmentRowIterator,
    };
}

/// Request shapes a serving host decodes before converting them to runtime options.
pub mod requests {
    pub use loonfs_types::{
        AdvanceRetentionRequest, CreateCheckpointRequest, CreateSnapshotRequest,
        ExtendSnapshotRequest, GcRequest, MetadataCompactionRequest, MetadataMaintenanceRequest,
        RunMaintenanceRequest,
    };
}

/// Commit types used by integrations that submit classified mutations to
/// the runtime publisher.
///
/// Server handlers build [`publish::CommitRequest`] values, and lower-level
/// integrations may submit [`publish::CommitCandidate`] values directly.
/// Most embedded applications do not need this module.
pub mod publish {
    pub use loonfs_core::limits::{
        MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_MESSAGE_BYTES,
        MAX_COMMIT_OPERATIONS, MAX_COMMIT_PRECONDITIONS,
    };
    pub use loonfs_core::path::parse_mutation_path;
    pub use loonfs_core::publish::{
        CommitCandidate, CommitRequest, ContentPreparationError, FilesystemOperation,
        InlineContent, PreparedContent,
    };
}

/// Content-preparation proof types used by server integrations.
///
/// A server mints a short-lived token after durable upload completion.
/// [`Namespace::prepare_content_token`] verifies the token against the
/// namespace catalog and returns process-local proof that keeps the token's
/// publication deadline.
/// Most embedded applications do not need this module.
pub mod content_tokens {
    pub use loonfs_core::content::{
        mint_content_token, CompletedUpload, CompletedUploadEvidence, ContentTokenError,
    };
    pub use loonfs_types::api::v0::ContentToken;
}

/// Direct-upload target types used by servers to create presigned URLs.
///
/// Targets describe either one whole-object PUT or individual multipart
/// part uploads. Most embedded applications do not need this module.
pub mod uploads {
    pub use loonfs_core::{
        DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget,
        MultipartPartTargets, ResolvedUploadCompletion, UploadSessionView,
    };
}

/// Direct-download target type used by servers to create a presigned
/// object-read URL. Most embedded applications do not need this module.
pub mod downloads {
    pub use loonfs_core::{DirectDownloadByInodeTarget, DirectDownloadTarget, GrantedRange};
}

/// Typed loaders for inspecting durable namespace control objects.
///
/// These functions bypass the runtime and are intended for layout tests and
/// operational inspection. Normal application reads and writes go through a
/// [`LoonFs`] runtime and its [`Namespace`] handles.
pub mod control {
    pub use loonfs_core::control::{
        load_checkpoint_statistics, load_namespace_catalog_entry, load_namespace_current_manifest,
        load_namespace_read_state, load_namespace_statistics, ControlObjectLoadError,
        CurrentManifest, LoadedControl, LoadedManifest, NamespaceReadState, NamespaceStatistics,
        VerifiedNamespaceCatalogEntry,
    };
}

pub use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig,
};

pub use config::{InlineContentPolicy, PublicationLimits};
pub use execution_budget::{
    ExecutionBudget, ExecutionBudgetBuilder, ExecutionBudgetStats,
    DEFAULT_MAX_CONCURRENT_COMPACTIONS, DEFAULT_MAX_CONCURRENT_FOLDS, DEFAULT_MAX_CONCURRENT_READS,
    DEFAULT_MAX_CONTENT_MERGE_BYTES, DEFAULT_MAX_READ_WORKING_BYTES,
};
pub use fs::{
    ChangesPager, CheckpointFilesPager, CheckpointsPager, FileRevisionsPager, InodeChildrenPager,
    PathEntriesPager, ReadView, SnapshotPolicy, SnapshotsPager, TrashPager,
};
pub use handle::{
    LoonFs, LoonFsBuilder, Maintenance, MaintenanceCancellation, Namespace, ReadOnly, Writable,
};
pub use metadata_cache::{
    MetadataCache, MetadataCacheBuilder, MetadataCacheStats, DEFAULT_MAX_HEAD_STATE_BYTES,
    DEFAULT_MAX_SEGMENT_BYTES,
};
pub use options::{
    AccessState, AdvanceRetentionOptions, AppendFileByInodeOptions, AppendFileOptions,
    AttributeChanges, CommitOptions, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    DeleteByInodeOptions, DeleteOptions, DirectMultipartUploadOptions, DownloadOptions,
    ForkNamespaceOptions, ListOptions, MetadataMaintenanceOptions, MoveOptions, PutFileOptions,
    ReadFileStreamOptions, StatOptions, UndeleteDestination, UndeleteOptions,
    UpdateAccessByInodeOptions, UpdateAccessOptions, UpdateAttributesByInodeOptions,
    UpdateAttributesOptions,
};
pub use publisher::{CloseNamespaceReport, NamespaceSessionState};
pub use trace::{payload_class, TraceMode, TraceStoreKind};

/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, Error>;

/// The embedded runtime's error type.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An error surfaced by the underlying `loonfs-core` engine.
    #[error(transparent)]
    Core(#[from] CoreError),
    /// A newer manifest replaced a captured view whose segment is missing.
    #[error("captured manifest `{expected_manifest_no}` has a missing segment; current manifest is `{actual_manifest_no}`")]
    StaleHead {
        /// Manifest captured by the read.
        expected_manifest_no: ManifestNo,
        /// Current manifest observed after the missing segment.
        actual_manifest_no: ManifestNo,
    },
    /// A request field fails runtime policy.
    #[error("invalid request: {message}")]
    InvalidRequest {
        /// Public reason the field was rejected.
        message: String,
        /// Header, query parameter, or JSON Pointer identifying the field.
        param: &'static str,
    },
    /// The runtime configuration is invalid.
    #[error("invalid runtime config: {0}")]
    Config(String),
    /// A task run on behalf of the runtime failed.
    #[error("runtime task failed: {0}")]
    RuntimeTask(String),
}

impl Error {
    /// Returns this error as the public API error body.
    pub fn to_api_error(&self) -> loonfs_types::ApiError {
        loonfs_types::ApiError {
            code: self.code().as_str().to_owned(),
            message: self.public_message().into_owned(),
            param: self.invalid_request_param(),
            feature: None,
            request_id: None,
            details: self.details().map(Box::new),
        }
    }

    /// Returns the stable machine-readable reason for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Core(error) => error.code(),
            Self::StaleHead { .. } => ErrorCode::StaleHead,
            Self::Config(_) | Self::InvalidRequest { .. } => ErrorCode::InvalidRequest,
            Self::RuntimeTask(_) => ErrorCode::ServerError,
        }
    }

    /// Returns the structured context the code's consumers report beside it.
    ///
    /// The embedded surface carries the same details a server puts in its
    /// error envelope for the same condition, so both backends serve one
    /// contract. Only the engine attaches any; runtime-local failures have
    /// no structured half.
    pub fn details(&self) -> Option<loonfs_types::ErrorDetails> {
        match self {
            Self::Core(error) => error.details(),
            Self::Config(_)
            | Self::RuntimeTask(_)
            | Self::InvalidRequest { .. }
            | Self::StaleHead { .. } => None,
        }
    }

    /// Identifies the rejected header, query parameter, or body field.
    pub fn invalid_request_param(&self) -> Option<String> {
        match self {
            Self::InvalidRequest { param, .. } => Some((*param).to_owned()),
            Self::Core(CoreError::InvalidCursor(_)) => Some("cursor".to_owned()),
            Self::Core(CoreError::ResumeOffsetOutOfRange { .. }) => Some("start_offset".to_owned()),
            Self::Core(CoreError::InvalidCheckpointRequest(_)) => Some("/name".to_owned()),
            Self::Core(CoreError::SubjectRequired { .. }) => Some("Loonfs-Principals".to_owned()),
            Self::Core(CoreError::FailedOperation {
                operation_index,
                source,
            }) => match source.as_ref() {
                CoreError::InvalidCommitField { field, .. } => {
                    Some(format!("/operations/{operation_index}/{field}"))
                }
                _ => None,
            },
            Self::Core(CoreError::InvalidCommitField {
                field,
                precondition_index: Some(precondition_index),
                ..
            }) => Some(format!("/preconditions/{precondition_index}/{field}")),
            _ => None,
        }
    }

    /// Returns an error message safe to show to users.
    pub fn public_message(&self) -> std::borrow::Cow<'static, str> {
        let store_message = match self {
            Self::Core(error) => error.object_store_public_message(),
            Self::Config(_)
            | Self::RuntimeTask(_)
            | Self::InvalidRequest { .. }
            | Self::StaleHead { .. } => None,
        };
        if let Some(message) = store_message {
            return message;
        }

        match self {
            Self::Config(message)
            | Self::RuntimeTask(message)
            | Self::InvalidRequest { message, .. } => std::borrow::Cow::Owned(message.clone()),
            Self::Core(error) => std::borrow::Cow::Owned(error.to_string()),
            Self::StaleHead { .. } => std::borrow::Cow::Owned(self.to_string()),
        }
    }
}
