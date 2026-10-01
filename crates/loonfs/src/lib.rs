//! Embedded LoonFS runtime.
//!
//! The crate has two nouns. A [`LoonFs`] is the runtime: it owns the store
//! client, the caches, and the read budgets. A [`Namespace`] handle acts on
//! one namespace, and its methods take no namespace id. Both have one of two
//! modes, [`ReadOnly`] or [`Writable`]. A writable runtime also creates and
//! forks namespaces, opens the writable handle that is a namespace's writer
//! session, and shuts down. Explicit maintenance is a capability of a
//! writable runtime: [`LoonFs::maintenance`] returns a [`Maintenance`].
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
//!     .create_namespace(&namespace_id, CreateNamespaceOptions::new(actor_id.clone()))
//!     .await?;
//!
//! let namespace = runtime.open_namespace(&namespace_id)?;
//! namespace
//!     .put_file_bytes("/hello.txt", b"hello", PutFileOptions::new(actor_id))
//!     .await?;
//! let file = namespace.get_file_bytes("/hello.txt").await?;
//! assert_eq!(file.bytes, b"hello");
//!
//! runtime.shutdown().await?;
//! # Ok(()) }
//! ```
//!
//! A runtime starts no maintenance by itself. A host that wants scheduled
//! maintenance registers jobs built over [`LoonFs::maintenance`] with a
//! [`MaintenanceRunner`], which is the only scheduler and is optional.

#![warn(missing_docs)]

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
compile_error!("the loonfs runtime needs a monotonic clock that counts host sleep; see StdMonotonicTimer in loonfs-api");

mod cache;
mod config;
mod fs;
mod handle;
mod maintenance;
pub mod metrics;
mod options;
pub mod publisher;
mod trace;

use thiserror::Error;

pub use loonfs_api::v0::{
    Commit, CompleteMultipartUploadRequest, CompleteUploadBody, CreateUploadBody, FilesystemChange,
    ListChangesResponse, ObjectTransferAccess, UploadContentClaim, UploadMode, UploadSession,
    UploadSessionStatus,
};
pub use loonfs_api::{
    ActorId, AdvanceRetentionResponse, AttributeKey, AttributeValue, Attributes,
    AttributesProjection, AttributesRevisionNo, CapabilityDocument, ChangeSeq, Checkpoint,
    CheckpointOwnerSummary, ChecksumAlgorithm, CommitId, CommitPrecondition, CompactionStepOutcome,
    ContentId, ContentRef, ContentRefKind, DeleteCheckpointResponse, DeleteDirectoryBehavior,
    DeleteNamespaceResponse, DeleteSnapshotResponse, DeletedCheckpointsByOwner,
    DeletedObjectCounts, DestinationBehavior, DirectoryPageCursor, EffectiveLimit, FileBytes,
    FileRevision, FileRevisionsPageCursor, FoldWalOutcome, FoldWalResponse, GcResponse, InodeId,
    InodeKind, ListCheckpointsResponse, ListFileRevisionsResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, ListSnapshotsResponse, ManifestNo, MetadataCompactionOutcome,
    MetadataCompactionRequest, MetadataCompactionResponse, MetadataMaintenanceResponse, NameKey,
    NamespaceDiagnostics, NamespaceId, NamespaceMetadata, Page, PageRequest, PaginationPolicy,
    PathEntry, PathEntryKind, PinId, RetainedCandidates, RetainedReason, RevisionNo,
    RunMaintenanceRequest, RunMaintenanceResponse, SnapshotSummary, TrashEntry, UploadId,
    WalFoldStepOutcome, WriterId, API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE,
    FEATURE_NAMESPACES_FORK, FEATURE_SNAPSHOTS, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, PROTOCOL_VERSION,
};
pub use loonfs_core::cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
    DecodedBlockCacheStats, DecodedBlockWeight, DecodedSegmentBlock, MetadataSegmentCacheConfig,
    Recency, SegmentBlockKind, SegmentCacheKey, StoredMetadataBlockCache,
    StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
pub use loonfs_core::limits::{
    DIRECT_TRANSFER_URL_TTL_MS, GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS,
    MAX_MULTIPART_PARTS, MAX_SIGNED_PARTS_PER_REQUEST, METADATA_PUBLICATION_BUDGET_MS,
    READ_REVALIDATION_BOUND_MS, UNREFERENCED_SEGMENT_MIN_AGE_MS,
};
pub use loonfs_core::time::{current_time_ms, Deadline, Observation, WallClock};
pub use loonfs_core::{
    delete_if_aged, grace_age, next_run_no_after, refill_iterators, select_next_iterator,
    write_segments_in_waves, CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor,
    CheckpointPageCursor, CreateNamespaceOptions, CurrentFileState, DeleteNamespaceOptions,
    Error as CoreError, ErrorCode, ErrorKind, FileContentStream, GcConfig, GraceAge,
    ListCheckpointFilesOptions, MetadataCompactionJobOutcome, MetadataCompactionPolicy,
    MetadataViewError, SegmentBlockLoader, SegmentRowIterator, StoreFailureClass, WriterFence,
    CONTENT_READ_CHUNK_BYTES, MAX_RESOLVE_CURRENT_FILES,
};
pub use publisher::{NamespaceAdvanceHint, NamespaceAdvanceObserver};

/// Request shapes a serving host decodes before converting them to runtime options.
pub mod requests {
    pub use loonfs_api::{
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
    pub use loonfs_api::v0::ContentToken;
    pub use loonfs_core::content::{
        mint_content_token, CompletedUpload, CompletedUploadEvidence, ContentTokenError,
    };
}

/// Direct-upload target types used by servers to create presigned URLs.
///
/// Targets describe either one whole-object PUT or individual multipart
/// part uploads. Most embedded applications do not need this module.
pub mod uploads {
    pub use loonfs_core::{
        BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
        DirectMultipartUploadTarget, MultipartPartTarget, MultipartPartTargets,
        ResolvedUploadCompletion, UploadSessionView,
    };
}

/// Direct-download target type used by servers to create a presigned
/// object-read URL. Most embedded applications do not need this module.
pub mod downloads {
    pub use loonfs_core::{DirectDownloadByInodeTarget, DirectDownloadTarget};
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

pub use cache::RuntimeCacheStats;
pub use config::{
    InlineContentOptions, PublicationLimits, RuntimeCacheConfig,
    DEFAULT_MAX_CONCURRENT_COMPACTIONS, DEFAULT_MAX_CONCURRENT_FOLDS,
    DEFAULT_MAX_CONCURRENT_MAINTENANCE,
};
pub use fs::{
    ChangesPager, CheckpointsPager, FileRevisionsPager, FsReadSnapshot, InodeChildrenPager,
    PathEntriesPager, SnapshotPolicy, SnapshotsPager, TrashPager,
};
pub use handle::{LoonFs, LoonFsBuilder, Maintenance, Namespace, ReadOnly, Writable};
pub use maintenance::{
    maintenance_hint_relay, GarbageCollectionJob, MaintenanceAssignment, MaintenanceCancellation,
    MaintenanceConclusion, MaintenanceHandle, MaintenanceHint, MaintenanceHintObserver,
    MaintenanceHintReceiver, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceRegistry, MaintenanceRunReport, MaintenanceRunner, MaintenanceRunnerBuilder,
    MaintenanceRunnerStats, MetadataCompactionJob, MetadataMaintenanceJob, NamespacePublication,
};
pub use options::{
    CommitOptions, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateSnapshotOptions, DeleteOptions, DirectMultipartUploadOptions, ForkNamespaceOptions,
    ListChangesOptions, ListInodeChildrenOptions, ListPathEntriesOptions,
    MetadataMaintenanceOptions, MoveOptions, PutFileOptions, ReadFileStreamOptions,
    RestoreRevisionOptions, StatPathOptions, UndeleteOptions, UpdateAccessOptions,
    UpdateAttributesOptions,
};
pub use publisher::{CloseNamespaceReport, NamespaceSessionState};
pub use trace::{payload_class, TraceMode, TraceStoreKind};

/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, RuntimeError>;

pub use self::RuntimeError as Error;

/// The embedded runtime's error type, also exported as [`enum@Error`].
#[derive(Debug, Clone, Error)]
#[non_exhaustive]
pub enum RuntimeError {
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

impl RuntimeError {
    /// Returns this error as the public API error body.
    pub fn to_api_error(&self) -> loonfs_api::ApiError {
        loonfs_api::ApiError {
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
    pub fn details(&self) -> Option<loonfs_api::ErrorDetails> {
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
