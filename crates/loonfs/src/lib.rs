//! Embedded LoonFS runtime.
//!
//! The crate root is the host API: what a host needs to build, configure,
//! run, read through, write through, and maintain a runtime. The parts an
//! extension uses to keep its own index over a namespace are in [`engine`].
//!
//! The durable protocol doesn't belong here: it's in `loonfs-core`, and this
//! crate adds caching, batching, budgets, and maintenance around it.
//!
//! The crate root re-exports each type that a public runtime signature, or a
//! field of a type that one returns, uses, except an extension's types, which
//! [`engine`] re-exports. The sub-modules re-export the items their docs
//! list. Nothing else is re-exported.
//!
//! The crate has two nouns. A [`LoonFs`] is the runtime: it owns the store
//! client and the read budgets, and reads through a [`MetadataCache`] that
//! several runtimes may share. A writable runtime runs its publications,
//! folds, and compactions under an [`ExecutionBudget`] that several runtimes may
//! share too. A [`Namespace`] handle acts on one namespace, and its methods
//! take no namespace id. Both have one of two modes, [`ReadOnly`] or
//! [`Writable`]. A writable runtime also creates and forks namespaces, opens
//! the writable handle that is a namespace's writer session, and shuts down.
//! Explicit maintenance is a capability of a writable runtime:
//! [`LoonFs::maintenance`] returns a [`Maintenance`].
//!
//! ```no_run
//! # async fn run(store_config: loonfs::StoreConfig) -> loonfs::Result<()> {
//! use loonfs::{ActorId, CreateNamespaceOptions, LoonFs, NamespaceId, PutFileOptions, WriterId};
//!
//! let actor_id = ActorId::parse("usr_8f3c").expect("valid actor id");
//! let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
//!
//! let runtime = LoonFs::builder(store_config)
//!     .writer_id(WriterId::parse("server-a").expect("valid writer id"))
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
//! compaction units over its namespace while a unit is due. A runtime starts
//! no other maintenance by itself. A host that wants more, such as folding
//! idle tails, finishing compaction a session left due after its last fold,
//! or collecting garbage, calls the operations on [`LoonFs::maintenance`] on
//! a schedule of its own. Each namespace has one fold path and one
//! compaction path in a runtime, which the session and every maintenance
//! call share, so a call waits for a fold or a compaction unit in progress
//! instead of repeating it.

#![warn(missing_docs)]

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
compile_error!("the loonfs runtime needs a monotonic clock that counts host sleep; see StdMonotonicTimer in loonfs-types");

mod config;
mod execution_budget;
mod maintenance;
mod metadata_cache;
pub mod metrics;
mod namespace;
mod options;
mod pagers;
mod pinned_read;
mod publisher;
mod read_view;
mod runtime;
mod runtime_core;
/// Runtime handles that test targets build the same way, available through
/// the `test-support` feature.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
mod trace;

pub use loonfs_core::cache::SegmentFilterStats;
pub use loonfs_core::limits::{
    DIRECT_TRANSFER_URL_TTL_MS, GC_DEFAULT_GRACE_WINDOW_MS, GC_MIN_GRACE_WINDOW_MS,
    MAX_MULTIPART_PARTS, MAX_PENDING_SEGMENT_BYTES, MAX_SIGNED_PARTS_PER_REQUEST,
    RETENTION_ADVANCE_MIN_COMMIT_AGE_MS, SSD_WRITE_QUEUE_BYTES,
};
pub use loonfs_core::time::WallClock;
pub use loonfs_core::{
    classify_store_failure, CoreError, CreateNamespaceOptions, CurrentFile, CurrentFileState,
    DeleteNamespaceOptions, ErrorCode, ErrorKind, FileBytes, FileContentStream, FoldWalOutcome,
    FoldWalResponse, GcOptions, ListSnapshotFilesOptions, RetainedRevisionsPage,
    RetainedRevisionsPageCursor, RetentionTarget, SnapshotFile, SnapshotFilesPage,
    SnapshotFilesPageCursor, SnapshotPageCursor, SnapshotRecordPageCursor, StreamRange,
    WriterFence, CONTENT_READ_CHUNK_BYTES, MAX_CURRENT_FILES_PER_STAT,
};
pub use loonfs_types::{
    AbsolutePath, AccessRevisionNo, ActorId, AdvanceRetentionResponse, AttributeKey,
    AttributeValue, Attributes, AttributesProjection, AttributesRevisionNo, BindingVersion,
    CapabilityDocument, ChecksumAlgorithm, CommitId, CommitPrecondition, CommitSeq, ContentId,
    ContentRef, ContentRefKind, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DeleteSnapshotResponse, DeletedObjectCounts, DeletedSnapshotsByOwner, DestinationBehavior,
    DirectoryPageCursor, DisplayName, EffectiveLimit, EntryInodeKind, FileRevision,
    FileRevisionsPageCursor, GcResponse, GcSkippedWork, InodeId, InodeKind,
    ListFileRevisionsResponse, ListInodeChildrenResponse, ListPathEntriesResponse,
    ListSnapshotRecordsResponse, ListSnapshotsResponse, ManifestNo, MetadataMaintenanceOutcome,
    MetadataMaintenanceResponse, MetadataSkippedWork, MonotonicTimer, NameKey,
    NamespaceDiagnostics, NamespaceId, NamespaceMetadata, Page, PageRequest, PathEntry,
    PathEntryKind, PoolDeletedObjectCounts, PoolGcResponse, PoolId, PoolRetainedCandidates,
    PoolRetainedReason, PrincipalId, PrincipalScope, PrincipalSet, RecoverAdministratorResponse,
    RetainedCandidates, RetainedReason, RevisionNo, SetRetentionWindowResponse, Snapshot,
    SnapshotId, SnapshotRecord, SnapshotRecordOwner, Subject, SubjectId, TrashEntry, UploadId,
    WriterId, API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_MERGES, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT, PROTOCOL_VERSION,
};
pub use loonfs_types::{
    Commit, CompleteUploadBody, CreateUploadBody, DiffEntry, DiffEntryKind, DiffResponse,
    FilesystemEvent, ListChangesResponse, MergeConflict, MergeConflictKind, MergeConflictRevision,
    MergeConflictVersion, NextMergeRangeOutcome, NextMergeRangeResponse, ObjectTransfer,
    ReplayForkResponse, UploadContentClaim, UploadMode, UploadPartChecksumClaim, UploadSession,
    UploadSessionStatus,
};

/// The parts an extension uses to keep its own index over a namespace.
///
/// The `loonfs-grep` crate is the example. A host does not need them.
pub mod engine {
    pub use loonfs_core::cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
        DecodedBlockKey, DecodedSegmentBlock, Recency, SegmentBlockKind, SegmentCacheKey,
    };
    pub use loonfs_core::limits::{
        READ_REVALIDATION_BOUND_MS, UNREFERENCED_SEGMENT_GRACE_WINDOW_MS,
    };
    pub use loonfs_core::{
        next_run_no_after, refill_iterators, select_next_iterator, BudgetPermit, ExtensionBasis,
        ExtensionPublication, LoadedBlocks, PermitCounts, Permits, SegmentBlockLoader,
        SegmentRowIterator,
    };
    pub use loonfs_types::format::manifest::{ExtensionEntry, ExtensionRunRef};
    pub use loonfs_types::{ExtensionName, RunNo};
}

/// Commit types used by integrations that submit classified mutations to
/// the runtime publisher.
///
/// Server handlers build [`publish::CommitIntent`] values, and lower-level
/// integrations may submit [`publish::CommitCandidate`] values directly.
/// Most embedded applications do not need this module.
pub mod publish {
    pub use loonfs_core::limits::{
        MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_MESSAGE_BYTES,
        MAX_COMMIT_OPERATIONS, MAX_COMMIT_PRECONDITIONS,
    };
    pub use loonfs_core::path::parse_mutation_path;
    pub use loonfs_core::publish::{
        CommitCandidate, CommitIntent, ContentPreparationError, FilesystemOperation, InlineContent,
        PreparedContent,
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
    pub use loonfs_types::ContentToken;
}

/// Direct-upload target types used by servers to create presigned URLs.
///
/// Targets describe either one whole-object PUT or individual multipart
/// part uploads. Most embedded applications do not need this module.
pub mod uploads {
    pub use loonfs_core::{
        DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget,
        MultipartPartTargets, UploadSessionView,
    };
}

/// Direct-download target type used by servers to create a presigned
/// object-read URL. Most embedded applications do not need this module.
pub mod downloads {
    pub use loonfs_core::{DirectDownloadByInodeTarget, DirectDownloadTarget, ObjectRange};
}

/// Durable namespace control objects, as the loaders of [`NamespaceStore`]
/// return them.
///
/// The loaders bypass the runtime and are intended for layout tests and
/// operational inspection. Normal application reads and writes go through a
/// [`LoonFs`] runtime and its [`Namespace`] handles.
///
/// [`NamespaceStore`]: loonfs_core::NamespaceStore
pub mod control {
    pub use loonfs_core::control::{
        ControlObjectLoadError, LoadedControl, LoadedManifest, NamespaceReadState,
        NamespaceStatistics, VerifiedNamespaceCatalogEntry,
    };
    pub use loonfs_core::NamespaceStore;
}

pub use loonfs_objectstore::{
    ByteStream, ConfiguredObjectStoreKind, ObjectStore, ObjectStoreError, ObjectStoreErrorKind,
    SharedObjectStore, StoreConfig,
};

pub use config::{
    InlineContentPolicy, PublicationPolicy, RuntimeConfig, DEFAULT_IDLE_FOLD_AFTER_MS,
    DEFAULT_MAX_READ_CONTENT_BYTES,
};
pub use execution_budget::{
    ExecutionBudget, ExecutionBudgetBuilder, ExecutionBudgetStats,
    DEFAULT_MAX_CONCURRENT_COMPACTIONS, DEFAULT_MAX_CONCURRENT_CONTENT_WRITES,
    DEFAULT_MAX_CONCURRENT_FOLDS, DEFAULT_MAX_CONCURRENT_READS, DEFAULT_MAX_MERGE_BYTES,
    DEFAULT_MAX_READ_WORKING_BYTES,
};
pub use loonfs_core::{phase_span, Phase};
pub use maintenance::{
    MaintainMetadataReport, Maintenance, MaintenanceCancellation, MetadataCompactionOutcome,
    MetadataCompactionReport, MetadataMaintenanceReport,
};
pub use metadata_cache::{
    MetadataCache, MetadataCacheBuilder, MetadataCacheStats, SsdBlockWriter, SsdSegmentBlocks,
    SsdSegmentBlocksStats, ASSUMED_STORED_BLOCK_BYTES, DEFAULT_MAX_CACHED_OBJECT_BYTES,
    DEFAULT_MAX_COMPLETED_UPLOAD_SESSION_BYTES, DEFAULT_MAX_CONTENT_OBJECT_BYTES,
    DEFAULT_MAX_HEAD_BYTES, DEFAULT_MAX_MANIFEST_BYTES, DEFAULT_MAX_SEGMENT_BYTES,
    DEFAULT_MAX_WAL_TAIL_BYTES, SSD_INDEX_ENTRY_BYTES,
};
pub use namespace::Namespace;
pub use options::{
    AccessState, AdvanceRetentionFloorOptions, AppendFileByInodeOptions, AppendFileOptions,
    AttributeChanges, CommitOptions, Consistency, CopyOptions, CreateDirectoryOptions,
    CreateSnapshotOptions, DeleteByInodeOptions, DeleteOptions, DiffOptions,
    DirectMultipartUploadOptions, DownloadOptions, ForkNamespaceOptions, ListChangesOptions,
    ListFileRevisionsOptions, ListOptions, ListTrashOptions, MaintainMetadataOptions,
    MergeForkOptions, MoveOptions, PutFileOptions, ReadAt, ReadConsistency, ReadFileOptions,
    ReadFileStreamOptions, ReadRange, RestoreDeletedDestination, RestoreDeletedOptions,
    StatOptions, UpdateAccessByInodeOptions, UpdateAccessOptions, UpdateAttributesByInodeOptions,
    UpdateAttributesOptions,
};
pub use pagers::{
    ChangesPager, FileRevisionsPager, InodeChildrenPager, PathEntriesPager, RetainedRevisionsPager,
    SnapshotFilesPager, SnapshotRecordsPager, SnapshotsPager, TrashPager,
};
pub use publisher::{CloseNamespaceReport, NamespaceSessionStatus};
pub use read_view::ReadView;
pub(crate) use runtime::WriterState;
pub use runtime::{LoonFs, LoonFsBuilder, Mode, ReadOnly, Writable};
pub use trace::TraceMode;

/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, Error>;

/// The embedded runtime's error type.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An error reported by the underlying `loonfs-core` engine.
    #[error(transparent)]
    Core(#[from] CoreError),
    /// A request field fails runtime policy.
    #[error("invalid request: {message}")]
    InvalidRequest {
        /// Public reason the field was rejected.
        message: String,
        /// Name of the rejected header, query parameter, or request field.
        param: &'static str,
    },
    /// The runtime doesn't serve a feature, such as merges when its merge
    /// allowance is zero.
    #[error("not supported: {message}")]
    NotSupported {
        /// The capability-document key of the feature.
        feature: &'static str,
        /// Public reason the feature isn't served.
        message: String,
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
    pub fn to_error_response(&self) -> loonfs_types::ErrorResponse {
        loonfs_types::ErrorResponse {
            code: self.code().as_str().to_owned(),
            message: self.public_message().into_owned(),
            param: self.invalid_request_param(),
            feature: match self {
                Self::NotSupported { feature, .. } => Some((*feature).to_owned()),
                _ => None,
            },
            request_id: None,
            details: self.details().map(Box::new),
        }
    }

    /// Returns the stable machine-readable reason for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Core(error) => error.code(),
            Self::Config(_) | Self::InvalidRequest { .. } => ErrorCode::InvalidRequest,
            Self::NotSupported { .. } => ErrorCode::NotSupported,
            Self::RuntimeTask(_) => ErrorCode::ServerError,
        }
    }

    /// Returns the structured context the code's consumers report beside it.
    ///
    /// The embedded surface includes the same details a server puts in its
    /// error envelope for the same condition, so both backends serve one
    /// contract. Only the engine attaches any; runtime-local failures have
    /// no structured half.
    pub fn details(&self) -> Option<loonfs_types::ErrorDetails> {
        match self {
            Self::Core(error) => error.details(),
            Self::Config(_)
            | Self::RuntimeTask(_)
            | Self::InvalidRequest { .. }
            | Self::NotSupported { .. } => None,
        }
    }

    /// Identifies the rejected header, query parameter, or request field by
    /// name. A rejected field of a commit operation or precondition is the
    /// field's name, and the details carry `operation_index` or
    /// `precondition_index`.
    pub fn invalid_request_param(&self) -> Option<String> {
        match self {
            Self::InvalidRequest { param, .. } => Some((*param).to_owned()),
            Self::Core(CoreError::InvalidCursor(_)) => Some("cursor".to_owned()),
            Self::Core(
                CoreError::InvalidDiffRange { param, .. } | CoreError::InvalidMerge { param, .. },
            ) => Some((*param).to_owned()),
            Self::Core(CoreError::InvalidEventIndex(_)) => Some("event_index".to_owned()),
            Self::Core(CoreError::ResumeOffsetOutOfRange { .. }) => Some("start_offset".to_owned()),
            Self::Core(CoreError::InvalidSnapshotRequest(_)) => Some("name".to_owned()),
            Self::Core(CoreError::RootGrantsWithoutAdministrator) => Some("root_grants".to_owned()),
            Self::Core(CoreError::SubjectRequired { .. }) => {
                Some(loonfs_types::headers::PRINCIPALS.to_owned())
            }
            Self::Core(
                CoreError::FailedOperation { error, .. }
                | CoreError::FailedPrecondition { error, .. },
            ) => match error.as_ref() {
                CoreError::InvalidCommitField { field, .. } => Some((*field).to_owned()),
                _ => None,
            },
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
            | Self::NotSupported { .. } => None,
        };
        if let Some(message) = store_message {
            return message;
        }

        match self {
            Self::Config(message)
            | Self::RuntimeTask(message)
            | Self::InvalidRequest { message, .. }
            | Self::NotSupported { message, .. } => std::borrow::Cow::Owned(message.clone()),
            Self::Core(error) => std::borrow::Cow::Owned(error.to_string()),
        }
    }
}
