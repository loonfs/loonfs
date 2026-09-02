//! Embedded LoonFS runtime.
//!
//! Use [`FsWriter`] for mutations, [`FsReader`] for reads, and [`FsAdmin`] for
//! explicit maintenance. [`FsBackgroundWork`] controls whether a writer also
//! schedules non-destructive maintenance.
//!
//! ```no_run
//! # async fn open(store_config: loonfs::StoreConfig) -> loonfs::Result<()> {
//! let writer = loonfs::FsWriter::builder(store_config)
//!     .writer_id("server-a")
//!     .background_work(loonfs::FsBackgroundWork::Enabled)
//!     .build()
//!     .await?;
//! let namespace_id = loonfs::NamespaceId::parse("demo").expect("valid namespace id");
//! writer
//!     .create_namespace(&namespace_id, loonfs::CreateNamespaceOptions::default())
//!     .await?;
//! let commit = loonfs::CommitOptions::new(loonfs::ActorRef::service(
//!     loonfs::ActorId::parse("document-worker").expect("valid actor id"),
//! ));
//! writer
//!     .put_file_bytes(
//!         &namespace_id,
//!         "/report.txt",
//!         b"report",
//!         loonfs::PutFileOptions {
//!             behavior: loonfs::DestinationBehavior::Replace,
//!             commit,
//!             expected_inode_id: None,
//!             expected_revision_no: None,
//!         },
//!     )
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Attribute types are available directly from this crate, so embedded
//! applications do not need a separate `loonfs-api` dependency:
//!
//! ```
//! use loonfs::{ActorId, ActorRef, AttributeKey, AttributeValue, UpdateAttributesOptions};
//!
//! let key = AttributeKey::parse("owner").expect("valid attribute key");
//! let mut options = UpdateAttributesOptions::new(ActorRef::user(
//!     ActorId::parse("example-user").expect("valid actor id"),
//! ));
//! options.set = [
//!     (key, AttributeValue::parse("platform").expect("valid attribute value")),
//! ]
//! .into_iter()
//! .collect();
//! assert_eq!(options.set.len(), 1);
//! ```
//!
//! Writers schedule maintenance only for namespaces touched by or assigned to
//! the process. [`FsWriter::shutdown`] drains that work. Readers and admins do
//! not start background tasks.
#![warn(missing_docs)]

mod cache;
mod config;
mod fs;
mod handle;
mod maintenance_runner;
pub mod metrics;
mod options;
pub mod publisher;
mod trace;

use thiserror::Error;

pub use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, CommitResponse, CommittedChange,
    CompleteMultipartUploadRequest, CompleteUploadRequest, FilesystemChange, ListChangesResponse,
    ObjectTransferAccess, UploadContentClaim, UploadContentResponse, UploadMode, UploadSession,
    UploadSessionStatus,
};
pub use loonfs_api::{
    ActorId, ActorKind, ActorRef, AdvanceRetentionResponse, AttributeKey, AttributeRevisionNo,
    AttributeValue, Attributes, AttributesProjection, CapabilityDocument, ChangeSeq, Checkpoint,
    CheckpointId, CheckpointOwnerSummary, ChecksumAlgorithm, CommitId, ContentId, ContentRef,
    ContentRefKind, DeleteDirectoryBehavior, DeleteNamespaceResponse, DeletedObjectCounts,
    DestinationBehavior, DirectoryPageCursor, EffectiveLimit, FileBytes, FileRevision,
    FileRevisionsPageCursor, FlushWalOutcome, FlushWalResponse, GcResponse, InodeId, InodeKind,
    ListCheckpointsResponse, ListFileRevisionsResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, ListSnapshotsResponse, MaintenanceStepResponse, ManifestNo,
    MetadataMaintenanceResponse, NameKey, Namespace, NamespaceDiagnostics, NamespaceId, Page,
    PageRequest, PaginationPolicy, PathEntry, PathEntryKind, ReleaseCheckpointResponse,
    ReleaseSnapshotResponse, ReleasedCheckpointCounts, ReorganizeStepOutcome, RetainedCandidates,
    RetainedReason, RevisionNo, SnapshotSummary, TrashEntry, UploadId, WalFlushStepOutcome,
    FEATURE_ATTRIBUTES, FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_INODES_LIST_CHILDREN,
    FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK,
    FEATURE_SNAPSHOTS, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
pub use loonfs_core::cache::{
    DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
    DecodedBlockCacheStats, DecodedBlockWeight, DecodedSegmentBlock, MetadataSegmentCacheConfig,
    Recency, SegmentBlockKind, SegmentCacheKey, StoredMetadataBlockCache,
    StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
pub use loonfs_core::limits::{
    DEFAULT_GC_MAX_OBJECTS, GC_MIN_GRACE_WINDOW_MS, MAX_MULTIPART_PARTS,
    MAX_SIGNED_PARTS_PER_REQUEST, METADATA_PUBLICATION_BUDGET_MS,
};
pub use loonfs_core::time::current_time_ms;
pub use loonfs_core::{
    delete_if_aged, ensure_metadata_publication_budget, next_run_no_after, refill_iterators,
    select_next_iterator, write_segments_in_waves, BootstrapNamespaceError, CheckpointFile,
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor, CurrentFileState,
    DeleteNamespaceOptions, Error as CoreError, ErrorCode, ErrorKind, FileContentStream, GcConfig,
    GcCursorKeyspace, GraceAge, MetadataCompactionJobOutcome, MetadataViewError, NamespaceGcCursor,
    PassBudget, SegmentBlockLoader, SegmentRowIterator, StoreFailureClass, WriterFence,
    CONTENT_READ_CHUNK_BYTES, MAX_RESOLVE_CURRENT_FILES,
};
pub use publisher::{NamespaceAdvanceHint, NamespaceAdvanceObserver};

/// Request shapes a serving host decodes before converting them to runtime options.
pub mod wire {
    pub use loonfs_api::{
        AdvanceRetentionRequest, CreateCheckpointRequest, CreateSnapshotRequest,
        ExtendSnapshotRequest, GcRequest, MaintenanceStepRequest, MetadataMaintenanceRequest,
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
        MAX_COMMIT_OPERATIONS,
    };
    pub use loonfs_core::path::parse_mutation_path;
    pub use loonfs_core::publish::{
        CommitCandidate, CommitRequest, ContentPreparationError, FilesystemOperation,
        PreparedContent,
    };
}

/// Content-preparation proof types used by server integrations.
///
/// A server mints a short-lived token after durable upload completion.
/// [`FsWriter::prepare_content_token`] verifies the token against the
/// namespace catalog and returns process-local proof that keeps the token's
/// publication deadline.
/// Most embedded applications do not need this module.
pub mod content_tokens {
    pub use loonfs_api::v0::ContentToken;
    pub use loonfs_core::content::{
        mint_content_token, CompletedUpload, CompletedUploadReceipt, ContentTokenError,
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
        ResolvedUploadCompletion,
    };
}

/// Direct-download target type used by servers to create a presigned
/// object-read URL. Most embedded applications do not need this module.
pub mod downloads {
    pub use loonfs_core::{DirectDownloadByInodeTarget, DirectDownloadTarget};
}

/// Typed loaders for inspecting durable namespace control objects.
///
/// These functions bypass runtime handles and are intended for layout tests
/// and operational inspection. Normal application reads and writes should
/// use [`FsReader`] and [`FsWriter`].
pub mod control {
    pub use loonfs_core::control::{
        load_namespace_catalog_entry, load_namespace_head_control,
        load_namespace_metadata_root_control, ControlObjectLoadError, LoadedControl,
        NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
}

pub use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig,
};

pub use cache::RuntimeCacheStats;
pub use config::{RuntimeCacheConfig, DEFAULT_MAX_CONCURRENT_MAINTENANCE};
pub use fs::{
    ChangesPager, CheckpointsPager, FileRevisionsPager, FsReadSnapshot, InodeChildrenPager,
    MetadataCompactionOutcome, PathEntriesPager, TrashPager,
};
pub use handle::{FsAdmin, FsAdminBuilder, FsReader, FsReaderBuilder, FsWriter, FsWriterBuilder};
pub use maintenance_runner::{
    FsBackgroundWork, MaintenanceHandle, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceStepConclusion, MaintenanceStepReport, NamespacePublication,
};
pub use options::{
    CommitOptions, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, CreateSnapshotOptions, DeleteOptions, DirectMultipartUploadOptions,
    ListChangesOptions, ListInodeChildrenOptions, ListPathEntriesOptions, MaintenancePlan,
    MetadataMaintenanceOptions, MoveOptions, PutFileOptions, ReadFileStreamOptions,
    RestoreRevisionOptions, StatPathOptions, UndeleteOptions, UpdateAttributesOptions,
};
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
    /// Bootstrapping a namespace failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapNamespaceError),
    /// The runtime configuration is invalid.
    #[error("invalid runtime config: {0}")]
    Config(String),
    /// A task run on behalf of the runtime failed.
    #[error("runtime task failed: {0}")]
    RuntimeTask(String),
}

impl RuntimeError {
    /// Returns the stable machine-readable reason for this error.
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::Core(error) => error.code(),
            Self::Bootstrap(error) => error.code(),
            Self::Config(_) => ErrorCode::InvalidRequest,
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
            Self::Bootstrap(error) => error.details(),
            Self::Config(_) | Self::RuntimeTask(_) => None,
        }
    }

    /// Returns an error message safe to show to users.
    pub fn public_message(&self) -> std::borrow::Cow<'static, str> {
        let store_message = match self {
            Self::Core(error) => error.object_store_public_message(),
            Self::Bootstrap(error) => error.object_store_public_message(),
            Self::Config(_) | Self::RuntimeTask(_) => None,
        };
        if let Some(message) = store_message {
            return message;
        }

        match self {
            Self::Config(message) | Self::RuntimeTask(message) => {
                std::borrow::Cow::Owned(message.clone())
            }
            Self::Core(error) => std::borrow::Cow::Owned(error.to_string()),
            Self::Bootstrap(error) => std::borrow::Cow::Owned(error.to_string()),
        }
    }
}
