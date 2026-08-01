//! Embedded LoonFS runtime.
//!
//! `loonfs` is the ergonomic runtime layer. It wraps `loonfs-core` with caching,
//! upload helpers, maintenance hooks, and an optional [`metrics`] surface. Use
//! it when you want LoonFS in-process, or when building the reference server.
//!
//! The runtime is opened through purpose-specific handles, each built
//! asynchronously inside the Tokio runtime that will own it:
//!
//! - [`FsWriter`] for writes, with [`FsBackgroundWork`] controlling whether
//!   the writer schedules non-destructive maintenance after writes.
//! - [`FsReader`] for read-only latest views.
//! - [`FsAdmin`] for explicit maintenance: status, checkpoints, retention,
//!   and garbage collection.
//!
//! ```no_run
//! # async fn open(store_config: loonfs::StoreConfig) -> loonfs::Result<()> {
//! let writer = loonfs::FsWriter::builder(store_config)
//!     .writer_id("server-a")
//!     .background_work(loonfs::FsBackgroundWork::Enabled)
//!     .build()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Background work belongs to the writer — publications, and the maintenance
//! a publication schedules. One [`MaintenanceJob`] runner admits every
//! background step: the writer nudges it, it decides when to run, and the
//! executors re-read durable state each time. It covers the namespaces this
//! process touches and the ones a host explicitly assigns to it, and it
//! discovers none. LoonFS never creates a hidden maintenance runtime: that
//! work is spawned on the writer's own owning runtime, and
//! [`FsWriter::shutdown_background`] settles it. Readers and admins start no
//! background work at all, so they have nothing to shut down.
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
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitResponse, CommittedChange,
    CompleteUploadRequest, CompleteUploadResponse, DirectPutContentClaim, DirectPutUpload,
    FilesystemChange, ObjectTransferAccess, UploadContentResponse, UploadMode,
};
pub use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument,
    ChangeSeq, CheckpointId, ChecksumAlgorithm, CommitId, ContentId, ContentRef, ContentRefKind,
    CreateCheckpointRequest, CreateCheckpointResponse, DeleteDirectoryBehavior,
    DeleteNamespaceResponse, DestinationBehavior, DirectoryPageCursor, EffectiveLimit,
    FileRevision, FileRevisionsPageCursor, FlushWalOutcome, FlushWalResponse, GcRequest,
    GcResponse, InodeId, InodeKind, ListFileRevisionsResponse, ListPathEntriesResponse,
    MaintenanceStepKind, MaintenanceStepRequest, MaintenanceStepResponse, ManifestId, NameKey,
    NamespaceId, NamespaceStatusResponse, NamespaceSummary, Page, PageRequest, PaginationPolicy,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RevisionNo, UploadId, WalFlushStepOutcome,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE,
    FEATURE_NAMESPACES_FORK, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
pub use loonfs_core::cache::{MetadataTableCacheConfig, Recency};
pub use loonfs_core::limits::{
    DEFAULT_GC_MAX_OBJECTS, GC_MIN_GRACE_WINDOW_MS, METADATA_PUBLICATION_BUDGET_MS,
};
pub use loonfs_core::time::current_time_ms;
pub use loonfs_core::{
    delete_if_aged, BootstrapNamespaceError, CheckpointFile, CheckpointFilesPage,
    CheckpointFilesPageCursor, CurrentFileState, DeleteNamespaceOptions, Error as CoreError,
    ErrorCode, ErrorKind, FileContentStream, GcConfig, MetadataViewError, PassBudget,
    StoreFailureClass, WriterFence, CONTENT_READ_CHUNK_BYTES, MAX_RESOLVE_CURRENT_FILES,
};
pub use publisher::PublishObserver;

/// Integration seam: the vocabulary for handing classified commit work to
/// the runtime's batch publication surface. The server's filesystem
/// handlers build [`publish::CommitRequest`]s for the [`publisher`]
/// front-end, which also accepts
/// [`publish::CommitCandidate`]s directly. Most embedded users
/// never need this module.
pub mod publish {
    pub use loonfs_core::limits::{
        MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_MESSAGE_BYTES,
        MAX_COMMIT_OPERATIONS,
    };
    pub use loonfs_core::path::{ensure_mutation_path, parse_mutation_path};
    pub use loonfs_core::publish::{
        CommitCandidate, CommitRequest, ContentPreparation, ContentPreparationError,
        FilesystemOperation, PreparedContent,
    };
}

/// Server-integration seam for producer-only content preparation proofs.
///
/// A serving session mints short-lived wire tokens after durable preparation;
/// [`FsWriter::prepare_content_token`] verifies them against the namespace's
/// catalog binding. The resulting admission records accepted durability
/// evidence and does not expire. Most embedded users never need this module.
pub mod content_tokens {
    pub use loonfs_core::content::{
        mint_content_token, CompletedUpload, CompletedUploadReceipt, ContentTokenError,
    };
}

/// Server-integration seam: the resolved direct upload targets a serving
/// session turns into presigned URLs for client-side object writes — one
/// whole-object PUT, or one PUT per multipart part. Most embedded users
/// never need this module.
pub mod uploads {
    pub use loonfs_core::{
        BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
        DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget,
        MultipartPartTargets,
    };
}

/// Server-integration seam: the resolved direct download target a serving
/// read turns into a presigned URL for a client-side object read. The
/// mirror of [`uploads`], and most embedded users never need it either.
pub mod downloads {
    pub use loonfs_core::DirectDownloadTarget;
}

/// White-box seam over the durable control plane: typed loaders for a
/// namespace's head, metadata root, and catalog entry.
///
/// These read durable control objects directly rather than going through a
/// handle, which is what makes them useful for asserting on layout in tests
/// and for operational inspection. Normal reads and writes go through
/// [`FsReader`] and [`FsWriter`]; nothing here is needed to use the runtime.
pub mod control {
    pub use loonfs_core::control::{
        load_namespace_catalog_entry, load_namespace_head_control,
        load_namespace_metadata_root_control, ControlObjectLoadError, LoadedHeadControl,
        LoadedMetadataRootControl, NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
}

pub use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig,
};

pub use cache::RuntimeCacheStats;
pub use config::{RuntimeCacheConfig, DEFAULT_MAX_CONCURRENT_MAINTENANCE};
pub use handle::{FsAdmin, FsAdminBuilder, FsReader, FsReaderBuilder, FsWriter, FsWriterBuilder};
pub use maintenance_runner::{
    FsBackgroundWork, MaintenanceHandle, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceStepConclusion, MaintenanceStepResult,
};
pub use options::{
    gc_config_from_request, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteOptions, ListChangesOptions, MaintenanceStepOptions, MoveOptions,
    PutFileOptions, ReadFileStreamOptions, RestoreRevisionOptions, UndeleteOptions,
};
pub use trace::{payload_class, TraceMode, TraceStoreKind};

/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, RuntimeError>;

pub use self::RuntimeError as Error;

/// The embedded runtime's error type, also exported as [`Error`].
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
}
