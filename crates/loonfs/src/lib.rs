//! Embedded LoonFS runtime.
//!
//! `loonfs` is the ergonomic runtime layer. It wraps `loonfs-core` with caching,
//! upload helpers, maintenance hooks, and optional object-store metrics. Use it
//! when you want LoonFS in-process, or when building the reference server.
//!
//! The runtime is opened through purpose-specific handles, each built
//! asynchronously inside the Tokio runtime that will own it:
//!
//! - [`FsWriter`] for writes, with [`FsBackgroundWork`] controlling whether
//!   the writer schedules non-destructive maintenance after writes.
//! - [`FsReader`] for read-only latest views.
//! - [`FsAdmin`] for explicit maintenance: status, checkpoints, retention,
//!   garbage collection, and incomplete-installation repair.
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
//! LoonFS never creates a hidden maintenance runtime: any background work a
//! handle starts is spawned on that handle's owning runtime, and each handle
//! has an async `shutdown_background()` that settles what it owns.
#![warn(missing_docs)]

mod background;
mod cache;
mod config;
mod fs;
mod handle;
mod options;
pub mod publisher;
mod time;
mod trace;
mod writer_session;

use thiserror::Error;

pub use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitOp, CommitPrecondition,
    CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest, CompleteUploadResponse,
    DirectPutUpload, DisableGrepIndexResponse, EnableGrepIndexResponse, FilesystemChange,
    ObjectTransferAccess, RepairNamespaceResponse, UploadContentResponse, UploadMode,
};
pub use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument,
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentRefKind, CreateCheckpointRequest,
    CreateCheckpointResponse, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DestinationBehavior, DirectoryPageCursor, EffectiveLimit, FileRevision,
    FileRevisionsPageCursor, FlushWalOutcome, FlushWalResponse, GcRequest, GcResponse, GrepMatch,
    GrepRequest, GrepResponse, InodeId, InodeKind, ListFileRevisionsResponse,
    ListPathEntriesResponse, MaintenanceStepKind, MaintenanceStepRequest, MaintenanceStepResponse,
    ManifestId, NameKey, NamespaceId, NamespaceStatusResponse, NamespaceSummary, Page, PageRequest,
    PaginationPolicy, ReleaseCheckpointResponse, ReorganizeStepOutcome, RepairNamespaceOutcome,
    RevisionNo, UploadId, WalFlushStepOutcome, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_PUT, PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0,
    PROTOCOL_VERSION,
};
pub use loonfs_core::cache::MetadataTableCacheConfig;
pub use loonfs_core::limits::DEFAULT_GC_MAX_OBJECTS;
pub use loonfs_core::{
    BootstrapNamespaceError, DeleteNamespaceOptions, Error as CoreError, ErrorCode, ErrorKind,
    GcConfig, WriterFence,
};
pub use loonfs_grep::GrepError;
pub use publisher::PublishObserver;

/// Integration seam: the vocabulary for handing classified mutation work to
/// the runtime's batch publication surface. The server's filesystem
/// handlers build [`publish::PathMutationIntent`]s for the [`publisher`]
/// front-end, and [`FsWriter::publish_namespace_mutations_batch`] accepts
/// [`publish::NamespaceMutationCandidate`]s directly. Most embedded users
/// never need this module.
pub mod publish {
    pub use loonfs_core::limits::{
        MAX_COMMIT_CONTENT_TOKENS, MAX_COMMIT_EXTERNAL_CONTENT_REFS, MAX_COMMIT_OPERATIONS,
    };
    pub use loonfs_core::path::{ensure_mutation_path, parse_mutation_path};
    pub use loonfs_core::publish::{
        ContentPreparation, ContentPreparationError, NamespaceMutation, NamespaceMutationCandidate,
        PathMutationIntent, PreparedContent,
    };
}

/// Server-integration seam for producer-only content preparation proofs.
///
/// A serving session mints short-lived wire tokens after durable preparation;
/// [`FsWriter::prepare_content_token`] verifies them against the namespace's
/// catalog binding. The resulting admission records accepted durability
/// evidence and does not expire. Most embedded users never need this module.
pub mod content_tokens {
    pub use loonfs_core::content::{mint_content_token, ContentTokenError};
}

/// Server-integration seam: the resolved direct-put upload target a serving
/// session turns into a presigned URL for client-side object PUT. Most
/// embedded users never need this module.
pub mod uploads {
    pub use loonfs_core::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
}

pub use loonfs_objectstore::metrics;
pub use loonfs_objectstore::{ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig};

pub use background::FsBackgroundWork;
pub use cache::RuntimeCacheStats;
pub use config::{RuntimeCacheConfig, DEFAULT_MAX_CONCURRENT_MAINTENANCE};
pub use handle::{FsAdmin, FsAdminBuilder, FsReader, FsReaderBuilder, FsWriter, FsWriterBuilder};
pub use options::{
    gc_config_from_request, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteOptions, ListChangesOptions, MaintenanceStepOptions, MoveOptions,
    PutFileOptions, RestoreRevisionOptions, UndeleteOptions,
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
    /// A grep query or grep-owned maintenance operation failed.
    #[error(transparent)]
    Grep(#[from] GrepError),
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
            Self::Grep(error) => error.code(),
            Self::Config(_) => ErrorCode::InvalidRequest,
            Self::RuntimeTask(_) => ErrorCode::ServerError,
        }
    }
}
