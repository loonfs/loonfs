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
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitDelta, CommitOp,
    CommitPrecondition, CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, DirectPutUpload, MoveBehavior, ObjectTransferAccess,
    UploadContentResponse, UploadMode,
};
pub use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument,
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentRefKind, CreateCheckpointRequest,
    CreateCheckpointResponse, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DirectoryPageCursor, DisableGramsIndexResponse, DisplayName, EffectiveLimit,
    EnableGramsIndexResponse, FileRevision, FileRevisionsPageCursor, FlushWalOutcome,
    FlushWalResponse, GrepMatch, GrepRequest, GrepResponse, InodeId, InodeKind,
    ListFileRevisionsResponse, ListPathEntriesResponse, ManifestId, NameKey, NamePolicy,
    NamespaceId, NamespaceStatusResponse, NamespaceSummary, Page, PageRequest, PaginationPolicy,
    PutBehavior, ReleaseCheckpointResponse, RevisionNo, UploadId, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_PUT, PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROFILE_QUERY_V0,
    PROTOCOL_VERSION,
};
pub use loonfs_core::cache::MetadataTableCacheConfig;
pub use loonfs_core::{
    BeginDirectPutUploadTargetResponse, BootstrapNamespaceError, DeleteNamespaceOptions,
    DirectPutUploadTarget, Error as CoreError, ErrorCode, ErrorKind, GcConfig, GcReport,
    GramIndexBuildPolicy, WriterFence,
};

/// Integration seam: the vocabulary for handing classified mutation work to
/// the runtime's batch publication surface. The server's filesystem
/// handlers build [`publish::PathMutationIntent`]s for the [`publisher`]
/// front-end, and [`FsWriter::publish_namespace_mutations_batch`] accepts
/// [`publish::NamespaceMutationCandidate`]s directly. Most embedded users
/// never need this module.
pub mod publish {
    pub use loonfs_core::path::parse_mutation_path;
    pub use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
}

/// Server-integration seam: mint/verify surface for the short-lived content
/// tokens a serving session uses to admit already-validated direct-put
/// content. Most embedded users never need this module.
pub mod content_tokens {
    pub use loonfs_core::content::{
        mint_content_token, verify_content_token, ContentAdmission, ContentTokenError,
    };
}
pub use loonfs_objectstore::metrics::{
    JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricSample, ObjectStoreMetricsRecorder,
    ObjectStoreOperation, ObjectStoreResultClass, PutModeClass, RangeClass,
};
pub use loonfs_objectstore::{ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig};

pub use background::FsBackgroundWork;
pub use cache::RuntimeCacheStats;
pub use config::{
    RuntimeCacheConfig, DEFAULT_MAX_CACHED_NAMESPACES,
    DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS, DEFAULT_MAX_WAL_TAIL_SEGMENTS,
    DEFAULT_MIN_PUBLISH_INTERVAL_MS,
};
pub use handle::{FsAdmin, FsAdminBuilder, FsReader, FsReaderBuilder, FsWriter, FsWriterBuilder};
pub use options::{
    gc_config_from_request, gc_response_from_report, CopyOptions, CreateCheckpointOptions,
    CreateDirectoryOptions, CreateNamespaceOptions, DeleteOptions, ListChangesOptions,
    MaintenanceTickOptions, MaintenanceTickOutcome, MaintenanceTickResult, MoveOptions,
    PutFileOptions, RestoreRevisionOptions,
};
pub use trace::{payload_class, TraceMode, TraceStoreKind};

/// Result type used by the embedded runtime.
pub type Result<T> = std::result::Result<T, RuntimeError>;

pub use self::RuntimeError as Error;

/// The embedded runtime's error type, also exported as [`Error`].
#[derive(Debug, Error)]
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
