//! Embedded LoonFS runtime.
//!
//! `loonfs` is the ergonomic runtime layer. It wraps `loonfs-core` with caching,
//! upload helpers, maintenance hooks, and optional object-store metrics. Use it
//! when you want LoonFS in-process, or when building the reference server.
#![warn(missing_docs)]

mod cache;
mod config;
mod fs;
mod options;
pub mod publisher;
mod time;
mod trace;

use thiserror::Error;

pub use loonfs_api::v0::{
    BeginUploadRequest, BeginUploadResponse, ChangesResponse, CommitDelta, CommitOp,
    CommitPrecondition, CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, DirectPutUpload, MoveBehavior, ObjectTransferAccess,
    UploadContentResponse, UploadMode,
};
pub use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument,
    ChangeSeq, CheckpointId, CommitId, ContentRef, ContentRefKind, CreateCheckpointResponse,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DirectoryPageCursor, DisplayName,
    EffectiveLimit, FileRevision, FileRevisionsPageCursor, FilesystemOperationResponse, InodeId,
    InodeKind, ListFileRevisionsResponse, ListPathEntriesResponse, ManifestId, MutationResult,
    NameKey, NamePolicy, NamespaceId, NamespaceStatusResponse, NamespaceSummary, Page, PageRequest,
    PaginationPolicy, PutBehavior, RevisionNo, UploadId, FEATURE_NAMESPACES_CREATE,
    FEATURE_NAMESPACES_DELETE, FEATURE_NAMESPACES_FORK, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
pub use loonfs_core::cache::MetadataTableCacheConfig;
pub use loonfs_core::{
    BeginDirectPutUploadTargetResponse, BootstrapNamespaceError, DeleteNamespaceOptions,
    DirectPutUploadTarget, Error as CoreError, ErrorCode, ErrorKind, GcConfig, GcReport,
};

/// Integration seam: the vocabulary for handing classified mutation work to
/// the runtime's batch publication surface. The server's filesystem
/// handlers build [`publish::PathMutationIntent`]s for the [`publisher`]
/// front-end, and [`Fs::publish_namespace_mutations_batch`] accepts
/// [`publish::NamespaceMutationCandidate`]s directly. Most embedded users
/// never need this module.
pub mod publish {
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
pub use loonfs_objectstore::{ObjectStore, ObjectStoreError, SharedObjectStore};

pub use cache::RuntimeCacheStats;
pub use config::{
    FsConfig, RuntimeCacheConfig, DEFAULT_MAX_CACHED_NAMESPACES,
    DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_MAX_CACHED_WAL_TAIL_PROJECTION_ROWS, DEFAULT_MAX_WAL_TAIL_SEGMENTS,
};
pub use fs::{Fs, FsBuilder};
pub use options::{
    CopyOptions, CreateDirOptions, CreateNamespaceOptions, DeleteOptions, ListChangesOptions,
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
