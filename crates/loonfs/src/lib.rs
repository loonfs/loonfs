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
mod time;
mod trace;
mod uploads;

use std::sync::Arc;

use thiserror::Error;

pub use loonfs_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitAnnotations, CommitDelta, CommitOp, CommitOpResult,
    CommitPrecondition, CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, RenameMode, UploadContentResponse, UploadMode,
};
pub use loonfs_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, CapabilityDocument,
    ChangeSeq, CommitId, ContentRef, ContentRefKind, CreateCheckpointResponse, DisplayName,
    FileRevision, FilesystemOperationResponse, InodeId, InodeKind, ListFileRevisionsResponse,
    ListPathEntriesResponse, ManifestId, MutationResult, NameKey, NamePolicy, NamespaceId,
    NamespaceSummary, RevisionNo, FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE,
    FEATURE_NAMESPACES_FORK, FEATURE_NAMESPACES_LIST, PROFILE_ADMIN_V0, PROFILE_CORE_V0,
    PROTOCOL_VERSION,
};
pub use loonfs_core::cache::MetadataTableCacheConfig;
pub use loonfs_core::{
    BootstrapNamespaceError, Error as CoreError, ErrorCode, ErrorKind, PutFileBehavior,
};

/// Server-integration seam: the vocabulary a batching publisher uses to
/// submit work to the runtime. Most embedded users never need this module.
pub mod publish {
    pub use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
}
pub use loonfs_objectstore::metrics::{
    JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricSample, ObjectStoreMetricsRecorder,
    ObjectStoreOperation, ObjectStoreResultClass, PutModeClass, RangeClass,
};
pub use loonfs_objectstore::{ObjectStore, ObjectStoreError};

pub use cache::RuntimeCacheStats;
pub use config::{
    FsConfig, RuntimeCacheConfig, DEFAULT_LEASE_DURATION_MS,
    DEFAULT_MAX_CACHED_BASIS_DECODED_BYTES, DEFAULT_MAX_CACHED_BASIS_ROWS,
    DEFAULT_MAX_CACHED_NAMESPACES, DEFAULT_MAX_WAL_TAIL_SEGMENTS,
};
pub use fs::{Fs, FsBuilder};
pub use options::{
    CopyOptions, CreateDirOptions, CreateNamespaceOptions, DeleteOptions, MaintenanceTickOptions,
    MaintenanceTickOutcome, MaintenanceTickResult, MoveOptions, NamespaceStatus, PutFileOptions,
    RestoreRevisionOptions,
};
pub use trace::{payload_class, TraceMode, TraceStoreKind};

/// Shared object-store handle used by the runtime.
pub type SharedObjectStore = Arc<dyn ObjectStore + Send + Sync>;
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
