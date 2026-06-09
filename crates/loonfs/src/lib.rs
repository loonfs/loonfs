//! Embedded LoonFS runtime.
//!
//! `loonfs` is the ergonomic runtime layer. It wraps `loon-core` with caching,
//! upload helpers, maintenance hooks, and optional object-store metrics. Use it
//! when you want LoonFS in-process, or when building the reference server.

mod cache;
mod config;
mod fs;
mod options;
mod time;
mod trace;
mod uploads;

use std::sync::Arc;

use thiserror::Error;

pub use loon_api::v0::{
    BeginUploadResponse, ChangesResponse, CommitAnnotations, CommitDelta, CommitOp, CommitOpResult,
    CommitPrecondition, CommitRequest, CommitResponse, CommittedChange, CompleteUploadRequest,
    CompleteUploadResponse, RenameMode, UploadContentResponse, UploadMode,
};
pub use loon_api::{
    AdvanceRetentionResponse, AuthoritativeFileBytes, AuthoritativePathEntry, ChangeSeq, CommitId,
    ContentRef, ContentRefKind, CreateCheckpointResponse, DisplayName, FileRevision,
    FilesystemOperationResponse, InodeId, InodeKind, ListFileRevisionsResponse, ManifestId,
    MutationResult, NameKey, NamePolicy, NamespaceId, NamespaceSummary, RevisionNo,
};
pub use loon_core::cache::MetadataTableCacheConfig;
pub use loon_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
pub use loon_core::{
    BootstrapNamespaceError, Error, Error as CoreError, ErrorCode, ErrorKind, PutFileBehavior,
};
pub use loon_objectstore::metrics::{
    JsonlObjectStoreMetricsRecorder, KeyClass, ObjectStoreMetricSample, ObjectStoreMetricsRecorder,
    ObjectStoreOperation, ObjectStoreResultClass, PutModeClass, RangeClass,
};
pub use loon_objectstore::{ObjectStore, ObjectStoreError};

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

/// Runtime error.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Bootstrap(#[from] BootstrapNamespaceError),
    #[error("invalid runtime config: {0}")]
    Config(String),
    #[error("runtime task failed: {0}")]
    RuntimeTask(String),
}
