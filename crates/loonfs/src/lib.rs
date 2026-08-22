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
//! - [`FsReader`] for namespace state and read-only latest views.
//! - [`FsAdmin`] for explicit maintenance: diagnostics, checkpoints,
//!   retention, and garbage collection.
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
//! A writer owns publication and any automatically scheduled maintenance.
//! One [`MaintenanceJob`] runner schedules work for namespaces touched or
//! explicitly assigned to this process; it does not discover namespaces.
//! Jobs run on the writer's Tokio runtime and re-read durable state before
//! acting. [`FsWriter::shutdown`] drains this work. Readers and admins do not
//! start background tasks.
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
    CompleteMultipartUploadRequest, CompleteUploadRequest, DirectPutUpload, FilesystemChange,
    ObjectTransferAccess, UploadContentClaim, UploadContentResponse, UploadMode,
    UploadSessionResponse, UploadSessionStatus,
};
pub use loonfs_api::{
    ActorId, ActorKind, ActorRef, AdvanceRetentionResponse, AttributeKey, AttributeRevisionNo,
    AttributeValue, Attributes, AttributesProjection, AuthoritativeFileBytes,
    AuthoritativePathEntry, AuthoritativePathEntryKind, CapabilityDocument, ChangeSeq, Checkpoint,
    CheckpointId, CheckpointOwnerSummary, ChecksumAlgorithm, CommitId, ContentId, ContentRef,
    ContentRefKind, CreateCheckpointRequest, CreateCheckpointResponse, DeleteDirectoryBehavior,
    DeleteNamespaceResponse, DestinationBehavior, DirectoryPageCursor, EffectiveLimit,
    FileRevision, FileRevisionsPageCursor, FlushWalOutcome, FlushWalResponse, GcRequest,
    GcResponse, InodeId, InodeKind, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListPathEntriesResponse, MaintenanceStepRequest, MaintenanceStepResponse, ManifestNo,
    MetadataMaintenanceRequest, MetadataMaintenanceResponse, NameKey, Namespace,
    NamespaceDiagnostics, NamespaceId, Page, PageRequest, PaginationPolicy,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RetainedCandidates, RetainedReason,
    RevisionNo, TrashEntry, UploadId, WalFlushStepOutcome, FEATURE_ATTRIBUTES,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_NAMESPACES_CREATE, FEATURE_NAMESPACES_DELETE,
    FEATURE_NAMESPACES_FORK, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    PROFILE_ADMIN_V0, PROFILE_CORE_V0, PROTOCOL_VERSION,
};
pub use loonfs_core::cache::{
    MetadataSegmentCacheConfig, Recency, StoredMetadataBlockCache,
    StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey, StoredMetadataBlockKind,
};
pub use loonfs_core::limits::{
    DEFAULT_GC_MAX_OBJECTS, GC_MIN_GRACE_WINDOW_MS, MAX_MULTIPART_PARTS,
    MAX_SIGNED_PARTS_PER_REQUEST, METADATA_PUBLICATION_BUDGET_MS,
};
pub use loonfs_core::time::current_time_ms;
pub use loonfs_core::{
    delete_if_aged, AgedSweep, BootstrapNamespaceError, CheckpointFile, CheckpointFilesPage,
    CheckpointFilesPageCursor, CheckpointPageCursor, CurrentFileState, DeleteNamespaceOptions,
    Error as CoreError, ErrorCode, ErrorKind, FileContentStream, GcConfig,
    MetadataCompactionJobOutcome, MetadataViewError, PassBudget, StoreFailureClass, WriterFence,
    CONTENT_READ_CHUNK_BYTES, MAX_RESOLVE_CURRENT_FILES,
};
pub use publisher::PublishObserver;

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
/// namespace catalog and returns process-local proof that remains valid.
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
        load_namespace_metadata_root_control, ControlObjectLoadError, LoadedHeadControl,
        LoadedMetadataRootControl, NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
}

pub use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, SharedObjectStore, StoreConfig,
};

pub use cache::RuntimeCacheStats;
pub use config::{RuntimeCacheConfig, DEFAULT_MAX_CONCURRENT_MAINTENANCE};
pub use fs::{
    ChangesPager, CheckpointsPager, FileRevisionsPager, MetadataCompactionOutcome,
    PathEntriesPager, TrashPager,
};
pub use handle::{FsAdmin, FsAdminBuilder, FsReader, FsReaderBuilder, FsWriter, FsWriterBuilder};
pub use maintenance_runner::{
    FsBackgroundWork, MaintenanceHandle, MaintenanceJob, MaintenanceJobId, MaintenanceProbe,
    MaintenanceStepConclusion, MaintenanceStepReport,
};
pub use options::{
    CommitOptions, CopyOptions, CreateCheckpointOptions, CreateDirectoryOptions,
    CreateNamespaceOptions, DeleteOptions, ListChangesOptions, ListPathEntriesOptions,
    MaintenancePlan, MetadataMaintenanceOptions, MoveOptions, PutFileOptions,
    ReadFileStreamOptions, RestoreRevisionOptions, StatPathOptions, UndeleteOptions,
    UpdateAttributesOptions,
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
