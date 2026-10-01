//! Core LoonFS namespace operations.
//!
//! Most callers should use the higher-level `loonfs` crate. Use
//! [`NamespaceEngine`] when working directly with the metadata protocol.
//!
//! # Example
//!
//! Commits are published as candidate batches through
//! [`publish::NamespaceCommitEngine`]; day-to-day reads and writes should go
//! through the `loonfs` crate's `LoonFs` runtime and `Namespace` handles,
//! which wrap this crate with caching and batching.
//!
//! ```no_run
//! use loonfs_types::{AbsolutePath, ActorId, CommitId, NamespaceId};
//! use loonfs_core::publish::{
//!     FilesystemOperation, CommitRequest, NamespaceCommitEngine, CommitCandidate,
//! };
//! use loonfs_core::time::Deadline;
//! use loonfs_objectstore::timing::StdMonotonicTimer;
//! use std::sync::Arc;
//! use loonfs_core::{CreateNamespaceOptions, MutationContext, NamespaceEngine};
//! use loonfs_types::WriterId;
//! use loonfs_objectstore::local_fs_store::LocalFsStore;
//!
//! let store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let namespace = NamespaceId::parse("docs").expect("valid namespace id");
//!
//! let writer_id = WriterId::parse("example-writer").expect("valid writer id");
//! let engine = NamespaceEngine::writer(store, namespace.clone(), writer_id.clone());
//! let actor_id = ActorId::parse("example-actor").expect("valid actor id");
//! let _ = engine.bootstrap_namespace(&actor_id, &CreateNamespaceOptions::default());
//!
//! let publish_store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let context = MutationContext {
//!     writer_id,
//!     now_ms: 0,
//! };
//! let mut publisher = NamespaceCommitEngine::new(namespace);
//! let _ = publisher.publish_batch(
//!     &publish_store,
//!     vec![CommitCandidate::new(CommitRequest::single(
//!         CommitId::generate(),
//!         ActorId::parse("example-service").expect("a static actor ID should parse"),
//!         None,
//!         FilesystemOperation::CreateDirectory {
//!             path: AbsolutePath::parse("/plans").expect("a static absolute path should parse"),
//!             parents: false,
//!         },
//!     ))],
//!     &context,
//!     &Deadline::start(Arc::new(StdMonotonicTimer::default())),
//! );
//! ```

pub(crate) mod authorize;
mod binding_version;
mod block_cache;
mod commit_engine;
mod commit_wal_size;
mod context;
mod control_object;
mod control_update;
mod engine;
mod error;
mod gc;
mod heap_bytes;
mod manifest;
mod namespace;
mod options;
mod pin;
mod protocol;
mod recency;
mod storage;
mod store_waves;
mod wal;
mod write_waves;

/// Commit planning, validation, and materialization. Consumed by the `loonfs`
/// publisher and by this crate's commit-validation integration tests.
pub mod commit;
/// Content staging and preparation-token creation used by the `loonfs` write
/// path and server integration code.
pub mod content;
/// Protocol and resource ceilings. Consumed by `loonfs` (re-exported to the
/// server for request validation) and by layout tests.
pub mod limits;
/// Durable metadata state and row codecs. This module is public so
/// integration tests can compare projected state with the reference model.
pub mod metadata;
/// Path parsing and current-state resolution. Consumed by `loonfs`'s write
/// path (`parse_mutation_path`).
pub mod path;
/// Test doubles for the public core APIs, available through the
/// `test-support` feature. They remain in this crate because
/// `loonfs-test-support` is a development dependency and cannot depend on
/// these types without creating a dependency cycle.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
/// Durable timestamps and monotonic publication budgets.
pub mod time;

/// Cache types used by runtime read paths. The `loonfs` metadata cache owns
/// the shared stores and their statistics; each runtime reads through its own
/// scoped views of them.
pub mod cache {
    pub use crate::block_cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
        DecodedBlockCacheStats, DecodedSegmentBlock, SegmentBlockKind, SegmentCacheKey,
    };
    pub use crate::recency::Recency;
    pub use crate::wal::ProjectedWalTail;

    pub use crate::manifest::metadata_maintenance_due;
    pub use crate::manifest::{
        CacheScope, CachedReadAnchor, HeadStateCache, HeadStateCacheStats, MetadataSegmentCache,
        MetadataSegmentCacheStats, NamespaceValidation, SharedHeadState, SharedSegmentBlocks,
        StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
        StoredMetadataBlockKind, WalTailProjectionCacheKey,
    };
    pub use crate::namespace::status::{
        load_namespace, load_namespace_diagnostics, NamespaceStorageDiagnostics,
    };
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::namespace::status::{load_namespace_wal_tail_usage, NamespaceWalTailUsage};
}

/// Typed loaders for namespace control objects and verified catalog state.
/// Used by `loonfs` read and write paths and by layout tests.
pub mod control {
    pub use crate::control_object::{ControlObjectLoadError, LoadedControl};
    pub use crate::manifest::{
        load_checkpoint_statistics, load_namespace_statistics, NamespaceStatistics,
    };
    pub use crate::namespace::catalog::{
        load_namespace_catalog_entry, VerifiedNamespaceCatalogEntry,
    };
    pub use crate::namespace::control::{
        load_namespace_current_manifest, load_namespace_read_state, CurrentManifest, LoadedHint,
        LoadedManifest,
    };
    pub use crate::namespace::read_anchor::{
        load_live_read_anchor, load_read_anchor, manifest_has_successor, project_anchor_tail,
        NamespaceReadAnchor,
    };
    pub use crate::namespace::state::NamespaceReadState;
    pub use crate::namespace::MetadataBasis;
    pub use crate::pin::{
        load_checkpoint_read_basis, load_snapshot_read_basis, CheckpointReadBasis,
    };
    pub use crate::wal::probe_namespace_wal;
}

/// Commit publication types for runtime integrations. Consumed by `loonfs`'s
/// publisher, and re-exported as `loonfs::publish` for the server's
/// filesystem handlers.
pub mod publish {
    pub use crate::commit::{CommitFingerprint, WalPublishError};
    pub use crate::commit_engine::{
        CommitCandidate, ContentPreparationError, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, ResultingReadState, SharedWriterSessionState,
        WalFoldInput, WriterSessionState,
    };
    pub use crate::path::write::{CommitRequest, FilesystemOperation};
    pub use crate::storage::content_admission::PreparedContent;
    pub use crate::storage::inline_content::InlineContent;
}

// Crate-root re-exports used by `loonfs` or required by public return types.
pub use context::MutationContext;
pub use engine::RuntimeReadContext;
pub use engine::{
    NamespaceEngine, NamespaceReaderEngine, NamespaceWriterEngine, ReadOnly, ResolvedFileContent,
    Writable,
};
pub use error::{
    Error, ErrorCode, ErrorKind, MetadataProjectionLoadError, MetadataViewError, StoreFailureClass,
    WriterFence,
};
pub use gc::{delete_if_aged, gc_namespace, grace_age, GcOptions, GraceAge};
pub use manifest::{
    fold_wal_tail, next_run_no_after, refill_iterators, select_next_iterator,
    CompactionStepOutcome, FoldedWalTail, MetadataCompactionCancellation,
    MetadataCompactionJobOutcome, MetadataCompactionPolicy, MetadataCompactionSpec,
    MetadataFamilyGroup, MetadataLsmPolicy, SegmentBlockLoader, SegmentRowIterator,
};
pub use manifest::{ManifestLoadError, ManifestLoadFailureClass};
pub use options::{CreateNamespaceOptions, DeleteNamespaceOptions};
pub use path::read::{
    CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, MAX_RESOLVE_CURRENT_FILES,
};
pub use pin::{
    CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor,
    ListCheckpointFilesOptions,
};
pub use protocol::{
    DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget, MultipartPartTargets,
    ResolvedUploadCompletion, UploadSessionView,
};
pub use write_waves::write_segments_in_waves;
// The streaming read `loonfs`'s namespace handle returns, and the chunk size it
// reads in.
pub use storage::content::{FileContentStream, CONTENT_READ_CHUNK_BYTES};
