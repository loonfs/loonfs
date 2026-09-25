//! Core LoonFS namespace operations.
//!
//! Most callers should use the higher-level `loonfs` crate. Use
//! [`NamespaceEngine`] when working directly with the metadata protocol.
//!
//! # Example
//!
//! Commits are published as candidate batches through
//! [`publish::NamespaceCommitEngine`]; day-to-day reads and writes should go
//! through the `loonfs` crate's `FsReader`/`FsWriter` handles, which wrap
//! this crate with caching and batching.
//!
//! ```no_run
//! use loonfs_api::{AbsolutePath, ActorId, CommitId, NamespaceId};
//! use loonfs_core::publish::{
//!     FilesystemOperation, CommitRequest, NamespaceCommitEngine, CommitCandidate,
//!     PublishTailOptions,
//! };
//! use loonfs_core::time::Deadline;
//! use loonfs_objectstore::timing::StdMonotonicTimer;
//! use std::sync::Arc;
//! use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine};
//! use loonfs_api::WriterId;
//! use loonfs_objectstore::local_fs_store::LocalFsStore;
//!
//! let store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let namespace = NamespaceId::parse("docs").expect("valid namespace id");
//!
//! let writer_id = WriterId::parse("example-writer").expect("valid writer id");
//! let engine = NamespaceEngine::writer(store, namespace.clone(), writer_id.clone());
//! let actor_id = ActorId::parse("example-actor").expect("valid actor id");
//! let _ = engine.bootstrap_namespace(BootstrapOptions::new(actor_id));
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
//!     &PublishTailOptions::default(),
//!     &Deadline::start(Arc::new(StdMonotonicTimer::default())),
//! );
//! ```

pub(crate) mod authorize;
mod binding_version;
mod block_cache;
mod checkpoint;
mod commit_engine;
mod commit_wal_size;
mod context;
mod control_object;
mod control_update;
mod engine;
mod error;
mod gc;
mod namespace;
mod options;
mod protocol;
mod recency;
mod storage;
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

/// Cache types and configuration used by runtime read paths. The `loonfs`
/// runtime owns these caches and their statistics.
pub mod cache {
    pub use crate::block_cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheObserver,
        DecodedBlockCacheStats, DecodedBlockWeight, DecodedSegmentBlock, SegmentBlockKind,
        SegmentCacheKey,
    };
    pub use crate::recency::Recency;
    pub use crate::wal::ProjectedWalTail;

    pub use crate::checkpoint::metadata_maintenance_due;
    pub use crate::checkpoint::{
        MetadataSegmentCache, MetadataSegmentCacheConfig, MetadataSegmentCacheStats,
        StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
        StoredMetadataBlockKind, WalTailProjectionCache, WalTailProjectionCacheConfig,
        WalTailProjectionCacheKey, WalTailProjectionCacheStats,
        DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_ROWS,
    };
    pub use crate::namespace::status::{
        load_namespace, load_namespace_diagnostics, load_namespace_flush_basis,
        NamespaceFlushBasis, NamespaceStorageDiagnostics,
    };
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::namespace::status::{load_namespace_wal_tail_usage, NamespaceWalTailUsage};
}

/// Typed loaders for namespace control objects and verified catalog state.
/// Used by `loonfs` read and write paths and by layout tests.
pub mod control {
    pub use crate::checkpoint::{
        load_checkpoint_read_basis, load_checkpoint_statistics, load_namespace_statistics,
        load_snapshot_read_basis, CheckpointReadBasis, NamespaceStatistics,
    };
    pub use crate::control_object::{ControlObjectLoadError, LoadedControl};
    pub use crate::namespace::catalog::{
        load_namespace_catalog_entry, VerifiedNamespaceCatalogEntry,
    };
    pub use crate::namespace::control::{
        load_namespace_checkpoint_record_control, load_namespace_current_manifest,
        load_namespace_read_state, raise_namespace_hint, CurrentManifest, LoadedHint,
        LoadedManifest,
    };
    pub use crate::namespace::read_anchor::{
        load_read_anchor, manifest_has_successor, NamespaceReadAnchor,
    };
    pub use crate::namespace::state::NamespaceReadState;
    pub use crate::namespace::MetadataBasis;
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
    pub use crate::protocol::{PublishTailOptions, PublishTailWeight};
    pub use crate::storage::content_admission::PreparedContent;
    pub use crate::storage::inline_content::InlineContent;
}

// Crate-root re-exports used by `loonfs` or required by public return types.
pub use checkpoint::{
    fold_wal_tail, next_run_no_after, refill_iterators, select_next_iterator, CheckpointFile,
    CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor,
    MetadataCompactionCancellation, MetadataCompactionJobOutcome, MetadataCompactionPolicy,
    MetadataCompactionSpec, MetadataFamilyGroup, MetadataReorganizeOutcome, SegmentBlockLoader,
    SegmentRowIterator,
};
pub use checkpoint::{ManifestLoadError, ManifestLoadFailureClass};
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
pub use gc::{delete_if_aged, gc_namespace, GcConfig, GraceAge};
pub use options::{BootstrapOptions, DeleteNamespaceOptions};
pub use path::read::{
    CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, MAX_RESOLVE_CURRENT_FILES,
};
pub use protocol::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    DirectMultipartUploadTarget, MultipartPartTarget, MultipartPartTargets,
    ResolvedUploadCompletion, UploadSessionView,
};
pub use write_waves::write_segments_in_waves;
// The streaming read `loonfs`'s reader handle returns, and the chunk size it
// reads in.
pub use storage::content::{FileContentStream, CONTENT_READ_CHUNK_BYTES};
