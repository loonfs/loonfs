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
//! use loonfs_api::{AbsolutePath, ActorId, ActorRef, CommitId, NamespaceId};
//! use loonfs_core::publish::{
//!     FilesystemOperation, CommitRequest, NamespaceCommitEngine, CommitCandidate,
//!     PublishTailOptions,
//! };
//! use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine};
//! use loonfs_objectstore::local_fs_store::LocalFsStore;
//!
//! let store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let namespace = NamespaceId::parse("docs").expect("valid namespace id");
//!
//! let engine = NamespaceEngine::writer(store, namespace.clone(), "example-writer")
//!     .expect("a writer engine with a non-empty identity should build");
//! let _ = engine.bootstrap_namespace(BootstrapOptions::default());
//!
//! let publish_store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let context = MutationContext {
//!     writer_id: "example-writer".to_owned(),
//!     now_ms: 0,
//! };
//! let mut publisher = NamespaceCommitEngine::new(namespace);
//! let _ = publisher.publish_batch(
//!     &publish_store,
//!     vec![CommitCandidate::new(CommitRequest::single(
//!         CommitId::generate(),
//!         ActorRef::service(
//!             ActorId::parse("example-service").expect("a static actor ID should parse"),
//!         ),
//!         None,
//!         FilesystemOperation::CreateDirectory {
//!             path: AbsolutePath::parse("/plans").expect("a static absolute path should parse"),
//!             parents: false,
//!         },
//!     ))],
//!     &context,
//!     &PublishTailOptions::default(),
//! );
//! ```

mod binding_generation;
mod block_cache;
mod checkpoint;
mod commit_engine;
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
/// Wall-clock access used to assign durable timestamps. Both core commits
/// and the `loonfs` runtime use this API so they share the same time source.
pub mod time;

/// Cache types and configuration used by runtime read paths. The `loonfs`
/// runtime owns these caches and their statistics.
pub mod cache {
    pub use crate::block_cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheObserver, DecodedBlockCacheStats,
    };
    pub use crate::recency::Recency;

    pub use crate::checkpoint::{
        ManifestLoadError, ManifestLoadFailureClass, MetadataSegmentCache,
        MetadataSegmentCacheConfig, MetadataSegmentCacheObserver, MetadataSegmentCacheStats,
        StoredMetadataBlockCache, StoredMetadataBlockCacheCloseError, StoredMetadataBlockKey,
        StoredMetadataBlockKind, WalTailProjectionCache, WalTailProjectionCacheConfig,
        WalTailProjectionCacheKey, WalTailProjectionCacheObserver, WalTailProjectionCacheStats,
        DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_ROWS,
    };
    pub use crate::namespace::status::{
        load_deleted_namespace_diagnostics, load_namespace, load_namespace_diagnostics,
        load_namespace_flush_basis, NamespaceFlushBasis, NamespaceStorageDiagnostics,
    };
}

/// Typed loaders for namespace control objects and verified catalog state.
/// Used by `loonfs` read and write paths and by layout tests.
pub mod control {
    pub use crate::checkpoint::{
        load_checkpoint_read_basis, load_snapshot_read_basis, CheckpointReadBasis,
    };
    pub use crate::control_object::{ControlObjectLoadError, LoadedControl};
    pub use crate::namespace::catalog::{
        load_namespace_catalog_entry, NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
    pub use crate::namespace::control::{
        load_namespace_checkpoint_record_control, load_namespace_head_control,
        load_namespace_metadata_root_control, load_namespace_read_anchor,
    };
    pub use crate::namespace::MetadataBasis;
}

/// Commit publication types for runtime integrations. Consumed by `loonfs`'s
/// publisher, and re-exported as `loonfs::publish` for the server's
/// filesystem handlers.
pub mod publish {
    pub use crate::commit::{CommitFingerprint, CommitHeadPublishError};
    pub use crate::commit_engine::{
        CommitCandidate, ContentPreparationError, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, ResultingReadState, SharedWriterSessionState,
        WriterSessionState,
    };
    pub use crate::path::write::{CommitRequest, FilesystemOperation};
    pub use crate::protocol::{PublishTailOptions, PublishTailWeight};
    pub use crate::storage::content_admission::PreparedContent;
}

// Crate-root re-exports used by `loonfs` or required by public return types.
// `MetadataReorganizeReport` remains public because
// `NamespaceEngine::reorganize_metadata` returns it.
pub use checkpoint::{
    ensure_metadata_publication_budget, next_run_no_after, refill_iterators, select_next_iterator,
    write_segments_in_waves, CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor,
    CheckpointPageCursor, FrozenBasePolicy, MetadataCompactionCancellation,
    MetadataCompactionJobOutcome, MetadataCompactionSpec, MetadataCompactionView,
    MetadataFamilyGroup, MetadataReorganizeOutcome, MetadataReorganizeReport, SegmentBlockLoader,
    SegmentRowIterator,
};
pub use context::MutationContext;
pub use engine::RuntimeReadContext;
pub use engine::{
    NamespaceEngine, NamespaceEngineBuildError, NamespaceReaderEngine, NamespaceWriterEngine,
    ReadOnly, Writable,
};
pub use error::{
    Error, ErrorCode, ErrorKind, MetadataProjectionLoadError, MetadataViewError, StoreFailureClass,
    WriterFence,
};
pub use gc::{
    delete_if_aged, gc_namespace, GcConfig, GcCursorKeyspace, GraceAge, NamespaceGcCursor,
    PassBudget,
};
pub use namespace::BootstrapNamespaceError;
pub use options::{BootstrapOptions, DeleteNamespaceOptions};
pub use path::read::{
    CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, MAX_RESOLVE_CURRENT_FILES,
};
pub use protocol::{
    BeginDirectMultipartUploadTargetResponse, BeginDirectPutUploadTargetResponse,
    DirectMultipartUploadTarget, MultipartPartTarget, MultipartPartTargets,
    ResolvedUploadCompletion,
};
// The streaming read `loonfs`'s reader handle returns, and the chunk size it
// reads in.
pub use storage::content::{FileContentStream, CONTENT_READ_CHUNK_BYTES};
