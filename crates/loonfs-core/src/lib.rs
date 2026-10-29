//! Core LoonFS namespace operations.
//!
//! Most callers should use the higher-level `loonfs` crate. Use
//! [`NamespaceEngine`] when working directly with the metadata protocol.
//!
//! Caching, batching, background work, and process lifecycle don't belong
//! here: core never spawns a task, and the `loonfs` runtime adds them.
//!
//! # Example
//!
//! A [`NamespaceStore`] holds one namespace's object store with the caches,
//! clocks, content write permits, and run policy its operations use. Commits
//! are published as candidate batches through
//! [`publish::NamespaceCommitEngine`]; day-to-day reads and writes should go
//! through the `loonfs` crate's `LoonFs` runtime and `Namespace` handles,
//! which wrap this crate with caching and batching.
//!
//! ```no_run
//! use loonfs_core::cache::{
//!     CacheScope, HeadAndWalTailCache, MetadataSegmentCache, ObjectCache, ObjectStoreScope,
//!     SharedHeadsAndWalTails, SharedObjects,
//! };
//! use loonfs_core::publish::{
//!     CommitCandidate, CommitIntent, FilesystemOperation, NamespaceCommitEngine,
//! };
//! use loonfs_core::time::SystemWallClock;
//! use loonfs_core::{
//!     CreateNamespaceOptions, MetadataRunPolicy, NamespaceEngine, NamespaceStore, PermitPool,
//! };
//! use loonfs_objectstore::local_fs_store::LocalFsStore;
//! use loonfs_types::StdMonotonicTimer;
//! use loonfs_types::{AbsolutePath, ActorId, CommitId, NamespaceId, WriterId};
//! use std::num::NonZeroUsize;
//! use std::sync::Arc;
//!
//! let store = LocalFsStore::new(std::env::temp_dir())
//!     .expect("a temporary-directory-backed store should initialize");
//! let namespace = NamespaceStore::new(
//!     Arc::new(store),
//!     NamespaceId::parse("docs").expect("valid namespace id"),
//!     Arc::new(MetadataSegmentCache::unshared(64 * 1024 * 1024)),
//!     Arc::new(HeadAndWalTailCache::new(
//!         Arc::new(SharedHeadsAndWalTails::new(1024 * 1024, 64 * 1024 * 1024)),
//!         CacheScope::new(0),
//!     )),
//!     Arc::new(ObjectCache::new(
//!         Arc::new(SharedObjects::new(64 * 1024 * 1024, 1024 * 1024)),
//!         ObjectStoreScope::new(0),
//!     )),
//!     Arc::new(SystemWallClock),
//!     Arc::new(StdMonotonicTimer::default()),
//!     Arc::new(PermitPool::new(
//!         NonZeroUsize::new(32).expect("a nonzero limit"),
//!         None,
//!     )),
//!     MetadataRunPolicy::default(),
//! );
//!
//! let writer_id = WriterId::parse("example-writer").expect("valid writer id");
//! let engine = NamespaceEngine::writer(namespace.clone(), writer_id.clone());
//! let actor_id = ActorId::parse("example-actor").expect("valid actor id");
//! let _ = engine.bootstrap_namespace(&actor_id, &CreateNamespaceOptions::default());
//!
//! let clock = namespace.start_clock();
//! let mut publisher = NamespaceCommitEngine::new(namespace, writer_id);
//! let _ = publisher.publish_batch(
//!     vec![CommitCandidate::new(CommitIntent::single(
//!         CommitId::generate(),
//!         ActorId::parse("example-service").expect("a static actor ID should parse"),
//!         None,
//!         FilesystemOperation::CreateDirectory {
//!             path: AbsolutePath::parse("/plans").expect("a static absolute path should parse"),
//!             parents: false,
//!         },
//!     ))],
//!     &clock,
//! );
//! ```

pub(crate) mod authorize;
mod binding_version;
mod block_cache;
mod bloom_filter;
mod commit_engine;
mod commit_wal_size;
mod control_object;
mod control_update;
mod engine;
mod error;
mod gc;
mod heap_bytes;
mod in_flight;
mod manifest;
mod namespace;
mod object_cache;
mod options;
mod permit_pool;
mod phase;
mod pin;
mod protocol;
mod read_working_memory;
mod recency;
mod shared_check;
mod storage;
mod upload_records;
mod wal;

/// Commit planning, validation, and materialization. Consumed by the `loonfs`
/// publisher and by this crate's commit-validation integration tests.
pub mod commit;
/// Content staging and preparation-token creation used by the `loonfs` write
/// path and server integration code.
pub mod content;
/// Protocol and resource ceilings.
///
/// Consumed by `loonfs` (re-exported to the server for request validation)
/// and by layout tests.
pub mod limits;
/// Durable metadata state and row codecs. This module is public so
/// integration tests can compare projected state with the reference model.
pub mod metadata;
/// Path parsing and current-state resolution. Consumed by `loonfs`'s write
/// path (`parse_mutation_path`).
pub mod path;
/// Test doubles for the public core APIs, available through the
/// `test-support` feature.
///
/// They remain in this crate because `loonfs-test-support` is a development
/// dependency and cannot depend on these types without creating a dependency
/// cycle.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
/// Durable timestamps and monotonic publication budgets.
pub mod time;

/// Cache types used by runtime read paths. The `loonfs` metadata cache owns
/// the shared stores and their statistics; each runtime reads through its own
/// scoped views of them.
pub mod cache {
    pub use crate::block_cache::{
        DecodedBlock, DecodedBlockCache, DecodedBlockCacheConfig, DecodedBlockCacheStats,
        DecodedBlockKey, DecodedSegmentBlock, Insertion, SegmentBlockKind, SegmentCacheKey,
    };
    pub use crate::gc::{CompletedUploadSessionCache, SharedCompletedUploadSessions};
    pub use crate::object_cache::{ObjectCache, SharedObjects};
    pub use crate::read_working_memory::{ReadWorkingMemory, ReadWorkingMemoryObserver};
    pub use crate::recency::Recency;
    pub use crate::shared_check::SharedCheck;
    pub use crate::wal::ProjectedWalTail;

    pub use crate::manifest::metadata_compaction_due;
    pub use crate::manifest::{
        decode_stored_block, CacheScope, CachedHead, DecodedMetadataSegmentBlock,
        HeadAndWalTailCache, LoadedSegmentBlock, MemorySegmentBlocks, MetadataRunManifest,
        MetadataSegmentCache, MetadataSegmentCacheKey, MetadataSegmentCacheStats, ObjectStoreScope,
        SegmentBlockCache, SegmentBlockLoad, SharedHeadsAndWalTails, SharedSegmentBlocks,
        WalTailCacheKey,
    };
    pub use crate::namespace::status::NamespaceStorageDiagnostics;
    #[cfg(any(test, feature = "test-support"))]
    pub use crate::namespace::status::NamespaceWalTailUsage;
}

/// Namespace control objects and verified catalog state, as the loaders of
/// [`NamespaceStore`] return them.
///
/// The `loonfs` read and write paths and the layout tests use this module.
pub mod control {
    pub use crate::control_object::{ControlObjectLoadError, LoadedControl};
    pub use crate::manifest::{load_position_basis, NamespaceStatistics};
    pub use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
    pub use crate::namespace::control::{LoadedHint, LoadedManifest};
    pub use crate::namespace::read_anchor::{project_anchor_tail, NamespaceReadAnchor};
    pub use crate::namespace::state::NamespaceReadState;
    pub use crate::namespace::{MetadataBasis, PublicationBasis, ReadBasis};
    pub use crate::wal::probe_namespace_wal;
}

/// Commit publication types for runtime integrations. Consumed by `loonfs`'s
/// publisher, and re-exported as `loonfs::publish` for the server's
/// filesystem handlers.
pub mod publish {
    pub use crate::commit::CommitFingerprint;
    pub use crate::commit_engine::{
        CommitCandidate, ContentPreparationError, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, SharedWriterSessionStatus, TailSource, WalTailReport,
        WriterSessionStatus,
    };
    pub use crate::namespace::writer_epoch::AcquiredWriter;
    pub use crate::path::write::{CommitIntent, FilesystemOperation};
    pub use crate::protocol::WriterPosition;
    pub use crate::storage::content_admission::PreparedContent;
    pub use crate::storage::inline_content::InlineContent;
    pub use crate::wal::{is_retryable_wal_publish, settle_publish_attempt, WalPublishError};
}

// Crate-root re-exports used by `loonfs` or required by public return types.
pub use engine::RuntimeReadContext;
pub use engine::{
    NamespaceEngine, NamespaceReaderEngine, NamespaceWriterEngine, ReadOnly, RootAccess, Writable,
};
pub use error::{
    classify_store_failure, CoreError, ErrorCode, ErrorKind, MetadataProjectionLoadError,
    MetadataViewError, Result, WriterFence,
};
pub use gc::{grace_age, GcOptions, GraceAge};
pub use manifest::ManifestLoadError;
pub use manifest::{
    next_run_no_after, refill_iterators, select_next_iterator, CompactionAdmission,
    CompactionUnitOutcome, FoldWalOutcome, FoldWalResponse, FoldedWalTail,
    MetadataCompactionCancellation, MetadataCompactionPolicy, MetadataFamilyGroup,
    MetadataRunPolicy, PublicationReport, PublishedManifest, RetentionTarget, SegmentBlockLoader,
    SegmentRowIterator,
};
pub use namespace::store::NamespaceStore;
pub use options::{CreateNamespaceOptions, DeleteNamespaceOptions};
pub use path::read::{
    CurrentFile, CurrentFileState, DirectDownloadByInodeTarget, DirectDownloadTarget, FileBytes,
    FileRevisionsPage, GrantedRange,
};
pub use permit_pool::{BudgetPermit, PermitPool, PermitPoolObserver, PoolCounts};
pub use phase::Phase;
pub use pin::{
    CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor, CheckpointPageCursor,
    ListCheckpointFilesOptions, SnapshotPageCursor,
};
pub use protocol::{
    DirectMultipartUploadTarget, DirectPutUploadTarget, MultipartPartTarget, MultipartPartTargets,
    UploadSessionView,
};
// The streaming read `loonfs`'s namespace handle returns, and the chunk size it
// reads in.
pub use limits::{CONTENT_READ_CHUNK_BYTES, MAX_CURRENT_FILES_PER_STAT};
pub use storage::content::{FileContentStream, StreamRange};
