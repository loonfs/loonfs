//! Core LoonFS namespace operations.
//!
//! `loonfs-core` is the low-level API for building directly on the LoonFS
//! metadata protocol. Most callers should start with [`NamespaceEngine`].
//!
//! A namespace is one durable filesystem history. File bytes are written to
//! object storage first, then metadata is published as a committed namespace
//! mutation. Reads rebuild or reuse a verified view of the namespace before
//! walking paths.
//!
//! # Example
//!
//! Mutations are published as candidate batches through
//! [`publish::NamespaceCommitEngine`]; day-to-day reads and writes should go
//! through the `loonfs` crate's `FsReader`/`FsWriter` handles, which wrap
//! this crate with caching and batching.
//!
//! ```no_run
//! use loonfs_api::{AbsolutePath, CommitId, NamespaceId};
//! use loonfs_core::publish::{
//!     NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent, PublishTailOptions,
//! };
//! use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine};
//! use loonfs_objectstore::local_fs_store::LocalFsStore;
//!
//! let store = LocalFsStore::new(std::env::temp_dir()).expect("store");
//! let namespace = NamespaceId::parse("docs").expect("valid namespace id");
//!
//! let engine = NamespaceEngine::builder(store)
//!     .namespace_id(namespace.clone())
//!     .writer_id("example-writer")
//!     .build()
//!     .expect("engine");
//! let _ = engine.bootstrap_namespace(BootstrapOptions::default());
//!
//! let publish_store = LocalFsStore::new(std::env::temp_dir()).expect("store");
//! let context = MutationContext {
//!     writer_id: "example-writer".to_owned(),
//!     writer_session_id: "example-session".to_owned(),
//!     writer_version: "example/0.1".to_owned(),
//!     now_ms: 0,
//! };
//! let mut publisher = NamespaceCommitEngine::new(namespace);
//! let _ = publisher.publish_batch(
//!     &publish_store,
//!     vec![NamespaceMutationCandidate::path(PathMutationIntent::CreateDir {
//!         commit_id: CommitId::generate(),
//!         message: None,
//!         absolute_path: AbsolutePath::parse("/plans").expect("path"),
//!         parents: false,
//!     })],
//!     &context,
//!     &PublishTailOptions::default(),
//! );
//! ```

// Sanctioned consumers of this crate's public surface, in full:
//
// - **`loonfs`** — the embedded runtime, the only production consumer. It
//   wraps everything below with caching, batching, and handles, and re-exports
//   what applications need. Application code depends on `loonfs`, never on
//   this crate.
// - **`loonfs-core`'s own integration tests** (`tests/it`) — a white-box
//   consumer that asserts on durable layout and replay directly. It is why
//   `metadata` and parts of `commit` are public at all.
//
// Nothing else depends on this crate. `loonfs-grep` was extracted and reads
// filesystem state through `loonfs`; `loonfs-sim`, `loonfs-model`, and
// `loonfs-test-support` never depended on it; `loonfs-server` and `loonfs-cli`
// reach the durable control plane through `loonfs::control`.
//
// The module list below is grouped by that intent: private modules are engine
// internals, and each public one names why it is public.

// --- engine internals: private, reachable only through the seams below ---
mod checkpoint;
mod commit_engine;
mod context;
mod control_update;
mod engine;
mod error;
mod gc;
mod invariants;
mod namespace;
mod options;
mod protocol;
mod storage;
mod timing;
mod wal;

// --- public seams ---
/// Commit planning, validation, and materialization. Consumed by the `loonfs`
/// publisher and by this crate's commit-validation integration tests.
pub mod commit;
/// Content staging and preparation-token minting. Consumed by `loonfs`'s
/// write path and its server-integration `content_tokens` seam.
pub mod content;
/// Protocol and resource ceilings. Consumed by `loonfs` (re-exported to the
/// server for request validation) and by layout tests.
pub mod limits;
/// Durable metadata state and its row codecs. Public for this crate's
/// white-box integration tests, which compare projected state against the
/// reference model; `loonfs` reaches metadata only through the seams above.
pub mod metadata;
/// Path parsing and current-state resolution. Consumed by `loonfs`'s write
/// path (`ensure_mutation_path`, `parse_mutation_path`).
pub mod path;

/// Cache types and configuration for runtime read paths. Consumed by
/// `loonfs`, which owns the runtime's cache configuration and stats.
pub mod cache {
    pub use crate::checkpoint::{
        ManifestLoadError, ManifestLoadFailureClass, MetadataTableCache, MetadataTableCacheConfig,
        MetadataTableCacheStats, WalTailProjectionCache, WalTailProjectionCacheConfig,
        WalTailProjectionCacheKey, WalTailProjectionCacheStats,
        DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        DEFAULT_WAL_TAIL_PROJECTION_ROWS,
    };
    pub use crate::namespace::status::{
        load_deleted_namespace_head_summary, load_namespace_head_summary, NamespaceHeadSummary,
    };
}

/// Typed namespace control-object loaders and verified catalog state.
/// Consumed by `loonfs`'s cache and write paths, and re-exported as
/// `loonfs::control` for the white-box layout assertions the server and this
/// crate's own tests make.
pub mod control {
    pub use crate::namespace::catalog::{
        load_namespace_catalog_entry, NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
    pub use crate::namespace::control::{
        load_namespace_checkpoint_record_control, load_namespace_head_control,
        load_namespace_metadata_root_control, load_namespace_read_anchor,
        load_namespace_wal_floor_control, ControlObjectIdentity, ControlObjectLoadError,
        LoadedHeadControl, LoadedMetadataRootControl, LoadedWalFloorControl,
    };
    pub use crate::namespace::{BasisManifest, MetadataBasis};
}

/// Commit publication types for runtime integrations. Consumed by `loonfs`'s
/// publisher, and re-exported as `loonfs::publish` for the server's
/// filesystem handlers.
pub mod publish {
    pub use crate::commit::{CommitHeadPublishError, SemanticMutationIdentity};
    pub use crate::commit_engine::{
        ContentPreparation, ContentPreparationError, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, NamespaceMutation, NamespaceMutationCandidate,
        ResultingReadState, SharedWriterSessionState, WalTailPolicy, WriterSessionState,
    };
    pub use crate::path::write::PathMutationIntent;
    pub use crate::protocol::PublishTailOptions;
    pub use crate::storage::content_admission::PreparedContent;
}

// Crate-root re-exports. Every name below has a named consumer: `loonfs`
// unless the comment says otherwise, or reachability through a public
// signature where noted.
// `MetadataReorganizeReport` has no caller that names it; it stays public
// because it is the return type of `NamespaceEngine::reorganize_metadata`.
pub use checkpoint::{
    CheckpointFile, CheckpointFilesPage, CheckpointFilesPageCursor, MetadataReorganizeOutcome,
    MetadataReorganizeReport,
};
pub use context::MutationContext;
pub use engine::RuntimeReadContext;
pub use engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
// The builder pair is reachable through `NamespaceEngine::builder()` and its
// `build()`, so both stay public even though no caller names them directly.
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{
    Error, ErrorCode, ErrorKind, MetadataProjectionLoadError, MetadataViewError, StoreFailureClass,
    WriterFence,
};
pub use gc::{gc_namespace, GcConfig};
pub use namespace::BootstrapNamespaceError;
pub use options::{BootstrapOptions, DeleteNamespaceOptions};
pub use path::read::{CurrentFileState, MAX_RESOLVE_CURRENT_FILES};
