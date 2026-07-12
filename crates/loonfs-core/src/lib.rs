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
//! use loonfs_api::{CommitId, NamespaceId};
//! use loonfs_core::publish::{
//!     NamespaceCommitEngine, NamespaceMutationCandidate, PathMutationIntent,
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
//!     vec![NamespaceMutationCandidate::Path(PathMutationIntent::CreateDir {
//!         commit_id: CommitId::generate(),
//!         absolute_path: "/plans".to_owned(),
//!     })],
//!     &context,
//! );
//! ```

mod checkpoint;
pub mod commit;
mod commit_engine;
pub mod content;
mod context;
mod control_update;
mod engine;
mod error;
pub mod gc;
mod invariants;
pub mod limits;
pub mod metadata;
pub mod namespace;
mod options;
pub mod path;
mod protocol;
mod query;
mod storage;
pub mod timing;
mod wal;

pub mod cache {
    pub use crate::checkpoint::{
        ManifestLoadError, ManifestLoadFailureClass, MetadataTableCache, MetadataTableCacheConfig,
        MetadataTableCacheStats, WalTailProjectionCache, WalTailProjectionCacheConfig,
        WalTailProjectionCacheKey, WalTailProjectionCacheStats,
        DEFAULT_METADATA_TABLE_CACHE_DECODED_BYTES, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        DEFAULT_WAL_TAIL_PROJECTION_ROWS,
    };
    pub use crate::namespace::status::{load_namespace_head_summary, NamespaceHeadSummary};
}

pub mod control {
    pub use crate::namespace::catalog::{
        load_namespace_catalog_entry, NamespaceCatalogLoadError, VerifiedNamespaceCatalogEntry,
    };
    pub use crate::namespace::control::{
        load_content_store_descriptor_control, load_namespace_checkpoint_record_control,
        load_namespace_descriptor_control, load_namespace_head_control,
        load_namespace_metadata_root_control, load_namespace_read_anchor,
        load_namespace_wal_floor_control, ControlObjectIdentity, ControlObjectLoadError,
        LoadedContentStoreDescriptorControl, LoadedHeadControl, LoadedMetadataRootControl,
        LoadedNamespaceDescriptorControl, LoadedWalFloorControl,
    };
}

pub mod publish {
    pub use crate::commit::{CommitHeadPublishError, SemanticMutationIdentity};
    pub use crate::commit_engine::{
        NamespaceCommitEngine, NamespaceCommitEnginePublishResult, NamespaceMutationCandidate,
        ResultingReadState,
    };
    pub use crate::path::write::PathMutationIntent;
    pub use crate::protocol::PublishTailOptions;
}

pub use checkpoint::{
    GramIndexBuildOutcome, GramIndexBuildPolicy, GramIndexBuildReport, GramIndexDisableOutcome,
    GramIndexEnableOutcome, GramIndexFoldOutcome, GramIndexFoldReport, MetadataReorganizeOutcome,
    MetadataReorganizeReport,
};
pub use context::MutationContext;
#[doc(hidden)]
pub use engine::RuntimeReadContext;
pub use engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{
    Error, ErrorCode, ErrorKind, MetadataProjectionLoadError, MetadataViewError, Result,
};
pub use gc::{gc_namespace, GcConfig, GcReport};
pub use namespace::BootstrapNamespaceError;
pub use options::{BootstrapOptions, DeleteNamespaceOptions, WriteOptions};
pub use timing::{MonotonicTimer, StdMonotonicTimer};
