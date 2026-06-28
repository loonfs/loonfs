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
//! ```no_run
//! use loonfs_api::NamespaceId;
//! use loonfs_core::{BootstrapOptions, NamespaceEngine, WriteOptions};
//! use loonfs_objectstore::fs::LocalFsStore;
//!
//! let store = LocalFsStore::new(std::env::temp_dir()).expect("store");
//! let namespace = NamespaceId::parse("docs").expect("valid namespace id");
//!
//! let engine = NamespaceEngine::builder(store)
//!     .namespace(namespace)
//!     .writer("example-writer")
//!     .build()
//!     .expect("engine");
//!
//! let _ = engine.bootstrap_namespace(BootstrapOptions::default());
//! let _ = engine.put_file("/plan.md", b"hello", WriteOptions::default());
//! let _ = engine.read_file("/plan.md");
//! ```

mod checkpoint;
pub mod commit;
pub mod content;
mod context;
mod control_update;
mod engine;
mod error;
mod invariants;
pub mod metadata;
pub mod namespace;
mod options;
mod path;
mod protocol;
mod publisher;
mod storage;
mod wal;

pub mod cache {
    pub use crate::checkpoint::{
        ManifestLoadError, ManifestLoadErrorKind, MetadataLsmPolicy, MetadataTableCache,
        MetadataTableCacheConfig, MetadataTableCacheStats, WalTailProjectionCache,
        WalTailProjectionCacheConfig, WalTailProjectionCacheKey, WalTailProjectionCacheStats,
    };
    pub use crate::namespace::status::{
        load_namespace_head_summary, probe_namespace_head_etag, NamespaceHeadEtagProbe,
        NamespaceHeadSummary,
    };
}

pub mod control {
    pub use crate::namespace::control::{
        load_content_store_descriptor_control, load_namespace_descriptor_control,
        load_namespace_head_control, ControlObjectIdentity, ControlObjectLoadError,
        LoadedContentStoreDescriptorControl, LoadedHeadControl, LoadedNamespaceDescriptorControl,
    };
}

pub mod publish {
    pub use crate::commit::{CommitHeadPublishError, SemanticMutationIdentity};
    pub use crate::path::write::PathMutationIntent;
    pub use crate::publisher::{
        DirectObjectStorePublisher, FlushPolicy, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, NamespaceMutationCandidate, PublishOptions,
    };
}

#[cfg(any(test, feature = "inspection"))]
pub mod inspection;

pub use context::MutationContext;
#[doc(hidden)]
pub use engine::RuntimeReadContext;
pub use engine::{BeginDirectPutUploadTargetResponse, DirectPutUploadTarget};
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{
    Error, ErrorCode, ErrorKind, MetadataProjectionLoadError, MetadataViewError, Result,
};
pub use namespace::BootstrapNamespaceError;
pub use options::{
    BootstrapOptions, CommitOptions, DeleteNamespaceOptions, ForkOptions, WriteOptions,
};
