//! Core LoonFS namespace operations.
//!
//! `loon-core` is the low-level API for building directly on the LoonFS
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
//! use loon_api::NamespaceId;
//! use loon_core::{BootstrapOptions, NamespaceEngine, ReadOptions, WriteOptions};
//! use loon_objectstore::fs::LocalFsStore;
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
//! let _ = engine.read_file("/plan.md", ReadOptions::default());
//! ```

#![forbid(unsafe_code)]

#[path = "checkpoint/mod.rs"]
mod checkpoint;
pub mod commit;
pub mod content;
mod context;
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
        MetadataTableCacheConfig, MetadataTableCacheStats,
    };
    pub use crate::namespace::basis::{
        load_namespace_head_summary, load_verified_namespace_basis,
        load_verified_namespace_basis_at_head, probe_namespace_head_etag, BasisLoadError,
        NamespaceHeadEtagProbe, NamespaceHeadSummary, VerifiedNamespaceBasis,
        VerifiedNamespaceBasisWeight,
    };
}

pub mod control {
    pub use crate::namespace::control::{
        load_content_store_descriptor_control, load_namespace_descriptor_control,
        load_namespace_head_control, load_namespace_lease_control, ControlObjectIdentity,
        ControlObjectLoadError, LoadedContentStoreDescriptorControl, LoadedHeadControl,
        LoadedLeaseControl, LoadedNamespaceDescriptorControl,
    };
}

pub mod publish {
    pub use crate::commit::{CommitHeadPublishError, SemanticMutationIdentity};
    pub use crate::path::write::PathMutationIntent;
    pub use crate::publisher::{
        BasisReuseEvent, DirectObjectStorePublisher, FlushPolicy, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, NamespaceMutationCandidate, PublishOptions,
        VerifiedBasisCacheUpdate,
    };
}

#[cfg(any(test, feature = "inspection"))]
pub mod inspection;

pub use context::MutationContext;
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{Error, ErrorCode, ErrorKind, Result};
pub use namespace::{list_namespaces, BootstrapNamespaceError};
pub use options::{
    BootstrapOptions, CommitOptions, ForkOptions, ReadOptions, ReadSource, WriteOptions,
};
pub use path::write::PutFileBehavior;
