//! Core LoonFS namespace operations.
//!
//! `loonfs-core` is the low-level API for building directly on the LoonFS
//! metadata protocol. Most callers should start with [`NamespaceEngine`].

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
pub mod metadata;
pub mod namespace;
mod options;
pub mod path;
mod protocol;
mod storage;
pub mod timing;
mod wal;

pub mod cache {
    pub use crate::checkpoint::{
        ManifestLoadError, ManifestLoadFailureClass, MetadataLsmPolicy, MetadataTableCache,
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
        DirectObjectStorePublisher, FlushPolicy, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, NamespaceMutationCandidate, PublishOptions,
    };
    pub use crate::path::write::PathMutationIntent;
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
pub use gc::{gc_namespace, GcConfig, GcReport};
pub use namespace::BootstrapNamespaceError;
pub use options::{BootstrapOptions, DeleteNamespaceOptions, WriteOptions};
pub use timing::{MonotonicTimer, StdMonotonicTimer};
