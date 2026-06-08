#![forbid(unsafe_code)]

mod basis;
mod checkpoint;
pub mod commit;
pub mod content;
mod context;
mod engine;
mod error;
mod genesis;
mod invariants;
mod lease;
mod loading;
pub mod metadata;
pub mod namespace;
mod options;
mod path;
mod protocol;
mod publisher;
mod wal;

pub mod cache {
    pub use crate::basis::{
        load_namespace_head_summary, load_verified_namespace_basis,
        load_verified_namespace_basis_at_head, probe_namespace_head_etag, BasisLoadError,
        NamespaceHeadEtagProbe, NamespaceHeadSummary, VerifiedNamespaceBasis,
        VerifiedNamespaceBasisWeight,
    };
    pub use crate::checkpoint::{
        CheckpointLoadError, CheckpointLoadErrorKind, MetadataLsmPolicy, MetadataTableCache,
        MetadataTableCacheConfig, MetadataTableCacheStats,
    };
}

pub mod control {
    pub use crate::loading::{
        load_content_store_descriptor_control, load_namespace_descriptor_control,
        load_namespace_head_control, load_namespace_lease_control, ControlObjectIdentity,
        ControlObjectLoadError, LoadedContentStoreDescriptorControl, LoadedHeadControl,
        LoadedLeaseControl, LoadedNamespaceDescriptorControl,
    };
}

pub mod publish {
    pub use crate::commit::{CommitHeadPublishError, SemanticMutationIdentity};
    pub use crate::path::intent::PathMutationIntent;
    pub use crate::publisher::{
        BasisReuseEvent, DirectObjectStorePublisher, FlushPolicy, NamespaceCommitEngine,
        NamespaceCommitEnginePublishResult, NamespaceMutationCandidate, PublishOptions,
        VerifiedBasisCacheUpdate,
    };
}

pub use context::MutationContext;
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{Error, ErrorCode, ErrorKind, Result};
pub use namespace::{list_namespaces, BootstrapNamespaceError};
pub use options::{
    BootstrapOptions, CommitOptions, ForkOptions, ReadOptions, ReadSource, WriteOptions,
};
pub use path::intent::PutFileBehavior;
