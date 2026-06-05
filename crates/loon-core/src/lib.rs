#![forbid(unsafe_code)]

mod basis;
mod checkpoint;
pub mod commit;
pub mod content;
mod context;
mod engine;
mod error;
mod genesis;
pub mod invariants;
mod lease;
mod loading;
pub mod metadata;
pub mod namespace;
mod options;
mod path;
mod protocol;
pub mod publisher;
pub mod wal;

pub use basis::{
    load_namespace_head_summary, load_verified_namespace_basis,
    load_verified_namespace_basis_at_head, probe_namespace_head_etag, BasisLoadError,
    NamespaceHeadEtagProbe, NamespaceHeadSummary, VerifiedNamespaceBasis,
    VerifiedNamespaceBasisWeight,
};
pub use checkpoint::{
    advance_retention_floor, create_checkpoint, create_checkpoint_with_policy, CheckpointLoadError,
    CheckpointLoadErrorKind, MetadataLsmPolicy, MetadataTableCache, MetadataTableCacheConfig,
    MetadataTableCacheStats,
};
pub use context::MutationContext;
pub use engine::{NamespaceEngine, NamespaceEngineBuildError, NamespaceEngineBuilder};
pub use error::{CoreError, CoreErrorKind};
pub use lease::{acquire_or_renew_namespace_lease, LeaseAcquireError};
pub use loading::{
    load_content_store_descriptor_control, load_namespace_descriptor_control,
    load_namespace_head_control, load_namespace_lease_control, ControlObjectIdentity,
    ControlObjectLoadError, LoadedContentStoreDescriptorControl, LoadedHeadControl,
    LoadedLeaseControl, LoadedNamespaceDescriptorControl,
};
pub use namespace::BootstrapNamespaceError;
pub use options::{BootstrapOptions, CommitOptions, ForkOptions, ReadOptions, WriteOptions};
pub use path::intent::{PathMutationIntent, PutFileBehavior};
pub use path::planner::PlannedPathMutation;
pub use protocol::{
    begin_upload, commit_operations, commit_operations_batch, complete_upload, list_changes_after,
    upload_content,
};
pub use publisher::{
    publish_namespace_mutations_batch, DirectObjectStorePublisher, FlushPolicy,
    NamespaceCommitEngine, NamespaceMutationCandidate, PublishOptions,
};
