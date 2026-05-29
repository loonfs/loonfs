#![forbid(unsafe_code)]

mod basis;
mod checkpoint;
pub mod commit;
pub mod content;
mod context;
mod error;
mod genesis;
pub mod invariants;
mod lease;
mod loading;
pub mod metadata;
pub mod namespace;
mod path;
mod protocol;
pub mod publisher;
pub mod services;
pub mod wal;

pub use basis::{
    load_namespace_head_identity, load_namespace_head_summary, load_verified_namespace_basis,
    BasisLoadError, NamespaceHeadIdentity, NamespaceHeadSummary, VerifiedNamespaceBasis,
};
pub use checkpoint::{
    advance_retention_floor, create_checkpoint, CheckpointLoadError, CheckpointLoadErrorKind,
};
pub use content::ContentValidationKey;
pub use context::MutationContext;
pub use error::{CoreError, CoreErrorKind};
pub use lease::{acquire_or_renew_namespace_lease, LeaseAcquireError};
pub use loading::{
    load_content_store_descriptor_control, load_namespace_descriptor_control,
    load_namespace_head_control, load_namespace_lease_control, ControlObjectIdentity,
    ControlObjectLoadError, LoadedContentStoreDescriptorControl, LoadedHeadControl,
    LoadedLeaseControl, LoadedNamespaceDescriptorControl,
};
pub use path::intent::{PathMutationIntent, PutFileBehavior};
pub use path::planner::PlannedPathMutation;
pub use protocol::{
    begin_upload, commit_operations, commit_operations_batch, complete_upload, list_changes_after,
    upload_content, upload_content_with_validation_key,
};
pub use publisher::{
    publish_namespace_mutations_batch,
    publish_namespace_mutations_batch_with_content_validation_hints, DirectObjectStorePublisher,
    FlushPolicy, NamespaceCommitEngine, NamespaceMutationCandidate, PublishOptions,
};
pub use services::{
    bootstrap_namespace, copy_file_path, create_dir_path, delete_path, delete_path_non_recursive,
    fork_namespace, list_namespaces, list_path, list_path_from_basis, move_path, put_file_bytes,
    put_file_content_ref, read_file_bytes, read_file_bytes_from_basis, resolve_path,
    resolve_path_from_basis, store_bytes_as_content, write_file_bytes, BootstrapNamespaceError,
    StoredContent,
};
