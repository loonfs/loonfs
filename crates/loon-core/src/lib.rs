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
pub use context::MutationContext;
pub use error::{CoreError, CoreErrorKind};
pub use lease::{acquire_or_renew_namespace_lease, LeaseAcquireError};
pub use protocol::{
    begin_upload, commit_operations, commit_operations_batch, complete_upload, list_changes_after,
    upload_content,
};
pub use publisher::{
    publish_namespace_mutations_batch, DirectObjectStorePublisher, FlushPolicy,
    NamespaceMutationCandidate, PathMutationIntent, PlannedPathMutation, PublishOptions,
};
pub use services::{
    bootstrap_namespace, copy_file_path, create_dir_path, delete_path, delete_path_non_recursive,
    fork_namespace, list_namespaces, list_path, list_path_from_basis, move_path, put_file_bytes,
    put_file_content_ref, read_file_bytes, read_file_bytes_from_basis, resolve_path,
    resolve_path_from_basis, store_bytes_as_content, write_file_bytes, BootstrapNamespaceError,
    PutFileBehavior, StoredContent,
};
