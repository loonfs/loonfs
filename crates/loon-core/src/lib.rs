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
    load_namespace_head_summary, load_verified_namespace_basis,
    load_verified_namespace_basis_at_head, probe_namespace_head_etag, BasisLoadError,
    NamespaceHeadEtagProbe, NamespaceHeadSummary, VerifiedNamespaceBasis,
};
pub use checkpoint::{
    advance_retention_floor, create_checkpoint, create_checkpoint_with_policy, CheckpointLoadError,
    CheckpointLoadErrorKind, MetadataLsmPolicy,
};
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
    upload_content,
};
pub use publisher::{
    publish_namespace_mutations_batch, DirectObjectStorePublisher, FlushPolicy,
    NamespaceCommitEngine, NamespaceMutationCandidate, PublishOptions,
};
pub use services::{
    bootstrap_namespace, copy_file_path, create_dir_path, delete_path, delete_path_non_recursive,
    fork_namespace, list_file_revisions, list_file_revisions_for_inode,
    list_file_revisions_for_inode_from_basis, list_file_revisions_from_basis, list_namespaces,
    list_path, list_path_from_basis, list_path_from_materialized_tables,
    list_path_from_materialized_tables_at_head, list_path_with_read_source, move_path,
    put_file_bytes, put_file_content_ref, read_file_bytes, read_file_bytes_from_basis,
    read_file_revision_bytes, read_file_revision_bytes_for_inode,
    read_file_revision_bytes_for_inode_from_basis, read_file_revision_bytes_from_basis,
    resolve_path, resolve_path_from_basis, resolve_path_from_materialized_tables,
    resolve_path_from_materialized_tables_at_head, resolve_path_with_read_source,
    restore_file_revision, store_bytes_as_content, write_file_bytes, BootstrapNamespaceError,
    MetadataRead, MetadataReadSource, StoredContent,
};
