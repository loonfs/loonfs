#![forbid(unsafe_code)]

mod basis;
mod checkpoint;
pub mod commit;
pub mod content;
mod genesis;
pub mod invariants;
mod lease;
mod loading;
pub mod metadata;
pub mod namespace;
mod protocol;
pub mod services;
pub mod wal;

pub use basis::{load_verified_namespace_basis, BasisLoadError, VerifiedNamespaceBasis};
pub use checkpoint::{
    advance_retention_floor, create_checkpoint, CheckpointLoadError, CheckpointLoadErrorKind,
};
pub use lease::{acquire_or_renew_namespace_lease, LeaseAcquireError};
pub use protocol::{
    begin_upload, commit_operations, complete_upload, list_changes_after, upload_block,
};
pub use services::{
    bootstrap_namespace, copy_file_path, create_namespace, delete_path, delete_path_non_recursive,
    list_namespaces, list_path, move_path, put_file_bytes, put_file_manifest, read_file_bytes,
    rename_namespace, resolve_namespace_selector, resolve_path, store_bytes_as_content,
    write_file_bytes, BootstrapNamespaceError, CoreError, CoreErrorKind, MutationContext,
    PutFileBehavior, StoredContent,
};
