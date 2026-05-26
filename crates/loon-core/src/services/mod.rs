pub(crate) mod content;
mod namespace_lifecycle;

pub use crate::context::MutationContext;
pub use crate::error::{CoreError, CoreErrorKind};
pub use crate::path::mutation::{
    copy_file_path, create_dir_path, delete_path, delete_path_non_recursive, move_path,
    put_file_bytes, put_file_content_ref, write_file_bytes, PutFileBehavior,
};
pub(crate) use crate::path::planner::{
    plan_path_mutation, plan_path_mutation_against_state,
    semantic_commit_fingerprint_for_path_intent,
};
pub use crate::path::query::{
    list_path, list_path_from_basis, read_file_bytes, read_file_bytes_from_basis, resolve_path,
    resolve_path_from_basis,
};
pub use content::{store_bytes_as_content, StoredContent};
pub use namespace_lifecycle::{
    bootstrap_namespace, fork_namespace, list_namespaces, BootstrapNamespaceError,
};
