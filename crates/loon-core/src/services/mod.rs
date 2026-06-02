mod namespace_lifecycle;

pub use crate::content::{store_bytes_as_content, StoredContent};
pub use crate::context::MutationContext;
pub use crate::error::{CoreError, CoreErrorKind};
pub use crate::path::intent::PutFileBehavior;
pub use crate::path::mutation::{
    copy_file_path, create_dir_path, delete_path, delete_path_non_recursive, move_path,
    put_file_bytes, put_file_content_ref, restore_file_revision, write_file_bytes,
};
pub use crate::path::query::{
    list_file_revisions, list_file_revisions_for_inode, list_file_revisions_for_inode_from_basis,
    list_file_revisions_from_basis, list_path, list_path_from_basis,
    list_path_from_materialized_tables, list_path_from_materialized_tables_at_head,
    list_path_from_materialized_tables_at_head_with_cache, list_path_with_read_source,
    read_file_bytes, read_file_bytes_from_basis, read_file_revision_bytes,
    read_file_revision_bytes_for_inode, read_file_revision_bytes_for_inode_from_basis,
    read_file_revision_bytes_from_basis, resolve_path, resolve_path_from_basis,
    resolve_path_from_materialized_tables, resolve_path_from_materialized_tables_at_head,
    resolve_path_from_materialized_tables_at_head_with_cache, resolve_path_with_read_source,
    MetadataRead, MetadataReadSource,
};
pub use namespace_lifecycle::{
    bootstrap_namespace, fork_namespace, list_namespaces, BootstrapNamespaceError,
};
