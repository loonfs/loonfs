//! Stages file bytes as durable content before a metadata publish.

use crate::error::Result;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::path::mutation_path::parse_mutation_path;
use crate::storage::content::{prepare_stored_content, stage_bytes_under_content_id};
use crate::storage::content_admission::PreparedContent;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(super) async fn store_file_bytes_before_metadata_publish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
) -> Result<PreparedContent> {
    parse_mutation_path(absolute_path)?;
    let catalog = load_namespace_catalog_entry(store, namespace_id).await?;
    let stored = stage_bytes_under_content_id(
        store,
        catalog.namespace_id().clone(),
        catalog.generation(),
        loonfs_api::ContentId::generate(),
        bytes,
    )
    .await?;
    Ok(prepare_stored_content(&catalog, stored))
}
