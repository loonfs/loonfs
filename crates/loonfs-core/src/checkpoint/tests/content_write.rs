//! Stages file bytes as durable content before a metadata publish.

use crate::error::Result;
use crate::namespace::catalog::load_namespace_catalog_entry;
use crate::path::mutation_path::parse_mutation_path;
use crate::storage::content::{prepare_stored_content, store_bytes_as_content_with_store_id};
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
    let stored =
        store_bytes_as_content_with_store_id(store, catalog.content_store_id().clone(), bytes)
            .await?;
    Ok(prepare_stored_content(&catalog, stored)?)
}
