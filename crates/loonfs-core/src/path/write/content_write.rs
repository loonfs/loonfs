//! Stages file bytes as durable content before a metadata publish.

use crate::error::Result;
use crate::path::helpers::validate_path_for_mutation;
use crate::storage::content::{prepare_stored_content, store_bytes_as_content};
use crate::storage::content_admission::PreparedContent;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;

pub(super) async fn store_file_bytes_before_metadata_publish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
) -> Result<PreparedContent> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes).await?;
    Ok(prepare_stored_content(namespace_id.clone(), stored))
}
