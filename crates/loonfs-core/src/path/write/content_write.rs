use crate::error::CoreError;
use crate::path::helpers::validate_path_for_mutation;
use crate::storage::content::store_bytes_as_content;
use loonfs_api::{ContentRef, NamespaceId};
use loonfs_objectstore::ObjectStore;

pub(super) async fn store_file_bytes_before_metadata_publish<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    absolute_path: &str,
    bytes: &[u8],
) -> Result<ContentRef, CoreError> {
    validate_path_for_mutation(absolute_path)?;
    let stored = store_bytes_as_content(store, namespace_id, bytes).await?;
    Ok(stored.content_ref)
}
