//! Replacement of a stored object's bytes.

use bytes::Bytes;
use loonfs_objectstore::ObjectStoreExtension;
use loonfs_objectstore::{ObjectMetadata, ObjectStore, ObjectStoreError};

/// Replaces the object at `key` with `bytes` by deleting it and creating it
/// again, for a test that corrupts or rewrites a stored object.
pub async fn replace_object<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    bytes: Bytes,
) -> Result<ObjectMetadata, ObjectStoreError> {
    store.delete(key).await?;
    store.put_if_absent(key, bytes).await
}
