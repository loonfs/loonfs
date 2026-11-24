//! Staging content objects under a content ID that the caller already
//! allocated, from a stream or from bytes.

use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::storage::content::StoredContent;
use bytes::Bytes;
use futures::StreamExt;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, ObjectStoreErrorKind, ObjectStoreExtension, PutMode,
};
use loonfs_types::{
    ChecksumAlgorithm, ChecksumDigest, ContentId, ContentRef, ContentRefKind, PoolId, UploadId,
};
use std::sync::{Arc, Mutex};

impl<S: ObjectStore> NamespaceStore<S> {
    /// Plants test content under a fresh identity, under the prefix of the
    /// current namespace's pool.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = crate::Phase::WriteContentBlob.as_str(), key_class = "content")
    )]
    #[cfg(any(test, feature = "test-support"))]
    pub async fn store_bytes_as_content(&self, bytes: &[u8]) -> Result<StoredContent> {
        let current = self.load_current_manifest().await?;
        stage_bytes_under_content_id(
            &self.store,
            &current.payload().pool_id,
            ContentId::generate(),
            bytes,
            ContentKeyOwner::FreshIdentity,
        )
        .await
    }
}

/// The writer of a content key, which decides what an occupied key that holds
/// other bytes means.
///
/// An occupied key that holds the same bytes settles the write in either case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ContentKeyOwner<'a> {
    /// The write is the only writer of a freshly minted identity, so other
    /// bytes at the key are corruption.
    FreshIdentity,
    /// Every request to this proxied upload session writes the session's key,
    /// so other bytes at the key conflict with an earlier request.
    ProxiedSession(&'a UploadId),
}

impl ContentKeyOwner<'_> {
    fn store_error(self, object_key: &str, error: &ObjectStoreError) -> CoreError {
        match (self, error) {
            (Self::ProxiedSession(upload_id), ObjectStoreError::DifferentObject { .. }) => {
                CoreError::UploadContentConflict {
                    upload_id: upload_id.clone(),
                }
            }
            _ => CoreError::store(object_key, error),
        }
    }
}

/// Stages and hashes a payload under the prefix of `pool_id`, without
/// buffering the complete stream.
///
/// The write is create-only, and the store enforces that atomically with
/// completion at every size, so when two requests write one content ID,
/// only one of them creates the object. The other request is settled by the
/// store's rule for an occupied immutable key, against the checksum of the
/// bytes that passed through this writer.
pub(crate) async fn stage_streamed_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    pool_id: &PoolId,
    content_id: ContentId,
    body: ByteStream,
    owner: ContentKeyOwner<'_>,
) -> Result<ContentRef> {
    let object_key = content_blob(pool_id, &content_id);
    let observed = Arc::new(Mutex::new(StreamedPayload::new(store.checksum_algorithm())));
    let hashed = {
        let observed = Arc::clone(&observed);
        body.map(move |chunk| {
            let chunk = chunk?;
            observed
                .lock()
                .expect("the streamed payload lock should not be poisoned")
                .update(&chunk);
            Ok(chunk)
        })
        .boxed()
    };

    let stored = store
        .put_streamed(&object_key, hashed, PutMode::CreateIfAbsent)
        .await;
    let content_ref = std::mem::replace(
        &mut *observed
            .lock()
            .expect("the streamed payload lock should not be poisoned"),
        StreamedPayload::new(store.checksum_algorithm()),
    )
    .into_content_ref(content_id);
    match stored {
        Ok(stored_bytes) if stored_bytes != content_ref.size_bytes => Err(CoreError::Store {
            object_key,
            message: format!(
                "the provider stored {stored_bytes} bytes, but {} passed through this writer",
                content_ref.size_bytes
            ),
            kind: ObjectStoreErrorKind::Other,
        }),
        Ok(_) => Ok(content_ref),
        Err(conflict @ ObjectStoreError::PreconditionFailed { .. }) => {
            match store
                .verify_occupied(&object_key, &content_ref.checksum, conflict)
                .await
            {
                Ok(_) => Ok(content_ref),
                Err(error) => Err(owner.store_error(&object_key, &error)),
            }
        }
        Err(error) => Err(owner.store_error(&object_key, &error)),
    }
}

#[derive(Debug)]
struct StreamedPayload {
    size_bytes: u64,
    checksum: ChecksumDigest,
}

impl StreamedPayload {
    fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            size_bytes: 0,
            checksum: ChecksumDigest::new(algorithm),
        }
    }

    fn update(&mut self, chunk: &[u8]) {
        self.size_bytes += chunk.len() as u64;
        self.checksum.update(chunk);
    }

    fn into_content_ref(self, content_id: ContentId) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id,
            size_bytes: self.size_bytes,
            checksum: self.checksum.finish(),
        }
    }
}

/// Stages bytes under an identity the caller already allocated, for writers
/// that minted the id earlier (an upload session allocates at `begin`), under
/// the prefix of `pool_id`.
pub(crate) async fn stage_bytes_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    pool_id: &PoolId,
    content_id: ContentId,
    bytes: &[u8],
    owner: ContentKeyOwner<'_>,
) -> Result<StoredContent> {
    let content_ref = ContentRef::blob_v1(content_id, bytes, store.checksum_algorithm());
    let object_key = content_blob(pool_id, &content_ref.content_id);
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await
        .map_err(|error| owner.store_error(&object_key, &error))?;

    Ok(StoredContent {
        #[cfg(any(test, feature = "test-support"))]
        object_key,
        content_ref,
    })
}

#[cfg(test)]
mod tests {
    use super::super::content::tests::test_store;
    use super::*;
    use bytes::Bytes;
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::ObjectStoreExtension;

    use loonfs_objectstore::ObjectStore;

    use loonfs_test_support::stores::{KeyPredicate, MetadataMapStore};
    use loonfs_types::ContentId;
    use std::assert_matches;

    #[tokio::test]
    async fn staging_over_an_object_without_an_equal_stored_checksum_is_corruption() {
        let (_temp_dir, local) = test_store();
        let pool_id = loonfs_test_support::ids::numbered_pool_id(1);
        let content_id = ContentId::generate();
        let key = content_blob(&pool_id, &content_id);
        local
            .put_if_absent(&key, Bytes::from_static(b"staged"))
            .await
            .expect("occupant");
        let without_checksum =
            MetadataMapStore::new(&local, KeyPredicate::exact(key.clone()), |mut metadata| {
                metadata.checksum = None;
                metadata
            });
        let cases: [(&dyn ObjectStore, &[u8], &str); 2] = [
            (&local, b"other", "already exists with different bytes"),
            (
                &without_checksum,
                b"staged",
                "already exists without a stored checksum",
            ),
        ];
        for (store, bytes, reason) in cases {
            let error = stage_bytes_under_content_id(
                store,
                &pool_id,
                content_id.clone(),
                bytes,
                ContentKeyOwner::FreshIdentity,
            )
            .await
            .expect_err("occupied key");
            assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
            assert_matches!(
                error,
                CoreError::NamespaceCorrupt(message)
                    if message.contains(&key) && message.ends_with(reason)
            );
        }
    }
}
