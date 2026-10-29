//! Staging content objects under a content ID that the caller already
//! allocated, from a stream or from bytes.

use crate::error::{CoreError, Result};
use crate::namespace::store::NamespaceStore;
use crate::storage::content::StoredContent;
use bytes::Bytes;
use futures::StreamExt;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{
    ByteStream, ObjectStore, ObjectStoreError, ObjectStoreErrorKind, PutMode,
};
use loonfs_types::{
    ChecksumAlgorithm, ChecksumDigest, ContentId, ContentRef, ContentRefKind, NamespaceId,
};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

impl<S: ObjectStore> NamespaceStore<S> {
    /// Plants test content under a fresh identity owned by the current namespace.
    #[tracing::instrument(
        level = "debug",
        name = "loonfs.phase",
        err(level = "warn"),
        skip_all,
        fields(phase = crate::Phase::WriteContentBlob.as_str(), key_class = "content")
    )]
    #[cfg(any(test, feature = "test-support"))]
    pub async fn store_bytes_as_content(&self, bytes: &[u8]) -> Result<StoredContent> {
        self.load_catalog_entry().await?;
        stage_bytes_under_content_id(
            &self.store,
            self.namespace_id.clone(),
            ContentId::generate(),
            bytes,
        )
        .await
    }
}

/// Result of staging streamed content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StagedStream {
    /// Identity, length, and checksum of the complete payload.
    pub content_ref: ContentRef,
    /// Whether the object existed before this write.
    pub already_present: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamedPayloadKind {
    Request,
    ContentImport,
}

/// Stages and hashes a payload without buffering the complete stream.
///
/// The write is create-only, and the store enforces that atomically with
/// completion at every size, so when two requests write one content ID,
/// only one of them creates the object and the other finds it present.
pub(crate) async fn stage_streamed_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    owner_namespace_id: NamespaceId,
    content_id: ContentId,
    body: ByteStream,
    payload_kind: StreamedPayloadKind,
    check_ownership: impl Future<Output = Result<()>>,
) -> Result<StagedStream> {
    let object_key = content_blob(&owner_namespace_id, &content_id);
    let observed = Arc::new(Mutex::new(StreamedPayload::new(store.checksum_algorithm())));
    let (request_check, check_requested) = oneshot::channel();
    let (allow_completion, completion_allowed) = oneshot::channel();
    let hashed = {
        let observed = Arc::clone(&observed);
        let object_key = object_key.clone();
        futures::stream::try_unfold(
            (body, request_check, completion_allowed),
            move |(mut body, request_check, completion_allowed)| {
                let observed = Arc::clone(&observed);
                let object_key = object_key.clone();
                async move {
                    if let Some(chunk) = body.next().await {
                        let chunk = chunk?;
                        observed
                            .lock()
                            .expect("the streamed payload lock should not be poisoned")
                            .update(&chunk);
                        return Ok(Some((chunk, (body, request_check, completion_allowed))));
                    }
                    // No caller bytes remain. A streamed write has one part in
                    // flight, so only the final part, the precondition, and the
                    // completion follow, each within one provider request bound.
                    // Together they finish inside the grace window, before an
                    // upload aborted after this check can lose its record.
                    let _ = request_check.send(());
                    completion_allowed.await.map_err(|_| {
                        ObjectStoreError::transport(&object_key, "upload session check failed")
                    })?;
                    Ok(None)
                }
            },
        )
        .boxed()
    };

    // ByteStream is 'static; the session check borrows the caller's store.
    let check = async {
        if check_requested.await.is_ok() {
            check_ownership.await?;
            let _ = allow_completion.send(());
        }
        Ok::<_, CoreError>(())
    };
    let (stored, checked) = futures::join!(
        store.put_streamed(&object_key, hashed, PutMode::CreateIfAbsent),
        check,
    );
    checked?;
    let observed = std::mem::replace(
        &mut *observed
            .lock()
            .expect("the streamed payload lock should not be poisoned"),
        StreamedPayload::new(store.checksum_algorithm()),
    );
    let already_present = match stored {
        Ok(stored_bytes) if stored_bytes != observed.size_bytes => {
            return Err(CoreError::Store {
                object_key,
                message: format!(
                    "the provider stored {stored_bytes} bytes, but {} passed through this writer",
                    observed.size_bytes
                ),
                kind: ObjectStoreErrorKind::Other,
            });
        }
        Ok(_) => false,
        // The caller compares the stored checksum with the bytes it sent.
        Err(ObjectStoreError::PreconditionFailed { .. }) => true,
        Err(ObjectStoreError::BodyAborted(message))
            if payload_kind == StreamedPayloadKind::ContentImport =>
        {
            return Err(CoreError::NamespaceCorrupt(message));
        }
        Err(err) => return Err(CoreError::store(&object_key, &err)),
    };

    Ok(StagedStream {
        content_ref: observed.into_content_ref(owner_namespace_id, content_id),
        already_present,
    })
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

    fn into_content_ref(
        self,
        owner_namespace_id: NamespaceId,
        content_id: ContentId,
    ) -> ContentRef {
        ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id,
            content_id,
            size_bytes: self.size_bytes,
            checksum: self.checksum.finish(),
        }
    }
}

/// Stages bytes under an identity the caller already allocated, for writers
/// that minted the id earlier (an upload session allocates at `begin`).
pub(crate) async fn stage_bytes_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    owner_namespace_id: NamespaceId,
    content_id: ContentId,
    bytes: &[u8],
) -> Result<StoredContent> {
    let content_ref = ContentRef::blob_v1(
        owner_namespace_id,
        content_id,
        bytes,
        store.checksum_algorithm(),
    );
    let object_key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await
        .map_err(|error| CoreError::store(&object_key, &error))?;

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
    async fn staging_identical_bytes_twice_mints_two_distinct_objects() {
        let (_temp_dir, store) = test_store();
        let bytes = b"identical payload";

        let first = stage_bytes_under_content_id(
            &store,
            loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            bytes,
        )
        .await
        .expect("first stage");
        let second = stage_bytes_under_content_id(
            &store,
            loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            bytes,
        )
        .await
        .expect("second stage");

        assert_ne!(
            first.content_ref().content_id,
            second.content_ref().content_id,
            "each staging write owns its own content object"
        );
        assert_ne!(first.object_key(), second.object_key());
        assert_eq!(
            first.content_ref().checksum,
            second.content_ref().checksum,
            "identical bytes still carry identical evidence"
        );
        for stored in [&first, &second] {
            assert_eq!(
                store
                    .get(stored.object_key(), None)
                    .await
                    .expect("read staged object")
                    .expect("staged object exists"),
                Bytes::from_static(b"identical payload")
            );
        }
    }

    #[tokio::test]
    async fn staging_over_an_object_without_an_equal_stored_checksum_is_corruption() {
        let (_temp_dir, local) = test_store();
        let namespace_id = loonfs_types::NamespaceId::parse("demo").expect("namespace id");
        let content_id = ContentId::generate();
        let key = content_blob(&namespace_id, &content_id);
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
                namespace_id.clone(),
                content_id.clone(),
                bytes,
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
