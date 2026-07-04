use crate::error::CoreError;
use crate::invariants::InvariantId;
use crate::namespace::catalog::load_namespace_content_store_id;
use bytes::Bytes;
use loonfs_api::{sha256_digest, ContentRef, ContentRefKind, ContentStoreId, NamespaceId};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedDurableContent {
    pub content_ref: ContentRef,
    pub object_key: String,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub checked_invariants: Vec<InvariantId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadDurableContent {
    pub validated: ValidatedDurableContent,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredContent {
    pub content_store_id: ContentStoreId,
    pub object_key: String,
    pub content_ref: ContentRef,
    pub file_digest_sha256: String,
    pub file_size_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ContentValidationTracker {
    validated: HashSet<(ContentStoreId, ContentRef)>,
}

impl ContentValidationTracker {
    pub(crate) async fn ensure_validated<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
    ) -> Result<(), DurableContentValidationError> {
        let key = (content_store_id.clone(), content_ref.clone());
        if self.validated.contains(&key) {
            return Ok(());
        }
        validate_durable_content_reference(store, content_store_id, content_ref).await?;
        self.validated.insert(key);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum DurableContentValidationError {
    #[error("unsupported content ref kind `{kind}`")]
    UnsupportedContentRefKind { kind: ContentRefKind },
    #[error("invalid content digest `{digest}`: {message}")]
    InvalidDigest { digest: String, message: String },
    #[error("missing content object `{object_key}`")]
    MissingContentObject { object_key: String },
    #[error("content length mismatch for `{object_key}`: expected {expected}, actual {actual}")]
    ContentLengthMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "content digest mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ContentDigestMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub(crate) enum ImmutableObjectWriteError {
    #[error("failed to write immutable object `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

pub async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    if let Some(validated) = validate_content_metadata(store, &object_key, content_ref).await? {
        return Ok(validated);
    }

    let bytes = load_required_object(store, &object_key).await?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)
}

pub async fn read_durable_content_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ReadDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let bytes = load_required_object(store, &object_key).await?;
    let validated = validate_loaded_content_bytes(object_key, content_ref, &bytes)?;

    Ok(ReadDurableContent { validated, bytes })
}

fn content_object_key_for_ref(
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<String, DurableContentValidationError> {
    if content_ref.kind != ContentRefKind::WholeFileV0 {
        return Err(DurableContentValidationError::UnsupportedContentRefKind {
            kind: content_ref.kind.clone(),
        });
    }

    content_blob(content_store_id.as_str(), &content_ref.digest).map_err(|err| {
        DurableContentValidationError::InvalidDigest {
            digest: content_ref.digest.clone(),
            message: err.to_string(),
        }
    })
}

fn validate_loaded_content_bytes(
    object_key: String,
    content_ref: &ContentRef,
    bytes: &[u8],
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let actual_size = bytes.len() as u64;
    if actual_size != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: actual_size,
        });
    }

    let actual_digest = sha256_digest(bytes);
    if actual_digest != content_ref.digest {
        return Err(DurableContentValidationError::ContentDigestMismatch {
            object_key,
            expected: content_ref.digest.clone(),
            actual: actual_digest,
        });
    }

    Ok(ValidatedDurableContent {
        content_ref: content_ref.clone(),
        object_key,
        file_size_bytes: actual_size,
        file_digest_sha256: actual_digest,
        checked_invariants: vec![
            InvariantId::WholeFileContentRefKindIsSupported,
            InvariantId::WholeFileContentObjectKeyMatchesDigest,
            InvariantId::WholeFileContentSizeMatchesRef,
            InvariantId::WholeFileContentDigestMatchesRef,
        ],
    })
}

async fn validate_content_metadata<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    content_ref: &ContentRef,
) -> Result<Option<ValidatedDurableContent>, DurableContentValidationError> {
    let metadata = match store.head_with_checksum(object_key).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            return Err(DurableContentValidationError::MissingContentObject {
                object_key: object_key.to_owned(),
            })
        }
        Err(err) => {
            return Err(DurableContentValidationError::Store {
                object_key: object_key.to_owned(),
                message: err.message(),
            })
        }
    };

    if metadata.size_bytes != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key: object_key.to_owned(),
            expected: content_ref.size_bytes,
            actual: metadata.size_bytes,
        });
    }

    let Some(actual_digest) = metadata.checksum_sha256 else {
        return Ok(None);
    };

    if actual_digest != content_ref.digest {
        return Err(DurableContentValidationError::ContentDigestMismatch {
            object_key: object_key.to_owned(),
            expected: content_ref.digest.clone(),
            actual: actual_digest,
        });
    }

    Ok(Some(ValidatedDurableContent {
        content_ref: content_ref.clone(),
        object_key: object_key.to_owned(),
        file_size_bytes: metadata.size_bytes,
        file_digest_sha256: actual_digest,
        checked_invariants: vec![
            InvariantId::WholeFileContentRefKindIsSupported,
            InvariantId::WholeFileContentObjectKeyMatchesDigest,
            InvariantId::WholeFileContentSizeMatchesRef,
            InvariantId::WholeFileContentDigestMatchesRef,
        ],
    }))
}

#[tracing::instrument(
    level = "info",
    name = "loon.phase",
    err,
    skip_all,
    fields(phase = "write_content_blob", key_class = "content_blob")
)]
pub async fn store_bytes_as_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id).await?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Internal(format!("failed to derive content blob key: {err}")))?;
    write_immutable_object(store, &object_key, bytes).await?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_digest_sha256: content_ref.digest.clone(),
        file_size_bytes: content_ref.size_bytes,
        content_ref,
    })
}

pub(crate) async fn write_immutable_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<(), ImmutableObjectWriteError> {
    match store
        .put_if_absent(object_key, Bytes::copy_from_slice(expected_bytes))
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            if existing_object_matches_expected_bytes(store, object_key, expected_bytes).await? {
                return Ok(());
            }
            Err(ImmutableObjectWriteError::Store {
                object_key: object_key.to_owned(),
                message: "object already exists with different bytes".to_owned(),
            })
        }
        Err(err) => Err(ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
        }),
    }
}

async fn existing_object_matches_expected_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<bool, ImmutableObjectWriteError> {
    let expected_size = expected_bytes.len() as u64;
    let expected_digest = sha256_digest(expected_bytes);
    if let Some(metadata) = store.head_with_checksum(object_key).await.map_err(|err| {
        ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
        }
    })? {
        if metadata.size_bytes != expected_size {
            return Ok(false);
        }
        if let Some(digest) = metadata.checksum_sha256.as_deref() {
            return Ok(digest == expected_digest);
        }
    }

    let existing = store
        .get(object_key, None)
        .await
        .map_err(|err| ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
        })?
        .ok_or_else(|| ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: "object is missing after precondition failure".to_owned(),
        })?;
    Ok(existing == expected_bytes)
}

async fn load_required_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> Result<Vec<u8>, DurableContentValidationError> {
    match store.get(object_key, None).await {
        Ok(Some(bytes)) => Ok(bytes.to_vec()),
        Ok(None) => Err(DurableContentValidationError::MissingContentObject {
            object_key: object_key.to_owned(),
        }),
        Err(err) => Err(DurableContentValidationError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_durable_content_bytes, validate_durable_content_reference,
        DurableContentValidationError,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::{ContentRef, ContentRefKind, ContentStoreId};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    #[tokio::test]
    async fn validate_whole_file_content_ref_success() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"whole file bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let validated = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(validated.content_ref, content_ref);
        assert_eq!(validated.file_size_bytes, bytes.len() as u64);
        assert_eq!(validated.file_digest_sha256, content_ref.digest);
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_falls_back_to_get_when_checksum_metadata_is_absent() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = NoChecksumStore::new(inner);
        let bytes = b"whole file bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset_content_blob_get_count();
        validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(store.content_blob_get_count(), 1);
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_accepts_empty_files() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("read empty content ref");
        assert_eq!(read.bytes, bytes);
        assert_eq!(read.validated.file_size_bytes, 0);
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_rejects_missing_object() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef::whole_file_v0(b"missing");

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let mut content_ref = ContentRef::whole_file_v0(b"abc");
        put_content_object(&store, &content_store_id, &content_ref, b"abc").await;
        content_ref.size_bytes += 1;

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("size mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_rejects_digest_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef::whole_file_v0(b"expected");
        put_content_object(&store, &content_store_id, &content_ref, b"mismatch").await;

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("digest mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentDigestMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn validate_whole_file_content_ref_rejects_unsupported_kind() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef {
            kind: ContentRefKind::Unsupported("kind_from_the_future".to_owned()),
            digest: ContentRef::whole_file_v0(b"bytes").digest,
            size_bytes: 5,
        };

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("unsupported content ref kind");
        assert!(matches!(
            err,
            DurableContentValidationError::UnsupportedContentRefKind { .. }
        ));
    }

    fn test_store() -> (tempfile::TempDir, LocalFsStore, ContentStoreId) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let content_store_id = ContentStoreId::parse("cs_00000000000000000000000000000001")
            .expect("valid content store id");
        (temp_dir, store, content_store_id)
    }

    async fn put_content_object(
        store: &impl ObjectStore,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
        bytes: &[u8],
    ) {
        let key =
            content_blob(content_store_id.as_str(), &content_ref.digest).expect("content key");
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("put content");
    }

    #[derive(Debug)]
    struct NoChecksumStore {
        inner: LocalFsStore,
        content_blob_gets: AtomicUsize,
    }

    impl NoChecksumStore {
        fn new(inner: LocalFsStore) -> Self {
            Self {
                inner,
                content_blob_gets: AtomicUsize::new(0),
            }
        }

        fn content_blob_get_count(&self) -> usize {
            self.content_blob_gets.load(Ordering::Relaxed)
        }

        fn reset_content_blob_get_count(&self) {
            self.content_blob_gets.store(0, Ordering::Relaxed);
        }
    }

    #[async_trait]
    impl ObjectStore for NoChecksumStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            let mut metadata = self.inner.head(key).await?;
            if let Some(metadata) = &mut metadata {
                metadata.checksum_sha256 = None;
            }
            Ok(metadata)
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            if key.starts_with("content-stores/") && key.contains("/blobs/") {
                self.content_blob_gets.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            if key.starts_with("content-stores/") && key.contains("/blobs/") {
                self.content_blob_gets.fetch_add(1, Ordering::Relaxed);
            }
            let mut body = self.inner.get_with_metadata(key).await?;
            if let Some(body) = &mut body {
                body.metadata.checksum_sha256 = None;
            }
            Ok(body)
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.inner.put(key, bytes, mode).await
        }

        async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key).await
        }

        fn list_prefix_stream(
            &self,
            prefix: &str,
        ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
            self.inner.list_prefix_stream(prefix)
        }
    }
}
