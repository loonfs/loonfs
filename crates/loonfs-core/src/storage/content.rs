//! Content blob reads and writes: content-addressed storage, reference
//! validation, and verified read-back.

use crate::error::CoreError;
use crate::namespace::catalog::{load_namespace_content_store_id, VerifiedNamespaceCatalogEntry};
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use loonfs_api::{sha256_digest, ContentRef, ContentRefKind, ContentStoreId, NamespaceId};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedDurableContent {
    pub content_ref: ContentRef,
    pub object_key: String,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ReadDurableContent {
    pub validated: ValidatedDurableContent,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoredContent {
    pub content_store_id: ContentStoreId,
    pub object_key: String,
    pub content_ref: ContentRef,
    pub file_digest_sha256: String,
    pub file_size_bytes: u64,
    #[serde(skip)]
    _write_acknowledged: StoredContentWriteAcknowledgement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredContentWriteAcknowledgement;

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
    #[error(
        "stored content belongs to content store `{actual}`, not namespace-bound store `{expected}`"
    )]
    ContentStoreMismatch {
        expected: ContentStoreId,
        actual: ContentStoreId,
    },
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

pub(crate) async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    validate_content_size(store, &object_key, content_ref).await?;

    let bytes = load_required_object(store, &object_key).await?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)
}

/// Prepares content from an acknowledged LoonFS-managed durable write.
///
/// Consuming [`StoredContent`] ties the proof to the successful return from
/// [`store_bytes_as_content`] or [`store_bytes_as_content_with_store_id`]. The
/// verified catalog prevents pairing that acknowledgement with an unrelated
/// namespace binding.
pub fn prepare_stored_content(
    catalog: &VerifiedNamespaceCatalogEntry,
    stored_content: StoredContent,
) -> Result<PreparedContent, DurableContentValidationError> {
    if stored_content.content_store_id != *catalog.content_store_id() {
        return Err(DurableContentValidationError::ContentStoreMismatch {
            expected: catalog.content_store_id().clone(),
            actual: stored_content.content_store_id,
        });
    }
    let content_store_id = stored_content.content_store_id;
    let content_ref = stored_content.content_ref;
    let admission = ContentAdmission::for_durable_content_write(content_store_id, content_ref);
    Ok(PreparedContent::from_admission(admission))
}

/// Fully validates an existing durable content reference for publication.
///
/// The verified catalog selects the store to validate. This performs one
/// object HEAD followed by one full GET and digest check.
pub async fn prepare_existing_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    catalog: &VerifiedNamespaceCatalogEntry,
    content_ref: ContentRef,
) -> Result<PreparedContent, DurableContentValidationError> {
    let content_store_id = catalog.content_store_id();
    validate_durable_content_reference(store, content_store_id, &content_ref).await?;
    let admission =
        ContentAdmission::for_durable_content_write(content_store_id.clone(), content_ref);
    Ok(PreparedContent::from_admission(admission))
}

/// Proves a content reference is durable — the object exists and carries the
/// declared size — from one HEAD, without reading the content.
///
/// This is the completion check for writes whose digest integrity the
/// provider already enforced at upload time (a `direct_put` transfer
/// capability signs the digest into the write, and the provider refuses a
/// body that does not hash to it — see
/// [`ObjectTransferIssuer`](loonfs_objectstore::presign::ObjectTransferIssuer)).
/// Re-hashing here would re-download the payload through the server and
/// prove nothing the write path has not already proven; the size check
/// stays because the declared `size_bytes` rides the reference, not the
/// digest, so a mis-declared size must fail completion. Callers without a
/// write-time digest guarantee use [`validate_durable_content_reference`],
/// which reads and hashes.
pub(crate) async fn probe_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    validate_content_size(store, &object_key, content_ref).await
}

pub(crate) async fn read_durable_content_bytes<S: ObjectStore + ?Sized>(
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
    })
}

/// Existence and size from one HEAD, serving two roles: the authoritative
/// read-and-hash uses it as cheap prevalidation, so a wrong-sized object
/// fails fast without downloading it, and the durability probe uses it as
/// the whole check, because the probe's callers hold a write-time digest
/// guarantee. When this crate itself verifies a digest it always reads the
/// bytes — provider checksums are not part of the read contract anywhere
/// in the fleet.
async fn validate_content_size<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let metadata = match store.head(object_key).await {
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
    Ok(())
}

#[tracing::instrument(
    level = "info",
    name = "loonfs.phase",
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
    store_bytes_as_content_with_store_id(store, content_store_id, bytes).await
}

/// Stages bytes when the caller already knows the namespace's content-store
/// binding (it is immutable, so a handle can resolve it once).
pub async fn store_bytes_as_content_with_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Internal(format!("failed to derive content blob key: {err}")))?;
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_digest_sha256: content_ref.digest.clone(),
        file_size_bytes: content_ref.size_bytes,
        content_ref,
        _write_acknowledged: StoredContentWriteAcknowledgement,
    })
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
        probe_durable_content_reference, read_durable_content_bytes,
        validate_durable_content_reference, DurableContentValidationError,
    };
    use bytes::Bytes;
    use loonfs_api::{ContentRef, ContentRefKind, ContentStoreId};
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
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
    async fn validate_whole_file_content_ref_reads_and_hashes_the_bytes() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"whole file bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(store.count(OperationClass::Read), 1);
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
    async fn probe_whole_file_content_ref_proves_durability_without_reading() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"provider-verified bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        probe_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("probe content ref");
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "the probe proves durability from metadata alone"
        );
    }

    #[tokio::test]
    async fn probe_whole_file_content_ref_rejects_missing_object() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef::whole_file_v0(b"missing");

        let err = probe_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[tokio::test]
    async fn probe_whole_file_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let mut content_ref = ContentRef::whole_file_v0(b"abc");
        put_content_object(&store, &content_store_id, &content_ref, b"abc").await;
        content_ref.size_bytes += 1;

        let err = probe_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("size mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
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
}
