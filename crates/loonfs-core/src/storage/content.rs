//! Content object reads and writes: minting immutable identities,
//! validating references, and verified read-back.

use crate::error::CoreError;
use crate::namespace::catalog::{load_namespace_content_store_id, VerifiedNamespaceCatalogEntry};
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use loonfs_api::{
    ChecksumAlgorithm, ContentId, ContentRef, ContentRefValidationError, ContentStoreId,
    NamespaceId, StorageChecksum,
};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::ObjectStore;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ValidatedDurableContent {
    pub content_ref: ContentRef,
    pub object_key: String,
    pub file_size_bytes: u64,
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
    pub file_size_bytes: u64,
    #[serde(skip)]
    _write_acknowledged: StoredContentWriteAcknowledgement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredContentWriteAcknowledgement;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum DurableContentValidationError {
    #[error("invalid content reference: {0}")]
    InvalidContentRef(ContentRefValidationError),
    #[error("missing content object `{object_key}`")]
    MissingContentObject { object_key: String },
    #[error("content length mismatch for `{object_key}`: expected {expected}, actual {actual}")]
    ContentLengthMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "content checksum mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ContentChecksumMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error(
        "content checksum for `{object_key}` uses `{algorithm}`, which this build cannot recompute"
    )]
    ContentChecksumUnverifiable {
        object_key: String,
        algorithm: ChecksumAlgorithm,
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
/// object HEAD followed by one full GET and checksum check.
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

/// Verifies the object a reference names against that reference, from the
/// provider's own stored checksum and size.
///
/// This is the completion check for bytes that never passed through the
/// LoonFS server. It verifies rather than trusts: the presigned write is
/// checksum-bound, but a provider that accepts a wrong claim at assembly
/// time (Cloudflare R2 does, at multipart completion) would otherwise leave
/// a corrupt object publishable. One `HeadObject` with checksum mode enabled
/// answers both questions and moves no payload.
///
/// A caller that gets a mismatch owns the repair: the object sits at a
/// random id nothing references yet, so deleting it costs nothing and
/// leaving it would leak.
pub(crate) async fn verify_durable_content_checksum<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<(), DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let stored = match store.head_stored_checksum(&object_key).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Err(DurableContentValidationError::MissingContentObject { object_key }),
        Err(err) => {
            return Err(DurableContentValidationError::Store {
                object_key,
                message: err.message(),
            })
        }
    };

    if stored.size_bytes != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: stored.size_bytes,
        });
    }
    if stored.storage_checksum != content_ref.storage_checksum {
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&content_ref.storage_checksum),
            actual: describe_checksum(&stored.storage_checksum),
        });
    }
    Ok(())
}

/// Removes the content object an upload session owned but never published.
///
/// The id is random and an upload session is the only thing that can name
/// one before publication, so exactly one session is ever talking about this
/// object and no metadata can reference it. Deleting is therefore safe and
/// keeping it would leak bytes nobody can name again. This runs strictly
/// after the durable transition that made the session terminal, and it is
/// idempotent, so a cleanup lost to a crash is simply repeated by the next
/// garbage-collection pass — which is why a failure here is logged rather
/// than propagated.
pub(crate) async fn delete_unpublished_content_object<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_id: &ContentId,
) {
    let object_key = content_blob(content_store_id.as_str(), content_id);
    if let Err(error) = store.delete(&object_key).await {
        tracing::warn!(
            content_id = %content_id,
            error = %error,
            "failed to remove the content object of a terminated upload session"
        );
    }
}

/// Abandons the provider multipart upload a terminated session opened.
///
/// Aborting is safe whatever the upload's real state: an upload that already
/// assembled its object survives the abort untouched, and one the provider
/// has never heard of succeeds anyway. So this runs strictly after the
/// durable transition without first proving what it is cleaning up, and a
/// failure is logged rather than propagated — the next garbage-collection
/// pass repeats it from the terminal record.
pub(crate) async fn abort_unpublished_multipart_upload<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_id: &ContentId,
    provider_upload_id: &str,
) {
    let object_key = content_blob(content_store_id.as_str(), content_id);
    if let Err(error) = store
        .abort_multipart_upload(&object_key, provider_upload_id)
        .await
    {
        tracing::warn!(
            content_id = %content_id,
            error = %error,
            "failed to abandon the multipart upload of a terminated upload session"
        );
    }
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
    content_ref
        .validate()
        .map_err(DurableContentValidationError::InvalidContentRef)?;
    Ok(content_blob(
        content_store_id.as_str(),
        &content_ref.content_id,
    ))
}

/// Checks fetched bytes against everything the reference claims about them.
///
/// The whole-file SHA-256 is the check whenever it is present; a reference
/// that carries only a CRC — which direct multipart produces, because a
/// provider-assembled object is never hashed by us — is verified by that
/// CRC instead. A reference whose checksum this build cannot recompute is
/// refused rather than waved through: an unverifiable read is not a
/// verified one.
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

    let expected = verifiable_checksum(content_ref);
    match expected.matches(bytes) {
        Some(true) => {}
        Some(false) => {
            let actual = match expected.algorithm {
                ChecksumAlgorithm::Sha256 => StorageChecksum::sha256(bytes),
                _ => StorageChecksum::crc64nvme(bytes),
            };
            return Err(DurableContentValidationError::ContentChecksumMismatch {
                object_key,
                expected: describe_checksum(&expected),
                actual: describe_checksum(&actual),
            });
        }
        None => {
            return Err(DurableContentValidationError::ContentChecksumUnverifiable {
                object_key,
                algorithm: content_ref.storage_checksum.algorithm,
            })
        }
    }

    Ok(ValidatedDurableContent {
        content_ref: content_ref.clone(),
        object_key,
        file_size_bytes: actual_size,
    })
}

/// The checksum a read holds these bytes to: the trusted whole-file digest
/// when one exists, and otherwise the reference's own storage checksum,
/// which for a provider-assembled object is the only evidence there is.
fn verifiable_checksum(content_ref: &ContentRef) -> StorageChecksum {
    match &content_ref.whole_file_sha256 {
        Some(digest) => StorageChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            value: digest.clone(),
        },
        None => content_ref.storage_checksum.clone(),
    }
}

fn describe_checksum(checksum: &StorageChecksum) -> String {
    format!("{}:{}", checksum.algorithm, checksum.value)
}

/// Existence and size from one HEAD, used as cheap prevalidation before the
/// authoritative read-and-hash: a wrong-sized object fails without being
/// downloaded.
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
///
/// Every call mints its own content identity, so two writers staging the
/// same bytes produce two objects rather than racing for one key. Sharing a
/// key was free deduplication and also a free existence oracle: anyone
/// allowed to upload could learn whether specific known bytes were already
/// in a shared content store. Retry idempotency, the thing that dedup was
/// quietly providing, belongs to the upload session instead.
pub async fn store_bytes_as_content_with_store_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    stage_bytes_under_content_id(store, content_store_id, ContentId::generate(), bytes).await
}

/// Stages bytes under an identity the caller already allocated, for writers
/// that minted the id earlier (an upload session allocates at `begin`).
pub(crate) async fn stage_bytes_under_content_id<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: ContentStoreId,
    content_id: ContentId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_ref = ContentRef::blob_v1(content_id, bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.content_id);
    // Create-only plus the byte check stay on this write even though a
    // random id cannot collide: if this key is ever occupied by different
    // bytes, that is corruption, and it must fail loudly rather than be
    // overwritten.
    store
        .put_immutable_verified(&object_key, Bytes::copy_from_slice(bytes))
        .await?;

    Ok(StoredContent {
        content_store_id,
        object_key,
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
        read_durable_content_bytes, store_bytes_as_content_with_store_id,
        validate_durable_content_reference, verify_durable_content_checksum,
        DurableContentValidationError,
    };
    use bytes::Bytes;
    use loonfs_api::{
        ChecksumAlgorithm, ContentId, ContentRef, ContentRefKind, ContentStoreId, StorageChecksum,
    };
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_test_support::stores::{CountingStore, KeyPredicate, OperationClass};
    use tempfile::tempdir;

    fn content_ref(bytes: &[u8]) -> ContentRef {
        ContentRef::blob_v1(ContentId::generate(), bytes)
    }

    #[tokio::test]
    async fn validate_content_ref_success() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let validated = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(validated.content_ref, content_ref);
        assert_eq!(validated.file_size_bytes, bytes.len() as u64);
    }

    #[tokio::test]
    async fn validate_content_ref_reads_and_hashes_the_bytes() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(store.count(OperationClass::Read), 1);
    }

    #[tokio::test]
    async fn validate_content_ref_accepts_empty_files() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("read empty content ref");
        assert_eq!(read.bytes, bytes);
        assert_eq!(read.validated.file_size_bytes, 0);
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_missing_object() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = content_ref(b"missing");

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let mut content_ref = content_ref(b"abc");
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
    async fn validate_content_ref_rejects_checksum_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let expected = content_ref(b"expected");
        // Same id, different bytes: identity alone can no longer prove
        // content, so the checksum has to.
        let planted = ContentRef::blob_v1(expected.content_id.clone(), b"mismatch");
        put_content_object(&store, &content_store_id, &planted, b"mismatch").await;

        let err = validate_durable_content_reference(&store, &content_store_id, &expected)
            .await
            .expect_err("checksum mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    /// A reference whose only evidence is a CRC this build cannot recompute
    /// must fail the read rather than be waved through unverified.
    #[tokio::test]
    async fn read_refuses_a_reference_it_cannot_verify() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"crc only";
        let mut content_ref = content_ref(bytes);
        content_ref.whole_file_sha256 = None;
        content_ref.storage_checksum = StorageChecksum {
            algorithm: ChecksumAlgorithm::Crc32c,
            value: "00000000".to_owned(),
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let err = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect_err("unverifiable checksum");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentChecksumUnverifiable { .. }
        ));
    }

    /// A direct multipart upload produces a reference whose only evidence is
    /// the CRC-64/NVME the provider computed over the assembly. Reads must
    /// verify it — the alternative is a whole write path whose bytes are
    /// never checked on the way back out.
    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc64nvme() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"provider-assembled bytes";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            storage_checksum: StorageChecksum::crc64nvme(bytes),
            whole_file_sha256: None,
        };
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .await
            .expect("a crc-only reference verifies by its crc");
        assert_eq!(read.bytes, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store, content_store_id) = test_store();
        let planted = ContentRef {
            storage_checksum: StorageChecksum::crc64nvme(b"provider-assembled BYTES"),
            ..content_ref.clone()
        };
        put_content_object(&store, &content_store_id, &planted, bytes).await;
        assert!(matches!(
            read_durable_content_bytes(&store, &content_store_id, &planted)
                .await
                .expect_err("crc mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn checksum_verification_proves_the_object_without_reading_it() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = CountingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"provider-verified bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset();
        verify_durable_content_checksum(&store, &content_store_id, &content_ref)
            .await
            .expect("verify content ref");
        assert_eq!(
            store.count(OperationClass::Read),
            0,
            "verification reads provider metadata, never the payload"
        );
    }

    #[tokio::test]
    async fn checksum_verification_rejects_missing_size_and_checksum_drift() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"abc";
        let content_ref = content_ref(bytes);

        let err = verify_durable_content_checksum(&store, &content_store_id, &content_ref)
            .await
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));

        put_content_object(&store, &content_store_id, &content_ref, bytes).await;
        let mut wrong_size = content_ref.clone();
        wrong_size.size_bytes += 1;
        assert!(matches!(
            verify_durable_content_checksum(&store, &content_store_id, &wrong_size)
                .await
                .expect_err("size mismatch"),
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));

        // The bytes at the key hash to something else: exactly the case the
        // completion check exists to catch on a provider that accepts a
        // wrong claim.
        let mut wrong_checksum = content_ref.clone();
        wrong_checksum.storage_checksum = StorageChecksum::sha256(b"other bytes");
        wrong_checksum.whole_file_sha256 = Some(wrong_checksum.storage_checksum.value.clone());
        assert!(matches!(
            verify_durable_content_checksum(&store, &content_store_id, &wrong_checksum)
                .await
                .expect_err("checksum mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        ));
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_unsupported_kind() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef {
            kind: ContentRefKind::Unsupported("kind_from_the_future".to_owned()),
            ..content_ref(b"bytes")
        };

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect_err("unsupported content ref kind");
        assert!(matches!(
            err,
            DurableContentValidationError::InvalidContentRef(_)
        ));
    }

    /// Two writers staging the same bytes get two objects. There is no
    /// shared key to coalesce on, so neither can observe the other.
    #[tokio::test]
    async fn staging_identical_bytes_twice_mints_two_distinct_objects() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"identical payload";

        let first = store_bytes_as_content_with_store_id(&store, content_store_id.clone(), bytes)
            .await
            .expect("first stage");
        let second = store_bytes_as_content_with_store_id(&store, content_store_id, bytes)
            .await
            .expect("second stage");

        assert_ne!(
            first.content_ref.content_id, second.content_ref.content_id,
            "each staging write owns its own content object"
        );
        assert_ne!(first.object_key, second.object_key);
        assert_eq!(
            first.content_ref.storage_checksum, second.content_ref.storage_checksum,
            "identical bytes still carry identical evidence"
        );
        for stored in [&first, &second] {
            assert_eq!(
                store
                    .get(&stored.object_key, None)
                    .await
                    .expect("read staged object")
                    .expect("staged object exists"),
                Bytes::from_static(b"identical payload")
            );
        }
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
        let key = content_blob(content_store_id.as_str(), &content_ref.content_id);
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("put content");
    }
}
