//! Content references: their validation and checksum verification against
//! the stored content objects, and each object's key. The streaming read is in
//! [`super::content_stream`], and staging is in [`super::content_staging`].

#[cfg(any(test, feature = "test-support"))]
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
#[cfg(any(test, feature = "test-support"))]
use crate::storage::content_admission::PreparedContent;
#[cfg(any(test, feature = "test-support"))]
use crate::storage::content_location::ContentLocation;
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ObjectStore, ObjectStoreError};
use loonfs_types::{Checksum, ContentRef, ErrorCode};
use thiserror::Error;

/// Confirms that LoonFS durably stored the content described by this
/// reference.
#[allow(
    unreachable_pub,
    reason = "`crate::content` re-exports the type only in test and test-support builds"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContent {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) object_key: String,
    pub(crate) content_ref: ContentRef,
}

#[cfg(any(test, feature = "test-support"))]
impl StoredContent {
    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }

    pub fn into_content_ref(self) -> ContentRef {
        self.content_ref
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum DurableContentValidationError {
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
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

impl DurableContentValidationError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::MissingContentObject { .. }
            | Self::ContentLengthMismatch { .. }
            | Self::ContentChecksumMismatch { .. } => ErrorCode::NamespaceCorrupt,
            Self::Store { .. } => ErrorCode::ServerError,
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_ref: &ContentRef,
) -> std::result::Result<(), DurableContentValidationError> {
    ContentLocation::whole(content_ref)
        .read_bytes(store, None, content_ref)
        .await
        .map(drop)
}

/// Prepares content from an acknowledged durable write.
#[cfg(any(test, feature = "test-support"))]
pub fn prepare_stored_content(
    _catalog: &VerifiedNamespaceCatalogEntry,
    stored_content: StoredContent,
) -> PreparedContent {
    PreparedContent::for_completed_upload(stored_content.content_ref, u64::MAX, None)
}

/// Fully validates an existing durable content reference for publication.
///
/// Performs one ranged GET of the bytes the reference names and a checksum
/// check.
#[cfg(any(test, feature = "test-support"))]
pub async fn prepare_existing_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    _catalog: &VerifiedNamespaceCatalogEntry,
    content_ref: ContentRef,
) -> std::result::Result<PreparedContent, DurableContentValidationError> {
    validate_durable_content_reference(store, &content_ref).await?;
    Ok(PreparedContent::for_durable_content_write(content_ref))
}

pub(crate) async fn verify_durable_content_checksum<S: ObjectStore + ?Sized>(
    store: &S,
    content_ref: &ContentRef,
) -> std::result::Result<(), DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_ref);
    let stored = match store.head(&object_key).await {
        Ok(Some(stored)) => stored,
        Ok(None) => return Err(DurableContentValidationError::MissingContentObject { object_key }),
        Err(err) => {
            return Err(DurableContentValidationError::Store {
                object_key,
                message: err.public_message().into_owned(),
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
    let checksum = stored
        .checksum
        .ok_or_else(|| DurableContentValidationError::Store {
            object_key: object_key.clone(),
            message: ObjectStoreError::StoredChecksumMissing {
                object_key: object_key.clone(),
            }
            .public_message()
            .into_owned(),
        })?;
    if checksum != content_ref.checksum {
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(&content_ref.checksum),
            actual: describe_checksum(&checksum),
        });
    }
    Ok(())
}

pub(crate) fn content_object_key_for_ref(content_ref: &ContentRef) -> String {
    content_blob(&content_ref.owner_namespace_id, &content_ref.content_id)
}

/// Checks fetched bytes against everything the reference claims about them.
///
/// The reference's checksum is recomputed over the complete payload for every
/// supported algorithm.
pub(crate) fn validate_loaded_content_bytes(
    object_key: String,
    content_ref: &ContentRef,
    bytes: &[u8],
) -> std::result::Result<(), DurableContentValidationError> {
    let actual_size = bytes.len() as u64;
    if actual_size != content_ref.size_bytes {
        return Err(DurableContentValidationError::ContentLengthMismatch {
            object_key,
            expected: content_ref.size_bytes,
            actual: actual_size,
        });
    }

    let expected = &content_ref.checksum;
    if !expected.matches(bytes) {
        let actual = Checksum::compute(expected.algorithm(), bytes);
        return Err(DurableContentValidationError::ContentChecksumMismatch {
            object_key,
            expected: describe_checksum(expected),
            actual: describe_checksum(&actual),
        });
    }

    Ok(())
}

pub(super) fn describe_checksum(checksum: &Checksum) -> String {
    format!("{}:{}", checksum.algorithm(), checksum.value())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        validate_durable_content_reference, verify_durable_content_checksum, ContentLocation,
        DurableContentValidationError,
    };
    use bytes::Bytes;
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_objectstore::ObjectStoreExtension;
    use loonfs_test_support::ids::content_ref;
    use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
    use loonfs_types::{Checksum, ContentId, ContentRef, ContentRefKind};
    use std::assert_matches;
    use tempfile::tempdir;

    async fn get_durable_content_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        content_ref: &ContentRef,
    ) -> std::result::Result<Vec<u8>, DurableContentValidationError> {
        ContentLocation::whole(content_ref)
            .read_bytes(store, None, content_ref)
            .await
    }

    #[tokio::test]
    async fn validate_content_ref_reads_and_hashes_the_bytes() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"whole file bytes";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_ref, bytes).await;

        store.reset();
        validate_durable_content_reference(&store, &content_ref)
            .await
            .expect("validate content ref");
        assert_eq!(store.count(OperationSelector::Read), 1);
    }

    #[tokio::test]
    async fn get_durable_content_bytes_accepts_empty_files() {
        let (_temp_dir, store) = test_store();
        let bytes = b"";
        let content_ref = content_ref(bytes);
        put_content_object(&store, &content_ref, bytes).await;

        let bytes = get_durable_content_bytes(&store, &content_ref)
            .await
            .expect("read empty content ref");
        assert!(bytes.is_empty());
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_missing_object() {
        let (_temp_dir, store) = test_store();
        let content_ref = content_ref(b"missing");

        let err = validate_durable_content_reference(&store, &content_ref)
            .await
            .expect_err("missing object");
        assert_matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        );
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store) = test_store();
        let mut content_ref = content_ref(b"abc");
        put_content_object(&store, &content_ref, b"abc").await;
        content_ref.size_bytes += 1;

        let err = validate_durable_content_reference(&store, &content_ref)
            .await
            .expect_err("size mismatch");
        assert_matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
        );
    }

    #[tokio::test]
    async fn validate_content_ref_rejects_checksum_mismatch() {
        let (_temp_dir, store) = test_store();
        let expected = content_ref(b"expected");
        // Same id, different bytes: identity alone can no longer prove
        // content, so the checksum has to.
        let planted = ContentRef::blob_v1(
            expected.owner_namespace_id.clone(),
            expected.content_id.clone(),
            b"mismatch",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        put_content_object(&store, &planted, b"mismatch").await;

        let err = validate_durable_content_reference(&store, &expected)
            .await
            .expect_err("checksum mismatch");
        assert_matches!(
            err,
            DurableContentValidationError::ContentChecksumMismatch { .. }
        );
    }

    #[tokio::test]
    async fn read_verifies_a_reference_whose_only_evidence_is_a_crc32c() {
        let (_temp_dir, store) = test_store();
        let bytes = b"transferred straight to the provider";
        let content_ref = ContentRef {
            kind: ContentRefKind::BlobV1,
            owner_namespace_id: loonfs_types::NamespaceId::parse("demo").expect("namespace id"),
            content_id: ContentId::generate(),
            size_bytes: bytes.len() as u64,
            checksum: Checksum::crc32c(bytes),
        };
        put_content_object(&store, &content_ref, bytes).await;

        let read = get_durable_content_bytes(&store, &content_ref)
            .await
            .expect("a crc32c-only reference verifies by its crc");
        assert_eq!(read, bytes);

        // Same length, different bytes: only the checksum can tell.
        let (_temp_dir, store) = test_store();
        let planted = ContentRef {
            checksum: Checksum::crc32c(b"transferred straight to the PROVIDER"),
            ..content_ref
        };
        put_content_object(&store, &planted, bytes).await;
        assert_matches!(
            get_durable_content_bytes(&store, &planted)
                .await
                .expect_err("crc mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        );
    }

    #[tokio::test]
    async fn checksum_verification_proves_the_object_without_reading_it() {
        let (_temp_dir, inner) = test_store();
        let store = RecordingStore::new(inner, KeyPredicate::content_blob());
        let bytes = b"provider-verified bytes";
        let mut content_ref = content_ref(bytes);
        content_ref.checksum = Checksum::crc64nvme(bytes);
        put_content_object(&store, &content_ref, bytes).await;

        store.reset();
        verify_durable_content_checksum(&store, &content_ref)
            .await
            .expect("verify content ref");
        assert_eq!(
            store.count(OperationSelector::Read),
            0,
            "verification reads provider metadata, never the payload"
        );
    }

    #[tokio::test]
    async fn checksum_verification_rejects_missing_size_and_checksum_drift() {
        let (_temp_dir, store) = test_store();
        let bytes = b"abc";
        let content_ref = content_ref(bytes);

        let err = verify_durable_content_checksum(&store, &content_ref)
            .await
            .expect_err("missing object");
        assert_matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        );

        put_content_object(&store, &content_ref, bytes).await;
        let mut wrong_size = content_ref.clone();
        wrong_size.size_bytes += 1;
        assert_matches!(
            verify_durable_content_checksum(&store, &wrong_size)
                .await
                .expect_err("size mismatch"),
            DurableContentValidationError::ContentLengthMismatch { .. }
        );

        // The bytes at the key hash to something else: exactly the case the
        // completion check exists to catch on a provider that accepts a
        // wrong claim.
        let mut wrong_checksum = content_ref.clone();
        wrong_checksum.checksum = Checksum::crc64nvme(b"other bytes");
        assert_matches!(
            verify_durable_content_checksum(&store, &wrong_checksum)
                .await
                .expect_err("checksum mismatch"),
            DurableContentValidationError::ContentChecksumMismatch { .. }
        );
    }

    pub(crate) fn test_store() -> (tempfile::TempDir, LocalFsStore) {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        (temp_dir, store)
    }

    pub(crate) async fn put_content_object(
        store: &impl ObjectStore,
        content_ref: &ContentRef,
        bytes: &[u8],
    ) {
        let key = content_blob(&content_ref.owner_namespace_id, &content_ref.content_id);
        store
            .put_if_absent(&key, Bytes::copy_from_slice(bytes))
            .await
            .expect("put content");
    }
}
