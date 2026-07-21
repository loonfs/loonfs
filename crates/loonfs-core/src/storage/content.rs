//! Content blob reads and writes: content-addressed storage, reference
//! validation, and verified read-back.

use crate::error::{CoreError, StoreFailureClass};
use crate::invariants::InvariantId;
use crate::namespace::catalog::load_namespace_content_store_id;
use crate::storage::content_admission::{ContentAdmission, PreparedContent};
use bytes::Bytes;
use loonfs_api::{sha256_digest, ContentRef, ContentRefKind, ContentStoreId, NamespaceId};
use loonfs_objectstore::keys::content_blob;
use loonfs_objectstore::{ObjectStore, ObjectStoreError, PROVIDER_MULTIPART_THRESHOLD_BYTES};
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
    Store {
        object_key: String,
        message: String,
        class: StoreFailureClass,
    },
}

pub async fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
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
/// [`store_bytes_as_content`] or [`store_bytes_as_content_with_store_id`].
pub fn prepare_stored_content(
    namespace_id: NamespaceId,
    stored_content: StoredContent,
) -> PreparedContent {
    let content_ref = stored_content.content_ref;
    let admission = ContentAdmission::for_durable_content_write(namespace_id, content_ref.clone());
    PreparedContent::from_admission(content_ref, admission)
}

/// Fully validates an existing durable content reference for publication.
///
/// This performs one object HEAD followed by one full GET and digest check.
pub async fn prepare_existing_content_ref<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    content_ref: ContentRef,
) -> Result<PreparedContent, DurableContentValidationError> {
    validate_durable_content_reference(store, content_store_id, &content_ref).await?;
    let admission =
        ContentAdmission::for_durable_content_write(namespace_id.clone(), content_ref.clone());
    Ok(PreparedContent::from_admission(content_ref, admission))
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
    write_immutable_object(store, &object_key, bytes).await?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_digest_sha256: content_ref.digest.clone(),
        file_size_bytes: content_ref.size_bytes,
        content_ref,
        _write_acknowledged: StoredContentWriteAcknowledgement,
    })
}

pub(crate) async fn write_immutable_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<(), ImmutableObjectWriteError> {
    // Payloads large enough for multipart upload are written with overwrite
    // semantics: providers complete multipart uploads as unconditional
    // overwrites, so create-if-absent cannot ride them. Overwrite loses
    // nothing here because every immutable-object key is collision-free by
    // construction — content blobs are addressed by their own digest, and
    // table/index objects carry a generated id owned by one writer — so any
    // writer of the same key carries the same bytes, and reads re-verify
    // digests regardless. Small payloads keep the create precondition as a
    // cheap corruption tripwire.
    let write_result = if expected_bytes.len() as u64 >= PROVIDER_MULTIPART_THRESHOLD_BYTES {
        store
            .put_overwrite(object_key, Bytes::copy_from_slice(expected_bytes))
            .await
    } else {
        store
            .put_if_absent(object_key, Bytes::copy_from_slice(expected_bytes))
            .await
    };
    match write_result {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed { .. }) => {
            if existing_object_matches_expected_bytes(store, object_key, expected_bytes).await? {
                return Ok(());
            }
            Err(ImmutableObjectWriteError::Store {
                object_key: object_key.to_owned(),
                message: "object already exists with different bytes".to_owned(),
                class: StoreFailureClass::Other,
            })
        }
        Err(err @ ObjectStoreError::Transport { .. }) => {
            let original = err.message();
            match existing_object_matches_expected_bytes(store, object_key, expected_bytes).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(ImmutableObjectWriteError::Store {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "{original}; immutable object exists with different bytes after transport error"
                    ),
                    class: StoreFailureClass::Other,
                }),
                Err(verify_err) => Err(ImmutableObjectWriteError::Store {
                    object_key: object_key.to_owned(),
                    message: format!(
                        "{original}; failed to verify immutable write after transport error: {verify_err}"
                    ),
                    class: StoreFailureClass::Other,
                }),
            }
        }
        Err(err) => Err(ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
            class: StoreFailureClass::of(&err),
        }),
    }
}

async fn existing_object_matches_expected_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<bool, ImmutableObjectWriteError> {
    let expected_size = expected_bytes.len() as u64;
    if let Some(metadata) =
        store
            .head(object_key)
            .await
            .map_err(|err| ImmutableObjectWriteError::Store {
                object_key: object_key.to_owned(),
                message: err.message(),
                class: StoreFailureClass::of(&err),
            })?
    {
        if metadata.size_bytes != expected_size {
            return Ok(false);
        }
    }

    let existing = store
        .get(object_key, None)
        .await
        .map_err(|err| ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: err.message(),
            class: StoreFailureClass::of(&err),
        })?
        .ok_or_else(|| ImmutableObjectWriteError::Store {
            object_key: object_key.to_owned(),
            message: "object is missing while verifying immutable write".to_owned(),
            class: StoreFailureClass::Other,
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
        probe_durable_content_reference, read_durable_content_bytes,
        validate_durable_content_reference, write_immutable_object, DurableContentValidationError,
        ImmutableObjectWriteError,
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use loonfs_api::{ContentRef, ContentRefKind, ContentStoreId};
    use loonfs_objectstore::keys::content_blob;
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::PROVIDER_MULTIPART_THRESHOLD_BYTES;
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
    async fn validate_whole_file_content_ref_reads_and_hashes_the_bytes() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = GetCountingStore::new(inner);
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
    async fn probe_whole_file_content_ref_proves_durability_without_reading() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = GetCountingStore::new(inner);
        let bytes = b"provider-verified bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes).await;

        store.reset_content_blob_get_count();
        probe_durable_content_reference(&store, &content_store_id, &content_ref)
            .await
            .expect("probe content ref");
        assert_eq!(
            store.content_blob_get_count(),
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

    #[tokio::test]
    async fn immutable_write_recovers_when_transport_error_hides_committed_object() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = TransportOnCreateStore::new(inner, TransportBehavior::WriteThenError);
        let bytes = b"metadata table bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        let object_key =
            content_blob(content_store_id.as_str(), &content_ref.digest).expect("content key");

        write_immutable_object(&store, &object_key, bytes)
            .await
            .expect("committed object is accepted after transport error");

        let stored = store
            .inner
            .get(&object_key, None)
            .await
            .expect("read object")
            .expect("object exists");
        assert_eq!(stored, Bytes::copy_from_slice(bytes));
    }

    #[tokio::test]
    async fn immutable_write_rejects_transport_error_without_committed_object() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = TransportOnCreateStore::new(inner, TransportBehavior::ErrorWithoutWrite);
        let bytes = b"metadata table bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        let object_key =
            content_blob(content_store_id.as_str(), &content_ref.digest).expect("content key");

        let err = write_immutable_object(&store, &object_key, bytes)
            .await
            .expect_err("missing object after transport error is rejected");

        assert_error_contains(&err, "simulated timeout");
        assert_error_contains(
            &err,
            "failed to verify immutable write after transport error",
        );
        assert_error_contains(&err, "object is missing while verifying immutable write");
    }

    #[tokio::test]
    async fn immutable_write_rejects_different_existing_bytes_after_transport_error() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let expected = b"expected immutable bytes";
        let different = b"different immutable bytes";
        let content_ref = ContentRef::whole_file_v0(expected);
        let object_key =
            content_blob(content_store_id.as_str(), &content_ref.digest).expect("content key");
        inner
            .put_if_absent(&object_key, Bytes::copy_from_slice(different))
            .await
            .expect("preload different object");
        let store = TransportOnCreateStore::new(inner, TransportBehavior::ErrorWithoutWrite);

        let err = write_immutable_object(&store, &object_key, expected)
            .await
            .expect_err("different existing bytes are rejected");

        assert_error_contains(&err, "simulated timeout");
        assert_error_contains(
            &err,
            "immutable object exists with different bytes after transport error",
        );
    }

    #[tokio::test]
    async fn immutable_write_mode_routes_by_multipart_threshold() {
        let (_temp_dir, inner, content_store_id) = test_store();
        let store = ModeRecordingStore::new(inner);

        let small = b"small immutable bytes".to_vec();
        let small_ref = ContentRef::whole_file_v0(&small);
        let small_key =
            content_blob(content_store_id.as_str(), &small_ref.digest).expect("content key");
        write_immutable_object(&store, &small_key, &small)
            .await
            .expect("small write");

        let large = vec![0u8; usize::try_from(PROVIDER_MULTIPART_THRESHOLD_BYTES).expect("usize")];
        let large_ref = ContentRef::whole_file_v0(&large);
        let large_key =
            content_blob(content_store_id.as_str(), &large_ref.digest).expect("content key");
        write_immutable_object(&store, &large_key, &large)
            .await
            .expect("large write");

        let modes = store.put_modes.lock().expect("modes").clone();
        assert_eq!(
            modes,
            vec![PutMode::CreateIfAbsent, PutMode::Overwrite],
            "small blobs keep the create precondition; multipart-sized blobs \
             use overwrite because multipart completion cannot carry one"
        );
    }

    fn assert_error_contains(error: &ImmutableObjectWriteError, expected: &str) {
        let message = error.to_string();
        assert!(
            message.contains(expected),
            "expected error to contain `{expected}`, got `{message}`"
        );
    }

    #[derive(Debug)]
    struct ModeRecordingStore {
        inner: LocalFsStore,
        put_modes: std::sync::Mutex<Vec<PutMode>>,
    }

    impl ModeRecordingStore {
        fn new(inner: LocalFsStore) -> Self {
            Self {
                inner,
                put_modes: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for ModeRecordingStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            self.put_modes.lock().expect("modes").push(mode.clone());
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

    #[derive(Debug, Clone, Copy)]
    enum TransportBehavior {
        WriteThenError,
        ErrorWithoutWrite,
    }

    #[derive(Debug)]
    struct TransportOnCreateStore {
        inner: LocalFsStore,
        behavior: TransportBehavior,
    }

    impl TransportOnCreateStore {
        fn new(inner: LocalFsStore, behavior: TransportBehavior) -> Self {
            Self { inner, behavior }
        }
    }

    #[async_trait]
    impl ObjectStore for TransportOnCreateStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if mode == PutMode::CreateIfAbsent {
                if matches!(self.behavior, TransportBehavior::WriteThenError) {
                    self.inner.put(key, bytes, mode).await?;
                }
                return Err(ObjectStoreError::transport(key, "simulated timeout"));
            }
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

    #[derive(Debug)]
    struct GetCountingStore {
        inner: LocalFsStore,
        content_blob_gets: AtomicUsize,
    }

    impl GetCountingStore {
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
    impl ObjectStore for GetCountingStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
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
            self.inner.get_with_metadata(key).await
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
