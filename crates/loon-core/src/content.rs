use crate::error::CoreError;
use crate::namespace::catalog::load_namespace_content_store_id;
use loon_api::{sha256_digest, ContentRef, ContentRefKind, ContentStoreId, NamespaceId};
use loon_objectstore::keys::content_blob;
use loon_objectstore::{ObjectStore, ObjectStoreError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedDurableContent {
    pub content_ref: ContentRef,
    pub object_key: String,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub checked_invariants: Vec<String>,
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
    pub(crate) fn ensure_validated<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
    ) -> Result<(), DurableContentValidationError> {
        let key = (content_store_id.clone(), content_ref.clone());
        if self.validated.contains(&key) {
            return Ok(());
        }
        validate_durable_content_reference(store, content_store_id, content_ref)?;
        self.validated.insert(key);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum DurableContentValidationError {
    #[error("unsupported content ref kind `{kind:?}`")]
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
    #[error("{0}")]
    Store(String),
}

pub fn validate_durable_content_reference<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let bytes = load_required_object(store, &object_key)?;
    validate_loaded_content_bytes(object_key, content_ref, &bytes)
}

pub fn read_durable_content_bytes<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<ReadDurableContent, DurableContentValidationError> {
    let object_key = content_object_key_for_ref(content_store_id, content_ref)?;
    let bytes = load_required_object(store, &object_key)?;
    let validated = validate_loaded_content_bytes(object_key, content_ref, &bytes)?;

    Ok(ReadDurableContent { validated, bytes })
}

fn content_object_key_for_ref(
    content_store_id: &ContentStoreId,
    content_ref: &ContentRef,
) -> Result<String, DurableContentValidationError> {
    if content_ref.kind != ContentRefKind::WholeFileV0 {
        return Err(DurableContentValidationError::UnsupportedContentRefKind {
            kind: content_ref.kind,
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
            "whole_file_content_ref_kind_is_supported".to_owned(),
            "whole_file_content_object_key_matches_digest".to_owned(),
            "whole_file_content_size_matches_ref".to_owned(),
            "whole_file_content_digest_matches_ref".to_owned(),
        ],
    })
}

pub fn store_bytes_as_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    bytes: &[u8],
) -> Result<StoredContent, CoreError> {
    let content_store_id = load_namespace_content_store_id(store, namespace_id)?;
    let content_ref = ContentRef::whole_file_v0(bytes);
    let object_key = content_blob(content_store_id.as_str(), &content_ref.digest)
        .map_err(|err| CoreError::Store(err.to_string()))?;
    write_immutable_object(store, &object_key, bytes)?;

    Ok(StoredContent {
        content_store_id,
        object_key,
        file_digest_sha256: content_ref.digest.clone(),
        file_size_bytes: content_ref.size_bytes,
        content_ref,
    })
}

pub(crate) fn write_immutable_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
    expected_bytes: &[u8],
) -> Result<(), ImmutableObjectWriteError> {
    match store.put_if_absent(object_key, expected_bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed) => {
            let existing = store
                .get(object_key, None)
                .map_err(|err| ImmutableObjectWriteError::Store(err.to_string()))?
                .ok_or_else(|| {
                    ImmutableObjectWriteError::Store(format!(
                        "missing immutable object `{object_key}` after precondition failure"
                    ))
                })?;
            if existing == expected_bytes {
                Ok(())
            } else {
                Err(ImmutableObjectWriteError::Store(format!(
                    "immutable object `{object_key}` already exists with different bytes"
                )))
            }
        }
        Err(err) => Err(ImmutableObjectWriteError::Store(err.to_string())),
    }
}

fn load_required_object<S: ObjectStore + ?Sized>(
    store: &S,
    object_key: &str,
) -> Result<Vec<u8>, DurableContentValidationError> {
    match store.get(object_key, None) {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) => Err(DurableContentValidationError::MissingContentObject {
            object_key: object_key.to_owned(),
        }),
        Err(err) => Err(DurableContentValidationError::Store {
            object_key: object_key.to_owned(),
            message: err.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_durable_content_bytes, validate_durable_content_reference,
        DurableContentValidationError,
    };
    use loon_api::{ContentRef, ContentRefKind, ContentStoreId};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::content_blob;
    use loon_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[test]
    fn validate_whole_file_content_ref_success() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"whole file bytes";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes);

        let validated = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .expect("validate content ref");
        assert_eq!(validated.content_ref, content_ref);
        assert_eq!(validated.file_size_bytes, bytes.len() as u64);
        assert_eq!(validated.file_digest_sha256, content_ref.digest);
    }

    #[test]
    fn validate_whole_file_content_ref_accepts_empty_files() {
        let (_temp_dir, store, content_store_id) = test_store();
        let bytes = b"";
        let content_ref = ContentRef::whole_file_v0(bytes);
        put_content_object(&store, &content_store_id, &content_ref, bytes);

        let read = read_durable_content_bytes(&store, &content_store_id, &content_ref)
            .expect("read empty content ref");
        assert_eq!(read.bytes, bytes);
        assert_eq!(read.validated.file_size_bytes, 0);
    }

    #[test]
    fn validate_whole_file_content_ref_rejects_missing_object() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef::whole_file_v0(b"missing");

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .expect_err("missing object");
        assert!(matches!(
            err,
            DurableContentValidationError::MissingContentObject { .. }
        ));
    }

    #[test]
    fn validate_whole_file_content_ref_rejects_size_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let mut content_ref = ContentRef::whole_file_v0(b"abc");
        put_content_object(&store, &content_store_id, &content_ref, b"abc");
        content_ref.size_bytes += 1;

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .expect_err("size mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentLengthMismatch { .. }
        ));
    }

    #[test]
    fn validate_whole_file_content_ref_rejects_digest_mismatch() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef::whole_file_v0(b"expected");
        put_content_object(&store, &content_store_id, &content_ref, b"mismatch");

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
            .expect_err("digest mismatch");
        assert!(matches!(
            err,
            DurableContentValidationError::ContentDigestMismatch { .. }
        ));
    }

    #[test]
    fn validate_whole_file_content_ref_rejects_unsupported_kind() {
        let (_temp_dir, store, content_store_id) = test_store();
        let content_ref = ContentRef {
            kind: ContentRefKind::Unsupported,
            digest: ContentRef::whole_file_v0(b"bytes").digest,
            size_bytes: 5,
        };

        let err = validate_durable_content_reference(&store, &content_store_id, &content_ref)
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

    fn put_content_object(
        store: &LocalFsStore,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
        bytes: &[u8],
    ) {
        let key =
            content_blob(content_store_id.as_str(), &content_ref.digest).expect("content key");
        store.put_if_absent(&key, bytes).expect("put content");
    }
}
