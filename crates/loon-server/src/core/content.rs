use crate::objectstore::keys::{blob, content_manifest};
use crate::objectstore::ObjectStore;
use loon_types::{
    decode_content_manifest_json, sha256_digest, ContentManifestEnvelope, NamespaceId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedDurableContent {
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub checked_invariants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadDurableContent {
    pub validated: ValidatedDurableContent,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum DurableContentValidationError {
    #[error("missing content manifest object `{object_key}`")]
    MissingManifestObject { object_key: String },
    #[error("content manifest codec error for `{object_key}`: {message}")]
    ManifestCodec { object_key: String, message: String },
    #[error(
        "content manifest digest mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestDigestMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error(
        "content manifest namespace mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    ManifestNamespaceMismatch {
        object_key: String,
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error("missing content block object `{object_key}`")]
    MissingBlockObject { object_key: String },
    #[error(
        "content block length mismatch for `{object_key}`: expected {expected}, actual {actual}"
    )]
    BlockLengthMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "content block digest mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    BlockDigestMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error(
        "content manifest file size mismatch for `{object_key}`: expected {expected}, actual {actual}"
    )]
    FileSizeMismatch {
        object_key: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "content manifest file digest mismatch for `{object_key}`: expected `{expected}`, actual `{actual}`"
    )]
    FileDigestMismatch {
        object_key: String,
        expected: String,
        actual: String,
    },
    #[error("object store error for `{object_key}`: {message}")]
    Store { object_key: String, message: String },
}

pub fn validate_durable_content_reference<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> Result<ValidatedDurableContent, DurableContentValidationError> {
    let manifest_object_key = content_manifest(namespace_id.as_str(), content_manifest_digest);
    let manifest_bytes = load_required_object(store, &manifest_object_key, true)?;
    let manifest_envelope = decode_content_manifest_json(&manifest_bytes).map_err(|err| {
        DurableContentValidationError::ManifestCodec {
            object_key: manifest_object_key.clone(),
            message: err.to_string(),
        }
    })?;

    let mut checked_invariants = vec!["content_manifest_checksum_matches_payload".to_owned()];
    let actual_manifest_digest = sha256_digest(&manifest_bytes);
    if actual_manifest_digest != content_manifest_digest {
        return Err(DurableContentValidationError::ManifestDigestMismatch {
            object_key: manifest_object_key,
            expected: content_manifest_digest.to_owned(),
            actual: actual_manifest_digest,
        });
    }
    checked_invariants.push("content_manifest_digest_matches_object".to_owned());

    if manifest_envelope.payload.namespace_id != *namespace_id {
        return Err(DurableContentValidationError::ManifestNamespaceMismatch {
            object_key: manifest_object_key,
            expected: namespace_id.clone(),
            actual: manifest_envelope.payload.namespace_id.clone(),
        });
    }
    checked_invariants.push("content_manifest_namespace_matches_request".to_owned());

    let mut file_hasher = Sha256::new();
    let mut actual_file_size = 0u64;
    for block_descriptor in &manifest_envelope.payload.blocks {
        let block_object_key = blob(
            namespace_id.as_str(),
            &block_descriptor.content_digest_sha256,
        );
        let block_bytes = load_required_object(store, &block_object_key, false)?;
        let actual_block_size = block_bytes.len() as u64;
        if actual_block_size != block_descriptor.plaintext_size_bytes {
            return Err(DurableContentValidationError::BlockLengthMismatch {
                object_key: block_object_key,
                expected: block_descriptor.plaintext_size_bytes,
                actual: actual_block_size,
            });
        }

        let actual_block_digest = sha256_digest(&block_bytes);
        if actual_block_digest != block_descriptor.content_digest_sha256 {
            return Err(DurableContentValidationError::BlockDigestMismatch {
                object_key: block_object_key,
                expected: block_descriptor.content_digest_sha256.clone(),
                actual: actual_block_digest,
            });
        }

        actual_file_size = actual_file_size.saturating_add(actual_block_size);
        file_hasher.update(&block_bytes);
    }
    checked_invariants.push("content_manifest_blocks_match_descriptors".to_owned());

    if actual_file_size != manifest_envelope.payload.file_size_bytes {
        return Err(DurableContentValidationError::FileSizeMismatch {
            object_key: manifest_object_key,
            expected: manifest_envelope.payload.file_size_bytes,
            actual: actual_file_size,
        });
    }

    let actual_file_digest = format!("sha256:{:x}", file_hasher.finalize());
    if actual_file_digest != manifest_envelope.payload.file_digest_sha256 {
        return Err(DurableContentValidationError::FileDigestMismatch {
            object_key: manifest_object_key,
            expected: manifest_envelope.payload.file_digest_sha256.clone(),
            actual: actual_file_digest,
        });
    }
    checked_invariants.push("content_manifest_file_digest_matches_blocks".to_owned());

    Ok(ValidatedDurableContent {
        content_manifest_digest: content_manifest_digest.to_owned(),
        manifest_object_key,
        manifest_envelope,
        file_size_bytes: actual_file_size,
        file_digest_sha256: actual_file_digest,
        checked_invariants,
    })
}

pub fn read_durable_content_bytes<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> Result<ReadDurableContent, DurableContentValidationError> {
    let validated =
        validate_durable_content_reference(store, namespace_id, content_manifest_digest)?;
    let mut bytes = Vec::with_capacity(usize::try_from(validated.file_size_bytes).unwrap_or(0));
    for block_descriptor in &validated.manifest_envelope.payload.blocks {
        let block_object_key = blob(
            namespace_id.as_str(),
            &block_descriptor.content_digest_sha256,
        );
        let block_bytes = load_required_object(store, &block_object_key, false)?;
        bytes.extend_from_slice(&block_bytes);
    }

    Ok(ReadDurableContent { validated, bytes })
}

fn load_required_object<S: ObjectStore>(
    store: &S,
    object_key: &str,
    manifest: bool,
) -> Result<Vec<u8>, DurableContentValidationError> {
    match store.get(object_key, None) {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) if manifest => Err(DurableContentValidationError::MissingManifestObject {
            object_key: object_key.to_owned(),
        }),
        Ok(None) => Err(DurableContentValidationError::MissingBlockObject {
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
    use crate::objectstore::fs::LocalFsStore;
    use crate::objectstore::keys::{blob, content_manifest};
    use crate::objectstore::ObjectStore;
    use loon_types::{
        content_manifest_digest_sha256, encode_content_manifest_json, sha256_digest,
        ContentBlockDescriptor, ContentManifestEnvelope, ContentManifestPayload, NamespaceId,
        CONTENT_BLOCK_SIZE_BYTES,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn validate_durable_content_reference_accepts_matching_manifest_and_blocks() {
        let temp_dir = TestDir::new("core-content-valid");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let namespace_id = NamespaceId::from("ns-1");
        let content_bytes = b"hello from loon\n";
        let block_digest = sha256_digest(content_bytes);
        let manifest_envelope = sample_manifest(&namespace_id, content_bytes);
        let manifest_digest =
            content_manifest_digest_sha256(&manifest_envelope).expect("compute manifest digest");

        store
            .put_if_absent(&blob(namespace_id.as_str(), &block_digest), content_bytes)
            .expect("seed block object");
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &manifest_digest),
                &encode_content_manifest_json(&manifest_envelope).expect("encode manifest"),
            )
            .expect("seed manifest object");

        let validated = validate_durable_content_reference(&store, &namespace_id, &manifest_digest)
            .expect("validate durable content");

        assert_eq!(validated.file_size_bytes, 16);
        assert_eq!(validated.file_digest_sha256, block_digest);
        assert!(validated
            .checked_invariants
            .contains(&"content_manifest_blocks_match_descriptors".to_owned()));
    }

    #[test]
    fn read_durable_content_bytes_returns_validated_bytes() {
        let temp_dir = TestDir::new("core-content-read-bytes");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let namespace_id = NamespaceId::from("ns-1");
        let content_bytes = b"hello from loon\n";
        let block_digest = sha256_digest(content_bytes);
        let manifest_envelope = sample_manifest(&namespace_id, content_bytes);
        let manifest_digest =
            content_manifest_digest_sha256(&manifest_envelope).expect("compute manifest digest");

        store
            .put_if_absent(&blob(namespace_id.as_str(), &block_digest), content_bytes)
            .expect("seed block object");
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &manifest_digest),
                &encode_content_manifest_json(&manifest_envelope).expect("encode manifest"),
            )
            .expect("seed manifest object");

        let read = read_durable_content_bytes(&store, &namespace_id, &manifest_digest)
            .expect("read durable content bytes");

        assert_eq!(read.bytes, content_bytes);
        assert_eq!(read.validated.file_size_bytes, content_bytes.len() as u64);
        assert_eq!(read.validated.file_digest_sha256, block_digest);
    }

    #[test]
    fn validate_durable_content_reference_rejects_missing_block() {
        let temp_dir = TestDir::new("core-content-missing-block");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let namespace_id = NamespaceId::from("ns-1");
        let content_bytes = b"hello from loon\n";
        let block_digest = sha256_digest(content_bytes);
        let manifest_envelope = sample_manifest(&namespace_id, content_bytes);
        let manifest_digest =
            content_manifest_digest_sha256(&manifest_envelope).expect("compute manifest digest");

        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &manifest_digest),
                &encode_content_manifest_json(&manifest_envelope).expect("encode manifest"),
            )
            .expect("seed manifest object");

        let error = validate_durable_content_reference(&store, &namespace_id, &manifest_digest)
            .expect_err("missing block should fail");

        assert_eq!(
            error,
            DurableContentValidationError::MissingBlockObject {
                object_key: blob(namespace_id.as_str(), &block_digest),
            }
        );
    }

    #[test]
    fn validate_durable_content_reference_rejects_manifest_namespace_mismatch() {
        let temp_dir = TestDir::new("core-content-namespace-mismatch");
        let store = LocalFsStore::new(temp_dir.path()).expect("create local object store");
        let namespace_id = NamespaceId::from("ns-1");
        let content_bytes = b"hello from loon\n";
        let block_digest = sha256_digest(content_bytes);
        let manifest_envelope = sample_manifest(&NamespaceId::from("ns-2"), content_bytes);
        let manifest_digest =
            content_manifest_digest_sha256(&manifest_envelope).expect("compute manifest digest");

        store
            .put_if_absent(&blob(namespace_id.as_str(), &block_digest), content_bytes)
            .expect("seed block object");
        store
            .put_if_absent(
                &content_manifest(namespace_id.as_str(), &manifest_digest),
                &encode_content_manifest_json(&manifest_envelope).expect("encode manifest"),
            )
            .expect("seed manifest object");

        let error = validate_durable_content_reference(&store, &namespace_id, &manifest_digest)
            .expect_err("namespace mismatch should fail");

        assert_eq!(
            error,
            DurableContentValidationError::ManifestNamespaceMismatch {
                object_key: content_manifest(namespace_id.as_str(), &manifest_digest),
                expected: NamespaceId::from("ns-1"),
                actual: NamespaceId::from("ns-2"),
            }
        );
    }

    fn sample_manifest(
        namespace_id: &NamespaceId,
        content_bytes: &[u8],
    ) -> ContentManifestEnvelope {
        ContentManifestEnvelope::from_payload(ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes: content_bytes.len() as u64,
            file_digest_sha256: sha256_digest(content_bytes),
            block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
            blocks: vec![ContentBlockDescriptor {
                content_digest_sha256: sha256_digest(content_bytes),
                plaintext_size_bytes: content_bytes.len() as u64,
            }],
        })
        .expect("build content manifest envelope")
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(label: &str) -> Self {
            static NEXT_DIR_ID: AtomicU64 = AtomicU64::new(0);

            let unique = NEXT_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time before unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("loon-{label}-{nanos}-{unique}"));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
