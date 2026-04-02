use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_types::{
    decode_content_manifest_json, sha256_digest, ContentManifestEnvelope, NamespaceId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadedBlockObject {
    pub object_key: String,
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadedContent {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub block_objects: Vec<UploadedBlockObject>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedUploadBlock {
    pub object_key: String,
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
    pub file_offset_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannedUpload {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub blocks: Vec<PlannedUploadBlock>,
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

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("failed to read local file `{path}`: {message}")]
    LocalFileRead { path: String, message: String },
    #[error(transparent)]
    ContentManifestCodec(#[from] loon_types::ContentManifestCodecError),
    #[error("failed to write immutable object `{object_key}`: {source}")]
    StoreWrite {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("failed to read immutable object `{object_key}` after precondition failure: {source}")]
    StoreRead {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("immutable object `{object_key}` existed during upload but could not be loaded")]
    ExistingObjectMissing { object_key: String },
    #[error("existing immutable object `{object_key}` does not match uploaded bytes")]
    ExistingObjectMismatch { object_key: String },
    #[error(
        "local file changed during upload `{path}` block `{block_index}` expected digest `{expected_digest}` actual `{actual_digest}`"
    )]
    LocalFileChangedDuringUpload {
        path: String,
        block_index: u64,
        expected_digest: String,
        actual_digest: String,
    },
    #[error(
        "local file truncated during upload `{path}` block `{block_index}` expected size `{expected_size}` actual `{actual_size}`"
    )]
    LocalFileTruncatedDuringUpload {
        path: String,
        block_index: u64,
        expected_size: u64,
        actual_size: u64,
    },
}

pub fn upload_small_file_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
) -> Result<UploadedContent, UploadError> {
    let plan = plan_upload_from_path(namespace_id, source_path)?;
    for block_index in 0..plan.blocks.len() {
        upload_planned_block_from_path(
            store,
            source_path,
            &plan,
            u64::try_from(block_index).expect("block index should fit in u64"),
        )?;
    }
    finalize_planned_upload(store, &plan)
}

pub fn plan_upload_from_path(
    namespace_id: &NamespaceId,
    source_path: &Path,
) -> Result<PlannedUpload, UploadError> {
    let mut file = File::open(source_path).map_err(|err| UploadError::LocalFileRead {
        path: source_path.display().to_string(),
        message: err.to_string(),
    })?;
    let mut file_hasher = Sha256::new();
    let mut manifest_blocks = Vec::new();
    let mut planned_blocks = Vec::new();
    let mut file_size_bytes = 0u64;
    let mut file_offset_bytes = 0u64;

    loop {
        let block_bytes = read_next_block(&mut file, source_path)?;
        if block_bytes.is_empty() {
            break;
        }

        file_hasher.update(&block_bytes);
        let content_digest_sha256 = sha256_digest(&block_bytes);
        let plaintext_size_bytes =
            u64::try_from(block_bytes.len()).expect("block length should fit in u64");
        let object_key = blob(namespace_id.as_str(), &content_digest_sha256);
        manifest_blocks.push(loon_types::ContentBlockDescriptor {
            content_digest_sha256: content_digest_sha256.clone(),
            plaintext_size_bytes,
        });
        planned_blocks.push(PlannedUploadBlock {
            object_key,
            content_digest_sha256,
            plaintext_size_bytes,
            file_offset_bytes,
        });
        file_size_bytes = file_size_bytes
            .checked_add(plaintext_size_bytes)
            .expect("file size should fit in u64");
        file_offset_bytes = file_offset_bytes
            .checked_add(plaintext_size_bytes)
            .expect("file offset should fit in u64");
    }

    let file_digest_sha256 = format!("sha256:{:x}", file_hasher.finalize());
    let manifest_envelope =
        ContentManifestEnvelope::from_payload(loon_types::ContentManifestPayload {
            namespace_id: namespace_id.clone(),
            file_size_bytes,
            file_digest_sha256: file_digest_sha256.clone(),
            block_size_bytes: loon_types::CONTENT_BLOCK_SIZE_BYTES,
            blocks: manifest_blocks,
        })?;
    let content_manifest_digest = loon_types::content_manifest_digest_sha256(&manifest_envelope)?;
    let manifest_object_key = content_manifest(namespace_id.as_str(), &content_manifest_digest);

    Ok(PlannedUpload {
        namespace_id: namespace_id.clone(),
        file_size_bytes,
        file_digest_sha256,
        content_manifest_digest,
        manifest_object_key,
        manifest_envelope,
        blocks: planned_blocks,
    })
}

pub fn upload_planned_block_from_path<S: ObjectStore>(
    store: &S,
    source_path: &Path,
    plan: &PlannedUpload,
    block_index: u64,
) -> Result<(), UploadError> {
    let block_index = usize::try_from(block_index)
        .unwrap_or(usize::MAX)
        .min(plan.blocks.len());
    if block_index == plan.blocks.len() {
        return Ok(());
    }

    let block = &plan.blocks[block_index];
    let mut file = File::open(source_path).map_err(|err| UploadError::LocalFileRead {
        path: source_path.display().to_string(),
        message: err.to_string(),
    })?;
    file.seek(SeekFrom::Start(block.file_offset_bytes))
        .map_err(|err| UploadError::LocalFileRead {
            path: source_path.display().to_string(),
            message: err.to_string(),
        })?;

    let absolute_block_index = u64::try_from(block_index).expect("block index should fit in u64");
    let bytes = read_exact_block(
        &mut file,
        source_path,
        absolute_block_index,
        block.plaintext_size_bytes,
    )?;
    let actual_digest = sha256_digest(&bytes);
    if actual_digest != block.content_digest_sha256 {
        return Err(UploadError::LocalFileChangedDuringUpload {
            path: source_path.display().to_string(),
            block_index: absolute_block_index,
            expected_digest: block.content_digest_sha256.clone(),
            actual_digest,
        });
    }
    put_immutable_verified(store, &block.object_key, &bytes)
}

pub fn finalize_planned_upload<S: ObjectStore>(
    store: &S,
    plan: &PlannedUpload,
) -> Result<UploadedContent, UploadError> {
    let manifest_bytes = loon_types::encode_content_manifest_json(&plan.manifest_envelope)?;
    put_immutable_verified(store, &plan.manifest_object_key, &manifest_bytes)?;

    Ok(UploadedContent {
        namespace_id: plan.namespace_id.clone(),
        file_size_bytes: plan.file_size_bytes,
        file_digest_sha256: plan.file_digest_sha256.clone(),
        content_manifest_digest: plan.content_manifest_digest.clone(),
        manifest_object_key: plan.manifest_object_key.clone(),
        manifest_envelope: plan.manifest_envelope.clone(),
        block_objects: plan
            .blocks
            .iter()
            .map(|block| UploadedBlockObject {
                object_key: block.object_key.clone(),
                content_digest_sha256: block.content_digest_sha256.clone(),
                plaintext_size_bytes: block.plaintext_size_bytes,
            })
            .collect(),
    })
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

fn read_next_block(file: &mut File, source_path: &Path) -> Result<Vec<u8>, UploadError> {
    let mut block = vec![0u8; loon_types::CONTENT_BLOCK_SIZE_BYTES as usize];
    let mut read_len = 0usize;
    while read_len < block.len() {
        let read = file
            .read(&mut block[read_len..])
            .map_err(|err| UploadError::LocalFileRead {
                path: source_path.display().to_string(),
                message: err.to_string(),
            })?;
        if read == 0 {
            break;
        }
        read_len += read;
    }
    block.truncate(read_len);
    Ok(block)
}

fn read_exact_block(
    file: &mut File,
    source_path: &Path,
    block_index: u64,
    expected_size: u64,
) -> Result<Vec<u8>, UploadError> {
    let expected_size = usize::try_from(expected_size).expect("block size should fit in usize");
    let mut block = vec![0u8; expected_size];
    let mut read_len = 0usize;
    while read_len < block.len() {
        let read = file
            .read(&mut block[read_len..])
            .map_err(|err| UploadError::LocalFileRead {
                path: source_path.display().to_string(),
                message: err.to_string(),
            })?;
        if read == 0 {
            return Err(UploadError::LocalFileTruncatedDuringUpload {
                path: source_path.display().to_string(),
                block_index,
                expected_size: u64::try_from(expected_size).expect("usize should fit in u64"),
                actual_size: u64::try_from(read_len).expect("usize should fit in u64"),
            });
        }
        read_len += read;
    }
    Ok(block)
}

fn put_immutable_verified<S: ObjectStore>(
    store: &S,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), UploadError> {
    match store.put_if_absent(object_key, bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed) => match store.get(object_key, None) {
            Ok(Some(existing)) if existing == bytes => Ok(()),
            Ok(Some(_)) => Err(UploadError::ExistingObjectMismatch {
                object_key: object_key.to_owned(),
            }),
            Ok(None) => Err(UploadError::ExistingObjectMissing {
                object_key: object_key.to_owned(),
            }),
            Err(source) => Err(UploadError::StoreRead {
                object_key: object_key.to_owned(),
                source,
            }),
        },
        Err(source) => Err(UploadError::StoreWrite {
            object_key: object_key.to_owned(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        read_durable_content_bytes, validate_durable_content_reference,
        DurableContentValidationError,
    };
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::{blob, content_manifest};
    use loon_objectstore::ObjectStore;
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
