use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_types::{
    content_manifest_digest_sha256, decode_content_manifest_json, sha256_digest,
    ContentManifestCodecError, ContentManifestEnvelope, NamespaceId,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedContent {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("failed to read manifest object `{object_key}`: {source}")]
    StoreRead {
        object_key: String,
        #[source]
        source: ObjectStoreError,
    },
    #[error("missing manifest object `{object_key}`")]
    MissingManifestObject { object_key: String },
    #[error("content manifest codec error: {0}")]
    ContentManifestCodec(#[from] ContentManifestCodecError),
    #[error("manifest digest mismatch: expected `{expected}`, actual `{actual}`")]
    ManifestDigestMismatch { expected: String, actual: String },
    #[error("manifest namespace mismatch: expected `{expected}`, actual `{actual}`")]
    ManifestNamespaceMismatch {
        expected: NamespaceId,
        actual: NamespaceId,
    },
    #[error("missing content block `{object_key}`")]
    MissingBlockObject { object_key: String },
    #[error(
        "content block descriptor mismatch for `{object_key}`: expected digest `{expected_digest}` size `{expected_size}`, actual digest `{actual_digest}` size `{actual_size}`"
    )]
    BlockDescriptorMismatch {
        object_key: String,
        expected_digest: String,
        expected_size: u64,
        actual_digest: String,
        actual_size: u64,
    },
    #[error("file size mismatch: expected `{expected}`, actual `{actual}`")]
    FileSizeMismatch { expected: u64, actual: u64 },
    #[error("file digest mismatch: expected `{expected}`, actual `{actual}`")]
    FileDigestMismatch { expected: String, actual: String },
}

pub fn download_file_to_bytes<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> Result<DownloadedContent, DownloadError> {
    let manifest_object_key = content_manifest(namespace_id.as_str(), content_manifest_digest);
    let manifest_bytes = store
        .get(&manifest_object_key, None)
        .map_err(|source| DownloadError::StoreRead {
            object_key: manifest_object_key.clone(),
            source,
        })?
        .ok_or_else(|| DownloadError::MissingManifestObject {
            object_key: manifest_object_key.clone(),
        })?;
    let manifest_envelope = decode_content_manifest_json(&manifest_bytes)?;
    let actual_manifest_digest = content_manifest_digest_sha256(&manifest_envelope)
        .expect("content manifest should always re-encode");
    if actual_manifest_digest != content_manifest_digest {
        return Err(DownloadError::ManifestDigestMismatch {
            expected: content_manifest_digest.to_owned(),
            actual: actual_manifest_digest,
        });
    }
    if manifest_envelope.payload.namespace_id != *namespace_id {
        return Err(DownloadError::ManifestNamespaceMismatch {
            expected: namespace_id.clone(),
            actual: manifest_envelope.payload.namespace_id.clone(),
        });
    }

    let mut bytes = Vec::new();
    for block in &manifest_envelope.payload.blocks {
        let object_key = blob(namespace_id.as_str(), &block.content_digest_sha256);
        let block_bytes = store
            .get(&object_key, None)
            .map_err(|source| DownloadError::StoreRead {
                object_key: object_key.clone(),
                source,
            })?
            .ok_or_else(|| DownloadError::MissingBlockObject {
                object_key: object_key.clone(),
            })?;
        let actual_digest = sha256_digest(&block_bytes);
        let actual_size = u64::try_from(block_bytes.len()).expect("block length should fit in u64");
        if actual_digest != block.content_digest_sha256 || actual_size != block.plaintext_size_bytes
        {
            return Err(DownloadError::BlockDescriptorMismatch {
                object_key,
                expected_digest: block.content_digest_sha256.clone(),
                expected_size: block.plaintext_size_bytes,
                actual_digest,
                actual_size,
            });
        }
        bytes.extend_from_slice(&block_bytes);
    }

    let actual_size = u64::try_from(bytes.len()).expect("file length should fit in u64");
    if actual_size != manifest_envelope.payload.file_size_bytes {
        return Err(DownloadError::FileSizeMismatch {
            expected: manifest_envelope.payload.file_size_bytes,
            actual: actual_size,
        });
    }
    let actual_digest = sha256_digest(&bytes);
    if actual_digest != manifest_envelope.payload.file_digest_sha256 {
        return Err(DownloadError::FileDigestMismatch {
            expected: manifest_envelope.payload.file_digest_sha256.clone(),
            actual: actual_digest,
        });
    }

    Ok(DownloadedContent {
        namespace_id: namespace_id.clone(),
        file_size_bytes: manifest_envelope.payload.file_size_bytes,
        file_digest_sha256: manifest_envelope.payload.file_digest_sha256.clone(),
        content_manifest_digest: content_manifest_digest.to_owned(),
        manifest_envelope,
        bytes,
    })
}
