use loon_server::objectstore::error::ObjectStoreError;
use loon_server::objectstore::keys::{blob, content_manifest};
use loon_server::objectstore::ObjectStore;
use loon_types::{
    content_manifest_digest_sha256, decode_content_manifest_json, sha256_digest,
    ContentBlockDescriptor, ContentManifestCodecError, ContentManifestEnvelope, NamespaceId,
};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedContentManifest {
    pub namespace_id: NamespaceId,
    pub content_manifest_digest: String,
    pub object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedContentSummary {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_envelope: ContentManifestEnvelope,
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
    #[error("failed to read local file during `{operation}` for `{path}`: {source}")]
    LocalFileRead {
        operation: &'static str,
        path: String,
        #[source]
        source: io::Error,
    },
}

pub fn load_content_manifest<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> Result<LoadedContentManifest, DownloadError> {
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

    Ok(LoadedContentManifest {
        namespace_id: namespace_id.clone(),
        content_manifest_digest: content_manifest_digest.to_owned(),
        object_key: manifest_object_key,
        manifest_envelope,
    })
}

pub fn expected_staged_file_size(
    manifest_envelope: &ContentManifestEnvelope,
    next_block_index: u64,
) -> u64 {
    let clamped = usize::try_from(next_block_index)
        .unwrap_or(usize::MAX)
        .min(manifest_envelope.payload.blocks.len());
    manifest_envelope
        .payload
        .blocks
        .iter()
        .take(clamped)
        .map(|block| block.plaintext_size_bytes)
        .sum()
}

pub fn load_validated_content_block<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    block: &ContentBlockDescriptor,
) -> Result<Vec<u8>, DownloadError> {
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
    if actual_digest != block.content_digest_sha256 || actual_size != block.plaintext_size_bytes {
        return Err(DownloadError::BlockDescriptorMismatch {
            object_key,
            expected_digest: block.content_digest_sha256.clone(),
            expected_size: block.plaintext_size_bytes,
            actual_digest,
            actual_size,
        });
    }
    Ok(block_bytes)
}

pub fn verify_downloaded_file_path(
    loaded_manifest: &LoadedContentManifest,
    file_path: &Path,
) -> Result<DownloadedContentSummary, DownloadError> {
    let mut file =
        File::open(file_path).map_err(|source| file_read_error("open_file", file_path, source))?;
    let mut hasher = Sha256::new();
    let mut total_size = 0u64;
    let mut buffer = [0u8; 8192];

    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| file_read_error("read_file", file_path, source))?;
        if read == 0 {
            break;
        }
        total_size = total_size
            .checked_add(u64::try_from(read).expect("read length should fit in u64"))
            .expect("downloaded file size should not overflow u64");
        hasher.update(&buffer[..read]);
    }

    if total_size != loaded_manifest.manifest_envelope.payload.file_size_bytes {
        return Err(DownloadError::FileSizeMismatch {
            expected: loaded_manifest.manifest_envelope.payload.file_size_bytes,
            actual: total_size,
        });
    }

    let actual_digest = format!("sha256:{:x}", hasher.finalize());
    if actual_digest != loaded_manifest.manifest_envelope.payload.file_digest_sha256 {
        return Err(DownloadError::FileDigestMismatch {
            expected: loaded_manifest
                .manifest_envelope
                .payload
                .file_digest_sha256
                .clone(),
            actual: actual_digest,
        });
    }

    Ok(DownloadedContentSummary {
        namespace_id: loaded_manifest.namespace_id.clone(),
        file_size_bytes: loaded_manifest.manifest_envelope.payload.file_size_bytes,
        file_digest_sha256: loaded_manifest
            .manifest_envelope
            .payload
            .file_digest_sha256
            .clone(),
        content_manifest_digest: loaded_manifest.content_manifest_digest.clone(),
        manifest_envelope: loaded_manifest.manifest_envelope.clone(),
    })
}

pub fn download_file_to_bytes<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
) -> Result<DownloadedContent, DownloadError> {
    let loaded_manifest = load_content_manifest(store, namespace_id, content_manifest_digest)?;

    let mut bytes = Vec::new();
    for block in &loaded_manifest.manifest_envelope.payload.blocks {
        bytes.extend_from_slice(&load_validated_content_block(store, namespace_id, block)?);
    }

    let actual_size = u64::try_from(bytes.len()).expect("file length should fit in u64");
    if actual_size != loaded_manifest.manifest_envelope.payload.file_size_bytes {
        return Err(DownloadError::FileSizeMismatch {
            expected: loaded_manifest.manifest_envelope.payload.file_size_bytes,
            actual: actual_size,
        });
    }
    let actual_digest = sha256_digest(&bytes);
    if actual_digest != loaded_manifest.manifest_envelope.payload.file_digest_sha256 {
        return Err(DownloadError::FileDigestMismatch {
            expected: loaded_manifest
                .manifest_envelope
                .payload
                .file_digest_sha256
                .clone(),
            actual: actual_digest,
        });
    }

    Ok(DownloadedContent {
        namespace_id: loaded_manifest.namespace_id,
        file_size_bytes: loaded_manifest.manifest_envelope.payload.file_size_bytes,
        file_digest_sha256: loaded_manifest
            .manifest_envelope
            .payload
            .file_digest_sha256
            .clone(),
        content_manifest_digest: loaded_manifest.content_manifest_digest,
        manifest_envelope: loaded_manifest.manifest_envelope,
        bytes,
    })
}

fn file_read_error(operation: &'static str, path: &Path, source: io::Error) -> DownloadError {
    DownloadError::LocalFileRead {
        operation,
        path: path.display().to_string(),
        source,
    }
}
