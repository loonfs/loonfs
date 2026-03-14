use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_types::{
    content_manifest_digest_sha256, encode_content_manifest_json, sha256_digest,
    ContentBlockDescriptor, ContentManifestCodecError, ContentManifestEnvelope,
    ContentManifestPayload, NamespaceId, CONTENT_BLOCK_SIZE_BYTES,
};
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedBlockObject {
    pub object_key: String,
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedContent {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub block_objects: Vec<UploadedBlockObject>,
}

#[derive(Debug, Error)]
pub enum UploadError {
    #[error("failed to read local file `{path}`: {message}")]
    LocalFileRead { path: String, message: String },
    #[error(transparent)]
    ContentManifestCodec(#[from] ContentManifestCodecError),
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
}

pub fn upload_small_file_from_path<S: ObjectStore>(
    store: &S,
    namespace_id: &NamespaceId,
    source_path: &Path,
) -> Result<UploadedContent, UploadError> {
    let bytes = fs::read(source_path).map_err(|err| UploadError::LocalFileRead {
        path: source_path.display().to_string(),
        message: err.to_string(),
    })?;

    let mut manifest_blocks = Vec::new();
    let mut block_objects = Vec::new();
    for block_bytes in bytes.chunks(CONTENT_BLOCK_SIZE_BYTES as usize) {
        let content_digest_sha256 = sha256_digest(block_bytes);
        let object_key = blob(namespace_id.as_str(), &content_digest_sha256);

        put_immutable_verified(store, &object_key, block_bytes)?;

        manifest_blocks.push(ContentBlockDescriptor {
            content_digest_sha256: content_digest_sha256.clone(),
            plaintext_size_bytes: block_bytes.len() as u64,
        });
        block_objects.push(UploadedBlockObject {
            object_key,
            content_digest_sha256,
            plaintext_size_bytes: block_bytes.len() as u64,
        });
    }

    let manifest_envelope = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256: sha256_digest(&bytes),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks: manifest_blocks,
    })?;
    let manifest_bytes = encode_content_manifest_json(&manifest_envelope)?;
    let content_manifest_digest = content_manifest_digest_sha256(&manifest_envelope)?;
    let manifest_object_key = content_manifest(namespace_id.as_str(), &content_manifest_digest);

    put_immutable_verified(store, &manifest_object_key, &manifest_bytes)?;

    Ok(UploadedContent {
        namespace_id: namespace_id.clone(),
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256: manifest_envelope.payload.file_digest_sha256.clone(),
        content_manifest_digest,
        manifest_object_key,
        manifest_envelope,
        block_objects,
    })
}

fn put_immutable_verified<S: ObjectStore>(
    store: &S,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), UploadError> {
    match store.put_if_absent(object_key, bytes) {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::PreconditionFailed) => {
            let existing =
                store
                    .get(object_key, None)
                    .map_err(|source| UploadError::StoreRead {
                        object_key: object_key.to_owned(),
                        source,
                    })?;
            match existing {
                Some(existing_bytes) if existing_bytes == bytes => Ok(()),
                Some(_) => Err(UploadError::ExistingObjectMismatch {
                    object_key: object_key.to_owned(),
                }),
                None => Err(UploadError::ExistingObjectMissing {
                    object_key: object_key.to_owned(),
                }),
            }
        }
        Err(source) => Err(UploadError::StoreWrite {
            object_key: object_key.to_owned(),
            source,
        }),
    }
}
