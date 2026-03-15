use loon_objectstore::error::ObjectStoreError;
use loon_objectstore::keys::{blob, content_manifest};
use loon_objectstore::ObjectStore;
use loon_types::{
    content_manifest_digest_sha256, encode_content_manifest_json, sha256_digest,
    ContentBlockDescriptor, ContentManifestCodecError, ContentManifestEnvelope,
    ContentManifestPayload, NamespaceId, CONTENT_BLOCK_SIZE_BYTES,
};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedUploadBlock {
    pub object_key: String,
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
    pub file_offset_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedUpload {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub content_manifest_digest: String,
    pub manifest_object_key: String,
    pub manifest_envelope: ContentManifestEnvelope,
    pub blocks: Vec<PlannedUploadBlock>,
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

pub(crate) fn plan_upload_from_path(
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
    let mut block_index = 0u64;
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
        manifest_blocks.push(ContentBlockDescriptor {
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
        block_index = block_index.saturating_add(1);
    }

    let file_digest_sha256 = format!("sha256:{:x}", file_hasher.finalize());
    let manifest_envelope = ContentManifestEnvelope::from_payload(ContentManifestPayload {
        namespace_id: namespace_id.clone(),
        file_size_bytes,
        file_digest_sha256: file_digest_sha256.clone(),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks: manifest_blocks,
    })?;
    let content_manifest_digest = content_manifest_digest_sha256(&manifest_envelope)?;
    let manifest_object_key = content_manifest(namespace_id.as_str(), &content_manifest_digest);

    let _ = block_index;

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

pub(crate) fn upload_planned_block_from_path<S: ObjectStore>(
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

pub(crate) fn finalize_planned_upload<S: ObjectStore>(
    store: &S,
    plan: &PlannedUpload,
) -> Result<UploadedContent, UploadError> {
    let manifest_bytes = encode_content_manifest_json(&plan.manifest_envelope)?;
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

fn read_next_block(file: &mut File, source_path: &Path) -> Result<Vec<u8>, UploadError> {
    let mut block = vec![0u8; CONTENT_BLOCK_SIZE_BYTES as usize];
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
                expected_size: u64::try_from(block.len()).expect("block length should fit in u64"),
                actual_size: u64::try_from(read_len).expect("read length should fit in u64"),
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
