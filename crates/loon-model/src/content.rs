use crate::{
    ModelContentValidationError, ModelInodeUploadDecision, ModelInodeUploadRecord,
    ModelInodeUploadValidationError, ModelLocalOnlyUploadDecision, ModelLocalOnlyUploadRecord,
    ModelLocalOnlyUploadValidationError, ModelMaterializedContent, ModelUploadedContent,
    ModelValidatedContent,
};
use loon_types::{
    content_manifest_digest_sha256, sha256_digest, ContentBlockDescriptor,
    ContentManifestCodecError, ContentManifestEnvelope, ContentManifestPayload, NamespaceId,
    CONTENT_BLOCK_SIZE_BYTES,
};
use std::collections::BTreeMap;

pub fn build_uploaded_content(
    namespace_id: NamespaceId,
    bytes: &[u8],
) -> Result<ModelUploadedContent, ContentManifestCodecError> {
    let blocks = bytes
        .chunks(CONTENT_BLOCK_SIZE_BYTES as usize)
        .map(|block_bytes| ContentBlockDescriptor {
            content_digest_sha256: sha256_digest(block_bytes),
            plaintext_size_bytes: block_bytes.len() as u64,
        })
        .collect();
    let payload = ContentManifestPayload {
        namespace_id,
        file_size_bytes: bytes.len() as u64,
        file_digest_sha256: sha256_digest(bytes),
        block_size_bytes: CONTENT_BLOCK_SIZE_BYTES,
        blocks,
    };
    let manifest_envelope = ContentManifestEnvelope::from_payload(payload)?;
    let content_manifest_digest = content_manifest_digest_sha256(&manifest_envelope)?;

    Ok(ModelUploadedContent {
        file_size_bytes: manifest_envelope.payload.file_size_bytes,
        file_digest_sha256: manifest_envelope.payload.file_digest_sha256.clone(),
        content_manifest_digest,
        manifest_envelope,
    })
}

pub fn validate_local_only_upload_record(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    upload: &ModelLocalOnlyUploadRecord,
) -> Result<String, ModelLocalOnlyUploadValidationError> {
    let local_content_digest = local_content_digest
        .ok_or(ModelLocalOnlyUploadValidationError::MissingLocalContentDigest)?;

    if &upload.namespace_id != namespace_id {
        return Err(ModelLocalOnlyUploadValidationError::NamespaceMismatch {
            expected: namespace_id.clone(),
            actual: upload.namespace_id.clone(),
        });
    }

    if upload.file_digest_sha256 != local_content_digest {
        return Err(ModelLocalOnlyUploadValidationError::FileDigestMismatch {
            expected: local_content_digest.to_owned(),
            actual: upload.file_digest_sha256.clone(),
        });
    }

    Ok(upload.content_manifest_digest.clone())
}

pub fn validate_inode_upload_record(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    upload: &ModelInodeUploadRecord,
) -> Result<String, ModelInodeUploadValidationError> {
    let local_content_digest =
        local_content_digest.ok_or(ModelInodeUploadValidationError::MissingLocalContentDigest)?;

    if &upload.namespace_id != namespace_id {
        return Err(ModelInodeUploadValidationError::NamespaceMismatch {
            expected: namespace_id.clone(),
            actual: upload.namespace_id.clone(),
        });
    }

    if upload.file_digest_sha256 != local_content_digest {
        return Err(ModelInodeUploadValidationError::FileDigestMismatch {
            expected: local_content_digest.to_owned(),
            actual: upload.file_digest_sha256.clone(),
        });
    }

    Ok(upload.content_manifest_digest.clone())
}

pub fn decide_local_only_upload_action(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    existing_upload: Option<&ModelLocalOnlyUploadRecord>,
) -> Result<ModelLocalOnlyUploadDecision, ModelLocalOnlyUploadValidationError> {
    match existing_upload {
        Some(upload) => {
            match validate_local_only_upload_record(namespace_id, local_content_digest, upload) {
                Ok(content_manifest_digest) => Ok(ModelLocalOnlyUploadDecision::ReuseExisting {
                    content_manifest_digest,
                }),
                Err(ModelLocalOnlyUploadValidationError::NamespaceMismatch { .. })
                | Err(ModelLocalOnlyUploadValidationError::FileDigestMismatch { .. }) => {
                    Ok(ModelLocalOnlyUploadDecision::UploadFresh)
                }
                Err(other) => Err(other),
            }
        }
        None => Ok(ModelLocalOnlyUploadDecision::UploadFresh),
    }
}

pub fn decide_inode_upload_action(
    namespace_id: &NamespaceId,
    local_content_digest: Option<&str>,
    existing_upload: Option<&ModelInodeUploadRecord>,
) -> Result<ModelInodeUploadDecision, ModelInodeUploadValidationError> {
    match existing_upload {
        Some(upload) => {
            match validate_inode_upload_record(namespace_id, local_content_digest, upload) {
                Ok(content_manifest_digest) => Ok(ModelInodeUploadDecision::ReuseExisting {
                    content_manifest_digest,
                }),
                Err(ModelInodeUploadValidationError::NamespaceMismatch { .. })
                | Err(ModelInodeUploadValidationError::FileDigestMismatch { .. }) => {
                    Ok(ModelInodeUploadDecision::UploadFresh)
                }
                Err(other) => Err(other),
            }
        }
        None => Ok(ModelInodeUploadDecision::UploadFresh),
    }
}

pub fn validate_uploaded_content_reference(
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
    manifest_envelope: &ContentManifestEnvelope,
    available_blocks: &BTreeMap<String, Vec<u8>>,
) -> Result<ModelValidatedContent, ModelContentValidationError> {
    let materialized = materialize_uploaded_content_reference(
        namespace_id,
        content_manifest_digest,
        manifest_envelope,
        available_blocks,
    )?;

    Ok(ModelValidatedContent {
        file_size_bytes: materialized.file_size_bytes,
        file_digest_sha256: materialized.file_digest_sha256,
        block_count: manifest_envelope.payload.blocks.len(),
    })
}

pub fn materialize_uploaded_content_reference(
    namespace_id: &NamespaceId,
    content_manifest_digest: &str,
    manifest_envelope: &ContentManifestEnvelope,
    available_blocks: &BTreeMap<String, Vec<u8>>,
) -> Result<ModelMaterializedContent, ModelContentValidationError> {
    let actual_manifest_digest = content_manifest_digest_sha256(manifest_envelope)
        .expect("content manifest envelope should always re-encode");
    if actual_manifest_digest != content_manifest_digest {
        return Err(ModelContentValidationError::ManifestDigestMismatch {
            expected: content_manifest_digest.to_owned(),
            actual: actual_manifest_digest,
        });
    }

    if &manifest_envelope.payload.namespace_id != namespace_id {
        return Err(ModelContentValidationError::ManifestNamespaceMismatch {
            expected: namespace_id.clone(),
            actual: manifest_envelope.payload.namespace_id.clone(),
        });
    }

    let mut reconstructed = Vec::new();
    for block in &manifest_envelope.payload.blocks {
        let bytes = available_blocks
            .get(&block.content_digest_sha256)
            .ok_or_else(|| ModelContentValidationError::MissingBlock {
                digest: block.content_digest_sha256.clone(),
            })?;
        let actual_size = bytes.len() as u64;
        if actual_size != block.plaintext_size_bytes {
            return Err(ModelContentValidationError::BlockLengthMismatch {
                digest: block.content_digest_sha256.clone(),
                expected: block.plaintext_size_bytes,
                actual: actual_size,
            });
        }

        let actual_digest = sha256_digest(bytes);
        if actual_digest != block.content_digest_sha256 {
            return Err(ModelContentValidationError::BlockDigestMismatch {
                expected: block.content_digest_sha256.clone(),
                actual: actual_digest,
            });
        }

        reconstructed.extend_from_slice(bytes);
    }

    let actual_file_size = reconstructed.len() as u64;
    if actual_file_size != manifest_envelope.payload.file_size_bytes {
        return Err(ModelContentValidationError::FileSizeMismatch {
            expected: manifest_envelope.payload.file_size_bytes,
            actual: actual_file_size,
        });
    }

    let actual_file_digest = sha256_digest(&reconstructed);
    if actual_file_digest != manifest_envelope.payload.file_digest_sha256 {
        return Err(ModelContentValidationError::FileDigestMismatch {
            expected: manifest_envelope.payload.file_digest_sha256.clone(),
            actual: actual_file_digest,
        });
    }

    Ok(ModelMaterializedContent {
        file_size_bytes: actual_file_size,
        file_digest_sha256: actual_file_digest,
        bytes: reconstructed,
    })
}
