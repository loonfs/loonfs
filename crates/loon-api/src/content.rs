use crate::digest::{sha256_digest, sha256_hex};
use crate::NamespaceId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CONTENT_MANIFEST_FORMAT_VERSION: u32 = 1;
pub const CONTENT_BLOCK_SIZE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentManifestKind {
    NamespaceContentManifest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentBlockDescriptor {
    pub content_digest_sha256: String,
    pub plaintext_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifestPayload {
    pub namespace_id: NamespaceId,
    pub file_size_bytes: u64,
    pub file_digest_sha256: String,
    pub block_size_bytes: u64,
    pub blocks: Vec<ContentBlockDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifestEnvelope {
    pub kind: ContentManifestKind,
    pub format_version: u32,
    pub payload_checksum_sha256: String,
    pub payload: ContentManifestPayload,
}

impl ContentManifestEnvelope {
    pub fn from_payload(
        payload: ContentManifestPayload,
    ) -> Result<Self, ContentManifestCodecError> {
        Ok(Self {
            kind: ContentManifestKind::NamespaceContentManifest,
            format_version: CONTENT_MANIFEST_FORMAT_VERSION,
            payload_checksum_sha256: content_manifest_payload_checksum_sha256(&payload)?,
            payload,
        })
    }

    pub fn has_valid_payload_checksum(&self) -> Result<bool, ContentManifestCodecError> {
        Ok(
            self.payload_checksum_sha256
                == content_manifest_payload_checksum_sha256(&self.payload)?,
        )
    }
}

#[derive(Debug, Error)]
pub enum ContentManifestCodecError {
    #[error("failed to encode content manifest payload to JSON: {0}")]
    PayloadEncode(String),
    #[error("failed to encode content manifest envelope to JSON: {0}")]
    EnvelopeEncode(String),
    #[error("failed to decode content manifest envelope from JSON: {0}")]
    EnvelopeDecode(String),
    #[error("content manifest payload checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },
}

pub fn content_manifest_payload_checksum_sha256(
    payload: &ContentManifestPayload,
) -> Result<String, ContentManifestCodecError> {
    let bytes = serde_json::to_vec(payload)
        .map_err(|err| ContentManifestCodecError::PayloadEncode(err.to_string()))?;
    Ok(sha256_hex(&bytes))
}

pub fn encode_content_manifest_json(
    envelope: &ContentManifestEnvelope,
) -> Result<Vec<u8>, ContentManifestCodecError> {
    serde_json::to_vec(envelope)
        .map_err(|err| ContentManifestCodecError::EnvelopeEncode(err.to_string()))
}

pub fn content_manifest_digest_sha256(
    envelope: &ContentManifestEnvelope,
) -> Result<String, ContentManifestCodecError> {
    let encoded = encode_content_manifest_json(envelope)?;
    Ok(sha256_digest(&encoded))
}

pub fn decode_content_manifest_json(
    bytes: &[u8],
) -> Result<ContentManifestEnvelope, ContentManifestCodecError> {
    let envelope: ContentManifestEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| ContentManifestCodecError::EnvelopeDecode(err.to_string()))?;
    let actual = content_manifest_payload_checksum_sha256(&envelope.payload)?;

    if actual != envelope.payload_checksum_sha256 {
        return Err(ContentManifestCodecError::ChecksumMismatch {
            expected: envelope.payload_checksum_sha256.clone(),
            actual,
        });
    }

    Ok(envelope)
}
