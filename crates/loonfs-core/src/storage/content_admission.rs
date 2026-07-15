//! Short-lived content tokens: a serving session mints one after
//! validating direct-put content, and batch validation accepts it instead
//! of re-probing object storage for the blob.

use base64::Engine as _;
use loonfs_api::v0::ValidatedContentToken;
use loonfs_api::{ContentRef, NamespaceId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const TOKEN_VERSION: &str = "vct0";
const DEFAULT_TOKEN_TTL_MS: u64 = 60 * 60 * 1000;
const SHA256_BLOCK_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentAdmission {
    namespace_id: NamespaceId,
    content_ref: ContentRef,
    expires_at_ms: u64,
}

impl ContentAdmission {
    /// Admits a content ref the caller itself wrote durably (and had acked)
    /// before submitting the publish, so batch validation does not re-probe
    /// object storage for a blob this process just stored. The caller's own
    /// completed write is the durability authority, exactly like a token
    /// minted after upload validation.
    pub fn for_durable_content_write(
        namespace_id: NamespaceId,
        content_ref: ContentRef,
        now_ms: u64,
    ) -> Self {
        Self {
            namespace_id,
            content_ref,
            // Generous on purpose: it must outlive batched publications
            // and stale-head retries. An expired admission only downgrades
            // to a durable-validation probe, never to a weaker check.
            expires_at_ms: now_ms.saturating_add(DEFAULT_TOKEN_TTL_MS),
        }
    }

    pub(crate) fn admits(
        &self,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        now_ms: u64,
    ) -> bool {
        self.namespace_id == *namespace_id
            && self.content_ref == *content_ref
            && self.expires_at_ms >= now_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentTokenPayload {
    version: String,
    namespace_id: NamespaceId,
    content_ref: ContentRef,
    expires_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContentTokenError {
    #[error("content token is malformed")]
    Malformed,
    #[error("content token signature mismatch")]
    BadSignature,
    #[error("content token namespace mismatch")]
    NamespaceMismatch,
    #[error("content token content ref mismatch")]
    ContentRefMismatch,
    #[error("content token has expired")]
    Expired,
    #[error("content token codec error: {0}")]
    Codec(String),
    #[error("content token timestamp overflow")]
    TimeOverflow,
}

pub fn mint_content_token(
    secret: &str,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
    now_ms: u64,
) -> Result<String, ContentTokenError> {
    let expires_at_ms = now_ms
        .checked_add(DEFAULT_TOKEN_TTL_MS)
        .ok_or(ContentTokenError::TimeOverflow)?;
    let payload = ContentTokenPayload {
        version: TOKEN_VERSION.to_owned(),
        namespace_id: namespace_id.clone(),
        content_ref: content_ref.clone(),
        expires_at_ms,
    };
    let payload_json = serde_json::to_vec(&payload)
        .map_err(|error| ContentTokenError::Codec(error.to_string()))?;
    let payload_part = base64_url(&payload_json);
    let signature_part = base64_url(&hmac_sha256(secret.as_bytes(), payload_part.as_bytes()));
    Ok(format!("{payload_part}.{signature_part}"))
}

pub fn verify_content_token(
    secret: &str,
    namespace_id: &NamespaceId,
    token: &ValidatedContentToken,
    now_ms: u64,
) -> Result<ContentAdmission, ContentTokenError> {
    let (payload_part, signature_part) = token
        .token
        .split_once('.')
        .ok_or(ContentTokenError::Malformed)?;
    let actual_signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature_part)
        .map_err(|_| ContentTokenError::Malformed)?;
    let expected_signature = hmac_sha256(secret.as_bytes(), payload_part.as_bytes());
    if !constant_time_eq(&actual_signature, &expected_signature) {
        return Err(ContentTokenError::BadSignature);
    }

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_part)
        .map_err(|_| ContentTokenError::Malformed)?;
    let payload: ContentTokenPayload = serde_json::from_slice(&payload_bytes)
        .map_err(|error| ContentTokenError::Codec(error.to_string()))?;
    if payload.version != TOKEN_VERSION {
        return Err(ContentTokenError::Malformed);
    }
    if payload.namespace_id != *namespace_id {
        return Err(ContentTokenError::NamespaceMismatch);
    }
    if payload.content_ref != token.content_ref {
        return Err(ContentTokenError::ContentRefMismatch);
    }
    if payload.expires_at_ms < now_ms {
        return Err(ContentTokenError::Expired);
    }

    Ok(ContentAdmission {
        namespace_id: payload.namespace_id,
        content_ref: payload.content_ref,
        expires_at_ms: payload.expires_at_ms,
    })
}

fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut normalized_key = if key.len() > SHA256_BLOCK_BYTES {
        Sha256::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    normalized_key.resize(SHA256_BLOCK_BYTES, 0);
    let mut outer = [0x5c; SHA256_BLOCK_BYTES];
    let mut inner = [0x36; SHA256_BLOCK_BYTES];
    for (idx, byte) in normalized_key.iter().enumerate() {
        outer[idx] ^= *byte;
        inner[idx] ^= *byte;
    }
    let inner_hash = Sha256::new()
        .chain_update(inner)
        .chain_update(value)
        .finalize();
    Sha256::new()
        .chain_update(outer)
        .chain_update(inner_hash)
        .finalize()
        .to_vec()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let diff = left
        .iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (*left ^ *right));
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::{mint_content_token, verify_content_token};
    use loonfs_api::v0::ValidatedContentToken;
    use loonfs_api::{ContentRef, NamespaceId};

    #[test]
    fn token_round_trips_and_admits_matching_content() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = ContentRef::whole_file_v0(b"hello");
        let token = mint_content_token("secret", &namespace, &content, 1_000).expect("mint");
        let token = ValidatedContentToken {
            content_ref: content.clone(),
            token,
        };

        let admission =
            verify_content_token("secret", &namespace, &token, 1_000).expect("verify token");

        assert!(admission.admits(&namespace, &content, 1_000));
    }

    #[test]
    fn token_rejects_wrong_secret_namespace_content_and_expiry() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let other_namespace = NamespaceId::parse("other").expect("namespace");
        let content = ContentRef::whole_file_v0(b"hello");
        let other_content = ContentRef::whole_file_v0(b"other");
        let token = mint_content_token("secret", &namespace, &content, 1_000).expect("mint");
        let token = ValidatedContentToken {
            content_ref: content.clone(),
            token,
        };

        assert!(verify_content_token("other", &namespace, &token, 1_000).is_err());
        assert!(verify_content_token("secret", &other_namespace, &token, 1_000).is_err());
        assert!(verify_content_token(
            "secret",
            &namespace,
            &ValidatedContentToken {
                content_ref: other_content,
                token: token.token.clone(),
            },
            1_000,
        )
        .is_err());
        assert!(
            verify_content_token("secret", &namespace, &token, 1_000 + 60 * 60 * 1000 + 1).is_err()
        );
    }
}
