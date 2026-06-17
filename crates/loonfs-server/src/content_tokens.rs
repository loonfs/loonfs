use base64::Engine as _;
use loonfs_api::{ContentRef, NamespaceId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const TOKEN_VERSION: &str = "vct0";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_secs(60 * 60);
const SHA256_BLOCK_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentTokenPayload {
    version: String,
    namespace_id: NamespaceId,
    content_ref: ContentRef,
    expires_at_ms: u64,
}

#[derive(Debug, Error)]
pub(crate) enum ContentTokenError {
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
    #[error("system time is before unix epoch: {0}")]
    Time(String),
}

pub(crate) fn mint_content_token(
    secret: &str,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
    now: SystemTime,
) -> Result<String, ContentTokenError> {
    let payload = ContentTokenPayload {
        version: TOKEN_VERSION.to_owned(),
        namespace_id: namespace_id.clone(),
        content_ref: content_ref.clone(),
        expires_at_ms: unix_ms(now)? + DEFAULT_TOKEN_TTL.as_millis() as u64,
    };
    let payload_json = serde_json::to_vec(&payload)
        .map_err(|error| ContentTokenError::Codec(error.to_string()))?;
    let payload_part = base64_url(&payload_json);
    let signature_part = base64_url(&hmac_sha256(secret.as_bytes(), payload_part.as_bytes()));
    Ok(format!("{payload_part}.{signature_part}"))
}

pub(crate) fn verify_content_token(
    secret: &str,
    namespace_id: &NamespaceId,
    content_ref: &ContentRef,
    token: &str,
    now: SystemTime,
) -> Result<(), ContentTokenError> {
    let (payload_part, signature_part) =
        token.split_once('.').ok_or(ContentTokenError::Malformed)?;
    let expected = base64_url(&hmac_sha256(secret.as_bytes(), payload_part.as_bytes()));
    if signature_part != expected {
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
    if payload.content_ref != *content_ref {
        return Err(ContentTokenError::ContentRefMismatch);
    }
    if payload.expires_at_ms < unix_ms(now)? {
        return Err(ContentTokenError::Expired);
    }
    Ok(())
}

fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn unix_ms(time: SystemTime) -> Result<u64, ContentTokenError> {
    time.duration_since(UNIX_EPOCH)
        .map_err(|error| ContentTokenError::Time(error.to_string()))
        .map(|duration| duration.as_millis() as u64)
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

#[cfg(test)]
mod tests {
    use super::{mint_content_token, verify_content_token};
    use loonfs_api::{ContentRef, NamespaceId};
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn token_round_trips_and_binds_namespace_and_content_ref() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let other_namespace = NamespaceId::parse("other").expect("namespace");
        let content = ContentRef::whole_file_v0(b"hello");
        let other_content = ContentRef::whole_file_v0(b"other");
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let token = mint_content_token("secret", &namespace, &content, now).expect("mint");

        verify_content_token("secret", &namespace, &content, &token, now).expect("verify");
        assert!(verify_content_token("other", &namespace, &content, &token, now).is_err());
        assert!(verify_content_token("secret", &other_namespace, &content, &token, now).is_err());
        assert!(verify_content_token("secret", &namespace, &other_content, &token, now).is_err());
        assert!(verify_content_token(
            "secret",
            &namespace,
            &content,
            &token,
            now + Duration::from_secs(60 * 60 + 1),
        )
        .is_err());
    }
}
