//! Content preparation proofs and short-lived tokens used by producers.
//!
//! A token can be created only from a durable, completed upload session. It
//! proves that the named content was verified before publication. Token
//! expiry is checked once when converting it to [`PreparedContent`]. The
//! resulting in-process proof remains valid for that process lifetime.

use crate::limits::CONTENT_RECEIPT_TTL_MS;
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use base64::Engine as _;
use loonfs_api::v0::ValidatedContentToken;
use loonfs_api::{ContentRef, ContentStoreId, NamespaceId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const TOKEN_VERSION: &str = "vct0";

/// Evidence read from a durable upload session in its completed state.
///
/// The type has no public constructor. Only the upload protocol can create
/// it after loading a completed session, so callers cannot create receipts
/// from an in-memory expectation or an unverified provider response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedUploadReceipt {
    namespace_id: NamespaceId,
    content_store_id: ContentStoreId,
    content_ref: ContentRef,
}

impl CompletedUploadReceipt {
    pub(crate) fn for_completed_session(
        namespace_id: NamespaceId,
        content_store_id: ContentStoreId,
        content_ref: ContentRef,
    ) -> Self {
        Self {
            namespace_id,
            content_store_id,
            content_ref,
        }
    }

    /// Returns the completed session's verified content reference.
    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentAdmission {
    content_store_id: ContentStoreId,
    content_ref: ContentRef,
}

impl ContentAdmission {
    pub(crate) fn for_durable_content_write(
        content_store_id: ContentStoreId,
        content_ref: ContentRef,
    ) -> Self {
        Self {
            content_store_id,
            content_ref,
        }
    }

    pub(crate) fn admits(
        &self,
        content_store_id: &ContentStoreId,
        content_ref: &ContentRef,
    ) -> bool {
        self.content_store_id == *content_store_id && self.content_ref == *content_ref
    }
}

/// Opaque evidence that a content reference was prepared for publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedContent {
    admission: ContentAdmission,
}

impl PreparedContent {
    pub(crate) fn from_admission(admission: ContentAdmission) -> Self {
        Self { admission }
    }

    /// Returns the prepared content reference.
    pub fn content_ref(&self) -> &ContentRef {
        &self.admission.content_ref
    }

    pub(crate) fn into_admission(self) -> ContentAdmission {
        self.admission
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentTokenPayload {
    version: String,
    namespace_id: NamespaceId,
    /// Where the content lives. Namespaces may share a content store, so
    /// binding both ends keeps a receipt from admitting anything outside the
    /// exact pairing the completed session was for.
    content_store_id: ContentStoreId,
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
    #[error("content token content store mismatch")]
    ContentStoreMismatch,
    #[error("content token has expired")]
    Expired,
    #[error("content token codec error: {0}")]
    Codec(String),
    #[error("content token timestamp overflow")]
    TimeOverflow,
}

/// Creates a short-lived token for a completed upload.
///
/// The completed session remains durable, so a caller can obtain a new token
/// by reading upload status. Losing a response therefore requires another
/// status request, not another upload.
pub fn mint_content_token(
    secret: &str,
    receipt: &CompletedUploadReceipt,
    now_ms: u64,
) -> Result<String, ContentTokenError> {
    let expires_at_ms = now_ms
        .checked_add(CONTENT_RECEIPT_TTL_MS)
        .ok_or(ContentTokenError::TimeOverflow)?;
    let payload = ContentTokenPayload {
        version: TOKEN_VERSION.to_owned(),
        namespace_id: receipt.namespace_id.clone(),
        content_store_id: receipt.content_store_id.clone(),
        content_ref: receipt.content_ref.clone(),
        expires_at_ms,
    };
    let payload_json = serde_json::to_vec(&payload)
        .map_err(|error| ContentTokenError::Codec(error.to_string()))?;
    let payload_part = base64_url(&payload_json);
    let signature_part = base64_url(&loonfs_objectstore::crypto::hmac_sha256(
        secret.as_bytes(),
        payload_part.as_bytes(),
    ));
    Ok(format!("{payload_part}.{signature_part}"))
}

pub fn verify_content_token(
    secret: &str,
    catalog: &VerifiedNamespaceCatalogEntry,
    token: &ValidatedContentToken,
    now_ms: u64,
) -> Result<PreparedContent, ContentTokenError> {
    let (payload_part, signature_part) = token
        .token
        .split_once('.')
        .ok_or(ContentTokenError::Malformed)?;
    let actual_signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(signature_part)
        .map_err(|_| ContentTokenError::Malformed)?;
    let expected_signature =
        loonfs_objectstore::crypto::hmac_sha256(secret.as_bytes(), payload_part.as_bytes());
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
    if payload.namespace_id != *catalog.namespace_id() {
        return Err(ContentTokenError::NamespaceMismatch);
    }
    if payload.content_store_id != *catalog.content_store_id() {
        return Err(ContentTokenError::ContentStoreMismatch);
    }
    if payload.content_ref != token.content_ref {
        return Err(ContentTokenError::ContentRefMismatch);
    }
    if payload.expires_at_ms < now_ms {
        return Err(ContentTokenError::Expired);
    }

    let admission =
        ContentAdmission::for_durable_content_write(payload.content_store_id, payload.content_ref);
    Ok(PreparedContent::from_admission(admission))
}

fn base64_url(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
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
    use super::*;

    use super::{mint_content_token, verify_content_token, CompletedUploadReceipt};
    use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
    use loonfs_api::v0::ValidatedContentToken;
    use loonfs_api::wire::control::HeadState;
    use loonfs_api::{ContentId, ContentRef, ContentStoreId, NamespaceId};

    const CONTENT_STORE: &str = "cs_00000000000000000000000000000001";

    fn catalog_entry(
        namespace_id: NamespaceId,
        content_store: &str,
    ) -> VerifiedNamespaceCatalogEntry {
        VerifiedNamespaceCatalogEntry::from_head(&HeadState::initial(
            namespace_id,
            ContentStoreId::parse(content_store).expect("content store id"),
        ))
    }

    fn receipt(
        namespace_id: &NamespaceId,
        content_store: &str,
        content_ref: &ContentRef,
    ) -> CompletedUploadReceipt {
        CompletedUploadReceipt::for_completed_session(
            namespace_id.clone(),
            ContentStoreId::parse(content_store).expect("content store id"),
            content_ref.clone(),
        )
    }

    #[test]
    fn token_round_trips_and_admits_matching_content() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let token = mint_content_token(
            "secret",
            &receipt(&namespace, CONTENT_STORE, &content),
            1_000,
        )
        .expect("mint");
        let token = ValidatedContentToken {
            content_ref: content.clone(),
            token,
        };
        let catalog = catalog_entry(namespace, CONTENT_STORE);

        let prepared =
            verify_content_token("secret", &catalog, &token, 1_000).expect("verify token");

        assert_eq!(prepared.content_ref(), &content);
        assert!(prepared
            .into_admission()
            .admits(catalog.content_store_id(), &content));
    }

    #[test]
    fn verified_token_admission_does_not_decay_after_token_expiry() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let issued_at_ms = 1_000;
        let token = mint_content_token(
            "secret",
            &receipt(&namespace, CONTENT_STORE, &content),
            issued_at_ms,
        )
        .expect("mint");
        let token = ValidatedContentToken {
            content_ref: content.clone(),
            token,
        };
        let catalog = catalog_entry(namespace, CONTENT_STORE);
        let prepared = verify_content_token(
            "secret",
            &catalog,
            &token,
            issued_at_ms + CONTENT_RECEIPT_TTL_MS,
        )
        .expect("verify token before expiry");

        // The prepared proof carries no clock: expiry was the token's
        // concern, checked exactly once by the verification above.
        assert!(prepared
            .into_admission()
            .admits(catalog.content_store_id(), &content));
    }

    #[test]
    fn token_rejects_wrong_secret_namespace_store_content_and_expiry() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let other_namespace = NamespaceId::parse("other").expect("namespace");
        let other_store = "cs_00000000000000000000000000000002";
        let content = ContentRef::blob_v1(ContentId::generate(), b"hello");
        let other_content = ContentRef::blob_v1(ContentId::generate(), b"other");
        let issued_at_ms = 1_000;
        let token = mint_content_token(
            "secret",
            &receipt(&namespace, CONTENT_STORE, &content),
            issued_at_ms,
        )
        .expect("mint");
        let token = ValidatedContentToken {
            content_ref: content.clone(),
            token,
        };
        let catalog = catalog_entry(namespace.clone(), CONTENT_STORE);
        let other_catalog = catalog_entry(other_namespace, CONTENT_STORE);

        assert!(verify_content_token("other", &catalog, &token, 1_000).is_err());
        assert_eq!(
            verify_content_token("secret", &other_catalog, &token, 1_000),
            Err(ContentTokenError::NamespaceMismatch),
            "sharing a content store must not share token authorization"
        );
        assert_eq!(
            verify_content_token(
                "secret",
                &catalog_entry(namespace, other_store),
                &token,
                1_000
            ),
            Err(ContentTokenError::ContentStoreMismatch),
            "a receipt names the store its content is durable in"
        );
        assert!(verify_content_token(
            "secret",
            &catalog,
            &ValidatedContentToken {
                content_ref: other_content,
                token: token.token.clone(),
            },
            1_000,
        )
        .is_err());
        assert!(verify_content_token(
            "secret",
            &catalog,
            &token,
            issued_at_ms + CONTENT_RECEIPT_TTL_MS + 1
        )
        .is_err());
    }
}
