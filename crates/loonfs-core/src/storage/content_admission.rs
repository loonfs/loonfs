//! Content preparation proofs and short-lived tokens used by producers.
//!
//! A token can be created only from a durable, completed upload session. It
//! proves that the named content was verified before publication. Namespace,
//! content reference, and token expiry are preserved when converting it to
//! [`PreparedContent`] and checked again when a publish batch admits the
//! proof.

use crate::limits::{COMPLETED_UPLOAD_RECEIPT_WINDOW_MS, CONTENT_RECEIPT_TTL_MS};
use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
use crate::storage::inline_content::InlineContent;
use loonfs_types::api::v0::ContentToken;
use loonfs_types::format::hex::{hex_decode_bytes, hex_encode_bytes};
use loonfs_types::{
    decode_token, encode_token, ChecksumAlgorithm, ContentId, ContentRef, NamespaceId, OpaqueToken,
    OpaqueTokenError, UploadId,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const CONTENT_TOKEN_FORMAT_VERSION: u8 = 1;

/// Evidence read from a durable upload session in its completed state.
///
/// The type has no public constructor. Only the upload protocol can create
/// it after loading a completed session, so callers cannot create evidence
/// from an in-memory expectation or an unverified provider response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedUploadEvidence {
    content_ref: ContentRef,
    completed_at_ms: u64,
}

impl CompletedUploadEvidence {
    pub(crate) fn for_completed_session(content_ref: ContentRef, completed_at_ms: u64) -> Self {
        Self {
            content_ref,
            completed_at_ms,
        }
    }

    /// Returns the completed session's verified content reference.
    pub fn content_ref(&self) -> &ContentRef {
        &self.content_ref
    }
}

/// Opaque evidence that a content reference was prepared for publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedContent {
    kind: PreparedContentKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PreparedContentKind {
    Inline(InlineContent),
    Staged {
        content_ref: ContentRef,
        expires_at_ms: u64,
        upload_id: Option<UploadId>,
    },
}

impl PreparedContent {
    /// Prepares bytes without store access or an expiry.
    pub fn inline(
        namespace_id: NamespaceId,
        bytes: bytes::Bytes,
        algorithm: ChecksumAlgorithm,
    ) -> Self {
        Self {
            kind: PreparedContentKind::Inline(InlineContent::new(
                namespace_id,
                ContentId::generate(),
                bytes,
                algorithm,
            )),
        }
    }

    /// Returns the bytes needed to retain an inline request for replay.
    pub fn inline_content(&self) -> Option<&InlineContent> {
        match &self.kind {
            PreparedContentKind::Inline(value) => Some(value),
            PreparedContentKind::Staged { .. } => None,
        }
    }

    pub(crate) fn estimated_payload_bytes(&self) -> usize {
        match &self.kind {
            PreparedContentKind::Inline(value) => value.bytes().len(),
            PreparedContentKind::Staged { content_ref, .. } => content_ref
                .owner_namespace_id
                .as_str()
                .len()
                .saturating_add(content_ref.content_id.as_str().len())
                .saturating_add(content_ref.checksum.value.as_str().len()),
        }
    }

    pub(crate) fn estimated_owned_staging_payload_bytes(
        namespace_id: &NamespaceId,
        value: &InlineContent,
    ) -> usize {
        let content_ref = value.content_ref();
        namespace_id
            .as_str()
            .len()
            .saturating_add(content_ref.content_id.as_str().len())
            .saturating_add(content_ref.checksum.value.as_str().len())
    }

    pub(crate) fn content_id(&self) -> &ContentId {
        &self.content_ref().content_id
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn for_durable_content_write(content_ref: ContentRef) -> Self {
        Self::for_completed_upload(content_ref, u64::MAX, None)
    }

    pub(crate) fn for_completed_upload(
        content_ref: ContentRef,
        expires_at_ms: u64,
        upload_id: Option<UploadId>,
    ) -> Self {
        Self {
            kind: PreparedContentKind::Staged {
                content_ref,
                expires_at_ms,
                upload_id,
            },
        }
    }

    /// Identifies the completed upload that can restore a staged preparation.
    pub fn upload_id(&self) -> Option<&UploadId> {
        match &self.kind {
            PreparedContentKind::Staged { upload_id, .. } => upload_id.as_ref(),
            PreparedContentKind::Inline(_) => None,
        }
    }

    pub(crate) fn admits(
        &self,
        namespace_id: &NamespaceId,
        content_ref: &ContentRef,
        now_ms: u64,
    ) -> bool {
        match &self.kind {
            PreparedContentKind::Inline(_) => false,
            PreparedContentKind::Staged {
                content_ref: expected_content_ref,
                expires_at_ms,
                ..
            } => {
                &expected_content_ref.owner_namespace_id == namespace_id
                    && expected_content_ref == content_ref
                    && now_ms < *expires_at_ms
            }
        }
    }

    /// Returns the reference fixed during preparation.
    pub fn content_ref(&self) -> &ContentRef {
        match &self.kind {
            PreparedContentKind::Inline(value) => value.content_ref(),
            PreparedContentKind::Staged { content_ref, .. } => content_ref,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ContentTokenPayload {
    content_ref: ContentRef,
    expires_at_ms: u64,
}

impl OpaqueToken for ContentTokenPayload {
    const KIND: &'static str = "content_token";
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
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

/// Creates a short-lived token for a completed upload.
///
/// The completed session remains durable, so a caller can obtain a new token
/// by reading upload status. Losing a response therefore requires another
/// status request, not another upload.
pub fn mint_content_token(
    secret: &str,
    evidence: &CompletedUploadEvidence,
    now_ms: u64,
) -> Result<ContentToken, ContentTokenError> {
    let issuance_deadline_ms = evidence
        .completed_at_ms
        .checked_add(COMPLETED_UPLOAD_RECEIPT_WINDOW_MS)
        .ok_or(ContentTokenError::TimeOverflow)?;
    if now_ms >= issuance_deadline_ms {
        return Err(ContentTokenError::Expired);
    }
    let expires_at_ms = now_ms
        .checked_add(CONTENT_RECEIPT_TTL_MS)
        .ok_or(ContentTokenError::TimeOverflow)?;
    let payload = ContentTokenPayload {
        content_ref: evidence.content_ref.clone(),
        expires_at_ms,
    };
    let payload_part = encode_token(&payload, CONTENT_TOKEN_FORMAT_VERSION)
        .map_err(|error| ContentTokenError::Codec(error.to_string()))?;
    let signature_part = hex_encode_bytes(&loonfs_objectstore::crypto::hmac_sha256(
        secret.as_bytes(),
        payload_part.as_bytes(),
    ));
    Ok(ContentToken {
        content_ref: evidence.content_ref.clone(),
        token: format!("{payload_part}.{signature_part}"),
    })
}

pub fn verify_content_token(
    secret: &str,
    catalog: &VerifiedNamespaceCatalogEntry,
    token: &ContentToken,
    now_ms: u64,
) -> Result<PreparedContent, ContentTokenError> {
    let (payload_part, signature_part) = token
        .token
        .split_once('.')
        .ok_or(ContentTokenError::Malformed)?;
    let actual_signature =
        hex_decode_bytes(signature_part).map_err(|_| ContentTokenError::Malformed)?;
    let expected_signature =
        loonfs_objectstore::crypto::hmac_sha256(secret.as_bytes(), payload_part.as_bytes());
    if !loonfs_objectstore::crypto::constant_time_eq(&actual_signature, &expected_signature) {
        return Err(ContentTokenError::BadSignature);
    }

    let payload: ContentTokenPayload = decode_token(payload_part, CONTENT_TOKEN_FORMAT_VERSION)
        .map_err(|error| match error {
            OpaqueTokenError::InvalidJson(message) => ContentTokenError::Codec(message),
            _ => ContentTokenError::Malformed,
        })?;
    if payload.content_ref.owner_namespace_id != *catalog.namespace_id() {
        return Err(ContentTokenError::NamespaceMismatch);
    }
    if payload.content_ref != token.content_ref {
        return Err(ContentTokenError::ContentRefMismatch);
    }
    if now_ms >= payload.expires_at_ms {
        return Err(ContentTokenError::Expired);
    }

    Ok(PreparedContent::for_completed_upload(
        payload.content_ref,
        payload.expires_at_ms,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::{mint_content_token, verify_content_token, CompletedUploadEvidence};
    use crate::namespace::catalog::VerifiedNamespaceCatalogEntry;
    use crate::namespace::state::NamespaceReadState;
    use loonfs_types::api::v0::ContentToken;
    use loonfs_types::{ContentId, ContentRef, NamespaceId};

    fn catalog_entry(namespace_id: NamespaceId) -> VerifiedNamespaceCatalogEntry {
        VerifiedNamespaceCatalogEntry::from_head(&NamespaceReadState::initial(
            namespace_id,
            1_000,
            loonfs_test_support::test_actor(),
        ))
    }

    fn evidence(content_ref: &ContentRef) -> CompletedUploadEvidence {
        CompletedUploadEvidence::for_completed_session(content_ref.clone(), 1_000)
    }

    #[test]
    fn minted_content_token_passes_unchanged_into_embedded_verification() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let value = InlineContent::new(
            namespace.clone(),
            ContentId::generate(),
            bytes::Bytes::from_static(b"hello"),
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let content = value.content_ref().clone();
        let evidence = CompletedUploadEvidence::for_completed_session(content.clone(), 1_000);
        let token = mint_content_token("secret", &evidence, 1_000).expect("mint");
        let catalog = catalog_entry(namespace);

        let prepared =
            verify_content_token("secret", &catalog, &token, 1_000).expect("verify token");

        assert_eq!(prepared.content_ref(), &content);
        assert!(prepared.admits(catalog.namespace_id(), &content, 1_000,));
    }

    #[test]
    fn prepared_admission_remains_bound_to_its_namespace() {
        let namespace = NamespaceId::parse("source").expect("namespace");
        let other_namespace = NamespaceId::parse("target").expect("namespace");
        let content = ContentRef::blob_v1(
            namespace.clone(),
            ContentId::generate(),
            b"hello",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let token = mint_content_token("secret", &evidence(&content), 1_000).expect("mint");
        let catalog = catalog_entry(namespace);
        let admission =
            verify_content_token("secret", &catalog, &token, 1_000).expect("verify token");

        assert!(!admission.admits(&other_namespace, &content, 1_000,));
    }

    fn fixed_content(namespace: &NamespaceId) -> ContentRef {
        ContentRef::blob_v1(
            namespace.clone(),
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"hello",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        )
    }

    #[test]
    fn a_content_token_matches_its_golden_bytes() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = fixed_content(&namespace);
        let token = mint_content_token("golden-secret", &evidence(&content), 1_000).expect("mint");

        crate::test_support::assert_matches_golden_token("content_token.txt", &token.token);
        let catalog = catalog_entry(namespace);
        let prepared = verify_content_token("golden-secret", &catalog, &token, 1_000)
            .expect("the golden token verifies");
        assert_eq!(prepared.content_ref(), &content);
    }

    #[test]
    fn a_content_token_in_the_earlier_base64_format_is_rejected() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let token = ContentToken {
            content_ref: fixed_content(&namespace),
            token: "eyJ2ZXJzaW9uIjoidmN0NCIsImNvbnRlbnRfcmVmIjp7ImtpbmQiOiJibG9iX3YxIiwib3duZXJfbmFtZXNwYWNlX2lkIjoiZGVtbyIsImNvbnRlbnRfaWQiOiJjb25fMDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWYiLCJzaXplX2J5dGVzIjo1LCJjaGVja3N1bSI6eyJhbGdvcml0aG0iOiJjcmM2NG52bWUiLCJ2YWx1ZSI6IjMzNzc4NTcwMDY1MjQyNTcifX0sImV4cGlyZXNfYXRfbXMiOjM2MDEwMDB9.FiiwJZ3mu8b7T5ops2TFaNSTunQRlkiQrRdSgUvxn-w".to_owned(),
        };
        let catalog = catalog_entry(namespace);
        assert_eq!(
            verify_content_token("secret", &catalog, &token, 1_000),
            Err(ContentTokenError::Malformed)
        );
    }

    #[test]
    fn a_signed_token_of_another_kind_is_rejected() {
        #[derive(Serialize, Deserialize)]
        struct OtherTokenPayload {
            content_ref: ContentRef,
            expires_at_ms: u64,
        }
        impl OpaqueToken for OtherTokenPayload {
            const KIND: &'static str = "other_token";
        }

        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = fixed_content(&namespace);
        let payload = OtherTokenPayload {
            content_ref: content.clone(),
            expires_at_ms: 2_000,
        };
        let payload_part = encode_token(&payload, CONTENT_TOKEN_FORMAT_VERSION).expect("encode");
        let signature_part = hex_encode_bytes(&loonfs_objectstore::crypto::hmac_sha256(
            b"secret",
            payload_part.as_bytes(),
        ));
        let token = ContentToken {
            content_ref: content,
            token: format!("{payload_part}.{signature_part}"),
        };
        let catalog = catalog_entry(namespace);
        assert_eq!(
            verify_content_token("secret", &catalog, &token, 1_000),
            Err(ContentTokenError::Malformed)
        );
    }

    #[test]
    fn tokens_and_proofs_are_valid_before_their_expiry_and_expired_at_it() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let content = ContentRef::blob_v1(
            namespace.clone(),
            ContentId::generate(),
            b"hello",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let issued_at_ms = 1_000;
        let expires_at_ms = issued_at_ms + CONTENT_RECEIPT_TTL_MS;
        let token = mint_content_token("secret", &evidence(&content), issued_at_ms).expect("mint");
        let catalog = catalog_entry(namespace);

        let prepared = verify_content_token("secret", &catalog, &token, expires_at_ms - 1)
            .expect("the token is valid one millisecond before its expiry");
        assert!(prepared.admits(catalog.namespace_id(), &content, expires_at_ms - 1));
        assert!(!prepared.admits(catalog.namespace_id(), &content, expires_at_ms));
        assert_eq!(
            verify_content_token("secret", &catalog, &token, expires_at_ms),
            Err(ContentTokenError::Expired)
        );
    }

    #[test]
    fn token_rejects_wrong_secret_namespace_content_and_expiry() {
        let namespace = NamespaceId::parse("demo").expect("namespace");
        let other_namespace = NamespaceId::parse("other").expect("namespace");
        let content = ContentRef::blob_v1(
            namespace.clone(),
            ContentId::generate(),
            b"hello",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let other_content = ContentRef::blob_v1(
            namespace.clone(),
            ContentId::generate(),
            b"other",
            loonfs_types::ChecksumAlgorithm::Crc64nvme,
        );
        let issued_at_ms = 1_000;
        let token = mint_content_token("secret", &evidence(&content), issued_at_ms).expect("mint");
        let catalog = catalog_entry(namespace.clone());
        let other_catalog = catalog_entry(other_namespace);

        assert!(verify_content_token("other", &catalog, &token, 1_000).is_err());
        assert_eq!(
            verify_content_token("secret", &other_catalog, &token, 1_000),
            Err(ContentTokenError::NamespaceMismatch),
            "evidence belongs to one namespace"
        );
        assert!(verify_content_token(
            "secret",
            &catalog,
            &ContentToken {
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
