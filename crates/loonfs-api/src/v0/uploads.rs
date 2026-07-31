//! Upload-session shapes for the v0 HTTP API: transport modes, session
//! begin/append/complete requests and responses, and the direct-put
//! presigned-access envelope. Content moves through these shapes; the
//! metadata that later references it commits through [`super::commits`].

use crate::{ContentRef, NamespaceId, UploadId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What a `direct_put` client promises about bytes it has not written yet.
///
/// The server mints the content object's identity — a client cannot name a
/// key it has not been given — so a direct upload declares only what it can
/// know about its own bytes. The server signs both into the provider write
/// and verifies them again at completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DirectPutContentClaim {
    /// Complete byte length the client will write.
    pub size_bytes: u64,
    /// SHA-256 over the complete payload, lowercase hex.
    pub sha256: String,
}

/// What a `direct_multipart` client promises about an object it will write
/// in pieces.
///
/// The digest is CRC-64/NVME rather than SHA-256 because that is the
/// checksum an S3-compatible provider computes over a multipart object: it
/// is the only full-object evidence the provider will ever be able to show
/// back, so it is the only thing worth claiming up front.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DirectMultipartContentClaim {
    /// Complete byte length the client will write across every part.
    pub size_bytes: u64,
    /// CRC-64/NVME over the complete assembled payload, lowercase hex.
    pub crc64nvme: String,
}

/// Upload transport mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    /// The service receives bytes and writes content to object storage.
    #[default]
    ServiceProxied,
    /// The service mints a short-lived presigned PUT URL for the content object.
    DirectPut,
    /// The service opens a provider multipart upload for the content object
    /// and signs one PUT per part, so a large object crosses the network
    /// once, in parallel, without passing through the server.
    DirectMultipart,
}

impl UploadMode {
    /// Reports whether the service, rather than the client, transfers bytes to object storage.
    pub fn is_service_proxied(&self) -> bool {
        matches!(self, Self::ServiceProxied)
    }
}

/// Request for starting an upload session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct BeginUploadRequest {
    /// Requested upload transport. Absent keeps the existing service-proxied path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<UploadMode>,
    /// Required for `direct_put`; the server signs exactly these bytes into
    /// the write it authorizes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<DirectPutContentClaim>,
    /// Required for `direct_multipart`; the server opens the provider upload
    /// for exactly this object and verifies it against this claim at
    /// completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart: Option<DirectMultipartContentClaim>,
}

/// Client-facing direct transfer capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectTransferAccess {
    /// Short-lived URL plus required headers for one object-store write.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ObjectTransferAccessPresignedUrl")
    )]
    PresignedUrl {
        /// HTTP method the client must use.
        method: String,
        /// Full presigned URL.
        url: String,
        /// Headers that are covered by the signature and must be sent.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
        /// Expiration timestamp in Unix milliseconds.
        expires_at_ms: u64,
    },
}

/// Presigned direct_put upload details. The raw object key is intentionally not public.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectPutUpload {
    /// Immutable object identity the server minted, plus the byte length and
    /// checksum covered by the signed request. Completion and the later
    /// commit both name exactly this reference.
    pub content_ref: ContentRef,
    /// Short-lived write capability the client uses without learning the raw object key.
    pub access: ObjectTransferAccess,
}

/// Direct multipart upload details: the object identity, and the geometry a
/// client needs to cut its payload into parts.
///
/// The provider's upload id is deliberately absent. A client asks this
/// server for part URLs by part number; it never talks to the provider's
/// multipart API in its own words, so it needs no provider vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectMultipartUpload {
    /// Immutable object identity the server minted, with the CRC-64/NVME and
    /// byte length completion will verify against. Completion and the later
    /// commit both name exactly this reference.
    pub content_ref: ContentRef,
    /// Byte length of every part except the last.
    pub part_size_bytes: u64,
    /// How many parts the declared size cuts into at `part_size_bytes`.
    pub part_count: u32,
}

/// One part's checksum, supplied by the client so the server can sign it
/// into that part's upload URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadPartChecksumClaim {
    /// One-based part number, at most the session's `part_count`.
    pub part_number: u32,
    /// CRC-64/NVME over this part's bytes, lowercase hex.
    pub crc64nvme: String,
}

/// Request for part-upload capabilities on an open multipart session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SignUploadPartsRequest {
    /// Parts to authorize, each with the checksum the provider will enforce
    /// on it. Asking again for a part already uploaded is how a client
    /// retries one: a repeated part is last-write-wins at the provider.
    pub parts: Vec<UploadPartChecksumClaim>,
}

/// One authorized part upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SignedUploadPart {
    /// Part number this capability writes.
    pub part_number: u32,
    /// Short-lived write capability for that part.
    pub access: ObjectTransferAccess,
}

/// Response carrying one capability per requested part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SignUploadPartsResponse {
    /// Namespace that owns the upload session.
    pub namespace_id: NamespaceId,
    /// Session the parts belong to.
    pub upload_id: UploadId,
    /// Capabilities in the order the request asked for them.
    pub parts: Vec<SignedUploadPart>,
}

/// One uploaded part, as the client observed the provider accept it.
///
/// The server keeps no durable record of any part. Part bookkeeping is the
/// client's, exactly as it is in the provider's own multipart API, and this
/// is where the client hands it back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CompletedUploadPart {
    /// One-based part number.
    pub part_number: u32,
    /// Entity tag the provider returned for the accepted part.
    pub etag: String,
    /// CRC-64/NVME the part was signed and accepted with, lowercase hex.
    pub crc64nvme: String,
}

/// Stateless proof that a LoonFS server already validated a content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ValidatedContentToken {
    /// Content identity the server attests it already verified.
    pub content_ref: ContentRef,
    /// Opaque, server-signed token. Clients must not parse it.
    pub token: String,
}

/// Response for starting an upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginUploadResponse {
    /// Namespace authorized to consume the eventual staged content.
    pub namespace_id: NamespaceId,
    /// Durable session identity used by subsequent append and completion calls.
    pub upload_id: UploadId,
    /// Transport selected after applying server capability and request validation.
    pub mode: UploadMode,
    /// Presigned write details for `DirectPut`, or `None` for `ServiceProxied`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put: Option<DirectPutUpload>,
    /// Object identity and part geometry for `DirectMultipart`, or `None` for
    /// every other mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_multipart: Option<DirectMultipartUpload>,
}

/// Response after uploading bytes into a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadContentResponse {
    /// Namespace that owns the upload session.
    pub namespace_id: NamespaceId,
    /// Session into which the service staged these bytes.
    pub upload_id: UploadId,
    /// Digest and byte length computed from the accepted body.
    pub content_ref: ContentRef,
}

/// Request to complete an upload with the expected content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CompleteUploadRequest {
    /// Content identity the caller expects the session to have staged.
    pub content_ref: ContentRef,
    /// Required for `direct_multipart`: every part the client uploaded, in
    /// ascending part order. The server holds no part records of its own, so
    /// this list is what it assembles the object from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_parts: Option<Vec<CompletedUploadPart>>,
}

/// Response after an upload session is completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadResponse {
    /// Namespace that owns the completed session.
    pub namespace_id: NamespaceId,
    /// Session whose result is now frozen for idempotent completion retries.
    pub upload_id: UploadId,
    /// Verified immutable content selected by the completed session.
    pub content_ref: ContentRef,
    /// Opaque server proof for a later commit, or `None` when the backend needs no token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_content_token: Option<String>,
}

/// Observed state of an upload session.
///
/// A session is `open`, then `completed` or `aborted`, and both of those are
/// final. Reading a completed session mints a fresh receipt for content that
/// is already durable, which is why losing a commit response never costs a
/// retransfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum UploadSessionStatus {
    /// Accepting content until its lease passes.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusOpen"))]
    Open {
        /// Unix-millisecond instant after which the session is abandoned and
        /// may be aborted by server-side cleanup.
        expires_at_ms: u64,
    },
    /// Final: the content is durable and verified.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusCompleted"))]
    Completed {
        /// Unix-millisecond stamp of the completion.
        completed_at_ms: u64,
        /// Verified immutable content this session settled on.
        content_ref: ContentRef,
        /// Freshly minted proof for a following commit, or `None` once the
        /// session has stopped minting them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        validated_content_token: Option<String>,
    },
    /// Final: the session selected no content and its object is gone.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusAborted"))]
    Aborted {
        /// Unix-millisecond stamp of the abort.
        aborted_at_ms: u64,
    },
}

/// Response for reading one upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadStatusResponse {
    /// Namespace that owns the session.
    pub namespace_id: NamespaceId,
    /// Session that was read.
    pub upload_id: UploadId,
    /// The session's state, with a fresh receipt when it is completed.
    pub status: UploadSessionStatus,
}

/// Response after aborting an upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AbortUploadResponse {
    /// Namespace that owns the session.
    pub namespace_id: NamespaceId,
    /// Session that is now final.
    pub upload_id: UploadId,
    /// Unix-millisecond stamp of the abort that stands, which for a repeated
    /// abort is the first one's.
    pub aborted_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::{
        BeginUploadRequest, BeginUploadResponse, DirectPutContentClaim, DirectPutUpload,
        ObjectTransferAccess, UploadMode, UploadSessionStatus,
    };
    use crate::{ContentId, ContentRef, NamespaceId, UploadId};
    use std::collections::BTreeMap;

    #[test]
    fn direct_put_upload_mode_serializes_as_expected() {
        assert_eq!(
            serde_json::to_string(&UploadMode::DirectPut).expect("serialize mode"),
            r#""direct_put""#
        );
    }

    #[test]
    fn begin_upload_request_keeps_service_proxied_as_default() {
        let request: BeginUploadRequest = serde_json::from_str("{}").expect("decode request");
        assert_eq!(request.mode.unwrap_or_default(), UploadMode::ServiceProxied);
    }

    #[test]
    fn direct_put_response_exposes_only_presigned_access() {
        let response = BeginUploadResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: UploadId::parse("upl_00000000000000000000000000000001")
                .expect("valid upload id"),
            mode: UploadMode::DirectPut,
            direct_put: Some(DirectPutUpload {
                content_ref: ContentRef::blob_v1(ContentId::generate(), b"hello"),
                access: ObjectTransferAccess::PresignedUrl {
                    method: "PUT".to_owned(),
                    url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                    headers: BTreeMap::from([
                        ("if-none-match".to_owned(), "*".to_owned()),
                        (
                            "x-provider-checksum".to_owned(),
                            "LPJNul+wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ=".to_owned(),
                        ),
                    ]),
                    expires_at_ms: 1,
                },
            }),
            direct_multipart: None,
        };

        let json = serde_json::to_string(&response).expect("serialize response");
        assert!(json.contains(r#""kind":"presigned_url""#));
        assert!(!json.contains("object_key"));
    }

    /// A direct-put client declares what it is about to write; it cannot
    /// declare *where*, because the server owns content identity.
    #[test]
    fn a_direct_put_claim_names_only_size_and_digest() {
        let request: BeginUploadRequest = serde_json::from_str(
            r#"{"mode":"direct_put","content":{"size_bytes":5,"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}"#,
        )
        .expect("decode direct-put begin request");
        let claim = request.content.expect("claim");
        assert_eq!(claim.size_bytes, 5);
        assert_eq!(claim.sha256.len(), 64);

        assert!(
            serde_json::from_str::<DirectPutContentClaim>(
                r#"{"size_bytes":5,"sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    #[test]
    fn upload_status_names_its_state_on_the_wire() {
        let open = serde_json::to_value(UploadSessionStatus::Open {
            expires_at_ms: 1_000,
        })
        .expect("serialize open status");
        assert_eq!(open["state"], "open");

        let aborted = serde_json::to_value(UploadSessionStatus::Aborted {
            aborted_at_ms: 2_000,
        })
        .expect("serialize aborted status");
        assert_eq!(aborted["state"], "aborted");

        let completed = serde_json::to_value(UploadSessionStatus::Completed {
            completed_at_ms: 3_000,
            content_ref: ContentRef::blob_v1(ContentId::generate(), b"hello"),
            validated_content_token: None,
        })
        .expect("serialize completed status");
        assert_eq!(completed["state"], "completed");
        assert!(
            completed.get("validated_content_token").is_none(),
            "a session past its receipt window reports no token at all"
        );
    }
}
