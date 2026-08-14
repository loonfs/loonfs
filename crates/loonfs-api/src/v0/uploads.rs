//! Upload-session shapes for the v0 HTTP API: transport modes, session
//! begin/append/complete requests and responses, and the direct-put
//! presigned-access envelope. Content moves through these shapes; the
//! metadata that later references it commits through [`super::commits`].

use crate::{Checksum, ChecksumAlgorithm, ContentRef, NamespaceId, UploadId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What an upload client claims about a complete payload.
///
/// The server mints the content object's identity — a client cannot name a
/// key it has not been given — so a direct upload declares only what it can
/// know about its own bytes. Direct PUT binds the claim into the provider
/// write; multipart verifies it against the assembled object at completion.
///
/// The digest names its own algorithm because providers do not agree on one:
/// each binds into a presigned write whatever its API can enforce. The
/// deployment advertises which it is, and a claim in any other algorithm is
/// refused at begin rather than signed into a write the provider would
/// reject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadContentClaim {
    /// Complete byte length the client will write.
    pub size_bytes: u64,
    /// Whole-payload checksum in the algorithm required by this operation.
    pub checksum: Checksum,
}

/// What a `direct_multipart` client asks for when it opens a session.
///
/// A begin request declares no length and no digest: the session exists to
/// receive bytes whose length may not be known yet. All it settles is the
/// geometry the client cuts to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct DirectMultipartUploadOptions {
    /// Byte length of every part except the last, or `None` for the
    /// server's default.
    ///
    /// The value bounds the object: a provider accepts at most 10,000
    /// parts, so this session can carry at most `part_size_bytes × 10_000`
    /// bytes. A client that knows its payload is very large asks for a
    /// larger part size; one that does not know its length at all takes the
    /// default and keeps asking for part URLs until its stream ends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub part_size_bytes: Option<u64>,
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

/// Request to start an upload session, tagged by transport mode.
///
/// Each variant contains only fields valid for that transport, so invalid
/// combinations are rejected during decoding. The `mode` field is required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum BeginUploadRequest {
    // Empty braces make serde reject fields from another transport. A unit
    // variant would silently ignore them.
    /// Send the bytes to the service, which writes the content object.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadServiceProxied"))]
    ServiceProxied {},
    /// Write the whole object through one presigned request. The server
    /// signs exactly these bytes into the write it authorizes, so the claim
    /// is required.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadDirectPut"))]
    DirectPut {
        /// Byte length and digest of the payload about to be written.
        content: UploadContentClaim,
    },
    /// Write the object in parts through presigned part uploads.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadDirectMultipart"))]
    DirectMultipart {
        /// Selects the part geometry; absent takes the server's default.
        /// A multipart upload claims its content at completion, so nothing
        /// about the payload is declared here.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        multipart: Option<DirectMultipartUploadOptions>,
    },
}

impl BeginUploadRequest {
    /// The transport this request asks for.
    pub fn mode(&self) -> UploadMode {
        match self {
            Self::ServiceProxied {} => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::DirectMultipart { .. } => UploadMode::DirectMultipart,
        }
    }
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

/// Part geometry returned for a direct multipart upload.
///
/// The begin response does not include a content reference or part count
/// because the final payload is not known yet. The server owns the provider
/// upload id and returns the content reference only after completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DirectMultipartUpload {
    /// Byte length of every part except the last. At most 10,000 parts may
    /// be uploaded, so this bounds the object at `part_size_bytes × 10_000`.
    pub part_size_bytes: u64,
    /// Checksum algorithm every part and the complete assembled payload must
    /// use for this session.
    pub checksum_algorithm: ChecksumAlgorithm,
}

/// One part's checksum, supplied by the client so the server can sign it
/// into that part's upload URL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadPartChecksumClaim {
    /// One-based part number, at most the provider's 10,000-part limit.
    pub part_number: u32,
    /// Checksum over this part's bytes.
    pub checksum: Checksum,
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
    /// Checksum the part was signed and accepted with.
    pub checksum: Checksum,
}

/// Proof that a specific `content_ref` may be used in a later commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ContentToken {
    /// Content authorized by this token.
    pub content_ref: ContentRef,
    /// Opaque, server-signed token. Clients must not parse it.
    pub token: String,
}

/// Response to starting an upload session, tagged by transport mode.
///
/// Each variant contains only the fields needed by that transport. Unknown
/// response fields are accepted for forward compatibility.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum BeginUploadResponse {
    /// The service will receive the bytes and write the content object.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "BeginUploadResponseServiceProxied")
    )]
    ServiceProxied {
        /// Namespace authorized to consume the eventual staged content.
        namespace_id: NamespaceId,
        /// Durable session identity used by subsequent append and completion
        /// calls.
        upload_id: UploadId,
    },
    /// One presigned request writes the whole object.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadResponseDirectPut"))]
    DirectPut {
        /// Namespace authorized to consume the eventual staged content.
        namespace_id: NamespaceId,
        /// Durable session identity used by subsequent completion calls.
        upload_id: UploadId,
        /// The object this session writes, and the capability to write it.
        direct_put: DirectPutUpload,
    },
    /// Presigned part uploads assemble the object.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "BeginUploadResponseDirectMultipart")
    )]
    DirectMultipart {
        /// Namespace authorized to consume the eventual staged content.
        namespace_id: NamespaceId,
        /// Durable session identity used by subsequent part-signing and
        /// completion calls.
        upload_id: UploadId,
        /// The geometry the client cuts its payload to.
        direct_multipart: DirectMultipartUpload,
    },
}

impl BeginUploadResponse {
    /// Namespace that owns the session.
    pub fn namespace_id(&self) -> &NamespaceId {
        match self {
            Self::ServiceProxied { namespace_id, .. }
            | Self::DirectPut { namespace_id, .. }
            | Self::DirectMultipart { namespace_id, .. } => namespace_id,
        }
    }

    /// Session the later append, part, completion, and abort calls name.
    pub fn upload_id(&self) -> &UploadId {
        match self {
            Self::ServiceProxied { upload_id, .. }
            | Self::DirectPut { upload_id, .. }
            | Self::DirectMultipart { upload_id, .. } => upload_id,
        }
    }

    /// The transport this session was opened with.
    pub fn mode(&self) -> UploadMode {
        match self {
            Self::ServiceProxied { .. } => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::DirectMultipart { .. } => UploadMode::DirectMultipart,
        }
    }
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

/// Request to complete an upload.
///
/// A service-proxied or `direct_put` session receives its content reference
/// before the upload and returns that reference at completion. A
/// `direct_multipart` session instead provides the completed parts and a
/// claim describing the assembled object.
///
/// The variants share no fields, so fields from one completion type are
/// rejected when used with the other type. The server also checks that the
/// completion type matches the transport stored in the upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "completion", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompleteUploadRequest {
    /// Completes a session the server named a content object for: the
    /// caller names it back and the server proves the object matches.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadContentRef"))]
    ContentRef {
        /// Content identity the caller expects the session to have settled
        /// on.
        content_ref: ContentRef,
    },
    /// Completes a `direct_multipart` session with what it uploaded.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadMultipart"))]
    Multipart {
        /// The assembled object's length and checksum, which completion
        /// verifies against the provider's own reading of the object.
        content: UploadContentClaim,
        /// Every part the client uploaded, in ascending part order. The
        /// server holds no part records of its own, so this list is what it
        /// assembles the object from.
        parts: Vec<CompletedUploadPart>,
    },
}

impl CompleteUploadRequest {
    /// Completes a session that already knows its content reference.
    pub fn for_content_ref(content_ref: ContentRef) -> Self {
        Self::ContentRef { content_ref }
    }

    /// Completes a `direct_multipart` session with what it assembled.
    pub fn for_multipart(content: UploadContentClaim, parts: Vec<CompletedUploadPart>) -> Self {
        Self::Multipart { content, parts }
    }
}

/// Response after an upload session is completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadResponse {
    /// Namespace that owns the completed session.
    pub namespace_id: NamespaceId,
    /// Session whose result is now frozen for idempotent completion retries.
    pub upload_id: UploadId,
    /// Verified content selected by the completed session.
    pub content_ref: ContentRef,
    /// Short-lived proof for a later commit. This is absent after the token
    /// minting window closes, while `content_ref` remains available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_token: Option<ContentToken>,
}

/// Observed state of an upload session.
///
/// A session starts as `Open` and ends as either `Completed` or `Aborted`.
/// Both final states are permanent. Reading a completed session issues a new
/// receipt for the durable content, so a lost commit response does not require
/// the content to be uploaded again.
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
        /// Verified content selected by this session.
        content_ref: ContentRef,
        /// Fresh proof for a later commit. This is absent after the token
        /// minting window closes, while `content_ref` remains available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_token: Option<ContentToken>,
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
        BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
        ContentToken, DirectMultipartUpload, DirectPutUpload, ObjectTransferAccess,
        UploadContentClaim, UploadMode, UploadSessionStatus,
    };
    use crate::{Checksum, ChecksumAlgorithm, ContentId, ContentRef, NamespaceId, UploadId};
    use std::collections::BTreeMap;

    #[test]
    fn direct_put_upload_mode_serializes_as_expected() {
        assert_eq!(
            serde_json::to_string(&UploadMode::DirectPut).expect("serialize mode"),
            r#""direct_put""#
        );
    }

    /// A begin request says how it means to move its bytes, or it is not a
    /// request. Nothing is inferred from what the body left out.
    #[test]
    fn a_begin_request_without_a_mode_does_not_decode() {
        assert!(serde_json::from_str::<BeginUploadRequest>("{}").is_err());
        assert_eq!(
            serde_json::from_str::<BeginUploadRequest>(r#"{"mode":"service_proxied"}"#)
                .expect("decode proxied begin request"),
            BeginUploadRequest::ServiceProxied {}
        );
    }

    /// The combinations a flat begin request could spell are refused where
    /// the body is read, not by a handler comparing fields afterwards.
    #[test]
    fn a_begin_request_carrying_another_modes_fields_does_not_decode() {
        for body in [
            r#"{"mode":"service_proxied","multipart":{"part_size_bytes":8388608}}"#,
            r#"{"mode":"service_proxied","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            r#"{"mode":"direct_multipart","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            // A direct put with nothing to sign is not a direct put.
            r#"{"mode":"direct_put"}"#,
        ] {
            assert!(
                serde_json::from_str::<BeginUploadRequest>(body).is_err(),
                "decoded a begin request that mixes modes: {body}"
            );
        }
    }

    /// A completion carries one shape's fields under that shape's tag.
    #[test]
    fn a_completion_mixing_its_two_shapes_does_not_decode() {
        for body in [
            r#"{"completion":"multipart","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}},"parts":[],"content_ref":{"kind":"blob_v1","content_id":"con_0123456789abcdef0123456789abcdef","size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            // Neither multipart-completion field stands without the other.
            r#"{"completion":"multipart","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}}}"#,
            r#"{"completion":"multipart","parts":[]}"#,
            r#"{"completion":"content_ref"}"#,
        ] {
            assert!(
                serde_json::from_str::<CompleteUploadRequest>(body).is_err(),
                "decoded a completion that mixes shapes: {body}"
            );
        }
    }

    #[test]
    fn direct_put_response_exposes_only_presigned_access() {
        let response = BeginUploadResponse::DirectPut {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            upload_id: UploadId::parse("upl_00000000000000000000000000000001")
                .expect("valid upload id"),
            direct_put: DirectPutUpload {
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
            },
        };

        let json = serde_json::to_string(&response).expect("serialize response");
        assert!(json.contains(r#""kind":"presigned_url""#));
        assert!(!json.contains("object_key"));
    }

    /// The bytes each transport answers with, pinned. A response names its
    /// transport in `mode` and carries that transport's field and no other's,
    /// which is the same wire the flat shape spelled by convention.
    #[test]
    fn a_begin_response_carries_only_its_transports_field() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let upload_id =
            UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id");
        let sha256 = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

        assert_eq!(
            serde_json::to_value(BeginUploadResponse::ServiceProxied {
                namespace_id: namespace_id.clone(),
                upload_id: upload_id.clone(),
            })
            .expect("serialize proxied response"),
            serde_json::json!({
                "mode": "service_proxied",
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001"
            })
        );

        assert_eq!(
            serde_json::to_value(BeginUploadResponse::DirectPut {
                namespace_id: namespace_id.clone(),
                upload_id: upload_id.clone(),
                direct_put: DirectPutUpload {
                    content_ref: ContentRef::blob_v1(
                        ContentId::parse("con_0123456789abcdef0123456789abcdef")
                            .expect("content id"),
                        b"hello",
                    ),
                    access: ObjectTransferAccess::PresignedUrl {
                        method: "PUT".to_owned(),
                        url: "https://bucket.example/object".to_owned(),
                        headers: BTreeMap::new(),
                        expires_at_ms: 1,
                    },
                },
            })
            .expect("serialize direct-put response"),
            serde_json::json!({
                "mode": "direct_put",
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "direct_put": {
                    "content_ref": {
                        "kind": "blob_v1",
                        "content_id": "con_0123456789abcdef0123456789abcdef",
                        "size_bytes": 5,
                        "checksum": { "algorithm": "sha256", "value": sha256 }
                    },
                    "access": {
                        "kind": "presigned_url",
                        "method": "PUT",
                        "url": "https://bucket.example/object",
                        "expires_at_ms": 1
                    }
                }
            })
        );

        assert_eq!(
            serde_json::to_value(BeginUploadResponse::DirectMultipart {
                namespace_id,
                upload_id,
                direct_multipart: DirectMultipartUpload {
                    part_size_bytes: 8 * 1024 * 1024,
                    checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
                },
            })
            .expect("serialize multipart response"),
            serde_json::json!({
                "mode": "direct_multipart",
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "direct_multipart": {
                    "part_size_bytes": 8 * 1024 * 1024,
                    "checksum_algorithm": "crc64nvme"
                }
            })
        );
    }

    /// The request refuses fields it does not know; the response must not.
    /// A reader that rejected them could not talk to a later server.
    #[test]
    fn a_begin_response_carrying_a_later_servers_field_still_decodes() {
        assert_eq!(
            serde_json::from_str::<BeginUploadResponse>(
                r#"{"mode":"service_proxied","namespace_id":"demo","upload_id":"upl_00000000000000000000000000000001","invented_later":true}"#
            )
            .expect("decode a proxied response carrying an unknown field"),
            BeginUploadResponse::ServiceProxied {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                upload_id: UploadId::parse("upl_00000000000000000000000000000001")
                    .expect("valid upload id"),
            }
        );
    }

    /// A direct-put client declares what it is about to write; it cannot
    /// declare *where*, because the server owns content identity.
    #[test]
    fn an_upload_content_claim_names_only_size_and_checksum() {
        let request: BeginUploadRequest = serde_json::from_str(
            r#"{"mode":"direct_put","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
        )
        .expect("decode direct-put begin request");
        assert_eq!(
            request,
            BeginUploadRequest::DirectPut {
                content: UploadContentClaim {
                    size_bytes: 5,
                    checksum: Checksum {
                        algorithm: ChecksumAlgorithm::Sha256,
                        value: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                            .to_owned(),
                    },
                },
            }
        );

        assert!(
            serde_json::from_str::<UploadContentClaim>(
                r#"{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},"content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    /// The claim names its algorithm, so a provider that enforces something
    /// other than SHA-256 is expressible without a wire change.
    #[test]
    fn an_upload_content_claim_carries_the_operations_required_algorithm() {
        let claim: UploadContentClaim = serde_json::from_str(
            r#"{"size_bytes":5,"checksum":{"algorithm":"crc32c","value":"a1b2c3d4"}}"#,
        )
        .expect("decode a non-sha256 direct-put claim");
        assert_eq!(claim.checksum.algorithm, ChecksumAlgorithm::Crc32c);
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
            content_token: None,
        })
        .expect("serialize completed status");
        assert_eq!(completed["state"], "completed");
        assert!(
            completed.get("content_token").is_none(),
            "a session past its receipt window reports no token at all"
        );
    }

    #[test]
    fn completion_status_and_commit_share_the_exact_content_token_shape() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
        let content_ref = ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"hello",
        );
        let content_token = ContentToken {
            content_ref: content_ref.clone(),
            token: "opaque-server-token".to_owned(),
        };
        let completion = serde_json::to_value(CompleteUploadResponse {
            namespace_id: namespace_id.clone(),
            upload_id,
            content_ref: content_ref.clone(),
            content_token: Some(content_token.clone()),
        })
        .expect("serialize completion");
        let status = serde_json::to_value(UploadSessionStatus::Completed {
            completed_at_ms: 3_000,
            content_ref,
            content_token: Some(content_token),
        })
        .expect("serialize completed status");

        let completion_token = completion["content_token"].clone();
        let status_token = status["content_token"].clone();
        assert_eq!(completion_token, status_token);
        assert_eq!(
            completion_token,
            serde_json::json!({
                "content_ref": {
                    "kind": "blob_v1",
                    "content_id": "con_0123456789abcdef0123456789abcdef",
                    "size_bytes": 5,
                    "checksum": {
                        "algorithm": "sha256",
                        "value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                    }
                },
                "token": "opaque-server-token"
            })
        );

        let request: crate::v0::CommitRequest = serde_json::from_value(serde_json::json!({
            "commit_id": "same-token-shape",
            "actor": crate::ActorRef::loonfs_system(),
            "content_tokens": [completion_token],
            "operations": [{
                "kind": "create_directory",
                "path": "/proof",
                "parents": false
            }]
        }))
        .expect("completion token decodes unchanged in a commit request");
        assert_eq!(
            serde_json::to_value(&request.content_tokens[0]).expect("serialize commit token"),
            status_token
        );
    }

    #[test]
    fn a_content_token_rejects_unknown_fields() {
        let token = serde_json::json!({
            "content_ref": {
                "kind": "blob_v1",
                "content_id": "con_0123456789abcdef0123456789abcdef",
                "size_bytes": 5,
                "checksum": {
                    "algorithm": "sha256",
                    "value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                }
            },
            "token": "opaque-server-token",
            "expires_at_ms": 1
        });
        assert!(serde_json::from_value::<ContentToken>(token).is_err());
    }
}
