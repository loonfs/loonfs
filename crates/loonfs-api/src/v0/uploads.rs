//! Upload requests and responses for the v0 HTTP API.

use crate::{Checksum, ChecksumAlgorithm, ContentRef, NamespaceId, UploadId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Size and checksum reported by the client for a complete payload.
///
/// Direct uploads provide this at completion. The server verifies it against
/// the object stored by the provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadContentClaim {
    /// Complete payload size in bytes.
    pub size_bytes: u64,
    /// Whole-payload checksum in the algorithm required by this operation.
    pub checksum: Checksum,
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
    /// The client uploads parts directly to object storage.
    DirectMultipart,
}

impl UploadMode {
    /// Returns the mode's wire spelling, the one `rename_all` above serializes.
    /// Messages that quote a mode back to a client read it from here so they
    /// cannot name a spelling the API no longer accepts.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ServiceProxied => "service_proxied",
            Self::DirectPut => "direct_put",
            Self::DirectMultipart => "direct_multipart",
        }
    }
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
    /// Write the whole object through one presigned request.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadDirectPut"))]
    DirectPut {
        /// Advisory byte length for an early provider-limit check.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        size_bytes: Option<u64>,
    },
    /// Write the object in parts through presigned part uploads.
    #[cfg_attr(feature = "openapi", schema(title = "BeginUploadDirectMultipart"))]
    DirectMultipart {
        /// Byte length of every part except the last. The server uses its
        /// default when this is omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        part_size_bytes: Option<u64>,
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
    /// Parts to authorize and the checksum for each part. Requesting a part
    /// again replaces the previous upload for that part number.
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
        /// Checksum algorithm the client must use for its completion claim.
        checksum_algorithm: ChecksumAlgorithm,
        /// Short-lived permission to write the object.
        access: ObjectTransferAccess,
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
        /// Byte length of every part except the last. At most 10,000 parts
        /// may be uploaded, so this bounds the object at 10,000 times the
        /// part size.
        part_size_bytes: u64,
        /// Checksum algorithm for every part and for the complete payload.
        checksum_algorithm: ChecksumAlgorithm,
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

/// Request to complete an upload session.
///
/// `mode` must match the mode used to start the session. Direct uploads
/// include the expected content details. Multipart also includes its parts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompleteUploadRequest {
    /// Complete a service-proxied upload.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadServiceProxied"))]
    ServiceProxied {},
    /// Complete a direct-PUT upload.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadDirectPut"))]
    DirectPut {
        /// Expected length and checksum of the stored object.
        content: UploadContentClaim,
    },
    /// Complete a direct multipart upload.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadDirectMultipart"))]
    DirectMultipart {
        /// Expected length and checksum of the assembled object.
        content: UploadContentClaim,
        /// Uploaded parts in ascending part order.
        parts: Vec<CompletedUploadPart>,
    },
}

impl CompleteUploadRequest {
    /// Returns the upload mode in this request.
    pub const fn mode(&self) -> UploadMode {
        match self {
            Self::ServiceProxied {} => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::DirectMultipart { .. } => UploadMode::DirectMultipart,
        }
    }
}

/// Information required to complete a `direct_multipart` upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CompleteMultipartUploadRequest {
    /// Expected length and checksum of the assembled object.
    pub content: UploadContentClaim,
    /// Uploaded parts in ascending part order.
    pub parts: Vec<CompletedUploadPart>,
}

/// Observed state of an upload session.
///
/// A session starts as `Open` and ends as either `Completed` or `Aborted`.
/// Both final states are permanent. Reading a completed session issues a new
/// receipt for the durable content, so a lost commit response does not require
/// the content to be uploaded again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
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
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_token: Option<ContentToken>,
    },
    /// Final: the session selected no content and its object is gone.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusAborted"))]
    Aborted {
        /// Unix-millisecond stamp of the abort.
        aborted_at_ms: u64,
    },
}

/// Current view of one upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadSession {
    /// Namespace that owns the session.
    pub namespace_id: NamespaceId,
    /// Session represented by this view.
    pub upload_id: UploadId,
    /// Transport selected when the session began.
    pub mode: UploadMode,
    /// The session's lifecycle and state-specific fields. Completed HTTP
    /// responses carry a fresh receipt while the minting window remains open.
    #[serde(flatten)]
    pub status: UploadSessionStatus,
}

impl UploadSession {
    /// Returns the completed content reference, or `None` before completion.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match &self.status {
            UploadSessionStatus::Completed { content_ref, .. } => Some(content_ref),
            UploadSessionStatus::Open { .. } | UploadSessionStatus::Aborted { .. } => None,
        }
    }

    /// Returns the completed session's current content token, when present.
    pub const fn content_token(&self) -> Option<&ContentToken> {
        match &self.status {
            UploadSessionStatus::Completed { content_token, .. } => content_token.as_ref(),
            UploadSessionStatus::Open { .. } | UploadSessionStatus::Aborted { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, ContentToken,
        ObjectTransferAccess, UploadContentClaim, UploadMode, UploadSession, UploadSessionStatus,
    };
    use crate::{Checksum, ChecksumAlgorithm, ContentId, ContentRef, NamespaceId, UploadId};
    use std::collections::BTreeMap;

    #[test]
    fn a_begin_request_without_a_mode_does_not_decode() {
        assert!(serde_json::from_str::<BeginUploadRequest>("{}").is_err());
        assert_eq!(
            serde_json::from_str::<BeginUploadRequest>(r#"{"mode":"service_proxied"}"#)
                .expect("decode proxied begin request"),
            BeginUploadRequest::ServiceProxied {}
        );
    }

    #[test]
    fn a_begin_request_carrying_another_modes_fields_does_not_decode() {
        for body in [
            r#"{"mode":"service_proxied","part_size_bytes":8388608}"#,
            r#"{"mode":"service_proxied","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            r#"{"mode":"direct_multipart","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            r#"{"mode":"direct_put","content":{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}}"#,
            r#"{"mode":"direct_put","part_size_bytes":8388608}"#,
            r#"{"mode":"direct_multipart","size_bytes":5}"#,
        ] {
            assert!(
                serde_json::from_str::<BeginUploadRequest>(body).is_err(),
                "decoded a begin request that mixes modes: {body}"
            );
        }
    }

    #[test]
    fn a_multipart_begin_names_its_part_size_beside_the_mode() {
        assert_eq!(
            serde_json::from_str::<BeginUploadRequest>(
                r#"{"mode":"direct_multipart","part_size_bytes":8388608}"#
            )
            .expect("decode multipart begin request"),
            BeginUploadRequest::DirectMultipart {
                part_size_bytes: Some(8 * 1024 * 1024),
            }
        );
        assert_eq!(
            serde_json::from_str::<BeginUploadRequest>(r#"{"mode":"direct_multipart"}"#)
                .expect("decode multipart begin without a part size"),
            BeginUploadRequest::DirectMultipart {
                part_size_bytes: None,
            }
        );
        assert_eq!(
            serde_json::to_value(BeginUploadRequest::DirectMultipart {
                part_size_bytes: None,
            })
            .expect("serialize multipart begin request"),
            serde_json::json!({ "mode": "direct_multipart" })
        );
    }

    #[test]
    fn completion_requests_are_tagged_and_mode_specific() {
        assert_eq!(
            serde_json::from_str::<CompleteUploadRequest>(r#"{"mode":"service_proxied"}"#)
                .expect("decode proxied completion"),
            CompleteUploadRequest::ServiceProxied {}
        );
        let direct_put = CompleteUploadRequest::DirectPut {
            content: UploadContentClaim {
                size_bytes: 5,
                checksum: Checksum::crc32c(b"hello"),
            },
        };
        assert_eq!(
            serde_json::to_value(&direct_put).expect("encode direct-put completion"),
            serde_json::json!({
                "mode": "direct_put",
                "content": {
                    "size_bytes": 5,
                    "checksum": Checksum::crc32c(b"hello"),
                },
            })
        );
        for body in [
            r#"{}"#,
            r#"{"mode":"service_proxied","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}},"parts":[]}"#,
            r#"{"mode":"direct_put"}"#,
            r#"{"mode":"direct_multipart"}"#,
        ] {
            assert!(
                serde_json::from_str::<CompleteUploadRequest>(body).is_err(),
                "decoded an invalid completion request: {body}"
            );
        }

        let missing_parts = r#"{"mode":"direct_multipart","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}}}"#;
        let error = serde_json::from_str::<CompleteUploadRequest>(missing_parts)
            .expect_err("multipart parts are required");
        assert!(
            error.to_string().contains("parts"),
            "the rejection should name the missing field: {error}"
        );

        let multipart = CompleteUploadRequest::DirectMultipart {
            content: UploadContentClaim {
                size_bytes: 5,
                checksum: Checksum::crc64nvme(b"hello"),
            },
            parts: Vec::new(),
        };
        let encoded = serde_json::to_string(&multipart).expect("encode multipart completion");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).expect("decode multipart JSON"),
            serde_json::json!({
                "mode": "direct_multipart",
                "content": {
                    "size_bytes": 5,
                    "checksum": Checksum::crc64nvme(b"hello"),
                },
                "parts": [],
            })
        );
    }

    #[test]
    fn a_begin_response_carries_only_its_transports_fields() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let upload_id =
            UploadId::parse("upl_00000000000000000000000000000001").expect("valid upload id");
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
                checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
                access: ObjectTransferAccess::PresignedUrl {
                    method: "PUT".to_owned(),
                    url: "https://bucket.example/object".to_owned(),
                    headers: BTreeMap::new(),
                    expires_at_ms: 1,
                },
            })
            .expect("serialize direct-put response"),
            serde_json::json!({
                "mode": "direct_put",
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "checksum_algorithm": "crc64nvme",
                "access": {
                    "kind": "presigned_url",
                    "method": "PUT",
                    "url": "https://bucket.example/object",
                    "expires_at_ms": 1
                }
            })
        );

        assert_eq!(
            serde_json::to_value(BeginUploadResponse::DirectMultipart {
                namespace_id,
                upload_id,
                part_size_bytes: 8 * 1024 * 1024,
                checksum_algorithm: ChecksumAlgorithm::Crc64nvme,
            })
            .expect("serialize multipart response"),
            serde_json::json!({
                "mode": "direct_multipart",
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "part_size_bytes": 8 * 1024 * 1024,
                "checksum_algorithm": "crc64nvme"
            })
        );
    }

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

    #[test]
    fn an_upload_content_claim_names_only_size_and_checksum() {
        let request: BeginUploadRequest =
            serde_json::from_str(r#"{"mode":"direct_put","size_bytes":5}"#)
                .expect("decode direct-put begin request");
        assert_eq!(
            request,
            BeginUploadRequest::DirectPut {
                size_bytes: Some(5),
            }
        );
        assert_eq!(
            serde_json::from_str::<BeginUploadRequest>(r#"{"mode":"direct_put"}"#)
                .expect("decode direct-put begin without a size"),
            BeginUploadRequest::DirectPut { size_bytes: None }
        );

        assert!(
            serde_json::from_str::<UploadContentClaim>(
                r#"{"size_bytes":5,"checksum":{"algorithm":"sha256","value":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},"content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    #[test]
    fn an_upload_session_is_flat_and_uses_one_status_vocabulary() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
        let open = serde_json::to_value(UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode: UploadMode::DirectMultipart,
            status: UploadSessionStatus::Open {
                expires_at_ms: 1_000,
            },
        })
        .expect("serialize open status");
        assert_eq!(
            open,
            serde_json::json!({
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "mode": "direct_multipart",
                "status": "open",
                "expires_at_ms": 1_000,
            })
        );

        let aborted = serde_json::to_value(UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id: upload_id.clone(),
            mode: UploadMode::ServiceProxied,
            status: UploadSessionStatus::Aborted {
                aborted_at_ms: 2_000,
            },
        })
        .expect("serialize aborted status");
        assert_eq!(aborted["status"], "aborted");
        assert_eq!(aborted["mode"], "service_proxied");
        assert_eq!(aborted["aborted_at_ms"], 2_000);
        assert!(aborted.get("state").is_none());

        let completed = serde_json::to_value(UploadSession {
            namespace_id,
            upload_id,
            mode: UploadMode::DirectPut,
            status: UploadSessionStatus::Completed {
                completed_at_ms: 3_000,
                content_ref: ContentRef::blob_v1(ContentId::generate(), b"hello"),
                content_token: None,
            },
        })
        .expect("serialize completed status");
        assert_eq!(completed["status"], "completed");
        assert_eq!(completed["mode"], "direct_put");
        assert!(completed.get("state").is_none());
        assert!(completed.get("status").is_some());
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
        let completion = serde_json::to_value(UploadSession {
            namespace_id: namespace_id.clone(),
            upload_id,
            mode: UploadMode::ServiceProxied,
            status: UploadSessionStatus::Completed {
                completed_at_ms: 3_000,
                content_ref: content_ref.clone(),
                content_token: Some(content_token.clone()),
            },
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
