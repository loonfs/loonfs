//! Upload requests and responses for the v0 HTTP API.

use crate::{Checksum, ChecksumAlgorithm, ContentRef, NamespaceId, UploadId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The size and checksum of a complete direct upload payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadContentClaim {
    /// The size of the complete payload in bytes.
    pub size_bytes: u64,
    /// The checksum of the whole payload, in the algorithm that this operation requires.
    pub checksum: Checksum,
}

/// The transport that an upload uses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum UploadMode {
    /// A transport in which the service receives the bytes and writes the content to object
    /// storage.
    #[default]
    ServiceProxied,
    /// A transport in which the service creates a short-lived presigned PUT URL for the content
    /// object.
    DirectPut,
    /// A transport in which the client uploads parts directly to object storage.
    DirectMultipart,
}

impl UploadMode {
    /// Returns the serialized value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ServiceProxied => "service_proxied",
            Self::DirectPut => "direct_put",
            Self::DirectMultipart => "direct_multipart",
        }
    }
}

/// The transport for a new upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CreateUploadBody {
    // Empty braces make serde reject fields from another transport. A unit
    // variant would silently ignore them.
    /// A transport in which the client sends the bytes to the service, which writes the content
    /// object.
    #[cfg_attr(feature = "openapi", schema(title = "CreateUploadBodyServiceProxied"))]
    ServiceProxied {},
    /// A transport in which the client writes the whole object through one presigned request.
    #[cfg_attr(feature = "openapi", schema(title = "CreateUploadBodyDirectPut"))]
    DirectPut {
        /// An advisory byte length, which the server uses to check the provider's size limit early.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        size_bytes: Option<u64>,
    },
    /// A transport in which the client writes the object in parts through presigned part uploads.
    #[cfg_attr(feature = "openapi", schema(title = "CreateUploadBodyDirectMultipart"))]
    DirectMultipart {
        /// The byte length of every part except the last. Omit it to use the server default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        part_size_bytes: Option<u64>,
    },
}

impl CreateUploadBody {
    /// Returns the transport this request asks for.
    pub fn mode(&self) -> UploadMode {
        match self {
            Self::ServiceProxied {} => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::DirectMultipart { .. } => UploadMode::DirectMultipart,
        }
    }
}

/// A capability that lets a client transfer bytes directly to or from object storage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObjectTransferAccess {
    /// A short-lived URL, with the headers that one object store read or write requires.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "ObjectTransferAccessPresignedUrl")
    )]
    PresignedUrl {
        /// The HTTP method that the client must use.
        method: String,
        /// The full presigned URL.
        url: String,
        /// The headers that the signature covers, which the client must send.
        headers: BTreeMap<String, String>,
        /// The time the URL expires, in Unix milliseconds.
        expires_at_ms: u64,
    },
}

/// One upload part number and its checksum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct UploadPartChecksumClaim {
    /// The part number, counting from 1. The number can't exceed the provider's limit of 10,000
    /// parts.
    pub part_number: u32,
    /// The checksum of this part's bytes.
    pub checksum: Checksum,
}

/// A request for part upload capabilities on an open multipart session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct SignUploadPartsRequest {
    /// The parts to authorize. A repeated part number replaces the earlier upload of that part.
    pub parts: Vec<UploadPartChecksumClaim>,
}

/// One authorized part upload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SignedUploadPart {
    /// The part number that this capability writes.
    pub part_number: u32,
    /// A short-lived write capability for this part.
    pub access: ObjectTransferAccess,
}

/// One capability for each requested part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SignUploadPartsResponse {
    /// The namespace that the upload session belongs to.
    pub namespace_id: NamespaceId,
    /// The session that the parts belong to.
    pub upload_id: UploadId,
    /// The capabilities, in the order that the request asked for them.
    pub parts: Vec<SignedUploadPart>,
}

/// One uploaded part that the object store provider accepted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CompletedUploadPart {
    /// The part number, counting from 1.
    pub part_number: u32,
    /// The entity tag that the provider returned for the accepted part.
    pub etag: String,
    /// The checksum that the part was signed and accepted with.
    pub checksum: Checksum,
}

/// A signed token that shows a `content_ref` was verified in a completed upload, so a later
/// commit can use that reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct ContentToken {
    /// The content that this token authorizes.
    pub content_ref: ContentRef,
    /// The opaque token that the server signed. Clients must not parse it.
    pub token: String,
}

/// A request to complete an upload, in the mode that started it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CompleteUploadBody {
    /// The completion of a service-proxied upload.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CompleteUploadBodyServiceProxied")
    )]
    ServiceProxied {},
    /// The completion of a direct PUT upload.
    #[cfg_attr(feature = "openapi", schema(title = "CompleteUploadBodyDirectPut"))]
    DirectPut {
        /// The expected length and checksum of the stored object.
        content: UploadContentClaim,
    },
    /// The completion of a direct multipart upload.
    #[cfg_attr(
        feature = "openapi",
        schema(title = "CompleteUploadBodyDirectMultipart")
    )]
    DirectMultipart {
        /// The expected length and checksum of the assembled object.
        content: UploadContentClaim,
        /// The uploaded parts, in ascending part order.
        parts: Vec<CompletedUploadPart>,
    },
}

impl CompleteUploadBody {
    /// Returns the upload mode in this request.
    pub const fn mode(&self) -> UploadMode {
        match self {
            Self::ServiceProxied {} => UploadMode::ServiceProxied,
            Self::DirectPut { .. } => UploadMode::DirectPut,
            Self::DirectMultipart { .. } => UploadMode::DirectMultipart,
        }
    }
}

/// The current state of an upload session.
///
/// A session starts as `Open` and ends as `Completed` or `Aborted`. Neither end state can change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum UploadSessionStatus {
    /// A session that accepts content until its lease ends.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusOpen"))]
    Open {
        /// The time after which cleanup can abort the session, in Unix milliseconds.
        expires_at_ms: u64,
        /// The checksum algorithm that the store uses for content.
        checksum_algorithm: ChecksumAlgorithm,
        /// The size of every part except the last, in bytes. It's present for `direct_multipart`
        /// sessions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        part_size_bytes: Option<u64>,
        /// The capability for the direct PUT. It's present for `direct_put` sessions, and the
        /// server creates a new one on every read.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        access: Option<ObjectTransferAccess>,
        /// The staged content. It's present after content is staged in a `service_proxied` session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_ref: Option<ContentRef>,
    },
    /// A session that ended with durable, verified content.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusCompleted"))]
    Completed {
        /// The time the session completed, in Unix milliseconds.
        completed_at_ms: u64,
        /// The verified content that the session completed with.
        content_ref: ContentRef,
        /// A new content token, which a later commit can use to reference the content. It's absent
        /// after the window for issuing tokens closes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "openapi", schema(nullable = false))]
        content_token: Option<ContentToken>,
    },
    /// A session that was aborted before it completed. Its object no longer exists.
    #[cfg_attr(feature = "openapi", schema(title = "UploadSessionStatusAborted"))]
    Aborted {
        /// The time the session was aborted, in Unix milliseconds.
        aborted_at_ms: u64,
    },
    /// A status from a newer server. This variant doesn't include the status's own fields.
    #[serde(other)]
    Unknown,
}

/// The current state of one upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadSession {
    /// The namespace that the session belongs to.
    pub namespace_id: NamespaceId,
    /// The upload session ID.
    pub upload_id: UploadId,
    /// The transport that was selected when the session began.
    pub mode: UploadMode,
    /// The session's lifecycle state and the fields specific to that state.
    #[serde(flatten)]
    pub status: UploadSessionStatus,
}

impl UploadSession {
    /// Returns the staged or completed content reference, when present.
    pub const fn content_ref(&self) -> Option<&ContentRef> {
        match &self.status {
            UploadSessionStatus::Completed { content_ref, .. } => Some(content_ref),
            UploadSessionStatus::Open { content_ref, .. } => content_ref.as_ref(),
            UploadSessionStatus::Aborted { .. } | UploadSessionStatus::Unknown => None,
        }
    }

    /// Returns the completed session's current content token, when present.
    pub const fn content_token(&self) -> Option<&ContentToken> {
        match &self.status {
            UploadSessionStatus::Completed { content_token, .. } => content_token.as_ref(),
            UploadSessionStatus::Open { .. }
            | UploadSessionStatus::Aborted { .. }
            | UploadSessionStatus::Unknown => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompleteUploadBody, ContentToken, CreateUploadBody, ObjectTransferAccess,
        UploadContentClaim, UploadMode, UploadSession, UploadSessionStatus,
    };
    use crate::{Checksum, ChecksumAlgorithm, ContentId, ContentRef, NamespaceId, UploadId};
    use std::collections::BTreeMap;

    #[test]
    fn an_upload_status_from_a_newer_server_decodes_as_unknown() {
        let session: UploadSession = serde_json::from_value(serde_json::json!({
            "namespace_id": "demo",
            "upload_id": "upl_00000000000000000000000000000001",
            "mode": "service_proxied",
            "status": "expiring",
            "expiring_at_ms": 7
        }))
        .expect("a newer status decodes");
        assert_eq!(session.status, UploadSessionStatus::Unknown);
        assert_eq!(session.content_ref(), None);
    }

    #[test]
    fn a_create_upload_body_without_a_mode_does_not_decode() {
        serde_json::from_str::<CreateUploadBody>("{}")
            .expect_err("a body without a mode should be rejected");
        assert_eq!(
            serde_json::from_str::<CreateUploadBody>(r#"{"mode":"service_proxied"}"#)
                .expect("decode proxied begin request"),
            CreateUploadBody::ServiceProxied {}
        );
    }

    #[test]
    fn a_create_upload_body_carrying_another_modes_fields_does_not_decode() {
        for body in [
            r#"{"mode":"service_proxied","part_size_bytes":8388608}"#,
            r#"{"mode":"service_proxied","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"3377857006524257"}}}"#,
            r#"{"mode":"direct_multipart","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"3377857006524257"}}}"#,
            r#"{"mode":"direct_put","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"3377857006524257"}}}"#,
            r#"{"mode":"direct_put","part_size_bytes":8388608}"#,
            r#"{"mode":"direct_multipart","size_bytes":5}"#,
        ] {
            serde_json::from_str::<CreateUploadBody>(body)
                .expect_err(&format!("decoded a begin request that mixes modes: {body}"));
        }
    }

    #[test]
    fn a_multipart_begin_names_its_part_size_beside_the_mode() {
        assert_eq!(
            serde_json::from_str::<CreateUploadBody>(
                r#"{"mode":"direct_multipart","part_size_bytes":8388608}"#
            )
            .expect("decode multipart begin request"),
            CreateUploadBody::DirectMultipart {
                part_size_bytes: Some(8 * 1024 * 1024),
            }
        );
        assert_eq!(
            serde_json::from_str::<CreateUploadBody>(r#"{"mode":"direct_multipart"}"#)
                .expect("decode multipart begin without a part size"),
            CreateUploadBody::DirectMultipart {
                part_size_bytes: None,
            }
        );
        assert_eq!(
            serde_json::to_value(CreateUploadBody::DirectMultipart {
                part_size_bytes: None,
            })
            .expect("serialize multipart begin request"),
            serde_json::json!({ "mode": "direct_multipart" })
        );
    }

    #[test]
    fn completion_requests_are_tagged_and_mode_specific() {
        assert_eq!(
            serde_json::from_str::<CompleteUploadBody>(r#"{"mode":"service_proxied"}"#)
                .expect("decode proxied completion"),
            CompleteUploadBody::ServiceProxied {}
        );
        let direct_put = CompleteUploadBody::DirectPut {
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
            serde_json::from_str::<CompleteUploadBody>(body)
                .expect_err(&format!("decoded an invalid completion request: {body}"));
        }

        let missing_parts = r#"{"mode":"direct_multipart","content":{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"0123456789abcdef"}}}"#;
        let error = serde_json::from_str::<CompleteUploadBody>(missing_parts)
            .expect_err("multipart parts are required");
        assert!(
            error.to_string().contains("parts"),
            "the rejection should name the missing field: {error}"
        );

        let multipart = CompleteUploadBody::DirectMultipart {
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
    fn open_sessions_name_the_store_algorithm_and_their_modes_fields() {
        let content_ref = ContentRef::blob_v1(
            NamespaceId::parse("demo").expect("namespace id"),
            ContentId::generate(),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        );
        let access = ObjectTransferAccess::PresignedUrl {
            method: "PUT".to_owned(),
            url: "https://bucket.example/object".to_owned(),
            headers: BTreeMap::new(),
            expires_at_ms: 1,
        };
        for (mode, checksum_algorithm, part_size_bytes, access, content_ref, fields) in [
            (
                UploadMode::DirectPut,
                ChecksumAlgorithm::Crc64nvme,
                None,
                Some(access.clone()),
                None,
                serde_json::json!({"checksum_algorithm": "crc64nvme", "access": access}),
            ),
            (
                UploadMode::DirectMultipart,
                ChecksumAlgorithm::Crc64nvme,
                Some(8388608),
                None,
                None,
                serde_json::json!({"checksum_algorithm": "crc64nvme", "part_size_bytes": 8388608}),
            ),
            (
                UploadMode::ServiceProxied,
                ChecksumAlgorithm::Crc64nvme,
                None,
                None,
                Some(content_ref.clone()),
                serde_json::json!({"checksum_algorithm": "crc64nvme", "content_ref": content_ref}),
            ),
            (
                UploadMode::ServiceProxied,
                ChecksumAlgorithm::Crc64nvme,
                None,
                None,
                None,
                serde_json::json!({"checksum_algorithm": "crc64nvme"}),
            ),
        ] {
            let session = UploadSession {
                namespace_id: NamespaceId::parse("demo").expect("namespace id"),
                upload_id: UploadId::parse("upl_00000000000000000000000000000001")
                    .expect("upload id"),
                mode,
                status: UploadSessionStatus::Open {
                    expires_at_ms: 1000,
                    checksum_algorithm,
                    part_size_bytes,
                    access,
                    content_ref,
                },
            };
            let mut expected = serde_json::json!({
                "namespace_id": "demo",
                "upload_id": "upl_00000000000000000000000000000001",
                "mode": mode,
                "status": "open",
                "expires_at_ms": 1000,
            });
            expected
                .as_object_mut()
                .expect("session object")
                .extend(fields.as_object().expect("mode fields").clone());
            assert_eq!(
                serde_json::to_value(&session).expect("serialize session"),
                expected
            );
            assert_eq!(
                serde_json::from_value::<UploadSession>(expected).expect("decode session"),
                session
            );
        }
    }

    #[test]
    fn an_upload_content_claim_names_only_size_and_checksum() {
        let request: CreateUploadBody =
            serde_json::from_str(r#"{"mode":"direct_put","size_bytes":5}"#)
                .expect("decode direct-put begin request");
        assert_eq!(
            request,
            CreateUploadBody::DirectPut {
                size_bytes: Some(5),
            }
        );
        assert_eq!(
            serde_json::from_str::<CreateUploadBody>(r#"{"mode":"direct_put"}"#)
                .expect("decode direct-put begin without a size"),
            CreateUploadBody::DirectPut { size_bytes: None }
        );

        serde_json::from_str::<UploadContentClaim>(
                r#"{"size_bytes":5,"checksum":{"algorithm":"crc64nvme","value":"3377857006524257"},"content_id":"con_0123456789abcdef0123456789abcdef"}"#
            ).expect_err("a client must not be able to name the content object");
    }

    #[test]
    fn an_upload_session_is_flat_and_uses_one_status_vocabulary() {
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let upload_id = UploadId::parse("upl_00000000000000000000000000000001").expect("upload id");
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
                content_ref: ContentRef::blob_v1(
                    crate::NamespaceId::parse("demo").expect("namespace id"),
                    ContentId::generate(),
                    b"hello",
                    crate::ChecksumAlgorithm::Crc64nvme,
                ),
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
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
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
                    "owner_namespace_id": "demo",
                    "content_id": "con_0123456789abcdef0123456789abcdef",
                    "size_bytes": 5,
                    "checksum": {
                        "algorithm": "crc64nvme",
                        "value": "3377857006524257"
                    }
                },
                "token": "opaque-server-token"
            })
        );

        let request: crate::api::v0::CommitRequest = serde_json::from_value(serde_json::json!({
            "commit_id": "same-token-shape",
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
}
