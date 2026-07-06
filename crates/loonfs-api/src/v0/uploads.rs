//! Upload-session shapes for the v0 HTTP API: transport modes, session
//! begin/append/complete requests and responses, and the direct-put
//! presigned-access envelope. Content moves through these shapes; the
//! metadata that later references it commits through [`super::commits`].

use crate::{ContentRef, NamespaceId, UploadId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
}

impl UploadMode {
    pub fn is_service_proxied(&self) -> bool {
        matches!(self, Self::ServiceProxied)
    }
}

/// Request for starting an upload session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginUploadRequest {
    /// Requested upload transport. Absent keeps the existing service-proxied path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<UploadMode>,
    /// Required for `direct_put`; the server signs exactly this content object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<ContentRef>,
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
    pub content_ref: ContentRef,
    pub access: ObjectTransferAccess,
}

/// Stateless proof that a LoonFS server already validated a content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ValidatedContentToken {
    pub content_ref: ContentRef,
    /// Opaque, server-signed token. Clients must not parse it.
    pub token: String,
}

/// Response for starting an upload session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub mode: UploadMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_put: Option<DirectPutUpload>,
}

/// Response after uploading bytes into a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct UploadContentResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub content_ref: ContentRef,
}

/// Request to complete an upload with the expected content ref.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadRequest {
    pub content_ref: ContentRef,
}

/// Response after an upload session is completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CompleteUploadResponse {
    pub namespace_id: NamespaceId,
    pub upload_id: UploadId,
    pub content_ref: ContentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_content_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{
        BeginUploadRequest, BeginUploadResponse, DirectPutUpload, ObjectTransferAccess, UploadMode,
    };
    use crate::{ContentRef, NamespaceId, UploadId};
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
                content_ref: ContentRef::whole_file_v0(b"hello"),
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
        };

        let json = serde_json::to_string(&response).expect("serialize response");
        assert!(json.contains(r#""kind":"presigned_url""#));
        assert!(!json.contains("object_key"));
    }
}
