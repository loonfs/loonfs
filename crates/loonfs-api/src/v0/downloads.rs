//! Download requests and responses for direct object-store reads in the v0 HTTP API.

use super::ObjectTransferAccess;
use crate::{AbsolutePath, ContentRef, InodeId, NamespaceId, RevisionNo};
use serde::{Deserialize, Serialize};

/// The path to download and, optionally, the revision to download.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct BeginDownloadRequest {
    /// Absolute path of the file to read.
    pub path: AbsolutePath,
    /// Revision to read, or `None` for the path's current revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub revision_no: Option<RevisionNo>,
}

impl BeginDownloadRequest {
    /// Asks for the path's current revision.
    pub fn for_path(path: AbsolutePath) -> Self {
        Self {
            path,
            revision_no: None,
        }
    }

    /// Asks for one prior revision of the path.
    pub fn for_revision(path: AbsolutePath, revision_no: RevisionNo) -> Self {
        Self {
            path,
            revision_no: Some(revision_no),
        }
    }
}

/// A presigned URL for one content object.
///
/// The URL expires at `access.expires_at_ms`; later path changes do not change the object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginDownloadResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    pub path: AbsolutePath,
    /// Revision the capability reads, resolved from the request.
    pub revision_no: RevisionNo,
    /// The identity, byte length, and checksum of the object to download.
    pub content_ref: ContentRef,
    /// Short-lived read capability the client uses without learning the raw object key.
    pub access: ObjectTransferAccess,
}

/// Empty request for an inode-addressed download.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct BeginDownloadByInodeRequest {}

/// A short-lived capability to read one inode revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BeginDownloadByInodeResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// File inode being read.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// Revision being read.
    pub revision_no: RevisionNo,
    /// Content identity, size, and checksum.
    pub content_ref: ContentRef,
    /// Short-lived provider access without the raw object key.
    pub access: ObjectTransferAccess,
}

#[cfg(test)]
mod tests {
    use super::{
        BeginDownloadByInodeRequest, BeginDownloadByInodeResponse, BeginDownloadRequest,
        BeginDownloadResponse,
    };
    use crate::v0::ObjectTransferAccess;
    use crate::{AbsolutePath, ContentId, ContentRef, NamespaceId, RevisionNo};
    use std::collections::BTreeMap;

    fn absolute_path() -> AbsolutePath {
        AbsolutePath::parse("/docs/report.txt").expect("absolute path")
    }

    fn content_ref() -> ContentRef {
        ContentRef::blob_v1(
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"hello",
        )
    }

    fn content_ref_json() -> serde_json::Value {
        serde_json::json!({
            "kind": "blob_v1",
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "checksum": {
                "algorithm": "sha256",
                "value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            }
        })
    }

    #[test]
    fn a_download_request_names_only_a_path_and_a_revision() {
        let request: BeginDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt"}"#).expect("decode request");
        assert_eq!(request.path, absolute_path());
        assert_eq!(request.revision_no, None);

        let pinned: BeginDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt","revision_no":3}"#)
                .expect("decode pinned request");
        assert_eq!(pinned.revision_no, Some(RevisionNo(3)));

        assert!(
            serde_json::from_str::<BeginDownloadRequest>(
                r#"{"path":"/docs/report.txt","content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    #[test]
    fn a_download_grant_exposes_only_presigned_access() {
        let response = BeginDownloadResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            path: absolute_path(),
            revision_no: RevisionNo(7),
            content_ref: content_ref(),
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                headers: BTreeMap::new(),
                expires_at_ms: 1,
            },
        };

        assert_eq!(
            serde_json::to_value(&response).expect("serialize response"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/docs/report.txt",
                "revision_no": 7,
                "content_ref": content_ref_json(),
                "access": {
                    "kind": "presigned_url",
                    "method": "GET",
                    "url": "https://bucket.example/object?X-Amz-Signature=abc",
                    "expires_at_ms": 1
                }
            })
        );
    }

    #[test]
    fn an_inode_download_request_is_strictly_empty_and_its_grant_is_path_free() {
        let request: BeginDownloadByInodeRequest =
            serde_json::from_str("{}").expect("decode empty request");
        assert_eq!(request, BeginDownloadByInodeRequest {});
        assert!(serde_json::from_str::<BeginDownloadByInodeRequest>(r#"{"path":"/old"}"#).is_err());

        let response = BeginDownloadByInodeResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            inode_id: crate::InodeId(42),
            revision_no: RevisionNo(7),
            content_ref: content_ref(),
            access: ObjectTransferAccess::PresignedUrl {
                method: "GET".to_owned(),
                url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                headers: BTreeMap::new(),
                expires_at_ms: 1,
            },
        };
        assert_eq!(
            serde_json::to_value(&response).expect("serialize response"),
            serde_json::json!({
                "namespace_id": "demo",
                "inode_id": "ino_42",
                "revision_no": 7,
                "content_ref": content_ref_json(),
                "access": {
                    "kind": "presigned_url",
                    "method": "GET",
                    "url": "https://bucket.example/object?X-Amz-Signature=abc",
                    "expires_at_ms": 1
                }
            })
        );
    }
}
