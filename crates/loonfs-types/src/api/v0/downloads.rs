//! Download requests and responses for direct object-store reads in the v0 HTTP API.

use super::ObjectTransferAccess;
use crate::{AbsolutePath, ContentRef, InodeId, NamespaceId, PinId, RevisionNo};
use serde::{Deserialize, Serialize};

/// The path to download and, optionally, the revision to download.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateDownloadRequest {
    /// Absolute path of the file to read.
    pub path: AbsolutePath,
    /// Revision to read. Omit it for the path's current revision.
    /// Cannot be combined with `snapshot_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub revision_no: Option<RevisionNo>,
    /// Read the file revision captured by this snapshot.
    /// Cannot be combined with `revision_no`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
    /// The first byte the grant reads. It names `[start_offset, size_bytes)`
    /// of the revision; a client that resumes asks for a new grant from the
    /// bytes it holds. Must be below the revision's size, except 0 for a
    /// revision of zero bytes.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub start_offset: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// Signed object ranges for one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateDownloadResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// Absolute path as rendered from stored display names.
    #[serde(deserialize_with = "crate::path::deserialize_derived_path")]
    pub path: AbsolutePath,
    /// Revision the capability reads, resolved from the request.
    pub revision_no: RevisionNo,
    /// The identity, byte length, and checksum of the revision's bytes.
    pub content_ref: ContentRef,
    /// The revision's bytes from the requested offset, in order.
    pub ranges: Vec<DownloadRange>,
}

/// A short-lived capability to read one inode revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateDownloadByInodeResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// File inode being read.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// Revision being read.
    pub revision_no: RevisionNo,
    /// Content identity, size, and checksum.
    pub content_ref: ContentRef,
    /// The revision's bytes from the requested offset, in order.
    pub ranges: Vec<DownloadRange>,
}

/// One contiguous run of a revision's bytes, read from one object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DownloadRange {
    /// Offset of the run's first byte in the revision.
    pub start_offset: u64,
    /// Bytes in the run.
    pub length: u64,
    /// Short-lived read capability for exactly those bytes.
    pub access: ObjectTransferAccess,
}

#[cfg(test)]
mod tests {
    use super::{CreateDownloadByInodeResponse, CreateDownloadRequest, CreateDownloadResponse};
    use crate::api::v0::{DownloadRange, ObjectTransferAccess};
    use crate::{AbsolutePath, ContentId, ContentRef, NamespaceId, PinId, RevisionNo};
    use std::collections::BTreeMap;

    fn absolute_path() -> AbsolutePath {
        AbsolutePath::parse("/docs/report.txt").expect("absolute path")
    }

    fn content_ref() -> ContentRef {
        ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::parse("con_0123456789abcdef0123456789abcdef").expect("content id"),
            b"hello",
        )
    }

    fn content_ref_json() -> serde_json::Value {
        serde_json::json!({
            "kind": "blob_v1",
            "owner_namespace_id": "demo",
            "content_id": "con_0123456789abcdef0123456789abcdef",
            "size_bytes": 5,
            "checksum": {
                "algorithm": "sha256",
                "value": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            }
        })
    }

    #[test]
    fn a_create_download_request_decodes_revision_and_snapshot_selectors() {
        let request: CreateDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt"}"#).expect("decode request");
        assert_eq!(request.path, absolute_path());
        assert_eq!(request.revision_no, None);

        let pinned: CreateDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt","revision_no":3}"#)
                .expect("decode pinned request");
        assert_eq!(pinned.revision_no, Some(RevisionNo(3)));

        let snapshot_id =
            PinId::parse("pin_00000000000000000001-0000000000000002").expect("snapshot id");
        for revision_no in [None, Some(RevisionNo(3))] {
            let request: CreateDownloadRequest = serde_json::from_value(serde_json::json!({
                "path": "/docs/report.txt",
                "revision_no": revision_no,
                "snapshot_id": snapshot_id,
            }))
            .expect("decode snapshot request");
            assert_eq!(request.snapshot_id, Some(snapshot_id.clone()));
            assert_eq!(request.revision_no, revision_no);
        }
        assert!(serde_json::from_str::<CreateDownloadRequest>(
            r#"{"path":"/docs/report.txt","snapshot_id":"invalid"}"#,
        )
        .is_err());

        assert_eq!(request.start_offset, 0);
        let resumed: CreateDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt","start_offset":10}"#)
                .expect("decode resumed request");
        assert_eq!(resumed.start_offset, 10);

        assert!(
            serde_json::from_str::<CreateDownloadRequest>(
                r#"{"path":"/docs/report.txt","content_id":"con_0123456789abcdef0123456789abcdef"}"#
            )
            .is_err(),
            "a client must not be able to name the content object"
        );
    }

    #[test]
    fn a_download_grant_exposes_only_presigned_access() {
        let response = CreateDownloadResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            path: absolute_path(),
            revision_no: RevisionNo(7),
            content_ref: content_ref(),
            ranges: vec![DownloadRange {
                start_offset: 0,
                length: 5,
                access: ObjectTransferAccess::PresignedUrl {
                    method: "GET".to_owned(),
                    url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                    headers: BTreeMap::new(),
                    expires_at_ms: 1,
                },
            }],
        };

        assert_eq!(
            serde_json::to_value(&response).expect("serialize response"),
            serde_json::json!({
                "namespace_id": "demo",
                "path": "/docs/report.txt",
                "revision_no": 7,
                "content_ref": content_ref_json(),
                "ranges": [{
                    "start_offset": 0,
                    "length": 5,
                    "access": {
                        "kind": "presigned_url",
                        "method": "GET",
                        "url": "https://bucket.example/object?X-Amz-Signature=abc",
                        "expires_at_ms": 1
                    }
                }]
            })
        );
    }

    #[test]
    fn an_inode_download_grant_is_path_free() {
        let response = CreateDownloadByInodeResponse {
            namespace_id: NamespaceId::parse("demo").expect("namespace id"),
            inode_id: crate::InodeId(42),
            revision_no: RevisionNo(7),
            content_ref: content_ref(),
            ranges: vec![DownloadRange {
                start_offset: 0,
                length: 5,
                access: ObjectTransferAccess::PresignedUrl {
                    method: "GET".to_owned(),
                    url: "https://bucket.example/object?X-Amz-Signature=abc".to_owned(),
                    headers: BTreeMap::new(),
                    expires_at_ms: 1,
                },
            }],
        };
        assert_eq!(
            serde_json::to_value(&response).expect("serialize response"),
            serde_json::json!({
                "namespace_id": "demo",
                "inode_id": "ino_42",
                "revision_no": 7,
                "content_ref": content_ref_json(),
                "ranges": [{
                    "start_offset": 0,
                    "length": 5,
                    "access": {
                        "kind": "presigned_url",
                        "method": "GET",
                        "url": "https://bucket.example/object?X-Amz-Signature=abc",
                        "expires_at_ms": 1
                    }
                }]
            })
        );
    }
}
