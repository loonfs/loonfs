//! Download requests and responses for direct object-store reads in the v0 HTTP API.

use super::ObjectTransferAccess;
use crate::options::ReadConsistency;
use crate::{AbsolutePath, ContentRef, InodeId, NamespaceId, PinId, RevisionNo};
use serde::{Deserialize, Serialize};

/// A request to download a file by path, and optionally a revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateDownloadRequest {
    /// The absolute path of the file to read.
    pub path: AbsolutePath,
    /// The revision to read. Omit it to read the path's current revision. It can't be combined with
    /// `snapshot_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub revision_no: Option<RevisionNo>,
    /// A snapshot to read the file from. The download reads the revision that the snapshot
    /// captured. It can't be combined with `revision_no`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read. It can't be combined with `snapshot_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub consistency: Option<ReadConsistency>,
    /// With `consistency` set to `eventual`, how old the server's last confirmation of the head can
    /// be, in milliseconds. The default is the server's `max_staleness_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub max_staleness_ms: Option<u64>,
    /// The first byte that the grant reads. The grant covers the revision's bytes from
    /// `start_offset` up to `size_bytes`, so a client that resumes a download asks for a new grant
    /// that starts after the bytes it already has. The offset must be less than the revision's
    /// size. For a revision of zero bytes, it must be 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub start_offset: u64,
}

/// A request to download a file inode's current revision.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateDownloadByInodeRequest {
    /// A snapshot to read the file from. The download reads the revision that the snapshot
    /// captured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub snapshot_id: Option<PinId>,
    /// How recently the read confirms the namespace head. `eventual` (the default, unless the
    /// server's `consistency` setting is `strong`) reads at the head that the server confirmed last
    /// while that confirmation is younger than the maximum staleness, and `strong` confirms the
    /// head before the read. It can't be combined with `snapshot_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub consistency: Option<ReadConsistency>,
    /// With `consistency` set to `eventual`, how old the server's last confirmation of the head can
    /// be, in milliseconds. The default is the server's `max_staleness_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub max_staleness_ms: Option<u64>,
    /// The first byte that the grant reads. It defaults to 0 and must be less than the file's size.
    /// For a file of zero bytes, it must be 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub start_offset: u64,
}

/// A request to download one retained revision of an inode.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(deny_unknown_fields)]
pub struct CreateRevisionDownloadByInodeRequest {
    /// How recently the read confirms the namespace head that it finds the revision in. `eventual`
    /// (the default, unless the server's `consistency` setting is `strong`) reads at the head that
    /// the server confirmed last while that confirmation is younger than the maximum staleness,
    /// and `strong` confirms the head before the read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub consistency: Option<ReadConsistency>,
    /// With `consistency` set to `eventual`, how old the server's last confirmation of the head can
    /// be, in milliseconds. The default is the server's `max_staleness_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "openapi", schema(nullable = false))]
    pub max_staleness_ms: Option<u64>,
    /// The first byte that the grant reads. It defaults to 0 and must be less than the revision's
    /// size. For a revision of zero bytes, it must be 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub start_offset: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// The signed object ranges for one revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateDownloadResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The absolute path, built from the stored display names.
    #[serde(deserialize_with = "crate::path::deserialize_derived_path")]
    pub path: AbsolutePath,
    /// The revision that the capability reads, as resolved from the request.
    pub revision_no: RevisionNo,
    /// The identity, byte length, and checksum of the revision's bytes.
    pub content_ref: ContentRef,
    /// The ranges that cover the revision's bytes from the requested offset, in order.
    pub ranges: Vec<DownloadRange>,
}

/// A short-lived capability to read one revision of an inode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateDownloadByInodeResponse {
    /// The namespace that was read.
    pub namespace_id: NamespaceId,
    /// The file inode being read.
    #[serde(with = "crate::public_inode_id")]
    pub inode_id: InodeId,
    /// The revision being read.
    pub revision_no: RevisionNo,
    /// The content's identity, size, and checksum.
    pub content_ref: ContentRef,
    /// The ranges that cover the revision's bytes from the requested offset, in order.
    pub ranges: Vec<DownloadRange>,
}

/// One contiguous run of a revision's bytes, which the client reads from one object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DownloadRange {
    /// The offset of the run's first byte in the revision.
    pub start_offset: u64,
    /// The number of bytes in the run.
    pub length: u64,
    /// A short-lived read capability for exactly this range's bytes.
    pub access: ObjectTransferAccess,
}

#[cfg(test)]
mod tests {
    use super::{
        CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
        CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, ReadConsistency,
    };
    use crate::{AbsolutePath, ContentId, ContentRef, NamespaceId, PinId, RevisionNo};
    use crate::{DownloadRange, ObjectTransferAccess};
    use std::collections::BTreeMap;

    fn absolute_path() -> AbsolutePath {
        AbsolutePath::parse("/docs/report.txt").expect("absolute path")
    }

    fn content_ref() -> ContentRef {
        ContentRef::blob_v1(
            crate::NamespaceId::parse("demo").expect("namespace id"),
            ContentId::parse("con_014d2pf2dbsqqg28t5cy4tqkff").expect("content id"),
            b"hello",
            crate::ChecksumAlgorithm::Crc64nvme,
        )
    }

    fn content_ref_json() -> serde_json::Value {
        serde_json::json!({
            "kind": "blob_v1",
            "owner_namespace_id": "demo",
            "content_id": "con_014d2pf2dbsqqg28t5cy4tqkff",
            "size_bytes": 5,
            "checksum": {
                "algorithm": "crc64nvme",
                "value": "3377857006524257"
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
        serde_json::from_str::<CreateDownloadRequest>(
            r#"{"path":"/docs/report.txt","snapshot_id":"invalid"}"#,
        )
        .expect_err("an invalid snapshot ID should be rejected");

        assert_eq!(request.start_offset, 0);
        let resumed: CreateDownloadRequest =
            serde_json::from_str(r#"{"path":"/docs/report.txt","start_offset":10}"#)
                .expect("decode resumed request");
        assert_eq!(resumed.start_offset, 10);

        serde_json::from_str::<CreateDownloadRequest>(
            r#"{"path":"/docs/report.txt","content_id":"con_014d2pf2dbsqqg28t5cy4tqkff"}"#,
        )
        .expect_err("a client must not be able to name the content object");
    }

    /// Checks that each download request decodes the consistency fields, and
    /// that a request without them serializes neither, so an older server
    /// that rejects unknown fields still accepts it.
    #[test]
    fn download_requests_take_the_consistency_fields_only_when_set() {
        let fields = serde_json::json!({"consistency": "eventual", "max_staleness_ms": 2500});
        let mut path_body = fields.clone();
        path_body["path"] = serde_json::json!("/docs/report.txt");
        let path: CreateDownloadRequest =
            serde_json::from_value(path_body).expect("decode path request");
        let inode: CreateDownloadByInodeRequest =
            serde_json::from_value(fields.clone()).expect("decode inode request");
        let revision: CreateRevisionDownloadByInodeRequest =
            serde_json::from_value(fields).expect("decode revision request");
        for (consistency, max_staleness_ms) in [
            (path.consistency, path.max_staleness_ms),
            (inode.consistency, inode.max_staleness_ms),
            (revision.consistency, revision.max_staleness_ms),
        ] {
            assert_eq!(consistency, Some(ReadConsistency::Eventual));
            assert_eq!(max_staleness_ms, Some(2500));
        }
        serde_json::from_str::<CreateDownloadByInodeRequest>(r#"{"consistency":"sometimes"}"#)
            .expect_err("an unknown consistency should be rejected");

        for unset in [
            serde_json::to_value(CreateDownloadByInodeRequest::default()),
            serde_json::to_value(CreateRevisionDownloadByInodeRequest::default()),
        ] {
            assert_eq!(unset.expect("serialize request"), serde_json::json!({}));
        }
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
                        "headers": {},
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
                        "headers": {},
                        "expires_at_ms": 1
                    }
                }]
            })
        );
    }
}
