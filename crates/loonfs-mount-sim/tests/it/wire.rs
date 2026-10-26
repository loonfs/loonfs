//! The responses the simulated server produces decode as the `loonfs-types` shapes, and encode
//! back to the same JSON.

use crate::support::{fixture, put, request};
use bytes::Bytes;
use http::{Method, StatusCode};
use loonfs_types::api::v0::{
    Commit, CreateDownloadByInodeResponse, CreateDownloadResponse, DeleteSnapshotResponse,
    FilesystemOperation, ListChangesResponse, Snapshot, UploadSession,
};
use loonfs_types::{
    AttributeKey, AttributeValue, CapabilityDocument, ErrorResponse, ListInodeChildrenResponse,
    ListPathEntriesResponse, NamespaceMetadata, PathEntry,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::BTreeMap;

/// Decodes `body` as `T`, and returns the body's JSON and the JSON that `T` encodes back to.
fn round_trip<T: DeserializeOwned + Serialize>(
    body: &[u8],
) -> (serde_json::Value, serde_json::Value) {
    let wire: serde_json::Value = serde_json::from_slice(body).expect("a JSON body");
    let typed: T = serde_json::from_value(wire.clone()).expect("the body decodes as the API type");
    (
        wire,
        serde_json::to_value(&typed).expect("the API type encodes"),
    )
}

type RoundTrip = fn(&[u8]) -> (serde_json::Value, serde_json::Value);

/// A request (the method, the URI, the headers, and the body) and the check of its response.
type Case<'a> = (Method, String, &'a [(&'a str, &'a str)], Vec<u8>, RoundTrip);

#[tokio::test]
async fn every_response_round_trips_through_its_api_type() {
    let fixture = fixture();
    let commit = fixture.commit(vec![
        put("/dir/file.txt", b"hello"),
        FilesystemOperation::UpdateAttributesByInode {
            inode_id: loonfs_types::InodeId(3),
            set: BTreeMap::from([(
                AttributeKey::parse("fs.mode").expect("key"),
                AttributeValue::parse("600").expect("value"),
            )]),
            remove: Vec::new(),
            expected_attributes_revision_no: None,
        },
    ]);
    assert_eq!(
        commit.events.len(),
        3,
        "a directory, a file, and its attributes"
    );
    let snapshot_body = br#"{"name":"wire","ttl_ms":60000}"#;
    let (_, _, snapshot) = fixture
        .send(
            Method::POST,
            "/v0/namespaces/demo/snapshots",
            &[],
            Bytes::from_static(snapshot_body),
        )
        .await;
    let snapshot: Snapshot = serde_json::from_slice(&snapshot).expect("a snapshot");
    let commit_body = serde_json::to_vec(&request(vec![put("/other.txt", b"x")])).expect("encode");
    let download = br#"{"path":"/dir/file.txt"}"#;
    let upload = br#"{"mode":"service_proxied"}"#;
    let snapshot_uri = format!("/v0/namespaces/demo/snapshots/{}", snapshot.snapshot_id);
    let actor = [("Loonfs-Actor", "tester")];
    let cases: Vec<Case<'_>> = vec![
        (Method::GET, "/v0/capabilities".into(), &[], vec![], round_trip::<CapabilityDocument>),
        (Method::GET, "/v0/namespaces/demo".into(), &[], vec![], round_trip::<NamespaceMetadata>),
        (Method::GET, "/v0/namespaces/demo/filesystem/entry?path=%2Fdir%2Ffile.txt".into(), &[], vec![], round_trip::<PathEntry>),
        (Method::GET, "/v0/namespaces/demo/inodes/ino_1?include_attributes=false".into(), &[], vec![], round_trip::<PathEntry>),
        (Method::GET, "/v0/namespaces/demo/filesystem/entries?path=%2F&recursive=true&include_attributes=true".into(), &[], vec![], round_trip::<ListPathEntriesResponse>),
        (Method::GET, "/v0/namespaces/demo/inodes/ino_2/children".into(), &[], vec![], round_trip::<ListInodeChildrenResponse>),
        (Method::GET, "/v0/namespaces/demo/changes?after_seq=0".into(), &[], vec![], round_trip::<ListChangesResponse>),
        (Method::POST, "/v0/namespaces/demo/commits".into(), &actor, commit_body, round_trip::<Commit>),
        (Method::GET, snapshot_uri.clone(), &[], vec![], round_trip::<Snapshot>),
        (Method::DELETE, snapshot_uri, &[], vec![], round_trip::<DeleteSnapshotResponse>),
        (Method::POST, "/v0/namespaces/demo/filesystem/downloads".into(), &[], download.to_vec(), round_trip::<CreateDownloadResponse>),
        (Method::POST, "/v0/namespaces/demo/inodes/ino_3/downloads".into(), &[], b"{}".to_vec(), round_trip::<CreateDownloadByInodeResponse>),
        (Method::POST, "/v0/namespaces/demo/uploads".into(), &[], upload.to_vec(), round_trip::<UploadSession>),
        (Method::GET, "/v0/namespaces/demo/inodes/ino_99".into(), &[], vec![], round_trip::<ErrorResponse>),
    ];
    for (method, uri, headers, body, check) in cases {
        let (status, _, bytes) = fixture
            .send(method.clone(), &uri, headers, Bytes::from(body))
            .await;
        let (wire, typed) = check(&bytes);
        assert_eq!(wire, typed, "{method} {uri} returned {status}");
    }
}

#[tokio::test]
async fn an_error_response_has_the_envelope_and_the_status_of_its_code() {
    let fixture = fixture();
    let cases = [
        (
            "/v0/namespaces/demo/inodes/ino_99",
            StatusCode::NOT_FOUND,
            "inode_not_found",
            None,
        ),
        (
            "/v0/namespaces/missing",
            StatusCode::NOT_FOUND,
            "namespace_not_found",
            None,
        ),
        (
            "/v0/namespaces/demo/filesystem/entry?path=%2F&colour=red",
            StatusCode::BAD_REQUEST,
            "invalid_request",
            Some("colour"),
        ),
        (
            "/v0/namespaces/demo/changes?after_seq=7",
            StatusCode::BAD_REQUEST,
            "invalid_request",
            Some("after_seq"),
        ),
        (
            "/v0/namespaces/demo/nowhere",
            StatusCode::NOT_FOUND,
            "route_not_found",
            None,
        ),
    ];
    for (uri, status, code, param) in cases {
        let (actual_status, headers, body) = fixture.get(uri).await;
        let error: ErrorResponse = serde_json::from_slice(&body).expect("an error envelope");
        assert_eq!(
            (actual_status, error.code.as_str(), error.param.as_deref()),
            (status, code, param),
            "{uri}"
        );
        assert_eq!(
            headers
                .get("x-request-id")
                .and_then(|value| value.to_str().ok()),
            error.request_id.as_deref(),
            "{uri}"
        );
    }
}

#[tokio::test]
async fn the_capability_switches_change_the_document_and_the_operations_they_gate() {
    let fixture = fixture();
    fixture.commit(vec![put("/f", b"f")]);
    fixture
        .server
        .set_capabilities(loonfs_mount_sim::CapabilitySwitches {
            inline_content: loonfs_mount_sim::InlineContent::NotAdvertised,
            advertise_direct_get: false,
        });
    let (_, _, body) = fixture.get("/v0/capabilities").await;
    let document: CapabilityDocument = serde_json::from_slice(&body).expect("a document");
    assert!(document.features.is_empty(), "{document:?}");
    assert!(!document
        .limits
        .contains_key(loonfs_types::LIMIT_COMMIT_MAX_INLINE_CONTENT_BYTES_PER_OPERATION));

    let error = fixture
        .try_commit(&request(vec![put("/g", b"g")]))
        .expect_err("inline content is off");
    assert_eq!(
        (error.code(), error.details().operation_index),
        (loonfs_types::ErrorCode::NotSupported, Some(0))
    );
    let (status, _, body) = fixture
        .send(
            Method::POST,
            "/v0/namespaces/demo/filesystem/downloads",
            &[],
            Bytes::from_static(br#"{"path":"/f"}"#),
        )
        .await;
    let error: ErrorResponse = serde_json::from_slice(&body).expect("an error envelope");
    assert_eq!(
        (status, error.feature.as_deref()),
        (
            StatusCode::NOT_IMPLEMENTED,
            Some(loonfs_types::FEATURE_DOWNLOADS_DIRECT_GET)
        )
    );
}
