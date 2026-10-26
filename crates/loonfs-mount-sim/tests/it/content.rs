//! Content: ranged reads and 416, the revision route, download grants read end to end through
//! the client, and the upload path.

use crate::support::{append, fixture, fixture_with, put, request};
use bytes::Bytes;
use http::header::CONTENT_RANGE;
use http::{Method, StatusCode};
use loonfs_client::NamespacePath;
use loonfs_mount_sim::{ByteRange, Operation, SimulatedServerConfig};
use loonfs_sim::{SimClock as _, SimDuration};
use loonfs_types::api::v0::{
    CommitPrecondition, CompleteUploadBody, ContentToken, CreateUploadBody, FilesystemOperation,
};
use loonfs_types::{ErrorCode, ErrorResponse, RevisionNo};

fn bytes(size: usize) -> Vec<u8> {
    (0..=250_u8).cycle().take(size).collect()
}

/// A ranged read: the route, the `Range` header, the status, the `Content-Range` header, and
/// the bytes of the file that the response returns.
type RangeCase<'a> = (
    &'a str,
    &'a str,
    StatusCode,
    Option<&'a str>,
    std::ops::Range<usize>,
);

#[tokio::test]
async fn a_ranged_read_returns_206_and_a_range_past_the_end_returns_416() {
    let fixture = fixture();
    let file = bytes(1000);
    fixture.commit(vec![put("/f", &file)]);
    let inode = loonfs_types::public_inode_id::encode(fixture.stat("/f").await.inode_id);
    let by_path = "/v0/namespaces/demo/filesystem/content?path=%2Ff";
    let by_inode = format!("/v0/namespaces/demo/inodes/{inode}/content");
    let cases: [RangeCase<'_>; 6] = [
        (
            by_path,
            "bytes=0-99",
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 0-99/1000"),
            0..100,
        ),
        (
            by_path,
            "bytes=950-1999",
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 950-999/1000"),
            950..1000,
        ),
        (
            &by_inode,
            "bytes=10-",
            StatusCode::PARTIAL_CONTENT,
            Some("bytes 10-999/1000"),
            10..1000,
        ),
        (
            &by_inode,
            "bytes=1000-",
            StatusCode::RANGE_NOT_SATISFIABLE,
            Some("bytes */1000"),
            0..0,
        ),
        (by_path, "bytes=-500", StatusCode::BAD_REQUEST, None, 0..0),
        (by_path, "bytes=5-1", StatusCode::BAD_REQUEST, None, 0..0),
    ];
    for (uri, range, status, content_range, expected) in cases {
        let (actual, headers, body) = fixture
            .send(Method::GET, uri, &[("range", range)], Bytes::new())
            .await;
        assert_eq!(actual, status, "{range}");
        assert_eq!(
            headers
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok()),
            content_range,
            "{range}"
        );
        if status.is_success() {
            assert_eq!(body, &file[expected], "{range}");
        }
    }
    let (_, _, body) = fixture
        .send(
            Method::GET,
            by_path,
            &[("range", "bytes=2000-")],
            Bytes::new(),
        )
        .await;
    let error: ErrorResponse = serde_json::from_slice(&body).expect("an error envelope");
    assert_eq!(
        (
            error.code.as_str(),
            error.details.and_then(|details| details.size_bytes)
        ),
        ("range_not_satisfiable", Some(1000))
    );
    let ranges: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .filter_map(|record| record.byte_range)
        .collect();
    let served = |start_offset, end_offset| ByteRange {
        start_offset,
        end_offset,
    };
    assert_eq!(
        ranges,
        [served(0, 100), served(950, 1000), served(10, 1000)]
    );
}

#[tokio::test]
async fn the_revision_route_reads_an_earlier_revision_and_a_deleted_file() {
    let fixture = fixture();
    fixture.commit(vec![put("/log", b"first")]);
    let log = fixture.stat("/log").await;
    fixture.commit(vec![append(log.inode_id, b" second")]);
    fixture.commit(vec![FilesystemOperation::DeleteByInode {
        inode_id: log.inode_id,
        expected_binding_version: log.binding_version.expect("version"),
        behavior: Default::default(),
    }]);
    let inode = loonfs_types::public_inode_id::encode(log.inode_id);
    for (revision, range, expected) in [(1, "bytes=0-", "first"), (2, "bytes=6-", "second")] {
        let uri = format!("/v0/namespaces/demo/inodes/{inode}/revisions/{revision}/content");
        let (status, _, body) = fixture
            .send(Method::GET, &uri, &[("range", range)], Bytes::new())
            .await;
        assert_eq!(
            (status, body.as_ref()),
            (StatusCode::PARTIAL_CONTENT, expected.as_bytes())
        );
    }
    let (status, _, _) = fixture
        .get(&format!("/v0/namespaces/demo/inodes/{inode}/content"))
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "the current-content route needs a visible inode"
    );
}

#[tokio::test]
async fn a_download_grant_is_read_end_to_end_through_the_client() {
    let fixture = fixture_with(SimulatedServerConfig {
        max_download_range_bytes: 300,
        ..SimulatedServerConfig::default()
    });
    let file = bytes(1000);
    fixture.commit(vec![put("/f", &file)]);
    let inode_id = fixture.stat("/f").await.inode_id;
    let grant = fixture
        .client
        .create_download_by_inode(&fixture.namespace_id, inode_id)
        .await
        .expect("a grant");
    let lengths: Vec<u64> = grant.ranges.iter().map(|range| range.length).collect();
    assert_eq!(lengths, [300, 300, 300, 100]);

    let mut stream = fixture
        .client
        .open_direct_download_by_inode(&grant)
        .await
        .expect("open");
    let mut read = Vec::new();
    while let Some(chunk) = stream.next_chunk().await.expect("a verified chunk") {
        read.extend_from_slice(&chunk);
    }
    assert_eq!(read, file);
    let signed: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .filter(|record| record.operation == Operation::ReadDownloadRange)
        .map(|record| {
            record
                .byte_range
                .expect("a signed range read returns bytes")
                .length()
        })
        .collect();
    assert_eq!(signed, [300, 300, 300, 100]);

    fixture
        .server
        .clock()
        .advance(SimDuration::from_millis(15 * 60 * 1000));
    let expired = fixture.client.open_direct_download_by_inode(&grant).await;
    assert!(
        expired.is_err(),
        "a grant's URLs expire on the virtual clock"
    );
    let resumed = fixture
        .client
        .create_revision_download_by_inode_with_options(
            &fixture.namespace_id,
            inode_id,
            RevisionNo(1),
            &loonfs_client::DownloadOptions {
                start_offset: 900,
                ..Default::default()
            },
        )
        .await
        .expect("a grant from an offset");
    let mut stream = fixture
        .client
        .open_direct_download_by_inode(&resumed)
        .await
        .expect("open");
    stream
        .fold_resumed_prefix(&file[..900])
        .expect("the prefix");
    let tail = stream.next_chunk().await.expect("a chunk").expect("bytes");
    assert_eq!(tail, file[900..]);
    assert!(stream.next_chunk().await.expect("verified").is_none());
}

#[tokio::test]
async fn an_upload_issues_a_token_that_a_commit_accepts_until_it_expires() {
    let fixture = fixture();
    let large = bytes(100 * 1024);
    let spec = NamespacePath::parse("demo", "/large.bin").expect("namespace path");
    fixture
        .client
        .put_file(&spec, &large, &fixture.actor)
        .await
        .expect("put through an upload");
    let operations: Vec<_> = fixture
        .server
        .trace()
        .iter()
        .map(|record| record.operation)
        .collect();
    assert_eq!(
        operations,
        [
            Operation::GetCapabilities,
            Operation::CreateUpload,
            Operation::PutUploadContent,
            Operation::CompleteUpload,
            Operation::CreateCommit,
        ]
    );
    assert_eq!(fixture.client.read_file(&spec).await.expect("read"), large);

    let session = fixture
        .client
        .create_upload(&fixture.namespace_id, &CreateUploadBody::ServiceProxied {})
        .await
        .expect("begin");
    fixture
        .client
        .put_upload_content(&fixture.namespace_id, &session.upload_id, b"payload")
        .await
        .expect("content");
    let completed = fixture
        .client
        .complete_upload(
            &fixture.namespace_id,
            &session.upload_id,
            &CompleteUploadBody::ServiceProxied {},
        )
        .await
        .expect("complete");
    let token = completed.content_token().expect("a token").clone();
    fixture
        .server
        .clock()
        .advance(SimDuration::from_millis(60 * 60 * 1000));
    let stale =
        put_uploaded("/up.bin", &token).preconditions(vec![CommitPrecondition::NamespaceHead {
            expected_head_seq: loonfs_types::ChangeSeq(99),
        }]);
    let error = fixture.try_commit(&stale).expect_err("an expired token");
    assert_eq!(
        (
            error.code(),
            error.details().operation_index,
            error.details().precondition_index
        ),
        (ErrorCode::ContentNotPrepared, None, None),
        "content tokens are checked before the preconditions"
    );

    let refreshed = fixture
        .client
        .get_upload(&fixture.namespace_id, &session.upload_id)
        .await
        .expect("read the session");
    let token = refreshed.content_token().expect("a new token").clone();
    fixture
        .try_commit(&put_uploaded("/up.bin", &token))
        .expect("a new token proves the content");
}

fn put_uploaded(path: &str, token: &ContentToken) -> loonfs_types::CommitRequest {
    let mut request = request(vec![FilesystemOperation::PutFile {
        path: crate::support::path(path),
        content_ref: Some(token.content_ref.clone()),
        inline_content: None,
        behavior: Default::default(),
        expected_inode_id: None,
        expected_revision_no: None,
    }]);
    request.content_tokens = vec![token.clone()];
    request
}
