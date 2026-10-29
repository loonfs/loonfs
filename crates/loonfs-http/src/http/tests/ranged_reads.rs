//! Ranged reads of the three content routes: the `Range` header, the 206,
//! 400, and 416 responses, the download limit, and the content objects that
//! a ranged read reads and never writes.

use super::fixtures::{test_app, test_options, TestAppOptions, TestOptions};
use crate::JobMode;
use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, Request, StatusCode};
use loonfs::{LoonFs, SharedObjectStore};
use loonfs_client::{Client, ClientConfig, ClientError, NamespacePath, ReadRange};
use loonfs_objectstore::keys::{content_blob, content_prefix};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};
use loonfs_types::{public_inode_id, ErrorCode, ErrorResponse, NamespaceId};
use std::num::NonZeroU64;
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt as _;

const PATH: &str = "/file.bin";
/// The bytes of revision 1, which a fold writes to a content object.
const FOLDED_BYTES: usize = 200;
/// The bytes that revision 2 appends, which stay pending WAL pieces.
const APPENDED_BYTES: usize = 100;

struct Fixture {
    /// Keeps the store's directory until the test ends.
    _directory: TempDir,
    /// Records the server's requests for content objects.
    recording: Arc<RecordingStore<LocalFsStore>>,
    router: axum::Router,
    namespace_id: NamespaceId,
    /// Revision 2 of the file: the folded bytes and then the appended ones.
    bytes: Vec<u8>,
    /// The file's inode ID, in its public form.
    inode: String,
    /// The snapshot that captured revision 1.
    snapshot_id: String,
    /// The one content object that the fold wrote.
    folded_object: String,
}

/// Writes revision 1 of `/file.bin`, folds it into a content object, takes a
/// snapshot, and appends revision 2, whose appended bytes stay pending WAL
/// pieces. A separate writer that never folds on its own writes them, and the
/// server reads them through `configure`'s options.
async fn fixture(configure: impl FnOnce(&mut TestOptions)) -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::content_blob(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = namespace_id("ranged");
    let actor = loonfs_test_support::test_actor();
    let bytes: Vec<u8> = (0..=250_u8)
        .cycle()
        .take(FOLDED_BYTES + APPENDED_BYTES)
        .collect();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("ranged-seed"))
        .idle_fold_after_ms(None)
        .build()
        .await
        .expect("writer");
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace
        .put_file(PATH, &bytes[..FOLDED_BYTES], &actor)
        .await
        .expect("revision 1");
    writer
        .maintenance(writer_id("ranged-seed-maintenance"))
        .fold_wal(&namespace_id)
        .await
        .expect("fold revision 1");
    let snapshot = namespace
        .create_snapshot("before-append")
        .await
        .expect("snapshot");
    namespace
        .append_file(PATH, &bytes[FOLDED_BYTES..], &actor)
        .await
        .expect("revision 2");
    let entry = namespace.stat(PATH).await.expect("stat");
    writer.shutdown().await.expect("writer shutdown");
    let content_ref = entry.content_ref().expect("file content");
    let mut options = test_options(directory.path(), "ranged-server");
    options.grep = JobMode::Disabled;
    configure(&mut options);
    let (router, _state) = test_app(
        options,
        TestAppOptions {
            store: Some(store),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");
    Fixture {
        folded_object: content_blob(&namespace_id, &content_ref.content_id),
        _directory: directory,
        recording,
        router,
        namespace_id,
        bytes,
        inode: public_inode_id::encode(entry.inode_id),
        snapshot_id: snapshot.snapshot_id.to_string(),
    }
}

impl Fixture {
    fn path_route(&self) -> String {
        format!(
            "/v0/namespaces/{}/filesystem/content?path=%2Ffile.bin",
            self.namespace_id
        )
    }

    fn inode_route(&self) -> String {
        format!(
            "/v0/namespaces/{}/inodes/{}/content",
            self.namespace_id, self.inode
        )
    }

    fn revision_route(&self, revision_no: u64) -> String {
        format!(
            "/v0/namespaces/{}/inodes/{}/revisions/{revision_no}/content",
            self.namespace_id, self.inode
        )
    }

    /// Sends a GET with one header for each value in `ranges`.
    async fn get(&self, uri: &str, ranges: &[&str]) -> (StatusCode, HeaderMap, Bytes) {
        let mut request = Request::builder()
            .uri(uri)
            .header(header::AUTHORIZATION, "Bearer test-token");
        for range in ranges {
            request = request.header(header::RANGE, *range);
        }
        let response = self
            .router
            .clone()
            .oneshot(request.body(Body::empty()).expect("request"))
            .await
            .expect("response");
        let (parts, body) = response.into_parts();
        let body = axum::body::to_bytes(body, usize::MAX)
            .await
            .expect("response body");
        (parts.status, parts.headers, body)
    }

    /// Checks a 206 response for `range`, which returns `expected` of the
    /// file's `size_bytes` bytes.
    async fn assert_range(
        &self,
        uri: &str,
        range: &str,
        expected: std::ops::Range<usize>,
        size_bytes: usize,
    ) {
        let (status, headers, body) = self.get(uri, &[range]).await;
        assert_eq!(
            status,
            StatusCode::PARTIAL_CONTENT,
            "{uri} {range}: {body:?}"
        );
        assert_eq!(
            headers[header::CONTENT_RANGE],
            format!("bytes {}-{}/{size_bytes}", expected.start, expected.end - 1),
            "{uri} {range}"
        );
        assert_eq!(
            headers[header::CONTENT_LENGTH],
            expected.len().to_string(),
            "{uri} {range}"
        );
        assert_eq!(headers[header::ACCEPT_RANGES], "bytes", "{uri} {range}");
        assert_eq!(body, self.bytes[expected], "{uri} {range}");
    }

    async fn content_objects(&self) -> Vec<String> {
        self.recording
            .list_prefix(&content_prefix(&self.namespace_id))
            .await
            .expect("list content objects")
    }
}

fn error(body: &Bytes) -> ErrorResponse {
    serde_json::from_slice(body).expect("error envelope")
}

/// Checks that a range inside the folded content object reads only that
/// part of the object, that a range inside the appended bytes reads no
/// object at all, and that a range across both reads only the object's
/// part, on both current-revision routes. No ranged read writes a content
/// object, so the appended bytes stay pending WAL pieces throughout.
#[tokio::test]
async fn ranged_reads_get_only_their_range_and_write_no_content_object() {
    let fixture = fixture(|_| {}).await;
    assert_eq!(
        fixture.content_objects().await,
        std::slice::from_ref(&fixture.folded_object)
    );
    let size = FOLDED_BYTES + APPENDED_BYTES;
    for uri in [fixture.path_route(), fixture.inode_route()] {
        for (range, expected, object_range) in [
            ("bytes=10-49", 10..50, Some((10, 50))),
            ("bytes=250-279", 250..280, None),
            ("bytes=150-249", 150..250, Some((150, 200))),
            ("bytes=290-", 290..300, None),
            ("bytes=0-", 0..300, Some((0, 200))),
            ("bytes=295-100000", 295..300, None),
            ("bytes=199-199", 199..200, Some((199, 200))),
        ] {
            fixture.recording.reset();
            fixture.assert_range(&uri, range, expected, size).await;
            assert_eq!(fixture.recording.count(OperationSelector::Put), 0);
            let gets = fixture.recording.take_gets();
            let expected_gets: Vec<_> = object_range
                .map(|object_range| (fixture.folded_object.clone(), Some(object_range)))
                .into_iter()
                .collect();
            assert_eq!(gets, expected_gets, "{uri} {range}");
        }
    }
    assert_eq!(
        fixture.content_objects().await,
        std::slice::from_ref(&fixture.folded_object)
    );
}

/// Checks that the revision route reads a range of an earlier revision,
/// that a snapshot read returns a range of the revision the snapshot
/// captured, and that a range runs only to the end of the revision it reads.
#[tokio::test]
async fn ranged_reads_select_revisions_and_snapshots() {
    let fixture = fixture(|_| {}).await;
    let snapshot = format!("snapshot_id={}", fixture.snapshot_id);
    fixture
        .assert_range(&fixture.revision_route(1), "bytes=190-", 190..200, 200)
        .await;
    fixture
        .assert_range(&fixture.revision_route(2), "bytes=190-209", 190..210, 300)
        .await;
    for uri in [
        format!("{}&{snapshot}", fixture.path_route()),
        format!("{}?{snapshot}", fixture.inode_route()),
    ] {
        fixture
            .assert_range(&uri, "bytes=100-1000", 100..200, 200)
            .await;
        let (status, _, body) = fixture.get(&uri, &["bytes=200-"]).await;
        assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE, "{uri}");
        assert_eq!(error(&body).details.expect("details").size_bytes, Some(200));
    }
    let (status, _, body) = fixture
        .get(&fixture.revision_route(1), &["bytes=200-200"])
        .await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(error(&body).code, ErrorCode::RangeNotSatisfiable.as_str());
}

/// Checks that a range starting at or past the end of the file returns 416
/// with the file's size, and that every other `Range` value returns 400
/// before the server reads anything.
#[tokio::test]
async fn unsatisfiable_and_unsupported_ranges_are_refused() {
    let fixture = fixture(|_| {}).await;
    for uri in [
        fixture.path_route(),
        fixture.inode_route(),
        fixture.revision_route(2),
    ] {
        for range in ["bytes=300-", "bytes=300-300", "bytes=1000-2000"] {
            let (status, headers, body) = fixture.get(&uri, &[range]).await;
            assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE, "{uri} {range}");
            assert_eq!(headers[header::CONTENT_RANGE], "bytes */300");
            let error = error(&body);
            assert_eq!(error.code, ErrorCode::RangeNotSatisfiable.as_str());
            assert_eq!(error.details.expect("details").size_bytes, Some(300));
        }
        fixture.recording.reset();
        for ranges in [
            &["bytes=-10"][..],
            &["bytes=0-1,5-6"],
            &["bytes=10-5"],
            &["items=0-1"],
            &["bytes=0-1", "bytes=5-6"],
        ] {
            let (status, _, body) = fixture.get(&uri, ranges).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri} {ranges:?}");
            let error = error(&body);
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
            assert_eq!(error.param.as_deref(), Some("Range"));
        }
        assert!(fixture.recording.snapshot().is_empty());
    }
}

/// Checks that a read without a `Range` header returns the whole file without
/// a Content-Length, and that its response includes `Accept-Ranges: bytes`.
#[tokio::test]
async fn unranged_reads_advertise_byte_ranges() {
    let fixture = fixture(|_| {}).await;
    for uri in [
        fixture.path_route(),
        fixture.inode_route(),
        fixture.revision_route(2),
    ] {
        let (status, headers, body) = fixture.get(&uri, &[]).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(headers[header::ACCEPT_RANGES], "bytes", "{uri}");
        assert!(!headers.contains_key(header::CONTENT_LENGTH), "{uri}");
        assert!(!headers.contains_key(header::CONTENT_RANGE), "{uri}");
        assert_eq!(body, fixture.bytes, "{uri}");
    }
}

/// Checks, through the client and a listening server, that the download
/// limit applies to the length of a ranged read: a range of a file over the
/// limit succeeds when the range fits, and a range longer than the limit
/// returns `content_too_large`.
#[tokio::test]
async fn the_download_limit_applies_to_the_length_of_a_range() {
    let fixture = fixture(|options| options.binding.max_download_bytes = Some(64)).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let router = fixture.router.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve app");
    });
    let client = Client::new(ClientConfig {
        server_url: format!("http://{addr}"),
        auth_token: Some("test-token".into()),
        request_timeout_ms: None,
        disable_transient_retry: false,
        ca_cert_path: None,
    })
    .expect("client");
    let spec = NamespacePath::parse("ranged", PATH).expect("namespace path");
    let range = |start_offset, length: Option<u64>| ReadRange {
        start_offset,
        length: length.map(|length| NonZeroU64::new(length).expect("nonzero length")),
    };

    let error = client.read_file(&spec).await.expect_err("over the limit");
    assert_eq!(error.code(), Some(ErrorCode::ContentTooLarge));
    for (requested, expected) in [(range(10, Some(64)), 10..74), (range(250, None), 250..300)] {
        let read = client
            .read_file_range_stream(&spec, requested)
            .await
            .expect("a range inside the limit");
        assert_eq!(read.start_offset, expected.start as u64);
        assert_eq!(read.size_bytes, 300);
        let mut body = Vec::new();
        let mut stream = read.body;
        while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
            body.extend_from_slice(&chunk.expect("chunk"));
        }
        assert_eq!(body, fixture.bytes[expected]);
    }
    for requested in [range(10, Some(65)), range(0, None)] {
        let error = client
            .read_file_range_stream(&spec, requested)
            .await
            .expect_err("a range over the limit");
        assert!(
            matches!(&error, ClientError::Api { status: 413, response, .. }
                if response.code == ErrorCode::ContentTooLarge.as_str()),
            "{error:?}"
        );
    }
    server.abort();
}
