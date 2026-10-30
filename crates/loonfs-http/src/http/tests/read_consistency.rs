//! The `consistency` and `max_staleness_ms` parameters of the live read
//! routes. The GET routes take them as query parameters, and the download
//! routes take them as fields of the JSON body.

use super::direct_download::LoopbackIssuer;
use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::JobMode;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::{ReadConsistency, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::{ErrorCode, ErrorResponse};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt as _;

/// A snapshot ID that parses, for the rejections that come before any read.
const SNAPSHOT_ID: &str = "pin_00000000000000000001-0000000000000002";

struct Fixture {
    /// Keeps the store's directory until the test ends.
    _directory: TempDir,
    /// Records every request the server sends to the store.
    recording: Arc<RecordingStore<LocalFsStore>>,
    router: axum::Router,
    /// The inode ID of `/file.txt`, in its public form.
    file_inode: String,
}

/// Creates a namespace `demo` with one file through a separate writer
/// runtime, and a server over the same store that has no writer session for
/// the namespace, so its reads start from the read cache. The server's
/// default maximum staleness is the revalidation bound, so an eventual read
/// stays inside it for the whole test, and `consistency` sets the runtime's
/// default consistency, or keeps the runtime's own default when `None`. The
/// server can presign object reads, so it grants downloads.
async fn fixture(name: &str, consistency: Option<ReadConsistency>) -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace = namespace_id("demo");
    let writer = super::test_runtime(store.clone(), &format!("{name}-writer")).await;
    writer
        .create_namespace(&namespace, &loonfs_test_support::test_actor())
        .await
        .expect("namespace");
    writer
        .open_namespace(&namespace)
        .expect("open namespace")
        .put_file("/file.txt", b"file", &loonfs_test_support::test_actor())
        .await
        .expect("commit file");
    let file_inode = writer
        .read_only()
        .namespace(&namespace)
        .stat("/file.txt")
        .await
        .expect("stat file")
        .inode_id;
    writer.shutdown().await.expect("writer shutdown");
    let mut options = test_options(directory.path(), name);
    options.grep = JobMode::Disabled;
    let (router, _state) = test_app(
        options,
        TestAppOptions {
            store: Some(store),
            max_staleness_ms: Some(READ_REVALIDATION_BOUND_MS),
            consistency,
            direct_transfers: Some(DirectTransferIssuers {
                get: LoopbackIssuer::at("http://objects"),
                put: None,
                multipart: None,
            }),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");
    Fixture {
        _directory: directory,
        recording,
        router,
        file_inode: file_inode.to_string(),
    }
}

/// A live read request, with its required parameters.
struct LiveRead {
    /// The route, with its required query parameters.
    uri: String,
    /// The JSON body of a download route, which takes the consistency
    /// parameters as fields next to the ones here, or `None` for a GET route,
    /// which takes them in its query string.
    body: Option<Value>,
    /// Whether the route takes `snapshot_id`.
    takes_snapshot: bool,
}

impl LiveRead {
    fn get(uri: String, takes_snapshot: bool) -> Self {
        Self {
            uri,
            body: None,
            takes_snapshot,
        }
    }

    fn download(uri: String, body: Value, takes_snapshot: bool) -> Self {
        Self {
            uri,
            body: Some(body),
            takes_snapshot,
        }
    }

    /// Returns the `param` that an error about `field` reports: the name of
    /// the query parameter, or the JSON Pointer of the body field.
    fn param(&self, field: &str) -> String {
        match self.body {
            None => field.to_owned(),
            Some(_) => format!("/{field}"),
        }
    }

    /// Returns the request for this read with `parameters` added.
    fn request(&self, parameters: &[(&str, Value)]) -> Request<Body> {
        let builder = Request::builder().header("authorization", "Bearer test-token");
        let Some(body) = &self.body else {
            let mut uri = self.uri.clone();
            for (name, value) in parameters {
                let value = value
                    .as_str()
                    .map_or_else(|| value.to_string(), str::to_owned);
                let separator = if uri.contains('?') { '&' } else { '?' };
                uri.push_str(&format!("{separator}{name}={value}"));
            }
            return builder
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .expect("request");
        };
        let mut body = body.clone();
        for (name, value) in parameters {
            body[*name] = value.clone();
        }
        builder
            .method("POST")
            .uri(&self.uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    }
}

impl Fixture {
    /// Returns every live read route, each with its required parameters.
    fn live_reads(&self) -> Vec<LiveRead> {
        let file = &self.file_inode;
        let namespace = "/v0/namespaces/demo";
        vec![
            LiveRead::get(format!("{namespace}/filesystem/entry?path=/file.txt"), true),
            LiveRead::get(format!("{namespace}/filesystem/entries?path=/"), true),
            LiveRead::get(
                format!("{namespace}/filesystem/content?path=/file.txt"),
                true,
            ),
            LiveRead::get(
                format!("{namespace}/filesystem/content?path=/file.txt&revision_no=1"),
                false,
            ),
            LiveRead::get(format!("{namespace}/filesystem/trash"), false),
            LiveRead::get(
                format!("{namespace}/filesystem/revisions?path=/file.txt"),
                false,
            ),
            LiveRead::get(format!("{namespace}/changes?after_seq=0"), true),
            LiveRead::get(
                format!("{namespace}/inodes/ino_00000000000000000000000000?include_attributes=false"),
                true,
            ),
            LiveRead::get(
                format!("{namespace}/inodes/ino_00000000000000000000000000/children?include_attributes=false"),
                true,
            ),
            LiveRead::get(format!("{namespace}/inodes/{file}/content"), true),
            LiveRead::get(format!("{namespace}/inodes/{file}/revisions"), false),
            LiveRead::get(
                format!("{namespace}/inodes/{file}/revisions/1/content"),
                false,
            ),
            LiveRead::download(
                format!("{namespace}/filesystem/downloads"),
                json!({"path": "/file.txt"}),
                true,
            ),
            LiveRead::download(
                format!("{namespace}/filesystem/downloads"),
                json!({"path": "/file.txt", "revision_no": 1}),
                false,
            ),
            LiveRead::download(
                format!("{namespace}/inodes/{file}/downloads"),
                json!({}),
                true,
            ),
            LiveRead::download(
                format!("{namespace}/inodes/{file}/revisions/1/downloads"),
                json!({}),
                false,
            ),
        ]
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, Vec<u8>) {
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, body.to_vec())
    }
}

/// Checks that every live read route accepts both consistency values, with and
/// without a maximum staleness, up to the revalidation bound.
#[tokio::test]
async fn consistency_parameters_parse_on_every_live_read_route() {
    let fixture = fixture("consistency-parse", None).await;
    for read in fixture.live_reads() {
        for parameters in [
            vec![("consistency", json!("strong"))],
            vec![("consistency", json!("eventual"))],
            vec![
                ("consistency", json!("eventual")),
                ("max_staleness_ms", json!(0)),
            ],
            vec![
                ("consistency", json!("eventual")),
                ("max_staleness_ms", json!(READ_REVALIDATION_BOUND_MS)),
            ],
        ] {
            let (status, body) = fixture.send(read.request(&parameters)).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{} {parameters:?}: {}",
                read.uri,
                String::from_utf8_lossy(&body)
            );
        }
    }
}

/// Checks that each live read route rejects `max_staleness_ms` without
/// `consistency=eventual`, a maximum staleness above the revalidation bound, an unknown
/// consistency value, and `consistency` together with `snapshot_id`, with
/// `invalid_request` for the parameter at fault and before any store
/// request.
#[tokio::test]
async fn consistency_rejections_identify_the_parameter() {
    let fixture = fixture("consistency-rejections", None).await;
    for read in fixture.live_reads() {
        let mut cases = vec![
            (vec![("max_staleness_ms", json!(10))], "max_staleness_ms"),
            (
                vec![
                    ("consistency", json!("strong")),
                    ("max_staleness_ms", json!(10)),
                ],
                "max_staleness_ms",
            ),
            (
                vec![
                    ("consistency", json!("eventual")),
                    ("max_staleness_ms", json!(READ_REVALIDATION_BOUND_MS + 1)),
                ],
                "max_staleness_ms",
            ),
            (
                vec![
                    ("consistency", json!("eventual")),
                    ("max_staleness_ms", json!("soon")),
                ],
                "max_staleness_ms",
            ),
            (vec![("consistency", json!("sometimes"))], "consistency"),
        ];
        // The revision routes refuse a snapshot on its own, and the trash
        // and revision listings don't take one, so they never reach the
        // consistency check with one.
        if read.takes_snapshot {
            for consistency in ["eventual", "strong"] {
                cases.push((
                    vec![
                        ("consistency", json!(consistency)),
                        ("snapshot_id", json!(SNAPSHOT_ID)),
                    ],
                    "consistency",
                ));
            }
        }
        for (parameters, field) in cases {
            fixture.recording.reset();
            let (status, body) = fixture.send(read.request(&parameters)).await;
            let context = format!("{} {parameters:?}", read.uri);
            assert_eq!(status, StatusCode::BAD_REQUEST, "{context}");
            let error: ErrorResponse = serde_json::from_slice(&body).expect("error body");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str(), "{context}");
            assert_eq!(error.param, Some(read.param(field)), "{context}");
            assert_eq!(fixture.recording.take(), [], "{context}");
        }
    }
}

/// Checks that an eventual read through the server within its maximum
/// staleness sends no store request, and that a `strong` read of the same
/// entry confirms the head with the store. A read that doesn't set
/// `consistency` takes the runtime's default, which is eventual unless the
/// server sets `strong`.
#[tokio::test]
async fn an_eventual_read_within_its_maximum_staleness_sends_no_store_request() {
    let entry = LiveRead::get(
        "/v0/namespaces/demo/filesystem/entry?path=/file.txt".to_owned(),
        true,
    );
    let children = LiveRead::get(
        "/v0/namespaces/demo/inodes/ino_00000000000000000000000000/children?include_attributes=false".to_owned(),
        true,
    );
    for (setting, unset_confirms) in [(None, false), (Some(ReadConsistency::Strong), true)] {
        let fixture = fixture("consistency-no-request", setting).await;
        for read in [&entry, &children] {
            let (status, _) = fixture.send(read.request(&[])).await;
            assert_eq!(status, StatusCode::OK, "warm read of {}", read.uri);
        }
        for (consistency, confirms) in [
            (None, unset_confirms),
            (Some("eventual"), false),
            (Some("strong"), true),
        ] {
            let parameters = consistency
                .map(|consistency| vec![("consistency", json!(consistency))])
                .unwrap_or_default();
            fixture.recording.reset();
            for read in [&entry, &children] {
                let (status, _) = fixture.send(read.request(&parameters)).await;
                assert_eq!(status, StatusCode::OK, "{} {parameters:?}", read.uri);
            }
            assert_eq!(
                !fixture.recording.take().is_empty(),
                confirms,
                "{setting:?} server, {consistency:?}"
            );
        }
    }
}

/// Checks that the trash and revision listings and the download grants see
/// a write made through another runtime when they're `strong`, and that an
/// `eventual` read within its maximum staleness reads the head that the
/// server confirmed before the write. The eventual reads come first,
/// because a strong read moves the server's confirmed head forward.
#[tokio::test]
async fn a_strong_read_sees_a_write_through_another_runtime_and_an_eventual_read_may_not() {
    let fixture = fixture("consistency-follows-writes", None).await;
    let warm = LiveRead::get(
        "/v0/namespaces/demo/filesystem/entry?path=/file.txt".to_owned(),
        true,
    );
    let (status, _) = fixture.send(warm.request(&[])).await;
    assert_eq!(status, StatusCode::OK, "warm read");

    let store: SharedObjectStore = fixture.recording.clone();
    let writer = super::test_runtime(store, "consistency-follows-writes-second").await;
    let namespace = writer
        .open_namespace(&namespace_id("demo"))
        .expect("open namespace");
    let actor = loonfs_test_support::test_actor();
    namespace
        .append_file("/file.txt", b" and more", &actor)
        .await
        .expect("append the second revision");
    namespace
        .put_file("/gone.txt", b"gone", &actor)
        .await
        .expect("put a file to delete");
    namespace
        .delete_path("/gone.txt", &actor)
        .await
        .expect("delete the file");
    writer.shutdown().await.expect("writer shutdown");

    let file = &fixture.file_inode;
    let namespace = "/v0/namespaces/demo";
    // Each read, the field that the write changes, and the field's value
    // before and after the write. A missing field reads as null.
    let reads = [
        (
            LiveRead::get(format!("{namespace}/filesystem/trash"), false),
            "/entries/0/inode_kind",
            Value::Null,
            json!("file"),
        ),
        (
            LiveRead::get(
                format!("{namespace}/filesystem/revisions?path=/file.txt"),
                false,
            ),
            "/revisions/0/revision_no",
            json!(1),
            json!(2),
        ),
        (
            LiveRead::get(format!("{namespace}/inodes/{file}/revisions"), false),
            "/revisions/0/revision_no",
            json!(1),
            json!(2),
        ),
        (
            LiveRead::download(
                format!("{namespace}/filesystem/downloads"),
                json!({"path": "/file.txt"}),
                true,
            ),
            "/revision_no",
            json!(1),
            json!(2),
        ),
        (
            LiveRead::download(
                format!("{namespace}/inodes/{file}/downloads"),
                json!({}),
                true,
            ),
            "/revision_no",
            json!(1),
            json!(2),
        ),
        (
            LiveRead::download(
                format!("{namespace}/inodes/{file}/revisions/2/downloads"),
                json!({}),
                false,
            ),
            "/revision_no",
            Value::Null,
            json!(2),
        ),
    ];
    for (consistency, expect_after) in [("eventual", false), ("strong", true)] {
        for (read, field, before, after) in &reads {
            let (_, body) = fixture
                .send(read.request(&[("consistency", json!(consistency))]))
                .await;
            let body: Value = serde_json::from_slice(&body).expect("JSON body");
            let observed = body.pointer(field).unwrap_or(&Value::Null);
            let expected = if expect_after { after } else { before };
            assert_eq!(observed, expected, "{consistency} {}: {body}", read.uri);
        }
    }
}
