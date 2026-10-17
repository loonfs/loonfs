//! The `consistency` and `max_staleness_ms` query parameters of the live
//! read routes.

use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::JobMode;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::engine::READ_REVALIDATION_BOUND_MS;
use loonfs::{Consistency, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_types::{ErrorCode, ErrorResponse};
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
/// default consistency, or keeps the runtime's own default when `None`.
async fn fixture(name: &str, consistency: Option<Consistency>) -> Fixture {
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
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");
    Fixture {
        _directory: directory,
        recording,
        router,
        file_inode: loonfs_types::public_inode_id::encode(file_inode),
    }
}

impl Fixture {
    /// Returns every live read route, each with its required parameters.
    fn live_read_routes(&self) -> Vec<String> {
        let file = &self.file_inode;
        vec![
            "/v0/namespaces/demo/filesystem/entry?path=/file.txt".to_owned(),
            "/v0/namespaces/demo/filesystem/entries?path=/".to_owned(),
            "/v0/namespaces/demo/filesystem/content?path=/file.txt".to_owned(),
            "/v0/namespaces/demo/filesystem/content?path=/file.txt&revision_no=1".to_owned(),
            "/v0/namespaces/demo/changes?after_seq=0".to_owned(),
            "/v0/namespaces/demo/inodes/ino_1?include_attributes=false".to_owned(),
            "/v0/namespaces/demo/inodes/ino_1/children?include_attributes=false".to_owned(),
            format!("/v0/namespaces/demo/inodes/{file}/content?"),
            format!("/v0/namespaces/demo/inodes/{file}/revisions/1/content?"),
        ]
    }

    async fn get(&self, uri: &str) -> (StatusCode, Vec<u8>) {
        let response = self
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("authorization", "Bearer test-token")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, body.to_vec())
    }
}

/// Appends `parameters` to a route that already has a query string or ends
/// in `?`.
fn with_parameters(route: &str, parameters: &str) -> String {
    if route.ends_with('?') {
        format!("{route}{parameters}")
    } else {
        format!("{route}&{parameters}")
    }
}

/// Checks that every live read route accepts both consistency values, with and
/// without a maximum staleness, up to the revalidation bound.
#[tokio::test]
async fn consistency_parameters_parse_on_every_live_read_route() {
    let fixture = fixture("consistency-parse", None).await;
    let bound = format!("consistency=eventual&max_staleness_ms={READ_REVALIDATION_BOUND_MS}");
    for route in fixture.live_read_routes() {
        for parameters in [
            "consistency=strong",
            "consistency=eventual",
            "consistency=eventual&max_staleness_ms=0",
            bound.as_str(),
        ] {
            let uri = with_parameters(&route, parameters);
            let (status, body) = fixture.get(&uri).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "{uri}: {}",
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
    let above = format!(
        "consistency=eventual&max_staleness_ms={}",
        READ_REVALIDATION_BOUND_MS + 1
    );
    let snapshot_eventual = format!("consistency=eventual&snapshot_id={SNAPSHOT_ID}");
    let snapshot_strong = format!("consistency=strong&snapshot_id={SNAPSHOT_ID}");
    for route in fixture.live_read_routes() {
        // The revision routes refuse a snapshot on its own, so they never
        // reach the consistency check with one.
        let takes_snapshot = !route.contains("revision");
        let mut cases = vec![
            ("max_staleness_ms=10", "max_staleness_ms"),
            ("consistency=strong&max_staleness_ms=10", "max_staleness_ms"),
            (above.as_str(), "max_staleness_ms"),
            (
                "consistency=eventual&max_staleness_ms=soon",
                "max_staleness_ms",
            ),
            ("consistency=sometimes", "consistency"),
        ];
        if takes_snapshot {
            cases.push((snapshot_eventual.as_str(), "consistency"));
            cases.push((snapshot_strong.as_str(), "consistency"));
        }
        for (parameters, param) in cases {
            let uri = with_parameters(&route, parameters);
            fixture.recording.reset();
            let (status, body) = fixture.get(&uri).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            let error: ErrorResponse = serde_json::from_slice(&body).expect("error body");
            assert_eq!(error.code, ErrorCode::InvalidRequest.as_str(), "{uri}");
            assert_eq!(error.param.as_deref(), Some(param), "{uri}");
            assert_eq!(fixture.recording.take(), [], "{uri}");
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
    let entry = "/v0/namespaces/demo/filesystem/entry?path=/file.txt";
    let children = "/v0/namespaces/demo/inodes/ino_1/children?include_attributes=false";
    for (setting, unset_confirms) in [(None, false), (Some(Consistency::Strong), true)] {
        let fixture = fixture("consistency-no-request", setting).await;
        for uri in [entry, children] {
            let (status, _) = fixture.get(uri).await;
            assert_eq!(status, StatusCode::OK, "warm read of {uri}");
        }
        for (parameters, confirms) in [
            (None, unset_confirms),
            (Some("consistency=eventual"), false),
            (Some("consistency=strong"), true),
        ] {
            fixture.recording.reset();
            for uri in [entry, children] {
                let uri = parameters.map_or(uri.to_owned(), |parameters| {
                    with_parameters(uri, parameters)
                });
                let (status, _) = fixture.get(&uri).await;
                assert_eq!(status, StatusCode::OK, "{uri}");
            }
            assert_eq!(
                !fixture.recording.take().is_empty(),
                confirms,
                "{setting:?} server, {parameters:?}"
            );
        }
    }
}
