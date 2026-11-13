//! The diff route: its pages, the parameters it rejects, and its error when
//! the diff starts below the retention floor.

use super::fixtures::{self, test_app, test_options, TestAppOptions};
use crate::JobMode;
use axum::http::StatusCode;
use loonfs::{DestinationBehavior, PutFileOptions, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_types::{CommitSeq, DiffEntryKind, DiffResponse, ErrorCode, ErrorResponse, RevisionNo};
use serde_json::Value;
use std::sync::Arc;
use tempfile::TempDir;

/// A server over a namespace `demo` that a separate writer runtime wrote
/// `/a.txt` into, then `/b.txt` and a second revision of `/a.txt`.
struct Fixture {
    /// The store's directory, which the server's options point at.
    directory: TempDir,
    store: SharedObjectStore,
    /// The sequence of the first write, which the diffs start after.
    from_seq: CommitSeq,
}

async fn fixture() -> Fixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let actor = test_actor();
    let writer = super::test_runtime(store.clone(), "diff-writer").await;
    writer
        .create_namespace(&namespace_id("demo"), &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id("demo"))
        .expect("open namespace");
    let from_seq = namespace
        .put_file("/a.txt", b"one", &actor)
        .await
        .expect("put")
        .committed_seq;
    namespace
        .put_file("/b.txt", b"two", &actor)
        .await
        .expect("put");
    namespace
        .put_file_with_options(
            "/a.txt",
            b"three",
            &actor,
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..PutFileOptions::default()
            },
        )
        .await
        .expect("replace");
    writer.shutdown().await.expect("writer shutdown");
    Fixture {
        directory,
        store,
        from_seq,
    }
}

impl Fixture {
    async fn router(&self) -> axum::Router {
        let mut options = test_options(self.directory.path(), "diff");
        options.grep = JobMode::Disabled;
        test_app(
            options,
            TestAppOptions {
                store: Some(self.store.clone()),
                ..TestAppOptions::default()
            },
        )
        .await
        .expect("app")
        .0
    }
}

async fn get(router: &axum::Router, uri: &str) -> (StatusCode, Vec<u8>) {
    let (status, _, body) = fixtures::send(router, fixtures::request("GET", uri, None, &[])).await;
    (status, body.to_vec())
}

/// Checks that a client reads every page by passing back the `to_seq` and
/// `next_after_seq` of the page before, and that each entry leaves out the
/// fields its kind doesn't report.
#[tokio::test]
async fn the_route_serves_pages_that_a_client_follows_to_the_end() {
    let fixture = fixture().await;
    let router = fixture.router().await;
    let base = format!("/v0/namespaces/demo/diff?from_seq={}", fixture.from_seq);

    let (status, body) = get(&router, &format!("{base}&limit=1")).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let json: Value = serde_json::from_slice(&body).expect("json");
    let created = &json["entries"][0];
    assert_eq!(
        (&created["kind"], &created["inode_kind"], &created["path"]),
        (
            &Value::from("created"),
            &Value::from("file"),
            &Value::from("/b.txt")
        )
    );
    assert!(created.get("previous_path").is_none(), "{created}");
    assert!(created.get("attributes_revision_no").is_none(), "{created}");
    let first: DiffResponse = serde_json::from_value(json).expect("page");
    let after_seq = first.next_after_seq.expect("a second page");

    let (status, body) = get(
        &router,
        &format!(
            "{base}&to_seq={}&after_seq={after_seq}&limit=1",
            first.to_seq
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let second: DiffResponse = serde_json::from_slice(&body).expect("page");
    assert_eq!((second.to_seq, second.next_after_seq), (first.to_seq, None));
    assert_eq!(second.entries.len(), 1);
    assert_eq!(second.entries[0].kind, DiffEntryKind::Changed);
    assert_eq!(second.entries[0].revision_no, Some(RevisionNo(2)));

    for (query, param) in [
        (format!("&to_seq={}", first.to_seq.0 + 1), "to_seq"),
        (
            format!("&after_seq={}", fixture.from_seq.0 - 1),
            "after_seq",
        ),
        ("&path=docs".to_owned(), "path"),
    ] {
        let (status, body) = get(&router, &format!("{base}{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        let error: ErrorResponse = serde_json::from_slice(&body).expect("error body");
        assert_eq!(error.code, ErrorCode::InvalidRequest.as_str(), "{query}");
        assert_eq!(error.param.as_deref(), Some(param), "{query}");
    }
}

#[tokio::test]
async fn a_diff_below_the_floor_returns_its_from_seq_and_the_floor() {
    let fixture = fixture().await;
    // The floor passes a commit once it's a grace window old, so this
    // engine's clock is that far ahead of the writer's.
    let clock = Arc::new(ManualClock::at_system_time());
    clock.advance_ms(loonfs::GC_GRACE_WINDOW_MS);
    let maintenance = loonfs_core::NamespaceWriterEngine::writer(
        loonfs_core::test_support::namespace_store_with_wall_clock(
            fixture.store.clone(),
            &namespace_id("demo"),
            clock,
        ),
        writer_id("maintainer"),
    );
    maintenance.fold_wal().await.expect("fold the writes");
    let floor = maintenance
        .advance_retention_floor(loonfs_core::RetentionTarget::Head)
        .await
        .expect("advance the floor")
        .response
        .retention_floor_seq;
    assert!(floor > fixture.from_seq);

    let router = fixture.router().await;
    let (status, body) = get(
        &router,
        &format!("/v0/namespaces/demo/diff?from_seq={}", fixture.from_seq),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let error: ErrorResponse = serde_json::from_slice(&body).expect("error body");
    assert_eq!(error.code, ErrorCode::BelowRetentionFloor.as_str());
    let details = error.details.expect("error details");
    assert_eq!(
        (
            details.from_seq,
            details.after_seq,
            details.retention_floor_seq
        ),
        (Some(fixture.from_seq), None, Some(floor))
    );
}
