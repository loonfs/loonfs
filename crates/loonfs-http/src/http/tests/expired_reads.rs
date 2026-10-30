//! The inode routes' responses for a file whose deletion the retention floor
//! reached.

use super::direct_download::LoopbackIssuer;
use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::JobMode;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::{DeleteDirectoryBehavior, DeleteOptions, SharedObjectStore};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_types::{ErrorCode, ErrorResponse};
use std::sync::Arc;
use tower::ServiceExt as _;

/// Every route that reads a file by its inode ID, as a method and the part of
/// the URI after the inode, with whether the route reads the file's history.
const INODE_ROUTES: [(&str, &str, bool); 7] = [
    ("GET", "?include_attributes=false", false),
    ("GET", "?include_attributes=true", false),
    ("GET", "/content", false),
    ("POST", "/downloads", false),
    ("GET", "/revisions", true),
    ("GET", "/revisions/1/content", true),
    ("POST", "/revisions/1/downloads", true),
];

/// Deletes `/old` with `/old/notes.txt` in it, then deletes `/kept.txt`, and
/// advances the retention floor to the deletion of `/old`, before the server
/// starts. Returns the public inode IDs of the two files.
async fn expire_old(store: &SharedObjectStore) -> (String, String) {
    let namespace_id = namespace_id("demo");
    let actor = test_actor();
    let writer = super::test_runtime(store.clone(), "expired-reads-writer").await;
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    for path in ["/kept.txt", "/old/notes.txt"] {
        namespace.put_file(path, b"one", &actor).await.expect("put");
    }
    let mut inodes = Vec::new();
    for path in ["/kept.txt", "/old/notes.txt"] {
        inodes.push(namespace.stat(path).await.expect("stat").inode_id);
    }
    let deletion = namespace
        .delete_path_with_options(
            "/old",
            &actor,
            &DeleteOptions {
                behavior: DeleteDirectoryBehavior::Recursive,
                ..DeleteOptions::default()
            },
        )
        .await
        .expect("delete directory");
    namespace
        .delete_path("/kept.txt", &actor)
        .await
        .expect("delete file");
    writer.shutdown().await.expect("writer shutdown");
    // The floor passes a commit once it's one grace window old, so this
    // engine's clock is that far ahead of the writer's.
    let clock = Arc::new(ManualClock::at_system_time());
    clock.advance_ms(loonfs::GC_DEFAULT_GRACE_WINDOW_MS);
    let maintenance = loonfs_core::NamespaceWriterEngine::writer(
        loonfs_core::test_support::namespace_store_with_wall_clock(
            store.clone(),
            &namespace_id,
            clock,
        ),
        writer_id("maintainer"),
    );
    maintenance.fold_wal().await.expect("fold the deletion");
    let advanced = maintenance
        .advance_retention_floor(loonfs_core::RetentionTarget::Seq(deletion.committed_seq))
        .await
        .expect("advance the floor to the deletion");
    assert_eq!(
        advanced.response.retention_floor_seq,
        deletion.committed_seq
    );
    (inodes[0].to_string(), inodes[1].to_string())
}

#[tokio::test]
async fn an_expired_subtree_is_an_unknown_inode_on_every_inode_route() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let (kept, notes) = expire_old(&store).await;
    let mut options = test_options(directory.path(), "expired-reads");
    options.grep = JobMode::Disabled;
    let (router, _state) = test_app(
        options,
        TestAppOptions {
            store: Some(store),
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

    for (inode, recoverable) in [(kept, true), (notes, false)] {
        for (method, route, history) in INODE_ROUTES {
            let uri = format!("/v0/namespaces/demo/inodes/{inode}{route}");
            let mut request = Request::builder()
                .method(method)
                .uri(&uri)
                .header("authorization", "Bearer test-token");
            if method == "POST" {
                request = request.header("content-type", "application/json");
            }
            let body = if method == "POST" { "{}" } else { "" };
            let response = router
                .clone()
                .oneshot(request.body(Body::from(body)).expect("request"))
                .await
                .expect("response");
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("body");
            // A deleted inode has no current entry or content, so only the
            // history routes serve a recoverable deletion.
            if recoverable && history {
                assert_eq!(status, StatusCode::OK, "{method} {uri}");
                continue;
            }
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {uri}");
            let error: ErrorResponse = serde_json::from_slice(&bytes).expect("error body");
            assert_eq!(
                error.code,
                ErrorCode::InodeNotFound.as_str(),
                "{method} {uri}"
            );
        }
    }
}
