//! The inode routes' responses for a file whose deletion the namespace's
//! writer expired.

use super::direct_download::LoopbackIssuer;
use super::fixtures::{test_app, test_options, TestAppOptions};
use crate::JobMode;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use loonfs::{DeleteDirectoryBehavior, DeleteOptions, SharedObjectStore};
use loonfs_core::publish::NamespaceCommitEngine;
use loonfs_core::time::Deadline;
use loonfs_core::MutationContext;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::presign::DirectTransferIssuers;
use loonfs_objectstore::timing::StdMonotonicTimer;
use loonfs_test_support::ids::{namespace_id, test_actor, writer_id};
use loonfs_types::{public_inode_id, CommitId, ErrorCode, ErrorResponse, ExpiredDeletion};
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

/// Deletes `/kept.txt`, deletes `/old` with `/old/notes.txt` in it, and
/// expires the deletion of `/old`, before the server starts. Returns the
/// public inode IDs of the two files.
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
    for path in ["/kept.txt", "/old/notes.txt", "/old"] {
        inodes.push(namespace.stat(path).await.expect("stat").inode_id);
    }
    namespace
        .delete_path("/kept.txt", &actor)
        .await
        .expect("delete file");
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
    writer.shutdown().await.expect("writer shutdown");
    NamespaceCommitEngine::new(namespace_id, Arc::new(tokio::sync::Semaphore::new(32)))
        .publish_expiry(
            store,
            CommitId::generate(),
            vec![ExpiredDeletion {
                inode_id: inodes[2],
                deletion_seq: deletion.committed_seq,
            }],
            &MutationContext {
                writer_id: writer_id("expiry-writer"),
                now_ms: 1_000,
            },
            &Deadline::start(Arc::new(StdMonotonicTimer::default())),
        )
        .await
        .results
        .pop()
        .expect("one result")
        .expect("expire the deletion");
    (
        public_inode_id::encode(inodes[0]),
        public_inode_id::encode(inodes[1]),
    )
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
