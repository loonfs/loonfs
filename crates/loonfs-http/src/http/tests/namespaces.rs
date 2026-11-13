//! The writable namespace handles the host holds across requests.

use super::fixtures::{self, test_app_with_sessions};
use super::*;
use crate::http::error::ApiResponseError;
use crate::{Namespaces, WriterSessions};
use axum::http::Method;
use axum::response::IntoResponse;
use loonfs::NamespaceSessionStatus;
use loonfs_test_support::stores::BlockingStore;
use std::assert_matches;
use std::num::NonZeroU64;

async fn send(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: String,
) -> (StatusCode, Bytes) {
    let actor = [("Loonfs-Actor", "namespace-writers")];
    let request = fixtures::request(method.as_str(), uri, Some(&body), &actor);
    let (status, _, body) = fixtures::send(router, request).await;
    (status, body)
}

async fn create_directory(router: &axum::Router, namespace_id: &NamespaceId, path: &str) {
    let body = serde_json::json!({
        "commit_id": format!("create{}", path.replace('/', "-")),
        "operations": [{"kind": "create_directory", "path": path}],
    });
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    assert_eq!(
        send(router, Method::POST, &uri, body.to_string()).await.0,
        StatusCode::OK
    );
}

async fn create_direct_put_upload(
    router: &axum::Router,
    namespace_id: &NamespaceId,
) -> (StatusCode, String) {
    let uri = format!("/v0/namespaces/{namespace_id}/uploads");
    let body = serde_json::json!({"mode": "direct_put", "size_bytes": 5});
    let (status, body) = send(router, Method::POST, &uri, body.to_string()).await;
    let error: loonfs_types::ErrorResponse = serde_json::from_slice(&body).expect("error body");
    (status, error.code)
}

async fn host(
    writer_id: &str,
) -> (
    tempfile::TempDir,
    axum::Router,
    HttpState,
    Arc<Namespaces<()>>,
) {
    let directory = tempdir().expect("tempdir");
    let (router, state, namespaces) = test_app_with_sessions(
        test_options(directory.path(), writer_id),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    (directory, router, state, namespaces)
}

async fn create_namespace(state: &HttpState, namespace_id: &NamespaceId) {
    state
        .runtime
        .create_namespace(namespace_id, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
}

async fn writer_epoch(state: &HttpState, namespace_id: &NamespaceId) -> u64 {
    loonfs_core::test_support::namespace_store(state.runtime.object_store().as_ref(), namespace_id)
        .load_read_state()
        .await
        .expect("read head")
        .writer_epoch
        .0
}

#[tokio::test]
async fn two_commits_to_one_namespace_use_one_writer_epoch() {
    let (_directory, router, state, _) = host("one-epoch-host").await;
    let namespace_id = namespace_id("one-epoch");
    create_namespace(&state, &namespace_id).await;
    let created = writer_epoch(&state, &namespace_id).await;

    create_directory(&router, &namespace_id, "/first").await;
    create_directory(&router, &namespace_id, "/second").await;

    assert_eq!(writer_epoch(&state, &namespace_id).await, created + 1);
}

#[tokio::test]
async fn a_deleted_namespace_leaves_no_writer_handle_in_the_host() {
    let (_directory, router, state, namespaces) = host("delete-host").await;
    let deleted = namespace_id("deleted");
    let kept = namespace_id("kept");
    for namespace_id in [&deleted, &kept] {
        create_namespace(&state, namespace_id).await;
        create_directory(&router, namespace_id, "/written").await;
    }

    let uri = format!("/v0/namespaces/{deleted}");
    assert_eq!(
        send(&router, Method::DELETE, &uri, String::new()).await.0,
        StatusCode::OK
    );

    let closed = namespaces.close(&deleted).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
    let closed = namespaces.close(&kept).await.expect("close");
    assert!(closed.is_some(), "the host holds the written namespace");
}

#[tokio::test]
async fn a_session_that_finds_its_namespace_deleted_leaves_the_host_at_the_next_open() {
    let (directory, router, state, namespaces) = host("holding-host").await;
    let (other_router, _, _) = test_app_with_sessions(
        test_options(directory.path(), "deleting-host"),
        TestAppOptions::default(),
    )
    .await
    .expect("other host");
    let deleted = namespace_id("deleted");
    create_namespace(&state, &deleted).await;
    let held = state.namespaces.open(&deleted).await.expect("held session");
    let uri = format!("/v0/namespaces/{deleted}");
    assert_eq!(
        send(&other_router, Method::DELETE, &uri, String::new())
            .await
            .0,
        StatusCode::OK
    );

    // The held session's first publish finds the other writer's tombstone,
    // and the next request's open drops the session.
    let commits = format!("{uri}/commits");
    let body = serde_json::json!({
        "commit_id": "after-delete",
        "operations": [{"kind": "create_directory", "path": "/late"}],
    })
    .to_string();
    for held_after in [true, false] {
        let (_, bytes) = send(&router, Method::POST, &commits, body.clone()).await;
        let error: loonfs_types::ErrorResponse = serde_json::from_slice(&bytes).expect("error");
        assert_eq!(error.code, ErrorCode::NamespaceDeleted.as_str());
        assert_eq!(held.session_status(), NamespaceSessionStatus::Closed);
        assert_eq!(namespaces.held_ids().contains(&deleted), held_after);
    }
}

#[tokio::test]
async fn a_create_request_with_a_naming_field_is_rejected_and_leaves_no_writer_handle() {
    let (_directory, router, _, namespaces) = host("naming-field-host").await;
    let named = namespace_id("named");
    // Every namespace compares sibling names by the same rule, so the create
    // request has no `naming` field.
    let body = serde_json::json!({"namespace_id": "named", "naming": "case_insensitive"});
    let (status, body) = send(&router, Method::POST, "/v0/namespaces", body.to_string()).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error: loonfs_types::ErrorResponse = serde_json::from_slice(&body).expect("error body");
    assert_eq!(error.code, ErrorCode::InvalidRequest.as_str());
    assert!(
        error.message.contains("unknown field `naming`"),
        "{}",
        error.message
    );
    let uri = format!("/v0/namespaces/{named}");
    assert_eq!(
        send(&router, Method::GET, &uri, String::new()).await.0,
        StatusCode::NOT_FOUND
    );
    let closed = namespaces.close(&named).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
}

#[tokio::test]
async fn a_request_for_a_missing_namespace_leaves_no_writer_handle_in_the_host() {
    let (_directory, router, _, namespaces) = host("missing-host").await;
    let missing = namespace_id("missing");
    let body = serde_json::json!({
        "commit_id": "create-missing",
        "operations": [{"kind": "create_directory", "path": "/missing"}],
    });
    let uri = format!("/v0/namespaces/{missing}/commits");
    assert_eq!(
        send(&router, Method::POST, &uri, body.to_string()).await.0,
        StatusCode::NOT_FOUND
    );

    let closed = namespaces.close(&missing).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
}

#[tokio::test]
async fn a_rejected_upload_for_a_missing_namespace_answers_not_found_and_leaves_no_writer_handle() {
    let (_directory, router, _, namespaces) = host("missing-upload-host").await;
    let missing = namespace_id("missing");

    assert_eq!(
        create_direct_put_upload(&router, &missing).await,
        (
            StatusCode::NOT_FOUND,
            ErrorCode::NamespaceNotFound.as_str().to_owned()
        )
    );

    let closed = namespaces.close(&missing).await.expect("close");
    assert!(closed.is_none(), "the host kept a handle: {closed:?}");
}

#[tokio::test]
async fn a_rejected_upload_for_an_existing_namespace_keeps_its_writer_handle() {
    let (_directory, router, state, namespaces) = host("existing-upload-host").await;
    let existing = namespace_id("existing");
    create_namespace(&state, &existing).await;

    // The local-storage fixture cannot presign, so it refuses `direct_put`.
    assert_eq!(
        create_direct_put_upload(&router, &existing).await,
        (
            StatusCode::NOT_IMPLEMENTED,
            ErrorCode::NotSupported.as_str().to_owned()
        )
    );

    let closed = namespaces.close(&existing).await.expect("close");
    assert!(closed.is_some(), "the host holds the existing namespace");
}

#[tokio::test]
async fn a_close_during_an_open_existence_read_leaves_an_open_writer_handle_in_the_host() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("close-during-open");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::hint(&namespace_id),
        OperationSelector::Read,
    ));
    let (router, state, namespaces) = test_app_with_sessions(
        test_options(directory.path(), "close-during-open-host"),
        options_with_store(blocking.clone()),
    )
    .await
    .expect("app");
    create_namespace(&state, &namespace_id).await;

    blocking.block_next();
    let paused = tokio::spawn({
        let namespaces = Arc::clone(&namespaces);
        let namespace_id = namespace_id.clone();
        async move { namespaces.open(&namespace_id).await }
    });
    blocking.wait_until_blocked().await;
    state
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open while the first open waits");
    let closed = namespaces.close(&namespace_id).await.expect("close");
    assert!(closed.is_some(), "the host holds the opened namespace");
    blocking.release();
    paused
        .await
        .expect("join the paused open")
        .expect("the paused open");

    let held = state.namespaces.open(&namespace_id).await.expect("open");
    assert_eq!(held.session_status(), NamespaceSessionStatus::Open);
    create_directory(&router, &namespace_id, "/after-close").await;
}

#[tokio::test]
async fn concurrent_opens_drop_a_fenced_session_once_and_keep_other_sessions_and_replacements() {
    let (directory, router, state, namespaces) = host("fenced-host").await;
    let (_, other, other_namespaces) = test_app_with_sessions(
        test_options(directory.path(), "winning-host"),
        TestAppOptions::default(),
    )
    .await
    .expect("other host");
    let fenced = namespace_id("fenced");
    let kept = namespace_id("kept");
    for namespace_id in [&fenced, &kept] {
        create_namespace(&state, namespace_id).await;
        create_directory(&router, namespace_id, "/first").await;
    }
    let old = state.namespaces.open(&fenced).await.expect("old session");
    let kept_handle = state.namespaces.open(&kept).await.expect("kept session");
    let winner = other.namespaces.open(&fenced).await.expect("winner");
    winner
        .create_directory("/winner", &loonfs_test_support::test_actor())
        .await
        .expect("take over");
    let winning_epoch = writer_epoch(&state, &fenced).await;
    let errors = futures::future::join_all((0..4).map(|index| {
        let old = old.clone();
        async move {
            old.create_directory(
                &format!("/refused-{index}"),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect_err("old session is fenced")
        }
    }))
    .await;
    for error in &errors {
        assert_eq!(error.code(), ErrorCode::WriterFenced);
        let response = ApiResponseError::runtime(error.clone()).into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
    assert_eq!(writer_epoch(&state, &fenced).await, winning_epoch);
    assert_eq!(old.session_status(), NamespaceSessionStatus::Fenced);

    // Every open finds the fenced session or the one replacement that the
    // first open put in its place.
    let replacements =
        futures::future::join_all((0..4).map(|_| state.namespaces.open(&fenced))).await;
    for replacement in &replacements {
        let replacement = replacement.as_ref().expect("replacement");
        assert_eq!(replacement.session_status(), NamespaceSessionStatus::Open);
    }
    assert_eq!(namespaces.held_ids(), [fenced.clone(), kept.clone()]);

    create_directory(&router, &fenced, "/replacement").await;
    let replacement = state.namespaces.open(&fenced).await.expect("replacement");
    old.create_directory("/late", &loonfs_test_support::test_actor())
        .await
        .expect_err("a late request through the old session is fenced");
    assert_eq!(namespaces.held_ids().len(), 2, "the replacement stays held");
    drop(old);
    create_directory(&router, &fenced, "/still-replacement").await;
    let held = state
        .namespaces
        .open(&fenced)
        .await
        .expect("held replacement");
    assert_eq!(held.last_published_seq(), replacement.last_published_seq());
    assert_eq!(writer_epoch(&state, &fenced).await, winning_epoch + 1);

    create_directory(&router, &kept, "/still-kept").await;
    let held = state
        .namespaces
        .open(&kept)
        .await
        .expect("held other namespace");
    assert_eq!(held.last_published_seq(), kept_handle.last_published_seq());

    let error = winner
        .create_directory("/refused-winner", &loonfs_test_support::test_actor())
        .await
        .expect_err("winner was fenced back");
    assert_eq!(error.code(), ErrorCode::WriterFenced);
    assert_eq!(winner.session_status(), NamespaceSessionStatus::Fenced);
    assert!(other_namespaces.held_ids().contains(&fenced));
    let reopened = other.namespaces.open(&fenced).await.expect("reopen");
    assert_eq!(reopened.session_status(), NamespaceSessionStatus::Open);
}

#[tokio::test]
async fn a_fenced_session_with_a_running_fold_refuses_open_until_its_work_ends() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("ending");
    let blocking = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::manifest(&namespace_id),
        OperationSelector::PutCreateIfAbsent,
    ));
    // The claim's fence and the first directory leave two WAL objects, so
    // the third, the put's, starts the session's fold.
    let (router, state, namespaces) = test_app_with_sessions(
        test_options(directory.path(), "ending-host"),
        TestAppOptions {
            session_fold_at_wal_objects: NonZeroU64::new(3),
            ..options_with_store(blocking.clone())
        },
    )
    .await
    .expect("host");
    let (_, other) = test_app(
        test_options(directory.path(), "winning-host"),
        options_with_store(Arc::new(
            LocalFsStore::new(directory.path()).expect("other store"),
        )),
    )
    .await
    .expect("other host");
    create_namespace(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;
    let old = state
        .namespaces
        .open(&namespace_id)
        .await
        .expect("old handle");
    blocking.block_next();
    old.put_file("/fold", b"body", &loonfs_test_support::test_actor())
        .await
        .expect("start fold");
    blocking.wait_until_blocked().await;
    let winner = other.namespaces.open(&namespace_id).await.expect("winner");
    winner
        .create_directory("/winner", &loonfs_test_support::test_actor())
        .await
        .expect("take over");

    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    let body = serde_json::json!({
        "commit_id": "during-ending",
        "operations": [{"kind": "create_directory", "path": "/after"}],
    })
    .to_string();
    let (status, bytes) = send(&router, Method::POST, &uri, body.clone()).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let error: loonfs_types::ErrorResponse = serde_json::from_slice(&bytes).expect("fenced error");
    assert_eq!(error.code, ErrorCode::WriterFenced.as_str());
    let (status, bytes) = send(&router, Method::POST, &uri, body.clone()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let error: loonfs_types::ErrorResponse = serde_json::from_slice(&bytes).expect("closing error");
    assert_eq!(error.code, ErrorCode::WriterSessionClosed.as_str());
    assert!(namespaces.held_ids().is_empty());

    blocking.release();
    state.runtime.drain().await.expect("old work ends");
    assert_eq!(
        send(&router, Method::POST, &uri, body).await.0,
        StatusCode::OK
    );
    let replacement = state
        .namespaces
        .open(&namespace_id)
        .await
        .expect("new handle");
    assert_eq!(replacement.session_status(), NamespaceSessionStatus::Open);
    assert_eq!(old.session_status(), NamespaceSessionStatus::Fenced);
    old.close().await.expect("close old clone");
    create_directory(&router, &namespace_id, "/replacement-still-held").await;
    assert_eq!(
        replacement.last_published_seq(),
        state
            .namespaces
            .open(&namespace_id)
            .await
            .expect("held")
            .last_published_seq()
    );
}

#[tokio::test]
async fn an_idle_close_rechecks_the_seq_and_counts_subject_scoped_handles() {
    let directory = tempdir().expect("tempdir");
    let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let (_router, state) = test_app(
        test_options(directory.path(), "idle-close-host"),
        TestAppOptions {
            monotonic_timer: Some(timer.clone()),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");
    let namespace_id = namespace_id("idle-close-checks");
    create_namespace(&state, &namespace_id).await;
    let namespaces = Namespaces::<()>::new(state.runtime.clone());
    let handle = namespaces.open(&namespace_id).await.expect("open");
    let initial = handle
        .put_file("/first", b"first", &loonfs_test_support::test_actor())
        .await
        .expect("publish");
    let current = handle
        .put_file("/second", b"second", &loonfs_test_support::test_actor())
        .await
        .expect("publish again");
    let scoped = handle.with_subject(loonfs_types::Subject {
        principal_scope: loonfs_types::PrincipalScope::parse("scope").expect("scope"),
        subject_id: loonfs_types::SubjectId::parse("reader").expect("subject"),
        principals: loonfs_types::PrincipalSet::new(std::collections::BTreeSet::from([
            loonfs_types::PrincipalId::parse("reader").expect("principal"),
        ]))
        .expect("principals"),
    });
    drop(handle);
    timer.advance_ms(2);
    assert!(namespaces
        .close_if_idle(&namespace_id, Some(current.committed_seq), 1, |_| true)
        .await
        .expect("check scoped clone")
        .is_none());
    drop(scoped);
    assert!(namespaces
        .close_if_idle(&namespace_id, None, 1, |_| true)
        .await
        .expect("check a publish after observing no seq")
        .is_none());
    assert!(namespaces
        .close_if_idle(&namespace_id, Some(initial.committed_seq), 1, |_| true)
        .await
        .expect("check stale seq")
        .is_none());
    namespaces.mark_index_dirty(&namespace_id);
    assert!(namespaces
        .close_if_idle(&namespace_id, Some(current.committed_seq), 1, |held| !held
            .index_dirty)
        .await
        .expect("recheck quiet under the table lock")
        .is_none());
    let closed = namespaces
        .close_if_idle(&namespace_id, Some(current.committed_seq), 1, |_| true)
        .await
        .expect("close idle session")
        .expect("session closed");
    assert_eq!(closed.drained_commits, Some(0));
    assert!(namespaces.held_ids().is_empty());
}

#[tokio::test]
async fn a_close_only_delays_opens_of_its_namespace_even_if_cancelled() {
    for (idle, cancel) in [(false, false), (false, true), (true, false), (true, true)] {
        let directory = tempdir().expect("tempdir");
        let other = namespace_id("other");
        let namespace_id = namespace_id("closing");
        let store = Arc::new(BlockingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::manifest(&namespace_id),
            OperationSelector::PutCreateIfAbsent,
        ));
        let timer = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
        // The claim's fence and the first directory leave two WAL objects,
        // so the third, the put's, starts the session's fold.
        let (_, state, namespaces) = test_app_with_sessions(
            test_options(directory.path(), "closing-host"),
            TestAppOptions {
                monotonic_timer: Some(timer.clone()),
                session_fold_at_wal_objects: NonZeroU64::new(3),
                ..options_with_store(store.clone())
            },
        )
        .await
        .expect("host");
        create_namespace(&state, &namespace_id).await;
        create_namespace(&state, &other).await;
        let old = state.namespaces.open(&namespace_id).await.expect("open");
        old.create_directory("/first", &loonfs_test_support::test_actor())
            .await
            .expect("first publish");
        let epoch = writer_epoch(&state, &namespace_id).await;
        store.block_next();
        old.put_file("/fold", b"body", &loonfs_test_support::test_actor())
            .await
            .expect("publish");
        store.wait_until_blocked().await;
        let seq = old.last_published_seq();
        let old_entry = namespaces.entry(&namespace_id).expect("held entry");
        drop(old);
        timer.advance_ms(2);
        let mut closing = Box::pin(async {
            if idle {
                namespaces
                    .close_if_idle(&namespace_id, seq, 1, |_| true)
                    .await
            } else {
                namespaces.close(&namespace_id).await
            }
        });
        assert!(futures::poll!(closing.as_mut()).is_pending());
        let mut opening = Box::pin(state.namespaces.open(&namespace_id));
        assert!(futures::poll!(opening.as_mut()).is_pending());
        let mut also_opening = Box::pin(state.namespaces.open(&namespace_id));
        assert!(futures::poll!(also_opening.as_mut()).is_pending());
        let other_handle = loonfs_test_support::clock::bounded(
            std::time::Duration::from_secs(5),
            state.namespaces.open(&other),
        )
        .await
        .expect("another namespace opens while the drain is parked")
        .expect("open other namespace");
        assert_matches!(
            futures::poll!(Box::pin(state.namespaces.open(&other))),
            std::task::Poll::Ready(Ok(_))
        );
        other_handle
            .create_directory("/during-close", &loonfs_test_support::test_actor())
            .await
            .expect("publish while the other namespace drains");
        assert!(futures::poll!(closing.as_mut()).is_pending());
        assert!(futures::poll!(opening.as_mut()).is_pending());
        let fresh = if cancel {
            drop(closing);
            assert!(futures::poll!(opening.as_mut()).is_pending());
            store.release();
            opening.await.expect("open waits without an error")
        } else {
            store.release();
            let fresh = opening.await.expect("open finishes the drain");
            assert!(closing.await.expect("close").is_some());
            fresh
        };
        assert_eq!(fresh.last_published_seq(), None);
        assert_eq!(
            old_entry.lock().expect("entry").handle.session_status(),
            NamespaceSessionStatus::Closed
        );
        fresh
            .create_directory("/fresh", &loonfs_test_support::test_actor())
            .await
            .expect("publish with fresh session");
        assert_eq!(writer_epoch(&state, &namespace_id).await, epoch + 1);
        let also_fresh = also_opening.await.expect("all waiting opens finish");
        assert_eq!(also_fresh.last_published_seq(), fresh.last_published_seq());
        state.runtime.shutdown().await.expect("shutdown");
    }
}
