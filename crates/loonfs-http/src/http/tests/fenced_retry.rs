//! A request that a newer writer fenced, sent once more in a new session.

use super::fixtures::test_app_with_sessions;
use super::namespaces::{create_directory, create_namespace, host, send, writer_epoch};
use super::*;
use axum::http::Method;
use loonfs_objectstore::layout::wal_no_of;
use loonfs_test_support::clock::bounded;
use loonfs_types::WalNo;
use std::assert_matches;
use std::sync::Mutex;
use std::time::Duration;

/// Returns the namespace's head, read from the store.
async fn head(
    state: &HttpState,
    namespace_id: &NamespaceId,
) -> loonfs_core::control::NamespaceReadState {
    loonfs_core::test_support::namespace_store(state.runtime.object_store().as_ref(), namespace_id)
        .load_read_state()
        .await
        .expect("read head")
}

/// Returns the WAL number of the namespace's newest WAL object.
async fn wal_tip(state: &HttpState, namespace_id: &NamespaceId) -> u64 {
    head(state, namespace_id).await.wal_no.0
}

/// Returns the sequence number of the namespace's newest commit.
async fn head_seq(state: &HttpState, namespace_id: &NamespaceId) -> u64 {
    head(state, namespace_id).await.seq.0
}

/// Returns the response's error body.
fn error_body(bytes: &Bytes) -> loonfs_types::ErrorResponse {
    serde_json::from_slice(bytes).expect("error body")
}

/// Matches the create-if-absent put of the WAL object whose number is in
/// `target`. Each test that uses it writes one namespace.
fn wal_put_at(target: &Mutex<Option<WalNo>>, operation: &OperationContext<'_>) -> bool {
    OperationSelector::PutCreateIfAbsent.matches(operation.kind())
        && wal_no_of(operation.key()).is_some_and(|wal_no| {
            *target
                .lock()
                .expect("the target lock should not be poisoned")
                == Some(wal_no)
        })
}

#[tokio::test]
async fn a_fenced_commit_is_retried_once_and_replays_its_receipt() {
    let (directory, router, state, _) = host("first-host").await;
    let (other_router, _, _) = test_app_with_sessions(
        test_options(directory.path(), "winning-host"),
        TestAppOptions::default(),
    )
    .await
    .expect("other host");
    let namespace_id = namespace_id("retried");
    create_namespace(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;

    // The commit is committed through the other host, which claims the
    // namespace, and its reply is lost, so the client sends it to this host.
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    let body = serde_json::json!({
        "commit_id": "sent-twice",
        "operations": [{"kind": "create_directory", "path": "/sent-twice"}],
    })
    .to_string();
    let (status, bytes) = send(&other_router, Method::POST, &uri, body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let committed: loonfs_types::Commit = serde_json::from_slice(&bytes).expect("commit");
    let winning_epoch = writer_epoch(&state, &namespace_id).await;

    // The held session finds the newer writer, and the second attempt claims
    // in a new session and finds the commit's receipt.
    let (status, bytes) = send(&router, Method::POST, &uri, body).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let replayed: loonfs_types::Commit = serde_json::from_slice(&bytes).expect("replay");
    assert_eq!(replayed.committed_seq, committed.committed_seq);
    assert_eq!(writer_epoch(&state, &namespace_id).await, winning_epoch + 1);
    create_directory(&router, &namespace_id, "/after").await;
    assert_eq!(writer_epoch(&state, &namespace_id).await, winning_epoch + 1);
}

#[tokio::test]
async fn a_retry_that_is_fenced_again_returns_writer_fenced_with_the_newer_writer() {
    let directory = tempdir().expect("tempdir");
    let target = Arc::new(Mutex::new(None));
    let blocking = Arc::new(BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        {
            let target = Arc::clone(&target);
            move |operation| wal_put_at(&target, operation)
        },
    ));
    let (router, state, _) = test_app_with_sessions(
        test_options(directory.path(), "displaced-host"),
        options_with_store(blocking.clone()),
    )
    .await
    .expect("host");
    let plain = || {
        options_with_store(Arc::new(
            LocalFsStore::new(directory.path()).expect("store"),
        ))
    };
    let (_, second, _) =
        test_app_with_sessions(test_options(directory.path(), "second-host"), plain())
            .await
            .expect("second host");
    let (_, third, _) =
        test_app_with_sessions(test_options(directory.path(), "third-host"), plain())
            .await
            .expect("third host");
    let namespace_id = namespace_id("displaced-twice");
    create_namespace(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;
    let actor = loonfs_test_support::test_actor();
    let open = |host: &HttpState| {
        let namespaces = Arc::clone(&host.namespaces);
        let namespace_id = namespace_id.clone();
        async move { namespaces.open(&namespace_id).await.expect("open") }
    };
    open(&second)
        .await
        .create_directory("/second", &actor)
        .await
        .expect("take over");

    // The retry's claim discovers the tail up to the second host's commit
    // and writes its fence at the next WAL number. The store keeps that put
    // waiting while the third host claims.
    let fence_no = wal_tip(&state, &namespace_id).await + 1;
    *target.lock().expect("target lock") = Some(WalNo(fence_no));
    blocking.block_next();
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    let body = serde_json::json!({
        "commit_id": "displaced-twice",
        "operations": [{"kind": "create_directory", "path": "/refused"}],
    })
    .to_string();
    let request = tokio::spawn({
        let router = router.clone();
        async move { send(&router, Method::POST, &uri, body).await }
    });
    bounded(Duration::from_secs(10), blocking.wait_until_blocked())
        .await
        .expect("the retry's fence put waits");
    open(&third)
        .await
        .create_directory("/third", &actor)
        .await
        .expect("claim again");
    blocking.release();

    let (status, bytes) = request.await.expect("join the request");
    assert_eq!(status, StatusCode::CONFLICT, "{bytes:?}");
    let error = error_body(&bytes);
    assert_eq!(error.code, ErrorCode::WriterFenced.as_str());
    let details = error.details.expect("fence details");
    let newest = head(&state, &namespace_id).await;
    let newest_writer = newest.writer.expect("the newest writer block");
    assert_eq!(details.active_writer_epoch, Some(newest.writer_epoch));
    assert_eq!(
        details.fenced_writer_epoch,
        Some(WriterEpoch::from(newest.writer_epoch.0 - 1))
    );
    let active_writer = details.active_writer.expect("the newer writer");
    assert_eq!(active_writer.active_writer_id, newest_writer.writer_id);
    assert_eq!(
        active_writer.active_acquired_at_ms,
        newest_writer.acquired_at_ms
    );
    assert!(
        error.message.contains(&format!(
            "(writer `{}`, acquired at {} ms)",
            newest_writer.writer_id, newest_writer.acquired_at_ms
        )),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn a_fenced_request_whose_retry_has_an_unknown_outcome_returns_outcome_unknown() {
    let directory = tempdir().expect("tempdir");
    let target = Arc::new(Mutex::new(None));
    let failing = Arc::new(FailStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        {
            let target = Arc::clone(&target);
            move |operation| wal_put_at(&target, operation)
        },
        InjectedError::Transport("the response was lost".to_owned()),
    ));
    failing.fail_all();
    let (router, state, _) = test_app_with_sessions(
        test_options(directory.path(), "unknown-host"),
        options_with_store(failing.clone()),
    )
    .await
    .expect("host");
    let (_, other, _) = test_app_with_sessions(
        test_options(directory.path(), "winning-host"),
        options_with_store(Arc::new(
            LocalFsStore::new(directory.path()).expect("store"),
        )),
    )
    .await
    .expect("other host");
    let namespace_id = namespace_id("unknown-retry");
    create_namespace(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;
    other
        .namespaces
        .open(&namespace_id)
        .await
        .expect("winner")
        .create_directory("/winner", &loonfs_test_support::test_actor())
        .await
        .expect("take over");

    // The retry's claim writes its fence after the winner's tail, and the
    // commit's put above that fence, and every later put at that number,
    // fails without an answer.
    let commit_no = wal_tip(&state, &namespace_id).await + 2;
    *target.lock().expect("target lock") = Some(WalNo(commit_no));
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    let body = serde_json::json!({
        "commit_id": "unknown-retry",
        "operations": [{"kind": "create_directory", "path": "/unknown"}],
    })
    .to_string();
    let (status, bytes) = send(&router, Method::POST, &uri, body).await;
    let error = error_body(&bytes);
    assert_eq!(error.code, ErrorCode::OutcomeUnknown.as_str(), "{status}");
    assert!(failing.attempts() > 0, "the retry sent the commit's put");
}

#[tokio::test]
async fn a_request_whose_unknown_outcome_is_followed_by_a_fence_returns_outcome_unknown_once() {
    let directory = tempdir().expect("tempdir");
    let target = Arc::new(Mutex::new(None));
    let predicate = |target: &Arc<Mutex<Option<WalNo>>>| {
        let target = Arc::clone(target);
        move |operation: &OperationContext<'_>| wal_put_at(&target, operation)
    };
    // The outer store fails the commit's put without an answer, and the
    // inner store keeps the next put at that number, the fence of the
    // session's new claim, waiting.
    let blocking = BlockingStore::matching(
        LocalFsStore::new(directory.path()).expect("store"),
        predicate(&target),
    );
    let failing = Arc::new(FailStore::matching(
        blocking,
        predicate(&target),
        InjectedError::Transport("the response was lost".to_owned()),
    ));
    let (router, state, _) = test_app_with_sessions(
        test_options(directory.path(), "unknown-host"),
        options_with_store(failing.clone()),
    )
    .await
    .expect("host");
    let (_, other, _) = test_app_with_sessions(
        test_options(directory.path(), "winning-host"),
        options_with_store(Arc::new(
            LocalFsStore::new(directory.path()).expect("store"),
        )),
    )
    .await
    .expect("other host");
    let namespace_id = namespace_id("unknown-then-fenced");
    create_namespace(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;
    let epoch_before = writer_epoch(&state, &namespace_id).await;

    let put_no = wal_tip(&state, &namespace_id).await + 1;
    *target.lock().expect("target lock") = Some(WalNo(put_no));
    failing.fail_next(1);
    failing.inner().block_next();
    let uri = format!("/v0/namespaces/{namespace_id}/commits");
    let body = serde_json::json!({
        "commit_id": "unknown-then-fenced",
        "operations": [{"kind": "create_directory", "path": "/unknown"}],
    })
    .to_string();
    let request = tokio::spawn({
        let router = router.clone();
        async move { send(&router, Method::POST, &uri, body).await }
    });
    bounded(
        Duration::from_secs(10),
        failing.inner().wait_until_blocked(),
    )
    .await
    .expect("the new claim's fence put waits");
    other
        .namespaces
        .open(&namespace_id)
        .await
        .expect("winner")
        .create_directory("/winner", &loonfs_test_support::test_actor())
        .await
        .expect("claim while the fence waits");
    failing.inner().release();

    let (_, bytes) = request.await.expect("join the request");
    let error = error_body(&bytes);
    assert_eq!(
        error.code,
        ErrorCode::OutcomeUnknown.as_str(),
        "{}",
        error.message
    );
    // The session's new claim and the winner's claim raised the epoch once
    // each, and no second attempt claimed again.
    assert_eq!(writer_epoch(&state, &namespace_id).await, epoch_before + 2);
    let newest = head(&state, &namespace_id).await;
    assert_eq!(
        newest
            .writer
            .expect("the newest writer block")
            .writer_id
            .as_str(),
        "winning-host"
    );
}

#[tokio::test]
async fn a_fenced_deletion_is_retried_and_checks_its_expected_head_again() {
    let (directory, router, state, namespaces) = host("deleting-host").await;
    let (other_router, _, _) = test_app_with_sessions(
        test_options(directory.path(), "winning-host"),
        TestAppOptions::default(),
    )
    .await
    .expect("other host");
    let [claimed, moved] = ["claimed", "moved-head"].map(namespace_id);
    for namespace_id in [&claimed, &moved] {
        create_namespace(&state, namespace_id).await;
        create_directory(&router, namespace_id, "/first").await;
    }
    let held_head = head_seq(&state, &claimed).await;
    assert_eq!(head_seq(&state, &moved).await, held_head);

    // The other host claims `claimed` with a deletion that its expected head
    // refuses, so the head stays where this host's session has it, and it
    // claims `moved` with a commit that moves the head.
    let uri = format!(
        "/v0/namespaces/{claimed}?expected_head_seq={}",
        held_head + 1
    );
    let (_, bytes) = send(&other_router, Method::DELETE, &uri, String::new()).await;
    assert_eq!(error_body(&bytes).code, ErrorCode::StaleHead.as_str());
    create_directory(&other_router, &moved, "/winner").await;

    // Each deletion passes its head check in the held session and then
    // finds the newer writer. The second attempt checks the expected head
    // again in a new session, against the head that the new claim found.
    let uri = format!("/v0/namespaces/{claimed}?expected_head_seq={held_head}");
    let (status, bytes) = send(&router, Method::DELETE, &uri, String::new()).await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    assert!(!namespaces.held_ids().contains(&claimed));
    let uri = format!("/v0/namespaces/{moved}?expected_head_seq={held_head}");
    let (_, bytes) = send(&router, Method::DELETE, &uri, String::new()).await;
    let error = error_body(&bytes);
    assert_eq!(
        error.code,
        ErrorCode::StaleHead.as_str(),
        "{}",
        error.message
    );
}

/// A host whose store keeps one WAL put waiting, a second host on the same
/// store, and a namespace with a fork that has two commits, which a merge
/// plans through.
struct FencedRangeHosts {
    _directory: tempfile::TempDir,
    router: axum::Router,
    state: HttpState,
    other: HttpState,
    blocking: Arc<BlockingStore<LocalFsStore>>,
    target: Arc<Mutex<Option<(String, WalNo)>>>,
    main: NamespaceId,
    fork: NamespaceId,
    planned_through_seq: u64,
}

impl FencedRangeHosts {
    async fn new(name: &str) -> Self {
        let directory = tempdir().expect("tempdir");
        let target = Arc::new(Mutex::new(None::<(String, WalNo)>));
        let blocking = Arc::new(BlockingStore::matching(
            LocalFsStore::new(directory.path()).expect("store"),
            {
                let target = Arc::clone(&target);
                move |operation| {
                    let key = operation.key();
                    OperationSelector::PutCreateIfAbsent.matches(operation.kind())
                        && target
                            .lock()
                            .expect("the target lock should not be poisoned")
                            .as_ref()
                            .is_some_and(|(prefix, wal_no)| {
                                key.starts_with(prefix.as_str()) && wal_no_of(key) == Some(*wal_no)
                            })
                }
            },
        ));
        let (router, state, _) = test_app_with_sessions(
            test_options(directory.path(), "first-host"),
            options_with_store(blocking.clone()),
        )
        .await
        .expect("host");
        let (_, other, _) = test_app_with_sessions(
            test_options(directory.path(), "winning-host"),
            options_with_store(Arc::new(
                LocalFsStore::new(directory.path()).expect("store"),
            )),
        )
        .await
        .expect("other host");
        let [main, fork] = [name, &format!("{name}-branch")].map(namespace_id);
        create_namespace(&state, &main).await;
        create_directory(&router, &main, "/first").await;
        state
            .runtime
            .fork_namespace(&main, &fork, &loonfs_test_support::test_actor())
            .await
            .expect("fork");
        for path in ["/one", "/two"] {
            create_directory(&router, &fork, path).await;
        }
        let planned_through_seq = head_seq(&state, &fork).await;
        Self {
            _directory: directory,
            router,
            state,
            other,
            blocking,
            target,
            main,
            fork,
            planned_through_seq,
        }
    }

    /// Keeps the next WAL put to `namespace_id` waiting.
    async fn hold_next_wal_put(&self, namespace_id: &NamespaceId) {
        let wal_no = WalNo(wal_tip(&self.state, namespace_id).await + 1);
        *self.target.lock().expect("target lock") =
            Some((loonfs_objectstore::keys::wal_prefix(namespace_id), wal_no));
        self.blocking.block_next();
    }

    /// Waits for the held put, lets the other host claim `namespace_id`, adds
    /// a third commit to the fork, and releases the put, which then finds the
    /// other host's claim.
    async fn take_over_and_extend_the_fork(&self, namespace_id: &NamespaceId) {
        bounded(Duration::from_secs(10), self.blocking.wait_until_blocked())
            .await
            .expect("the planned commit's put waits");
        self.other
            .namespaces
            .open(namespace_id)
            .await
            .expect("winner")
            .create_directory("/winner", &loonfs_test_support::test_actor())
            .await
            .expect("take over");
        create_directory(&self.router, &self.fork, "/three").await;
        assert!(head_seq(&self.state, &self.fork).await > self.planned_through_seq);
        self.blocking.release();
    }
}

#[tokio::test]
async fn a_fenced_merge_or_replay_is_retried_through_the_range_that_its_first_plan_resolved() {
    for route in ["merges", "replays"] {
        let hosts = FencedRangeHosts::new(&format!("fenced-{route}")).await;
        // A replay commits into a rebase's new fork, which this host claims
        // first, as it claimed the merge's destination.
        let target = match route {
            "merges" => hosts.main.clone(),
            _ => {
                let new_fork = namespace_id(&format!("fenced-{route}-rebased"));
                hosts
                    .state
                    .runtime
                    .fork_namespace_for_rebase(
                        &hosts.main,
                        &hosts.fork,
                        &new_fork,
                        &loonfs_test_support::test_actor(),
                    )
                    .await
                    .expect("rebase");
                create_directory(&hosts.router, &new_fork, "/held").await;
                new_fork
            }
        };
        let epoch_before = writer_epoch(&hosts.state, &target).await;

        // The request omits `through_seq`, so its plan resolves the range's
        // end to the fork's head, and the plan's commit waits at its put.
        hosts.hold_next_wal_put(&target).await;
        let uri = format!("/v0/namespaces/{target}/{route}");
        let body = serde_json::json!({
            "source_namespace_id": hosts.fork,
            "commit_id": format!("fenced-{route}"),
        })
        .to_string();
        let request = tokio::spawn({
            let router = hosts.router.clone();
            async move { send(&router, Method::POST, &uri, body).await }
        });
        hosts.take_over_and_extend_the_fork(&target).await;

        let (status, bytes) = request.await.expect("join the request");
        assert_eq!(status, StatusCode::OK, "{route}: {bytes:?}");
        let commit: loonfs_types::Commit = match route {
            "merges" => serde_json::from_slice(&bytes).expect("merge"),
            _ => {
                serde_json::from_slice::<loonfs_types::ReplayForkResponse>(&bytes)
                    .expect("replay")
                    .commit
            }
        };
        assert_eq!(commit.commit_id.as_str(), format!("fenced-{route}"));
        let merge = commit.merge.expect("the merge record");
        assert_eq!(
            merge.source_through_seq.0, hosts.planned_through_seq,
            "{route}"
        );
        // The other host's claim and this host's claim in the second attempt.
        assert_eq!(writer_epoch(&hosts.state, &target).await, epoch_before + 2);
    }
}

#[tokio::test]
async fn a_fenced_merge_reports_the_request_that_its_first_plan_resolved() {
    let hosts = FencedRangeHosts::new("fenced-embedded").await;
    let open = || hosts.state.namespaces.open(&hosts.main);
    let held = open().await.expect("open");
    let fork_point_seq = held
        .next_merge_range(&hosts.fork)
        .await
        .expect("next range")
        .fork_point_seq;
    let sent = loonfs::publish::MergeRequest {
        source_namespace_id: hosts.fork.clone(),
        through_seq: None,
        after_seq: None,
        commit_id: CommitId::parse("fenced-embedded").expect("commit id"),
        actor_id: loonfs_test_support::test_actor(),
        message: None,
    };

    hosts.hold_next_wal_put(&hosts.main).await;
    let merge = tokio::spawn({
        let sent = sent.clone();
        async move { held.merge(sent).await }
    });
    hosts.take_over_and_extend_the_fork(&hosts.main).await;
    let report = merge.await.expect("join the merge");
    assert_matches!(
        report.result,
        Err(loonfs::Error::Core(loonfs::CoreError::WriterFenced(_)))
    );
    let resolved = report.request;
    assert_eq!(resolved.commit_id, sent.commit_id);
    assert_eq!(resolved.after_seq, Some(fork_point_seq));
    assert_eq!(
        resolved.through_seq,
        Some(CommitSeq(hosts.planned_through_seq))
    );

    // The resolved request commits the first plan's range in a new session.
    // A request with that range and the caller's request both find its
    // receipt, so the second attempt's commit is the one that a request with
    // an explicit range would make.
    hosts
        .state
        .runtime
        .wait_for_fenced_session(&hosts.main)
        .await;
    let reopened = open().await.expect("reopen");
    let commit = reopened
        .merge(resolved.clone())
        .await
        .result
        .expect("merge again");
    assert_eq!(commit.commit_id, sent.commit_id);
    let range = commit.merge.as_ref().expect("the merge record");
    assert_eq!(range.source_after_seq, fork_point_seq);
    assert_eq!(range.source_through_seq.0, hosts.planned_through_seq);
    for request in [resolved, sent] {
        let replayed = reopened.merge(request).await;
        assert_eq!(replayed.result.expect("receipt"), commit);
    }
}

/// Checks that a maintenance job whose held session another host fenced is
/// retried once in a new session, which claims the namespace back and makes
/// the job's publication as the namespace's writer.
///
/// The other host's clock is two grace windows behind, so its first commit
/// is old enough for the floor to pass. This host's commit opens its held
/// session, and the other host's next commit claims the namespace. The
/// retention job's floor object then finds this host's session fenced, and
/// the retry's session claims and advances the floor past the old commit.
#[tokio::test]
async fn a_fenced_maintenance_job_is_retried_once_in_a_new_session() {
    let (directory, router, state, _) = host("job-host").await;
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::at_system_time());
    clock.set_ms(clock.now_ms() - 2 * loonfs::GC_GRACE_WINDOW_MS);
    let (past_router, _, _) = test_app_with_sessions(
        test_options(directory.path(), "past-host"),
        TestAppOptions {
            wall_clock: Some(clock as Arc<dyn loonfs::WallClock>),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("past host");
    let namespace_id = namespace_id("fenced-job");
    create_namespace(&state, &namespace_id).await;
    create_directory(&past_router, &namespace_id, "/old").await;
    let old_seq = head_seq(&state, &namespace_id).await;
    create_directory(&router, &namespace_id, "/first").await;
    create_directory(&past_router, &namespace_id, "/second").await;
    let winning_epoch = writer_epoch(&state, &namespace_id).await;
    let held = state.namespaces.open(&namespace_id).await.expect("open");
    assert_eq!(held.session_status(), loonfs::NamespaceSessionStatus::Open);

    let uri = format!("/v0/maintenance/namespaces/{namespace_id}/jobs");
    let (status, bytes) = send(
        &router,
        Method::POST,
        &uri,
        serde_json::json!({"kind": "retention"}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bytes:?}");
    let advanced: serde_json::Value = serde_json::from_slice(&bytes).expect("response");
    assert_eq!(advanced["retention_floor_seq"], old_seq, "{advanced}");
    assert_eq!(
        held.session_status(),
        loonfs::NamespaceSessionStatus::Fenced,
        "the first attempt ran in the held session and found it fenced"
    );
    assert_eq!(
        writer_epoch(&state, &namespace_id).await,
        winning_epoch + 1,
        "the retry's session claims the namespace back"
    );
    assert_eq!(
        head(&state, &namespace_id).await.retention_floor_seq.0,
        old_seq
    );
}
