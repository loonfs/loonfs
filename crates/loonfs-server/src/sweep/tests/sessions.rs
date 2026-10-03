//! Session progress, collection cadence, and store request costs.

use super::{seed_unfolded_tail, seed_writer, sweep_config, SweepServer, DAY_MS};
use crate::sweep::lock;
use loonfs::SharedObjectStore;
use loonfs_grep::manifest::GrepIndexStatus;
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn session_passes_retry_an_idle_tail_then_read_nothing_until_another_publish() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("held-tail");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    store.reset();
    for _ in 0..3 {
        server.sweep.run_session_pass(true).await;
    }
    assert!(
        store.take().is_empty(),
        "a session that published nothing is skipped"
    );

    for (index, contents) in [b"first", b"again"].into_iter().enumerate() {
        held.put_file(&format!("/note-{index}"), contents, &test_actor())
            .await
            .expect("publish");
        for _ in 0..3 {
            store.reset();
            server.sweep.run_session_pass(false).await;
            assert!(
                !store.take().is_empty(),
                "the tail still waits for its idle fold"
            );
            assert!(server.wal_tail_objects(&namespace_id).await > 0);
        }
        server.clock.advance_ms(config.idle_fold_after_ms);
        server.sweep.run_session_pass(false).await;
        assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
        store.reset();
        for _ in 0..3 {
            server.sweep.run_session_pass(false).await;
        }
        assert!(
            store.take().is_empty(),
            "caught-up sessions cost no requests"
        );
    }
    server
        .sweep
        .inner
        .namespaces
        .close(&namespace_id)
        .await
        .expect("close namespace");
    server.sweep.run_session_pass(false).await;
    assert!(lock(&server.sweep.inner.sessions).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn session_passes_leave_unheld_namespaces_for_the_full_pass() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let (writer, now_ms) = seed_writer(&shared).await;
    let namespace_id = namespace_id("unheld");
    seed_unfolded_tail(&writer, &namespace_id).await;
    writer.shutdown().await.expect("shutdown writer");
    let server = SweepServer::start(store.clone(), &sweep_config(None), now_ms + DAY_MS).await;
    store.reset();
    for collect in [false, true, false] {
        server.sweep.run_session_pass(collect).await;
    }
    assert!(store.take().is_empty());
    assert!(server.wal_tail_objects(&namespace_id).await > 0);
    assert_eq!(server.sweep.run_pass(true).await.expect("full pass"), 1);
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
}

#[tokio::test]
async fn session_passes_continue_a_metadata_backlog_without_another_publish() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let server = SweepServer::start(store.clone(), &sweep_config(None), 0).await;
    let namespace_id = namespace_id("held-backlog");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    for index in 0..3 * loonfs_types::format::sst_blocks::DEFAULT_MAX_DELTA_RUNS {
        held.put_file(&format!("/file-{index}"), b"data", &test_actor())
            .await
            .expect("publish");
        server
            .maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold");
    }
    let before = server
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics")
        .current_manifest_no
        .expect("manifest");
    server.sweep.run_session_pass(false).await;
    let after = server
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics")
        .current_manifest_no
        .expect("manifest");
    assert_eq!(
        after.0 - before.0,
        17,
        "one epoch claim and 16 compaction units"
    );
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert!(
        !store.take().is_empty(),
        "the unit cap leaves this session due"
    );
    for _ in 0..8 {
        server.sweep.run_session_pass(false).await;
    }
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert!(store.take().is_empty(), "the backlog eventually catches up");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_session_stays_due_until_its_grep_build_catches_up() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("held-index");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    for index in 0..20 {
        held.put_file(&format!("/file-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
    }
    server
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    drop(held);
    server
        .clock
        .advance_ms(config.idle_session_close_after_ms + 1);
    server.sweep.run_session_pass(false).await;
    assert_eq!(server.sweep.inner.namespaces.held().len(), 1);
    assert!(matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. }
    ));
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert!(!store.take().is_empty());
    assert!(matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Active { .. }
    ));
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert!(
        !store.take().is_empty(),
        "the completed reorganization needs a step that reports nothing left"
    );
    server.sweep.run_session_pass(false).await;
    assert!(store.take().is_empty(), "closing costs zero store requests");
    assert!(server.sweep.inner.namespaces.held().is_empty());
    assert!(lock(&server.sweep.inner.sessions).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn session_passes_continue_grep_reorganization_until_nothing_remains() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    config.idle_fold_after_ms = 0;
    let mut server = SweepServer::start(store.clone(), &config, 0).await;
    let policy = &mut Arc::get_mut(&mut server.sweep.inner)
        .expect("one sweep")
        .grep
        .as_mut()
        .expect("grep indexing")
        .policy;
    policy.max_delta_runs = NonZeroUsize::new(2).expect("positive limit");
    policy.max_decoded_input_rows_per_step = NonZeroUsize::new(1).expect("positive limit");
    let namespace_id = namespace_id("pending-reorganization");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    for index in 0..2 {
        held.put_file(&format!("/file-{index}"), b"abc", &test_actor())
            .await
            .expect("publish");
    }
    let worker = server.grep_worker.as_ref().expect("grep worker");
    worker.enable(&namespace_id).await.expect("enable grep");
    server.sweep.run_session_pass(false).await;
    assert!(
        worker
            .get_grep_index(&namespace_id)
            .await
            .expect("index progress")
            .reorganize_pending
    );

    let mut pending = true;
    for _ in 0..32 {
        store.reset();
        server.sweep.run_session_pass(false).await;
        assert!(
            !store.take().is_empty(),
            "a session with unfinished reorganization stays selected"
        );
        pending = worker
            .get_grep_index(&namespace_id)
            .await
            .expect("index progress")
            .reorganize_pending;
        if !pending {
            break;
        }
    }
    assert!(!pending, "consecutive passes finish reorganization");
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert!(
        !store.take().is_empty(),
        "a completed unit still needs a step that reports nothing left"
    );
    server.sweep.run_session_pass(false).await;
    assert!(
        store.take().is_empty(),
        "a finished session costs no requests"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn the_loop_collects_moved_sessions_on_the_collection_interval() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.maintenance_interval_ms = 10_000;
    config.gc_interval_ms = 30_000;
    config.full_sweep_interval_ms = 120_000;
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("collected-session");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    let initial = held
        .put_file("/note", b"initial", &test_actor())
        .await
        .expect("publish");
    let running = server.sweep.start();
    while lock(&server.sweep.inner.sessions)
        .get(&namespace_id)
        .and_then(|progress| progress.collected)
        != Some(initial.committed_seq)
    {
        tokio::task::yield_now().await;
    }
    {
        let _pass = server.sweep.inner.pass.lock().await;
    }
    assert_eq!(
        server.counter("loonfs.maintenance.sweep_passes", &[("result", "ok")]),
        1
    );
    let committed = held
        .put_file("/moved", b"moved", &test_actor())
        .await
        .expect("publish");
    store.reset();
    tokio::time::advance(Duration::from_secs(10)).await;
    while lock(&server.sweep.inner.sessions)
        .get(&namespace_id)
        .and_then(|progress| progress.maintained)
        != Some(committed.committed_seq)
    {
        tokio::task::yield_now().await;
    }
    assert!(!store
        .take()
        .iter()
        .any(|operation| matches!(operation, RecordedOperation::List { .. })));
    tokio::time::advance(Duration::from_secs(10)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(10)).await;
    while lock(&server.sweep.inner.sessions)
        .get(&namespace_id)
        .and_then(|progress| progress.collected)
        != Some(committed.committed_seq)
    {
        tokio::task::yield_now().await;
    }
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|operation| matches!(operation, RecordedOperation::List { .. }))
            .count(),
        9,
        "collection lists core and grep garbage without listing namespaces"
    );
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(10)).await;
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
    }
    assert!(
        store.take().is_empty(),
        "the next collection interval skips an idle session"
    );
    tokio::time::advance(Duration::from_secs(60)).await;
    while server.counter("loonfs.maintenance.sweep_passes", &[("result", "ok")]) < 2 {
        tokio::task::yield_now().await;
    }
    assert!(store.take().iter().any(|operation| matches!(
        operation,
        RecordedOperation::List { .. }
    ) && operation.key() == "namespaces/"));
    running.cancel();
    running.task.await.expect("stop sweep");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_session_visit_records_the_seq_before_a_concurrent_publish() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("concurrent-publish");
    let recording = RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    );
    let store = Arc::new(BlockingStore::new(
        recording,
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id)),
        OperationClass::Any,
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("hold namespace");
    held.put_file("/first", b"first", &test_actor())
        .await
        .expect("publish");
    store.block_next();
    let pass = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.run_session_pass(false).await }
    });
    store.wait_until_blocked().await;
    held.put_file("/second", b"second", &test_actor())
        .await
        .expect("publish during visit");
    store.release();
    pass.await.expect("finish visit");
    store.inner().reset();
    server.sweep.run_session_pass(false).await;
    assert!(
        !store.inner().take().is_empty(),
        "a publish during the visit remains eligible"
    );
    server.sweep.run_session_pass(false).await;
    assert!(store.inner().take().is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn idle_sessions_close_after_requests_release_them_and_reopen_with_a_new_epoch() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("idle-close");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = namespaces.open(&namespace_id).await.expect("open session");
    held.put_file("/first", b"first", &test_actor())
        .await
        .expect("publish");
    server
        .grep_worker
        .as_ref()
        .expect("grep worker")
        .enable(&namespace_id)
        .await
        .expect("enable grep");
    drop(held);
    server.sweep.run_session_pass(true).await;
    server.sweep.run_index_pass().await;
    let first_epoch = loonfs_core::control::load_read_anchor(&*store, &namespace_id)
        .await
        .expect("read writer epoch")
        .read_state
        .writer_epoch;

    server.clock.advance_ms(config.idle_session_close_after_ms);
    store.reset();
    server.sweep.run_session_pass(true).await;
    assert_eq!(namespaces.held().len(), 1, "the threshold must be passed");
    assert!(
        store.take().is_empty(),
        "a caught-up session costs zero requests"
    );

    let request = namespaces
        .open(&namespace_id)
        .await
        .expect("take handle again");
    request
        .put_file("/recent", b"recent", &test_actor())
        .await
        .expect("publish again");
    drop(request);
    server.sweep.run_session_pass(true).await;
    server.clock.advance_ms(1);
    store.reset();
    server.sweep.run_session_pass(true).await;
    assert_eq!(namespaces.held().len(), 1, "an open resets idle time");
    assert!(
        store.take().is_empty(),
        "a recent write keeps the caught-up session open"
    );

    let request = namespaces
        .open(&namespace_id)
        .await
        .expect("hold request clone");
    let clone = request.clone();
    drop(request);
    server
        .clock
        .advance_ms(config.idle_session_close_after_ms + 1);
    store.reset();
    server.sweep.run_session_pass(true).await;
    assert_eq!(
        namespaces.held().len(),
        1,
        "a request clone prevents closing"
    );
    assert!(
        store.take().is_empty(),
        "checking a held clone costs zero requests"
    );
    drop(clone);
    server.sweep.run_session_pass(true).await;
    assert!(
        namespaces.held().is_empty(),
        "the next pass closes the session"
    );
    assert!(lock(&server.sweep.inner.sessions).is_empty());
    assert!(lock(
        &server
            .sweep
            .inner
            .grep
            .as_ref()
            .expect("grep indexing")
            .progress
    )
    .indexed
    .is_empty());
    assert!(
        store.take().is_empty(),
        "an idle close costs zero store requests"
    );

    let reopened = namespaces
        .open(&namespace_id)
        .await
        .expect("reopen session");
    assert_eq!(reopened.last_published_seq(), None);
    let committed = reopened
        .put_file("/after-close", b"reopened", &test_actor())
        .await
        .expect("publish after close");
    let anchor = loonfs_core::control::load_read_anchor(&*store, &namespace_id)
        .await
        .expect("read new writer epoch");
    assert_eq!(anchor.read_state.writer_epoch.0, first_epoch.0 + 1);
    assert_eq!(anchor.read_state.seq, committed.committed_seq);
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_session_that_only_aborted_an_upload_closes_after_the_idle_threshold() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("aborted-upload");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = namespaces.open(&namespace_id).await.expect("open session");
    let upload = held.create_upload().await.expect("create upload");
    held.abort_upload(&upload.upload_id)
        .await
        .expect("abort upload");
    assert_eq!(held.last_published_seq(), None);
    drop(held);

    server
        .clock
        .advance_ms(config.idle_session_close_after_ms - 1);
    store.reset();
    server.sweep.run_session_pass(true).await;
    assert_eq!(namespaces.held().len(), 1, "a recent session stays open");
    assert!(store.take().is_empty());

    server.clock.advance_ms(2);
    server.sweep.run_session_pass(false).await;
    assert!(
        namespaces.held().is_empty(),
        "the next pass closes the session"
    );
    assert!(store.take().is_empty(), "closing costs zero store requests");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn an_idle_session_waiting_for_its_fold_is_visited_and_kept_open() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("local store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_session_close_after_ms = 1;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("idle-unfolded");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create namespace");
    let held = namespaces.open(&namespace_id).await.expect("open session");
    held.put_file("/note", b"note", &test_actor())
        .await
        .expect("publish");
    drop(held);
    server
        .clock
        .advance_ms(config.idle_session_close_after_ms + 1);
    store.reset();
    server.sweep.run_session_pass(false).await;
    assert_eq!(
        namespaces.held().len(),
        1,
        "unfinished metadata prevents closing"
    );
    assert!(!store.take().is_empty(), "the session is visited");
    assert!(server.wal_tail_objects(&namespace_id).await > 0);

    server.clock.advance_ms(config.idle_fold_after_ms);
    server.sweep.run_session_pass(false).await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    assert_eq!(namespaces.held().len(), 1, "this pass had unfinished work");
    store.reset();
    server.sweep.run_session_pass(true).await;
    assert!(namespaces.held().is_empty());
    assert!(
        !store.take().is_empty(),
        "the due collection runs before closing"
    );
    server.runtime.shutdown().await.expect("shutdown");
}
