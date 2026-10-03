//! Metadata retry, collection, and close costs for held sessions.

use super::{seed_unfolded_tail, seed_writer, sweep_config, SweepServer, DAY_MS};
use loonfs::SharedObjectStore;
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use loonfs_test_support::test_actor;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn ticks_leave_active_sessions_alone_and_fold_idle_tails_then_cost_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("held-tail");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    store.reset();
    for _ in 0..100 {
        server.tick_after(config.tick_interval_ms).await;
    }
    assert!(
        store.take().is_empty(),
        "a session that published nothing is skipped"
    );
    assert!(held.metadata_caught_up());
    assert_eq!(held.last_published_ms(), None);
    for index in 0..2 {
        let commit = held
            .put_file(&format!("/note-{index}"), b"data", &test_actor())
            .await
            .expect("publish");
        assert_eq!(held.last_published_seq(), Some(commit.committed_seq));
        assert_eq!(
            held.last_published_ms(),
            Some(server.sweep.inner.namespaces.now_ms())
        );
        assert!(!held.metadata_caught_up());
        store.reset();
        server.sweep.tick().await;
        assert!(
            store.take().is_empty(),
            "a publish less than one tick old stays active"
        );
        server.tick_after(config.tick_interval_ms).await;
        assert!(
            !store.take().is_empty(),
            "the first tick observes the waiting tail"
        );
        server.tick_after(config.maintenance_interval_ms - 1).await;
        assert!(store.take().is_empty(), "metadata waits for its retry time");
        server.tick_after(1).await;
        assert!(!store.take().is_empty(), "unfinished metadata is retried");
        assert!(server.wal_tail_objects(&namespace_id).await > 0);
        server.tick_after(config.idle_fold_after_ms).await;
        assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
        assert!(held.metadata_caught_up());
        store.reset();
        for _ in 0..20 {
            server.tick_after(config.tick_interval_ms).await;
        }
        assert!(
            store.take().is_empty(),
            "caught-up sessions cost no requests"
        );
    }
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ticks_leave_unheld_namespaces_for_the_daily_pass() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let (writer, now_ms) = seed_writer(&shared).await;
    let namespace_id = namespace_id("unheld");
    seed_unfolded_tail(&writer, &namespace_id).await;
    writer.shutdown().await.expect("shutdown writer");
    let server = SweepServer::start(store.clone(), &sweep_config(None), now_ms + DAY_MS).await;
    store.reset();
    for _ in 0..100 {
        server.tick_after(5_000).await;
    }
    assert!(store.take().is_empty());
    assert!(server.wal_tail_objects(&namespace_id).await > 0);
    assert_eq!(server.sweep.run_pass(true).await.expect("daily pass"), 1);
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
}

#[tokio::test]
async fn ticks_finish_more_than_sixteen_metadata_units_without_another_publish() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("held-backlog");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
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
    server.tick_after(config.tick_interval_ms).await;
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
    assert!(!held.metadata_caught_up());
    store.reset();
    server.tick_after(config.maintenance_interval_ms).await;
    assert!(
        !store.take().is_empty(),
        "the unit cap leaves this session due"
    );
    for _ in 0..8 {
        server.tick_after(config.maintenance_interval_ms).await;
    }
    assert!(held.metadata_caught_up());
    store.reset();
    server.sweep.tick().await;
    assert!(store.take().is_empty(), "the backlog eventually catches up");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_fenced_compactor_costs_six_requests_and_waits_for_the_retry_interval() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("fenced-compactor");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    for index in 0..2 * loonfs_types::format::sst_blocks::DEFAULT_MAX_DELTA_RUNS {
        held.put_file(&format!("/note-{index}"), b"data", &test_actor())
            .await
            .expect("publish");
        server
            .maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold");
    }
    server
        .maintenance
        .compact_metadata(&namespace_id)
        .await
        .expect("claim compactor");
    let other = loonfs::LoonFs::builder_with_store(store.clone())
        .writer_id("other")
        .build()
        .await
        .expect("other runtime");
    other
        .maintenance(writer_id("other"))
        .compact_metadata(&namespace_id)
        .await
        .expect("claim newer compactor");
    store.reset();
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(store.take().len(), 6);
    assert!(!held.metadata_caught_up());
    for _ in 0..59 {
        server.tick_after(config.tick_interval_ms).await;
    }
    assert!(
        store.take().is_empty(),
        "a fenced compactor is not retried on every tick"
    );
    server.tick_after(config.tick_interval_ms).await;
    assert!(
        !store.take().is_empty(),
        "the next metadata attempt waits for its retry time"
    );
    other.shutdown().await.expect("shutdown other");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn collection_runs_hourly_only_when_the_session_moved() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = 0;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let moved = namespace_id("moved");
    let quiet = namespace_id("quiet");
    for id in [&moved, &quiet] {
        server
            .runtime
            .create_namespace(id, &test_actor())
            .await
            .expect("create");
    }
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&moved)
        .await
        .expect("open");
    let _quiet = server
        .sweep
        .inner
        .namespaces
        .open(&quiet)
        .await
        .expect("hold quiet");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    server.tick_after(config.tick_interval_ms).await;
    server.sweep.tick().await;
    store.reset();
    server
        .tick_after(config.gc_interval_ms - config.tick_interval_ms - 1)
        .await;
    assert!(store.take().is_empty());
    server.tick_after(1).await;
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|op| matches!(op, RecordedOperation::List { .. }))
            .count(),
        9,
        "collection lists core and grep garbage without listing namespaces"
    );
    assert!(operations.iter().all(|op| op
        .key()
        .starts_with(&loonfs_objectstore::keys::namespace_prefix(&moved))));
    server.tick_after(config.gc_interval_ms).await;
    assert!(
        store.take().is_empty(),
        "an unchanged session is never collected by ticks"
    );
    held.put_file("/again", b"data", &test_actor())
        .await
        .expect("publish");
    server.tick_after(config.tick_interval_ms).await;
    server.sweep.tick().await;
    store.reset();
    server.sweep.tick().await;
    assert_eq!(
        store
            .take()
            .iter()
            .filter(|op| matches!(op, RecordedOperation::List { .. }))
            .count(),
        9
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn quiet_sessions_close_only_after_the_idle_threshold_and_all_callers_release_them() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
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
        .expect("create");
    let held = namespaces.open(&namespace_id).await.expect("open");
    held.put_file("/first", b"data", &test_actor())
        .await
        .expect("publish");
    server.enable_index(&namespace_id).await;
    server.tick_after(config.tick_interval_ms).await;
    server.sweep.tick().await;
    drop(held);
    let first_epoch = loonfs_core::control::load_read_anchor(&*store, &namespace_id)
        .await
        .expect("anchor")
        .read_state
        .writer_epoch;
    store.reset();
    server
        .tick_after(config.idle_session_close_after_ms - config.tick_interval_ms)
        .await;
    assert_eq!(
        namespaces.held_ids().len(),
        1,
        "the threshold must be passed"
    );
    assert!(
        store.take().is_empty(),
        "a quiet session costs zero requests"
    );
    let request = namespaces
        .open(&namespace_id)
        .await
        .expect("open resets idle age");
    let clone = request.clone();
    drop(request);
    server.tick_after(1).await;
    assert_eq!(namespaces.held_ids().len(), 1);
    assert!(store.take().is_empty());
    server
        .tick_after(config.idle_session_close_after_ms + 1)
        .await;
    server.sweep.tick().await;
    store.reset();
    server.sweep.tick().await;
    assert_eq!(
        namespaces.held_ids().len(),
        1,
        "a request clone prevents closing"
    );
    assert!(
        store.take().is_empty(),
        "checking a clone costs zero requests"
    );
    drop(clone);
    server.sweep.tick().await;
    assert!(namespaces.held_ids().is_empty());
    assert!(store.take().is_empty(), "closing costs zero requests");
    let reopened = namespaces.open(&namespace_id).await.expect("reopen");
    assert_eq!(reopened.last_published_seq(), None);
    reopened
        .put_file("/after-close", b"data", &test_actor())
        .await
        .expect("publish");
    assert_eq!(
        loonfs_core::control::load_read_anchor(&*store, &namespace_id)
            .await
            .expect("anchor")
            .read_state
            .writer_epoch
            .0,
        first_epoch.0 + 1
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn aborted_uploads_close_without_a_publish_but_an_unfolded_tail_prevents_close() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_session_close_after_ms = 1;
    config.idle_fold_after_ms = 60_000;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let aborted = namespace_id("aborted-upload");
    let unfolded = namespace_id("idle-unfolded");
    for id in [&aborted, &unfolded] {
        server
            .runtime
            .create_namespace(id, &test_actor())
            .await
            .expect("create");
    }
    let handle = namespaces.open(&aborted).await.expect("open");
    let upload = handle.create_upload().await.expect("upload");
    handle.abort_upload(&upload.upload_id).await.expect("abort");
    assert_eq!(handle.last_published_seq(), None);
    drop(handle);
    store.reset();
    server.tick_after(1).await;
    assert_eq!(namespaces.held_ids().len(), 1);
    assert!(store.take().is_empty());
    server.tick_after(1).await;
    assert!(namespaces.held_ids().is_empty());
    assert!(
        store.take().is_empty(),
        "an aborted upload closes without requests"
    );
    let handle = namespaces.open(&unfolded).await.expect("open");
    handle
        .put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    drop(handle);
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        namespaces.held_ids().len(),
        1,
        "unfinished metadata prevents close"
    );
    assert!(!store.take().is_empty());
    assert!(server.wal_tail_objects(&unfolded).await > 0);
    store.reset();
    server
        .tick_after(config.idle_fold_after_ms - config.tick_interval_ms - 1)
        .await;
    assert!(
        store.take().is_empty(),
        "a waiting tail needs no read before its age passes"
    );
    server.tick_after(1).await;
    assert_eq!(server.wal_tail_objects(&unfolded).await, 0);
    assert_eq!(namespaces.held_ids().len(), 1, "one tick does one due step");
    store.reset();
    server.sweep.tick().await;
    assert!(namespaces.held_ids().is_empty());
    assert!(store.take().is_empty(), "closing costs zero requests");
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_session_records_when_its_own_maintenance_catches_up() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(super::SettableWallClock(std::sync::atomic::AtomicU64::new(
        0,
    )));
    let runtime = loonfs::LoonFs::builder_with_store(store.clone())
        .writer_id("self-maintenance")
        .wall_clock(clock.clone())
        .monotonic_timer(clock.clone())
        .inline_content(loonfs::InlineContentPolicy {
            inline_content_fold_at_bytes: 1,
            ..loonfs::InlineContentPolicy::default()
        })
        .build()
        .await
        .expect("runtime");
    let namespace_id = namespace_id("self-maintained");
    runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = runtime.open_namespace(&namespace_id).expect("open");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    runtime.drain().await.expect("self-maintenance finishes");
    assert!(held.metadata_caught_up());
    assert_eq!(held.last_published_ms(), Some(0));
    let recorder = loonfs::metrics::DefaultMetricsRecorder::new();
    let namespaces = Arc::new(loonfs_http::Namespaces::new_with_timer(
        runtime.clone(),
        clock.clone(),
    ));
    namespaces.open(&namespace_id).await.expect("hold");
    let sweep = super::Sweep::new(
        &sweep_config(None),
        runtime.object_store(),
        runtime.maintenance(writer_id("maintenance")),
        namespaces,
        None,
        &recorder,
    )
    .expect("sweep");
    store.reset();
    for _ in 0..100 {
        clock.advance_ms(5_000);
        sweep.tick().await;
    }
    assert!(
        store.take().is_empty(),
        "self-maintenance leaves no work for the host"
    );
    runtime.shutdown().await.expect("shutdown");
}
