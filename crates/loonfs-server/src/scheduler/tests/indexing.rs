//! Index lifecycle changes and unfinished index work on held entries.

use super::{
    held_ids, idle_fold_after_ms, scheduler_config, seed_writer, with_held, SchedulerServer,
};
use loonfs::{CommitSeq, SharedObjectStore};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::GrepWorkerConfig;
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_test_support::test_actor;
use std::assert_matches;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn session_steps_index_publishes_and_lifecycle_changes_then_cost_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = scheduler_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = Some(0);
    let server = SchedulerServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("index-changes");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    for index in 0..2 {
        let commit = held
            .put_file(&format!("/note-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
        server
            .run_steps_after(config.maintenance_interval_ms.get())
            .await;
        server.scheduler.run_due_steps().await;
        store.reset();
        for _ in 0..20 {
            server.scheduler.run_due_steps().await;
        }
        assert!(
            store.take().is_empty(),
            "an idle held session costs no store request"
        );
        server.enable_index(&namespace_id).await;
        server.scheduler.run_due_steps().await;
        assert_eq!(
            server
                .grep_status(&namespace_id)
                .await
                .active_watermark()
                .expect("active")
                .built_through_seq(),
            commit.committed_seq
        );
        // The first build publishes a manifest, and the session's position
        // moves onto it and the manifest is cached, so the next
        // metadata step sends no request. Enabling the active index again
        // publishes nothing.
        store.reset();
        server.scheduler.run_due_steps().await;
        let after = store.take();
        assert!(after.is_empty(), "{after:#?}");
        server.scheduler.run_due_steps().await;
        assert!(store.take().is_empty(), "the lifecycle change is finished");
    }
    server.disable_index(&namespace_id).await;
    store.reset();
    server.scheduler.run_due_steps().await;
    assert!(!store.take().is_empty(), "disabling marks the entry dirty");
    server.scheduler.run_due_steps().await;
    assert!(
        store.take().is_empty(),
        "a disabled index costs no further requests"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enabling_an_empty_namespace_holds_it_and_session_steps_build_without_a_commit() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let server = SchedulerServer::start(
        store.clone(),
        &scheduler_config(Some(GrepWorkerConfig::default())),
        0,
    )
    .await;
    let held_id = namespace_id("empty-held");
    let unheld_id = namespace_id("empty-unheld");
    for id in [&held_id, &unheld_id] {
        server
            .runtime
            .create_namespace(id, &test_actor())
            .await
            .expect("create");
    }
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&held_id)
        .await
        .expect("open");
    store.reset();
    server.scheduler.run_due_steps().await;
    assert!(
        store.take().is_empty(),
        "no commit or lifecycle change costs no requests"
    );
    for id in [&held_id, &unheld_id] {
        server.enable_index(id).await;
    }
    server.scheduler.run_due_steps().await;
    for id in [&held_id, &unheld_id] {
        assert_eq!(
            server
                .grep_status(id)
                .await
                .active_watermark()
                .expect("active")
                .built_through_seq(),
            CommitSeq(0)
        );
    }
    assert_eq!(held.last_published_seq(), None);
    store.reset();
    for _ in 0..20 {
        server.scheduler.run_due_steps().await;
    }
    assert!(
        store.take().is_empty(),
        "finished lifecycle changes cost no requests"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session that receives a commit at every maintenance
/// interval gets its index step at every interval and stays open, because each commit opens the session
/// again, and that the session closes once the commits stop and its tail
/// is folded.
#[tokio::test]
async fn a_session_written_at_every_interval_is_indexed_at_every_interval_and_stays_open() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let mut config = scheduler_config(Some(GrepWorkerConfig::default()));
    // Two intervals without an open would make the session idle.
    config.idle_session_close_after_ms =
        NonZeroU64::new(2 * config.maintenance_interval_ms.get()).expect("positive");
    let server = SchedulerServer::start(store, &config, 0).await;
    let namespaces = &server.scheduler.inner.namespaces;
    let namespace_id = namespace_id("steady");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    server.enable_index(&namespace_id).await;
    for index in 0..12 {
        // Each commit opens the session and drops its handle, as an HTTP
        // request does.
        let commit = namespaces
            .open(&namespace_id)
            .await
            .expect("open")
            .put_file(&format!("/note-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
        server
            .run_steps_after(config.maintenance_interval_ms.get())
            .await;
        assert_eq!(
            server
                .grep_status(&namespace_id)
                .await
                .active_watermark()
                .expect("active")
                .built_through_seq(),
            commit.committed_seq,
            "run {index} indexes the commit before it"
        );
        assert_eq!(
            held_ids(namespaces),
            std::slice::from_ref(&namespace_id),
            "run {index} leaves the session open"
        );
    }
    server.run_steps_after(idle_fold_after_ms(&config)).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "once the commits stop, the run that folds the idle tail closes the session"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session with a commit at every interval still collects on its
/// collection interval, because a collection that's due runs before the
/// index step.
#[tokio::test]
async fn a_session_written_at_every_interval_still_collects_on_its_interval() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let mut config = scheduler_config(Some(GrepWorkerConfig::default()));
    config.gc_interval_ms =
        NonZeroU64::new(3 * config.maintenance_interval_ms.get()).expect("positive");
    let server = SchedulerServer::start(store, &config, 0).await;
    let namespaces = &server.scheduler.inner.namespaces;
    let namespace_id = namespace_id("steady-collected");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    server.enable_index(&namespace_id).await;
    let collected_seq = || {
        with_held(namespaces, &namespace_id, |held| {
            held.host_state.collected_seq
        })
        .expect("a held session")
    };
    let mut collections = 0;
    for index in 0..12 {
        namespaces
            .open(&namespace_id)
            .await
            .expect("open")
            .put_file(&format!("/note-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
        let before = collected_seq();
        server
            .run_steps_after(config.maintenance_interval_ms.get())
            .await;
        collections += usize::from(collected_seq() != before);
    }
    assert_eq!(
        collections, 4,
        "60 seconds of session steps collect once every 15 seconds"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn session_steps_finish_a_backfill_without_a_local_commit_before_closing() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let (writer, now_ms) = seed_writer(&shared).await;
    let namespace_id = namespace_id("pending-backfill");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let session = writer.open_namespace(&namespace_id).expect("open");
    let files = super::super::MAX_GREP_STEPS_PER_INDEX_STEP + 4;
    for index in 0..files {
        session
            .put_file(&format!("/file-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
    }
    writer.shutdown().await.expect("stop writer");
    let config = scheduler_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    let server = SchedulerServer::start(store.clone(), &config, now_ms).await;
    server.enable_index(&namespace_id).await;
    server
        .run_steps_after(config.idle_session_close_after_ms.get() + 1)
        .await;
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. }
    );
    assert_eq!(held_ids(&server.scheduler.inner.namespaces).len(), 1);
    store.reset();
    server.scheduler.run_due_steps().await;
    assert!(
        !store.take().is_empty(),
        "unfinished backfill stays selected without a commit"
    );
    assert_eq!(
        server
            .grep_status(&namespace_id)
            .await
            .active_watermark()
            .expect("active")
            .built_through_seq(),
        CommitSeq(u64::try_from(files).expect("count"))
    );
    assert!(
        !server
            .grep_worker
            .as_ref()
            .expect("worker")
            .get_grep_index(&namespace_id)
            .await
            .expect("status")
            .reorganize_pending,
        "the index steps reorganized each full delta level before the next build step"
    );
    assert_eq!(
        with_held(&server.scheduler.inner.namespaces, &namespace_id, |held| {
            held.handle.last_published_seq()
        }),
        Some(None)
    );
    store.reset();
    server.scheduler.run_due_steps().await;
    assert!(store.take().is_empty(), "closing costs no store requests");
    assert!(held_ids(&server.scheduler.inner.namespaces).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn pending_reorganization_keeps_an_idle_session_open() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = scheduler_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    config.idle_fold_after_ms = Some(0);
    let mut server = SchedulerServer::start(store.clone(), &config, 0).await;
    let policy = &mut Arc::get_mut(&mut server.scheduler.inner)
        .expect("one sweep")
        .grep
        .as_mut()
        .expect("grep")
        .policy;
    policy.max_delta_runs = NonZeroUsize::new(2).expect("positive");
    policy.max_decoded_input_rows_per_step = NonZeroUsize::new(1).expect("positive");
    let namespace_id = namespace_id("pending-reorganization");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let held = server
        .scheduler
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    // Each file has about 40 grams, so the reorganization of the two
    // files' runs, one row at a time, takes more grep steps than one index step runs.
    for index in 0..2 {
        held.put_file(
            &format!("/file-{index}"),
            b"the quick brown fox jumps over the lazy dog",
            &test_actor(),
        )
        .await
        .expect("publish");
    }
    server.enable_index(&namespace_id).await;
    drop(held);
    server
        .run_steps_after(config.idle_session_close_after_ms.get() + 1)
        .await;
    server.scheduler.run_due_steps().await;
    let worker = server.grep_worker.as_ref().expect("worker");
    assert!(
        worker
            .get_grep_index(&namespace_id)
            .await
            .expect("status")
            .reorganize_pending
    );
    let mut pending = true;
    for _ in 0..32 {
        store.reset();
        server.scheduler.run_due_steps().await;
        assert!(
            !store.take().is_empty(),
            "unfinished reorganization stays selected"
        );
        assert_eq!(held_ids(&server.scheduler.inner.namespaces).len(), 1);
        pending = worker
            .get_grep_index(&namespace_id)
            .await
            .expect("status")
            .reorganize_pending;
        if !pending {
            break;
        }
    }
    assert!(!pending);
    // The index step that finished the reorganization goes on to a build
    // step that finds nothing left, unless the index step's bound stopped
    // it first, and then the next index step does.
    for _ in 0..2 {
        store.reset();
        server.scheduler.run_due_steps().await;
        if held_ids(&server.scheduler.inner.namespaces).is_empty() {
            break;
        }
    }
    assert!(
        store.take().is_empty(),
        "a finished session closes without requests"
    );
    assert!(held_ids(&server.scheduler.inner.namespaces).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}
