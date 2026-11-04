//! Index lifecycle changes and unfinished index work on held entries.

use super::{held_ids, idle_fold_after_ms, seed_writer, sweep_config, visit_held, SweepServer};
use loonfs::{ChangeSeq, SharedObjectStore};
use loonfs_grep::entry::GrepIndexStatus;
use loonfs_grep::GrepWorkerConfig;
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_test_support::test_actor;
use std::assert_matches;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn ticks_index_publishes_and_lifecycle_changes_then_cost_nothing() {
    let directory = tempdir().expect("tempdir");
    // The runtime raises the hint in the background after each publication,
    // so the recording leaves out the hint's requests.
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any().except_hints(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = Some(0);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("index-changes");
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
    for index in 0..2 {
        let commit = held
            .put_file(&format!("/note-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
        server.tick_after(config.tick_interval_ms).await;
        server.sweep.tick().await;
        store.reset();
        for _ in 0..20 {
            server.sweep.tick().await;
        }
        assert!(
            store.take().is_empty(),
            "an idle held session costs no store request"
        );
        server.enable_index(&namespace_id).await;
        server.sweep.tick().await;
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
        // moves onto it and the manifest is cached, so the next tick's
        // metadata step sends no request. Enabling the active index again
        // publishes nothing.
        store.reset();
        server.sweep.tick().await;
        let after = store.take();
        assert!(after.is_empty(), "{after:#?}");
        server.sweep.tick().await;
        assert!(store.take().is_empty(), "the lifecycle change is finished");
    }
    server.disable_index(&namespace_id).await;
    store.reset();
    server.sweep.tick().await;
    assert!(!store.take().is_empty(), "disabling marks the entry dirty");
    server.sweep.tick().await;
    assert!(
        store.take().is_empty(),
        "a disabled index costs no further requests"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn enabling_an_empty_namespace_holds_it_and_ticks_build_without_a_commit() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let server = SweepServer::start(
        store.clone(),
        &sweep_config(Some(GrepWorkerConfig::default())),
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
        .sweep
        .inner
        .namespaces
        .open(&held_id)
        .await
        .expect("open");
    store.reset();
    server.sweep.tick().await;
    assert!(
        store.take().is_empty(),
        "no commit or lifecycle change costs no requests"
    );
    for id in [&held_id, &unheld_id] {
        server.enable_index(id).await;
    }
    server.sweep.tick().await;
    for id in [&held_id, &unheld_id] {
        assert_eq!(
            server
                .grep_status(id)
                .await
                .active_watermark()
                .expect("active")
                .built_through_seq(),
            ChangeSeq(0)
        );
    }
    assert_eq!(held.last_published_seq(), None);
    store.reset();
    for _ in 0..20 {
        server.sweep.tick().await;
    }
    assert!(
        store.take().is_empty(),
        "finished lifecycle changes cost no requests"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session that receives a commit on every tick gets its index
/// step on every tick and stays open, because each commit opens the session
/// again, and that the session closes once the commits stop and its tail
/// is folded.
#[tokio::test]
async fn a_session_written_on_every_tick_is_indexed_on_every_tick_and_stays_open() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    // Two ticks without an open would make the session idle.
    config.idle_session_close_after_ms = 2 * config.tick_interval_ms;
    let server = SweepServer::start(store, &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
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
        server.tick_after(config.tick_interval_ms).await;
        assert_eq!(
            server
                .grep_status(&namespace_id)
                .await
                .active_watermark()
                .expect("active")
                .built_through_seq(),
            commit.committed_seq,
            "tick {index} indexes the commit before it"
        );
        assert_eq!(
            held_ids(namespaces),
            std::slice::from_ref(&namespace_id),
            "tick {index} leaves the session open"
        );
    }
    server.tick_after(idle_fold_after_ms(&config)).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "once the commits stop, the tick that folds the idle tail closes the session"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session with a commit on every tick still collects on its
/// collection interval, because a collection that's due runs before the
/// index step.
#[tokio::test]
async fn a_session_written_on_every_tick_still_collects_on_its_interval() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.gc_interval_ms = 3 * config.tick_interval_ms;
    let server = SweepServer::start(store, &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("steady-collected");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    server.enable_index(&namespace_id).await;
    let collected_seq = || {
        visit_held(namespaces, &namespace_id, |held| {
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
        server.tick_after(config.tick_interval_ms).await;
        collections += usize::from(collected_seq() != before);
    }
    assert_eq!(
        collections, 4,
        "60 seconds of ticks collect once every 15 seconds"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ticks_finish_a_backfill_without_a_local_commit_before_closing() {
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
    let files = super::super::MAX_GREP_STEPS_PER_VISIT + 4;
    for index in 0..files {
        session
            .put_file(&format!("/file-{index}"), b"needle", &test_actor())
            .await
            .expect("publish");
    }
    writer.shutdown().await.expect("stop writer");
    let config = sweep_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    let server = SweepServer::start(store.clone(), &config, now_ms).await;
    server.enable_index(&namespace_id).await;
    server
        .tick_after(config.idle_session_close_after_ms + 1)
        .await;
    assert_matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. }
    );
    assert_eq!(held_ids(&server.sweep.inner.namespaces).len(), 1);
    store.reset();
    server.sweep.tick().await;
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
        ChangeSeq(u64::try_from(files).expect("count"))
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
        "the visits reorganized each full delta level before the next build step"
    );
    assert_eq!(
        visit_held(&server.sweep.inner.namespaces, &namespace_id, |held| held
            .handle
            .last_published_seq()),
        Some(None)
    );
    store.reset();
    server.sweep.tick().await;
    assert!(store.take().is_empty(), "closing costs no store requests");
    assert!(held_ids(&server.sweep.inner.namespaces).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn pending_reorganization_keeps_an_idle_session_open() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig {
        max_files_per_step: 1,
        ..GrepWorkerConfig::default()
    }));
    config.idle_fold_after_ms = Some(0);
    let mut server = SweepServer::start(store.clone(), &config, 0).await;
    let policy = &mut Arc::get_mut(&mut server.sweep.inner)
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
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    // Each file has about 40 grams, so the reorganization of the two
    // files' runs, one row at a time, takes more steps than a visit runs.
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
        .tick_after(config.idle_session_close_after_ms + 1)
        .await;
    server.sweep.tick().await;
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
        server.sweep.tick().await;
        assert!(
            !store.take().is_empty(),
            "unfinished reorganization stays selected"
        );
        assert_eq!(held_ids(&server.sweep.inner.namespaces).len(), 1);
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
    // The visit that finished the reorganization goes on to a build step
    // that finds nothing left, unless the visit's step bound stopped it
    // first, and then the next visit does.
    for _ in 0..2 {
        store.reset();
        server.sweep.tick().await;
        if held_ids(&server.sweep.inner.namespaces).is_empty() {
            break;
        }
    }
    assert!(
        store.take().is_empty(),
        "a finished session closes without requests"
    );
    assert!(held_ids(&server.sweep.inner.namespaces).is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}
