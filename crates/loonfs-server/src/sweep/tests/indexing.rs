//! Index lifecycle changes and unfinished index work on held entries.

use super::{seed_writer, sweep_config, SweepServer};
use loonfs::{ChangeSeq, SharedObjectStore};
use loonfs_grep::manifest::GrepIndexStatus;
use loonfs_grep::GrepWorkerConfig;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use loonfs_test_support::test_actor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn ticks_index_publishes_and_lifecycle_changes_then_cost_nothing() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = 0;
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
        store.reset();
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
    let files = super::super::MAX_GREP_BUILD_STEPS_PER_VISIT + 4;
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
    assert!(matches!(
        server.grep_status(&namespace_id).await,
        GrepIndexStatus::Backfilling { .. }
    ));
    assert_eq!(server.sweep.inner.namespaces.held_ids().len(), 1);
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
    store.reset();
    server.sweep.tick().await;
    assert!(
        !store.take().is_empty(),
        "reorganization needs a step that finds nothing left"
    );
    assert_eq!(
        server
            .sweep
            .inner
            .namespaces
            .with_held(&namespace_id, |held| held.handle.last_published_seq()),
        Some(None)
    );
    server.sweep.tick().await;
    assert!(store.take().is_empty(), "closing costs no store requests");
    assert!(server.sweep.inner.namespaces.held_ids().is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn pending_reorganization_keeps_an_idle_entry_settling() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
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
    for index in 0..2 {
        held.put_file(&format!("/file-{index}"), b"abc", &test_actor())
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
        assert_eq!(server.sweep.inner.namespaces.held_ids().len(), 1);
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
    store.reset();
    server.sweep.tick().await;
    assert!(
        !store.take().is_empty(),
        "a completed unit needs a step that finds nothing left"
    );
    server.sweep.tick().await;
    assert!(
        store.take().is_empty(),
        "a finished session closes without requests"
    );
    assert!(server.sweep.inner.namespaces.held_ids().is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}
