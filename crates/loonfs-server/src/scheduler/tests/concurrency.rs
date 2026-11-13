//! Concurrent steps preserve session progress and share the host's slots.

use super::{scheduler_config, with_held, SchedulerServer};
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn a_sweep_and_the_session_steps_never_run_on_the_same_namespace_together() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("one-claim");
    // The runtime raises the hint in the background after each publication,
    // so neither the gate nor the recording selects the hint.
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any().except_hints(),
        ),
        KeyPredicate::new({
            let namespace_prefix = loonfs_objectstore::keys::namespace_prefix(&namespace_id);
            move |key| key.starts_with(&namespace_prefix) || key.starts_with("pools/")
        })
        .except_hints(),
        OperationSelector::Any,
    ));
    let mut config = scheduler_config(None);
    // The session's metadata step and the sweep's metadata call each fold
    // the session's tail, so the gate parks either one at its first request,
    // which writes the folded content under the pool's prefix. A gate on the
    // namespace's prefix alone would park one of the fold's concurrent segment
    // puts and let the others through.
    config.idle_fold_after_ms = Some(1);
    let server = SchedulerServer::start(store.clone(), &config, 0).await;
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
    for steps_first in [true, false] {
        held.put_file(
            if steps_first { "/first" } else { "/second" },
            b"data",
            &test_actor(),
        )
        .await
        .expect("publish");
        server
            .clock
            .advance_ms(config.maintenance_interval_ms.get());
        store.block_next();
        let parked = tokio::spawn({
            let scheduler = server.scheduler.clone();
            async move {
                if steps_first {
                    scheduler.run_due_steps().await;
                } else {
                    scheduler.run_sweep().await.expect("sweep");
                }
            }
        });
        store.wait_until_blocked().await;
        store.inner().reset();
        if steps_first {
            assert_eq!(server.scheduler.run_sweep().await.expect("sweep"), 1);
            let operations = store.inner().take();
            assert_matches!(&operations[0], RecordedOperation::List { .. });
            assert_eq!(operations[0].key(), "namespaces/");
            // The sweep's collection of the pool reads the namespace's roots
            // and lists its snapshots. A collection of the namespace would list
            // these four prefixes, and the sweep runs none of the namespace's
            // steps.
            let own_prefixes = [
                loonfs_objectstore::keys::metadata_manifest_prefix(&namespace_id),
                loonfs_objectstore::keys::wal_prefix(&namespace_id),
                loonfs_objectstore::keys::metadata_segment_prefix(&namespace_id),
                loonfs_objectstore::keys::extensions_prefix(&namespace_id),
            ];
            assert!(
                !operations.iter().any(|operation| {
                    matches!(operation, RecordedOperation::List { .. })
                        && own_prefixes
                            .iter()
                            .any(|prefix| operation.key().starts_with(prefix.as_str()))
                }),
                "{operations:?}"
            );
        } else {
            server.scheduler.run_due_steps().await;
            assert!(store.inner().take().is_empty());
        }
        store.release();
        parked.await.expect("parked");
    }
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_publish_during_a_metadata_step_is_folded_by_the_next_run() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("concurrent-publish");
    // The runtime raises the hint in the background after each publication,
    // so neither the gate nor the recording selects the hint.
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any().except_hints(),
        ),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id))
            .except_hints(),
        OperationSelector::Any,
    ));
    let mut config = scheduler_config(None);
    // The session's metadata step folds the session's tail, so the gate
    // parks the step at the fold's first request.
    config.idle_fold_after_ms = Some(1);
    let server = SchedulerServer::start(store.clone(), &config, 0).await;
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
    held.put_file("/first", b"first", &test_actor())
        .await
        .expect("publish");
    server
        .clock
        .advance_ms(config.maintenance_interval_ms.get());
    store.block_next();
    let steps = tokio::spawn({
        let scheduler = server.scheduler.clone();
        async move { scheduler.run_due_steps().await }
    });
    store.wait_until_blocked().await;
    held.put_file("/second", b"second", &test_actor())
        .await
        .expect("publish during the step");
    store.release();
    steps.await.expect("finish the step");
    store.inner().reset();
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    assert!(
        !store.inner().take().is_empty(),
        "the next run folds the tail that the concurrent publish left"
    );
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    // The run after a fold reads the fold's manifest once.
    server.scheduler.run_due_steps().await;
    store.inner().reset();
    server.scheduler.run_due_steps().await;
    assert!(store.inner().take().is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn session_steps_share_slots_with_a_parked_sweep_and_continue_when_a_slot_is_free() {
    for slots in [1, 2] {
        let directory = tempdir().expect("tempdir");
        let parked = namespace_id("a-parked");
        let later = namespace_id("b-later");
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            ),
            KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&parked)),
            OperationSelector::Any,
        ));
        let mut config = scheduler_config(None);
        config.max_concurrent_maintenance =
            std::num::NonZeroUsize::new(slots).expect("a nonzero slot count");
        // The session's metadata step folds the held session's tail, so a
        // run of the session steps sends requests.
        config.idle_fold_after_ms = Some(1);
        let server = SchedulerServer::start(store.clone(), &config, 0).await;
        server
            .runtime
            .create_namespace(&parked, &test_actor())
            .await
            .expect("create");
        store.block_next();
        let sweep = tokio::spawn({
            let scheduler = server.scheduler.clone();
            async move { scheduler.run_sweep().await }
        });
        store.wait_until_blocked().await;
        server
            .runtime
            .create_namespace(&later, &test_actor())
            .await
            .expect("create after listing");
        let held = server
            .scheduler
            .inner
            .namespaces
            .open(&later)
            .await
            .expect("open");
        held.put_file("/note", b"data", &test_actor())
            .await
            .expect("publish");
        server
            .clock
            .advance_ms(config.maintenance_interval_ms.get());
        store.inner().reset();
        let mut steps = Box::pin(server.scheduler.run_due_steps());
        if slots == 1 {
            assert!(futures::poll!(steps.as_mut()).is_pending());
            assert!(
                store.inner().take().is_empty(),
                "the sweep holds the only slot"
            );
        } else {
            steps.as_mut().await;
            assert!(
                server.metadata_caught_up(&later).await,
                "a free slot lets the session steps finish while the sweep waits"
            );
            assert!(!store.inner().take().is_empty());
        }
        assert!(!sweep.is_finished());
        store.release();
        sweep.await.expect("join the sweep").expect("sweep");
        if slots == 1 {
            steps.await;
        }
        server.runtime.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn cancelled_index_work_and_a_lifecycle_change_during_a_build_stay_dirty() {
    for cancel in [false, true] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = namespace_id("index-during-steps");
        // The gate holds the manifest that publishes the build's steps.
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            ),
            KeyPredicate::prefix(loonfs_objectstore::keys::metadata_manifest_prefix(
                &namespace_id,
            )),
            OperationSelector::PutCreateIfAbsent,
        ));
        let config = scheduler_config(Some(loonfs_grep::GrepWorkerConfig::default()));
        let server = SchedulerServer::start(store.clone(), &config, 0).await;
        server
            .runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create");
        server.enable_index(&namespace_id).await;
        store.block_next();
        let mut steps = Box::pin(server.scheduler.run_due_steps());
        // The index step reads the store before it publishes, so the steps
        // run until the gate holds the index step's put.
        tokio::select! {
            () = steps.as_mut() => panic!("the steps finished without publishing"),
            () = store.wait_until_blocked() => {}
        }
        if cancel {
            drop(steps);
            store.release();
        } else {
            server.disable_index(&namespace_id).await;
            store.release();
            steps.await;
        }
        assert_eq!(
            with_held(&server.scheduler.inner.namespaces, &namespace_id, |held| {
                held.index_dirty
            }),
            Some(true)
        );
        store.inner().reset();
        server.scheduler.run_due_steps().await;
        assert!(
            !store.inner().take().is_empty(),
            "the dirty entry gets another index step"
        );
        server.scheduler.run_due_steps().await;
        assert!(store.inner().take().is_empty());
        server.runtime.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn a_collection_finishing_after_close_does_not_update_the_replacement_entry() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("replacement-during-collection");
    // The runtime raises the hint in the background after each publication,
    // so neither the gate nor the recording selects the hint.
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any().except_hints(),
        ),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&namespace_id))
            .except_hints(),
        OperationSelector::Any,
    ));
    let mut config = scheduler_config(None);
    config.idle_fold_after_ms = Some(0);
    let server = SchedulerServer::start(store.clone(), &config, 0).await;
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let namespaces = &server.scheduler.inner.namespaces;
    let held = namespaces.open(&namespace_id).await.expect("open");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    let seq = held.last_published_seq();
    drop(held);
    server
        .run_steps_after(config.maintenance_interval_ms.get())
        .await;
    server.clock.advance_ms(config.gc_interval_ms.get());
    store.block_next();
    let steps = tokio::spawn({
        let scheduler = server.scheduler.clone();
        async move { scheduler.run_due_steps().await }
    });
    store.wait_until_blocked().await;
    namespaces
        .close_if_idle(&namespace_id, seq, 0, |_| true)
        .await
        .expect("close during collection")
        .expect("the idle session closes");
    let fresh = namespaces
        .open(&namespace_id)
        .await
        .expect("open replacement");
    assert_eq!(fresh.last_published_seq(), None);
    store.release();
    steps.await.expect("finish collection");
    assert_eq!(
        with_held(namespaces, &namespace_id, |held| held
            .host_state
            .collected_seq),
        Some(None)
    );
    store.inner().reset();
    server.run_steps_after(config.gc_interval_ms.get()).await;
    assert!(
        store.inner().take().is_empty(),
        "the replacement never published and owes no collection"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test(start_paused = true)]
async fn a_step_parked_in_one_session_does_not_stop_another_sessions_fold_on_the_next_interval() {
    let directory = tempdir().expect("tempdir");
    let parked = namespace_id("a-parked");
    let folded = namespace_id("b-folded");
    // The runtime raises the hint in the background after each publication,
    // so the gate doesn't select the hint.
    let store = Arc::new(BlockingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::prefix(loonfs_objectstore::keys::namespace_prefix(&parked)).except_hints(),
        OperationSelector::Any,
    ));
    let mut config = scheduler_config(None);
    // The session's metadata step folds a session's tail once the manual
    // clock moves, so the gate parks the steps of `parked` at the fold.
    config.idle_fold_after_ms = Some(1);
    // The runs before the first commit make the step from the store, and
    // that step waits the retry interval, which the manual clock then passes.
    config.metadata_retry_interval_ms = std::num::NonZeroU64::MIN;
    let server = SchedulerServer::start(store.clone(), &config, 0).await;
    server
        .runtime
        .create_namespace(&parked, &test_actor())
        .await
        .expect("create");
    let namespaces = &server.scheduler.inner.namespaces;
    let parked_handle = namespaces.open(&parked).await.expect("open");
    let running = server.scheduler.start();
    // The first sweep starts with the loop. It finishes before the gate is
    // armed, so the gate parks the session steps and not the sweep.
    loonfs_test_support::clock::bounded(Duration::from_secs(60), async {
        while server.counter("loonfs.maintenance.sweeps", &[("result", "ok")]) == 0 {
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the first sweep finishes");
    parked_handle
        .put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    store.block_next();
    server
        .clock
        .advance_ms(config.maintenance_interval_ms.get());
    loonfs_test_support::clock::bounded(Duration::from_secs(60), store.wait_until_blocked())
        .await
        .expect("the session steps park at the fold of the first session");

    server
        .runtime
        .create_namespace(&folded, &test_actor())
        .await
        .expect("create");
    let folded_handle = namespaces.open(&folded).await.expect("open");
    folded_handle
        .put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    server
        .clock
        .advance_ms(config.maintenance_interval_ms.get());
    loonfs_test_support::clock::bounded(Duration::from_secs(60), async {
        while server.wal_tail_objects(&folded).await > 0 {
            tokio::time::advance(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("a later run folds the other session's tail while the first session's step is parked");

    store.release();
    running.cancel();
    running.task.await.expect("the scheduler stops");
    drop((parked_handle, folded_handle));
    server.runtime.shutdown().await.expect("shutdown");
}
