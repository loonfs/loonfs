//! Concurrent visits preserve session progress and share the host's slots.

use super::{sweep_config, SweepServer};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::namespace_id;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, OperationClass, RecordedOperation, RecordingStore,
};
use loonfs_test_support::test_actor;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn a_daily_pass_and_a_tick_never_visit_the_same_namespace_together() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("one-visit");
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
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
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    for tick_first in [true, false] {
        held.put_file(
            if tick_first { "/first" } else { "/second" },
            b"data",
            &test_actor(),
        )
        .await
        .expect("publish");
        server.clock.advance_ms(config.tick_interval_ms);
        store.block_next();
        let visit = tokio::spawn({
            let sweep = server.sweep.clone();
            async move {
                if tick_first {
                    sweep.tick().await;
                } else {
                    sweep.run_pass(false).await.expect("daily");
                }
            }
        });
        store.wait_until_blocked().await;
        store.inner().reset();
        if tick_first {
            assert_eq!(server.sweep.run_pass(false).await.expect("daily"), 1);
            let operations = store.inner().take();
            assert_eq!(operations.len(), 1);
            assert!(matches!(&operations[0], RecordedOperation::List { .. }));
            assert_eq!(operations[0].key(), "namespaces/");
        } else {
            server.sweep.tick().await;
            assert!(store.inner().take().is_empty());
        }
        store.release();
        visit.await.expect("visit");
    }
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_publish_during_metadata_maintenance_stays_due() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("concurrent-publish");
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
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
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&namespace_id)
        .await
        .expect("open");
    held.put_file("/first", b"first", &test_actor())
        .await
        .expect("publish");
    server.clock.advance_ms(config.tick_interval_ms);
    store.block_next();
    let visit = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.tick().await }
    });
    store.wait_until_blocked().await;
    held.put_file("/second", b"second", &test_actor())
        .await
        .expect("publish during visit");
    store.release();
    visit.await.expect("finish visit");
    assert!(!held.metadata_caught_up());
    store.inner().reset();
    server.tick_after(config.tick_interval_ms).await;
    assert!(
        !store.inner().take().is_empty(),
        "the concurrent publish stays due"
    );
    server.sweep.tick().await;
    assert!(store.inner().take().is_empty());
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn ticks_share_slots_with_a_parked_daily_pass_and_continue_when_a_slot_is_free() {
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
            OperationClass::Any,
        ));
        let mut config = sweep_config(None);
        config.max_concurrent_maintenance = slots;
        config.idle_fold_after_ms = 0;
        let server = SweepServer::start(store.clone(), &config, 0).await;
        server
            .runtime
            .create_namespace(&parked, &test_actor())
            .await
            .expect("create");
        store.block_next();
        let daily = tokio::spawn({
            let sweep = server.sweep.clone();
            async move { sweep.run_pass(false).await }
        });
        store.wait_until_blocked().await;
        server
            .runtime
            .create_namespace(&later, &test_actor())
            .await
            .expect("create after listing");
        let held = server
            .sweep
            .inner
            .namespaces
            .open(&later)
            .await
            .expect("open");
        held.put_file("/note", b"data", &test_actor())
            .await
            .expect("publish");
        server.clock.advance_ms(config.tick_interval_ms);
        store.inner().reset();
        let mut tick = Box::pin(server.sweep.tick());
        assert!(futures::poll!(tick.as_mut()).is_pending());
        if slots == 1 {
            assert!(
                store.inner().take().is_empty(),
                "the daily pass holds the only slot"
            );
        } else {
            tick.as_mut().await;
            assert!(
                held.metadata_caught_up(),
                "a free slot lets the tick finish while the daily pass waits"
            );
            assert!(!store.inner().take().is_empty());
        }
        assert!(!daily.is_finished());
        store.release();
        daily.await.expect("join daily").expect("daily");
        if slots == 1 {
            tick.await;
        }
        server.runtime.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn cancelled_index_work_and_a_lifecycle_change_during_a_build_stay_dirty() {
    for cancel in [false, true] {
        let directory = tempdir().expect("tempdir");
        let namespace_id = namespace_id("index-during-visit");
        let store = Arc::new(BlockingStore::new(
            RecordingStore::new(
                LocalFsStore::new(directory.path()).expect("store"),
                KeyPredicate::any(),
            ),
            KeyPredicate::prefix(loonfs_grep::keyspace::grep_prefix(&namespace_id)),
            OperationClass::Any,
        ));
        let config = sweep_config(Some(loonfs_grep::GrepWorkerConfig::default()));
        let server = SweepServer::start(store.clone(), &config, 0).await;
        server
            .runtime
            .create_namespace(&namespace_id, &test_actor())
            .await
            .expect("create");
        server.enable_index(&namespace_id).await;
        store.block_next();
        let mut tick = Box::pin(server.sweep.tick());
        assert!(futures::poll!(tick.as_mut()).is_pending());
        store.wait_until_blocked().await;
        if cancel {
            drop(tick);
            store.release();
        } else {
            server.disable_index(&namespace_id).await;
            store.release();
            tick.await;
        }
        assert_eq!(
            server
                .sweep
                .inner
                .namespaces
                .with_held(&namespace_id, |held| held.index_dirty),
            Some(true)
        );
        store.inner().reset();
        server.sweep.tick().await;
        assert!(
            !store.inner().take().is_empty(),
            "the dirty entry is visited again"
        );
        server.sweep.tick().await;
        assert!(store.inner().take().is_empty());
        server.runtime.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn a_collection_finishing_after_close_does_not_update_the_replacement_entry() {
    let directory = tempdir().expect("tempdir");
    let namespace_id = namespace_id("replacement-during-collection");
    let store = Arc::new(BlockingStore::new(
        RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        ),
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
        .expect("create");
    let namespaces = &server.sweep.inner.namespaces;
    let held = namespaces.open(&namespace_id).await.expect("open");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    server.tick_after(config.tick_interval_ms).await;
    server.clock.advance_ms(config.gc_interval_ms);
    store.block_next();
    let visit = tokio::spawn({
        let sweep = server.sweep.clone();
        async move { sweep.tick().await }
    });
    store.wait_until_blocked().await;
    namespaces
        .close(&namespace_id)
        .await
        .expect("close during collection");
    let fresh = namespaces
        .open(&namespace_id)
        .await
        .expect("open replacement");
    assert_eq!(fresh.last_published_seq(), None);
    store.release();
    visit.await.expect("finish collection");
    assert_eq!(
        namespaces.with_held(&namespace_id, |held| held.collected_seq),
        Some(None)
    );
    store.inner().reset();
    server.tick_after(config.gc_interval_ms).await;
    assert!(
        store.inner().take().is_empty(),
        "the replacement never published and owes no collection"
    );
    server.runtime.shutdown().await.expect("shutdown");
}
