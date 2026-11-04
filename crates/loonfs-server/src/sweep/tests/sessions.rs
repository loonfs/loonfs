//! Metadata retry, collection, and close costs for held sessions.

use super::{
    held_ids, idle_fold_after_ms, list_count, seed_unfolded_tail, seed_writer, sweep_config,
    SweepServer, DAY_MS,
};
use crate::sweep::retries::SweepCall;
use crate::sweep::GarbageCollection;
use loonfs::SharedObjectStore;
use loonfs_grep::GrepWorkerConfig;
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    write_keys, BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordingStore,
};
use loonfs_test_support::test_actor;
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

/// Checks that a session that never published gets no metadata step, that a
/// tick right after a commit reads the waiting tail from the session's
/// position without a store request, that the step waits for its retry time,
/// and that the tick at the idle fold age folds the tail, after which the
/// ticks cost nothing.
#[tokio::test]
async fn ticks_fold_idle_tails_from_the_position_and_then_cost_nothing() {
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
        server.tick_after(config.tick_interval_ms.get()).await;
    }
    assert!(
        store.take().is_empty(),
        "a session without a writer position gets no metadata step"
    );
    assert_eq!(held.last_published_seq(), None);
    for index in 0..2 {
        let commit = held
            .put_file(&format!("/note-{index}"), b"data", &test_actor())
            .await
            .expect("publish");
        assert_eq!(held.last_published_seq(), Some(commit.committed_seq));
        assert!(held.has_current_writer_position());
        store.reset();
        server.sweep.tick().await;
        assert!(
            store.take().is_empty(),
            "a tick right after a commit reads the waiting tail from the session's position"
        );
        assert!(server.metadata_left_due(&namespace_id));
        let retry_at_ms = || server.retry(&namespace_id, SweepCall::Metadata).retry_at_ms;
        let first_retry_at_ms = retry_at_ms();
        server
            .tick_after(config.maintenance_interval_ms.get() - 1)
            .await;
        assert_eq!(
            retry_at_ms(),
            first_retry_at_ms,
            "metadata waits for its retry time"
        );
        server.tick_after(1).await;
        assert!(
            retry_at_ms() > first_retry_at_ms,
            "unfinished metadata is retried"
        );
        assert!(
            store.take().is_empty(),
            "the retry reads the session's position too"
        );
        assert!(server.wal_tail_objects(&namespace_id).await > 0);
        server
            .tick_after(idle_fold_after_ms(&config) - config.maintenance_interval_ms.get())
            .await;
        assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
        server.sweep.tick().await;
        store.reset();
        for _ in 0..20 {
            server.tick_after(config.tick_interval_ms.get()).await;
        }
        assert!(
            store.take().is_empty(),
            "a session with nothing due costs no requests"
        );
    }
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tick folds at the runtime's configured inline fold size, so
/// a quiet session's 3 MiB tail under an 8 MiB trigger waits for the idle
/// fold age.
#[tokio::test]
async fn a_tick_leaves_a_tail_below_the_configured_inline_fold_size_until_it_is_idle() {
    const MIB: usize = 1024 * 1024;
    const VALUE_BYTES: usize = 256 * 1024;
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.inline_content.inline_content_threshold_bytes = Some(VALUE_BYTES);
    config.inline_content.inline_content_fold_at_bytes = 8 * MIB;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("large-fold-trigger");
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
    for index in 0..3 * MIB / VALUE_BYTES {
        held.put_file(
            &format!("/value-{index}"),
            &vec![b'x'; VALUE_BYTES],
            &test_actor(),
        )
        .await
        .expect("publish");
    }
    let content_prefix = loonfs_objectstore::keys::content_prefix(&namespace_id);
    assert!(
        store
            .take()
            .iter()
            .all(|operation| !operation.key().starts_with(&content_prefix)),
        "every value is written inline, so the tail has 3 MiB of inline bytes"
    );
    let tail_objects = server.wal_tail_objects(&namespace_id).await;
    server.tick_after(config.tick_interval_ms.get()).await;
    assert!(
        server.metadata_left_due(&namespace_id),
        "the tail waits for its idle fold age"
    );
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        tail_objects,
        "a tail below the configured fold size stays unfolded"
    );
    server.tick_after(idle_fold_after_ms(&config)).await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tick's metadata step waits for the fold that the
/// session is running, instead of folding the same tail at the same time.
/// The session's fold is parked at its first content write while the tick
/// runs. Afterward, each content object of the tail has been written once,
/// and no manifest put has failed its precondition.
#[tokio::test]
async fn a_tick_waits_for_the_sessions_fold_instead_of_folding_the_tail_again() {
    const FILES: usize = 8;
    const FILE_BYTES: usize = 1024;
    // The memory store responds at once, so a second fold that ran
    // alongside the parked one would write its content before the release.
    let blocking = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
    ));
    let store = Arc::new(RecordingStore::new(blocking.clone(), KeyPredicate::any()));
    let mut config = sweep_config(None);
    config.inline_content.inline_content_fold_at_bytes = FILES * FILE_BYTES;
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("held-folding");
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
    blocking.block_next();
    for index in 0..FILES {
        let fill = b'a' + u8::try_from(index).expect("a small index");
        held.put_file(
            &format!("/file-{index}"),
            &[fill; FILE_BYTES],
            &test_actor(),
        )
        .await
        .expect("publish");
    }
    blocking.wait_until_blocked().await;
    let release = async {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        blocking.release();
    };
    tokio::join!(server.tick_after(config.tick_interval_ms.get()), release);

    let operations = store.take();
    let content_puts = write_keys(&operations, &KeyPredicate::content_blob());
    assert_eq!(content_puts.len(), FILES, "{content_puts:#?}");
    assert_eq!(
        content_puts.iter().collect::<BTreeSet<_>>().len(),
        FILES,
        "{content_puts:#?}"
    );
    let manifest_puts = write_keys(&operations, &KeyPredicate::manifest(&namespace_id));
    assert_eq!(
        manifest_puts.iter().collect::<BTreeSet<_>>().len(),
        manifest_puts.len(),
        "{manifest_puts:#?}"
    );
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    assert!(server.metadata_caught_up(&namespace_id).await);
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a daily pass folds every idle tail: the tail of a held
/// session with a current writer position, the tail of a held session
/// without a position, and the tail of a namespace without a session. The
/// session without a position never published, so the ticks give it no
/// metadata step, and its tail, which an earlier writer left, waits for the
/// pass.
#[tokio::test]
async fn a_daily_pass_folds_every_idle_tail() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let (writer, now_ms) = seed_writer(&shared).await;
    let [positioned, unpositioned, unheld] =
        ["positioned", "unpositioned", "without-a-session"].map(namespace_id);
    for namespace_id in [&unpositioned, &unheld] {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("shutdown writer");
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, now_ms + DAY_MS).await;
    let namespaces = &server.sweep.inner.namespaces;
    server
        .runtime
        .create_namespace(&positioned, &test_actor())
        .await
        .expect("create");
    let held = namespaces.open(&positioned).await.expect("open");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    drop(namespaces.open(&unpositioned).await.expect("open"));
    for _ in 0..3 {
        server.tick_after(config.tick_interval_ms.get()).await;
    }
    assert!(
        server.wal_tail_objects(&unpositioned).await > 0,
        "the ticks give a session without a position no metadata step"
    );
    assert!(server.wal_tail_objects(&positioned).await > 0);
    server.clock.advance_ms(idle_fold_after_ms(&config));
    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("daily pass");
    for namespace_id in [&positioned, &unpositioned, &unheld] {
        assert_eq!(
            server.wal_tail_objects(namespace_id).await,
            0,
            "the pass folds the idle tail of {namespace_id}"
        );
    }
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tick folds a tail at its idle fold age after the session's
/// writer position has passed the revalidation bound. The tick right after
/// the commit reads the idle fold's due time from the position. The idle
/// fold age is longer than the bound, so until the age the ticks give the
/// session no metadata step, and at the age one step reads the tail from the
/// store and folds it. The fold leaves the session a current position again,
/// so later ticks cost nothing.
#[tokio::test]
async fn a_tick_folds_an_idle_tail_after_the_position_passes_the_bound() {
    const IDLE_FOLD_AFTER_MS: u64 = 25 * 60 * 1000;
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = Some(IDLE_FOLD_AFTER_MS);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("stale-position");
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
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    server.sweep.tick().await;
    server
        .tick_after(loonfs_core::limits::READ_REVALIDATION_BOUND_MS + 1)
        .await;
    assert!(!held.has_current_writer_position());
    store.reset();
    server
        .tick_after(IDLE_FOLD_AFTER_MS - loonfs_core::limits::READ_REVALIDATION_BOUND_MS - 2)
        .await;
    assert!(
        store.take().is_empty(),
        "a session without a position gets no metadata step before the idle fold age"
    );
    server.tick_after(1).await;
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    assert!(held.has_current_writer_position());
    server.sweep.tick().await;
    store.reset();
    for _ in 0..20 {
        server.tick_after(config.tick_interval_ms.get()).await;
    }
    assert!(store.take().is_empty(), "the idle fold runs once");
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tick folds a tail at its idle fold age when no tick ran
/// between the session's commit and its writer position passing the
/// revalidation bound. The first tick after the bound reads the tail from
/// the store, because the session has committed since the sweep last dated
/// its tail, and that step reports when the idle fold comes due. The ticks
/// until that age then cost nothing, and the step at that age folds the
/// tail.
#[tokio::test]
async fn a_tick_folds_an_idle_tail_when_no_tick_saw_the_position() {
    const IDLE_FOLD_AFTER_MS: u64 = 25 * 60 * 1000;
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = Some(IDLE_FOLD_AFTER_MS);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespace_id = namespace_id("unseen-position");
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
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    store.reset();
    server
        .tick_after(loonfs_core::limits::READ_REVALIDATION_BOUND_MS + 1)
        .await;
    assert!(!held.has_current_writer_position());
    assert!(
        !store.take().is_empty(),
        "the first tick after the commit dates the tail from the store"
    );
    assert!(server.idle_fold_due_at_ms(&namespace_id).is_some());
    server
        .tick_after(IDLE_FOLD_AFTER_MS - loonfs_core::limits::READ_REVALIDATION_BOUND_MS - 2)
        .await;
    assert!(
        store.take().is_empty(),
        "the ticks before the idle fold age make no request"
    );
    server.tick_after(1).await;
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "the step at the idle fold age folds the tail"
    );
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
    assert_eq!(
        server
            .sweep
            .run_pass(GarbageCollection::Collect)
            .await
            .expect("daily pass"),
        1
    );
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
    for index in 0..4 * loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() {
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
    server.tick_after(config.tick_interval_ms.get()).await;
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
    assert!(server.metadata_left_due(&namespace_id));
    store.reset();
    server
        .tick_after(config.maintenance_interval_ms.get())
        .await;
    assert!(
        !store.take().is_empty(),
        "the unit cap leaves this session due"
    );
    for _ in 0..8 {
        server
            .tick_after(config.maintenance_interval_ms.get())
            .await;
    }
    assert!(server.metadata_caught_up(&namespace_id).await);
    store.reset();
    server.sweep.tick().await;
    assert!(store.take().is_empty(), "the backlog eventually catches up");
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session whose compactor another runtime has claimed finds
/// the claim before it plans a compaction unit. Its position doesn't show
/// the claim, so one HEAD finds the manifests after the position's manifest,
/// the position moves onto them, and the unit stops as fenced without
/// writing a segment or a manifest. The session's own claim manifest is in
/// the segment cache from its publication, so the tick doesn't read it
/// again. The session waits for the metadata retry interval before it tries
/// again.
#[tokio::test]
async fn a_fenced_compactor_stops_before_a_unit_and_waits_for_the_retry_interval() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = Some(0);
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
    for index in 0..2 * loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS.get() {
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
        .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        .await
        .expect("claim compactor");
    let other = loonfs::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("other"))
        .build()
        .await
        .expect("other runtime");
    other
        .maintenance(writer_id("other"))
        .compact_metadata(&namespace_id, &loonfs::MaintenanceCancellation::new())
        .await
        .expect("claim newer compactor");
    // A runtime skips a hint raise less than a second after its last raise
    // of the same hint, so the test raises the hint to the current manifest
    // before it counts the tick's requests.
    loonfs_core::test_support::namespace_store(store.as_ref(), &namespace_id)
        .raise_hint_to_current_manifest()
        .await;
    store.reset();
    server.tick_after(config.tick_interval_ms.get()).await;
    let operations = store.take();
    assert_eq!(
        write_keys(&operations, &KeyPredicate::any()),
        Vec::<String>::new(),
        "the session writes no segment and no manifest"
    );
    assert_eq!(operations.len(), 5, "{operations:#?}");
    assert!(server.metadata_left_due(&namespace_id));
    for _ in 0..59 {
        server.tick_after(config.tick_interval_ms.get()).await;
    }
    assert!(
        store.take().is_empty(),
        "a fenced compactor is not retried on every tick"
    );
    server.tick_after(config.tick_interval_ms.get()).await;
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
    config.idle_fold_after_ms = Some(0);
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
    server.tick_after(config.tick_interval_ms.get()).await;
    server.sweep.tick().await;
    store.reset();
    server
        .tick_after(config.gc_interval_ms.get() - config.tick_interval_ms.get() - 1)
        .await;
    assert!(store.take().is_empty());
    server.tick_after(1).await;
    let operations = store.take();
    assert_eq!(
        list_count(&operations),
        10,
        "collection lists the namespace's garbage without listing namespaces"
    );
    assert!(operations.iter().all(|op| op
        .key()
        .starts_with(&loonfs_objectstore::keys::namespace_prefix(&moved))));
    server.tick_after(config.gc_interval_ms.get()).await;
    assert!(
        store.take().is_empty(),
        "an unchanged session is never collected by ticks"
    );
    held.put_file("/again", b"data", &test_actor())
        .await
        .expect("publish");
    store.reset();
    server.tick_after(config.tick_interval_ms.get()).await;
    assert_eq!(
        list_count(&store.take()),
        10,
        "the first tick after the commit collects, because collection comes before the \
         index step"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn idle_sessions_close_only_after_the_idle_threshold_and_all_callers_release_them() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(Some(GrepWorkerConfig::default()));
    config.idle_fold_after_ms = Some(0);
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
    server.tick_after(config.tick_interval_ms.get()).await;
    server.sweep.tick().await;
    drop(held);
    let first_epoch = loonfs_core::test_support::namespace_store(&*store, &namespace_id)
        .load_read_anchor(loonfs_core::time::Observation::now(std::sync::Arc::new(
            loonfs_types::StdMonotonicTimer::default(),
        )))
        .await
        .expect("anchor")
        .read_state
        .writer_epoch;
    store.reset();
    server
        .tick_after(config.idle_session_close_after_ms.get() - config.tick_interval_ms.get())
        .await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "the threshold must be passed"
    );
    assert!(
        store.take().is_empty(),
        "a session with nothing due costs zero requests"
    );
    let request = namespaces
        .open(&namespace_id)
        .await
        .expect("open resets idle age");
    let clone = request.clone();
    drop(request);
    server.tick_after(1).await;
    assert_eq!(held_ids(namespaces).len(), 1);
    assert!(store.take().is_empty());
    server
        .tick_after(config.idle_session_close_after_ms.get() + 1)
        .await;
    server.sweep.tick().await;
    store.reset();
    server.sweep.tick().await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "a request clone prevents closing"
    );
    assert!(
        store.take().is_empty(),
        "checking a clone costs zero requests"
    );
    drop(clone);
    server.sweep.tick().await;
    assert!(held_ids(namespaces).is_empty());
    assert!(store.take().is_empty(), "closing costs zero requests");
    let reopened = namespaces.open(&namespace_id).await.expect("reopen");
    assert_eq!(reopened.last_published_seq(), None);
    reopened
        .put_file("/after-close", b"data", &test_actor())
        .await
        .expect("publish");
    assert_eq!(
        loonfs_core::test_support::namespace_store(&*store, &namespace_id)
            .load_read_anchor(loonfs_core::time::Observation::now(std::sync::Arc::new(
                loonfs_types::StdMonotonicTimer::default()
            )))
            .await
            .expect("anchor")
            .read_state
            .writer_epoch
            .0,
        first_epoch.0 + 1
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a tick closes a session that another writer fenced once no
/// caller has its handle, without a store request, and that a tick closes
/// the session of a namespace that the session deleted.
#[tokio::test]
async fn ticks_close_a_fenced_session_and_the_session_of_a_deleted_namespace() {
    let directory = tempdir().expect("tempdir");
    // The runtimes raise hints in the background after each publication, so
    // the recording doesn't select the hint.
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any().except_hints(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let [fenced, deleted] = ["fenced", "deleted"].map(namespace_id);
    for id in [&fenced, &deleted] {
        server
            .runtime
            .create_namespace(id, &test_actor())
            .await
            .expect("create");
    }
    let other = loonfs::LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("other"))
        .build()
        .await
        .expect("other runtime");
    let held = namespaces.open(&fenced).await.expect("open");
    held.put_file("/mine", b"data", &test_actor())
        .await
        .expect("publish");
    other
        .open_namespace(&fenced)
        .expect("open the other writer's session")
        .put_file("/theirs", b"data", &test_actor())
        .await
        .expect("the other writer takes the namespace");
    let error = held
        .put_file("/refused", b"data", &test_actor())
        .await
        .expect_err("the session is fenced");
    assert_eq!(error.code(), loonfs::ErrorCode::WriterFenced, "{error:?}");
    store.reset();
    server.tick_after(config.tick_interval_ms.get()).await;
    assert_eq!(
        held_ids(namespaces),
        std::slice::from_ref(&fenced),
        "the caller's handle keeps the fenced session open"
    );
    assert!(
        store.take().is_empty(),
        "the tick gives a fenced session no metadata step"
    );
    drop(held);
    server.tick_after(config.tick_interval_ms.get()).await;
    assert!(held_ids(namespaces).is_empty(), "the fenced session closes");

    let held = namespaces.open(&deleted).await.expect("open");
    held.delete().await.expect("delete the namespace");
    drop(held);
    server.tick_after(config.tick_interval_ms.get()).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "the session of a deleted namespace closes"
    );
    assert_eq!(server.visit_failures(SweepCall::Metadata), 0);
    other.shutdown().await.expect("shutdown other");
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session whose namespace another runtime deleted closes when
/// its index step finds the namespace deleted, though its position shows no
/// metadata work, so the metadata step doesn't see the deletion.
#[tokio::test]
async fn a_session_closes_when_its_index_step_finds_the_namespace_deleted() {
    let directory = tempdir().expect("tempdir");
    let store: SharedObjectStore = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let config = sweep_config(Some(GrepWorkerConfig::default()));
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("deleted-elsewhere");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    namespaces
        .open(&namespace_id)
        .await
        .expect("open")
        .put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    let other = loonfs::LoonFs::builder_with_store(store)
        .writer_id(writer_id("other"))
        .build()
        .await
        .expect("other runtime");
    other
        .open_namespace(&namespace_id)
        .expect("open the other runtime's session")
        .delete()
        .await
        .expect("delete the namespace");
    server.tick_after(config.tick_interval_ms.get()).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "the session of a deleted namespace closes"
    );
    assert_eq!(server.visit_failures(SweepCall::GrepIndex), 0);
    other.shutdown().await.expect("shutdown other");
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
    config.idle_session_close_after_ms = std::num::NonZeroU64::MIN;
    config.idle_fold_after_ms = Some(60_000);
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
    assert_eq!(held_ids(namespaces).len(), 1);
    assert!(store.take().is_empty());
    server.tick_after(1).await;
    assert!(held_ids(namespaces).is_empty());
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
    server.tick_after(config.tick_interval_ms.get()).await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "unfinished metadata prevents close"
    );
    assert!(!store.take().is_empty());
    assert!(server.wal_tail_objects(&unfolded).await > 0);
    store.reset();
    server
        .tick_after(idle_fold_after_ms(&config) - config.tick_interval_ms.get() - 1)
        .await;
    assert!(
        store.take().is_empty(),
        "a waiting tail needs no read before its age passes"
    );
    server.tick_after(1).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "the tick that folds the tail finds nothing else due and closes the session"
    );
    assert_eq!(server.wal_tail_objects(&unfolded).await, 0);
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session whose own fold and compaction caught its metadata
/// up leaves the ticks nothing to do, so they cost no requests.
#[tokio::test]
async fn a_session_that_maintained_itself_leaves_the_ticks_nothing_to_do() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let clock = Arc::new(loonfs_test_support::clock::ManualClock::new(0));
    let runtime = loonfs::LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("self-maintenance"))
        .wall_clock(clock.clone())
        .monotonic_timer(clock.clone())
        .config(loonfs::RuntimeConfig {
            inline_content: loonfs::InlineContentPolicy {
                inline_content_fold_at_bytes: 1,
                ..loonfs::InlineContentPolicy::default()
            },
            ..loonfs::RuntimeConfig::default()
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
    let recorder = loonfs::metrics::DefaultMetricsRecorder::new();
    let namespaces = Arc::new(loonfs_http::Namespaces::new(runtime.clone()));
    namespaces.open(&namespace_id).await.expect("hold");
    let sweep = super::Sweep::new(
        sweep_config(None).sweep_config(),
        runtime.object_store(),
        runtime.maintenance(writer_id("maintenance")),
        namespaces,
        runtime.monotonic_timer(),
        None,
        &recorder,
    );
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

/// Checks that a tick reads the runtime's paused timer, so a held session's
/// tail folds and the session closes exactly when that timer says each is idle.
#[tokio::test]
async fn a_tick_sees_a_session_as_idle_exactly_when_the_runtime_timer_does() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let mut config = sweep_config(None);
    config.idle_fold_after_ms = Some(60_000);
    // The runtime's timer starts a day ahead of the system timer, so a host
    // that read a second timer would disagree with it.
    let server = SweepServer::start(store.clone(), &config, DAY_MS).await;
    let namespaces = &server.sweep.inner.namespaces;
    let namespace_id = namespace_id("one-host-clock");
    server
        .runtime
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create");
    let handle = namespaces.open(&namespace_id).await.expect("open");
    handle
        .put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    drop(handle);

    server.tick_after(config.tick_interval_ms.get()).await;
    assert_eq!(
        server.idle_fold_due_at_ms(&namespace_id),
        Some(DAY_MS + idle_fold_after_ms(&config)),
        "the step reports the idle fold on the runtime's timer"
    );
    assert!(server.wal_tail_objects(&namespace_id).await > 0);
    store.reset();
    server
        .tick_after(idle_fold_after_ms(&config) - config.tick_interval_ms.get() - 1)
        .await;
    assert!(
        store.take().is_empty(),
        "the tail is not idle yet on the runtime's timer"
    );
    server.tick_after(1).await;
    assert_eq!(
        server.wal_tail_objects(&namespace_id).await,
        0,
        "the tail folds once it is idle"
    );

    server
        .tick_after(config.idle_session_close_after_ms.get() - idle_fold_after_ms(&config))
        .await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "the session is not idle yet on the runtime's timer"
    );
    server.tick_after(1).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "the session closes once it is idle"
    );
    server.runtime.shutdown().await.expect("shutdown");
}
