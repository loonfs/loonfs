//! Metadata retry, collection, and close costs for held sessions.

use super::{
    held_ids, idle_fold_after_ms, list_count, metadata_caught_up, seed_unfolded_tail, seed_writer,
    sweep_config, SweepServer, DAY_MS, UNPOSITIONED_TAIL_READ_REQUESTS,
};
use crate::sweep::retries::SweepCall;
use crate::sweep::GarbageCollection;
use loonfs::SharedObjectStore;
use loonfs_grep::GrepWorkerConfig;
use loonfs_http::WriterSessions as _;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    write_keys, BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation,
    RecordingStore,
};
use loonfs_test_support::test_actor;
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

/// Checks that a tick asks a session for its metadata work right after a
/// commit, that a session with nothing due costs no store requests, and that
/// a waiting tail is asked about again at the metadata retry time and folded
/// at its idle fold age.
#[tokio::test]
async fn ticks_ask_each_session_and_fold_idle_tails_then_cost_nothing() {
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
    server.tick_after(config.tick_interval_ms).await;
    let first = store.take();
    assert_eq!(
        first.len(),
        UNPOSITIONED_TAIL_READ_REQUESTS,
        "only the first tick reads the tail of a session that never published: {first:#?}"
    );
    for _ in 0..100 {
        server.tick_after(config.tick_interval_ms).await;
    }
    assert!(
        store.take().is_empty(),
        "the session keeps its caught-up answer until it publishes"
    );
    assert!(metadata_caught_up(&held).await);
    for index in 0..2 {
        let commit = held
            .put_file(&format!("/note-{index}"), b"data", &test_actor())
            .await
            .expect("publish");
        assert_eq!(held.last_published_seq(), Some(commit.committed_seq));
        store.reset();
        server.sweep.tick().await;
        assert!(
            store.take().is_empty(),
            "a tick right after a commit reads the waiting tail from the session's position"
        );
        assert!(server.metadata_left_due(&namespace_id));
        let retry_at_ms = || server.retry(&namespace_id, SweepCall::Metadata).retry_at_ms;
        let first_retry_at_ms = retry_at_ms();
        server.tick_after(config.maintenance_interval_ms - 1).await;
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
        server.tick_after(idle_fold_after_ms(&config)).await;
        assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
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
    server.tick_after(config.tick_interval_ms).await;
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

/// Checks that a tick's metadata step on a held namespace waits for the
/// fold that the namespace's session is running, instead of folding the same
/// tail at the same time. The session's fold is parked at its first content
/// write while the tick runs. Afterward, each content object of the tail has
/// been written once, and no manifest put has failed its precondition.
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
    tokio::join!(server.tick_after(config.tick_interval_ms), release);

    let operations = store.take();
    let put_keys = |keys: KeyPredicate| -> Vec<String> {
        operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    RecordedOperation::Put { .. }
                        | RecordedOperation::PutStreamed { .. }
                        | RecordedOperation::Assemble { .. }
                )
            })
            .map(RecordedOperation::key)
            .filter(|key| keys.matches(key))
            .map(str::to_owned)
            .collect()
    };
    let content_puts = put_keys(KeyPredicate::content_blob());
    assert_eq!(content_puts.len(), FILES, "{content_puts:#?}");
    assert_eq!(
        content_puts.iter().collect::<BTreeSet<_>>().len(),
        FILES,
        "{content_puts:#?}"
    );
    let manifest_puts = put_keys(KeyPredicate::manifest(&namespace_id));
    assert_eq!(
        manifest_puts.iter().collect::<BTreeSet<_>>().len(),
        manifest_puts.len(),
        "{manifest_puts:#?}"
    );
    assert_eq!(server.wal_tail_objects(&namespace_id).await, 0);
    assert!(metadata_caught_up(&held).await);
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a daily pass makes no metadata call on a namespace that the
/// host holds a session for, and folds the idle tail of a namespace without
/// one. The held session never published, so its first tick reads the tail
/// that the earlier writer left and folds it, and the session then closes
/// once it's idle.
#[tokio::test]
async fn a_daily_pass_leaves_a_held_namespace_to_the_ticks_and_visits_one_without_a_session() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let shared: SharedObjectStore = store.clone();
    let (writer, now_ms) = seed_writer(&shared).await;
    let [held_id, unheld_id] = ["held-by-the-host", "without-a-session"].map(namespace_id);
    for namespace_id in [&held_id, &unheld_id] {
        seed_unfolded_tail(&writer, namespace_id).await;
    }
    writer.shutdown().await.expect("shutdown writer");
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, now_ms + DAY_MS).await;
    let namespaces = &server.sweep.inner.namespaces;
    drop(namespaces.open(&held_id).await.expect("open"));
    server
        .sweep
        .run_pass(GarbageCollection::Skip)
        .await
        .expect("daily pass");
    assert_eq!(server.wal_tail_objects(&unheld_id).await, 0);
    assert!(
        server.wal_tail_objects(&held_id).await > 0,
        "the pass leaves the held namespace's idle tail to the ticks"
    );

    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        server.wal_tail_objects(&held_id).await,
        0,
        "the tick asks the session, which folds the tail it reads"
    );
    store.reset();
    server
        .tick_after(config.idle_session_close_after_ms + 1)
        .await;
    assert!(held_ids(namespaces).is_empty(), "the idle session closes");
    let closing = store.take();
    assert_eq!(
        closing.len(),
        UNPOSITIONED_TAIL_READ_REQUESTS,
        "the session's answer expired at the revalidation bound, so the tick reads the \
         store once, and closing costs no requests: {closing:#?}"
    );
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a held session reads the store once after a fork of its
/// namespace folds the session's tail and drops the session's position, and
/// that later ticks cost no requests.
#[tokio::test]
async fn a_held_source_reads_the_store_once_after_a_fork_drops_its_position() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let config = sweep_config(None);
    let server = SweepServer::start(store.clone(), &config, 0).await;
    let source = namespace_id("forked-source");
    server
        .runtime
        .create_namespace(&source, &test_actor())
        .await
        .expect("create");
    let held = server
        .sweep
        .inner
        .namespaces
        .open(&source)
        .await
        .expect("open");
    held.put_file("/note", b"data", &test_actor())
        .await
        .expect("publish");
    server
        .runtime
        .fork_namespace(&source, &namespace_id("forked-copy"), &test_actor())
        .await
        .expect("fork");
    // A runtime skips a hint raise less than a second after its last raise
    // of the same hint, so the test raises the hint to the current manifest
    // before it counts the tick's requests.
    loonfs_core::test_support::namespace_store(store.as_ref(), &source)
        .raise_hint_to_current_manifest()
        .await;
    store.reset();
    server.tick_after(config.tick_interval_ms).await;
    let first = store.take();
    assert_eq!(
        first.len(),
        UNPOSITIONED_TAIL_READ_REQUESTS,
        "the first tick reads the tail of a session without a position: {first:#?}"
    );
    assert!(metadata_caught_up(&held).await);
    for _ in 0..20 {
        server.tick_after(config.tick_interval_ms).await;
    }
    assert!(
        store.take().is_empty(),
        "the session keeps its caught-up answer until it publishes"
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
    assert!(server.metadata_left_due(&namespace_id));
    store.reset();
    server.tick_after(config.maintenance_interval_ms).await;
    assert!(
        !store.take().is_empty(),
        "the unit cap leaves this session due"
    );
    for _ in 0..8 {
        server.tick_after(config.maintenance_interval_ms).await;
    }
    assert!(!server.metadata_left_due(&namespace_id));
    store.reset();
    server.sweep.tick().await;
    assert!(store.take().is_empty(), "the backlog eventually catches up");
    server.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session whose compactor another runtime has claimed finds
/// the claim before it plans a compaction unit. Its position doesn't show
/// the claim, so one HEAD finds the manifest after the position's manifest,
/// and the session loads the current manifest, finds the newer compactor
/// epoch, and stops without writing a segment or a manifest. The session
/// waits for the metadata retry interval before it tries again.
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
    let position_manifest_no = server
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics")
        .current_manifest_no
        .expect("manifest");
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
    let current = server
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .expect("diagnostics")
        .current_manifest_no;
    store.reset();
    server.tick_after(config.tick_interval_ms).await;
    let operations = store.take();
    let successor = loonfs_objectstore::keys::metadata_manifest_object(
        &namespace_id,
        &position_manifest_no.successor().expect("the next manifest"),
    );
    let successor_heads = operations
        .iter()
        .filter(
            |operation| matches!(operation, RecordedOperation::Head { key } if *key == successor),
        )
        .count();
    assert_eq!(
        successor_heads, 1,
        "one HEAD finds the claim above the position: {operations:#?}"
    );
    assert_eq!(
        write_keys(&operations, &KeyPredicate::any()),
        Vec::<String>::new(),
        "the session writes no segment and no manifest"
    );
    assert_eq!(
        operations.len(),
        6,
        "the position's manifest, the HEAD, and a load of the current manifest: {operations:#?}"
    );
    assert_eq!(
        server
            .maintenance
            .diagnostics(&namespace_id)
            .await
            .expect("diagnostics")
            .current_manifest_no,
        current,
        "the fenced unit publishes nothing"
    );
    store.reset();
    assert!(server.metadata_left_due(&namespace_id));
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
    server.tick_after(config.tick_interval_ms).await;
    server.sweep.tick().await;
    store.reset();
    server
        .tick_after(config.gc_interval_ms - config.tick_interval_ms - 1)
        .await;
    assert_eq!(
        list_count(&store.take()),
        0,
        "the sessions' positions passed the revalidation bound, so the tick reads \
         each tail, but collects nothing before the interval"
    );
    server.tick_after(1).await;
    let operations = store.take();
    assert_eq!(
        operations
            .iter()
            .filter(|op| matches!(op, RecordedOperation::List { .. }))
            .count(),
        11,
        "collection lists the namespace's garbage without listing namespaces"
    );
    assert!(operations.iter().all(|op| op
        .key()
        .starts_with(&loonfs_objectstore::keys::namespace_prefix(&moved))));
    server.tick_after(config.gc_interval_ms).await;
    assert_eq!(
        list_count(&store.take()),
        0,
        "an unchanged session is never collected by ticks"
    );
    held.put_file("/again", b"data", &test_actor())
        .await
        .expect("publish");
    store.reset();
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        list_count(&store.take()),
        11,
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
    server.tick_after(config.tick_interval_ms).await;
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
        .tick_after(config.idle_session_close_after_ms - config.tick_interval_ms)
        .await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "the threshold must be passed"
    );
    let idle = store.take();
    assert_eq!(
        idle.len(),
        UNPOSITIONED_TAIL_READ_REQUESTS,
        "the session's position passed the revalidation bound, so the tick reads the \
         store once: {idle:#?}"
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
        .tick_after(config.idle_session_close_after_ms + 1)
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
/// caller has its handle, and doesn't ask the fenced session for metadata
/// work while a caller has it, and that a tick closes the session of a
/// namespace that another runtime deleted.
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
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        held_ids(namespaces),
        std::slice::from_ref(&fenced),
        "the caller's handle keeps the fenced session open"
    );
    assert!(
        store.take().is_empty(),
        "the tick doesn't ask a fenced session for metadata work"
    );
    drop(held);
    server.tick_after(config.tick_interval_ms).await;
    assert!(held_ids(namespaces).is_empty(), "the fenced session closes");

    drop(namespaces.open(&deleted).await.expect("open"));
    other
        .open_namespace(&deleted)
        .expect("open the other runtime's session")
        .delete()
        .await
        .expect("delete the namespace");
    server.tick_after(config.tick_interval_ms).await;
    assert!(
        held_ids(namespaces).is_empty(),
        "the session of a deleted namespace closes"
    );
    assert_eq!(server.visit_failures(SweepCall::Metadata), 0);
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
    config.idle_session_close_after_ms = 1;
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
    assert_eq!(
        store.take().len(),
        UNPOSITIONED_TAIL_READ_REQUESTS,
        "only the first tick reads the tail of a session that never published"
    );
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
    server.tick_after(config.tick_interval_ms).await;
    assert_eq!(
        held_ids(namespaces).len(),
        1,
        "unfinished metadata prevents close"
    );
    assert!(!store.take().is_empty());
    assert!(server.wal_tail_objects(&unfolded).await > 0);
    store.reset();
    server
        .tick_after(idle_fold_after_ms(&config) - config.tick_interval_ms - 1)
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
    assert_eq!(
        handle.idle_fold_due_at_ms(),
        Some(DAY_MS + idle_fold_after_ms(&config))
    );
    drop(handle);

    server.tick_after(config.tick_interval_ms).await;
    assert!(server.wal_tail_objects(&namespace_id).await > 0);
    store.reset();
    server
        .tick_after(idle_fold_after_ms(&config) - config.tick_interval_ms - 1)
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
        .tick_after(config.idle_session_close_after_ms - idle_fold_after_ms(&config))
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
