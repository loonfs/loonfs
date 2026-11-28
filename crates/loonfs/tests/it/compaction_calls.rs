//! A host's metadata call on a namespace that has a writer session open:
//! how a close of the session stops the call's compaction, why the session's
//! folds and commits never wait for that compaction, and how two calls'
//! compaction units take turns.

use loonfs::{
    ActorId, ErrorCode, LoonFs, MaintenanceCancellation, Namespace, RuntimeConfig, Writable,
};
use loonfs_test_support::clock::bounded;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    write_keys, BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordingStore,
};
use loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS;
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

/// WAL objects that make the session's fold due. The backlog folds after
/// each commit, so its commits leave one object each.
const FOLD_AT_WAL_OBJECTS: NonZeroU64 = NonZeroU64::new(2).unwrap();

/// A runtime over a store that can park a metadata segment read, with a
/// held session whose namespace has more delta runs than one compaction
/// unit merges.
struct Backlog {
    store: Arc<BlockingStore<MemoryStore>>,
    recording: Arc<RecordingStore<Arc<BlockingStore<MemoryStore>>>>,
    runtime: LoonFs<Writable>,
    held: Namespace<Writable>,
    actor: ActorId,
}

async fn backlog(name: &str) -> Backlog {
    let store = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::metadata_segment(),
        OperationSelector::Read,
    ));
    let recording = Arc::new(RecordingStore::new(store.clone(), KeyPredicate::any()));
    let runtime = LoonFs::builder_with_store(recording.clone(), loonfs::Context::Service)
        .writer_id(writer_id("compaction-calls"))
        .session_fold_at_wal_objects(FOLD_AT_WAL_OBJECTS)
        .config(RuntimeConfig {
            ..RuntimeConfig::default()
        })
        .build()
        .await
        .expect("runtime");
    let actor = loonfs_test_support::test_actor();
    let namespace_id = namespace_id(name);
    runtime
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("create");
    let held = runtime.open_namespace(&namespace_id).expect("open");
    let maintenance = runtime.clone();
    // One fold per commit leaves more delta runs than one compaction unit
    // merges.
    for index in 0..2 * DEFAULT_MAX_DELTA_RUNS.get() {
        held.put_file(&format!("/note-{index}"), b"data", &actor)
            .await
            .expect("publish");
        maintenance
            .open_namespace(&namespace_id)
            .expect("open namespace")
            .fold_wal()
            .await
            .expect("fold");
    }
    Backlog {
        store,
        recording,
        runtime,
        held,
        actor,
    }
}

impl Backlog {
    /// Commits files until the tail reaches the fold trigger, so the session
    /// starts its fold, and waits for that fold.
    async fn publish_over_the_fold_trigger(&self) {
        for index in 0..FOLD_AT_WAL_OBJECTS.get() {
            self.held
                .put_file(&format!("/trigger-{index}"), b"data", &self.actor)
                .await
                .expect("publish toward the fold trigger");
        }
        self.held.wait_for_fold().await.expect("the session's fold");
    }
}

/// Checks that a close of a session waits for a host's metadata call that is
/// in a compaction unit of the session's namespace, and that the close's
/// stop ends the call's unit, so the unit publishes nothing over a session
/// that the close ended, and the call returns `writer_session_closed`, the
/// session's refusal, instead of a report. The call is parked at its first
/// metadata segment read, and a fold of the session then starts the
/// session's own compaction, which waits for the namespace's compaction lock
/// until the close cancels it.
#[tokio::test]
async fn a_close_waits_for_a_hosts_compaction_and_cancels_its_unit() {
    let backlog = backlog("closing").await;
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let maintenance = backlog.runtime.clone();
    let handle = maintenance
        .open_namespace(backlog.held.id())
        .expect("open namespace");
    let call = handle.maintain_metadata_while_due(&cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    backlog.publish_over_the_fold_trigger().await;
    let close = backlog.held.clone().close();
    tokio::pin!(close);
    assert!(
        bounded(Duration::from_millis(200), &mut close)
            .await
            .is_err(),
        "the close waits for the call's unit"
    );
    backlog.store.release();
    let (closed, maintained) = tokio::join!(close, call);
    closed.expect("close");
    let error = maintained.expect_err("the close stops the call's unit before its publication");
    assert_eq!(error.code(), ErrorCode::WriterSessionClosed, "{error:?}");
    backlog.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a host's metadata call whose session another writer fenced
/// while the call's unit ran returns `writer_fenced`, the session's
/// refusal, which a server sends once more in a new session, instead of a
/// report whose outcome is `due`. The call is parked at its first metadata
/// segment read while a rival runtime claims the namespace and a commit of
/// the session finds the rival's claim, which sets the session's stop.
#[tokio::test]
async fn a_hosts_call_whose_session_is_fenced_during_its_unit_returns_writer_fenced() {
    let backlog = backlog("fenced-call").await;
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let handle = backlog
        .runtime
        .open_namespace(backlog.held.id())
        .expect("open namespace");
    let call = handle.maintain_metadata_while_due(&cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    let rival = LoonFs::builder_with_store(backlog.store.clone(), loonfs::Context::Service)
        .writer_id(writer_id("fenced-call-rival"))
        .build()
        .await
        .expect("the rival's runtime");
    rival
        .open_namespace(backlog.held.id())
        .expect("open on the rival")
        .put_file("/rival", b"rival", &backlog.actor)
        .await
        .expect("the rival claims with a commit");
    let error = backlog
        .held
        .put_file("/fenced", b"fenced", &backlog.actor)
        .await
        .expect_err("the rival's claim fences the session");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    backlog.store.release();
    let error = call.await.expect_err("the fence stops the call's unit");
    assert_eq!(error.code(), ErrorCode::WriterFenced, "{error:?}");
    rival.shutdown().await.expect("shutdown the rival");
    backlog.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a session's fold and commits never wait for a host's
/// metadata call on the session's namespace. The session has no position
/// when the call starts, because a fork of its namespace dropped it, and the
/// call's compaction unit is parked at its first metadata segment read. A
/// commit that reaches the fold trigger then completes, and the session's
/// fold publishes, while the call is still compacting.
#[tokio::test]
async fn a_sessions_fold_runs_while_a_hosts_metadata_call_compacts() {
    let backlog = backlog("folding").await;
    backlog
        .runtime
        .fork_namespace(
            backlog.held.id(),
            &namespace_id("folding-copy"),
            &backlog.actor,
        )
        .await
        .expect("fork");
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let maintenance = backlog.runtime.clone();
    let namespace_2 = maintenance
        .open_namespace(backlog.held.id())
        .expect("open namespace");
    let call = namespace_2.maintain_metadata_while_due(&cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    bounded(
        Duration::from_secs(10),
        backlog.publish_over_the_fold_trigger(),
    )
    .await
    .expect("the commit and the session's fold don't wait for the call");
    let diagnostics = backlog
        .runtime
        .maintenance()
        .diagnostics(backlog.held.id())
        .await
        .expect("diagnostics");
    assert_eq!(
        diagnostics.wal_tail_objects, 0,
        "the session's fold published"
    );
    assert!(
        futures::poll!(call.as_mut()).is_pending(),
        "the call is still compacting"
    );
    backlog.store.release();
    call.await.expect("the call");
    backlog.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a host's metadata call whose compaction unit waits for the
/// namespace's compaction lock plans from the manifest that the unit before
/// it published, though the call read an older manifest before it took the
/// lock. A fork drops the session's position, so each call reads the current
/// manifest from the store, and the first call's unit is parked at its first
/// metadata segment read while the second call waits for the lock. No
/// manifest put of either call loses to the other's, so no unit is
/// abandoned.
#[tokio::test]
async fn two_metadata_calls_plan_their_units_from_each_others_manifests() {
    let backlog = backlog("overlapping").await;
    let copy_id = namespace_id("overlapping-copy");
    let namespace_id = backlog.held.id();
    backlog
        .runtime
        .fork_namespace(namespace_id, &copy_id, &backlog.actor)
        .await
        .expect("fork");
    let maintenance = backlog.runtime.clone();
    backlog.recording.reset();
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let namespace_3 = maintenance
        .open_namespace(namespace_id)
        .expect("open namespace");
    let first = namespace_3.maintain_metadata_while_due(&cancellation);
    tokio::pin!(first);
    tokio::select! {
        _ = &mut first => panic!("the first call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    let namespace_4 = maintenance
        .open_namespace(namespace_id)
        .expect("open namespace");
    let second = namespace_4.maintain_metadata_while_due(&cancellation);
    let release = async {
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        backlog.store.release();
    };
    let (first, second, ()) = tokio::join!(first, second, release);
    let units = first.expect("the first call").response.compaction_units
        + second.expect("the second call").response.compaction_units;
    assert!(units > 0, "the calls compact the backlog");
    let manifest_puts = write_keys(
        &backlog.recording.take(),
        &KeyPredicate::manifest(namespace_id),
    );
    let distinct: BTreeSet<_> = manifest_puts.iter().collect();
    assert_eq!(
        distinct.len(),
        manifest_puts.len(),
        "a manifest put failed its precondition: {manifest_puts:#?}"
    );
    backlog.runtime.shutdown().await.expect("shutdown");
}
