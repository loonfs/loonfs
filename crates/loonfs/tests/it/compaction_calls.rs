//! A host's metadata call on a namespace that has a writer session open:
//! how a close of the session stops the call's compaction, why the session's
//! folds and commits never wait for that compaction, and how two calls'
//! compaction units take turns.

use loonfs::{
    ActorId, LoonFs, Maintenance, MaintenanceCancellation, Namespace, RuntimeConfig, Writable,
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
    let runtime = LoonFs::builder_with_store(recording.clone())
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
    let maintenance = runtime.maintenance(writer_id("compaction-calls"));
    // One fold per commit leaves more delta runs than one compaction unit
    // merges.
    for index in 0..2 * DEFAULT_MAX_DELTA_RUNS.get() {
        held.put_file(&format!("/note-{index}"), b"data", &actor)
            .await
            .expect("publish");
        maintenance.fold_wal(&namespace_id).await.expect("fold");
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
    fn maintenance(&self) -> Maintenance {
        self.runtime.maintenance(writer_id("compaction-calls"))
    }

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

/// Checks that a close of a session returns while a host's metadata call is
/// in a compaction unit of the session's namespace, and that the call stops
/// after that unit. The call is parked at its first metadata segment read,
/// and a fold of the session then starts the session's own compaction,
/// which waits for the namespace's compaction lock until the close cancels
/// it.
#[tokio::test]
async fn a_close_returns_during_a_hosts_compaction_and_stops_it_after_its_unit() {
    let backlog = backlog("closing").await;
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let maintenance = backlog.maintenance();
    let call = maintenance.maintain_metadata_while_due(backlog.held.id(), &cancellation);
    tokio::pin!(call);
    tokio::select! {
        _ = &mut call => panic!("the call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    backlog.publish_over_the_fold_trigger().await;
    bounded(Duration::from_secs(10), backlog.held.clone().close())
        .await
        .expect("the close doesn't wait for the call's unit")
        .expect("close");
    backlog.store.release();
    let maintained = call.await.expect("the call");
    assert_eq!(
        maintained.response.compaction_units, 1,
        "the close stops the call after its unit in progress"
    );
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
    let maintenance = backlog.maintenance();
    let call = maintenance.maintain_metadata_while_due(backlog.held.id(), &cancellation);
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
    let maintenance = backlog.maintenance();
    maintenance
        .compact_metadata(namespace_id, &MaintenanceCancellation::new())
        .await
        .expect("claim the compactor epoch");
    backlog.recording.reset();
    backlog.store.block_next();
    let cancellation = MaintenanceCancellation::new();
    let first = maintenance.maintain_metadata_while_due(namespace_id, &cancellation);
    tokio::pin!(first);
    tokio::select! {
        _ = &mut first => panic!("the first call finished without reading a segment"),
        () = backlog.store.wait_until_blocked() => {}
    }
    let second = maintenance.maintain_metadata_while_due(namespace_id, &cancellation);
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
