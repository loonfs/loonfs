//! Store requests of snapshot calls on a writer's runtime after a fold:
//! creating a snapshot, reading at it, and deleting it.

use loonfs::{
    CreateSnapshotOptions, FoldWalOutcome, InlineContentPolicy, LoonFs, MaintenanceCancellation,
    MetadataMaintenanceOutcome, Namespace, SharedObjectStore, Writable,
};
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, MemoryStore, RecordingStore};
use loonfs_types::{ChecksumAlgorithm, NamespaceId};
use std::sync::Arc;

/// Which fold leaves the namespace's WAL tail folded before the calls.
#[derive(Debug, Clone, Copy)]
enum Fold {
    /// The session's own fold, which runs after every commit because the
    /// runtime folds at one unfolded inline byte.
    Session,
    /// An explicit fold through the maintenance handle.
    Explicit,
}

struct Fixture {
    recording: Arc<RecordingStore<MemoryStore>>,
    writer: LoonFs<Writable>,
    namespace_id: NamespaceId,
    namespace: Namespace<Writable>,
}

/// Creates a writer runtime and a namespace with four committed files, folds
/// the WAL tail with `fold`, waits until no fold or compaction is running,
/// and lets the hint catch up with the current manifest.
async fn fixture(fold: Fold) -> Fixture {
    let name = format!("snapshot-calls-{fold:?}").to_lowercase();
    let namespace_id = namespace_id(&name);
    let recording = Arc::new(RecordingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let inline_content = match fold {
        Fold::Session => InlineContentPolicy {
            inline_content_fold_at_bytes: 1,
            ..InlineContentPolicy::default()
        },
        Fold::Explicit => InlineContentPolicy::default(),
    };
    let clock = Arc::new(ManualClock::new(1_000_000));
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id(&name))
        .min_publish_interval_ms(0)
        .inline_content(inline_content)
        .monotonic_timer(clock.clone())
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    for index in 0..4 {
        let path = format!("/doc-{index}");
        let put = namespace.put_file(&path, b"doc", &actor);
        put.await.expect("put");
    }
    let maintenance = writer.maintenance(writer_id(&name));
    if let Fold::Explicit = fold {
        let folded = maintenance.fold_wal(&namespace_id).await.expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
    }
    writer.drain().await.expect("the fold settles");
    // The setup publishes several manifests within one hint raise interval,
    // so the hint lags. A metadata step after the interval raises it.
    clock.advance_ms(loonfs_core::limits::MIN_HINT_RAISE_INTERVAL_MS);
    let caught_up = maintenance
        .maintain_metadata_while_due(&namespace_id, &MaintenanceCancellation::new())
        .await
        .expect("metadata step");
    assert_eq!(caught_up.outcome, MetadataMaintenanceOutcome::CaughtUp);
    Fixture {
        recording,
        writer,
        namespace_id,
        namespace,
    }
}

impl Fixture {
    /// Checks that the calls since the last reset sent `expected` requests.
    fn assert_requests(&self, call: &str, expected: usize) {
        let operations = self.recording.take();
        assert_eq!(operations.len(), expected, "{call}: {operations:?}");
    }
}

/// Checks that after a fold, each snapshot creation on the writer's runtime
/// costs 11 store requests, and each read at a snapshot and each deletion
/// costs 2, whichever fold folded the tail.
///
/// A creation loads the current manifest and probes the WAL (6 requests),
/// finds nothing to fold, writes the pin, and loads the current manifest
/// again to verify the pin (4 requests). A read at a snapshot checks the
/// writer position's manifest for a successor with one HEAD and loads the
/// pin, and the pinned manifest's segments are cached. A deletion loads the
/// pin and deletes it.
#[tokio::test]
async fn snapshot_calls_after_a_fold_cost_the_same_whichever_fold_ran() {
    for fold in [Fold::Session, Fold::Explicit] {
        let fixture = fixture(fold).await;
        let mut snapshots = Vec::new();
        for index in 0..3 {
            fixture.recording.reset();
            let name = format!("admin-{index}");
            let options = CreateSnapshotOptions::default();
            let created = fixture.namespace.create_snapshot(&name, &options).await;
            snapshots.push(created.expect("create").snapshot_id);
            fixture.namespace.wait_for_fold().await.expect("settle");
            fixture.assert_requests(&format!("{fold:?}: create {index}"), 11);
        }
        let reader = fixture.namespace.read_only();
        for (index, snapshot) in snapshots.iter().enumerate() {
            fixture.recording.reset();
            let view = reader.read_view_at_snapshot(snapshot).await;
            let stat = view.expect("view").stat(&format!("/doc-{index}")).await;
            stat.expect("stat");
            fixture.assert_requests(&format!("{fold:?}: read at {index}"), 2);
        }
        for (index, snapshot) in snapshots.iter().enumerate() {
            fixture.recording.reset();
            let deleted = fixture.namespace.delete_snapshot(snapshot);
            deleted.await.expect("delete");
            fixture.assert_requests(&format!("{fold:?}: delete {index}"), 2);
        }
    }
}

/// Checks that creating a snapshot of a namespace with 32 fork pins costs
/// the same 11 store requests as creating one with none, so the cost doesn't
/// grow with the number of pins.
///
/// (A count of the namespace's live snapshots before and after the pin
/// write, which loads every pin twice, would make this call cost 86
/// requests.)
#[tokio::test]
async fn a_snapshot_of_a_namespace_with_32_fork_pins_costs_11_requests() {
    for fold in [Fold::Session, Fold::Explicit] {
        let fixture = fixture(fold).await;
        let actor = loonfs_test_support::test_actor();
        for index in 0..32 {
            let target = namespace_id(&format!("fork-{fold:?}-{index}").to_lowercase());
            let forked = fixture
                .writer
                .fork_namespace(&fixture.namespace_id, &target, &actor);
            forked.await.expect("fork");
        }
        fixture.namespace.wait_for_fold().await.expect("settle");
        fixture.recording.reset();
        let options = CreateSnapshotOptions::default();
        let created = fixture.namespace.create_snapshot("forked", &options).await;
        created.expect("create");
        fixture.namespace.wait_for_fold().await.expect("settle");
        fixture.assert_requests(&format!("{fold:?}: create with 32 fork pins"), 11);
    }
}
