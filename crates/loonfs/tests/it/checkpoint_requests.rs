//! Store requests of checkpoint calls on a writer's runtime after a fold:
//! creating a checkpoint, reading at it, and deleting it.

use loonfs::{
    FoldWalOutcome, InlineContentPolicy, LoonFs, Maintenance, Namespace, SharedObjectStore,
    Writable,
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
    namespace_id: NamespaceId,
    namespace: Namespace<Writable>,
    maintenance: Maintenance,
}

/// Creates a writer runtime and a namespace with four committed files, folds
/// the WAL tail with `fold`, and waits until no fold or compaction is
/// running.
async fn fixture(fold: Fold) -> Fixture {
    let name = format!("checkpoint-calls-{fold:?}").to_lowercase();
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
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id(&name))
        .min_publish_interval_ms(0)
        .inline_content(inline_content)
        .monotonic_timer(Arc::new(ManualClock::new(1_000_000)))
        .build()
        .await
        .expect("writer");
    let actor = loonfs_test_support::test_actor();
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace = writer.open_namespace(&namespace_id).expect("open");
    let maintenance = writer.maintenance(writer_id(&name));
    for index in 0..4 {
        let path = format!("/doc-{index}");
        let put = namespace.put_file(&path, b"doc", &actor);
        put.await.expect("put");
    }
    if let Fold::Explicit = fold {
        let folded = maintenance.fold_wal(&namespace_id).await.expect("fold");
        assert_eq!(folded.outcome, FoldWalOutcome::Published);
    }
    writer.drain().await.expect("the fold settles");
    Fixture {
        recording,
        namespace_id,
        namespace,
        maintenance,
    }
}

impl Fixture {
    /// Checks that the calls since the last reset sent `expected` requests.
    fn assert_requests(&self, call: &str, expected: usize) {
        let operations = self.recording.take();
        assert_eq!(operations.len(), expected, "{call}: {operations:?}");
    }
}

/// Checks that after a fold, each checkpoint creation on the writer's
/// runtime costs 11 store requests, and each read at a checkpoint and each
/// deletion costs 2, whichever fold folded the tail.
///
/// A creation loads the current manifest and probes the WAL (6 requests),
/// finds nothing to fold, writes the pin, and loads the current manifest
/// again to verify the pin (4 requests). A read at a checkpoint checks the
/// writer position's manifest for a successor with one HEAD and loads the
/// pin, and the pinned manifest's segments are cached. A deletion loads the
/// pin and deletes it.
#[tokio::test]
async fn checkpoint_calls_after_a_fold_cost_the_same_whichever_fold_ran() {
    for fold in [Fold::Session, Fold::Explicit] {
        let fixture = fixture(fold).await;
        let (maintenance, namespace_id) = (&fixture.maintenance, &fixture.namespace_id);
        let mut checkpoints = Vec::new();
        for index in 0..3 {
            fixture.recording.reset();
            let name = format!("admin-{index}");
            let created = maintenance.create_checkpoint(namespace_id, &name).await;
            checkpoints.push(created.expect("create").checkpoint_id);
            fixture.namespace.wait_for_fold().await.expect("settle");
            fixture.assert_requests(&format!("{fold:?}: create {index}"), 11);
        }
        let reader = fixture.namespace.read_only();
        for (index, checkpoint) in checkpoints.iter().enumerate() {
            fixture.recording.reset();
            let view = reader.read_view_at_checkpoint(checkpoint).await;
            let stat = view.expect("view").stat(&format!("/doc-{index}")).await;
            stat.expect("stat");
            fixture.assert_requests(&format!("{fold:?}: read at {index}"), 2);
        }
        for (index, checkpoint) in checkpoints.iter().enumerate() {
            fixture.recording.reset();
            let deleted = maintenance.delete_checkpoint(namespace_id, checkpoint);
            deleted.await.expect("delete");
            fixture.assert_requests(&format!("{fold:?}: delete {index}"), 2);
        }
    }
}
