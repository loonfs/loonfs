//! Acknowledging a namespace's retention floor through maintenance: a fold
//! acknowledges a floor that the writer committed against, and a fence
//! acknowledges the floor of a namespace that nobody wrote to.

use crate::common::{compact_until_done, writer_epoch};
use loonfs::{
    AcknowledgeRetentionFloorOutcome, ChangeSeq, DestinationBehavior, ErrorCode, LoonFs, Namespace,
    NamespaceId, PutFileOptions, RetentionFloors, RevisionNo, SharedObjectStore, Writable,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_test_support::clock::ManualClock;
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{KeyPredicate, RecordedOperation, RecordingStore};
use loonfs_test_support::test_actor;
use std::sync::Arc;
use tempfile::tempdir;

type Store = Arc<RecordingStore<LocalFsStore>>;

fn recording_store(root: &std::path::Path) -> Store {
    Arc::new(RecordingStore::new(
        LocalFsStore::new(root).expect("local store"),
        KeyPredicate::any(),
    ))
}

/// A runtime over `store` whose wall clock reads `clock`.
async fn runtime(store: &Store, clock: &Arc<ManualClock>, writer: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id(writer))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .build()
        .await
        .expect("build the runtime")
}

/// The keys of the WAL objects that `operations` created.
fn wal_puts(operations: &[RecordedOperation], namespace_id: &NamespaceId) -> Vec<String> {
    let prefix = loonfs_objectstore::keys::wal_prefix(namespace_id);
    operations
        .iter()
        .filter_map(|operation| match operation {
            RecordedOperation::Put { key, .. } if key.starts_with(&prefix) => Some(key.clone()),
            _ => None,
        })
        .collect()
}

async fn epoch(store: &Store, namespace_id: &NamespaceId) -> u64 {
    writer_epoch(&(store.clone() as SharedObjectStore), namespace_id).await
}

/// Writes one file through a writer that then shuts down, folds it, and
/// returns the file's commit.
async fn seed_one_folded_commit(
    store: &Store,
    clock: &Arc<ManualClock>,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    let seed = runtime(store, clock, "seed").await;
    seed.create_namespace(namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let committed_seq = seed
        .open_namespace(namespace_id)
        .expect("open the namespace")
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    seed.maintenance(writer_id("seed"))
        .fold_wal(namespace_id)
        .await
        .expect("fold the commit");
    seed.shutdown().await.expect("stop the seed writer");
    committed_seq
}

/// Checks that acknowledging the floor of a namespace that nobody wrote to
/// after an advance writes exactly one fence, which records the floor,
/// raises the writer epoch by one, and leaves the acknowledged floor at the
/// retention floor, and that a second call publishes nothing.
#[tokio::test]
async fn an_idle_namespace_is_acknowledged_by_one_fence_and_then_left_alone() {
    let temp_dir = tempdir().expect("tempdir");
    let store = recording_store(temp_dir.path());
    let clock = Arc::new(ManualClock::at_system_time());
    let namespace_id = namespace_id("idle");
    let floor = seed_one_folded_commit(&store, &clock, &namespace_id).await;
    clock.advance_ms(loonfs::GC_DEFAULT_GRACE_WINDOW_MS);
    let maintainer = runtime(&store, &clock, "maintainer").await;
    let maintenance = maintainer.maintenance(writer_id("maintainer"));
    maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    let epoch_before = epoch(&store, &namespace_id).await;
    store.reset();

    let acknowledged = maintenance
        .acknowledge_retention_floor(&namespace_id)
        .await
        .expect("acknowledge the floor");
    assert_eq!(
        acknowledged.outcome,
        AcknowledgeRetentionFloorOutcome::Fenced
    );
    assert_eq!(
        (acknowledged.before, acknowledged.after),
        (
            RetentionFloors {
                retention_floor_seq: floor,
                acknowledged_floor_seq: ChangeSeq(0),
            },
            RetentionFloors {
                retention_floor_seq: floor,
                acknowledged_floor_seq: floor,
            },
        )
    );
    let fences = wal_puts(&store.take(), &namespace_id);
    assert_eq!(fences.len(), 1, "one fence and no commit: {fences:?}");
    let bytes = store
        .get(&fences[0], None)
        .await
        .expect("read the fence")
        .expect("the fence exists");
    let fence = loonfs_types::format::wal::decode_wal_object_envelope_zstd(&bytes)
        .expect("decode the fence")
        .into_payload();
    assert!(fence.records.is_empty(), "{fence:?}");
    assert_eq!(fence.basis_floor_seq, floor);
    assert_eq!(epoch(&store, &namespace_id).await, epoch_before + 1);

    store.reset();
    let again = maintenance
        .acknowledge_retention_floor(&namespace_id)
        .await
        .expect("acknowledge the floor again");
    assert_eq!(
        again.outcome,
        AcknowledgeRetentionFloorOutcome::AlreadyAcknowledged
    );
    assert_eq!(
        (again.before, again.after),
        (acknowledged.after, acknowledged.after)
    );
    let operations = store.take();
    assert!(
        operations.iter().all(|operation| matches!(
            operation,
            RecordedOperation::Get { .. }
                | RecordedOperation::GetWithMetadata { .. }
                | RecordedOperation::Head { .. }
        )),
        "a second call only reads: {operations:?}"
    );
    assert_eq!(epoch(&store, &namespace_id).await, epoch_before + 1);
    maintainer.shutdown().await.expect("shut down");
}

/// Checks that once the writer commits after an advance, acknowledging the
/// floor folds that commit's WAL object, which records the advanced floor,
/// without a fence or a new writer epoch, and that the writer session keeps
/// committing.
#[tokio::test]
async fn a_namespace_whose_writer_committed_after_the_advance_is_acknowledged_by_a_fold() {
    let temp_dir = tempdir().expect("tempdir");
    let store = recording_store(temp_dir.path());
    let clock = Arc::new(ManualClock::at_system_time());
    let namespace_id = namespace_id("active");
    let writer = runtime(&store, &clock, "writer").await;
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let floor = namespace
        .put_file("/old.txt", b"history", &test_actor())
        .await
        .expect("write a file")
        .committed_seq;
    let maintenance = writer.maintenance(writer_id("writer"));
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the commit");
    clock.advance_ms(loonfs::GC_DEFAULT_GRACE_WINDOW_MS);
    maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    namespace
        .put_file("/new.txt", b"after the advance", &test_actor())
        .await
        .expect("commit after the advance");
    let epoch_before = epoch(&store, &namespace_id).await;
    writer.drain().await.expect("drain");
    store.reset();

    let acknowledged = maintenance
        .acknowledge_retention_floor(&namespace_id)
        .await
        .expect("acknowledge the floor");
    assert_eq!(
        acknowledged.outcome,
        AcknowledgeRetentionFloorOutcome::Folded
    );
    assert_eq!(
        (
            acknowledged.before.acknowledged_floor_seq,
            acknowledged.after
        ),
        (
            ChangeSeq(0),
            RetentionFloors {
                retention_floor_seq: floor,
                acknowledged_floor_seq: floor,
            },
        )
    );
    assert_eq!(wal_puts(&store.take(), &namespace_id), Vec::<String>::new());
    assert_eq!(epoch(&store, &namespace_id).await, epoch_before);
    namespace
        .put_file("/later.txt", b"still the writer", &test_actor())
        .await
        .expect("the session still commits");
    writer.shutdown().await.expect("shut down");
}

/// The revision numbers of `path`'s retained revisions, newest first.
async fn revision_nos(namespace: &Namespace<Writable>, path: &str) -> Vec<RevisionNo> {
    namespace
        .list_file_revisions(path)
        .page()
        .await
        .expect("list the revisions")
        .revisions
        .iter()
        .map(|revision| revision.revision_no)
        .collect()
}

/// Checks that a restore copies a revision at or below the floor only while
/// it's the file's newest at or below the floor, whether or not the older
/// revisions are still stored, and that a base compaction removes the older
/// revisions once a fold acknowledges the floor, and not before.
#[tokio::test]
async fn a_restore_finds_only_the_newest_revision_at_or_below_the_floor() {
    let temp_dir = tempdir().expect("tempdir");
    let store = recording_store(temp_dir.path());
    let clock = Arc::new(ManualClock::at_system_time());
    let namespace_id = namespace_id("restore");
    // The writer's maintenance compacts every family group that has a delta
    // run, so each compaction below ends in one base run per group.
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .wall_clock(clock.clone())
        .run_policy(loonfs_core::test_support::one_delta_run_policy())
        .build()
        .await
        .expect("build the runtime");
    writer
        .create_namespace(&namespace_id, &test_actor())
        .await
        .expect("create the namespace");
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open the namespace");
    let maintenance = writer.maintenance(writer_id("writer"));
    let replace = PutFileOptions {
        behavior: DestinationBehavior::Replace,
        ..PutFileOptions::default()
    };
    // Each revision is folded into its own run, so the compactions below are
    // base compactions, which apply the floor.
    let mut floor = ChangeSeq(0);
    for body in ["draft one", "draft two", "draft three"] {
        floor = namespace
            .put_file_with_options("/report.txt", body.as_bytes(), &test_actor(), &replace)
            .await
            .expect("write a revision")
            .committed_seq;
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold the revision");
    }
    clock.advance_ms(loonfs::GC_DEFAULT_GRACE_WINDOW_MS);
    let advanced = maintenance
        .advance_retention_floor(&namespace_id)
        .await
        .expect("advance the floor");
    assert_eq!(advanced.retention_floor_seq, floor);

    // The older revisions are still stored, and a restore finds neither.
    for revision_no in [RevisionNo(1), RevisionNo(2)] {
        let error = namespace
            .restore_revision("/report.txt", revision_no, &test_actor())
            .await
            .expect_err("an older revision at or below the floor isn't restored");
        assert_eq!(error.code(), ErrorCode::RevisionNotFound, "{error:?}");
    }
    compact_until_done(&maintenance, &namespace_id).await;
    assert_eq!(
        revision_nos(&namespace, "/report.txt").await,
        [RevisionNo(3), RevisionNo(2), RevisionNo(1)],
        "a compaction before the writer's log acknowledges the floor keeps every revision"
    );

    // The restore is validated against the floor, and its fold acknowledges
    // the floor, so the next base compaction removes the older revisions.
    namespace
        .restore_revision("/report.txt", RevisionNo(3), &test_actor())
        .await
        .expect("restore the newest revision at the floor");
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold the restore");
    compact_until_done(&maintenance, &namespace_id).await;
    assert_eq!(
        revision_nos(&namespace, "/report.txt").await,
        [RevisionNo(4), RevisionNo(3)]
    );
    assert_eq!(
        namespace
            .read_file("/report.txt")
            .await
            .expect("read the restored file")
            .bytes,
        b"draft three"
    );
    let error = namespace
        .restore_revision("/report.txt", RevisionNo(2), &test_actor())
        .await
        .expect_err("a removed revision isn't restored");
    assert_eq!(error.code(), ErrorCode::RevisionNotFound, "{error:?}");
    writer.shutdown().await.expect("shut down");
}
