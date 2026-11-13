//! Tests of how many times the operations that fold a namespace's WAL tail
//! fold it: a merge or a rebase, a snapshot, a fork, the maintenance calls
//! that fold, and a delete. Every fold of a namespace in a runtime runs
//! under the namespace's fold lock, so a fold that the namespace's writer
//! session is already running isn't repeated, and without a session the
//! operation folds the tail itself, once.

use loonfs::{
    ActorId, LoonFs, MaintainMetadataOptions, Maintenance, MaintenanceCancellation, Namespace,
    NamespaceId, RuntimeConfig, Writable,
};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    write_keys, BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordingStore,
};
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;

/// The number of files that the fork writes, each stored inline in the
/// fork's WAL tail.
const FILES: usize = 8;
/// The size of each file, in bytes.
const FILE_BYTES: usize = 1024;
/// The WAL objects that make a session's fold due: the session's claim
/// writes a fence, so the last of the fork's [`FILES`] files brings the tail
/// to the fold trigger.
const FOLD_AT_WAL_OBJECTS: NonZeroU64 = NonZeroU64::new(FILES as u64 + 1).unwrap();

type Store = RecordingStore<Arc<BlockingStore<MemoryStore>>>;

/// A runtime with a namespace and its fork, over a store that can park a
/// content write. The fork's session folds once the fork's tail has
/// [`FILES`] files.
struct Family {
    blocking: Arc<BlockingStore<MemoryStore>>,
    store: Arc<Store>,
    runtime: LoonFs<Writable>,
    main: Namespace<Writable>,
    fork: Namespace<Writable>,
}

/// An operation that folds the fork's WAL tail.
#[derive(Debug, Clone, Copy)]
enum Operation {
    /// A merge of the fork into main.
    Merge,
    /// A snapshot of the fork.
    Snapshot,
    /// A fork of the fork's head.
    Fork,
    /// [`Maintenance::fold_wal`] on the fork.
    FoldWal,
    /// [`Maintenance::maintain_metadata_while_due_with_options`] on the fork,
    /// the call that the HTTP `metadata` job makes, with a fold threshold of
    /// one WAL object.
    Metadata,
}

fn actor() -> ActorId {
    loonfs_test_support::test_actor()
}

async fn family() -> Family {
    let blocking = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        KeyPredicate::content_blob(),
        OperationSelector::Put,
    ));
    let store = Arc::new(RecordingStore::new(blocking.clone(), KeyPredicate::any()));
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("session-folds"))
        .session_fold_at_wal_objects(FOLD_AT_WAL_OBJECTS)
        .config(RuntimeConfig {
            ..RuntimeConfig::default()
        })
        .build()
        .await
        .expect("runtime");
    let [main_id, fork_id] = ["main", "main-fork"].map(namespace_id);
    runtime
        .create_namespace(&main_id, &actor())
        .await
        .expect("main");
    let main = runtime.open_namespace(&main_id).expect("open main");
    main.put_file("/seed.txt", b"seed", &actor())
        .await
        .expect("seed main");
    runtime
        .fork_namespace(&main_id, &fork_id, &actor())
        .await
        .expect("fork");
    let fork = runtime.open_namespace(&fork_id).expect("open the fork");
    Family {
        blocking,
        store,
        runtime,
        main,
        fork,
    }
}

impl Family {
    /// Writes `files` distinct files in the fork, each inline.
    async fn write_fork(&self, files: usize) {
        for index in 0..files {
            let fill = b'a' + u8::try_from(index).expect("a small index");
            self.fork
                .put_file(&format!("/fork-{index}.txt"), &[fill; FILE_BYTES], &actor())
                .await
                .expect("write in the fork");
        }
    }

    /// Writes [`FILES`] files in the fork, so the last write starts the
    /// session's fold, and parks that fold at its first content write. The
    /// store's log is cleared before the writes, because the fold's other
    /// content writes can finish before the first one parks.
    async fn write_fork_and_park_its_fold(&self) {
        self.store.reset();
        self.blocking.block_next();
        self.write_fork(FILES).await;
        self.blocking.wait_until_blocked().await;
    }

    /// Runs `route` while the parked fold stays parked, and releases the
    /// fold once `route` has had time to start a fold of its own.
    async fn run_beside_the_parked_fold<T>(
        &self,
        route: impl std::future::Future<Output = T>,
    ) -> T {
        let release = async {
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            self.blocking.release();
        };
        let (output, ()) = tokio::join!(route, release);
        output
    }

    fn maintenance(&self) -> Maintenance {
        self.runtime.maintenance(writer_id("session-folds"))
    }

    /// Runs `operation` on the fork. A merge checks that it merged through
    /// the fork's last commit.
    async fn fold(&self, operation: Operation) {
        let fork_id = self.fork.id();
        match operation {
            Operation::Merge => {
                let last = self.fork.last_published_seq();
                let merged = self
                    .main
                    .merge_fork(fork_id, &actor())
                    .await
                    .expect("the merge");
                assert_eq!(
                    Some(merged.merge.expect("a merge record").source_through_seq),
                    last
                );
            }
            Operation::Snapshot => {
                self.fork
                    .create_snapshot_with_options(&loonfs::CreateSnapshotOptions {
                        name: Some("beside".to_owned()),
                        ..loonfs::CreateSnapshotOptions::default()
                    })
                    .await
                    .expect("the snapshot");
            }
            Operation::Fork => {
                self.runtime
                    .fork_namespace(fork_id, &namespace_id("fork-copy"), &actor())
                    .await
                    .expect("the fork");
            }
            Operation::FoldWal => {
                self.maintenance()
                    .fold_wal(fork_id)
                    .await
                    .expect("the fold");
            }
            Operation::Metadata => {
                let options = MaintainMetadataOptions {
                    max_wal_tail_objects: NonZeroU64::MIN,
                };
                self.maintenance()
                    .maintain_metadata_while_due_with_options(
                        fork_id,
                        &MaintenanceCancellation::new(),
                        &options,
                    )
                    .await
                    .expect("the metadata call");
            }
        }
    }

    /// Checks that the store recorded one write of each of the fork's
    /// `files` content objects and, for each of `namespaces`, no manifest
    /// put to a manifest number that an earlier put had already taken.
    fn assert_folded_once(&self, what: &str, files: usize, namespaces: &[&NamespaceId]) {
        let operations = self.store.take();
        let content_puts = write_keys(&operations, &KeyPredicate::content_blob());
        let distinct: BTreeSet<_> = content_puts.iter().collect();
        assert_eq!(content_puts.len(), files, "{what}: {content_puts:#?}");
        assert_eq!(distinct.len(), files, "{what}: {content_puts:#?}");
        for namespace_id in namespaces {
            let manifest_puts = write_keys(&operations, &KeyPredicate::manifest(namespace_id));
            let distinct: BTreeSet<_> = manifest_puts.iter().collect();
            assert_eq!(
                distinct.len(),
                manifest_puts.len(),
                "{what}: a manifest put of `{namespace_id}` failed its precondition: {manifest_puts:#?}"
            );
        }
    }
}

/// Checks that each operation that folds a fork whose session is folding
/// waits for that fold instead of folding the same tail at the same time,
/// so each of the tail's content objects is written once, and no manifest
/// put of the fork or of main fails.
#[tokio::test]
async fn folds_of_a_namespace_wait_for_its_sessions_fold() {
    for operation in [
        Operation::Merge,
        Operation::Snapshot,
        Operation::Fork,
        Operation::FoldWal,
        Operation::Metadata,
    ] {
        let family = family().await;
        family.write_fork_and_park_its_fold().await;
        family
            .run_beside_the_parked_fold(family.fold(operation))
            .await;
        family.assert_folded_once(
            &format!("{operation:?}"),
            FILES,
            &[family.fork.id(), family.main.id()],
        );
        family.runtime.shutdown().await.expect("shutdown");
    }
}

/// Checks that a rebase of a fork whose session is folding waits for that
/// fold, as a merge does.
#[tokio::test]
async fn a_rebase_waits_for_the_fork_sessions_fold() {
    let family = family().await;
    let rebase_id = namespace_id("main-rebase");
    family
        .runtime
        .fork_namespace_for_rebase(family.main.id(), family.fork.id(), &rebase_id, &actor())
        .await
        .expect("the rebase's new fork");
    let rebase = family
        .runtime
        .open_namespace(&rebase_id)
        .expect("open the new fork");
    family.write_fork_and_park_its_fold().await;
    let replayed = family
        .run_beside_the_parked_fold(rebase.replay_fork(family.fork.id(), &actor()))
        .await
        .expect("the replay");
    assert_eq!(replayed.conflicts, Some(Vec::new()));
    family.assert_folded_once("a rebase", FILES, &[family.fork.id(), &rebase_id]);
    family.runtime.shutdown().await.expect("shutdown");
}

/// Checks that each operation that folds a fork without a session in the
/// runtime folds the fork's tail itself, once. The fork writes one file
/// fewer than its fold trigger, so its session ends with the whole tail
/// unfolded. A snapshot needs the fork's session, so it isn't one of the
/// operations.
#[tokio::test]
async fn folds_of_a_namespace_without_a_session_fold_its_tail_once() {
    for operation in [
        Operation::Merge,
        Operation::Fork,
        Operation::FoldWal,
        Operation::Metadata,
    ] {
        let family = family().await;
        family.write_fork(FILES - 1).await;
        family
            .fork
            .clone()
            .close()
            .await
            .expect("end the fork's session");
        family.store.reset();
        family.fold(operation).await;
        let operations = family.store.take();
        let content_puts = write_keys(&operations, &KeyPredicate::content_blob());
        assert_eq!(
            content_puts.len(),
            FILES - 1,
            "{operation:?}: {content_puts:#?}"
        );
        assert_eq!(
            write_keys(&operations, &KeyPredicate::manifest(family.fork.id())).len(),
            1,
            "{operation:?}: one fold of the fork"
        );
        family.runtime.shutdown().await.expect("shutdown");
    }
}

/// Checks that a delete waits for a fold of the namespace that a caller
/// started, so the delete's own fold doesn't fold the same tail at the same
/// time. The fork writes one file fewer than
/// its fold trigger, so only the requested fold folds the tail before the
/// delete.
#[tokio::test]
async fn a_delete_waits_for_a_requested_fold() {
    let family = family().await;
    family.write_fork(FILES - 1).await;
    family.store.reset();
    family.blocking.block_next();
    let maintenance = family.maintenance();
    let folded = maintenance.fold_wal(family.fork.id());
    tokio::pin!(folded);
    tokio::select! {
        _ = &mut folded => panic!("the requested fold finished without writing content"),
        () = family.blocking.wait_until_blocked() => {}
    }
    let (folded, deleted) = family
        .run_beside_the_parked_fold(async { tokio::join!(folded, family.fork.delete()) })
        .await;
    folded.expect("the requested fold");
    deleted.expect("the delete");
    family.assert_folded_once("a delete", FILES - 1, &[family.fork.id()]);
    family.runtime.shutdown().await.expect("shutdown");
}

/// A snapshot of a namespace's current head, which folds the namespace's WAL tail
/// before the snapshot.
#[derive(Debug, Clone, Copy)]
enum HeadSnapshot {
    Snapshot,
    Fork,
}

/// Checks that a fold of the namespace's session waits for a snapshot's or a
/// fork's snapshot until the snapshot is verified, so the snapshot isn't written again. The
/// snapshot put is parked while more commits make the session's fold due. No
/// manifest is published while the snapshot is parked, the snapshot keeps the
/// manifest of its own fold, and the session's fold publishes once the snapshot
/// returns.
#[tokio::test]
async fn a_sessions_fold_waits_for_a_snapshot_or_a_fork_to_verify_its_snapshot_record() {
    for head_snapshot in [HeadSnapshot::Snapshot, HeadSnapshot::Fork] {
        fold_beside_a_parked_snapshot(head_snapshot).await;
    }
}

async fn fold_beside_a_parked_snapshot(head_snapshot: HeadSnapshot) {
    let fork_id = namespace_id("snapshot-wait-fork");
    let namespace_id = namespace_id("snapshot-wait");
    let snapshots = KeyPredicate::prefix(loonfs_objectstore::keys::snapshot_prefix(&namespace_id));
    let blocking = Arc::new(BlockingStore::new(
        MemoryStore::new(ChecksumAlgorithm::Crc64nvme),
        snapshots.clone(),
        OperationSelector::Put,
    ));
    let store = Arc::new(RecordingStore::new(blocking.clone(), KeyPredicate::any()));
    let runtime = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("snapshot-wait"))
        .session_fold_at_wal_objects(FOLD_AT_WAL_OBJECTS)
        .build()
        .await
        .expect("runtime");
    runtime
        .create_namespace(&namespace_id, &actor())
        .await
        .expect("create");
    let namespace = runtime.open_namespace(&namespace_id).expect("open");
    let write = |first: usize, files: usize| {
        let namespace = &namespace;
        async move {
            for index in first..first + files {
                let fill = b'a' + u8::try_from(index % 26).expect("a small index");
                namespace
                    .put_file(&format!("/file-{index}"), &[fill; FILE_BYTES], &actor())
                    .await
                    .expect("write a file");
            }
        }
    };
    let current_manifest_no = || async {
        runtime
            .maintenance(writer_id("snapshot-wait"))
            .diagnostics(&namespace_id)
            .await
            .expect("diagnostics")
            .current_manifest_no
            .expect("a manifest")
    };
    write(0, FILES - 1).await;
    store.reset();
    blocking.block_next();
    let created = async {
        match head_snapshot {
            HeadSnapshot::Snapshot => namespace
                .create_snapshot_with_options(&loonfs::CreateSnapshotOptions {
                    name: Some("parked".to_owned()),
                    ..loonfs::CreateSnapshotOptions::default()
                })
                .await
                .expect("the snapshot")
                .snapshot_id
                .manifest_no(),
            HeadSnapshot::Fork => runtime
                .fork_namespace(&namespace_id, &fork_id, &actor())
                .await
                .expect("the fork")
                .fork_basis
                .expect("a fork basis")
                .source_snapshot_id
                .manifest_no(),
        }
    };
    tokio::pin!(created);
    tokio::select! {
        _ = &mut created => panic!("{head_snapshot:?}: the snapshot finished without writing its record"),
        () = blocking.wait_until_blocked() => {}
    }
    let folded = current_manifest_no().await;
    let before = store.take();
    // The snapshot's fold folded the fence, so one more file than before brings
    // the tail to the fold trigger.
    write(FILES - 1, FILES + 1).await;
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    let parked = store.take();
    assert_eq!(
        write_keys(&parked, &KeyPredicate::manifest(&namespace_id)),
        Vec::<String>::new(),
        "{head_snapshot:?}: no fold publishes while the snapshot is parked"
    );
    blocking.release();
    assert_eq!(created.await, folded, "{head_snapshot:?}");
    namespace.wait_for_fold().await.expect("the session's fold");
    assert_eq!(
        current_manifest_no().await.0,
        folded.0 + 1,
        "{head_snapshot:?}"
    );
    let operations = [before, parked, store.take()].concat();
    assert_eq!(
        write_keys(&operations, &snapshots).len(),
        1,
        "{head_snapshot:?}: one snapshot record put"
    );
    runtime.shutdown().await.expect("shutdown");
}
