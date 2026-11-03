//! Tests of how many times a merge or a rebase folds the fork's WAL tail. When the
//! fork has a writer session in the same runtime, the session folds the
//! tail, and a fold that the session is already running isn't repeated.
//! Without a session, the route folds the tail itself.

use loonfs::{
    ActorId, InlineContentPolicy, LoonFs, Namespace, NamespaceId, RuntimeConfig, Writable,
};
use loonfs_test_support::ids::{namespace_id, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MemoryStore, OperationSelector, RecordedOperation, RecordingStore,
};
use loonfs_types::ChecksumAlgorithm;
use std::collections::BTreeSet;
use std::sync::Arc;

/// The number of files that the fork writes, each stored inline in the
/// fork's WAL tail.
const FILES: usize = 8;
/// The size of each file, in bytes. The fork's last file brings the tail's inline bytes
/// to the fold trigger.
const FILE_BYTES: usize = 1024;

type Store = RecordingStore<Arc<BlockingStore<MemoryStore>>>;

/// A runtime whose fork session folds once its tail has [`FILES`] files,
/// over a store that can park a content write.
struct Family {
    blocking: Arc<BlockingStore<MemoryStore>>,
    store: Arc<Store>,
    runtime: LoonFs<Writable>,
    main: Namespace<Writable>,
    fork: Namespace<Writable>,
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
        .writer_id(writer_id("merge-folds"))
        .config(RuntimeConfig {
            inline_content: InlineContentPolicy {
                inline_content_fold_at_bytes: FILES * FILE_BYTES,
                ..InlineContentPolicy::default()
            },
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

    /// Runs `route` while the session's fold stays parked, and releases the
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

    /// Checks that the store recorded one write of each of the fork's
    /// `files` content objects and, for each of `namespaces`, no manifest
    /// put to a manifest number that an earlier put had already taken.
    fn assert_folded_once(&self, files: usize, namespaces: &[&NamespaceId]) {
        let operations = self.store.take();
        let content_puts = put_keys(&operations, &KeyPredicate::content_blob());
        let distinct: BTreeSet<_> = content_puts.iter().collect();
        assert_eq!(content_puts.len(), files, "{content_puts:#?}");
        assert_eq!(distinct.len(), files, "{content_puts:#?}");
        for namespace_id in namespaces {
            let manifest_puts = put_keys(&operations, &KeyPredicate::manifest(namespace_id));
            let distinct: BTreeSet<_> = manifest_puts.iter().collect();
            assert_eq!(
                distinct.len(),
                manifest_puts.len(),
                "a manifest put of `{namespace_id}` failed its precondition: {manifest_puts:#?}"
            );
        }
    }
}

/// Returns the keys of the puts in `operations` that `keys` selects, in
/// order, with a key repeated for each put to it.
fn put_keys(operations: &[RecordedOperation], keys: &KeyPredicate) -> Vec<String> {
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
}

/// Checks that a merge of a fork whose session is folding waits for that
/// fold instead of folding the same tail at the same time, so each of the
/// tail's content objects is written once, and no manifest put of the fork
/// or of main fails.
#[tokio::test]
async fn a_merge_waits_for_the_fork_sessions_fold() {
    let family = family().await;
    family.write_fork_and_park_its_fold().await;
    let merged = family
        .run_beside_the_parked_fold(family.main.merge_fork(family.fork.id(), &actor()))
        .await
        .expect("the merge");
    assert_eq!(
        merged.merge.expect("a merge record").source_through_seq,
        family
            .fork
            .last_published_seq()
            .expect("the fork's last commit")
    );
    family.assert_folded_once(FILES, &[family.fork.id(), family.main.id()]);
    family.runtime.shutdown().await.expect("shutdown");
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
    family.assert_folded_once(FILES, &[family.fork.id(), &rebase_id]);
    family.runtime.shutdown().await.expect("shutdown");
}

/// Checks that a merge of a fork without a session in the runtime folds
/// the fork's tail itself, once. The fork writes one file fewer than its
/// fold trigger, so its session ends with the whole tail unfolded.
#[tokio::test]
async fn a_merge_of_a_fork_without_a_session_folds_its_tail() {
    let family = family().await;
    family.write_fork(FILES - 1).await;
    let last = family
        .fork
        .last_published_seq()
        .expect("the fork's last commit");
    let Family {
        store,
        runtime,
        main,
        fork,
        ..
    } = family;
    let fork_id = fork.id().clone();
    drop(fork);
    runtime.drain().await.expect("end the fork's session");
    store.reset();

    let merged = main
        .merge_fork(&fork_id, &actor())
        .await
        .expect("the merge");
    assert_eq!(
        merged.merge.expect("a merge record").source_through_seq,
        last
    );
    let operations = store.take();
    let content_puts = put_keys(&operations, &KeyPredicate::content_blob());
    assert_eq!(content_puts.len(), FILES - 1, "{content_puts:#?}");
    assert_eq!(
        put_keys(&operations, &KeyPredicate::manifest(&fork_id)).len(),
        1,
        "one fold of the fork"
    );
    runtime.shutdown().await.expect("shutdown");
}
