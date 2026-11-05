//! A write, a merge, or a replay planned on a manifest that a newer manifest
//! replaced, whose segments a collection removed, is planned again on the
//! current manifest.

use crate::common::{compact_until_done, expect_code};
use loonfs::{
    AttributeChanges, Commit, DestinationBehavior, ErrorCode, LoonFs, Maintenance, Namespace,
    NamespaceId, PutFileOptions, Writable,
};
use loonfs_objectstore::keys::{metadata_manifest_prefix, metadata_segment_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore, ObjectStoreExtension};
use loonfs_test_support::ids::{attribute_key, attribute_text, writer_id};
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MetadataMapStore, OperationSelector,
};
use loonfs_test_support::test_actor;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

type AgedBlockingStore = BlockingStore<MetadataMapStore<LocalFsStore>>;

/// The file that each namespace has before its forks and folds. The first
/// fold annotates it, and the plan under test applies a change to it, so
/// the plan reads the attributes row that the first fold wrote.
const SHARED_FILE: &str = "/a";

/// The content that a fork writes to [`SHARED_FILE`], which a merge or a
/// replay applies.
const FORKED_CONTENT: &[u8] = b"/a from the fork";

/// The files that the two folds write, one per fold.
const FOLDED_FILES: [&str; 2] = ["/e", "/g"];

/// Returns a store that blocks the next metadata segment read, and the set
/// of segment keys that, with every manifest of `namespace`, report an
/// ancient age, so a collection removes a replaced manifest's segments at
/// once.
fn aged_blocking_store(
    directory: &Path,
    namespace: &NamespaceId,
) -> (Arc<AgedBlockingStore>, Arc<Mutex<BTreeSet<String>>>) {
    let old_segments = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let selected = old_segments.clone();
    let manifest_prefix = metadata_manifest_prefix(namespace);
    let store = Arc::new(BlockingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory).expect("store"),
            KeyPredicate::new(move |key| {
                key.starts_with(&manifest_prefix)
                    || selected.lock().expect("old segments").contains(key)
            }),
        ),
        KeyPredicate::metadata_segment(),
        OperationSelector::Get,
    ));
    (store, old_segments)
}

/// The metadata segments that `namespace` has now.
async fn segment_keys(store: &impl ObjectStore, namespace: &NamespaceId) -> BTreeSet<String> {
    store
        .list_prefix(&metadata_segment_prefix(namespace))
        .await
        .expect("list metadata")
        .into_iter()
        .filter(|key| {
            parse_object_key(key)
                .is_some_and(|parsed| parsed.family() == DurableObjectFamily::MetadataSegment)
        })
        .collect()
}

/// Selects every metadata segment that `namespace` has now, so a collection
/// removes the ones that a compaction replaces.
async fn select_old_segments(
    store: &impl ObjectStore,
    namespace: &NamespaceId,
    old_segments: &Mutex<BTreeSet<String>>,
) {
    let keys = segment_keys(store, namespace).await;
    old_segments.lock().expect("old segments").extend(keys);
}

/// Creates `namespace` with [`SHARED_FILE`] in it.
async fn create_namespace_with_shared_file(seed: &LoonFs<Writable>, namespace: &NamespaceId) {
    seed.create_namespace(namespace, &test_actor())
        .await
        .expect("namespace");
    seed.open_namespace(namespace)
        .expect("open namespace")
        .put_file(SHARED_FILE, SHARED_FILE.as_bytes(), &test_actor())
        .await
        .expect("the shared file");
}

/// Replaces [`SHARED_FILE`]'s content with [`FORKED_CONTENT`] in `fork`.
async fn change_shared_file(seed: &LoonFs<Writable>, fork: &NamespaceId) {
    seed.open_namespace(fork)
        .expect("open the fork")
        .put_file_with_options(
            SHARED_FILE,
            FORKED_CONTENT,
            &test_actor(),
            &PutFileOptions {
                behavior: DestinationBehavior::Replace,
                ..Default::default()
            },
        )
        .await
        .expect("a commit in the fork");
}

/// Sets the attribute `key` to `value` on [`SHARED_FILE`].
async fn annotate_shared_file(
    namespace: &Namespace<Writable>,
    key: &str,
    value: &str,
) -> loonfs::Result<Commit> {
    namespace
        .update_attributes(
            SHARED_FILE,
            &test_actor(),
            AttributeChanges {
                set: BTreeMap::from([(attribute_key(key), attribute_text(value))]),
                ..Default::default()
            },
        )
        .await
}

/// Folds `namespace` once per file of [`FOLDED_FILES`], so the compaction
/// below rewrites both folds' segments. The first fold also annotates
/// [`SHARED_FILE`], so its newest attributes row is in a segment that the
/// collection removes.
async fn fold_twice(seed: &LoonFs<Writable>, namespace: &NamespaceId) {
    let seed_namespace = seed.open_namespace(namespace).expect("open namespace");
    let seed_maintenance = seed.maintenance(writer_id("seed"));
    annotate_shared_file(&seed_namespace, "folded", "yes")
        .await
        .expect("annotate the shared file");
    for path in FOLDED_FILES {
        seed_namespace
            .put_file(path, path.as_bytes(), &test_actor())
            .await
            .expect("file");
        seed_maintenance
            .fold_wal(namespace)
            .await
            .expect("fold the file");
    }
}

/// Opens a runtime on `store` under `name`, so that it holds no segment of
/// the namespace it reads.
async fn cold_runtime(store: Arc<AgedBlockingStore>, name: &str) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store)
        .writer_id(writer_id(name))
        .build()
        .await
        .expect("runtime")
}

/// Opens a cold runtime whose maintenance compacts every family group that
/// has a delta run.
async fn maintainer_runtime(store: Arc<AgedBlockingStore>) -> LoonFs<Writable> {
    LoonFs::builder_with_store(store)
        .writer_id(writer_id("maintainer"))
        .run_policy(loonfs_core::test_support::one_delta_run_policy())
        .build()
        .await
        .expect("runtime")
}

/// Waits until the plan's segment read is blocked, replaces `namespace`'s
/// manifest with a compaction, removes the replaced manifest's segments
/// with a collection, and lets the read go on.
async fn replace_and_collect(
    store: &AgedBlockingStore,
    maintenance: &Maintenance,
    namespace: &NamespaceId,
) {
    loonfs_test_support::clock::bounded(
        std::time::Duration::from_secs(10),
        store.wait_until_blocked(),
    )
    .await
    .expect("the plan reached an uncached segment");
    compact_until_done(maintenance, namespace).await;
    let gc = maintenance.gc(namespace).await.expect("GC");
    assert!(
        gc.deleted.metadata_segments > 0,
        "the collection removed the replaced manifest's segments"
    );
    store.release();
}

/// Checks through a fresh reader that `namespace` has the folded files,
/// that [`SHARED_FILE`] holds `content`, and that its attributes hold each
/// of `attributes`.
async fn assert_shared_file(
    store: Arc<AgedBlockingStore>,
    namespace: &NamespaceId,
    content: &[u8],
    attributes: &[(&str, &str)],
) {
    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let read_namespace = reader.namespace(namespace);
    for path in FOLDED_FILES {
        let file = read_namespace.read_file(path).await.expect("read the file");
        assert_eq!(file.bytes, path.as_bytes(), "{path}");
    }
    let file = read_namespace
        .read_file(SHARED_FILE)
        .await
        .expect("read the shared file");
    assert_eq!(file.bytes, content);
    let entry = read_namespace
        .stat(SHARED_FILE)
        .await
        .expect("stat the shared file");
    let projection = entry.attributes.expect("the shared file's attributes");
    for (key, value) in attributes {
        assert_eq!(
            projection.attributes.get(&attribute_key(key)),
            Some(&attribute_text(value)),
            "{key}"
        );
    }
}

/// Checks that a writer whose session planned a commit on a manifest that a
/// compaction replaced, and whose segments a collection then removed, plans
/// the commit again on the current manifest and commits it, instead of
/// reporting the namespace corrupt.
#[tokio::test]
async fn a_write_planned_on_a_replaced_manifest_commits_on_the_current_one() {
    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("writer-gc-probe").expect("namespace");
    let (store, old_segments) = aged_blocking_store(directory.path(), &namespace);
    let seed = cold_runtime(store.clone(), "seed").await;
    create_namespace_with_shared_file(&seed, &namespace).await;
    fold_twice(&seed, &namespace).await;
    seed.shutdown().await.expect("end the seed writer");
    select_old_segments(store.as_ref(), &namespace, &old_segments).await;

    let writer = cold_runtime(store.clone(), "cold-writer").await;
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let maintainer = maintainer_runtime(store.clone()).await;
    let maintenance = maintainer.maintenance(writer_id("maintainer"));
    store.block_next();
    let write = annotate_shared_file(&namespace_writer, "written", "cold");
    let (committed, ()) =
        tokio::join!(write, replace_and_collect(&store, &maintenance, &namespace));
    committed.expect("the write commits on the current manifest");

    assert_shared_file(
        store,
        &namespace,
        SHARED_FILE.as_bytes(),
        &[("folded", "yes"), ("written", "cold")],
    )
    .await;
    writer.shutdown().await.expect("shut down the writer");
    maintainer
        .shutdown()
        .await
        .expect("shut down the maintainer");
}

/// Checks that a merge whose plan read this namespace on a manifest that a
/// compaction replaced, and whose segments a collection then removed, plans
/// again on the current manifest and commits, instead of reporting the
/// namespace corrupt.
#[tokio::test]
async fn a_merge_planned_on_a_replaced_manifest_commits_on_the_current_one() {
    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("merge-gc-probe").expect("namespace");
    let fork = NamespaceId::parse("merge-gc-probe-fork").expect("fork");
    let (store, old_segments) = aged_blocking_store(directory.path(), &namespace);
    let seed = cold_runtime(store.clone(), "seed").await;
    create_namespace_with_shared_file(&seed, &namespace).await;
    // The fork pins the manifest before the folds below, so the collection
    // can remove the folds' segments once a compaction replaces them.
    seed.fork_namespace(&namespace, &fork, &test_actor())
        .await
        .expect("fork");
    change_shared_file(&seed, &fork).await;
    fold_twice(&seed, &namespace).await;
    seed.shutdown().await.expect("end the seed writer");
    select_old_segments(store.as_ref(), &namespace, &old_segments).await;

    let writer = cold_runtime(store.clone(), "cold-writer").await;
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let maintainer = maintainer_runtime(store.clone()).await;
    let maintenance = maintainer.maintenance(writer_id("maintainer"));
    store.block_next();
    let actor = test_actor();
    let merge = namespace_writer.merge_fork(&fork, &actor);
    let (merged, ()) = tokio::join!(merge, replace_and_collect(&store, &maintenance, &namespace));
    merged.expect("the merge commits on the current manifest");

    assert_shared_file(store, &namespace, FORKED_CONTENT, &[("folded", "yes")]).await;
    writer.shutdown().await.expect("shut down the writer");
    maintainer
        .shutdown()
        .await
        .expect("shut down the maintainer");
}

/// Checks that a replay commit whose plan read the rebase's new fork on a
/// manifest that a compaction replaced, and whose segments a collection
/// then removed, plans again on the current manifest and commits, instead
/// of reporting the new fork corrupt.
#[tokio::test]
async fn a_replay_planned_on_a_replaced_manifest_commits_on_the_current_one() {
    let directory = tempdir().expect("tempdir");
    let main = NamespaceId::parse("replay-gc-probe").expect("namespace");
    let fork = NamespaceId::parse("replay-gc-probe-fork").expect("fork");
    let rebase = NamespaceId::parse("replay-gc-probe-rebase").expect("new fork");
    let (store, old_segments) = aged_blocking_store(directory.path(), &rebase);
    let seed = cold_runtime(store.clone(), "seed").await;
    create_namespace_with_shared_file(&seed, &main).await;
    seed.fork_namespace(&main, &fork, &test_actor())
        .await
        .expect("fork");
    change_shared_file(&seed, &fork).await;
    seed.fork_namespace_for_rebase(&main, &fork, &rebase, &test_actor())
        .await
        .expect("the rebase's new fork");
    fold_twice(&seed, &rebase).await;
    seed.shutdown().await.expect("end the seed writer");
    select_old_segments(store.as_ref(), &rebase, &old_segments).await;

    let writer = cold_runtime(store.clone(), "cold-writer").await;
    let rebase_writer = writer.open_namespace(&rebase).expect("open the new fork");
    let maintainer = maintainer_runtime(store.clone()).await;
    let maintenance = maintainer.maintenance(writer_id("maintainer"));
    store.block_next();
    let actor = test_actor();
    let replay = rebase_writer.replay_fork(&fork, &actor);
    let (replayed, ()) = tokio::join!(replay, replace_and_collect(&store, &maintenance, &rebase));
    let replayed = replayed.expect("the replay commits on the current manifest");
    assert_eq!(replayed.conflicts, Some(Vec::new()));

    assert_shared_file(store, &rebase, FORKED_CONTENT, &[("folded", "yes")]).await;
    writer.shutdown().await.expect("shut down the writer");
    maintainer
        .shutdown()
        .await
        .expect("shut down the maintainer");
}

/// Checks that a merge whose plan finds a segment of this namespace's
/// current manifest missing, with no newer manifest, reports the namespace
/// corrupt.
#[tokio::test]
async fn a_merge_plan_that_finds_a_segment_missing_under_the_current_manifest_reports_corruption() {
    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("merge-corrupt-probe").expect("namespace");
    let fork = NamespaceId::parse("merge-corrupt-probe-fork").expect("fork");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let seed = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("seed"))
        .build()
        .await
        .expect("seed writer");
    create_namespace_with_shared_file(&seed, &namespace).await;
    seed.fork_namespace(&namespace, &fork, &test_actor())
        .await
        .expect("fork");
    change_shared_file(&seed, &fork).await;
    let inherited = segment_keys(store.as_ref(), &namespace).await;
    fold_twice(&seed, &namespace).await;
    seed.shutdown().await.expect("end the seed writer");
    // The segments that only this namespace's current manifest lists go
    // missing, and the fork keeps reading the ones it inherited.
    let own = segment_keys(store.as_ref(), &namespace).await;
    assert!(own.len() > inherited.len());
    for key in own.difference(&inherited) {
        store.delete(key).await.expect("remove the segment");
    }

    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id("cold-writer"))
        .build()
        .await
        .expect("cold writer");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    expect_code(
        namespace_writer.merge_fork(&fork, &test_actor()).await,
        ErrorCode::NamespaceCorrupt,
    );
    writer.shutdown().await.expect("shut down the writer");
}
