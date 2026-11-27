//! A write, a merge, or a replay planned on a manifest that a newer manifest
//! replaced, whose segments a collection removed, is planned again on the
//! current manifest.

use crate::common::expect_code;
use loonfs::{
    AttributeChanges, Commit, DestinationBehavior, ErrorCode, LoonFs, Namespace, NamespaceId,
    PutFileOptions, Writable,
};
use loonfs_objectstore::keys::metadata_segment_prefix;
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStore, ObjectStoreExtension};
use loonfs_test_support::ids::{attribute_key, attribute_text, writer_id};
use loonfs_test_support::test_actor;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::tempdir;

/// The file that each namespace has before its forks and folds. The first
/// fold annotates it, and the plan under test applies a change to it, so
/// the plan reads the attributes row that the first fold wrote.
const SHARED_FILE: &str = "/a";

/// The content that a fork writes to [`SHARED_FILE`], which a merge or a
/// replay applies.
const FORKED_CONTENT: &[u8] = b"/a from the fork";

/// The files that the two folds write, one per fold.
const FOLDED_FILES: [&str; 2] = ["/e", "/g"];

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
    let seed_maintenance = seed.clone();
    annotate_shared_file(&seed_namespace, "folded", "yes")
        .await
        .expect("annotate the shared file");
    for path in FOLDED_FILES {
        seed_namespace
            .put_file(path, path.as_bytes(), &test_actor())
            .await
            .expect("file");
        seed_maintenance
            .open_namespace(namespace)
            .expect("open namespace")
            .fold_wal()
            .await
            .expect("fold the file");
    }
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
    let seed = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
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

    let writer = LoonFs::builder_with_store(store, loonfs::Context::Service)
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

/// Checks that a fork of a namespace whose WAL tail appends to a folded file
/// returns `namespace_corrupt` when the source's segment that has the file's
/// layout row is listed by the source's current manifest but deleted from
/// the store. The fork folds the source's tail into its manifest 2 and reads
/// the layout row through its manifest 1, which lists the source's runs. The
/// fork runs in a runtime that caches none of the source's segments. The test
/// checks the final `namespace_corrupt` and nothing more.
#[tokio::test]
async fn a_fork_whose_sources_layout_segment_is_missing_reports_corruption() {
    use loonfs_types::format::manifest::MetadataRowFamily;
    let directory = tempdir().expect("tempdir");
    let source = NamespaceId::parse("fork-corrupt-source").expect("namespace");
    let fork = NamespaceId::parse("fork-corrupt-fork").expect("fork");
    let store = Arc::new(LocalFsStore::new(directory.path()).expect("store"));
    let seed = LoonFs::builder_with_store(store.clone(), loonfs::Context::Service)
        .writer_id(writer_id("seed"))
        .build()
        .await
        .expect("seed writer");
    seed.create_namespace(&source, &test_actor())
        .await
        .expect("namespace");
    seed.open_namespace(&source)
        .expect("open the source")
        .put_file(SHARED_FILE, &[b's'; 1024], &test_actor())
        .await
        .expect("the shared file");
    seed.open_namespace(&source)
        .expect("open namespace")
        .fold_wal()
        .await
        .expect("fold the shared file");
    seed.open_namespace(&source)
        .expect("open the source")
        .append_file(SHARED_FILE, b" and more", &test_actor())
        .await
        .expect("append to the shared file");
    let current = loonfs_core::test_support::namespace_store(store.clone(), &source)
        .load_current_manifest()
        .await
        .expect("the source's manifest");
    for segment in current
        .payload()
        .runs
        .iter()
        .flat_map(|run| run.segments.iter())
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
    {
        store
            .delete(&loonfs_objectstore::keys::metadata_segment_object_key(
                segment,
            ))
            .await
            .expect("remove the layout segment");
    }
    seed.shutdown().await.expect("shut down the seed writer");
    // A runtime that caches none of the source's segments forks it.
    let forker = LoonFs::builder_with_store(store, loonfs::Context::Service)
        .writer_id(writer_id("forker"))
        .build()
        .await
        .expect("forking runtime");
    expect_code(
        forker.fork_namespace(&source, &fork, &test_actor()).await,
        ErrorCode::NamespaceCorrupt,
    );
    forker
        .shutdown()
        .await
        .expect("shut down the forking runtime");
}
