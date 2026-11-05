//! A write planned on a manifest that a newer manifest replaced, whose
//! segments a collection removed, is planned again on the current manifest.

use crate::common::compact_until_done;
use loonfs::{LoonFs, NamespaceId};
use loonfs_objectstore::keys::{metadata_manifest_prefix, metadata_segment_prefix};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{local_fs_store::LocalFsStore, ObjectStoreExtension};
use loonfs_test_support::ids::writer_id;
use loonfs_test_support::stores::{
    BlockingStore, KeyPredicate, MetadataMapStore, OperationSelector,
};
use loonfs_test_support::test_actor;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// Checks that a writer whose session planned a commit on a manifest that a
/// compaction replaced, and whose segments a collection then removed, plans
/// the commit again on the current manifest and commits it, instead of
/// reporting the namespace corrupt.
#[tokio::test]
async fn a_write_planned_on_a_replaced_manifest_commits_on_the_current_one() {
    let directory = tempdir().expect("tempdir");
    let namespace = NamespaceId::parse("writer-gc-probe").expect("namespace");
    let old_segments = Arc::new(Mutex::new(BTreeSet::<String>::new()));
    let selected = old_segments.clone();
    // The old segments and every manifest report an ancient age, so the
    // collection removes a replaced manifest's segments at once.
    let manifest_prefix = metadata_manifest_prefix(&namespace);
    let store = Arc::new(BlockingStore::new(
        MetadataMapStore::aged(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::new(move |key| {
                key.starts_with(&manifest_prefix)
                    || selected.lock().expect("old segments").contains(key)
            }),
        ),
        KeyPredicate::metadata_segment(),
        OperationSelector::Get,
    ));
    let seed = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("seed"))
        .build()
        .await
        .expect("seed writer");
    seed.create_namespace(&namespace, &test_actor())
        .await
        .expect("namespace");
    let seed_namespace = seed.open_namespace(&namespace).expect("open namespace");
    let seed_maintenance = seed.maintenance(writer_id("seed"));
    // Two folded runs, so the compaction below rewrites their segments.
    for name in ["a", "b"] {
        seed_namespace
            .put_file(&format!("/{name}"), name.as_bytes(), &test_actor())
            .await
            .expect("file");
        seed_maintenance
            .fold_wal(&namespace)
            .await
            .expect("fold the file");
    }
    seed.shutdown().await.expect("end the seed writer");
    let segment_keys = store
        .list_prefix(&metadata_segment_prefix(&namespace))
        .await
        .expect("list metadata");
    old_segments
        .lock()
        .expect("old segments")
        .extend(segment_keys.into_iter().filter(|key| {
            parse_object_key(key)
                .is_some_and(|parsed| parsed.family() == DurableObjectFamily::MetadataSegment)
        }));
    assert!(!old_segments.lock().expect("old segments").is_empty());

    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("cold-writer"))
        .build()
        .await
        .expect("cold writer");
    let namespace_writer = writer.open_namespace(&namespace).expect("open namespace");
    let maintainer = LoonFs::builder_with_store(store.clone())
        .writer_id(writer_id("maintainer"))
        .build()
        .await
        .expect("maintainer");
    let maintenance = maintainer.maintenance(writer_id("maintainer"));
    store.block_next();
    let actor = test_actor();
    let write = namespace_writer.put_file("/c", b"c", &actor);
    let replace_and_collect = async {
        loonfs_test_support::clock::bounded(
            std::time::Duration::from_secs(10),
            store.wait_until_blocked(),
        )
        .await
        .expect("the plan reached an uncached segment");
        compact_until_done(&maintenance, &namespace).await;
        let gc = maintenance.gc(&namespace).await.expect("GC");
        assert!(
            gc.deleted.metadata_segments > 0,
            "the collection removed the replaced manifest's segments"
        );
        store.release();
    };
    let (committed, ()) = tokio::join!(write, replace_and_collect);
    committed.expect("the write commits on the current manifest");

    let reader = LoonFs::builder_with_store(store)
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let read_namespace = reader.namespace(&namespace);
    for (path, bytes) in [("/a", &b"a"[..]), ("/b", &b"b"[..]), ("/c", &b"c"[..])] {
        let file = read_namespace.read_file(path).await.expect("read the file");
        assert_eq!(file.bytes, bytes, "{path}");
    }
    writer.shutdown().await.expect("shut down the writer");
    maintainer
        .shutdown()
        .await
        .expect("shut down the maintainer");
}
