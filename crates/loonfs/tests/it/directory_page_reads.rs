//! Store reads and entry metadata for a directory page across folded batches.

use loonfs::{DestinationBehavior, LoonFs, PutFileOptions, SharedObjectStore, StatOptions};
use loonfs_core::test_support::STORE_READS_IN_FLIGHT;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{ConcurrencyWatchStore, KeyPredicate, RecordingStore};
use loonfs_types::format::manifest::MetadataRowFamily;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

async fn family_keys(
    store: &SharedObjectStore,
    namespace_id: &loonfs::NamespaceId,
    family: MetadataRowFamily,
) -> BTreeSet<String> {
    loonfs_core::test_support::namespace_store(store, namespace_id)
        .load_current_manifest()
        .await
        .expect("load manifest")
        .payload()
        .runs
        .iter()
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == family)
        .map(metadata_segment_object_key)
        .collect()
}

#[tokio::test]
async fn directory_pages_use_bindings_and_load_revision_heads_concurrently() {
    let temporary = tempdir().expect("temporary directory");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temporary.path()).expect("local store"));
    let namespace_id = namespace_id("directory-page-reads");
    let actor = loonfs_test_support::test_actor();
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(loonfs_test_support::ids::writer_id("directory-page-writer"))
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(loonfs_test_support::ids::writer_id(
        "directory-page-maintenance",
    ));
    writer
        .create_namespace(&namespace_id, &actor)
        .await
        .expect("namespace");
    let namespace_writer = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    namespace_writer
        .create_directory("/files", &actor)
        .await
        .expect("directory");
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold parents");
    let parent_inode_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Inodes).await;
    // The writer's own maintenance folds before the session's fold
    // threshold, so the session never folds, and so never compacts these
    // runs into fewer segments.
    for batch in 0..12 {
        for index in batch * 25..(batch + 1) * 25 {
            namespace_writer
                .put_file(
                    &format!("/files/{index:03}.txt"),
                    &vec![b'a'; index + 1],
                    &actor,
                )
                .await
                .expect("create file");
        }
        maintenance
            .fold_wal(&namespace_id)
            .await
            .expect("fold batch");
    }
    for (replaced, index) in (0..300).step_by(3).enumerate() {
        if replaced % 25 == 24 {
            maintenance
                .fold_wal(&namespace_id)
                .await
                .expect("fold replacements");
        }
        namespace_writer
            .put_file_with_options(
                &format!("/files/{index:03}.txt"),
                &vec![b'b'; index + 2],
                &loonfs_types::ActorId::parse("editor").expect("actor"),
                &PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..Default::default()
                },
            )
            .await
            .expect("replace file");
    }
    maintenance
        .fold_wal(&namespace_id)
        .await
        .expect("fold replacements");
    let child_inode_keys: BTreeSet<_> =
        family_keys(&store, &namespace_id, MetadataRowFamily::Inodes)
            .await
            .difference(&parent_inode_keys)
            .cloned()
            .collect();
    assert!(child_inode_keys.len() >= 6);
    let revision_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Revisions).await;
    assert!(revision_keys.len() >= 6);
    // A child inode segment's key range can include the directory's own ID,
    // and a Bloom filter false positive can make the directory's own lookup
    // read that segment. A one-entry page of the directory reads the same
    // segments for the directory's lookup.
    let one_entry = Arc::new(RecordingStore::metadata_segments(store.clone()));
    LoonFs::builder_with_store(one_entry.clone())
        .read_only()
        .build()
        .await
        .expect("one-entry reader")
        .namespace(&namespace_id)
        .list("/files")
        .page_limit(page_limit(1))
        .page()
        .await
        .expect("one-entry page");
    let directory_reads: BTreeSet<String> = one_entry.take_get_keys().into_iter().collect();
    let revisions = Arc::new(ConcurrencyWatchStore::new(
        store,
        KeyPredicate::new(move |key| revision_keys.contains(key)),
    ));
    let recording = Arc::new(RecordingStore::metadata_segments(revisions.clone()));
    let reader = LoonFs::builder_with_store(recording.clone())
        .read_only()
        .build()
        .await
        .expect("fresh reader");
    let namespace = reader.namespace(&namespace_id);
    let page = namespace
        .list("/files")
        .page_limit(page_limit(300))
        .page()
        .await
        .expect("directory page");
    assert_eq!(page.entries.len(), 300);
    assert!(page.next_cursor.is_none());
    for key in recording.take_get_keys() {
        assert!(
            !child_inode_keys.contains(&key) || directory_reads.contains(&key),
            "read child inode object {key}"
        );
    }
    let concurrency = revisions.reads();
    assert!(concurrency.peak_in_flight > 1, "{concurrency:?}");
    assert!(
        concurrency.peak_in_flight <= STORE_READS_IN_FLIGHT,
        "{concurrency:?}"
    );
    for entry in page.entries {
        let stat = namespace
            .stat_with_options(entry.path.as_str(), &StatOptions::default())
            .await
            .expect("stat listed file");
        assert_eq!(entry.created_by, actor);
        assert_eq!(entry.created_by, stat.created_by);
        assert_eq!(entry.created_at_ms, stat.created_at_ms);
        assert_eq!(entry.kind, stat.kind);
    }
    let stat_reads = recording.take_get_keys();
    assert!(
        stat_reads.is_empty(),
        "stats after the listing read metadata segments: {stat_reads:?}"
    );
}
