//! Store reads and entry metadata for a directory page across folded batches.

use loonfs::{
    CreateDirectoryOptions, CreateNamespaceOptions, DestinationBehavior, FsMaintenance, FsReader,
    FsWriter, PageRequest, PutFileOptions, SharedObjectStore, StatPathOptions,
};
use loonfs_api::wire::manifest::MetadataRowFamily;
use loonfs_objectstore::keys::metadata_segment_object_key;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::{namespace_id, page_limit};
use loonfs_test_support::stores::{ConcurrencyWatchStore, KeyPredicate, RecordingStore};
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::tempdir;

async fn family_keys(
    store: &SharedObjectStore,
    namespace_id: &loonfs::NamespaceId,
    family: MetadataRowFamily,
) -> BTreeSet<String> {
    loonfs_core::control::load_namespace_current_manifest(store, namespace_id)
        .await
        .expect("load manifest")
        .state
        .envelope
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
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("directory-page-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("directory-page-maintenance")
        .build()
        .await
        .expect("maintenance");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::new(actor.clone()))
        .await
        .expect("namespace");
    writer
        .create_directory(
            &namespace_id,
            "/files",
            CreateDirectoryOptions::new(actor.clone()),
        )
        .await
        .expect("directory");
    maintenance
        .flush_wal(&namespace_id)
        .await
        .expect("fold parents");
    let parent_inode_keys = family_keys(&store, &namespace_id, MetadataRowFamily::Inodes).await;
    for batch in 0..6 {
        for index in batch * 50..(batch + 1) * 50 {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/files/{index:03}.txt"),
                    &vec![b'a'; index + 1],
                    PutFileOptions::new(actor.clone()),
                )
                .await
                .expect("create file");
        }
        maintenance
            .flush_wal(&namespace_id)
            .await
            .expect("fold batch");
    }
    for index in (0..300).step_by(3) {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/files/{index:03}.txt"),
                &vec![b'b'; index + 2],
                PutFileOptions {
                    behavior: DestinationBehavior::Replace,
                    ..PutFileOptions::new(loonfs_api::ActorId::parse("editor").expect("actor"))
                },
            )
            .await
            .expect("replace file");
    }
    maintenance
        .flush_wal(&namespace_id)
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
    let revisions = Arc::new(ConcurrencyWatchStore::new(
        store,
        KeyPredicate::new(move |key| revision_keys.contains(key)),
    ));
    let recording = Arc::new(RecordingStore::metadata_segments(revisions.clone()));
    let reader = FsReader::builder_with_store(recording.clone())
        .build()
        .await
        .expect("fresh reader");
    let page = reader
        .list_path_entries_page(
            &namespace_id,
            "/files",
            PageRequest {
                limit: page_limit(300),
                cursor: None,
            },
            Default::default(),
        )
        .await
        .expect("directory page");
    assert_eq!(page.entries.len(), 300);
    assert!(page.next_cursor.is_none());
    for key in recording.take_get_keys() {
        assert!(
            !child_inode_keys.contains(&key),
            "read child inode object {key}"
        );
    }
    let concurrency = revisions.reads();
    assert!(concurrency.peak_in_flight > 1, "{concurrency:?}");
    assert!(concurrency.peak_in_flight <= 16, "{concurrency:?}");
    for entry in page.entries {
        let stat = reader
            .get_path_entry(
                &namespace_id,
                entry.path.as_str(),
                StatPathOptions::default(),
            )
            .await
            .expect("stat listed file");
        assert_eq!(entry.created_by, actor);
        assert_eq!(entry.created_by, stat.created_by);
        assert_eq!(entry.created_at_ms, stat.created_at_ms);
        assert_eq!(entry.kind, stat.kind);
    }
}
