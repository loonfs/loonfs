//! Cached reads and current retention-floor reads use different manifest requests.

use loonfs::{
    CreateNamespaceOptions, FsMaintenance, FsReader, FsWriter, MetadataMaintenanceOptions,
    NamespaceId, PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use std::sync::Arc;
use tempfile::tempdir;

fn manifest_gets(gets: &[String]) -> Vec<String> {
    gets.iter()
        .filter(|key| loonfs_objectstore::layout::manifest_no_of(key).is_some())
        .cloned()
        .collect()
}

async fn build_namespace(store: &SharedObjectStore, namespace_id: &NamespaceId) {
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("seed-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let maintenance = FsMaintenance::builder_with_store(store.clone())
        .actor_id("seed-maintenance")
        .build()
        .await
        .expect("build maintenance");
    writer
        .create_namespace(namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    for index in 0..4 {
        writer
            .put_file_bytes(
                namespace_id,
                &format!("/docs/file-{index}.txt"),
                b"body",
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("seed file");
    }
    maintenance
        .maintain_metadata(
            namespace_id,
            MetadataMaintenanceOptions {
                max_wal_tail_segments: std::num::NonZeroU64::MIN,
                ..Default::default()
            },
        )
        .await
        .expect("step");
}

#[tokio::test]
async fn warm_reads_and_writes_reuse_their_manifest() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("pins").expect("valid namespace id");
    build_namespace(&store, &namespace_id).await;

    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    reader
        .get_path_entry(&namespace_id, "/docs/file-0.txt", Default::default())
        .await
        .expect("first stat");
    let warmup = manifest_gets(&recording.take_get_keys());
    assert!(!warmup.is_empty(), "the first read loads its manifest");

    reader
        .get_path_entry(&namespace_id, "/docs/file-1.txt", Default::default())
        .await
        .expect("second stat");
    let repeats = manifest_gets(&recording.take_get_keys());
    assert_eq!(
        repeats,
        Vec::<String>::new(),
        "a warm reader reuses its manifest"
    );

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("warm-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/file-5.txt",
            b"body",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("first write");
    let warmup = manifest_gets(&recording.take_get_keys());
    assert!(!warmup.is_empty(), "the first write loads its manifest");

    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/file-6.txt",
            b"body",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("second write");
    let repeats = manifest_gets(&recording.take_get_keys());
    assert_eq!(
        repeats,
        Vec::<String>::new(),
        "a warm writer reuses its manifest"
    );
}
