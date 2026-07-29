//! A namespace's immutable view input — the manifest object its head
//! resolves to — is loaded once per handle, not once per operation. These
//! tests pin that a warm handle stops re-fetching it entirely.

use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsReader, FsWriter, MaintenanceStepOptions, NamespaceId,
    PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::stores::{KeyPredicate, RecordingStore};
use std::sync::Arc;
use tempfile::tempdir;

fn immutable_input_gets(gets: &[String]) -> Vec<String> {
    gets.iter()
        .filter(|key| key.ends_with(".manifest.json"))
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
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("seed-admin")
        .build()
        .await
        .expect("build admin");
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
                PutFileOptions::default(),
            )
            .await
            .expect("seed file");
    }
    admin
        .maintenance_step_namespace(
            namespace_id,
            MaintenanceStepOptions {
                max_wal_tail_segments: 1,
                ..MaintenanceStepOptions::default()
            },
        )
        .await
        .expect("step");
}

#[tokio::test]
async fn warm_reader_stops_fetching_immutable_view_inputs() {
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
        .stat_path(&namespace_id, "/docs/file-0.txt")
        .await
        .expect("first stat");
    let warmup = immutable_input_gets(&recording.take_get_keys());
    assert!(
        !warmup.is_empty(),
        "the first read on a handle loads the immutable inputs"
    );

    reader
        .stat_path(&namespace_id, "/docs/file-1.txt")
        .await
        .expect("second stat");
    let repeats = immutable_input_gets(&recording.take_get_keys());
    assert_eq!(
        repeats,
        Vec::<String>::new(),
        "a warm handle must not re-fetch the namespace config, the \
         content-store descriptor, or the manifest object"
    );
}

#[tokio::test]
async fn warm_writer_stops_fetching_immutable_view_inputs() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(RecordingStore::new(
        LocalFsStore::new(temp_dir.path()).expect("create local-fs store"),
        KeyPredicate::any(),
    ));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("pins").expect("valid namespace id");
    build_namespace(&store, &namespace_id).await;

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
            PutFileOptions::default(),
        )
        .await
        .expect("first write");
    let warmup = immutable_input_gets(&recording.take_get_keys());
    assert!(
        !warmup.is_empty(),
        "the first write on a handle loads the immutable inputs"
    );

    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/file-6.txt",
            b"body",
            PutFileOptions::default(),
        )
        .await
        .expect("second write");
    let repeats = immutable_input_gets(&recording.take_get_keys());
    assert_eq!(
        repeats,
        Vec::<String>::new(),
        "a warm writer must not re-fetch the namespace config, the \
         content-store descriptor, or the manifest object"
    );
}
