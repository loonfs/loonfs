//! A namespace's immutable view inputs — its config, its content-store
//! descriptor, and the manifest object — are loaded once per handle, not
//! once per operation. These tests pin that a warm handle stops re-fetching
//! them entirely.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsReader, FsWriter, MaintenanceTickOptions, NamespaceId,
    PutFileOptions, SharedObjectStore,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[derive(Debug)]
struct GetRecordingStore {
    inner: LocalFsStore,
    gets: Mutex<Vec<String>>,
}

impl GetRecordingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            gets: Mutex::new(Vec::new()),
        }
    }

    fn take_gets(&self) -> Vec<String> {
        std::mem::take(&mut *self.gets.lock().expect("get log lock poisoned"))
    }

    fn record(&self, key: &str) {
        self.gets
            .lock()
            .expect("get log lock poisoned")
            .push(key.to_owned());
    }
}

#[async_trait]
impl ObjectStore for GetRecordingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record(key);
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record(key);
        self.inner.get_with_metadata(key).await
    }

    async fn put(
        &self,
        key: &str,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        self.inner.put(key, bytes, mode).await
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await
    }

    fn list_prefix_stream(
        &self,
        prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

fn immutable_input_gets(gets: &[String]) -> Vec<String> {
    gets.iter()
        .filter(|key| {
            key.ends_with("/namespace.json")
                || key.ends_with("/descriptor.json")
                || key.ends_with(".manifest.json")
        })
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
        .maintenance_tick_namespace(
            namespace_id,
            MaintenanceTickOptions {
                max_wal_tail_segments: 1,
                ..MaintenanceTickOptions::default()
            },
        )
        .await
        .expect("tick");
}

#[tokio::test]
async fn warm_reader_stops_fetching_immutable_view_inputs() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(GetRecordingStore::new(temp_dir.path()));
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
    let warmup = immutable_input_gets(&recording.take_gets());
    assert!(
        !warmup.is_empty(),
        "the first read on a handle loads the immutable inputs"
    );

    reader
        .stat_path(&namespace_id, "/docs/file-1.txt")
        .await
        .expect("second stat");
    let repeats = immutable_input_gets(&recording.take_gets());
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
    let recording = Arc::new(GetRecordingStore::new(temp_dir.path()));
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
    let warmup = immutable_input_gets(&recording.take_gets());
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
    let repeats = immutable_input_gets(&recording.take_gets());
    assert_eq!(
        repeats,
        Vec::<String>::new(),
        "a warm writer must not re-fetch the namespace config, the \
         content-store descriptor, or the manifest object"
    );
}
