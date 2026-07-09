//! Post-publish cache behavior: a landed publish seeds the read caches
//! instead of dropping them, and cache invalidation never erases writer
//! fencing.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, FsWriter, NamespaceId, PutFileOptions, RuntimeError, SharedObjectStore,
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

async fn writer(store: &SharedObjectStore, writer_id: &str) -> FsWriter {
    FsWriter::builder_with_store(store.clone())
        .writer_id(writer_id)
        .commit_window_ms(0)
        .build()
        .await
        .expect("build writer")
}

/// A fenced writer session must stay fenced: before this fix, the runtime
/// dropped the commit engine after every successful publish and maintenance
/// pass, so a superseded writer forgot its fencing and silently re-acquired
/// the epoch — two live writers would fence each other back and forth
/// instead of one surfacing `writer_fenced`.
#[tokio::test]
async fn fenced_writer_stays_fenced_instead_of_reacquiring() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("create store"));
    let namespace_id = NamespaceId::parse("fence").expect("valid namespace id");

    let writer_a = writer(&store, "writer-a").await;
    writer_a
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer_a
        .put_file_bytes(&namespace_id, "/a1.txt", b"a", PutFileOptions::default())
        .await
        .expect("writer a first put");

    let writer_b = writer(&store, "writer-b").await;
    writer_b
        .put_file_bytes(&namespace_id, "/b1.txt", b"b", PutFileOptions::default())
        .await
        .expect("writer b takes over the epoch");

    let fenced = writer_a
        .put_file_bytes(&namespace_id, "/a2.txt", b"a", PutFileOptions::default())
        .await
        .expect_err("superseded writer surfaces fencing");
    assert!(
        matches!(
            &fenced,
            RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
        "unexpected error: {fenced:?}"
    );

    // The fenced session stays fenced on the next attempt too, and the live
    // writer keeps publishing undisturbed.
    let still_fenced = writer_a
        .put_file_bytes(&namespace_id, "/a3.txt", b"a", PutFileOptions::default())
        .await
        .expect_err("fenced session never reacquires on its own");
    assert!(
        matches!(
            &still_fenced,
            RuntimeError::Core(error) if error.code() == loonfs::ErrorCode::WriterFenced
        ),
        "unexpected error: {still_fenced:?}"
    );
    writer_b
        .put_file_bytes(&namespace_id, "/b2.txt", b"b", PutFileOptions::default())
        .await
        .expect("live writer is not fenced back");
}

/// A landed publish seeds the read caches with the state it just produced,
/// so read-after-write on the same core issues no store GETs at all: the
/// anchor, catalog, manifest, tail projection, and table blocks are all in
/// memory.
#[tokio::test]
async fn read_after_write_is_served_from_seeded_caches() {
    let temp_dir = tempdir().expect("tempdir");
    let recording = Arc::new(GetRecordingStore::new(temp_dir.path()));
    let store: SharedObjectStore = recording.clone();
    let namespace_id = NamespaceId::parse("seeded").expect("valid namespace id");

    let writer = writer(&store, "seed-writer").await;
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let reader = writer.reader();
    for index in 0..3 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/docs/warm-{index}.txt"),
                b"warm",
                PutFileOptions::default(),
            )
            .await
            .expect("warmup put");
    }
    reader
        .stat_path(&namespace_id, "/docs/warm-0.txt")
        .await
        .expect("warmup stat");

    recording.take_gets();
    writer
        .put_file_bytes(
            &namespace_id,
            "/docs/fresh.txt",
            b"fresh",
            PutFileOptions::default(),
        )
        .await
        .expect("steady-state put");
    // The publish itself must read the live head and root for freshness;
    // nothing else.
    let write_gets = recording.take_gets();
    assert!(
        write_gets
            .iter()
            .all(|key| key.ends_with("/wal/head.json") || key.ends_with("/metadata/root.json")),
        "a steady-state write reads only the live head and root, got {write_gets:?}"
    );

    reader
        .stat_path(&namespace_id, "/docs/fresh.txt")
        .await
        .expect("read after write");
    let read_gets = recording.take_gets();
    assert_eq!(
        read_gets,
        Vec::<String>::new(),
        "read-after-write must be served from the seeded caches"
    );
}
