//! Diagnostic (ignored): per-section request accounting for the warm-ops
//! benchmark shape. Builds a bench-like namespace, then runs each warm
//! phase on a fresh handle while classifying every store GET — by object
//! class, and for metadata tables by section (index / filter / data),
//! family, and run level, using the live manifest's own descriptors.
//!
//! Run with:
//!   cargo test -p loonfs --test request_accounting -- --ignored --nocapture

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsReader, FsWriter, MaintenanceTickOptions, NamespaceId,
    PageRequest, PaginationPolicy, PutFileOptions, SharedObjectStore,
};
use loonfs_api::AbsolutePath;

use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

const FILES: usize = 10_000;
const BATCH: usize = 100;
const TICK_EVERY_BATCHES: usize = 33;

type RecordedGet = (String, Option<(u64, u64)>);

#[derive(Debug, Default)]
struct RequestLog {
    gets: Mutex<Vec<RecordedGet>>,
}

impl RequestLog {
    fn record(&self, key: &str, range: Option<&ByteRange>) {
        self.gets.lock().expect("request log lock poisoned").push((
            key.to_owned(),
            range.map(|range| (range.start_inclusive, range.end_exclusive)),
        ));
    }

    fn take(&self) -> Vec<RecordedGet> {
        std::mem::take(&mut *self.gets.lock().expect("request log lock poisoned"))
    }
}

#[derive(Debug)]
struct RecordingStore {
    inner: LocalFsStore,
    log: Arc<RequestLog>,
}

impl RecordingStore {
    fn new(root: &Path, log: Arc<RequestLog>) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            log,
        }
    }
}

#[async_trait]
impl ObjectStore for RecordingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.log.record(key, range.as_ref());
        self.inner.get(key, range).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.log.record(key, None);
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

/// (family, level, index_offset, filter_offset) per table object key.
type TableMap = BTreeMap<String, (String, u32, u64, u64)>;

async fn table_map(store: &SharedObjectStore, namespace_id: &NamespaceId) -> TableMap {
    let root = loonfs_core::control::load_namespace_metadata_root_control(store, namespace_id)
        .await
        .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = decode_namespace_manifest_json(&bytes).expect("decode manifest");
    manifest
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| {
            (
                descriptor.object_key.clone(),
                (
                    format!("{:?}", descriptor.family),
                    descriptor.level,
                    descriptor.index_block.offset,
                    descriptor.filter_block.offset,
                ),
            )
        })
        .collect()
}

#[allow(clippy::print_stdout)]
fn report(phase: &str, gets: &[RecordedGet], tables: &TableMap) {
    let mut by_class: BTreeMap<String, usize> = BTreeMap::new();
    let mut by_table: BTreeMap<String, usize> = BTreeMap::new();
    for (key, range) in gets {
        let class = if let Some((family, level, index_offset, filter_offset)) = tables.get(key) {
            let section = match range {
                Some((start, _)) if start == index_offset => "index",
                Some((start, _)) if start == filter_offset => "filter",
                Some(_) => "data",
                None => "whole",
            };
            by_table
                .entry(format!("L{level} {family} {section}"))
                .and_modify(|count| *count += 1)
                .or_insert(1);
            format!("table:{section}")
        } else if key.contains("/wal/segments/") {
            "wal".to_owned()
        } else if key.contains("/manifests/") {
            "manifest".to_owned()
        } else if key.contains("content-stores/") {
            "content".to_owned()
        } else {
            "control".to_owned()
        };
        by_class
            .entry(class)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
    println!("== {phase}: {} GETs", gets.len());
    for (class, count) in &by_class {
        println!("   {class:<16} {count}");
    }
    for (bucket, count) in &by_table {
        println!("     {bucket:<44} {count}");
    }
}

#[allow(clippy::print_stdout)]
#[tokio::test]
#[ignore = "diagnostic: prints warm-phase request accounting"]
async fn warm_phase_request_accounting() {
    let temp_dir = tempdir().expect("tempdir");
    let log = Arc::new(RequestLog::default());
    let store: SharedObjectStore = Arc::new(RecordingStore::new(temp_dir.path(), Arc::clone(&log)));
    let namespace_id = NamespaceId::parse("acct").expect("valid namespace id");

    // Build phase: bench-like shape — one wide hot directory, maintenance
    // ticks a few times so the manifest ends with a seed base plus a few L0
    // runs and a WAL tail, like the 10k benchmark build.
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("acct-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("acct-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    // The bench publishes 100-mutation batches (one WAL segment each) and
    // ticks a few times across the build; mirror that shape.
    let mut index = 0usize;
    while index < FILES {
        let mut candidates = Vec::with_capacity(BATCH);
        for _ in 0..BATCH.min(FILES - index) {
            let bytes = format!("body {index}");
            let content_ref = loonfs_core::content::store_bytes_as_content(
                &store,
                &namespace_id,
                bytes.as_bytes(),
            )
            .await
            .expect("stage content")
            .content_ref;
            candidates.push(loonfs::publish::NamespaceMutationCandidate::Path(
                loonfs::publish::PathMutationIntent::PutFile {
                    commit_id: loonfs::CommitId::generate(),
                    absolute_path: AbsolutePath::parse(format!("/hot/file-{index:05}.txt"))
                        .expect("path"),
                    content_ref,
                    behavior: loonfs::PutBehavior::NoReplace,
                },
            ));
            index += 1;
        }
        for outcome in writer
            .publish_namespace_mutations_batch(&namespace_id, candidates)
            .await
        {
            outcome.expect("publish batch member");
        }
        if (index / BATCH) % TICK_EVERY_BATCHES == 0 {
            admin
                .maintenance_tick_namespace(
                    &namespace_id,
                    MaintenanceTickOptions {
                        max_wal_tail_segments: 1,
                        ..MaintenanceTickOptions::default()
                    },
                )
                .await
                .expect("tick");
        }
    }
    let tables = table_map(&store, &namespace_id).await;
    println!(
        "layout: {} table objects across levels {:?}",
        tables.len(),
        tables
            .values()
            .map(|(_, level, _, _)| *level)
            .collect::<std::collections::BTreeSet<_>>()
    );
    let _ = log.take();

    // Warm phases, each on the same fresh handle like the bench: a full
    // paged list, then stat, read, write.
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let limit = PaginationPolicy::from_values(1_000, 1_000)
        .expect("valid pagination policy")
        .resolve_limit(Some(1_000))
        .expect("valid limit");
    let mut listed = 0usize;
    let mut cursor = None;
    loop {
        let page = reader
            .list_path_entries_page(&namespace_id, "/hot", PageRequest { limit, cursor })
            .await
            .expect("list page");
        listed += page.entries.len();
        match page.next_cursor.as_deref() {
            Some(encoded) => {
                cursor = Some(
                    loonfs_api::decode_directory_cursor(encoded).expect("valid directory cursor"),
                );
            }
            None => break,
        }
    }
    assert_eq!(listed, FILES);
    report("warm full list", &log.take(), &tables);

    reader
        .stat_path(&namespace_id, "/hot/file-04999.txt")
        .await
        .expect("stat");
    report("warm stat", &log.take(), &tables);

    reader
        .read_file_bytes(&namespace_id, "/hot/file-05000.txt")
        .await
        .expect("read");
    report("warm read", &log.take(), &tables);

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("acct-writer-2")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build second writer");
    writer
        .put_file_bytes(
            &namespace_id,
            "/hot/file-05001.txt",
            b"replaced",
            PutFileOptions {
                behavior: loonfs::PutBehavior::Replace,
                commit_id: None,
            },
        )
        .await
        .expect("warm write");
    report("warm write", &log.take(), &tables);

    // A second write on the same handle separates per-handle warmup cost
    // from per-write cost.
    writer
        .put_file_bytes(
            &namespace_id,
            "/hot/file-05002.txt",
            b"replaced again",
            PutFileOptions {
                behavior: loonfs::PutBehavior::Replace,
                commit_id: None,
            },
        )
        .await
        .expect("second warm write");
    report("warm write (same handle, second)", &log.take(), &tables);
}
