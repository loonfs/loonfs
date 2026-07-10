//! Pins the cold first-stat request shape: a fresh handle resolving a path
//! over a manifest that carries several unfolded L0 runs must not pay a
//! per-run filter fetch (the manifest's inline filter copies answer those),
//! and every metadata-table object it does touch is small enough to load
//! whole with a single ranged GET.
//!
//! This is the regression guard for the cold-stat latency cliff: the run
//! count multiplying into filter-block round-trips on `DirentryBinds`
//! lookups was the scale term, and dependent per-section GETs were the
//! wave count.

use async_trait::async_trait;
use bytes::Bytes;
use loonfs::{
    CreateNamespaceOptions, FsAdmin, FsReader, FsWriter, MaintenanceTickOptions, NamespaceId,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

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
    ) -> futures::stream::BoxStream<'static, Result<String, ObjectStoreError>> {
        self.inner.list_prefix_stream(prefix)
    }
}

#[tokio::test]
async fn cold_stat_pays_no_per_run_filter_fetches() {
    let temp_dir = tempdir().expect("tempdir");
    let log = Arc::new(RequestLog::default());
    let store: loonfs::SharedObjectStore =
        Arc::new(RecordingStore::new(temp_dir.path(), Arc::clone(&log)));
    let namespace_id = NamespaceId::parse("coldstat").expect("valid namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("coldstat-writer")
        .commit_window_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("coldstat-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");

    // Three commit batches, checkpointed one L0 run each, with names
    // interleaved by stride so every run's direntry key range straddles the
    // whole directory span: range pruning alone cannot rule any run out of
    // a name lookup, which is exactly the shape a bulk-loaded backlog takes.
    const FILES: usize = 180;
    const RUNS: usize = 3;
    for run in 0..RUNS {
        let mut candidates = Vec::new();
        for index in (0..FILES).filter(|index| index % RUNS == run) {
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
                    absolute_path: format!("/tree/dir-000000/file-{index:09}.txt"),
                    content_ref,
                    behavior: loonfs::PutBehavior::NoReplace,
                },
            ));
        }
        for outcome in writer
            .publish_namespace_mutations_batch(&namespace_id, candidates)
            .await
        {
            outcome.expect("publish batch member");
        }
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

    // Confirm the manifest really carries several straddling L0 direntry
    // runs with inline filters — the premise of the assertions below.
    let root = loonfs_core::control::load_namespace_metadata_root_control(&store, &namespace_id)
        .await
        .expect("load metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let direntry_l0_runs: BTreeSet<_> = manifest
        .payload
        .metadata_files
        .iter()
        .filter(|descriptor| {
            descriptor.level == 0 && format!("{:?}", descriptor.family) == "DirentryBinds"
        })
        .map(|descriptor| descriptor.run_seq)
        .collect();
    assert!(
        direntry_l0_runs.len() >= RUNS,
        "expected at least {RUNS} unfolded L0 direntry runs, found {}",
        direntry_l0_runs.len()
    );
    assert!(
        manifest
            .payload
            .metadata_files
            .iter()
            .filter(|descriptor| descriptor.level == 0)
            .all(|descriptor| descriptor.filter_inline.is_some()),
        "every L0 segment should carry an inline filter"
    );
    let filter_offsets: BTreeSet<(String, u64)> = manifest
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| {
            (
                descriptor.object_key.clone(),
                descriptor.filter_block.offset,
            )
        })
        .collect();
    let table_keys: BTreeSet<String> = manifest
        .payload
        .metadata_files
        .iter()
        .map(|descriptor| descriptor.object_key.clone())
        .collect();

    // The measured operation: first stat on a fresh handle, nothing warm.
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let _ = log.take();
    let entry = reader
        .stat_path(&namespace_id, "/tree/dir-000000/file-000000042.txt")
        .await
        .expect("cold stat");
    assert_eq!(entry.display_name, "file-000000042.txt");
    assert!(entry.revision_no.is_some(), "file stat carries a revision");

    let gets = log.take();
    let table_gets: Vec<&RecordedGet> = gets
        .iter()
        .filter(|(key, _)| table_keys.contains(key))
        .collect();
    assert!(!table_gets.is_empty(), "a cold stat reads metadata tables");
    for (key, range) in &table_gets {
        let start = range.map(|(start, _)| start);
        assert!(
            !filter_offsets.contains(&(key.clone(), start.unwrap_or(0))),
            "cold stat fetched a filter block from `{key}`: inline copies \
             and whole-object reads should answer every filter consultation"
        );
        assert_eq!(
            start,
            Some(0),
            "small segments should load whole with one ranged GET, got a \
             partial read of `{key}` at {start:?}"
        );
    }
    // The wave budget: a handful of control-plane reads plus one
    // whole-object read per touched segment. The exact count is pinned
    // loosely so genuine wave regressions (per-run filter sweeps, split
    // filter/index/data fetches) trip it while block-size tuning does not.
    assert!(
        gets.len() <= 16,
        "cold stat issued {} GETs; expected a bounded, near-flat set: {:?}",
        gets.len(),
        gets
    );
}
