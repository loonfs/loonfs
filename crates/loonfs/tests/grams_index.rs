#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise failure diagnostics.

//! Handle-level lifecycle of the gram index: enable through `FsAdmin`,
//! build through maintenance ticks, query through `FsReader`, disable.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    ChangeSeq, CreateNamespaceOptions, ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter,
    GramIndexBuildPolicy, GrepRequest, MaintenanceTickOptions, NamespaceId, PutFileOptions,
};
use loonfs_api::decode_grep_cursor;
use loonfs_api::wire::index_grams::{
    IndexGramsFeature, INDEX_GRAMS_FEATURE_KEY, INDEX_GRAMS_MAX_FILE_BYTES,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

fn grep_request(pattern: &str) -> GrepRequest {
    GrepRequest {
        pattern: pattern.to_owned(),
        case_insensitive: false,
        path_prefix: None,
        cursor: None,
        limit: None,
        allow_stale: false,
        allow_scan: false,
    }
}

/// Content blob object keys currently in the store, for pinpointing the
/// object behind one file's bytes by diffing around its write.
async fn content_blob_keys(store: &loonfs::SharedObjectStore) -> BTreeSet<String> {
    store
        .list_prefix("content-stores/")
        .await
        .expect("list content blobs")
        .into_iter()
        .filter(|key| key.contains("/blobs/sha256/"))
        .collect()
}

/// The `index.grams` watermark from the namespace's current manifest.
async fn grams_built_through_seq(
    store: &loonfs::SharedObjectStore,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    let root = loonfs_core::control::load_namespace_metadata_root_control(&**store, namespace_id)
        .await
        .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let value = manifest
        .payload
        .features
        .get(INDEX_GRAMS_FEATURE_KEY)
        .expect("index.grams feature present");
    IndexGramsFeature::from_value(value)
        .expect("decode feature value")
        .built_through_seq
}

#[tokio::test]
async fn maintenance_ticks_build_the_gram_index_once_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-runtime").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"a needle in alpha\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    writer
        .put_file_bytes(
            &namespace_id,
            "/bravo.txt",
            b"nothing here\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write bravo");

    // Before enablement, grep names the missing data half.
    let error = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect_err("grep without the feature must be refused");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NotSupported);

    let enabled = admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    assert!(!enabled.already_enabled);
    let again = admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("re-enable");
    assert!(again.already_enabled);

    // Explicit maintenance ticks run the backfill and keep the watermark
    // current; two ticks comfortably cover backfill plus catch-up here.
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }

    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep after ticks");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/alpha.txt");

    // New commits are visible immediately through the exhaustive tail, and
    // a later tick absorbs them into the index.
    writer
        .put_file_bytes(
            &namespace_id,
            "/charlie.txt",
            b"another needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write charlie");
    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep with tail");
    assert_eq!(response.matches.len(), 2);
    admin
        .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
        .await
        .expect("tick after write");
    let response = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep after catch-up tick");
    assert_eq!(response.matches.len(), 2);
    assert!(response.built_through_seq.0 > 0);

    let disabled = admin
        .disable_grams_index(&namespace_id)
        .await
        .expect("disable");
    assert!(disabled.was_enabled);
    let error = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect_err("grep after disable must be refused");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NotSupported);

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Index catch-up is scheduled by index lag, not by the WAL-segment
/// threshold: one small publish must still get the index built in the
/// background, with no explicit ticks and a tail far below the threshold.
#[tokio::test]
async fn a_publish_below_the_wal_threshold_still_schedules_index_catch_up() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-auto-tick").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-auto-writer")
        .min_publish_interval_ms(0)
        .background_work(FsBackgroundWork::Enabled)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-auto-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    // The writer runtime has not observed this namespace's feature yet, so
    // this publish exercises the discovery path of the scheduling hint.
    writer
        .put_file_bytes(
            &namespace_id,
            "/delta.txt",
            b"a needle in delta\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write delta");
    writer
        .wait_for_background_work()
        .await
        .expect("background work quiesces");

    let status = admin.namespace_status(&namespace_id).await.expect("status");
    assert!(
        status.wal_tail_segments < MaintenanceTickOptions::default().max_wal_tail_segments,
        "the tail must stay below the flush threshold for this test to \
         exercise index-only scheduling: {status:?}"
    );

    // A stale grep serves from the index alone, so a match here proves the
    // background drain advanced the watermark past the publish.
    let mut stale = grep_request("needle");
    stale.allow_stale = true;
    let response = reader
        .grep(&namespace_id, &stale)
        .await
        .expect("stale grep after background catch-up");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/delta.txt");

    writer.shutdown_background().await.expect("writer shutdown");
}

/// A build policy set through the handle builder reaches the maintenance
/// tick path: with `max_files_per_step: 3`, one tick's build step consumes
/// exactly three of the five pending file commits — the watermark lands on
/// the third put's committed seq — and the next tick consumes the rest.
/// Under the default 256-file budget the first tick would have caught up
/// to the head outright, so the intermediate watermark is exactly the
/// configured budget observed in effect.
#[tokio::test]
async fn a_configured_build_policy_bounds_each_ticks_build_step() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-config-policy").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-config-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-config-admin")
        .gram_index_build(GramIndexBuildPolicy {
            max_files_per_step: 3,
            ..GramIndexBuildPolicy::default()
        })
        .build()
        .await
        .expect("build admin");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    // Materialize the (empty) backfill so the ticks below run pure WAL
    // catch-up, where the file budget maps one-to-one onto the puts.
    admin
        .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
        .await
        .expect("materializing tick");

    let mut put_seqs = Vec::new();
    for index in 0..5u32 {
        let result = writer
            .put_file_bytes(
                &namespace_id,
                &format!("/notes/needle-{index}.txt"),
                format!("a needle numbered {index}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
        put_seqs.push(result.committed_seq);
    }

    admin
        .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
        .await
        .expect("first bounded tick");
    let after_first = grams_built_through_seq(&store, &namespace_id).await;
    assert_eq!(
        after_first, put_seqs[2],
        "a three-file budget must stop the build step exactly after the \
         third put's commit"
    );

    admin
        .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
        .await
        .expect("second bounded tick");
    let after_second = grams_built_through_seq(&store, &namespace_id).await;
    assert_eq!(
        after_second, put_seqs[4],
        "the next tick must consume the remaining two commits"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Ten one-file rounds cross the default delta-fold threshold, so the
/// maintenance ticks tier the index (delta segments fold into a mid run)
/// while the rounds run. Grep must answer identically before, across, and
/// after the transition — levels are fold bookkeeping the read path never
/// sees.
#[tokio::test]
async fn grep_answers_identically_across_tiered_folds() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-tiered").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-tiered-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-tiered-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    let mut expected_paths = Vec::new();
    for round in 0..10u32 {
        let path = format!("/notes/needle-{round:02}.txt");
        writer
            .put_file_bytes(
                &namespace_id,
                &path,
                format!("a needle numbered {round}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
        expected_paths.push(path);

        let response = reader
            .grep(&namespace_id, &grep_request("needle"))
            .await
            .expect("grep");
        let mut matched: Vec<String> = response
            .matches
            .iter()
            .map(|found| found.absolute_path.clone())
            .collect();
        matched.sort();
        assert_eq!(
            matched, expected_paths,
            "round {round} must return every file written so far"
        );
    }

    // The premise of the test: the rounds really did tier the layout.
    let root = loonfs_core::control::load_namespace_metadata_root_control(&*store, &namespace_id)
        .await
        .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let grams_levels: Vec<u32> = manifest
        .payload
        .index_files
        .iter()
        .filter(|descriptor| descriptor.family == "grams")
        .map(|descriptor| descriptor.level)
        .collect();
    assert!(
        grams_levels.contains(&1),
        "ten one-delta rounds must fold at least once into a mid run, got levels {grams_levels:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Counts store GETs against gram index segment objects, so tests can
/// assert how many posting-block reads a query cost.
#[derive(Debug)]
struct IndexSegmentGetCountingStore {
    inner: LocalFsStore,
    index_segment_gets: AtomicUsize,
}

impl IndexSegmentGetCountingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            index_segment_gets: AtomicUsize::new(0),
        }
    }

    fn index_segment_get_count(&self) -> usize {
        self.index_segment_gets.load(Ordering::SeqCst)
    }

    fn record_if_index_segment(&self, key: &str) {
        if key.contains("/metadata/indexes/") {
            self.index_segment_gets.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl ObjectStore for IndexSegmentGetCountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_if_index_segment(key);
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_if_index_segment(key);
        self.inner.get(key, range).await
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

/// Index segment blocks are immutable and keyed by payload checksum, so a
/// reader's decoded-block cache must serve a repeated query's posting
/// probes without re-fetching the segments it already decoded.
#[tokio::test]
async fn repeated_grep_serves_posting_blocks_from_the_table_cache() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(IndexSegmentGetCountingStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-cache").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-cache-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-cache-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"a needle in alpha\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    writer
        .put_file_bytes(
            &namespace_id,
            "/bravo.txt",
            b"nothing here\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write bravo");

    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }

    let before_first = raw_store.index_segment_get_count();
    let first = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("first grep");
    assert_eq!(first.matches.len(), 1);
    let after_first = raw_store.index_segment_get_count();
    assert!(
        after_first > before_first,
        "the first grep must read posting blocks from the store"
    );

    let second = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("second grep");
    assert_eq!(second.matches, first.matches);
    let after_second = raw_store.index_segment_get_count();
    assert_eq!(
        after_second, after_first,
        "an identical grep through the same reader must serve every \
         posting block from the decoded-block cache"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Concurrent candidate content reads keep the serial loop's error
/// positions: a failed read surfaces only when the in-order walk reaches
/// that candidate, so a page that fills first still returns its full
/// matches and a cursor, and the error waits for the next page.
#[tokio::test]
async fn a_failed_candidate_read_surfaces_in_traversal_order() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-read-fault").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-fault-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-fault-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    // Two matches in alpha, so a one-match page fills before the walk
    // reaches bravo.
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"needle one\nneedle two\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    let blobs_before_bravo = content_blob_keys(&store).await;
    writer
        .put_file_bytes(
            &namespace_id,
            "/bravo.txt",
            b"needle three\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write bravo");
    let new_blobs: Vec<String> = content_blob_keys(&store)
        .await
        .difference(&blobs_before_bravo)
        .cloned()
        .collect();
    let [bravo_content_key] = new_blobs.as_slice() else {
        panic!("bravo must add exactly one content blob, got {new_blobs:?}");
    };

    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }
    let healthy = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("grep before the fault");
    assert_eq!(healthy.matches.len(), 3);

    // Break bravo's content read out from under the query.
    store
        .delete(bravo_content_key)
        .await
        .expect("delete bravo's content object");

    // An unlimited page walks past alpha into bravo, so the failed read
    // fails that page, exactly as the serial scan did.
    let error = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect_err("a reached candidate's failed read must fail its page");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NamespaceCorrupt);

    // With a one-match limit, alpha's second match fills the page before
    // the walk reaches bravo: the speculative failed read is discarded and
    // the full page comes back with a cursor.
    let mut first_page = grep_request("needle");
    first_page.limit = Some(1);
    let response = reader
        .grep(&namespace_id, &first_page)
        .await
        .expect("a page that fills before the failed candidate must succeed");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/alpha.txt");
    assert_eq!(response.matches[0].line, "needle one");
    let cursor = response
        .next_cursor
        .expect("a truncated page must carry a cursor");

    // The next page's walk reaches bravo and surfaces the read error at
    // the position the serial scan would have.
    let mut second_page = grep_request("needle");
    second_page.limit = Some(1);
    second_page.cursor = Some(cursor);
    let error = reader
        .grep(&namespace_id, &second_page)
        .await
        .expect_err("the deferred read error must surface on the next page");
    let loonfs::Error::Core(core) = &error else {
        panic!("expected a core error, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NamespaceCorrupt);

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Records the key of every store GET against a content blob object, so
/// tests can assert exactly which file contents a query fetched.
#[derive(Debug)]
struct ContentBlobGetRecordingStore {
    inner: LocalFsStore,
    content_blob_gets: Mutex<Vec<String>>,
}

impl ContentBlobGetRecordingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            content_blob_gets: Mutex::new(Vec::new()),
        }
    }

    fn content_blob_get_keys(&self) -> Vec<String> {
        self.content_blob_gets
            .lock()
            .expect("content GET log lock")
            .clone()
    }

    fn record_if_content_blob(&self, key: &str) {
        if key.contains("/blobs/sha256/") {
            self.content_blob_gets
                .lock()
                .expect("content GET log lock")
                .push(key.to_owned());
        }
    }
}

#[async_trait]
impl ObjectStore for ContentBlobGetRecordingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.record_if_content_blob(key);
        self.inner.get_with_metadata(key).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.record_if_content_blob(key);
        self.inner.get(key, range).await
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

/// An unindexed-tail candidate larger than the index eligibility cap can
/// never pass verification, so grep must skip it on its declared size
/// alone: no content GET for it, unchanged page budgets, and a cursor
/// that resumes past it.
#[tokio::test]
async fn an_oversized_tail_candidate_is_skipped_without_a_content_read() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(ContentBlobGetRecordingStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-oversized-tail").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-oversized-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-oversized-admin")
        .build()
        .await
        .expect("build admin");
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    writer
        .put_file_bytes(
            &namespace_id,
            "/alpha.txt",
            b"a needle in alpha\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write alpha");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");
    for _ in 0..2 {
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }

    // Oversized and full of matches: were it ever fetched and scanned, it
    // would flood the results instead of being skipped.
    let oversized_bytes = b"needle\n".repeat(INDEX_GRAMS_MAX_FILE_BYTES as usize / 7 + 1);
    assert!(oversized_bytes.len() as u64 > INDEX_GRAMS_MAX_FILE_BYTES);
    let blobs_before_bravo = content_blob_keys(&store).await;
    writer
        .put_file_bytes(
            &namespace_id,
            "/bravo.big",
            &oversized_bytes,
            PutFileOptions::default(),
        )
        .await
        .expect("write bravo");
    let new_blobs: Vec<String> = content_blob_keys(&store)
        .await
        .difference(&blobs_before_bravo)
        .cloned()
        .collect();
    let [oversized_content_key] = new_blobs.as_slice() else {
        panic!("the oversized write must add exactly one content blob, got {new_blobs:?}");
    };
    writer
        .put_file_bytes(
            &namespace_id,
            "/charlie.txt",
            b"a needle in charlie\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write charlie");
    // No tick after these writes: bravo and charlie stay in the unindexed
    // tail, where no gram filter screens candidates before verification.

    let gets_before_greps = raw_store.content_blob_get_keys().len();

    let mut first_page = grep_request("needle");
    first_page.limit = Some(1);
    let page_one = reader
        .grep(&namespace_id, &first_page)
        .await
        .expect("first page");
    assert_eq!(page_one.matches.len(), 1);
    assert_eq!(page_one.matches[0].absolute_path, "/alpha.txt");
    let cursor_token = page_one
        .next_cursor
        .clone()
        .expect("a truncated page must carry a cursor");

    // The cursor already stands past the oversized file: fully scanned,
    // never to be re-verified by a later page.
    let cursor = decode_grep_cursor(&cursor_token).expect("decode grep cursor");
    assert!(
        cursor.last_inode_id > page_one.matches[0].inode_id,
        "the cursor must have advanced past the oversized candidate"
    );
    assert_eq!(cursor.last_byte_offset, u64::MAX);

    let mut second_page = grep_request("needle");
    second_page.limit = Some(1);
    second_page.cursor = Some(cursor_token);
    let page_two = reader
        .grep(&namespace_id, &second_page)
        .await
        .expect("second page");
    assert_eq!(page_two.matches.len(), 1);
    assert_eq!(page_two.matches[0].absolute_path, "/charlie.txt");
    assert!(page_two.next_cursor.is_none());
    assert!(
        cursor.last_inode_id < page_two.matches[0].inode_id,
        "the first page's cursor must sit between alpha and charlie"
    );

    let content_gets = raw_store.content_blob_get_keys();
    let fetched_during_greps = &content_gets[gets_before_greps..];
    assert!(
        !fetched_during_greps.is_empty(),
        "the greps must fetch the small candidates' contents"
    );
    assert!(
        fetched_during_greps
            .iter()
            .all(|key| key != oversized_content_key),
        "no content GET may touch the oversized object: {fetched_during_greps:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Fold steps resolve their merge reads through the same decoded-block
/// cache queries fill: a reader derived from the writer shares its
/// runtime core, so a grep that decoded the delta segments must spare the
/// next fold those segments' store reads.
///
/// Two identically shaped delta folds run through one runtime — eight
/// one-file rounds each, background ticks folding on the eighth. The
/// first fold runs cold (nothing read those segments before) and is the
/// in-test control; before the second, a grep warms seven of its eight
/// inputs. Only the tick that triggers a fold can write its eighth delta,
/// so that segment is always cold and a zero-read fold is unreachable
/// through the runtime; strictly fewer reads than the identically shaped
/// cold fold is the deterministic form of "already-warm blocks are not
/// re-fetched", and it stays true if segment block layout changes.
#[tokio::test]
async fn a_fold_reuses_the_index_blocks_a_grep_already_decoded() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(IndexSegmentGetCountingStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-fold-cache").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-fold-writer")
        .min_publish_interval_ms(0)
        .background_work(FsBackgroundWork::Enabled)
        .build()
        .await
        .expect("build writer");
    // The derived reader shares the writer's runtime core, so its greps
    // fill the cache the writer's background fold steps read through.
    let reader = writer.reader();
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-fold-admin")
        .build()
        .await
        .expect("build admin");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    // One delta segment per round: every put schedules a background drain
    // that builds exactly the new revision, and the drain's fold step
    // tiers the deltas into a mid run on the eighth round.
    let put_round = |round: u32| {
        let writer = writer.clone();
        let namespace_id = namespace_id.clone();
        async move {
            writer
                .put_file_bytes(
                    &namespace_id,
                    &format!("/notes/needle-{round:02}.txt"),
                    format!("a needle numbered {round}\n").as_bytes(),
                    PutFileOptions::default(),
                )
                .await
                .expect("write file");
            writer
                .wait_for_background_work()
                .await
                .expect("background work quiesces");
        }
    };

    for round in 1..8u32 {
        put_round(round).await;
    }
    let before_cold_fold = raw_store.index_segment_get_count();
    put_round(8).await;
    let cold_fold_gets = raw_store.index_segment_get_count() - before_cold_fold;
    assert!(
        cold_fold_gets > 0,
        "the eighth round's fold must read its snapshot segments from the store"
    );

    for round in 9..16u32 {
        put_round(round).await;
    }
    // The warm-up: one grep that matches every file decodes the pending
    // delta segments' index and posting blocks into the shared cache.
    let warm = reader
        .grep(&namespace_id, &grep_request("needle"))
        .await
        .expect("warming grep");
    assert_eq!(warm.matches.len(), 15);

    let before_warm_fold = raw_store.index_segment_get_count();
    put_round(16).await;
    let warm_fold_gets = raw_store.index_segment_get_count() - before_warm_fold;
    assert!(
        warm_fold_gets > 0,
        "the sixteenth round's fold must still read the delta its own tick wrote"
    );
    assert!(
        warm_fold_gets < cold_fold_gets,
        "a fold whose snapshot a grep already decoded must serve those \
         blocks from the table cache: cold fold read {cold_fold_gets} \
         sections, query-warmed fold read {warm_fold_gets}"
    );

    // The premise of the comparison: both rounds really did fold, leaving
    // two mid runs and no deltas.
    let root = loonfs_core::control::load_namespace_metadata_root_control(&*store, &namespace_id)
        .await
        .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let grams: Vec<(u32, u64)> = manifest
        .payload
        .index_files
        .iter()
        .filter(|descriptor| descriptor.family == "grams")
        .map(|descriptor| (descriptor.level, descriptor.run_seq.0))
        .collect();
    assert!(
        grams.iter().all(|(level, _)| *level == 1),
        "sixteen one-delta rounds must leave only mid-level segments, got {grams:?}"
    );
    let mid_runs: std::collections::BTreeSet<u64> =
        grams.iter().map(|(_, run_seq)| *run_seq).collect();
    assert_eq!(
        mid_runs.len(),
        2,
        "each eight-round batch must have folded into its own mid run, got {grams:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Tracks how many GETs against gram index segment objects are in flight
/// at once. The yield before each forwarded read lets sibling fetches
/// issued in the same fan-out begin before this one completes, so the
/// peak observes overlap exactly when the caller issued the GETs
/// concurrently; serial callers can never raise it above one.
#[derive(Debug)]
struct InFlightIndexGetProbeStore {
    inner: LocalFsStore,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

impl InFlightIndexGetProbeStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            in_flight: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn peak_in_flight(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    async fn probed<T, F>(&self, key: &str, read: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        if !key.contains("/metadata/indexes/") {
            return read.await;
        }
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        tokio::task::yield_now().await;
        let result = read.await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

#[async_trait]
impl ObjectStore for InFlightIndexGetProbeStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        self.probed(key, self.inner.get_with_metadata(key)).await
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        self.probed(key, self.inner.get(key, range)).await
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

/// A cold fold — nothing decoded its snapshot before — must fan out its
/// per-segment cursor opens instead of paying one round trip per
/// segment, and the fan-out must stay within the maintenance IO cap.
///
/// Put-and-tick rounds accumulate delta runs until the threshold folds
/// them; single-step ticks lag the puts and may batch two puts into one
/// run, so the rounds run until the fold's reads appear rather than to a
/// fixed count. No query ever touches the namespace and build steps only
/// write gram segments, so the fold's reads are the only gram-segment
/// GETs the probe can see: the peak measures exactly the fold's opens.
#[tokio::test]
async fn a_cold_fold_fans_out_its_segment_opens_within_the_io_cap() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(InFlightIndexGetProbeStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-fold-fan-out").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-fan-out-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("grams-fan-out-admin")
        .build()
        .await
        .expect("build admin");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    admin
        .enable_grams_index(&namespace_id)
        .await
        .expect("enable");

    // Each round writes one file and runs one bounded build step plus
    // one bounded fold step; the first gram-segment GET is, by
    // construction, the triggered fold reading its snapshot of every
    // accumulated delta run.
    let mut rounds = 0u32;
    while raw_store.peak_in_flight() == 0 {
        rounds += 1;
        assert!(
            rounds <= 24,
            "the delta threshold must fold within a bounded number of rounds"
        );
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/notes/needle-{rounds:02}.txt"),
                format!("a needle numbered {rounds}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write file");
        admin
            .maintenance_tick_namespace(&namespace_id, MaintenanceTickOptions::default())
            .await
            .expect("maintenance tick");
    }

    let peak = raw_store.peak_in_flight();
    assert!(
        peak > 1,
        "a cold fold's segment opens must overlap, got a serial peak of {peak}"
    );
    assert!(
        peak <= 8,
        "the fold's fan-out must respect the maintenance IO cap, got {peak}"
    );

    // The premise of the probe: the reads the peak observed were a real
    // delta fold, which leaves a mid run behind.
    let root = loonfs_core::control::load_namespace_metadata_root_control(&*store, &namespace_id)
        .await
        .expect("metadata root");
    let manifest_key =
        metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let manifest_bytes = store
        .get(&manifest_key, None)
        .await
        .expect("read namespace manifest")
        .expect("namespace manifest exists");
    let manifest = decode_namespace_manifest_json(&manifest_bytes).expect("decode manifest");
    let grams: Vec<u32> = manifest
        .payload
        .index_files
        .iter()
        .filter(|descriptor| descriptor.family == "grams")
        .map(|descriptor| descriptor.level)
        .collect();
    assert!(
        grams.contains(&1),
        "the observed fold must have left a mid run behind, got {grams:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}
