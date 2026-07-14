#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise failure diagnostics.

//! Handle-level lifecycle of the gram index: enable through `FsAdmin`,
//! build through maintenance ticks, query through `FsReader`, disable.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use loonfs::{
    CreateNamespaceOptions, ErrorCode, FsAdmin, FsBackgroundWork, FsReader, FsWriter, GrepRequest,
    MaintenanceTickOptions, NamespaceId, PutFileOptions,
};
use loonfs_api::decode_grep_cursor;
use loonfs_api::wire::index_grams::INDEX_GRAMS_MAX_FILE_BYTES;
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

#[tokio::test]
async fn maintenance_ticks_build_the_gram_index_once_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-runtime").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-writer")
        .commit_window_ms(0)
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
        .commit_window_ms(0)
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
        .commit_window_ms(0)
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
        .commit_window_ms(0)
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
        .commit_window_ms(0)
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
        .commit_window_ms(0)
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
