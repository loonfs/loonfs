#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise failure diagnostics.

//! Host-composed grep over the runtime handles, plus direct `GrepWorker`
//! building and folding.

mod common;

use async_trait::async_trait;
use bytes::Bytes;
use common::GrepHost;
use futures::stream::BoxStream;
use loonfs::{
    ChangeSeq, CommitId, CommitOp, CommitRequest, CreateNamespaceOptions, ErrorCode, FsWriter,
    InodeId, NamespaceId, PutFileOptions, SharedObjectStore,
};
use loonfs_api::{decode_cursor, GrepPageCursor, GrepRequest};
use loonfs_grep::codec::INDEX_GRAMS_MAX_FILE_BYTES;
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepError, GrepWorker};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use loonfs_test_support::ids::nonzero_usize;
use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

fn request(pattern: &str) -> GrepRequest {
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
async fn content_blob_keys(store: &SharedObjectStore) -> BTreeSet<String> {
    store
        .list_prefix("content-stores/")
        .await
        .expect("list content blobs")
        .into_iter()
        .filter(|key| key.contains("/blobs/sha256/"))
        .collect()
}

const GRAMS_TEST_VERSION: &str = "grep-worker-tests/0.1";

async fn drive_worker_step(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    worker
        .build_step(namespace_id, policy)
        .await
        .expect("grep build step");
    worker
        .reorganize_step(namespace_id, policy)
        .await
        .expect("grep fold step");
}

/// The watermark from the namespace's verified grep root.
async fn grams_built_through_seq(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> ChangeSeq {
    loonfs_grep::root::load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .state()
        .index()
        .built_through_seq
}

#[tokio::test]
async fn grep_worker_builds_the_gram_index_once_enabled() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-runtime").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let host = GrepHost::new(&store, "grams-admin", GRAMS_TEST_VERSION).await;

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

    // Request validation precedes feature materialization; snapshot
    // construction must preserve that error ordering.
    let mut invalid_limit = request("needle");
    invalid_limit.limit = Some(0);
    let error = host
        .grep(&namespace_id, &invalid_limit)
        .await
        .expect_err("invalid limit must win before the missing feature");
    let GrepError::Runtime(core) = &error else {
        panic!("expected a grep core passthrough, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::InvalidRequest);

    // Before enablement, grep names the missing data half.
    let error = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect_err("grep without the feature must be refused");
    assert!(matches!(error, GrepError::NotEnabled));
    assert_eq!(error.code(), ErrorCode::NotSupported);

    let enabled = host.enable_grep_index(&namespace_id).await.expect("enable");
    assert!(!enabled.already_enabled);
    let again = host
        .enable_grep_index(&namespace_id)
        .await
        .expect("re-enable");
    assert!(again.already_enabled);

    // Explicit worker steps run the backfill and keep the watermark current.
    for _ in 0..2 {
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    }

    let response = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("grep after steps");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/alpha.txt");

    // New commits are visible immediately through the exhaustive tail, and
    // a later step absorbs them into the index.
    writer
        .put_file_bytes(
            &namespace_id,
            "/charlie.txt",
            b"another needle\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write charlie");
    let response = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("grep with tail");
    assert_eq!(response.matches.len(), 2);
    drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("grep after catch-up step");
    assert_eq!(response.matches.len(), 2);
    assert!(response.built_through_seq.0 > 0);

    let disabled = host
        .disable_grep_index(&namespace_id)
        .await
        .expect("disable");
    assert!(disabled.was_enabled);
    let error = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect_err("grep after disable must be refused");
    assert!(matches!(error, GrepError::NotEnabled));
    assert_eq!(error.code(), ErrorCode::NotSupported);

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Runtime background maintenance is metadata-only: a small publish leaves
/// grep backfilling until an explicit worker step runs.
#[tokio::test]
async fn a_publish_below_the_wal_threshold_does_not_schedule_grep_work() {
    let temp_dir = tempdir().expect("tempdir");
    let store: loonfs::SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grams-auto-step").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-auto-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let host = GrepHost::new(&store, "grams-auto-admin", GRAMS_TEST_VERSION).await;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    // Worker-level enable publishes the backfilling root without driving
    // it (a host drives it to quiescence), so the test can observe that
    // nothing else drives it either.
    match host.worker.enable(&namespace_id).await.expect("enable") {
        loonfs_grep::GrepEnableOutcome::Enabled { .. } => {}
        outcome => panic!("expected fresh enable, got {outcome:?}"),
    }

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

    let root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    assert!(matches!(
        root.state().lifecycle(),
        loonfs_grep::root::GrepLifecycle::Backfilling { .. }
    ));
    let error = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect_err("background metadata work must not materialize grep");
    assert!(matches!(error, GrepError::Backfilling));
    assert_eq!(error.code(), ErrorCode::NotSupported);

    drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    // A stale grep serves from the index alone, proving only the explicit
    // worker advanced the watermark past the publish.
    let mut stale = request("needle");
    stale.allow_stale = true;
    let response = host
        .grep(&namespace_id, &stale)
        .await
        .expect("stale grep after explicit worker catch-up");
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/delta.txt");

    writer.shutdown_background().await.expect("writer shutdown");
}

/// A policy passed to the worker bounds each explicit build step: with
/// `max_files_per_step: 3`, one step consumes
/// exactly three of the five pending file commits — the watermark lands on
/// the third put's committed seq — and the next step consumes the rest.
/// Under the default 256-file budget the first step would have caught up
/// to the head outright, so the intermediate watermark is exactly the
/// configured budget observed in effect.
#[tokio::test]
async fn a_worker_policy_bounds_each_build_step() {
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
    let host = GrepHost::new(&store, "grams-config-admin", GRAMS_TEST_VERSION).await;
    let policy = GramIndexBuildPolicy {
        max_files_per_step: nonzero_usize(3),
        ..GramIndexBuildPolicy::default()
    };

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    host.enable_grep_index(&namespace_id).await.expect("enable");
    // Materialize the empty backfill so the steps below run pure WAL
    // catch-up, where the file budget maps one-to-one onto the puts.
    drive_worker_step(&host.worker, &namespace_id, policy).await;

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

    drive_worker_step(&host.worker, &namespace_id, policy).await;
    let after_first = grams_built_through_seq(&store, &namespace_id).await;
    assert_eq!(
        after_first, put_seqs[2],
        "a three-file budget must stop the build step exactly after the \
         third put's commit"
    );

    drive_worker_step(&host.worker, &namespace_id, policy).await;
    let after_second = grams_built_through_seq(&store, &namespace_id).await;
    assert_eq!(
        after_second, put_seqs[4],
        "the next step must consume the remaining two commits"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// A single legal commit can carry thousands of file revisions. The content
/// budget must split that one WAL record at a durable delta cursor, queries
/// must combine the indexed prefix with only its unindexed suffix, and a new
/// worker must resume without reading the prefix again.
#[tokio::test]
async fn a_thousand_file_commit_is_byte_bounded_query_complete_and_crash_resumable() {
    const FILES: usize = 1_000;
    const FILES_PER_STEP: usize = 500;

    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(BuildContentAccountingStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-thousand-atomic").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-thousand-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let host = GrepHost::new(&store, "grams-thousand-admin", GRAMS_TEST_VERSION).await;
    let first_worker = &host.worker;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    host.enable_grep_index(&namespace_id).await.expect("enable");
    first_worker
        .build_step(&namespace_id, GramIndexBuildPolicy::default())
        .await
        .expect("materialize empty backfill");

    let content_bytes = format!("bounded needle file {:04}\n", 0).len();
    let max_content_bytes_per_step = (content_bytes * FILES_PER_STEP) as u64;
    let policy = GramIndexBuildPolicy {
        max_files_per_step: nonzero_usize(FILES),
        max_content_bytes_per_step: NonZeroU64::new(max_content_bytes_per_step)
            .expect("content budget should be nonzero"),
        max_l0_runs: nonzero_usize(usize::MAX),
        ..GramIndexBuildPolicy::default()
    };
    let mut ops = Vec::with_capacity(FILES);
    let mut prepared = Vec::with_capacity(FILES);
    for index in 0..FILES {
        let bytes = format!("bounded needle file {index:04}\n");
        assert_eq!(bytes.len(), content_bytes);
        let content = writer
            .prepare_file_bytes(&namespace_id, bytes.as_bytes())
            .await
            .expect("prepare atomic-commit content");
        ops.push(CommitOp::CreateFile {
            parent_inode_id: InodeId(1),
            display_name: loonfs_api::DisplayName::parse(format!("bounded-{index:04}.txt"))
                .expect("valid display name"),
            content_ref: content.content_ref().clone(),
        });
        prepared.push(content);
    }
    let commit = writer
        .commit_operations_prepared(
            &namespace_id,
            CommitRequest {
                commit_id: CommitId::parse("thousand-file-atomic").expect("commit id"),
                preconditions: Vec::new(),
                ops,
                message: None,
            },
            prepared,
        )
        .await
        .expect("publish thousand-file commit");

    raw_store.reset_content_reads();
    let first = first_worker
        .build_step(&namespace_id, policy)
        .await
        .expect("first bounded build");
    assert!(matches!(
        first.outcome,
        GrepBuildOutcome::Published {
            indexed_revisions,
            ..
        } if indexed_revisions == FILES_PER_STEP as u64
    ));
    let first_reads = raw_store.take_content_reads();
    assert_eq!(first_reads.len(), FILES_PER_STEP);
    assert!(
        content_read_bytes(&first_reads) <= max_content_bytes_per_step,
        "first step read {} content bytes past its {max_content_bytes_per_step}-byte budget",
        content_read_bytes(&first_reads)
    );

    let partial = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load partial grep root")
        .expect("partial grep root");
    assert_eq!(
        partial.state().index().built_through_seq,
        commit.committed_seq
    );
    assert!(
        partial.state().index().next_event_index > 0,
        "the first step must stop within the atomic commit"
    );
    let prefix_segment_ids: BTreeSet<_> = partial
        .state()
        .segments()
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect();

    assert_grep_paths(
        &host,
        &namespace_id,
        "file 0000",
        BTreeSet::from(["/bounded-0000.txt".to_owned()]),
    )
    .await;
    assert_grep_paths(
        &host,
        &namespace_id,
        "file 0999",
        BTreeSet::from(["/bounded-0999.txt".to_owned()]),
    )
    .await;
    assert_eq!(
        collect_grep_paths(&host, &namespace_id, "bounded needle")
            .await
            .len(),
        FILES,
        "the indexed prefix and unindexed suffix must produce every file exactly once"
    );

    // Simulate a crash: the fresh worker has no in-memory collection state and
    // can resume only from the published manifest cursor.
    let resumed_host = GrepHost::new(&store, "grams-resumed-worker", GRAMS_TEST_VERSION).await;
    let resumed_worker = &resumed_host.worker;
    raw_store.reset_content_reads();
    let second = resumed_worker
        .build_step(&namespace_id, policy)
        .await
        .expect("resumed bounded build");
    assert!(matches!(
        second.outcome,
        GrepBuildOutcome::Published {
            indexed_revisions,
            ..
        } if indexed_revisions == FILES_PER_STEP as u64
    ));
    let second_reads = raw_store.take_content_reads();
    assert_eq!(second_reads.len(), FILES_PER_STEP);
    assert!(
        content_read_bytes(&second_reads) <= max_content_bytes_per_step,
        "resumed step read {} content bytes past its {max_content_bytes_per_step}-byte budget",
        content_read_bytes(&second_reads)
    );
    let first_keys: BTreeSet<_> = first_reads.iter().map(|(key, _)| key).collect();
    let second_keys: BTreeSet<_> = second_reads.iter().map(|(key, _)| key).collect();
    assert!(
        first_keys.is_disjoint(&second_keys),
        "the fresh worker must not re-read any indexed-prefix content"
    );
    assert_eq!(first_keys.len() + second_keys.len(), FILES);

    let complete = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load complete grep root")
        .expect("complete grep root");
    assert_eq!(
        complete.state().index().built_through_seq,
        commit.committed_seq
    );
    assert_eq!(complete.state().index().next_event_index, 0);
    let complete_segment_ids: BTreeSet<_> = complete
        .state()
        .segments()
        .iter()
        .map(|segment| segment.segment_id.clone())
        .collect();
    assert!(
        prefix_segment_ids.is_subset(&complete_segment_ids),
        "resume must retain the prefix segments instead of replacing them"
    );
    assert_eq!(
        collect_grep_paths(&host, &namespace_id, "bounded needle")
            .await
            .len(),
        FILES
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

async fn assert_grep_paths(
    host: &GrepHost,
    namespace_id: &NamespaceId,
    pattern: &str,
    expected: BTreeSet<String>,
) {
    assert_eq!(
        collect_grep_paths(host, namespace_id, pattern).await,
        expected
    );
}

async fn collect_grep_paths(
    host: &GrepHost,
    namespace_id: &NamespaceId,
    pattern: &str,
) -> BTreeSet<String> {
    let mut request = request(pattern);
    request.limit = Some(1_000);
    let mut paths = BTreeSet::new();
    loop {
        let response = host
            .grep(namespace_id, &request)
            .await
            .expect("grep bounded atomic commit");
        paths.extend(
            response
                .matches
                .into_iter()
                .map(|found| found.absolute_path.to_string()),
        );
        let Some(cursor) = response.next_cursor else {
            return paths;
        };
        request.cursor = Some(cursor);
    }
}

fn content_read_bytes(reads: &[(String, usize)]) -> u64 {
    reads.iter().map(|(_, bytes)| *bytes as u64).sum::<u64>()
}

/// Ten one-file rounds cross the default delta-fold threshold, so the
/// worker steps tier the index (delta segments fold into a mid run)
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
    let host = GrepHost::new(&store, "grams-tiered-admin", GRAMS_TEST_VERSION).await;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    host.enable_grep_index(&namespace_id).await.expect("enable");

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
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
        expected_paths.push(path);

        let response = host
            .grep(&namespace_id, &request("needle"))
            .await
            .expect("grep");
        let mut matched: Vec<String> = response
            .matches
            .iter()
            .map(|found| found.absolute_path.to_string())
            .collect();
        matched.sort();
        assert_eq!(
            matched, expected_paths,
            "round {round} must return every file written so far"
        );
    }

    // The premise of the test: the rounds really did tier the layout.
    let root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    let grams_levels: Vec<u32> = root
        .state()
        .segments()
        .iter()
        .map(|segment| segment.level)
        .collect();
    assert!(
        grams_levels.contains(&1),
        "ten one-delta rounds must fold at least once into a mid run, got levels {grams_levels:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}

/// Accounts full content-blob GETs issued by one explicit worker step.
#[derive(Debug)]
struct BuildContentAccountingStore {
    inner: LocalFsStore,
    content_reads: Mutex<Vec<(String, usize)>>,
}

impl BuildContentAccountingStore {
    fn new(root: &Path) -> Self {
        Self {
            inner: LocalFsStore::new(root).expect("create local-fs store"),
            content_reads: Mutex::new(Vec::new()),
        }
    }

    fn reset_content_reads(&self) {
        self.content_reads
            .lock()
            .expect("content read accounting lock")
            .clear();
    }

    fn take_content_reads(&self) -> Vec<(String, usize)> {
        std::mem::take(
            &mut *self
                .content_reads
                .lock()
                .expect("content read accounting lock"),
        )
    }

    fn record_content_read(&self, key: &str, bytes: usize) {
        if key.contains("/blobs/sha256/") {
            self.content_reads
                .lock()
                .expect("content read accounting lock")
                .push((key.to_owned(), bytes));
        }
    }
}

#[async_trait]
impl ObjectStore for BuildContentAccountingStore {
    async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        self.inner.head(key).await
    }

    async fn get_with_metadata(&self, key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        let body = self.inner.get_with_metadata(key).await?;
        if let Some(body) = &body {
            self.record_content_read(key, body.bytes.len());
        }
        Ok(body)
    }

    async fn get(
        &self,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        let bytes = self.inner.get(key, range).await?;
        if let Some(bytes) = &bytes {
            self.record_content_read(key, bytes.len());
        }
        Ok(bytes)
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
        if key.contains("/extensions/grep/segments/") && key.ends_with(".sst.zst") {
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

/// Index segment blocks are immutable and keyed by payload checksum, so the
/// grep-private decoded-block cache must serve a repeated query's posting
/// probes without re-fetching the segments it already decoded.
#[tokio::test]
async fn repeated_grep_serves_posting_blocks_from_the_grep_cache() {
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
    let host = GrepHost::new(&store, "grams-cache-admin", GRAMS_TEST_VERSION).await;

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

    host.enable_grep_index(&namespace_id).await.expect("enable");
    for _ in 0..2 {
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    }

    let before_first = raw_store.index_segment_get_count();
    let first = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("first grep");
    assert_eq!(first.matches.len(), 1);
    let after_first = raw_store.index_segment_get_count();
    assert!(
        after_first > before_first,
        "the first grep must read posting blocks from the store"
    );

    let second = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("second grep");
    assert_eq!(second.matches, first.matches);
    let after_second = raw_store.index_segment_get_count();
    assert_eq!(
        after_second, after_first,
        "an identical grep through the same service must serve every \
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
    let host = GrepHost::new(&store, "grams-fault-admin", GRAMS_TEST_VERSION).await;

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

    host.enable_grep_index(&namespace_id).await.expect("enable");
    for _ in 0..2 {
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    }
    let healthy = host
        .grep(&namespace_id, &request("needle"))
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
    let error = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect_err("a reached candidate's failed read must fail its page");
    let GrepError::Runtime(core) = &error else {
        panic!("expected a grep core passthrough, got {error:?}");
    };
    assert_eq!(core.code(), ErrorCode::NamespaceCorrupt);

    // With a one-match limit, alpha's second match fills the page before
    // the walk reaches bravo: the speculative failed read is discarded and
    // the full page comes back with a cursor.
    let mut first_page = request("needle");
    first_page.limit = Some(1);
    let response = host
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
    let mut second_page = request("needle");
    second_page.limit = Some(1);
    second_page.cursor = Some(cursor);
    let error = host
        .grep(&namespace_id, &second_page)
        .await
        .expect_err("the deferred read error must surface on the next page");
    let GrepError::Runtime(core) = &error else {
        panic!("expected a grep core passthrough, got {error:?}");
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
    let host = GrepHost::new(&store, "grams-oversized-admin", GRAMS_TEST_VERSION).await;

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
    host.enable_grep_index(&namespace_id).await.expect("enable");
    for _ in 0..2 {
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
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
    // No step after these writes: bravo and charlie stay in the unindexed
    // tail, where no gram filter screens candidates before verification.

    let gets_before_greps = raw_store.content_blob_get_keys().len();

    let mut first_page = request("needle");
    first_page.limit = Some(1);
    let page_one = host
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
    let cursor: GrepPageCursor = decode_cursor(&cursor_token).expect("decode grep cursor");
    assert!(
        cursor.last_inode_id > page_one.matches[0].inode_id,
        "the cursor must have advanced past the oversized candidate"
    );
    assert_eq!(cursor.last_byte_offset, u64::MAX);

    let mut second_page = request("needle");
    second_page.limit = Some(1);
    second_page.cursor = Some(cursor_token);
    let page_two = host
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

/// Fold steps and grep queries deliberately use separate decoded-block
/// caches: warming grep must not warm the worker's folding cache.
///
/// Two identically shaped delta folds run through one runtime — eight
/// one-file rounds each, explicit worker steps folding on the eighth. The
/// first fold runs cold and is the in-test control; before the second, a
/// grep warms seven of its eight inputs in the grep-private cache. The two
/// identically shaped folds must therefore issue the same section reads.
#[tokio::test]
async fn a_fold_does_not_reuse_grep_private_index_blocks() {
    let temp_dir = tempdir().expect("tempdir");
    let raw_store = Arc::new(IndexSegmentGetCountingStore::new(temp_dir.path()));
    let store: loonfs::SharedObjectStore = raw_store.clone();
    let namespace_id = NamespaceId::parse("grams-fold-cache").expect("namespace id");

    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grams-fold-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    // The host's query service and the worker keep separate block caches:
    // one warms on posting probes, the other on folding.
    let host = GrepHost::new(&store, "grams-fold-admin", GRAMS_TEST_VERSION).await;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    host.enable_grep_index(&namespace_id).await.expect("enable");
    drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;

    // One delta segment per round; the explicit fold step tiers the deltas
    // into a mid run on the eighth round.
    let put_round = |round: u32| {
        let writer = writer.clone();
        let namespace_id = namespace_id.clone();
        let grep_worker = &host.worker;
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
            drive_worker_step(grep_worker, &namespace_id, GramIndexBuildPolicy::default()).await;
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
    let warm = host
        .grep(&namespace_id, &request("needle"))
        .await
        .expect("warming grep");
    assert_eq!(warm.matches.len(), 15);

    let before_warm_fold = raw_store.index_segment_get_count();
    put_round(16).await;
    let warm_fold_gets = raw_store.index_segment_get_count() - before_warm_fold;
    assert!(
        warm_fold_gets > 0,
        "the sixteenth round's fold must still read the delta its own step wrote"
    );
    assert_eq!(
        warm_fold_gets, cold_fold_gets,
        "grep-private cache entries must not alter worker fold reads: cold \
         fold read {cold_fold_gets} sections, query-warmed fold read \
         {warm_fold_gets}"
    );

    // The premise of the comparison: both rounds really did fold, leaving
    // two mid runs and no deltas.
    let root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    let grams: Vec<(u32, u64)> = root
        .state()
        .segments()
        .iter()
        .map(|segment| (segment.level, segment.run_ordinal))
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
        if !key.contains("/extensions/grep/segments/") || !key.ends_with(".sst.zst") {
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
/// Put-and-step rounds accumulate delta runs until the threshold folds
/// them; single-step steps lag the puts and may batch two puts into one
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
    let host = GrepHost::new(&store, "grams-fan-out-admin", GRAMS_TEST_VERSION).await;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    host.enable_grep_index(&namespace_id).await.expect("enable");

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
        drive_worker_step(&host.worker, &namespace_id, GramIndexBuildPolicy::default()).await;
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
    let root = loonfs_grep::root::load_grep_root(&*store, &namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists");
    let grams: Vec<u32> = root
        .state()
        .segments()
        .iter()
        .map(|segment| segment.level)
        .collect();
    assert!(
        grams.contains(&1),
        "the observed fold must have left a mid run behind, got {grams:?}"
    );

    writer.shutdown_background().await.expect("writer shutdown");
}
