#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise full-pipeline diagnostics.

//! Frozen full-pipeline `GrepService` query semantics and budgets.

use loonfs::{
    CommitId, CoreError, CreateDirectoryOptions, CreateNamespaceOptions, DeleteOptions,
    DestinationBehavior, FsAdmin, FsWriter, GrepRequest, GrepResponse, MoveOptions, NamespaceId,
    PutFileOptions, SharedObjectStore,
};
use loonfs_api::AbsolutePath;
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{NamespaceEngine, RuntimeReadContext};
use loonfs_grep::root::load_grep_root;
use loonfs_grep::GramIndexBuildPolicy;
use loonfs_grep::{
    GrepBuildOutcome, GrepIndexSnapshot, GrepReorganizeOutcome, GrepService, GrepWorker,
};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_test_support::ids::nonzero_usize;
use std::collections::BTreeSet;
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

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

async fn read_context(store: &SharedObjectStore, namespace_id: &NamespaceId) -> RuntimeReadContext {
    let (head, root) = load_namespace_read_anchor(&**store, namespace_id)
        .await
        .expect("load read anchor");
    RuntimeReadContext {
        head: head.state,
        head_etag: head.identity.etag,
        manifest_id: root.state.manifest_id,
        manifest_object_id: root.state.manifest_object_id,
        table_cache: Arc::new(MetadataTableCache::new(MetadataTableCacheConfig::default())),
        tail_cache: Arc::new(WalTailProjectionCache::new(WalTailProjectionCacheConfig {
            max_entries: 4,
            max_rows: DEFAULT_WAL_TAIL_PROJECTION_ROWS,
            max_decoded_bytes: DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
        })),
        catalog: None,
    }
}

struct ServiceHarness {
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    engine: NamespaceEngine<SharedObjectStore>,
    service: GrepService,
}

impl ServiceHarness {
    fn new(store: SharedObjectStore, namespace_id: NamespaceId) -> Self {
        let engine = NamespaceEngine::builder(store.clone())
            .namespace_id(namespace_id.clone())
            .writer_id("grep-service-query")
            .build()
            .expect("build query engine");
        Self {
            store,
            namespace_id,
            engine,
            service: GrepService::new(),
        }
    }

    async fn result(&self, grep_request: &GrepRequest) -> loonfs_grep::Result<GrepResponse> {
        let context = read_context(&self.store, &self.namespace_id).await;
        let view = self.engine.load_grep_view(&context).await?;
        let snapshot =
            GrepIndexSnapshot::from_grep_root(&*self.store, &self.namespace_id, &self.service)
                .await;
        self.service
            .query(grep_request, &snapshot, &view, &self.store)
            .await
    }

    async fn success(&self, case: &str, grep_request: &GrepRequest) -> GrepResponse {
        self.result(grep_request)
            .await
            .unwrap_or_else(|error| panic!("expected success for {case}, got {error:?}"))
    }

    async fn error(&self, case: &str, grep_request: &GrepRequest, expected: &CoreError) {
        let error = match self.result(grep_request).await {
            Err(error) => error,
            Ok(response) => panic!("expected error for {case}, got {response:?}"),
        };
        assert_eq!(error.code(), expected.code(), "error code for {case}");
        assert_eq!(
            error.to_string(),
            expected.to_string(),
            "error text for {case}"
        );
    }
}

async fn publish_same_content_files(
    writer: &FsWriter,
    namespace_id: &NamespaceId,
    prefix: &str,
    count: usize,
    content: &[u8],
) {
    let prepared = writer
        .prepare_file_bytes(namespace_id, content)
        .await
        .expect("prepare shared content");
    let content_ref = prepared.content_ref().clone();
    let candidates = (0..count)
        .map(|index| {
            NamespaceMutationCandidate::path_prepared(
                PathMutationIntent::PutFile {
                    commit_id: CommitId::generate(),
                    message: None,
                    absolute_path: AbsolutePath::parse(format!("/{prefix}-{index:04}.txt"))
                        .expect("batch path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                },
                vec![prepared.clone()],
            )
        })
        .collect();
    let results = writer
        .publish_namespace_mutations_batch(namespace_id, candidates)
        .await;
    for result in results {
        result.expect("publish batch file");
    }
}

fn worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    GrepWorker::new(
        store.clone(),
        "grep-worker-tests",
        "grep-worker-tests-session",
        "grep-worker-tests/0.1",
    )
}

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

async fn drive_worker_to_current(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..512 {
        let build = worker
            .build_step(namespace_id, policy)
            .await
            .expect("grep build step");
        let fold = worker
            .reorganize_step(namespace_id, policy)
            .await
            .expect("grep fold step");
        if matches!(build.outcome, GrepBuildOutcome::UpToDate { .. })
            && matches!(fold.outcome, GrepReorganizeOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("grep worker backlog must drain");
}

struct PlanlessBoundaryFixture {
    _temp_dir: TempDir,
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    writer: FsWriter,
    admin: FsAdmin,
}

async fn planless_boundary_fixture(namespace: &str) -> PlanlessBoundaryFixture {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse(namespace).expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("planless-boundary-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id("planless-boundary-admin")
        .build()
        .await
        .expect("build admin");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    let worker = worker(&store);
    worker.enable(&namespace_id).await.expect("enable grep");
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    PlanlessBoundaryFixture {
        _temp_dir: temp_dir,
        store,
        namespace_id,
        writer,
        admin,
    }
}

async fn gram_segment_levels(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<u32> {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("grep root exists")
        .state()
        .segments()
        .iter()
        .map(|segment| segment.level)
        .collect()
}

#[tokio::test]
async fn planless_scan_returns_exact_materialized_and_wal_boundary_revisions_once_each() {
    let fixture = planless_boundary_fixture("grep-planless-boundary").await;
    fixture
        .writer
        .put_file_bytes(
            &fixture.namespace_id,
            "/materialized.txt",
            b"x materialized\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write materialized file");
    let (materialized_head, _) = load_namespace_read_anchor(&*fixture.store, &fixture.namespace_id)
        .await
        .expect("load materialized head");
    fixture
        .admin
        .flush_wal(&fixture.namespace_id)
        .await
        .expect("flush materialized commit");
    let (_, materialized_root) = load_namespace_read_anchor(&*fixture.store, &fixture.namespace_id)
        .await
        .expect("load materialized root");
    assert_eq!(
        materialized_root.state.manifest_head_seq,
        materialized_head.state.seq
    );

    fixture
        .writer
        .put_file_bytes(
            &fixture.namespace_id,
            "/wal-only.txt",
            b"x wal\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write WAL-only file");
    let (head, root) = load_namespace_read_anchor(&*fixture.store, &fixture.namespace_id)
        .await
        .expect("load boundary");
    assert_eq!(root.state.manifest_head_seq, materialized_head.state.seq);
    assert_eq!(
        head.state.seq.0,
        root.state.manifest_head_seq.0 + 1,
        "the WAL-only file must be committed immediately after the materialized boundary"
    );

    let harness = ServiceHarness::new(fixture.store.clone(), fixture.namespace_id.clone());
    let mut scan = request("x");
    scan.allow_scan = true;
    let response = harness
        .success("exact materialization boundary", &scan)
        .await;
    let paths = response
        .matches
        .iter()
        .map(|found| found.absolute_path.as_str())
        .collect::<Vec<_>>();
    assert_eq!(paths.len(), 2);
    assert_eq!(
        paths
            .iter()
            .filter(|path| **path == "/materialized.txt")
            .count(),
        1
    );
    assert_eq!(
        paths
            .iter()
            .filter(|path| **path == "/wal-only.txt")
            .count(),
        1
    );

    fixture
        .writer
        .shutdown_background()
        .await
        .expect("writer shutdown");
}

#[tokio::test]
async fn planless_scan_deduplicates_an_inode_revised_across_materialization() {
    let fixture = planless_boundary_fixture("grep-planless-dedup").await;
    fixture
        .writer
        .put_file_bytes(
            &fixture.namespace_id,
            "/overlap.txt",
            b"x materialized revision\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write materialized revision");
    fixture
        .admin
        .flush_wal(&fixture.namespace_id)
        .await
        .expect("flush materialized revision");
    fixture
        .writer
        .put_file_bytes(
            &fixture.namespace_id,
            "/overlap.txt",
            b"x WAL revision\n",
            PutFileOptions {
                behavior: DestinationBehavior::Replace,
                commit_id: None,
                message: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("write WAL revision");

    let harness = ServiceHarness::new(fixture.store.clone(), fixture.namespace_id.clone());
    let mut scan = request("x");
    scan.allow_scan = true;
    let response = harness.success("overlapping inode sources", &scan).await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].absolute_path, "/overlap.txt");
    assert_eq!(response.matches[0].revision_no.0, 2);
    assert_eq!(response.matches[0].line, "x WAL revision");

    fixture
        .writer
        .shutdown_background()
        .await
        .expect("writer shutdown");
}

#[tokio::test]
async fn grep_service_pins_query_semantics_response_shapes_and_budgets() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grep-service-differential").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grep-service-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let policy = GramIndexBuildPolicy {
        max_l0_runs: nonzero_usize(2),
        max_mid_runs: nonzero_usize(2),
        ..GramIndexBuildPolicy::default()
    };
    let worker = worker(&store);

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    worker.enable(&namespace_id).await.expect("enable grep");
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let folded_corpus: [(&str, &[u8]); 10] = [
        ("/docs/indexed.txt", b"indexed-needle\n"),
        ("/docs/deleted.txt", b"visibility-token deleted\n"),
        ("/docs/moved-source.txt", b"visibility-token moved\n"),
        ("/docs/case.txt", b"MiXeD CaSe ToKeN\n"),
        ("/docs/binary.bin", b"binary-token\0payload"),
        ("/docs/short.txt", b"ab short token\n"),
        ("/docs/empty.txt", b""),
        ("/docs/pages.txt", b"page-token one\npage-token two\n"),
        ("/fold/filler-08.txt", b"fold filler eight\n"),
        ("/fold/filler-09.txt", b"fold filler nine\n"),
    ];
    for (path, content) in folded_corpus {
        writer
            .put_file_bytes(&namespace_id, path, content, PutFileOptions::default())
            .await
            .expect("write folded corpus file");
        drive_worker_step(&worker, &namespace_id, policy).await;
    }

    // The ten rounds above finish a base fold. Two more one-file rounds
    // fold into a fresh mid run, then the large batch below remains delta.
    for round in 10..12u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/fold/filler-{round:02}.txt"),
                format!("fold filler {round}\n").as_bytes(),
                PutFileOptions::default(),
            )
            .await
            .expect("write mid-run filler");
        drive_worker_step(&worker, &namespace_id, policy).await;
    }

    // One final indexed delta supplies enough false-positive candidates to
    // force the 256-file verification budget while preserving delta, mid,
    // and base levels in the same caller-provided snapshot.
    publish_same_content_files(
        &writer,
        &namespace_id,
        "budget/candidate",
        270,
        b"budget-needle without the final letter\n",
    )
    .await;
    drive_worker_step(&worker, &namespace_id, policy).await;
    assert_eq!(
        gram_segment_levels(&store, &namespace_id).await,
        BTreeSet::from([0, 1, 2]),
        "the service snapshot must exercise delta, mid, and base segments"
    );

    writer
        .put_file_bytes(
            &namespace_id,
            "/tail/tail-hit.txt",
            b"ab tail-only-token\n",
            PutFileOptions::default(),
        )
        .await
        .expect("write unindexed tail hit");
    writer
        .delete_path(&namespace_id, "/docs/deleted.txt", DeleteOptions::default())
        .await
        .expect("delete indexed file");
    writer
        .create_directory(&namespace_id, "/archive", CreateDirectoryOptions::default())
        .await
        .expect("create move destination");
    writer
        .move_path(
            &namespace_id,
            "/docs/moved-source.txt",
            "/archive/moved.txt",
            MoveOptions::default(),
        )
        .await
        .expect("move indexed file");

    let harness = ServiceHarness::new(store.clone(), namespace_id.clone());

    let indexed = harness
        .success("indexed hits", &request("indexed-needle"))
        .await;
    assert_eq!(indexed.namespace_id, namespace_id);
    assert!(indexed.built_through_seq < indexed.head_seq);
    assert!(indexed.tail_scanned);
    assert_eq!(indexed.matches.len(), 1);
    assert_eq!(indexed.matches[0].absolute_path, "/docs/indexed.txt");
    assert_eq!(indexed.matches[0].line_number, 1);
    assert_eq!(indexed.matches[0].byte_offset, 0);
    assert_eq!(indexed.matches[0].line, "indexed-needle");
    assert!(!indexed.matches[0].line_truncated);
    assert!(indexed.next_cursor.is_none());

    let tail = harness
        .success("unindexed-tail hits", &request("tail-only-token"))
        .await;
    assert_eq!(tail.matches.len(), 1);
    assert!(tail.tail_scanned);
    assert!(tail.built_through_seq < tail.head_seq);
    assert_eq!(tail.matches[0].absolute_path, "/tail/tail-hit.txt");

    let mut scan_off = request("ab");
    harness
        .error(
            "allow_scan off",
            &scan_off,
            &CoreError::QueryUnindexable(
                "the pattern has no run of at least 3 literal bytes for the trigram index; set \
                 allow_scan to search without it"
                    .to_owned(),
            ),
        )
        .await;
    scan_off.allow_scan = true;
    let mut scanned_paths = BTreeSet::new();
    let mut scan_pages = 0usize;
    loop {
        let scanned = harness
            .success("allow_scan on and short pattern", &scan_off)
            .await;
        scan_pages += 1;
        assert!(scanned.tail_scanned);
        scanned_paths.extend(
            scanned
                .matches
                .iter()
                .map(|found| found.absolute_path.as_str().to_owned()),
        );
        let Some(cursor) = scanned.next_cursor else {
            break;
        };
        scan_off.cursor = Some(cursor);
    }
    assert!(scan_pages > 1);
    assert_eq!(
        scanned_paths,
        BTreeSet::from(["/docs/short.txt", "/tail/tail-hit.txt"])
            .into_iter()
            .map(str::to_owned)
            .collect()
    );

    let mut case_folded = request("mixed case token");
    case_folded.case_insensitive = true;
    let case_folded = harness.success("case folding", &case_folded).await;
    assert_eq!(case_folded.matches.len(), 1);
    assert_eq!(case_folded.matches[0].absolute_path, "/docs/case.txt");

    let visible = harness
        .success("deleted and moved visibility", &request("visibility-token"))
        .await;
    assert_eq!(visible.matches.len(), 1);
    assert_eq!(visible.matches[0].absolute_path, "/archive/moved.txt");

    let mut binary = request("ry");
    binary.allow_scan = true;
    let binary = harness.success("binary eligibility", &binary).await;
    assert!(binary.matches.is_empty());

    let empty = harness
        .success("empty result", &request("definitely-absent-token"))
        .await;
    assert!(empty.matches.is_empty());
    assert!(empty.next_cursor.is_none());

    let mut paged_request = request("budget-needle");
    paged_request.limit = Some(17);
    let mut pages = 0usize;
    let mut matches = 0usize;
    let mut matched_paths = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    loop {
        let page = harness
            .success("multi-page cursor walk", &paged_request)
            .await;
        pages += 1;
        matches += page.matches.len();
        assert_eq!(page.namespace_id, namespace_id);
        assert!(page.tail_scanned);
        assert!(page.matches.len() <= 17);
        for found in &page.matches {
            assert!(found.absolute_path.starts_with("/budget/candidate-"));
            assert_eq!(found.line_number, 1);
            assert_eq!(found.byte_offset, 0);
            assert_eq!(found.line, "budget-needle without the final letter");
            assert!(matched_paths.insert(found.absolute_path.clone()));
        }
        let Some(cursor) = page.next_cursor else {
            break;
        };
        assert!(cursors.insert(cursor.clone()));
        paged_request.cursor = Some(cursor);
    }
    assert!(pages > 1);
    assert_eq!(matches, 270);

    let budget_page = harness
        .success("verified-file budget page", &request("budget-needle.*z"))
        .await;
    assert!(budget_page.matches.is_empty());
    let mut budget_resume = request("budget-needle.*z");
    budget_resume.cursor = budget_page.next_cursor.clone();
    assert!(
        budget_resume.cursor.is_some(),
        "budget exit must return a cursor"
    );
    let budget_end = harness
        .success("verified-file budget resume", &budget_resume)
        .await;
    assert!(budget_end.next_cursor.is_none());

    // Add 512 more unindexed revisions without advancing the index, taking
    // the existing tail past the exact scan budget.
    publish_same_content_files(
        &writer,
        &namespace_id,
        "tail/lagging",
        512,
        b"tail-only-token bulk\n",
    )
    .await;
    let stale_request = request("indexed-needle");
    harness
        .error(
            "allow_stale off over tail budget",
            &stale_request,
            &CoreError::IndexLagging {
                behind_commits: 530,
            },
        )
        .await;
    let mut stale_request = stale_request;
    stale_request.allow_stale = true;
    let stale = harness
        .success("allow_stale on over tail budget", &stale_request)
        .await;
    assert!(!stale.tail_scanned);
    assert_eq!(stale.matches.len(), 1);

    writer.shutdown_background().await.expect("writer shutdown");
}
