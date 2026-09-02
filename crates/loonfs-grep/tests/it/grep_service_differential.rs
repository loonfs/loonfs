#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise full-pipeline diagnostics.

//! Frozen full-pipeline `GrepService` query semantics and budgets.

use crate::common::{control, default_page_limit, grep_with, page_limit, GrepHost};
use loonfs::publish::{CommitCandidate, CommitRequest, FilesystemOperation};
use loonfs::{
    CommitId, CoreError, CreateDirectoryOptions, CreateNamespaceOptions, DeleteOptions,
    DestinationBehavior, FsAdmin, FsReader, FsWriter, MaintenancePlan, MetadataMaintenanceOptions,
    MoveOptions, NamespaceId, PutFileOptions, SharedObjectStore,
};
use loonfs_api::{AbsolutePath, EffectiveLimit, GrepRequest, GrepResponse};
use loonfs_grep::root::load_grep_root;
use loonfs_grep::GramIndexBuildPolicy;
use loonfs_grep::{GrepBuildOutcome, GrepReorganizeOutcome, GrepService, GrepWorker};
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
        allow_stale: false,
        allow_scan: false,
    }
}

/// The query half of a host's composition: one service and one reader over
/// the namespace under test.
struct ServiceHarness {
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    reader: FsReader,
    service: GrepService,
}

impl ServiceHarness {
    async fn new(store: SharedObjectStore, namespace_id: NamespaceId) -> Self {
        let reader = FsReader::builder_with_store(store.clone())
            .build()
            .await
            .expect("build query reader");
        Self {
            store,
            namespace_id,
            reader,
            service: GrepService::new(),
        }
    }

    async fn result(&self, grep_request: &GrepRequest) -> loonfs_grep::Result<GrepResponse> {
        self.page(grep_request, default_page_limit()).await
    }

    async fn page(
        &self,
        grep_request: &GrepRequest,
        limit: EffectiveLimit,
    ) -> loonfs_grep::Result<GrepResponse> {
        grep_with(
            &self.service,
            &self.reader,
            &self.store,
            &self.namespace_id,
            grep_request,
            limit,
        )
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
            CommitCandidate::prepared(
                CommitRequest::single(
                    CommitId::generate(),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/{prefix}-{index:04}.txt"))
                            .expect("batch path"),
                        content_ref: content_ref.clone(),
                        behavior: DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![prepared.clone()],
            )
        })
        .collect::<Vec<_>>();
    // Every candidate is admitted before the publisher's worker can take
    // any of them, so they coalesce into one publication.
    let publisher = writer.publisher();
    let submissions = candidates
        .into_iter()
        .map(|candidate| publisher.submit_candidate(namespace_id.clone(), candidate));
    for result in futures::future::join_all(submissions).await {
        result.expect("publish batch file");
    }
}

async fn worker(store: &SharedObjectStore) -> GrepWorker<SharedObjectStore> {
    GrepHost::new(store, "grep-worker-tests").await.worker
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
        .expect("grep reorganize step");
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
        let reorganize = worker
            .reorganize_step(namespace_id, policy)
            .await
            .expect("grep reorganize step");
        if matches!(build, GrepBuildOutcome::UpToDate { .. })
            && matches!(reorganize, GrepReorganizeOutcome::NotNeeded { .. })
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
    let worker = worker(&store).await;
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
        .manifest_state()
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write materialized file");
    let materialized_head = control::head(&fixture.store, &fixture.namespace_id).await;
    fixture
        .admin
        .run_maintenance(
            &fixture.namespace_id,
            MaintenancePlan {
                metadata: Some(MetadataMaintenanceOptions {
                    max_wal_tail_segments: std::num::NonZeroU64::MIN,
                }),
                ..MaintenancePlan::default()
            },
        )
        .await
        .expect("flush materialized commit");
    let materialized_root = control::metadata_root(&fixture.store, &fixture.namespace_id).await;
    assert_eq!(
        materialized_root.manifest.manifest_head_seq,
        materialized_head.seq
    );

    fixture
        .writer
        .put_file_bytes(
            &fixture.namespace_id,
            "/wal-only.txt",
            b"x wal\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write WAL-only file");
    let head = control::head(&fixture.store, &fixture.namespace_id).await;
    let root = control::metadata_root(&fixture.store, &fixture.namespace_id).await;
    assert_eq!(root.manifest.manifest_head_seq, materialized_head.seq);
    assert_eq!(
        head.seq.0,
        root.manifest.manifest_head_seq.0 + 1,
        "the WAL-only file must be committed immediately after the materialized boundary"
    );

    let harness = ServiceHarness::new(fixture.store.clone(), fixture.namespace_id.clone()).await;
    let mut scan = request("x");
    scan.allow_scan = true;
    let response = harness
        .success("exact materialization boundary", &scan)
        .await;
    let paths = response
        .matches
        .iter()
        .map(|found| found.path.as_str())
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

    fixture.writer.shutdown().await.expect("writer shutdown");
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write materialized revision");
    fixture
        .admin
        .run_maintenance(
            &fixture.namespace_id,
            MaintenancePlan {
                metadata: Some(MetadataMaintenanceOptions {
                    max_wal_tail_segments: std::num::NonZeroU64::MIN,
                }),
                ..MaintenancePlan::default()
            },
        )
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
                commit: loonfs::CommitOptions::new(loonfs_test_support::test_actor()),
                expected_inode_id: None,
                expected_revision_no: None,
            },
        )
        .await
        .expect("write WAL revision");

    let harness = ServiceHarness::new(fixture.store.clone(), fixture.namespace_id.clone()).await;
    let mut scan = request("x");
    scan.allow_scan = true;
    let response = harness.success("overlapping inode sources", &scan).await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].path, "/overlap.txt");
    assert_eq!(response.matches[0].revision_no.0, 2);
    assert_eq!(response.matches[0].line, "x WAL revision");

    fixture.writer.shutdown().await.expect("writer shutdown");
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
        max_delta_runs: nonzero_usize(2),
        max_mid_runs: nonzero_usize(2),
        ..GramIndexBuildPolicy::default()
    };
    let worker = worker(&store).await;

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    worker.enable(&namespace_id).await.expect("enable grep");
    drive_worker_to_current(&worker, &namespace_id, policy).await;

    let reorganized_corpus: [(&str, &[u8]); 10] = [
        ("/docs/indexed.txt", b"indexed-needle\n"),
        ("/docs/deleted.txt", b"visibility-token deleted\n"),
        ("/docs/moved-source.txt", b"visibility-token moved\n"),
        ("/docs/case.txt", b"MiXeD CaSe ToKeN\n"),
        ("/docs/binary.bin", b"binary-token\0payload"),
        ("/docs/short.txt", b"ab short token\n"),
        ("/docs/empty.txt", b""),
        ("/docs/pages.txt", b"page-token one\npage-token two\n"),
        ("/reorganize/filler-08.txt", b"reorganize filler eight\n"),
        ("/reorganize/filler-09.txt", b"reorganize filler nine\n"),
    ];
    for (path, content) in reorganized_corpus {
        writer
            .put_file_bytes(
                &namespace_id,
                path,
                content,
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write reorganized corpus file");
        drive_worker_step(&worker, &namespace_id, policy).await;
    }

    // Add a new mid-level run, then leave the large batch below at the delta level.
    for round in 10..12u32 {
        writer
            .put_file_bytes(
                &namespace_id,
                &format!("/reorganize/filler-{round:02}.txt"),
                format!("reorganize filler {round}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
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
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write unindexed tail hit");
    writer
        .delete_path(
            &namespace_id,
            "/docs/deleted.txt",
            DeleteOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("delete indexed file");
    writer
        .create_directory(
            &namespace_id,
            "/archive",
            CreateDirectoryOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("create move destination");
    writer
        .move_path(
            &namespace_id,
            "/docs/moved-source.txt",
            "/archive/moved.txt",
            MoveOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("move indexed file");

    let harness = ServiceHarness::new(store.clone(), namespace_id.clone()).await;

    let indexed = harness
        .success("indexed hits", &request("indexed-needle"))
        .await;
    assert_eq!(indexed.namespace_id, namespace_id);
    assert!(indexed.built_through_seq < indexed.head_seq);
    assert!(indexed.tail_scanned);
    assert_eq!(indexed.matches.len(), 1);
    assert_eq!(indexed.matches[0].path, "/docs/indexed.txt");
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
    assert_eq!(tail.matches[0].path, "/tail/tail-hit.txt");

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
                .map(|found| found.path.as_str().to_owned()),
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
    assert_eq!(case_folded.matches[0].path, "/docs/case.txt");

    let visible = harness
        .success("deleted and moved visibility", &request("visibility-token"))
        .await;
    assert_eq!(visible.matches.len(), 1);
    assert_eq!(visible.matches[0].path, "/archive/moved.txt");

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
    let mut pages = 0usize;
    let mut matches = 0usize;
    let mut matched_paths = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    loop {
        let page = harness
            .page(&paged_request, page_limit(17))
            .await
            .expect("multi-page cursor walk");
        pages += 1;
        matches += page.matches.len();
        assert_eq!(page.namespace_id, namespace_id);
        assert!(page.tail_scanned);
        assert!(page.matches.len() <= 17);
        for found in &page.matches {
            assert!(found.path.starts_with("/budget/candidate-"));
            assert_eq!(found.line_number, 1);
            assert_eq!(found.byte_offset, 0);
            assert_eq!(found.line, "budget-needle without the final letter");
            assert!(matched_paths.insert(found.path.clone()));
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

    writer.shutdown().await.expect("writer shutdown");
}
