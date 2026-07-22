#![allow(clippy::panic)]
// Lifecycle assertions use panic for precise differential diagnostics.

//! Full-pipeline differential lock between core's reference grep and GrepService.

use loonfs::{
    CommitId, CreateDirectoryOptions, CreateNamespaceOptions, DeleteOptions, DestinationBehavior,
    ErrorCode, FsWriter, GramIndexBuildPolicy, GrepRequest, GrepResponse, MoveOptions, NamespaceId,
    PutFileOptions, SharedObjectStore,
};
use loonfs_api::wire::manifest::decode_namespace_manifest_json;
use loonfs_api::AbsolutePath;
use loonfs_core::cache::{
    MetadataTableCache, MetadataTableCacheConfig, WalTailProjectionCache,
    WalTailProjectionCacheConfig, DEFAULT_WAL_TAIL_PROJECTION_DECODED_BYTES,
    DEFAULT_WAL_TAIL_PROJECTION_ROWS,
};
use loonfs_core::control::load_namespace_read_anchor;
use loonfs_core::publish::{NamespaceMutationCandidate, PathMutationIntent};
use loonfs_core::{NamespaceEngine, RuntimeReadContext};
use loonfs_grep::{GrepIndexSnapshot, GrepService};
use loonfs_objectstore::keys::metadata_manifest_object;
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
use std::sync::Arc;
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

struct DifferentialHarness {
    store: SharedObjectStore,
    namespace_id: NamespaceId,
    engine: NamespaceEngine<SharedObjectStore>,
    service: GrepService,
}

impl DifferentialHarness {
    fn new(store: SharedObjectStore, namespace_id: NamespaceId) -> Self {
        let engine = NamespaceEngine::builder(store.clone())
            .namespace_id(namespace_id.clone())
            .writer_id("grep-differential-reference")
            .build()
            .expect("build reference engine");
        Self {
            store,
            namespace_id,
            engine,
            service: GrepService::new(),
        }
    }

    async fn results(
        &self,
        grep_request: &GrepRequest,
    ) -> (
        loonfs_core::Result<GrepResponse>,
        loonfs_core::Result<GrepResponse>,
    ) {
        let context = read_context(&self.store, &self.namespace_id).await;
        // This old core entry point intentionally survives only as the
        // differential oracle until the final deletion PR.
        let core = self
            .engine
            .grep_with_runtime_context(grep_request, &context)
            .await;
        let service = async {
            let view = self
                .engine
                .load_grep_view_with_runtime_context(&context)
                .await?;
            let snapshot = GrepIndexSnapshot::from_core_parts(view.grep_index_snapshot_parts());
            self.service
                .query(grep_request, &snapshot, &view, &self.store)
                .await
        }
        .await;
        (core, service)
    }

    async fn success(&self, case: &str, grep_request: &GrepRequest) -> GrepResponse {
        let (core, service) = self.results(grep_request).await;
        match (core, service) {
            (Ok(core), Ok(service)) => {
                assert_eq!(service, core, "full response diverged for {case}");
                service
            }
            (core, service) => {
                panic!("expected success for {case}, got core={core:?}, service={service:?}")
            }
        }
    }

    async fn error(&self, case: &str, grep_request: &GrepRequest, code: ErrorCode) {
        let (core, service) = self.results(grep_request).await;
        match (core, service) {
            (Err(core), Err(service)) => {
                assert_eq!(core.code(), code, "unexpected core code for {case}");
                assert_eq!(service.code(), code, "unexpected service code for {case}");
                assert_eq!(
                    service.to_string(),
                    core.to_string(),
                    "error text for {case}"
                );
            }
            (core, service) => {
                panic!("expected error for {case}, got core={core:?}, service={service:?}")
            }
        }
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
                    absolute_path: AbsolutePath::parse(format!("/{prefix}-{index:04}.txt"))
                        .expect("batch path"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
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

async fn drive_old_index_step(
    engine: &NamespaceEngine<SharedObjectStore>,
    policy: GramIndexBuildPolicy,
) {
    engine
        .build_grams_index_step(policy, None)
        .await
        .expect("old core build step");
    engine
        .fold_grams_index_step(policy, None)
        .await
        .expect("old core fold step");
}

async fn gram_segment_levels(
    store: &SharedObjectStore,
    namespace_id: &NamespaceId,
) -> BTreeSet<u32> {
    let (_, root) = load_namespace_read_anchor(&**store, namespace_id)
        .await
        .expect("load root");
    let key = metadata_manifest_object(namespace_id.as_str(), &root.state.manifest_object_id);
    let bytes = store
        .get(&key, None)
        .await
        .expect("read manifest")
        .expect("manifest exists");
    decode_namespace_manifest_json(&bytes)
        .expect("decode manifest")
        .payload
        .index_files
        .into_iter()
        .filter(|segment| segment.family == "grams")
        .map(|segment| segment.level)
        .collect()
}

#[tokio::test]
async fn grep_service_matches_core_across_query_semantics_and_budgets() {
    let temp_dir = tempdir().expect("tempdir");
    let store: SharedObjectStore =
        Arc::new(LocalFsStore::new(temp_dir.path()).expect("local store"));
    let namespace_id = NamespaceId::parse("grep-service-differential").expect("namespace id");
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("grep-differential-writer")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("build writer");
    let policy = GramIndexBuildPolicy {
        max_l0_runs: 2,
        max_mid_runs: 2,
        ..GramIndexBuildPolicy::default()
    };
    let old_engine = NamespaceEngine::builder(store.clone())
        .namespace_id(namespace_id.clone())
        .writer_id("grep-differential-old-index")
        .build()
        .expect("build old index engine");

    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    old_engine.enable_grams_index().await.expect("enable index");
    drive_old_index_step(&old_engine, policy).await;

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
        drive_old_index_step(&old_engine, policy).await;
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
        drive_old_index_step(&old_engine, policy).await;
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
    drive_old_index_step(&old_engine, policy).await;
    assert_eq!(
        gram_segment_levels(&store, &namespace_id).await,
        BTreeSet::from([0, 1, 2]),
        "the differential snapshot must exercise delta, mid, and base segments"
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

    let harness = DifferentialHarness::new(store.clone(), namespace_id.clone());

    let indexed = harness
        .success("indexed hits", &request("indexed-needle"))
        .await;
    assert_eq!(indexed.matches.len(), 1);

    let tail = harness
        .success("unindexed-tail hits", &request("tail-only-token"))
        .await;
    assert_eq!(tail.matches.len(), 1);
    assert!(tail.tail_scanned);

    let mut scan_off = request("ab");
    harness
        .error("allow_scan off", &scan_off, ErrorCode::QueryUnindexable)
        .await;
    scan_off.allow_scan = true;
    let scanned = harness
        .success("allow_scan on and short pattern", &scan_off)
        .await;
    assert_eq!(scanned.matches.len(), 1);
    assert_eq!(scanned.matches[0].absolute_path, "/tail/tail-hit.txt");

    let mut case_folded = request("mixed case token");
    case_folded.case_insensitive = true;
    let case_folded = harness.success("case folding", &case_folded).await;
    assert_eq!(case_folded.matches.len(), 1);

    let visible = harness
        .success("deleted and moved visibility", &request("visibility-token"))
        .await;
    assert_eq!(visible.matches.len(), 1);
    assert_eq!(visible.matches[0].absolute_path, "/archive/moved.txt");

    let mut binary = request("bi");
    binary.allow_scan = true;
    let binary = harness.success("binary eligibility", &binary).await;
    assert!(binary.matches.is_empty());

    let empty = harness
        .success("empty result", &request("definitely-absent-token"))
        .await;
    assert!(empty.matches.is_empty());

    let mut paged_request = request("budget-needle");
    paged_request.limit = Some(17);
    let mut pages = 0usize;
    let mut matches = 0usize;
    loop {
        let page = harness
            .success("multi-page cursor walk", &paged_request)
            .await;
        pages += 1;
        matches += page.matches.len();
        let Some(cursor) = page.next_cursor else {
            break;
        };
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

    // Raise the unindexed revision tail to 513 files without advancing the
    // index, one over the exact tail budget.
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
            ErrorCode::IndexLagging,
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
