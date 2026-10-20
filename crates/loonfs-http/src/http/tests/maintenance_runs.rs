//! Maintenance runs: the metadata job's loop, and runs against namespaces
//! that do not exist or are deleted.

use super::*;
use loonfs::{MaintainMetadataOptions, MaintenanceCancellation};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::ids::writer_id;
use loonfs_test_support::stores::{RecordedOperation, RecordingStore};
use loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS;
use loonfs_types::MetadataCompactionOutcome;
use tower::ServiceExt;

async fn run(
    router: &axum::Router,
    namespace_id: &NamespaceId,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!("/v0/maintenance/namespaces/{namespace_id}/runs"))
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body.to_owned()))
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body");
    (
        status,
        serde_json::from_slice(&bytes).expect("JSON response"),
    )
}

/// Writes enough folded commits to each namespace that a metadata
/// compaction is due, through a runtime that shuts down before the test's
/// app starts, so no writer session compacts the namespaces on its own.
async fn write_delta_runs(store: SharedObjectStore, namespaces: &[&NamespaceId]) {
    let actor = loonfs_test_support::test_actor();
    let writer = LoonFs::builder_with_store(store)
        .writer_id(writer_id("history-writer"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let maintenance = writer.maintenance(writer_id("history-maintenance"));
    for namespace_id in namespaces {
        writer
            .create_namespace(namespace_id, &actor)
            .await
            .expect("create namespace");
        let namespace = writer.open_namespace(namespace_id).expect("open namespace");
        for index in 0..DEFAULT_MAX_DELTA_RUNS + 2 {
            namespace
                .put_file(&format!("/file-{index}"), b"body", &actor)
                .await
                .expect("put a file");
            // Each fold publishes the commit as its own delta run.
            maintenance
                .fold_wal(namespace_id)
                .await
                .expect("fold the tail");
        }
    }
    writer.shutdown().await.expect("shut down the writer");
}

/// The metadata segment reads and the manifest publications that the store
/// recorded.
fn segment_reads_and_manifest_puts(store: &RecordingStore<LocalFsStore>) -> (usize, usize) {
    let mut counts = (0, 0);
    for operation in store.snapshot() {
        let family = parse_object_key(operation.key()).map(|key| key.family());
        match (&operation, family) {
            (
                RecordedOperation::Get { .. } | RecordedOperation::GetWithMetadata { .. },
                Some(DurableObjectFamily::MetadataSegment),
            ) => counts.0 += 1,
            (
                RecordedOperation::Put { .. } | RecordedOperation::PutStreamed { .. },
                Some(DurableObjectFamily::MetadataManifest),
            ) => counts.1 += 1,
            _ => {}
        }
    }
    counts
}

/// Checks that one `metadata` run does the work that a client's `metadata`
/// and `metadata_compaction` runs did, with the same metadata segment reads
/// and the same manifest publications.
#[tokio::test]
async fn one_metadata_run_does_the_work_of_the_step_and_compaction_runs() {
    let directory = tempdir().expect("tempdir");
    let store = Arc::new(RecordingStore::new(
        LocalFsStore::new(directory.path()).expect("store"),
        KeyPredicate::any(),
    ));
    let [stepped, looping] = ["stepped", "looping"].map(namespace_id);
    write_delta_runs(store.clone(), &[&stepped, &looping]).await;
    let mut options = test_options(directory.path(), "metadata-loop");
    options.grep = JobMode::Disabled;
    let (router, state) = test_app(
        options,
        TestAppOptions {
            store: Some(store.clone()),
            ..TestAppOptions::default()
        },
    )
    .await
    .expect("app");

    // These are the calls of a client that ran one pass per `metadata` run,
    // and one unit per `metadata_compaction` run whenever a pass reported a
    // window too large for it, until a pass found nothing due.
    store.reset();
    let mut units = 0;
    loop {
        let step = state
            .maintenance
            .maintain_metadata_with_options(&stepped, &MaintainMetadataOptions::default())
            .await
            .expect("metadata step");
        match step.compaction {
            MetadataCompactionOutcome::NotNeeded => break,
            MetadataCompactionOutcome::Published { .. } => units += 1,
            MetadataCompactionOutcome::MetadataCompactionRequired => {
                let unit = state
                    .maintenance
                    .compact_metadata(&stepped, &MaintenanceCancellation::new())
                    .await
                    .expect("compaction unit");
                assert_matches!(unit.compaction, MetadataCompactionOutcome::Published { .. });
                units += 1;
            }
            outcome => panic!("no other publisher runs in this test, not {outcome:?}"),
        }
    }
    let stepped_requests = segment_reads_and_manifest_puts(&store);

    store.reset();
    let (status, body) = run(&router, &looping, r#"{"kind":"metadata"}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "caught_up");
    assert_eq!(body["compaction_units"], units);
    let looping_requests = segment_reads_and_manifest_puts(&store);

    assert!(units > 0, "the history needs a compaction unit");
    assert_eq!(looping_requests, stepped_requests);
    let mut manifests = Vec::new();
    for namespace_id in [&stepped, &looping] {
        let diagnostics = state
            .maintenance
            .diagnostics(namespace_id)
            .await
            .expect("diagnostics");
        manifests.push(diagnostics.current_manifest_no);
    }
    assert_eq!(
        manifests[0], manifests[1],
        "both histories end on the same manifest"
    );
    state.runtime.shutdown().await.expect("shutdown writer");
}

#[tokio::test]
async fn runtime_jobs_answer_a_missing_and_a_deleted_namespace_with_fixed_codes() {
    let directory = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(directory.path(), "maintenance-runs"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let missing = namespace_id("missing");
    let deleted = namespace_id("deleted");
    state
        .runtime
        .create_namespace(&deleted, &loonfs_test_support::test_actor())
        .await
        .expect("create namespace");
    state
        .runtime
        .open_namespace(&deleted)
        .expect("open namespace")
        .delete()
        .await
        .expect("delete namespace");

    let mut answers = Vec::new();
    for kind in ["metadata", "gc", "retention"] {
        for namespace_id in [&missing, &deleted] {
            let body = format!(r#"{{"kind":"{kind}"}}"#);
            let (status, body) = run(&router, namespace_id, &body).await;
            let code = body["code"]
                .as_str()
                .unwrap_or_else(|| body["kind"].as_str().expect("job kind"));
            answers.push(format!("{kind} {namespace_id}: {} {code}", status.as_u16()));
        }
    }
    assert_eq!(
        answers,
        [
            "metadata missing: 404 namespace_not_found",
            "metadata deleted: 410 namespace_deleted",
            "gc missing: 404 namespace_not_found",
            "gc deleted: 200 gc",
            "retention missing: 404 namespace_not_found",
            "retention deleted: 410 namespace_deleted",
        ]
    );
    state.runtime.shutdown().await.expect("shutdown writer");
}
