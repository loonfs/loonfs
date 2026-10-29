//! Maintenance runs: the metadata job's loop, the retention window job, and
//! runs against namespaces that do not exist or are deleted.

use super::*;
use loonfs::MetadataCompactionOutcome;
use loonfs::{MaintainMetadataOptions, MaintenanceCancellation};
use loonfs_objectstore::layout::{parse_object_key, DurableObjectFamily};
use loonfs_test_support::ids::writer_id;
use loonfs_test_support::stores::{RecordedOperation, RecordingStore};
use loonfs_types::format::segment_blocks::DEFAULT_MAX_DELTA_RUNS;
use tower::ServiceExt;

async fn run(
    router: &axum::Router,
    namespace_id: &NamespaceId,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let uri = format!("/v0/maintenance/namespaces/{namespace_id}/runs");
    send(router, "POST", &uri, body).await
}

async fn send(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let response = router
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", "Bearer test-token")
                .header("content-type", "application/json")
                .header("Loonfs-Actor", "maintenance-runs")
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
        .config(loonfs::RuntimeConfig {
            min_publish_interval_ms: 0,
            ..loonfs::RuntimeConfig::default()
        })
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
        for index in 0..DEFAULT_MAX_DELTA_RUNS.get() + 2 {
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
    for kind in ["metadata", "gc", "retention", "retention_window"] {
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
            "retention_window missing: 404 namespace_not_found",
            "retention_window deleted: 410 namespace_deleted",
        ]
    );
    state.runtime.shutdown().await.expect("shutdown writer");
}

/// Checks that a namespace's retention window is set at creation, changed
/// and cleared by `retention_window` runs, included in the namespace's
/// metadata, and copied by a fork, and that a window of zero is rejected.
#[tokio::test]
async fn a_retention_window_is_set_at_creation_changed_by_runs_and_copied_by_a_fork() {
    let directory = tempdir().expect("tempdir");
    let (router, state) = test_app(
        test_options(directory.path(), "retention-window-runs"),
        TestAppOptions::default(),
    )
    .await
    .expect("app");
    let windowed = namespace_id("windowed");
    let window = |body: &serde_json::Value| body.get("retention_window_ms").cloned();
    let rejected = |(status, body): (StatusCode, serde_json::Value)| {
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            (&body["code"], &body["param"]),
            (
                &serde_json::json!("invalid_request"),
                &serde_json::json!("/retention_window_ms")
            )
        );
    };

    rejected(
        send(
            &router,
            "POST",
            "/v0/namespaces",
            r#"{"namespace_id":"zero","retention_window_ms":0}"#,
        )
        .await,
    );
    let (status, created) = send(
        &router,
        "POST",
        "/v0/namespaces",
        r#"{"namespace_id":"windowed","retention_window_ms":86400000}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(window(&created), Some(serde_json::json!(86_400_000)));

    rejected(
        run(
            &router,
            &windowed,
            r#"{"kind":"retention_window","retention_window_ms":0}"#,
        )
        .await,
    );
    let (status, changed) = run(
        &router,
        &windowed,
        r#"{"kind":"retention_window","retention_window_ms":3600000}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    assert_eq!(
        changed,
        serde_json::json!({"kind": "retention_window", "namespace_id": "windowed", "retention_window_ms": 3_600_000})
    );
    let (_, shown) = send(&router, "GET", "/v0/namespaces/windowed", "").await;
    assert_eq!(window(&shown), Some(serde_json::json!(3_600_000)));
    let (status, forked) = send(
        &router,
        "POST",
        "/v0/namespaces/windowed/forks",
        r#"{"new_namespace_id":"forked"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{forked}");
    assert_eq!(
        window(&forked),
        Some(serde_json::json!(3_600_000)),
        "a fork copies its source's window"
    );

    let manifest_no = || async {
        state
            .maintenance
            .diagnostics(&windowed)
            .await
            .expect("diagnostics")
            .current_manifest_no
    };
    let before = manifest_no().await;
    for _ in 0..2 {
        let (status, cleared) = run(&router, &windowed, r#"{"kind":"retention_window"}"#).await;
        assert_eq!(status, StatusCode::OK, "{cleared}");
        assert_eq!(
            cleared,
            serde_json::json!({"kind": "retention_window", "namespace_id": "windowed"})
        );
    }
    assert_eq!(
        manifest_no().await.map(|number| number.0),
        before.map(|number| number.0 + 1),
        "the first clear publishes a manifest, and the second finds nothing to change"
    );
    let (_, shown) = send(&router, "GET", "/v0/namespaces/windowed", "").await;
    assert_eq!(window(&shown), None);
    let (_, shown) = send(&router, "GET", "/v0/namespaces/forked", "").await;
    assert_eq!(
        window(&shown),
        Some(serde_json::json!(3_600_000)),
        "the fork keeps its own copy"
    );
    state.runtime.shutdown().await.expect("shutdown writer");
}
