#![allow(clippy::panic)]
//! In-process HTTP matrix for the four things a deployment can do about
//! grep: answer searches, keep the index built, both, or neither.

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::{CreateNamespaceOptions, FsWriter, PutFileOptions};
use loonfs::{FsAdmin, FsReader};
use loonfs_api::v0::{GrepGcResponse, GrepIndex, GrepIndexLifecycle};
use loonfs_api::{
    ApiError, CapabilityDocument, ChangeSeq, GrepResponse, NamespaceId, FEATURE_ADMIN_GREP_INDEX,
    FEATURE_QUERY_GREP, LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX,
    LIMIT_QUERY_GREP_SCAN_BUDGET_FILES, LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, PROFILE_QUERY_V0,
};
use loonfs_grep::root::{load_grep_root, GrepIndexStatus};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepWorker, GREP_INDEX_JOB};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_server::{
    app, AppOptions, GrepConfig, GrepMode, MaintenanceMode, ReadConsistencyConfig,
    RuntimeCacheConfigOverrides, ServerConfig, StoreConfig,
};
use serde::de::DeserializeOwned;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn disabled_mode_returns_not_supported_and_omits_grep_capabilities() {
    let temp_dir = tempdir().expect("store tempdir");
    let (_store, _writer, namespace_id) = seed_namespace(temp_dir.path(), "disabled").await;
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::Disabled),
        AppOptions::default(),
    )
    .await
    .expect("build app");

    let capabilities = capabilities(&router).await;
    assert!(!capabilities.features.contains_key(FEATURE_QUERY_GREP));
    assert!(!capabilities.features.contains_key(FEATURE_ADMIN_GREP_INDEX));
    assert!(
        !capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0),
        "a deployment that answers `not_supported` on every query route must not advertise \
         the plane"
    );
    for limit in grep_limits() {
        assert!(!capabilities.limits.contains_key(limit));
    }
    assert!(!maintains_grep_index(&server.writer));

    // Searching and administering gate on their own keys, so a refusal names
    // the key whose absence the client just read.
    assert_not_supported(
        &router,
        Method::GET,
        &query_path(&namespace_id),
        None,
        FEATURE_QUERY_GREP,
    )
    .await;
    for path in admin_grep_paths(&namespace_id) {
        assert_not_supported(&router, Method::POST, &path, None, FEATURE_ADMIN_GREP_INDEX).await;
    }
    assert_not_supported(
        &router,
        Method::GET,
        &status_path(&namespace_id),
        None,
        FEATURE_ADMIN_GREP_INDEX,
    )
    .await;
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn grep_get_query_parameters_use_the_list_route_grammar() {
    let temp_dir = tempdir().expect("store tempdir");
    let (_store, _writer, namespace_id) = seed_namespace(temp_dir.path(), "query-grammar").await;
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeOnly),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    let path = query_path(&namespace_id);

    let response = send(&router, Method::GET, &path, None).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = response_json(response).await;
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("pattern"));

    for name in ["case_insensitive", "allow_scan", "allow_stale"] {
        for value in ["yes", "1", "TRUE", ""] {
            let response = send(
                &router,
                Method::GET,
                &format!("{path}?pattern=needle&{name}={value}"),
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let error: ApiError = response_json(response).await;
            assert_eq!(error.code, "invalid_request");
            assert_eq!(error.param.as_deref(), Some(name));
        }
        for value in ["true", "false"] {
            let response = send(
                &router,
                Method::GET,
                &format!("{path}?pattern=needle&{name}={value}"),
                None,
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        }
    }

    let at_bound = "n".repeat(1024);
    let response = send(
        &router,
        Method::GET,
        &format!("{path}?pattern={at_bound}"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);

    let over_bound = "n".repeat(1025);
    let response = send(
        &router,
        Method::GET,
        &format!("{path}?pattern={over_bound}"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: ApiError = response_json(response).await;
    assert_eq!(error.code, "invalid_request");
    assert_eq!(error.param.as_deref(), Some("pattern"));
    assert!(error.message.contains("maximum is 1024 bytes"));

    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn serving_and_maintaining_enables_queries_nudges_and_disables_per_namespace() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "both").await;
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeAndMaintain),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(maintains_grep_index(&server.writer));
    // Before anything is enabled the status route answers honestly rather
    // than inventing a namespace-not-found.
    assert_eq!(
        index_status(&router, &namespace_id).await.lifecycle,
        GrepIndexLifecycle::Disabled
    );

    let enabled: GrepIndex = response_json(
        send(
            &router,
            Method::POST,
            &format!("/v0/admin/namespaces/{namespace_id}/grep/index/enable"),
            None,
        )
        .await,
    )
    .await;
    // A fresh enable publishes a backfill and reports the sequence its
    // checkpoint captured — not a watermark it has not reached.
    assert!(
        matches!(
            &enabled.lifecycle,
            GrepIndexLifecycle::Backfilling {
                target_seq: ChangeSeq(0),
                cursor_inode_id: None,
                ..
            }
        ),
        "{:?}",
        enabled.lifecycle
    );
    assert_eq!(
        index_status(&router, &namespace_id).await.lifecycle,
        enabled.lifecycle,
        "the status route and the enable response describe the same root"
    );
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(0));
    let active = index_status(&router, &namespace_id).await;
    assert_eq!(
        active.lifecycle,
        GrepIndexLifecycle::Active {
            built_through_seq: ChangeSeq(0),
            next_event_index: 0,
        }
    );
    assert!(!active.reorganize_pending);

    // Re-enabling an active root reports the phase it found, still tagged.
    let again: GrepIndex = response_json(
        send(
            &router,
            Method::POST,
            &format!("/v0/admin/namespaces/{namespace_id}/grep/index/enable"),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(again.lifecycle, active.lifecycle);

    // The file lands through a writer of its own, so nothing in this server
    // observed the publish: the index stays where it was until a request
    // touches the namespace again.
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"automatic needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(0));

    let capabilities = capabilities(&router).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    assert!(
        capabilities.supports(FEATURE_ADMIN_GREP_INDEX),
        "a deployment doing both jobs advertises both keys"
    );
    assert!(capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0));
    for limit in grep_limits() {
        assert!(capabilities.limits.contains_key(limit));
    }
    assert_served_document_covers_the_spec_example(&capabilities);

    // The first search over a namespace whose index trails answers from the
    // exhaustive tail and nudges the index at the same time.
    let response = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].path, "/note.txt");
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let caught_up = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(caught_up.matches.len(), 1);
    assert_eq!(caught_up.built_through_seq, caught_up.head_seq);

    // Disabling is one durable compare-and-swap; the runner discovers it.
    let disabled_response = disable_grep(&router, &namespace_id).await;
    assert_eq!(disabled_response.lifecycle, GrepIndexLifecycle::Disabled);
    assert!(!disabled_response.reorganize_pending);
    let disabled = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load disabled root")
        .expect("disabled root");
    assert!(matches!(
        disabled.manifest_state().status(),
        GrepIndexStatus::Disabled {}
    ));
    settle(&server.writer).await;
    assert!(
        matches!(
            load_grep_root(&*store, &namespace_id)
                .await
                .expect("reload disabled root")
                .expect("disabled root")
                .manifest_state()
                .status(),
            GrepIndexStatus::Disabled {}
        ),
        "no step may resurrect a root the operator disabled"
    );

    let gc: GrepGcResponse = response_json(
        send(
            &router,
            Method::POST,
            &format!("/v0/admin/namespaces/{namespace_id}/grep/index/gc"),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(gc.namespace_id, namespace_id);
    assert_eq!(
        gc.next_cursor, None,
        "an unbudgeted pass walks the whole grep keyspace"
    );

    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let reenabled = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(reenabled.matches.len(), 1);
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn first_query_after_restart_resumes_stale_and_mid_backfill_namespaces() {
    let temp_dir = tempdir().expect("store tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id("restart-seed")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let stale = NamespaceId::parse("restart-stale").expect("namespace id");
    let backfill = NamespaceId::parse("restart-backfill").expect("namespace id");
    for namespace_id in [&stale, &backfill] {
        writer
            .create_namespace(namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace");
    }
    writer
        .put_file_bytes(
            &stale,
            "/indexed.txt",
            b"indexed before restart\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write indexed file");
    for index in 0..3 {
        writer
            .put_file_bytes(
                &backfill,
                &format!("/backfill-{index}.txt"),
                format!("mid-backfill needle {index}\n").as_bytes(),
                PutFileOptions::new(loonfs_test_support::test_actor()),
            )
            .await
            .expect("write backfill file");
    }

    let worker = grep_worker(&store, "restart-worker").await;
    worker.enable(&stale).await.expect("enable stale namespace");
    drive_worker_to_current(&worker, &stale, GramIndexBuildPolicy::default()).await;
    writer
        .put_file_bytes(
            &stale,
            "/tail.txt",
            b"stale steady needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write unindexed tail");

    worker
        .enable(&backfill)
        .await
        .expect("enable backfill namespace");
    worker
        .build_step(
            &backfill,
            GramIndexBuildPolicy {
                max_files_per_step: NonZeroUsize::MIN,
                ..GramIndexBuildPolicy::default()
            },
        )
        .await
        .expect("leave mid-backfill root");
    let root = load_grep_root(&*store, &backfill)
        .await
        .expect("load root")
        .expect("backfill root");
    assert!(matches!(
        root.manifest_state().status(),
        GrepIndexStatus::Backfilling { .. }
    ));
    writer.shutdown().await.expect("shutdown writer");
    drop(writer);
    drop(worker);
    drop(store);

    // Nothing has nudged either namespace in this process: the first search
    // is what re-admits the index that trails its head.
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeAndMaintain),
        AppOptions::default(),
    )
    .await
    .expect("reopen app");
    let stale_response = grep(&router, &stale, "stale steady needle").await;
    assert_eq!(stale_response.matches.len(), 1);
    settle(&server.writer).await;
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    assert_eq!(watermark(&store, &stale).await, ChangeSeq(2));

    let not_materialized = send(
        &router,
        Method::GET,
        &query_path_with_pattern(&backfill, "mid-backfill needle"),
        None,
    )
    .await;
    assert!(
        matches!(
            not_materialized.status(),
            StatusCode::OK | StatusCode::NOT_IMPLEMENTED
        ),
        "first touch either observes backfill or its concurrently completed root"
    );
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &backfill).await, ChangeSeq(3));
    let resumed = grep(&router, &backfill, "mid-backfill needle").await;
    assert_eq!(resumed.matches.len(), 3);
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn serve_only_answers_searches_over_an_index_it_refuses_to_administer() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "serve-only").await;
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeOnly),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(!maintains_grep_index(&server.writer));

    // The document says the same thing the routes do: searches yes,
    // administration no.
    let capabilities = capabilities(&router).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    assert!(capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0));
    assert!(
        !capabilities.features.contains_key(FEATURE_ADMIN_GREP_INDEX),
        "a deployment that refuses every index-admin route must not advertise that it \
         administers one"
    );

    // Every route that would mutate a grep root belongs where the index is
    // maintained, so this deployment refuses all three.
    for path in admin_grep_paths(&namespace_id) {
        let error =
            assert_not_supported(&router, Method::POST, &path, None, FEATURE_ADMIN_GREP_INDEX)
                .await;
        assert!(
            error.message.contains("does not maintain"),
            "{}",
            error.message
        );
    }
    // Reading the index's lifecycle is administering it: a deployment that
    // maintains nothing has no authority over the state it would report.
    let error = assert_not_supported(
        &router,
        Method::GET,
        &status_path(&namespace_id),
        None,
        FEATURE_ADMIN_GREP_INDEX,
    )
    .await;
    assert!(
        error.message.contains("does not maintain"),
        "{}",
        error.message
    );

    let worker = grep_worker(&store, "external-grep-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"external needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    settle(&server.writer).await;
    assert_eq!(
        lifecycle_of(&store, &namespace_id).await.active_watermark(),
        None,
        "a deployment that maintains nothing must leave the backfill where it was"
    );

    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    let response = grep(&router, &namespace_id, "external needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.built_through_seq, ChangeSeq(1));
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn maintain_only_keeps_the_index_built_without_serving_searches() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "maintain-only").await;
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::MaintainOnly),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(maintains_grep_index(&server.writer));

    let capabilities = capabilities(&router).await;
    assert!(
        !capabilities.features.contains_key(FEATURE_QUERY_GREP),
        "a deployment that answers no searches must not advertise that it does"
    );
    assert!(
        capabilities.supports(FEATURE_ADMIN_GREP_INDEX),
        "the index this deployment maintains is administered through these routes"
    );
    // The administration key is parented by the admin plane the runtime
    // already advertises, which is why a deployment that serves no searches
    // can still advertise it: a `query.` key here would name a plane this
    // document does not carry.
    assert!(
        !capabilities.profiles.iter().any(|p| p == PROFILE_QUERY_V0),
        "maintaining an index is not serving one"
    );
    let error = assert_not_supported(
        &router,
        Method::GET,
        &query_path(&namespace_id),
        None,
        FEATURE_QUERY_GREP,
    )
    .await;
    assert!(
        error.message.contains("does not serve grep queries"),
        "{}",
        error.message
    );

    // The index itself is this deployment's job: enabling it here admits the
    // backfill, and the runner carries it to the namespace's head.
    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"unserved needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    settle(&server.writer).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let root = load_grep_root(&*store, &namespace_id)
        .await
        .expect("load root")
        .expect("maintained root");
    assert!(matches!(
        root.manifest_state().status(),
        GrepIndexStatus::Active { .. }
    ));
    assert!(
        !root.manifest_state().segments().is_empty(),
        "the index this deployment maintains holds real segments"
    );
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn manual_maintenance_registers_no_index_job_and_still_administers_one() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "manual-maintenance").await;
    let (router, server) = app(
        ServerConfig {
            maintenance: MaintenanceMode::Manual,
            ..test_config(temp_dir.path(), GrepMode::ServeAndMaintain)
        },
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(
        !maintains_grep_index(&server.writer),
        "a manual deployment registers no automatic index job, whatever the grep mode maintains"
    );

    writer
        .put_file_bytes(
            &namespace_id,
            "/note.txt",
            b"unscheduled needle\n",
            PutFileOptions::new(loonfs_test_support::test_actor()),
        )
        .await
        .expect("write file");
    // Every index route still answers: manual maintenance withdraws the
    // scheduler, not the operator's reach.
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    assert!(
        matches!(
            index_status(&router, &namespace_id).await.lifecycle,
            GrepIndexLifecycle::Backfilling { .. }
        ),
        "the enable published a backfill this deployment left for someone else"
    );

    settle(&server.writer).await;
    assert_eq!(
        lifecycle_of(&store, &namespace_id).await.active_watermark(),
        None,
        "nothing here schedules the backfill it published"
    );

    // And an assigned host — or an operator — carries it the rest of the way.
    let worker = grep_worker(&store, "assigned-grep-host").await;
    drive_worker_to_current(&worker, &namespace_id, GramIndexBuildPolicy::default()).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    assert_eq!(
        grep(&router, &namespace_id, "unscheduled needle")
            .await
            .matches
            .len(),
        1
    );
    server
        .writer
        .shutdown()
        .await
        .expect("settle the server writer");
}

async fn seed_namespace(root: &Path, name: &str) -> (SharedObjectStore, FsWriter, NamespaceId) {
    let store = Arc::new(LocalFsStore::new(root).expect("store")) as SharedObjectStore;
    let writer = FsWriter::builder_with_store(store.clone())
        .writer_id(format!("grep-mode-seed-{name}"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .expect("create namespace");
    (store, writer, namespace_id)
}

fn test_config(store_root: &Path, mode: GrepMode) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1:0".to_owned(),
        auth_token: Some("test-token".into()),
        content_token_secret: "test-content-token-secret".into(),
        writer_id: format!("grep-mode-{mode:?}"),
        runtime_cache: RuntimeCacheConfigOverrides::default(),
        read_consistency: ReadConsistencyConfig::default(),
        local_cache: None,
        grep: GrepConfig {
            mode,
            ..GrepConfig::default()
        },
        maintenance: MaintenanceMode::Automatic,
        min_publish_interval_ms: 0,
        request_deadline_ms: 60_000,
        shutdown_deadline_ms: 600_000,
        max_upload_bytes: 1024 * 1024,
        max_download_bytes: 1024 * 1024,
        snapshot_max_ttl_ms: 86_400_000,
        snapshot_max_lifetime_ms: 604_800_000,
        snapshot_max_live_per_namespace: 16,
        max_concurrent_uploads: 2,
        max_concurrent_downloads: 2,
        max_concurrent_maintenance: 2,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: None,
        },
    }
}

/// Waits for every maintenance step this deployment admitted to settle, so
/// the durable state read next is the state those steps left.
///
/// A drain, not a per-namespace wait: the runner admits work per
/// `{job, namespace}` key and reports progress durably, so what this waits
/// for is quiet and what it then reads is durable state.
async fn settle(server: &FsWriter) {
    server.flush_background().await.expect("settle maintenance");
}

/// Whether this deployment maintains the grep index, asked where the answer
/// lives: the index job is registered on the server's writer, or it is not.
fn maintains_grep_index(server: &FsWriter) -> bool {
    server.maintenance_job(GREP_INDEX_JOB).is_some()
}

/// The sequence this namespace's index is built through.
async fn watermark(store: &SharedObjectStore, namespace_id: &NamespaceId) -> ChangeSeq {
    lifecycle_of(store, namespace_id)
        .await
        .active_watermark()
        .expect("an active grep root has a watermark")
        .built_through_seq()
}

/// This namespace's durable grep lifecycle, read where an operator reads it.
async fn lifecycle_of(store: &SharedObjectStore, namespace_id: &NamespaceId) -> GrepIndexStatus {
    load_grep_root(&**store, namespace_id)
        .await
        .expect("load grep root")
        .expect("an enabled namespace has a grep root")
        .manifest_state()
        .status()
        .clone()
}

/// This deployment's capability document, checked for well-formedness on
/// the way past: every advertised feature key has to be parented by an
/// advertised profile, and each grep mode advertises a different set.
async fn capabilities(router: &Router) -> CapabilityDocument {
    let document: CapabilityDocument =
        response_json(send(router, Method::GET, "/v0/capabilities", None).await).await;
    document
        .validate()
        .expect("the advertised document must be well-formed");
    document
}

/// Asserts one route answers `not_supported`, naming the capability key a
/// client gates on, and hands the error back for any further check.
async fn assert_not_supported(
    router: &Router,
    method: Method,
    path: &str,
    body: Option<Vec<u8>>,
    feature: &str,
) -> ApiError {
    let response = send(router, method, path, body).await;
    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
    let error: ApiError = response_json(response).await;
    assert_eq!(error.code, "not_supported", "{path}");
    assert_eq!(error.feature.as_deref(), Some(feature), "{path}");
    error
}

fn query_path(namespace_id: &NamespaceId) -> String {
    format!("/v0/namespaces/{namespace_id}/grep")
}

fn query_path_with_pattern(namespace_id: &NamespaceId, pattern: &str) -> String {
    format!(
        "{}?pattern={}",
        query_path(namespace_id),
        pattern.replace(' ', "%20")
    )
}

fn admin_grep_paths(namespace_id: &NamespaceId) -> Vec<String> {
    ["enable", "disable", "gc"]
        .into_iter()
        .map(|action| format!("/v0/admin/namespaces/{namespace_id}/grep/index/{action}"))
        .collect()
}

fn status_path(namespace_id: &NamespaceId) -> String {
    format!("/v0/admin/namespaces/{namespace_id}/grep/index")
}

async fn index_status(router: &Router, namespace_id: &NamespaceId) -> GrepIndex {
    let response = send(router, Method::GET, &status_path(namespace_id), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn enable_grep(router: &Router, namespace_id: &NamespaceId) -> StatusCode {
    send(
        router,
        Method::POST,
        &format!("/v0/admin/namespaces/{namespace_id}/grep/index/enable"),
        None,
    )
    .await
    .status()
}

async fn disable_grep(router: &Router, namespace_id: &NamespaceId) -> GrepIndex {
    let response = send(
        router,
        Method::POST,
        &format!("/v0/admin/namespaces/{namespace_id}/grep/index/disable"),
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn grep(router: &Router, namespace_id: &NamespaceId, pattern: &str) -> GrepResponse {
    response_json(
        send(
            router,
            Method::GET,
            &query_path_with_pattern(namespace_id, pattern),
            None,
        )
        .await,
    )
    .await
}

async fn drive_worker_to_current(
    worker: &GrepWorker<SharedObjectStore>,
    namespace_id: &NamespaceId,
    policy: GramIndexBuildPolicy,
) {
    for _ in 0..64 {
        let build = worker
            .build_step(namespace_id, policy)
            .await
            .expect("build step");
        let fold = worker
            .reorganize_step(namespace_id, policy)
            .await
            .expect("fold step");
        if matches!(build, GrepBuildOutcome::UpToDate { .. })
            && matches!(fold, loonfs_grep::GrepReorganizeOutcome::NotNeeded { .. })
        {
            return;
        }
    }
    panic!("grep worker did not catch up");
}

async fn send(
    router: &Router,
    method: Method,
    uri: &str,
    body: Option<Vec<u8>>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", "Bearer test-token");
    if body.is_some() {
        request = request.header("content-type", "application/json");
    }
    router
        .clone()
        .oneshot(
            request
                .body(body.map_or_else(Body::empty, Body::from))
                .expect("request"),
        )
        .await
        .expect("route request")
}

async fn response_json<T: DeserializeOwned>(response: axum::response::Response) -> T {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    serde_json::from_slice(&bytes).expect("decode response JSON")
}

const API_SPEC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/specs/api.md");

/// A grep worker over the same handles the server composes.
async fn grep_worker(store: &SharedObjectStore, actor: &str) -> GrepWorker<SharedObjectStore> {
    let reader = FsReader::builder_with_store(store.clone())
        .build()
        .await
        .expect("build reader");
    let admin = FsAdmin::builder_with_store(store.clone())
        .actor_id(actor)
        .build()
        .await
        .expect("build admin");
    GrepWorker::new(store.clone(), reader, admin)
}

/// The api.md section 2.1 example describes a reference deployment: the
/// runtime's core and admin planes plus the query plane the server composes
/// from `loonfs-grep`. `loonfs`'s `capability_conformance` pins the
/// runtime's half; this pins the merged document a served deployment
/// answers with. A deployment adds its own limits on top, so the example is
/// a subset rather than an equality.
fn assert_served_document_covers_the_spec_example(served: &CapabilityDocument) {
    let spec = std::fs::read_to_string(API_SPEC_PATH).expect("read docs/specs/api.md");
    let example = spec
        .split("### 2.1")
        .nth(1)
        .expect("api.md section 2.1")
        .split("### 2.2")
        .next()
        .expect("section end")
        .split("```json")
        .nth(1)
        .expect("capability example block")
        .split("```")
        .next()
        .expect("fenced block end");
    let expected: CapabilityDocument =
        serde_json::from_str(example).expect("spec capability example parses");
    let served_features = served.features.clone();

    served.validate().expect("served document is well-formed");
    assert_eq!(served.protocol_version, expected.protocol_version);
    assert_eq!(
        served.profiles, expected.profiles,
        "the served profiles drifted from the api.md section 2.1 example"
    );
    assert_eq!(
        served_features, expected.features,
        "the served features drifted from the api.md section 2.1 example"
    );
    for (limit, value) in &expected.limits {
        assert_eq!(
            served.limits.get(limit),
            Some(value),
            "the served `{limit}` limit drifted from the api.md section 2.1 example"
        );
    }
}

fn grep_limits() -> [&'static str; 4] {
    [
        LIMIT_QUERY_GREP_DEFAULT,
        LIMIT_QUERY_GREP_MAX,
        LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
        LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
    ]
}
