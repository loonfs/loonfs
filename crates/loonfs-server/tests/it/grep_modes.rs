#![allow(clippy::panic)]
//! In-process HTTP matrix for the four things a deployment can do about
//! grep: answer searches, keep the index built, both, or neither.

use axum::body::{to_bytes, Body};
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use loonfs::{LoonFs, Writable};
use loonfs_grep::manifest::{load_current_grep_manifest, GrepIndexStatus};
use loonfs_grep::{GramIndexBuildPolicy, GrepBuildOutcome, GrepWorker};
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::SharedObjectStore;
use loonfs_server::{
    app, AppOptions, GrepConfig, GrepMode, MaintenanceMode, MetadataCacheOverrides, ServerConfig,
    StoreConfig,
};
use loonfs_types::api::v0::{GrepIndex, GrepIndexLifecycle};
use loonfs_types::{
    ApiError, CapabilityDocument, ChangeSeq, GrepResponse, NamespaceId, RunMaintenanceResponse,
    API_GROUP_QUERY_V0, FEATURE_MAINTENANCE_GREP_INDEX, FEATURE_QUERY_GREP,
    LIMIT_QUERY_GREP_DEFAULT, LIMIT_QUERY_GREP_MAX, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES,
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
    assert!(!capabilities
        .features
        .contains_key(FEATURE_MAINTENANCE_GREP_INDEX));
    assert!(
        !capabilities
            .api_groups
            .iter()
            .any(|api_group| api_group == API_GROUP_QUERY_V0),
        "a deployment that answers `not_supported` on every query route must not advertise \
         the API group"
    );
    for limit in grep_limits() {
        assert!(!capabilities.limits.contains_key(limit));
    }
    assert!(!maintains_grep_index(&server));

    // Searching and maintaining gate on their own keys, so a refusal names
    // the key whose absence the client just read.
    assert_not_supported(
        &router,
        Method::GET,
        &query_path(&namespace_id),
        None,
        FEATURE_QUERY_GREP,
    )
    .await;
    for path in maintenance_grep_paths(&namespace_id) {
        assert_not_supported(
            &router,
            Method::POST,
            &path,
            None,
            FEATURE_MAINTENANCE_GREP_INDEX,
        )
        .await;
    }
    assert_not_supported(
        &router,
        Method::GET,
        &status_path(&namespace_id),
        None,
        FEATURE_MAINTENANCE_GREP_INDEX,
    )
    .await;
    server
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn grep_gc_requires_grep_maintenance() {
    for mode in [GrepMode::Disabled, GrepMode::ServeOnly] {
        let temp_dir = tempdir().expect("store tempdir");
        let (router, server) = app(test_config(temp_dir.path(), mode), AppOptions::default())
            .await
            .expect("build app");
        assert_not_supported(
            &router,
            Method::POST,
            "/v0/maintenance/namespaces/demo/runs",
            Some(br#"{"kind":"grep_gc"}"#.to_vec()),
            FEATURE_MAINTENANCE_GREP_INDEX,
        )
        .await;
        server
            .runtime
            .shutdown()
            .await
            .expect("settle the server writer");
    }
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
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn serving_and_maintaining_enables_queries_and_disables_per_namespace() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "both").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeAndMaintain),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(maintains_grep_index(&server));
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
            &format!("/v0/maintenance/namespaces/{namespace_id}/grep/index/enable"),
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
                captured_seq: ChangeSeq(0),
                cursor_inode_id: None,
                ..
            }
        ),
        "{:?}",
        enabled.lifecycle
    );
    sweep(&server).await;
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

    // Re-enabling an active manifest reports the phase it found, still tagged.
    let again: GrepIndex = response_json(
        send(
            &router,
            Method::POST,
            &format!("/v0/maintenance/namespaces/{namespace_id}/grep/index/enable"),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(again.lifecycle, active.lifecycle);

    // The file lands through a writer of its own, so this server holds no
    // session that saw the publish: the index stays where it was until the
    // next sweep pass.
    namespace
        .put_file(
            "/note.txt",
            b"automatic needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(0));

    let capabilities = capabilities(&router).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    assert!(
        capabilities.supports(FEATURE_MAINTENANCE_GREP_INDEX),
        "a deployment doing both jobs advertises both keys"
    );
    assert!(capabilities
        .api_groups
        .iter()
        .any(|api_group| api_group == API_GROUP_QUERY_V0));
    for limit in grep_limits() {
        assert!(capabilities.limits.contains_key(limit));
    }
    assert_served_document_covers_the_spec_example(&capabilities);

    // A search over a namespace whose index trails answers from the
    // exhaustive tail, and the next sweep pass catches the index up.
    let response = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(response.matches.len(), 1);
    assert_eq!(response.matches[0].path, "/note.txt");
    sweep(&server).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let caught_up = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(caught_up.matches.len(), 1);
    assert_eq!(caught_up.built_through_seq, caught_up.head_seq);

    // Disabling is one durable compare-and-swap; the sweep reads it.
    let disabled_response = disable_grep(&router, &namespace_id).await;
    assert_eq!(disabled_response.lifecycle, GrepIndexLifecycle::Disabled);
    assert!(!disabled_response.reorganize_pending);
    let disabled = load_current_grep_manifest(&*store, &namespace_id, observation())
        .await
        .expect("load disabled manifest")
        .expect("disabled manifest");
    assert!(matches!(
        disabled.manifest_state().status(),
        GrepIndexStatus::Disabled {}
    ));
    sweep(&server).await;
    assert!(
        matches!(
            load_current_grep_manifest(&*store, &namespace_id, observation())
                .await
                .expect("reload disabled manifest")
                .expect("disabled manifest")
                .manifest_state()
                .status(),
            GrepIndexStatus::Disabled {}
        ),
        "no step may resurrect a manifest the operator disabled"
    );

    let response = send(
        &router,
        Method::POST,
        &format!("/v0/maintenance/namespaces/{namespace_id}/runs"),
        Some(br#"{"kind":"grep_gc"}"#.to_vec()),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let gc: RunMaintenanceResponse = response_json(response).await;
    let RunMaintenanceResponse::GrepGc {
        namespace_id: actual_namespace_id,
        deleted_segments,
        deleted_other_objects,
        namespace_reaped,
        retained_candidates,
    } = gc
    else {
        panic!("expected grep collection, got {gc:?}");
    };
    assert_eq!(actual_namespace_id, namespace_id);
    assert_eq!(deleted_segments, 0);
    assert_eq!(deleted_other_objects, 0);
    assert!(!namespace_reaped);
    assert!(retained_candidates > 0);

    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    sweep(&server).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let reenabled = grep(&router, &namespace_id, "automatic needle").await;
    assert_eq!(reenabled.matches.len(), 1);
    server
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn one_sweep_after_restart_resumes_stale_and_mid_backfill_namespaces() {
    let temp_dir = tempdir().expect("store tempdir");
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id("restart-seed")
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let stale = NamespaceId::parse("restart-stale").expect("namespace id");
    let backfill = NamespaceId::parse("restart-backfill").expect("namespace id");
    for namespace_id in [&stale, &backfill] {
        writer
            .create_namespace(namespace_id, &loonfs_test_support::test_actor())
            .await
            .expect("create namespace");
    }
    let stale_writer = writer.open_namespace(&stale).expect("open namespace");
    let backfill_writer = writer.open_namespace(&backfill).expect("open namespace");
    stale_writer
        .put_file(
            "/indexed.txt",
            b"indexed before restart\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write indexed file");
    for index in 0..3 {
        backfill_writer
            .put_file(
                &format!("/backfill-{index}.txt"),
                format!("mid-backfill needle {index}\n").as_bytes(),
                &loonfs_test_support::test_actor(),
            )
            .await
            .expect("write backfill file");
    }

    let worker = grep_worker(&store, "restart-worker").await;
    worker.enable(&stale).await.expect("enable stale namespace");
    drive_worker_to_current(&worker, &stale, GramIndexBuildPolicy::default()).await;
    stale_writer
        .put_file(
            "/tail.txt",
            b"stale steady needle\n",
            &loonfs_test_support::test_actor(),
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
        .expect("leave mid-backfill manifest");
    let manifest = load_current_grep_manifest(&*store, &backfill, observation())
        .await
        .expect("load manifest")
        .expect("backfill manifest");
    assert!(matches!(
        manifest.manifest_state().status(),
        GrepIndexStatus::Backfilling { .. }
    ));
    writer.shutdown().await.expect("shutdown writer");
    drop(writer);
    drop(worker);
    drop(store);

    // Nothing in this process has touched either namespace: one sweep pass
    // carries both indexes to their heads.
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeAndMaintain),
        AppOptions::default(),
    )
    .await
    .expect("reopen app");
    let stale_response = grep(&router, &stale, "stale steady needle").await;
    assert_eq!(stale_response.matches.len(), 1);
    let not_materialized = send(
        &router,
        Method::GET,
        &query_path_with_pattern(&backfill, "mid-backfill needle"),
        None,
    )
    .await;
    assert_eq!(not_materialized.status(), StatusCode::NOT_IMPLEMENTED);

    sweep(&server).await;
    let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedObjectStore;
    assert_eq!(watermark(&store, &stale).await, ChangeSeq(2));
    assert_eq!(watermark(&store, &backfill).await, ChangeSeq(3));
    let resumed = grep(&router, &backfill, "mid-backfill needle").await;
    assert_eq!(resumed.matches.len(), 3);
    server
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn serve_only_answers_searches_over_an_index_it_refuses_to_maintain() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "serve-only").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::ServeOnly),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(!maintains_grep_index(&server));

    // The document says the same thing the routes do: searches yes,
    // maintenance no.
    let capabilities = capabilities(&router).await;
    assert!(capabilities.supports(FEATURE_QUERY_GREP));
    assert!(capabilities
        .api_groups
        .iter()
        .any(|api_group| api_group == API_GROUP_QUERY_V0));
    assert!(
        !capabilities
            .features
            .contains_key(FEATURE_MAINTENANCE_GREP_INDEX),
        "a deployment that refuses every index-maintenance route must not advertise that it \
         maintains one"
    );

    // Every route that would mutate a grep manifest belongs where the index is
    // maintained, so this deployment refuses all three.
    for path in maintenance_grep_paths(&namespace_id) {
        let error = assert_not_supported(
            &router,
            Method::POST,
            &path,
            None,
            FEATURE_MAINTENANCE_GREP_INDEX,
        )
        .await;
        assert!(
            error.message.contains("does not maintain"),
            "{}",
            error.message
        );
    }
    // Reading the index's lifecycle is maintaining it: a deployment that
    // maintains nothing has no authority over the state it would report.
    let error = assert_not_supported(
        &router,
        Method::GET,
        &status_path(&namespace_id),
        None,
        FEATURE_MAINTENANCE_GREP_INDEX,
    )
    .await;
    assert!(
        error.message.contains("does not maintain"),
        "{}",
        error.message
    );

    let worker = grep_worker(&store, "external-grep-worker").await;
    worker.enable(&namespace_id).await.expect("enable grep");
    namespace
        .put_file(
            "/note.txt",
            b"external needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    sweep(&server).await;
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
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn maintain_only_keeps_the_index_built_without_serving_searches() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "maintain-only").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let (router, server) = app(
        test_config(temp_dir.path(), GrepMode::MaintainOnly),
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(maintains_grep_index(&server));

    let capabilities = capabilities(&router).await;
    assert!(
        !capabilities.features.contains_key(FEATURE_QUERY_GREP),
        "a deployment that answers no searches must not advertise that it does"
    );
    assert!(
        capabilities.supports(FEATURE_MAINTENANCE_GREP_INDEX),
        "the index this deployment maintains is maintained through these routes"
    );
    // The maintenance key is parented by the maintenance API group the runtime
    // already advertises, which is why a deployment that serves no searches
    // can still advertise it: a `query.` key here would name an API group this
    // document does not carry.
    assert!(
        !capabilities
            .api_groups
            .iter()
            .any(|api_group| api_group == API_GROUP_QUERY_V0),
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

    // The index itself is this deployment's job: enabling it here publishes
    // the backfill, and the next sweep pass carries it to the namespace's
    // head.
    namespace
        .put_file(
            "/note.txt",
            b"unserved needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    sweep(&server).await;
    assert_eq!(watermark(&store, &namespace_id).await, ChangeSeq(1));
    let manifest = load_current_grep_manifest(&*store, &namespace_id, observation())
        .await
        .expect("load manifest")
        .expect("maintained manifest");
    assert!(matches!(
        manifest.manifest_state().status(),
        GrepIndexStatus::Active { .. }
    ));
    assert!(
        !manifest.manifest_state().segments().is_empty(),
        "the index this deployment maintains holds real segments"
    );
    server
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

#[tokio::test]
async fn serve_only_maintenance_serves_the_index_routes_without_a_sweep() {
    let temp_dir = tempdir().expect("store tempdir");
    let (store, writer, namespace_id) = seed_namespace(temp_dir.path(), "manual-maintenance").await;
    let namespace = writer
        .open_namespace(&namespace_id)
        .expect("open namespace");
    let (router, server) = app(
        ServerConfig {
            maintenance: MaintenanceMode::ServeOnly,
            ..test_config(temp_dir.path(), GrepMode::ServeAndMaintain)
        },
        AppOptions::default(),
    )
    .await
    .expect("build app");
    assert!(
        maintains_grep_index(&server),
        "a serve-only deployment still serves the index routes"
    );
    assert!(server.sweep.is_none(), "serve-only mode builds no sweep");

    namespace
        .put_file(
            "/note.txt",
            b"unscheduled needle\n",
            &loonfs_test_support::test_actor(),
        )
        .await
        .expect("write file");
    // Every index route still answers: serve-only maintenance withdraws the
    // sweep, not the operator's reach.
    assert_eq!(enable_grep(&router, &namespace_id).await, StatusCode::OK);
    assert!(
        matches!(
            index_status(&router, &namespace_id).await.lifecycle,
            GrepIndexLifecycle::Backfilling { .. }
        ),
        "the enable published a backfill this deployment left for someone else"
    );

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
        .runtime
        .shutdown()
        .await
        .expect("settle the server writer");
}

async fn seed_namespace(
    root: &Path,
    name: &str,
) -> (SharedObjectStore, LoonFs<Writable>, NamespaceId) {
    let store = Arc::new(LocalFsStore::new(root).expect("store")) as SharedObjectStore;
    let writer = LoonFs::builder_with_store(store.clone())
        .writer_id(format!("grep-mode-seed-{name}"))
        .min_publish_interval_ms(0)
        .build()
        .await
        .expect("writer");
    let namespace_id = NamespaceId::parse(name).expect("namespace id");
    writer
        .create_namespace(&namespace_id, &loonfs_test_support::test_actor())
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
        max_concurrent_folds: loonfs::DEFAULT_MAX_CONCURRENT_FOLDS,
        max_concurrent_compactions: loonfs::DEFAULT_MAX_CONCURRENT_COMPACTIONS,
        publication: Default::default(),
        inline_content: Default::default(),
        metadata_cache: MetadataCacheOverrides::default(),
        local_cache: None,
        grep: GrepConfig {
            mode,
            ..GrepConfig::default()
        },
        maintenance: MaintenanceMode::ServeAndMaintain,
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
        maintenance_interval_ms: 300_000,
        gc_interval_ms: 3_600_000,
        full_sweep_interval_ms: 86_400_000,
        idle_session_close_after_ms: 1_800_000,
        max_merge_input_bytes: loonfs_types::format::sst_blocks::DEFAULT_MAX_COMPACTION_INPUT_BYTES,
        manifest_revalidation_interval_ms: None,
        max_block_memo_bytes: None,
        idle_fold_after_ms: loonfs::MetadataMaintenanceOptions::default().idle_fold_after_ms,
        allow_unauthenticated_remote: false,
        allow_remote_without_tls: false,
        tls: None,
        store: StoreConfig::LocalFs {
            root: store_root.display().to_string(),
            key_prefix: None,
        },
    }
}

/// Runs one maintenance sweep pass, so the durable state read next is the
/// state that pass left.
async fn sweep(server: &loonfs_server::AppState) {
    server
        .sweep
        .as_ref()
        .expect("a maintaining server has a sweep")
        .run_pass(false)
        .await
        .expect("list the namespaces");
}

/// Whether this deployment serves the index-maintenance routes.
fn maintains_grep_index(server: &loonfs_server::AppState) -> bool {
    server.binding.options.maintains_grep_index
}

/// The sequence this namespace's index is built through.
async fn watermark(store: &SharedObjectStore, namespace_id: &NamespaceId) -> ChangeSeq {
    lifecycle_of(store, namespace_id)
        .await
        .active_watermark()
        .expect("an active grep manifest has a watermark")
        .built_through_seq()
}

/// This namespace's durable grep lifecycle, read where an operator reads it.
async fn lifecycle_of(store: &SharedObjectStore, namespace_id: &NamespaceId) -> GrepIndexStatus {
    load_current_grep_manifest(&**store, namespace_id, observation())
        .await
        .expect("load grep manifest")
        .expect("an enabled namespace has a grep manifest")
        .manifest_state()
        .status()
        .clone()
}

/// This deployment's capability document, checked for well-formedness on
/// the way past: every advertised feature key has to be parented by an
/// advertised API group, and each grep mode advertises a different set.
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

fn maintenance_grep_paths(namespace_id: &NamespaceId) -> Vec<String> {
    ["enable", "disable"]
        .into_iter()
        .map(|action| format!("/v0/maintenance/namespaces/{namespace_id}/grep/index/{action}"))
        .collect()
}

fn status_path(namespace_id: &NamespaceId) -> String {
    format!("/v0/maintenance/namespaces/{namespace_id}/grep/index")
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
        &format!("/v0/maintenance/namespaces/{namespace_id}/grep/index/enable"),
        None,
    )
    .await
    .status()
}

async fn disable_grep(router: &Router, namespace_id: &NamespaceId) -> GrepIndex {
    let response = send(
        router,
        Method::POST,
        &format!("/v0/maintenance/namespaces/{namespace_id}/grep/index/disable"),
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
    let reader = LoonFs::builder_with_store(store.clone())
        .read_only()
        .build()
        .await
        .expect("build reader");
    let maintenance = LoonFs::builder_with_store(store.clone())
        .writer_id(actor)
        .build()
        .await
        .expect("build maintenance")
        .maintenance(loonfs_test_support::ids::writer_id(actor));
    GrepWorker::new(
        store.clone(),
        reader,
        maintenance,
        loonfs_grep::GrepStepBudget::default(),
    )
}

/// The api.md section 2.1 example describes a reference deployment: the
/// runtime's filesystem and maintenance API groups plus the query API group the server composes
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
        served.api_groups, expected.api_groups,
        "the served API groups drifted from the api.md section 2.1 example"
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

fn observation() -> loonfs::engine::Observation {
    loonfs::engine::Observation::now(Arc::new(
        loonfs_objectstore::timing::StdMonotonicTimer::default(),
    ))
}
