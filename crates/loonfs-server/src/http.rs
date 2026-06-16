use crate::config::{ServerConfig, ServerConfigError, StoreConfig};
use crate::publisher::PublisherRegistry;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use loonfs::publish::PathMutationIntent;
use loonfs::{
    payload_class, BootstrapNamespaceError, ChangeSeq, CoreError, CreateNamespaceOptions,
    DeleteNamespaceOptions, ErrorCode, Fs, JsonlObjectStoreMetricsRecorder,
    ObjectStoreMetricsRecorder, PutFileBehavior, RuntimeError, SharedObjectStore, TraceMode,
    TraceStoreKind,
};
use loonfs_api::{
    v0::{
        BeginUploadRequest, BeginUploadResponse, ChangesResponse,
        CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, DirectPutUpload, ObjectTransferAccess,
        UploadContentResponse, UploadMode,
    },
    AdvanceRetentionResponse, ApiError, CreateCheckpointResponse, CreateNamespaceRequest,
    FilesystemOperation, FilesystemOperationRequest, FilesystemOperationResponse,
    FilesystemPutBehavior, ForkNamespaceRequest, InodeId, ListFileRevisionsResponse,
    ListNamespacesResponse, NamespaceId, NamespaceIdValidationError, RestoreFileRevisionRequest,
    RevisionNo, FEATURE_UPLOADS_DIRECT_PUT,
};
use loonfs_objectstore::{
    presign::{
        ObjectTransferIssuer, PresignedPutRequest, S3CompatiblePresigner, S3PresignerConfig,
    },
    ObjectStoreError,
};
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::Instrument;

type SharedStore = SharedObjectStore;
const OBJECT_STORE_METRICS_JSONL_ENV: &str = "LOONFS_OBJECT_STORE_METRICS_JSONL";
const DIRECT_PUT_URL_TTL: Duration = Duration::from_secs(15 * 60);

#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    fs: Arc<Fs>,
    publisher: PublisherRegistry,
    transfer_issuer: Option<Arc<dyn ObjectTransferIssuer>>,
}

#[derive(Debug, serde::Deserialize)]
struct PathQuery {
    path: String,
}

#[derive(Debug, serde::Deserialize)]
struct ContentQuery {
    path: String,
    revision_no: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ChangesQuery {
    after_seq: u64,
}

#[derive(Debug, serde::Deserialize)]
struct DeleteNamespaceQuery {
    /// Delete only if the head is still at this sequence (`stale_head` on
    /// mismatch).
    expected_head_seq: Option<u64>,
}

pub fn app(config: ServerConfig) -> Result<Router, ServerConfigError> {
    let store = Arc::new(config.object_store()?) as SharedStore;
    app_with_store(config, store)
}

fn app_with_store(config: ServerConfig, store: SharedStore) -> Result<Router, ServerConfigError> {
    let fs = Arc::new(build_fs(&config, store)?);
    Ok(app_with_fs(config, fs))
}

fn app_with_fs(config: ServerConfig, fs: Arc<Fs>) -> Router {
    let config = Arc::new(config);
    let publisher = PublisherRegistry::new(fs.clone());
    let transfer_issuer = presigned_transfer_issuer(config.as_ref());
    let state = AppState {
        config,
        fs,
        publisher,
        transfer_issuer,
    };
    Router::new()
        .route("/healthz", get(healthz))
        .route("/v0/config", get(config_handler))
        .route(
            "/v0/namespaces",
            post(create_namespace).get(list_namespaces_handler),
        )
        .route(
            "/v0/namespaces/:namespace",
            get(namespace_status_handler).delete(delete_namespace_handler),
        )
        .route(
            "/v0/namespaces/:namespace/forks",
            post(fork_namespace_handler),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/list",
            get(list_entries),
        )
        .route("/v0/namespaces/:namespace/filesystem/stat", get(stat_entry))
        .route(
            "/v0/namespaces/:namespace/filesystem/content",
            get(get_content),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/revisions",
            get(list_path_revisions),
        )
        .route(
            "/v0/namespaces/:namespace/filesystem/operations",
            post(filesystem_operation),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions",
            get(list_inode_revisions),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:revision_no/content",
            get(get_inode_revision_content),
        )
        .route(
            "/v0/namespaces/:namespace/inodes/:inode_id/revisions/:source_revision_no/restore",
            post(restore_inode_revision),
        )
        .route(
            "/v0/namespaces/:namespace/uploads",
            post(begin_upload_handler),
        )
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/content",
            put(upload_content_handler),
        )
        .route(
            "/v0/namespaces/:namespace/uploads/:upload_id/complete",
            post(complete_upload_handler),
        )
        .route(
            "/v0/namespaces/:namespace/commits",
            post(commit_operations_handler),
        )
        .route(
            "/v0/namespaces/:namespace/changes",
            get(list_changes_handler),
        )
        .route(
            "/v0/admin/namespaces/:namespace/checkpoint",
            post(create_checkpoint_handler),
        )
        .route(
            "/v0/admin/namespaces/:namespace/retention/advance",
            post(advance_retention_handler),
        )
        .with_state(state)
}

fn build_fs(config: &ServerConfig, store: SharedStore) -> Result<Fs, ServerConfigError> {
    build_fs_with_metrics_jsonl_path(
        config,
        store,
        std::env::var_os(OBJECT_STORE_METRICS_JSONL_ENV),
    )
}

fn build_fs_with_metrics_jsonl_path(
    config: &ServerConfig,
    store: SharedStore,
    metrics_jsonl_path: Option<OsString>,
) -> Result<Fs, ServerConfigError> {
    let trace_store_kind = trace_store_kind(&config.store);
    let mut builder = Fs::builder(store)
        .writer_id(config.writer_id.clone())
        .writer_version(config.writer_version.clone())
        .lease_duration_ms(config.lease_duration_ms)
        .runtime_cache(config.runtime_cache_config())
        .trace_mode(TraceMode::Remote)
        .trace_store_kind(trace_store_kind);

    if let Some(recorder) = object_store_metrics_recorder(metrics_jsonl_path)? {
        builder = builder.with_metrics_recorder(recorder);
    }

    builder
        .build()
        .map_err(|error| ServerConfigError::InvalidField {
            field: "runtime",
            reason: error.to_string(),
        })
}

fn object_store_metrics_recorder(
    metrics_jsonl_path: Option<OsString>,
) -> Result<Option<Arc<dyn ObjectStoreMetricsRecorder>>, ServerConfigError> {
    let Some(path) = metrics_jsonl_path else {
        return Ok(None);
    };
    if path.is_empty() {
        return Ok(None);
    }
    let path = std::path::PathBuf::from(path);
    JsonlObjectStoreMetricsRecorder::create(&path)
        .map(|recorder| Some(Arc::new(recorder) as Arc<dyn ObjectStoreMetricsRecorder>))
        .map_err(|error| ServerConfigError::InvalidField {
            field: OBJECT_STORE_METRICS_JSONL_ENV,
            reason: error.to_string(),
        })
}

fn trace_store_kind(store: &StoreConfig) -> TraceStoreKind {
    match store {
        StoreConfig::LocalFs { .. } => TraceStoreKind::LocalFs,
        StoreConfig::AwsS3 { .. } => TraceStoreKind::S3,
        StoreConfig::CloudflareR2 { .. } => TraceStoreKind::R2,
        StoreConfig::GcpGcs { .. } => TraceStoreKind::Gcs,
        StoreConfig::AzureAbs { .. } => TraceStoreKind::Abs,
    }
}

pub async fn serve(config: ServerConfig) -> Result<(), String> {
    let bind: SocketAddr = config
        .bind
        .parse()
        .map_err(|err: std::net::AddrParseError| err.to_string())?;
    let app = app(config).map_err(|err| err.to_string())?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|err| err.to_string())?;
    axum::serve(listener, app)
        .await
        .map_err(|err| err.to_string())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn config_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::CapabilityDocument>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let mut capabilities = state.fs.capabilities();
    capabilities.features.insert(
        FEATURE_UPLOADS_DIRECT_PUT.to_owned(),
        state.transfer_issuer.is_some(),
    );
    Ok(Json(capabilities))
}

async fn delete_namespace_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    Query(query): Query<DeleteNamespaceQuery>,
    headers: HeaderMap,
) -> Result<Json<loonfs::DeleteNamespaceResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let options = DeleteNamespaceOptions {
        expected_head_seq: query.expected_head_seq.map(ChangeSeq),
    };
    let response = state
        .publisher
        .submit_delete(namespace_id.clone(), options)
        .await
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn create_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(request.namespace_id)?;
    let summary = state
        .fs
        .create_namespace(&namespace_id, CreateNamespaceOptions::default())
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(summary))
}

async fn list_namespaces_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ListNamespacesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespaces = state
        .fs
        .list_namespaces()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(ListNamespacesResponse { namespaces }))
}

async fn fork_namespace_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<ForkNamespaceRequest>,
) -> Result<Json<loonfs_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let source_namespace_id = parse_namespace_id(namespace)?;
    let new_namespace_id = parse_namespace_id(request.new_namespace_id)?;
    let summary = state
        .fs
        .fork_namespace(&source_namespace_id, &new_namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&source_namespace_id, error))?;
    Ok(Json(summary))
}

async fn list_entries(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<loonfs_api::ListPathEntriesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let listing = state
        .fs
        .list_path_entries(&namespace_id, &path)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(listing))
}

async fn namespace_status_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<loonfs_api::NamespaceStatusResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let status = state
        .fs
        .namespace_status(&namespace_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(loonfs_api::NamespaceStatusResponse {
        namespace_id: status.namespace_id,
        head_seq: status.head_seq,
        current_manifest_id: status.current_manifest_id,
        latest_checkpoint_id: status.latest_checkpoint_id,
        wal_tail_segments: status.wal_tail_segments,
        retention_floor_seq: status.retention_floor_seq,
    }))
}

async fn stat_entry(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<loonfs_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let entry = state
        .fs
        .stat_path(&namespace_id, &path)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(entry))
}

async fn get_content(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let revision_no = query
        .revision_no
        .as_deref()
        .map(parse_revision_no)
        .transpose()?;
    let file = match revision_no {
        Some(revision_no) => {
            state
                .fs
                .read_file_revision_bytes(&namespace_id, &path, revision_no)
                .await
        }
        None => state.fs.read_file_bytes(&namespace_id, &path).await,
    }
    .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok((StatusCode::OK, file.bytes).into_response())
}

async fn list_path_revisions(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let response = state
        .fs
        .list_file_revisions(&namespace_id, &path)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn list_inode_revisions(
    State(state): State<AppState>,
    AxumPath((namespace, inode_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let inode_id = parse_inode_id(&inode_id)?;
    let response = state
        .fs
        .list_file_revisions_for_inode(&namespace_id, inode_id)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn get_inode_revision_content(
    State(state): State<AppState>,
    AxumPath((namespace, inode_id, revision_no)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let inode_id = parse_inode_id(&inode_id)?;
    let revision_no = parse_revision_no(&revision_no)?;
    let bytes = state
        .fs
        .read_file_revision_bytes_for_inode(&namespace_id, inode_id, revision_no)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok((StatusCode::OK, bytes).into_response())
}

async fn restore_inode_revision(
    State(state): State<AppState>,
    AxumPath((namespace, inode_id, source_revision_no)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
    Json(request): Json<RestoreFileRevisionRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let inode_id = parse_inode_id(&inode_id)?;
    let source_revision_no = parse_revision_no(&source_revision_no)?;
    let commit = ApiCommitRequest {
        commit_id: request.commit_id,
        preconditions: vec![loonfs_api::v0::CommitPrecondition::InodeRevisionIs {
            inode_id,
            revision_no: request.base_revision_no,
        }],
        ops: vec![loonfs_api::v0::CommitOp::RestoreRevision {
            inode_id,
            source_revision_no,
            base_revision_no: request.base_revision_no,
        }],
        message: None,
        annotations: None,
    };
    let response = state
        .publisher
        .submit_commit(namespace_id.clone(), commit)
        .await
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn filesystem_operation(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<FilesystemOperationRequest>,
) -> Result<Json<FilesystemOperationResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let FilesystemOperationRequest {
        commit_id,
        operation,
    } = request;
    let put_payload_class = match &operation {
        FilesystemOperation::PutFile { content_ref, .. } => Some(payload_class(
            usize::try_from(content_ref.size_bytes).unwrap_or(usize::MAX),
        )),
        _ => None,
    };
    let intent = match operation {
        FilesystemOperation::CreateDir { path } => PathMutationIntent::CreateDir {
            commit_id,
            absolute_path: path,
        },
        FilesystemOperation::PutFile {
            path,
            content_ref,
            behavior,
        } => PathMutationIntent::PutFile {
            commit_id,
            absolute_path: path,
            content_ref,
            behavior: map_filesystem_put_behavior(behavior),
        },
        FilesystemOperation::DeletePath { path } => PathMutationIntent::DeletePath {
            commit_id,
            absolute_path: path,
            recursive: false,
        },
        FilesystemOperation::MovePath {
            from_path,
            to_path,
            mode,
        } => PathMutationIntent::MovePath {
            commit_id,
            from_path,
            to_path,
            mode,
        },
        FilesystemOperation::CopyPath { from_path, to_path } => PathMutationIntent::CopyFilePath {
            commit_id,
            from_path,
            to_path,
        },
        FilesystemOperation::RestoreRevision {
            path,
            source_revision_no,
        } => PathMutationIntent::RestoreRevision {
            commit_id,
            absolute_path: path,
            source_revision_no,
        },
    };
    let response_result = if let Some(payload_class) = put_payload_class {
        let span = tracing::info_span!(
            "loon.put",
            operation = "put",
            mode = "remote",
            store_kind = trace_store_kind(&state.config.store).as_str(),
            payload_class,
        );
        state
            .publisher
            .submit_path_intent(namespace_id.clone(), intent)
            .instrument(span)
            .await
    } else {
        state
            .publisher
            .submit_path_intent(namespace_id.clone(), intent)
            .await
    };
    let response = response_result
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    let result = FilesystemOperationResponse {
        namespace_id: response.namespace_id,
        committed_seq: response.committed_seq,
    };
    Ok(Json(result))
}

async fn begin_upload_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    request: Option<Json<BeginUploadRequest>>,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let request = request.map(|Json(request)| request).unwrap_or_default();
    if request.mode.unwrap_or_default() == UploadMode::DirectPut {
        return begin_direct_put_upload(state, namespace_id, request).await;
    }

    let response = state
        .fs
        .begin_upload_with_request(&namespace_id, request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn begin_direct_put_upload(
    state: AppState,
    namespace_id: NamespaceId,
    request: BeginUploadRequest,
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    let Some(issuer) = state.transfer_issuer.as_ref() else {
        return Err(ApiResponseError::not_supported(
            FEATURE_UPLOADS_DIRECT_PUT,
            "direct_put requires a presigned URL capable object store",
        ));
    };
    let Some(content_ref) = request.content_ref else {
        return Err(ApiResponseError::runtime_for_namespace(
            &namespace_id,
            RuntimeError::Core(CoreError::InvalidUploadContent(
                "direct_put requires content_ref at begin_upload".to_owned(),
            )),
        ));
    };

    let prepared = state
        .fs
        .begin_direct_put_upload_target(&namespace_id, content_ref)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    let content_ref = prepared.target.content_ref;
    let signed = issuer
        .presign_put(
            PresignedPutRequest {
                object_key: &prepared.target.object_key,
                content_ref: &content_ref,
                expires_in: DIRECT_PUT_URL_TTL,
            },
            direct_put_presign_time(),
        )
        .map_err(direct_put_issuer_error)?;

    Ok(Json(BeginUploadResponse {
        namespace_id: prepared.namespace_id,
        upload_id: prepared.upload_id,
        mode: UploadMode::DirectPut,
        direct_put: Some(DirectPutUpload {
            content_ref,
            access: ObjectTransferAccess::PresignedUrl {
                method: signed.method,
                url: signed.url,
                headers: signed.headers,
                expires_at_ms: signed.expires_at_ms,
            },
        }),
    }))
}

fn direct_put_issuer_error(error: ObjectStoreError) -> ApiResponseError {
    match error {
        ObjectStoreError::InvalidContentRef(message) => ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidUploadContent,
            &message,
        ),
        error => ApiResponseError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::ServerError,
            &error.to_string(),
        ),
    }
}

#[allow(clippy::disallowed_methods)]
fn direct_put_presign_time() -> SystemTime {
    // Issuing a short-lived transfer capability is an explicit wall-clock boundary.
    SystemTime::now()
}

fn presigned_transfer_issuer(config: &ServerConfig) -> Option<Arc<dyn ObjectTransferIssuer>> {
    match &config.store {
        StoreConfig::LocalFs { .. } => None,
        StoreConfig::AwsS3 {
            bucket,
            region,
            endpoint_url,
            access_key_id,
            secret_access_key,
            session_token,
            key_prefix,
            force_path_style,
        } => Some(Arc::new(
            S3CompatiblePresigner::new(S3PresignerConfig {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint_url: endpoint_url.clone(),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: session_token.clone(),
                key_prefix: key_prefix.clone(),
                force_path_style: force_path_style.unwrap_or(false),
            })
            .expect("validated server config constructs S3 presigner"),
        )),
        StoreConfig::CloudflareR2 {
            bucket,
            endpoint_url,
            access_key_id,
            secret_access_key,
            key_prefix,
            ..
        } => Some(Arc::new(
            S3CompatiblePresigner::new(S3PresignerConfig {
                bucket: bucket.clone(),
                region: "auto".to_owned(),
                endpoint_url: Some(endpoint_url.clone()),
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                session_token: None,
                key_prefix: key_prefix.clone(),
                force_path_style: false,
            })
            .expect("validated server config constructs R2 presigner"),
        )),
    }
}

async fn upload_content_handler(
    State(state): State<AppState>,
    AxumPath((namespace, upload_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<UploadContentResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let bytes = body.to_vec();
    let response = state
        .fs
        .upload_content(&namespace_id, &upload_id, &bytes)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn complete_upload_handler(
    State(state): State<AppState>,
    AxumPath((namespace, upload_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CompleteUploadRequest>,
) -> Result<Json<CompleteUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let response = state
        .fs
        .complete_upload(&namespace_id, &upload_id, &request)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn commit_operations_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<ApiCommitRequest>,
) -> Result<Json<ApiCommitResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let response = state
        .publisher
        .submit_commit(namespace_id.clone(), request)
        .await
        .map_err(|error| ApiResponseError::core_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn list_changes_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let after_seq = loonfs_api::ChangeSeq(query.after_seq);
    let response = state
        .fs
        .list_changes_after(&namespace_id, after_seq)
        .await
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))?;
    Ok(Json(response))
}

async fn create_checkpoint_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let response = state
        .fs
        .create_checkpoint(&namespace_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

async fn advance_retention_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<AdvanceRetentionResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespace_id = parse_namespace_id(namespace)?;
    let response = state
        .fs
        .advance_retention_floor(&namespace_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

fn authorize(config: &ServerConfig, headers: &HeaderMap) -> Result<(), ApiResponseError> {
    let Some(expected) = &config.auth_token else {
        return Ok(());
    };
    let actual = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if actual == format!("Bearer {expected}") {
        Ok(())
    } else {
        Err(ApiResponseError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid bearer token",
        ))
    }
}

fn map_filesystem_put_behavior(value: FilesystemPutBehavior) -> PutFileBehavior {
    match value {
        FilesystemPutBehavior::CreateOnly => PutFileBehavior::CreateOnly,
        FilesystemPutBehavior::ReplaceExisting => PutFileBehavior::ReplaceExisting,
    }
}

fn parse_namespace_id(value: String) -> Result<NamespaceId, ApiResponseError> {
    NamespaceId::parse(&value).map_err(ApiResponseError::invalid_namespace_id)
}

fn parse_inode_id(value: &str) -> Result<InodeId, ApiResponseError> {
    value.parse::<u64>().map(InodeId).map_err(|err| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidInodeId,
            &format!("invalid inode_id `{value}`: {err}"),
        )
    })
}

fn parse_revision_no(value: &str) -> Result<RevisionNo, ApiResponseError> {
    value.parse::<u64>().map(RevisionNo).map_err(|err| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidRevisionNo,
            &format!("invalid revision_no `{value}`: {err}"),
        )
    })
}

struct ApiResponseError {
    status: StatusCode,
    body: ApiError,
}

impl ApiResponseError {
    fn new(status: StatusCode, code: ErrorCode, message: &str) -> Self {
        Self {
            status,
            body: ApiError {
                code: code.as_str().to_owned(),
                feature: None,
                message: message.to_owned(),
            },
        }
    }

    fn not_supported(feature: &str, message: &str) -> Self {
        Self {
            status: StatusCode::NOT_IMPLEMENTED,
            body: ApiError {
                code: ErrorCode::NotSupported.as_str().to_owned(),
                feature: Some(feature.to_owned()),
                message: message.to_owned(),
            },
        }
    }

    fn invalid_namespace_id(error: NamespaceIdValidationError) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidNamespaceId,
            &error.to_string(),
        )
    }

    fn bootstrap(error: BootstrapNamespaceError) -> Self {
        match error {
            BootstrapNamespaceError::InvalidNamespaceId(error) => Self::invalid_namespace_id(error),
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => Self::new(
                StatusCode::CONFLICT,
                ErrorCode::NamespaceExists,
                &error.to_string(),
            ),
            BootstrapNamespaceError::NamespacePartiallyInitialized { .. } => Self::new(
                StatusCode::CONFLICT,
                ErrorCode::NamespacePartial,
                &error.to_string(),
            ),
            BootstrapNamespaceError::NamespaceDeleted { .. } => Self::new(
                StatusCode::GONE,
                ErrorCode::NamespaceDeleted,
                &error.to_string(),
            ),
            BootstrapNamespaceError::EmptyHolderId
            | BootstrapNamespaceError::EmptyWriterVersion => Self::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::InvalidConfig,
                &error.to_string(),
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::BootstrapFailed,
                &error.to_string(),
            ),
        }
    }

    fn runtime(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Core(error) => Self::core(error),
            RuntimeError::Bootstrap(error) => Self::bootstrap(error),
            RuntimeError::Config(message) => {
                Self::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidConfig, &message)
            }
            error => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::ServerError,
                &error.to_string(),
            ),
        }
    }

    fn core(error: CoreError) -> Self {
        let status = status_for_core_error_code(error.code());
        Self::new(status, error.code(), &error.to_string())
    }

    fn core_for_namespace(namespace: &NamespaceId, error: CoreError) -> Self {
        if matches!(error.code(), ErrorCode::NamespaceNotFound) {
            return Self::new(
                StatusCode::NOT_FOUND,
                ErrorCode::NamespaceNotFound,
                &format!("namespace `{}` does not exist", namespace.as_str()),
            );
        }

        Self::core(error)
    }

    fn runtime_for_namespace(namespace: &NamespaceId, error: RuntimeError) -> Self {
        match error {
            RuntimeError::Core(error) => Self::core_for_namespace(namespace, error),
            RuntimeError::Bootstrap(error) => Self::bootstrap(error),
            RuntimeError::Config(message) => {
                Self::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidConfig, &message)
            }
            error => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::ServerError,
                &error.to_string(),
            ),
        }
    }
}

fn status_for_core_error_code(code: ErrorCode) -> StatusCode {
    match code {
        ErrorCode::InvalidPath
        | ErrorCode::InvalidNamespaceId
        | ErrorCode::InvalidCommitId
        | ErrorCode::InvalidUploadId
        | ErrorCode::InvalidInodeId
        | ErrorCode::InvalidRevisionNo
        | ErrorCode::InvalidConfig
        | ErrorCode::UnsupportedRenameMode
        | ErrorCode::InvalidUploadContent => StatusCode::BAD_REQUEST,
        ErrorCode::NamespaceNotFound
        | ErrorCode::PathNotFound
        | ErrorCode::RevisionNotFound
        | ErrorCode::UploadNotFound => StatusCode::NOT_FOUND,
        ErrorCode::CommitQueueFull
        | ErrorCode::CommitOutcomeUnknown
        | ErrorCode::CheckpointUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NamespaceDeleted => StatusCode::GONE,
        ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
        ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorCode::NotSupported => StatusCode::NOT_IMPLEMENTED,
        ErrorCode::NamespaceCorrupt | ErrorCode::ServerError | ErrorCode::BootstrapFailed => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
        ErrorCode::NamespaceExists
        | ErrorCode::NamespacePartial
        | ErrorCode::PathConflict
        | ErrorCode::DirectoryNotEmpty
        | ErrorCode::StaleHead
        | ErrorCode::StaleRevision
        | ErrorCode::TombstoneConflict
        | ErrorCode::LeaseConflict
        | ErrorCode::WouldCycle
        | ErrorCode::CommitIdReuseConflict
        | ErrorCode::UploadAlreadyCompleted
        | ErrorCode::UploadContentConflict
        | ErrorCode::RebootstrapRequired => StatusCode::CONFLICT,
        // A code without an explicit arm serves as 500 until someone decides
        // its real status. The spec-table test below fails on any code whose
        // served status disagrees with the api.md registry, so new codes
        // cannot ship on this default silently.
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiResponseError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::disallowed_methods)]
    // HTTP smoke helpers use wall-clock lease timestamps and panic in unexpected match arms.

    /// The compile-time forcing function for new error codes moved here when
    /// `ErrorCode` became `#[non_exhaustive]`: every registered code must
    /// appear in the api.md error table, and the status this server serves
    /// must be the status the table documents.
    #[test]
    fn error_status_mapping_matches_the_api_spec_table() {
        let spec = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/specs/api.md"
        ))
        .expect("read docs/specs/api.md");
        let table = spec
            .split("The full registry")
            .nth(1)
            .expect("api.md error registry intro")
            .split("Precondition failures surface")
            .next()
            .expect("api.md error registry end");

        let mut documented = std::collections::BTreeMap::new();
        for line in table.lines() {
            let Some(rest) = line.strip_prefix("| `") else {
                continue;
            };
            let mut cells = rest.split(" | ");
            let code = cells
                .next()
                .expect("code cell")
                .trim_end_matches('`')
                .to_owned();
            let status: u16 = cells
                .next()
                .expect("status cell")
                .trim()
                .parse()
                .expect("numeric status cell");
            documented.insert(code, status);
        }

        for code in ErrorCode::ALL {
            let documented_status = documented.remove(code.as_str()).unwrap_or_else(|| {
                panic!(
                    "`{}` is registered in loonfs-api but missing from the api.md error table",
                    code.as_str()
                )
            });
            assert_eq!(
                status_for_core_error_code(code).as_u16(),
                documented_status,
                "served status for `{}` disagrees with the api.md error table",
                code.as_str()
            );
        }
        assert!(
            documented.is_empty(),
            "api.md documents codes this build does not register: {documented:?}"
        );
    }

    use super::{app_with_store, build_fs_with_metrics_jsonl_path, SharedStore};
    use super::{status_for_core_error_code, ErrorCode};
    use crate::config::RuntimeCacheConfigOverrides;
    use crate::{ServerConfig, StoreConfig};
    use async_trait::async_trait;
    use axum::body::Bytes;
    use futures::stream::BoxStream;
    use loonfs::{
        CreateNamespaceOptions, Fs, FsConfig, PutFileBehavior, PutFileOptions, RuntimeCacheConfig,
        TraceMode, TraceStoreKind,
    };
    use loonfs_api::{ChangeSeq, CommitId, NamespaceId};
    use loonfs_client::{Client, ClientConfig, ClientError, NamespacePath};
    use loonfs_core::{BootstrapOptions, MutationContext, NamespaceEngine, WriteOptions};
    use loonfs_objectstore::fs::LocalFsStore;
    use loonfs_objectstore::keys::namespace_head;
    use loonfs_objectstore::{
        ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
    };
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::tempdir;

    #[derive(Debug)]
    struct StaleHeadOnceStore {
        inner: LocalFsStore,
        head_key: String,
        armed: AtomicBool,
    }

    impl StaleHeadOnceStore {
        fn new(root: impl AsRef<Path>, namespace: &str) -> Self {
            Self {
                inner: LocalFsStore::new(root.as_ref()).expect("construct local store"),
                head_key: namespace_head(namespace),
                armed: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for StaleHeadOnceStore {
        async fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Bytes>, ObjectStoreError> {
            self.inner.get(key, range).await
        }

        async fn get_with_metadata(
            &self,
            key: &str,
        ) -> Result<Option<ObjectBody>, ObjectStoreError> {
            self.inner.get_with_metadata(key).await
        }

        async fn put(
            &self,
            key: &str,
            bytes: Bytes,
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key
                && matches!(mode, PutMode::CompareAndSwap { .. })
                && self.armed.swap(false, Ordering::SeqCst)
            {
                if let Some(existing) = self.inner.get(key, None).await? {
                    let _ = self.inner.put_overwrite(key, existing).await?;
                }
            }
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

    #[test]
    fn build_fs_installs_jsonl_object_store_metrics_recorder() {
        let store_dir = tempdir().expect("store tempdir");
        let metrics_dir = tempdir().expect("metrics tempdir");
        let store = Arc::new(LocalFsStore::new(store_dir.path()).expect("store")) as SharedStore;
        let config = test_config(store_dir.path(), "server-writer");
        let metrics_path = metrics_dir.path().join("object-store.ndjson");

        {
            let fs = build_fs_with_metrics_jsonl_path(
                &config,
                store,
                Some(metrics_path.clone().into_os_string()),
            )
            .expect("build fs");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(
                    fs.create_namespace(
                        &namespace_id("metrics"),
                        CreateNamespaceOptions::default(),
                    ),
                )
                .expect("create namespace");
        }

        let jsonl = std::fs::read_to_string(metrics_path).expect("read metrics");
        assert!(!jsonl.is_empty());
        assert!(!jsonl.contains("namespaces/metrics"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_created_state_is_readable_through_http() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let fs = test_runtime(store.clone(), "runtime-writer");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .await
            .expect("create namespace through runtime");
        fs.put_file_bytes(
            &namespace_id,
            "/notes/hello.txt",
            b"hello from runtime",
            PutFileOptions {
                behavior: PutFileBehavior::CreateOnly,
                commit_id: Some(CommitId::parse("runtime-put").expect("valid commit id")),
            },
        )
        .await
        .expect("write file through runtime");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/notes/hello.txt").expect("target");
            let stat = harness.client.stat_path(&target).expect("stat file");
            assert_eq!(stat.absolute_path, "/notes/hello.txt");
            assert_eq!(stat.size_bytes, Some(18));
            let bytes = harness.client.read_file_bytes(&target).expect("read file");
            assert_eq!(bytes, b"hello from runtime");
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_created_state_is_readable_through_runtime() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let fs = test_runtime(store.clone(), "runtime-reader");
        let harness = start_server(store.clone(), temp_dir.path(), "server-writer").await;

        tokio::task::spawn_blocking(move || {
            harness
                .client
                .create_namespace("demo")
                .expect("create namespace through http");
            let target = NamespacePath::parse("demo:/notes/from-http.txt").expect("target");
            harness
                .client
                .write_file_bytes(&target, b"hello from http")
                .expect("write file through http");
        })
        .await
        .expect("join blocking task");

        let file = fs
            .read_file_bytes(
                &NamespaceId::parse("demo").expect("valid namespace id"),
                "/notes/from-http.txt",
            )
            .await
            .expect("read file through runtime");
        assert_eq!(file.bytes, b"hello from http");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_missing_namespace_mutations_return_namespace_not_found() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let harness = start_server(store, temp_dir.path(), "server-writer").await;

        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("missing:/notes/hello.txt").expect("target");
            assert_api_error(
                harness.client.write_file_bytes(&target, b"hello"),
                404,
                "namespace_not_found",
                Some("namespace `missing` does not exist"),
            );
            assert_api_error(
                harness.client.delete_path(&target),
                404,
                "namespace_not_found",
                Some("namespace `missing` does not exist"),
            );
            let destination = NamespacePath::parse("missing:/notes/renamed.txt").expect("target");
            assert_api_error(
                harness.client.move_path(&target, &destination),
                404,
                "namespace_not_found",
                Some("namespace `missing` does not exist"),
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_missing_namespace_reads_return_namespace_not_found() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let harness = start_server(store, temp_dir.path(), "server-writer").await;

        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("missing:/").expect("target");
            assert_api_error(
                harness.client.list_path(&target),
                404,
                "namespace_not_found",
                Some("namespace `missing` does not exist"),
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_delete_missing_path_returns_path_not_found() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let now_ms = now_ms();
        bootstrap_namespace(
            store.as_ref(),
            &namespace_id("demo"),
            &context("server-writer", now_ms),
            false,
        )
        .await
        .expect("bootstrap namespace");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/missing.txt").expect("target");
            assert_api_error(
                harness.client.delete_path(&target),
                404,
                "path_not_found",
                None,
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_put_over_directory_and_move_into_existing_target_return_path_conflict() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let now_ms = now_ms();
        let context = context("server-writer", now_ms);
        bootstrap_namespace(store.as_ref(), &namespace_id("demo"), &context, false)
            .await
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/readme.txt",
            b"readme",
            &context,
            Some("seed-docs"),
        )
        .await
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/tmp/a.txt",
            b"from tmp",
            &context,
            Some("seed-tmp"),
        )
        .await
        .expect("seed tmp");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/a.txt",
            b"in docs",
            &context,
            Some("seed-target"),
        )
        .await
        .expect("seed target");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let dir_target = NamespacePath::parse("demo:/docs").expect("dir target");
            assert_api_error(
                harness.client.write_file_bytes(&dir_target, b"not a file"),
                409,
                "path_conflict",
                None,
            );

            let from = NamespacePath::parse("demo:/tmp/a.txt").expect("from");
            let to = NamespacePath::parse("demo:/docs/a.txt").expect("to");
            assert_api_error(
                harness.client.move_path(&from, &to),
                409,
                "path_conflict",
                None,
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_put_and_move_under_tombstoned_ancestor_return_tombstone_conflict() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let now_ms = now_ms();
        let context = context("server-writer", now_ms);
        bootstrap_namespace(store.as_ref(), &namespace_id("demo"), &context, false)
            .await
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/old.txt",
            b"old",
            &context,
            Some("seed-docs"),
        )
        .await
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/tmp/source.txt",
            b"source",
            &context,
            Some("seed-source"),
        )
        .await
        .expect("seed source");
        delete_path(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs",
            &context,
            Some("delete-docs"),
        )
        .await
        .expect("delete docs");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let put_target = NamespacePath::parse("demo:/docs/new.txt").expect("put target");
            assert_api_error(
                harness.client.write_file_bytes(&put_target, b"new"),
                409,
                "tombstone_conflict",
                None,
            );

            let from = NamespacePath::parse("demo:/tmp/source.txt").expect("from");
            let to = NamespacePath::parse("demo:/docs/source.txt").expect("to");
            assert_api_error(
                harness.client.move_path(&from, &to),
                409,
                "tombstone_conflict",
                None,
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_path_mutation_retries_transient_stale_head_cas() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(StaleHeadOnceStore::new(temp_dir.path(), "demo")) as SharedStore;
        let now_ms = now_ms();
        bootstrap_namespace(
            store.as_ref(),
            &namespace_id("demo"),
            &context("server-writer", now_ms),
            false,
        )
        .await
        .expect("bootstrap namespace");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/notes/race.txt").expect("target");
            let result = harness
                .client
                .write_file_bytes(&target, b"race")
                .expect("path write retries stale head");
            assert_eq!(result.committed_seq, ChangeSeq(1));
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_active_lease_held_by_other_writer_returns_lease_conflict() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let now_ms = now_ms();
        bootstrap_namespace(
            store.as_ref(),
            &namespace_id("demo"),
            &context("other-writer", now_ms),
            false,
        )
        .await
        .expect("bootstrap namespace");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/notes/blocked.txt").expect("target");
            assert_api_error(
                harness.client.write_file_bytes(&target, b"blocked"),
                409,
                "lease_conflict",
                None,
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    struct TestHarness {
        client: Client,
        server: tokio::task::JoinHandle<()>,
    }

    async fn start_server(store: SharedStore, root: &Path, writer_id: &str) -> TestHarness {
        let config = test_config(root, writer_id);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let router = app_with_store(config, store).expect("build app");
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve app");
        });

        TestHarness {
            client: Client::new(ClientConfig {
                server_url: format!("http://{}", addr),
                auth_token: Some("test-token".to_owned()),
            }),
            server,
        }
    }

    fn test_runtime(store: SharedStore, writer_id: &str) -> Fs {
        Fs::open(
            store,
            FsConfig {
                writer_id: writer_id.to_owned(),
                writer_version: format!("{writer_id}/0.1.0"),
                lease_duration_ms: 60_000,
                runtime_cache: RuntimeCacheConfig::default(),
                trace_mode: TraceMode::Remote,
                trace_store_kind: TraceStoreKind::LocalFs,
            },
        )
        .expect("open runtime")
    }

    fn test_config(root: &Path, writer_id: &str) -> ServerConfig {
        ServerConfig {
            bind: "127.0.0.1:0".to_owned(),
            auth_token: Some("test-token".to_owned()),
            writer_id: writer_id.to_owned(),
            writer_version: format!("{writer_id}/0.1.0"),
            lease_duration_ms: 60_000,
            runtime_cache: RuntimeCacheConfigOverrides::default(),
            store: StoreConfig::LocalFs {
                root: root.display().to_string(),
                key_prefix: Some("http-tests".to_owned()),
            },
        }
    }

    fn context(writer_id: &str, now_ms: u64) -> MutationContext {
        MutationContext {
            writer_id: writer_id.to_owned(),
            writer_version: format!("{writer_id}/0.1.0"),
            now_ms,
            lease_duration_ms: 60_000,
        }
    }

    fn namespace_engine<'a, S: ObjectStore + ?Sized>(
        store: &'a S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
    ) -> NamespaceEngine<&'a S> {
        NamespaceEngine::builder(store)
            .namespace(namespace_id.clone())
            .writer(context.writer_id.clone())
            .writer_version(context.writer_version.clone())
            .lease_duration_ms(context.lease_duration_ms)
            .build()
            .expect("test context should build namespace engine")
    }

    async fn bootstrap_namespace<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        context: &MutationContext,
        allow_existing: bool,
    ) -> Result<loonfs_api::NamespaceSummary, loonfs_core::BootstrapNamespaceError> {
        namespace_engine(store, namespace_id, context)
            .bootstrap_namespace(BootstrapOptions { allow_existing })
            .await
    }

    async fn write_file_bytes<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        bytes: &[u8],
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_api::MutationResult, loonfs_core::Error> {
        namespace_engine(store, namespace_id, context)
            .put_file(
                absolute_path,
                bytes,
                WriteOptions {
                    commit_id: commit_id
                        .map(|value| CommitId::parse(value).expect("valid test commit id")),
                    put_file_behavior: PutFileBehavior::ReplaceExisting,
                    ..WriteOptions::default()
                },
            )
            .await
    }

    async fn delete_path<S: ObjectStore + ?Sized>(
        store: &S,
        namespace_id: &NamespaceId,
        absolute_path: &str,
        context: &MutationContext,
        commit_id: Option<&str>,
    ) -> Result<loonfs_api::MutationResult, loonfs_core::Error> {
        namespace_engine(store, namespace_id, context)
            .delete_path(
                absolute_path,
                WriteOptions {
                    commit_id: commit_id
                        .map(|value| CommitId::parse(value).expect("valid test commit id")),
                    recursive_delete: true,
                    ..WriteOptions::default()
                },
            )
            .await
    }

    fn namespace_id(value: &str) -> NamespaceId {
        NamespaceId::parse(value).expect("valid namespace id")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis() as u64
    }

    fn assert_api_error<T: std::fmt::Debug>(
        result: Result<T, ClientError>,
        status: u16,
        code: &str,
        message: Option<&str>,
    ) {
        match result {
            Err(ClientError::Api {
                status: actual_status,
                code: actual_code,
                message: actual_message,
                ..
            }) => {
                assert_eq!(actual_status, status);
                assert_eq!(actual_code, code);
                if let Some(expected_message) = message {
                    assert_eq!(actual_message, expected_message);
                }
            }
            other => panic!("expected api error {status} {code}, got {other:?}"),
        }
    }
}
