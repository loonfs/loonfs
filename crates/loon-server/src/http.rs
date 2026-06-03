use crate::config::{ServerConfig, ServerConfigError, StoreConfig};
use crate::publisher::PublisherRegistry;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use loon_api::{
    v0::{
        BeginUploadResponse, ChangesResponse, CommitRequest as ApiCommitRequest,
        CommitResponse as ApiCommitResponse, CompleteUploadRequest, CompleteUploadResponse,
        UploadContentResponse,
    },
    AdvanceRetentionResponse, ApiError, CreateCheckpointResponse, CreateNamespaceRequest,
    FilesystemOperation, FilesystemOperationRequest, FilesystemOperationResponse,
    FilesystemPutBehavior, ForkNamespaceRequest, InodeId, ListFileRevisionsResponse,
    ListNamespacesResponse, NamespaceId, NamespaceIdValidationError, RestoreFileRevisionRequest,
    RevisionNo,
};
use loonfs::{
    BootstrapNamespaceError, CoreError, CoreErrorKind, CreateNamespaceOptions, Fs, FsConfig,
    PathMutationIntent, PutFileBehavior, RuntimeCacheConfig, RuntimeError, SharedObjectStore,
    TraceMode, TraceStoreKind,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task;
use tracing::{field, Instrument};

type SharedStore = SharedObjectStore;

#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    fs: Arc<Fs>,
    publisher: PublisherRegistry,
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
    let state = AppState {
        config,
        fs,
        publisher,
    };
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/v0/namespaces",
            post(create_namespace).get(list_namespaces_handler),
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
    Fs::open(
        store,
        FsConfig {
            writer_id: config.writer_id.clone(),
            writer_version: config.writer_version.clone(),
            lease_duration_ms: config.lease_duration_ms,
            runtime_cache: RuntimeCacheConfig::default(),
            trace_mode: TraceMode::Remote,
            trace_store_kind: trace_store_kind(&config.store),
        },
    )
    .map_err(|error| ServerConfigError::InvalidField {
        field: "runtime",
        reason: error.to_string(),
    })
}

fn trace_store_kind(store: &StoreConfig) -> TraceStoreKind {
    match store {
        StoreConfig::LocalFs { .. } => TraceStoreKind::LocalFs,
        StoreConfig::AwsS3 { .. } => TraceStoreKind::S3,
        StoreConfig::CloudflareR2 { .. } => TraceStoreKind::R2,
    }
}

fn trace_store_kind_label(store: &StoreConfig) -> &'static str {
    match store {
        StoreConfig::LocalFs { .. } => "local_fs",
        StoreConfig::AwsS3 { .. } => "s3",
        StoreConfig::CloudflareR2 { .. } => "r2",
    }
}

fn trace_payload_class(size_bytes: u64) -> &'static str {
    match size_bytes {
        0..=16_383 => "small",
        16_384..=1_048_575 => "medium",
        _ => "large",
    }
}

fn trace_result_label<T, E>(result: &Result<T, E>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "error"
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

async fn create_namespace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateNamespaceRequest>,
) -> Result<Json<loon_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(request.namespace_id)?;
    let summary = run_blocking(move || {
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
            .map_err(ApiResponseError::runtime)
    })
    .await?;
    Ok(Json(summary))
}

async fn list_namespaces_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ListNamespacesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespaces =
        run_blocking(move || fs.list_namespaces().map_err(ApiResponseError::runtime)).await?;
    Ok(Json(ListNamespacesResponse { namespaces }))
}

async fn fork_namespace_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<ForkNamespaceRequest>,
) -> Result<Json<loon_api::NamespaceSummary>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let source_namespace_id = parse_namespace_id(namespace)?;
    let new_namespace_id = parse_namespace_id(request.new_namespace_id)?;
    let summary = run_blocking(move || {
        fs.fork_namespace(&source_namespace_id, &new_namespace_id)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&source_namespace_id, error))
    })
    .await?;
    Ok(Json(summary))
}

async fn list_entries(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<Vec<loon_api::AuthoritativePathEntry>>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let entries = run_blocking(move || {
        fs.list_path(&namespace_id, &path)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(entries))
}

async fn stat_entry(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<loon_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let entry = run_blocking(move || {
        fs.stat_path(&namespace_id, &path)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(entry))
}

async fn get_content(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<ContentQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let revision_no = query
        .revision_no
        .as_deref()
        .map(parse_revision_no)
        .transpose()?;
    let file = run_blocking(move || {
        match revision_no {
            Some(revision_no) => fs.read_file_revision_bytes(&namespace_id, &path, revision_no),
            None => fs.read_file_bytes(&namespace_id, &path),
        }
        .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok((StatusCode::OK, file.bytes).into_response())
}

async fn list_path_revisions(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let path = query.path;
    let response = run_blocking(move || {
        fs.list_file_revisions(&namespace_id, &path)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(response))
}

async fn list_inode_revisions(
    State(state): State<AppState>,
    AxumPath((namespace, inode_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ListFileRevisionsResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let inode_id = parse_inode_id(&inode_id)?;
    let response = run_blocking(move || {
        fs.list_file_revisions_for_inode(&namespace_id, inode_id)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(response))
}

async fn get_inode_revision_content(
    State(state): State<AppState>,
    AxumPath((namespace, inode_id, revision_no)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let inode_id = parse_inode_id(&inode_id)?;
    let revision_no = parse_revision_no(&revision_no)?;
    let bytes = run_blocking(move || {
        fs.read_file_revision_bytes_for_inode(&namespace_id, inode_id, revision_no)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
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
        preconditions: vec![loon_api::v0::CommitPrecondition::InodeRevisionIs {
            inode_id,
            revision_no: request.base_revision_no,
        }],
        ops: vec![loon_api::v0::CommitOp::RestoreRevision {
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
        FilesystemOperation::PutFile { content_ref, .. } => {
            Some(trace_payload_class(content_ref.size_bytes))
        }
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
            store_kind = trace_store_kind_label(&state.config.store),
            payload_class,
            result = field::Empty
        );
        let result = state
            .publisher
            .submit_path_intent(namespace_id.clone(), intent)
            .instrument(span.clone())
            .await;
        span.record("result", trace_result_label(&result));
        result
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
) -> Result<Json<BeginUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let response = run_blocking(move || {
        fs.begin_upload(&namespace_id)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(response))
}

async fn upload_content_handler(
    State(state): State<AppState>,
    AxumPath((namespace, upload_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<UploadContentResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let bytes = body.to_vec();
    let response = run_blocking(move || {
        fs.upload_content(&namespace_id, &upload_id, &bytes)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(response))
}

async fn complete_upload_handler(
    State(state): State<AppState>,
    AxumPath((namespace, upload_id)): AxumPath<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<CompleteUploadRequest>,
) -> Result<Json<CompleteUploadResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let response = run_blocking(move || {
        fs.complete_upload(&namespace_id, &upload_id, &request)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
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
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let after_seq = loon_api::ChangeSeq(query.after_seq);
    let response = run_blocking(move || {
        fs.list_changes_after(&namespace_id, after_seq)
            .map_err(|error| ApiResponseError::runtime_for_namespace(&namespace_id, error))
    })
    .await?;
    Ok(Json(response))
}

async fn create_checkpoint_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<CreateCheckpointResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let response = run_blocking(move || {
        fs.create_checkpoint(&namespace_id)
            .map_err(ApiResponseError::runtime)
    })
    .await?;
    Ok(Json(response))
}

async fn advance_retention_handler(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
) -> Result<Json<AdvanceRetentionResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let fs = state.fs.clone();
    let namespace_id = parse_namespace_id(namespace)?;
    let response = run_blocking(move || {
        fs.advance_retention_floor(&namespace_id)
            .map_err(ApiResponseError::runtime)
    })
    .await?;
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
            "unauthorized",
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
            "invalid_inode_id",
            &format!("invalid inode_id `{value}`: {err}"),
        )
    })
}

fn parse_revision_no(value: &str) -> Result<RevisionNo, ApiResponseError> {
    value.parse::<u64>().map(RevisionNo).map_err(|err| {
        ApiResponseError::new(
            StatusCode::BAD_REQUEST,
            "invalid_revision_no",
            &format!("invalid revision_no `{value}`: {err}"),
        )
    })
}

async fn run_blocking<T, F>(operation: F) -> Result<T, ApiResponseError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, ApiResponseError> + Send + 'static,
{
    task::spawn_blocking(operation).await.map_err(|err| {
        ApiResponseError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            &format!("blocking operation failed: {err}"),
        )
    })?
}

struct ApiResponseError {
    status: StatusCode,
    body: ApiError,
}

impl ApiResponseError {
    fn new(status: StatusCode, code: &str, message: &str) -> Self {
        Self {
            status,
            body: ApiError {
                code: code.to_owned(),
                message: message.to_owned(),
            },
        }
    }

    fn invalid_namespace_id(error: NamespaceIdValidationError) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_namespace_id",
            &error.to_string(),
        )
    }

    fn bootstrap(error: BootstrapNamespaceError) -> Self {
        match error {
            BootstrapNamespaceError::InvalidNamespaceId(error) => Self::invalid_namespace_id(error),
            BootstrapNamespaceError::NamespaceAlreadyExists { .. } => {
                Self::new(StatusCode::CONFLICT, "namespace_exists", &error.to_string())
            }
            BootstrapNamespaceError::NamespacePartiallyInitialized { .. } => Self::new(
                StatusCode::CONFLICT,
                "namespace_partial",
                &error.to_string(),
            ),
            BootstrapNamespaceError::EmptyHolderId
            | BootstrapNamespaceError::EmptyWriterVersion => Self::new(
                StatusCode::BAD_REQUEST,
                "invalid_config",
                &error.to_string(),
            ),
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "bootstrap_failed",
                &error.to_string(),
            ),
        }
    }

    fn runtime(error: RuntimeError) -> Self {
        match error {
            RuntimeError::Core(error) => Self::core(error),
            RuntimeError::Bootstrap(error) => Self::bootstrap(error),
            RuntimeError::Config(message) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_config", &message)
            }
        }
    }

    fn core(error: CoreError) -> Self {
        let (status, code) = match error.kind() {
            CoreErrorKind::InvalidPath => (StatusCode::BAD_REQUEST, "invalid_path"),
            CoreErrorKind::InvalidNamespaceId => (StatusCode::BAD_REQUEST, "invalid_namespace_id"),
            CoreErrorKind::InvalidCommitId => (StatusCode::BAD_REQUEST, "invalid_commit_id"),
            CoreErrorKind::InvalidUploadId => (StatusCode::BAD_REQUEST, "invalid_upload_id"),
            CoreErrorKind::NamespaceNotFound => (StatusCode::NOT_FOUND, "namespace_not_found"),
            CoreErrorKind::NamespaceExists => (StatusCode::CONFLICT, "namespace_exists"),
            CoreErrorKind::NamespacePartial => (StatusCode::CONFLICT, "namespace_partial"),
            CoreErrorKind::PathNotFound => (StatusCode::NOT_FOUND, "path_not_found"),
            CoreErrorKind::RevisionNotFound => (StatusCode::CONFLICT, "revision_not_found"),
            CoreErrorKind::PathConflict => (StatusCode::CONFLICT, "path_conflict"),
            CoreErrorKind::DirectoryNotEmpty => (StatusCode::CONFLICT, "directory_not_empty"),
            CoreErrorKind::StaleHead => (StatusCode::CONFLICT, "stale_head"),
            CoreErrorKind::StaleRevision => (StatusCode::CONFLICT, "stale_revision"),
            CoreErrorKind::TombstoneConflict => (StatusCode::CONFLICT, "tombstone_conflict"),
            CoreErrorKind::LeaseConflict => (StatusCode::CONFLICT, "lease_conflict"),
            CoreErrorKind::WouldCycle => (StatusCode::CONFLICT, "would_cycle"),
            CoreErrorKind::UnsupportedRenameMode => {
                (StatusCode::BAD_REQUEST, "unsupported_rename_mode")
            }
            CoreErrorKind::CommitIdReuseConflict => {
                (StatusCode::CONFLICT, "commit_id_reuse_conflict")
            }
            CoreErrorKind::CommitQueueFull => {
                (StatusCode::SERVICE_UNAVAILABLE, "commit_queue_full")
            }
            CoreErrorKind::CheckpointUnavailable => {
                (StatusCode::CONFLICT, "checkpoint_unavailable")
            }
            CoreErrorKind::UploadNotFound => (StatusCode::NOT_FOUND, "upload_not_found"),
            CoreErrorKind::UploadAlreadyCompleted => {
                (StatusCode::CONFLICT, "upload_already_completed")
            }
            CoreErrorKind::UploadContentConflict => {
                (StatusCode::CONFLICT, "upload_content_conflict")
            }
            CoreErrorKind::InvalidUploadContent => {
                (StatusCode::BAD_REQUEST, "invalid_upload_content")
            }
            CoreErrorKind::RebootstrapRequired => (StatusCode::CONFLICT, "rebootstrap_required"),
            CoreErrorKind::NamespaceCorrupt => {
                (StatusCode::INTERNAL_SERVER_ERROR, "namespace_corrupt")
            }
            CoreErrorKind::ServerError => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
        };
        Self::new(status, code, &error.to_string())
    }

    fn core_for_namespace(namespace: &NamespaceId, error: CoreError) -> Self {
        if matches!(error.kind(), CoreErrorKind::NamespaceNotFound) {
            return Self::new(
                StatusCode::NOT_FOUND,
                "namespace_not_found",
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
                Self::new(StatusCode::BAD_REQUEST, "invalid_config", &message)
            }
        }
    }
}

impl IntoResponse for ApiResponseError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{app_with_store, SharedStore};
    use crate::{ServerConfig, StoreConfig};
    use loon_api::{ChangeSeq, CommitId, NamespaceId};
    use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
    use loon_core::{bootstrap_namespace, delete_path, write_file_bytes, MutationContext};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
    use loonfs::{
        CreateNamespaceOptions, Fs, FsConfig, PutFileBehavior, PutFileOptions, RuntimeCacheConfig,
        TraceMode, TraceStoreKind,
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

    impl ObjectStore for StaleHeadOnceStore {
        fn head(&self, key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
            self.inner.head(key)
        }

        fn get(
            &self,
            key: &str,
            range: Option<ByteRange>,
        ) -> Result<Option<Vec<u8>>, ObjectStoreError> {
            self.inner.get(key, range)
        }

        fn put(
            &self,
            key: &str,
            bytes: &[u8],
            mode: PutMode,
        ) -> Result<ObjectMetadata, ObjectStoreError> {
            if key == self.head_key
                && matches!(mode, PutMode::CompareAndSwap { .. })
                && self.armed.swap(false, Ordering::SeqCst)
            {
                if let Some(existing) = self.inner.get(key, None)? {
                    let _ = self.inner.put_overwrite(key, &existing)?;
                }
            }
            self.inner.put(key, bytes, mode)
        }

        fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
            self.inner.delete(key)
        }

        fn list_prefix(&self, prefix: &str) -> Result<Vec<String>, ObjectStoreError> {
            self.inner.list_prefix(prefix)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_created_state_is_readable_through_http() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(LocalFsStore::new(temp_dir.path()).expect("store")) as SharedStore;
        let fs = test_runtime(store.clone(), "runtime-writer");
        let namespace_id = NamespaceId::parse("demo").expect("valid namespace id");
        fs.create_namespace(&namespace_id, CreateNamespaceOptions::default())
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
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/readme.txt",
            b"readme",
            &context,
            Some("seed-docs"),
        )
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/tmp/a.txt",
            b"from tmp",
            &context,
            Some("seed-tmp"),
        )
        .expect("seed tmp");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/a.txt",
            b"in docs",
            &context,
            Some("seed-target"),
        )
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
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs/old.txt",
            b"old",
            &context,
            Some("seed-docs"),
        )
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &namespace_id("demo"),
            "/tmp/source.txt",
            b"source",
            &context,
            Some("seed-source"),
        )
        .expect("seed source");
        delete_path(
            store.as_ref(),
            &namespace_id("demo"),
            "/docs",
            &context,
            Some("delete-docs"),
        )
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
