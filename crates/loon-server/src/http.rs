use crate::config::ServerConfig;
use axum::body::Bytes;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use loon_api::{
    ApiError, CreateNamespaceRequest, ListNamespacesResponse, MoveEntryRequest, MutationResult,
    NamespaceId,
};
use loon_core::{
    bootstrap_namespace, delete_path, list_namespaces, list_path, move_path, read_file_bytes,
    resolve_path, write_file_bytes, BootstrapNamespaceError, CoreError, MutationContext,
};
use loon_objectstore::ConfiguredObjectStore;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    store: Arc<ConfiguredObjectStore>,
}

#[derive(Debug, serde::Deserialize)]
struct PathQuery {
    path: String,
}

pub fn app(config: ServerConfig) -> Result<Router, String> {
    let store = Arc::new(config.object_store()?);
    let state = AppState {
        config: Arc::new(config),
        store,
    };
    Ok(Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/v1/namespaces",
            post(create_namespace).get(list_namespaces_handler),
        )
        .route(
            "/v1/namespaces/:namespace/entries",
            get(list_entries).delete(delete_entry),
        )
        .route("/v1/namespaces/:namespace/stat", get(stat_entry))
        .route(
            "/v1/namespaces/:namespace/content",
            get(get_content).put(put_content),
        )
        .route("/v1/namespaces/:namespace/move", post(move_entry))
        .with_state(state))
}

pub async fn serve(config: ServerConfig) -> Result<(), String> {
    let bind: SocketAddr = config
        .bind
        .parse()
        .map_err(|err: std::net::AddrParseError| err.to_string())?;
    let app = app(config)?;
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
    let namespace_id = NamespaceId::from(request.name);
    let summary = bootstrap_namespace(
        state.store.as_ref(),
        &namespace_id,
        &mutation_context(&state.config),
        false,
    )
    .map_err(ApiResponseError::bootstrap)?;
    Ok(Json(summary))
}

async fn list_namespaces_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<ListNamespacesResponse>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let namespaces = list_namespaces(state.store.as_ref()).map_err(ApiResponseError::core)?;
    Ok(Json(ListNamespacesResponse { namespaces }))
}

async fn list_entries(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<Vec<loon_api::AuthoritativePathEntry>>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let entries = list_path(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &query.path,
    )
    .map_err(ApiResponseError::core)?;
    Ok(Json(entries))
}

async fn stat_entry(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<loon_api::AuthoritativePathEntry>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let entry = resolve_path(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &query.path,
    )
    .map_err(ApiResponseError::core)?;
    Ok(Json(entry))
}

async fn get_content(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Response, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let file = read_file_bytes(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &query.path,
    )
    .map_err(ApiResponseError::core)?;
    Ok((StatusCode::OK, file.bytes).into_response())
}

async fn put_content(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
    body: Bytes,
) -> Result<Json<MutationResult>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let result = write_file_bytes(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &query.path,
        &body,
        &mutation_context(&state.config),
        None,
    )
    .map_err(ApiResponseError::core)?;
    Ok(Json(result))
}

async fn delete_entry(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Query(query): Query<PathQuery>,
) -> Result<Json<MutationResult>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let result = delete_path(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &query.path,
        &mutation_context(&state.config),
        None,
    )
    .map_err(ApiResponseError::core)?;
    Ok(Json(result))
}

async fn move_entry(
    State(state): State<AppState>,
    AxumPath(namespace): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<MoveEntryRequest>,
) -> Result<Json<MutationResult>, ApiResponseError> {
    authorize(&state.config, &headers)?;
    let result = move_path(
        state.store.as_ref(),
        &NamespaceId::from(namespace),
        &request.from_path,
        &request.to_path,
        &mutation_context(&state.config),
        Some(&request.request_id),
    )
    .map_err(ApiResponseError::core)?;
    Ok(Json(result))
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

fn mutation_context(config: &ServerConfig) -> MutationContext {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    MutationContext {
        writer_id: config.writer_id.clone(),
        writer_version: config.writer_version.clone(),
        now_ms,
        lease_duration_ms: config.lease_duration_ms,
    }
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

    fn bootstrap(error: BootstrapNamespaceError) -> Self {
        match error {
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

    fn core(error: CoreError) -> Self {
        match error {
            CoreError::InvalidPath(_)
            | CoreError::RootMutationForbidden
            | CoreError::NonDirectoryPathComponent(_) => {
                Self::new(StatusCode::BAD_REQUEST, "invalid_path", &error.to_string())
            }
            CoreError::MissingPath(_) | CoreError::VisiblePath(_) => {
                Self::new(StatusCode::NOT_FOUND, "not_found", &error.to_string())
            }
            CoreError::ExpectedFile { .. }
            | CoreError::ExpectedDirectory { .. }
            | CoreError::DestinationExists(_) => {
                Self::new(StatusCode::CONFLICT, "path_conflict", &error.to_string())
            }
            _ => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "server_error",
                &error.to_string(),
            ),
        }
    }
}

impl IntoResponse for ApiResponseError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}
