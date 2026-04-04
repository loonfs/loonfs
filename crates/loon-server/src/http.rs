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
    resolve_path, write_file_bytes, BootstrapNamespaceError, CoreError, CoreErrorKind,
    MutationContext,
};
use loon_objectstore::ObjectStore;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

type SharedStore = Arc<dyn ObjectStore + Send + Sync>;

#[derive(Clone)]
struct AppState {
    config: Arc<ServerConfig>,
    store: SharedStore,
}

#[derive(Debug, serde::Deserialize)]
struct PathQuery {
    path: String,
}

pub fn app(config: ServerConfig) -> Result<Router, String> {
    let store = Arc::new(config.object_store()?) as SharedStore;
    Ok(app_with_store(config, store))
}

fn app_with_store(config: ServerConfig, store: SharedStore) -> Router {
    let state = AppState {
        config: Arc::new(config),
        store,
    };
    Router::new()
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
        .with_state(state)
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
        let (status, code) = match error.kind() {
            CoreErrorKind::InvalidPath => (StatusCode::BAD_REQUEST, "invalid_path"),
            CoreErrorKind::NamespaceNotFound => (StatusCode::NOT_FOUND, "namespace_not_found"),
            CoreErrorKind::PathNotFound => (StatusCode::NOT_FOUND, "path_not_found"),
            CoreErrorKind::PathConflict => (StatusCode::CONFLICT, "path_conflict"),
            CoreErrorKind::StaleHead => (StatusCode::CONFLICT, "stale_head"),
            CoreErrorKind::StaleRevision => (StatusCode::CONFLICT, "stale_revision"),
            CoreErrorKind::TombstoneConflict => (StatusCode::CONFLICT, "tombstone_conflict"),
            CoreErrorKind::LeaseConflict => (StatusCode::CONFLICT, "lease_conflict"),
            CoreErrorKind::WouldCycle => (StatusCode::CONFLICT, "would_cycle"),
            CoreErrorKind::NamespaceCorrupt => {
                (StatusCode::INTERNAL_SERVER_ERROR, "namespace_corrupt")
            }
            CoreErrorKind::ServerError => (StatusCode::INTERNAL_SERVER_ERROR, "server_error"),
        };
        Self::new(status, code, &error.to_string())
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
    use loon_client::{Client, ClientConfig, ClientError, NamespacePath};
    use loon_core::{bootstrap_namespace, delete_path, write_file_bytes, MutationContext};
    use loon_objectstore::fs::LocalFsStore;
    use loon_objectstore::keys::namespace_head;
    use loon_objectstore::{ByteRange, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode};
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
            );
            assert_api_error(
                harness.client.delete_path(&target),
                404,
                "namespace_not_found",
            );
            let destination = NamespacePath::parse("missing:/notes/renamed.txt").expect("target");
            assert_api_error(
                harness.client.move_path(&target, &destination),
                404,
                "namespace_not_found",
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
            &"demo".into(),
            &context("server-writer", now_ms),
            false,
        )
        .expect("bootstrap namespace");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/missing.txt").expect("target");
            assert_api_error(harness.client.delete_path(&target), 404, "path_not_found");
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
        bootstrap_namespace(store.as_ref(), &"demo".into(), &context, false)
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &"demo".into(),
            "/docs/readme.txt",
            b"readme",
            &context,
            Some("seed-docs"),
        )
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &"demo".into(),
            "/tmp/a.txt",
            b"from tmp",
            &context,
            Some("seed-tmp"),
        )
        .expect("seed tmp");
        write_file_bytes(
            store.as_ref(),
            &"demo".into(),
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
            );

            let from = NamespacePath::parse("demo:/tmp/a.txt").expect("from");
            let to = NamespacePath::parse("demo:/docs/a.txt").expect("to");
            assert_api_error(harness.client.move_path(&from, &to), 409, "path_conflict");
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
        bootstrap_namespace(store.as_ref(), &"demo".into(), &context, false)
            .expect("bootstrap namespace");
        write_file_bytes(
            store.as_ref(),
            &"demo".into(),
            "/docs/old.txt",
            b"old",
            &context,
            Some("seed-docs"),
        )
        .expect("seed docs");
        write_file_bytes(
            store.as_ref(),
            &"demo".into(),
            "/tmp/source.txt",
            b"source",
            &context,
            Some("seed-source"),
        )
        .expect("seed source");
        delete_path(
            store.as_ref(),
            &"demo".into(),
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
            );

            let from = NamespacePath::parse("demo:/tmp/source.txt").expect("from");
            let to = NamespacePath::parse("demo:/docs/source.txt").expect("to");
            assert_api_error(
                harness.client.move_path(&from, &to),
                409,
                "tombstone_conflict",
            );
        })
        .await
        .expect("join blocking task");

        harness.server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_stale_head_conflict_surfaces_as_409() {
        let temp_dir = tempdir().expect("tempdir");
        let store = Arc::new(StaleHeadOnceStore::new(temp_dir.path(), "demo")) as SharedStore;
        let now_ms = now_ms();
        bootstrap_namespace(
            store.as_ref(),
            &"demo".into(),
            &context("server-writer", now_ms),
            false,
        )
        .expect("bootstrap namespace");

        let harness = start_server(store, temp_dir.path(), "server-writer").await;
        tokio::task::spawn_blocking(move || {
            let target = NamespacePath::parse("demo:/notes/race.txt").expect("target");
            assert_api_error(
                harness.client.write_file_bytes(&target, b"race"),
                409,
                "stale_head",
            );
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
            &"demo".into(),
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
        let router = app_with_store(config, store);
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
    ) {
        match result {
            Err(ClientError::Api {
                status: actual_status,
                code: actual_code,
                ..
            }) => {
                assert_eq!(actual_status, status);
                assert_eq!(actual_code, code);
            }
            other => panic!("expected api error {status} {code}, got {other:?}"),
        }
    }
}
