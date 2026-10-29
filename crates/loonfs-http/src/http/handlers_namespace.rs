//! Namespace lifecycle, status, and capability discovery handlers.

use super::error::ApiResponseError;
use super::extractors::{ActorHeader, SubjectHeaders};

use super::{AppJson, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{CreateNamespaceOptions, DeleteNamespaceOptions};
use loonfs_types::ChangeSeq;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    CapabilityDocument, CreateNamespaceRequest, ForkNamespaceRequest, API_GROUP_FILESYSTEM_V0,
    API_GROUP_MAINTENANCE_V0, API_GROUP_QUERY_V0, FEATURE_DOWNLOADS_DIRECT_GET,
    FEATURE_MAINTENANCE_GREP_INDEX, FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART,
    FEATURE_UPLOADS_DIRECT_PUT, LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES,
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
};
use std::sync::Arc;

/// Advertises a feature, or removes the key: an absent key and an
/// advertised-false key both mean unsupported, and the document stays small.
fn set_feature(capabilities: &mut CapabilityDocument, feature: &str, supported: bool) {
    if supported {
        capabilities.features.insert(feature.to_owned(), true);
    } else {
        capabilities.features.remove(feature);
    }
}

/// The query of a namespace delete.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct DeleteNamespaceQuery {
    /// The head sequence that the namespace must still have for the delete to succeed.
    expected_head_seq: Option<ChangeSeq>,
}

/// The capability value for a transfer limit. The API lists these limits on
/// every deployment, so no limit is advertised as `u64::MAX`.
fn advertised_limit(limit: Option<u64>) -> u64 {
    limit.unwrap_or(u64::MAX)
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_capabilities",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/capabilities",
        tag = "system",
        summary = "Get capabilities",
        description = "Returns a summary of supported features and limits.",
        responses(
            (status = 200, description = "Capability document", body = loonfs_types::CapabilityDocument),
            (status = 400, description = "Unknown query parameter", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_capabilities(
    State(state): State<Arc<BindingState>>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::CapabilityDocument>, ApiResponseError> {
    let mut capabilities = state.runtime.capabilities();
    // Each direct transport is advertised from the issuer that performs it,
    // so a provider that signs whole-object writes but has no multipart API
    // says exactly that. The read is advertised for the bundle as a whole:
    // a deployment that lets a client create an object too large to proxy
    // back has to be able to hand it back.
    let direct_put = state
        .direct_transfers
        .as_ref()
        .and_then(|transfers| transfers.put.as_ref());
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_PUT,
        direct_put.is_some(),
    );
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_MULTIPART,
        state
            .direct_transfers
            .as_ref()
            .is_some_and(|transfers| transfers.multipart.is_some()),
    );
    set_feature(
        &mut capabilities,
        FEATURE_DOWNLOADS_DIRECT_GET,
        state.direct_transfers.is_some(),
    );
    if let Some(issuer) = direct_put {
        capabilities.limits.insert(
            LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES.to_owned(),
            issuer.max_content_bytes(),
        );
    }
    capabilities.limits.insert(
        LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES.to_owned(),
        advertised_limit(state.options.max_upload_bytes),
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES.to_owned(),
        super::MAX_COMPLETION_BODY_BYTES as u64,
    );
    capabilities.limits.insert(
        LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES.to_owned(),
        super::MAX_JSON_BODY_BYTES as u64,
    );
    capabilities.limits.insert(
        LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES.to_owned(),
        advertised_limit(state.options.max_download_bytes),
    );
    capabilities.limits.insert(
        LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS.to_owned(),
        state.options.max_concurrent_uploads as u64,
    );
    capabilities.limits.insert(
        LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS.to_owned(),
        state.options.max_concurrent_downloads as u64,
    );
    // The HTTP deployment replaces the embedded document's API groups with
    // the routes this router mounts. Serving searches and maintaining their
    // index remain separately deployable.
    capabilities.api_groups = vec![API_GROUP_FILESYSTEM_V0.to_owned()];
    if state.options.serves_maintenance {
        capabilities
            .api_groups
            .push(API_GROUP_MAINTENANCE_V0.to_owned());
    }
    set_feature(
        &mut capabilities,
        FEATURE_MAINTENANCE_GREP_INDEX,
        state.options.serves_maintenance && state.grep.maintains(),
    );
    if state.grep.serves() {
        capabilities.api_groups.push(API_GROUP_QUERY_V0.to_owned());
        capabilities
            .features
            .insert(FEATURE_QUERY_GREP.to_owned(), true);
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_SCAN_BUDGET_FILES.to_owned(),
            loonfs_grep::MAX_GREP_SCAN_FILES as u64,
        );
        capabilities.limits.insert(
            LIMIT_QUERY_GREP_TAIL_BUDGET_FILES.to_owned(),
            loonfs_grep::MAX_GREP_TAIL_FILES as u64,
        );
    }
    Ok(Json(capabilities))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_namespace",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-retry" = json!("not_idempotent")),
        ),
        path = "/v0/namespaces",
        tag = "namespaces",
        summary = "Create namespace",
        description = "Creates a new empty namespace.",
        request_body = CreateNamespaceRequest,
        responses(
            (status = 200, description = "Namespace created", body = loonfs_types::NamespaceMetadata),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 409, description = "Namespace already exists", body = ErrorResponse),
            (status = 410, description = "The namespace ID was deleted and can't be used again (`namespace_deleted`)", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_namespace(
    State(state): State<Arc<BindingState>>,
    ActorHeader(actor_id): ActorHeader,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let namespace = state
        .runtime
        .create_namespace_with_options(
            &request.namespace_id,
            &actor_id,
            &CreateNamespaceOptions {
                access: request.access,
                retention_window_ms: request.retention_window_ms,
                ..Default::default()
            },
        )
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}",
        tag = "namespaces",
        summary = "Get namespace",
        description = "Returns the current head and retention state for a namespace.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        responses(
            (status = 200, description = "Namespace", body = loonfs_types::NamespaceMetadata),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_namespace(
    State(state): State<Arc<BindingState>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let namespace = state.runtime.namespace(&namespace_id);
    let response = namespace
        .metadata()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace_diagnostics",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
        tag = "maintenance",
        summary = "Get namespace diagnostics",
        description = "Returns namespace state together with the current manifest and visible WAL tail.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        responses(
            (status = 200, description = "Namespace diagnostics", body = loonfs_types::NamespaceDiagnostics),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_namespace_diagnostics(
    State(state): State<Arc<BindingState>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::NamespaceDiagnostics>, ApiResponseError> {
    let diagnostics = state
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(diagnostics))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        operation_id = "delete_namespace",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/namespaces/{namespace_id}",
        tag = "namespaces",
        summary = "Delete namespace",
        description = "Marks a namespace as deleted. The namespace ID can never be used again to create or fork a namespace.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            DeleteNamespaceQuery
        ),
        responses(
            (status = 200, description = "Namespace deleted", body = loonfs_types::DeleteNamespaceResponse),
            (status = 400, description = "Invalid request", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 409, description = "Delete conflict", body = ErrorResponse),
            (status = 410, description = "Namespace already deleted", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn delete_namespace(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<DeleteNamespaceQuery>,
) -> Result<Json<loonfs_types::DeleteNamespaceResponse>, ApiResponseError> {
    let options = DeleteNamespaceOptions {
        expected_head_seq: query.expected_head_seq,
    };
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let response = namespace.delete_with_options(&options).await.map_err(
        ApiResponseError::for_namespace_writer(state.namespaces.as_ref(), &namespace_id),
    )?;
    state.namespaces.forget(&namespace_id);
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "fork_namespace",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-retry" = json!("not_idempotent")),
        ),
        path = "/v0/namespaces/{namespace_id}/forks",
        tag = "namespaces",
        summary = "Fork namespace",
        description = "Creates a new namespace from the source namespace's current head or from an unexpired snapshot.",
        params(("namespace_id" = String, Path, description = "The source namespace ID.")),
        request_body = ForkNamespaceRequest,
        responses(
            (status = 200, description = "Namespace forked", body = loonfs_types::NamespaceMetadata),
            (status = 400, description = "Invalid namespace ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Source namespace not found, or the snapshot is missing or expired", body = ErrorResponse),
            (status = 409, description = "Fork conflict", body = ErrorResponse),
            (status = 410, description = "The source or target namespace ID was deleted and can't be used again (`namespace_deleted`)", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn fork_namespace(
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    ActorHeader(actor_id): ActorHeader,
    NamespaceIdPath(source_namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ForkNamespaceRequest>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime
        .fork_namespace_with_options(
            &source_namespace_id,
            &request.new_namespace_id,
            &actor_id,
            &loonfs::ForkNamespaceOptions {
                snapshot_id: request.snapshot_id,
            },
        )
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(namespace))
}
