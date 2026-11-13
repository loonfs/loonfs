//! Namespace lifecycle, status, and capability discovery handlers.

use super::error::ApiResponseError;
use super::extractors::{ActorHeader, SubjectHeaders};
use super::handlers_uploads::{direct_multipart_issuer, direct_put_issuer};
use super::query_params::required_query_param;

use super::{AppJson, AppQuery, HttpState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{CreateNamespaceOptions, DeleteNamespaceOptions, MergeForkOptions};
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    CapabilityDocument, Commit, CommitSeq, CreateNamespaceRequest, ForkNamespaceForRebaseRequest,
    ForkNamespaceRequest, MergeForkRequest, NamespaceId, NextMergeRangeResponse, ReplayForkRequest,
    ReplayForkResponse, API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0, API_GROUP_QUERY_V0,
    FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_MAINTENANCE_GREP_INDEX, FEATURE_QUERY_GREP,
    FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES, LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
};
use std::sync::Arc;

/// The JSON Pointers of the merge and replay request fields that a rejection can name.
const MERGE_BODY_FIELDS: &[&str] = &["/source_namespace_id", "/through_seq"];

/// The JSON Pointers of the rebase request fields that a rejection can name.
const REBASE_BODY_FIELDS: &[&str] = &["/source_namespace_id", "/new_namespace_id"];

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
    expected_head_seq: Option<CommitSeq>,
}

/// The query of the next-range query.
#[derive(Debug, serde::Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::IntoParams))]
#[cfg_attr(feature = "openapi", into_params(parameter_in = Query))]
#[serde(deny_unknown_fields)]
pub(super) struct NextMergeRangeQuery {
    /// The fork whose next merge into the namespace the query reads. It has to be a fork of the
    /// namespace, or the fork that a rebase replays into the namespace.
    #[cfg_attr(feature = "openapi", param(required = true))]
    source: Option<NamespaceId>,
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
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["capabilities"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
        ),
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
    State(state): State<Arc<HttpState>>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::CapabilityDocument>, ApiResponseError> {
    let mut capabilities = state.runtime.capabilities();
    // Each direct transport is advertised from the issuer that performs it,
    // so a provider that signs whole-object writes but has no multipart API
    // says exactly that. The read is advertised for the bundle as a whole:
    // a deployment that lets a client create an object too large to proxy
    // back has to be able to hand it back.
    let direct_put = direct_put_issuer(&state);
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_PUT,
        direct_put.is_some(),
    );
    set_feature(
        &mut capabilities,
        FEATURE_UPLOADS_DIRECT_MULTIPART,
        direct_multipart_issuer(&state).is_some(),
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
        state.options.serves_maintenance && state.grep_worker.is_some(),
    );
    if state.grep_service.is_some() {
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
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("create")),
        ),
        path = "/v0/namespaces",
        tag = "namespaces",
        summary = "Create namespace",
        description = "Creates a new empty namespace. Without `namespace_id` in the request, LoonFS generates an ID. The response's `namespace_id` is the new namespace's ID.",
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
    State(state): State<Arc<HttpState>>,
    ActorHeader(actor_id): ActorHeader,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let namespace = state
        .runtime
        .create_namespace_with_options(
            &request.namespace_id.unwrap_or_else(NamespaceId::generate),
            &actor_id,
            &CreateNamespaceOptions {
                access: request.access,
                retention_window_ms: request.retention_window_ms,
                ..Default::default()
            },
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime(error).with_body_field_params(&["/access/root_grants"])
        })?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_namespace",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
            ("x-fern-request-name" = json!("GetNamespaceRequest")),
        ),
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
    State(state): State<Arc<HttpState>>,
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
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["maintenance", "diagnostics"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
            ("x-fern-request-name" = json!("GetNamespaceDiagnosticsRequest")),
        ),
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
    State(state): State<Arc<HttpState>>,
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
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("delete")),
            ("x-fern-request-name" = json!("DeleteNamespaceRequest")),
        ),
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
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<DeleteNamespaceQuery>,
) -> Result<Json<loonfs_types::DeleteNamespaceResponse>, ApiResponseError> {
    let options = DeleteNamespaceOptions {
        expected_head_seq: query.expected_head_seq,
    };
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let response = namespace
        .delete_with_options(&options)
        .await
        .map_err(ApiResponseError::runtime)?;
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
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("fork")),
        ),
        path = "/v0/namespaces/{namespace_id}/forks",
        tag = "namespaces",
        summary = "Fork namespace",
        description = "Creates a new namespace from the source namespace's current head or from an unexpired snapshot. Without `new_namespace_id` in the request, LoonFS generates an ID. The response's `namespace_id` is the new namespace's ID.",
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
    State(state): State<Arc<HttpState>>,
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
            &request
                .new_namespace_id
                .unwrap_or_else(NamespaceId::generate),
            &actor_id,
            &loonfs::ForkNamespaceOptions {
                snapshot_id: request.snapshot_id,
            },
        )
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(namespace))
}

/// Merges a fork into the namespace. The server stores the `Loonfs-Actor`
/// header, and the shared token doesn't verify the header. A runtime whose
/// merge allowance is zero returns `not_supported` for `filesystem.merges`.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "merge_fork",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-retry" = json!("replayable")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("merge")),
        ),
        path = "/v0/namespaces/{namespace_id}/merges",
        tag = "namespaces",
        summary = "Merge a fork",
        description = "Merges a fork's commits into the namespace as one commit. The merge applies the fork's net change since its previous merge, or since the fork point, through `through_seq` or the fork's head. A change that conflicts with the namespace returns `merge_conflict`, and nothing is committed. The commit ID makes retries idempotent. The merge's WAL object counts toward the namespace's fold triggers like any commit.",
        params(("namespace_id" = String, Path, description = "The namespace that the merge commits into.")),
        request_body = MergeForkRequest,
        responses(
            (status = 200, description = "Fork merged", body = Commit),
            (status = 400, description = "Invalid request, or the namespace isn't a fork of this namespace", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "The namespace or the fork doesn't exist, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 409, description = "Merge conflict, range conflict, or history below a retention floor", body = ErrorResponse),
            (status = 410, description = "The namespace or the fork was deleted, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 413, description = "The merge is too large, or the JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Merges are disabled", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn merge_fork(
    State(state): State<Arc<HttpState>>,
    ActorHeader(actor_id): ActorHeader,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<MergeForkRequest>,
) -> Result<Json<Commit>, ApiResponseError> {
    let MergeForkRequest {
        source_namespace_id,
        through_seq,
        commit_id,
        message,
    } = request;
    let namespace = state
        .open_writer(&namespace_id, subject)
        .await
        .map_err(|error| error.with_commit_id(&commit_id))?;
    let options = MergeForkOptions {
        commit_id: Some(commit_id.clone()),
        message,
        through_seq,
    };
    let response = namespace
        .merge_fork_with_options(&source_namespace_id, &actor_id, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime(error)
                .with_body_field_params(MERGE_BODY_FIELDS)
                .with_commit_id(&commit_id)
        })?;
    Ok(Json(response))
}

/// Returns where a fork's next merge into the namespace starts. A request
/// with a subject needs an administrator in both namespaces, as the change
/// feed does. A runtime whose merge allowance is zero returns
/// `not_supported` for `filesystem.merges` before it reads a namespace.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_next_merge_range",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("nextMergeRange")),
            ("x-fern-request-name" = json!("GetNextMergeRangeRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/merges",
        tag = "namespaces",
        summary = "Get a fork's next merge range",
        description = "Returns where the fork's next merge into the namespace starts, without planning the merge: the fork's newest merge into the namespace, the fork sequence that the next merge starts after, the fork point, and the fork's head. When the namespace is a rebase's new fork for the fork, the range continues the new fork's replay commits. When a retention floor would refuse the merge with `below_retention_floor`, the response's `outcome` says which floor instead of failing.",
        params(
            ("namespace_id" = String, Path, description = "The namespace that the fork merges into."),
            NextMergeRangeQuery
        ),
        responses(
            (status = 200, description = "The fork's next merge range", body = NextMergeRangeResponse),
            (status = 400, description = "Invalid request, or the source isn't a fork of this namespace", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "The namespace or the fork doesn't exist, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 410, description = "The namespace or the fork was deleted, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 501, description = "Merges are disabled", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_next_merge_range(
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<NextMergeRangeQuery>,
) -> Result<Json<NextMergeRangeResponse>, ApiResponseError> {
    let source_namespace_id = required_query_param(query.source, "source")?;
    let response = state
        .runtime_for(subject)
        .namespace(&namespace_id)
        .next_merge_range(&source_namespace_id)
        .await
        .map_err(|error| {
            // The runtime names the source `source_namespace_id`, which this
            // route reads from the `source` parameter.
            let source_refused =
                error.invalid_request_param().as_deref() == Some("source_namespace_id");
            let response = ApiResponseError::runtime(error);
            if source_refused {
                response.with_param("source")
            } else {
                response
            }
        })?;
    Ok(Json(response))
}

/// Creates a rebase's new fork: a new fork of the namespace's head for a
/// fork of the namespace, which the fork's replay commits then go into. The
/// server stores the `Loonfs-Actor` header, and the shared token doesn't
/// verify the header. A runtime whose merge allowance is zero returns
/// `not_supported` for `filesystem.merges` before it reads or creates a
/// namespace.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "fork_namespace_for_rebase",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("forkForRebase")),
        ),
        path = "/v0/namespaces/{namespace_id}/rebases",
        tag = "namespaces",
        summary = "Fork a namespace for a rebase",
        description = "Creates a rebase's new fork from the namespace's head, for a fork of the namespace whose unmerged commits the replay route then replays into the new fork. Without `new_namespace_id` in the request, LoonFS generates an ID. The response's `namespace_id` is the new fork's ID, and its `fork_basis.rebased_from` identifies the fork. A request whose new fork already exists returns `namespace_exists`.",
        params(("namespace_id" = String, Path, description = "The namespace that the fork was forked from.")),
        request_body = ForkNamespaceForRebaseRequest,
        responses(
            (status = 200, description = "New fork created", body = loonfs_types::NamespaceMetadata),
            (status = 400, description = "Invalid request, or the source isn't a fork of this namespace", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "The namespace or the fork doesn't exist, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 409, description = "The new fork already exists, or history is below a retention floor", body = ErrorResponse),
            (status = 410, description = "The namespace or the fork was deleted, or the new fork's ID was deleted and can't be used again, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Merges are disabled", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn fork_namespace_for_rebase(
    State(state): State<Arc<HttpState>>,
    ActorHeader(actor_id): ActorHeader,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ForkNamespaceForRebaseRequest>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let namespace = state
        .runtime_for(subject)
        .fork_namespace_for_rebase(
            &namespace_id,
            &request.source_namespace_id,
            &request
                .new_namespace_id
                .unwrap_or_else(NamespaceId::generate),
            &actor_id,
        )
        .await
        .map_err(|error| {
            ApiResponseError::runtime(error).with_body_field_params(REBASE_BODY_FIELDS)
        })?;
    Ok(Json(namespace))
}

/// Replays a fork's unmerged commits into the namespace, a rebase's new fork
/// for the fork, as one replay commit. The server stores the `Loonfs-Actor`
/// header, and the shared token doesn't verify the header. A runtime whose
/// merge allowance is zero returns `not_supported` for `filesystem.merges`
/// before it reads a namespace.
#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "replay_fork",
        extensions(
            ("x-loonfs-actor" = json!("required")),
            ("x-loonfs-retry" = json!("replayable")),
            ("x-fern-sdk-group-name" = json!(["namespaces"])),
            ("x-fern-sdk-method-name" = json!("replay")),
        ),
        path = "/v0/namespaces/{namespace_id}/replays",
        tag = "namespaces",
        summary = "Replay a fork into a rebase's new fork",
        description = "Replays a fork's unmerged commits into the namespace, a rebase's new fork for the fork, as one replay commit, which applies the changes that don't conflict with the new fork and reports the ones that do. Each request continues after the new fork's previous replay commit. The commit ID makes retries idempotent, and a retry of a committed replay commit returns the new fork without the conflict report.",
        params(("namespace_id" = String, Path, description = "The rebase's new fork, which the replay commit commits into.")),
        request_body = ReplayForkRequest,
        responses(
            (status = 200, description = "Fork replayed", body = ReplayForkResponse),
            (status = 400, description = "Invalid request, or the namespace isn't a rebase's new fork for the fork", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 403, description = "Forbidden", body = ErrorResponse),
            (status = 404, description = "The new fork or the fork doesn't exist, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 409, description = "Range conflict, or history below a retention floor", body = ErrorResponse),
            (status = 410, description = "The new fork or the fork was deleted, and `details.namespace_id` identifies which", body = ErrorResponse),
            (status = 413, description = "The replay commit is too large, or the JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            (status = 501, description = "Merges are disabled", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn replay_fork(
    State(state): State<Arc<HttpState>>,
    ActorHeader(actor_id): ActorHeader,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ReplayForkRequest>,
) -> Result<Json<ReplayForkResponse>, ApiResponseError> {
    let ReplayForkRequest {
        source_namespace_id,
        through_seq,
        commit_id,
        message,
    } = request;
    let new_fork = state
        .open_writer(&namespace_id, subject)
        .await
        .map_err(|error| error.with_commit_id(&commit_id))?;
    let options = MergeForkOptions {
        commit_id: Some(commit_id.clone()),
        message,
        through_seq,
    };
    let response = new_fork
        .replay_fork_with_options(&source_namespace_id, &actor_id, &options)
        .await
        .map_err(|error| {
            ApiResponseError::runtime(error)
                .with_body_field_params(MERGE_BODY_FIELDS)
                .with_commit_id(&commit_id)
        })?;
    Ok(Json(response))
}
