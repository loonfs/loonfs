//! Namespace lifecycle, status, capability discovery, and maintenance
//! handlers.

use super::error::ApiResponseError;
use super::extractors::{missing_actor, ActorHeader, OptionalActorHeader, SubjectHeaders};
use super::handlers_filesystem::PageQuery;
use super::handlers_query::map_grep_error;
use super::query_params::{
    checked_cursor, effective_page_limit, parse_path_id, parse_public_ordinal,
};
use super::{AppJson, AppPath, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{
    AdvanceRetentionFloorOptions, CheckpointPageCursor, CreateNamespaceOptions,
    DeleteNamespaceOptions, GcOptions, MaintainMetadataOptions, SnapshotPageCursor,
};
use loonfs_types::ChangeSeq;
#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    AccessRight, CapabilityDocument, Checkpoint, CreateNamespaceRequest, CreateSnapshotRequest,
    DeleteSnapshotResponse, ErrorCode, ExtendSnapshotRequest, ForkNamespaceRequest,
    ListCheckpointsResponse, ListSnapshotsResponse, NamespaceAccess, PinId, RunMaintenanceRequest,
    RunMaintenanceResponse, Snapshot, API_GROUP_FILESYSTEM_V0, API_GROUP_MAINTENANCE_V0,
    API_GROUP_QUERY_V0, FEATURE_DOWNLOADS_DIRECT_GET, FEATURE_MAINTENANCE_GREP_INDEX,
    FEATURE_QUERY_GREP, FEATURE_UPLOADS_DIRECT_MULTIPART, FEATURE_UPLOADS_DIRECT_PUT,
    LIMIT_COMMIT_MAX_REQUEST_BODY_BYTES, LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_DOWNLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES, LIMIT_QUERY_GREP_SCAN_BUDGET_FILES,
    LIMIT_QUERY_GREP_TAIL_BUDGET_FILES, LIMIT_UPLOAD_COMPLETE_MAX_REQUEST_BODY_BYTES,
    LIMIT_UPLOAD_DIRECT_PUT_MAX_CONTENT_BYTES,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONCURRENT_REQUESTS,
    LIMIT_UPLOAD_SERVICE_PROXIED_MAX_CONTENT_BYTES,
};
use std::num::NonZeroU64;

/// Advertises a feature, or removes the key: an absent key and an
/// advertised-false key both mean unsupported, and the document stays small.
fn set_feature(capabilities: &mut CapabilityDocument, feature: &str, supported: bool) {
    if supported {
        capabilities.features.insert(feature.to_owned(), true);
    } else {
        capabilities.features.remove(feature);
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeleteNamespaceQuery {
    /// Delete only if the head is still at this sequence (`stale_head` on
    /// mismatch).
    expected_head_seq: Option<String>,
}

/// Reads the `retention_window_ms` field of a request body. A window must be
/// positive, so zero is an invalid request.
fn retention_window_ms(value: Option<u64>) -> Result<Option<NonZeroU64>, ApiResponseError> {
    value
        .map(|value| {
            NonZeroU64::new(value).ok_or_else(|| {
                ApiResponseError::new(
                    ErrorCode::InvalidRequest,
                    "`retention_window_ms` must be greater than zero",
                )
                .with_param("/retention_window_ms")
            })
        })
        .transpose()
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
    State(state): State<BindingState>,
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
    State(state): State<BindingState>,
    ActorHeader(actor_id): ActorHeader,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateNamespaceRequest>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    if let NamespaceAccess::Acl { root_grants, .. } = &request.access {
        if !root_grants
            .iter()
            .any(|(_, rights)| rights.contains(AccessRight::Admin))
        {
            return Err(ApiResponseError::new(
                ErrorCode::InvalidRequest,
                "an ACL namespace needs at least one administrator in root_grants",
            )
            .with_param("/access/root_grants"));
        }
    }
    let namespace = state
        .runtime
        .create_namespace_with_options(
            &request.namespace_id,
            &actor_id,
            &CreateNamespaceOptions {
                access: request.access,
                retention_window_ms: retention_window_ms(request.retention_window_ms)?,
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
    State(state): State<BindingState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::NamespaceMetadata>, ApiResponseError> {
    let namespace = state.runtime.namespace(&namespace_id);
    let response = namespace
        .metadata()
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
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
    State(state): State<BindingState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<loonfs_types::NamespaceDiagnostics>, ApiResponseError> {
    let diagnostics = state
        .maintenance
        .diagnostics(&namespace_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
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
            ("expected_head_seq" = Option<ChangeSeq>, Query, description = "The head sequence that the namespace must still have for the delete to succeed.")
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
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<DeleteNamespaceQuery>,
) -> Result<Json<loonfs_types::DeleteNamespaceResponse>, ApiResponseError> {
    let options = DeleteNamespaceOptions {
        expected_head_seq: query
            .expected_head_seq
            .as_deref()
            .map(parse_expected_head_seq)
            .transpose()?,
    };
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let response = namespace.delete_with_options(&options).await.map_err(
        ApiResponseError::for_namespace_writer(&state.namespaces, &namespace_id),
    )?;
    state.namespaces.forget(&namespace_id);
    Ok(Json(response))
}

fn parse_expected_head_seq(value: &str) -> Result<ChangeSeq, ApiResponseError> {
    parse_public_ordinal("expected_head_seq", value, ChangeSeq::parse)
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
    State(state): State<BindingState>,
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
        .map_err(ApiResponseError::for_namespace(&source_namespace_id))?;
    Ok(Json(namespace))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_snapshot",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "Create snapshot",
        description = "Creates a snapshot of the current namespace state. The server first folds any WAL tail after the current manifest. Every accepted call creates a new snapshot with a new ID, and the name is a label, not a key. A snapshot with `ttl_ms` expires that many milliseconds from the server's current time, and a snapshot without it doesn't expire. The snapshot keeps its manifest until it's deleted, either explicitly or by garbage collection after it expires and its grace window passes.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        request_body = CreateSnapshotRequest,
        responses(
            (status = 200, description = "Snapshot created", body = Snapshot),
            (status = 400, description = "Invalid namespace ID, name, or ttl", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn create_snapshot(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateSnapshotRequest>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let snapshot = namespace
        .create_snapshot(
            &request.name,
            &loonfs::CreateSnapshotOptions {
                ttl_ms: request.ttl_ms,
            },
        )
        .await
        .map_err(ApiResponseError::for_namespace_writer(
            &state.namespaces,
            &namespace_id,
        ))?;
    Ok(Json(snapshot))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_snapshots",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "List snapshots",
        description = "Lists snapshots in snapshot ID order. An expired snapshot stays in the list until garbage collection deletes it, after its expiry plus the grace window. The list leaves out fork pins, which the maintenance checkpoint inventory lists.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned.")
        ),
        responses(
            (status = 200, description = "Snapshots", body = ListSnapshotsResponse),
            (status = 400, description = "Invalid namespace ID, limit, or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_snapshots(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListSnapshotsResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let cursor = checked_cursor::<SnapshotPageCursor>(query.cursor)?;
    let response = namespace
        .list_snapshots()
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(cursor)
        .page()
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "extend_snapshot",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
        tag = "namespaces",
        summary = "Extend snapshot",
        description = "Moves an unexpired snapshot's expiry to `ttl_ms` from the server's current time. The expiry never moves earlier, so a snapshot without an expiry is returned unchanged. A missing or expired snapshot returns `snapshot_not_found`. Repeating the request has the same result.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::PinId, Path, description = "The snapshot ID.")
        ),
        request_body = ExtendSnapshotRequest,
        responses(
            (status = 200, description = "Snapshot extended", body = Snapshot),
            (status = 400, description = "Invalid ID or ttl", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Snapshot not found or expired", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn extend_snapshot(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ExtendSnapshotRequest>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let snapshot_id = super::query_params::parse_snapshot_id(&snapshot_id)?;
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let response = namespace
        .extend_snapshot(&snapshot_id, request.ttl_ms)
        .await
        .map_err(ApiResponseError::for_namespace_writer(
            &state.namespaces,
            &namespace_id,
        ))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_snapshot",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        tag = "namespaces",
        summary = "Retrieve snapshot",
        description = "Returns one snapshot, as the snapshot listing returns it, including an expired snapshot that garbage collection hasn't deleted. A missing ID, or the ID of a fork pin, returns `snapshot_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::PinId, Path, description = "The snapshot ID.")
        ),
        responses(
            (status = 200, description = "Snapshot", body = Snapshot),
            (status = 400, description = "Invalid namespace or snapshot ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or snapshot not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_snapshot(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let snapshot_id = super::query_params::parse_snapshot_id(&snapshot_id)?;
    let runtime = state.runtime_for(subject);
    let snapshot = runtime
        .namespace(&namespace_id)
        .get_snapshot(&snapshot_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(snapshot))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        operation_id = "delete_snapshot",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        tag = "namespaces",
        summary = "Delete snapshot",
        description = "Deletes a snapshot. Garbage collection can then reclaim its unreferenced manifest and runs. Deleting a missing snapshot succeeds, so repeating the request has the same result. The ID of a fork pin returns `snapshot_not_found`. Deletion stays available after the namespace is deleted.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::PinId, Path, description = "The snapshot ID.")
        ),
        responses(
            (status = 200, description = "Snapshot deleted, or already absent", body = DeleteSnapshotResponse),
            (status = 400, description = "Invalid namespace or snapshot ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found, or the ID belongs to a fork pin", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn delete_snapshot(
    State(state): State<BindingState>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<DeleteSnapshotResponse>, ApiResponseError> {
    let snapshot_id = super::query_params::parse_snapshot_id(&snapshot_id)?;
    let namespace = match state.namespaces.open(&namespace_id).await {
        // The session registry opens sessions for active namespaces only, but
        // a snapshot without an expiry has to stay deletable after its
        // namespace is deleted. Deleting a pin publishes nothing, so a
        // session outside the registry is enough.
        Err(error) if error.code() == ErrorCode::NamespaceDeleted => state
            .runtime
            .open_namespace(&namespace_id)
            .map_err(ApiResponseError::for_namespace(&namespace_id))?,
        opened => opened.map_err(ApiResponseError::for_namespace(&namespace_id))?,
    };
    let scoped_namespace = subject.map(|subject| namespace.with_subject(subject));
    let namespace = scoped_namespace.as_ref().unwrap_or(&namespace);
    let response = namespace.delete_snapshot(&snapshot_id).await.map_err(
        ApiResponseError::for_namespace_writer(&state.namespaces, &namespace_id),
    )?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct SnapshotPathParams {
    snapshot_id: String,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_checkpoints",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
        tag = "maintenance",
        summary = "List checkpoints",
        description = "Lists every pin, snapshots and fork pins, in checkpoint ID order. This is the inventory of what keeps manifests from garbage collection. An expired snapshot stays in the list until garbage collection deletes it, after its expiry plus the grace window. A cursor continues the listing against the current set of pins.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("limit" = inline(Option<super::query_params::OpenApiPageLimit>), Query, description = "The maximum page size."),
            ("cursor" = Option<String>, Query, description = "The opaque cursor that the previous page returned.")
        ),
        responses(
            (status = 200, description = "Existing checkpoint objects", body = ListCheckpointsResponse),
            (status = 400, description = "Invalid namespace ID, limit, or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_checkpoints(
    State(state): State<BindingState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListCheckpointsResponse>, ApiResponseError> {
    let cursor = checked_cursor::<CheckpointPageCursor>(query.cursor)?;
    let response = state
        .maintenance
        .list_checkpoints(&namespace_id)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(cursor)
        .page()
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_checkpoint",
        extensions(("x-loonfs-retry" = json!("idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
        tag = "maintenance",
        summary = "Retrieve checkpoint",
        description = "Returns one existing pin, as the checkpoint listing returns it, including an expired snapshot that garbage collection hasn't deleted. A missing ID returns `snapshot_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("checkpoint_id" = String, Path, description = "The checkpoint ID.")
        ),
        responses(
            (status = 200, description = "Existing checkpoint", body = Checkpoint),
            (status = 400, description = "Invalid namespace or checkpoint ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or checkpoint not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_checkpoint(
    State(state): State<BindingState>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(CheckpointPathParams { checkpoint_id }): AppPath<CheckpointPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<Checkpoint>, ApiResponseError> {
    let checkpoint_id = parse_path_id::<PinId>("checkpoint_id", &checkpoint_id)?;
    let checkpoint = state
        .maintenance
        .get_checkpoint(&namespace_id, &checkpoint_id)
        .await
        .map_err(ApiResponseError::for_namespace(&namespace_id))?;
    Ok(Json(checkpoint))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct CheckpointPathParams {
    checkpoint_id: String,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "run_maintenance",
        extensions(("x-loonfs-retry" = json!("not_idempotent"))),
        path = "/v0/maintenance/namespaces/{namespace_id}/runs",
        tag = "maintenance",
        summary = "Run one maintenance job",
        description = "Runs one maintenance job for the namespace. The body's `kind` selects the job, which is one of `metadata`, `gc`, `retention`, `retention_window`, or `recover_administrator`. The response includes the same `kind` and the job's result. A deleted namespace accepts only `gc`. A `metadata` job folds the WAL tail and compacts metadata until nothing is due, or until it publishes 16 compaction units. A compaction unit can take minutes, so the call can too. When the result's `outcome` is `due`, another `metadata` run continues the work. A `gc` job reads the current manifest, lists the pins, and then sweeps every object family to the end. Each listing starts at the beginning, and the job doesn't keep a position to continue from. When the deployment maintains a grep index, the job then deletes aged, unreferenced grep index objects, and the result includes their counts in `extensions.grep`. A `retention` job advances the retention floor to the folded manifest head, or to the one target that `to_seq` or `cutoff_at_ms` sets. A `retention_window` job sets the namespace's retention window to `retention_window_ms`, or clears it when the body doesn't include that field.",
        params(("namespace_id" = String, Path, description = "The namespace ID.")),
        request_body(content = RunMaintenanceRequest, description = "The maintenance job to run"),
        responses(
            (status = 200, description = "Maintenance job completed", body = RunMaintenanceResponse),
            (status = 400, description = "Invalid namespace ID or options", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            (status = 410, description = "Namespace deleted", body = ErrorResponse),
            (status = 413, description = "JSON body exceeds the 2 MiB limit", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn run_maintenance(
    State(state): State<BindingState>,
    OptionalActorHeader(actor_id): OptionalActorHeader,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<RunMaintenanceRequest>,
) -> Result<Json<RunMaintenanceResponse>, ApiResponseError> {
    let maintenance = &state.maintenance;
    let response = match request {
        RunMaintenanceRequest::Metadata(request) => {
            let options = MaintainMetadataOptions::from_request(request)
                .map_err(ApiResponseError::for_namespace(&namespace_id))?;
            maintenance
                .maintain_metadata_while_due_with_options(
                    &namespace_id,
                    &loonfs::MaintenanceCancellation::new(),
                    &options,
                )
                .await
                .map(RunMaintenanceResponse::Metadata)
        }
        RunMaintenanceRequest::Gc(request) => {
            return crate::gc_namespace(
                maintenance,
                state.grep.worker(),
                &namespace_id,
                &GcOptions::from_request(request),
            )
            .await
            .map(|collected| Json(RunMaintenanceResponse::Gc(collected)))
            .map_err(|error| map_grep_error(&namespace_id, error));
        }
        RunMaintenanceRequest::Retention(request) => {
            let options = AdvanceRetentionFloorOptions::from_request(request)
                .map_err(ApiResponseError::for_namespace(&namespace_id))?;
            maintenance
                .advance_retention_floor_with_options(&namespace_id, &options)
                .await
                .map(RunMaintenanceResponse::Retention)
        }
        RunMaintenanceRequest::RetentionWindow(request) => {
            let window_ms = retention_window_ms(request.retention_window_ms)?;
            maintenance
                .set_retention_window(&namespace_id, window_ms)
                .await
                .map(RunMaintenanceResponse::RetentionWindow)
        }
        RunMaintenanceRequest::RecoverAdministrator(request) => {
            let actor_id = actor_id.ok_or_else(missing_actor)?;
            let namespace = state.open_writer(&namespace_id, None).await?;
            return namespace
                .recover_administrator(&request.principal_id, &actor_id)
                .await
                .map(|recovered| Json(RunMaintenanceResponse::RecoverAdministrator(recovered)))
                .map_err(ApiResponseError::for_namespace_writer(
                    &state.namespaces,
                    &namespace_id,
                ));
        }
    };
    response
        .map(Json)
        .map_err(ApiResponseError::for_namespace(&namespace_id))
}
