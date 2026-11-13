//! Snapshot and snapshot inventory handlers.

use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::PageQuery;

use super::query_params::effective_page_limit;
use super::{AppJson, AppPath, AppQuery, HttpState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;

#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    CreateSnapshotRequest, DeleteSnapshotResponse, ErrorCode, ExtendSnapshotRequest,
    ListSnapshotRecordsResponse, ListSnapshotsResponse, Snapshot, SnapshotId, SnapshotRecord,
};
use std::sync::Arc;

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "create_snapshot",
        extensions(
            ("x-loonfs-retry" = json!("not_idempotent")),
            ("x-fern-sdk-group-name" = json!(["snapshots"])),
            ("x-fern-sdk-method-name" = json!("create")),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "Create snapshot",
        description = "Creates a snapshot of the current namespace state. The server first folds any WAL tail after the current manifest. Every accepted call creates a new snapshot with a new ID, and the name is a label, not a key. A snapshot with `ttl_ms` expires that many milliseconds from the server's current time, and a snapshot without it doesn't expire. The snapshot keeps its manifest until garbage collection deletes it, after it expires or is deleted and its grace window passes.",
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
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateSnapshotRequest>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let snapshot = namespace
        .create_snapshot_with_options(&loonfs::CreateSnapshotOptions {
            name: request.name,
            ttl_ms: request.ttl_ms,
        })
        .await
        .map_err(|error| ApiResponseError::runtime(error).with_body_field_params(&["/name"]))?;
    Ok(Json(snapshot))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_snapshots",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["snapshots"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListSnapshotsRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots",
        tag = "namespaces",
        summary = "List snapshots",
        description = "Lists snapshots in snapshot ID order. An expired snapshot stays in the list until garbage collection deletes it, after its expiry plus the grace window. The list leaves out deleted snapshots and fork snapshots, which the maintenance snapshot inventory lists.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            PageQuery
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
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListSnapshotsResponse>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let namespace = runtime.namespace(&namespace_id);
    let response = namespace
        .list_snapshots()
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(query.cursor)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        post,
        operation_id = "extend_snapshot",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["snapshots"])),
            ("x-fern-sdk-method-name" = json!("extend")),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
        tag = "namespaces",
        summary = "Extend snapshot",
        description = "Moves an unexpired snapshot's expiry to `ttl_ms` from the server's current time. The expiry never moves earlier, so a snapshot without an expiry is returned unchanged. A missing, deleted, or expired snapshot returns `snapshot_not_found`. Repeating the request has the same result.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::SnapshotId, Path, description = "The snapshot ID.")
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
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<ExtendSnapshotRequest>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let response = namespace
        .extend_snapshot(&snapshot_id, request.ttl_ms)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_snapshot",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["snapshots"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
            ("x-fern-request-name" = json!("GetSnapshotRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        tag = "namespaces",
        summary = "Retrieve snapshot",
        description = "Returns one snapshot, as the snapshot listing returns it, including an expired snapshot that garbage collection hasn't deleted. A missing ID, the ID of a deleted snapshot, or the ID of a fork snapshot returns `snapshot_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::SnapshotId, Path, description = "The snapshot ID.")
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
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let runtime = state.runtime_for(subject);
    let snapshot = runtime
        .namespace(&namespace_id)
        .get_snapshot(&snapshot_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(snapshot))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        delete,
        operation_id = "delete_snapshot",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["snapshots"])),
            ("x-fern-sdk-method-name" = json!("delete")),
            ("x-fern-request-name" = json!("DeleteSnapshotRequest")),
        ),
        path = "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        tag = "namespaces",
        summary = "Delete snapshot",
        description = "Deletes a snapshot by recording the server's current time as its delete time. From then on, the snapshot can't be read, forked, extended, or retrieved, and the snapshot listing leaves it out. Garbage collection can then reclaim its unreferenced manifest and runs after the grace window. Deleting a missing or deleted snapshot succeeds without a write, so repeating the request has the same result. The ID of a fork snapshot returns `snapshot_not_found`. Deletion stays available after the namespace is deleted.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = loonfs_types::SnapshotId, Path, description = "The snapshot ID.")
        ),
        responses(
            (status = 200, description = "Snapshot deleted, or already absent", body = DeleteSnapshotResponse),
            (status = 400, description = "Invalid namespace or snapshot ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found, or the ID belongs to a fork snapshot", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn delete_snapshot(
    State(state): State<Arc<HttpState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<DeleteSnapshotResponse>, ApiResponseError> {
    let namespace = match state.namespaces.open(&namespace_id).await {
        // The session registry opens sessions for active namespaces only, but
        // a snapshot without an expiry has to stay deletable after its
        // namespace is deleted. Deleting a snapshot publishes nothing, so a
        // session outside the registry is enough.
        Err(error) if error.code() == ErrorCode::NamespaceDeleted => state
            .runtime
            .open_namespace(&namespace_id)
            .map_err(ApiResponseError::runtime)?,
        opened => opened.map_err(ApiResponseError::runtime)?,
    };
    let scoped_namespace = subject.map(|subject| namespace.with_subject(subject));
    let namespace = scoped_namespace.as_ref().unwrap_or(&namespace);
    let response = namespace
        .delete_snapshot(&snapshot_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct SnapshotPathParams {
    snapshot_id: SnapshotId,
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "list_snapshot_records",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["maintenance", "snapshots"])),
            ("x-fern-sdk-method-name" = json!("list")),
            ("x-fern-request-name" = json!("ListSnapshotRecordsRequest")),
        ),
        path = "/v0/maintenance/namespaces/{namespace_id}/snapshots",
        tag = "maintenance",
        summary = "List snapshot records",
        description = "Lists every snapshot, user snapshots and fork snapshots, in snapshot ID order. This is the inventory of what keeps manifests from garbage collection. An expired snapshot stays in the list until garbage collection deletes it, after its expiry plus the grace window, and a deleted snapshot stays, with its `deleted_at_ms`, until its delete time plus the grace window. A cursor continues the listing against the current set of snapshots.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            PageQuery
        ),
        responses(
            (status = 200, description = "Existing snapshot objects", body = ListSnapshotRecordsResponse),
            (status = 400, description = "Invalid namespace ID, limit, or cursor", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn list_snapshot_records(
    State(state): State<Arc<HttpState>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(query): AppQuery<PageQuery>,
) -> Result<Json<ListSnapshotRecordsResponse>, ApiResponseError> {
    let response = state
        .maintenance
        .list_snapshot_records(&namespace_id)
        .page_limit(effective_page_limit(query.limit)?)
        .cursor(query.cursor)
        .page()
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(response))
}

#[cfg_attr(
    feature = "openapi",
    utoipa::path(
        get,
        operation_id = "get_snapshot_record",
        extensions(
            ("x-loonfs-retry" = json!("idempotent")),
            ("x-fern-sdk-group-name" = json!(["maintenance", "snapshots"])),
            ("x-fern-sdk-method-name" = json!("retrieve")),
            ("x-fern-request-name" = json!("GetSnapshotRecordRequest")),
        ),
        path = "/v0/maintenance/namespaces/{namespace_id}/snapshots/{snapshot_id}",
        tag = "maintenance",
        summary = "Retrieve snapshot record",
        description = "Returns one existing snapshot of any owner, as the snapshot inventory lists it, including an expired or deleted snapshot that garbage collection hasn't deleted. A missing ID returns `snapshot_not_found`.",
        params(
            ("namespace_id" = String, Path, description = "The namespace ID."),
            ("snapshot_id" = String, Path, description = "The snapshot ID.")
        ),
        responses(
            (status = 200, description = "Existing snapshot", body = SnapshotRecord),
            (status = 400, description = "Invalid namespace or snapshot ID", body = ErrorResponse),
            (status = 401, description = "Unauthorized", body = ErrorResponse),
            (status = 404, description = "Namespace or snapshot not found", body = ErrorResponse),
            crate::http::openapi::UnavailableResponses
        )
    )
)]
pub(super) async fn get_snapshot_record(
    State(state): State<Arc<HttpState>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<SnapshotRecord>, ApiResponseError> {
    let snapshot = state
        .maintenance
        .get_snapshot_record(&namespace_id, &snapshot_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(snapshot))
}
