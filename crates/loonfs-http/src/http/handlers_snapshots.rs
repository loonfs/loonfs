//! Snapshot and checkpoint handlers.

use super::error::ApiResponseError;
use super::extractors::SubjectHeaders;
use super::handlers_filesystem::PageQuery;

use super::query_params::{checked_cursor, effective_page_limit};
use super::{AppJson, AppPath, AppQuery, BindingState, NamespaceIdPath, NoQuery};
use axum::extract::State;
use axum::Json;
use loonfs::{CheckpointPageCursor, SnapshotPageCursor};

#[cfg(feature = "openapi")]
use loonfs_types::ErrorResponse;
use loonfs_types::{
    Checkpoint, CreateSnapshotRequest, DeleteSnapshotResponse, ErrorCode, ExtendSnapshotRequest,
    ListCheckpointsResponse, ListSnapshotsResponse, PinId, Snapshot,
};
use std::sync::Arc;

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
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppQuery(_): AppQuery<NoQuery>,
    AppJson(request): AppJson<CreateSnapshotRequest>,
) -> Result<Json<Snapshot>, ApiResponseError> {
    let namespace = state.open_writer(&namespace_id, subject).await?;
    let snapshot = namespace
        .create_snapshot_with_options(
            &request.name,
            &loonfs::CreateSnapshotOptions {
                ttl_ms: request.ttl_ms,
            },
        )
        .await
        .map_err(ApiResponseError::for_namespace_writer(
            state.namespaces.as_ref(),
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
    State(state): State<Arc<BindingState>>,
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
        .map_err(ApiResponseError::runtime)?;
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
    State(state): State<Arc<BindingState>>,
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
        .map_err(ApiResponseError::for_namespace_writer(
            state.namespaces.as_ref(),
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
    State(state): State<Arc<BindingState>>,
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
    State(state): State<Arc<BindingState>>,
    SubjectHeaders(subject): SubjectHeaders,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(SnapshotPathParams { snapshot_id }): AppPath<SnapshotPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<DeleteSnapshotResponse>, ApiResponseError> {
    let namespace = match state.namespaces.open(&namespace_id).await {
        // The session registry opens sessions for active namespaces only, but
        // a snapshot without an expiry has to stay deletable after its
        // namespace is deleted. Deleting a pin publishes nothing, so a
        // session outside the registry is enough.
        Err(error) if error.code() == ErrorCode::NamespaceDeleted => state
            .runtime
            .open_namespace(&namespace_id)
            .map_err(ApiResponseError::runtime)?,
        opened => opened.map_err(ApiResponseError::runtime)?,
    };
    let scoped_namespace = subject.map(|subject| namespace.with_subject(subject));
    let namespace = scoped_namespace.as_ref().unwrap_or(&namespace);
    let response = namespace.delete_snapshot(&snapshot_id).await.map_err(
        ApiResponseError::for_namespace_writer(state.namespaces.as_ref(), &namespace_id),
    )?;
    Ok(Json(response))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct SnapshotPathParams {
    snapshot_id: PinId,
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
            PageQuery
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
    State(state): State<Arc<BindingState>>,
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
        .map_err(ApiResponseError::runtime)?;
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
    State(state): State<Arc<BindingState>>,
    NamespaceIdPath(namespace_id): NamespaceIdPath,
    AppPath(CheckpointPathParams { checkpoint_id }): AppPath<CheckpointPathParams>,
    AppQuery(_): AppQuery<NoQuery>,
) -> Result<Json<Checkpoint>, ApiResponseError> {
    let checkpoint = state
        .maintenance
        .get_checkpoint(&namespace_id, &checkpoint_id)
        .await
        .map_err(ApiResponseError::runtime)?;
    Ok(Json(checkpoint))
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct CheckpointPathParams {
    checkpoint_id: PinId,
}
