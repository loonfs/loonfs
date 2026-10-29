//! The contract routes, the gate on routes that need an optional handle,
//! and the fallbacks for unserved paths and methods.

use super::error::ApiResponseError;
use super::handlers_downloads::{
    create_download, create_download_by_inode, create_revision_download_by_inode,
};
use super::handlers_filesystem::{
    create_commit, get_file_content, get_path_entry, list_changes, list_file_revisions,
    list_path_entries, list_trash,
};
use super::handlers_inodes::{
    get_file_content_by_inode, get_file_revision_content_by_inode, get_inode,
    list_file_revisions_by_inode, list_inode_children,
};
use super::handlers_namespace::{
    create_namespace, create_snapshot, delete_namespace, delete_snapshot, extend_snapshot,
    fork_namespace, get_capabilities, get_checkpoint, get_namespace, get_namespace_diagnostics,
    get_snapshot, list_checkpoints, list_snapshots, run_maintenance,
};
use super::handlers_query::{
    disable_grep_index, enable_grep_index, get_grep_index, grep, grep_index_not_maintained,
    grep_queries_not_served,
};
use super::handlers_store::probe_store;
use super::handlers_uploads::{
    abort_upload, complete_upload, create_upload, get_upload, put_upload_content, sign_upload_parts,
};
use super::middleware::{authenticate_routes, observe_routes};
use crate::BindingState;
use axum::routing::{any, get, post, put, MethodRouter};
use axum::{Extension, Router};
use loonfs::ErrorCode;

/// Serves `served` with `handle` as a request extension where the binding
/// holds the handle, and `denied` where it doesn't.
fn gated<T: Clone + Send + Sync + 'static>(
    handle: Option<T>,
    served: MethodRouter<BindingState>,
    denied: MethodRouter<BindingState>,
) -> MethodRouter<BindingState> {
    match handle {
        Some(handle) => served.layer(Extension(handle)),
        None => denied,
    }
}

/// Builds contract routes. Hosts register their own health, readiness, and metrics routes.
pub fn router(state: BindingState) -> Router {
    // Searching an index and keeping one built are separate jobs, so they
    // are separately deployable: the query route exists where this server
    // serves grep, and the three routes that mutate a grep manifest exist where
    // it maintains one.
    let grep_service = state.grep.service().cloned();
    let index_worker = state.grep.worker().cloned();
    let mut authenticated = Router::new()
        .route("/v0/capabilities", get(get_capabilities))
        .route("/v0/namespaces", post(create_namespace))
        .route(
            "/v0/namespaces/{namespace_id}",
            get(get_namespace).delete(delete_namespace),
        )
        .route("/v0/namespaces/{namespace_id}/forks", post(fork_namespace))
        .route(
            "/v0/namespaces/{namespace_id}/snapshots",
            post(create_snapshot).get(list_snapshots),
        )
        .route(
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend",
            post(extend_snapshot),
        )
        .route(
            "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}",
            get(get_snapshot).delete(delete_snapshot),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/entries",
            get(list_path_entries),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/entry",
            get(get_path_entry),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/content",
            get(get_file_content),
        )
        // The read this deployment authorizes rather than performs. It sits
        // beside the proxied read because it answers the same question
        // about the same path; the route exists everywhere and refuses with
        // `not_supported` where no issuer does, exactly as the direct
        // upload modes do on `POST .../uploads`.
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/downloads",
            post(create_download),
        )
        .route(
            "/v0/namespaces/{namespace_id}/grep",
            gated(grep_service, get(grep), get(grep_queries_not_served)),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/revisions",
            get(list_file_revisions),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}",
            get(get_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children",
            get(list_inode_children),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content",
            get(get_file_content_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads",
            post(create_download_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions",
            get(list_file_revisions_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content",
            get(get_file_revision_content_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads",
            post(create_revision_download_by_inode),
        )
        .route(
            "/v0/namespaces/{namespace_id}/filesystem/trash",
            get(list_trash),
        )
        // A commit that reaches the deadline may still land. The publisher
        // owns an accepted candidate, as after a client disconnect, and the
        // commit receipt makes retry safe.
        .route("/v0/namespaces/{namespace_id}/commits", post(create_commit))
        .route("/v0/namespaces/{namespace_id}/uploads", post(create_upload))
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content",
            // No body-limit layer: the upload route never buffers its
            // body, so a framework limit measured against a buffered read
            // would never fire. `UploadBodyStream` counts the bytes as it
            // forwards them and enforces `upload.service_proxied.max_content_bytes` itself.
            put(put_upload_content),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts",
            post(sign_upload_parts),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete",
            post(complete_upload),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort",
            post(abort_upload),
        )
        .route(
            "/v0/namespaces/{namespace_id}/uploads/{upload_id}",
            get(get_upload),
        )
        .route("/v0/namespaces/{namespace_id}/changes", get(list_changes));
    if state.options.serves_maintenance {
        authenticated = authenticated
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/diagnostics",
                get(get_namespace_diagnostics),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index",
                gated(
                    index_worker.clone(),
                    get(get_grep_index),
                    get(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable",
                gated(
                    index_worker.clone(),
                    post(enable_grep_index),
                    post(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable",
                gated(
                    index_worker,
                    post(disable_grep_index),
                    post(grep_index_not_maintained),
                ),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/checkpoints",
                get(list_checkpoints),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}",
                get(get_checkpoint),
            )
            .route(
                "/v0/maintenance/namespaces/{namespace_id}/runs",
                post(run_maintenance),
            )
            // The one maintenance route whose subject is the store rather than a
            // namespace, so it sits beside them rather than under one.
            .route("/v0/maintenance/store/probe", post(probe_store));
    } else {
        authenticated = authenticated.route("/v0/maintenance/{*path}", any(maintenance_not_served));
    }
    let authenticated = authenticate_routes(authenticated.with_state(state.clone()), &state);
    observe_routes(authenticated.fallback(route_not_found), &state)
}

/// 404 for paths outside the served surface. Deliberately unauthenticated:
/// the route set is public in the API spec.
async fn route_not_found() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::RouteNotFound,
        "no v0 route matches this path; see the API spec for the served surface",
    )
}

/// 404 for the maintenance routes on a deployment that does not serve that
/// API group. The group is a capability-document `api_groups` entry, not a
/// feature key, so `not_supported` and its `feature` field do not apply.
async fn maintenance_not_served() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::RouteNotFound,
        "this deployment does not serve the maintenance API group; set `maintenance` to \
         `serve_only` or `serve_and_maintain`",
    )
}

/// 405 for matched paths hit with an unserved method.
pub(super) async fn method_not_allowed() -> ApiResponseError {
    ApiResponseError::new(
        ErrorCode::MethodNotAllowed,
        "this path exists but does not serve this HTTP method",
    )
}
