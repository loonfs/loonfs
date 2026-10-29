//! The table of contract routes, the router built from the table, the gate
//! on routes that need an optional handle, and the fallbacks for unserved
//! paths and methods.

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
use super::middleware::{authenticate, observe};
use crate::BindingState;
use axum::http::Method;
use axum::routing::{any, get, on, post, MethodFilter, MethodRouter};
use axum::{Extension, Router};
use loonfs::ErrorCode;
use std::sync::Arc;

/// Whether the binding's request deadline applies to a route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Deadline {
    /// The request deadline cancels the request.
    Bounded,
    /// The route streams content or runs operator work that is long by
    /// design, so the request deadline doesn't apply.
    Exempt,
}

/// One contract route: its method, its path template, its operation ID,
/// and whether the request deadline applies to the route.
#[derive(Debug)]
pub(super) struct Route {
    pub(super) method: Method,
    pub(super) path: &'static str,
    /// The operation ID, which is the handler's function name.
    pub(super) operation: &'static str,
    pub(super) deadline: Deadline,
}

/// Declares the routes of one route group: a table of each route's method,
/// path, operation ID, and deadline, and a function that adds the routes to
/// a router. A route that a binding serves only with an optional handle
/// lists its method router after `=`.
macro_rules! routes {
    (
        $table:ident, $add:ident($state:ident);
        $($path:literal => {
            $($method:ident $operation:ident $([$exempt:ident])? $(= $router:expr)?),+ $(,)?
        }),+ $(,)?
    ) => {
        pub(super) const $table: &[Route] = &[$($(Route {
            method: Method::$method,
            path: $path,
            operation: stringify!($operation),
            deadline: routes!(@deadline $($exempt)?),
        },)+)+];

        fn $add(
            $state: &BindingState,
            router: Router<Arc<BindingState>>,
        ) -> Router<Arc<BindingState>> {
            router $($(.route($path, routes!(@router $method $operation $($router)?)))+)+
        }
    };
    (@deadline) => { Deadline::Bounded };
    (@deadline deadline_exempt) => { Deadline::Exempt };
    (@router $method:ident $operation:ident) => { on(MethodFilter::$method, $operation) };
    (@router $method:ident $operation:ident $router:expr) => { $router };
}

routes! {
    ROUTES, add_routes(state);
    "/v0/capabilities" => { GET get_capabilities },
    "/v0/namespaces" => { POST create_namespace },
    "/v0/namespaces/{namespace_id}" => { GET get_namespace, DELETE delete_namespace },
    "/v0/namespaces/{namespace_id}/forks" => { POST fork_namespace },
    "/v0/namespaces/{namespace_id}/snapshots" => { POST create_snapshot, GET list_snapshots },
    "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}/extend" => { POST extend_snapshot },
    "/v0/namespaces/{namespace_id}/snapshots/{snapshot_id}" => {
        GET get_snapshot,
        DELETE delete_snapshot,
    },
    "/v0/namespaces/{namespace_id}/filesystem/entries" => { GET list_path_entries },
    "/v0/namespaces/{namespace_id}/filesystem/entry" => { GET get_path_entry },
    "/v0/namespaces/{namespace_id}/filesystem/content" => {
        GET get_file_content [deadline_exempt],
    },
    // The read this deployment authorizes rather than performs. It sits
    // beside the proxied read because it answers the same question about
    // the same path; the route exists everywhere and refuses with
    // `not_supported` where no issuer does, exactly as the direct upload
    // modes do on `POST .../uploads`.
    "/v0/namespaces/{namespace_id}/filesystem/downloads" => { POST create_download },
    // Searching an index and keeping one built are separate jobs, so they
    // are separately deployable: the query route exists where this server
    // serves grep, and the three routes that mutate a grep manifest exist
    // where it maintains one.
    "/v0/namespaces/{namespace_id}/grep" => {
        GET grep = gated(state.grep.service().cloned(), get(grep), get(grep_queries_not_served)),
    },
    "/v0/namespaces/{namespace_id}/filesystem/revisions" => { GET list_file_revisions },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}" => { GET get_inode },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/children" => { GET list_inode_children },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/content" => {
        GET get_file_content_by_inode [deadline_exempt],
    },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/downloads" => {
        POST create_download_by_inode,
    },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions" => {
        GET list_file_revisions_by_inode,
    },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/content" => {
        GET get_file_revision_content_by_inode [deadline_exempt],
    },
    "/v0/namespaces/{namespace_id}/inodes/{inode_id}/revisions/{revision_no}/downloads" => {
        POST create_revision_download_by_inode,
    },
    "/v0/namespaces/{namespace_id}/filesystem/trash" => { GET list_trash },
    // A commit that reaches the deadline may still land. The publisher
    // owns an accepted candidate, as after a client disconnect, and the
    // commit receipt makes retry safe.
    "/v0/namespaces/{namespace_id}/commits" => { POST create_commit },
    "/v0/namespaces/{namespace_id}/uploads" => { POST create_upload },
    // No body-limit layer: the upload route never buffers its body, so a
    // framework limit measured against a buffered read would never fire.
    // `UploadBodyStream` counts the bytes as it forwards them and enforces
    // `upload.service_proxied.max_content_bytes` itself.
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/content" => {
        PUT put_upload_content [deadline_exempt],
    },
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/parts" => { POST sign_upload_parts },
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/complete" => { POST complete_upload },
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}/abort" => { POST abort_upload },
    "/v0/namespaces/{namespace_id}/uploads/{upload_id}" => { GET get_upload },
    "/v0/namespaces/{namespace_id}/changes" => { GET list_changes },
}

routes! {
    MAINTENANCE_ROUTES, add_maintenance_routes(state);
    "/v0/maintenance/namespaces/{namespace_id}/diagnostics" => { GET get_namespace_diagnostics },
    "/v0/maintenance/namespaces/{namespace_id}/grep/index" => {
        GET get_grep_index = gated(
            state.grep.worker().cloned(),
            get(get_grep_index),
            get(grep_index_not_maintained),
        ),
    },
    "/v0/maintenance/namespaces/{namespace_id}/grep/index/enable" => {
        POST enable_grep_index = gated(
            state.grep.worker().cloned(),
            post(enable_grep_index),
            post(grep_index_not_maintained),
        ),
    },
    "/v0/maintenance/namespaces/{namespace_id}/grep/index/disable" => {
        POST disable_grep_index = gated(
            state.grep.worker().cloned(),
            post(disable_grep_index),
            post(grep_index_not_maintained),
        ),
    },
    "/v0/maintenance/namespaces/{namespace_id}/checkpoints" => { GET list_checkpoints },
    "/v0/maintenance/namespaces/{namespace_id}/checkpoints/{checkpoint_id}" => {
        GET get_checkpoint,
    },
    "/v0/maintenance/namespaces/{namespace_id}/runs" => { POST run_maintenance [deadline_exempt] },
    // The one maintenance route whose subject is the store rather than a
    // namespace, so it sits beside them rather than under one.
    "/v0/maintenance/store/probe" => { POST probe_store [deadline_exempt] },
}

/// Returns the contract route that serves `method` on the template `path`.
/// A HEAD request finds the GET route, because axum serves a HEAD request
/// through the route's GET handler.
pub(super) fn find_route(method: &Method, path: &str) -> Option<&'static Route> {
    let method = match *method {
        Method::HEAD => &Method::GET,
        ref method => method,
    };
    ROUTES
        .iter()
        .chain(MAINTENANCE_ROUTES)
        .find(|route| route.method == method && route.path == path)
}

/// Returns the table's spelling of the template `path`, for a path that a
/// contract route has.
pub(super) fn find_route_path(path: &str) -> Option<&'static str> {
    ROUTES
        .iter()
        .chain(MAINTENANCE_ROUTES)
        .find(|route| route.path == path)
        .map(|route| route.path)
}

/// Serves `served` with `handle` as a request extension where the binding
/// holds the handle, and `denied` where it doesn't.
fn gated<T: Clone + Send + Sync + 'static>(
    handle: Option<T>,
    served: MethodRouter<Arc<BindingState>>,
    denied: MethodRouter<Arc<BindingState>>,
) -> MethodRouter<Arc<BindingState>> {
    match handle {
        Some(handle) => served.layer(Extension(handle)),
        None => denied,
    }
}

/// Builds contract routes. Hosts register their own health, readiness, and metrics routes.
pub fn router(state: BindingState) -> Router {
    let mut authenticated = add_routes(&state, Router::new());
    authenticated = if state.options.serves_maintenance {
        add_maintenance_routes(&state, authenticated)
    } else {
        authenticated.route("/v0/maintenance/{*path}", any(maintenance_not_served))
    };
    let state = Arc::new(state);
    let authenticated = authenticate(
        authenticated.with_state(Arc::clone(&state)),
        Arc::clone(&state),
    );
    observe(authenticated.fallback(route_not_found), state)
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
