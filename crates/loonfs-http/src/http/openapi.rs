//! Static OpenAPI document assembly for the v0 HTTP API.
//!
//! Handlers define their own `#[utoipa::path]` metadata. This module combines
//! those operations and their schemas into one document. Each handler sets an
//! explicit operation ID, so changing its Rust function name does not rename
//! the generated SDK method.

mod host;

/// Builds the static OpenAPI document for the v0 HTTP API.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    let mut document = <LoonfsOpenApi as utoipa::OpenApi>::openapi();
    loonfs_types::openapi::register(
        &mut document
            .components
            .as_mut()
            .expect("the HTTP OpenAPI document should have a components section")
            .schemas,
    );
    document
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "LoonFS HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "This static OpenAPI document describes the LoonFS v0 HTTP API."
    ),
    paths(
        host::get_health,
        host::get_readiness,
        host::get_metrics,
        crate::http::handlers_namespace::get_capabilities,
        crate::http::handlers_namespace::create_namespace,
        crate::http::handlers_namespace::get_namespace,
        crate::http::handlers_namespace::get_namespace_diagnostics,
        crate::http::handlers_namespace::delete_namespace,
        crate::http::handlers_namespace::fork_namespace,
        crate::http::handlers_snapshots::create_snapshot,
        crate::http::handlers_snapshots::list_snapshots,
        crate::http::handlers_snapshots::get_snapshot,
        crate::http::handlers_snapshots::extend_snapshot,
        crate::http::handlers_snapshots::delete_snapshot,
        crate::http::handlers_filesystem::list_path_entries,
        crate::http::handlers_filesystem::get_path_entry,
        crate::http::handlers_filesystem::get_file_content,
        crate::http::handlers_downloads::create_download,
        crate::http::handlers_filesystem::list_file_revisions,
        crate::http::handlers_inodes::get_inode,
        crate::http::handlers_inodes::list_inode_children,
        crate::http::handlers_inodes::get_file_content_by_inode,
        crate::http::handlers_downloads::create_download_by_inode,
        crate::http::handlers_inodes::list_file_revisions_by_inode,
        crate::http::handlers_inodes::get_file_revision_content_by_inode,
        crate::http::handlers_downloads::create_revision_download_by_inode,
        crate::http::handlers_filesystem::list_trash,
        crate::http::handlers_filesystem::create_commit,
        crate::http::handlers_uploads::create_upload,
        crate::http::handlers_uploads::put_upload_content,
        crate::http::handlers_uploads::sign_upload_parts,
        crate::http::handlers_uploads::complete_upload,
        crate::http::handlers_uploads::abort_upload,
        crate::http::handlers_uploads::get_upload,
        crate::http::handlers_filesystem::list_changes,
        crate::http::handlers_snapshots::list_checkpoints,
        crate::http::handlers_snapshots::get_checkpoint,
        crate::http::handlers_maintenance::run_maintenance,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::get_grep_index,
        crate::http::handlers_query::enable_grep_index,
        crate::http::handlers_query::disable_grep_index,
        crate::http::handlers_store::probe_store
    ),
    components(
        responses(UnavailableResponse)
    ),
    // Applies to every operation that does not override it. `/health` and
    // `/readiness` do, with `security(())`: they are the probe surface and
    // respond unauthenticated by design.
    security(("bearer_auth" = [])),
    modifiers(&BearerAuth),
    tags(
        (name = "system", description = "Server health, readiness, metrics, and capability discovery"),
        (name = "namespaces", description = "Namespace lifecycle and status"),
        (name = "filesystem", description = "Path-oriented filesystem APIs"),
        (name = "inodes", description = "Inode-addressed read APIs"),
        (name = "uploads", description = "Upload session APIs"),
        (name = "maintenance", description = "Maintenance APIs"),
        (name = "query", description = "Derived-index query APIs")
    )
)]
struct LoonfsOpenApi;

/// OpenAPI definition for the 503 response every operation can return.
#[derive(utoipa::ToResponse)]
#[response(
    description = "The server can't complete the request now. Check `code` to find whether the cause is a deadline, shutdown, load, writer session admission, required maintenance, or invalid storage credentials. When its request cap is full, the reference server returns `server_busy` with `Retry-After: 1` before it reads the body. Health and readiness probes are exempt from the cap. A mutation might still complete after a deadline or a lost acknowledgment, so check its outcome before retrying."
)]
#[expect(
    dead_code,
    reason = "used only to generate the reusable OpenAPI response schema"
)]
pub(super) struct UnavailableResponse(#[to_schema] loonfs_types::ErrorResponse);

/// Adds the shared 503 response to an OpenAPI operation.
pub(super) struct UnavailableResponses;

impl utoipa::IntoResponses for UnavailableResponses {
    fn responses() -> std::collections::BTreeMap<
        String,
        utoipa::openapi::RefOr<utoipa::openapi::response::Response>,
    > {
        let (name, _) = <UnavailableResponse as utoipa::ToResponse>::response();
        utoipa::openapi::ResponsesBuilder::new()
            .response("503", utoipa::openapi::Ref::from_response_name(name))
            .build()
            .into()
    }
}

/// Declares the scheme the global requirement above names.
///
/// This adds to the components the derive already built rather than
/// replacing them, so the schema set survives.
struct BearerAuth;

impl utoipa::Modify for BearerAuth {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .description(Some(
                            "The client sends the deployment's `auth_token` as \
                             `Authorization: Bearer <token>`. A server without a \
                             configured token accepts every request. A server with a \
                             configured token returns 401 `unauthorized` when the token \
                             is missing or wrong.",
                        ))
                        .build(),
                ),
            );
    }
}
