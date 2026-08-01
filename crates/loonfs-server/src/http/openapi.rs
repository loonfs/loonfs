//! Static OpenAPI document assembly for the v0 HTTP API.
//!
//! The `#[utoipa::path]` operation metadata lives on the handlers
//! themselves; this module registers those operations and the schema set
//! into one document. utoipa derives each operation id from the handler fn
//! name, so renaming a handler changes the published id — regenerate
//! `docs/specs/openapi.json` deliberately when that happens.

use loonfs_api::ChangeSeq;
use loonfs_api::{
    v0::{
        BeginDownloadRequest, BeginDownloadResponse, BeginUploadRequest, BeginUploadResponse,
        ChangesResponse, CommitResponse as ApiCommitResponse, CompleteUploadRequest,
        CompleteUploadResponse, DirectPutUpload, ObjectTransferAccess, UploadContentResponse,
        UploadMode, ValidatedContentToken,
    },
    ApiError, CommitRequest, ContentRef, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, FilesystemOperation, ForkNamespaceRequest, GcRequest, GcResponse,
    InodeId, ListFileRevisionsResponse, ListTrashResponse, MaintenanceStepRequest,
    MaintenanceStepResponse, ReleaseCheckpointResponse, RevisionNo, TrashEntry,
    WalFlushStepOutcome,
};

pub fn openapi_document() -> utoipa::openapi::OpenApi {
    <LoonfsOpenApi as utoipa::OpenApi>::openapi()
}

pub fn openapi_json_pretty() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&openapi_document())
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "LoonFS HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Static OpenAPI document for the LoonFS v0 HTTP API."
    ),
    paths(
        crate::http::health,
        crate::http::readiness,
        crate::http::serve_metrics,
        crate::http::handlers_namespace::capabilities,
        crate::http::handlers_namespace::create_namespace,
        crate::http::handlers_namespace::namespace_status,
        crate::http::handlers_namespace::delete_namespace,
        crate::http::handlers_namespace::fork_namespace,
        crate::http::handlers_filesystem::list_path_entries,
        crate::http::handlers_filesystem::stat_path,
        crate::http::handlers_filesystem::get_file_bytes,
        crate::http::handlers_downloads::begin_download,
        crate::http::handlers_filesystem::list_file_revisions,
        crate::http::handlers_filesystem::list_trash,
        crate::http::handlers_filesystem::apply_commit,
        crate::http::handlers_uploads::begin_upload,
        crate::http::handlers_uploads::upload_content,
        crate::http::handlers_uploads::sign_upload_parts,
        crate::http::handlers_uploads::complete_upload,
        crate::http::handlers_uploads::abort_upload,
        crate::http::handlers_uploads::read_upload_status,
        crate::http::handlers_filesystem::list_changes,
        crate::http::handlers_namespace::create_checkpoint,
        crate::http::handlers_namespace::release_checkpoint,
        crate::http::handlers_namespace::maintenance_step,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::grep_index_status,
        crate::http::handlers_query::enable_grep_index,
        crate::http::handlers_query::disable_grep_index,
        crate::http::handlers_query::gc_grep_index,
        crate::http::handlers_store::probe_store
    ),
    components(schemas(
        loonfs_api::CapabilityDocument,
        ApiError,
        loonfs_api::ErrorDetails,
        loonfs_api::WriterEpoch,
        CreateNamespaceRequest,
        ForkNamespaceRequest,
        loonfs_api::NamespaceSummary,
        loonfs_api::NamespaceStatusResponse,
        loonfs_api::DeleteNamespaceResponse,
        loonfs_api::DestinationBehavior,
        loonfs_api::DeleteDirectoryBehavior,
        FilesystemOperation,
        CommitRequest,
        loonfs_api::FileRevision,
        ListFileRevisionsResponse,
        ListTrashResponse,
        TrashEntry,
        CreateCheckpointRequest,
        CreateCheckpointResponse,
        ReleaseCheckpointResponse,
        MaintenanceStepRequest,
        WalFlushStepOutcome,
        MaintenanceStepResponse,
        GcRequest,
        GcResponse,
        ContentRef,
        loonfs_api::StorageChecksum,
        loonfs_api::ChecksumAlgorithm,
        loonfs_api::ContentId,
        loonfs_api::v0::DirectPutContentClaim,
        loonfs_api::v0::DirectMultipartContentClaim,
        loonfs_api::v0::DirectMultipartUploadOptions,
        loonfs_api::NamespaceId,
        loonfs_api::ContentStoreId,
        loonfs_api::CommitId,
        InodeId,
        RevisionNo,
        ChangeSeq,
        loonfs_api::ManifestId,
        loonfs_api::NameKey,
        loonfs_api::InodeKind,
        loonfs_api::AuthoritativePathEntry,
        loonfs_api::ListPathEntriesResponse,
        BeginUploadRequest,
        BeginUploadResponse,
        UploadContentResponse,
        CompleteUploadRequest,
        CompleteUploadResponse,
        DirectPutUpload,
        loonfs_api::v0::DirectMultipartUpload,
        loonfs_api::v0::UploadPartChecksumClaim,
        loonfs_api::v0::SignUploadPartsRequest,
        loonfs_api::v0::SignedUploadPart,
        loonfs_api::v0::SignUploadPartsResponse,
        loonfs_api::v0::CompletedUploadPart,
        BeginDownloadRequest,
        BeginDownloadResponse,
        ObjectTransferAccess,
        UploadMode,
        ValidatedContentToken,
        ApiCommitResponse,
        loonfs_api::v0::FilesystemChange,
        loonfs_api::v0::CommittedChange,
        ChangesResponse,
        loonfs_api::v0::GrepRequest,
        loonfs_api::v0::GrepMatch,
        loonfs_api::v0::GrepResponse,
        loonfs_api::v0::GrepIndexLifecycle,
        loonfs_api::v0::GrepIndexStatusResponse,
        loonfs_api::v0::EnableGrepIndexResponse,
        loonfs_api::v0::DisableGrepIndexResponse,
        loonfs_api::v0::GrepGcRequest,
        loonfs_api::v0::GrepGcResponse,
        loonfs_api::v0::StoreProbeRequest,
        loonfs_api::v0::StoreProbeCheckOutcome,
        loonfs_api::v0::StoreProbeCheckResult,
        loonfs_api::v0::StoreProbeResponse
    )),
    // Applies to every operation that does not override it. `/health` and
    // `/readiness` do, with `security(())`: they are the probe surface and
    // answer unauthenticated by design.
    security(("bearer_auth" = [])),
    modifiers(&BearerAuth),
    tags(
        (name = "health", description = "Server health"),
        (name = "capabilities", description = "Capability discovery"),
        (name = "namespaces", description = "Namespace lifecycle and status"),
        (name = "filesystem", description = "Path-oriented filesystem APIs"),
        (name = "uploads", description = "Upload session APIs"),
        (name = "admin", description = "Administrative maintenance APIs"),
        (name = "query", description = "Derived-index query APIs")
    )
)]
struct LoonfsOpenApi;

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
                            "The deployment's `auth_token`, sent as \
                             `Authorization: Bearer <token>`. A server configured \
                             without a token accepts every request; one configured \
                             with a token answers 401 `unauthorized` without it.",
                        ))
                        .build(),
                ),
            );
    }
}
