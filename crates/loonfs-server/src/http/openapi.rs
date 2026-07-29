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
        BeginUploadRequest, BeginUploadResponse, ChangesResponse,
        CommitResponse as ApiCommitResponse, CompleteUploadRequest, CompleteUploadResponse,
        DirectPutUpload, ObjectTransferAccess, UploadContentResponse, UploadMode,
        ValidatedContentToken,
    },
    ApiError, ContentRef, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, FilesystemOperation, FilesystemOperationRequest, ForkNamespaceRequest,
    GcRequest, GcResponse, InodeId, ListFileRevisionsResponse, ListTrashResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, ReleaseCheckpointResponse, RevisionNo,
    TrashEntry, WalFlushStepOutcome,
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
        crate::http::handlers_namespace::capabilities,
        crate::http::handlers_namespace::create_namespace,
        crate::http::handlers_namespace::namespace_status,
        crate::http::handlers_namespace::delete_namespace,
        crate::http::handlers_namespace::fork_namespace,
        crate::http::handlers_filesystem::list_path_entries,
        crate::http::handlers_filesystem::stat_path,
        crate::http::handlers_filesystem::get_file_bytes,
        crate::http::handlers_filesystem::list_file_revisions,
        crate::http::handlers_filesystem::list_trash,
        crate::http::handlers_filesystem::apply_filesystem_operation,
        crate::http::handlers_uploads::begin_upload,
        crate::http::handlers_uploads::upload_content,
        crate::http::handlers_uploads::complete_upload,
        crate::http::handlers_filesystem::list_changes,
        crate::http::handlers_namespace::create_checkpoint,
        crate::http::handlers_namespace::release_checkpoint,
        crate::http::handlers_namespace::maintenance_step,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::enable_grep_index,
        crate::http::handlers_query::disable_grep_index,
        crate::http::handlers_query::gc_grep_index
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
        FilesystemOperationRequest,
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
        loonfs_api::v0::EnableGrepIndexResponse,
        loonfs_api::v0::DisableGrepIndexResponse,
        loonfs_api::v0::GrepGcResponse
    )),
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
