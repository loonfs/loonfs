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
        CommitRequest as ApiCommitRequest, CommitResponse as ApiCommitResponse,
        CompleteUploadRequest, CompleteUploadResponse, DirectPutUpload, ObjectTransferAccess,
        UploadContentResponse, UploadMode, ValidatedContentToken,
    },
    AdvanceRetentionResponse, ApiError, ContentRef, CreateCheckpointRequest,
    CreateCheckpointResponse, CreateNamespaceRequest, FilesystemOperation,
    FilesystemOperationRequest, FlushWalOutcome, FlushWalResponse, ForkNamespaceRequest, GcRequest,
    GcResponse, InodeId, ListFileRevisionsResponse, MaintenanceTickOutcome, MaintenanceTickRequest,
    MaintenanceTickResponse, ReleaseCheckpointResponse, RestoreFileRevisionRequest, RevisionNo,
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
        crate::http::health_ready,
        crate::http::handlers_namespace::config,
        crate::http::handlers_namespace::create_namespace,
        crate::http::handlers_namespace::namespace_status,
        crate::http::handlers_namespace::delete_namespace,
        crate::http::handlers_namespace::fork_namespace,
        crate::http::handlers_filesystem::list_entries,
        crate::http::handlers_filesystem::stat_entry,
        crate::http::handlers_filesystem::get_content,
        crate::http::handlers_filesystem::list_path_revisions,
        crate::http::handlers_filesystem::filesystem_operation,
        crate::http::handlers_filesystem::list_inode_revisions,
        crate::http::handlers_filesystem::get_inode_revision_content,
        crate::http::handlers_filesystem::restore_inode_revision,
        crate::http::handlers_uploads::begin_upload,
        crate::http::handlers_uploads::upload_content,
        crate::http::handlers_uploads::complete_upload,
        crate::http::handlers_filesystem::commit_operations,
        crate::http::handlers_filesystem::list_changes,
        crate::http::handlers_namespace::create_checkpoint,
        crate::http::handlers_namespace::release_checkpoint,
        crate::http::handlers_namespace::flush_wal,
        crate::http::handlers_namespace::advance_retention,
        crate::http::handlers_namespace::maintenance_tick,
        crate::http::handlers_namespace::gc_namespace,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::enable_grams_index,
        crate::http::handlers_query::disable_grams_index
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
        loonfs_api::PutBehavior,
        loonfs_api::DeleteDirectoryBehavior,
        loonfs_api::MoveBehavior,
        loonfs_api::CopyBehavior,
        FilesystemOperation,
        FilesystemOperationRequest,
        loonfs_api::FileRevision,
        ListFileRevisionsResponse,
        RestoreFileRevisionRequest,
        CreateCheckpointRequest,
        CreateCheckpointResponse,
        ReleaseCheckpointResponse,
        FlushWalOutcome,
        FlushWalResponse,
        AdvanceRetentionResponse,
        MaintenanceTickRequest,
        MaintenanceTickOutcome,
        MaintenanceTickResponse,
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
        ApiCommitRequest,
        ApiCommitResponse,
        loonfs_api::PutBehavior,
        loonfs_api::DeleteDirectoryBehavior,
        loonfs_api::v0::CommitOp,
        loonfs_api::v0::CommitPrecondition,
        loonfs_api::v0::CommitDelta,
        loonfs_api::v0::CommittedChange,
        ChangesResponse,
        loonfs_api::v0::GrepRequest,
        loonfs_api::v0::GrepMatch,
        loonfs_api::v0::GrepResponse,
        loonfs_api::v0::EnableGramsIndexResponse,
        loonfs_api::v0::DisableGramsIndexResponse
    )),
    tags(
        (name = "health", description = "Server health"),
        (name = "config", description = "Capability discovery"),
        (name = "namespaces", description = "Namespace lifecycle and status"),
        (name = "filesystem", description = "Path-oriented filesystem APIs"),
        (name = "inodes", description = "Inode revision APIs"),
        (name = "uploads", description = "Upload session APIs"),
        (name = "commits", description = "Commit and change-feed APIs"),
        (name = "admin", description = "Administrative maintenance APIs"),
        (name = "query", description = "Derived-index query APIs")
    )
)]
struct LoonfsOpenApi;
