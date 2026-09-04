//! Static OpenAPI document assembly for the v0 HTTP API.
//!
//! Handlers define their own `#[utoipa::path]` metadata. This module combines
//! those operations and their schemas into one document. Each handler sets an
//! explicit operation ID, so changing its Rust function name does not rename
//! the generated SDK method.

use loonfs_api::ChangeSeq;
use loonfs_api::{
    v0::{
        BeginDownloadByInodeRequest, BeginDownloadByInodeResponse, BeginDownloadRequest,
        BeginDownloadResponse, BeginUploadRequest, BeginUploadResponse,
        CommitResponse as ApiCommitResponse, CompleteUploadRequest, ContentToken, DirectoryBinding,
        ListChangesResponse, ObjectTransferAccess, UploadContentResponse, UploadMode,
        UploadSession, UploadSessionStatus,
    },
    AdvanceRetentionRequest, AdvanceRetentionResponse, ApiError, Checkpoint, CheckpointId,
    CheckpointOwnerSummary, CommitRequest, ContentRef, CreateCheckpointRequest,
    CreateNamespaceRequest, CreateSnapshotRequest, DeletedObjectCounts, ExtendSnapshotRequest,
    FilesystemOperation, ForkNamespaceRequest, GcRequest, GcResponse, ListCheckpointsResponse,
    ListFileRevisionsResponse, ListSnapshotsResponse, ListTrashResponse, MaintenanceRunRequest,
    MaintenanceRunResponse, MetadataCompactionOutcome, MetadataCompactionRequest,
    MetadataCompactionResponse, MetadataMaintenanceRequest, MetadataMaintenanceResponse,
    ReleaseCheckpointResponse, ReleaseSnapshotResponse, ReleasedCheckpointCounts,
    ReorganizeStepOutcome, RetainedCandidates, RevisionNo, SnapshotSummary, TrashEntry,
    WalFlushStepOutcome,
};

/// Builds the static OpenAPI document for the v0 HTTP API.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    <LoonfsOpenApi as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "LoonFS HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Static OpenAPI document for the LoonFS v0 HTTP API."
    ),
    paths(
        crate::http::get_health,
        crate::http::get_readiness,
        crate::http::get_metrics,
        crate::http::handlers_namespace::get_capabilities,
        crate::http::handlers_namespace::create_namespace,
        crate::http::handlers_namespace::get_namespace,
        crate::http::handlers_namespace::get_namespace_diagnostics,
        crate::http::handlers_namespace::delete_namespace,
        crate::http::handlers_namespace::fork_namespace,
        crate::http::handlers_namespace::create_snapshot,
        crate::http::handlers_namespace::list_snapshots,
        crate::http::handlers_namespace::extend_snapshot,
        crate::http::handlers_namespace::release_snapshot,
        crate::http::handlers_filesystem::list_path_entries,
        crate::http::handlers_filesystem::get_path_entry,
        crate::http::handlers_filesystem::get_file_bytes,
        crate::http::handlers_downloads::create_download,
        crate::http::handlers_filesystem::list_file_revisions,
        crate::http::handlers_inodes::get_inode,
        crate::http::handlers_inodes::list_inode_children,
        crate::http::handlers_inodes::list_file_revisions_by_inode,
        crate::http::handlers_inodes::get_file_revision_bytes_by_inode,
        crate::http::handlers_downloads::create_download_by_inode,
        crate::http::handlers_filesystem::list_trash,
        crate::http::handlers_filesystem::create_commit,
        crate::http::handlers_uploads::create_upload,
        crate::http::handlers_uploads::put_upload_content,
        crate::http::handlers_uploads::sign_upload_parts,
        crate::http::handlers_uploads::complete_upload,
        crate::http::handlers_uploads::abort_upload,
        crate::http::handlers_uploads::get_upload,
        crate::http::handlers_filesystem::list_changes,
        crate::http::handlers_namespace::create_checkpoint,
        crate::http::handlers_namespace::list_checkpoints,
        crate::http::handlers_namespace::release_checkpoint,
        crate::http::handlers_namespace::run_maintenance,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::get_grep_index,
        crate::http::handlers_query::enable_grep_index,
        crate::http::handlers_query::disable_grep_index,
        crate::http::handlers_query::gc_grep_index,
        crate::http::handlers_store::probe_store
    ),
    components(
        schemas(
        loonfs_api::CapabilityDocument,
        ApiError,
        loonfs_api::ErrorDetails,
        loonfs_api::WriterEpoch,
        CreateNamespaceRequest,
        ForkNamespaceRequest,
        loonfs_api::Namespace,
        loonfs_api::NamespaceDiagnostics,
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
        Checkpoint,
        CheckpointId,
        CheckpointOwnerSummary,
        ListCheckpointsResponse,
        ReleaseCheckpointResponse,
        CreateSnapshotRequest,
        ExtendSnapshotRequest,
        SnapshotSummary,
        ListSnapshotsResponse,
        ReleaseSnapshotResponse,
        MaintenanceRunRequest,
        MetadataMaintenanceRequest,
        MetadataCompactionRequest,
        AdvanceRetentionRequest,
        WalFlushStepOutcome,
        ReorganizeStepOutcome,
        MetadataMaintenanceResponse,
        MetadataCompactionResponse,
        MetadataCompactionOutcome,
        AdvanceRetentionResponse,
        MaintenanceRunResponse,
        GcRequest,
        GcResponse,
        DeletedObjectCounts,
        ReleasedCheckpointCounts,
        RetainedCandidates,
        ContentRef,
        loonfs_api::Checksum,
        loonfs_api::ChecksumAlgorithm,
        loonfs_api::ContentId,
        loonfs_api::v0::UploadContentClaim,
        loonfs_api::NamespaceId,
        loonfs_api::CommitId,
        RevisionNo,
        ChangeSeq,
        loonfs_api::ManifestNo,
        loonfs_api::NameKey,
        loonfs_api::AttributeKey,
        loonfs_api::AttributeValue,
        loonfs_api::Attributes,
        loonfs_api::AttributeRevisionNo,
        loonfs_api::InodeKind,
        loonfs_api::PathEntry,
        loonfs_api::PathEntryKind,
        loonfs_api::AttributesProjection,
        loonfs_api::ListPathEntriesResponse,
        loonfs_api::ListInodeChildrenResponse,
        UploadMode,
        BeginUploadRequest,
        BeginUploadResponse,
        UploadContentResponse,
        CompleteUploadRequest,
        UploadSession,
        UploadSessionStatus,
        loonfs_api::v0::UploadPartChecksumClaim,
        loonfs_api::v0::SignUploadPartsRequest,
        loonfs_api::v0::SignedUploadPart,
        loonfs_api::v0::SignUploadPartsResponse,
        loonfs_api::v0::CompletedUploadPart,
        BeginDownloadRequest,
        BeginDownloadResponse,
        BeginDownloadByInodeRequest,
        BeginDownloadByInodeResponse,
        ObjectTransferAccess,
        ContentToken,
        ApiCommitResponse,
        loonfs_api::v0::FilesystemChange,
        DirectoryBinding,
        loonfs_api::v0::CommittedChange,
        ListChangesResponse,
        loonfs_api::v0::GrepMatch,
        loonfs_api::v0::GrepResponse,
        loonfs_api::v0::GrepIndexLifecycle,
        loonfs_api::v0::GrepIndex,
        loonfs_api::v0::GrepGcRequest,
        loonfs_api::v0::GrepGcResponse,
        loonfs_api::v0::StoreProbeRequest,
        loonfs_api::v0::StoreProbeCheckOutcome,
        loonfs_api::v0::StoreProbeCheckResult,
        loonfs_api::v0::StoreProbeResponse
        ),
        responses(UnavailableResponse)
    ),
    // Applies to every operation that does not override it. `/health` and
    // `/readiness` do, with `security(())`: they are the probe surface and
    // answer unauthenticated by design.
    security(("bearer_auth" = [])),
    modifiers(&BearerAuth),
    tags(
        (name = "system", description = "Server health, readiness, metrics, and capability discovery"),
        (name = "namespaces", description = "Namespace lifecycle and status"),
        (name = "filesystem", description = "Path-oriented filesystem APIs"),
        (name = "inodes", description = "Identity-oriented inode read APIs"),
        (name = "uploads", description = "Upload session APIs"),
        (name = "maintenance", description = "Maintenance API group"),
        (name = "query", description = "Derived-index query APIs")
    )
)]
struct LoonfsOpenApi;

/// OpenAPI definition for the 503 response every operation can return.
#[derive(utoipa::ToResponse)]
#[response(
    description = "The server cannot complete the request now. Inspect `code` to determine whether the cause is a deadline, shutdown, load, required maintenance, or invalid storage credentials. A mutation may still complete after a deadline or lost acknowledgment, so determine its outcome before retrying."
)]
#[expect(
    dead_code,
    reason = "used only to generate the reusable OpenAPI response schema"
)]
pub(super) struct UnavailableResponse(#[to_schema] ApiError);

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
