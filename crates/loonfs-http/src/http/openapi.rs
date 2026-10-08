//! Static OpenAPI document assembly for the v0 HTTP API.
//!
//! Handlers define their own `#[utoipa::path]` metadata. This module combines
//! those operations and their schemas into one document. Each handler sets an
//! explicit operation ID, so changing its Rust function name does not rename
//! the generated SDK method.

mod host;

use loonfs_types::ChangeSeq;
use loonfs_types::{
    api::v0::{
        Commit, CompleteUploadBody, ContentToken, CreateDownloadByInodeResponse,
        CreateDownloadRequest, CreateDownloadResponse, CreateUploadBody, DirectoryBinding,
        ListChangesResponse, ObjectTransferAccess, UploadMode, UploadSession, UploadSessionStatus,
    },
    AdvanceRetentionRequest, AdvanceRetentionResponse, ApiError, Checkpoint,
    CheckpointOwnerSummary, CommitRequest, CompactionStepOutcome, ContentRef,
    CreateCheckpointRequest, CreateNamespaceRequest, CreateSnapshotRequest,
    DeleteCheckpointResponse, DeleteSnapshotResponse, DeletedCheckpointsByOwner,
    DeletedObjectCounts, ExtendSnapshotRequest, FilesystemOperation, ForkNamespaceRequest,
    GcRequest, GcResponse, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListSnapshotsResponse, ListTrashResponse, MetadataCompactionOutcome, MetadataCompactionRequest,
    MetadataCompactionResponse, MetadataMaintenanceRequest, MetadataMaintenanceResponse, PinId,
    RetainedCandidates, RevisionNo, RunMaintenanceRequest, RunMaintenanceResponse, SnapshotSummary,
    TrashEntry, WalFoldStepOutcome,
};

/// Builds the static OpenAPI document for the v0 HTTP API.
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    let mut document = <LoonfsOpenApi as utoipa::OpenApi>::openapi();
    loonfs_types::api::v0::openapi::register(
        &mut document
            .components
            .as_mut()
            .expect("HTTP components")
            .schemas,
    );
    document
}

#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "LoonFS HTTP API",
        version = env!("CARGO_PKG_VERSION"),
        description = "Static OpenAPI document for the LoonFS v0 HTTP API."
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
        crate::http::handlers_namespace::create_snapshot,
        crate::http::handlers_namespace::list_snapshots,
        crate::http::handlers_namespace::extend_snapshot,
        crate::http::handlers_namespace::delete_snapshot,
        crate::http::handlers_filesystem::list_path_entries,
        crate::http::handlers_filesystem::get_path_entry,
        crate::http::handlers_filesystem::get_file_bytes,
        crate::http::handlers_downloads::create_download,
        crate::http::handlers_filesystem::list_file_revisions,
        crate::http::handlers_inodes::get_inode,
        crate::http::handlers_inodes::list_inode_children,
        crate::http::handlers_inodes::get_file_bytes_by_inode,
        crate::http::handlers_downloads::create_download_by_inode,
        crate::http::handlers_inodes::list_file_revisions_by_inode,
        crate::http::handlers_inodes::get_file_revision_bytes_by_inode,
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
        crate::http::handlers_namespace::create_checkpoint,
        crate::http::handlers_namespace::list_checkpoints,
        crate::http::handlers_namespace::delete_checkpoint,
        crate::http::handlers_namespace::run_maintenance,
        crate::http::handlers_query::grep,
        crate::http::handlers_query::get_grep_index,
        crate::http::handlers_query::enable_grep_index,
        crate::http::handlers_query::disable_grep_index,
        crate::http::handlers_store::probe_store
    ),
    components(
        schemas(
        loonfs_types::CapabilityDocument,
        ApiError,
        loonfs_types::ErrorDetails,
        loonfs_types::WriterEpoch,
        CreateNamespaceRequest,
        ForkNamespaceRequest,
        loonfs_types::NamespaceMetadata,
        loonfs_types::NamespaceDiagnostics,
        loonfs_types::DeleteNamespaceResponse,
        loonfs_types::DestinationBehavior,
        loonfs_types::DeleteDirectoryBehavior,
        FilesystemOperation,
        CommitRequest,
        loonfs_types::FileRevision,
        ListFileRevisionsResponse,
        ListTrashResponse,
        TrashEntry,
        CreateCheckpointRequest,
        Checkpoint,
        PinId,
        CheckpointOwnerSummary,
        ListCheckpointsResponse,
        DeleteCheckpointResponse,
        CreateSnapshotRequest,
        ExtendSnapshotRequest,
        SnapshotSummary,
        ListSnapshotsResponse,
        DeleteSnapshotResponse,
        RunMaintenanceRequest,
        MetadataMaintenanceRequest,
        MetadataCompactionRequest,
        AdvanceRetentionRequest,
        WalFoldStepOutcome,
        CompactionStepOutcome,
        MetadataMaintenanceResponse,
        MetadataCompactionResponse,
        MetadataCompactionOutcome,
        AdvanceRetentionResponse,
        RunMaintenanceResponse,
        GcRequest,
        GcResponse,
        DeletedObjectCounts,
        DeletedCheckpointsByOwner,
        RetainedCandidates,
        ContentRef,
        loonfs_types::Checksum,
        loonfs_types::ChecksumAlgorithm,
        loonfs_types::ContentId,
        loonfs_types::api::v0::UploadContentClaim,
        loonfs_types::NamespaceId,
        loonfs_types::CommitId,
        RevisionNo,
        ChangeSeq,
        loonfs_types::ManifestNo,
        loonfs_types::NameKey,
        loonfs_types::AttributeKey,
        loonfs_types::AttributeValue,
        loonfs_types::Attributes,
        loonfs_types::AttributesRevisionNo,
        loonfs_types::PathEntry,
        loonfs_types::PathEntryKind,
        loonfs_types::AttributesProjection,
        loonfs_types::ListPathEntriesResponse,
        loonfs_types::ListInodeChildrenResponse,
        UploadMode,
        CreateUploadBody,
        CompleteUploadBody,
        UploadSession,
        UploadSessionStatus,
        loonfs_types::api::v0::UploadPartChecksumClaim,
        loonfs_types::api::v0::SignUploadPartsRequest,
        loonfs_types::api::v0::SignedUploadPart,
        loonfs_types::api::v0::SignUploadPartsResponse,
        loonfs_types::api::v0::CompletedUploadPart,
        CreateDownloadRequest,
        CreateDownloadResponse,
        CreateDownloadByInodeResponse,
        ObjectTransferAccess,
        ContentToken,
        Commit,
        loonfs_types::api::v0::FilesystemChange,
        DirectoryBinding,
        ListChangesResponse,
        loonfs_types::api::v0::GrepMatch,
        loonfs_types::api::v0::GrepResponse,
        loonfs_types::api::v0::GrepIndexLifecycle,
        loonfs_types::api::v0::GrepIndex,
        loonfs_types::api::v0::StoreProbeRequest,
        loonfs_types::api::v0::StoreProbeCheckOutcome,
        loonfs_types::api::v0::StoreProbeCheckResult,
        loonfs_types::api::v0::StoreProbeResponse
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
        (name = "maintenance", description = "Maintenance APIs"),
        (name = "query", description = "Derived-index query APIs")
    )
)]
struct LoonfsOpenApi;

/// OpenAPI definition for the 503 response every operation can return.
#[derive(utoipa::ToResponse)]
#[response(
    description = "The server cannot complete the request now. Inspect `code` to determine whether the cause is a deadline, shutdown, load, writer-session admission, required maintenance, or invalid storage credentials. The reference server returns `server_busy` with `Retry-After: 1` before reading a body when its request cap is full; health and readiness probes are exempt. A mutation may still complete after a deadline or lost acknowledgment, so determine its outcome before retrying."
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
                             with a token answers 401 `unauthorized` without it after request admission.",
                        ))
                        .build(),
                ),
            );
    }
}
