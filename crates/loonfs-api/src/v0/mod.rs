//! The v0 HTTP protocol shapes.
//!
//! Every request/response body served by the v0 HTTP API lives under this
//! module, re-exported flat so `loonfs_api::v0::X` is the canonical path for
//! any v0 shape. The submodules group the surface by plane:
//!
//! - `operations` — namespace lifecycle, path-oriented filesystem
//!   operations, file revisions, maintenance, and the `ApiError` body.
//! - `reads` — authoritative read results (stat/list entries, file bytes).
//! - `commits` — commit results and the change feed.
//! - `search` — content search and grep-index administration.
//! - `uploads` — upload sessions and direct-put access.
//! - `downloads` — direct-get download grants.
//!
//! The crate root re-exports the common surface for convenience; see the
//! crate docs for the rule.

mod commits;
mod downloads;
mod operations;
mod reads;
mod search;
mod uploads;

pub use commits::{ChangesResponse, CommitResponse, CommittedChange, FilesystemChange};
pub use downloads::{BeginDownloadRequest, BeginDownloadResponse};
pub use operations::{
    AdvanceRetentionResponse, ApiError, CheckpointOwnerSummary, CheckpointSummary, CommitRequest,
    CreateCheckpointRequest, CreateCheckpointResponse, CreateNamespaceRequest,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DestinationBehavior, ErrorDetails,
    FileRevision, FilesystemOperation, FlushWalOutcome, FlushWalResponse, ForkNamespaceRequest,
    GcRequest, GcResponse, ListCheckpointsResponse, ListFileRevisionsResponse,
    MaintenanceStepRequest, MaintenanceStepResponse, MetadataMaintenanceRequest,
    MetadataMaintenanceResponse, NamespaceStatusResponse, NamespaceSummary,
    ReleaseCheckpointResponse, ReorganizeStepOutcome, RetainedCandidates, RetainedReason,
    StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeRequest, StoreProbeResponse,
    WalFlushStepOutcome,
};
pub use reads::{
    AuthoritativeFileBytes, AuthoritativePathEntry, ListPathEntriesResponse, ListTrashResponse,
    TrashEntry,
};
pub use search::{
    DisableGrepIndexResponse, EnableGrepIndexResponse, GrepGcRequest, GrepGcResponse,
    GrepIndexLifecycle, GrepIndexStatusResponse, GrepMatch, GrepRequest, GrepResponse,
};
pub use uploads::{
    AbortUploadResponse, BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest,
    CompleteUploadResponse, CompletedUploadPart, DirectMultipartContentClaim,
    DirectMultipartUpload, DirectMultipartUploadOptions, DirectPutContentClaim, DirectPutUpload,
    ObjectTransferAccess, SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart,
    UploadContentResponse, UploadMode, UploadPartChecksumClaim, UploadSessionStatus,
    UploadStatusResponse, ValidatedContentToken,
};
