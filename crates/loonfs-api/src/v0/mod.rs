//! The v0 HTTP protocol shapes.
//!
//! Every request/response body served by the v0 HTTP API lives under this
//! module, re-exported flat so `loonfs_api::v0::X` is the canonical path for
//! any v0 shape. The submodules group the surface by plane:
//!
//! - [`operations`] — namespace lifecycle, path-oriented filesystem
//!   operations, file revisions, maintenance, and the `ApiError` body.
//! - [`reads`] — authoritative read results (stat/list entries, file bytes).
//! - [`commits`] — explicit semantic commits and the change feed.
//! - [`uploads`] — upload sessions and direct-put access.
//!
//! The crate root re-exports the common surface for convenience; see the
//! crate docs for the rule.

mod commits;
mod operations;
mod reads;
mod search;
mod uploads;

pub use commits::{
    ChangesResponse, CommitDelta, CommitOp, CommitPrecondition, CommitRequest, CommitResponse,
    CommittedChange,
};
pub use operations::{
    AdvanceRetentionResponse, ApiError, CreateCheckpointRequest, CreateCheckpointResponse,
    CreateNamespaceRequest, DeleteDirectoryBehavior, DeleteNamespaceResponse, ErrorDetails,
    FileRevision, FilesystemOperation, FilesystemOperationRequest, FlushWalOutcome,
    FlushWalResponse, ForkNamespaceRequest, GcRequest, GcResponse, ListFileRevisionsResponse,
    MaintenanceTickOutcome, MaintenanceTickRequest, MaintenanceTickResponse,
    NamespaceStatusResponse, NamespaceSummary, PutBehavior, ReleaseCheckpointResponse,
    RestoreFileRevisionRequest,
};
pub use reads::{AuthoritativeFileBytes, AuthoritativePathEntry, ListPathEntriesResponse};
pub use search::{
    DisableGramsIndexResponse, EnableGramsIndexResponse, GrepMatch, GrepRequest, GrepResponse,
};
pub use uploads::{
    BeginUploadRequest, BeginUploadResponse, CompleteUploadRequest, CompleteUploadResponse,
    DirectPutUpload, ObjectTransferAccess, UploadContentResponse, UploadMode,
    ValidatedContentToken,
};
