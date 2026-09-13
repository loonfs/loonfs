//! Request and response shapes for the v0 HTTP API.
//!
#[cfg(feature = "openapi")]
pub mod openapi;

mod commits;
mod downloads;
mod operations;
mod reads;
mod search;
mod uploads;

pub use commits::{
    CommitResponse, CommittedChange, DirectoryBinding, FilesystemChange, ListChangesResponse,
};
pub use downloads::{
    BeginDownloadByInodeRequest, BeginDownloadByInodeResponse, BeginDownloadRequest,
    BeginDownloadResponse,
};
pub use operations::{
    validate_attributes_precondition, AdvanceRetentionRequest, AdvanceRetentionResponse, ApiError,
    Checkpoint, CheckpointOwnerSummary, CommitPrecondition, CommitRequest, CreateCheckpointRequest,
    CreateNamespaceRequest, CreateSnapshotRequest, DeleteCheckpointResponse,
    DeleteDirectoryBehavior, DeleteNamespaceResponse, DeleteSnapshotResponse,
    DeletedCheckpointsByOwner, DeletedObjectCounts, DestinationBehavior, DestinationPrecondition,
    DestinationPreconditionError, ErrorDetails, ExpectedFileState, ExtendSnapshotRequest,
    FileRevision, FilesystemOperation, FlushWalOutcome, FlushWalResponse, ForkNamespaceRequest,
    GcRequest, GcResponse, ListCheckpointsResponse, ListFileRevisionsResponse,
    ListSnapshotsResponse, MetadataCompactionOutcome, MetadataCompactionRequest,
    MetadataCompactionResponse, MetadataMaintenanceRequest, MetadataMaintenanceResponse, Namespace,
    NamespaceDiagnostics, PreconditionFields, ReorganizeStepOutcome, RetainedCandidates,
    RetainedReason, RunMaintenanceRequest, RunMaintenanceResponse, SnapshotSummary,
    StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeRequest, StoreProbeResponse,
    WalFlushStepOutcome,
};
pub use reads::{
    AttributesProjection, FileBytes, ListInodeChildrenResponse, ListPathEntriesResponse,
    ListTrashResponse, PathEntry, PathEntryKind, TrashEntry,
};
pub use search::{
    GrepGcRequest, GrepGcResponse, GrepIndex, GrepIndexLifecycle, GrepMatch, GrepRequest,
    GrepResponse,
};
pub use uploads::{
    BeginUploadRequest, BeginUploadResponse, CompleteMultipartUploadRequest, CompleteUploadRequest,
    CompletedUploadPart, ContentToken, ObjectTransferAccess, SignUploadPartsRequest,
    SignUploadPartsResponse, SignedUploadPart, UploadContentClaim, UploadContentResponse,
    UploadMode, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
