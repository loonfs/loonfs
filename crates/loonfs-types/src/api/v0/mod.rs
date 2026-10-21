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

pub use commits::{Commit, DirectoryBinding, FilesystemChange, ListChangesResponse};
pub use downloads::{
    CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
    CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, DownloadRange,
};
pub use operations::{
    validate_access_precondition, validate_attributes_precondition, AdvanceRetentionRequest,
    AdvanceRetentionResponse, Checkpoint, CheckpointOwner, CommitPrecondition, CommitRequest,
    CompactionStepOutcome, CreateCheckpointRequest, CreateNamespaceRequest, CreateSnapshotRequest,
    DeleteCheckpointResponse, DeleteDirectoryBehavior, DeleteNamespaceResponse,
    DeleteSnapshotResponse, DeletedCheckpointsByOwner, DeletedObjectCounts, DestinationBehavior,
    DestinationPrecondition, DestinationPreconditionError, ErrorDetails, ErrorResponse,
    ExpectedFileState, ExtendSnapshotRequest, FileRevision, FilesystemOperation,
    ForkNamespaceRequest, GcRequest, GcResponse, ListCheckpointsResponse,
    ListFileRevisionsResponse, ListSnapshotsResponse, MetadataCompactionOutcome,
    MetadataCompactionResponse, MetadataMaintenanceOutcome, MetadataMaintenanceRequest,
    MetadataMaintenanceResponse, MetadataPassReport, NamespaceAccessMode, NamespaceDiagnostics,
    NamespaceForkBasis, NamespaceMetadata, PreconditionFields, RecoverAdministratorRequest,
    RecoverAdministratorResponse, RetainedCandidates, RetainedReason, RunMaintenanceRequest,
    RunMaintenanceResponse, Snapshot, StoreProbeCheckOutcome, StoreProbeCheckResult,
    StoreProbeRequest, StoreProbeResponse, WalFoldStepOutcome,
};
pub use reads::{
    AttributesProjection, EntryInodeKind, ListInodeChildrenResponse, ListPathEntriesResponse,
    ListTrashResponse, PathEntry, PathEntryKind, TrashEntry,
};
pub use search::{GrepIndex, GrepIndexStatus, GrepMatch, GrepRequest, GrepResponse};
pub use uploads::{
    CompleteUploadBody, CompletedUploadPart, ContentToken, CreateUploadBody, ObjectTransferAccess,
    SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart, UploadContentClaim,
    UploadMode, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
