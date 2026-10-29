//! Request and response shapes for the v0 HTTP API.
//!
#[cfg(feature = "openapi")]
pub mod openapi;

mod commits;
mod downloads;
mod errors;
mod gc;
mod maintenance;
mod namespaces;
mod operations;
mod preconditions;
mod reads;
mod search;
mod snapshots;
mod store_probe;
mod uploads;

pub use commits::{
    Commit, CommitPrecondition, CommitRequest, DirectoryBinding, FilesystemChange,
    ListChangesResponse,
};
pub use downloads::{
    CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
    CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, DownloadRange,
};
pub use errors::{ErrorDetails, ErrorResponse};
pub use gc::{
    DeletedCheckpointsByOwner, DeletedObjectCounts, ExtensionGcReports, GcRequest, GcResponse,
    GrepGcReport, RetainedCandidates, RetainedReason,
};
pub use maintenance::{
    AdvanceRetentionRequest, AdvanceRetentionResponse, MetadataCompactionOutcome,
    MetadataCompactionResponse, MetadataMaintenanceOutcome, MetadataMaintenanceRequest,
    MetadataMaintenanceResponse, MetadataPassReport, RecoverAdministratorRequest,
    RecoverAdministratorResponse, RunMaintenanceRequest, RunMaintenanceResponse,
    SetRetentionWindowRequest, SetRetentionWindowResponse, WalFoldStepOutcome,
};
pub use namespaces::{
    CreateNamespaceRequest, DeleteNamespaceResponse, ForkNamespaceRequest, NamespaceAccessMode,
    NamespaceDiagnostics, NamespaceForkBasis, NamespaceMetadata,
};
pub use operations::{DeleteDirectoryBehavior, FilesystemOperation};
pub use preconditions::{
    validate_access_precondition, validate_attributes_precondition, DestinationBehavior,
    DestinationPrecondition, DestinationPreconditionError, ExpectedFileState, PreconditionFields,
};
pub use reads::{
    AttributesProjection, EntryInodeKind, FileRevision, ListFileRevisionsResponse,
    ListInodeChildrenResponse, ListPathEntriesResponse, ListTrashResponse, PathEntry,
    PathEntryKind, TrashEntry,
};
pub use search::{GrepIndex, GrepIndexStatus, GrepMatch, GrepRequest, GrepResponse};
pub use snapshots::{
    Checkpoint, CheckpointOwner, CreateSnapshotRequest, DeleteSnapshotResponse,
    ExtendSnapshotRequest, ListCheckpointsResponse, ListSnapshotsResponse, Snapshot,
};
pub use store_probe::{
    StoreProbeCheckOutcome, StoreProbeCheckResult, StoreProbeRequest, StoreProbeResponse,
};
pub use uploads::{
    CompleteUploadBody, CompletedUploadPart, ContentToken, CreateUploadBody, ObjectTransferAccess,
    SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart, UploadContentClaim,
    UploadMode, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
