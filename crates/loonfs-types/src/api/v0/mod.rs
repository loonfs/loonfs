//! Request and response shapes for the v0 HTTP API.
//!
pub mod headers;
#[cfg(feature = "openapi")]
pub mod openapi;

mod commits;
mod downloads;
mod errors;
mod gc;
mod maintenance;
mod merges;
mod namespaces;
mod operations;
mod preconditions;
mod reads;
mod search;
mod snapshots;
mod store_probe;
mod uploads;

pub use commits::{
    Commit, CommitPrecondition, CommitRequest, DiffEntry, DiffEntryKind, DiffResponse,
    DirectoryBinding, FilesystemEvent, ListChangesResponse, Merge,
};
pub use downloads::{
    CreateDownloadByInodeRequest, CreateDownloadByInodeResponse, CreateDownloadRequest,
    CreateDownloadResponse, CreateRevisionDownloadByInodeRequest, DownloadRange,
};
pub use errors::{ActiveWriter, CommittedReceipt, ErrorDetails, ErrorResponse};
pub use gc::{
    DeletedObjectCounts, DeletedSnapshotsByOwner, GcRequest, GcResponse, GcSkippedWork,
    PoolDeletedObjectCounts, PoolGcResponse, PoolRetainedCandidates, PoolRetainedReason,
    RetainedCandidates, RetainedReason,
};
pub use maintenance::{
    AdvanceRetentionRequest, AdvanceRetentionResponse, MetadataMaintenanceOutcome,
    MetadataMaintenanceRequest, MetadataMaintenanceResponse, MetadataSkippedWork,
    RecoverAdministratorRequest, RecoverAdministratorResponse, RunMaintenanceJobRequest,
    RunMaintenanceJobResponse, RunPoolMaintenanceJobRequest, RunPoolMaintenanceJobResponse,
    SetRetentionWindowRequest, SetRetentionWindowResponse,
};
pub use merges::{
    ForkNamespaceForRebaseRequest, MergeBound, MergeConflict, MergeConflictKind,
    MergeConflictRevision, MergeConflictVersion, MergeForkRequest, NextMergeRangeOutcome,
    NextMergeRangeResponse, ReplayForkRequest, ReplayForkResponse,
};
pub use namespaces::{
    CreateNamespaceRequest, DeleteNamespaceResponse, ForkNamespaceRequest, NamespaceAccessMode,
    NamespaceDiagnostics, NamespaceForkBasis, NamespaceMetadata, NamespaceRebasedFrom,
};
pub use operations::{DeleteDirectoryBehavior, FileContent, FilesystemOperation};
pub use preconditions::{DestinationBehavior, DestinationPrecondition};
pub use reads::{
    AttributesProjection, EntryInodeKind, FileRevision, ListFileRevisionsResponse,
    ListInodeChildrenResponse, ListPathEntriesResponse, ListTrashResponse, PathEntry,
    PathEntryKind, TrashEntry,
};
pub use search::{GrepIndex, GrepIndexStatus, GrepMatch, GrepRequest, GrepResponse, GrepScope};
pub use snapshots::{
    CreateSnapshotRequest, DeleteSnapshotResponse, ExtendSnapshotRequest,
    ListSnapshotRecordsResponse, ListSnapshotsResponse, Snapshot, SnapshotRecord,
    SnapshotRecordOwner,
};
pub use store_probe::{
    StoreProbeCheck, StoreProbeCheckOutcome, StoreProbeRequest, StoreProbeResponse,
};
pub use uploads::{
    CompleteUploadBody, CompletedUploadPart, ContentToken, CreateUploadBody, ObjectTransfer,
    SignUploadPartsRequest, SignUploadPartsResponse, SignedUploadPart, UploadContentClaim,
    UploadMode, UploadPartChecksumClaim, UploadSession, UploadSessionStatus,
};
