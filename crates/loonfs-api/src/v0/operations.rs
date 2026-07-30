//! Request/response shapes for the v0 HTTP API's operation endpoints:
//! namespace lifecycle (create/fork/status/delete), path-oriented filesystem
//! operations, file revisions, maintenance (checkpoint/retention), and the
//! shared [`ApiError`] body. Explicit commits and the change feed live in
//! [`super::commits`]; read-result shapes live in [`super::reads`].

use super::ValidatedContentToken;
use crate::{
    AbsolutePath, ChangeSeq, CheckpointId, CommitId, ContentRef, InodeId, ManifestId, NamespaceId,
    RevisionNo, WriterEpoch,
};
use serde::{Deserialize, Serialize};

/// HTTP error body used by LoonFS APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ApiError {
    /// Stable machine-readable reason from the [`ErrorCode`](crate::ErrorCode)
    /// registry.
    ///
    /// Carried as a string so clients keep working when a newer server
    /// introduces a code they do not know; use
    /// [`ErrorCode::parse`](crate::ErrorCode::parse) for typed access.
    pub code: String,
    /// For `not_supported` errors, the capability-document feature key the
    /// client should reconcile against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature: Option<String>,
    /// Human-readable error message.
    pub message: String,
    /// Correlation id the server assigned to the failed request; the same
    /// value is sent as the `x-request-id` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Structured context for the code, present when the failure carries
    /// machine-usable identity (API spec, "Standard error contract"). Boxed
    /// so the rare detailed error does not widen every error-carrying result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Box<ErrorDetails>>,
}

/// Structured, machine-readable context accompanying an [`ApiError`].
///
/// Every field is optional: a code populates the fields that apply to it
/// (API spec, "Standard error contract"), and clients must tolerate absent
/// fields exactly as they tolerate unknown codes. Retry decisions still key
/// off the code; these fields carry the identity a caller needs to act —
/// which commit to resubmit, which epoch displaced it, which revision the
/// precondition saw.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorDetails {
    /// Idempotency key of the mutation the error concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<CommitId>,
    /// Position, in the request's operation list, of the operation that
    /// failed. A mutation commits all of its operations or none of them, so
    /// this names the one that stopped the whole request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_index: Option<u32>,
    /// Epoch the failing writer session held when it was displaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fenced_epoch: Option<WriterEpoch>,
    /// Epoch that currently owns the namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer_epoch: Option<WriterEpoch>,
    /// Writer id recorded by the current epoch's acquirer, when the head
    /// recorded one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_writer: Option<String>,
    /// Unix milliseconds at which the current epoch's acquirer took it, when
    /// the head recorded one. Writer ids are process labels, so two runs on
    /// one machine can share one; the stamp is what tells them apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_acquired_at_ms: Option<u64>,
    /// Inode the failed precondition or operation targeted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode_id: Option<InodeId>,
    /// Revision the request expected to be current.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<RevisionNo>,
    /// Revision that is actually current; absent when the inode has none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_revision: Option<RevisionNo>,
    /// Change-feed cursor the request asked to resume after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_seq: Option<ChangeSeq>,
    /// Oldest sequence still promised for incremental replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_floor_seq: Option<ChangeSeq>,
    /// Deletion generation an undelete asked to recover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_deletion_seq: Option<ChangeSeq>,
    /// Deletion generation actually active for the inode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_deletion_seq: Option<ChangeSeq>,
}

/// Request to create a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateNamespaceRequest {
    /// Durable namespace id to create.
    pub namespace_id: NamespaceId,
}

/// Request to fork a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ForkNamespaceRequest {
    /// Durable namespace id for the fork target.
    pub new_namespace_id: NamespaceId,
}

/// Short namespace identifier returned by namespace create/fork operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceSummary {
    /// Durable namespace id.
    pub namespace_id: NamespaceId,
}

/// Status summary for one namespace.
///
/// This is the point-lookup answer to "does this namespace exist, and where
/// is its head?" — cheaper than listing all namespaces when only one matters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct NamespaceStatusResponse {
    /// Namespace being inspected.
    pub namespace_id: NamespaceId,
    /// Current visible namespace sequence.
    pub head_seq: ChangeSeq,
    /// Current manifest pointer recorded by the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_manifest_id: Option<ManifestId>,
    /// Number of visible WAL segments after the current manifest.
    pub wal_tail_segments: u64,
    /// Oldest sequence still promised for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// Result of deleting a namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct DeleteNamespaceResponse {
    /// Namespace whose history ended.
    pub namespace_id: NamespaceId,
    /// The head's last committed sequence; the delete linearized
    /// immediately after it, so this is where history ended.
    pub head_seq: ChangeSeq,
}

/// Destination-conflict behavior for path-oriented puts, moves, and copies.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationBehavior {
    /// Fail if the destination path already exists.
    #[default]
    NoReplace,
    /// Replace the current file at the destination; only a file
    /// destination can be replaced.
    Replace,
}

/// Directory delete behavior for path-oriented deletes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DeleteDirectoryBehavior {
    /// Fail if the target is a non-empty directory.
    #[default]
    NonRecursive,
    /// Delete a directory subtree.
    Recursive,
}

/// One path-oriented filesystem operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FilesystemOperation {
    /// Create one directory.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpCreateDirectory"))]
    CreateDirectory {
        /// Absolute destination path, rejected when invalid or already bound.
        path: AbsolutePath,
        /// Also create missing ancestor directories (the same auto-create
        /// `put_file` performs). The final component must still be new.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        parents: bool,
    },
    /// Create or replace one file with an already-durable content ref.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpPutFile"))]
    PutFile {
        /// Absolute destination path; missing ancestors are created automatically.
        path: AbsolutePath,
        /// Immutable bytes that must be covered by a valid preparation proof.
        content_ref: ContentRef,
        /// Whether an existing file may receive a new revision instead of causing a conflict.
        #[serde(default)]
        behavior: DestinationBehavior,
        /// When set (with `replace` behavior), the put applies only while
        /// the file's current revision is still this one; a raced write
        /// fails the request instead of silently stacking on it, and a
        /// missing file answers `path_not_found`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_revision_no: Option<RevisionNo>,
    },
    /// Delete one path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpDeletePath"))]
    DeletePath {
        /// Absolute path that must resolve to a visible inode.
        path: AbsolutePath,
        /// Whether a non-empty directory may be tombstoned recursively.
        #[serde(default)]
        behavior: DeleteDirectoryBehavior,
        /// When set, the delete applies only if the path still resolves to
        /// this inode; a raced rebinding fails the request instead of
        /// deleting (and reporting a recovery handle for) the wrong inode.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_inode_id: Option<InodeId>,
    },
    /// Move one path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpMovePath"))]
    MovePath {
        /// Absolute source path that must resolve to a visible inode.
        from_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        to_path: AbsolutePath,
        /// Whether an existing destination file may be replaced.
        #[serde(default)]
        behavior: DestinationBehavior,
    },
    /// Copy one file path to another path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpCopyPath"))]
    CopyPath {
        /// Absolute source path that must resolve to a visible file.
        from_path: AbsolutePath,
        /// Absolute destination whose parent must be visible and writable.
        to_path: AbsolutePath,
        /// Whether an existing destination file may receive a copied revision.
        #[serde(default)]
        behavior: DestinationBehavior,
    },
    /// Recover a deleted file or subtree: revoke the deletion of
    /// `inode_id` recorded at `deleted_at_seq` (both reported by the
    /// delete and by the change feed) and re-bind it at `path`. Answers
    /// `not_deleted` when that generation is not the live one, so a stale
    /// request never cancels a later delete.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpUndelete"))]
    Undelete {
        /// Deleted inode to make reachable again.
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer tombstone generation.
        deleted_at_seq: ChangeSeq,
        /// Absolute destination path whose parent must be visible and whose name must be absent.
        path: AbsolutePath,
    },
    /// Restore an older revision as the current revision for a path.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpRestoreRevision"))]
    RestoreRevision {
        /// Absolute path that must resolve to a visible file.
        path: AbsolutePath,
        /// Existing historical revision whose content will be copied into a new current revision.
        source_revision_no: RevisionNo,
    },
}

/// Request wrapper for one path-oriented operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FilesystemOperationRequest {
    /// Caller-supplied idempotency key for this operation.
    pub commit_id: CommitId,
    /// Caller annotation recorded on the commit and reported by the change
    /// feed. Part of the operation's identity: reusing `commit_id` with a
    /// different message is a `commit_id_reuse_conflict`, exactly as it is
    /// for an explicit commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Proofs for any new external content refs introduced by this operation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ValidatedContentToken>,
    /// Operation to apply.
    pub operation: FilesystemOperation,
}

/// One immutable file revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FileRevision {
    /// File inode that owns this revision.
    pub inode_id: InodeId,
    /// Revision number within the file inode.
    pub revision_no: RevisionNo,
    /// Namespace sequence that created this revision.
    pub committed_seq: ChangeSeq,
    /// Wall-clock stamp of the commit that created this revision, in Unix
    /// milliseconds. Observational: `committed_seq` is the order.
    pub committed_at_ms: u64,
    /// Content stored for this revision.
    pub content_ref: ContentRef,
}

/// Response for listing file revisions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListFileRevisionsResponse {
    /// Namespace that was read.
    pub namespace_id: NamespaceId,
    /// File inode whose revisions were returned.
    pub inode_id: InodeId,
    /// Namespace head sequence used for the read.
    pub head_seq: ChangeSeq,
    /// Retained revisions in order.
    pub revisions: Vec<FileRevision>,
    /// Opaque cursor for the next page, if more revisions are available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Request to create a durable checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateCheckpointRequest {
    /// Label recorded on the checkpoint record. A label, not a key: several
    /// records may carry the same name over different bases.
    pub name: String,
    /// Optional lifetime; the server computes the record's expiry from its
    /// own clock. Absent means the pin holds until explicitly released.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
}

/// Result of creating a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CreateCheckpointResponse {
    /// Namespace that was checkpointed.
    pub namespace_id: NamespaceId,
    /// Durable checkpoint id.
    pub checkpoint_id: CheckpointId,
    /// Sequence covered by the checkpoint.
    pub checkpoint_seq: ChangeSeq,
    /// Manifest pinned by the checkpoint.
    pub manifest_id: ManifestId,
    /// Manifest `metadata/root.json` references after the operation.
    pub current_manifest_id: Option<ManifestId>,
    /// Expiry recorded on the record, when the request carried a `ttl_ms`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
}

/// Result of releasing a checkpoint pin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ReleaseCheckpointResponse {
    /// Namespace the checkpoint belonged to.
    pub namespace_id: NamespaceId,
    /// Checkpoint the release targeted.
    pub checkpoint_id: CheckpointId,
    /// True when this call flipped an active record to released; false when
    /// the record was already released or no longer exists. Release is
    /// idempotent — the end state is the same either way.
    pub was_active: bool,
}

/// How one WAL flush satisfied its goal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum FlushWalOutcome {
    /// The root already covered the head; nothing was published.
    AlreadyCurrent,
    /// This call published a new manifest and advanced the root to it.
    Published,
    /// This call published a manifest, but a newer root already covered
    /// the attempted sequence.
    Superseded,
}

/// Result of one WAL flush: how the metadata root covers the head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct FlushWalResponse {
    /// Namespace whose WAL tail was flushed.
    pub namespace_id: NamespaceId,
    /// Head sequence the flush attempted to cover.
    pub target_head_seq: ChangeSeq,
    /// Manifest `metadata/root.json` references after the operation.
    pub manifest_id: ManifestId,
    /// Sequence covered by that manifest.
    pub manifest_head_seq: ChangeSeq,
    /// How the root came to cover the head.
    pub outcome: FlushWalOutcome,
}

/// Optional overrides for one garbage-collection pass. Absent fields use
/// the server's conservative defaults.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcRequest {
    /// Objects younger than this are never deleted, reachable or not. The
    /// window has a derived safety floor (publication budgets plus provider
    /// deadlines); a smaller value is rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_window_ms: Option<u64>,
    /// Upload sessions older than this may be reaped. Must be at least the
    /// grace window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reap_window_ms: Option<u64>,
    /// Maximum candidates examined by this invocation. Omit to retain the
    /// run-to-completion behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque resume token returned as `next_cursor` by an earlier pass
    /// against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of one mark-and-sweep garbage-collection pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResponse {
    /// Namespace the pass ran against.
    pub namespace_id: NamespaceId,
    /// Unreferenced WAL segments deleted.
    pub deleted_wal_segments: u64,
    /// Unreferenced metadata tables deleted.
    pub deleted_metadata_tables: u64,
    /// Unreferenced manifests deleted.
    pub deleted_manifests: u64,
    /// Released checkpoint records deleted after their grace window.
    pub deleted_checkpoint_records: u64,
    /// Fork-owned checkpoint records released because their target namespace
    /// is provably gone.
    pub released_fork_checkpoints: u64,
    /// Checkpoint records released because their expiry passed, or because
    /// they sit on a terminally deleted namespace.
    #[serde(default)]
    pub released_expired_checkpoints: u64,
    /// Upload-session control objects deleted after the reap window.
    #[serde(default)]
    pub deleted_upload_sessions: u64,
    /// Active checkpoint records released because their basis manifest is
    /// verifiably gone.
    #[serde(default)]
    pub released_missing_basis_checkpoints: u64,
    /// Candidates retained at delete time (grace window, missing
    /// timestamps, or reachable from the fresh root set).
    pub retained_candidates: u64,
    /// True when ambiguous roots suppressed manifest/table deletion.
    pub degraded_retention: bool,
    /// Opaque resume token when more candidates remain. Resuming rebuilds
    /// every safety proof; the token carries enumeration position only and
    /// is valid only against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl GcResponse {
    /// An empty report for `namespace_id`, before any candidate is examined.
    pub fn empty(namespace_id: NamespaceId) -> Self {
        Self {
            namespace_id,
            deleted_wal_segments: 0,
            deleted_metadata_tables: 0,
            deleted_manifests: 0,
            deleted_checkpoint_records: 0,
            released_fork_checkpoints: 0,
            released_expired_checkpoints: 0,
            deleted_upload_sessions: 0,
            released_missing_basis_checkpoints: 0,
            retained_candidates: 0,
            degraded_retention: false,
            next_cursor: None,
        }
    }
}

/// Result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// Namespace whose retention floor changed.
    pub namespace_id: NamespaceId,
    /// New minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// One sub-step of a maintenance step, for callers that want to run exactly
/// one of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceStepKind {
    /// Fold the visible WAL tail into metadata tables and advance the root.
    WalFlush,
    /// Merge one bounded group of metadata delta runs into its base.
    Reorganize,
    /// Advance the retention floor behind a verified checkpoint.
    Retention,
    /// Run the mark-and-sweep garbage collector.
    Gc,
}

/// Options for one explicit maintenance step. Absent fields use the
/// server's defaults; retention advance runs only when `retention` is true
/// and garbage collection only when `gc` is present.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceStepRequest {
    /// Flush the visible WAL tail into metadata tables when it reaches this
    /// many segments. Values above the write-rejection threshold are
    /// rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_tail_segments: Option<u64>,
    /// Advance the retention floor to the flushed manifest head. Nothing
    /// surrenders replay history unless this is true or `only` selects
    /// `retention`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<bool>,
    /// Run the mark-and-sweep garbage collector after the step's
    /// flush work. Nothing sweeps unless this is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcRequest>,
    /// Restrict the step to one sub-step. Absent runs the whole step: WAL
    /// flush, then reorganization, then retention if `retention` opted in,
    /// then garbage collection if `gc` opted in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only: Option<MaintenanceStepKind>,
}

/// What the WAL-flush part of a maintenance step did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WalFlushStepOutcome {
    /// The step did not run this sub-step: the tail was below the threshold,
    /// or `only` selected something else.
    NotNeeded,
    /// The step flushed the WAL tail and advanced the metadata root.
    Flushed {
        /// Sequence covered by the published manifest.
        manifest_head_seq: ChangeSeq,
    },
    /// The root already covered the attempted sequence — another publisher
    /// got there first.
    Superseded {
        /// Sequence this step attempted to flush through.
        attempted_seq: ChangeSeq,
        /// Manifest the root currently references.
        current_manifest_id: ManifestId,
    },
    /// A concurrent head update won the race.
    RaceLost {
        /// Head sequence observed before the advance attempt.
        observed_head_seq: ChangeSeq,
    },
}

/// What the metadata-reorganization part of a maintenance step did.
///
/// Deliberately coarse: the run counts and byte budgets a reorganization
/// consumes are engine policy, not a wire contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReorganizeStepOutcome {
    /// No family group had enough delta runs to merge, or `only` selected
    /// something else.
    NotNeeded,
    /// One family group was merged and a manifest published.
    UnitPublished,
    /// A group needs merging but no progress-making subset fits the
    /// per-step budget.
    BudgetExhausted,
    /// Another publisher advanced the root first; a later step retries.
    Superseded,
}

/// Result of one explicit maintenance step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceStepResponse {
    /// Namespace the step ran against.
    pub namespace_id: NamespaceId,
    /// Namespace status observed before the step acted.
    pub status_before: NamespaceStatusResponse,
    /// What the WAL-flush sub-step did.
    pub wal_flush: WalFlushStepOutcome,
    /// What the metadata-reorganization sub-step did.
    pub reorganize: ReorganizeStepOutcome,
    /// The retention floor after the step. Compare with
    /// `status_before.retention_floor_seq` to see whether it moved.
    pub retention_floor_seq: ChangeSeq,
    /// Garbage-collection report when the step opted into sweeping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcResponse>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> AbsolutePath {
        AbsolutePath::parse(value).expect("valid test path")
    }

    #[test]
    fn behavior_enums_use_snake_case_wire_values() {
        assert_eq!(
            DestinationBehavior::default(),
            DestinationBehavior::NoReplace
        );
        assert_eq!(
            DeleteDirectoryBehavior::default(),
            DeleteDirectoryBehavior::NonRecursive
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::NoReplace)
                .expect("destination behavior json"),
            serde_json::json!("no_replace")
        );
        assert_eq!(
            serde_json::to_value(DestinationBehavior::Replace).expect("destination behavior json"),
            serde_json::json!("replace")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::NonRecursive)
                .expect("delete behavior json"),
            serde_json::json!("non_recursive")
        );
        assert_eq!(
            serde_json::to_value(DeleteDirectoryBehavior::Recursive).expect("delete behavior json"),
            serde_json::json!("recursive")
        );
    }

    #[test]
    fn filesystem_delete_and_move_operations_use_behavior_field() {
        let create_directory = FilesystemOperation::CreateDirectory {
            path: path("/docs"),
            parents: false,
        };
        assert_eq!(
            serde_json::to_value(&create_directory).expect("create directory op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs"
            })
        );

        let create_directory_with_parents = FilesystemOperation::CreateDirectory {
            path: path("/docs/notes"),
            parents: true,
        };
        assert_eq!(
            serde_json::to_value(&create_directory_with_parents)
                .expect("create directory with parents op json"),
            serde_json::json!({
                "kind": "create_directory",
                "path": "/docs/notes",
                "parents": true
            })
        );

        let delete = FilesystemOperation::DeletePath {
            path: path("/docs"),
            behavior: DeleteDirectoryBehavior::Recursive,
            expected_inode_id: None,
        };
        assert_eq!(
            serde_json::to_value(&delete).expect("delete op json"),
            serde_json::json!({
                "kind": "delete_path",
                "path": "/docs",
                "behavior": "recursive"
            })
        );

        let move_path = FilesystemOperation::MovePath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::Replace,
        };
        assert_eq!(
            serde_json::to_value(&move_path).expect("move op json"),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace"
            })
        );

        let copy_path = FilesystemOperation::CopyPath {
            from_path: path("/docs/a.txt"),
            to_path: path("/docs/b.txt"),
            behavior: DestinationBehavior::Replace,
        };
        assert_eq!(
            serde_json::to_value(&copy_path).expect("copy op json"),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/docs/a.txt",
                "to_path": "/docs/b.txt",
                "behavior": "replace"
            })
        );
    }

    #[test]
    fn filesystem_operations_default_omitted_behavior_fields() {
        let put: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "put_file",
            "path": "/docs/a.txt",
            "content_ref": {
                "kind": "whole_file_v0",
                "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size_bytes": 1
            }
        }))
        .expect("put op defaults behavior");
        assert!(matches!(
            put,
            FilesystemOperation::PutFile {
                behavior: DestinationBehavior::NoReplace,
                expected_revision_no: None,
                ..
            }
        ));

        let delete: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "delete_path",
            "path": "/docs"
        }))
        .expect("delete op defaults behavior");
        assert_eq!(
            delete,
            FilesystemOperation::DeletePath {
                path: path("/docs"),
                behavior: DeleteDirectoryBehavior::NonRecursive,
                expected_inode_id: None,
            }
        );

        let move_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "move_path",
            "from_path": "/docs/a.txt",
            "to_path": "/docs/b.txt"
        }))
        .expect("move op defaults behavior");
        assert_eq!(
            move_path,
            FilesystemOperation::MovePath {
                from_path: path("/docs/a.txt"),
                to_path: path("/docs/b.txt"),
                behavior: DestinationBehavior::NoReplace,
            }
        );

        let copy_path: FilesystemOperation = serde_json::from_value(serde_json::json!({
            "kind": "copy_path",
            "from_path": "/docs/a.txt",
            "to_path": "/docs/b.txt"
        }))
        .expect("copy op defaults behavior");
        assert_eq!(
            copy_path,
            FilesystemOperation::CopyPath {
                from_path: path("/docs/a.txt"),
                to_path: path("/docs/b.txt"),
                behavior: DestinationBehavior::NoReplace,
            }
        );
    }

    #[test]
    fn filesystem_operation_paths_keep_the_plain_string_wire_shape() {
        let content_ref = ContentRef::whole_file_v0(b"hello");
        let cases = [
            (
                FilesystemOperation::PutFile {
                    path: path("/docs/a.txt"),
                    content_ref: content_ref.clone(),
                    behavior: DestinationBehavior::NoReplace,
                    expected_revision_no: None,
                },
                serde_json::json!({
                    "kind": "put_file",
                    "path": "/docs/a.txt",
                    "content_ref": content_ref,
                    "behavior": "no_replace"
                }),
            ),
            (
                FilesystemOperation::Undelete {
                    inode_id: InodeId(7),
                    deleted_at_seq: ChangeSeq(8),
                    path: path("/docs/restored"),
                },
                serde_json::json!({
                    "kind": "undelete",
                    "inode_id": 7,
                    "deleted_at_seq": 8,
                    "path": "/docs/restored"
                }),
            ),
            (
                FilesystemOperation::RestoreRevision {
                    path: path("/docs/a.txt"),
                    source_revision_no: RevisionNo(2),
                },
                serde_json::json!({
                    "kind": "restore_revision",
                    "path": "/docs/a.txt",
                    "source_revision_no": 2
                }),
            ),
        ];

        for (operation, string_shaped_json) in cases {
            assert_eq!(
                serde_json::to_value(operation).expect("serialize filesystem operation"),
                string_shaped_json
            );
        }
    }

    #[test]
    fn filesystem_operation_paths_validate_during_deserialization() {
        for encoded in [
            serde_json::json!({"kind": "create_directory", "path": "relative", "parents": false}),
            serde_json::json!({
                "kind": "put_file",
                "path": "relative",
                "content_ref": ContentRef::whole_file_v0(b"hello")
            }),
            serde_json::json!({"kind": "delete_path", "path": "relative"}),
            serde_json::json!({
                "kind": "move_path",
                "from_path": "relative",
                "to_path": "/target"
            }),
            serde_json::json!({
                "kind": "copy_path",
                "from_path": "/source",
                "to_path": "relative"
            }),
            serde_json::json!({
                "kind": "undelete",
                "inode_id": 7,
                "deleted_at_seq": 8,
                "path": "relative"
            }),
            serde_json::json!({
                "kind": "restore_revision",
                "path": "relative",
                "source_revision_no": 2
            }),
        ] {
            assert!(serde_json::from_value::<FilesystemOperation>(encoded).is_err());
        }
    }
}
