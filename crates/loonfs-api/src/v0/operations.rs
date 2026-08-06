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
    /// Idempotency key of the commit the error concerns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_id: Option<CommitId>,
    /// Sequence at which that commit id already landed. Present when the
    /// failure was decided against a durable commit receipt, which is what
    /// holds the sequence; absent when nothing has committed under the id
    /// yet and two live requests are simply claiming it at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_seq: Option<ChangeSeq>,
    /// Semantic identity of the mutation that already landed under that
    /// commit id, from the same receipt as `committed_seq` and present
    /// exactly when it is. A retry recomputes this value from the request it
    /// just made — see
    /// [`put_retry_fingerprint`](crate::put_retry_fingerprint) — and equality
    /// is what proves the two are the same request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_fingerprint: Option<String>,
    /// Position, in the request's operation list, of the operation that
    /// failed. A commit applies all of its operations or none of them, so
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
    /// Head sequence a namespace delete required the namespace to still be
    /// at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_head_seq: Option<ChangeSeq>,
    /// Head sequence the namespace was actually at, which is what a caller
    /// that still means to delete it retries against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_head_seq: Option<ChangeSeq>,
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
    /// delete and by the change feed) and re-bind it. Answers
    /// `not_deleted` when that generation is not the live one, so a stale
    /// request never cancels a later delete.
    #[cfg_attr(feature = "openapi", schema(title = "FsOpUndelete"))]
    Undelete {
        /// Deleted inode to make reachable again.
        inode_id: InodeId,
        /// Observed deletion sequence, which prevents cancelling a newer tombstone generation.
        deleted_at_seq: ChangeSeq,
        /// Absolute destination path whose parent must be visible and whose
        /// name must be absent. Absent means restore in place: re-bind
        /// under the parent and name the deletion recorded, anchored on
        /// the parent's identity rather than any remembered spelling, so
        /// the entry lands correctly even when ancestors were renamed
        /// since. A deletion that recorded no binding needs the explicit
        /// path.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        path: Option<AbsolutePath>,
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

/// One commit: an idempotency key, an optional annotation, and an ordered
/// list of path operations that commit together (API spec, section 5.1).
///
/// A one-operation request is the one-element case of this shape, not a
/// different request: a convenience call and a batch produce the same commit
/// and the same fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CommitRequest {
    /// Caller-supplied idempotency key for the whole request.
    pub commit_id: CommitId,
    /// Caller annotation recorded on the commit and reported by the change
    /// feed. Part of the commit's identity: reusing `commit_id` with a
    /// different message is a `commit_id_reuse_conflict`, exactly as it is
    /// for an explicit commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Proofs for any new external content refs introduced by this request.
    /// One proof covers every operation that names its content ref.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_tokens: Vec<ValidatedContentToken>,
    /// Ordered operations to apply. Must be non-empty; they commit all
    /// together or not at all.
    pub operations: Vec<FilesystemOperation>,
}

impl CommitRequest {
    /// A request carrying exactly one operation.
    pub fn single(
        commit_id: CommitId,
        message: Option<String>,
        operation: FilesystemOperation,
    ) -> Self {
        Self {
            commit_id,
            message,
            content_tokens: Vec::new(),
            operations: vec![operation],
        }
    }
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

/// Who a checkpoint record answers to, as the record durably records it.
///
/// The two owners have different releases, so a listing that names the
/// owner also says which records the release endpoint will act on: a user
/// pin is released by id, and a fork lease is released by deleting the
/// target namespace it protects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointOwnerSummary {
    /// An operator-created pin, released by id or by its own expiry.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerUser"))]
    User {
        /// The label the creator recorded. Not a key: several records may
        /// carry one label over different bases.
        name: String,
    },
    /// A fork target keeping its source basis alive for the length of one
    /// fork attempt.
    #[cfg_attr(feature = "openapi", schema(title = "CheckpointOwnerFork"))]
    Fork {
        /// Namespace whose continued existence keeps this pin standing.
        target_namespace_id: NamespaceId,
    },
}

/// One active checkpoint record, reported from what the record carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CheckpointSummary {
    /// Durable checkpoint id, as the creation response returned it. This is
    /// what the release endpoint takes.
    pub checkpoint_id: CheckpointId,
    /// Who the record answers to, and the label a user pin carries.
    pub owner: CheckpointOwnerSummary,
    /// When the record was written, in Unix milliseconds.
    pub created_at_ms: u64,
    /// When garbage collection may release the record without being asked,
    /// in Unix milliseconds. Absent means the pin holds until it is
    /// released. An instant already in the past is a record whose expiry
    /// has passed and which no collection pass has reached yet: it is still
    /// a root, so it is still listed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    /// Sequence the pinned basis covers — the same number the creation
    /// response reported as `checkpoint_seq`.
    pub checkpoint_seq: ChangeSeq,
    /// Manifest the record pins.
    pub manifest_id: ManifestId,
}

/// Every active checkpoint record a namespace currently carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ListCheckpointsResponse {
    /// Namespace the records belong to.
    pub namespace_id: NamespaceId,
    /// Active records, oldest first. Released records are absent because a
    /// release is what stops a record pinning anything; a released record
    /// that garbage collection has not yet deleted is not reported either.
    pub checkpoints: Vec<CheckpointSummary>,
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
    /// Maximum objects this invocation may read or decide. Omit to retain
    /// the run-to-completion behavior.
    ///
    /// A completed upload session past its reclamation grace makes the pass
    /// read every live manifest and retained WAL segment to find out
    /// whether anything still references its content, and that read is
    /// charged here like any other. A budget too small to finish it does
    /// not stall the pass: the session is retained, the response sets
    /// `content_reclamation_deferred`, and the sweep carries on through
    /// everything else. What a chronically small budget costs is content
    /// left unreclaimed, not progress. Give a pass at least as many objects
    /// as the namespace has live manifests and retained segments for that
    /// content to come back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<u64>,
    /// Opaque resume token returned as `next_cursor` by an earlier pass
    /// against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Why a pass kept what it kept: `retained_candidates` split by the
/// decision that spared each candidate.
///
/// The reasons are a closed set — one per place the sweep decides against
/// deleting — so every field is always reported, and a zero is the answer
/// that nothing was kept for that reason. The counts sum to
/// `retained_candidates`.
///
/// Retention is a decision per candidate examined, not per object in the
/// namespace: one object examined by two passes is counted by each.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RetainedCandidates {
    /// Selected as unreachable, then found reachable by the re-verification
    /// that runs immediately before every deletion. A candidate the pass
    /// already knew was reachable is never examined at all, so this counts
    /// the namespace moving underneath the pass rather than the size of its
    /// live set.
    pub referenced: u64,
    /// Unreachable, but younger than the grace window by the object's own
    /// provider timestamp. A later pass deletes it.
    pub grace_window: u64,
    /// Unreachable, and the provider reported no last-modified time at all,
    /// so the object's age is unknown and it is treated as young.
    pub no_provider_timestamp: u64,
    /// Root resolution failed somewhere in this pass, so manifest and table
    /// deletion was suppressed wholesale (`degraded_retention` is set too).
    pub degraded_roots: u64,
    /// A key under a swept family that this collector does not recognize as
    /// one of its own. Never deleted, whatever its age.
    pub unrecognized_key: u64,
    /// A checkpoint record this pass could have advanced but could not
    /// prove ready: a lost compare-and-swap, an unreadable record, a fork
    /// target not provably gone, a released record still inside its grace
    /// window, or an active pin that is simply doing its job. The pins
    /// themselves are listed by
    /// `GET /v0/admin/namespaces/{ns}/checkpoints`.
    pub checkpoint_not_releasable: u64,
    /// An upload session waiting out a window a clock resolves: an open
    /// session's lease plus the grace, an aborted session's grace, or a
    /// completed session's derived content-reclamation grace.
    /// `next_reclamation_at_ms` reports the soonest of these.
    pub upload_session_window: u64,
    /// An upload session held over for a reason no clock resolves: a lost
    /// compare-and-swap, a record that vanished mid-pass, or a reference
    /// set this pass could not establish. Only a later pass answers it.
    pub upload_session_undecided: u64,
    /// A completed session whose content reclamation was skipped because
    /// the reference scan did not fit in `max_objects`
    /// (`content_reclamation_deferred` is set too).
    pub content_scan_deferred: u64,
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
    /// Content objects reclaimed because their upload session completed,
    /// aged past the derived reclamation grace, and nothing the namespace
    /// can reach references them. The upload half's cleanup of abandoned
    /// sessions is not counted here: it deletes unconditionally, whether or
    /// not the session ever wrote anything.
    #[serde(default)]
    pub deleted_content_objects: u64,
    /// Active checkpoint records released because their basis manifest is
    /// verifiably gone.
    #[serde(default)]
    pub released_missing_basis_checkpoints: u64,
    /// Candidates retained at delete time (grace window, missing
    /// timestamps, or reachable from the fresh root set).
    pub retained_candidates: u64,
    /// The same total, split by the decision that spared each candidate.
    /// The total above stays because it is what every existing consumer
    /// reads; this says why.
    #[serde(default)]
    pub retained: RetainedCandidates,
    /// True when ambiguous roots suppressed manifest/table deletion.
    pub degraded_retention: bool,
    /// True when the pass skipped completed-content reclamation because
    /// what it needs — the namespace's live roots, then the reference
    /// collection over them — did not fit in `max_objects`. Nothing was
    /// ever decided from a partial collection; a later pass with room for
    /// the whole scan reclaims what this one left behind. A pass that had
    /// room for the roots swept every other candidate normally around the
    /// skip, and one that did not also reports `budget_exhausted`.
    #[serde(default)]
    pub content_reclamation_deferred: bool,
    /// True when the pass stopped because `max_objects` ran out before it
    /// finished. Whatever it did before that is reported here and stands;
    /// rerun with the returned cursor, or with a larger budget, to
    /// continue. A budget too small for the namespace's own roots stops a
    /// pass before it decides anything at all, which is what this says and
    /// an empty report on its own does not.
    #[serde(default)]
    pub budget_exhausted: bool,
    /// Opaque resume token when more candidates remain. Resuming rebuilds
    /// every safety proof; the token carries enumeration position only and
    /// is valid only against the same namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// The soonest instant still ahead of this pass at which something it
    /// retained becomes reclaimable: an open session's lease plus the grace
    /// window, an aborted session's grace, or a completed session's derived
    /// content-reclamation grace. A scheduler reads this to decide when to
    /// come back, so a namespace needs no other side channel to have its
    /// reclamation happen.
    ///
    /// It reports what this pass saw and nothing more. A pass that stopped
    /// on `next_cursor` examined only part of the keyspace, and candidates
    /// that age out under a plain grace window on their object timestamps
    /// carry no deadline here at all, so `None` is never a claim that
    /// nothing is owed.
    ///
    /// Always serialized, `null` included, unlike `next_cursor` beside it:
    /// a cursor's presence is what says the enumeration is unfinished,
    /// while this is an answer every pass has — and one whose absence
    /// otherwise makes the response's shape depend on what happened to be
    /// in the namespace.
    #[serde(default)]
    pub next_reclamation_at_ms: Option<u64>,
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
            deleted_content_objects: 0,
            released_missing_basis_checkpoints: 0,
            retained_candidates: 0,
            retained: RetainedCandidates::default(),
            degraded_retention: false,
            content_reclamation_deferred: false,
            budget_exhausted: false,
            next_cursor: None,
            next_reclamation_at_ms: None,
        }
    }

    /// Records one retained candidate under the reason that spared it.
    ///
    /// The total and the breakdown move together here so they cannot drift:
    /// every sweep site names a reason, and no site can count a retention
    /// without naming one.
    pub fn retain(&mut self, reason: RetainedReason) {
        self.retained_candidates += 1;
        *reason.counter(&mut self.retained) += 1;
    }
}

/// The reason one candidate was retained, as the sweep site knows it. Each
/// variant is the field of [`RetainedCandidates`] it counts into, where the
/// reason itself is described.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RetainedReason {
    /// Counts into [`RetainedCandidates::referenced`].
    Referenced,
    /// Counts into [`RetainedCandidates::grace_window`].
    GraceWindow,
    /// Counts into [`RetainedCandidates::no_provider_timestamp`].
    NoProviderTimestamp,
    /// Counts into [`RetainedCandidates::degraded_roots`].
    DegradedRoots,
    /// Counts into [`RetainedCandidates::unrecognized_key`].
    UnrecognizedKey,
    /// Counts into [`RetainedCandidates::checkpoint_not_releasable`].
    CheckpointNotReleasable,
    /// Counts into [`RetainedCandidates::upload_session_window`].
    UploadSessionWindow,
    /// Counts into [`RetainedCandidates::upload_session_undecided`].
    UploadSessionUndecided,
    /// Counts into [`RetainedCandidates::content_scan_deferred`].
    ContentScanDeferred,
}

impl RetainedReason {
    fn counter(self, retained: &mut RetainedCandidates) -> &mut u64 {
        match self {
            Self::Referenced => &mut retained.referenced,
            Self::GraceWindow => &mut retained.grace_window,
            Self::NoProviderTimestamp => &mut retained.no_provider_timestamp,
            Self::DegradedRoots => &mut retained.degraded_roots,
            Self::UnrecognizedKey => &mut retained.unrecognized_key,
            Self::CheckpointNotReleasable => &mut retained.checkpoint_not_releasable,
            Self::UploadSessionWindow => &mut retained.upload_session_window,
            Self::UploadSessionUndecided => &mut retained.upload_session_undecided,
            Self::ContentScanDeferred => &mut retained.content_scan_deferred,
        }
    }
}

impl RetainedCandidates {
    /// Every reason and its count, in a fixed order, for callers that
    /// report the breakdown rather than read one field of it.
    pub fn by_reason(&self) -> [(&'static str, u64); 9] {
        [
            ("referenced", self.referenced),
            ("grace_window", self.grace_window),
            ("no_provider_timestamp", self.no_provider_timestamp),
            ("degraded_roots", self.degraded_roots),
            ("unrecognized_key", self.unrecognized_key),
            ("checkpoint_not_releasable", self.checkpoint_not_releasable),
            ("upload_session_window", self.upload_session_window),
            ("upload_session_undecided", self.upload_session_undecided),
            ("content_scan_deferred", self.content_scan_deferred),
        ]
    }

    /// Folds another pass's breakdown into this one.
    pub fn add(&mut self, other: &Self) {
        self.referenced += other.referenced;
        self.grace_window += other.grace_window;
        self.no_provider_timestamp += other.no_provider_timestamp;
        self.degraded_roots += other.degraded_roots;
        self.unrecognized_key += other.unrecognized_key;
        self.checkpoint_not_releasable += other.checkpoint_not_releasable;
        self.upload_session_window += other.upload_session_window;
        self.upload_session_undecided += other.upload_session_undecided;
        self.content_scan_deferred += other.content_scan_deferred;
    }

    /// The reason with the highest count, and that count. `None` when
    /// nothing was retained. Ties go to the first in [`Self::by_reason`]
    /// order, so one pass's report is stable.
    pub fn top_reason(&self) -> Option<(&'static str, u64)> {
        self.by_reason()
            .into_iter()
            .filter(|(_, count)| *count > 0)
            // `max_by_key` keeps the last of equal maxima, so the reversal
            // is what makes a tie report the earlier reason.
            .rev()
            .max_by_key(|(_, count)| *count)
    }
}

/// Result of advancing the retention floor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct AdvanceRetentionResponse {
    /// New minimum sequence for incremental replay.
    pub retention_floor_seq: ChangeSeq,
}

/// One explicit maintenance step: the actions it selects, and nothing more.
///
/// Selection is presence. Each field names one independent action, and a
/// step runs exactly the ones the body carries — a request that selects
/// nothing is rejected rather than quietly doing nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceStepRequest {
    /// Fold the visible WAL tail into metadata tables and merge one bounded
    /// reorganization unit. The two travel together: folding a tail is what
    /// creates the delta runs a reorganization merges.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataMaintenanceRequest>,
    /// Advance the retention floor to the flushed manifest head. Nothing
    /// surrenders replay history unless this is true.
    #[serde(default)]
    pub advance_retention: bool,
    /// Run one bounded mark-and-sweep garbage-collection pass. Nothing
    /// sweeps unless this is present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcRequest>,
}

/// Overrides for the metadata-upkeep action.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceRequest {
    /// Flush the visible WAL tail once it reaches this many segments.
    /// Absent uses the server's default threshold; zero, and any value above
    /// the write-rejection threshold, are rejected as `invalid_request`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wal_tail_segments: Option<u64>,
}

/// What the WAL-flush part of a maintenance step did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WalFlushStepOutcome {
    /// The tail was below the threshold, so there was nothing to fold.
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
    /// No family group had enough delta runs to merge.
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
///
/// One report per action the request selected, and none for an action it
/// did not: an absent field means "not selected", never "ran and found
/// nothing to do". The latter is what the outcomes inside a report say.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MaintenanceStepResponse {
    /// Namespace the step ran against.
    pub namespace_id: NamespaceId,
    /// Namespace status observed before the step acted.
    pub status_before: NamespaceStatusResponse,
    /// What the metadata-upkeep action did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataMaintenanceResponse>,
    /// Where the retention floor ended up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<AdvanceRetentionResponse>,
    /// What the collection pass reclaimed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcResponse>,
}

/// What one metadata-upkeep action did, part by part.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct MetadataMaintenanceResponse {
    /// What the WAL fold did.
    pub wal_flush: WalFlushStepOutcome,
    /// What the reorganization unit did.
    pub reorganize: ReorganizeStepOutcome,
}

/// Options for one store contract probe. Empty today; a body is still sent
/// so later options do not change the shape of the request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeRequest {}

/// What one store contract probe observed, check by check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeResponse {
    /// Label the server minted for this run. It scopes the objects the run
    /// wrote, so it identifies the run in provider logs too.
    pub run_id: String,
    /// Every check the run performed, in the order it performed them. A
    /// failed check lives here rather than in an error: the probe answered
    /// the question, and the answer is that the store is wrong.
    pub checks: Vec<StoreProbeCheckResult>,
}

/// One named contract check and what the store did with it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct StoreProbeCheckResult {
    /// Stable check name.
    pub name: String,
    /// What the store did.
    pub outcome: StoreProbeCheckOutcome,
    /// What was expected and what happened instead. Present only on
    /// `failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// What one contract check concluded about the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum StoreProbeCheckOutcome {
    /// The store behaved as the contract requires.
    Passed,
    /// The store declares it cannot do this at all. Only the optional
    /// capabilities answer this way, and it is an answer rather than a
    /// fault.
    Unsupported,
    /// The store did something the contract forbids, or the operation
    /// failed outright.
    Failed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentId;

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
                "kind": "blob_v1",
                "content_id": "con_0123456789abcdef0123456789abcdef",
                "size_bytes": 1,
                "storage_checksum": {
                    "algorithm": "sha256",
                    "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
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
        let content_ref = ContentRef::blob_v1(ContentId::generate(), b"hello");
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
                    path: Some(path("/docs/restored")),
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
                "content_ref": ContentRef::blob_v1(ContentId::generate(), b"hello")
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
