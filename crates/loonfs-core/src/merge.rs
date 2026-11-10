//! Merging a fork into a namespace. The merge is one commit that applies the
//! fork's net change over a range of its commits, planned against the
//! namespace's head under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)).

mod classify;
mod net_change;
mod next_range;
mod operations;
mod order;
mod plan;
mod replay;
mod size;
mod state;
mod targets;

use crate::authorize::Authorizer;
use crate::commit::candidates::commit_response_from_commit_receipt;
use crate::commit::engine::CommitCandidate;
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, HistoryPosition, MergeBound, Result};
use crate::metadata::MetadataView;
use crate::namespace::state::NamespaceReadState;
use crate::path::read::LoadedMetadataView;
use crate::path::write::{merge_fingerprint, CommitIntent, PublishPlanningSession};
use crate::storage::tail_content::ContentWrites;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{ForkBasis, RebasedFrom};
use loonfs_types::format::manifest::MergeIndexRecord;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{
    ActorId, ChangeSeq, Commit, CommitId, CommitPrecondition, FilesystemOperation, MergeConflict,
    NamespaceId,
};
use std::fmt;
use std::sync::Arc;

pub use replay::ReplayPlan;
pub use size::check_planned_bytes;

/// A request to merge a fork's commits into a namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeRequest {
    /// The fork whose commits the merge applies.
    pub source_namespace_id: NamespaceId,
    /// The fork sequence to merge through, or `None` for the fork's folded
    /// head.
    pub through_seq: Option<ChangeSeq>,
    /// The fork sequence that the range has to start after, or `None` to
    /// take the start from the merge index. A caller that plans a merge again
    /// sets the first plan's range, so that the merge never applies a longer
    /// range than it first planned, and another merge of the fork in between
    /// returns `merge_range_conflict`.
    pub after_seq: Option<ChangeSeq>,
    /// The merge's idempotency key.
    pub commit_id: CommitId,
    /// The actor responsible for the merge.
    pub actor_id: ActorId,
    /// An optional annotation, which is part of the merge's identity.
    pub message: Option<String>,
}

/// The publisher's merge allowance, which limits the estimated peak memory of
/// the merges that plan and publish at once, with one merge's reservation of
/// the allowance.
///
/// The estimate is `peak_per_planned_byte` times the merge's planned WAL
/// bytes. The planner checks the allowance as it plans, so it stops planning
/// a merge that admission would refuse, and the check grows the merge's
/// reservation to the estimate. Admission checks the merge's commit.
#[derive(Clone)]
pub struct MergeAllowance {
    /// The allowance, in bytes.
    pub max_bytes: usize,
    /// The multiple of a merge's planned WAL bytes that estimates the peak.
    pub peak_per_planned_byte: usize,
    /// The merge's reservation of the allowance.
    pub reservation: Arc<dyn MergeReservation>,
}

/// One merge's reservation of the merge allowance, which grows while the
/// merge plans and doesn't shrink while the merge is admitted.
pub trait MergeReservation: Send + Sync {
    /// Grows the reservation to at least `estimated_bytes`, and returns
    /// whether the allowance had room. A refused growth leaves the
    /// reservation as it was.
    fn grow(&self, estimated_bytes: usize) -> bool;
}

/// The reservation of a merge that shares the allowance with no other merge,
/// so every growth within the allowance fits.
struct UnsharedReservation;

impl MergeReservation for UnsharedReservation {
    fn grow(&self, _estimated_bytes: usize) -> bool {
        true
    }
}

impl fmt::Debug for MergeAllowance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MergeAllowance")
            .field("max_bytes", &self.max_bytes)
            .field("peak_per_planned_byte", &self.peak_per_planned_byte)
            .finish_non_exhaustive()
    }
}

impl MergeAllowance {
    /// Returns an allowance of `max_bytes` that one merge shares with no
    /// other merge, for a caller without a publisher.
    pub fn unshared(max_bytes: usize, peak_per_planned_byte: usize) -> Self {
        Self {
            max_bytes,
            peak_per_planned_byte,
            reservation: Arc::new(UnsharedReservation),
        }
    }

    /// Returns `merge_too_large` when the estimated peak of a merge whose
    /// planned WAL bytes are `planned_bytes` exceeds the allowance, and
    /// otherwise grows the merge's reservation to the estimate. When other
    /// merges' reservations leave no room for the growth, the check returns
    /// `server_busy`. An allowance of zero turns merges off, so it refuses
    /// every merge.
    pub fn check(&self, planned_bytes: usize) -> Result<()> {
        let estimated_bytes = planned_bytes.saturating_mul(self.peak_per_planned_byte);
        if self.max_bytes == 0 || estimated_bytes > self.max_bytes {
            return Err(CoreError::MergeTooLarge {
                bound: MergeBound::MergeAllowance,
                estimated_bytes,
                max_bytes: self.max_bytes,
                split_at_seq: None,
            });
        }
        if !self.reservation.grow(estimated_bytes) {
            return Err(CoreError::ServerBusy);
        }
        Ok(())
    }

    /// Returns this allowance with an unshared reservation, for a measure
    /// of a range that the merge doesn't publish.
    fn unshared_copy(&self) -> Self {
        Self::unshared(self.max_bytes, self.peak_per_planned_byte)
    }
}

/// A merge planned against the head of the namespace that it merges into.
#[derive(Debug)]
pub struct MergePlan {
    candidate: CommitCandidate,
    record: MergeRecord,
    /// The number of conflicting inodes, and the one with the smallest ID.
    conflict: Option<(u64, MergeConflict)>,
}

impl MergePlan {
    /// Returns the merge's record, with the range of the fork's commits that
    /// the merge applies.
    pub fn record(&self) -> &MergeRecord {
        &self.record
    }

    /// Returns the merge's commit, or `merge_conflict` when the fork's change
    /// conflicts with the namespace being merged into.
    pub fn into_candidate(self) -> Result<CommitCandidate> {
        match self.conflict {
            None => Ok(self.candidate),
            Some((conflict_count, conflict)) => Err(CoreError::MergeConflict {
                conflict_count,
                conflict: Box::new(conflict),
            }),
        }
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Returns the merge that this namespace committed under the request's
    /// commit ID, or `None` when it has no commit with that ID.
    ///
    /// A merge's range comes from the merge index, so a retry after a merge
    /// committed would plan the next range under the same commit ID. Looking
    /// up the receipt first returns the original commit instead. A commit
    /// under the ID that isn't the same merge, or that applied another range
    /// than the request's `through_seq`, returns `commit_id_reuse_conflict`.
    /// The caller plans the merge at the same `context`, so the receipt and
    /// the merge index come from one view.
    pub async fn find_merge(
        &self,
        context: &RuntimeReadContext,
        request: &MergeRequest,
    ) -> Result<Option<Commit>> {
        let view = self.namespace.load_metadata_view(context).await?;
        let metadata = view.metadata_view();
        let Some(receipt) = metadata.find_commit_receipt(&request.commit_id).await? else {
            return Ok(None);
        };
        // The request's identity takes its range from the merge that the
        // receipt names, because the merge index has moved past that range.
        let committed = metadata
            .commit_at_seq(receipt.committed_seq)
            .await?
            .and_then(|commit| commit.merge);
        let record = MergeRecord {
            source_namespace_id: request.source_namespace_id.clone(),
            source_after_seq: request
                .after_seq
                .or(committed.as_ref().map(|merge| merge.source_after_seq))
                .unwrap_or(ChangeSeq(0)),
            source_through_seq: request
                .through_seq
                .or(committed.as_ref().map(|merge| merge.source_through_seq))
                .unwrap_or(ChangeSeq(0)),
        };
        let namespace_id = &self.namespace.namespace_id;
        let identity = merge_fingerprint(
            namespace_id,
            &self.merge_intent(request, Vec::new(), Vec::new()),
            &record,
        );
        commit_response_from_commit_receipt(metadata, namespace_id, &receipt, &identity)
            .await
            .map(Some)
    }

    /// Plans a merge of `fork`, read at `fork_context`, into this namespace,
    /// read at `context`
    /// ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md)).
    ///
    /// The plan checks the fork point against this namespace's retention
    /// floor, takes the range's start from this namespace's merge index and
    /// checks it against the fork's own floor, computes the fork's net change
    /// over the range, and classifies it against this namespace's head. It
    /// fails with `merge_too_large` when the commit wouldn't fit one WAL
    /// object or `allowance`, or with `server_busy` when other merges'
    /// reservations of `allowance` leave no room for the commit, before it
    /// reports a conflict, and records the
    /// conflicts for [`MergePlan::into_candidate`]. The subject of `fork` has to be able to
    /// read each inode whose change the merge applies. The proofs of the
    /// merge's content expire one direct transfer lifetime after
    /// `prepared_at_ms`.
    ///
    /// The plan reads the content layouts that the fork's fold wrote, so the
    /// caller folds the fork first, and the range ends at the fork's folded
    /// head unless the request sets an earlier `through_seq`. When the
    /// merge deletes or moves a directory, its commit expects this
    /// namespace's head to be the one that the plan read, because another
    /// writer can add an item below the directory without changing a row
    /// that the commit's other preconditions cover. The commit also expects
    /// the head when the merge moves an inode through a temporary name,
    /// because the move off the temporary name finds the inode and its new
    /// directory by path, and another writer can rename a directory on
    /// either path without changing a row that those preconditions cover.
    /// A file's delete expects the file's revision, because another writer
    /// can change the file's content without changing its binding version.
    ///
    /// In a namespace with ACLs, the plan also plans the merge's commit as
    /// the writer does, against the head that the plan read, and returns the
    /// writer's error. The writer checks the subject's rights on each
    /// operation, and it returns `inode_not_found` for an inode on which the
    /// subject has no rights, the code that it returns for an inode that a
    /// deletion hid. So a subject's missing rights fail the plan, and the
    /// commit's `inode_not_found` means that this namespace changed after
    /// the plan.
    pub async fn plan_merge<F>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
        request: &MergeRequest,
        prepared_at_ms: u64,
        allowance: MergeAllowance,
    ) -> Result<MergePlan> {
        let main = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        let range = range_start(&main, &fork_view, request, &main.head().namespace_id).await?;
        let planned = self
            .plan_merge_commit(
                &main,
                None,
                fork,
                &fork_view,
                range,
                request,
                prepared_at_ms,
                &allowance,
            )
            .await?;
        if planned.first_conflict.is_none() {
            self.check_merge_rights(&main, &planned.candidate, prepared_at_ms)
                .await?;
        }
        Ok(MergePlan {
            candidate: planned.candidate,
            record: planned.record,
            conflict: planned.first_conflict,
        })
    }

    /// Plans `candidate` as the writer does, against `main`, in a namespace
    /// with ACLs, and returns the writer's error. The writer checks the
    /// subject's rights on each operation against the state that the
    /// earlier operations leave, so the plan runs the writer's planning
    /// instead of a copy of its checks. A namespace without ACLs checks no
    /// rights, so its plan skips this.
    async fn check_merge_rights(
        &self,
        main: &LoadedMetadataView<'_, S>,
        candidate: &CommitCandidate,
        prepared_at_ms: u64,
    ) -> Result<()> {
        let head = main.head();
        let authorizer =
            Authorizer::for_request(&head.namespace_id, &head.access, candidate.authority())?;
        if matches!(authorizer, Authorizer::Unrestricted) {
            return Ok(());
        }
        PublishPlanningSession::new(
            head,
            &main.wal_tail,
            main.metadata_view(),
            ContentWrites::new(&self.namespace.store, &self.namespace.content_writes, head),
            prepared_at_ms,
        )
        .prepare_commit(
            candidate,
            authorizer,
            candidate.semantic_identity(&head.namespace_id),
        )
        .await
        .map(drop)
    }

    /// Checks that `fork`, read at `fork_context`, is a fork of this
    /// namespace, read at `context`, and that the merge's range starts at or
    /// above both namespaces' retention floors, as [`Self::plan_merge`]
    /// does. A caller runs the checks before it folds the fork, so a request
    /// that would fail them folds nothing.
    pub async fn check_merge<F>(
        &self,
        context: &RuntimeReadContext,
        fork: &NamespaceEngine<S, F>,
        fork_context: &RuntimeReadContext,
        request: &MergeRequest,
    ) -> Result<()> {
        let main = self.namespace.load_metadata_view(context).await?;
        let fork_view = fork.namespace.load_metadata_view(fork_context).await?;
        range_start(&main, &fork_view, request, &main.head().namespace_id)
            .await
            .map(|_| ())
    }

    /// Returns the commit request of a merge with `operations` and
    /// `preconditions`, as this engine's subject.
    fn merge_intent(
        &self,
        request: &MergeRequest,
        operations: Vec<FilesystemOperation>,
        preconditions: Vec<CommitPrecondition>,
    ) -> CommitIntent {
        CommitIntent {
            commit_id: request.commit_id.clone(),
            actor_id: request.actor_id.clone(),
            subject: self.subject.clone(),
            message: request.message.clone(),
            operations,
            preconditions,
        }
    }
}

/// A retention floor that refuses a merge: the position of the merge's
/// history that is below the floor, and the floor.
pub(crate) struct FloorRefusal {
    below: HistoryPosition,
    retention_floor_seq: ChangeSeq,
}

impl From<FloorRefusal> for CoreError {
    fn from(refusal: FloorRefusal) -> Self {
        CoreError::RebootstrapRequired {
            below: refusal.below,
            retention_floor_seq: refusal.retention_floor_seq,
        }
    }
}

/// Where the next merge from a fork into a namespace starts, under the range
/// rule.
pub(crate) struct MergeStart {
    /// The fork sequence that the next merge starts after.
    pub(crate) after_seq: ChangeSeq,
    /// The fork's newest merge that the range rule reads, or `None` before
    /// the fork's first merge, or before a rebase's first replay commit.
    pub(crate) previous: Option<MergeIndexRecord>,
}

/// Returns where the next merge from `source_namespace_id`, a fork made at
/// `fork_point_seq`, into the namespace that `head` and `view` describe
/// starts, under the range rule and the floor rule
/// ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#merges)).
///
/// The merge starts after the fork's newest merge in the merge index, or
/// after the fork point when the fork has none, and the fork point has to be
/// at or above the namespace's retention floor, or the floor refuses the
/// merge. When the namespace's fork
/// basis records a `rebased_from` that identifies the fork, the namespace is
/// a rebase's new fork, and the rules read its own fork point in place of the
/// fork's: only the merge index rows above it count, the first replay commit
/// starts after `resume_after_seq`, and the namespace's own fork point has to
/// be at or above its floor.
pub(crate) async fn merge_start<S: ObjectStore + ?Sized>(
    view: &MetadataView<'_, '_, S>,
    head: &NamespaceReadState,
    source_namespace_id: &NamespaceId,
    fork_point_seq: ChangeSeq,
) -> Result<std::result::Result<MergeStart, FloorRefusal>> {
    let rebase = rebase_basis(head, source_namespace_id);
    let checked_fork_point_seq =
        rebase.map_or(fork_point_seq, |(basis, _)| basis.manifest.head_seq);
    let retention_floor_seq = view.retention_floor_seq();
    if checked_fork_point_seq < retention_floor_seq {
        return Ok(Err(FloorRefusal {
            below: HistoryPosition::ForkPoint {
                fork_point_seq: checked_fork_point_seq,
            },
            retention_floor_seq,
        }));
    }
    let newest = view.newest_merge(source_namespace_id).await?;
    let (previous, first_after_seq) = match rebase {
        // The new fork inherited the source's merge index rows at or below
        // its own fork point, and only its own replay rows count.
        Some((basis, rebased_from)) => (
            newest.filter(|previous| previous.committed_seq > basis.manifest.head_seq),
            rebased_from.resume_after_seq,
        ),
        None => (newest, fork_point_seq),
    };
    Ok(Ok(MergeStart {
        after_seq: previous
            .as_ref()
            .map_or(first_after_seq, |previous| previous.source_through_seq),
        previous,
    }))
}

/// Returns the fork basis of the namespace whose head is `head`, with the
/// basis's `rebased_from`, when the namespace is a rebase's new fork for
/// `fork`.
fn rebase_basis<'a>(
    head: &'a NamespaceReadState,
    fork: &NamespaceId,
) -> Option<(&'a ForkBasis, &'a RebasedFrom)> {
    let basis = head.fork_basis.as_ref()?;
    let rebased_from = basis
        .rebased_from
        .as_ref()
        .filter(|rebased_from| rebased_from.source_namespace_id == *fork)?;
    Some((basis, rebased_from))
}

/// Returns the fork point and the start of the range of a merge into `main`.
/// The fork point is the sequence of `source` that `fork_view`'s namespace
/// was made at, where `source` is `main` itself, or the namespace that a
/// rebase made `main` from. The range starts where [`merge_start`] says,
/// and that start has to be at or above the fork's own floor, and equal to
/// the request's `after_seq` when the request sets one.
async fn range_start<S: ObjectStore + ?Sized>(
    main: &LoadedMetadataView<'_, S>,
    fork_view: &LoadedMetadataView<'_, S>,
    request: &MergeRequest,
    source: &NamespaceId,
) -> Result<(ChangeSeq, ChangeSeq)> {
    let fork_head = fork_view.head();
    let fork_point_seq = fork_point(fork_head, source)?;
    let source_namespace_id = &fork_head.namespace_id;
    let source_after_seq = merge_start(
        &main.metadata_view(),
        main.head(),
        source_namespace_id,
        fork_point_seq,
    )
    .await??
    .after_seq;
    if let Some(planned_after_seq) = request.after_seq {
        if planned_after_seq != source_after_seq {
            return Err(CoreError::MergeRangeConflict {
                source_namespace_id: source_namespace_id.clone(),
                expected_after_seq: source_after_seq,
                actual_after_seq: planned_after_seq,
            });
        }
    }
    ensure_range_start_retained(fork_view, source_after_seq)?;
    Ok((fork_point_seq, source_after_seq))
}

/// Refuses a range that starts after `source_after_seq` below the fork's own
/// retention floor, in the fork that `fork_view` reads, because the fork's
/// net change over the range can't be computed from history that the fork
/// no longer keeps.
fn ensure_range_start_retained<S: ObjectStore + ?Sized>(
    fork_view: &LoadedMetadataView<'_, S>,
    source_after_seq: ChangeSeq,
) -> std::result::Result<(), FloorRefusal> {
    let fork_floor_seq = fork_view.retention_floor_seq();
    if source_after_seq < fork_floor_seq {
        return Err(FloorRefusal {
            below: HistoryPosition::RangeStart {
                source_namespace_id: fork_view.head().namespace_id.clone(),
                source_after_seq,
            },
            retention_floor_seq: fork_floor_seq,
        });
    }
    Ok(())
}

/// Returns the fork point of the fork whose head is `fork_head`, which is the
/// folded head of the fork's basis manifest, a sequence of `namespace_id`. A
/// namespace whose basis manifest belongs to another namespace isn't a fork
/// of `namespace_id`.
fn fork_point(fork_head: &NamespaceReadState, namespace_id: &NamespaceId) -> Result<ChangeSeq> {
    match &fork_head.fork_basis {
        Some(basis) if basis.manifest.owner_namespace_id == *namespace_id => {
            Ok(basis.manifest.head_seq)
        }
        _ => Err(CoreError::InvalidMerge {
            param: "source_namespace_id",
            message: format!(
                "namespace `{}` isn't a fork of `{namespace_id}`",
                fork_head.namespace_id
            ),
        }),
    }
}

/// Returns the fork sequence that a merge whose range starts after
/// `after_seq` applies the fork's commits through, which is `requested` or
/// the fork's folded head, `folded_seq`.
fn through_seq(
    fork: &NamespaceId,
    folded_seq: ChangeSeq,
    after_seq: ChangeSeq,
    requested: Option<ChangeSeq>,
) -> Result<ChangeSeq> {
    let through_seq = requested.unwrap_or(folded_seq);
    let refused = |message: String| CoreError::InvalidMerge {
        param: "through_seq",
        message,
    };
    if through_seq > folded_seq {
        return Err(refused(format!(
            "`{through_seq}` is ahead of fork `{fork}`'s head `{folded_seq}`"
        )));
    }
    if through_seq <= after_seq {
        return Err(refused(format!(
            "fork `{fork}` has no commits after `{after_seq}` through `{through_seq}` to merge"
        )));
    }
    Ok(through_seq)
}
