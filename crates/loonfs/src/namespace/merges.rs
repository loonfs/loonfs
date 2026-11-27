//! Merging a fork into a writable [`Namespace`]: the receipt lookup, the
//! merge's reservation, the checks, the snapshot of the fork, the plan, and
//! the commit. Also the next-range query, which reads where a fork's next
//! merge starts.

use crate::pinned_read::PinnedRead;
use crate::publish::CommitCandidate;
use crate::publisher::MergeReservation;
use crate::runtime_core::HeadConfirmation;
use crate::trace::{self, operation_span};
use crate::{
    ActorId, Commit, CommitId, Consistency, CoreError, Error, MergeConflict, MergeForkOptions,
    Mode, Namespace, NamespaceId, NextMergeRangeResponse, Result, Writable,
};
use futures::future::BoxFuture;
use loonfs_core::limits::{MAX_CONTENTION_ATTEMPTS, OPERATION_SNAPSHOT_TTL_MS};
use loonfs_core::{MergeAllowance, MergeRequest};
use loonfs_types::format::wal::MergeRecord;
use std::sync::{Mutex, MutexGuard};

/// The plans that one merge makes before it returns `store_contention`,
/// which are the first and three more after this namespace changed between
/// a plan and its commit.
const MAX_MERGE_PLANS: usize = 4;

/// A merge request, or a rebase's replay commit request, at one view of the
/// namespace that it commits into.
pub(crate) enum PlannedMerge<R = ()> {
    /// The namespace committed a merge under the request's commit ID, with
    /// the conflict report that its merge record stores.
    Committed {
        commit: Commit,
        conflicts: Vec<MergeConflict>,
    },
    /// The commit that makes the merge.
    Candidate(Box<PlannedCommit<R>>),
}

/// The commit that makes a merge, the reservation that the merge keeps, the
/// merge's record, whose range a later plan of the request applies, and what
/// the plan reports with the commit (nothing for a merge, which refuses a
/// conflict, and the conflict report for a replay commit).
pub(crate) struct PlannedCommit<R = ()> {
    pub(crate) candidate: CommitCandidate,
    pub(crate) reservation: MergeReservation,
    pub(crate) record: MergeRecord,
    pub(crate) report: R,
}

/// What one call of [`Namespace::merge`] or [`Namespace::replay`] did: the
/// request, with the range that its first plan resolved, and the result.
#[derive(Debug)]
pub struct MergeReport<T> {
    /// The caller's request, with `after_seq` and `through_seq` set to the
    /// range of the first plan, or the caller's request as it was when the
    /// call failed before a plan resolved a range.
    pub request: MergeRequest,
    /// The merge's commit, or the replay commit's response, or the error.
    pub result: Result<T>,
}

/// How the commit of a merge, or of a replay commit, ended.
pub(crate) enum MergeOutcome<R = ()> {
    /// The namespace had committed a merge under the request's commit ID
    /// before this request committed one. `conflicts` is the conflict report
    /// that the merge record stores, which is empty for a merge.
    Found {
        commit: Commit,
        conflicts: Vec<MergeConflict>,
    },
    /// This request committed the merge, from a plan that reported `report`.
    Committed { commit: Commit, report: R },
}

impl<M: Mode> Namespace<M> {
    /// Returns where the fork `source_namespace_id`'s next merge into this
    /// namespace starts, without planning the merge
    /// ([API section 6.15](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#615-get-merges)).
    ///
    /// The response has the fork point, the fork's head, the fork's newest
    /// merge into this namespace, and the fork sequence that the next merge
    /// starts after, as [`Namespace::merge_fork`] finds it. When this
    /// namespace is a rebase's new fork for `source_namespace_id`, the start
    /// comes from the new fork's replay commits or its `resume_after_seq`, as
    /// it does for a rebase. When a retention floor would refuse the merge
    /// with `below_retention_floor`, the response's outcome says which floor
    /// instead of returning the error. A handle with a subject needs an
    /// administrator in both namespaces, as the change feed does. A source
    /// that doesn't exist, was deleted, or isn't a fork of this namespace
    /// returns `invalid_request`. Both heads are read at this handle's
    /// consistency, and the read runs again from this namespace's newest
    /// manifest when a newer manifest replaced the one that it read, as an
    /// ordinary read does.
    pub async fn next_merge_range(
        &self,
        source_namespace_id: &NamespaceId,
    ) -> Result<NextMergeRangeResponse> {
        trace::in_operation(
            operation_span!(self.core, "next_merge_range", self.namespace_id),
            async {
                let _permit = self.core.read_permit().await;
                self.read_head(|main| async move {
                    let main_administrator =
                        main.engine.verify_administrator(&main.context).await?;
                    let fork = self
                        .core
                        .pinned_read(source_namespace_id, self.consistency)
                        .await?;
                    let fork_administrator =
                        fork.engine.verify_administrator(&fork.context).await?;
                    Ok(main
                        .engine
                        .next_merge_range(
                            &main_administrator,
                            &main.context,
                            &fork.engine,
                            &fork_administrator,
                            &fork.context,
                        )
                        .await?)
                })
                .await
            },
        )
        .await
    }
}

impl Namespace<Writable> {
    /// Merges the fork `source_namespace_id`'s commits into this namespace,
    /// through the fork's head, as one commit.
    ///
    /// See [`Self::merge_fork_with_options`].
    pub async fn merge_fork(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<Commit> {
        self.merge_fork_with_options(source_namespace_id, actor, &MergeForkOptions::default())
            .await
    }

    /// Merges the fork `source_namespace_id`'s commits into this namespace as
    /// one commit, through `options.through_seq` or the fork's head
    /// ([API section 6.14](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#614-post-merges)).
    ///
    /// The merge applies the fork's net change since the fork's previous merge,
    /// or since the fork point, and refuses a change that conflicts with this
    /// namespace with `merge_conflict`. It reads the fork's current manifest
    /// and WAL tail, and writes the tail's pieces that the merged files need,
    /// so it folds the fork only when the fork's tail has a commit older than
    /// [`MAX_TAIL_COMMIT_AGE_MS`](loonfs_core::limits::MAX_TAIL_COMMIT_AGE_MS).
    /// A retry with the commit ID of a merge that committed returns that merge,
    /// and folds neither namespace. When the commit fails because this
    /// namespace changed after the plan, the merge plans again, up to three
    /// times, and then returns the commit's error. A later plan reports a
    /// changed row that isn't the fork's own as `merge_conflict`. The merge's
    /// WAL object counts toward this namespace's fold triggers like any commit.
    /// The subject of this handle has to be able to read each inode whose
    /// change the merge applies in the fork, and needs the rights in this
    /// namespace that each planned operation needs. A merge whose
    /// reservation of the merge allowance
    /// ([`ExecutionBudgetBuilder::max_merge_bytes`](crate::ExecutionBudgetBuilder::max_merge_bytes))
    /// doesn't fit beside other merges' reservations returns `server_busy`.
    pub async fn merge_fork_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        self.merge(MergeRequest {
            source_namespace_id: source_namespace_id.clone(),
            through_seq: options.through_seq,
            after_seq: None,
            commit_id: options.commit_id.clone().unwrap_or_else(CommitId::generate),
            actor_id: actor.clone(),
            message: options.message.clone(),
        })
        .await
        .result
    }

    /// Merges the fork `request.source_namespace_id`'s commits into this
    /// namespace as one commit, as [`Self::merge_fork_with_options`] does,
    /// through `request.through_seq` or the fork's head, and after
    /// `request.after_seq` or where the fork's next merge starts.
    ///
    /// A host that sends a fenced merge again in a new session uses this
    /// form, because the report's request has the range that the first plan
    /// resolved, and sending that request plans the same merge, as a plan
    /// again after a stale plan does. A range that no longer starts where
    /// the fork's next merge starts returns `merge_range_conflict`.
    pub async fn merge(&self, mut request: MergeRequest) -> MergeReport<Commit> {
        let result = trace::in_operation(
            operation_span!(self.core, "merge_fork", self.namespace_id),
            async {
                let planned = self.plan_merge(&request, None).await?;
                let (MergeOutcome::Found { commit, .. } | MergeOutcome::Committed { commit, .. }) =
                    self.commit_merge(&mut request, planned, |request, reservation| {
                        Box::pin(async move { self.plan_merge(&request, Some(reservation)).await })
                    })
                    .await?;
                Ok(commit)
            },
        )
        .await;
        MergeReport { request, result }
    }

    /// Returns the merge that this namespace committed under the request's
    /// commit ID, or the commit that makes the merge, planned against this
    /// namespace's head under `reservation`, or under a reservation of the
    /// merge allowance that the plan takes (see [`Self::plan_commit`]).
    /// The plan checks the fork and the floors and finds the range's start,
    /// takes a snapshot of the fork, and plans. It checks the merge's size,
    /// and then its conflicts. The checks read the fork's view as
    /// [`HeadConfirmation::View`] does, and the plan reads the view that the
    /// snapshot was taken on (see [`Self::snapshot_fork`]).
    pub(crate) async fn plan_merge(
        &self,
        request: &MergeRequest,
        reservation: Option<MergeReservation>,
    ) -> Result<PlannedMerge> {
        let source_namespace_id = &request.source_namespace_id;
        // Boxed so the merge's future stays within the compiler's layout depth.
        self.plan_commit(request, reservation, |main, allowance| {
            Box::pin(async move {
                // The plan runs inside this namespace's read loop, which runs
                // the plan again after a replaced manifest of either
                // namespace, so the fork is read here without a loop of its
                // own.
                let fork = self
                    .core
                    .pinned_read(source_namespace_id, HeadConfirmation::View)
                    .await?;
                main.engine
                    .check_merge(&main.context, &fork.engine, &fork.context, request)
                    .await?;
                let prepared_at_ms = self.core.now_ms();
                let fork = self
                    .snapshot_fork(source_namespace_id, prepared_at_ms)
                    .await?;
                let plan = main
                    .engine
                    .plan_merge(
                        &main.context,
                        &fork.engine,
                        &fork.context,
                        request,
                        prepared_at_ms,
                        allowance,
                    )
                    .await?;
                let record = plan.record().clone();
                Ok::<_, Error>((plan.into_candidate()?, record, ()))
            })
        })
        .await
    }

    /// Returns the merge or replay commit that this namespace committed under
    /// the request's commit ID, or the commit, its record, and its report
    /// that `plan` plans from this namespace's head under `reservation`, or
    /// under a reservation of the merge allowance that this call takes.
    /// `plan` plans against the merge allowance with the reservation, and
    /// grows the reservation as it plans.
    ///
    /// The receipt lookup comes first, so a retry of a commit that committed
    /// reserves nothing and writes nothing, even after the fork was deleted.
    /// The receipt lookup and the plan read one view of this namespace, as a
    /// read does: a view that a newer manifest replaced, whose segments a
    /// collection removed, is read once more from the current manifest, and
    /// the reservation that the first view took stays with the second. The
    /// read is `strong`, because the receipt lookup's result decides whether
    /// the plan touches the fork at all. Another runtime may commit the merge
    /// and then delete the fork. A retry of that merge through this runtime
    /// has to find the receipt and not return `namespace_deleted`.
    pub(super) async fn plan_commit<R, Plan>(
        &self,
        request: &MergeRequest,
        reservation: Option<MergeReservation>,
        plan: impl Fn(PinnedRead, MergeAllowance) -> Plan,
    ) -> Result<PlannedMerge<R>>
    where
        Plan: std::future::Future<Output = Result<(CommitCandidate, MergeRecord, R)>>,
    {
        let held = Mutex::new(reservation);
        self.core
            .read(&self.namespace_id, Consistency::Strong, |target| async {
                if let Some((commit, conflicts)) =
                    target.engine.find_merge(&target.context, request).await?
                {
                    return Ok(PlannedMerge::Committed { commit, conflicts });
                }
                let reservation = match lock_reservation(&held).take() {
                    Some(reservation) => reservation,
                    None => self.session().reserve_merge()?,
                };
                match plan(target, reservation.allowance()).await {
                    Ok((candidate, record, report)) => {
                        Ok(PlannedMerge::Candidate(Box::new(PlannedCommit {
                            candidate,
                            reservation,
                            record,
                            report,
                        })))
                    }
                    Err(error) => {
                        *lock_reservation(&held) = Some(reservation);
                        Err(error)
                    }
                }
            })
            .await
    }

    /// Commits the merge, or the replay commit, that `planned` found or
    /// planned, and calls `replan` when the commit fails because this
    /// namespace changed after the plan (see
    /// [`CoreError::is_stale_plan`]), up to
    /// [`MAX_MERGE_PLANS`] plans in all. A later plan applies the first
    /// plan's range, so it never merges a longer range, and a merge of the
    /// fork that committed in between returns `merge_range_conflict`. The
    /// call sets that range in `request`, so a caller that sends the request
    /// again in a new session plans the same range.
    ///
    /// When the last plan's commit fails the same way, the call returns
    /// `store_contention`, with the cause `lost_race`. The failure is a
    /// precondition that the merge set for itself, not one that the caller
    /// stated, so the caller's remedy is to send the same request again.
    pub(crate) async fn commit_merge<'a, R>(
        &'a self,
        request: &mut MergeRequest,
        mut planned: PlannedMerge<R>,
        mut replan: impl FnMut(MergeRequest, MergeReservation) -> BoxFuture<'a, Result<PlannedMerge<R>>>,
    ) -> Result<MergeOutcome<R>> {
        let mut plans = 1;
        loop {
            let PlannedCommit {
                candidate,
                reservation,
                record,
                report,
            } = match planned {
                PlannedMerge::Committed { commit, conflicts } => {
                    return Ok(MergeOutcome::Found { commit, conflicts })
                }
                PlannedMerge::Candidate(planned) => *planned,
            };
            request.after_seq = Some(record.source_after_seq);
            request.through_seq = Some(record.source_through_seq);
            match self.submit_merge(candidate, &reservation).await {
                Err(Error::Core(error)) if error.is_stale_plan() => {
                    if plans == MAX_MERGE_PLANS {
                        return Err(Error::Core(CoreError::StoreContention {
                            object_key: format!("{} merge commit", self.namespace_id),
                        }));
                    }
                    plans += 1;
                    planned = replan(request.clone(), reservation).await?;
                }
                result => return result.map(|commit| MergeOutcome::Committed { commit, report }),
            }
        }
    }

    /// Submits a merge's commit under the reservation that the merge keeps,
    /// as this handle's subject.
    pub(crate) async fn submit_merge(
        &self,
        candidate: CommitCandidate,
        reservation: &MergeReservation,
    ) -> Result<Commit> {
        let candidate = match self.core.subject() {
            Some(subject) => candidate.with_subject(subject.clone()),
            None => candidate,
        };
        self.session().submit_merge(candidate, reservation).await
    }

    /// Reads the fork's head and takes a snapshot of the fork's current
    /// manifest without a fold, for a merge or a replay commit prepared at
    /// `prepared_at_ms`, and returns the read ([Cross-namespace copies and
    /// moves](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#97-cross-namespace-copies-and-moves)).
    ///
    /// The read is the fork's current manifest and WAL tail, as
    /// [`HeadConfirmation::View`] confirms them, and it's read once more from
    /// the current manifest when a newer manifest replaced it, as any read is.
    /// Because the read includes the tail, the merge's range ends at the fork's
    /// head, and the plan writes the tail's pieces that the merge needs. The
    /// snapshot keeps the manifest, its segments, and the content its layouts
    /// refer to while the merge plans and commits, because a fold, a retention
    /// advance, and a compaction of the fork in between can replace the
    /// manifest, and the collector keeps only what the current manifest and the
    /// snapshots' manifests refer to. The snapshot expires with the plan's
    /// content proofs, and garbage collection of the fork deletes its record
    /// the grace window after it expires. When a newer manifest replaced the
    /// one that the read found, the call drops this runtime's view of the
    /// fork, so the next read loads the fork's head from the store, and takes
    /// the snapshot again, up to `MAX_CONTENTION_ATTEMPTS` snapshots in all.
    ///
    /// When the fork's tail has a commit older than
    /// [`MAX_TAIL_COMMIT_AGE_MS`](loonfs_core::limits::MAX_TAIL_COMMIT_AGE_MS),
    /// the call folds the fork instead (see
    /// [`Self::snapshot_after_fork_fold`]).
    pub(super) async fn snapshot_fork(
        &self,
        fork_id: &NamespaceId,
        prepared_at_ms: u64,
    ) -> Result<PinnedRead> {
        let expires_at_ms = Some(prepared_at_ms + OPERATION_SNAPSHOT_TTL_MS);
        let engine = self
            .core
            .writer_engine(&self.mode.writer.writer_id, fork_id);
        let mut attempts = 1;
        loop {
            let (fork, folds) = self
                .core
                .read(fork_id, HeadConfirmation::View, |fork| async move {
                    let folds = fork
                        .engine
                        .tail_exceeds_max_commit_age(&fork.context, prepared_at_ms)
                        .await?;
                    Ok((fork, folds))
                })
                .await?;
            if folds {
                return self.snapshot_after_fork_fold(fork_id, expires_at_ms).await;
            }
            match engine
                .create_verified_snapshot_of_view(expires_at_ms, &fork.context)
                .await
            {
                Err(CoreError::HeadMoved(_)) if attempts < MAX_CONTENTION_ATTEMPTS => {
                    attempts += 1;
                    self.core.forget_namespace_view(fork_id).await;
                }
                created => {
                    created?;
                    return Ok(fork);
                }
            }
        }
    }

    /// Folds the fork, takes a snapshot of the manifest that covers its tail,
    /// and reads the fork through the snapshot, so the merge's range ends at
    /// the fork's folded head.
    ///
    /// The fold is the namespace's one fold path, so when the fork's writer
    /// session in this runtime is already folding, the fold waits for that
    /// fold instead of folding the same tail. The read of the fork starts
    /// from the snapshot record in memory, and the snapshot's verification
    /// after the record write is the check of the fork's position that the
    /// read's live head needs (see
    /// [`RuntimeCore::pinned_read_at_created_snapshot`](crate::runtime_core::RuntimeCore::pinned_read_at_created_snapshot)).
    async fn snapshot_after_fork_fold(
        &self,
        fork_id: &NamespaceId,
        expires_at_ms: Option<u64>,
    ) -> Result<PinnedRead> {
        let (created, verified) = self
            .mode
            .publisher
            .snapshot_after_fold(fork_id, |manifest| {
                let engine = self
                    .core
                    .writer_engine(&self.mode.writer.writer_id, fork_id);
                async move {
                    engine
                        .create_verified_snapshot(None, expires_at_ms, manifest)
                        .await
                }
            })
            .await?;
        self.core
            .pinned_read_at_created_snapshot(fork_id, &created, &verified)
            .await
    }

    /// Returns a maintenance value that folds as this runtime's writer.
    #[cfg(test)]
    pub(super) fn maintenance(&self) -> crate::Maintenance {
        crate::Maintenance::new(
            self.core.clone(),
            self.mode.publisher.clone(),
            std::sync::Arc::clone(&self.mode.writer),
            self.mode.writer.writer_id.clone(),
        )
    }
}

/// Locks the reservation that a plan keeps while its view is read once more.
fn lock_reservation(
    held: &Mutex<Option<MergeReservation>>,
) -> MutexGuard<'_, Option<MergeReservation>> {
    // A critical section under this lock only moves the reservation, so it
    // can't panic and poison the lock.
    held.lock()
        .expect("the merge reservation lock should not be poisoned")
}

#[cfg(test)]
mod tests;
