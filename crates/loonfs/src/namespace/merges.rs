//! Merging a fork into a writable [`Namespace`]: the receipt lookup, the
//! merge's reservation, the checks, the fork's fold, the plan, and the commit. Also
//! the next-range query, which reads where a fork's next merge starts.

use crate::pinned_read::PinnedRead;
use crate::publish::CommitCandidate;
use crate::publisher::MergeReservation;
use crate::trace::{self, operation_span};
use crate::{
    ActorId, Commit, CommitId, Consistency, CoreError, Error, MergeForkOptions, Mode, Namespace,
    NamespaceId, NextMergeRangeResponse, PinId, Result, Writable,
};
use futures::future::BoxFuture;
use loonfs_core::limits::DIRECT_TRANSFER_URL_TTL_MS;
use loonfs_core::{MergeAllowance, MergeRequest};
use loonfs_types::format::wal::MergeRecord;
use std::sync::{Mutex, MutexGuard};

/// The plans that one merge makes before it returns its commit's error,
/// which are the first and three more after this namespace changed between
/// a plan and its commit.
const MAX_MERGE_PLANS: usize = 4;

/// A merge request, or a rebase's replay commit request, at one view of the
/// namespace that it commits into.
pub(crate) enum PlannedMerge<R = ()> {
    /// The namespace committed a merge under the request's commit ID.
    Committed(Commit),
    /// The commit that makes the merge.
    Candidate(Box<PlannedCommit<R>>),
}

/// The commit that makes a merge, the reservation that the merge keeps, the
/// merge's record, whose range a later plan of the request applies, what
/// the plan reports with the commit (nothing for a merge, which refuses a
/// conflict, and the conflict report for a replay commit), and the pin on
/// the fork's manifest that the plan read, which the commit releases.
pub(crate) struct PlannedCommit<R = ()> {
    pub(crate) candidate: CommitCandidate,
    pub(crate) reservation: MergeReservation,
    pub(crate) record: MergeRecord,
    pub(crate) report: R,
    pub(crate) fork_pin: PinId,
}

/// How the commit of a merge, or of a replay commit, ended.
pub(crate) enum MergeOutcome<R = ()> {
    /// The namespace had committed a merge under the request's commit ID
    /// before this request committed one.
    Found(Commit),
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
    /// with `rebootstrap_required`, the response's outcome says which floor
    /// instead of returning the error. A handle with a subject needs an
    /// administrator in both namespaces, as the change feed does. A source
    /// that doesn't exist, was deleted, or isn't a fork of this namespace
    /// returns `invalid_request`. A runtime whose merge allowance is zero
    /// returns `not_supported` for `filesystem.merges` before it reads a
    /// namespace.
    pub async fn next_merge_range(
        &self,
        source_namespace_id: &NamespaceId,
    ) -> Result<NextMergeRangeResponse> {
        trace::in_operation(
            operation_span!(self.core, "next_merge_range", self.namespace_id),
            async {
                self.core.ensure_merges_enabled()?;
                let _permit = self.core.read_permit().await;
                let main = self
                    .core
                    .pinned_read(&self.namespace_id, Consistency::Strong)
                    .await?;
                main.engine.ensure_administrator(&main.context).await?;
                let fork = self
                    .core
                    .pinned_read(source_namespace_id, Consistency::Strong)
                    .await
                    .map_err(|error| missing_fork(source_namespace_id, error))?;
                fork.engine.ensure_administrator(&fork.context).await?;
                Ok(main
                    .engine
                    .next_merge_range(&main.context, &fork.engine, &fork.context)
                    .await?)
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
    /// The merge folds the fork, then applies the fork's net change since
    /// the fork's previous merge, or since the fork point, and refuses a
    /// change that conflicts with this namespace with `merge_conflict`. A
    /// retry with the commit ID of a merge that committed returns that
    /// merge, and folds neither namespace. When the commit fails because
    /// this namespace changed after the plan, the merge plans again, up to
    /// three times, and then returns the commit's error. A later plan reports a changed row that isn't the
    /// fork's own as `merge_conflict`. The merge's WAL object counts toward
    /// this namespace's fold triggers like any commit. The subject of
    /// this handle has to be able to read each inode whose change the merge
    /// applies in the fork, and needs the rights in this namespace that each
    /// planned operation needs. A runtime whose merge allowance
    /// ([`ExecutionBudgetBuilder::max_merge_bytes`](crate::ExecutionBudgetBuilder::max_merge_bytes))
    /// is zero has merges turned off, and a merge returns `not_supported` for
    /// `filesystem.merges` before it reads either namespace. A merge whose
    /// reservation of the allowance doesn't fit beside other merges'
    /// reservations returns `server_busy`.
    pub async fn merge_fork_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "merge_fork", self.namespace_id),
            async {
                self.core.ensure_merges_enabled()?;
                let request = MergeRequest {
                    source_namespace_id: source_namespace_id.clone(),
                    through_seq: options.through_seq,
                    after_seq: None,
                    commit_id: options.commit_id.clone().unwrap_or_else(CommitId::generate),
                    actor_id: actor.clone(),
                    message: options.message.clone(),
                };
                let planned = self.plan_merge(&request, None).await?;
                let (MergeOutcome::Found(commit) | MergeOutcome::Committed { commit, .. }) = self
                    .commit_merge(&request, planned, |request, reservation| {
                        Box::pin(async move { self.plan_merge(&request, Some(reservation)).await })
                    })
                    .await?;
                Ok(commit)
            },
        )
        .await
    }

    /// Returns the merge that this namespace committed under the request's
    /// commit ID, or the commit that makes the merge, planned against this
    /// namespace's head under `reservation`, or under a reservation that the
    /// plan takes (see [`Self::plan_commit`]). The plan checks the fork and the
    /// floors and finds the range's start, folds and pins the fork, and
    /// plans. It checks the merge's size, and then its conflicts.
    pub(crate) async fn plan_merge(
        &self,
        request: &MergeRequest,
        reservation: Option<MergeReservation>,
    ) -> Result<PlannedMerge> {
        let source_namespace_id = &request.source_namespace_id;
        // Boxed so the merge's future stays within the compiler's layout depth.
        self.plan_commit(request, reservation, |main, allowance| {
            Box::pin(async move {
                let main = &main;
                self.core
                    .read(
                        source_namespace_id,
                        Some(Consistency::Strong),
                        |fork| async move {
                            main.engine
                                .check_merge(&main.context, &fork.engine, &fork.context, request)
                                .await?;
                            Ok(())
                        },
                    )
                    .await?;
                let prepared_at_ms = self.core.now_ms();
                let (fork, fork_pin) = self
                    .pin_fork(source_namespace_id, request, prepared_at_ms)
                    .await?;
                let planned = async {
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
                }
                .await;
                self.planned_with_fork_pin(source_namespace_id, fork_pin, planned)
                    .await
            })
        })
        .await
    }

    /// Returns the merge or replay commit that this namespace committed under
    /// the request's commit ID, or the commit, its record, its report, and
    /// the pin on the fork that `plan` plans from this namespace's head under
    /// `reservation`, or under a reservation that this call takes. `plan`
    /// plans against the merge allowance with the reservation, and grows
    /// the reservation as it plans.
    ///
    /// The receipt lookup comes first, so a retry of a commit that committed
    /// reserves nothing and folds nothing, even after the fork was deleted.
    /// The receipt lookup and the plan read one view of this namespace, as a
    /// read does: a view that a newer manifest replaced, whose segments a
    /// collection removed, is read once more from the current manifest, and
    /// the reservation that the first view took stays with the second.
    pub(super) async fn plan_commit<R, Plan>(
        &self,
        request: &MergeRequest,
        reservation: Option<MergeReservation>,
        plan: impl Fn(PinnedRead, MergeAllowance) -> Plan,
    ) -> Result<PlannedMerge<R>>
    where
        Plan: std::future::Future<Output = Result<(CommitCandidate, MergeRecord, R, PinId)>>,
    {
        let held = Mutex::new(reservation);
        self.core
            .read(
                &self.namespace_id,
                Some(Consistency::Strong),
                |target| async {
                    if let Some(commit) = target.engine.find_merge(&target.context, request).await?
                    {
                        return Ok(PlannedMerge::Committed(commit));
                    }
                    let reservation = match lock_reservation(&held).take() {
                        Some(reservation) => reservation,
                        None => self.session().reserve_merge()?,
                    };
                    match plan(target, reservation.allowance()).await {
                        Ok((candidate, record, report, fork_pin)) => {
                            Ok(PlannedMerge::Candidate(Box::new(PlannedCommit {
                                candidate,
                                reservation,
                                record,
                                report,
                                fork_pin,
                            })))
                        }
                        Err(error) => {
                            *lock_reservation(&held) = Some(reservation);
                            Err(missing_fork(&request.source_namespace_id, error))
                        }
                    }
                },
            )
            .await
    }

    /// Commits the merge, or the replay commit, that `planned` found or
    /// planned, and calls `replan` when the commit fails because this
    /// namespace changed after the plan (see
    /// [`ErrorCode::is_stale_plan`](crate::ErrorCode::is_stale_plan)), up to
    /// [`MAX_MERGE_PLANS`] plans in all. A later plan applies the first
    /// plan's range, so it never merges a longer range, and a merge of the
    /// fork that committed in between returns `merge_range_conflict`. Each
    /// plan's pin on the fork is released once its commit returns.
    pub(crate) async fn commit_merge<'a, R>(
        &'a self,
        request: &MergeRequest,
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
                fork_pin,
            } = match planned {
                PlannedMerge::Committed(commit) => return Ok(MergeOutcome::Found(commit)),
                PlannedMerge::Candidate(planned) => *planned,
            };
            let committed = self.submit_merge(candidate, &reservation).await;
            self.release_fork_pin(&request.source_namespace_id, &fork_pin)
                .await;
            match committed {
                Err(error) if error.code().is_stale_plan() && plans < MAX_MERGE_PLANS => {
                    plans += 1;
                    let request = MergeRequest {
                        after_seq: Some(record.source_after_seq),
                        through_seq: Some(record.source_through_seq),
                        ..request.clone()
                    };
                    planned = replan(request, reservation).await?;
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

    /// Folds the fork, pins the manifest that covers its tail, and reads the
    /// fork through the pin, so the merge's range ends at the fork's folded
    /// head. The pin keeps that manifest, its segments, and the content its
    /// layouts refer to while the merge plans and commits, as a snapshot
    /// does: a fold, a retention advance, and a compaction of the fork in
    /// between can replace the manifest, and the collector keeps only what
    /// the current manifest and the pinned manifests refer to. The pin
    /// expires with the plan's content proofs, so a merge that can't release
    /// it leaves it to the fork's collector.
    ///
    /// The fold is the namespace's one fold path, so when the fork's writer
    /// session in this runtime is already folding, the fold waits for that
    /// fold instead of folding the same tail.
    pub(super) async fn pin_fork(
        &self,
        fork_id: &NamespaceId,
        request: &MergeRequest,
        prepared_at_ms: u64,
    ) -> Result<(PinnedRead, PinId)> {
        let name = format!("merge {}", request.commit_id);
        let expires_at_ms = Some(prepared_at_ms + DIRECT_TRANSFER_URL_TTL_MS);
        let snapshot = self
            .mode
            .publisher
            .pin_after_fold(fork_id, |manifest| {
                let engine = self
                    .core
                    .writer_engine(&self.mode.writer.writer_id, fork_id);
                let name = name.clone();
                async move { engine.create_snapshot(name, expires_at_ms, manifest).await }
            })
            .await?;
        let fork = self
            .core
            .pinned_read_at_snapshot(fork_id, &snapshot.snapshot_id)
            .await?;
        Ok((fork, snapshot.snapshot_id))
    }

    /// Returns `planned` with the pin its plan read the fork through, or
    /// releases the pin and returns the plan's error.
    pub(super) async fn planned_with_fork_pin<R>(
        &self,
        fork_id: &NamespaceId,
        fork_pin: PinId,
        planned: Result<(CommitCandidate, MergeRecord, R)>,
    ) -> Result<(CommitCandidate, MergeRecord, R, PinId)> {
        match planned {
            Ok((candidate, record, report)) => Ok((candidate, record, report, fork_pin)),
            Err(error) => {
                self.release_fork_pin(fork_id, &fork_pin).await;
                Err(error)
            }
        }
    }

    /// Deletes the pin that a merge's plan took on the fork. A pin that the
    /// delete leaves behind expires, and the fork's collector deletes it.
    async fn release_fork_pin(&self, fork_id: &NamespaceId, fork_pin: &PinId) {
        let engine = self
            .core
            .writer_engine(&self.mode.writer.writer_id, fork_id);
        if let Err(error) = engine.delete_snapshot(fork_pin).await {
            tracing::warn!(
                namespace_id = %fork_id,
                snapshot_id = %fork_pin,
                error = %error,
                "could not release a merge's pin on the fork"
            );
        }
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

/// Returns `invalid_request` for a fork that doesn't exist or was deleted,
/// as for a namespace that isn't a fork, so the error identifies the
/// request's field and not a namespace that the caller's session writes.
/// Every other error, including one about the namespace that the merge
/// commits into, stays as it is.
pub(super) fn missing_fork(source_namespace_id: &NamespaceId, error: Error) -> Error {
    let message = match &error {
        Error::Core(CoreError::NamespaceNotFound { namespace_id })
            if namespace_id == source_namespace_id =>
        {
            format!("namespace `{source_namespace_id}` doesn't exist")
        }
        Error::Core(CoreError::NamespaceDeleted { namespace_id })
            if namespace_id == source_namespace_id =>
        {
            format!("namespace `{source_namespace_id}` was deleted")
        }
        _ => return error,
    };
    CoreError::InvalidMerge {
        param: "source_namespace_id",
        message,
    }
    .into()
}

#[cfg(test)]
mod tests;
