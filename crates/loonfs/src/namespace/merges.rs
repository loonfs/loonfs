//! Merging a fork into a writable [`Namespace`]: the receipt lookup, the
//! merge slot, the checks, the fork's fold, the plan, the commit, and the
//! fold of the namespace.

use crate::pinned_read::PinnedRead;
use crate::publish::CommitCandidate;
use crate::publisher::MergeSlot;
use crate::trace::{self, operation_span};
use crate::{
    ActorId, Commit, CommitId, Consistency, CoreError, Error, ErrorCode, Maintenance,
    MergeForkOptions, Namespace, NamespaceId, Result, Writable,
};
use futures::future::BoxFuture;
use loonfs_core::MergeRequest;
use loonfs_types::format::wal::MergeRecord;
use std::sync::Arc;

/// The plans that one merge makes before it returns its commit's error,
/// which are the first and three more after this namespace changed between
/// a plan and its commit.
const MAX_MERGE_PLANS: usize = 4;

/// A merge request at one view of the namespace that it merges into.
pub(crate) enum PlannedMerge {
    /// The namespace committed a merge under the request's commit ID.
    Committed(Commit),
    /// The commit that makes the merge.
    Candidate(Box<PlannedCommit>),
}

/// The commit that makes a merge, the merge slot that the merge keeps, and
/// the merge's record, whose range a later plan of the request applies.
pub(crate) struct PlannedCommit {
    pub(crate) candidate: CommitCandidate,
    pub(crate) slot: MergeSlot,
    pub(crate) record: MergeRecord,
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
    /// merge. When the commit fails because this namespace changed after
    /// the plan, the merge plans again, up to three times, and then returns
    /// the commit's error. A later plan reports a changed row that isn't the
    /// fork's own as `merge_conflict`. After the
    /// commit, the merge folds this namespace's WAL tail through it, unless
    /// the merge has already run for the runtime's
    /// [`merge_fold_budget_ms`](crate::PublicationPolicy::merge_fold_budget_ms),
    /// in which case the next maintenance pass folds the tail. The subject of
    /// this handle has to be able to read each inode whose change the merge
    /// applies in the fork, and needs the rights in this namespace that each
    /// planned operation needs. A runtime whose merge allowance
    /// ([`ExecutionBudgetBuilder::max_merge_bytes`](crate::ExecutionBudgetBuilder::max_merge_bytes))
    /// is zero has merges turned off, and a merge returns `merge_too_large`
    /// with `max_bytes` 0 before it takes the merge slot or folds the fork.
    pub async fn merge_fork_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<Commit> {
        trace::in_operation(
            operation_span!(self.core, "merge_fork", self.namespace_id),
            async {
                let started_at_ms = self.core.timer().monotonic_now_ms();
                let request = MergeRequest {
                    source_namespace_id: source_namespace_id.clone(),
                    through_seq: options.through_seq,
                    after_seq: None,
                    commit_id: options.commit_id.clone().unwrap_or_else(CommitId::generate),
                    actor_id: actor.clone(),
                    message: options.message.clone(),
                };
                let planned = self.plan_merge(&request, None).await?;
                let commit = self
                    .commit_merge(&request, planned, |request, slot| {
                        Box::pin(async move { self.plan_merge(&request, Some(slot)).await })
                    })
                    .await?;
                self.fold_after_merge(started_at_ms).await;
                Ok(commit)
            },
        )
        .await
    }

    /// Returns the merge that this namespace committed under the request's
    /// commit ID, or the commit that makes the merge, planned against this
    /// namespace's head under `slot`, or under a merge slot that the plan
    /// takes.
    ///
    /// The receipt lookup comes first, so a retry of a merge that committed
    /// takes no slot and folds nothing, even after the fork was deleted. The
    /// plan then takes the slot, checks the fork and the floors and finds
    /// the range's start, folds the fork, and plans, with the receipt lookup
    /// and the plan on one view of this namespace. The plan checks the
    /// merge's size, and then its conflicts.
    pub(crate) async fn plan_merge(
        &self,
        request: &MergeRequest,
        slot: Option<MergeSlot>,
    ) -> Result<PlannedMerge> {
        let source_namespace_id = &request.source_namespace_id;
        let main = self
            .core
            .pinned_read(&self.namespace_id, Consistency::Strong)
            .await?;
        if let Some(commit) = main.engine.find_merge(&main.context, request).await? {
            return Ok(PlannedMerge::Committed(commit));
        }
        let slot = match slot {
            Some(slot) => slot,
            None => self.session().acquire_merge_slot()?,
        };
        let plan = async {
            let fork = self
                .core
                .pinned_read(source_namespace_id, Consistency::Strong)
                .await?;
            main.engine
                .check_merge(&main.context, &fork.engine, &fork.context, request)
                .await?;
            let fork = self.fold_and_read(source_namespace_id).await?;
            Ok(main
                .engine
                .plan_merge(
                    &main.context,
                    &fork.engine,
                    &fork.context,
                    request,
                    self.core.now_ms(),
                    self.session().merge_allowance(),
                )
                .await?)
        }
        .await
        .map_err(|error| missing_fork(source_namespace_id, error))?;
        self.session().validate_merge(plan.candidate())?;
        let record = plan.record().clone();
        Ok(PlannedMerge::Candidate(Box::new(PlannedCommit {
            candidate: plan.into_candidate()?,
            slot,
            record,
        })))
    }

    /// Commits the merge that `planned` found or planned, and calls
    /// `replan` when the commit fails because this namespace changed after
    /// the plan (see [`is_stale_plan`]), up to [`MAX_MERGE_PLANS`] plans in
    /// all. A later plan applies the first
    /// plan's range, so it never merges a longer range, and a merge of the
    /// fork that committed in between returns `merge_range_conflict`.
    pub(crate) async fn commit_merge<'a>(
        &'a self,
        request: &MergeRequest,
        mut planned: PlannedMerge,
        mut replan: impl FnMut(MergeRequest, MergeSlot) -> BoxFuture<'a, Result<PlannedMerge>>,
    ) -> Result<Commit> {
        let mut plans = 1;
        loop {
            let PlannedCommit {
                candidate,
                slot,
                record,
            } = match planned {
                PlannedMerge::Committed(commit) => return Ok(commit),
                PlannedMerge::Candidate(planned) => *planned,
            };
            match self.submit_merge(candidate, &slot).await {
                Err(error) if is_stale_plan(error.code()) && plans < MAX_MERGE_PLANS => {
                    plans += 1;
                    let request = MergeRequest {
                        after_seq: Some(record.source_after_seq),
                        through_seq: Some(record.source_through_seq),
                        ..request.clone()
                    };
                    planned = replan(request, slot).await?;
                }
                result => return result,
            }
        }
    }

    /// Submits a merge's commit under the merge slot that the merge keeps, as this
    /// handle's subject.
    pub(crate) async fn submit_merge(
        &self,
        candidate: CommitCandidate,
        slot: &MergeSlot,
    ) -> Result<Commit> {
        let candidate = match self.core.subject() {
            Some(subject) => candidate.with_subject(subject.clone()),
            None => candidate,
        };
        self.session().submit_merge(candidate, slot).await
    }

    /// Folds this namespace's WAL tail through the merge's commit, within
    /// what is left of the merge's fold budget. A failed fold doesn't fail
    /// the merge, which is committed, and the next maintenance pass folds
    /// the tail.
    async fn fold_after_merge(&self, started_at_ms: u64) {
        let elapsed_ms = self
            .core
            .timer()
            .monotonic_now_ms()
            .saturating_sub(started_at_ms);
        let remaining_ms = self
            .core
            .config()
            .publication
            .merge_fold_budget_ms
            .saturating_sub(elapsed_ms);
        if remaining_ms == 0 {
            return;
        }
        let maintenance = self.maintenance();
        // When the budget runs out first, the dropped fold leaves the segments
        // that it wrote unlisted by any manifest, and garbage collection
        // removes them, as it does the output of an abandoned compaction
        // unit.
        tokio::select! {
            folded = maintenance.fold_wal(&self.namespace_id) => {
                if let Err(error) = folded {
                    tracing::warn!(
                        namespace_id = %self.namespace_id,
                        %error,
                        "the fold after a merge failed, and the next maintenance pass folds the tail"
                    );
                }
            }
            () = crate::publisher::sleep_ms(remaining_ms) => {}
        }
    }

    /// Folds the WAL tail of `namespace_id`, and returns a strong read of
    /// it at the fold's manifest or a newer one.
    ///
    /// A strong read confirms a namespace's head, but the manifest that the
    /// read starts from is the cached head's, which a runtime checks for a
    /// successor only once per
    /// [`manifest_revalidation_interval_ms`](crate::RuntimeConfig::manifest_revalidation_interval_ms)
    /// when it has no writer session for the namespace, such as a fork
    /// whose handle the caller dropped, or one that another process writes.
    /// A read right after the fold could then start from the manifest before
    /// the fold, or from one older than a fold that another process
    /// published, and a merge, whose range ends at the fork's folded head,
    /// would quietly merge a shorter range. So the fold drops the
    /// namespace's cached head before the read, whether or not it published.
    pub(crate) async fn fold_and_read(&self, namespace_id: &NamespaceId) -> Result<PinnedRead> {
        self.maintenance().fold_wal(namespace_id).await?;
        self.core.invalidate_namespace_read_cache(namespace_id);
        self.core
            .pinned_read(namespace_id, Consistency::Strong)
            .await
    }

    /// Returns a maintenance value that folds as this runtime's writer.
    fn maintenance(&self) -> Maintenance {
        Maintenance::new(
            self.core.clone(),
            self.mode.publisher.clone(),
            Arc::clone(&self.mode.writer),
            self.mode.writer.writer_id.clone(),
        )
    }
}

/// Returns whether `code` is an error that a merge's commit returns when
/// this namespace changed after the plan, so that a later plan reads the
/// change. Those errors are:
///
/// - `stale_head`, `stale_revision`, `stale_attributes`, `stale_access`,
///   and `binding_version_mismatch`, from the commit's preconditions.
/// - `path_conflict`, when another inode took a name that a create, move,
///   or undelete needs.
/// - `inode_not_found`, when a deletion hid an inode that the merge
///   changes, or the directory that an inode goes into.
/// - `not_deleted`, when another writer undeleted an inode that the merge
///   undeletes.
///
/// A later plan reports a changed row that isn't the fork's own as
/// `merge_conflict`, such as a `name` conflict for a taken name. In a
/// namespace with ACLs, the plan checks the subject's rights as the commit
/// does, so the commit's `inode_not_found` isn't a subject's missing rights.
fn is_stale_plan(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::StaleHead
            | ErrorCode::StaleRevision
            | ErrorCode::StaleAttributes
            | ErrorCode::StaleAccess
            | ErrorCode::BindingVersionMismatch
            | ErrorCode::PathConflict
            | ErrorCode::InodeNotFound
            | ErrorCode::NotDeleted
    )
}

/// Returns `invalid_request` for a fork that doesn't exist or was deleted,
/// as for a namespace that isn't a fork, so the error identifies the
/// request's field and not a namespace that the caller's session writes.
/// Every other error, including one about the namespace that the merge
/// commits into, stays as it is.
fn missing_fork(source_namespace_id: &NamespaceId, error: Error) -> Error {
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
        param: "/source_namespace_id",
        message,
    }
    .into()
}

#[cfg(test)]
mod tests;
