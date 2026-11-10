//! Rebasing a fork: the new fork that a rebase creates or continues into,
//! and the replay commit that replays the fork's unmerged commits into it
//! ([design note](https://github.com/loonfs/loonfs/blob/main/docs/design/merge.md#rebasing)).

use super::merges::{missing_fork, MergeOutcome, PlannedMerge};
use crate::publisher::MergeSlot;
use crate::trace::{self, operation_span};
use crate::{
    ActorId, CommitId, Consistency, CoreError, CreationId, Error, LoonFs, MergeConflict,
    MergeForkOptions, Namespace, NamespaceId, NamespaceMetadata, RebaseForkResponse, Result,
    Writable,
};
use loonfs_core::MergeRequest;

impl LoonFs<Writable> {
    /// Creates `new_namespace_id` as a rebase's new fork of
    /// `namespace_id`'s head for the fork `source_namespace_id`, or returns
    /// it when it already is one
    /// ([API section 6.16](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#616-post-rebases)).
    ///
    /// Before it creates anything, the call checks that
    /// `source_namespace_id` is a fork of `namespace_id`, that the fork
    /// point is at or above `namespace_id`'s retention floor, and that the
    /// start of the fork's unmerged range is at or above the fork's own
    /// retention floor. Creating the new fork needs the right to fork
    /// `namespace_id`, as [`Self::fork_namespace`] does. A namespace that
    /// already exists and isn't a rebase's new fork of `namespace_id` for
    /// `source_namespace_id` returns `invalid_request`. A runtime whose merge
    /// allowance is zero returns `not_supported` for `filesystem.merges`
    /// before it reads or creates a namespace.
    pub async fn fork_namespace_for_rebase(
        &self,
        namespace_id: &NamespaceId,
        source_namespace_id: &NamespaceId,
        new_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<NamespaceMetadata> {
        trace::in_operation(
            operation_span!(self.core, "fork_namespace_for_rebase", namespace_id),
            async {
                self.core.ensure_merges_enabled()?;
                if let Some(existing) = self.find_rebase_target(new_namespace_id).await? {
                    return ensure_rebase(
                        existing,
                        namespace_id,
                        source_namespace_id,
                        new_namespace_id,
                    );
                }
                self.namespace(namespace_id).ensure_administrator().await?;
                let main = self
                    .core
                    .pinned_read(namespace_id, Consistency::Strong)
                    .await?;
                let fork_point_seq = async {
                    let fork = self
                        .core
                        .pinned_read(source_namespace_id, Consistency::Strong)
                        .await?;
                    Ok(main
                        .engine
                        .fork_point_for_rebase(&main.context, &fork.engine, &fork.context)
                        .await?)
                }
                .await
                .map_err(|error| missing_fork(source_namespace_id, error))?;
                // A rebase recognizes its own new fork by the fork basis, so
                // the new fork's creation ID only has to be unique.
                let creation_id = CreationId::generate();
                let creation_id = &creation_id;
                self.install_fork(
                    namespace_id,
                    new_namespace_id,
                    |engine, manifest| async move {
                        engine
                            .fork_namespace_for_rebase(
                                new_namespace_id,
                                actor,
                                creation_id,
                                source_namespace_id,
                                fork_point_seq,
                                manifest,
                            )
                            .await
                    },
                )
                .await
            },
        )
        .await
    }
}

/// Returns `existing` when it's a rebase's new fork of `namespace_id` for
/// `source_namespace_id`, and `invalid_request` otherwise.
fn ensure_rebase(
    existing: NamespaceMetadata,
    namespace_id: &NamespaceId,
    source_namespace_id: &NamespaceId,
    new_namespace_id: &NamespaceId,
) -> Result<NamespaceMetadata> {
    let rebase = existing
        .fork_basis
        .as_ref()
        .filter(|basis| basis.source_namespace_id == *namespace_id)
        .and_then(|basis| basis.rebased_from.as_ref());
    if rebase.is_some_and(|rebased_from| rebased_from.source_namespace_id == *source_namespace_id) {
        return Ok(existing);
    }
    Err(CoreError::InvalidMerge {
        param: "new_namespace_id",
        message: format!(
            "namespace `{new_namespace_id}` isn't a rebase of fork `{source_namespace_id}` of \
             `{namespace_id}`"
        ),
    }
    .into())
}

impl Namespace<Writable> {
    /// Replays the fork `source_namespace_id`'s unmerged commits into this
    /// namespace, a rebase's new fork, through the fork's head.
    ///
    /// See [`Self::replay_fork_with_options`].
    pub async fn replay_fork(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
    ) -> Result<RebaseForkResponse> {
        self.replay_fork_with_options(source_namespace_id, actor, &MergeForkOptions::default())
            .await
    }

    /// Replays the fork `source_namespace_id`'s unmerged commits into this
    /// namespace, a rebase's new fork, through `options.through_seq` or the
    /// fork's head, as one replay commit
    /// ([API section 6.16](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#616-post-rebases)).
    ///
    /// The replay commit is a merge of the fork into this namespace. It
    /// applies the fork's net change since its previous replay commit, or
    /// since the `resume_after_seq` that this namespace's fork basis
    /// records, leaves out each change that conflicts with this namespace
    /// under the conflict rule, and returns them as the conflict report with
    /// this namespace's metadata. A retry with the commit ID of a replay
    /// commit that committed returns the metadata without a report. When
    /// the commit fails because this namespace changed after the plan, the
    /// replay plans again, up to three times, as a merge does, and a later
    /// plan reports a changed row that isn't the fork's own. The subject of
    /// this handle has to be able to read each inode whose change the replay
    /// commit applies in the fork, and needs the rights in this namespace
    /// that each planned operation needs. A runtime whose merge allowance is
    /// zero returns `not_supported` for `filesystem.merges` before it reads
    /// either namespace.
    pub async fn replay_fork_with_options(
        &self,
        source_namespace_id: &NamespaceId,
        actor: &ActorId,
        options: &MergeForkOptions,
    ) -> Result<RebaseForkResponse> {
        trace::in_operation(
            operation_span!(self.core, "replay_fork", self.namespace_id),
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
                let planned = self.plan_replay(&request, None).await?;
                let conflicts = match self
                    .commit_merge(&request, planned, |request, slot| {
                        Box::pin(async move { self.plan_replay(&request, Some(slot)).await })
                    })
                    .await?
                {
                    MergeOutcome::Found(_) => None,
                    MergeOutcome::Committed { report, .. } => Some(report),
                };
                Ok(RebaseForkResponse {
                    namespace: self.metadata().await?,
                    conflicts,
                })
            },
        )
        .await
    }

    /// Returns the replay commit that this namespace committed under the
    /// request's commit ID, or the replay commit with its conflict report,
    /// planned as [`Self::plan_merge`] plans a merge.
    ///
    /// The plan checks that this namespace is a rebase's new fork for the
    /// fork and checks both floors, folds and pins the fork, and plans
    /// against this namespace's head, reading the source through this
    /// namespace's fork pin.
    pub(crate) async fn plan_replay(
        &self,
        request: &MergeRequest,
        slot: Option<MergeSlot>,
    ) -> Result<PlannedMerge<Vec<MergeConflict>>> {
        let source_namespace_id = &request.source_namespace_id;
        // Boxed so the replay's future stays within the compiler's layout depth.
        self.plan_commit(request, slot, |new_fork| {
            Box::pin(async move {
                let new_fork = &new_fork;
                let basis = self
                    .core
                    .read(
                        source_namespace_id,
                        Some(Consistency::Strong),
                        |fork| async move {
                            Ok(new_fork
                                .engine
                                .check_replay(
                                    &new_fork.context,
                                    &fork.engine,
                                    &fork.context,
                                    request,
                                )
                                .await?)
                        },
                    )
                    .await?;
                let prepared_at_ms = self.core.now_ms();
                let (fork, fork_pin) = self
                    .pin_fork(source_namespace_id, request, prepared_at_ms)
                    .await?;
                let planned = async {
                    let source = self
                        .core
                        .pinned_read_at_snapshot(
                            &basis.manifest.owner_namespace_id,
                            &basis.source_pin_id,
                        )
                        .await?;
                    let plan = new_fork
                        .engine
                        .plan_replay(
                            &new_fork.context,
                            &fork.engine,
                            &fork.context,
                            &source.engine,
                            &source.context,
                            request,
                            prepared_at_ms,
                            self.session().merge_allowance(),
                        )
                        .await?;
                    let record = plan.record().clone();
                    let report = plan.conflicts().to_vec();
                    Ok::<_, Error>((plan.into_candidate(), record, report))
                }
                .await;
                self.planned_with_fork_pin(source_namespace_id, fork_pin, planned)
                    .await
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests;
