//! [`PublishPlanningSession`]: plans one candidate against the state that a
//! live publish planning session lends it.

use super::planned_pieces::CommitPieces;
use super::planner::{CreatedInodeIds, OperationPlanningSession};
use super::publish_path_planning::PlanningView;
use crate::authorize::Authorizer;
use crate::commit::engine::{CommitCandidate, MergeIntent};
use crate::commit::{AppendedContent, CommitFingerprint, CommitPlan};
use crate::error::{CoreError, Result};
use crate::frozen_write_path::validate::CommitValidationSession;
use crate::merge::merge_start;
use crate::metadata::{DurableVisibilityCache, MetadataState, MetadataView};
use crate::namespace::state::NamespaceReadState;
use crate::storage::tail_content::{cut_layout, materialize_content_layout, ContentWrites};
use crate::wal::{ProjectedContent, ProjectedWalTail};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{WalDelta, WalInlineContent};
use loonfs_types::{next_public_ordinal, CommitSeq, ContentRef, InodeId, NamespaceId};
use std::collections::BTreeSet;
use std::num::NonZeroU32;

/// Looks up the pieces of one content that the batch's accepted commits
/// wrote, before the WAL tail's pieces are consulted.
pub(crate) type AcceptedContent<'a> = dyn Fn(&ContentRef) -> Option<ProjectedContent> + Sync + 'a;

/// The state of one publish attempt, borrowed from the live session that
/// prepares the same candidate.
pub(crate) struct PublishPlanningSession<'a, S: ObjectStore + ?Sized> {
    /// The head after the batch's accepted candidates.
    pub(crate) head: &'a NamespaceReadState,
    pub(crate) tail: &'a ProjectedWalTail,
    pub(crate) base_view: MetadataView<'a, 'a, S>,
    pub(crate) content_writes: ContentWrites<'a, S>,
    pub(crate) committed_at_ms: u64,
    pub(crate) accepted_rows: &'a MetadataState,
    pub(crate) accepted_content: &'a AcceptedContent<'a>,
    pub(crate) durable_cache: &'a DurableVisibilityCache,
}

impl<S: ObjectStore + ?Sized> PublishPlanningSession<'_, S> {
    /// The namespace that the batch commits to.
    pub(super) fn namespace_id(&self) -> &NamespaceId {
        &self.head.namespace_id
    }

    pub(super) fn content(&self, content_ref: &ContentRef) -> Option<ProjectedContent> {
        (self.accepted_content)(content_ref).or_else(|| self.tail.content(content_ref))
    }

    /// Checks a candidate's preconditions and plans and validates its
    /// operations in one pass, against the batch's state before the
    /// candidate.
    pub(crate) async fn prepare_commit(
        &self,
        candidate: &CommitCandidate,
        authorizer: Authorizer<'_>,
        semantic_identity: CommitFingerprint,
    ) -> Result<CommitPlan> {
        let base_view = self.base_view.with_durable_cache(self.durable_cache);
        let pre_state = base_view.with_overlay(None, self.accepted_rows, self.head.seq);
        if let Some(merge) = candidate.merge() {
            ensure_merge_is_admissible(&pre_state, self.head, merge).await?;
        }
        self.planning_view(authorizer, pre_state)
            .evaluate_preconditions(&candidate.intent().preconditions, self.head.seq)
            .await?;
        self.plan_commit(
            candidate,
            authorizer,
            semantic_identity,
            base_view,
            &pre_state,
        )
        .await
    }

    fn planning_view<'v>(
        &'v self,
        authorizer: Authorizer<'v>,
        metadata: MetadataView<'v, 'v, S>,
    ) -> PlanningView<'v, S> {
        PlanningView {
            namespace_id: &self.head.namespace_id,
            authorizer,
            metadata,
        }
    }

    /// Plans each operation against the view, validates its compiled
    /// operations at once, and applies the accepted effects, so later
    /// operations observe earlier ones. The first failure aborts the request
    /// and attaches the failing operation's index.
    ///
    /// The head is the validated source of the namespace and writer epoch:
    /// `load_publish_metadata_view` fences on epoch equality, and the batch
    /// rejects namespace mismatches before admission.
    async fn plan_commit(
        &self,
        candidate: &CommitCandidate,
        authorizer: Authorizer<'_>,
        semantic_identity: CommitFingerprint,
        base_view: MetadataView<'_, '_, S>,
        pre_state: &MetadataView<'_, '_, S>,
    ) -> Result<CommitPlan> {
        let head = self.head;
        let request = candidate.intent();
        let committed_seq = next_public_ordinal(head.seq.0)
            .map(CommitSeq)
            .ok_or_else(|| {
                CoreError::NamespaceCorrupt(format!(
                    "the namespace head is at sequence {}, the largest sequence number, so no \
                     commit can follow it",
                    head.seq
                ))
            })?;
        let mut validation = CommitValidationSession::new(
            base_view,
            self.accepted_rows,
            committed_seq,
            &request.commit_id,
            &request.actor_id,
            self.committed_at_ms,
        );
        let mut deltas = Vec::new();
        let mut operation_indexes = Vec::new();
        let mut appended: Vec<AppendedContent> = Vec::new();
        let mut pieces = CommitPieces::new(self);
        let mut next_create_no = NonZeroU32::MIN;
        let proofs = candidate.proofs_by_reference();
        for value in candidate.inline_content() {
            pieces.insert_piece(
                value.content_ref(),
                &WalInlineContent {
                    content_id: value.content_ref().content_id.clone(),
                    offset: 0,
                    bytes: value.bytes().to_vec(),
                },
                committed_seq,
            );
        }
        for (index, operation) in request.operations.iter().enumerate() {
            let temporary_move = candidate
                .merge()
                .and_then(|merge| merge.temporary_moves.get(&index).copied());
            let created_inode_ids = match candidate.merge() {
                Some(merge) => {
                    let assigned = merge.created_inode_ids.get(&index).copied();
                    if let Some(inode_id) = assigned {
                        ensure_merged_inode_is_new(&validation.view(), inode_id)
                            .await
                            .map_err(|error| error.at_operation(index))?;
                    }
                    CreatedInodeIds::Merge(assigned)
                }
                None => CreatedInodeIds::Generated {
                    next_create_no: &mut next_create_no,
                },
            };
            let mut operation_session = OperationPlanningSession {
                view: self.planning_view(authorizer, validation.view()),
                pieces: &pieces,
                content_writes: self.content_writes,
                committed_seq,
                created_inode_ids,
                temporary_move,
            };
            let compiled = operation_session
                .plan(operation)
                .await
                .map_err(|error| error.at_operation(index))?;
            if let CreatedInodeIds::Merge(Some(inode_id)) = operation_session.created_inode_ids {
                // Only a bug in the merge's planner assigns an ID to an
                // operation that creates no inode. A panic would return
                // `outcome_unknown` and panic again on every retry, so
                // the merge fails with an error that the caller doesn't retry.
                return Err(CoreError::InvalidCommitRequest(format!(
                    "a merge assigned inode `{inode_id}` to an operation that creates no inode"
                ))
                .at_operation(index));
            }
            if let Some(value) = &compiled.appended {
                for piece in &value.pieces {
                    pieces.insert_piece(&value.content_ref, piece, committed_seq);
                }
            }
            appended.extend(compiled.appended);
            let operation_deltas = validation
                .validate(&compiled.ops, |content_ref| {
                    if let Some(value) = appended
                        .iter()
                        .find(|value| value.content_ref == *content_ref)
                    {
                        return value.layout.clone();
                    }
                    proofs
                        .get(content_ref)
                        .and_then(|proof| proof.revision_layout(&head.namespace_id))
                })
                .await
                .map_err(|error| error.at_operation(index))?;
            operation_indexes.extend(std::iter::repeat_n(index, operation_deltas.len()));
            deltas.extend(operation_deltas);
        }

        let mut plan = CommitPlan {
            namespace_id: head.namespace_id.clone(),
            commit_id: request.commit_id.clone(),
            actor_id: request.actor_id.clone(),
            writer_epoch: head.writer_epoch,
            message: request.message.clone(),
            merge: candidate.merge().map(|merge| merge.record.clone()),
            semantic_identity,
            apply_after_seq: head.seq,
            assigned_seq: committed_seq,
            committed_at_ms: self.committed_at_ms,
            deltas,
            appended: appended
                .into_iter()
                .flat_map(|value| value.pieces)
                .collect(),
            inline_content: candidate.inline_content().to_vec(),
        };
        self.carry_revision_layouts(
            &validation.view(),
            pre_state,
            &pieces,
            candidate,
            &operation_indexes,
            &mut plan,
        )
        .await?;
        Ok(plan)
    }

    async fn carry_revision_layouts(
        &self,
        view: &MetadataView<'_, '_, S>,
        before: &MetadataView<'_, '_, S>,
        pieces: &CommitPieces<'_, S>,
        candidate: &CommitCandidate,
        operation_indexes: &[usize],
        plan: &mut CommitPlan,
    ) -> Result<()> {
        let mut revision_content_ids = BTreeSet::new();
        for (delta, &index) in plan.deltas.iter_mut().zip(operation_indexes) {
            let WalDelta::AppendFileRevision {
                content_ref,
                layout,
                ..
            } = &mut delta.delta
            else {
                continue;
            };
            let earlier_revision = !revision_content_ids.insert(content_ref.content_id.clone());
            let first_offset = candidate
                .inline_content()
                .iter()
                .filter(|value| value.content_ref().content_id == content_ref.content_id)
                .map(|_| 0)
                .chain(
                    plan.appended
                        .iter()
                        .filter(|piece| piece.content_id == content_ref.content_id)
                        .map(|piece| piece.offset),
                )
                .min();
            if first_offset == Some(0) {
                *layout = None;
                continue;
            }
            if first_offset.is_some()
                && (earlier_revision || before.content_published(&content_ref.content_id).await?)
            {
                *layout = None;
                continue;
            }
            let mut prefix = content_ref.clone();
            prefix.size_bytes = first_offset.unwrap_or(content_ref.size_bytes);
            if let Some(layout) = layout
                .as_mut()
                .filter(|layout| layout.size_bytes() >= prefix.size_bytes)
            {
                cut_layout(layout, prefix.size_bytes);
            } else {
                assert!(
                    first_offset.is_none(),
                    "append planning should give fresh content `{}` a layout covering {} bytes",
                    content_ref.content_id,
                    prefix.size_bytes
                );
                let content = pieces.content(&prefix);
                *layout = Some(
                    materialize_content_layout(
                        self.content_writes,
                        view,
                        content.as_ref(),
                        &prefix,
                    )
                    .await
                    .map_err(|error| error.at_operation(index))?,
                );
            }
        }
        Ok(())
    }
}

/// Requires a merge into the namespace at `head` and `view` to pass the floor
/// rule and to start where [`merge_start`] says, so that a fork's commits
/// are applied once and in order.
///
/// The floor check runs first because the range check depends on it. A base
/// compaction removes the merge index rows below the floor, so if the fork
/// point were below the floor, the fork's newest row might be gone, and the
/// range check would expect the next merge to start at the fork point.
async fn ensure_merge_is_admissible<S: ObjectStore + ?Sized>(
    view: &MetadataView<'_, '_, S>,
    head: &NamespaceReadState,
    merge: &MergeIntent,
) -> Result<()> {
    let source_namespace_id = &merge.record.source_namespace_id;
    let expected_after_seq = merge_start(view, head, source_namespace_id, merge.fork_point_seq)
        .await??
        .after_seq;
    if merge.record.source_after_seq != expected_after_seq {
        return Err(CoreError::MergeRangeConflict {
            source_namespace_id: source_namespace_id.clone(),
            expected_after_seq,
            actual_after_seq: merge.record.source_after_seq,
        });
    }
    Ok(())
}

/// Requires an inode that a merge creates under the fork's ID to be absent
/// from `view`, which includes the merge's earlier operations, so an ID
/// assigned to two creates fails at the second.
///
/// An inode row is immutable and unique, so a second row for the same ID
/// would corrupt the namespace. The ID is present only if the merge's planner
/// has a bug: it mistook an inode that this namespace already has (for
/// example, one that an earlier merge created) for a new one, or it assigned
/// one ID to two creates.
async fn ensure_merged_inode_is_new<S: ObjectStore + ?Sized>(
    view: &MetadataView<'_, '_, S>,
    inode_id: InodeId,
) -> Result<()> {
    if view.inode_at_seq(inode_id).await?.is_some() {
        return Err(CoreError::InvalidCommitRequest(format!(
            "a merge creates inode `{inode_id}`, which already exists"
        )));
    }
    Ok(())
}
