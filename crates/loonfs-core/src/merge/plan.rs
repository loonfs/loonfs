//! The commit of a merge or of a rebase's replay commit, planned from the
//! fork's net change over a range against the head of the namespace that the
//! commit commits into.

use super::classify::Classifier;
use super::memory::PlanningMemory;
use super::operations::BindingChange;
use super::order::MergeOperations;
use super::replay::Replay;
use super::size::with_split_hint;
use super::state::StateReader;
use super::{through_seq, MergeAllowance, MergeRequest};
use crate::authorize::{Authorizer, CommitAuthority};
use crate::commit::engine::CommitCandidate;
use crate::commit::wal_size::commit_fields_bytes;
use crate::engine::NamespaceEngine;
use crate::error::Result;
use crate::limits::OPERATION_SNAPSHOT_TTL_MS;
use crate::path::read::LoadedMetadataView;
use crate::storage::content_admission::PreparedContent;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{u64_from_usize, CommitPrecondition, CommitSeq, InodeKind, MergeConflict};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The commit of a merge or of a replay commit, as one plan made it, with
/// the conflicts that its classification found.
pub(super) struct PlannedMergeCommit {
    pub(super) candidate: CommitCandidate,
    pub(super) record: MergeRecord,
    /// The number of conflicting inodes, and the one with the smallest ID.
    pub(super) first_conflict: Option<(u64, MergeConflict)>,
    /// A replay commit's conflict report, or `None` for a merge.
    pub(super) report: Option<Vec<MergeConflict>>,
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Plans the commit of a merge of `fork`, read as `fork_view`, into
    /// `main`, over the range that starts after `source_after_seq` of a fork
    /// made at `fork_point_seq`, or of a rebase's replay commit, when
    /// `snapshot` is the source that `main`, the rebase's new fork, was forked
    /// from, read through its snapshot (see [`Self::plan_merge`] and
    /// [`Self::plan_replay`]).
    ///
    /// The commit expects `main`'s head when it deletes or moves a
    /// directory, or moves an inode through a temporary name, for a replay
    /// commit too, because a rebase's new fork takes ordinary commits while
    /// the rebase runs. A replay commit skips its conflicting changes and the
    /// changes that depend on them. The plan returns `merge_too_large` when
    /// the plans so far stop fitting one WAL object or `allowance` during
    /// the classification, or when the ordered commit, with the temporary
    /// moves that the ordering adds, doesn't fit them. Either refusal has
    /// the split hint that the fork's net change over shorter ranges gives
    /// (see [`with_split_hint`]). Each check grows the merge's reservation of
    /// the allowance to cover the planned bytes and the memory that the plan
    /// uses (see [`PlanningMemory`]), and returns `server_busy` when other
    /// merges' reservations leave no room for the growth.
    #[allow(
        clippy::too_many_arguments,
        reason = "a merge and a replay commit share every input, and a replay commit adds the snapshot"
    )]
    pub(super) async fn plan_merge_commit<'v, F>(
        &self,
        main: &'v LoadedMetadataView<'v, S>,
        snapshot: Option<&'v LoadedMetadataView<'v, S>>,
        fork: &NamespaceEngine<S, F>,
        fork_view: &'v LoadedMetadataView<'v, S>,
        (fork_point_seq, source_after_seq): (CommitSeq, CommitSeq),
        request: &MergeRequest,
        prepared_at_ms: u64,
        allowance: &MergeAllowance,
    ) -> Result<PlannedMergeCommit> {
        let fork_head = fork_view.head();
        let source_namespace_id = &fork_head.namespace_id;
        let record = MergeRecord {
            source_namespace_id: source_namespace_id.clone(),
            source_after_seq,
            source_through_seq: through_seq(
                source_namespace_id,
                fork_view.anchor.manifest_head_seq,
                source_after_seq,
                request.through_seq,
            )?,
        };
        let intent = self.merge_intent(request, Vec::new(), Vec::new());
        let fields_bytes = commit_fields_bytes(
            &intent.commit_id,
            &intent.actor_id,
            intent.message.as_deref(),
            Some(&record),
        );
        let memory = PlanningMemory::new(allowance.clone());
        let mut classifier = Classifier {
            main,
            theirs: StateReader::new(main.metadata_view(), &memory),
            base: StateReader::new(
                snapshot.unwrap_or(main).metadata_view_at(fork_point_seq),
                &memory,
            ),
            ours: StateReader::new(
                fork_view.metadata_view_at(record.source_through_seq),
                &memory,
            ),
            before: StateReader::new(fork_view.metadata_view_at(source_after_seq), &memory),
            fork_authorizer: Authorizer::for_request(
                source_namespace_id,
                &fork_head.access,
                CommitAuthority::Subject(fork.subject.as_ref()),
            )?,
            record: &record,
            fork_point_seq,
            memory: Arc::clone(&memory),
            own_seqs: HashMap::new(),
            plans: Vec::new(),
            layouts: HashMap::new(),
            planned_bytes: fields_bytes,
            subtree_checks: Vec::new(),
            conflicted: HashSet::new(),
            first_conflict: None,
            replay: snapshot.map(|snapshot| Replay {
                snapshot,
                report: Vec::new(),
            }),
        };
        let range = (source_after_seq, record.source_through_seq);
        with_split_hint(
            classifier.classify().await,
            fork_view,
            range,
            fields_bytes,
            allowance,
        )
        .await?;
        let holders = classifier.check_targets().await?;
        if classifier.replay.is_some() {
            classifier.skip_conflicts(&holders).await?;
        }
        let MergeOperations {
            operations,
            created_inode_ids,
            temporary_moves,
            preconditions: file_revisions,
        } = classifier.order(&holders).await?;
        let moves_or_deletes_a_directory = classifier.plans.iter().any(|plan| {
            plan.kind == InodeKind::Directory
                && matches!(
                    plan.binding,
                    Some(BindingChange::Move { .. } | BindingChange::Delete { .. })
                )
        });
        let head = (moves_or_deletes_a_directory || !temporary_moves.is_empty()).then(|| {
            CommitPrecondition::NamespaceHead {
                expected_head_seq: main.head().seq,
            }
        });
        let preconditions = head.into_iter().chain(file_revisions).collect();
        let first_conflict = classifier
            .first_conflict
            .take()
            .map(|first| (u64_from_usize(classifier.conflicted.len()), first));
        // A replay commit leaves out the plans that it skips, and each
        // proof goes with a content reference that an operation writes.
        let applied: HashSet<_> = operations
            .iter()
            .filter_map(|operation| operation.content_ref())
            .collect();
        let expires_at_ms = prepared_at_ms.saturating_add(OPERATION_SNAPSHOT_TTL_MS);
        let proofs = classifier
            .layouts
            .drain()
            .filter(|(content_ref, _)| applied.contains(content_ref))
            .map(|(content_ref, layout)| {
                PreparedContent::for_pool_member(
                    self.namespace.namespace_id.clone(),
                    content_ref,
                    layout,
                    expires_at_ms,
                )
            })
            .collect();
        let report = classifier.replay.take().map(|replay| replay.report);
        // The classifier borrows the record, which the commit takes.
        drop(classifier);
        let candidate = CommitCandidate::prepared(
            self.merge_intent(request, operations, preconditions),
            proofs,
        )
        .with_merge(record.clone(), fork_point_seq, created_inode_ids)
        .with_temporary_moves(temporary_moves);
        with_split_hint(
            memory.check_planned(candidate.wal_record_bytes_upper_bound()),
            fork_view,
            range,
            fields_bytes,
            allowance,
        )
        .await?;
        Ok(PlannedMergeCommit {
            candidate,
            record,
            first_conflict,
            report,
        })
    }
}
