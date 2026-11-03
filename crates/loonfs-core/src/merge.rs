//! Merging a fork into a namespace. The merge is one commit that applies the
//! fork's net change over a range of its commits, planned against the
//! namespace's head under the conflict rule
//! ([format section 6.2](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#62-validation)).

mod classify;
mod operations;
mod order;
mod state;
mod targets;

use crate::authorize::{Authorizer, CommitAuthority};
use crate::commit::candidates::commit_response_from_commit_receipt;
use crate::commit::engine::CommitCandidate;
use crate::commit::wal_size::commit_fields_bytes;
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, MergeBound, MergeHistory, Result};
use crate::limits::DIRECT_TRANSFER_URL_TTL_MS;
use crate::namespace::state::NamespaceReadState;
use crate::path::read::LoadedMetadataView;
use crate::path::write::{merge_fingerprint, CommitIntent, PublishPlanningSession};
use crate::storage::content_admission::PreparedContent;
use crate::storage::tail_content::ContentWrites;
use classify::Classifier;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::MergeRecord;
use loonfs_types::{
    u64_from_usize, ActorId, ChangeSeq, Commit, CommitId, CommitPrecondition, FilesystemOperation,
    InodeKind, MergeConflict, NamespaceId,
};
use operations::BindingChange;
use order::MergeOperations;
use state::StateReader;
use std::collections::{HashMap, HashSet};

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
/// publishing a merge.
///
/// The estimate is `peak_per_planned_byte` times the merge's planned WAL
/// bytes. The planner checks the allowance as it plans, so it stops planning
/// a merge that admission would refuse, and admission checks the merge's
/// commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeAllowance {
    /// The allowance, in bytes.
    pub max_bytes: usize,
    /// The multiple of a merge's planned WAL bytes that estimates the peak.
    pub peak_per_planned_byte: usize,
}

impl MergeAllowance {
    /// Returns `merge_too_large` when the estimated peak of a merge whose
    /// planned WAL bytes are `planned_bytes` exceeds the allowance. An
    /// allowance of zero turns merges off, so it refuses every merge.
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
        Ok(())
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
    /// Returns the merge's commit, which its caller checks against the
    /// publisher's merge allowance before it checks the conflicts.
    pub fn candidate(&self) -> &CommitCandidate {
        &self.candidate
    }

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
    /// object or `allowance`, before it reports a conflict, and records the
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
        let fork_head = fork_view.head();
        let (fork_point_seq, source_after_seq) = range_start(&main, &fork_view, request).await?;
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
        let mut classifier = Classifier {
            main: &main,
            theirs: StateReader::new(main.metadata_view()),
            base: StateReader::new(main.metadata_view_at(fork_point_seq)),
            ours: StateReader::new(fork_view.metadata_view_at(record.source_through_seq)),
            before: StateReader::new(fork_view.metadata_view_at(source_after_seq)),
            fork_authorizer: Authorizer::for_request(
                source_namespace_id,
                &fork_head.access,
                CommitAuthority::Subject(fork.subject.as_ref()),
            )?,
            record: &record,
            fork_point_seq,
            allowance,
            own_seqs: HashMap::new(),
            plans: Vec::new(),
            layouts: HashMap::new(),
            planned_bytes: commit_fields_bytes(
                &intent.commit_id,
                &intent.actor_id,
                intent.message.as_deref(),
                Some(&record),
            ),
            subtree_checks: Vec::new(),
            conflicted: HashSet::new(),
            first_conflict: None,
        };
        classifier.classify().await?;
        let holders = classifier.check_targets().await?;
        let MergeOperations {
            operations,
            created_inode_ids,
            temporary_moves,
        } = classifier.order(&holders).await?;
        let moves_or_deletes_a_directory = classifier.plans.iter().any(|plan| {
            plan.kind == InodeKind::Directory
                && matches!(
                    plan.binding,
                    Some(BindingChange::Move { .. } | BindingChange::Delete { .. })
                )
        });
        let preconditions = if moves_or_deletes_a_directory || !temporary_moves.is_empty() {
            vec![CommitPrecondition::NamespaceHead {
                expected_head_seq: main.head().seq,
            }]
        } else {
            Vec::new()
        };
        let conflict = classifier
            .first_conflict
            .take()
            .map(|first| (u64_from_usize(classifier.conflicted.len()), first));
        let expires_at_ms = prepared_at_ms.saturating_add(DIRECT_TRANSFER_URL_TTL_MS);
        let proofs = classifier
            .layouts
            .drain()
            .map(|(content_ref, layout)| {
                PreparedContent::for_family_member(
                    self.namespace.namespace_id.clone(),
                    content_ref,
                    layout,
                    expires_at_ms,
                )
            })
            .collect();
        // The classifier borrows the record, which the commit takes.
        drop(classifier);
        let candidate = CommitCandidate::prepared(
            self.merge_intent(request, operations, preconditions),
            proofs,
        )
        .with_merge(record.clone(), fork_point_seq, created_inode_ids)
        .with_temporary_moves(temporary_moves);
        if conflict.is_none() {
            self.check_merge_rights(&main, &candidate, prepared_at_ms)
                .await?;
        }
        Ok(MergePlan {
            candidate,
            record,
            conflict,
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
        range_start(&main, &fork_view, request).await.map(|_| ())
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

/// Returns the fork point and the start of a merge's range. The fork point is
/// the sequence of `main` that `fork_view`'s namespace was made at, and it
/// has to be at or above `main`'s retention floor. The range starts after the
/// fork's newest merge in `main`'s merge index, or after the fork point, and
/// that start has to be at or above the fork's own floor, and equal to the
/// request's `after_seq` when the request sets one.
async fn range_start<S: ObjectStore + ?Sized>(
    main: &LoadedMetadataView<'_, S>,
    fork_view: &LoadedMetadataView<'_, S>,
    request: &MergeRequest,
) -> Result<(ChangeSeq, ChangeSeq)> {
    let fork_head = fork_view.head();
    let fork_point_seq = fork_point(fork_head, &main.head().namespace_id)?;
    let retention_floor_seq = main.retention_floor_seq();
    if fork_point_seq < retention_floor_seq {
        return Err(CoreError::MergeRebootstrapRequired {
            below: MergeHistory::ForkPoint { fork_point_seq },
            retention_floor_seq,
        });
    }
    let source_namespace_id = &fork_head.namespace_id;
    let source_after_seq = main
        .metadata_view()
        .newest_merge(source_namespace_id)
        .await?
        .map_or(fork_point_seq, |previous| previous.source_through_seq);
    if let Some(planned_after_seq) = request.after_seq {
        if planned_after_seq != source_after_seq {
            return Err(CoreError::MergeRangeConflict {
                source_namespace_id: source_namespace_id.clone(),
                expected_after_seq: source_after_seq,
                actual_after_seq: planned_after_seq,
            });
        }
    }
    let fork_floor_seq = fork_view.retention_floor_seq();
    if source_after_seq < fork_floor_seq {
        return Err(CoreError::MergeRebootstrapRequired {
            below: MergeHistory::RangeStart {
                source_namespace_id: source_namespace_id.clone(),
                source_after_seq,
            },
            retention_floor_seq: fork_floor_seq,
        });
    }
    Ok((fork_point_seq, source_after_seq))
}

/// Returns the fork point of the fork whose head is `fork_head`, which is the
/// sequence of `namespace_id` that the fork's basis manifest materializes. A
/// namespace whose basis manifest belongs to another namespace isn't a fork
/// of `namespace_id`.
fn fork_point(fork_head: &NamespaceReadState, namespace_id: &NamespaceId) -> Result<ChangeSeq> {
    match &fork_head.fork_basis {
        Some(basis) if basis.manifest.owner_namespace_id == *namespace_id => {
            Ok(basis.manifest.head_seq)
        }
        _ => Err(CoreError::InvalidMerge {
            param: "/source_namespace_id",
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
        param: "/through_seq",
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
