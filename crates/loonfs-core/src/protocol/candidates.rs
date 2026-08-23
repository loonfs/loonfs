//! Per-candidate admission for publish batches: single-pass commit
//! preparation, commit-id validation, and duplicate resolution against
//! durable receipts and same-batch primaries.

use super::publish_view::PublishMetadataView;
use crate::commit::{CandidateAllocation, CommitFingerprint, ValidatedCommitPlan};
use crate::commit_engine::{CommitCandidate, ContentPreparation, ContentPreparationError};
use crate::error::{CoreError, Result};
use crate::metadata::CommitReceiptRecord;
use crate::path::write::{FilesystemOperation, PublishPlanningSession};
use crate::storage::content_admission::ContentAdmission;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::{CommitId, ContentRef, ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

pub(super) struct PreparedCandidateCommit {
    pub(super) validated: ValidatedCommitPlan,
    pub(super) allocation: CandidateAllocation,
}

/// How one batch candidate resolved during admission.
pub(super) enum CandidateAdmission {
    /// A new primary request, planned and validated in one pass, ready for
    /// content coverage and materialization.
    Prepared(PreparedCandidateCommit),
    /// A same-batch duplicate of the primary at the given outcome slot; the
    /// alias slot inherits the primary's final outcome.
    AliasOf(usize),
    /// The outcome was decided during admission: an idempotent replay of a
    /// durable commit receipt, or a rejection.
    Settled(Result<ApiCommitResponse>),
}

#[derive(Debug, Clone)]
struct InBatchRequest {
    primary_index: usize,
    semantic_identity: CommitFingerprint,
}

/// Duplicate-commit-id bookkeeping for one publish batch.
///
/// Tracks which outcome slot first claimed each commit id (the primary) and
/// which later slots alias it, so alias slots can inherit their primary's
/// final outcome once the batch resolves.
#[derive(Default)]
pub(super) struct BatchDedup {
    in_batch_requests: HashMap<CommitId, InBatchRequest>,
    aliases: Vec<(usize, usize)>,
}

impl BatchDedup {
    /// Admits the candidate at `index` against earlier same-batch requests.
    ///
    /// Returns `None` for a new primary (recorded for later duplicates to
    /// find) and `Some` when the commit id was already claimed in this batch:
    /// an alias when the semantic identity matches, a settled rejection when
    /// it conflicts. The caller records aliases with [`Self::record_alias`].
    fn admit(
        &mut self,
        index: usize,
        commit_id: &CommitId,
        semantic_identity: &CommitFingerprint,
    ) -> Option<CandidateAdmission> {
        let Some(existing) = self.in_batch_requests.get(commit_id) else {
            self.in_batch_requests.insert(
                commit_id.clone(),
                InBatchRequest {
                    primary_index: index,
                    semantic_identity: semantic_identity.clone(),
                },
            );
            return None;
        };
        if existing.semantic_identity != *semantic_identity {
            // Neither claim has committed, so there is no landed commit to
            // name: the caller cannot reconcile this one by reading the
            // feed, and the conflict is the whole answer.
            return Some(CandidateAdmission::Settled(Err(
                CoreError::CommitIdReuseConflict {
                    commit_id: commit_id.to_string(),
                    committed_seq: None,
                    committed_fingerprint: None,
                },
            )));
        }
        Some(CandidateAdmission::AliasOf(existing.primary_index))
    }

    /// Records that the slot at `alias_index` inherits the outcome of the
    /// primary slot at `primary_index` when the batch resolves.
    pub(super) fn record_alias(&mut self, alias_index: usize, primary_index: usize) {
        self.aliases.push((alias_index, primary_index));
    }

    /// Resolves alias slots to their primary's outcome and unwraps the rest.
    pub(super) fn finish(
        &self,
        mut outcomes: Vec<Option<Result<ApiCommitResponse>>>,
    ) -> Vec<Result<ApiCommitResponse>> {
        for (alias_index, primary_index) in &self.aliases {
            let primary_outcome = outcomes
                .get(*primary_index)
                .and_then(Clone::clone)
                .unwrap_or_else(|| {
                    Err(CoreError::Internal(
                        "missing primary batch outcome".to_owned(),
                    ))
                });
            outcomes[*alias_index] = Some(primary_outcome);
        }
        outcomes
            .into_iter()
            .map(|outcome| {
                outcome
                    .unwrap_or_else(|| Err(CoreError::Internal("missing batch outcome".to_owned())))
            })
            .collect()
    }
}

/// Prepares one batch candidate as a validated commit plan, resolving
/// commit-id reuse against durable receipts and same-batch primaries.
///
/// Hard failures return `Err`; the caller settles the candidate's outcome
/// slot with them exactly as with [`CandidateAdmission::Settled`].
pub(super) async fn prepare_candidate_request<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    session: &PublishPlanningSession,
    candidate: &CommitCandidate,
    index: usize,
    committed_at_ms: u64,
    dedup: &mut BatchDedup,
) -> Result<CandidateAdmission> {
    let mutation = candidate.request();
    let semantic_identity = candidate.semantic_identity(namespace_id)?;
    if let Some(admission) = resolve_commit_id_reuse(
        namespace_id,
        view,
        dedup,
        index,
        &mutation.commit_id,
        &semantic_identity,
    )
    .await?
    {
        return Ok(admission);
    }
    validate_new_primary(candidate)?;
    let mut allocation = session.begin_candidate();
    match session
        .prepare_commit(
            mutation,
            semantic_identity,
            view.metadata_view(),
            committed_at_ms,
            &mut allocation,
        )
        .await
    {
        Ok(validated) => Ok(CandidateAdmission::Prepared(PreparedCandidateCommit {
            validated,
            allocation,
        })),
        Err(error) => {
            session.discard_candidate(allocation);
            Err(error)
        }
    }
}

fn validate_new_primary(candidate: &CommitCandidate) -> Result<()> {
    // For new primaries, request limits precede rejected content preparation.
    candidate.validate_request_limits()?;
    match candidate.content_preparation() {
        ContentPreparation::Ready(_) => Ok(()),
        ContentPreparation::Rejected(error) => Err(error.clone().into()),
    }
}

/// Settles a candidate whose commit id was already used, either by a durable
/// commit receipt (idempotent replay or reuse conflict) or by an earlier
/// candidate in the same batch (alias or reuse conflict).
///
/// Returns `None` when the commit id is new: the candidate is recorded as
/// the primary for that id and must be prepared.
async fn resolve_commit_id_reuse<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    dedup: &mut BatchDedup,
    index: usize,
    commit_id: &CommitId,
    semantic_identity: &CommitFingerprint,
) -> Result<Option<CandidateAdmission>> {
    if let Some(existing) = view.find_commit_receipt(commit_id).await? {
        return Ok(Some(CandidateAdmission::Settled(
            if existing.semantic_commit_fingerprint != semantic_identity.as_str() {
                // The receipt holds where the id landed and what landed
                // there; reporting both is what turns a caller's
                // reconciliation into one feed read and one comparison.
                Err(CoreError::CommitIdReuseConflict {
                    commit_id: commit_id.to_string(),
                    committed_seq: Some(existing.committed_seq),
                    committed_fingerprint: Some(existing.semantic_commit_fingerprint.clone()),
                })
            } else {
                commit_response_from_commit_receipt(namespace_id, view, &existing).await
            },
        )));
    }
    Ok(dedup.admit(index, commit_id, semantic_identity))
}

/// Builds a replay response from the commit receipt and retained WAL record.
/// A recent replay usually reads one WAL segment. If the WAL record has been
/// retired but the receipt remains, the response omits `events`.
async fn commit_response_from_commit_receipt<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    record: &CommitReceiptRecord,
) -> Result<ApiCommitResponse> {
    let change = match view.find_committed_change_at(record.committed_seq).await {
        Ok(Some(change)) => change,
        Ok(None) => {
            return Err(CoreError::Internal(format!(
            "commit receipt for `{}` names sequence `{}`, where the change feed reports no commit",
            record.commit_id, record.committed_seq
        )))
        }
        Err(CoreError::RebootstrapRequired { .. }) => {
            return Ok(ApiCommitResponse {
                namespace_id: namespace_id.clone(),
                commit_id: record.commit_id.clone(),
                committed_seq: record.committed_seq,
                committed_by: record.committed_by.clone(),
                committed_at_ms: record.committed_at_ms,
                message: record.message.clone(),
                events: None,
            })
        }
        Err(error) => return Err(error),
    };
    if change.commit_id != record.commit_id {
        return Err(CoreError::Internal(format!(
            "commit receipt for `{}` names sequence `{}`, where the change feed reports commit `{}`",
            record.commit_id, record.committed_seq, change.commit_id
        )));
    }
    Ok(ApiCommitResponse::from_committed_change(
        namespace_id.clone(),
        change,
    ))
}

struct CommitContentAdmissions<'a> {
    content_store_id: &'a ContentStoreId,
    admissions: &'a [ContentAdmission],
}

impl CommitContentAdmissions<'_> {
    fn admits(&self, content_ref: &ContentRef) -> bool {
        self.admissions
            .iter()
            .any(|admission| admission.admits(self.content_store_id, content_ref))
    }
}

/// Checks every new external content ref against in-memory preparation proofs.
///
/// Copy and restore reuse content already retained by the namespace, whose
/// durability is guaranteed; only a put introduces bytes that need proof.
pub(super) fn validate_commit_content_references(
    candidate: &CommitCandidate,
    content_store_id: &ContentStoreId,
) -> Result<()> {
    let admissions = CommitContentAdmissions {
        content_store_id,
        admissions: match candidate.content_preparation() {
            ContentPreparation::Ready(admissions) => admissions,
            ContentPreparation::Rejected(_) => {
                return Err(CoreError::Internal(
                    "rejected content preparation reached coverage validation".to_owned(),
                ));
            }
        },
    };
    for operation in &candidate.request().operations {
        if let FilesystemOperation::PutFile { content_ref, .. } = operation {
            require_content_admission(&admissions, content_ref)?;
        }
    }

    Ok(())
}

fn require_content_admission(
    admissions: &CommitContentAdmissions<'_>,
    content_ref: &ContentRef,
) -> Result<()> {
    if admissions.admits(content_ref) {
        return Ok(());
    }
    Err(ContentPreparationError::ContentNotPrepared {
        content_id: content_ref.content_id.clone(),
    }
    .into())
}
