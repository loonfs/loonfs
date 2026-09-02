//! Per-candidate admission for publish batches: single-pass commit
//! preparation, commit-id validation, and duplicate resolution against
//! durable receipts and same-batch primaries.

use super::batch::BatchOutcomeSlot;
use super::publish_view::PublishMetadataView;
use crate::commit::{CandidateAllocation, CommitFingerprint, ValidatedCommitPlan};
use crate::commit_engine::{CommitCandidate, ContentPreparation, ContentPreparationError};
use crate::error::{CoreError, Result};
use crate::metadata::CommitReceiptRecord;
use crate::path::write::{CommitRequest, FilesystemOperation, PublishPlanningSession};
use crate::storage::content_admission::ContentAdmission;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::{CommitId, ContentId, ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

pub(super) struct PreparedCandidateCommit {
    pub(super) validated: ValidatedCommitPlan,
    pub(super) allocation: CandidateAllocation,
}

/// How one batch candidate resolved during admission.
pub(super) enum CandidateAdmission {
    /// A new request ready for content validation and materialization.
    Prepared(PreparedCandidateCommit),
    /// A result decided during admission.
    Settled(BatchOutcomeSlot),
}

impl CandidateAdmission {
    fn independent(outcome: Result<ApiCommitResponse>) -> Self {
        Self::Settled(BatchOutcomeSlot::Settled {
            outcome,
            depends_on_batch: false,
        })
    }
}

#[derive(Debug, Clone)]
struct InBatchRequest {
    primary_index: usize,
    semantic_identity: CommitFingerprint,
}

/// Tracks the first request for each commit ID in the batch.
#[derive(Default)]
pub(super) struct BatchDedup {
    in_batch_requests: HashMap<CommitId, InBatchRequest>,
}

impl BatchDedup {
    /// Returns an alias or conflict when the commit ID already exists in the batch.
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
            // No commit has landed, so the conflict has no sequence.
            return Some(CandidateAdmission::independent(Err(
                CoreError::CommitIdReuseConflict {
                    commit_id: commit_id.to_string(),
                    committed_seq: None,
                    committed_fingerprint: None,
                },
            )));
        }
        Some(CandidateAdmission::Settled(BatchOutcomeSlot::AliasOf(
            existing.primary_index,
        )))
    }
}

/// Prepares a request and resolves commit-ID reuse.
pub(super) async fn prepare_candidate_request<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    session: &PublishPlanningSession,
    candidate: &CommitCandidate,
    index: usize,
    committed_at_ms: u64,
    dedup: &mut BatchDedup,
) -> CandidateAdmission {
    if let Err(error) = candidate.validate_request_has_operations() {
        return CandidateAdmission::independent(Err(error));
    }
    let mutation = candidate.request();
    let semantic_identity = match candidate.semantic_identity(namespace_id) {
        Ok(semantic_identity) => semantic_identity,
        Err(error) => return CandidateAdmission::independent(Err(error)),
    };
    match resolve_commit_id_reuse(
        namespace_id,
        view,
        dedup,
        index,
        &mutation.commit_id,
        &semantic_identity,
    )
    .await
    {
        Ok(Some(admission)) => return admission,
        Ok(None) => {}
        Err(error) => return CandidateAdmission::independent(Err(error)),
    }
    let admissions = match validate_new_primary(candidate) {
        Ok(admissions) => admissions,
        Err(error) => return CandidateAdmission::independent(Err(error)),
    };
    if let Err(error) = validate_commit_content_references(
        mutation,
        admissions,
        namespace_id,
        view.content_store_id(),
        committed_at_ms,
    ) {
        return CandidateAdmission::independent(Err(error));
    }
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
        Ok(validated) => CandidateAdmission::Prepared(PreparedCandidateCommit {
            validated,
            allocation,
        }),
        Err(error) => CandidateAdmission::Settled(BatchOutcomeSlot::Settled {
            outcome: Err(error),
            depends_on_batch: true,
        }),
    }
}

fn validate_new_primary(candidate: &CommitCandidate) -> Result<&[ContentAdmission]> {
    // Check request limits before content preparation errors.
    candidate.validate_request_limits()?;
    match candidate.content_preparation() {
        ContentPreparation::Ready(admissions) => Ok(admissions),
        ContentPreparation::Rejected(error) => Err(error.clone().into()),
    }
}

/// Resolves a commit ID against durable receipts and earlier requests in the batch.
async fn resolve_commit_id_reuse<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    dedup: &mut BatchDedup,
    index: usize,
    commit_id: &CommitId,
    semantic_identity: &CommitFingerprint,
) -> Result<Option<CandidateAdmission>> {
    if let Some(existing) = view.find_commit_receipt(commit_id).await? {
        return Ok(Some(CandidateAdmission::independent(
            if existing.semantic_commit_fingerprint != *semantic_identity {
                Err(CoreError::CommitIdReuseConflict {
                    commit_id: commit_id.to_string(),
                    committed_seq: Some(existing.committed_seq),
                    committed_fingerprint: Some(
                        existing.semantic_commit_fingerprint.as_str().to_owned(),
                    ),
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

/// Checks that each put has a matching content preparation proof.
fn validate_commit_content_references(
    request: &CommitRequest,
    admissions: &[ContentAdmission],
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    now_ms: u64,
) -> Result<()> {
    let mut admissions_by_content_id: HashMap<&ContentId, Vec<&ContentAdmission>> =
        HashMap::with_capacity(admissions.len());
    for admission in admissions {
        admissions_by_content_id
            .entry(admission.content_id())
            .or_default()
            .push(admission);
    }
    for content_ref in request
        .operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
    {
        let admitted = admissions_by_content_id
            .get(&content_ref.content_id)
            .is_some_and(|candidates| {
                candidates.iter().any(|admission| {
                    admission.admits(namespace_id, content_store_id, content_ref, now_ms)
                })
            });
        if !admitted {
            return Err(ContentPreparationError::ContentNotPrepared {
                content_id: content_ref.content_id.clone(),
            }
            .into());
        }
    }

    Ok(())
}
