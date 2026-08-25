//! Per-candidate admission for publish batches: single-pass commit
//! preparation, commit-id validation, and duplicate resolution against
//! durable receipts and same-batch primaries.

use super::batch::BatchOutcomeSlot;
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
    /// A new request ready for content validation and materialization.
    Prepared(PreparedCandidateCommit),
    /// A candidate whose slot admission already decided.
    Settled(BatchOutcomeSlot),
}

impl CandidateAdmission {
    /// Decided against durable state or the request itself.
    fn independent(outcome: Result<ApiCommitResponse>) -> Self {
        Self::Settled(BatchOutcomeSlot::SettledIndependent(outcome))
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
    if let Err(error) = validate_new_primary(candidate) {
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
        Err(error) => {
            session.discard_candidate(allocation);
            CandidateAdmission::Settled(BatchOutcomeSlot::SettledContingent(Err(error)))
        }
    }
}

fn validate_new_primary(candidate: &CommitCandidate) -> Result<()> {
    // Check request limits before content preparation errors.
    candidate.validate_request_limits()?;
    match candidate.content_preparation() {
        ContentPreparation::Ready(_) => Ok(()),
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
            if existing.semantic_commit_fingerprint != semantic_identity.as_str() {
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

/// Checks that each put has a matching content preparation proof.
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
