//! Per-candidate admission for publish batches: single-pass commit
//! preparation, commit-id validation, and duplicate resolution against
//! durable receipts and same-batch primaries.

use super::batch::BatchOutcomeSlot;
use super::publish_view::PublishMetadataView;
use crate::authorize::Authorizer;
use crate::commit::{CandidateAllocation, CommitFingerprint, ValidatedCommitPlan};
use crate::commit_engine::{CommitCandidate, ContentPreparation, ContentPreparationError};
use crate::error::{CoreError, MetadataViewError, Result};
use crate::limits::MAX_UNFLUSHED_WAL_SEGMENTS;
use crate::metadata::CommitReceiptRecord;
use crate::path::write::{CommitRequest, FilesystemOperation, PublishPlanningSession};
use crate::storage::content_admission::PreparedContent;
use crate::storage::inline_content::InlineContent;
use loonfs_api::v0::Commit;
use loonfs_api::{CommitId, ContentId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, HashSet};

pub(super) struct PreparedCandidateCommit {
    pub(super) validated: ValidatedCommitPlan,
    pub(super) allocation: CandidateAllocation,
}

/// Keeps the commit timestamp fixed while content evidence ages across retries.
pub(super) struct CandidateTime {
    pub(super) committed_at_ms: u64,
    pub(super) admission_now_ms: u64,
}

/// How one batch candidate resolved during admission.
pub(super) enum CandidateAdmission {
    /// A new request ready for content validation and materialization.
    Prepared(PreparedCandidateCommit),
    /// A result decided during admission.
    Settled(BatchOutcomeSlot),
}

impl CandidateAdmission {
    fn independent(outcome: Result<Commit>) -> Self {
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
    time: CandidateTime,
    dedup: &mut BatchDedup,
) -> CandidateAdmission {
    if let Err(error) =
        Authorizer::for_request(namespace_id, &view.head.access, candidate.authority())
    {
        return CandidateAdmission::independent(Err(error));
    }
    if let Err(error) = candidate.validate_request_has_operations() {
        return CandidateAdmission::independent(Err(error));
    }
    let mutation = candidate.request();
    let semantic_identity = match candidate.semantic_identity(namespace_id) {
        Ok(semantic_identity) => semantic_identity,
        Err(error) => return CandidateAdmission::independent(Err(error)),
    };
    match resolve_commit_id_reuse(view, dedup, index, &mutation.commit_id, &semantic_identity).await
    {
        Ok(Some(admission)) => return admission,
        Ok(None) => {}
        Err(error) => return CandidateAdmission::independent(Err(error)),
    }
    if let Some(wal_tail_segments) = view.write_stop() {
        return CandidateAdmission::independent(Err(MetadataViewError::MaintenanceRequired {
            namespace_id: namespace_id.clone(),
            reason: format!(
                "WAL tail has {wal_tail_segments} segments; publishes resume once maintenance brings it back under {MAX_UNFLUSHED_WAL_SEGMENTS}"
            ),
        }
        .into()));
    }
    if let Err(error) = candidate.validate_request_limits() {
        return CandidateAdmission::independent(Err(error));
    }
    if let Err(error) =
        validate_candidate_content_references(candidate, namespace_id, time.admission_now_ms)
    {
        return CandidateAdmission::independent(Err(error));
    }
    let mut allocation = session.begin_candidate();
    match session
        .prepare_commit(
            candidate,
            semantic_identity,
            view.metadata_view(),
            time.committed_at_ms,
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

pub(super) fn validate_candidate_content_references(
    candidate: &CommitCandidate,
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<()> {
    match candidate.content_preparation() {
        ContentPreparation::Ready(admissions) => validate_commit_content_references(
            candidate.request(),
            admissions,
            candidate.inline_content(),
            namespace_id,
            now_ms,
        ),
        ContentPreparation::Rejected(error) => Err(error.clone().into()),
    }
}

/// Resolves a commit ID against durable receipts and earlier requests in the batch.
async fn resolve_commit_id_reuse<S: ObjectStore + ?Sized>(
    view: &PublishMetadataView<'_, S>,
    dedup: &mut BatchDedup,
    index: usize,
    commit_id: &CommitId,
    semantic_identity: &CommitFingerprint,
) -> Result<Option<CandidateAdmission>> {
    if let Some(existing) = view.find_commit_receipt(commit_id).await? {
        return Ok(Some(CandidateAdmission::independent(
            commit_response_from_commit_receipt(view, &existing, semantic_identity).await,
        )));
    }
    Ok(dedup.admit(index, commit_id, semantic_identity))
}

/// Builds a replay response from the commit receipt and its retained commit row.
async fn commit_response_from_commit_receipt<S: ObjectStore + ?Sized>(
    view: &PublishMetadataView<'_, S>,
    record: &CommitReceiptRecord,
    semantic_identity: &CommitFingerprint,
) -> Result<Commit> {
    // Commits and receipts share a family group and floor rule, so the commit
    // remains while its receipt does.
    let commit = match view
        .metadata_view()
        .commit_at_seq(record.committed_seq)
        .await
    {
        Ok(Some(commit)) => commit,
        Ok(None) => {
            return Err(CoreError::Internal(format!(
            "commit receipt for `{}` names sequence `{}`, where the change feed reports no commit",
            record.commit_id, record.committed_seq
        )))
        }
        Err(error) => return Err(error),
    };
    if commit.commit_id != record.commit_id {
        return Err(CoreError::Internal(format!(
            "commit receipt for `{}` names sequence `{}`, where the change feed reports commit `{}`",
            record.commit_id, record.committed_seq, commit.commit_id
        )));
    }
    if commit.semantic_commit_fingerprint != *semantic_identity {
        return Err(CoreError::CommitIdReuseConflict {
            commit_id: record.commit_id.to_string(),
            committed_seq: Some(record.committed_seq),
            committed_fingerprint: Some(commit.semantic_commit_fingerprint.as_str().to_owned()),
        });
    }
    super::changes::committed_change_from_wal_record(&view.head.namespace_id, &commit)
}

pub(crate) fn validate_inline_content_references<'a>(
    request: &CommitRequest,
    inline_content: &'a [InlineContent],
    namespace_id: &NamespaceId,
) -> Result<HashMap<&'a ContentId, &'a loonfs_api::ContentRef>> {
    let references: HashSet<_> = request
        .operations
        .iter()
        .filter_map(FilesystemOperation::content_ref)
        .collect();
    let mut inline_by_content_id = HashMap::new();
    for value in inline_content {
        let reference = value.content_ref();
        let content_id = &reference.content_id;
        if !references.contains(reference) {
            return Err(CoreError::InvalidCommitRequest(format!(
                "inline content `{content_id}` is not referenced by an operation"
            )));
        }
        if inline_by_content_id
            .insert(&reference.content_id, reference)
            .is_some()
        {
            return Err(CoreError::InvalidCommitRequest(format!(
                "inline content `{content_id}` appears more than once"
            )));
        }
        if reference.owner_namespace_id != *namespace_id {
            return Err(CoreError::InvalidCommitRequest(format!(
                "inline content `{content_id}` is not owned by the committing namespace"
            )));
        }
    }
    for reference in &references {
        if inline_by_content_id
            .get(&reference.content_id)
            .is_some_and(|inline| *inline != *reference)
        {
            return Err(CoreError::InvalidCommitRequest(format!(
                "inline content `{}` does not match every reference",
                reference.content_id
            )));
        }
    }
    Ok(inline_by_content_id)
}

/// Checks that each put's content is covered by a staged proof or an equal inline value.
fn validate_commit_content_references(
    request: &CommitRequest,
    admissions: &[PreparedContent],
    inline_content: &[InlineContent],
    namespace_id: &NamespaceId,
    now_ms: u64,
) -> Result<()> {
    let inline_by_content_id =
        validate_inline_content_references(request, inline_content, namespace_id)?;
    let mut admissions_by_content_id: HashMap<&ContentId, Vec<&PreparedContent>> =
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
        let content_id = &content_ref.content_id;
        if inline_by_content_id.contains_key(content_id) {
            continue;
        }
        let admitted = admissions_by_content_id
            .get(&content_ref.content_id)
            .is_some_and(|candidates| {
                candidates
                    .iter()
                    .any(|admission| admission.admits(namespace_id, content_ref, now_ms))
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
