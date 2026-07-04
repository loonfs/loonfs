//! Per-candidate admission for publish batches: request conversion,
//! commit-id validation, and duplicate resolution against durable receipts
//! and same-batch primaries.

use super::publish_view::PublishMetadataView;
use crate::commit::{
    commit_request_from_v0, core_commit_fingerprint, CommitExecutionContext, CommitIdentitySource,
    CommitOp, CommitRequest as CoreCommitRequest, SemanticMutationIdentity,
};
use crate::commit_engine::NamespaceMutationCandidate;
use crate::content::ContentAdmission;
use crate::error::{CoreError, Result};
use crate::metadata::CommitReceiptRecord;
use crate::path::write::{path_intent_fingerprint_for_path_intent, PublishPlanningSession};
use crate::storage::content::ContentValidationTracker;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::{CommitId, ContentRef, ContentStoreId, NamespaceId};
use loonfs_objectstore::ObjectStore;
use std::collections::HashMap;

pub(super) struct CandidateCoreRequest {
    pub(super) request: CoreCommitRequest,
    pub(super) identity_source: CommitIdentitySource,
}

/// How one batch candidate resolved during admission.
pub(super) enum CandidateAdmission {
    /// A new primary request, ready for validation and materialization.
    Prepared(CandidateCoreRequest),
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
    semantic_identity: SemanticMutationIdentity,
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
        semantic_identity: &SemanticMutationIdentity,
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
            return Some(CandidateAdmission::Settled(Err(
                CoreError::CommitIdReuseConflict(commit_id.to_string()),
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

/// Converts one batch candidate into a core commit request, resolving
/// commit-id reuse against durable receipts and same-batch primaries.
///
/// Hard failures return `Err`; the caller settles the candidate's outcome
/// slot with them exactly as with [`CandidateAdmission::Settled`].
pub(super) async fn prepare_candidate_request<S: ObjectStore + ?Sized>(
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    session: &PublishPlanningSession,
    candidate: &NamespaceMutationCandidate,
    index: usize,
    dedup: &mut BatchDedup,
) -> Result<CandidateAdmission> {
    let acquired_writer = view
        .acquired_writer
        .as_ref()
        .expect("publish view should carry acquired writer");
    let conversion_context = CommitExecutionContext {
        namespace_id: namespace_id.clone(),
        writer_id: acquired_writer.writer_id.clone(),
        writer_session_id: acquired_writer.writer_session_id.clone(),
        writer_epoch: acquired_writer.writer_epoch,
    };
    match candidate {
        NamespaceMutationCandidate::Commit(request) => {
            validate_commit_id(&request.commit_id)?;
            let request = commit_request_from_v0(conversion_context, request.clone())
                .map_err(CoreError::from)?;
            let semantic_identity = core_commit_fingerprint(&request).map_err(|error| {
                CoreError::Internal(format!("failed to fingerprint commit request: {error}"))
            })?;
            let semantic_identity = SemanticMutationIdentity::CoreCommit(semantic_identity);
            if let Some(admission) = resolve_commit_id_reuse(
                namespace_id,
                view,
                dedup,
                index,
                &request.commit_id,
                &semantic_identity,
            )
            .await?
            {
                return Ok(admission);
            }
            Ok(CandidateAdmission::Prepared(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::CoreCommitRequest,
            }))
        }
        NamespaceMutationCandidate::Path(intent)
        | NamespaceMutationCandidate::PathWithContentAdmission { intent, .. } => {
            validate_commit_id(intent.commit_id())?;
            let path_intent_fingerprint =
                path_intent_fingerprint_for_path_intent(namespace_id, intent)?;
            let semantic_identity =
                SemanticMutationIdentity::PathIntent(path_intent_fingerprint.clone());
            if let Some(admission) = resolve_commit_id_reuse(
                namespace_id,
                view,
                dedup,
                index,
                intent.commit_id(),
                &semantic_identity,
            )
            .await?
            {
                return Ok(admission);
            }
            let planned = session
                .plan_path_mutation(namespace_id, intent, view.metadata_view())
                .await?;
            let request = commit_request_from_v0(conversion_context, planned.commit_request)
                .map_err(CoreError::from)?;
            Ok(CandidateAdmission::Prepared(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::PathIntent(planned.path_intent_fingerprint),
            }))
        }
    }
}

fn validate_commit_id(commit_id: &CommitId) -> Result<()> {
    CommitId::parse(commit_id.as_str())
        .map(|_| ())
        .map_err(CoreError::InvalidCommitId)
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
    semantic_identity: &SemanticMutationIdentity,
) -> Result<Option<CandidateAdmission>> {
    if let Some(existing) = view.find_commit_receipt(commit_id).await? {
        return Ok(Some(CandidateAdmission::Settled(
            if existing.semantic_commit_fingerprint != semantic_identity.as_str() {
                Err(CoreError::CommitIdReuseConflict(commit_id.to_string()))
            } else {
                Ok(commit_response_from_commit_receipt(namespace_id, &existing))
            },
        )));
    }
    Ok(dedup.admit(index, commit_id, semantic_identity))
}

fn commit_response_from_commit_receipt(
    namespace_id: &NamespaceId,
    record: &CommitReceiptRecord,
) -> ApiCommitResponse {
    ApiCommitResponse {
        namespace_id: namespace_id.clone(),
        commit_id: record.commit_id.clone(),
        committed_seq: record.committed_seq,
    }
}

struct CommitContentAdmissions<'a> {
    namespace_id: &'a NamespaceId,
    admissions: &'a [ContentAdmission],
    now_ms: u64,
}

impl CommitContentAdmissions<'_> {
    fn admits(&self, content_ref: &ContentRef) -> bool {
        self.admissions
            .iter()
            .any(|admission| admission.admits(self.namespace_id, content_ref, self.now_ms))
    }
}

/// Validates that every content ref a candidate commits either was admitted
/// by the request's presigned content admissions or is provably durable.
pub(super) async fn validate_commit_content_references<S: ObjectStore + ?Sized>(
    store: &S,
    content_store_id: &ContentStoreId,
    request: &CoreCommitRequest,
    resolved_restore_content_refs: &[Option<ContentRef>],
    candidate: &NamespaceMutationCandidate,
    now_ms: u64,
    content_validation: &mut ContentValidationTracker,
) -> Result<()> {
    let admissions = CommitContentAdmissions {
        namespace_id: &request.namespace_id,
        admissions: candidate_content_admissions(candidate),
        now_ms,
    };
    let mut content_refs = Vec::new();
    for (index, op) in request.ops.iter().enumerate() {
        match op {
            CommitOp::CreateFile { content_ref, .. }
            | CommitOp::ReplaceFile { content_ref, .. } => {
                content_refs.push(content_ref);
            }
            CommitOp::RestoreRevision { .. } => {
                if let Some(content_ref) = resolved_restore_content_refs
                    .get(index)
                    .and_then(|content_ref| content_ref.as_ref())
                {
                    content_refs.push(content_ref);
                }
            }
            _ => {}
        }
    }

    if content_refs.is_empty() {
        return Ok(());
    }

    for content_ref in content_refs {
        if admissions.admits(content_ref) {
            continue;
        }
        content_validation
            .ensure_validated(store, content_store_id, content_ref)
            .await?;
    }

    Ok(())
}

fn candidate_content_admissions(candidate: &NamespaceMutationCandidate) -> &[ContentAdmission] {
    match candidate {
        NamespaceMutationCandidate::PathWithContentAdmission { admissions, .. } => admissions,
        NamespaceMutationCandidate::Commit(_) | NamespaceMutationCandidate::Path(_) => &[],
    }
}
