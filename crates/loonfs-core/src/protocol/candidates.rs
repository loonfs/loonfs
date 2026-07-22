//! Per-candidate admission for publish batches: request conversion,
//! commit-id validation, and duplicate resolution against durable receipts
//! and same-batch primaries.

use super::publish_view::PublishMetadataView;
use crate::commit::{
    core_commit_fingerprint, CommitExecutionContext, CommitIdentitySource,
    CommitRequest as CoreCommitRequest, SemanticMutationIdentity,
};
use crate::commit_engine::{
    ContentPreparation, ContentPreparationError, NamespaceMutation, NamespaceMutationCandidate,
};
use crate::error::{CoreError, Result};
use crate::metadata::CommitReceiptRecord;
use crate::path::write::{path_intent_fingerprint_for_path_intent, PublishPlanningSession};
use crate::storage::content_admission::ContentAdmission;
use loonfs_api::v0::{CommitOp, CommitResponse as ApiCommitResponse};
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
    candidate.validate_request_limits()?;
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
    match candidate.mutation() {
        NamespaceMutation::Commit(request) => {
            validate_commit_id(&request.commit_id)?;
            let request = CoreCommitRequest::from_v0(conversion_context, request.clone());
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
            reject_failed_content_preparation(candidate)?;
            Ok(CandidateAdmission::Prepared(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::CoreCommitRequest,
            }))
        }
        NamespaceMutation::Path(intent) => {
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
            reject_failed_content_preparation(candidate)?;
            let planned = session
                .plan_path_mutation(namespace_id, intent, view.metadata_view())
                .await?;
            let request = CoreCommitRequest::from_v0(conversion_context, planned.commit_request);
            Ok(CandidateAdmission::Prepared(CandidateCoreRequest {
                request,
                identity_source: CommitIdentitySource::PathIntent(planned.path_intent_fingerprint),
            }))
        }
    }
}

fn reject_failed_content_preparation(candidate: &NamespaceMutationCandidate) -> Result<()> {
    match candidate.content_preparation() {
        ContentPreparation::Ready(_) => Ok(()),
        ContentPreparation::Rejected(error) => Err(error.clone().into()),
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
pub(super) fn validate_commit_content_references(
    request: &CoreCommitRequest,
    candidate: &NamespaceMutationCandidate,
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
    match candidate.mutation() {
        NamespaceMutation::Commit(_) => {
            for content_ref in request.ops.iter().filter_map(|op| match op {
                CommitOp::CreateFile { content_ref, .. }
                | CommitOp::ReplaceFile { content_ref, .. } => Some(content_ref),
                // Restore content is resolved from retained namespace metadata,
                // whose durability is already guaranteed.
                _ => None,
            }) {
                require_content_admission(&admissions, content_ref)?;
            }
        }
        NamespaceMutation::Path(crate::path::write::PathMutationIntent::PutFile {
            content_ref,
            ..
        }) => require_content_admission(&admissions, content_ref)?,
        NamespaceMutation::Path(_) => return Ok(()),
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
        content_ref_digest: content_ref.digest.clone(),
    }
    .into())
}
