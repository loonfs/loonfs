//! Batch publication: admitted mutation candidates become one WAL segment
//! plus one head compare-and-swap, with outcomes fanned back to every
//! candidate slot.

use super::candidates::{
    prepare_candidate_request, validate_commit_content_references, BatchDedup, CandidateAdmission,
};
use super::publish_view::PublishMetadataView;
use crate::commit::{
    build_commit_plan_for_publish, materialize_commit, prepare_commit_head_publish,
    publish_commit_head, resolve_restore_content_refs_for_publish,
    wal_payload_from_materialized_commit, CommitHeadPublishError, MaterializedCommit,
    PreparedCommit, PreparedCommitHeadPublish, PublishCommitValidationContext,
};
use crate::commit_engine::NamespaceMutationCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::path::write::PublishPlanningSession;
use crate::storage::content::ContentValidationTracker;
use crate::timing::MonotonicTimer;
use crate::wal::{prepare_wal_segment, PreparedWalSegment};
use bytes::Bytes;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::wal::WalCommitPayload;
use loonfs_api::NamespaceId;
use loonfs_objectstore::{ObjectMetadata, ObjectStore};
use tracing::Instrument;

pub(crate) use crate::limits::WAL_PUBLISH_BUDGET_MS as PUBLISH_BUDGET_MS;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchAgainstViewResult {
    pub(crate) results: Vec<Result<ApiCommitResponse>>,
    pub(crate) published_records: Vec<WalCommitPayload>,
    pub(crate) resulting_head: Option<HeadState>,
    pub(crate) resulting_head_etag: Option<String>,
    pub(crate) can_reuse_loaded_projection: bool,
}

impl PublishBatchAgainstViewResult {
    fn new(results: Vec<Result<ApiCommitResponse>>) -> Self {
        Self {
            results,
            published_records: Vec::new(),
            resulting_head: None,
            resulting_head_etag: None,
            can_reuse_loaded_projection: true,
        }
    }

    fn invalidate_projection(results: Vec<Result<ApiCommitResponse>>) -> Self {
        Self {
            results,
            published_records: Vec::new(),
            resulting_head: None,
            resulting_head_etag: None,
            can_reuse_loaded_projection: false,
        }
    }

    fn published(
        results: Vec<Result<ApiCommitResponse>>,
        published_records: Vec<WalCommitPayload>,
        resulting_head: HeadState,
        resulting_head_etag: Option<String>,
    ) -> Self {
        Self {
            results,
            published_records,
            resulting_head: Some(resulting_head),
            resulting_head_etag,
            can_reuse_loaded_projection: false,
        }
    }
}

pub(crate) async fn publish_namespace_mutations_batch_against_publish_view<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: &[NamespaceMutationCandidate],
    context: &MutationContext,
    view: &PublishMetadataView<'_, S>,
    timer: &dyn MonotonicTimer,
) -> PublishBatchAgainstViewResult {
    if candidates.is_empty() {
        return PublishBatchAgainstViewResult::new(Vec::new());
    }
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    if view.head.namespace_id != *namespace_id {
        return PublishBatchAgainstViewResult::new(
            (0..candidates.len())
                .map(|_| {
                    Err(CoreError::Internal(
                        "publish view namespace mismatch".to_owned(),
                    ))
                })
                .collect(),
        );
    }
    let mut outcomes: Vec<Option<Result<ApiCommitResponse>>> =
        (0..candidates.len()).map(|_| None).collect();
    let mut session = PublishPlanningSession::new(&view.head);
    let mut accepted: Vec<(usize, MaterializedCommit)> = Vec::new();
    let mut dedup = BatchDedup::default();
    let mut content_validation = ContentValidationTracker::default();

    let prepare_span = tracing::info_span!(
        "publisher.batch_prepare",
        phase = "batch_prepare",
        batch_size,
        accepted_count = tracing::field::Empty
    );
    async {
        for (index, candidate) in candidates.iter().enumerate() {
            let admission = prepare_candidate_request(
                namespace_id,
                view,
                &session,
                candidate,
                index,
                &mut dedup,
            )
            .instrument(tracing::info_span!("loon.phase", phase = "prepare_commit"))
            .await
            .unwrap_or_else(|error| CandidateAdmission::Settled(Err(error)));
            let candidate_request = match admission {
                CandidateAdmission::Prepared(candidate_request) => candidate_request,
                CandidateAdmission::AliasOf(primary_index) => {
                    dedup.record_alias(index, primary_index);
                    continue;
                }
                CandidateAdmission::Settled(outcome) => {
                    outcomes[index] = Some(outcome);
                    continue;
                }
            };
            let validation = PublishCommitValidationContext {
                head: session.head(),
                metadata_view: view
                    .metadata_view()
                    .with_durable_cache(session.durable_cache()),
                accepted_rows: session.accepted_rows(),
            };
            let request = candidate_request.request;
            let resolved_restore_content_refs =
                match resolve_restore_content_refs_for_publish(&request, &validation).await {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                };
            if let Err(error) = validate_commit_content_references(
                store,
                &view.content_store_id,
                &request,
                &resolved_restore_content_refs,
                candidate,
                context.now_ms,
                &mut content_validation,
            )
            .await
            {
                outcomes[index] = Some(Err(error));
                continue;
            }
            let plan = {
                let span = tracing::info_span!("loon.phase", phase = "build_commit_plan");
                match build_commit_plan_for_publish(&request, context.now_ms, &validation)
                    .instrument(span)
                    .await
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        outcomes[index] = Some(Err(error));
                        continue;
                    }
                }
            };
            let prepared = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "PreparedCommit::prepare").entered();
                match PreparedCommit::prepare(
                    request,
                    plan.clone(),
                    candidate_request.identity_source,
                ) {
                    Ok(value) => value,
                    Err(error) => {
                        outcomes[index] = Some(Err(CoreError::Internal(format!(
                            "commit preparation failed: {error}"
                        ))));
                        continue;
                    }
                }
            };
            let materialized = {
                let _span =
                    tracing::info_span!("loon.phase", phase = "materialize_commit").entered();
                materialize_commit(prepared, context.now_ms)
            };
            let preview = {
                let _span = tracing::info_span!(
                    "loon.phase",
                    phase = "wal_payload_from_materialized_commit"
                )
                .entered();
                match wal_payload_from_materialized_commit(&materialized) {
                    Ok(payload) => payload,
                    Err(error) => {
                        outcomes[index] = Some(Err(error.into()));
                        continue;
                    }
                }
            };
            let applied = {
                let _span = tracing::info_span!("loon.phase", phase = "apply_committed_wal_record")
                    .entered();
                session.apply_accepted_commit(&preview, &plan)
            };
            match applied {
                Ok(()) => accepted.push((index, materialized)),
                Err(error) => outcomes[index] = Some(Err(error.into())),
            }
        }
    }
    .instrument(prepare_span.clone())
    .await;
    prepare_span.record(
        "accepted_count",
        u64::try_from(accepted.len()).unwrap_or(u64::MAX),
    );
    drop(prepare_span);

    if accepted.is_empty() {
        return PublishBatchAgainstViewResult::new(dedup.finish(outcomes));
    }
    let records = accepted
        .iter()
        .map(|(_, record)| record.clone())
        .collect::<Vec<_>>();
    let accepted_count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    let put_started_ms = timer.monotonic_now_ms();
    let wal = match write_batch_wal_segment(
        store,
        namespace_id,
        view,
        &records,
        context,
        batch_size,
        accepted_count,
    )
    .await
    {
        Ok(wal) => wal,
        Err(error) => return abort_batch(outcomes, &dedup, &accepted, &error),
    };

    let last_plan = &records
        .last()
        .expect("non-empty accepted records")
        .prepared
        .plan;
    let head_publish =
        prepare_commit_head_publish(&view.head, last_plan, &wal, &context.writer_version);
    let head_publish = match head_publish {
        Ok(value) => value,
        Err(error) => {
            let error = CoreError::Internal(format!("head publish preparation failed: {error}"));
            return abort_batch(outcomes, &dedup, &accepted, &error);
        }
    };
    let elapsed_ms = timer.monotonic_now_ms().saturating_sub(put_started_ms);
    if elapsed_ms > PUBLISH_BUDGET_MS {
        let error = CoreError::HeadPublish(CommitHeadPublishError::PublishBudgetExceeded {
            elapsed_ms,
            budget_ms: PUBLISH_BUDGET_MS,
        });
        return abort_batch(outcomes, &dedup, &accepted, &error);
    }
    let resulting_head_etag = match cas_batch_head(
        store,
        &view.head_etag,
        &head_publish,
        batch_size,
        accepted_count,
    )
    .await
    {
        Ok(metadata) => metadata.etag,
        Err(error) => return abort_batch(outcomes, &dedup, &accepted, &error),
    };

    let published_records = wal.envelope.payload.records.clone();
    for (accepted_index, (outcome_index, record)) in accepted.into_iter().enumerate() {
        outcomes[outcome_index] = Some(Ok(ApiCommitResponse {
            namespace_id: namespace_id.clone(),
            commit_id: record.prepared.request.commit_id,
            committed_seq: published_records[accepted_index].seq,
        }));
    }
    let results = dedup.finish(outcomes);
    PublishBatchAgainstViewResult::published(
        results,
        published_records,
        head_publish.resulting_head,
        resulting_head_etag,
    )
}

/// Aborts a batch whose accepted candidates never became durable.
///
/// Fails every outcome contingent on the unpublished batch, then finalizes
/// the remaining slots and invalidates the caller's loaded projection.
fn abort_batch(
    mut outcomes: Vec<Option<Result<ApiCommitResponse>>>,
    dedup: &BatchDedup,
    accepted: &[(usize, MaterializedCommit)],
    error: &CoreError,
) -> PublishBatchAgainstViewResult {
    fail_outcomes_contingent_on_unpublished_batch(&mut outcomes, accepted, error);
    PublishBatchAgainstViewResult::invalidate_projection(dedup.finish(outcomes))
}

/// Writes the accepted records as one durable WAL segment.
async fn write_batch_wal_segment<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    view: &PublishMetadataView<'_, S>,
    records: &[MaterializedCommit],
    context: &MutationContext,
    batch_size: u64,
    accepted_count: u64,
) -> Result<PreparedWalSegment> {
    let span = tracing::info_span!(
        "publisher.batch_write_wal",
        phase = "batch_write_wal",
        batch_size,
        accepted_count,
        wal_segment_count = 1_u64,
        key_class = "wal_segment",
        result = tracing::field::Empty
    );
    let result = async {
        let wal = prepare_wal_segment(
            namespace_id.clone(),
            view.acquired_writer
                .as_ref()
                .expect("publish view should carry acquired writer")
                .writer_epoch,
            view.head.visible_wal_tip.clone(),
            records,
            &context.writer_version,
        )
        .map_err(|error| CoreError::Internal(format!("wal build failed: {error}")))?;
        store
            .put_if_absent(&wal.object_key, Bytes::copy_from_slice(&wal.encoded_bytes))
            .await
            .map_err(|error| CoreError::WalWrite {
                object_key: wal.object_key.clone(),
                message: error.message(),
            })?;
        Ok(wal)
    }
    .instrument(span.clone())
    .await;
    span.record("result", if result.is_ok() { "ok" } else { "error" });
    result
}

/// Advances the namespace head past the new WAL segment by compare-and-swap.
async fn cas_batch_head<S: ObjectStore + ?Sized>(
    store: &S,
    head_etag: &str,
    head_publish: &PreparedCommitHeadPublish,
    batch_size: u64,
    accepted_count: u64,
) -> Result<ObjectMetadata> {
    let span = tracing::info_span!(
        "publisher.batch_cas_head",
        phase = "batch_cas_head",
        batch_size,
        accepted_count,
        key_class = "wal_head",
        result = tracing::field::Empty
    );
    let result = publish_commit_head(store, head_etag, head_publish)
        .instrument(span.clone())
        .await;
    span.record("result", if result.is_ok() { "ok" } else { "error" });
    result.map_err(CoreError::from)
}

/// Fails every outcome that was contingent on this batch publishing durably.
///
/// The accepted candidates take the batch error: they never committed. So do
/// rejections recorded after the first acceptance, because their verdicts
/// were decided against session state advanced by tentatively accepted
/// candidates — state that never became durable. Reporting them would hand a
/// client a definitive semantic error (path conflict, missing path, stale
/// revision, ...) it correctly treats as non-retryable, for a precondition
/// that was never durably true (format.md section 3.1.5).
///
/// Rejections recorded before any acceptance were decided against the loaded
/// durable publish view and stand. Idempotent `Ok` completions replay durable
/// commit receipts and stand. Alias slots stay unfilled here and inherit
/// their primary's final outcome.
fn fail_outcomes_contingent_on_unpublished_batch(
    outcomes: &mut [Option<Result<ApiCommitResponse>>],
    accepted: &[(usize, MaterializedCommit)],
    error: &CoreError,
) {
    let Some(first_accepted_index) = accepted.first().map(|(index, _)| *index) else {
        return;
    };
    for (index, _) in accepted {
        outcomes[*index] = Some(Err(error.clone()));
    }
    for outcome in outcomes.iter_mut().skip(first_accepted_index + 1) {
        if matches!(outcome, Some(Err(_))) {
            *outcome = Some(Err(error.clone()));
        }
    }
}
