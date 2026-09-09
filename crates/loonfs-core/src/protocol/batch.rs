//! Batch publication: admitted mutation candidates become one WAL segment
//! plus one numbered WAL put, with outcomes fanned back to every
//! candidate slot.

use super::candidates::{
    prepare_candidate_request, validate_candidate_content_references, BatchDedup,
    CandidateAdmission,
};
use super::changes::committed_change_from_wal_record;
use super::publish_view::PublishMetadataView;
use crate::commit::{
    materialize_commit, publish_wal, wal_payload_from_materialized_commit, CommitHeadPublishError,
};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::WAL_PUBLISH_BUDGET_MS;
use crate::namespace::state::NamespaceReadState;
use crate::path::write::PublishPlanningSession;
use crate::time::MonotonicTimer;
use crate::wal::prepare_wal_segment;
use loonfs_api::v0::CommitResponse as ApiCommitResponse;
use loonfs_api::wire::wal::WalCommitPayload;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;
use tracing::Instrument;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchAgainstViewResult {
    pub(crate) results: Vec<Result<ApiCommitResponse>>,
    pub(crate) effect: PublishViewEffect,
}

/// What one batch did to the publish view it ran against — the whole of what
/// a caller needs to decide the fate of the projection it loaded.
#[derive(Debug, Clone)]
#[allow(
    clippy::large_enum_variant,
    reason = "one value per published batch, moved once: indirection would buy an allocation and save nothing"
)]
pub(crate) enum PublishViewEffect {
    /// Nothing was written: the loaded projection still describes the tail.
    Unchanged,
    /// The batch may have left durable state the loaded projection does not
    /// account for, so that projection must be dropped.
    Invalidated,
    Advanced {
        records: Vec<WalCommitPayload>,
        head: NamespaceReadState,
        head_etag: String,
    },
}

impl PublishBatchAgainstViewResult {
    fn unchanged(results: Vec<Result<ApiCommitResponse>>) -> Self {
        Self {
            results,
            effect: PublishViewEffect::Unchanged,
        }
    }
}

/// Tracks one candidate's result and whether publication can change it.
#[derive(Debug, Clone)]
pub(super) enum BatchOutcomeSlot {
    Accepted,
    Settled {
        outcome: Result<ApiCommitResponse>,
        depends_on_batch: bool,
    },
    AliasOf(usize),
}

impl BatchOutcomeSlot {
    fn settled(&self) -> Option<&Result<ApiCommitResponse>> {
        match self {
            Self::Settled { outcome, .. } => Some(outcome),
            Self::Accepted | Self::AliasOf(_) => None,
        }
    }
}

pub(crate) struct PublicationClock<'a> {
    pub(crate) timer: &'a dyn MonotonicTimer,
    pub(crate) attempt_started_ms: u64,
    pub(crate) tip_observed_ms: u64,
}

pub(crate) async fn publish_namespace_commits_batch_against_publish_view<
    S: ObjectStore + ?Sized,
>(
    store: &S,
    namespace_id: &NamespaceId,
    candidates: &[CommitCandidate],
    context: &MutationContext,
    view: &PublishMetadataView<'_, S>,
    clock: PublicationClock<'_>,
) -> PublishBatchAgainstViewResult {
    if candidates.is_empty() {
        return PublishBatchAgainstViewResult::unchanged(Vec::new());
    }
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    if view.head.namespace_id != *namespace_id {
        return PublishBatchAgainstViewResult::unchanged(vec![
            Err(CoreError::Internal(
                "publish view namespace mismatch".to_owned()
            ));
            candidates.len()
        ]);
    }
    let mut slots = Vec::with_capacity(candidates.len());
    let mut session = PublishPlanningSession::new(&view.head);
    let mut accepted_commits = Vec::new();
    let mut dedup = BatchDedup::default();

    let prepare_span = tracing::debug_span!(
        "loonfs.phase",
        phase = "prepare_batch",
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
                context.now_ms,
                &mut dedup,
            )
            .instrument(tracing::debug_span!(
                "loonfs.phase",
                phase = "prepare_commit"
            ))
            .await;
            let candidate_request = match admission {
                CandidateAdmission::Prepared(candidate_request) => candidate_request,
                CandidateAdmission::Settled(slot) => {
                    slots.push(settle_admission(slot, !accepted_commits.is_empty()));
                    continue;
                }
            };
            let validated = candidate_request.validated;
            let allocation = candidate_request.allocation;
            let resulting_next_inode_id = match session.commit_candidate(allocation) {
                Ok(resulting_next_inode_id) => resulting_next_inode_id,
                Err(error) => {
                    slots.push(settle_admission(
                        BatchOutcomeSlot::Settled {
                            outcome: Err(error),
                            depends_on_batch: true,
                        },
                        !accepted_commits.is_empty(),
                    ));
                    continue;
                }
            };
            let plan = {
                let _span =
                    tracing::debug_span!("loonfs.phase", phase = "finish_commit_plan").entered();
                validated.finish(resulting_next_inode_id)
            };
            let materialized = {
                let _span =
                    tracing::debug_span!("loonfs.phase", phase = "materialize_commit").entered();
                materialize_commit(plan, context.now_ms)
            };
            let preview = {
                let _span =
                    tracing::debug_span!("loonfs.phase", phase = "build_wal_payload").entered();
                wal_payload_from_materialized_commit(&materialized)
            };
            {
                let _span =
                    tracing::debug_span!("loonfs.phase", phase = "apply_committed_wal_record")
                        .entered();
                session.apply_accepted_commit(&preview, &materialized.commit);
            }
            slots.push(BatchOutcomeSlot::Accepted);
            accepted_commits.push(materialized);
        }
    }
    .instrument(prepare_span.clone())
    .await;
    prepare_span.record(
        "accepted_count",
        u64::try_from(accepted_commits.len()).unwrap_or(u64::MAX),
    );
    drop(prepare_span);

    if accepted_commits.is_empty() {
        return PublishBatchAgainstViewResult::unchanged(finish_batch_outcomes(&slots));
    }
    let wal_no = match view.head.wal_no.successor() {
        Ok(number) => number,
        Err(error) => {
            return abort_batch(slots, &CoreError::Internal(format!("WAL number {error}")))
        }
    };
    let wal = match prepare_wal_segment(
        namespace_id.clone(),
        view.acquired_writer.writer_epoch,
        wal_no,
        &accepted_commits,
    ) {
        Ok(wal) => wal,
        Err(error) => {
            return abort_batch(
                slots,
                &CoreError::Internal(format!("WAL build failed: {error}")),
            )
        }
    };
    let last_plan = &accepted_commits
        .last()
        .expect("accepted commits should be nonempty")
        .commit;
    let resulting_head = NamespaceReadState {
        seq: wal.envelope().payload().end_seq,
        head_commit_id: last_plan.commit_id.clone(),
        next_inode_id: wal.envelope().payload().next_inode_id,
        wal_no,
        ..view.head.clone()
    };
    let now_ms = clock.timer.monotonic_now_ms();
    let elapsed_ms = now_ms.saturating_sub(clock.attempt_started_ms);
    let Some(publication_now_ms) = context.now_ms.checked_add(elapsed_ms) else {
        return abort_batch(
            slots,
            &CoreError::Internal("publication time overflow".to_owned()),
        );
    };
    for (candidate, slot) in candidates.iter().zip(&slots) {
        if matches!(slot, BatchOutcomeSlot::Accepted) {
            if let Err(error) = validate_candidate_content_references(
                candidate,
                namespace_id,
                view.content_store_id(),
                publication_now_ms,
            ) {
                return abort_batch(slots, &error);
            }
        }
    }
    let tip_age_ms = now_ms.saturating_sub(clock.tip_observed_ms);
    if tip_age_ms > WAL_PUBLISH_BUDGET_MS {
        return abort_batch(
            slots,
            &CoreError::HeadPublish(CommitHeadPublishError::PublishBudgetExceeded {
                elapsed_ms: tip_age_ms,
                budget_ms: WAL_PUBLISH_BUDGET_MS,
            }),
        );
    }
    if let Err(error) = publish_wal(store, &wal).await {
        return abort_batch(slots, &error);
    }
    let head_etag = view.head_etag.clone();

    let wal_records = wal.envelope().payload().records.clone();
    assert_eq!(
        slots
            .iter()
            .filter(|slot| matches!(slot, BatchOutcomeSlot::Accepted))
            .count(),
        wal_records.len(),
        "accepted slot count should match WAL record count"
    );
    let mut records = wal_records.iter();
    for slot in &mut slots {
        if matches!(slot, BatchOutcomeSlot::Accepted) {
            let record = records
                .next()
                .expect("accepted slot count should match WAL record count");
            *slot = BatchOutcomeSlot::Settled {
                outcome: committed_change_from_wal_record(namespace_id, record).map(|change| {
                    ApiCommitResponse::from_committed_change(namespace_id.clone(), change)
                }),
                depends_on_batch: false,
            };
        }
    }
    PublishBatchAgainstViewResult {
        results: finish_batch_outcomes(&slots),
        effect: PublishViewEffect::Advanced {
            records: wal_records,
            head: resulting_head,
            head_etag,
        },
    }
}

/// Applies the publication error and invalidates the loaded projection.
fn abort_batch(
    mut slots: Vec<BatchOutcomeSlot>,
    error: &CoreError,
) -> PublishBatchAgainstViewResult {
    fail_unpublished_slots(&mut slots, error);
    PublishBatchAgainstViewResult {
        results: finish_batch_outcomes(&slots),
        effect: PublishViewEffect::Invalidated,
    }
}

/// Before the first accepted commit, admission uses durable state only.
fn settle_admission(slot: BatchOutcomeSlot, has_accepted_commits: bool) -> BatchOutcomeSlot {
    match slot {
        BatchOutcomeSlot::Settled {
            outcome,
            depends_on_batch,
        } => BatchOutcomeSlot::Settled {
            outcome,
            depends_on_batch: depends_on_batch && has_accepted_commits,
        },
        slot => slot,
    }
}

/// Replaces results that depend on unpublished changes with the publication error.
fn fail_unpublished_slots(slots: &mut [BatchOutcomeSlot], error: &CoreError) {
    for slot in slots {
        if matches!(slot, BatchOutcomeSlot::Accepted)
            || matches!(
                slot,
                BatchOutcomeSlot::Settled {
                    depends_on_batch: true,
                    ..
                }
            )
        {
            *slot = BatchOutcomeSlot::Settled {
                outcome: Err(error.clone()),
                depends_on_batch: false,
            };
        }
    }
}

/// Resolves aliases and rejects any slot that did not settle.
fn finish_batch_outcomes(slots: &[BatchOutcomeSlot]) -> Vec<Result<ApiCommitResponse>> {
    slots
        .iter()
        .map(|slot| {
            match slot {
                BatchOutcomeSlot::AliasOf(primary_index) => slots
                    .get(*primary_index)
                    .and_then(BatchOutcomeSlot::settled),
                slot => slot.settled(),
            }
            .cloned()
            .expect("batch outcome should be settled")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::bootstrap::bootstrap_namespace;
    use crate::namespace::writer_epoch::acquire_writer_epoch;
    use crate::path::write::{CommitRequest, FilesystemOperation};
    use crate::protocol::{load_publish_metadata_view, PublishTailOptions};
    use crate::time::StdMonotonicTimer;
    use loonfs_api::{AbsolutePath, ChangeSeq, CommitId, MAX_PUBLIC_INTEGER};
    use loonfs_objectstore::keys::{hint, wal_segment_prefix};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn sequence_exhaustion_writes_neither_wal_nor_head() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = MutationContext {
            writer_id: loonfs_api::WriterId::parse("writer").expect("writer id"),
            now_ms: 1_000,
        };
        bootstrap_namespace(&store, &namespace_id, &context, false)
            .await
            .expect("bootstrap namespace");
        let acquired_writer = acquire_writer_epoch(&store, &namespace_id, &context)
            .await
            .expect("acquire writer");
        let (mut view, _projection) = load_publish_metadata_view(
            &store,
            None,
            &namespace_id,
            acquired_writer,
            None,
            &PublishTailOptions::default(),
        )
        .await
        .expect("load publish view");

        let head_key = hint(&namespace_id);
        let head_before = store
            .get(&head_key, None)
            .await
            .expect("read head")
            .expect("head exists");
        let wal_before = store
            .list_prefix(&wal_segment_prefix(&namespace_id))
            .await
            .expect("list WAL before");

        // Change only the in-memory head so this exercises the sequence limit
        // without modifying the stored metadata.
        view.head.seq = ChangeSeq(MAX_PUBLIC_INTEGER);
        let candidate = CommitCandidate::new(CommitRequest::single(
            CommitId::parse("past-sequence-limit").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/blocked").expect("path"),
                parents: false,
            },
        ));
        let result = publish_namespace_commits_batch_against_publish_view(
            &store,
            &namespace_id,
            &[candidate],
            &context,
            &view,
            PublicationClock {
                timer: &StdMonotonicTimer::default(),
                attempt_started_ms: 0,
                tip_observed_ms: 0,
            },
        )
        .await;

        let error = result.results[0]
            .as_ref()
            .expect_err("exhausted sequence must reject the commit");
        assert_eq!(error.code(), loonfs_api::ErrorCode::ServerError);
        assert!(error.to_string().contains("cannot exceed"));
        assert!(matches!(result.effect, PublishViewEffect::Unchanged));
        assert_eq!(
            store
                .get(&head_key, None)
                .await
                .expect("read head after")
                .expect("head exists"),
            head_before
        );
        assert_eq!(
            store
                .list_prefix(&wal_segment_prefix(&namespace_id))
                .await
                .expect("list WAL after"),
            wal_before
        );
    }
}
