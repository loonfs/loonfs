//! Batch publication: admitted mutation candidates become one WAL segment
//! plus one numbered WAL put, with outcomes fanned back to every
//! candidate slot.

use super::candidates::{
    prepare_candidate_request, validate_candidate_content_references, BatchDedup,
    CandidateAdmission, CandidateTime,
};
use super::changes::committed_change_from_wal_record;
use super::publish_view::PublishMetadataView;
use crate::commit::{materialize_commit, wal_payload_from_materialized_commit, WalPublishError};
use crate::commit_engine::CommitCandidate;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::PublishPlanningSession;
use crate::storage::inline_content::InlineContent;
use crate::time::{Deadline, Observation};
use crate::wal::{prepare_segment, publish_segment};
use loonfs_api::v0::Commit;
use loonfs_api::wire::wal::WalCommitPayload;
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;
use tracing::Instrument;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchAgainstViewResult {
    pub(crate) results: Vec<Result<Commit>>,
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
        inline_content: Vec<InlineContent>,
        head: NamespaceReadState,
    },
}

impl PublishBatchAgainstViewResult {
    fn unchanged(results: Vec<Result<Commit>>) -> Self {
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
        outcome: Result<Commit>,
        depends_on_batch: bool,
    },
    AliasOf(usize),
}

impl BatchOutcomeSlot {
    fn settled(&self) -> Option<&Result<Commit>> {
        match self {
            Self::Settled { outcome, .. } => Some(outcome),
            Self::Accepted | Self::AliasOf(_) => None,
        }
    }
}

pub(crate) struct PublicationClock<'a> {
    pub(crate) batch: &'a Deadline,
    pub(crate) attempt: Observation,
    pub(crate) tip: Observation,
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
    let elapsed_before_attempt_ms = clock.batch.elapsed_at(&clock.attempt);
    let Some(admission_now_ms) = context.now_ms.checked_add(elapsed_before_attempt_ms) else {
        return PublishBatchAgainstViewResult::unchanged(vec![
            Err(CoreError::Internal(
                "publication time overflow".to_owned()
            ));
            candidates.len()
        ]);
    };
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
                CandidateTime {
                    committed_at_ms: context.now_ms,
                    admission_now_ms,
                },
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
                materialize_commit(plan, context.now_ms, candidate.inline_content())
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
    let wal = match prepare_segment(
        namespace_id.clone(),
        view.acquired_writer.writer_epoch,
        &view.head,
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
    let resulting_head = view.head.after_segment(wal.envelope().payload());
    let elapsed_ms = clock.batch.elapsed_ms();
    let Some(publication_now_ms) = context.now_ms.checked_add(elapsed_ms) else {
        return abort_batch(
            slots,
            &CoreError::Internal("publication time overflow".to_owned()),
        );
    };
    for (candidate, slot) in candidates.iter().zip(&mut slots) {
        if matches!(slot, BatchOutcomeSlot::Accepted) {
            if let Err(error) =
                validate_candidate_content_references(candidate, namespace_id, publication_now_ms)
            {
                *slot = BatchOutcomeSlot::Settled {
                    outcome: Err(error),
                    depends_on_batch: false,
                };
                return abort_batch(slots, &CoreError::WalPublish(WalPublishError::StaleHead));
            }
        }
    }
    if let Err(error) = publish_segment(store, &wal, &clock.tip).await {
        return abort_batch(slots, &error);
    }

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
                outcome: committed_change_from_wal_record(namespace_id, record),
                depends_on_batch: false,
            };
        }
    }
    PublishBatchAgainstViewResult {
        results: finish_batch_outcomes(&slots),
        effect: PublishViewEffect::Advanced {
            records: wal_records,
            inline_content: accepted_commits
                .into_iter()
                .flat_map(|commit| commit.inline_content)
                .collect(),
            head: resulting_head,
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
fn finish_batch_outcomes(slots: &[BatchOutcomeSlot]) -> Vec<Result<Commit>> {
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
    use crate::protocol::load_publish_metadata_view;
    use crate::time::StdMonotonicTimer;
    use loonfs_api::{AbsolutePath, ChangeSeq, CommitId, MAX_PUBLIC_INTEGER};
    use loonfs_objectstore::keys::{hint, wal_segment_prefix};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use tempfile::tempdir;

    #[tokio::test]
    async fn one_expired_candidate_replans_the_other_without_writing() {
        use crate::storage::content_admission::PreparedContent;
        use loonfs_test_support::clock::ManualClock;
        use loonfs_test_support::stores::{KeyPredicate, OperationClass, RecordingStore};

        let directory = tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = MutationContext {
            writer_id: loonfs_api::WriterId::parse("writer").expect("writer id"),
            now_ms: 1_000,
        };
        bootstrap_namespace(
            &store,
            &namespace_id,
            &context,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::unrestricted(),
            false,
        )
        .await
        .expect("bootstrap");
        let acquired = acquire_writer_epoch(&store, &namespace_id, &context)
            .await
            .expect("acquire writer");
        let (view, _projection) =
            load_publish_metadata_view(&store, None, &namespace_id, acquired, None)
                .await
                .expect("publish view");
        let candidates = [("expired", 1_000), ("valid", 2_000)].map(|(name, expires_at_ms)| {
            let content_ref = PreparedContent::inline(
                namespace_id.clone(),
                bytes::Bytes::from_static(b"content"),
            )
            .content_ref()
            .clone();
            CommitCandidate::prepared(
                CommitRequest::single(
                    CommitId::parse(name).expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/{name}")).expect("path"),
                        content_ref: Some(content_ref.clone()),
                        inline_content: None,
                        behavior: loonfs_api::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![PreparedContent::for_completed_upload(
                    content_ref,
                    expires_at_ms,
                )],
            )
        });
        store.reset();
        let timer = std::sync::Arc::new(ManualClock::new(0));
        let batch = Deadline::start(timer.clone());
        let attempt = batch.observe();
        timer.advance_ms(1);
        let result = publish_namespace_commits_batch_against_publish_view(
            &store,
            &namespace_id,
            &candidates,
            &context,
            &view,
            PublicationClock {
                batch: &batch,
                tip: attempt.clone(),
                attempt,
            },
        )
        .await;

        assert_eq!(
            result.results[0]
                .as_ref()
                .expect_err("expired content")
                .code(),
            loonfs_api::ErrorCode::ContentNotPrepared
        );
        assert!(matches!(
            result.results[1],
            Err(CoreError::WalPublish(WalPublishError::StaleHead))
        ));
        assert_eq!(store.count(OperationClass::Put), 0);
        assert_eq!(store.count(OperationClass::CompareAndSwap), 0);
        assert_eq!(store.count(OperationClass::Delete), 0);
    }

    #[tokio::test]
    async fn sequence_exhaustion_writes_neither_wal_nor_hint() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = MutationContext {
            writer_id: loonfs_api::WriterId::parse("writer").expect("writer id"),
            now_ms: 1_000,
        };
        bootstrap_namespace(
            &store,
            &namespace_id,
            &context,
            &loonfs_test_support::test_actor(),
            &loonfs_api::NamespaceAccess::Unrestricted {},
            false,
        )
        .await
        .expect("bootstrap namespace");
        let acquired_writer = acquire_writer_epoch(&store, &namespace_id, &context)
            .await
            .expect("acquire writer");
        let (mut view, _projection) =
            load_publish_metadata_view(&store, None, &namespace_id, acquired_writer, None)
                .await
                .expect("load publish view");

        let hint_key = hint(&namespace_id);
        let hint_before = store
            .get(&hint_key, None)
            .await
            .expect("read hint")
            .expect("hint exists");
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
        let batch = Deadline::start(std::sync::Arc::new(StdMonotonicTimer::default()));
        let result = publish_namespace_commits_batch_against_publish_view(
            &store,
            &namespace_id,
            &[candidate],
            &context,
            &view,
            PublicationClock {
                batch: &batch,
                attempt: batch.observe(),
                tip: batch.observe(),
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
                .get(&hint_key, None)
                .await
                .expect("read hint after")
                .expect("hint exists"),
            hint_before
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
