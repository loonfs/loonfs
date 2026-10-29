//! Batch publication: admitted mutation candidates become one WAL object
//! plus one numbered WAL put, with outcomes fanned back to every
//! candidate slot.

use super::candidates::{
    prepare_candidate_request, validate_candidate_content_references, BatchDedup,
    CandidateAdmission,
};
use super::engine::CommitCandidate;
use super::publish_view::PublishMetadataView;
use super::wal_payload_from_commit_plan;
use crate::changes::committed_change_from_wal_record;
use crate::error::{CoreError, Result};
use crate::namespace::state::NamespaceReadState;
use crate::path::write::PublishPlanningSession;
use crate::phase::Phase;
use crate::time::{Observation, OperationClock};
use crate::wal::WalPublishError;
use crate::wal::{prepare_wal_object_from_payloads, publish_wal_object};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::{
    WalCommitPayload, MAX_WAL_OBJECT_BYTES, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES,
    WAL_OBJECT_OVERHEAD_BYTES,
};
use loonfs_types::Commit;
use loonfs_types::NamespaceId;
use tracing::Instrument;

#[derive(Debug, Clone)]
pub(crate) struct PublishBatchReport {
    pub(crate) results: Vec<Result<Commit>>,
    pub(crate) effect: PublishViewEffect,
}

/// What one batch did to the publish view it ran against — the whole of what
/// a caller needs to determine what happens to the projection it loaded.
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
    },
}

impl PublishBatchReport {
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
    pub(crate) batch: &'a OperationClock,
    pub(crate) attempt: Observation,
    pub(crate) tip: Observation,
}

pub(crate) async fn publish_namespace_commits_batch_against_publish_view<
    S: ObjectStore + ?Sized,
>(
    candidates: &[CommitCandidate],
    view: &PublishMetadataView<'_, S>,
    clock: PublicationClock<'_>,
) -> PublishBatchReport {
    if candidates.is_empty() {
        return PublishBatchReport::unchanged(Vec::new());
    }
    let batch_size = u64::try_from(candidates.len()).unwrap_or(u64::MAX);
    let namespace_id = &view.head().namespace_id;
    let admission_now_ms = clock.batch.observed_at_ms(&clock.attempt);
    let mut slots = Vec::with_capacity(candidates.len());
    let mut session = PublishPlanningSession::new(
        view.head(),
        view.wal_tail(),
        view.metadata_view(),
        view.content_writes(),
        clock.batch.request_at_ms(),
    );
    let mut accepted_payloads = Vec::new();
    let mut wal_bytes = WAL_OBJECT_OVERHEAD_BYTES;
    let mut wal_inline_bytes = 0_usize;
    let mut dedup = BatchDedup::default();

    let prepare_span = crate::phase_span!(
        Phase::PrepareBatch,
        batch_size,
        accepted_count = tracing::field::Empty
    );
    async {
        for (index, candidate) in candidates.iter().enumerate() {
            let admission = prepare_candidate_request(
                view,
                &session,
                candidate,
                index,
                admission_now_ms,
                &mut dedup,
            )
            .instrument(crate::phase_span!(Phase::PrepareCommit))
            .await;
            let plan = match admission {
                CandidateAdmission::Prepared(plan) => plan,
                CandidateAdmission::Settled(slot) => {
                    slots.push(settle_admission(slot, !accepted_payloads.is_empty()));
                    continue;
                }
            };
            let record_bytes = candidate.planned_wal_record_bytes(&plan.deltas, &plan.appended);
            let inline_bytes = candidate
                .inline_content()
                .iter()
                .map(|value| value.bytes().len())
                .chain(plan.appended.iter().map(|piece| piece.bytes.len()))
                .fold(wal_inline_bytes, usize::saturating_add);
            let estimated_bytes = wal_bytes.saturating_add(record_bytes);
            if let Some((estimated_bytes, max_bytes)) =
                exceeded_wal_object_limit(inline_bytes, estimated_bytes)
            {
                slots.push(settle_admission(
                    BatchOutcomeSlot::Settled {
                        outcome: Err(CoreError::CommitTooLarge {
                            estimated_bytes,
                            max_bytes,
                        }),
                        depends_on_batch: !accepted_payloads.is_empty(),
                    },
                    !accepted_payloads.is_empty(),
                ));
                continue;
            }
            assert_eq!(
                plan.namespace_id, *namespace_id,
                "every record in a WAL object should belong to namespace `{namespace_id}`"
            );
            let preview = {
                let _span = crate::phase_span!(Phase::BuildWalPayload).entered();
                wal_payload_from_commit_plan(&plan)
            };
            {
                let _span = crate::phase_span!(Phase::ApplyCommittedWalRecord).entered();
                session.apply_accepted_commit(&preview, &plan);
            }
            slots.push(BatchOutcomeSlot::Accepted);
            wal_bytes = estimated_bytes;
            wal_inline_bytes = inline_bytes;
            accepted_payloads.push(preview);
        }
    }
    .instrument(prepare_span.clone())
    .await;
    prepare_span.record(
        "accepted_count",
        u64::try_from(accepted_payloads.len()).unwrap_or(u64::MAX),
    );
    drop(prepare_span);

    if accepted_payloads.is_empty() {
        return PublishBatchReport::unchanged(finish_batch_outcomes(&slots));
    }
    let wal = match prepare_wal_object_from_payloads(
        namespace_id.clone(),
        view.acquired_writer.writer_epoch,
        view.head(),
        view.retention_floor_seq(),
        session.next_inode_id(),
        accepted_payloads,
    ) {
        Ok(wal) => wal,
        Err(error) => return abort_batch(slots, &error),
    };
    let resulting_head = view.head().after_wal_object(wal.envelope().payload());
    let publication_now_ms = clock.batch.now_ms();
    if let Err(error) = ensure_accepted_content_still_valid(
        candidates,
        &mut slots,
        namespace_id,
        publication_now_ms,
    ) {
        return abort_batch(slots, &error);
    }
    if let Err(error) = publish_wal_object(view.store(), &wal, &clock.tip).await {
        return abort_batch(slots, &error);
    }

    let wal_records = wal.into_envelope().into_payload().records;
    settle_published_slots(&mut slots, namespace_id, &wal_records);
    PublishBatchReport {
        results: finish_batch_outcomes(&slots),
        effect: PublishViewEffect::Advanced {
            records: wal_records,
            head: resulting_head,
        },
    }
}

/// Returns the bytes and the limit of the first WAL object limit that the
/// batch would exceed with one more commit, or `None` when the commit fits.
fn exceeded_wal_object_limit(
    inline_bytes: usize,
    estimated_bytes: usize,
) -> Option<(usize, usize)> {
    if inline_bytes > MAX_WAL_OBJECT_INLINE_CONTENT_BYTES {
        Some((inline_bytes, MAX_WAL_OBJECT_INLINE_CONTENT_BYTES))
    } else if estimated_bytes > MAX_WAL_OBJECT_BYTES {
        Some((estimated_bytes, MAX_WAL_OBJECT_BYTES))
    } else {
        None
    }
}

/// Checks the content references of every accepted candidate again at
/// publication time. A candidate whose content no longer passes settles with
/// its error, and the batch publishes nothing.
fn ensure_accepted_content_still_valid(
    candidates: &[CommitCandidate],
    slots: &mut [BatchOutcomeSlot],
    namespace_id: &NamespaceId,
    publication_now_ms: u64,
) -> Result<()> {
    for (candidate, slot) in candidates.iter().zip(slots) {
        if matches!(slot, BatchOutcomeSlot::Accepted) {
            if let Err(error) =
                validate_candidate_content_references(candidate, namespace_id, publication_now_ms)
            {
                *slot = BatchOutcomeSlot::Settled {
                    outcome: Err(error),
                    depends_on_batch: false,
                };
                return Err(CoreError::WalPublish(WalPublishError::NumberTaken));
            }
        }
    }
    Ok(())
}

/// Settles each accepted slot with the commit that its record in the
/// published WAL object describes.
fn settle_published_slots(
    slots: &mut [BatchOutcomeSlot],
    namespace_id: &NamespaceId,
    wal_records: &[WalCommitPayload],
) {
    assert_eq!(
        slots
            .iter()
            .filter(|slot| matches!(slot, BatchOutcomeSlot::Accepted))
            .count(),
        wal_records.len(),
        "accepted slot count should match WAL record count"
    );
    let mut records = wal_records.iter();
    for slot in slots {
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
}

/// Applies the publication error and invalidates the loaded projection.
fn abort_batch(mut slots: Vec<BatchOutcomeSlot>, error: &CoreError) -> PublishBatchReport {
    fail_unpublished_slots(&mut slots, error);
    PublishBatchReport {
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
    use crate::path::write::{CommitIntent, FilesystemOperation};
    use crate::test_support::WriterAt;

    use loonfs_objectstore::keys::{hint, wal_prefix};
    use loonfs_objectstore::local_fs_store::LocalFsStore;
    use loonfs_objectstore::ObjectStore;
    use loonfs_objectstore::ObjectStoreExtension;
    use loonfs_types::{AbsolutePath, ChangeSeq, CommitId, NamespaceId, MAX_PUBLIC_INTEGER};
    use std::assert_matches;
    use tempfile::tempdir;

    #[tokio::test]
    async fn one_expired_candidate_replans_the_other_without_writing() {
        use crate::storage::content_admission::PreparedContent;
        use loonfs_test_support::clock::ManualClock;
        use loonfs_test_support::stores::{KeyPredicate, OperationSelector, RecordingStore};

        let directory = tempdir().expect("directory");
        let store = RecordingStore::new(
            LocalFsStore::new(directory.path()).expect("store"),
            KeyPredicate::any(),
        );
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = WriterAt {
            writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
            now_ms: 1_000,
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap");
        let acquired = crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context.writer_id)
            .await
            .expect("acquire writer");
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let view = namespace
            .load_publish_metadata_view(acquired, None, None)
            .await
            .expect("publish view");
        let candidates = [("expired", 1_001), ("valid", 2_000)].map(|(name, expires_at_ms)| {
            let content_ref = PreparedContent::inline(
                namespace_id.clone(),
                bytes::Bytes::from_static(b"content"),
                loonfs_types::ChecksumAlgorithm::Crc64nvme,
            )
            .content_ref()
            .clone();
            CommitCandidate::prepared(
                CommitIntent::single(
                    CommitId::parse(name).expect("commit id"),
                    loonfs_test_support::test_actor(),
                    None,
                    FilesystemOperation::PutFile {
                        path: AbsolutePath::parse(format!("/{name}")).expect("path"),
                        content: loonfs_types::FileContent::Ref(content_ref.clone()),
                        behavior: loonfs_types::DestinationBehavior::NoReplace,
                        expected_inode_id: None,
                        expected_revision_no: None,
                    },
                ),
                vec![PreparedContent::for_completed_upload(
                    content_ref,
                    expires_at_ms,
                    None,
                )],
            )
        });
        store.reset();
        let timer = std::sync::Arc::new(ManualClock::new(0));
        let batch = OperationClock::start(context.now_ms, timer.clone());
        let attempt = batch.observe();
        timer.advance_ms(1);
        let result = publish_namespace_commits_batch_against_publish_view(
            &candidates,
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
            loonfs_types::ErrorCode::ContentNotPrepared
        );
        assert_matches!(
            result.results[1],
            Err(CoreError::WalPublish(WalPublishError::NumberTaken))
        );
        assert_eq!(store.count(OperationSelector::Put), 0);
        assert_eq!(store.count(OperationSelector::CompareAndSwap), 0);
        assert_eq!(store.count(OperationSelector::Delete), 0);
    }

    #[tokio::test]
    async fn sequence_exhaustion_writes_neither_wal_nor_hint() {
        let temp_dir = tempdir().expect("tempdir");
        let store = LocalFsStore::new(temp_dir.path()).expect("store");
        let namespace_id = NamespaceId::parse("demo").expect("namespace id");
        let context = WriterAt {
            writer_id: loonfs_types::WriterId::parse("writer").expect("writer id"),
            now_ms: 1_000,
        };
        crate::test_support::NamespaceFixture::new(&store, &namespace_id, &context)
            .create()
            .await
            .expect("bootstrap namespace");
        let acquired_writer = crate::test_support::namespace_store(&store, &namespace_id)
            .acquire_writer_epoch(&context.writer_id)
            .await
            .expect("acquire writer");
        let namespace = crate::test_support::namespace_store(&store, &namespace_id);
        let mut view = namespace
            .load_publish_metadata_view(acquired_writer, None, None)
            .await
            .expect("load publish view");

        let hint_key = hint(&namespace_id);
        let hint_before = store
            .get(&hint_key, None)
            .await
            .expect("read hint")
            .expect("hint exists");
        let wal_before = store
            .list_prefix(&wal_prefix(&namespace_id))
            .await
            .expect("list WAL before");

        // Change only the in-memory head so this exercises the sequence limit
        // without modifying the stored metadata.
        view.position.read_basis.head.seq = ChangeSeq(MAX_PUBLIC_INTEGER);
        let candidate = CommitCandidate::new(CommitIntent::single(
            CommitId::parse("past-sequence-limit").expect("commit id"),
            loonfs_test_support::test_actor(),
            None,
            FilesystemOperation::CreateDirectory {
                path: AbsolutePath::parse("/blocked").expect("path"),
                parents: false,
            },
        ));
        let batch = context.clock();
        let result = publish_namespace_commits_batch_against_publish_view(
            &[candidate],
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
        assert_eq!(error.code(), loonfs_types::ErrorCode::NamespaceCorrupt);
        assert!(error.to_string().contains("largest sequence number"));
        assert_matches!(result.effect, PublishViewEffect::Unchanged);
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
                .list_prefix(&wal_prefix(&namespace_id))
                .await
                .expect("list WAL after"),
            wal_before
        );
    }
}
