//! Submission and admission: the candidates and deletes that enter a
//! namespace's queue, and the work items the worker takes from it.

use super::admission::{AdmissionPermit, AdmittedWaiter, PublicationAdmission};
use super::{NamespacePublisher, NamespacePublisherState};
use crate::publish::CommitCandidate;
use crate::trace::phase_event;
use crate::{CoreError, DeleteNamespaceOptions, DeleteNamespaceResponse, Result};
use loonfs_core::commit::CommitFingerprint;
use loonfs_core::limits::MAX_CONTENTION_ATTEMPTS;
use loonfs_core::publish::WalPublishError;
use loonfs_core::Phase;
use loonfs_types::format::wal::{MAX_WAL_OBJECT_BYTES, WAL_OBJECT_OVERHEAD_BYTES};
use loonfs_types::Commit;
use loonfs_types::{u64_from_usize, CommitId, MonotonicTimer, NamespaceId};
use std::sync::Arc;
use tokio::sync::oneshot;

pub(super) struct PendingDelete {
    pub(super) options: DeleteNamespaceOptions,
    pub(super) waiters: Vec<AdmittedWaiter<Result<DeleteNamespaceResponse>>>,
}

pub(super) enum WorkItem {
    Batch(OpenBatch),
    Delete(PendingDelete),
}

pub(super) struct OpenBatch {
    pub(super) candidates: Vec<BatchCandidate>,
    pub(super) wal_record_bytes_upper_bound: usize,
    pub(super) inline_content_bytes: usize,
}

pub(super) struct PreparedCandidate {
    pub(super) candidate: CommitCandidate,
    pub(super) estimated_retained_bytes: usize,
    pub(super) wal_record_bytes_upper_bound: usize,
    pub(super) inline_content_bytes: usize,
}

impl PreparedCandidate {
    pub(super) fn new(candidate: CommitCandidate) -> std::result::Result<Self, CoreError> {
        Ok(Self {
            estimated_retained_bytes: candidate.estimated_retained_bytes()?,
            wal_record_bytes_upper_bound: candidate.wal_record_bytes_upper_bound(),
            inline_content_bytes: candidate.inline_content_bytes(),
            candidate,
        })
    }

    pub(super) fn with_inline_placement(
        candidate: CommitCandidate,
        namespace_id: &NamespaceId,
        retained_inline_content: &[loonfs_core::publish::InlineContent],
        staged_inline_content: &[loonfs_core::publish::InlineContent],
    ) -> std::result::Result<Self, CoreError> {
        Ok(Self {
            estimated_retained_bytes: candidate.estimated_retained_bytes_after_inline_staging(
                namespace_id,
                retained_inline_content,
                staged_inline_content,
            )?,
            wal_record_bytes_upper_bound: candidate
                .wal_record_bytes_upper_bound_with_inline_content(retained_inline_content),
            inline_content_bytes: retained_inline_content
                .iter()
                .fold(candidate.appended_bytes(), |total, value| {
                    total.saturating_add(value.bytes().len())
                }),
            candidate,
        })
    }
}

#[derive(Clone)]
pub(super) struct BatchCandidate {
    pub(super) permit: Arc<AdmissionPermit>,
    pub(super) commit_id: CommitId,
    pub(super) candidate: CommitCandidate,
    pub(super) enqueued_at: u64,
    /// Whether this commit was queued when a wait at the write stop ended, so
    /// `write_stop_waits` records it once.
    pub(super) write_stop_wait_recorded: bool,
    /// Whether the engine refused this commit at the write stop when the
    /// worker last published it.
    ///
    /// Only such a commit can reach the end of the write-stop wait without
    /// another publish, because the engine returns a retry's result from its
    /// receipt.
    pub(super) refused_by_write_stop: bool,
}

pub(super) struct InFlightRequest {
    pub(super) semantic_identity: CommitFingerprint,
    pub(super) waiters: Vec<AdmittedWaiter<Result<Commit>>>,
}

pub(super) enum SubmissionAdmission {
    /// This submission owns the published outcome, either as the primary or
    /// as an exact duplicate of it.
    OwnOutcome,
    /// A different claim currently owns the commit ID. Its successful outcome
    /// supplies the receipt evidence this submission needs for a useful
    /// conflict; if it fails, this submission gets another turn at admission.
    Contended {
        primary_identity: CommitFingerprint,
        candidate: Box<PreparedCandidate>,
        semantic_identity: CommitFingerprint,
    },
}

/// An admitted submission, and the queue depth and reason that its
/// admission traces once the state lock is released.
pub(super) struct AdmittedSubmission {
    pub(super) admission: SubmissionAdmission,
    pub(super) queue_depth: usize,
    pub(super) reason: &'static str,
}

impl NamespacePublisher {
    /// Places the candidate's inline content, then admits it before awaiting
    /// its result.
    ///
    /// Once the candidate enters the queue, cancelling the caller only drops
    /// result delivery; the worker still owns and publishes the request.
    pub(super) async fn submit_candidate(&self, candidate: CommitCandidate) -> Result<Commit> {
        drop(self.admitted_session(&self.lock_state())?);
        let plan = self.plan_inline_candidate(candidate)?;
        let permit = self.acquire_candidate(&plan.candidate)?;
        let candidate = self.stage_inline_candidate(plan, &permit).await?;
        PublicationAdmission::validate_candidate(&candidate)?;
        submit_with_admission(
            &self.namespace_id,
            candidate,
            self.context.runtime_core.timer().as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                let admitted = self.admit(
                    commit_id,
                    candidate,
                    semantic_identity,
                    AdmittedWaiter::new(waiter, &permit),
                    enqueued_at,
                )?;
                self.trace_enqueue(admitted.queue_depth, admitted.reason);
                Ok(admitted.admission)
            },
        )
        .await
    }

    /// Admits the request before awaiting its result.
    ///
    /// The first poll either admits the candidate, waits behind an in-flight
    /// claim on its commit ID, or returns an error. Once the candidate enters
    /// the queue, cancelling the caller only drops result delivery; the worker
    /// still owns and publishes the request.
    #[cfg(test)]
    pub(super) async fn submit(&self, candidate: CommitCandidate) -> Result<Commit> {
        let candidate = PreparedCandidate::new(candidate)?;
        let permit = self.acquire_candidate(&candidate)?;
        submit_with_admission(
            &self.namespace_id,
            candidate,
            self.context.runtime_core.timer().as_ref(),
            |commit_id, candidate, semantic_identity, waiter, enqueued_at| {
                let admitted = self.admit(
                    commit_id,
                    candidate,
                    semantic_identity,
                    AdmittedWaiter::new(waiter, &permit),
                    enqueued_at,
                )?;
                self.trace_enqueue(admitted.queue_depth, admitted.reason);
                Ok(admitted.admission)
            },
        )
        .await
    }

    fn acquire_candidate(
        &self,
        candidate: &PreparedCandidate,
    ) -> std::result::Result<Arc<AdmissionPermit>, CoreError> {
        let state = self.lock_state();
        let _session = self.admitted_session(&state)?;
        self.context
            .admission
            .acquire_candidate(&self.namespace_id, candidate)
    }

    pub(super) fn admit(
        &self,
        commit_id: CommitId,
        candidate: PreparedCandidate,
        semantic_identity: CommitFingerprint,
        waiter: AdmittedWaiter<Result<Commit>>,
        enqueued_at: u64,
    ) -> std::result::Result<AdmittedSubmission, CoreError> {
        let mut state = self.lock_state();
        let _session = self.admitted_session(&state)?;
        if let Some(existing) = state.in_flight.get_mut(&commit_id) {
            if existing.semantic_identity != semantic_identity {
                let primary_identity = existing.semantic_identity.clone();
                existing.waiters.push(waiter);
                return Ok(AdmittedSubmission {
                    admission: SubmissionAdmission::Contended {
                        primary_identity,
                        candidate: Box::new(candidate),
                        semantic_identity,
                    },
                    queue_depth: queued_candidates(&state),
                    reason: "contended",
                });
            }
            existing.waiters.push(waiter);
            return Ok(AdmittedSubmission {
                admission: SubmissionAdmission::OwnOutcome,
                queue_depth: queued_candidates(&state),
                reason: "duplicate",
            });
        }

        let queued = queued_candidates(&state);
        let wal_record_bytes_upper_bound = candidate.wal_record_bytes_upper_bound;
        let inline_content_bytes = candidate.inline_content_bytes;
        let candidate = BatchCandidate {
            permit: Arc::clone(&waiter.permit),
            commit_id: commit_id.clone(),
            candidate: candidate.candidate,
            enqueued_at,
            write_stop_wait_recorded: false,
            refused_by_write_stop: false,
        };
        match state.queue.back_mut() {
            // Coalesce with the tail batch while its bound stays under the
            // WAL object limit. A delete at the tail, or a full batch, opens a
            // batch behind it; work behind a delete publishes only if that
            // delete fails.
            Some(WorkItem::Batch(batch))
                if batch
                    .wal_record_bytes_upper_bound
                    .saturating_add(wal_record_bytes_upper_bound)
                    .saturating_add(WAL_OBJECT_OVERHEAD_BYTES)
                    <= MAX_WAL_OBJECT_BYTES
                    && batch
                        .inline_content_bytes
                        .saturating_add(inline_content_bytes)
                        <= self
                            .context
                            .runtime_core
                            .config()
                            .inline_content
                            .inline_content_wal_object_budget_bytes =>
            {
                batch.wal_record_bytes_upper_bound += wal_record_bytes_upper_bound;
                batch.inline_content_bytes += inline_content_bytes;
                batch.candidates.push(candidate);
            }
            _ => state.queue.push_back(WorkItem::Batch(OpenBatch {
                candidates: vec![candidate],
                wal_record_bytes_upper_bound,
                inline_content_bytes,
            })),
        }
        state.in_flight.insert(
            commit_id,
            InFlightRequest {
                semantic_identity,
                waiters: vec![waiter],
            },
        );
        self.spawn_worker_if_idle(&mut state);
        Ok(AdmittedSubmission {
            admission: SubmissionAdmission::OwnOutcome,
            queue_depth: queued + 1,
            reason: "new",
        })
    }

    /// Enqueues the delete as a barrier: requests admitted before it
    /// publish first, and requests admitted after it fail with
    /// `namespace_deleted` once it succeeds. If the delete fails (for
    /// example a stale `expected_head_seq`), later requests publish
    /// normally — nothing is rejected for a delete that did not happen.
    pub(super) async fn submit_delete(
        &self,
        options: DeleteNamespaceOptions,
    ) -> Result<DeleteNamespaceResponse> {
        let receiver = self.admit_delete(options)?;
        receive_delete(receiver).await
    }

    pub(super) fn admit_delete(
        &self,
        options: DeleteNamespaceOptions,
    ) -> std::result::Result<oneshot::Receiver<Result<DeleteNamespaceResponse>>, CoreError> {
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.lock_state();
            let _session = self.admitted_session(&state)?;
            let permit = self.context.admission.acquire(&self.namespace_id, 0)?;
            let sender = AdmittedWaiter::new(sender, &permit);
            match state.queue.back_mut() {
                // A delete queued with the same options is the same request:
                // both callers share its outcome. Different options ask for
                // different operations and settle separately, in order.
                Some(WorkItem::Delete(pending)) if pending.options == options => {
                    pending.waiters.push(sender);
                }
                _ => state.queue.push_back(WorkItem::Delete(PendingDelete {
                    options,
                    waiters: vec![sender],
                })),
            }
            self.spawn_worker_if_idle(&mut state);
        }
        Ok(receiver)
    }

    fn trace_enqueue(&self, queue_depth: usize, reason: &'static str) {
        self.context
            .runtime_core
            .instruments()
            .publisher
            .queue_depth(queue_depth);
        phase_event!(
            self.context.runtime_core,
            Phase::Enqueue,
            self.namespace_id,
            tracing::Level::DEBUG,
            queue_depth = u64_from_usize(queue_depth),
            reason
        );
    }
}

async fn submit_with_admission<F>(
    namespace_id: &NamespaceId,
    candidate: PreparedCandidate,
    timer: &dyn MonotonicTimer,
    mut admit: F,
) -> Result<Commit>
where
    F: FnMut(
        CommitId,
        PreparedCandidate,
        CommitFingerprint,
        oneshot::Sender<Result<Commit>>,
        u64,
    ) -> std::result::Result<SubmissionAdmission, CoreError>,
{
    let commit_id = candidate.candidate.commit_id().clone();
    let enqueued_at = timer.monotonic_now_ms();
    let mut candidate = candidate;
    let mut semantic_identity = candidate.candidate.semantic_identity(namespace_id);
    for _ in 0..MAX_CONTENTION_ATTEMPTS {
        let (sender, receiver) = oneshot::channel();
        let admission = admit(
            commit_id.clone(),
            candidate,
            semantic_identity,
            sender,
            enqueued_at,
        )?;
        let result = receiver.await.map_err(|_| {
            CoreError::WalPublish(WalPublishError::OutcomeUnknown(
                "publisher task stopped before reporting an outcome".to_owned(),
            ))
        })?;
        match admission {
            SubmissionAdmission::OwnOutcome => return result,
            SubmissionAdmission::Contended {
                primary_identity,
                candidate: returned_candidate,
                semantic_identity: returned_identity,
            } => match result {
                Ok(response) => {
                    return Err(CoreError::CommitIdReuseConflict {
                        commit_id: commit_id.to_string(),
                        committed: Some(loonfs_types::CommittedReceipt {
                            committed_seq: response.committed_seq,
                            committed_fingerprint: primary_identity,
                        }),
                    }
                    .into())
                }
                Err(_) => {
                    candidate = *returned_candidate;
                    semantic_identity = returned_identity;
                }
            },
        }
    }
    Err(CoreError::CommitIdReuseConflict {
        commit_id: commit_id.to_string(),
        committed: None,
    }
    .into())
}

async fn receive_delete(
    receiver: oneshot::Receiver<Result<DeleteNamespaceResponse>>,
) -> Result<DeleteNamespaceResponse> {
    receiver.await.unwrap_or_else(|_| {
        Err(CoreError::WalPublish(WalPublishError::OutcomeUnknown(
            "publisher task stopped mid-delete".to_owned(),
        ))
        .into())
    })
}

/// Waiters a published delete barrier leaves behind, one vector per result
/// type.
#[derive(Default)]
pub(super) struct QueuedWaiters {
    pub(super) commits: Vec<AdmittedWaiter<Result<Commit>>>,
    pub(super) deletes: Vec<AdmittedWaiter<Result<DeleteNamespaceResponse>>>,
}

/// Empties the queue and hands back every waiter it kept. Called once the
/// delete barrier is published: nothing queued behind a tombstone may publish.
pub(super) fn take_queued_waiters(state: &mut NamespacePublisherState) -> QueuedWaiters {
    let mut waiters = QueuedWaiters::default();
    for item in std::mem::take(&mut state.queue) {
        match item {
            WorkItem::Batch(batch) => {
                for candidate in batch.candidates {
                    if let Some(request) = state.in_flight.remove(&candidate.commit_id) {
                        waiters.commits.extend(request.waiters);
                    }
                }
            }
            WorkItem::Delete(pending) => waiters.deletes.extend(pending.waiters),
        }
    }
    waiters
}

/// Candidates queued but not yet taken by the worker: the depth admission
/// bounds and traces.
pub(super) fn queued_candidates(state: &NamespacePublisherState) -> usize {
    state
        .queue
        .iter()
        .map(|item| match item {
            WorkItem::Batch(batch) => batch.candidates.len(),
            WorkItem::Delete(_) => 0,
        })
        .sum()
}
