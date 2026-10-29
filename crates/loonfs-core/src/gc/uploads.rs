//! Garbage collection for upload sessions and their content.

use super::live_set::{LiveSet, RetirementStatus};
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::CONTENT_RECLAMATION_GRACE_WINDOW_MS;
use crate::namespace::store::NamespaceStore;
use crate::protocol::AbandonedUpload;
use crate::time::OperationClock;
use crate::upload_records::{OutcomeCreation, UploadSessionState};
use futures::{future, Stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::upload_session_prefix;
use loonfs_objectstore::layout::{upload_id_of, upload_outcome_id_of};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::format::control::{UploadOutcome, UploadSessionPayload};
use loonfs_types::{ContentId, UploadId};
use std::pin::pin;

/// One upload session's records, or one other key, as a listing of the
/// uploads prefix returned them.
pub(super) struct ListedUpload {
    pub(super) kind: ListedUploadKind,
    /// The bytes of the listed records, as the listing reported them.
    pub(super) size_bytes: u64,
}

/// The records in one [`ListedUpload`].
pub(super) enum ListedUploadKind {
    /// A session record, and whether the listing returned its outcome record.
    Session {
        upload_id: UploadId,
        outcome_listing: OutcomeListing,
    },
    /// An outcome record listed without its session record, with the
    /// listing's modification time. It's usually left over from a deletion
    /// that stopped between the two records, or from a completion or abort
    /// that found the session gone. But a listing isn't a snapshot, so it can
    /// also belong to a session that was created and ended while the listing
    /// ran, whose session record the listing missed.
    OrphanOutcome { entry: ListedObject },
    /// A key under the uploads prefix that isn't an upload record.
    Unrecognized,
}

/// Whether a listing of the uploads prefix returned a session's outcome key
/// after its session key.
#[derive(Clone, Copy)]
pub(super) enum OutcomeListing {
    Listed,
    Absent,
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Lists the uploads prefix and pairs each session record with its outcome
    /// record.
    ///
    /// An outcome key sorts directly after its session key, so the pairing looks
    /// one key ahead and keeps no other state across the listing.
    pub(super) fn list_uploads(&self) -> impl Stream<Item = Result<ListedUpload>> + Send + 'static {
        let prefix = upload_session_prefix(&self.namespace_id);
        let entries = self
            .store
            .list_entries_from_stream(&prefix, None)
            .map_err(move |error| CoreError::store(&prefix, &error))
            .boxed()
            .peekable();
        futures::stream::try_unfold(Box::pin(entries), |mut entries| async move {
            let Some(entry) = entries.next().await.transpose()? else {
                return Ok(None);
            };
            let mut size_bytes = entry.size_bytes;
            let kind = if let Some(upload_id) = upload_id_of(&entry.key) {
                let outcome = entries
                    .as_mut()
                    .next_if(|next| {
                        matches!(next, Ok(next) if upload_outcome_id_of(&next.key).as_ref() == Some(&upload_id))
                    })
                    .await
                    .transpose()?;
                let outcome_listing = match outcome {
                    Some(outcome) => {
                        size_bytes += outcome.size_bytes;
                        OutcomeListing::Listed
                    }
                    None => OutcomeListing::Absent,
                };
                ListedUploadKind::Session {
                    upload_id,
                    outcome_listing,
                }
            } else if upload_outcome_id_of(&entry.key).is_some() {
                ListedUploadKind::OrphanOutcome { entry }
            } else {
                ListedUploadKind::Unrecognized
            };
            Ok(Some((ListedUpload { kind, size_bytes }, entries)))
        })
    }

    /// Lists every upload session in the namespace and calls `visit` with each
    /// one, so the caller can charge each session to its budget as it arrives.
    ///
    /// A completed session that `session_cache` has costs no store request. The
    /// pass reads one record of every other session, and both records of an
    /// aborted one, with at most `STORE_READS_IN_FLIGHT` reads in flight. An
    /// outcome record without its session record isn't a root, because nothing
    /// can issue admission evidence from it.
    pub(super) async fn load_upload_sessions(
        &self,
        session_cache: Option<&CompletedUploadSessionCache>,
        mut visit: impl FnMut(ListedUploadSession) -> Result<()>,
    ) -> Result<()> {
        let mut sessions = pin!(self
            .list_uploads()
            .try_filter_map(|listed| future::ok(match listed.kind {
                ListedUploadKind::Session {
                    upload_id,
                    outcome_listing,
                } => Some((upload_id, outcome_listing)),
                ListedUploadKind::OrphanOutcome { .. } | ListedUploadKind::Unrecognized => None,
            }))
            .map_ok(|(upload_id, outcome_listing)| async move {
                if let Some(session) = session_cache.and_then(|cache| cache.get(&upload_id)) {
                    return Ok(SessionLookup::Cached(ListedUploadSession::Completed {
                        upload_id,
                        session,
                    }));
                }
                self.load_listed_session(upload_id, outcome_listing)
                    .await
                    .map(SessionLookup::Read)
            })
            .try_buffer_unordered(STORE_READS_IN_FLIGHT));
        let mut hits = 0_u64;
        let mut misses = 0_u64;
        while let Some(lookup) = sessions.try_next().await? {
            let session = match lookup {
                SessionLookup::Cached(session) => {
                    hits += 1;
                    Some(session)
                }
                SessionLookup::Read(session) => {
                    misses += 1;
                    session
                }
            };
            if let Some(session) = session {
                visit(session)?;
            }
        }
        tracing::debug!(
            namespace_id = %self.namespace_id,
            session_cache_hits = hits,
            session_cache_misses = misses,
            "listed upload sessions"
        );
        Ok(())
    }

    /// Reads what a pass needs from one listed session: the outcome record if
    /// the listing included one, and the session record unless a completed
    /// outcome already identifies the content.
    async fn load_listed_session(
        &self,
        upload_id: UploadId,
        outcome_listing: OutcomeListing,
    ) -> Result<Option<ListedUploadSession>> {
        if let OutcomeListing::Listed = outcome_listing {
            if let Some(UploadOutcome::Completed {
                completed_at_ms,
                content_ref,
            }) = self.load_upload_outcome(&upload_id).await?
            {
                return Ok(Some(ListedUploadSession::Completed {
                    upload_id,
                    session: CompletedUploadSession {
                        content_id: content_ref.content_id,
                        completed_at_ms,
                    },
                }));
            }
        }
        Ok(self
            .load_upload_session_record(&upload_id)
            .await?
            .map(|session| ListedUploadSession::Incomplete {
                content_id: session.content_id,
            }))
    }

    /// Reads the records of a listed session that the pass doesn't already know
    /// to be completed, or returns `None` if the session record is gone.
    ///
    /// A session listed without an outcome is read as open, without reading the
    /// outcome key. If the session ended since the listing, the sweep's abort of
    /// an expired session finds that outcome, because its own create of the
    /// outcome record fails.
    pub(super) async fn load_swept_session(
        &self,
        upload_id: &UploadId,
        outcome_listing: OutcomeListing,
    ) -> Result<Option<UploadSessionState>> {
        let outcome = match outcome_listing {
            OutcomeListing::Listed => self.load_upload_outcome(upload_id).await?,
            OutcomeListing::Absent => None,
        };
        Ok(self
            .load_upload_session_record(upload_id)
            .await?
            .map(|session| UploadSessionState { session, outcome }))
    }

    pub(super) async fn sweep_upload_session(
        &self,
        grace_window_ms: u64,
        live: &LiveSet,
        clock: &OperationClock,
        state: &UploadSessionState,
    ) -> Result<UploadSessionSweep> {
        if let RetirementStatus::Retained { reclaimable_at_ms } = live.retirement_status() {
            return Ok(UploadSessionSweep::Retain { reclaimable_at_ms });
        }
        match &state.outcome {
            None => {
                self.abort_expired_session(grace_window_ms, clock, &state.session)
                    .await
            }
            Some(UploadOutcome::Aborted { aborted_at_ms }) => {
                if clock.request_at_ms().saturating_sub(*aborted_at_ms) < grace_window_ms {
                    return Ok(retain_until(aborted_at_ms.saturating_add(grace_window_ms)));
                }
                // Repeat provider cleanup so a later pass completes work left by a crash
                // after the abort.
                if !AbandonedUpload::of(&state.session)
                    .release(&self.store)
                    .await
                {
                    return Ok(retain_undated());
                }
                // Abort cleanup runs even when no content object was written.
                Ok(UploadSessionSweep::Delete)
            }
            Some(UploadOutcome::Completed {
                completed_at_ms, ..
            }) => Ok(sweep_completed_session(
                live,
                clock.request_at_ms(),
                *completed_at_ms,
            )),
        }
    }

    /// Aborts a session after its lease and grace period expire, then cleans up
    /// its unpublished content.
    ///
    /// The outcome record is created with put-if-absent, so this abort can't
    /// replace a completion that was recorded first. A completion that arrives
    /// after the lease is refused, so the grace only covers a completing host
    /// whose clock runs behind the collector's.
    async fn abort_expired_session(
        &self,
        grace_window_ms: u64,
        clock: &OperationClock,
        session: &UploadSessionPayload,
    ) -> Result<UploadSessionSweep> {
        let expires_at_ms = session.expires_at_ms;
        if clock.request_at_ms().saturating_sub(expires_at_ms) < grace_window_ms {
            return Ok(retain_until(expires_at_ms.saturating_add(grace_window_ms)));
        }
        let aborted = UploadOutcome::Aborted {
            aborted_at_ms: clock.request_at_ms(),
        };
        match self.create_upload_outcome(session, &aborted).await {
            // Keep the newly aborted session until its post-abort grace period expires.
            Ok(OutcomeCreation::Created) => {
                let _ = AbandonedUpload::of(session).release(&self.store).await;
                Ok(retain_until(
                    clock.request_at_ms().saturating_add(grace_window_ms),
                ))
            }
            Ok(OutcomeCreation::Existing(_)) => {
                tracing::debug!(
                    namespace_id = %session.namespace_id,
                    upload_id = %session.upload_id,
                    "another request ended the expired upload session first; retaining"
                );
                Ok(retain_undated())
            }
            // Another pass deleted the session after this one listed it.
            Err(CoreError::UploadNotFound { .. }) => Ok(retain_undated()),
            Err(error) => Err(error),
        }
    }
}

/// One upload session that a pass listed, with what the pass needs from it.
pub(super) enum ListedUploadSession {
    /// A completed session, from the cache or from its outcome record.
    Completed {
        upload_id: UploadId,
        session: CompletedUploadSession,
    },
    /// An open or aborted session, whose records the sweep reads again.
    Incomplete { content_id: ContentId },
}

/// Where a pass found one listed upload session.
enum SessionLookup {
    /// The cache had the session.
    Cached(ListedUploadSession),
    /// The pass read the records. `None` means that another pass deleted the
    /// session after this pass listed it.
    Read(Option<ListedUploadSession>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UploadSessionSweep {
    /// The pass keeps the session's records. It might have aborted the session.
    Retain {
        /// Earliest time this session may be reconsidered when retention is
        /// time-based. `None` means the pass must retry later for a non-time-based
        /// reason, such as a lost race or failed cleanup.
        reclaimable_at_ms: Option<u64>,
    },
    /// No later pass needs the session, so its records can be deleted.
    Delete,
}

/// Returns whether a completed session's records are kept or deleted, using
/// only the session's completion time.
pub(super) fn sweep_completed_session(
    live: &LiveSet,
    now_ms: u64,
    completed_at_ms: u64,
) -> UploadSessionSweep {
    match live.retirement_status() {
        RetirementStatus::Retained { reclaimable_at_ms } => {
            UploadSessionSweep::Retain { reclaimable_at_ms }
        }
        RetirementStatus::Active
            if now_ms.saturating_sub(completed_at_ms) < CONTENT_RECLAMATION_GRACE_WINDOW_MS =>
        {
            retain_until(completed_at_ms.saturating_add(CONTENT_RECLAMATION_GRACE_WINDOW_MS))
        }
        RetirementStatus::Active | RetirementStatus::Eligible => UploadSessionSweep::Delete,
    }
}

/// A session held over for a wait that ends at `at_ms`.
fn retain_until(at_ms: u64) -> UploadSessionSweep {
    UploadSessionSweep::Retain {
        reclaimable_at_ms: Some(at_ms),
    }
}

/// A session held over for a reason no clock resolves.
fn retain_undated() -> UploadSessionSweep {
    UploadSessionSweep::Retain {
        reclaimable_at_ms: None,
    }
}
