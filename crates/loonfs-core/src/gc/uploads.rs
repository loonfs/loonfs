//! Garbage collection for upload sessions and their content.

use super::live_set::{LiveSet, RetirementStatus};
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::CONTENT_RECLAMATION_GRACE_WINDOW_MS;
use crate::protocol::AbandonedUpload;
use crate::upload_records::{
    create_upload_outcome, load_upload_outcome, load_upload_session_record, OutcomeCreation,
    UploadSessionState,
};
use futures::{future, Stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::upload_session_prefix;
use loonfs_objectstore::layout::{upload_id_of, upload_outcome_id_of};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{UploadOutcome, UploadSessionPayload};
use loonfs_types::{ContentId, NamespaceId, UploadId};
use std::pin::pin;

/// One upload session's records, as a listing of the uploads prefix
/// returned them.
pub(super) enum ListedUpload {
    /// A session record, followed in the listing by its outcome record when
    /// `outcome_listed` is true.
    Session {
        upload_id: UploadId,
        outcome_listed: bool,
    },
    /// An outcome record listed without its session record. It's left over
    /// from a deletion that stopped between the two records, or from a
    /// completion or abort that found the session gone. No request acts on it.
    OrphanOutcome { upload_id: UploadId },
    /// A key under the uploads prefix that isn't an upload record.
    Unrecognized,
}

/// Lists the uploads prefix and pairs each session record with its outcome
/// record.
///
/// An outcome key sorts directly after its session key, so the pairing looks
/// one key ahead and keeps no other state across the listing.
pub(super) fn list_uploads<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
) -> impl Stream<Item = Result<ListedUpload>> + Send + 'static {
    let prefix = upload_session_prefix(namespace_id);
    let keys = store
        .list_prefix_stream(&prefix)
        .map_err(move |error| CoreError::store(&prefix, &error))
        .boxed()
        .peekable();
    futures::stream::try_unfold(Box::pin(keys), |mut keys| async move {
        let Some(key) = keys.next().await.transpose()? else {
            return Ok(None);
        };
        let listed = if let Some(upload_id) = upload_id_of(&key) {
            let outcome_listed = keys
                .as_mut()
                .next_if(|next| {
                    matches!(next, Ok(next) if upload_outcome_id_of(next).as_ref() == Some(&upload_id))
                })
                .await
                .is_some();
            ListedUpload::Session {
                upload_id,
                outcome_listed,
            }
        } else if let Some(upload_id) = upload_outcome_id_of(&key) {
            ListedUpload::OrphanOutcome { upload_id }
        } else {
            ListedUpload::Unrecognized
        };
        Ok(Some((listed, keys)))
    })
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

/// Lists every upload session in the namespace and calls `visit` with each
/// one, so the caller can charge each session to its budget as it arrives.
///
/// A completed session that `session_cache` has costs no store request. The
/// pass reads one record of every other session, and both records of an
/// aborted one, with at most `STORE_READS_IN_FLIGHT` reads in flight. An
/// outcome record without its session record isn't a root, because nothing
/// can issue admission evidence from it.
pub(super) async fn load_upload_sessions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    session_cache: Option<&CompletedUploadSessionCache>,
    mut visit: impl FnMut(ListedUploadSession) -> Result<()>,
) -> Result<()> {
    let mut sessions = pin!(list_uploads(store, namespace_id)
        .try_filter_map(|listed| future::ok(match listed {
            ListedUpload::Session {
                upload_id,
                outcome_listed,
            } => Some((upload_id, outcome_listed)),
            ListedUpload::OrphanOutcome { .. } | ListedUpload::Unrecognized => None,
        }))
        .map_ok(|(upload_id, outcome_listed)| async move {
            if let Some(session) = session_cache.and_then(|cache| cache.get(&upload_id)) {
                return Ok(SessionLookup::Cached(ListedUploadSession::Completed {
                    upload_id,
                    session,
                }));
            }
            load_listed_session(store, namespace_id, upload_id, outcome_listed)
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
        namespace_id = %namespace_id,
        session_cache_hits = hits,
        session_cache_misses = misses,
        "listed upload sessions"
    );
    Ok(())
}

/// Reads what a pass needs from one listed session: the outcome record if
/// the listing included one, and the session record unless a completed
/// outcome already identifies the content.
async fn load_listed_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: UploadId,
    outcome_listed: bool,
) -> Result<Option<ListedUploadSession>> {
    if outcome_listed {
        if let Some(UploadOutcome::Completed {
            completed_at_ms,
            content_ref,
        }) = load_upload_outcome(store, namespace_id, &upload_id).await?
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
    Ok(load_upload_session_record(store, namespace_id, &upload_id)
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
pub(super) async fn load_swept_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    upload_id: &UploadId,
    outcome_listed: bool,
) -> Result<Option<UploadSessionState>> {
    let outcome = if outcome_listed {
        load_upload_outcome(store, namespace_id, upload_id).await?
    } else {
        None
    };
    Ok(load_upload_session_record(store, namespace_id, upload_id)
        .await?
        .map(|session| UploadSessionState { session, outcome }))
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

pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    grace_window_ms: u64,
    live: &LiveSet,
    context: &MutationContext,
    state: &UploadSessionState,
) -> Result<UploadSessionSweep> {
    if let RetirementStatus::Retained { reclaimable_at_ms } = live.retirement_status() {
        return Ok(UploadSessionSweep::Retain { reclaimable_at_ms });
    }
    match &state.outcome {
        None => abort_expired_session(store, grace_window_ms, context, &state.session).await,
        Some(UploadOutcome::Aborted { aborted_at_ms }) => {
            if context.now_ms.saturating_sub(*aborted_at_ms) < grace_window_ms {
                return Ok(retain_until(aborted_at_ms.saturating_add(grace_window_ms)));
            }
            // Repeat provider cleanup so a later pass completes work left by a crash
            // after the abort.
            if !AbandonedUpload::of(&state.session).release(store).await {
                return Ok(retain_undated());
            }
            // Abort cleanup runs even when no content object was written.
            Ok(UploadSessionSweep::Delete)
        }
        Some(UploadOutcome::Completed {
            completed_at_ms, ..
        }) => Ok(sweep_completed_session(
            live,
            context.now_ms,
            *completed_at_ms,
        )),
    }
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

/// Aborts a session after its lease and grace period expire, then cleans up
/// its unpublished content.
///
/// The outcome record is created with put-if-absent, so this abort can't
/// replace a completion that was recorded first. A completion that arrives
/// after the lease is refused, so the grace only covers a completing host
/// whose clock runs behind the collector's.
async fn abort_expired_session<S: ObjectStore + ?Sized>(
    store: &S,
    grace_window_ms: u64,
    context: &MutationContext,
    session: &UploadSessionPayload,
) -> Result<UploadSessionSweep> {
    let expires_at_ms = session.expires_at_ms;
    if context.now_ms.saturating_sub(expires_at_ms) < grace_window_ms {
        return Ok(retain_until(expires_at_ms.saturating_add(grace_window_ms)));
    }
    let aborted = UploadOutcome::Aborted {
        aborted_at_ms: context.now_ms,
    };
    match create_upload_outcome(store, session, &aborted).await {
        // Keep the newly aborted session until its post-abort grace period expires.
        Ok(OutcomeCreation::Created) => {
            let _ = AbandonedUpload::of(session).release(store).await;
            Ok(retain_until(context.now_ms.saturating_add(grace_window_ms)))
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
