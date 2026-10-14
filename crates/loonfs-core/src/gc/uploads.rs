//! Garbage collection for upload sessions and their content.

use super::live_set::{LiveSet, RetirementStatus};
use super::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::context::MutationContext;
use crate::control_update::{
    load_upload_session_state, try_update_upload_session, CasOutcome, UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::limits::CONTENT_RECLAMATION_GRACE_WINDOW_MS;
use crate::protocol::AbandonedUpload;
use futures::{future, TryStreamExt};
use loonfs_objectstore::keys::upload_session_prefix;
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{
    UploadAbortOutcome, UploadAlreadyCompletedError, UploadSessionPayload,
    UploadSessionRecordStatus,
};
use loonfs_types::{ContentId, NamespaceId, UploadId};
use std::pin::pin;

/// One upload session that a pass listed, with what the pass needs from it.
pub(super) enum ListedUploadSession {
    /// A completed session, from the cache or from its record.
    Completed {
        upload_id: UploadId,
        session: CompletedUploadSession,
    },
    /// An open or aborted session, whose record the sweep reads again.
    Incomplete { content_id: ContentId },
}

impl ListedUploadSession {
    fn of(state: UploadSessionPayload) -> Self {
        match state.status {
            UploadSessionRecordStatus::Completed {
                completed_at_ms, ..
            } => Self::Completed {
                upload_id: state.upload_id,
                session: CompletedUploadSession {
                    content_id: state.content_id,
                    completed_at_ms,
                },
            },
            UploadSessionRecordStatus::Open { .. } | UploadSessionRecordStatus::Aborted { .. } => {
                Self::Incomplete {
                    content_id: state.content_id,
                }
            }
        }
    }
}

/// Where a pass found one listed upload session.
enum SessionLookup {
    /// The cache had the session.
    Cached(ListedUploadSession),
    /// The pass read the record. `None` means that another pass deleted the
    /// record after this pass listed it.
    Read(Option<ListedUploadSession>),
}

/// Lists every upload session in the namespace and calls `visit` with each
/// one, so the caller can charge each session to its budget as it arrives.
///
/// A completed session that `session_cache` has costs no store request. The
/// pass reads every other record, with at most `STORE_READS_IN_FLIGHT` reads
/// in flight.
pub(super) async fn load_upload_sessions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    session_cache: Option<&CompletedUploadSessionCache>,
    mut visit: impl FnMut(ListedUploadSession) -> Result<()>,
) -> Result<()> {
    let prefix = upload_session_prefix(namespace_id);
    let mut sessions = pin!(store
        .list_prefix_stream(&prefix)
        .map_err(|error| CoreError::store(&prefix, &error))
        .try_filter_map(|key| future::ok(upload_id_of(&key)))
        .map_ok(|upload_id| async move {
            if let Some(session) = session_cache.and_then(|cache| cache.get(&upload_id)) {
                return Ok(SessionLookup::Cached(ListedUploadSession::Completed {
                    upload_id,
                    session,
                }));
            }
            match load_upload_session_state(store, namespace_id, &upload_id).await {
                Ok(state) => Ok(SessionLookup::Read(Some(ListedUploadSession::of(state)))),
                Err(CoreError::UploadNotFound { .. }) => Ok(SessionLookup::Read(None)),
                Err(error) => Err(error),
            }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UploadSessionSweep {
    /// The session key survives this pass. It may have advanced a state.
    Retain {
        /// Earliest time this session may be reconsidered when retention is
        /// time-based. `None` means the pass must retry later for a non-time-based
        /// reason, such as a lost CAS or failed cleanup.
        reclaimable_at_ms: Option<u64>,
    },
    /// The session has nothing left to say and its key may be deleted.
    Delete,
}

pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    grace_window_ms: u64,
    live: &LiveSet,
    context: &MutationContext,
    state: &UploadSessionPayload,
) -> Result<UploadSessionSweep> {
    if let RetirementStatus::Retained { reclaimable_at_ms } = live.retirement_status() {
        return Ok(UploadSessionSweep::Retain { reclaimable_at_ms });
    }
    match &state.status {
        UploadSessionRecordStatus::Open { expires_at_ms, .. } => {
            abort_expired_session(store, grace_window_ms, context, state, *expires_at_ms).await
        }
        UploadSessionRecordStatus::Aborted { aborted_at_ms } => {
            if context.now_ms.saturating_sub(*aborted_at_ms) < grace_window_ms {
                return Ok(retain_until(aborted_at_ms.saturating_add(grace_window_ms)));
            }
            // Repeat provider cleanup so a later pass completes work left by a crash
            // after the abort CAS.
            if !AbandonedUpload::of(state).release(store).await {
                return Ok(retain_undated());
            }
            // Abort cleanup runs even when no content object was written.
            Ok(UploadSessionSweep::Delete)
        }
        UploadSessionRecordStatus::Completed {
            completed_at_ms, ..
        } => Ok(sweep_completed_session(
            live,
            context.now_ms,
            *completed_at_ms,
        )),
    }
}

/// Returns whether a completed session's record is kept or deleted, using
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
/// The CAS provides safety. A completion that arrives after the lease is
/// refused, so the grace only covers a completing host whose clock runs
/// behind the collector's.
async fn abort_expired_session<S: ObjectStore + ?Sized>(
    store: &S,
    grace_window_ms: u64,
    context: &MutationContext,
    state: &UploadSessionPayload,
    expires_at_ms: u64,
) -> Result<UploadSessionSweep> {
    if context.now_ms.saturating_sub(expires_at_ms) < grace_window_ms {
        return Ok(retain_until(expires_at_ms.saturating_add(grace_window_ms)));
    }
    let aborted = try_update_upload_session(
        store,
        &state.namespace_id,
        &state.upload_id,
        |mut state: UploadSessionPayload| async move {
            let abandoned = AbandonedUpload::of(&state);
            match state.status.abort(context.now_ms) {
                Ok(UploadAbortOutcome::Aborted) => Ok(UploadSessionUpdate::Replace {
                    next: Box::new(state),
                    outcome: Some(abandoned),
                }),
                Ok(UploadAbortOutcome::AlreadyAborted { .. })
                | Err(UploadAlreadyCompletedError) => Ok(UploadSessionUpdate::Noop(None)),
            }
        },
    )
    .await;
    let aborted = match aborted {
        Ok(aborted) => aborted,
        // Another pass removed the record after this one listed it.
        Err(CoreError::UploadNotFound { .. }) => return Ok(retain_undated()),
        Err(error) => return Err(error),
    };
    match aborted {
        // Keep the newly aborted record until its post-abort grace period expires.
        CasOutcome::Settled(Some(abandoned)) => {
            let _ = abandoned.release(store).await;
            Ok(retain_until(context.now_ms.saturating_add(grace_window_ms)))
        }
        CasOutcome::Settled(None) => Ok(retain_undated()),
        CasOutcome::Contended(_) => {
            tracing::debug!(
                namespace_id = %state.namespace_id,
                upload_id = %state.upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(retain_undated())
        }
    }
}
