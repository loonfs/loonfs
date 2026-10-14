//! Garbage collection for upload sessions and their content.

use super::live_set::{LiveSet, RetirementStatus};
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
use loonfs_types::NamespaceId;
use std::pin::pin;

/// Reads every upload session record in the namespace, with at most
/// `STORE_READS_IN_FLIGHT` reads in flight, and calls `visit` with each record
/// as soon as its read finishes, so the caller can charge each record to its
/// budget as the record arrives.
pub(super) async fn load_upload_sessions<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut visit: impl FnMut(UploadSessionPayload) -> Result<()>,
) -> Result<()> {
    let prefix = upload_session_prefix(namespace_id);
    let mut sessions = pin!(store
        .list_prefix_stream(&prefix)
        .map_err(|error| CoreError::store(&prefix, &error))
        .try_filter_map(|key| future::ok(upload_id_of(&key)))
        .map_ok(|upload_id| async move {
            match load_upload_session_state(store, namespace_id, &upload_id).await {
                Ok(state) => Ok(Some(state)),
                Err(CoreError::UploadNotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
        })
        .try_buffer_unordered(STORE_READS_IN_FLIGHT));
    while let Some(state) = sessions.try_next().await? {
        if let Some(state) = state {
            visit(state)?;
        }
    }
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
