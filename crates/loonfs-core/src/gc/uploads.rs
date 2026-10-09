//! Garbage collection for upload sessions and their content.

use super::live_set::RetirementState;
use super::sweep::Sweep;
use crate::control_update::{
    load_upload_session_state, try_update_upload_session, CasAttempt, UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::limits::CONTENT_RECLAMATION_GRACE_MS;
use crate::protocol::AbandonedUpload;
use futures::StreamExt;
use loonfs_objectstore::keys::upload_session_prefix;
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::{UploadSessionPayload, UploadSessionRecordStatus};
use loonfs_types::{ContentId, NamespaceId};

/// Hands the content ID of every upload session record in the namespace to
/// `protect`, one record at a time, so the caller's budget sees each id as
/// it arrives.
pub(super) async fn protect_session_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    mut protect: impl FnMut(ContentId) -> Result<()>,
) -> Result<()> {
    let prefix = upload_session_prefix(namespace_id);
    let mut listing = store.list_prefix_stream(&prefix);
    while let Some(key) = listing
        .next()
        .await
        .transpose()
        .map_err(|error| CoreError::store(&prefix, &error))?
    {
        let Some(upload_id) = upload_id_of(&key) else {
            continue;
        };
        match load_upload_session_state(store, namespace_id, &upload_id).await {
            Ok(state) => protect(state.content_id)?,
            Err(CoreError::UploadNotFound { .. }) => {}
            Err(error) => return Err(error),
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
    sweep: &Sweep<'_, S>,
    state: &UploadSessionPayload,
) -> Result<UploadSessionSweep> {
    match sweep.live.retirement_state() {
        RetirementState::Retained { until_ms } => {
            return Ok(UploadSessionSweep::Retain {
                reclaimable_at_ms: until_ms,
            })
        }
        RetirementState::Active | RetirementState::Eligible => {}
    }
    let retired = sweep.live.retirement_state() == RetirementState::Eligible;
    match &state.status {
        UploadSessionRecordStatus::Open { expires_at_ms, .. } => {
            abort_expired_session(sweep, state, *expires_at_ms).await
        }
        UploadSessionRecordStatus::Aborted { aborted_at_ms } => {
            if sweep.mutation.now_ms.saturating_sub(*aborted_at_ms) < sweep.grace_window_ms {
                return Ok(retain_until(
                    aborted_at_ms.saturating_add(sweep.grace_window_ms),
                ));
            }
            // Repeat provider cleanup so a later pass completes work left by a crash
            // after the abort CAS.
            if !AbandonedUpload::of(state).release(sweep.store).await {
                return Ok(retain_undated());
            }
            // Abort cleanup runs even when no content object was written.
            Ok(UploadSessionSweep::Delete)
        }
        UploadSessionRecordStatus::Completed {
            completed_at_ms, ..
        } => {
            if !retired
                && sweep.mutation.now_ms.saturating_sub(*completed_at_ms)
                    < CONTENT_RECLAMATION_GRACE_MS
            {
                return Ok(retain_until(
                    completed_at_ms.saturating_add(CONTENT_RECLAMATION_GRACE_MS),
                ));
            }
            Ok(UploadSessionSweep::Delete)
        }
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
    sweep: &Sweep<'_, S>,
    state: &UploadSessionPayload,
    expires_at_ms: u64,
) -> Result<UploadSessionSweep> {
    if sweep.mutation.now_ms.saturating_sub(expires_at_ms) < sweep.grace_window_ms {
        return Ok(retain_until(
            expires_at_ms.saturating_add(sweep.grace_window_ms),
        ));
    }
    let aborted = try_update_upload_session(
        sweep.store,
        &state.namespace_id,
        &state.upload_id,
        |mut state: UploadSessionPayload| async move {
            if !matches!(state.status, UploadSessionRecordStatus::Open { .. }) {
                return Ok(UploadSessionUpdate::Noop(None));
            }
            let abandoned = AbandonedUpload::of(&state);
            state.status = UploadSessionRecordStatus::Aborted {
                aborted_at_ms: sweep.mutation.now_ms,
            };
            Ok(UploadSessionUpdate::Replace {
                next: Box::new(state),
                outcome: Some(abandoned),
            })
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
        CasAttempt::Settled(Some(abandoned)) => {
            let _ = abandoned.release(sweep.store).await;
            Ok(retain_until(
                sweep.mutation.now_ms.saturating_add(sweep.grace_window_ms),
            ))
        }
        CasAttempt::Settled(None) => Ok(retain_undated()),
        CasAttempt::Contended(_) => {
            tracing::debug!(
                namespace_id = %state.namespace_id,
                upload_id = %state.upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(retain_undated())
        }
    }
}
