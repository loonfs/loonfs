//! Garbage collection for upload sessions and their content.

use super::live_set::RetirementState;
use super::sweep::Sweep;
use crate::control_update::{try_update_upload_session, CasAttempt, UploadSessionUpdate};
use crate::error::{CoreError, Result};
use crate::limits::CONTENT_RECLAMATION_GRACE_MS;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::read_anchor::NamespaceReadAnchor;
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use crate::protocol::AbandonedUpload;
use crate::storage::content::delete_unpublished_content_object;
use loonfs_api::wire::control::{UploadSessionPayload, UploadSessionRecordStatus};
use loonfs_api::NamespaceId;
use loonfs_objectstore::ObjectStore;
use tokio::sync::OnceCell;

/// The metadata view that says whether completed content was ever
/// published, loaded the first time a call needs it. Most calls never do.
pub(super) struct PublicationView<'a, 'store, S: ObjectStore + ?Sized> {
    store: &'store S,
    namespace_id: &'a NamespaceId,
    anchor: &'a NamespaceReadAnchor,
    basis: &'a MetadataBasis,
    view: OnceCell<LoadedMetadataView<'store, S>>,
}

impl<'a, 'store, S: ObjectStore + ?Sized> PublicationView<'a, 'store, S> {
    pub(super) fn new(
        store: &'store S,
        namespace_id: &'a NamespaceId,
        anchor: &'a NamespaceReadAnchor,
        basis: &'a MetadataBasis,
    ) -> Self {
        Self {
            store,
            namespace_id,
            anchor,
            basis,
            view: OnceCell::new(),
        }
    }

    async fn load(&self) -> Result<&LoadedMetadataView<'store, S>> {
        self.view
            .get_or_try_init(|| {
                load_metadata_view(
                    self.store,
                    self.namespace_id,
                    ReadLoadContext::pinned_head(&self.anchor.read_state, self.basis, None, None),
                )
            })
            .await
    }
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
    Delete {
        /// This sweep also removed the content object the session
        /// completed and nothing ever published.
        reclaimed_content: bool,
    },
}

pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    sweep: &Sweep<'_, '_, S>,
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
    let retired_content = sweep.live.retirement_state() == RetirementState::Eligible;
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
            Ok(UploadSessionSweep::Delete {
                reclaimed_content: false,
            })
        }
        UploadSessionRecordStatus::Completed {
            completed_at_ms,
            content_ref,
        } => {
            if !retired_content
                && sweep.mutation.now_ms.saturating_sub(*completed_at_ms)
                    < CONTENT_RECLAMATION_GRACE_MS
            {
                return Ok(retain_until(
                    completed_at_ms.saturating_add(CONTENT_RECLAMATION_GRACE_MS),
                ));
            }
            let published = !retired_content
                && sweep
                    .view
                    .load()
                    .await?
                    .metadata_view()
                    .find_content_publication(&content_ref.content_id)
                    .await?
                    .is_some();
            if published {
                return Ok(UploadSessionSweep::Delete {
                    reclaimed_content: false,
                });
            }
            if !delete_unpublished_content_object(
                sweep.store,
                &state.namespace_id,
                &state.content_id,
            )
            .await
            {
                return Ok(retain_undated());
            }
            Ok(UploadSessionSweep::Delete {
                reclaimed_content: true,
            })
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
/// The CAS provides safety. The additional grace period only reduces races
/// with completions that arrive shortly after lease expiry.
async fn abort_expired_session<S: ObjectStore + ?Sized>(
    sweep: &Sweep<'_, '_, S>,
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
    match aborted {
        // Keep the newly aborted record until its post-abort grace period expires.
        Ok(CasAttempt::Settled(Some(abandoned))) => {
            let _ = abandoned.release(sweep.store).await;
            Ok(retain_until(
                sweep.mutation.now_ms.saturating_add(sweep.grace_window_ms),
            ))
        }
        Ok(CasAttempt::Settled(None)) => Ok(retain_undated()),
        Ok(CasAttempt::Contended(_)) => {
            tracing::debug!(
                namespace_id = %state.namespace_id,
                upload_id = %state.upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(retain_undated())
        }
        Ok(CasAttempt::Ambiguous(error, ())) => Err(CoreError::store(
            loonfs_objectstore::keys::upload_session(&state.namespace_id, &state.upload_id),
            &error,
        )),
        Err(CoreError::UploadNotFound { .. }) => Ok(retain_undated()),
        Err(error) => Err(error),
    }
}
