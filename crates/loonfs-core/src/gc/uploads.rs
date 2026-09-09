//! Garbage collection for upload sessions and their content.

use crate::context::MutationContext;
use crate::control_update::{
    load_upload_session_state, try_update_upload_session, CasAttempt, UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::limits::CONTENT_RECLAMATION_GRACE_MS;
use crate::namespace::basis::MetadataBasis;
use crate::namespace::control_snapshot::NamespaceControlSnapshot;
use crate::path::read::{load_metadata_view, LoadedMetadataView, ReadLoadContext};
use crate::protocol::AbandonedUpload;
use crate::storage::content::delete_unpublished_content_object;
use loonfs_api::wire::control::{UploadSessionRecordStatus, UploadSessionState};
use loonfs_api::{ContentStoreId, NamespaceId, UploadId};
use loonfs_objectstore::ObjectStore;
use tokio::sync::OnceCell;

/// The metadata view that says whether completed content was ever
/// published, loaded the first time a call needs it. Most calls never do.
pub(super) struct PublicationView<'a, 'store, S: ObjectStore + ?Sized> {
    store: &'store S,
    namespace_id: &'a NamespaceId,
    snapshot: Option<&'a NamespaceControlSnapshot>,
    basis: &'a MetadataBasis,
    view: OnceCell<LoadedMetadataView<'store, S>>,
}

impl<'a, 'store, S: ObjectStore + ?Sized> PublicationView<'a, 'store, S> {
    /// `snapshot` is `None` on a deleted namespace, whose metadata answers
    /// nothing.
    pub(super) fn new(
        store: &'store S,
        namespace_id: &'a NamespaceId,
        snapshot: Option<&'a NamespaceControlSnapshot>,
        basis: &'a MetadataBasis,
    ) -> Self {
        Self {
            store,
            namespace_id,
            snapshot,
            basis,
            view: OnceCell::new(),
        }
    }

    async fn load(&self) -> Result<Option<&LoadedMetadataView<'store, S>>> {
        let Some(snapshot) = self.snapshot else {
            return Ok(None);
        };
        self.view
            .get_or_try_init(|| {
                load_metadata_view(
                    self.store,
                    self.namespace_id,
                    ReadLoadContext::pinned_head(
                        &snapshot.head.state,
                        &snapshot.head.etag,
                        self.basis,
                        None,
                        None,
                    ),
                )
            })
            .await
            .map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UploadSessionSweep {
    /// The session key survives this pass. It may have advanced a state.
    Retain {
        /// Earliest time this session may be reconsidered when retention is
        /// time-based. `None` means the pass must retry later for a non-time-based
        /// reason, such as a lost CAS or a deleted namespace awaiting retirement.
        reclaimable_at_ms: Option<u64>,
    },
    /// The session has nothing left to say and its key may be deleted.
    Delete {
        /// This sweep also removed the content object the session
        /// completed and nothing ever published.
        reclaimed_content: bool,
    },
}

pub(super) struct UploadSweepContext<'a, S: ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    content_store_id: ContentStoreId,
    retired_content: bool,
    grace_window_ms: u64,
    context: &'a MutationContext,
}

impl<'a, S: ?Sized> UploadSweepContext<'a, S> {
    pub(super) fn new(
        store: &'a S,
        namespace_id: &'a NamespaceId,
        content_store_id: ContentStoreId,
        retired_content: bool,
        grace_window_ms: u64,
        context: &'a MutationContext,
    ) -> Self {
        Self {
            store,
            namespace_id,
            content_store_id,
            retired_content,
            grace_window_ms,
            context,
        }
    }
}

/// Advances one upload session and reclaims content it no longer owns.
///
/// State transitions use a CAS against the ETag that was read with the
/// session. A CAS conflict keeps the session for a later pass. Provider
/// cleanup runs only after the durable state transition, so a crash may leave
/// extra data to clean up but cannot delete data from an open session.
pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    sweep: &UploadSweepContext<'_, S>,
    upload_id: &UploadId,
    view: &PublicationView<'_, '_, S>,
) -> Result<UploadSessionSweep> {
    // This read selects the lifecycle branch only. Any state change is applied
    // through a later CAS using a fresh ETag.
    let state = match load_upload_session_state(sweep.store, sweep.namespace_id, upload_id).await {
        Ok(state) => state,
        Err(CoreError::UploadNotFound { .. }) => return Ok(retain_undated()),
        Err(error) => return Err(error),
    };

    match state.status {
        UploadSessionRecordStatus::Open { expires_at_ms, .. } => {
            abort_expired_session(sweep, upload_id, expires_at_ms).await
        }
        UploadSessionRecordStatus::Aborted { aborted_at_ms } => {
            if sweep.context.now_ms.saturating_sub(aborted_at_ms) < sweep.grace_window_ms {
                return Ok(retain_until(
                    aborted_at_ms.saturating_add(sweep.grace_window_ms),
                ));
            }
            // Repeat provider cleanup so a later pass completes work left by a crash
            // after the abort CAS.
            if !AbandonedUpload::of(&state)
                .release(sweep.store, &sweep.content_store_id)
                .await
            {
                return Ok(retain_undated());
            }
            // Do not count this as reclaimed content. Abort cleanup runs even when no
            // object was written. Only a completed session with an `Absent` reference
            // result proves that a content object was eligible for reclamation.
            Ok(UploadSessionSweep::Delete {
                reclaimed_content: false,
            })
        }
        UploadSessionRecordStatus::Completed {
            completed_at_ms,
            content_ref,
        } => {
            if !sweep.retired_content
                && sweep.context.now_ms.saturating_sub(completed_at_ms)
                    < CONTENT_RECLAMATION_GRACE_MS
            {
                return Ok(retain_until(
                    completed_at_ms.saturating_add(CONTENT_RECLAMATION_GRACE_MS),
                ));
            }
            let reference = if sweep.retired_content {
                ContentReference::Absent
            } else {
                match view.load().await? {
                    Some(view)
                        if view
                            .metadata_view()
                            .find_content_publication(&content_ref.content_id)
                            .await?
                            .is_some() =>
                    {
                        ContentReference::Referenced
                    }
                    Some(_) => ContentReference::Absent,
                    None => ContentReference::Unknown,
                }
            };
            match reference {
                ContentReference::Unknown => Ok(retain_undated()),
                // Metadata now owns the published content. Delete only the completed
                // upload-session record.
                ContentReference::Referenced => Ok(UploadSessionSweep::Delete {
                    reclaimed_content: false,
                }),
                ContentReference::Absent => {
                    if !delete_unpublished_content_object(
                        sweep.store,
                        &sweep.content_store_id,
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
    sweep: &UploadSweepContext<'_, S>,
    upload_id: &UploadId,
    expires_at_ms: u64,
) -> Result<UploadSessionSweep> {
    if sweep.context.now_ms.saturating_sub(expires_at_ms) < sweep.grace_window_ms {
        return Ok(retain_until(
            expires_at_ms.saturating_add(sweep.grace_window_ms),
        ));
    }
    let aborted = try_update_upload_session(
        sweep.store,
        sweep.namespace_id,
        upload_id,
        |mut state: UploadSessionState| async move {
            if !matches!(state.status, UploadSessionRecordStatus::Open { .. }) {
                return Ok(UploadSessionUpdate::Noop(None));
            }
            let abandoned = AbandonedUpload::of(&state);
            state.status = UploadSessionRecordStatus::Aborted {
                aborted_at_ms: sweep.context.now_ms,
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
            let _ = abandoned
                .release(sweep.store, &sweep.content_store_id)
                .await;
            Ok(retain_until(
                sweep.context.now_ms.saturating_add(sweep.grace_window_ms),
            ))
        }
        Ok(CasAttempt::Settled(None)) => Ok(retain_undated()),
        Ok(CasAttempt::Contended(_)) => {
            tracing::debug!(
                namespace_id = %sweep.namespace_id,
                upload_id = %upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(retain_undated())
        }
        Ok(CasAttempt::Ambiguous(error, ())) => Err(CoreError::store(
            loonfs_objectstore::keys::upload_session(sweep.namespace_id, upload_id),
            &error,
        )),
        Err(CoreError::UploadNotFound { .. }) => Ok(retain_undated()),
        Err(error) => Err(error),
    }
}

/// Whether the current view can decide one content object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContentReference {
    Referenced,
    Absent,
    Unknown,
}
