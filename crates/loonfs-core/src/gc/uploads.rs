//! Garbage collection for upload sessions and their content.
//!
//! Before completion, an expired open session can be aborted and its
//! unpublished object deleted using only the session record.
//!
//! After completion, content may already be referenced by metadata. The
//! collector waits for `CONTENT_RECLAMATION_GRACE_MS`, then scans namespace
//! metadata before deleting the object. The grace period covers the receipt
//! lifetime and any publication that receipt can authorize, so no new
//! reference can appear after the scan becomes eligible.

use super::budget::PassBudget;
use super::live_set::LiveSet;
use crate::checkpoint::{load_verified_manifest_tables, ManifestLoadFailureClass};
use crate::context::MutationContext;
use crate::control_update::{
    load_upload_session_state, try_update_upload_session, UploadSessionCas, UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::limits::CONTENT_RECLAMATION_GRACE_MS;
use crate::protocol::AbandonedUpload;
use crate::storage::content::delete_unpublished_content_object;
use loonfs_api::wire::control::{UploadSessionRecordStatus, UploadSessionState};
use loonfs_api::wire::manifest::{lookup_keys, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{ContentId, ContentStoreId, NamespaceId, UploadId};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Rows read from one metadata table per request while collecting content
/// references. Each wave is one page of rows and costs one budget unit, so
/// this sets both how much of a revision family is held in memory at a time
/// and how finely the scan can be interrupted.
const REVISION_SCAN_WAVE_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UploadSessionSweep {
    /// The session key survives this pass. It may have advanced a state.
    Retain {
        /// Earliest time this session may be reconsidered when retention is
        /// time-based. `None` means the pass must retry later for a non-time-based
        /// reason, such as a lost CAS or an incomplete reference scan.
        reclaimable_at_ms: Option<u64>,
    },
    /// The session has nothing left to say and its key may be deleted.
    Delete {
        /// This sweep also removed the content object the session
        /// completed and nothing ever published.
        reclaimed_content: bool,
    },
    /// The remaining pass budget was too small to scan content references. Keep
    /// the session, disable completed-content reclamation for this invocation,
    /// and continue sweeping other candidates.
    ContentReclamationDeferred,
}

/// Advances one upload session and reclaims content it no longer owns.
///
/// State transitions use a CAS against the ETag that was read with the
/// session. A CAS conflict keeps the session for a later pass. Provider
/// cleanup runs only after the durable state transition, so a crash may leave
/// extra data to clean up but cannot delete data from an open session.
#[allow(
    clippy::too_many_arguments,
    reason = "the sweep decision needs session state, policy, budget, and context together"
)]
pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    grace_window_ms: u64,
    references: &mut ContentReferences,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<UploadSessionSweep> {
    // This read selects the lifecycle branch only. Any state change is applied
    // through a later CAS using a fresh ETag.
    let state = match load_upload_session_state(store, namespace_id, upload_id).await {
        Ok(state) => state,
        Err(CoreError::UploadNotFound { .. }) => return Ok(retain_undated()),
        Err(error) => return Err(error),
    };

    match state.status {
        UploadSessionRecordStatus::Open { expires_at_ms, .. } => {
            abort_expired_session(
                store,
                namespace_id,
                content_store_id,
                upload_id,
                expires_at_ms,
                grace_window_ms,
                context,
            )
            .await
        }
        UploadSessionRecordStatus::Aborted { aborted_at_ms } => {
            if context.now_ms.saturating_sub(aborted_at_ms) < grace_window_ms {
                return Ok(retain_until(aborted_at_ms.saturating_add(grace_window_ms)));
            }
            // Repeat provider cleanup so a later pass completes work left by a crash
            // after the abort CAS.
            AbandonedUpload::of(&state)
                .release(store, content_store_id)
                .await;
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
            if context.now_ms.saturating_sub(completed_at_ms) < CONTENT_RECLAMATION_GRACE_MS {
                return Ok(retain_until(
                    completed_at_ms.saturating_add(CONTENT_RECLAMATION_GRACE_MS),
                ));
            }
            match references
                .lookup(store, namespace_id, &content_ref.content_id, budget)
                .await?
            {
                ContentReference::Unknown => Ok(retain_undated()),
                ContentReference::Deferred => Ok(UploadSessionSweep::ContentReclamationDeferred),
                // Metadata now owns the published content. Delete only the completed
                // upload-session record.
                ContentReference::Referenced => Ok(UploadSessionSweep::Delete {
                    reclaimed_content: false,
                }),
                ContentReference::Absent => {
                    delete_unpublished_content_object(
                        store,
                        content_store_id,
                        &content_ref.content_id,
                    )
                    .await;
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
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    expires_at_ms: u64,
    grace_window_ms: u64,
    context: &MutationContext,
) -> Result<UploadSessionSweep> {
    if context.now_ms.saturating_sub(expires_at_ms) < grace_window_ms {
        return Ok(retain_until(expires_at_ms.saturating_add(grace_window_ms)));
    }
    let aborted = try_update_upload_session(
        store,
        namespace_id,
        upload_id,
        |mut state: UploadSessionState| async move {
            if !matches!(state.status, UploadSessionRecordStatus::Open { .. }) {
                return Ok(UploadSessionUpdate::Noop(None));
            }
            let abandoned = AbandonedUpload::of(&state);
            state.status = UploadSessionRecordStatus::Aborted {
                aborted_at_ms: context.now_ms,
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
        Ok(UploadSessionCas::Applied(Some(abandoned))) => {
            abandoned.release(store, content_store_id).await;
            Ok(retain_until(context.now_ms.saturating_add(grace_window_ms)))
        }
        Ok(UploadSessionCas::Applied(None)) => Ok(retain_undated()),
        Ok(UploadSessionCas::Conflict) => {
            tracing::debug!(
                namespace_id = %namespace_id,
                upload_id = %upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(retain_undated())
        }
        Err(CoreError::UploadNotFound { .. }) => Ok(retain_undated()),
        Err(error) => Err(error),
    }
}

/// Whether the namespace's metadata references one content object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentReference {
    Referenced,
    Absent,
    /// The reference set could not be established, so nothing is reclaimed
    /// (format spec, "Garbage collection", rule 5).
    Unknown,
    /// The set did not fit in the budget the pass had left, so content
    /// reclamation is skipped for the rest of this invocation. It is not an
    /// answer of any kind — see [`CollectedReferences::Deferred`].
    Deferred,
}

/// What the reference scan has produced for this invocation.
#[derive(Debug)]
pub(super) enum CollectedReferences {
    /// The scan has not run yet. Only the memo starts here.
    NotYet,
    /// A root could not be read, so this pass has no reference set at all
    /// (format spec, "Garbage collection", rule 5).
    Unavailable,
    /// The reference scan exceeded the pass budget. Do not retry it during this
    /// invocation, and discard the partial result because missing IDs could make
    /// live content appear unreferenced.
    Deferred,
    /// Every root was read. The set is complete and may decide deletions.
    Referenced(BTreeSet<ContentId>),
}

/// Memoized set of content IDs referenced by live namespace metadata.
///
/// The set is collected at most once per garbage-collection invocation and
/// only when an aged completed session needs it. It is not stored in the
/// pagination cursor because the cursor records position, not permission to
/// delete. A resumed invocation performs a new budgeted scan.
pub(super) struct ContentReferences {
    live: Arc<LiveSet>,
    collected: CollectedReferences,
}

impl ContentReferences {
    pub(super) fn over(live: Arc<LiveSet>) -> Self {
        Self {
            live,
            collected: CollectedReferences::NotYet,
        }
    }

    async fn lookup<S: ObjectStore + ?Sized>(
        &mut self,
        store: &S,
        namespace_id: &NamespaceId,
        content_id: &ContentId,
        budget: &mut PassBudget,
    ) -> Result<ContentReference> {
        if matches!(self.collected, CollectedReferences::NotYet) {
            self.collected = if self.live.degraded {
                CollectedReferences::Unavailable
            } else {
                collect_referenced_content(store, namespace_id, &self.live, budget).await?
            };
        }
        Ok(match &self.collected {
            CollectedReferences::NotYet | CollectedReferences::Unavailable => {
                ContentReference::Unknown
            }
            CollectedReferences::Deferred => ContentReference::Deferred,
            CollectedReferences::Referenced(referenced) => {
                if referenced.contains(content_id) {
                    ContentReference::Referenced
                } else {
                    ContentReference::Absent
                }
            }
        })
    }
}

/// Collects every content ID reachable from protected manifests and the
/// retained WAL chain. Protected manifests include fork bases pinned by
/// checkpoints. Other namespaces cannot reference these content IDs because
/// each namespace publishes content created by its own upload sessions.
///
/// Only the manifest half is read here. The chain's references were
/// harvested off the bodies marking already decoded, so this half is a set
/// union rather than a second pass over the same objects.
///
/// Every manifest root read costs a budget unit — one per manifest opened,
/// one per page of revision rows — so this scan is part of the pass's bound
/// rather than an exception to it. Running out stops the scan where it
/// stands and reports [`CollectedReferences::Deferred`]; the caller retains
/// the session that triggered it, marks the pass as having deferred content
/// reclamation, and carries on. A namespace whose scan never fits therefore
/// keeps its completed content — `max_objects` has to be at least the
/// scan's size for content reclamation to happen at all — but the sweep
/// around it still advances, which is the difference between leaking
/// content for a while and not collecting anything ever.
///
/// Every return is a verdict the memo can hold: an attempt that has run
/// never comes back as [`CollectedReferences::NotYet`].
pub(super) async fn collect_referenced_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    live: &LiveSet,
    budget: &mut PassBudget,
) -> Result<CollectedReferences> {
    let mut referenced = BTreeSet::new();
    for manifest_object_id in &live.manifests {
        if !budget.try_charge() {
            return Ok(CollectedReferences::Deferred);
        }
        let tables =
            match load_verified_manifest_tables(store, namespace_id, manifest_object_id).await {
                Ok(tables) => tables,
                Err(error) => match error.failure_class() {
                    ManifestLoadFailureClass::Store => {
                        tracing::warn!(
                            namespace_id = %namespace_id,
                            manifest_object_id = %manifest_object_id,
                            error = %error,
                            "a content-reference manifest did not read; retaining completed content"
                        );
                        return Ok(CollectedReferences::Unavailable);
                    }
                    ManifestLoadFailureClass::Corrupt => {
                        return Err(CoreError::NamespaceCorrupt(format!(
                            "a content-reference manifest does not load: {error}"
                        )));
                    }
                },
            };
        let mut lower_bound = lookup_keys::REVISION_ROW_PREFIX.to_owned();
        let upper_bound = string_prefix_upper_bound(lookup_keys::REVISION_ROW_PREFIX);
        loop {
            if !budget.try_charge() {
                return Ok(CollectedReferences::Deferred);
            }
            let rows = match tables
                .scan_range_page_with_keys(
                    MetadataTableFamily::Revisions,
                    &lower_bound,
                    upper_bound.as_deref(),
                    REVISION_SCAN_WAVE_ROWS,
                )
                .await
            {
                Ok(rows) => rows,
                Err(error) => match error.failure_class() {
                    ManifestLoadFailureClass::Store => {
                        tracing::warn!(
                            namespace_id = %namespace_id,
                            manifest_object_id = %manifest_object_id,
                            error = %error,
                            "content-reference rows did not read; retaining completed content"
                        );
                        return Ok(CollectedReferences::Unavailable);
                    }
                    ManifestLoadFailureClass::Corrupt => {
                        return Err(CoreError::NamespaceCorrupt(format!(
                            "content-reference rows do not load: {error}"
                        )));
                    }
                },
            };
            let exhausted = rows.len() < REVISION_SCAN_WAVE_ROWS;
            match rows.last() {
                Some((row_key, _)) => lower_bound = format!("{row_key}\0"),
                None => break,
            }
            for (_, row) in rows {
                if let MetadataRow::Revision { content_ref, .. } = row {
                    referenced.insert(content_ref.content_id);
                }
            }
            if exhausted {
                break;
            }
        }
    }

    // The retained chain's own references came back with the marking that
    // validated it, already paid for there, so no segment is fetched twice
    // in one pass.
    referenced.extend(live.wal_content_ids.iter().cloned());

    Ok(CollectedReferences::Referenced(referenced))
}
