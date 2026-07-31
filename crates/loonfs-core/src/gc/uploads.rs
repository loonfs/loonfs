//! Upload and content reclamation, split at the completed line.
//!
//! Everything before a session completes belongs to upload collection: an
//! open session whose lease has passed is aborted and the object it was
//! writing is deleted, and that reasoning is entirely session-local, because
//! a random content id that was never published can only ever have had one
//! owner.
//!
//! Everything at or after completion belongs to content collection, which is
//! the harder half: completed content may or may not have been published, and
//! the only honest way to tell is to look at what the namespace's metadata
//! actually references. That is decidable — rather than a race against
//! writers — because a reference can only enter metadata through a receipt,
//! receipts stop being minted a fixed window after completion, and
//! `CONTENT_RECLAMATION_GRACE_MS` outlasts that window plus the last
//! receipt's life plus the publication it could admit. Past the grace, the
//! set of references to a completed session's content can no longer grow.

use super::budget::PassBudget;
use super::live_set::LiveSet;
use crate::checkpoint::load_verified_manifest_tables;
use crate::context::MutationContext;
use crate::control_update::{
    read_upload_session_state, try_update_upload_session, UploadSessionCas, UploadSessionUpdate,
};
use crate::error::{CoreError, Result};
use crate::limits::CONTENT_RECLAMATION_GRACE_MS;
use crate::protocol::AbandonedUpload;
use crate::storage::content::delete_unpublished_content_object;
use loonfs_api::wire::control::{UploadSessionLifecycle, UploadSessionState};
use loonfs_api::wire::manifest::{lookup_keys, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::wire::wal::{decode_wal_segment_envelope_zstd, WalDelta};
use loonfs_api::{ContentId, ContentStoreId, NamespaceId, UploadId};
use loonfs_objectstore::ObjectStore;
use std::collections::BTreeSet;

/// Rows read from one metadata table per request while collecting content
/// references. Each wave is one page of rows and costs one budget unit, so
/// this sets both how much of a revision family is held in memory at a time
/// and how finely the scan can be interrupted.
const REVISION_SCAN_WAVE_ROWS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UploadSessionSweep {
    /// The session key survives this pass. It may have advanced a state.
    Retain,
    /// The session has nothing left to say and its key may be deleted.
    Delete {
        /// This sweep also removed the content object the session
        /// completed and nothing ever published.
        reclaimed_content: bool,
    },
    /// The pass ran out of budget before it could establish whether this
    /// session's content is referenced. Nothing was decided and nothing was
    /// deleted; the pass stops here and its cursor still points before this
    /// session's key, so a resume reconsiders it from the start.
    BudgetExhausted,
}

/// Advances one upload session and reclaims whatever it stops owning.
///
/// Every transition here is a compare-and-swap on exactly the etag inspected
/// with the state, and a lost swap retains without retrying, so a racing
/// completion can never be overwritten by a second read. Provider cleanup
/// always follows the durable transition: a crash in between leaves an
/// object the next pass deletes from the terminal record, never an object
/// deleted out from under a session that is still open.
#[allow(clippy::too_many_arguments)]
pub(super) async fn sweep_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    upload_id: &UploadId,
    grace_window_ms: u64,
    references: &mut ContentReferences<'_>,
    budget: &mut PassBudget,
    context: &MutationContext,
) -> Result<UploadSessionSweep> {
    // Reading first only selects the arm. Nothing here acts on this
    // observation without re-reading under an etag, so a session that
    // changes underneath the read is decided correctly anyway.
    let state = match read_upload_session_state(store, namespace_id, upload_id).await {
        Ok(state) => state,
        Err(CoreError::UploadNotFound { .. }) => return Ok(UploadSessionSweep::Retain),
        Err(error) => return Err(error),
    };

    match state.state {
        UploadSessionLifecycle::Open { expires_at_ms } => {
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
        UploadSessionLifecycle::Aborted { aborted_at_ms } => {
            if context.now_ms.saturating_sub(aborted_at_ms) < grace_window_ms {
                return Ok(UploadSessionSweep::Retain);
            }
            // Repeating the abort's own cleanup is what makes a crash
            // between the abort swap and its provider work cost nothing.
            AbandonedUpload::of(&state)
                .release(store, content_store_id)
                .await;
            // Not counted as a reclaimed content object: this delete is
            // unconditional cleanup that runs whether or not the session
            // ever wrote anything, and it repeats on every pass until the
            // record ages out. Only the content half's Absent verdict
            // reports a reclamation, because only it establishes that
            // there was something to reclaim.
            Ok(UploadSessionSweep::Delete {
                reclaimed_content: false,
            })
        }
        UploadSessionLifecycle::Completed {
            completed_at_ms,
            content_ref,
        } => {
            if context.now_ms.saturating_sub(completed_at_ms) < CONTENT_RECLAMATION_GRACE_MS {
                return Ok(UploadSessionSweep::Retain);
            }
            match references
                .lookup(store, namespace_id, &content_ref.content_id, budget)
                .await?
            {
                ContentReference::Unknown => Ok(UploadSessionSweep::Retain),
                ContentReference::BudgetExhausted => Ok(UploadSessionSweep::BudgetExhausted),
                // Published content answers to metadata now, not to the
                // session that uploaded it, so the record is all that goes.
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

/// Aborts one session whose lease has passed, then deletes what it was
/// writing.
///
/// The grace on top of the lease is not part of the safety argument — the
/// compare-and-swap is — it just keeps a completion that arrives moments
/// late from being a race anyone has to think about.
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
        return Ok(UploadSessionSweep::Retain);
    }
    let aborted = try_update_upload_session(
        store,
        namespace_id,
        upload_id,
        |mut state: UploadSessionState, _metadata| async move {
            if !matches!(state.state, UploadSessionLifecycle::Open { .. }) {
                return Ok(UploadSessionUpdate::Noop(None));
            }
            let abandoned = AbandonedUpload::of(&state);
            state.state = UploadSessionLifecycle::Aborted {
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
        Ok(UploadSessionCas::Applied(Some(abandoned))) => {
            abandoned.release(store, content_store_id).await;
            Ok(UploadSessionSweep::Retain)
        }
        Ok(UploadSessionCas::Applied(None)) => Ok(UploadSessionSweep::Retain),
        Ok(UploadSessionCas::Conflict) => {
            tracing::debug!(
                namespace_id = %namespace_id,
                upload_id = %upload_id,
                "upload-session abort lost its inspected etag; retaining"
            );
            Ok(UploadSessionSweep::Retain)
        }
        Err(CoreError::UploadNotFound { .. }) => Ok(UploadSessionSweep::Retain),
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
    /// The pass ran out of budget partway through establishing the set. It
    /// is not an answer of any kind — see [`ScanOutcome::BudgetExhausted`].
    BudgetExhausted,
}

enum CollectedReferences {
    NotYet,
    Unavailable,
    Referenced(BTreeSet<ContentId>),
}

/// What one attempt at the reference scan produced.
enum ScanOutcome {
    /// Every root was read. The set is complete and may decide deletions.
    Complete(BTreeSet<ContentId>),
    /// A root could not be read, so this pass has no reference set at all
    /// (format spec, "Garbage collection", rule 5).
    Unavailable,
    /// The pass budget ran out before the last root was read. The ids
    /// gathered so far are dropped: a partial set is not a smaller answer,
    /// it is no answer. Every id it is missing looks unreferenced, so
    /// deciding a deletion from one would delete live content.
    BudgetExhausted,
}

/// Every content id the namespace's live metadata references, collected at
/// most once per invocation and only when a completed session has actually
/// aged into reclamation — which in a healthy namespace is never, because
/// published sessions are swept before their content ever becomes a
/// question.
///
/// The memo lives for one invocation, not one cursor-paged sweep. Carrying
/// it further would mean putting "already scanned through here, found
/// nothing" into the cursor, and the cursor is a client-supplied token that
/// carries enumeration position only and never authorizes a deletion. So a
/// resumed sweep collects again, and the budget above is what keeps that
/// honest: the scan pays its own way every time it runs.
pub(super) struct ContentReferences<'a> {
    live: &'a LiveSet,
    collected: CollectedReferences,
}

impl<'a> ContentReferences<'a> {
    pub(super) fn over(live: &'a LiveSet) -> Self {
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
            if self.live.degraded {
                self.collected = CollectedReferences::Unavailable;
            } else {
                match collect_referenced_content(store, namespace_id, self.live, budget).await? {
                    ScanOutcome::Complete(referenced) => {
                        self.collected = CollectedReferences::Referenced(referenced);
                    }
                    ScanOutcome::Unavailable => {
                        self.collected = CollectedReferences::Unavailable;
                    }
                    // Deliberately leaves the memo unset: nothing partial
                    // is ever remembered, and the pass is about to stop
                    // and hand its caller a cursor anyway.
                    ScanOutcome::BudgetExhausted => {
                        return Ok(ContentReference::BudgetExhausted);
                    }
                }
            }
        }
        Ok(match &self.collected {
            CollectedReferences::NotYet | CollectedReferences::Unavailable => {
                ContentReference::Unknown
            }
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

/// Collects every content id the namespace can still reach.
///
/// The roots are the same ones the rest of the pass uses: every manifest the
/// live set protects, which includes each fork basis a fork-owned checkpoint
/// record pins, plus every retained WAL segment for commits that are durable
/// but not yet materialized. A fork target can only carry forward references
/// that were in the basis it forked from, and a commit in any other
/// namespace can only name content minted by that namespace's own sessions,
/// so those two sources are the whole reachable set.
///
/// Every root read costs a budget unit — one per manifest opened, one per
/// page of revision rows, one per WAL segment fetched — so this scan is
/// part of the pass's bound rather than an exception to it. Running out
/// stops the scan where it stands and reports
/// [`ScanOutcome::BudgetExhausted`]; the caller retains the session that
/// triggered it and returns its cursor. A namespace whose scan costs more
/// than one whole budget therefore never finishes it, which is why
/// `max_objects` has to be at least the scan's size for content
/// reclamation to make progress at all.
async fn collect_referenced_content<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    live: &LiveSet,
    budget: &mut PassBudget,
) -> Result<ScanOutcome> {
    let mut referenced = BTreeSet::new();
    for manifest_object_id in &live.manifests {
        if !budget.try_charge() {
            return Ok(ScanOutcome::BudgetExhausted);
        }
        let Ok(tables) =
            load_verified_manifest_tables(store, namespace_id, manifest_object_id).await
        else {
            return Ok(ScanOutcome::Unavailable);
        };
        let mut lower_bound = lookup_keys::REVISION_ROW_PREFIX.to_owned();
        let upper_bound = string_prefix_upper_bound(lookup_keys::REVISION_ROW_PREFIX);
        loop {
            if !budget.try_charge() {
                return Ok(ScanOutcome::BudgetExhausted);
            }
            let Ok(rows) = tables
                .scan_range_page_with_keys(
                    MetadataTableFamily::Revisions,
                    &lower_bound,
                    upper_bound.as_deref(),
                    REVISION_SCAN_WAVE_ROWS,
                )
                .await
            else {
                return Ok(ScanOutcome::Unavailable);
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

    for segment_key in &live.wal_segments {
        if !budget.try_charge() {
            return Ok(ScanOutcome::BudgetExhausted);
        }
        let Some(bytes) = store
            .get(segment_key, None)
            .await
            .map_err(|error| CoreError::store(segment_key, &error))?
        else {
            // A retained segment that vanished mid-pass is exactly the
            // ambiguity rule 5 is about.
            return Ok(ScanOutcome::Unavailable);
        };
        let Ok(envelope) = decode_wal_segment_envelope_zstd(&bytes) else {
            return Ok(ScanOutcome::Unavailable);
        };
        for record in &envelope.payload.records {
            for delta in &record.deltas {
                if let WalDelta::AppendFileRevision { content_ref, .. } = &delta.delta {
                    referenced.insert(content_ref.content_id.clone());
                }
            }
        }
    }

    Ok(ScanOutcome::Complete(referenced))
}
