//! The GC entry point: orchestrates root collection, verification,
//! and the bounded, resumable sweep.

use super::budget::PassBudget;
use super::compaction_staging::{CompactionLeases, StagedObject};
use super::config::GcConfig;
use super::cursor::{CandidateFamily, GcCursor};
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
};
use super::live_set::{collect_live_set, LiveSet, LiveSetCollection, SweepStep, SweepVerifier};
use super::reap::{
    grace_age, manifest_object_id_of, sweep_checkpoint_record, CheckpointSweep, GraceAge,
};
use super::uploads::{sweep_upload_session, ContentReferences, UploadSessionSweep};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_STAGING_GRACE_MS;
use crate::namespace::basis::read_head_and_metadata_basis;
use crate::namespace::control::ControlObjectLoadError;
use futures::StreamExt;
use loonfs_api::v0::GcResponse;
use loonfs_api::{ContentStoreId, NamespaceId, RetainedReason, UploadId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

pub async fn gc_namespace<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
) -> Result<GcResponse> {
    gc_namespace_with_reverify_chunk(store, namespace_id, config, context, SWEEP_REVERIFY_CHUNK)
        .await
}

/// How many sweep candidates may be decided against one live set before the
/// set is re-collected (rule 3: candidate selection may be stale, deletion
/// may not).
const SWEEP_REVERIFY_CHUNK: usize = 1024;

pub(super) async fn gc_namespace_with_reverify_chunk<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    reverify_chunk: usize,
) -> Result<GcResponse> {
    config.validate()?;
    let resume = match config.cursor.as_deref() {
        Some(token) => GcCursor::decode(token, namespace_id)?,
        None => GcCursor::initial(namespace_id),
    };
    let mut report = GcResponse::empty(namespace_id.clone());
    // The budget opens before the first read, so marking spends out of it
    // too. A pass that cannot afford its own roots reports that. It does
    // not read the whole retained chain for free and then meter only what
    // comes after.
    let mut budget = PassBudget::of(config);

    // The head is the namespace: without one there is nothing to collect,
    // and nothing could have been written under the prefix either, because
    // the head is every installation's first and only write. It also names
    // the content store a session's object lives in. The metadata root is
    // read alongside it and charged with it as one unit.
    budget.charge();
    let loaded = match read_head_and_metadata_basis(store, namespace_id).await {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
        Err(error) => return Err(CoreError::load_head(error)),
    };
    let content_store_id = loaded.head.state.content_store_id.clone();

    // Every invocation rebuilds all roots before interpreting the cursor.
    // The cursor can skip enumeration only; it never carries safety state.
    let mark = match collect_live_set(
        store,
        namespace_id,
        &loaded,
        config.grace_window_ms,
        None,
        &mut budget,
        context,
    )
    .await?
    {
        LiveSetCollection::Complete(live) => Arc::new(live),
        // The pass has no root set, so it may not decide any candidate. It
        // deletes nothing and hands back the token it was given, because it
        // made no progress to report. It also skips content reclamation for
        // the same reason the scan skips it. The collection that decision
        // needs did not fit in the budget.
        LiveSetCollection::BudgetExhausted => {
            report.budget_exhausted = true;
            report.content_reclamation_deferred = true;
            report.next_cursor.clone_from(&config.cursor);
            return Ok(report);
        }
    };
    let mut sweep = SweepVerifier::seeded(Arc::clone(&mark), reverify_chunk);
    // Content references are collected from this same root set, lazily and
    // at most once per invocation. The derived reclamation grace is what
    // lets one collection stand for every candidate that follows it: past
    // it, no receipt survives that could add a reference. The scan is
    // charged against the budget like everything else, and an invocation
    // that cannot afford it skips content reclamation instead of
    // overrunning — the sessions waiting on it are retained and the sweep
    // continues.
    let mut references = ContentReferences::over(&mark);
    // What this pass has learned about the compaction job whose objects it is
    // walking. One lease read per job rather than one per object.
    let mut leases = CompactionLeases::default();
    // The position this pass walked to, or `None` while it has decided
    // nothing and has no progress of its own to report.
    let mut position: Option<GcCursor> = None;

    // Data precedes mutable records. A crash or bounded return can therefore
    // leave data protected for an extra pass, never a readable record whose
    // basis was removed underneath it.
    for &family in &CandidateFamily::ALL[resume.family.index()..] {
        let prefix = family.prefix(namespace_id);
        let mut stream = store.list_prefix_stream(&prefix);
        while let Some(item) = stream.next().await {
            let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
            if family == resume.family
                && resume
                    .last_key
                    .as_ref()
                    .is_some_and(|last_key| key <= *last_key)
            {
                continue;
            }
            // This one-key lookahead proves work remains. It performs no
            // candidate reads or mutations, and the key is reconsidered
            // from the exclusive last-examined position on resume.
            if budget.exhausted() {
                leases.finish(store).await?;
                return stopped_on_budget(report, config, position.as_ref(), &sweep);
            }

            let step = process_candidate(
                store,
                namespace_id,
                &content_store_id,
                config,
                context,
                family,
                &key,
                &mark,
                &mut sweep,
                &mut references,
                &mut leases,
                &mut budget,
                &mut report,
            )
            .await?;
            if step == SweepStep::BudgetExhausted {
                // The re-verification this candidate's decision needs could
                // not be paid for, so the candidate stays undecided and is
                // reconsidered from the same exclusive position on resume.
                leases.finish(store).await?;
                return stopped_on_budget(report, config, position.as_ref(), &sweep);
            }
            // Every candidate that gets past the check above comes back
            // decided one way or the other, so the cursor always advances
            // past it. The one thing a pass can run out of budget to
            // answer is whether a completed session's content is still
            // referenced. The sweep answers that by retaining the session
            // rather than by leaving the key undecided. A budget below that
            // scan's cost would otherwise pin the walk to one key forever.
            budget.charge();
            position = Some(GcCursor::after(namespace_id, family, key));
        }
    }

    // The last job the walk claimed has no following key to close it out, so
    // the pass closes it here: its lease goes after the objects it covered.
    leases.finish(store).await?;
    report.degraded_retention = sweep.degraded;
    Ok(report)
}

/// Closes a pass that stopped on its budget partway through the sweep. The
/// pass reports the work it did.
///
/// A pass that decided at least one candidate returns the position it
/// walked to. A pass that decided none returns the token it was given, byte
/// for byte, because it made no progress to report. The runner compares
/// that token against the one it submitted to tell a pass that walked from
/// a pass that did not, so re-encoding the position here would have to rely
/// on decode-then-encode producing the same bytes.
fn stopped_on_budget(
    mut report: GcResponse,
    config: &GcConfig,
    position: Option<&GcCursor>,
    sweep: &SweepVerifier,
) -> Result<GcResponse> {
    report.budget_exhausted = true;
    match position {
        Some(position) => report.next_cursor = Some(position.encode()?),
        None => report.next_cursor.clone_from(&config.cursor),
    }
    report.degraded_retention = sweep.degraded;
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn process_candidate<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    config: &GcConfig,
    context: &MutationContext,
    family: CandidateFamily,
    key: &str,
    mark: &LiveSet,
    sweep: &mut SweepVerifier,
    references: &mut ContentReferences<'_>,
    leases: &mut CompactionLeases,
    budget: &mut PassBudget,
    report: &mut GcResponse,
) -> Result<SweepStep> {
    if family == CandidateFamily::UploadSessions {
        process_upload_session(
            store,
            namespace_id,
            content_store_id,
            config,
            context,
            key,
            references,
            budget,
            report,
        )
        .await?;
        return Ok(SweepStep::Continue);
    }

    if family == CandidateFamily::Checkpoints && mark.missing_basis_records.contains(key) {
        if release_missing_basis_checkpoint(
            store,
            namespace_id,
            key,
            config.grace_window_ms,
            context,
        )
        .await?
        {
            report.released_missing_basis_checkpoints += 1;
        } else {
            report.retain(RetainedReason::CheckpointNotReleasable);
        }
        return Ok(SweepStep::Continue);
    }

    // Objects reachable from the invocation's root snapshot — either
    // anchor's — are skipped, while every selected candidate is re-verified
    // immediately before its decision.
    let selected = match family {
        CandidateFamily::WalSegments => !mark.protects_wal_segment(key),
        // A staged segment a publication landed is named by the manifest like
        // any other table, so the same root set answers for both families.
        CandidateFamily::MetadataTables | CandidateFamily::CompactionStaging => {
            !mark.tables.contains(key)
        }
        CandidateFamily::Manifests => match manifest_object_id_of(key) {
            Some(Ok(id)) => !mark.manifests.contains(&id),
            // A key that names no readable manifest is reachable from no
            // root, so it is selected like any other unreferenced
            // candidate; the decision below is where it is kept and said.
            None | Some(Err(_)) => true,
        },
        CandidateFamily::Checkpoints => !mark.checkpoint_keys.contains(key),
        CandidateFamily::UploadSessions => false,
    };
    if !selected {
        return Ok(SweepStep::Continue);
    }

    if sweep
        .refresh_if_due(store, namespace_id, config.grace_window_ms, budget, context)
        .await?
        == SweepStep::BudgetExhausted
    {
        return Ok(SweepStep::BudgetExhausted);
    }
    // Each arm names the one reason it kept a candidate. The decisions are
    // exactly the ones they always were — the `||` conditions below are
    // split only so a retention can say which half of the condition held.
    match family {
        CandidateFamily::WalSegments => {
            if sweep.live.protects_wal_segment(key) {
                report.retain(RetainedReason::Referenced);
            } else if sweep_aged(store, key, config.grace_window_ms, context, sweep, report).await?
            {
                report.deleted_wal_segments += 1;
            }
        }
        CandidateFamily::MetadataTables => {
            // Rule 5 is sticky across every re-collection in this pass.
            if sweep.degraded {
                report.retain(RetainedReason::DegradedRoots);
            } else if sweep.live.tables.contains(key) {
                report.retain(RetainedReason::Referenced);
            } else if sweep_aged(store, key, config.grace_window_ms, context, sweep, report).await?
            {
                report.deleted_metadata_tables += 1;
            }
        }
        // A compaction job's prefix holds the segments it has written and the
        // lease that says who owns them. A published segment is named by the
        // manifest and is live like any other table; everything else is
        // decided by the lease, because a job outlives the ordinary grace
        // window and its output would otherwise read as an aged orphan while
        // it was still being written. Only the segments are counted as
        // metadata tables — a lease is a control object, and a pass that
        // reclaims one reclaims that job's segments in the same walk.
        CandidateFamily::CompactionStaging => {
            if sweep.degraded {
                report.retain(RetainedReason::DegradedRoots);
            } else if sweep.live.tables.contains(key) {
                report.retain(RetainedReason::Referenced);
            } else {
                match leases
                    .owner_of(store, namespace_id, key, context.now_ms)
                    .await?
                {
                    // The same reason an object inside its window is kept:
                    // something that has not run out yet still protects it.
                    StagedObject::OwnedByALiveJob => report.retain(RetainedReason::GraceWindow),
                    StagedObject::UnrecognizedKey => {
                        report.retain(RetainedReason::UnrecognizedKey);
                    }
                    // Decided, and deleted when the walk leaves this prefix:
                    // the claim goes after the objects it fenced, so a pass
                    // that stops in between leaves a lease that says the reap
                    // is unfinished rather than nothing at all.
                    StagedObject::ClaimedLease => {}
                    StagedObject::Orphaned => {
                        if sweep_aged(
                            store,
                            key,
                            METADATA_COMPACTION_STAGING_GRACE_MS,
                            context,
                            sweep,
                            report,
                        )
                        .await?
                            && key.ends_with(".sst.zst")
                        {
                            report.deleted_metadata_tables += 1;
                        }
                    }
                }
            }
        }
        CandidateFamily::Manifests => {
            if sweep.degraded {
                report.retain(RetainedReason::DegradedRoots);
            } else {
                match manifest_object_id_of(key) {
                    Some(Ok(id)) if sweep.live.manifests.contains(&id) => {
                        report.retain(RetainedReason::Referenced);
                    }
                    Some(Ok(_)) => {
                        if sweep_aged(store, key, config.grace_window_ms, context, sweep, report)
                            .await?
                        {
                            report.deleted_manifests += 1;
                        }
                    }
                    // A key under the manifest prefix that does not name a
                    // manifest is never deleted: this pass cannot tell what
                    // wrote it, so it is retained and reported.
                    None | Some(Err(_)) => report.retain(RetainedReason::UnrecognizedKey),
                }
            }
        }
        CandidateFamily::Checkpoints => {
            process_checkpoint(store, namespace_id, config, context, key, sweep, report).await?;
        }
        CandidateFamily::UploadSessions => {}
    }
    Ok(SweepStep::Continue)
}

/// Ages one unreferenced key out, recording the reason when it stays.
/// Answers whether the key was deleted, so each family still counts its own
/// deletions.
///
/// Two things have to hold before the key goes. The object's own write time
/// has to be `grace_window_ms` old, which is what protects a publication
/// still in flight and every object the reference anchor predates. And the
/// reference anchor has to say the object was already unreferenced when the
/// window opened, which is what protects a reader still holding an anchor it
/// pinned inside the window. A pass with no anchor cannot answer the second
/// question, so it keeps every aged candidate and says so.
///
/// The window is a parameter because one family does not use the configured
/// one: an object under a compaction job's prefix whose lease is stale or
/// gone ages under [`METADATA_COMPACTION_STAGING_GRACE_MS`], which covers the
/// lease's own expiry plus the publication that could still be naming the
/// object.
async fn sweep_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    grace_window_ms: u64,
    context: &MutationContext,
    sweep: &SweepVerifier,
    report: &mut GcResponse,
) -> Result<bool> {
    match grace_age(store, key, grace_window_ms, context.now_ms)
        .await
        .map_err(|error| CoreError::store(key, &error))?
    {
        // Nothing was decided about a key that is already gone, so nothing
        // is counted for it either.
        GraceAge::Gone => return Ok(false),
        GraceAge::Young => {
            report.retain(RetainedReason::GraceWindow);
            return Ok(false);
        }
        GraceAge::Unknown => {
            report.retain(RetainedReason::NoProviderTimestamp);
            return Ok(false);
        }
        GraceAge::Aged if !sweep.live.anchor.proves_unreferencing() => {
            report.retain(RetainedReason::NoReferenceManifest);
            return Ok(false);
        }
        GraceAge::Aged => {}
    }
    store
        .delete(key)
        .await
        .map_err(|error| CoreError::store(key, &error))?;
    Ok(true)
}

async fn process_checkpoint<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    config: &GcConfig,
    context: &MutationContext,
    key: &str,
    sweep: &SweepVerifier,
    report: &mut GcResponse,
) -> Result<()> {
    if sweep.live.checkpoint_keys.contains(key) {
        report.retain(RetainedReason::Referenced);
        return Ok(());
    }
    match maybe_release_fork_checkpoint(store, key, context).await? {
        ForkCheckpointSweep::Released => {
            report.released_fork_checkpoints += 1;
            return Ok(());
        }
        ForkCheckpointSweep::Retained => {
            report.retain(RetainedReason::CheckpointNotReleasable);
            return Ok(());
        }
        ForkCheckpointSweep::NotAnActiveFork => {}
    }
    match sweep_checkpoint_record(
        store,
        namespace_id,
        key,
        config.grace_window_ms,
        sweep.live.namespace_deleted,
        context,
    )
    .await?
    {
        CheckpointSweep::Delete => {
            store
                .delete(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?;
            report.deleted_checkpoint_records += 1;
        }
        CheckpointSweep::Released => report.released_expired_checkpoints += 1,
        CheckpointSweep::Retain => report.retain(RetainedReason::CheckpointNotReleasable),
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_upload_session<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    content_store_id: &ContentStoreId,
    config: &GcConfig,
    context: &MutationContext,
    key: &str,
    references: &mut ContentReferences<'_>,
    budget: &mut PassBudget,
    report: &mut GcResponse,
) -> Result<()> {
    let Some(upload_id) = upload_id_of(key) else {
        report.retain(RetainedReason::UnrecognizedKey);
        return Ok(());
    };
    match sweep_upload_session(
        store,
        namespace_id,
        content_store_id,
        &upload_id,
        config.grace_window_ms,
        references,
        budget,
        context,
    )
    .await?
    {
        UploadSessionSweep::Delete { reclaimed_content } => {
            store
                .delete(key)
                .await
                .map_err(|error| CoreError::store(key, &error))?;
            report.deleted_upload_sessions += 1;
            if reclaimed_content {
                report.deleted_content_objects += 1;
            }
        }
        UploadSessionSweep::Retain { reclaimable_at_ms } => {
            // A deadline is the difference between "come back then" and
            // "ask again next pass", and the sweep already draws that line.
            report.retain(match reclaimable_at_ms {
                Some(_) => RetainedReason::UploadSessionWindow,
                None => RetainedReason::UploadSessionUndecided,
            });
            note_reclamation_deadline(report, reclaimable_at_ms, context.now_ms);
        }
        UploadSessionSweep::ContentReclamationDeferred => {
            // Retained for the same reason any undecidable session is,
            // plus a flag, so a caller can tell a pass that swept
            // everything from one that skipped a question it could not
            // afford to ask.
            report.retain(RetainedReason::ContentScanDeferred);
            report.content_reclamation_deferred = true;
        }
    }
    Ok(())
}

/// Folds one retained candidate's deadline into the soonest the pass will
/// report.
///
/// Only deadlines still ahead of this pass's own clock are kept. One
/// already past is not an obligation this pass is leaving behind — it is
/// something the pass just decided against for another reason — and
/// reporting it would only ask a scheduler to come straight back.
fn note_reclamation_deadline(report: &mut GcResponse, at_ms: Option<u64>, now_ms: u64) {
    let Some(at_ms) = at_ms.filter(|at_ms| *at_ms > now_ms) else {
        return;
    };
    report.next_reclamation_at_ms = Some(match report.next_reclamation_at_ms {
        Some(soonest_ms) => soonest_ms.min(at_ms),
        None => at_ms,
    });
}

fn upload_id_of(key: &str) -> Option<UploadId> {
    let name = key.rsplit('/').next()?.strip_suffix(".json")?;
    UploadId::parse(name).ok()
}
