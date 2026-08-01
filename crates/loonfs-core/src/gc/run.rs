//! The GC entry point: orchestrates root collection, verification,
//! and the bounded, resumable sweep.

use super::budget::PassBudget;
use super::config::GcConfig;
use super::cursor::{CandidateFamily, GcCursor};
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
};
use super::live_set::{collect_live_set, LiveSet, SweepVerifier};
use super::reap::{
    delete_if_aged, manifest_object_id_of, sweep_checkpoint_record, CheckpointSweep,
};
use super::uploads::{sweep_upload_session, ContentReferences, UploadSessionSweep};
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::namespace::control::{read_head_object, ControlObjectLoadError};
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
    // The head is the namespace: without one there is nothing to collect,
    // and nothing could have been written under the prefix either, because
    // the head is every installation's first and only write. It also names
    // the content store a session's object lives in.
    let content_store_id = match read_head_object(store, namespace_id).await {
        Ok(head) => head.envelope.state.content_store_id,
        Err(ControlObjectLoadError::MissingObject { .. }) => {
            return Ok(GcResponse::empty(namespace_id.clone()))
        }
        Err(error) => return Err(CoreError::load_head(error)),
    };

    let resume = match config.cursor.as_deref() {
        Some(token) => GcCursor::decode(token, namespace_id)?,
        None => GcCursor::initial(namespace_id),
    };

    // Every invocation rebuilds all roots before interpreting the cursor.
    // The cursor can skip enumeration only; it never carries safety state.
    let mark = Arc::new(collect_live_set(store, namespace_id, context).await?);
    let mut sweep = SweepVerifier::seeded(Arc::clone(&mark), reverify_chunk);
    // Content references are collected from this same root set, lazily and
    // at most once per invocation. The derived reclamation grace is what
    // lets one collection stand for every candidate that follows it: past
    // it, no receipt survives that could add a reference. The scan is
    // charged against the budget below like everything else, and an
    // invocation that cannot afford it skips content reclamation instead
    // of overrunning — the sessions waiting on it are retained and the
    // sweep continues.
    let mut references = ContentReferences::over(&mark);
    let mut report = GcResponse::empty(namespace_id.clone());
    let mut budget = PassBudget::of(config);
    let mut position = resume.clone();

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
            if budget.exhausted() {
                // This one-key lookahead proves work remains. It performs no
                // candidate reads or mutations; the key is reconsidered from
                // the exclusive last-examined position on resume.
                report.next_cursor = Some(position.encode()?);
                report.degraded_retention = sweep.degraded;
                return Ok(report);
            }

            process_candidate(
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
                &mut budget,
                &mut report,
            )
            .await?;
            // Every candidate this loop hands to `process_candidate` comes
            // back decided one way or the other, so the cursor always
            // advances past it. The one thing a pass can run out of budget
            // to answer — whether a completed session's content is still
            // referenced — is answered by retaining the session, never by
            // leaving it undecided and stopping: a budget below that scan's
            // cost would otherwise pin the walk to one key forever.
            budget.charge();
            position = GcCursor::after(namespace_id, family, key);
        }
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
    budget: &mut PassBudget,
    report: &mut GcResponse,
) -> Result<()> {
    if family == CandidateFamily::UploadSessions {
        return process_upload_session(
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
        .await;
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
        return Ok(());
    }

    // Preserve the existing mark selection exactly: objects reachable from
    // the invocation's root snapshot are skipped, while every selected
    // candidate is re-verified immediately before its decision.
    let selected = match family {
        CandidateFamily::WalSegments => !mark.wal_segments.contains(key),
        CandidateFamily::MetadataTables => !mark.tables.contains(key),
        CandidateFamily::Manifests => match manifest_object_id_of(key) {
            Some(Ok(id)) => !mark.manifests.contains(&id),
            None | Some(Err(_)) => false,
        },
        CandidateFamily::Checkpoints => !mark.checkpoint_keys.contains(key),
        CandidateFamily::UploadSessions => false,
    };
    if !selected {
        return Ok(());
    }

    sweep.refresh_if_due(store, namespace_id, context).await?;
    // Each arm names the one reason it kept a candidate. The decisions are
    // exactly the ones they always were — the `||` conditions below are
    // split only so a retention can say which half of the condition held.
    match family {
        CandidateFamily::WalSegments => {
            if sweep.live.wal_segments.contains(key) {
                report.retain(RetainedReason::Referenced);
            } else if sweep_aged(store, key, config, context, report).await? {
                report.deleted_wal_segments += 1;
            }
        }
        CandidateFamily::MetadataTables => {
            // Rule 5 is sticky across every re-collection in this pass.
            if sweep.degraded {
                report.retain(RetainedReason::DegradedRoots);
            } else if sweep.live.tables.contains(key) {
                report.retain(RetainedReason::Referenced);
            } else if sweep_aged(store, key, config, context, report).await? {
                report.deleted_metadata_tables += 1;
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
                        if sweep_aged(store, key, config, context, report).await? {
                            report.deleted_manifests += 1;
                        }
                    }
                    // Candidate selection above never picks an unreadable
                    // manifest key, so this arm is the same belt-and-braces
                    // the selection already wears.
                    None | Some(Err(_)) => report.retain(RetainedReason::UnrecognizedKey),
                }
            }
        }
        CandidateFamily::Checkpoints => {
            process_checkpoint(store, namespace_id, config, context, key, sweep, report).await?;
        }
        CandidateFamily::UploadSessions => {}
    }
    Ok(())
}

/// Ages one unreferenced key out, recording the reason when it stays.
/// Answers whether the key was deleted, so each family still counts its own
/// deletions.
async fn sweep_aged<S: ObjectStore + ?Sized>(
    store: &S,
    key: &str,
    config: &GcConfig,
    context: &MutationContext,
    report: &mut GcResponse,
) -> Result<bool> {
    let outcome = delete_if_aged(store, key, config.grace_window_ms, context.now_ms)
        .await
        .map_err(|error| CoreError::store(key, &error))?;
    if let Some(reason) = outcome.retained_reason() {
        report.retain(reason);
    }
    Ok(outcome.deleted())
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
