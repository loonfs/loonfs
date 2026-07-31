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
use loonfs_api::{ContentStoreId, NamespaceId, UploadId};
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
    // charged against the budget below like everything else, so an
    // invocation that cannot afford it stops rather than overrunning.
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

            let outcome = process_candidate(
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
            if outcome == CandidateOutcome::Parked {
                // The budget died inside the work this candidate needed, so
                // it was decided neither way and the cursor stays where it
                // was: the resume re-enumerates this key and starts its
                // reasoning over from a complete root set. Charging nothing
                // for it keeps the resumed pass's whole budget available
                // for the retry.
                report.next_cursor = Some(position.encode()?);
                report.degraded_retention = sweep.degraded;
                return Ok(report);
            }
            budget.charge();
            position = GcCursor::after(namespace_id, family, key);
        }
    }

    report.degraded_retention = sweep.degraded;
    Ok(report)
}

/// Whether the pass decided a candidate, or stopped short of deciding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateOutcome {
    /// The candidate was decided; the cursor may advance past it.
    Decided,
    /// The budget ran out before the candidate could be decided. Nothing
    /// was deleted for it and the cursor does not advance.
    Parked,
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
) -> Result<CandidateOutcome> {
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
            report.retained_candidates += 1;
        }
        return Ok(CandidateOutcome::Decided);
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
        return Ok(CandidateOutcome::Decided);
    }

    sweep.refresh_if_due(store, namespace_id, context).await?;
    match family {
        CandidateFamily::WalSegments => {
            if sweep.live.wal_segments.contains(key) {
                report.retained_candidates += 1;
            } else if delete_if_aged(store, key, config.grace_window_ms, context, report).await? {
                report.deleted_wal_segments += 1;
            }
        }
        CandidateFamily::MetadataTables => {
            // Rule 5 is sticky across every re-collection in this pass.
            if sweep.degraded || sweep.live.tables.contains(key) {
                report.retained_candidates += 1;
            } else if delete_if_aged(store, key, config.grace_window_ms, context, report).await? {
                report.deleted_metadata_tables += 1;
            }
        }
        CandidateFamily::Manifests => {
            let live_or_unrecognized = match manifest_object_id_of(key) {
                Some(Ok(id)) => sweep.live.manifests.contains(&id),
                None | Some(Err(_)) => true,
            };
            if sweep.degraded || live_or_unrecognized {
                report.retained_candidates += 1;
            } else if delete_if_aged(store, key, config.grace_window_ms, context, report).await? {
                report.deleted_manifests += 1;
            }
        }
        CandidateFamily::Checkpoints => {
            process_checkpoint(store, namespace_id, config, context, key, sweep, report).await?;
        }
        CandidateFamily::UploadSessions => {}
    }
    Ok(CandidateOutcome::Decided)
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
        report.retained_candidates += 1;
        return Ok(());
    }
    match maybe_release_fork_checkpoint(store, key, context).await? {
        ForkCheckpointSweep::Released => {
            report.released_fork_checkpoints += 1;
            return Ok(());
        }
        ForkCheckpointSweep::Retained => {
            report.retained_candidates += 1;
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
        CheckpointSweep::Retain => report.retained_candidates += 1,
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
) -> Result<CandidateOutcome> {
    let Some(upload_id) = upload_id_of(key) else {
        report.retained_candidates += 1;
        return Ok(CandidateOutcome::Decided);
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
        UploadSessionSweep::Retain => report.retained_candidates += 1,
        UploadSessionSweep::BudgetExhausted => return Ok(CandidateOutcome::Parked),
    }
    Ok(CandidateOutcome::Decided)
}

fn upload_id_of(key: &str) -> Option<UploadId> {
    let name = key.rsplit('/').next()?.strip_suffix(".json")?;
    UploadId::parse(name).ok()
}
