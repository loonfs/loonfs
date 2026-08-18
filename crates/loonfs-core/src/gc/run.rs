//! The GC entry point: orchestrates root collection, verification,
//! and the bounded, resumable sweep.

use super::budget::PassBudget;
use super::compaction_staging::{CompactionLeases, StagedObject};
use super::config::{GcConfig, GcPolicy};
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
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_STAGING_GRACE_MS;
use crate::namespace::basis::load_head_and_metadata_basis;
use futures::StreamExt;
use loonfs_api::{ContentStoreId, GcResponse, NamespaceId, RetainedReason, UploadId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepCandidateFamily {
    WalSegments,
    MetadataTables,
    CompactionStaging,
    Manifests,
    Checkpoints,
}

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
    let policy = GcPolicy::settle(config, namespace_id)?;
    let mut report = GcResponse::empty(namespace_id.clone());
    // The budget opens before the first read, so marking spends out of it
    // too. A pass that cannot afford its own roots reports that. It does
    // not read the whole retained chain for free and then meter only what
    // comes after.
    let mut budget = PassBudget::new(policy.max_objects);

    // The head is the namespace: without one there is nothing to collect,
    // and nothing could have been written under the prefix either, because
    // the head is every installation's first and only write. It also names
    // the content store a session's object lives in. The metadata root is
    // read alongside it and charged with it as one unit.
    budget.charge();
    let loaded = match load_head_and_metadata_basis(store, namespace_id).await {
        Ok(loaded) => loaded,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
        Err(error) => return Err(CoreError::load_head(error)),
    };
    let content_store_id = loaded.head.state.content_store_id.clone();

    // Every invocation rebuilds all roots before interpreting the cursor.
    // The cursor can skip enumeration only; it never carries safety state.
    let initial_live = match collect_live_set(
        store,
        namespace_id,
        &loaded,
        policy.grace_window_ms,
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
            report.next_cursor.clone_from(&policy.unchanged_cursor);
            return Ok(report);
        }
    };

    // A pass exists only after root collection succeeds. From here on it is
    // the one owner of every mutable sweep concern, including cleanup owed by
    // an early budget stop or error.
    GcPass {
        store,
        namespace_id,
        content_store_id,
        policy,
        mutation: context,
        initial_live: Arc::clone(&initial_live),
        sweep: SweepVerifier::seeded(Arc::clone(&initial_live), reverify_chunk),
        references: ContentReferences::over(initial_live),
        leases: CompactionLeases::default(),
        budget,
        report,
        position: None,
    }
    .run()
    .await
}

/// One initialized garbage-collection pass.
///
/// These fields share a lifecycle rather than merely sharing a call site:
/// candidate decisions mutate the verifier, reference memo, lease claim,
/// budget, cursor, and report together, and [`Self::run`] settles all of them
/// exactly once.
struct GcPass<'a, S: ?Sized> {
    store: &'a S,
    namespace_id: &'a NamespaceId,
    content_store_id: ContentStoreId,
    policy: GcPolicy,
    mutation: &'a MutationContext,
    /// The root snapshot used to select candidates and collect content
    /// references. Re-verification has its own newer snapshot in `sweep`.
    initial_live: Arc<LiveSet>,
    sweep: SweepVerifier,
    references: ContentReferences,
    leases: CompactionLeases,
    budget: PassBudget,
    report: GcResponse,
    /// The last candidate this pass decided, if it has made progress.
    position: Option<GcCursor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PassEnd {
    Complete,
    BudgetExhausted,
}

impl<S: ObjectStore + ?Sized> GcPass<'_, S> {
    /// Runs and settles the pass. Lease cleanup deliberately happens before
    /// the walk result is propagated, so no error or budget return can bypass
    /// a claimed compaction lease.
    async fn run(mut self) -> Result<GcResponse> {
        let walked = self.walk_candidates().await;
        let leases_finished = self.leases.finish(self.store).await;
        leases_finished?;
        self.finish_report(walked?)
    }

    async fn walk_candidates(&mut self) -> Result<PassEnd> {
        let resume_family = self.policy.resume.family;
        let resume_last_key = self.policy.resume.last_key.clone();

        // Data precedes mutable records. A crash or bounded return can
        // therefore leave data protected for an extra pass, never a readable
        // record whose basis was removed underneath it.
        for &family in &CandidateFamily::ALL[resume_family.index()..] {
            let prefix = family.prefix(self.namespace_id);
            let start_after = if family == resume_family {
                resume_last_key.as_deref()
            } else {
                None
            };
            let mut stream = self.store.list_prefix_from_stream(&prefix, start_after);
            while let Some(item) = stream.next().await {
                let key = item.map_err(|error| CoreError::store(&prefix, &error))?;
                // This one-key lookahead proves work remains. It performs no
                // candidate reads or mutations, and the key is reconsidered
                // from the exclusive last-examined position on resume.
                if self.budget.exhausted() {
                    return Ok(PassEnd::BudgetExhausted);
                }

                if self.process_candidate(family, &key).await? == SweepStep::BudgetExhausted {
                    // The re-verification this candidate's decision needs
                    // could not be paid for, so the candidate stays undecided
                    // and is reconsidered from the same exclusive position on
                    // resume.
                    return Ok(PassEnd::BudgetExhausted);
                }
                // Every candidate that gets past the check above comes back
                // decided one way or the other, so the cursor always advances
                // past it. A content-reference scan that exhausts the budget
                // retains its session instead of pinning the cursor forever.
                self.budget.charge();
                self.position = Some(GcCursor::after(self.namespace_id, family, key));
            }
        }
        Ok(PassEnd::Complete)
    }

    /// Decides one candidate using the pass-owned verifier and family state.
    async fn process_candidate(&mut self, family: CandidateFamily, key: &str) -> Result<SweepStep> {
        let family = match family {
            CandidateFamily::UploadSessions => {
                self.process_upload_session(key).await?;
                return Ok(SweepStep::Continue);
            }
            CandidateFamily::Checkpoints
                if self.initial_live.missing_basis_records.contains(key) =>
            {
                self.process_missing_basis_checkpoint(key).await?;
                return Ok(SweepStep::Continue);
            }
            CandidateFamily::WalSegments => SweepCandidateFamily::WalSegments,
            CandidateFamily::MetadataTables => SweepCandidateFamily::MetadataTables,
            CandidateFamily::CompactionStaging => SweepCandidateFamily::CompactionStaging,
            CandidateFamily::Manifests => SweepCandidateFamily::Manifests,
            CandidateFamily::Checkpoints => SweepCandidateFamily::Checkpoints,
        };

        // Objects reachable from the invocation's root snapshot — either
        // anchor's — are skipped, while every selected candidate is
        // re-verified immediately before its decision.
        if !self.is_selected(family, key) {
            return Ok(SweepStep::Continue);
        }
        if self
            .sweep
            .refresh_if_due(
                self.store,
                self.namespace_id,
                self.policy.grace_window_ms,
                &mut self.budget,
                self.mutation,
            )
            .await?
            == SweepStep::BudgetExhausted
        {
            return Ok(SweepStep::BudgetExhausted);
        }

        match family {
            SweepCandidateFamily::WalSegments => self.process_wal_segment(key).await?,
            SweepCandidateFamily::MetadataTables => self.process_metadata_table(key).await?,
            SweepCandidateFamily::CompactionStaging => self.process_compaction_staging(key).await?,
            SweepCandidateFamily::Manifests => self.process_manifest(key).await?,
            SweepCandidateFamily::Checkpoints => self.process_checkpoint(key).await?,
        }
        Ok(SweepStep::Continue)
    }

    fn is_selected(&self, family: SweepCandidateFamily, key: &str) -> bool {
        match family {
            SweepCandidateFamily::WalSegments => !self.initial_live.protects_wal_segment(key),
            // A staged segment a publication landed is named by the manifest
            // like any other table, so the same root set answers for both
            // families.
            SweepCandidateFamily::MetadataTables | SweepCandidateFamily::CompactionStaging => {
                !self.initial_live.tables.contains(key)
            }
            SweepCandidateFamily::Manifests => match manifest_object_id_of(key) {
                Some(Ok(id)) => !self.initial_live.manifests.contains(&id),
                // A key that names no readable manifest is reachable from no
                // root. The family decision retains unrecognized names.
                None | Some(Err(_)) => true,
            },
            SweepCandidateFamily::Checkpoints => !self.initial_live.checkpoint_keys.contains(key),
        }
    }

    async fn process_wal_segment(&mut self, key: &str) -> Result<()> {
        if self.sweep.live.protects_wal_segment(key) {
            self.report.retain(RetainedReason::Referenced);
        } else if self.sweep_aged(key, self.policy.grace_window_ms).await? {
            self.report.deleted_wal_segments += 1;
        }
        Ok(())
    }

    async fn process_metadata_table(&mut self, key: &str) -> Result<()> {
        // Rule 5 is sticky across every re-collection in this pass.
        if self.sweep.degraded {
            self.report.retain(RetainedReason::DegradedRoots);
        } else if self.sweep.live.tables.contains(key) {
            self.report.retain(RetainedReason::Referenced);
        } else if self.sweep_aged(key, self.policy.grace_window_ms).await? {
            self.report.deleted_metadata_tables += 1;
        }
        Ok(())
    }

    async fn process_compaction_staging(&mut self, key: &str) -> Result<()> {
        if self.sweep.degraded {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        if self.sweep.live.tables.contains(key) {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }

        // A compaction job's prefix holds the segments it has written and
        // the lease that says who owns them. A claimed lease goes only after
        // the objects it fenced, which [`Self::run`] enforces at every exit.
        match self
            .leases
            .owner_of(self.store, self.namespace_id, key, self.mutation.now_ms)
            .await?
        {
            StagedObject::OwnedByALiveJob => self.report.retain(RetainedReason::GraceWindow),
            StagedObject::UnrecognizedKey => {
                self.report.retain(RetainedReason::UnrecognizedKey);
            }
            StagedObject::ClaimedLease => {}
            StagedObject::Orphaned => {
                if self
                    .sweep_aged(key, METADATA_COMPACTION_STAGING_GRACE_MS)
                    .await?
                    && key.ends_with(".sst.zst")
                {
                    self.report.deleted_metadata_tables += 1;
                }
            }
        }
        Ok(())
    }

    async fn process_manifest(&mut self, key: &str) -> Result<()> {
        if self.sweep.degraded {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        match manifest_object_id_of(key) {
            Some(Ok(id)) if self.sweep.live.manifests.contains(&id) => {
                self.report.retain(RetainedReason::Referenced);
            }
            Some(Ok(_)) => {
                if self.sweep_aged(key, self.policy.grace_window_ms).await? {
                    self.report.deleted_manifests += 1;
                }
            }
            // A key under the manifest prefix that does not name a manifest
            // is never deleted because this pass cannot tell what wrote it.
            None | Some(Err(_)) => self.report.retain(RetainedReason::UnrecognizedKey),
        }
        Ok(())
    }

    async fn process_missing_basis_checkpoint(&mut self, key: &str) -> Result<()> {
        if release_missing_basis_checkpoint(
            self.store,
            self.namespace_id,
            key,
            self.policy.grace_window_ms,
            self.mutation,
        )
        .await?
        {
            self.report.released_missing_basis_checkpoints += 1;
        } else {
            self.report.retain(RetainedReason::CheckpointNotReleasable);
        }
        Ok(())
    }

    async fn process_checkpoint(&mut self, key: &str) -> Result<()> {
        if self.sweep.live.checkpoint_keys.contains(key) {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        match maybe_release_fork_checkpoint(self.store, key, self.mutation).await? {
            ForkCheckpointSweep::Released => {
                self.report.released_fork_checkpoints += 1;
                return Ok(());
            }
            ForkCheckpointSweep::Retained => {
                self.report.retain(RetainedReason::CheckpointNotReleasable);
                return Ok(());
            }
            ForkCheckpointSweep::NotAnActiveFork => {}
        }
        match sweep_checkpoint_record(
            self.store,
            self.namespace_id,
            key,
            self.policy.grace_window_ms,
            self.sweep.live.namespace_deleted,
            self.mutation,
        )
        .await?
        {
            CheckpointSweep::Delete => {
                self.delete_key(key).await?;
                self.report.deleted_checkpoint_records += 1;
            }
            CheckpointSweep::Released => self.report.released_expired_checkpoints += 1,
            CheckpointSweep::Retain => self.report.retain(RetainedReason::CheckpointNotReleasable),
        }
        Ok(())
    }

    async fn process_upload_session(&mut self, key: &str) -> Result<()> {
        let Some(upload_id) = upload_id_of(key) else {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(());
        };
        match sweep_upload_session(
            self.store,
            self.namespace_id,
            &self.content_store_id,
            &upload_id,
            self.policy.grace_window_ms,
            &mut self.references,
            &mut self.budget,
            self.mutation,
        )
        .await?
        {
            UploadSessionSweep::Delete { reclaimed_content } => {
                self.delete_key(key).await?;
                self.report.deleted_upload_sessions += 1;
                if reclaimed_content {
                    self.report.deleted_content_objects += 1;
                }
            }
            UploadSessionSweep::Retain { reclaimable_at_ms } => {
                // A deadline is the difference between "come back then" and
                // "ask again next pass", and the sweep already draws that
                // line.
                self.report.retain(match reclaimable_at_ms {
                    Some(_) => RetainedReason::UploadSessionWindow,
                    None => RetainedReason::UploadSessionUndecided,
                });
                self.note_reclamation_deadline(reclaimable_at_ms);
            }
            UploadSessionSweep::ContentReclamationDeferred => {
                self.report.retain(RetainedReason::ContentScanDeferred);
                self.report.content_reclamation_deferred = true;
            }
        }
        Ok(())
    }

    /// Ages one unreferenced key out, recording the reason when it stays.
    async fn sweep_aged(&mut self, key: &str, grace_window_ms: u64) -> Result<bool> {
        match grace_age(self.store, key, grace_window_ms, self.mutation.now_ms)
            .await
            .map_err(|error| CoreError::store(key, &error))?
        {
            // Nothing was decided about a key that is already gone, so
            // nothing is counted for it either.
            GraceAge::Gone => return Ok(false),
            GraceAge::Young => {
                self.report.retain(RetainedReason::GraceWindow);
                return Ok(false);
            }
            GraceAge::Unknown => {
                self.report.retain(RetainedReason::NoProviderTimestamp);
                return Ok(false);
            }
            GraceAge::Aged if !self.sweep.live.anchor.proves_unreferencing() => {
                self.report.retain(RetainedReason::NoReferenceManifest);
                return Ok(false);
            }
            GraceAge::Aged => {}
        }
        self.delete_key(key).await?;
        Ok(true)
    }

    async fn delete_key(&self, key: &str) -> Result<()> {
        self.store
            .delete(key)
            .await
            .map_err(|error| CoreError::store(key, &error))
    }

    /// Folds one retained candidate's future deadline into the report.
    fn note_reclamation_deadline(&mut self, at_ms: Option<u64>) {
        let Some(at_ms) = at_ms.filter(|at_ms| *at_ms > self.mutation.now_ms) else {
            return;
        };
        self.report.next_reclamation_at_ms = Some(
            self.report
                .next_reclamation_at_ms
                .map_or(at_ms, |soonest_ms| soonest_ms.min(at_ms)),
        );
    }

    /// Produces the response after lease settlement. Budget-stop cursor and
    /// degraded-root reporting live here so every completed walk uses the
    /// same finalization path.
    fn finish_report(mut self, end: PassEnd) -> Result<GcResponse> {
        if end == PassEnd::BudgetExhausted {
            self.report.budget_exhausted = true;
            match self.position.as_ref() {
                Some(position) => self.report.next_cursor = Some(position.encode()?),
                None => self
                    .report
                    .next_cursor
                    .clone_from(&self.policy.unchanged_cursor),
            }
        }
        self.report.degraded_retention = self.sweep.degraded;
        Ok(self.report)
    }
}

fn upload_id_of(key: &str) -> Option<UploadId> {
    let name = key.rsplit('/').next()?.strip_suffix(".json")?;
    UploadId::parse(name).ok()
}
