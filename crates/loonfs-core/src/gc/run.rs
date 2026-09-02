//! The GC entry point: orchestrates root collection, verification,
//! and the bounded, resumable sweep.

use super::budget::PassBudget;
use super::compaction_staging::CompactionLeases;
use super::config::{GcConfig, GcPolicy};
use super::cursor::{cursor_after, CandidateFamily, GcCursor};
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
    MissingBasisCheckpointSweep,
};
use super::live_set::{collect_live_set, LiveSet, LiveSetCollection, SweepStep, SweepVerifier};
use super::reap::{
    grace_age, manifest_object_id_of, sweep_checkpoint_record, CheckpointSweep, GraceAge,
};
use super::uploads::{
    sweep_upload_session, ContentReferences, UploadSessionSweep, UploadSweepContext,
};
use crate::checkpoint::CompactionPrefixOwner;
use crate::context::MutationContext;
use crate::control_object::ControlObjectLoadError;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_STAGING_GRACE_MS;
use crate::namespace::control_snapshot::load_control_snapshot;
use futures::StreamExt;
use loonfs_api::{DeletedObjectCounts, GcResponse, NamespaceId, RetainedReason, UploadId};
use loonfs_objectstore::ObjectStore;
use std::sync::Arc;

fn is_live(live: &LiveSet, family: CandidateFamily, key: &str) -> bool {
    match family {
        CandidateFamily::WalSegments => live.protects_wal_segment(key),
        CandidateFamily::MetadataSegments | CandidateFamily::CompactionStaging => {
            live.segments.contains(key)
        }
        CandidateFamily::Manifests => manifest_object_id_of(key)
            .and_then(std::result::Result::ok)
            .is_some_and(|manifest_object_id| live.manifests.contains(&manifest_object_id)),
        CandidateFamily::Checkpoints => live.checkpoint_keys.contains(key),
        CandidateFamily::UploadSessions => false,
    }
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

    // Read the namespace's head, root, and floor as one budget unit.
    budget.charge();
    let snapshot = match load_control_snapshot(store, namespace_id).await {
        Ok(snapshot) => snapshot,
        Err(ControlObjectLoadError::MissingObject { .. }) => return Ok(report),
        Err(error) => return Err(CoreError::ControlObjectLoad(error)),
    };
    let content_store_id = snapshot.head.state.content_store_id.clone();

    // Every invocation rebuilds all roots before interpreting the cursor.
    // The cursor can skip enumeration only; it never carries safety state.
    let initial_live = match collect_live_set(
        store,
        namespace_id,
        &snapshot,
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
    let upload_sweep = UploadSweepContext::new(
        store,
        namespace_id,
        content_store_id,
        policy.grace_window_ms,
        context,
    );
    GcPass {
        store,
        namespace_id,
        policy,
        mutation: context,
        initial_live: Arc::clone(&initial_live),
        sweep: SweepVerifier::seeded(Arc::clone(&initial_live), reverify_chunk),
        references: ContentReferences::over(initial_live),
        upload_sweep,
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
    policy: GcPolicy,
    mutation: &'a MutationContext,
    /// The root snapshot used to select candidates and collect content
    /// references. Re-verification has its own newer snapshot in `sweep`.
    initial_live: Arc<LiveSet>,
    sweep: SweepVerifier,
    references: ContentReferences,
    upload_sweep: UploadSweepContext<'a, S>,
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
        let resume_family = self.policy.resume.keyspace().family;
        let resume_last_key = self.policy.resume.last_key().map(str::to_owned);

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
                self.position = Some(cursor_after(self.namespace_id, family, key));
            }
        }
        Ok(PassEnd::Complete)
    }

    /// Decides one candidate using the pass-owned verifier and family state.
    async fn process_candidate(&mut self, family: CandidateFamily, key: &str) -> Result<SweepStep> {
        if !family.recognizes(key) {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(SweepStep::Continue);
        }
        match family {
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
            CandidateFamily::WalSegments
            | CandidateFamily::MetadataSegments
            | CandidateFamily::CompactionStaging
            | CandidateFamily::Manifests
            | CandidateFamily::Checkpoints => {}
        };

        // Objects reachable from the invocation's root snapshot — either
        // anchor's — are skipped, while every selected candidate is
        // re-verified immediately before its decision.
        if is_live(&self.initial_live, family, key) {
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
            CandidateFamily::WalSegments => {
                self.process_aged_family(family, key, |deleted| &mut deleted.wal_segments)
                    .await?
            }
            CandidateFamily::MetadataSegments => {
                self.process_aged_family(family, key, |deleted| &mut deleted.metadata_segments)
                    .await?
            }
            CandidateFamily::Manifests => {
                self.process_aged_family(family, key, |deleted| &mut deleted.manifests)
                    .await?
            }
            CandidateFamily::CompactionStaging => self.process_compaction_staging(key).await?,
            CandidateFamily::Checkpoints => self.process_checkpoint(key).await?,
            CandidateFamily::UploadSessions => self.process_upload_session(key).await?,
        }
        Ok(SweepStep::Continue)
    }

    async fn process_aged_family(
        &mut self,
        family: CandidateFamily,
        key: &str,
        deleted: fn(&mut DeletedObjectCounts) -> &mut u64,
    ) -> Result<()> {
        // Rule 5 is sticky across every re-collection in this pass.
        if self.sweep.degraded
            && matches!(
                family,
                CandidateFamily::MetadataSegments | CandidateFamily::Manifests
            )
        {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        if is_live(&self.sweep.live, family, key) {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        if self.sweep_aged(key, self.policy.grace_window_ms).await? {
            *deleted(&mut self.report.deleted) += 1;
        }
        Ok(())
    }

    async fn process_compaction_staging(&mut self, key: &str) -> Result<()> {
        if self.sweep.degraded {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        if is_live(&self.sweep.live, CandidateFamily::CompactionStaging, key) {
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
            Some(CompactionPrefixOwner::LiveJob) => {
                self.report.retain(RetainedReason::WithinGraceWindow);
            }
            None => {
                self.report.retain(RetainedReason::UnrecognizedKey);
            }
            // The claimed lease is deleted once its prefix is processed.
            Some(CompactionPrefixOwner::ThisCollector) if self.leases.is_claimed_lease(key) => {}
            Some(CompactionPrefixOwner::ThisCollector | CompactionPrefixOwner::NoOne) => {
                if key.ends_with(".sst.zst")
                    && self
                        .sweep_aged(key, METADATA_COMPACTION_STAGING_GRACE_MS)
                        .await?
                {
                    self.report.deleted.metadata_segments += 1;
                }
            }
        }
        Ok(())
    }

    async fn process_missing_basis_checkpoint(&mut self, key: &str) -> Result<()> {
        match release_missing_basis_checkpoint(
            self.store,
            self.namespace_id,
            key,
            self.policy.grace_window_ms,
            self.mutation,
        )
        .await?
        {
            MissingBasisCheckpointSweep::Released => {
                self.report.released_checkpoints.missing_basis += 1;
            }
            MissingBasisCheckpointSweep::Retained => {
                self.report.retain(RetainedReason::CheckpointNotReleasable);
            }
        }
        Ok(())
    }

    async fn process_checkpoint(&mut self, key: &str) -> Result<()> {
        if is_live(&self.sweep.live, CandidateFamily::Checkpoints, key) {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        match maybe_release_fork_checkpoint(self.store, key, self.mutation).await? {
            ForkCheckpointSweep::Released => {
                self.report.released_checkpoints.fork += 1;
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
                self.report.deleted.checkpoint_records += 1;
            }
            CheckpointSweep::Released => self.report.released_checkpoints.expired += 1,
            CheckpointSweep::ReleasedSnapshot => self.report.released_checkpoints.snapshot += 1,
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
            &self.upload_sweep,
            &upload_id,
            &mut self.references,
            &mut self.budget,
        )
        .await?
        {
            UploadSessionSweep::Delete { reclaimed_content } => {
                self.delete_key(key).await?;
                self.report.deleted.upload_sessions += 1;
                if reclaimed_content {
                    self.report.deleted.content_objects += 1;
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
                self.report.retain(RetainedReason::WithinGraceWindow);
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

    /// Records the earliest future reclamation deadline.
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
        self.report.retention_degraded = self.sweep.degraded;
        Ok(self.report)
    }
}

fn upload_id_of(key: &str) -> Option<UploadId> {
    let name = key.rsplit('/').next()?.strip_suffix(".json")?;
    UploadId::parse(name).ok()
}
