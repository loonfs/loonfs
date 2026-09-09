//! Sweep candidates against the live set loaded for this call.
use super::families::CandidateFamily;
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
    MissingBasisCheckpointSweep,
};
use super::live_set::LiveSet;
use super::reap::{grace_age, sweep_checkpoint_record, CheckpointSweep, GraceAge};
use super::uploads::{
    sweep_upload_session, PublicationView, UploadSessionSweep, UploadSweepContext,
};
use super::PassBudget;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS;
use loonfs_api::{DeletedObjectCounts, GcResponse, NamespaceId, RetainedReason};
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::ObjectStore;

pub(super) struct Sweep<'a, 'store, S: ObjectStore + ?Sized> {
    pub(super) store: &'store S,
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) grace_window_ms: u64,
    pub(super) mutation: &'a MutationContext,
    pub(super) live: &'a LiveSet,
    pub(super) view: &'a PublicationView<'a, 'store, S>,
    pub(super) budget: &'a mut PassBudget,
    pub(super) upload_sweep: UploadSweepContext<'a, S>,
    pub(super) checkpoints_retained: &'a mut bool,
    pub(super) report: &'a mut GcResponse,
}

impl<S: ObjectStore + ?Sized> Sweep<'_, '_, S> {
    /// Decides one listed key. Returns `false` when the key needs a store
    /// request and the budget has none left; the key is then untouched.
    pub(super) async fn candidate(&mut self, family: CandidateFamily, key: &str) -> Result<bool> {
        if !family.recognizes(key) {
            self.report.retain(RetainedReason::UnrecognizedKey);
            *self.checkpoints_retained |= family == CandidateFamily::Checkpoints;
            return Ok(true);
        }
        if family == CandidateFamily::Checkpoints
            && self.live.missing_basis_checkpoints.contains(key)
        {
            if !self.budget.try_charge() {
                return Ok(false);
            }
            self.process_missing_basis_checkpoint(key).await?;
            *self.checkpoints_retained = true;
            return Ok(true);
        }
        match family {
            CandidateFamily::WalSegments => {
                self.process_aged_family(family, key, |counts| &mut counts.wal_segments)
                    .await
            }
            CandidateFamily::MetadataSegments => {
                self.process_aged_family(family, key, |counts| &mut counts.metadata_segments)
                    .await
            }
            CandidateFamily::Manifests => {
                self.process_aged_family(family, key, |counts| &mut counts.manifests)
                    .await
            }
            CandidateFamily::Checkpoints => {
                if !self.budget.try_charge() {
                    return Ok(false);
                }
                *self.checkpoints_retained |= !self.process_checkpoint(key).await?;
                Ok(true)
            }
            CandidateFamily::UploadSessions => {
                if !self.budget.try_charge() {
                    return Ok(false);
                }
                self.process_upload_session(key).await?;
                Ok(true)
            }
            CandidateFamily::OwnedContent => {
                let prefix = family.prefix(self.namespace_id, self.live);
                if !key.starts_with(&prefix)
                    || loonfs_objectstore::layout::parse_object_key(key).is_none_or(|parsed| {
                        parsed.owner_namespace_id() != Some(self.namespace_id.as_str())
                    })
                {
                    self.report.retain(RetainedReason::UnrecognizedKey);
                    return Ok(true);
                }
                if !self.budget.try_charge() {
                    return Ok(false);
                }
                self.delete_key(key).await?;
                self.report.deleted.retired_content_objects += 1;
                Ok(true)
            }
        }
    }
    async fn process_aged_family(
        &mut self,
        family: CandidateFamily,
        key: &str,
        deleted: fn(&mut DeletedObjectCounts) -> &mut u64,
    ) -> Result<bool> {
        if family == CandidateFamily::Manifests && !self.live.namespace_deleted {
            let number = loonfs_objectstore::layout::manifest_no_of(key);
            if self
                .live
                .discovery_start_manifest_no
                .is_none_or(|current| number.is_none_or(|number| number >= current))
            {
                self.report.retain(RetainedReason::Referenced);
                return Ok(true);
            }
        }
        if self.live.objects.contains(key)
            || (family == CandidateFamily::WalSegments && self.live.protects_wal(key))
        {
            self.report.retain(RetainedReason::Referenced);
            return Ok(true);
        }
        if !self.budget.try_charge() {
            return Ok(false);
        }
        // A compaction may publish output that is exactly the minimum age
        // old, so a segment must be strictly older before it goes.
        let min_age_ms = if family == CandidateFamily::MetadataSegments {
            UNREFERENCED_SEGMENT_MIN_AGE_MS + 1
        } else {
            self.grace_window_ms
        };
        if self.sweep_aged(key, min_age_ms).await? {
            *deleted(&mut self.report.deleted) += 1;
        }
        Ok(true)
    }
    async fn process_missing_basis_checkpoint(&mut self, key: &str) -> Result<()> {
        match release_missing_basis_checkpoint(
            self.store,
            self.namespace_id,
            key,
            self.grace_window_ms,
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

    async fn process_checkpoint(&mut self, key: &str) -> Result<bool> {
        match maybe_release_fork_checkpoint(self.store, key, self.mutation).await? {
            ForkCheckpointSweep::Released => {
                self.report.released_checkpoints.fork += 1;
                return Ok(false);
            }
            ForkCheckpointSweep::Retained => {
                self.report.retain(RetainedReason::CheckpointNotReleasable);
                return Ok(false);
            }
            ForkCheckpointSweep::NotAnActiveFork => {}
        }
        match sweep_checkpoint_record(
            self.store,
            self.namespace_id,
            key,
            self.grace_window_ms,
            self.live.namespace_deleted,
            self.mutation,
        )
        .await?
        {
            CheckpointSweep::Delete => {
                self.delete_key(key).await?;
                self.report.deleted.checkpoint_records += 1;
                return Ok(true);
            }
            CheckpointSweep::Released => self.report.released_checkpoints.expired += 1,
            CheckpointSweep::ReleasedSnapshot => self.report.released_checkpoints.snapshot += 1,
            CheckpointSweep::Retain => self.report.retain(RetainedReason::CheckpointNotReleasable),
        }
        Ok(false)
    }

    async fn process_upload_session(&mut self, key: &str) -> Result<()> {
        let Some(upload_id) = upload_id_of(key) else {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(());
        };
        match sweep_upload_session(&self.upload_sweep, &upload_id, self.view).await? {
            UploadSessionSweep::Delete { reclaimed_content } => {
                self.delete_key(key).await?;
                self.report.deleted.upload_sessions += 1;
                if reclaimed_content {
                    self.report.deleted.content_objects += 1;
                }
            }
            UploadSessionSweep::Retain { reclaimable_at_ms } => {
                self.report.retain(match reclaimable_at_ms {
                    Some(_) => RetainedReason::UploadSessionWindow,
                    None => RetainedReason::UploadSessionUndecided,
                });
                self.note_reclamation_deadline(reclaimable_at_ms);
            }
        }
        Ok(())
    }

    async fn sweep_aged(&mut self, key: &str, grace_window_ms: u64) -> Result<bool> {
        match grace_age(self.store, key, grace_window_ms, self.mutation.now_ms)
            .await
            .map_err(|error| CoreError::store(key, &error))?
        {
            GraceAge::Gone => return Ok(false),
            GraceAge::Young => {
                self.report.retain(RetainedReason::WithinGraceWindow);
                return Ok(false);
            }
            GraceAge::Unknown => {
                self.report.retain(RetainedReason::NoProviderTimestamp);
                return Ok(false);
            }
            GraceAge::Aged => {}
        }
        self.delete_key(key).await?;
        Ok(true)
    }

    async fn delete_key(&self, key: &str) -> Result<()> {
        match self.store.delete(key).await {
            Ok(()) | Err(loonfs_objectstore::ObjectStoreError::NotFound { .. }) => Ok(()),
            Err(error) => Err(CoreError::store(key, &error)),
        }
    }

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
}
