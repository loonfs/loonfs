//! Sweep one candidate using completed durable marking evidence.
use super::compaction_staging::CompactionLeases;
use super::cursor::{CandidateFamily, CandidateFamilyExt};
use super::fork_checkpoints::{
    maybe_release_fork_checkpoint, release_missing_basis_checkpoint, ForkCheckpointSweep,
    MissingBasisCheckpointSweep,
};
use super::reap::{grace_age, sweep_checkpoint_record, CheckpointSweep, GraceAge};
use super::references::References;
use super::uploads::{sweep_upload_session, UploadSessionSweep, UploadSweepContext};
use crate::checkpoint::CompactionPrefixOwner;
use crate::context::MutationContext;
use crate::error::{CoreError, Result};
use crate::limits::METADATA_COMPACTION_STAGING_GRACE_MS;
use futures::StreamExt;
use loonfs_api::{DeletedObjectCounts, GcResponse, NamespaceId, RetainedReason};
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::ObjectStore;

pub(super) struct Sweep<'a, 'store, S: ?Sized> {
    pub(super) store: &'store S,
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) grace_window_ms: u64,
    pub(super) mutation: &'a MutationContext,
    pub(super) references: References<'a, 'store, S>,
    pub(super) upload_sweep: UploadSweepContext<'a, S>,
    pub(super) leases: &'a mut CompactionLeases,
    pub(super) report: &'a mut GcResponse,
}

impl<S: ObjectStore + ?Sized> Sweep<'_, '_, S> {
    pub(super) async fn candidate(&mut self, family: CandidateFamily, key: &str) -> Result<()> {
        if !family.recognizes(key) {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(());
        }
        if family == CandidateFamily::Checkpoints && self.references.missing_basis(key).await? {
            return self.process_missing_basis_checkpoint(key).await;
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
            CandidateFamily::CompactionStaging => {
                self.leases.load_once(self.store, self.namespace_id).await?;
                self.process_compaction_staging(key).await
            }
            CandidateFamily::Checkpoints => self.process_checkpoint(key).await,
            CandidateFamily::UploadSessions => self.process_upload_session(key).await,
        }
    }
    async fn process_aged_family(
        &mut self,
        family: CandidateFamily,
        key: &str,
        deleted: fn(&mut DeletedObjectCounts) -> &mut u64,
    ) -> Result<()> {
        if self.references.roots.degraded
            && matches!(
                family,
                CandidateFamily::MetadataSegments | CandidateFamily::Manifests
            )
        {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        if self.references.object(key).await? {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        if self.sweep_aged(key, self.grace_window_ms).await? {
            *deleted(&mut self.report.deleted) += 1;
        }
        Ok(())
    }

    async fn process_compaction_staging(&mut self, key: &str) -> Result<()> {
        if key.ends_with("/protection.json") {
            return self.process_compaction_output_protection(key).await;
        }
        if self.references.roots.degraded {
            self.report.retain(RetainedReason::DegradedRoots);
            return Ok(());
        }
        if self.references.object(key).await? {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }

        match self
            .leases
            .owner_of(self.store, self.namespace_id, key, self.mutation.now_ms)
            .await?
        {
            Some(CompactionPrefixOwner::Protected) => {
                self.report.retain(RetainedReason::WithinGraceWindow);
            }
            None => {
                self.report.retain(RetainedReason::UnrecognizedKey);
            }
            Some(CompactionPrefixOwner::Fenced | CompactionPrefixOwner::Unclaimed) => {
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

    /// Output protection stays readable until no output remains. In particular,
    /// a newer collector must not remove the protection an older one still needs.
    async fn process_compaction_output_protection(&mut self, key: &str) -> Result<()> {
        use loonfs_objectstore::layout::parse_object_key;
        let parsed = parse_object_key(key).expect("recognized output protection");
        let job_id = loonfs_api::MetadataCompactionId::parse(parsed.identifier().expect("job id"))
            .map_err(|error| CoreError::NamespaceCorrupt(error.to_string()))?;
        let Some(loaded) =
            crate::checkpoint::load_output_protection(self.store, self.namespace_id, &job_id)
                .await?
        else {
            return Ok(());
        };
        if self.mutation.now_ms <= loaded.state.expires_at_ms {
            self.report.retain(RetainedReason::WithinGraceWindow);
            return Ok(());
        }
        let prefix = format!(
            "{}segments/",
            key.strip_suffix("protection.json")
                .expect("protection suffix")
        );
        let mut outputs = self.store.list_prefix_from_stream(&prefix, None);
        match outputs.next().await {
            Some(Ok(_)) => {
                self.report.retain(RetainedReason::Referenced);
                Ok(())
            }
            Some(Err(error)) => Err(CoreError::store(&prefix, &error)),
            None => self.delete_key(key).await,
        }
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

    async fn process_checkpoint(&mut self, key: &str) -> Result<()> {
        if self.references.object(key).await? {
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
            self.grace_window_ms,
            self.references.roots.namespace_deleted,
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
        match sweep_upload_session(&self.upload_sweep, &upload_id, &mut self.references).await? {
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
            GraceAge::Aged
                if matches!(
                    self.references.roots.anchor,
                    loonfs_api::wire::gc::GcReferenceAnchor::Missing {}
                ) =>
            {
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
}
