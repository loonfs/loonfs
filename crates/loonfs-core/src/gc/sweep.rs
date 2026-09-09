//! Sweep candidates against the live set loaded for this call.
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use super::reap::{grace_age, sweep_checkpoint_record, CheckpointSweep, GraceAge};
use super::uploads::{
    sweep_upload_session, PublicationView, UploadSessionSweep, UploadSweepContext,
};
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
    pub(super) upload_sweep: UploadSweepContext<'a, S>,
    pub(super) checkpoints_retained: &'a mut bool,
    pub(super) report: &'a mut GcResponse,
}

impl<S: ObjectStore + ?Sized> Sweep<'_, '_, S> {
    pub(super) async fn candidate(&mut self, family: CandidateFamily, key: &str) -> Result<()> {
        if !family.recognizes(key) {
            self.report.retain(RetainedReason::UnrecognizedKey);
            *self.checkpoints_retained |= family == CandidateFamily::Checkpoints;
            return Ok(());
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
                *self.checkpoints_retained |= !self.process_checkpoint(key).await?;
                Ok(())
            }
            CandidateFamily::UploadSessions => self.process_upload_session(key).await,
            CandidateFamily::OwnedContent => {
                let prefix = family.prefix(self.namespace_id, self.live);
                if !key.starts_with(&prefix)
                    || loonfs_objectstore::layout::parse_object_key(key).is_none_or(|parsed| {
                        parsed.owner_namespace_id() != Some(self.namespace_id.as_str())
                    })
                {
                    self.report.retain(RetainedReason::UnrecognizedKey);
                    return Ok(());
                }
                self.delete_key(key).await?;
                self.report.deleted.retired_content_objects += 1;
                Ok(())
            }
        }
    }
    async fn process_aged_family(
        &mut self,
        family: CandidateFamily,
        key: &str,
        deleted: fn(&mut DeletedObjectCounts) -> &mut u64,
    ) -> Result<()> {
        if family == CandidateFamily::Manifests
            && loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= self.live.discovery_start_manifest_no)
        {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        if self.live.objects.contains(key)
            || (family == CandidateFamily::WalSegments && self.live.protects_wal(key))
        {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
        }
        if family == CandidateFamily::Manifests {
            let successor = loonfs_objectstore::layout::manifest_no_of(key)
                .and_then(|number| number.successor().ok());
            if let Some(successor) = successor {
                let successor_key = loonfs_objectstore::keys::metadata_manifest_object(
                    self.namespace_id,
                    &successor,
                );
                let age = grace_age(
                    self.store,
                    &successor_key,
                    self.grace_window_ms,
                    self.mutation.now_ms,
                )
                .await
                .map_err(|error| CoreError::store(&successor_key, &error))?;
                // A reader that loaded a lagging hint may still be fetching the
                // predecessor while its successor is young.
                if let Some(reason) = age.retained_reason() {
                    self.report.retain(reason);
                    return Ok(());
                }
            }
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
        Ok(())
    }
    async fn process_checkpoint(&mut self, key: &str) -> Result<bool> {
        let decision = sweep_checkpoint_record(
            self.store,
            key,
            self.grace_window_ms,
            self.live.namespace_deleted,
            self.mutation,
        )
        .await?;
        let count = match decision {
            CheckpointSweep::DeleteFork => &mut self.report.released_checkpoints.fork,
            CheckpointSweep::DeleteUser => &mut self.report.released_checkpoints.expired,
            CheckpointSweep::DeleteSnapshot => &mut self.report.released_checkpoints.snapshot,
            CheckpointSweep::Gone => return Ok(true),
            CheckpointSweep::Retain => {
                self.report.retain(RetainedReason::CheckpointNotReleasable);
                return Ok(false);
            }
        };
        *count += 1;
        self.delete_key(key).await?;
        self.report.deleted.checkpoint_records += 1;
        Ok(true)
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
