//! Sweep candidates against the live set loaded for this call.
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use super::reap::{grace_age, sweep_pin, GraceAge, PinSweep};
use super::uploads::{sweep_upload_session, UploadSessionSweep};
use crate::context::MutationContext;
use crate::control_update::load_upload_session_state;
use crate::error::{CoreError, Result};
use crate::limits::UNREFERENCED_SEGMENT_MIN_AGE_MS;
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{DeletedObjectCounts, GcResponse, NamespaceId, RetainedReason};

pub(super) struct Sweep<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) grace_window_ms: u64,
    pub(super) mutation: &'a MutationContext,
    pub(super) live: &'a LiveSet,
    pub(super) report: &'a mut GcResponse,
}

impl<S: ObjectStore + ?Sized> Sweep<'_, S> {
    pub(super) async fn candidate(&mut self, family: CandidateFamily, key: &str) -> Result<()> {
        if !family.recognizes(key) {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(());
        }
        match family {
            CandidateFamily::WalObjects => {
                self.process_aged_family(family, key, |counts| &mut counts.wal_objects)
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
            CandidateFamily::Pins => self.process_pin(key).await,
            CandidateFamily::UploadSessions => self.process_upload_session(key).await,
            CandidateFamily::Content => {
                self.process_aged_family(family, key, |counts| &mut counts.content_objects)
                    .await
            }
            CandidateFamily::Scratch => {
                self.process_aged_family(family, key, |counts| &mut counts.scratch_objects)
                    .await
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
            || (family == CandidateFamily::WalObjects && self.live.protects_wal(key))
            || (family == CandidateFamily::Content && self.live.protects_content(key))
        {
            self.report.retain(RetainedReason::Referenced);
            return Ok(());
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
    async fn process_pin(&mut self, key: &str) -> Result<()> {
        let decision = sweep_pin(
            self.store,
            key,
            self.grace_window_ms,
            self.live,
            self.mutation,
        )
        .await?;
        let count = match decision {
            PinSweep::DeleteFork => &mut self.report.deleted_checkpoints_by_owner.fork,
            PinSweep::DeleteUser => &mut self.report.deleted_checkpoints_by_owner.user,
            PinSweep::DeleteSnapshot => &mut self.report.deleted_checkpoints_by_owner.snapshot,
            PinSweep::Gone => return Ok(()),
            PinSweep::Retain { reclaimable_at_ms } => {
                self.report.retain(RetainedReason::CheckpointNotDeletable);
                self.note_reclamation_deadline(reclaimable_at_ms);
                return Ok(());
            }
        };
        *count += 1;
        self.delete_key(key).await?;
        Ok(())
    }

    async fn process_upload_session(&mut self, key: &str) -> Result<()> {
        let Some(upload_id) = upload_id_of(key) else {
            self.report.retain(RetainedReason::UnrecognizedKey);
            return Ok(());
        };
        let state = match load_upload_session_state(self.store, self.namespace_id, &upload_id).await
        {
            Ok(state) => state,
            Err(CoreError::UploadNotFound { .. }) => {
                self.report.retain(RetainedReason::UploadSessionUndecided);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        match sweep_upload_session(self, &state).await? {
            UploadSessionSweep::Delete => {
                self.delete_key(key).await?;
                self.report.deleted.upload_sessions += 1;
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
        let age = grace_age(self.store, key, grace_window_ms, self.mutation.now_ms)
            .await
            .map_err(|error| CoreError::store(key, &error))?;
        if let Some(reason) = age.retained_reason() {
            self.report.retain(reason);
            return Ok(false);
        }
        if age == GraceAge::Gone {
            return Ok(false);
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
