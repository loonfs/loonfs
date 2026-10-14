//! Sweep candidates against the live set loaded for this call.
use super::deletes::DeletedCounter;
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use super::reap::{sweep_pin, GraceAge, PinSweep};
use super::session_cache::CompletedUploadSessionCache;
use super::uploads::{sweep_completed_session, sweep_upload_session, UploadSessionSweep};
use crate::context::MutationContext;
use crate::control_update::load_upload_session_state;
use crate::error::{CoreError, Result};
use crate::limits::UNREFERENCED_SEGMENT_GRACE_WINDOW_MS;
use loonfs_objectstore::layout::upload_id_of;
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{NamespaceId, RetainedReason};

pub(super) enum Decision {
    Retain {
        reason: RetainedReason,
        reclaimable_at_ms: Option<u64>,
    },
    Delete {
        key: String,
        counter: DeletedCounter,
    },
    Gone,
}

pub(super) struct Sweep<'a, S: ObjectStore + ?Sized> {
    pub(super) store: &'a S,
    pub(super) namespace_id: &'a NamespaceId,
    pub(super) grace_window_ms: u64,
    pub(super) mutation: &'a MutationContext,
    pub(super) live: &'a LiveSet,
    pub(super) session_cache: Option<&'a CompletedUploadSessionCache>,
}

impl<S: ObjectStore + ?Sized> Sweep<'_, S> {
    pub(super) async fn candidate(
        &self,
        family: CandidateFamily,
        entry: ListedObject,
    ) -> Result<Decision> {
        if !family.recognizes(&entry.key) {
            return Ok(self.retain(RetainedReason::UnrecognizedKey, None));
        }
        Ok(match family {
            CandidateFamily::WalObjects => {
                self.process_aged_family(family, entry, |report| &mut report.deleted.wal_objects)
            }
            CandidateFamily::MetadataSegments => {
                self.process_aged_family(family, entry, |report| {
                    &mut report.deleted.metadata_segments
                })
            }
            CandidateFamily::Manifests => {
                self.process_aged_family(family, entry, |report| &mut report.deleted.manifests)
            }
            CandidateFamily::Pins => self.process_pin(entry.key).await?,
            CandidateFamily::UploadSessions => self.process_upload_session(entry.key).await?,
            CandidateFamily::Content => self
                .process_aged_family(family, entry, |report| &mut report.deleted.content_objects),
            CandidateFamily::Temporary => self.process_aged_family(family, entry, |report| {
                &mut report.deleted.temporary_objects
            }),
        })
    }
    fn process_aged_family(
        &self,
        family: CandidateFamily,
        entry: ListedObject,
        deleted: DeletedCounter,
    ) -> Decision {
        let key = entry.key.as_str();
        if family == CandidateFamily::Manifests
            && loonfs_objectstore::layout::manifest_no_of(key)
                .is_some_and(|number| number >= self.live.discovery_start_manifest_no)
        {
            return self.retain(RetainedReason::Referenced, None);
        }
        if self.live.objects.contains(key)
            || (family == CandidateFamily::WalObjects && self.live.protects_wal(key))
        {
            return self.retain(RetainedReason::Referenced, None);
        }
        // A compaction may publish output that is exactly the minimum age
        // old, so a segment must be strictly older before it goes.
        let min_age_ms = if family == CandidateFamily::MetadataSegments {
            UNREFERENCED_SEGMENT_GRACE_WINDOW_MS + 1
        } else {
            self.grace_window_ms
        };
        self.sweep_aged(entry, min_age_ms, deleted)
    }
    async fn process_pin(&self, key: String) -> Result<Decision> {
        let decision = sweep_pin(
            self.store,
            &key,
            self.grace_window_ms,
            self.live,
            self.mutation,
        )
        .await?;
        let counter: DeletedCounter = match decision {
            PinSweep::DeleteFork => |report| &mut report.deleted_checkpoints_by_owner.fork,
            PinSweep::DeleteUser => |report| &mut report.deleted_checkpoints_by_owner.user,
            PinSweep::DeleteSnapshot => |report| &mut report.deleted_checkpoints_by_owner.snapshot,
            PinSweep::Gone => return Ok(Decision::Gone),
            PinSweep::Retain { reclaimable_at_ms } => {
                return Ok(self.retain(RetainedReason::CheckpointNotDeletable, reclaimable_at_ms));
            }
        };
        Ok(Decision::Delete { key, counter })
    }

    async fn process_upload_session(&self, key: String) -> Result<Decision> {
        let Some(upload_id) = upload_id_of(&key) else {
            return Ok(self.retain(RetainedReason::UnrecognizedKey, None));
        };
        let sweep = match self.live.completed_sessions.get(&upload_id) {
            Some(session) => {
                let sweep = sweep_completed_session(
                    self.live,
                    self.mutation.now_ms,
                    session.completed_at_ms,
                );
                // The sweep adds a kept session to the cache, so a later pass
                // doesn't read its record. Adding it here instead of when the
                // record is read means that this pass's inserts can't drop an
                // entry that its own listing hasn't looked up yet.
                if let (Some(cache), UploadSessionSweep::Retain { .. }) =
                    (self.session_cache, sweep)
                {
                    cache.insert(upload_id.clone(), session.clone());
                }
                sweep
            }
            // An open session's record can change during the pass, and an
            // aborted session's cleanup needs the whole record, so the sweep
            // reads these records again.
            None => {
                let state = match load_upload_session_state(
                    self.store,
                    self.namespace_id,
                    &upload_id,
                )
                .await
                {
                    Ok(state) => state,
                    Err(CoreError::UploadNotFound { .. }) => {
                        return Ok(self.retain(RetainedReason::UploadSessionUndecided, None));
                    }
                    Err(error) => return Err(error),
                };
                sweep_upload_session(
                    self.store,
                    self.grace_window_ms,
                    self.live,
                    self.mutation,
                    &state,
                )
                .await?
            }
        };
        if let (Some(cache), UploadSessionSweep::Delete) = (self.session_cache, sweep) {
            cache.invalidate(&upload_id);
        }
        Ok(self.upload_session_decision(key, sweep))
    }

    fn upload_session_decision(&self, key: String, sweep: UploadSessionSweep) -> Decision {
        match sweep {
            UploadSessionSweep::Delete => Decision::Delete {
                key,
                counter: |report| &mut report.deleted.upload_sessions,
            },
            UploadSessionSweep::Retain { reclaimable_at_ms } => self.retain(
                match reclaimable_at_ms {
                    Some(_) => RetainedReason::UploadSessionWindow,
                    None => RetainedReason::UploadSessionUndecided,
                },
                reclaimable_at_ms,
            ),
        }
    }

    pub(super) fn content_candidate(&self, entry: ListedObject, protected: bool) -> Decision {
        if !CandidateFamily::Content.recognizes(&entry.key) {
            self.retain(RetainedReason::UnrecognizedKey, None)
        } else if protected {
            self.retain(RetainedReason::Referenced, None)
        } else {
            self.sweep_aged(entry, self.grace_window_ms, |report| {
                &mut report.deleted.content_objects
            })
        }
    }

    fn sweep_aged(
        &self,
        entry: ListedObject,
        grace_window_ms: u64,
        counter: DeletedCounter,
    ) -> Decision {
        let age = GraceAge::of(
            entry.last_modified_at_ms,
            grace_window_ms,
            self.mutation.now_ms,
        );
        match age.retained_reason() {
            Some(reason) => self.retain(reason, None),
            None => Decision::Delete {
                key: entry.key,
                counter,
            },
        }
    }

    fn retain(&self, reason: RetainedReason, reclaimable_at_ms: Option<u64>) -> Decision {
        Decision::Retain {
            reason,
            reclaimable_at_ms: reclaimable_at_ms.filter(|at_ms| *at_ms > self.mutation.now_ms),
        }
    }
}
