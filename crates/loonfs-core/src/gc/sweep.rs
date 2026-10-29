//! Sweep candidates against the live set loaded for this call.
use super::deletes::DeletedCounter;
use super::families::CandidateFamily;
use super::live_set::LiveSet;
use super::pins::{GraceAge, PinSweep};
use super::session_cache::CompletedUploadSessionCache;
use super::uploads::{
    sweep_completed_session, ListedUpload, ListedUploadKind, OutcomeListing, UploadSessionSweep,
};
use crate::error::Result;
use crate::namespace::store::NamespaceStore;
use crate::time::OperationClock;
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{RetainedReason, UploadId};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) enum Decision {
    Retain {
        reason: RetainedReason,
        reclaimable_at_ms: Option<u64>,
    },
    Delete {
        key: String,
        counter: DeletedCounter,
    },
    /// Deletes an ended upload session's records, in the order that
    /// [`crate::upload_records::delete_upload_session_records`] requires.
    DeleteUploadSession {
        upload_id: UploadId,
    },
    Gone,
}

/// Whether the pass's live set references one listed content object.
#[derive(Clone, Copy)]
pub(super) enum ContentReference {
    Referenced,
    Unreferenced,
}

pub(super) struct Sweep<'a, S: ObjectStore> {
    pub(super) namespace: &'a NamespaceStore<S>,
    pub(super) grace_window_ms: u64,
    pub(super) clock: &'a OperationClock,
    pub(super) live: &'a LiveSet,
    pub(super) session_cache: Option<&'a CompletedUploadSessionCache>,
    /// The bytes of the listed objects that reached this sweep, as the
    /// listings reported them.
    pub(super) listed_bytes: AtomicU64,
}

impl<S: ObjectStore> Sweep<'_, S> {
    pub(super) async fn candidate(
        &self,
        family: CandidateFamily,
        entry: ListedObject,
    ) -> Result<Decision> {
        self.add_listed_bytes(entry.size_bytes);
        if !family.recognizes(&entry.key) {
            return Ok(self.retain(RetainedReason::UnrecognizedKey, None));
        }
        let Some(deleted) = family.deleted_counter() else {
            return self.process_pin(entry.key).await;
        };
        if self.live.protects(family, &entry.key) {
            return Ok(self.retain(RetainedReason::Referenced, None));
        }
        Ok(self.sweep_aged(entry, family.min_age_ms(self.grace_window_ms), deleted))
    }

    async fn process_pin(&self, key: String) -> Result<Decision> {
        let decision = self
            .namespace
            .sweep_pin(&key, self.grace_window_ms, self.live, self.clock)
            .await?;
        let counter: DeletedCounter = match decision {
            PinSweep::DeleteFork => |report| &mut report.deleted_checkpoints_by_owner.fork,
            PinSweep::DeleteUser => |report| &mut report.deleted_checkpoints_by_owner.user,
            PinSweep::Gone => return Ok(Decision::Gone),
            PinSweep::Retain { reclaimable_at_ms } => {
                return Ok(self.retain(RetainedReason::CheckpointNotDeletable, reclaimable_at_ms));
            }
        };
        Ok(Decision::Delete { key, counter })
    }

    /// Decides what to do with one upload session's records.
    pub(super) async fn upload(&self, listed: ListedUpload) -> Result<Decision> {
        self.add_listed_bytes(listed.size_bytes);
        match listed.kind {
            ListedUploadKind::Session {
                upload_id,
                outcome_listing,
            } => {
                self.process_upload_session(upload_id, outcome_listing)
                    .await
            }
            // An outcome record without its session record is deleted only
            // once it's at least the grace window old by the listing's
            // modification time, the age rule of the other unreferenced
            // families. A listing isn't a snapshot, so it can miss the
            // session record of a session that was created and ended while
            // the listing ran. Deleting that session's new outcome would make
            // the session read as open, and a later pass would abort it and
            // delete content that a commit might reference. That outcome was
            // created after the pass started, so it's younger than the window.
            ListedUploadKind::OrphanOutcome { entry } => {
                Ok(self.sweep_aged(entry, self.grace_window_ms, |report| {
                    &mut report.deleted.upload_sessions
                }))
            }
            ListedUploadKind::Unrecognized => {
                Ok(self.retain(RetainedReason::UnrecognizedKey, None))
            }
        }
    }

    async fn process_upload_session(
        &self,
        upload_id: UploadId,
        outcome_listing: OutcomeListing,
    ) -> Result<Decision> {
        let sweep = match self.live.completed_sessions.get(&upload_id) {
            Some(session) => {
                let sweep = sweep_completed_session(
                    self.live,
                    self.clock.request_at_ms(),
                    session.completed_at_ms,
                );
                // The sweep adds a kept session to the cache, so a later pass
                // doesn't read its record. Adding it here instead of when the
                // record is read means that this pass's inserts can't evict an
                // entry that its own listing hasn't looked up yet.
                if let (Some(cache), UploadSessionSweep::Retain { .. }) =
                    (self.session_cache, sweep)
                {
                    cache.insert(upload_id.clone(), session.clone());
                }
                sweep
            }
            // An open session can end during the pass, and an aborted
            // session's cleanup needs its session record, so the sweep reads
            // these records again.
            None => {
                let Some(state) = self
                    .namespace
                    .load_swept_session(&upload_id, outcome_listing)
                    .await?
                else {
                    return Ok(self.retain(RetainedReason::UploadSessionUndecided, None));
                };
                self.namespace
                    .sweep_upload_session(self.grace_window_ms, self.live, self.clock, &state)
                    .await?
            }
        };
        if let (Some(cache), UploadSessionSweep::Delete) = (self.session_cache, sweep) {
            cache.invalidate(&upload_id);
        }
        Ok(match sweep {
            UploadSessionSweep::Delete => Decision::DeleteUploadSession { upload_id },
            UploadSessionSweep::Retain { reclaimable_at_ms } => self.retain(
                match reclaimable_at_ms {
                    Some(_) => RetainedReason::UploadSessionWindow,
                    None => RetainedReason::UploadSessionUndecided,
                },
                reclaimable_at_ms,
            ),
        })
    }

    pub(super) fn content_candidate(
        &self,
        entry: ListedObject,
        reference: ContentReference,
    ) -> Decision {
        self.add_listed_bytes(entry.size_bytes);
        if !CandidateFamily::Content.recognizes(&entry.key) {
            return self.retain(RetainedReason::UnrecognizedKey, None);
        }
        match reference {
            ContentReference::Referenced => self.retain(RetainedReason::Referenced, None),
            ContentReference::Unreferenced => {
                self.sweep_aged(entry, self.grace_window_ms, |report| {
                    &mut report.deleted.content_objects
                })
            }
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
            self.clock.request_at_ms(),
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
            reclaimable_at_ms: reclaimable_at_ms
                .filter(|at_ms| *at_ms > self.clock.request_at_ms()),
        }
    }

    fn add_listed_bytes(&self, size_bytes: u64) {
        self.listed_bytes.fetch_add(size_bytes, Ordering::Relaxed);
    }
}
