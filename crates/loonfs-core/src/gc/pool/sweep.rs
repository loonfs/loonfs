//! The decisions of a pool's collection about the content objects, upload
//! records, and temporary objects under the pool's prefix.

use super::members::PoolRoots;
use super::uploads::{
    sweep_completed_session, ListedUpload, ListedUploadKind, OutcomeListing, UploadSessionSweep,
};
use crate::error::Result;
use crate::gc::deletes::Decision;
use crate::gc::session_cache::{CompletedUploadSession, CompletedUploadSessionCache};
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::pool_store::PoolStore;
use crate::time::OperationClock;
use crate::upload::records::upload_session_record_keys;
use loonfs_objectstore::layout::{content_id_of, parse_object_key, DurableObjectFamily};
use loonfs_objectstore::{ListedObject, ObjectStore};
use loonfs_types::{PoolGcResponse, PoolRetainedReason, UploadId};
use std::sync::atomic::{AtomicU64, Ordering};

/// Whether the collection's roots reference one listed content object.
#[derive(Clone, Copy)]
pub(super) enum ContentReference {
    Referenced,
    Unreferenced,
}

pub(super) struct PoolSweep<'a, S: ObjectStore> {
    pub(super) pool: &'a PoolStore<S>,
    pub(super) clock: &'a OperationClock,
    pub(super) roots: &'a PoolRoots,
    pub(super) session_cache: Option<&'a CompletedUploadSessionCache>,
    /// The bytes of the listed objects that reached this sweep, as the
    /// listings reported them.
    pub(super) listed_bytes: AtomicU64,
    /// The content objects and temporary objects that the sweep's listings
    /// returned.
    pub(super) listed_objects: AtomicU64,
}

impl<S: ObjectStore> PoolSweep<'_, S> {
    pub(super) fn content_candidate(
        &self,
        entry: ListedObject,
        reference: ContentReference,
    ) -> Decision<PoolGcResponse> {
        self.add_listed_object(entry.size_bytes);
        if content_id_of(&entry.key).is_none() {
            return self.retain(PoolRetainedReason::UnrecognizedKey, None);
        }
        match reference {
            ContentReference::Referenced => self.retain(PoolRetainedReason::Referenced, None),
            ContentReference::Unreferenced => {
                self.aged(entry, |report| &mut report.deleted.content_objects)
            }
        }
    }

    pub(super) fn temporary_candidate(&self, entry: ListedObject) -> Decision<PoolGcResponse> {
        self.add_listed_object(entry.size_bytes);
        let family = parse_object_key(&entry.key).map(|parsed| parsed.family());
        if family != Some(DurableObjectFamily::TemporaryObject) {
            return self.retain(PoolRetainedReason::UnrecognizedKey, None);
        }
        self.aged(entry, |report| &mut report.deleted.temporary_objects)
    }

    /// Determines what to do with the records of one entry of the uploads
    /// listing that isn't a completed session.
    pub(super) async fn upload(&self, listed: &ListedUpload) -> Result<Decision<PoolGcResponse>> {
        match &listed.kind {
            ListedUploadKind::Session {
                upload_id,
                outcome_listing,
            } => {
                self.process_upload_session(upload_id.clone(), *outcome_listing)
                    .await
            }
            // An outcome record without its session record is deleted only
            // once it's at least the grace window old by the listing's
            // modification time, the age rule of the other unreferenced
            // families. A listing isn't a snapshot, so it can miss the
            // session record of a session that was created and ended while
            // the listing ran. Deleting that session's new outcome would make
            // the session read as open, and a later collection would abort it and
            // delete content that a commit might reference. That outcome was
            // created after the collection started, so it's younger than the window.
            ListedUploadKind::OrphanOutcome { entry } => {
                Ok(self.aged(entry.clone(), |report| &mut report.deleted.upload_sessions))
            }
            ListedUploadKind::Unrecognized => {
                Ok(self.retain(PoolRetainedReason::UnrecognizedKey, None))
            }
        }
    }

    /// Determines what to do with the records of one session that the
    /// collection listed as completed.
    ///
    /// The collection listed the session as completed before the member
    /// listing, so the collection decides the session from what it knows of
    /// the session's namespace, including when the listing didn't include the
    /// namespace because it has been retired.
    pub(super) fn completed_session(
        &self,
        upload_id: &UploadId,
        session: &CompletedUploadSession,
    ) -> Decision<PoolGcResponse> {
        let sweep = sweep_completed_session(
            self.roots.find_session_namespace(&session.namespace_id),
            self.clock.request_at_ms(),
            session.completed_at_ms,
        );
        // The sweep adds a kept session to the cache, so a later collection
        // doesn't read its record. Adding it here instead of when the
        // record is read means that this collection's inserts can't evict an
        // entry that its own listing hasn't looked up yet.
        if let (Some(cache), UploadSessionSweep::Retain { .. }) = (self.session_cache, sweep) {
            cache.insert(upload_id.clone(), session.clone());
        }
        self.session_decision(upload_id, sweep)
    }

    /// Determines what to do with the records of a listed session that the
    /// collection didn't list as completed.
    ///
    /// An open session can end during the collection, and an aborted
    /// session's cleanup needs its session record, so the sweep reads these
    /// records again.
    async fn process_upload_session(
        &self,
        upload_id: UploadId,
        outcome_listing: OutcomeListing,
    ) -> Result<Decision<PoolGcResponse>> {
        let Some(state) = self
            .pool
            .load_swept_session(&upload_id, outcome_listing)
            .await?
        else {
            return Ok(self.retain(PoolRetainedReason::UploadSessionUndecided, None));
        };
        let sweep = self
            .pool
            .sweep_upload_session(self.roots, self.clock, &state)
            .await?;
        Ok(self.session_decision(&upload_id, sweep))
    }

    fn session_decision(
        &self,
        upload_id: &UploadId,
        sweep: UploadSessionSweep,
    ) -> Decision<PoolGcResponse> {
        match sweep {
            UploadSessionSweep::Delete => {
                if let Some(cache) = self.session_cache {
                    cache.invalidate(upload_id);
                }
                Decision::Delete {
                    keys: upload_session_record_keys(&self.pool.pool_id, upload_id).into(),
                    counter: |report| &mut report.deleted.upload_sessions,
                }
            }
            UploadSessionSweep::Retain { reclaimable_at_ms } => self.retain(
                match reclaimable_at_ms {
                    Some(_) => PoolRetainedReason::UploadSessionWindow,
                    None => PoolRetainedReason::UploadSessionUndecided,
                },
                reclaimable_at_ms,
            ),
        }
    }

    fn aged(
        &self,
        entry: ListedObject,
        counter: fn(&mut PoolGcResponse) -> &mut u64,
    ) -> Decision<PoolGcResponse> {
        Decision::aged(
            entry,
            GC_GRACE_WINDOW_MS,
            self.clock.request_at_ms(),
            counter,
        )
    }

    fn retain(
        &self,
        reason: PoolRetainedReason,
        reclaimable_at_ms: Option<u64>,
    ) -> Decision<PoolGcResponse> {
        Decision::retain(reason, reclaimable_at_ms, self.clock.request_at_ms())
    }

    fn add_listed_bytes(&self, size_bytes: u64) {
        self.listed_bytes.fetch_add(size_bytes, Ordering::Relaxed);
    }

    /// Adds one object that a listing of the content or temporary prefix
    /// returned.
    fn add_listed_object(&self, size_bytes: u64) {
        self.add_listed_bytes(size_bytes);
        self.listed_objects.fetch_add(1, Ordering::Relaxed);
    }
}
