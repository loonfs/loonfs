//! One collection of a pool's content objects and upload records, with a
//! fixed clock and no durable progress.

mod content;
mod members;
mod sweep;
mod uploads;

use super::charged_set::CollectionMemory;
use super::deletes::apply_decisions;
use super::session_cache::CompletedUploadSessionCache;
use crate::error::{CoreError, Result};
use crate::limits::MAX_COLLECTION_BYTES;
use crate::pool_store::PoolStore;
use crate::time::OperationClock;
use content::ContentFilters;
use futures::{stream, StreamExt};
use loonfs_objectstore::ObjectStore;
use loonfs_types::PoolGcResponse;
use members::PoolRoots;
use std::sync::atomic::{AtomicU64, Ordering};
use sweep::PoolSweep;

impl<S: ObjectStore> PoolStore<S> {
    /// Collects the pool's content objects and upload records, with roots
    /// that it reads from every member of the pool, found through the pool's
    /// member list (format spec, "Content roots").
    ///
    /// The collection lists the pool's upload sessions and member entries,
    /// reads each listed member's roots, and deletes the entry of a listed
    /// namespace that isn't a member. A pool with no member entries has no
    /// member roots, and the age rules delete its objects. The collection
    /// returns `pool_not_found` when no listing returned an entry.
    ///
    /// The content sweep reads the extent filter of each distinct layout
    /// segment of the members' rooted manifests through the segment cache,
    /// charges the filters to the collection's live set, and lists the
    /// content prefix once. The collection doesn't read the record of a
    /// completed upload session that `session_cache` has, and it adds each
    /// completed session that it keeps to `session_cache`.
    ///
    /// The collection's live set is at most [`MAX_COLLECTION_BYTES`], and a
    /// collection that needs more fails.
    pub async fn gc(
        &self,
        session_cache: Option<&CompletedUploadSessionCache>,
        clock: &OperationClock,
    ) -> Result<PoolGcResponse> {
        let memory = CollectionMemory::new(self.pool_id.to_string(), MAX_COLLECTION_BYTES);
        self.gc_within(session_cache, clock, memory).await
    }

    /// Collects as [`Self::gc`] does, with a live set charged to `memory`.
    pub(super) async fn gc_within(
        &self,
        session_cache: Option<&CompletedUploadSessionCache>,
        clock: &OperationClock,
        memory: CollectionMemory,
    ) -> Result<PoolGcResponse> {
        let mut report = PoolGcResponse::empty(self.pool_id.clone());
        let roots = PoolRoots::load(self, session_cache, clock, memory).await?;
        let filters = ContentFilters::load(self, &roots, &mut report).await?;
        let sweep = PoolSweep {
            pool: self,
            clock,
            roots: &roots,
            session_cache,
            listed_bytes: AtomicU64::new(0),
            listed_objects: AtomicU64::new(0),
        };
        match filters {
            Some(filters) => filters.sweep(&sweep, &mut report).await?,
            // A skipped content sweep leaves the content unlisted, but the
            // content still occupies storage.
            None => {
                let (bytes, objects) = self.list_content_bytes().await?;
                report.listed_bytes += bytes;
                sweep.listed_objects.fetch_add(objects, Ordering::Relaxed);
            }
        }
        // Upload sessions are swept as pairs of records, from the listing
        // that rooted their content.
        let completed = stream::iter(roots.completed_sessions.iter())
            .map(|(upload_id, session)| Ok(sweep.completed_session(upload_id, session)));
        let listed = stream::iter(roots.uploads.iter()).then(|listed| sweep.upload(listed));
        apply_decisions(&self.store, completed.chain(listed).boxed(), &mut report).await?;
        if roots.listed_objects + sweep.listed_objects.into_inner() == 0 {
            return Err(CoreError::PoolNotFound {
                pool_id: self.pool_id.clone(),
            });
        }
        report.listed_bytes +=
            roots.member_list_bytes + roots.upload_record_bytes + sweep.listed_bytes.into_inner();
        Ok(report)
    }
}
