//! One collection of a pool's content objects, upload records, and temporary
//! objects, with a fixed clock and no durable progress.

mod content;
mod members;
mod sweep;
mod uploads;

use super::deletes::apply_decisions;
use super::session_cache::CompletedUploadSessionCache;
use crate::error::{CoreError, Result};
use crate::pool_store::PoolStore;
use crate::time::OperationClock;
use content::ContentFilters;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::temporary_prefix;
use loonfs_objectstore::ObjectStore;
use loonfs_types::PoolGcResponse;
pub(crate) use members::MemberState;
use members::PoolRoots;
use std::sync::atomic::{AtomicU64, Ordering};
use sweep::PoolSweep;

impl<S: ObjectStore> PoolStore<S> {
    /// Collects the pool's content objects, upload records, and temporary
    /// objects, with roots that it reads from every member of the pool, found
    /// through the pool's member list (format spec, "Content roots").
    ///
    /// The collection lists the pool's upload sessions and member entries,
    /// reads each listed member's roots, and deletes the entry of a listed
    /// namespace that isn't a member. A pool with no member entries has no
    /// member roots, and the age rules delete its objects. The collection
    /// returns `pool_not_found` when no listing returned an entry.
    ///
    /// The content sweep reads the extent filter of each distinct layout
    /// segment of the members' rooted manifests through the segment cache,
    /// charges the filters to that cache's read working memory, and lists the
    /// content prefix once. The collection doesn't read the record of a
    /// completed upload session that `session_cache` has, and it adds each
    /// completed session that it keeps to `session_cache`.
    pub async fn gc(
        &self,
        session_cache: Option<&CompletedUploadSessionCache>,
        clock: &OperationClock,
    ) -> Result<PoolGcResponse> {
        let mut report = PoolGcResponse::empty(self.pool_id.clone());
        let roots = PoolRoots::load(self, session_cache, clock).await?;
        let sweep = PoolSweep {
            pool: self,
            clock,
            roots: &roots,
            session_cache,
            listed_bytes: AtomicU64::new(0),
            listed_objects: AtomicU64::new(0),
        };
        match ContentFilters::load(&sweep, &mut report).await? {
            Some(filters) => filters.sweep(&sweep, &mut report).await?,
            // A skipped content sweep leaves the content unlisted, but the
            // content still occupies storage.
            None => {
                let (bytes, objects) = self.list_content_bytes().await?;
                report.listed_bytes += bytes;
                sweep.listed_objects.fetch_add(objects, Ordering::Relaxed);
            }
        }
        let prefix = temporary_prefix(&self.pool_id);
        let decisions = self
            .store
            .list_entries_from_stream(&prefix, None)
            .map_err(|error| CoreError::store(&prefix, &error))
            .map_ok(|entry| sweep.temporary_candidate(entry))
            .boxed();
        apply_decisions(&self.store, decisions, &mut report).await?;
        // Upload sessions are swept as pairs of records, from the listing
        // that rooted their content.
        let decisions = stream::iter(roots.uploads.iter())
            .then(|listed| sweep.upload(listed))
            .boxed();
        apply_decisions(&self.store, decisions, &mut report).await?;
        if roots.listed_objects + sweep.listed_objects.into_inner() == 0 {
            return Err(CoreError::PoolNotFound {
                pool_id: self.pool_id.clone(),
            });
        }
        report.listed_bytes += roots.member_list_bytes + sweep.listed_bytes.into_inner();
        Ok(report)
    }
}
