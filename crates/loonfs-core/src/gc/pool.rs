//! One collection of a pool's content objects, upload records, and temporary
//! objects, with a fixed clock and no durable progress.

mod content;
mod members;
mod sweep;
mod uploads;

use super::deletes::apply_decisions;
use super::session_cache::CompletedUploadSessionCache;
use crate::error::{CoreError, Result};
use crate::limits::GC_GRACE_WINDOW_MS;
use crate::pool_store::PoolStore;
use crate::time::{Observation, OperationClock};
use content::ContentFilters;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{pool_record, temporary_prefix};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::control::PoolPayload;
use loonfs_types::PoolGcResponse;
pub(crate) use members::MemberState;
use members::PoolRoots;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use sweep::PoolSweep;

impl<S: ObjectStore> PoolStore<S> {
    /// Collects the pool's content objects, upload records, and temporary
    /// objects, with roots that it reads from every member of the pool (format
    /// spec, "Content roots").
    ///
    /// The collection reads the pool record to find the pool's first member,
    /// and returns `pool_not_found` without deleting anything when the record
    /// is missing. A pool whose create call failed or lost has no members, and
    /// the collection then deletes the pool record once the record's creation
    /// grace has passed, and lists nothing.
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
        let record = self
            .load_pool_record()
            .await?
            .ok_or_else(|| CoreError::PoolNotFound {
                pool_id: self.pool_id.clone(),
            })?;
        let mut report = PoolGcResponse::empty(self.pool_id.clone());
        let first_member = self.member(record.first_member_namespace_id.clone());
        let observed = Observation::now(Arc::clone(&first_member.timer));
        let first_current = match first_member.load_current_manifest().await {
            Err(CoreError::NamespaceNotFound { .. }) => None,
            Ok(manifest) if manifest.payload().pool_id != self.pool_id => None,
            current => Some(current),
        };
        let Some(first_current) = first_current else {
            self.collect_pool_record(&record, clock, &mut report)
                .await?;
            return Ok(report);
        };
        let roots = PoolRoots::load(
            self,
            &record.first_member_namespace_id,
            first_current,
            observed,
            session_cache,
            clock,
        )
        .await?;
        let sweep = PoolSweep {
            pool: self,
            clock,
            roots: &roots,
            session_cache,
            listed_bytes: AtomicU64::new(0),
        };
        match ContentFilters::load(&sweep, &mut report).await? {
            Some(filters) => filters.sweep(&sweep, &mut report).await?,
            // A skipped content sweep leaves the content unlisted, but the
            // content still occupies storage.
            None => report.listed_bytes += self.list_content_bytes().await?,
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
        report.listed_bytes += sweep.listed_bytes.into_inner();
        Ok(report)
    }

    /// Deletes the record of a pool without members once the record's
    /// creation grace has passed, or reports when the grace ends.
    ///
    /// The creation grace covers a create call that's still running, which the
    /// collection can't tell from a failed one.
    async fn collect_pool_record(
        &self,
        record: &PoolPayload,
        clock: &OperationClock,
        report: &mut PoolGcResponse,
    ) -> Result<()> {
        if clock.request_at_ms().saturating_sub(record.created_at_ms) < GC_GRACE_WINDOW_MS {
            report.next_reclamation_at_ms =
                Some(record.created_at_ms.saturating_add(GC_GRACE_WINDOW_MS));
            return Ok(());
        }
        let key = pool_record(&self.pool_id);
        self.store
            .delete(&key)
            .await
            .map_err(|error| CoreError::store(&key, &error))?;
        tracing::info!(pool_id = %self.pool_id, namespace_id = %record.first_member_namespace_id,
            "deleted the record of a pool without members");
        Ok(())
    }
}
