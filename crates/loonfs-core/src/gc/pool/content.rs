//! Content roots from the extent filters of the layout segments that the
//! rooted manifests of a pool's members list.

use super::members::{MemberReadOutcome, PoolRoots, UnreadRoot};
use super::sweep::{ContentReference, PoolSweep};
use crate::error::{CoreError, Result};
use crate::gc::charged_set::Charge;
use crate::gc::deletes::apply_decisions;
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::pool_store::PoolStore;
use crate::segment::index_block_load::load_segment_extent_filter;
use futures::{future, stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::content_prefix;
use loonfs_objectstore::layout::content_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::segment_blocks::{filter_key_hashes, SegmentFilter};
use loonfs_types::{GcSkippedWork, PoolGcResponse};
use std::sync::Arc;

/// The extent filters of the distinct layout segments that the members'
/// rooted manifests list, which protect the content objects that those
/// segments' layout rows refer to.
///
/// A manifest's combined layout rows are rows of its layout segments, so every
/// extent of every rooted view is in the extent filter of some distinct
/// segment. A filter also covers superseded layout rows (a shorter layout of
/// a chain that a later row replaces), and it can report a name that no row
/// of its segment refers to. An object that only such a row or such a false
/// match keeps stays until no rooted manifest lists the segment, which delays
/// reclamation but never removes an object that a rooted view reads.
pub(super) struct ContentFilters {
    filters: Vec<Arc<SegmentFilter>>,
    _filters_charge: Charge,
}

impl ContentFilters {
    /// Loads the extent filters of the rooted layout segments, or returns
    /// `None` when the collection couldn't read every member's roots, and
    /// records why in `report`.
    ///
    /// A filter that can't be loaded makes the collection skip content
    /// deletion, as an unreadable member does (see
    /// [`MemberReadReport::stop_at_unreadable_member`](super::members::MemberReadReport::stop_at_unreadable_member)).
    /// The segment can be gone because the member published a compaction
    /// after the collection read its roots, and the member's own collection
    /// deleted the replaced segment.
    pub(super) async fn load<S: ObjectStore>(
        pool: &PoolStore<S>,
        roots: &mut PoolRoots,
        report: &mut PoolGcResponse,
    ) -> Result<Option<Self>> {
        let skipped = match roots.member_read.outcome {
            MemberReadOutcome::Complete => match Self::load_filters(pool, roots).await? {
                Some(filters) => return Ok(Some(filters)),
                None => GcSkippedWork::ContentDeletionUnreadableMember,
            },
            MemberReadOutcome::BudgetExceeded => GcSkippedWork::ContentDeletionPoolBudget,
            MemberReadOutcome::UnreadableMember => GcSkippedWork::ContentDeletionUnreadableMember,
        };
        tracing::error!(pool_id = %pool.pool_id, ?skipped,
            members = roots.member_read.members,
            members_read = roots.member_read.members_read,
            "skipped the pool's content sweep");
        report.skipped.push(skipped);
        Ok(None)
    }

    /// Loads the extent filter of each rooted layout segment and charges it
    /// to the collection's budget, or returns `None` when a filter can't be
    /// loaded, and records that in `roots`.
    async fn load_filters<S: ObjectStore>(
        pool: &PoolStore<S>,
        roots: &mut PoolRoots,
    ) -> Result<Option<Self>> {
        let mut filters_charge = Charge::new(&roots.memory);
        let store = &pool.store;
        let segment_cache = pool.segment_cache.as_ref();
        let mut filters = Vec::new();
        let mut extent_filter_bytes = 0;
        // Unboxed, rustc cannot prove the collection future `Send` through this stream.
        let mut loads = stream::iter(roots.layout_segments.iter())
            .map(|(key, descriptor)| async move {
                let filter = load_segment_extent_filter(store, segment_cache, descriptor).await;
                (key, filter)
            })
            .buffered(STORE_READS_IN_FLIGHT)
            .boxed();
        while let Some((key, filter)) = loads.next().await {
            let filter = match filter {
                Ok(filter) => filter,
                Err(error) => {
                    roots.member_read.stop_at_unreadable_member(
                        &pool.pool_id,
                        UnreadRoot::ExtentFilter(key),
                        error.into(),
                    )?;
                    return Ok(None);
                }
            };
            filters_charge.add(filter.decoded_bytes())?;
            extent_filter_bytes += filter.decoded_bytes();
            filters.push(filter);
        }
        tracing::info!(pool_id = %pool.pool_id, layout_segments = filters.len(),
            extent_filter_bytes, members = roots.member_read.members,
            members_read = roots.member_read.members_read, "collecting pool");
        Ok(Some(Self {
            filters,
            _filters_charge: filters_charge,
        }))
    }

    /// Lists the pool's content prefix once and decides each listed object
    /// from the extent filters and from the content IDs of the WAL tails and
    /// upload sessions.
    pub(super) async fn sweep<S: ObjectStore>(
        &self,
        sweep: &PoolSweep<'_, S>,
        report: &mut PoolGcResponse,
    ) -> Result<()> {
        let prefix = content_prefix(&sweep.pool.pool_id);
        let decisions = sweep
            .pool
            .store
            .list_entries_from_stream(&prefix, None)
            .map_err(|error| CoreError::store(&prefix, &error))
            .map_ok(|entry| {
                let name = entry.key.strip_prefix(prefix.as_str()).unwrap_or_default();
                let hashes = filter_key_hashes(name);
                let reference = if self
                    .filters
                    .iter()
                    .any(|filter| filter.may_contain_hashes(hashes))
                    || content_id_of(&entry.key)
                        .is_some_and(|id| sweep.roots.content_ids.contains(&id))
                {
                    ContentReference::Referenced
                } else {
                    ContentReference::Unreferenced
                };
                sweep.content_candidate(entry, reference)
            });
        apply_decisions(&sweep.pool.store, decisions, report).await
    }
}

impl<S: ObjectStore> PoolStore<S> {
    /// Lists the pool's content prefix and returns the sum of the listed
    /// object sizes and the number of listed objects.
    pub(super) async fn list_content_bytes(&self) -> Result<(u64, u64)> {
        let prefix = content_prefix(&self.pool_id);
        self.store
            .list_entries_from_stream(&prefix, None)
            .map_err(|error| CoreError::store(&prefix, &error))
            .try_fold((0, 0), |(bytes, objects), entry| {
                future::ok((bytes + entry.size_bytes, objects + 1))
            })
            .await
    }
}
