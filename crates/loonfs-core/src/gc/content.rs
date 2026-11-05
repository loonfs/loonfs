//! Content roots from the extent filters of the layout segments that rooted
//! manifests list. On a family root, the rooted manifests include the layout
//! views of every other member of its fork family.

use super::charged_set::{Charge, ChargedSet};
use super::fork_family::FamilyWalkOutcome;
use super::live_set::{LiveSet, RetirementStatus};
use super::sweep::{ContentReference, Sweep};
use crate::error::{CoreError, Result};
use crate::in_flight::STORE_READS_IN_FLIGHT;
use crate::segment::index_block_load::load_segment_extent_filter;
use futures::{stream, StreamExt, TryStreamExt};
use loonfs_objectstore::keys::{content_prefix, metadata_segment_object_key};
use loonfs_objectstore::layout::content_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{MetadataRowFamily, MetadataSegmentRef};
use loonfs_types::format::segment_blocks::{filter_key_hashes, SegmentFilter};
use loonfs_types::GcResponse;
use std::sync::Arc;

/// The extent filters of the distinct layout segments that the collection's rooted
/// manifests list, which protect the content objects that those segments'
/// layout rows refer to.
///
/// A manifest's combined layout rows are rows of its layout segments, so every
/// extent of every rooted view is in the extent filter of some distinct
/// segment. A filter also covers superseded layout rows (a shorter layout of
/// a chain that a later row replaces), and it can report a name that no row
/// of its segment refers to. An object that only such a row or such a false
/// match keeps stays until no rooted manifest lists the segment, which delays
/// reclamation but never removes an object that a rooted view reads.
pub(super) struct ContentSweep {
    filters: Vec<Arc<SegmentFilter>>,
    _filters_charge: Charge,
}

impl ContentSweep {
    pub(super) async fn load<S: ObjectStore>(sweep: &Sweep<'_, S>) -> Result<Option<Self>> {
        let live = sweep.live;
        let namespace_id = &sweep.namespace.namespace_id;
        let (family_members, family_members_read) = (live.family.members, live.family.members_read);
        // A fork's content prefix stores nothing. A family root sweeps its
        // family's content whether it's active or deleted, until its
        // retirement lists and deletes the whole content prefix.
        if !live.collects_family_content() || live.retirement_status() == RetirementStatus::Eligible
        {
            tracing::info!(namespace_id = %namespace_id, layout_segments = 0,
                extent_filter_bytes = 0, family_members, family_members_read,
                "collecting namespace");
            return Ok(None);
        }
        match live.family.outcome {
            FamilyWalkOutcome::Complete => {}
            FamilyWalkOutcome::BudgetExceeded => {
                tracing::info!(namespace_id = %namespace_id, family_members, family_members_read,
                    "the fork family's roots weren't all read within the budget; skipped content sweep");
                return Ok(None);
            }
            FamilyWalkOutcome::UnreadableMember => {
                tracing::info!(namespace_id = %namespace_id, family_members, family_members_read,
                    "a fork family member's roots couldn't be read; skipped content sweep");
                return Ok(None);
            }
        }
        let segments = distinct_layout_segments(live)?;
        let mut filters_charge = Charge::new(&live.memory);
        let store = &sweep.namespace.store;
        let segment_cache = sweep.namespace.segment_cache.as_ref();
        let mut filters = Vec::with_capacity(segments.len());
        let mut extent_filter_bytes = 0;
        // Unboxed, rustc cannot prove the collection future `Send` through this stream.
        let mut loads = stream::iter(segments)
            .map(|descriptor| load_segment_extent_filter(store, segment_cache, descriptor))
            .buffered(STORE_READS_IN_FLIGHT)
            .boxed();
        while let Some(filter) = loads.try_next().await? {
            filters_charge.add(filter.decoded_bytes())?;
            extent_filter_bytes += filter.decoded_bytes();
            filters.push(filter);
        }
        tracing::info!(namespace_id = %namespace_id, layout_segments = filters.len(),
            extent_filter_bytes, family_members, family_members_read, "collecting namespace");
        Ok(Some(Self {
            filters,
            _filters_charge: filters_charge,
        }))
    }

    /// Lists the family root's content prefix once and decides each listed
    /// object from the extent filters and from the content IDs of the WAL
    /// tails and upload sessions.
    pub(super) async fn sweep<S: ObjectStore>(
        &self,
        sweep: &Sweep<'_, S>,
        report: &mut GcResponse,
    ) -> Result<()> {
        // Only a family root's collection sweeps content, so the collected
        // namespace's prefix is the family's.
        let prefix = content_prefix(&sweep.namespace.namespace_id);
        let decisions = sweep
            .namespace
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
                        .is_some_and(|id| sweep.live.content_ids.contains(&id))
                {
                    ContentReference::Referenced
                } else {
                    ContentReference::Unreferenced
                };
                sweep.content_candidate(entry, reference)
            });
        sweep.namespace.apply_decisions(decisions, report).await
    }
}

/// Lists each layout segment of the rooted manifests once, however many
/// manifests list it.
fn distinct_layout_segments(live: &LiveSet) -> Result<Vec<&MetadataSegmentRef>> {
    let mut covered = ChargedSet::new(&live.memory);
    let mut segments = Vec::new();
    for descriptor in live
        .manifests
        .iter()
        .flat_map(|manifest| &manifest.payload().runs)
        .flat_map(|run| &run.segments)
        .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
    {
        let key = metadata_segment_object_key(descriptor);
        if !covered.contains(&key) {
            covered.insert(key)?;
            segments.push(descriptor);
        }
    }
    Ok(segments)
}
