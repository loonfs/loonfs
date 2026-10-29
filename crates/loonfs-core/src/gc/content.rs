//! Content roots from the layout segments of rooted manifests, kept one ID
//! shard at a time.

use super::charged_set::{Charge, ChargedSet};
use super::live_set::LiveSet;
use super::sweep::{ContentReference, Sweep};
use crate::bloom_filter::BloomFilter;
use crate::error::{CoreError, Result};
use crate::metadata::row_decode::content_layout_from_manifest_row;
use crate::segment::{visit_segment_rows, ScanDescriptor};
use crate::storage::content_location::extent_object_key;
use futures::TryStreamExt;
use loonfs_objectstore::keys::{content_prefix, metadata_segment_object_key};
use loonfs_objectstore::layout::content_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{ContentLayoutRecord, MetadataRowFamily};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{generated_id_body_prefixes, u64_from_usize, ContentExtent, GcResponse};
use std::ops::ControlFlow;

/// Most characters of a content ID body that one shard prefix covers. At
/// width 4 a pass makes 262,144 shard listings. Below about 17 billion layout
/// rows, shards of the default 65,536 rows need no more than four characters,
/// so the cap doesn't bind.
const MAX_CONTENT_SHARD_WIDTH: usize = 4;

/// The layout segments that the pass's rooted manifests list, each scanned on
/// its own to protect the content objects that its layout rows refer to.
///
/// Each scan reads the rows of every distinct layout segment once, and doesn't
/// merge the segments of each rooted manifest. A manifest's merged layout rows
/// are the rows of its layout segments, with one row kept for a key that two
/// of its segments repeat. So every row of every manifest's merge is a row of
/// some distinct segment, and the scan protects at least the extents that each
/// merge would protect. It protects more only when two segments of one
/// manifest list the same key, which a valid manifest doesn't do (a compaction
/// refuses such input). Like a merge, the scan includes superseded layout rows
/// (a shorter layout of a chain that a later row replaces). An extent that
/// only a superseded row lists stays protected until a compaction drops that
/// row and no rooted manifest lists the segment that contains it, which delays
/// reclamation but never removes an object that a rooted view reads.
pub(super) struct ContentSweep<'a> {
    segments: Vec<ScanDescriptor<'a>>,
    _segments_charge: Charge,
    shared: BloomFilter,
    shard_width: usize,
}

impl<'a> ContentSweep<'a> {
    pub(super) async fn load<S: ObjectStore>(
        sweep: &Sweep<'a, S>,
        shard_rows: usize,
        max_bytes: usize,
    ) -> Result<Option<Self>> {
        let live = sweep.live;
        let namespace_id = &sweep.namespace.namespace_id;
        if live.current_tombstone.is_some() {
            tracing::info!(namespace_id = %namespace_id, shard_width = 0,
                layout_segments = 0, "collecting namespace");
            return Ok(None);
        }
        let (segments, segments_charge) = distinct_layout_segments(live)?;
        let mut shard_width = 0;
        while shard_width < MAX_CONTENT_SHARD_WIDTH
            && live.content_layout_rows
                / u64_from_usize(generated_id_body_prefixes(shard_width).len())
                > u64_from_usize(shard_rows)
        {
            shard_width += 1;
        }
        let expected_entries = live.content_layout_rows;
        if !BloomFilter::fits_at_full_size(expected_entries, max_bytes) {
            tracing::info!(namespace_id = %namespace_id, shard_width, layout_segments = 0, expected_entries,
                "shared base filter exceeded its byte limit; skipped content sweep");
            return Ok(None);
        }
        // The pass time seeds the filter, so a false match on an unreferenced
        // object in one pass is unlikely to repeat in the next pass.
        let mut shared =
            BloomFilter::with_max_bytes(expected_entries, sweep.clock.request_at_ms(), max_bytes);
        tracing::info!(namespace_id = %namespace_id, shard_width,
            layout_segments = segments.len(), "collecting namespace");
        let scanned = scan_layout_extents(sweep, &segments, "", |row, extent| {
            if extent.content_id != row.content_id
                && shared
                    .insert(extent_object_key(extent).as_bytes())
                    .is_none()
            {
                return Ok(ControlFlow::Break(()));
            }
            Ok(ControlFlow::Continue(()))
        })
        .await?;
        if scanned.is_break() {
            tracing::info!(namespace_id = %namespace_id, shard_width, layout_segments = segments.len(), expected_entries,
                "shared base filter exceeded its byte limit; skipped content sweep");
            return Ok(None);
        }
        Ok(Some(Self {
            segments,
            _segments_charge: segments_charge,
            shared,
            shard_width,
        }))
    }

    pub(super) async fn sweep<S: ObjectStore>(
        &self,
        sweep: &Sweep<'_, S>,
        report: &mut GcResponse,
    ) -> Result<()> {
        for body_prefix in generated_id_body_prefixes(self.shard_width) {
            let id_prefix = format!("con_{body_prefix}");
            let mut keys = ChargedSet::new(&sweep.live.memory);
            let _ = scan_layout_extents(sweep, &self.segments, &id_prefix, |_, extent| {
                if extent.content_id.as_str().starts_with(&id_prefix) {
                    keys.insert(extent_object_key(extent))?;
                }
                Ok(ControlFlow::Continue(()))
            })
            .await?;
            let prefix = format!(
                "{}{id_prefix}",
                content_prefix(&sweep.namespace.namespace_id)
            );
            let decisions = sweep
                .namespace
                .store
                .list_entries_from_stream(&prefix, None)
                .map_err(|error| CoreError::store(&prefix, &error))
                .map_ok(|entry| {
                    let reference = if keys.contains(entry.key.as_str())
                        || self.shared.may_contain(entry.key.as_bytes())
                        || content_id_of(&entry.key)
                            .is_some_and(|id| sweep.live.content_ids.contains(&id))
                    {
                        ContentReference::Referenced
                    } else {
                        ContentReference::Unreferenced
                    };
                    sweep.content_candidate(entry, reference)
                });
            sweep.namespace.apply_decisions(decisions, report).await?;
        }
        Ok(())
    }
}

/// Lists each layout segment of the rooted manifests once, with the sequence of
/// the first run that lists it, and charges the list to the pass.
fn distinct_layout_segments(live: &LiveSet) -> Result<(Vec<ScanDescriptor<'_>>, Charge)> {
    let mut covered = ChargedSet::new(&live.memory);
    let mut segments = Vec::new();
    for run in live
        .manifests
        .iter()
        .flat_map(|manifest| &manifest.payload().runs)
    {
        for descriptor in run
            .segments
            .iter()
            .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
        {
            let key = metadata_segment_object_key(descriptor);
            if !covered.contains(&key) {
                covered.insert(key)?;
                segments.push(ScanDescriptor {
                    descriptor,
                    max_seq: run.run_seq,
                });
            }
        }
    }
    let mut charge = Charge::new(&live.memory);
    charge.add(segments.capacity() * std::mem::size_of::<ScanDescriptor<'_>>())?;
    Ok((segments, charge))
}

async fn scan_layout_extents<S: ObjectStore>(
    sweep: &Sweep<'_, S>,
    segments: &[ScanDescriptor<'_>],
    id_prefix: &str,
    mut visit: impl FnMut(&ContentLayoutRecord, &ContentExtent) -> Result<ControlFlow<()>>,
) -> Result<ControlFlow<()>> {
    let prefix = format!(
        "{}{id_prefix}",
        MetadataRowFamily::ContentLayouts.row_key_prefix()
    );
    let upper_bound = string_prefix_upper_bound(&prefix);
    visit_segment_rows(
        &sweep.namespace.store,
        Some(sweep.namespace.segment_cache.as_ref()),
        segments,
        &prefix,
        upper_bound.as_deref(),
        |row| {
            let row = content_layout_from_manifest_row(row)?;
            for extent in &row.layout.extents {
                if extent.owner_namespace_id == sweep.namespace.namespace_id
                    && visit(&row, extent)?.is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        },
    )
    .await
}
