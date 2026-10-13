//! Content roots from layout views, held one id shard at a time.

use super::charged_set::ChargedSet;
use super::deletes::apply_decisions;
use super::live_set::LiveSet;
use super::sweep::Sweep;
use crate::bloom_filter::BloomFilter;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::{metadata_basis_from_manifest, MetadataSegmentCache};
use crate::metadata::row_decode::content_layout_from_manifest_row;
use crate::namespace::control::LoadedManifest;
use crate::storage::content_location::extent_object_key;
use futures::TryStreamExt;
use loonfs_objectstore::keys::{content_prefix, metadata_segment_object_key};
use loonfs_objectstore::layout::content_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{lookup_keys, ContentLayoutRecord, MetadataRowFamily};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{ContentExtent, GcResponse, NamespaceId};
use std::ops::ControlFlow;

const LAYOUT_PAGE_ROWS: usize = 1024;

/// Values one hex digit of a content ID takes, so each digit of shard width
/// multiplies a pass's shard count by this many.
const SHARDS_PER_HEX_DIGIT: u64 = 16;

/// Most hex digits of a content ID that one shard prefix covers. At width 4 a
/// pass makes 65,536 shard listings. Below about 4.3 billion layout rows,
/// shards of the default 65,536 rows need no more than four digits, so the cap
/// doesn't bind.
const MAX_CONTENT_SHARD_WIDTH: u32 = 4;

pub(super) struct ContentSweep<'a> {
    views: Vec<&'a LoadedManifest>,
    shared: BloomFilter,
    shard_width: u32,
}

impl<'a> ContentSweep<'a> {
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        cache: Option<&MetadataSegmentCache>,
        namespace_id: &NamespaceId,
        live: &'a LiveSet,
        shard_rows: usize,
        seed: u64,
        max_bytes: usize,
    ) -> Result<Option<Self>> {
        if live.current_tombstone.is_some() {
            tracing::info!(namespace_id = %namespace_id, shard_width = 0,
                layout_views = 0, "collecting namespace");
            return Ok(None);
        }
        let mut covered = ChargedSet::new(&live.memory);
        let mut views = Vec::new();
        for manifest in &live.manifests {
            let mut adds_segments = false;
            for key in manifest
                .state
                .envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
                .map(metadata_segment_object_key)
            {
                if !covered.contains(&key) {
                    covered.insert(key)?;
                    adds_segments = true;
                }
            }
            if adds_segments {
                views.push(manifest);
            }
        }
        let mut shard_width = 0;
        while shard_width < MAX_CONTENT_SHARD_WIDTH
            && live.content_layout_rows / SHARDS_PER_HEX_DIGIT.pow(shard_width) > shard_rows as u64
        {
            shard_width += 1;
        }
        let expected_entries = live.content_layout_rows;
        let Some(mut shared) = BloomFilter::with_max_bytes(expected_entries, seed, max_bytes)
        else {
            tracing::info!(namespace_id = %namespace_id, shard_width, layout_views = 0, expected_entries,
                "shared base filter exceeded its byte limit; skipped content sweep");
            return Ok(None);
        };
        tracing::info!(namespace_id = %namespace_id, shard_width,
            layout_views = views.len(), "collecting namespace");
        let scanned = scan_layout_extents(store, cache, namespace_id, &views, "", |row, extent| {
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
            tracing::info!(namespace_id = %namespace_id, shard_width, layout_views = views.len(), expected_entries,
                "shared base filter exceeded its byte limit; skipped content sweep");
            return Ok(None);
        }
        Ok(Some(Self {
            views,
            shared,
            shard_width,
        }))
    }

    pub(super) async fn sweep<S: ObjectStore + ?Sized>(
        &self,
        sweep: &Sweep<'_, S>,
        report: &mut GcResponse,
        cache: Option<&MetadataSegmentCache>,
    ) -> Result<()> {
        for shard in 0..SHARDS_PER_HEX_DIGIT.pow(self.shard_width) {
            let id_prefix = if self.shard_width == 0 {
                "con_".to_owned()
            } else {
                format!("con_{shard:0width$x}", width = self.shard_width as usize)
            };
            let mut keys = ChargedSet::new(&sweep.live.memory);
            let _ = scan_layout_extents(
                sweep.store,
                cache,
                sweep.namespace_id,
                &self.views,
                &id_prefix,
                |_, extent| {
                    if extent.content_id.as_str().starts_with(&id_prefix) {
                        keys.insert(extent_object_key(extent))?;
                    }
                    Ok(ControlFlow::Continue(()))
                },
            )
            .await?;
            let prefix = format!("{}{id_prefix}", content_prefix(sweep.namespace_id));
            let decisions = sweep
                .store
                .list_entries_from_stream(&prefix, None)
                .map_err(|error| CoreError::store(&prefix, &error))
                .map_ok(|entry| {
                    let protected = keys.contains(entry.key.as_str())
                        || self.shared.may_contain(entry.key.as_bytes())
                        || content_id_of(&entry.key)
                            .is_some_and(|id| sweep.live.content_ids.contains(&id));
                    sweep.content_candidate(entry, protected)
                });
            apply_decisions(sweep.store, decisions, report).await?;
        }
        Ok(())
    }
}

async fn scan_layout_extents<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    views: &[&LoadedManifest],
    id_prefix: &str,
    mut visit: impl FnMut(&ContentLayoutRecord, &ContentExtent) -> Result<ControlFlow<()>>,
) -> Result<ControlFlow<()>> {
    let family = MetadataRowFamily::ContentLayouts;
    let prefix = format!("{}{id_prefix}", family.row_key_prefix());
    let upper_bound = string_prefix_upper_bound(&prefix);
    for manifest in views {
        let mut lower_bound = prefix.clone();
        loop {
            // The scan memo can fill the budget that the shard's keys need.
            let rows = {
                let segments = metadata_basis_from_manifest(store, cache, manifest).segments;
                segments
                    .scan_range_page_with_keys(
                        family,
                        &lower_bound,
                        upper_bound.as_deref(),
                        LAYOUT_PAGE_ROWS,
                    )
                    .await
                    .map_err(MetadataProjectionLoadError::from)?
            };
            let Some((last_key, _)) = rows.last() else {
                break;
            };
            lower_bound = lookup_keys::after_row_key(last_key);
            let exhausted = rows.len() < LAYOUT_PAGE_ROWS;
            for (_, row) in rows {
                let row = content_layout_from_manifest_row(row)?;
                for extent in &row.layout.extents {
                    if extent.owner_namespace_id == *namespace_id && visit(&row, extent)?.is_break()
                    {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
            if exhausted {
                break;
            }
        }
    }
    Ok(ControlFlow::Continue(()))
}
