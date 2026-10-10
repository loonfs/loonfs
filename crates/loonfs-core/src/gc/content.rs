//! Content roots from layout views, held one id shard at a time.

use super::charged_set::ChargedSet;
use super::live_set::LiveSet;
use super::sweep::Sweep;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::manifest::cache::read_working_memory;
use crate::manifest::{metadata_basis_from_manifest, MetadataSegmentCache};
use crate::metadata::row_decode::content_layout_from_manifest_row;
use crate::namespace::control::LoadedManifest;
use crate::storage::content_location::extent_object_key;
use futures::StreamExt;
use loonfs_objectstore::keys::{content_prefix, metadata_segment_object_key};
use loonfs_objectstore::layout::content_id_of;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{lookup_keys, ContentLayoutRecord, MetadataRowFamily};
use loonfs_types::format::sst_blocks::string_prefix_upper_bound;
use loonfs_types::{ContentExtent, NamespaceId};
use std::collections::HashSet;

const LAYOUT_PAGE_ROWS: usize = 1024;

pub(super) struct ContentSweep<'a> {
    views: Vec<&'a LoadedManifest>,
    shared: ChargedSet<String>,
    pub(super) shard_width: u32,
}

impl<'a> ContentSweep<'a> {
    pub(super) async fn load<S: ObjectStore + ?Sized>(
        store: &S,
        cache: Option<&MetadataSegmentCache>,
        namespace_id: &NamespaceId,
        live: &'a LiveSet,
        shard_rows: usize,
    ) -> Result<Self> {
        let mut covered = HashSet::new();
        let mut views = Vec::new();
        for manifest in &live.manifests {
            let segments: HashSet<_> = manifest
                .state
                .envelope
                .payload()
                .runs
                .iter()
                .flat_map(|run| &run.segments)
                .filter(|segment| segment.family == MetadataRowFamily::ContentLayouts)
                .map(metadata_segment_object_key)
                .collect();
            if !segments.is_subset(&covered) {
                covered.extend(segments);
                views.push(manifest);
            }
        }
        let mut shard_width = 0;
        while shard_width < 4
            && live.content_layout_rows / 16_u64.pow(shard_width) > shard_rows as u64
        {
            shard_width += 1;
        }
        let shared = collect_layout_keys(store, cache, namespace_id, &views, "", |row, extent| {
            extent.content_id != row.content_id
        })
        .await?;
        Ok(Self {
            views,
            shared,
            shard_width,
        })
    }

    pub(super) fn view_count(&self) -> usize {
        self.views.len()
    }

    pub(super) async fn sweep<S: ObjectStore + ?Sized>(
        &self,
        sweep: &mut Sweep<'_, S>,
        cache: Option<&MetadataSegmentCache>,
    ) -> Result<()> {
        for shard in 0..16_u32.pow(self.shard_width) {
            let id_prefix = if self.shard_width == 0 {
                "con_".to_owned()
            } else {
                format!("con_{shard:0width$x}", width = self.shard_width as usize)
            };
            let keys = collect_layout_keys(
                sweep.store,
                cache,
                sweep.namespace_id,
                &self.views,
                &id_prefix,
                |_, extent| extent.content_id.as_str().starts_with(&id_prefix),
            )
            .await?;
            let prefix = format!("{}{id_prefix}", content_prefix(sweep.namespace_id));
            let mut listing = sweep.store.list_entries_from_stream(&prefix, None);
            while let Some(entry) = listing
                .next()
                .await
                .transpose()
                .map_err(|error| CoreError::store(&prefix, &error))?
            {
                let protected = keys.contains(entry.key.as_str())
                    || self.shared.contains(entry.key.as_str())
                    || content_id_of(&entry.key)
                        .is_some_and(|id| sweep.live.content_ids.contains(&id));
                sweep.content_candidate(&entry, protected).await?;
            }
        }
        Ok(())
    }
}

async fn collect_layout_keys<S: ObjectStore + ?Sized>(
    store: &S,
    cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    views: &[&LoadedManifest],
    id_prefix: &str,
    include: impl Fn(&ContentLayoutRecord, &ContentExtent) -> bool,
) -> Result<ChargedSet<String>> {
    let mut keys = ChargedSet::new(namespace_id, read_working_memory(cache));
    let family = MetadataRowFamily::ContentLayouts;
    let prefix = format!("{}{id_prefix}", family.row_key_prefix());
    let upper_bound = string_prefix_upper_bound(&prefix);
    for manifest in views {
        let segments = metadata_basis_from_manifest(store, cache, manifest).segments;
        let mut lower_bound = prefix.clone();
        loop {
            let rows = segments
                .scan_range_page_with_keys(
                    family,
                    &lower_bound,
                    upper_bound.as_deref(),
                    LAYOUT_PAGE_ROWS,
                )
                .await
                .map_err(MetadataProjectionLoadError::from)?;
            let Some((last_key, _)) = rows.last() else {
                break;
            };
            lower_bound = lookup_keys::after_row_key(last_key);
            let exhausted = rows.len() < LAYOUT_PAGE_ROWS;
            for (_, row) in rows {
                let row = content_layout_from_manifest_row(row)?;
                for extent in &row.layout.extents {
                    if extent.owner_namespace_id == *namespace_id && include(&row, extent) {
                        keys.insert(extent_object_key(extent))?;
                    }
                }
            }
            if exhausted {
                break;
            }
        }
    }
    Ok(keys)
}
