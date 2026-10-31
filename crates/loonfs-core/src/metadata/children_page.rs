//! One page of a directory's visible children in a
//! [`MetadataViewSession`]: one scan of the parent's slot-index rows, merged
//! with the WAL tail's rows, that reads each edge's value at the session's
//! sequence and skips the edge's older rows.

use super::durable_cache::ParentNameCacheKey;
use super::manifest_index;
use super::manifest_tail_merge::{ManifestPage, ManifestTailMerge, TieOrder};
use super::visibility;
use super::DirentryBindingRecord;
use crate::error::Result;
use crate::limits::DIRECTORY_PAGE_RAW_SCAN_LIMIT;
use crate::metadata::view_session::preload_inode_groups;
use crate::metadata::view_session::MetadataViewSession;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys;
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{InodeId, InodeKind, NameKey};

#[derive(Debug, Clone)]
pub(crate) struct VisibleChildEntry {
    /// The edge's value at the session's sequence: a bound slot-index row,
    /// which includes a file's revision summary, and whose sequence is a
    /// directory's subtree sequence.
    pub(crate) binding: DirentryBindingRecord,
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataViewSession<'a, 'store, S> {
    /// Lists up to `limit` visible children of `parent_inode_id` after
    /// `start_after_name_key`, in name-key order.
    ///
    /// The scan reads the parent's `direntry_binds` rows in key order. Each
    /// edge's rows run newest first, so the scan takes an edge's first row at
    /// or below the session's sequence as the edge's value, and then seeks
    /// past the edge's older rows. A bound value is a child, and an unbound
    /// value is nothing. The scan reads no revision row and no inode row.
    pub(crate) async fn visible_children_page_by_name_key(
        &mut self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&NameKey>,
        limit: usize,
    ) -> Result<Vec<VisibleChildEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Directory {
            return Ok(Vec::new());
        }

        let lower_bound = match start_after_name_key {
            Some(name_key) => string_prefix_upper_bound(&lookup_keys::direntry_bind_prefix(
                parent_inode_id,
                name_key,
            ))
            .expect("a slot prefix should end in a hyphen"),
            None => lookup_keys::direntry_parent_prefix(parent_inode_id),
        };
        let mut merge = ManifestTailMerge::new(
            lower_bound.clone(),
            self.base.tail_direntry_binds(parent_inode_id, &lower_bound),
            TieOrder::ManifestFirst,
        );
        let mut children = Vec::with_capacity(limit);
        while children.len() < limit {
            let bound = self
                .next_bound_edges(&mut merge, parent_inode_id, limit - children.len())
                .await?;
            if bound.is_empty() {
                break;
            }
            self.preload_edge_visibility(parent_inode_id, &bound)
                .await?;
            for binding in bound {
                if let Some(active) = visibility::visible_page_child(self, binding).await? {
                    children.push(VisibleChildEntry { binding: active });
                }
            }
        }
        Ok(children)
    }

    /// Reads the merged rows until it has the values of up to `edge_limit`
    /// bound edges, or the parent's rows run out.
    async fn next_bound_edges(
        &mut self,
        merge: &mut ManifestTailMerge<DirentryBindingRecord>,
        parent_inode_id: InodeId,
        edge_limit: usize,
    ) -> Result<Vec<DirentryBindingRecord>> {
        let visible_seq = self.base.visible_seq();
        let segments = self.base.manifest_segments();
        let counters = &mut self.counters;
        // An empty page is the end of the range.
        let mut refill = |lower_bound: String| {
            counters.scan_range_page_calls = counters.scan_range_page_calls.saturating_add(1);
            async move {
                let rows = manifest_index::direntry_binds_page(
                    segments,
                    parent_inode_id,
                    &lower_bound,
                    edge_limit.max(DIRECTORY_PAGE_RAW_SCAN_LIMIT),
                )
                .await?;
                Ok(ManifestPage {
                    more: !rows.is_empty(),
                    rows,
                })
            }
        };
        let mut bound = Vec::new();
        while bound.len() < edge_limit {
            let Some((_, record)) = merge.next(&mut refill).await? else {
                break;
            };
            if record.committed_seq > visible_seq {
                continue;
            }
            merge.seek(&manifest_index::slot_edge_end(&record));
            if record.is_bound() {
                bound.push(record);
            }
        }
        Ok(bound)
    }

    /// Seeds the session's caches with what the bound edges show, and reads
    /// the hiding tombstones of their children, which decide whether each
    /// child is visible.
    async fn preload_edge_visibility(
        &mut self,
        parent_inode_id: InodeId,
        bound: &[DirentryBindingRecord],
    ) -> Result<()> {
        let mut child_inode_ids = Vec::with_capacity(bound.len());
        for binding in bound {
            child_inode_ids.push(binding.child_inode_id);
            // A slot binds one child at a time, so a bound edge's value is
            // the slot's value, and the child's binding.
            self.latest_parent_binding_cache
                .insert(binding.child_inode_id, Some(binding.clone()));
            self.bound_child_cache.insert(
                ParentNameCacheKey {
                    parent_inode_id,
                    name_key: binding.name_key.clone(),
                },
                Some(binding.clone()),
            );
        }
        child_inode_ids.sort_unstable();
        child_inode_ids.dedup();
        let base = &self.base;
        let pending: Vec<_> = child_inode_ids
            .into_iter()
            .filter(|inode_id| !self.hiding_tombstone_cache.contains_key(inode_id))
            .collect();
        let loaded =
            preload_inode_groups(&pending, |inode_id| base.hiding_subtree_tombstone(inode_id))
                .await?;
        self.counters.list_preload_child_lookups = self
            .counters
            .list_preload_child_lookups
            .saturating_add(loaded.len() as u64);
        self.hiding_tombstone_cache.extend(loaded);
        Ok(())
    }
}
