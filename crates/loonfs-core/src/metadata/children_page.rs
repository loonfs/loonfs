//! One page of a directory's visible children in a
//! [`MetadataViewSession`]: the
//! manifest and WAL-tail binding rows merged a group of name keys at a time,
//! with the visibility of each group's children preloaded.

use super::durable_cache::ParentNameCacheKey;
use super::manifest_index;
use super::manifest_tail_merge::{ManifestPage, ManifestTailMerge, TieOrder};
use super::view::latest_visible_binding;
use super::visibility;
use super::DirentryBindingRecord;
use crate::error::Result;
use crate::limits::DIRECTORY_PAGE_RAW_SCAN_LIMIT;
use crate::metadata::view_session::preload_inode_groups;
use crate::metadata::view_session::MetadataViewSession;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{InodeId, InodeKind, NameKey};

#[derive(Debug, Clone)]
pub(crate) struct VisibleChildEntry {
    pub(crate) binding: DirentryBindingRecord,
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataViewSession<'a, 'store, S> {
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

        let mut stream = DirentryBindingNameGroupStream {
            merge: ManifestTailMerge::new(
                self.base
                    .tail_direntry_bind_page_candidates(parent_inode_id, start_after_name_key),
                TieOrder::ManifestFirst,
            ),
            pushed_back: None,
        };
        let mut children = Vec::with_capacity(limit);
        'pages: while children.len() < limit {
            let groups = self
                .next_candidate_name_groups(
                    &mut stream,
                    parent_inode_id,
                    start_after_name_key,
                    // Raw scans keep their block-oriented readahead, but
                    // visibility preloading needs only one page of names.
                    limit,
                )
                .await?;
            if groups.is_empty() {
                break;
            }
            self.preload_group_visibility(parent_inode_id, &groups)
                .await?;
            for group in &groups {
                let Some(binding) = self.bound_child(parent_inode_id, &group.name_key).await?
                else {
                    continue;
                };
                let Some(active) = visibility::visible_page_child(self, binding).await? else {
                    continue;
                };
                children.push(VisibleChildEntry { binding: active });
                if children.len() == limit {
                    break 'pages;
                }
            }
        }
        Ok(children)
    }

    /// Pulls up to `group_limit` complete name-key groups from the merged
    /// manifest+tail candidate stream, in row-key order. A group contains
    /// every bind row for its name, so the group's newest visible row IS the
    /// latest bound child for that name.
    async fn next_candidate_name_groups(
        &mut self,
        stream: &mut DirentryBindingNameGroupStream,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&NameKey>,
        group_limit: usize,
    ) -> Result<Vec<DirentryBindingNameGroup>> {
        let mut groups: Vec<DirentryBindingNameGroup> = Vec::new();
        let segments = self.base.manifest_segments();
        let counters = &mut self.counters;
        // An empty page is the end of the range.
        let mut refill = |after: Option<String>| {
            counters.scan_range_page_calls = counters.scan_range_page_calls.saturating_add(1);
            async move {
                let rows = manifest_index::direntry_binds_for_parent_name_key_page(
                    segments,
                    parent_inode_id,
                    start_after_name_key,
                    after.as_deref(),
                    group_limit.max(DIRECTORY_PAGE_RAW_SCAN_LIMIT),
                )
                .await?;
                Ok(ManifestPage {
                    more: !rows.is_empty(),
                    rows: rows
                        .into_iter()
                        .map(|candidate| (candidate.row_key, candidate.record))
                        .collect(),
                })
            }
        };
        loop {
            let record = match stream.pushed_back.take() {
                Some(record) => record,
                None => match stream.merge.next(&mut refill).await? {
                    Some((_, record)) => record,
                    None => break,
                },
            };
            match groups.last_mut() {
                Some(group) if group.name_key == record.name_key => {
                    group.rows.push(record);
                }
                _ => {
                    // A full batch closes only at a name boundary, so the
                    // finished group is guaranteed complete.
                    if groups.len() == group_limit {
                        stream.pushed_back = Some(record);
                        break;
                    }
                    groups.push(DirentryBindingNameGroup {
                        name_key: record.name_key.clone(),
                        rows: vec![record],
                    });
                }
            }
        }
        Ok(groups)
    }

    async fn preload_group_visibility(
        &mut self,
        parent_inode_id: InodeId,
        groups: &[DirentryBindingNameGroup],
    ) -> Result<()> {
        let visible_seq = self.base.visible_seq();
        let mut child_inode_ids = Vec::with_capacity(groups.len());
        for group in groups {
            let latest = latest_visible_binding(group.rows.iter(), visible_seq);
            if let Some(binding) = latest.as_ref().filter(|binding| binding.is_bound()) {
                child_inode_ids.push(binding.child_inode_id);
                self.latest_parent_binding_cache
                    .insert(binding.child_inode_id, Some(binding.clone()));
            }
            self.bound_child_cache.insert(
                ParentNameCacheKey {
                    parent_inode_id,
                    name_key: group.name_key.clone(),
                },
                latest,
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

/// The merged manifest and tail direntry-bind stream of one listing, read
/// a group of name keys at a time.
struct DirentryBindingNameGroupStream {
    merge: ManifestTailMerge<DirentryBindingRecord>,
    /// The first row of the next group, which the last read took from the
    /// merge to find the end of its last group.
    pushed_back: Option<DirentryBindingRecord>,
}

/// Every bind row the stream returned for one name key, in row-key order.
struct DirentryBindingNameGroup {
    name_key: NameKey,
    rows: Vec<DirentryBindingRecord>,
}
