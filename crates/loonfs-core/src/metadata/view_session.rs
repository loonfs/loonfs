//! A [`MetadataViewSession`] caches repeated lookups for one read operation
//! over a [`MetadataView`] and merges manifest and WAL directory page rows.
//! Every visibility decision routes through [`super::visibility`]; the session
//! memoizes the inputs and results of those rules.

use super::durable_cache::ParentNameCacheKey;
use super::manifest_index;
use super::visibility::{self, MetadataVisibilityReads};
use super::{
    AccessRevisionRecord, ActiveDeletionRange, DirentryBindingRecord, InodeRecord, MetadataView,
    RecoverableDeletion, ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::error::{CoreError, Result};
use crate::metadata::children_page::VisibleChildEntry;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AbsolutePath, AttributesProjection, ChangeSeq, InodeId, NameKey, ROOT_INODE_ID,
};
use std::collections::HashMap;
use std::future::Future;

/// Groups a view session splits a page's inode lookups into. The groups run
/// together and each looks up its inodes in order, so at most 16 lookups are
/// in flight.
const VIEW_LOOKUP_GROUPS: usize = 16;

pub(crate) async fn preload_inode_groups<T, Lookup, LookupFuture>(
    inode_ids: &[InodeId],
    lookup: Lookup,
) -> Result<Vec<(InodeId, T)>>
where
    Lookup: Fn(InodeId) -> LookupFuture,
    LookupFuture: Future<Output = Result<T>>,
{
    // Adjacent inode ids often share a read. Separate ranges let each
    // group start in a different part of the page's segments.
    let group_size = inode_ids.len().div_ceil(VIEW_LOOKUP_GROUPS).max(1);
    let lookup = &lookup;
    let loaded =
        futures::future::try_join_all(inode_ids.chunks(group_size).map(|group| async move {
            let mut rows = Vec::with_capacity(group.len());
            for &inode_id in group {
                rows.push((inode_id, lookup(inode_id).await?));
            }
            Ok::<_, CoreError>(rows)
        }))
        .await?;
    Ok(loaded.into_iter().flatten().collect())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeafRevisionPreload {
    Preload,
    Skip,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetadataViewSessionCounters {
    pub(crate) visible_inode_calls: u64,
    pub(crate) current_parent_binding_calls: u64,
    pub(crate) covering_tombstone_calls: u64,
    pub(crate) latest_revision_calls: u64,
    pub(crate) latest_attributes_calls: u64,
    pub(crate) access_row_calls: u64,
    pub(crate) preload_attribute_lookups: u64,
    pub(crate) direntry_child_scan_calls: u64,
    pub(crate) scan_prefix_calls: u64,
    pub(crate) scan_range_page_calls: u64,
    pub(crate) list_preload_child_lookups: u64,
}

pub(crate) type MetadataViewSessionCounterField =
    (&'static str, fn(&MetadataViewSessionCounters) -> u64);

pub(crate) const METADATA_VIEW_SESSION_COUNTER_FIELDS: [MetadataViewSessionCounterField; 11] = [
    ("list_page_visible_inode_calls", |counters| {
        counters.visible_inode_calls
    }),
    ("list_page_current_parent_binding_calls", |counters| {
        counters.current_parent_binding_calls
    }),
    ("list_page_covering_tombstone_calls", |counters| {
        counters.covering_tombstone_calls
    }),
    ("list_page_latest_revision_calls", |counters| {
        counters.latest_revision_calls
    }),
    ("list_page_latest_attributes_calls", |counters| {
        counters.latest_attributes_calls
    }),
    ("list_page_direntry_child_scan_calls", |counters| {
        counters.direntry_child_scan_calls
    }),
    ("list_page_scan_prefix_calls", |counters| {
        counters.scan_prefix_calls
    }),
    ("list_page_scan_range_page_calls", |counters| {
        counters.scan_range_page_calls
    }),
    ("list_page_preload_child_lookups", |counters| {
        counters.list_preload_child_lookups
    }),
    ("list_page_preload_attribute_lookups", |counters| {
        counters.preload_attribute_lookups
    }),
    ("access_row_calls", |counters| counters.access_row_calls),
];

impl MetadataViewSessionCounters {
    pub(crate) fn record_on(self, span: &tracing::Span) {
        for (field, value) in METADATA_VIEW_SESSION_COUNTER_FIELDS {
            span.record(field, value(&self));
        }
    }
}

/// One loaded-view session with memoized visibility reads.
pub(crate) struct MetadataViewSession<'a, 'store, S: ObjectStore + ?Sized> {
    pub(crate) base: MetadataView<'a, 'store, S>,
    // In each cache below whose value is an `Option`, `None` caches an
    // absence: the lookup ran and found nothing, so it isn't read again.
    inode_at_seq_cache: HashMap<InodeId, Option<InodeRecord>>,
    visible_inode_cache: HashMap<InodeId, Option<InodeRecord>>,
    pub(super) bound_child_cache: HashMap<ParentNameCacheKey, Option<DirentryBindingRecord>>,
    pub(crate) latest_parent_binding_cache: HashMap<InodeId, Option<DirentryBindingRecord>>,
    latest_revision_head_cache: HashMap<InodeId, Option<RevisionRecord>>,
    attributes_cache: HashMap<InodeId, AttributesProjection>,
    access_row_cache: HashMap<InodeId, Option<AccessRevisionRecord>>,
    pub(crate) hiding_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    covering_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    pub(crate) counters: MetadataViewSessionCounters,
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataViewSession<'a, 'store, S> {
    pub(super) fn new(base: MetadataView<'a, 'store, S>) -> Self {
        Self {
            base,
            inode_at_seq_cache: HashMap::new(),
            visible_inode_cache: HashMap::new(),
            bound_child_cache: HashMap::new(),
            latest_parent_binding_cache: HashMap::new(),
            latest_revision_head_cache: HashMap::new(),
            attributes_cache: HashMap::new(),
            access_row_cache: HashMap::new(),
            hiding_tombstone_cache: HashMap::new(),
            covering_tombstone_cache: HashMap::new(),
            counters: MetadataViewSessionCounters::default(),
        }
    }

    pub(crate) fn counters(&self) -> MetadataViewSessionCounters {
        self.counters
    }

    fn cached_inode_at_seq(&self, inode_id: InodeId) -> Option<Option<InodeRecord>> {
        self.inode_at_seq_cache.get(&inode_id).cloned()
    }

    fn cached_hiding_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Option<Option<SubtreeTombstoneRecord>> {
        self.hiding_tombstone_cache.get(&root_inode_id).cloned()
    }

    fn cached_latest_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Option<Option<DirentryBindingRecord>> {
        self.latest_parent_binding_cache
            .get(&child_inode_id)
            .cloned()
    }

    fn cached_bound_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Option<Option<DirentryBindingRecord>> {
        self.bound_child_cache
            .get(&ParentNameCacheKey {
                parent_inode_id,
                name_key: name_key.clone(),
            })
            .cloned()
    }

    fn cached_latest_revision_head_of_visible(
        &self,
        inode_id: InodeId,
    ) -> Option<Option<RevisionRecord>> {
        self.latest_revision_head_cache.get(&inode_id).cloned()
    }

    pub(crate) async fn preload_visibility(&mut self, inode_ids: &[InodeId]) -> Result<()> {
        let mut pending: Vec<InodeId> = inode_ids
            .iter()
            .copied()
            .filter(|inode_id| {
                !(self.inode_at_seq_cache.contains_key(inode_id)
                    && self.latest_parent_binding_cache.contains_key(inode_id)
                    && self.hiding_tombstone_cache.contains_key(inode_id))
            })
            .collect();
        pending.sort_unstable();
        pending.dedup();
        if pending.is_empty() {
            return Ok(());
        }
        self.counters.list_preload_child_lookups = self
            .counters
            .list_preload_child_lookups
            .saturating_add(pending.len() as u64);

        let visible_seq = self.base.visible_seq();
        let base = &self.base;
        let parent_bindings = &self.latest_parent_binding_cache;
        let lookups = preload_inode_groups(&pending, |inode_id| async move {
            futures::try_join!(
                base.inode_at_seq(inode_id),
                async {
                    if let Some(binding) = parent_bindings.get(&inode_id) {
                        Ok(binding.clone())
                    } else {
                        base.latest_parent_binding_for_child(inode_id).await
                    }
                },
                base.tombstones_for_root(inode_id),
            )
        })
        .await?;

        for (inode_id, (inode, binding, tombstones)) in lookups {
            self.inode_at_seq_cache.insert(inode_id, inode);
            self.latest_parent_binding_cache.insert(inode_id, binding);
            let hiding_tombstone =
                super::rows::hiding_tombstone_from_records(tombstones.iter().cloned(), visible_seq);
            self.hiding_tombstone_cache
                .insert(inode_id, hiding_tombstone);
        }
        Ok(())
    }

    pub(crate) async fn resolve_visible_path(
        &mut self,
        absolute_path: &AbsolutePath,
        preload_leaf_revision: LeafRevisionPreload,
    ) -> Result<ResolvedVisiblePath> {
        self.preload_path_walk(absolute_path, preload_leaf_revision)
            .await?;
        visibility::resolve_visible_path(self, absolute_path).await
    }

    async fn preload_path_walk(
        &mut self,
        absolute_path: &AbsolutePath,
        preload_leaf_revision: LeafRevisionPreload,
    ) -> Result<()> {
        let component_name_keys: Vec<NameKey> = absolute_path
            .components()
            .iter()
            .map(|component| NameKey::for_display_name(&component.to_display_name()))
            .collect();

        let is_root_path = absolute_path.is_root();
        let mut current_inode_id = ROOT_INODE_ID;
        let mut pending_binding: Option<DirentryBindingRecord> = None;
        for wave in 0..=component_name_keys.len() {
            let lookup_name_key = component_name_keys.get(wave);
            let is_leaf_wave = wave == component_name_keys.len();

            if let Some(binding) = pending_binding.take() {
                self.latest_parent_binding_cache
                    .insert(current_inode_id, Some(binding));
            }
            let preload_revision =
                is_leaf_wave && preload_leaf_revision == LeafRevisionPreload::Preload;

            let base = &self.base;
            let (inode, tombstone, parent_binding, bound_child, revision) = futures::try_join!(
                async {
                    if !is_root_path {
                        return Ok(None);
                    }
                    match self.cached_inode_at_seq(current_inode_id) {
                        Some(inode) => Ok(inode),
                        None => base.inode_at_seq(current_inode_id).await,
                    }
                },
                async {
                    match self.cached_hiding_subtree_tombstone(current_inode_id) {
                        Some(tombstone) => Ok(tombstone),
                        None => base.hiding_subtree_tombstone(current_inode_id).await,
                    }
                },
                async {
                    match self.cached_latest_parent_binding_for_child(current_inode_id) {
                        Some(binding) => Ok(binding),
                        None => base.latest_parent_binding_for_child(current_inode_id).await,
                    }
                },
                async {
                    let Some(name_key) = lookup_name_key else {
                        return Ok(None);
                    };
                    let cache_key = ParentNameCacheKey {
                        parent_inode_id: current_inode_id,
                        name_key: name_key.clone(),
                    };
                    let binding = match self.cached_bound_child(current_inode_id, name_key) {
                        Some(binding) => binding,
                        None => base.bound_child(current_inode_id, name_key).await?,
                    };
                    Ok(Some((cache_key, binding)))
                },
                async {
                    if !preload_revision {
                        return Ok(None);
                    }
                    match self.cached_latest_revision_head_of_visible(current_inode_id) {
                        Some(revision) => Ok(revision),
                        None => base.latest_revision_record(current_inode_id).await,
                    }
                },
            )?;

            if is_root_path {
                self.inode_at_seq_cache.insert(current_inode_id, inode);
            }
            self.hiding_tombstone_cache
                .insert(current_inode_id, tombstone);
            self.latest_parent_binding_cache
                .insert(current_inode_id, parent_binding);
            if preload_revision {
                self.latest_revision_head_cache
                    .insert(current_inode_id, revision);
            }
            let Some((cache_key, bound)) = bound_child else {
                break;
            };
            self.bound_child_cache.insert(cache_key, bound.clone());
            let Some(binding) = bound.filter(DirentryBindingRecord::is_bound) else {
                break;
            };
            current_inode_id = binding.child_inode_id;
            pending_binding = Some(binding);
        }
        Ok(())
    }

    /// Returns the inode when it is visible in this session's snapshot.
    pub(crate) async fn visible_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>> {
        self.counters.visible_inode_calls = self.counters.visible_inode_calls.saturating_add(1);
        if let Some(cached) = self.visible_inode_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }

        let visible = visibility::visible_inode(self, inode_id).await?;
        self.visible_inode_cache.insert(inode_id, visible.clone());
        Ok(visible)
    }

    pub(crate) async fn inode_at_seq(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>> {
        if let Some(cached) = self.cached_inode_at_seq(inode_id) {
            return Ok(cached);
        }
        let inode = self.base.inode_at_seq(inode_id).await?;
        self.inode_at_seq_cache.insert(inode_id, inode.clone());
        Ok(inode)
    }

    /// Returns the latest revision for an inode already proven visible at
    /// this session's seq, for a stat. Unlike
    /// [`MetadataView::latest_revision_head`], this skips re-deriving the
    /// inode's visibility, because it can't disagree with the resolution
    /// that admitted the inode at the same seq. A listing reads no revision:
    /// it reports the revision summary in each file's binding row.
    pub(crate) async fn latest_revision_head_of_visible(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>> {
        self.counters.latest_revision_calls = self.counters.latest_revision_calls.saturating_add(1);
        if let Some(cached) = self.cached_latest_revision_head_of_visible(inode_id) {
            return Ok(cached);
        }
        let revision = self.base.latest_revision_record(inode_id).await?;
        self.latest_revision_head_cache
            .insert(inode_id, revision.clone());
        Ok(revision)
    }

    pub(crate) async fn preload_access_rows(
        &mut self,
        children: &[VisibleChildEntry],
    ) -> Result<()> {
        let mut pending: Vec<_> = children
            .iter()
            .map(|child| child.binding.child_inode_id)
            .filter(|inode_id| !self.access_row_cache.contains_key(inode_id))
            .collect();
        pending.sort_unstable();
        pending.dedup();
        let base = &self.base;
        let loaded =
            preload_inode_groups(&pending, |inode_id| base.latest_access_revision(inode_id))
                .await?;
        self.access_row_cache.extend(loaded);
        Ok(())
    }

    pub(crate) async fn access_row(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<AccessRevisionRecord>> {
        self.counters.access_row_calls = self.counters.access_row_calls.saturating_add(1);
        if let Some(cached) = self.access_row_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let access = self.base.latest_access_revision(inode_id).await?;
        self.access_row_cache.insert(inode_id, access.clone());
        Ok(access)
    }

    /// Returns the attribute map and revision of an inode already proven
    /// visible at this session's seq.
    ///
    /// This is the attribute half of
    /// [`Self::latest_revision_head_of_visible`], and it skips re-deriving
    /// visibility for the same reason: the enumeration that admitted the
    /// entry already checked its visibility at this seq. An inode with no attribute record is
    /// at revision 0 with an empty map, which is a result rather than a
    /// miss, so the cache keeps it like any other.
    pub(crate) async fn attributes_of_visible(
        &mut self,
        inode_id: InodeId,
    ) -> Result<AttributesProjection> {
        self.counters.latest_attributes_calls =
            self.counters.latest_attributes_calls.saturating_add(1);
        if let Some(cached) = self.attributes_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let attributes = self.base.attributes(inode_id).await?;
        self.attributes_cache.insert(inode_id, attributes.clone());
        Ok(attributes)
    }

    /// Reads a whole page's attribute maps as one concurrent wave and seeds
    /// the cache the per-entry build reads through.
    ///
    /// Without this the build loop awaits one probe per entry in turn, which
    /// is a thousand serialized round trips on a full page. The preload
    /// determines nothing: seeded values are what the per-entry lookup would
    /// have fetched, so an entry built with or without it is identical.
    pub(crate) async fn preload_attributes(&mut self, inode_ids: &[InodeId]) -> Result<()> {
        let mut pending: Vec<InodeId> = inode_ids
            .iter()
            .copied()
            .filter(|inode_id| !self.attributes_cache.contains_key(inode_id))
            .collect();
        pending.sort_unstable();
        pending.dedup();
        if pending.is_empty() {
            return Ok(());
        }
        self.counters.preload_attribute_lookups = self
            .counters
            .preload_attribute_lookups
            .saturating_add(pending.len() as u64);

        let base = &self.base;
        let loaded = preload_inode_groups(&pending, |inode_id| base.attributes(inode_id)).await?;
        self.attributes_cache.extend(loaded);
        Ok(())
    }

    pub(crate) async fn revisions_by_inode_page_desc(
        &mut self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>> {
        self.counters.scan_range_page_calls = self.counters.scan_range_page_calls.saturating_add(1);
        self.base
            .revisions_by_inode_page_desc(inode_id, start_after, limit)
            .await
    }

    /// Returns the child's active parent binding at the visible sequence.
    pub(crate) async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>> {
        self.counters.current_parent_binding_calls =
            self.counters.current_parent_binding_calls.saturating_add(1);
        visibility::current_parent_binding_for_child(self, child_inode_id).await
    }

    async fn latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>> {
        if let Some(cached) = self.cached_latest_parent_binding_for_child(child_inode_id) {
            return Ok(cached);
        }
        self.counters.direntry_child_scan_calls =
            self.counters.direntry_child_scan_calls.saturating_add(1);
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let latest = self
            .base
            .latest_parent_binding_for_child(child_inode_id)
            .await?;
        self.latest_parent_binding_cache
            .insert(child_inode_id, latest.clone());
        Ok(latest)
    }

    pub(crate) async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        self.counters.covering_tombstone_calls =
            self.counters.covering_tombstone_calls.saturating_add(1);
        if let Some(cached) = self.covering_tombstone_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }

        let tombstone = visibility::covering_subtree_tombstone(self, inode_id).await?;
        self.covering_tombstone_cache
            .insert(inode_id, tombstone.clone());
        Ok(tombstone)
    }

    /// One page of the namespace's deletions in `range` that no undelete has
    /// cancelled, oldest deletion first. Uncached: one call serves one trash
    /// listing page.
    pub(crate) async fn active_deletions_page(
        &mut self,
        range: &ActiveDeletionRange,
        limit: usize,
    ) -> Result<Vec<RecoverableDeletion>> {
        self.counters.scan_range_page_calls = self.counters.scan_range_page_calls.saturating_add(1);
        self.base.active_deletions_page(range, limit).await
    }

    pub(crate) async fn hiding_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        if let Some(cached) = self.cached_hiding_subtree_tombstone(root_inode_id) {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let tombstone = self.base.hiding_subtree_tombstone(root_inode_id).await?;
        self.hiding_tombstone_cache
            .insert(root_inode_id, tombstone.clone());
        Ok(tombstone)
    }

    pub(crate) async fn bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.clone(),
        };
        if let Some(cached) = self.cached_bound_child(parent_inode_id, name_key) {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let binding = self.base.bound_child(parent_inode_id, name_key).await?;
        self.bound_child_cache.insert(cache_key, binding.clone());
        Ok(binding)
    }
}

/// The session as a [`MetadataVisibilityReads`] source. Its primitives route
/// through the per-session caches, and it overrides exactly the composite
/// rules it memoizes (`visible_inode`, `current_parent_binding_for_child`,
/// `covering_subtree_tombstone`) so a cache hit short-circuits the walk;
/// every override's miss path, and the un-overridden composites, still determine
/// through the canonical [`super::visibility`] rule bodies.
impl<S: ObjectStore + ?Sized> MetadataVisibilityReads for MetadataViewSession<'_, '_, S> {
    type Error = CoreError;

    fn retention_floor_seq(&self) -> ChangeSeq {
        self.base.retention_floor_seq()
    }

    async fn find_inode(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<InodeRecord>, Self::Error> {
        self.inode_at_seq(inode_id).await
    }

    async fn find_latest_slot_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> std::result::Result<Option<DirentryBindingRecord>, Self::Error> {
        self.bound_child(parent_inode_id, name_key).await
    }

    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> std::result::Result<Option<DirentryBindingRecord>, Self::Error> {
        self.latest_parent_binding_for_child(child_inode_id).await
    }

    async fn find_hiding_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> std::result::Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.hiding_subtree_tombstone(root_inode_id).await
    }

    async fn find_access_row(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<AccessRevisionRecord>, Self::Error> {
        self.access_row(inode_id).await
    }

    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> std::result::Result<Option<DirentryBindingRecord>, Self::Error> {
        MetadataViewSession::current_parent_binding_for_child(self, child_inode_id).await
    }

    async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        MetadataViewSession::covering_subtree_tombstone(self, inode_id).await
    }

    async fn visible_inode(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<InodeRecord>, Self::Error> {
        MetadataViewSession::visible_inode(self, inode_id).await
    }
}
