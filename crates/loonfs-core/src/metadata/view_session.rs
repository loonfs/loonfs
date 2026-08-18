//! [`MetadataViewSession`]: one read operation's memo over a
//! [`MetadataView`](super::view::MetadataView).
//!
//! A session caches the primitive lookups a multi-step read repeats — path
//! walks, listing pages, grep candidate walks — and layers the directory
//! page stream that merges manifest candidates with the WAL tail. Every
//! visibility decision still routes through the canonical
//! [`super::visibility`] rule bodies; the session only memoizes their
//! primitive inputs.

use super::durable_cache::{BindingCacheKey, ParentNameCacheKey};
use super::manifest_index;
use super::view::DIRECTORY_PAGE_RAW_SCAN_LIMIT;
use super::visibility::{self, MetadataVisibilityReads};
use super::{
    DirentryBindRecord, InodeRecord, MetadataView, RecoverableDeletion, ResolvedVisiblePath,
    RevisionRecord, SubtreeTombstoneRecord,
};
use crate::error::CoreError;
use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
use loonfs_api::{
    AbsolutePath, AttributeRevisionNo, Attributes, ChangeSeq, InodeId, InodeKind, NameKey,
    ROOT_INODE_ID,
};
use loonfs_objectstore::ObjectStore;
use std::collections::{HashMap, HashSet, VecDeque};

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    /// Opens a fresh session over this view; the view is `Copy`, so the
    /// session owns its copy.
    pub(crate) fn session(self) -> MetadataViewSession<'a, 'store, S> {
        MetadataViewSession::new(self)
    }

    fn tail_direntry_bind_page_candidates(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
    ) -> Vec<DirentryBindPageCandidate> {
        let mut candidates = self
            .row_states()
            .flat_map(|state| state.direntry_binds())
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && start_after_name_key
                        .is_none_or(|last_name_key| direntry.name_key.as_str() > last_name_key)
            })
            .map(|record| DirentryBindPageCandidate {
                row_key: direntry_bind_row_key(record),
                record: record.clone(),
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.row_key.cmp(&right.row_key));
        candidates
    }
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleChildEntry {
    pub(crate) binding: DirentryBindRecord,
    pub(crate) inode: InodeRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeafRevisionPrefetch {
    Prefetch,
    Skip,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetadataViewSessionCounters {
    pub(crate) visible_child_calls: u64,
    pub(crate) visible_inode_calls: u64,
    pub(crate) current_parent_binding_calls: u64,
    pub(crate) covering_tombstone_calls: u64,
    pub(crate) latest_revision_calls: u64,
    pub(crate) latest_attributes_calls: u64,
    pub(crate) preload_attribute_lookups: u64,
    pub(crate) direntry_child_scan_calls: u64,
    pub(crate) scan_prefix_calls: u64,
    pub(crate) scan_range_page_calls: u64,
    pub(crate) list_preload_unbind_range_scans: u64,
    pub(crate) list_preload_child_lookups: u64,
}

pub(crate) const METADATA_VIEW_SESSION_COUNTER_FIELDS: [&str; 12] = [
    "list_page_visible_child_calls",
    "list_page_visible_inode_calls",
    "list_page_current_parent_binding_calls",
    "list_page_covering_tombstone_calls",
    "list_page_latest_revision_calls",
    "list_page_latest_attributes_calls",
    "list_page_direntry_child_scan_calls",
    "list_page_scan_prefix_calls",
    "list_page_scan_range_page_calls",
    "list_page_preload_unbind_range_scans",
    "list_page_preload_child_lookups",
    "list_page_preload_attribute_lookups",
];

impl MetadataViewSessionCounters {
    pub(crate) fn record_on(self, span: &tracing::Span) {
        let values = [
            self.visible_child_calls,
            self.visible_inode_calls,
            self.current_parent_binding_calls,
            self.covering_tombstone_calls,
            self.latest_revision_calls,
            self.latest_attributes_calls,
            self.direntry_child_scan_calls,
            self.scan_prefix_calls,
            self.scan_range_page_calls,
            self.list_preload_unbind_range_scans,
            self.list_preload_child_lookups,
            self.preload_attribute_lookups,
        ];
        for (field, value) in METADATA_VIEW_SESSION_COUNTER_FIELDS.iter().zip(values) {
            span.record(*field, value);
        }
    }
}

/// One loaded-view session with memoized visibility reads.
pub(crate) struct MetadataViewSession<'a, 'store, S: ObjectStore + ?Sized> {
    base: MetadataView<'a, 'store, S>,
    inode_at_seq_cache: HashMap<InodeId, Option<InodeRecord>>,
    visible_inode_cache: HashMap<InodeId, Option<InodeRecord>>,
    bound_child_cache: HashMap<ParentNameCacheKey, Option<DirentryBindRecord>>,
    current_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
    latest_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
    latest_revision_head_cache: HashMap<InodeId, Option<RevisionRecord>>,
    attributes_cache: HashMap<
        InodeId,
        (
            AttributeRevisionNo,
            Attributes,
            Option<loonfs_api::ActorRef>,
            Option<u64>,
        ),
    >,
    active_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    covering_tombstone_cache: HashMap<InodeId, Option<SubtreeTombstoneRecord>>,
    unbind_cache: HashMap<BindingCacheKey, bool>,
    counters: MetadataViewSessionCounters,
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataViewSession<'a, 'store, S> {
    fn new(base: MetadataView<'a, 'store, S>) -> Self {
        Self {
            base,
            inode_at_seq_cache: HashMap::new(),
            visible_inode_cache: HashMap::new(),
            bound_child_cache: HashMap::new(),
            current_parent_binding_cache: HashMap::new(),
            latest_parent_binding_cache: HashMap::new(),
            latest_revision_head_cache: HashMap::new(),
            attributes_cache: HashMap::new(),
            active_tombstone_cache: HashMap::new(),
            covering_tombstone_cache: HashMap::new(),
            unbind_cache: HashMap::new(),
            counters: MetadataViewSessionCounters::default(),
        }
    }

    pub(crate) fn counters(&self) -> MetadataViewSessionCounters {
        self.counters
    }

    pub(crate) async fn visible_children_page_by_name_key(
        &mut self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        limit: usize,
    ) -> Result<Vec<VisibleChildEntry>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Directory {
            return Ok(Vec::new());
        }

        let raw_scan_limit = limit.max(DIRECTORY_PAGE_RAW_SCAN_LIMIT);
        let mut stream = DirentryBindNameGroupStream::new(
            self.base
                .tail_direntry_bind_page_candidates(parent_inode_id, start_after_name_key),
            self.base.manifest_tables().is_none(),
        );
        let mut children = Vec::with_capacity(limit);
        'pages: while children.len() < limit {
            let groups = self
                .next_candidate_name_groups(
                    &mut stream,
                    parent_inode_id,
                    start_after_name_key,
                    raw_scan_limit,
                )
                .await?;
            if groups.is_empty() {
                break;
            }
            self.preload_group_visibility(parent_inode_id, &groups)
                .await?;
            for group in &groups {
                // One group per name key: the stream already deduplicated
                // and ordered candidates, and the preload seeded the caches
                // the canonical visibility rules read through.
                let Some(active) = self.visible_child(parent_inode_id, &group.name_key).await?
                else {
                    continue;
                };
                let Some(inode) = self.visible_inode(active.child_inode_id).await? else {
                    continue;
                };
                children.push(VisibleChildEntry {
                    binding: active,
                    inode,
                });
                if children.len() == limit {
                    break 'pages;
                }
            }
        }
        Ok(children)
    }

    /// Pulls up to `group_limit` complete name-key groups from the merged
    /// manifest+tail candidate stream, in row-key order. A group carries
    /// every bind row for its name, so the group's newest visible row IS the
    /// latest bound child for that name.
    async fn next_candidate_name_groups(
        &mut self,
        stream: &mut DirentryBindNameGroupStream,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&str>,
        group_limit: usize,
    ) -> Result<Vec<DirentryBindNameGroup>, CoreError> {
        let mut groups: Vec<DirentryBindNameGroup> = Vec::new();
        loop {
            if stream.manifest_candidates.is_empty() && !stream.manifest_exhausted {
                self.counters.scan_range_page_calls =
                    self.counters.scan_range_page_calls.saturating_add(1);
                let page = if let Some(tables) = self.base.manifest_tables() {
                    manifest_index::direntry_binds_for_parent_name_key_page(
                        tables,
                        parent_inode_id,
                        start_after_name_key,
                        stream.manifest_after_row_key.as_deref(),
                        group_limit.max(DIRECTORY_PAGE_RAW_SCAN_LIMIT),
                    )
                    .await?
                } else {
                    Vec::new()
                };
                if page.is_empty() {
                    stream.manifest_exhausted = true;
                } else {
                    stream.manifest_after_row_key =
                        page.last().map(|candidate| candidate.row_key.clone());
                    stream
                        .manifest_candidates
                        .extend(page.into_iter().map(|candidate| DirentryBindPageCandidate {
                            row_key: candidate.row_key,
                            record: candidate.record,
                        }));
                }
                continue;
            }

            let candidate = if let Some(pushed_back) = stream.pushed_back.take() {
                pushed_back
            } else {
                let next_manifest = stream.manifest_candidates.front();
                let next_tail = stream.tail_candidates.get(stream.tail_index);
                let take_tail = match (next_manifest, next_tail) {
                    (Some(manifest), Some(tail)) => tail.row_key < manifest.row_key,
                    (None, Some(_)) => true,
                    (Some(_), None) => false,
                    (None, None) => {
                        if stream.manifest_exhausted {
                            break;
                        }
                        continue;
                    }
                };
                if take_tail {
                    let candidate = stream.tail_candidates[stream.tail_index].clone();
                    stream.tail_index += 1;
                    candidate
                } else {
                    stream
                        .manifest_candidates
                        .pop_front()
                        .expect("manifest candidate should exist")
                }
            };

            match groups.last_mut() {
                Some(group) if group.name_key == candidate.record.name_key => {
                    group.rows.push(candidate.record);
                }
                _ => {
                    // A full batch closes only at a name boundary, so the
                    // finished group is guaranteed complete.
                    if groups.len() == group_limit {
                        stream.pushed_back = Some(candidate);
                        break;
                    }
                    groups.push(DirentryBindNameGroup {
                        name_key: candidate.record.name_key.clone(),
                        rows: vec![candidate.record],
                    });
                }
            }
        }
        Ok(groups)
    }

    /// Seeds the session caches for one wave of name groups: the latest bind
    /// per name from rows the stream already carried, unbind facts from one
    /// range scan, and the child-keyed lookups batched concurrently. The
    /// canonical visibility rules then decide over cache hits; anything the
    /// preload cannot answer (cross-directory binding chases) falls back to
    /// the ordinary per-key scans.
    async fn preload_group_visibility(
        &mut self,
        parent_inode_id: InodeId,
        groups: &[DirentryBindNameGroup],
    ) -> Result<(), CoreError> {
        let visible_seq = self.base.visible_seq();
        let mut latest_binds = Vec::with_capacity(groups.len());
        for group in groups {
            let latest = group
                .rows
                .iter()
                .filter(|direntry| direntry.bind_seq <= visible_seq)
                .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
                .cloned();
            self.bound_child_cache.insert(
                ParentNameCacheKey {
                    parent_inode_id,
                    name_key: group.name_key.clone(),
                },
                latest.clone(),
            );
            if let Some(latest) = latest {
                latest_binds.push(latest);
            }
        }
        let (Some(first_group), Some(last_group)) = (groups.first(), groups.last()) else {
            return Ok(());
        };

        self.counters.list_preload_unbind_range_scans = self
            .counters
            .list_preload_unbind_range_scans
            .saturating_add(1);
        let unbinds = self
            .base
            .direntry_unbinds_for_parent_name_range(
                parent_inode_id,
                &first_group.name_key,
                &last_group.name_key,
            )
            .await?;
        let unbound_identities: HashSet<BindingCacheKey> = unbinds
            .iter()
            .filter(|unbind| unbind.unbind_seq <= visible_seq)
            .map(BindingCacheKey::from)
            .collect();
        for direntry in &latest_binds {
            let cache_key = BindingCacheKey::from(direntry);
            let unbound = unbound_identities.contains(&cache_key);
            self.unbind_cache.insert(cache_key, unbound);
        }

        let mut pending_child_ids: Vec<InodeId> = latest_binds
            .iter()
            .map(|direntry| direntry.child_inode_id)
            .filter(|child_inode_id| {
                !(self.inode_at_seq_cache.contains_key(child_inode_id)
                    && self
                        .latest_parent_binding_cache
                        .contains_key(child_inode_id)
                    && self.active_tombstone_cache.contains_key(child_inode_id))
            })
            .collect();
        pending_child_ids.sort_unstable();
        pending_child_ids.dedup();
        if pending_child_ids.is_empty() {
            return Ok(());
        }
        self.counters.list_preload_child_lookups = self
            .counters
            .list_preload_child_lookups
            .saturating_add(pending_child_ids.len() as u64);

        let base = &self.base;
        let lookups = futures::future::try_join_all(pending_child_ids.iter().map(
            |&child_inode_id| async move {
                let (inode, bindings, tombstones) = futures::try_join!(
                    base.inode_at_seq(child_inode_id),
                    base.direntry_binds_for_child(child_inode_id),
                    base.tombstones_for_root(child_inode_id),
                )?;
                Ok::<_, CoreError>((child_inode_id, inode, bindings, tombstones))
            },
        ))
        .await?;

        for (child_inode_id, inode, bindings, tombstones) in lookups {
            self.inode_at_seq_cache.insert(child_inode_id, inode);
            let latest_binding = bindings
                .iter()
                .filter(|direntry| direntry.bind_seq <= visible_seq)
                .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
                .cloned();
            if let Some(latest_binding) = &latest_binding {
                // A child bound in this directory within the preloaded name
                // range gets its unbind fact from the range scan; bindings
                // elsewhere fall back to the ordinary per-binding scan.
                if latest_binding.parent_inode_id == parent_inode_id
                    && latest_binding.name_key.as_str() >= first_group.name_key.as_str()
                    && latest_binding.name_key.as_str() <= last_group.name_key.as_str()
                {
                    let cache_key = BindingCacheKey::from(latest_binding);
                    let unbound = unbound_identities.contains(&cache_key);
                    self.unbind_cache.entry(cache_key).or_insert(unbound);
                }
            }
            self.latest_parent_binding_cache
                .insert(child_inode_id, latest_binding);
            let active_tombstone =
                super::rows::active_tombstone_from_records(tombstones.iter().cloned(), visible_seq);
            self.active_tombstone_cache
                .insert(child_inode_id, active_tombstone);
        }
        Ok(())
    }

    /// Resolves `absolute_path` with the canonical visibility rules after a
    /// pipelined preload of the walk's storage lookups.
    ///
    /// For each path component, the preload issues the independent probes
    /// (inode, tombstone, child-keyed binding, the previous binding's
    /// unbind fact) as one concurrent wave alongside the next component's
    /// bound-child scan — the only lookup the walk truly serializes on —
    /// and seeds the session's caches with exactly what those lookups would
    /// have fetched. The canonical rules then decide over cache hits, so a
    /// cold resolution costs one round-trip wave per path component instead
    /// of five-plus sequential lookups each.
    pub(crate) async fn resolve_visible_path(
        &mut self,
        absolute_path: &AbsolutePath,
        prefetch_leaf_revision: LeafRevisionPrefetch,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        self.preload_path_walk(absolute_path, prefetch_leaf_revision)
            .await?;
        visibility::resolve_visible_path(self, absolute_path).await
    }

    /// The preload behind [`Self::resolve_visible_path`]: batches storage
    /// probes and seeds primitive caches, decides nothing. Seeded values are
    /// byte-identical to what the corresponding cache-miss paths compute, so
    /// visibility decisions are unchanged; anything the preload skips (a
    /// renamed child's foreign binding, an unbound name) falls back to the
    /// ordinary per-key scans. Stops probing once a component has no bound
    /// child or its binding is revoked — the canonical walk reports those.
    async fn preload_path_walk(
        &mut self,
        absolute_path: &AbsolutePath,
        prefetch_leaf_revision: LeafRevisionPrefetch,
    ) -> Result<(), CoreError> {
        let visible_seq = self.base.visible_seq();
        let component_name_keys: Vec<NameKey> = absolute_path
            .components()
            .iter()
            .map(|component| NameKey::for_display_name(&component.to_display_name()))
            .collect();

        let mut current_inode_id = ROOT_INODE_ID;
        let mut pending_binding: Option<DirentryBindRecord> = None;
        for wave in 0..=component_name_keys.len() {
            let lookup_name_key = component_name_keys.get(wave);
            let is_leaf_wave = wave == component_name_keys.len();

            let arrived_by_binding = pending_binding.take();
            let prefetch_revision =
                is_leaf_wave && prefetch_leaf_revision == LeafRevisionPrefetch::Prefetch;

            let base = &self.base;
            let (inode, tombstone, parent_binding, unbind, bound_child, revision) = futures::try_join!(
                async {
                    match self.inode_at_seq_cache.get(&current_inode_id).cloned() {
                        Some(inode) => Ok(inode),
                        None => base.inode_at_seq(current_inode_id).await,
                    }
                },
                async {
                    match self.active_tombstone_cache.get(&current_inode_id).cloned() {
                        Some(tombstone) => Ok(tombstone),
                        None => base
                            .tombstones_for_root(current_inode_id)
                            .await
                            .map(|rows| {
                                super::rows::active_tombstone_from_records(
                                    rows.iter().cloned(),
                                    visible_seq,
                                )
                            }),
                    }
                },
                async {
                    match self
                        .latest_parent_binding_cache
                        .get(&current_inode_id)
                        .cloned()
                    {
                        Some(binding) => Ok(binding),
                        None => base
                            .direntry_binds_for_child(current_inode_id)
                            .await
                            .map(|rows| {
                                rows.iter()
                                    .filter(|direntry| direntry.bind_seq <= visible_seq)
                                    .max_by_key(|direntry| {
                                        (direntry.bind_seq, direntry.bind_delta_index)
                                    })
                                    .cloned()
                            }),
                    }
                },
                async {
                    let Some(binding) = &arrived_by_binding else {
                        return Ok(None);
                    };
                    let cache_key = BindingCacheKey::from(binding);
                    let unbound = match self.unbind_cache.get(&cache_key).copied() {
                        Some(unbound) => unbound,
                        None => base
                            .direntry_unbinds_for_binding(binding)
                            .await?
                            .iter()
                            .any(|unbind| unbind.unbind_seq <= visible_seq),
                    };
                    Ok(Some((cache_key, unbound)))
                },
                async {
                    let Some(name_key) = lookup_name_key else {
                        return Ok(None);
                    };
                    let cache_key = ParentNameCacheKey {
                        parent_inode_id: current_inode_id,
                        name_key: name_key.clone(),
                    };
                    let binding = match self.bound_child_cache.get(&cache_key).cloned() {
                        Some(binding) => binding,
                        None => base
                            .direntry_binds_for_parent_name(current_inode_id, name_key)
                            .await?
                            .iter()
                            .filter(|direntry| direntry.bind_seq <= visible_seq)
                            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
                            .cloned(),
                    };
                    Ok(Some((cache_key, binding)))
                },
                async {
                    if !prefetch_revision {
                        return Ok(None);
                    }
                    match self
                        .latest_revision_head_cache
                        .get(&current_inode_id)
                        .cloned()
                    {
                        Some(revision) => Ok(revision),
                        None => base.latest_revision_record(current_inode_id).await,
                    }
                },
            )?;

            self.inode_at_seq_cache.insert(current_inode_id, inode);
            self.active_tombstone_cache
                .insert(current_inode_id, tombstone);
            self.latest_parent_binding_cache
                .insert(current_inode_id, parent_binding);
            if prefetch_revision {
                self.latest_revision_head_cache
                    .insert(current_inode_id, revision);
            }
            if let Some((cache_key, unbound)) = unbind {
                self.unbind_cache.insert(cache_key, unbound);
                if unbound {
                    // The walk dead-ends at the previous component; probing
                    // deeper would warm caches for nothing.
                    break;
                }
            }

            let Some((cache_key, bound)) = bound_child else {
                break;
            };
            self.bound_child_cache.insert(cache_key, bound.clone());
            let Some(binding) = bound else {
                break;
            };
            current_inode_id = binding.child_inode_id;
            pending_binding = Some(binding);
        }
        Ok(())
    }

    pub(crate) async fn visible_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.counters.visible_child_calls = self.counters.visible_child_calls.saturating_add(1);
        visibility::visible_child(self, parent_inode_id, name_key).await
    }

    /// Returns the inode when it is visible in this session's snapshot.
    ///
    pub(crate) async fn visible_inode(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        self.counters.visible_inode_calls = self.counters.visible_inode_calls.saturating_add(1);
        if let Some(cached) = self.visible_inode_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }

        let visible = visibility::visible_inode(self, inode_id).await?;
        self.visible_inode_cache.insert(inode_id, visible.clone());
        Ok(visible)
    }

    pub(crate) async fn inode_at_seq(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        if let Some(cached) = self.inode_at_seq_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let inode = self.base.inode_at_seq(inode_id).await?;
        self.inode_at_seq_cache.insert(inode_id, inode.clone());
        Ok(inode)
    }

    /// Returns the latest revision for an inode already proven visible at
    /// this session's seq. Unlike [`MetadataView::latest_revision_head`],
    /// this skips re-deriving the inode's visibility: the re-check costs a
    /// full child-binds scan and tombstone probe per entry on a wide listing,
    /// and it cannot disagree with the enumeration that admitted the entry
    /// at the same seq.
    ///
    pub(crate) async fn latest_revision_head_of_visible(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        self.counters.latest_revision_calls = self.counters.latest_revision_calls.saturating_add(1);
        if let Some(cached) = self.latest_revision_head_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let revision = self.base.latest_revision_record(inode_id).await?;
        self.latest_revision_head_cache
            .insert(inode_id, revision.clone());
        Ok(revision)
    }

    /// Returns the attribute map and revision of an inode already proven
    /// visible at this session's seq.
    ///
    /// This is the attribute half of
    /// [`Self::latest_revision_head_of_visible`], and it skips re-deriving
    /// visibility for the same reason: the enumeration that admitted the
    /// entry decided that at this seq. An inode with no attribute record is
    /// at revision 0 with an empty map, which is an answer rather than a
    /// miss, so the cache holds it like any other.
    pub(crate) async fn attributes_of_visible(
        &mut self,
        inode_id: InodeId,
    ) -> Result<
        (
            AttributeRevisionNo,
            Attributes,
            Option<loonfs_api::ActorRef>,
            Option<u64>,
        ),
        CoreError,
    > {
        self.counters.latest_attributes_calls =
            self.counters.latest_attributes_calls.saturating_add(1);
        if let Some(cached) = self.attributes_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let attributes = self
            .base
            .attributes_projection_at_visible_seq(inode_id)
            .await?;
        self.attributes_cache.insert(inode_id, attributes.clone());
        Ok(attributes)
    }

    /// Reads a whole page's attribute maps as one concurrent wave and seeds
    /// the cache the per-entry build reads through.
    ///
    /// Without this the build loop awaits one probe per entry in turn, which
    /// is a thousand serialized round trips on a full page. The preload
    /// decides nothing: seeded values are what the per-entry lookup would
    /// have fetched, so an entry built with or without it is identical.
    pub(crate) async fn preload_attributes(
        &mut self,
        inode_ids: &[InodeId],
    ) -> Result<(), CoreError> {
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
        let loaded =
            futures::future::try_join_all(pending.into_iter().map(|inode_id| async move {
                Ok::<_, CoreError>((
                    inode_id,
                    base.attributes_projection_at_visible_seq(inode_id).await?,
                ))
            }))
            .await?;
        self.attributes_cache.extend(loaded);
        Ok(())
    }

    pub(crate) async fn revisions_for_inode_page_desc(
        &mut self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        self.counters.scan_range_page_calls = self.counters.scan_range_page_calls.saturating_add(1);
        self.base
            .revisions_for_inode_page_desc(inode_id, start_after, limit)
            .await
    }

    /// Returns the child's active parent binding at the visible sequence.
    ///
    pub(crate) async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.counters.current_parent_binding_calls =
            self.counters.current_parent_binding_calls.saturating_add(1);
        if let Some(cached) = self
            .current_parent_binding_cache
            .get(&child_inode_id)
            .cloned()
        {
            return Ok(cached);
        }

        let binding = visibility::current_parent_binding_for_child(self, child_inode_id).await?;
        self.current_parent_binding_cache
            .insert(child_inode_id, binding.clone());
        Ok(binding)
    }

    /// Latest binding whose child is `child_inode_id` at the visible seq,
    /// regardless of whether it has since been unbound. The
    /// [`MetadataVisibilityReads`] primitive backing the session's cached
    /// [`Self::current_parent_binding_for_child`].
    async fn latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        if let Some(cached) = self
            .latest_parent_binding_cache
            .get(&child_inode_id)
            .cloned()
        {
            return Ok(cached);
        }
        self.counters.direntry_child_scan_calls =
            self.counters.direntry_child_scan_calls.saturating_add(1);
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let bindings = self.base.direntry_binds_for_child(child_inode_id).await?;
        let latest = bindings
            .iter()
            .filter(|direntry| direntry.bind_seq <= self.base.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned();
        self.latest_parent_binding_cache
            .insert(child_inode_id, latest.clone());
        Ok(latest)
    }

    pub(crate) async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
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

    /// One page of the namespace's recoverable deletions, oldest deletion
    /// first. Uncached: one call serves one trash listing page request.
    pub(crate) async fn active_deletions_page(
        &mut self,
        start_after: Option<(ChangeSeq, InodeId)>,
        limit: usize,
    ) -> Result<Vec<RecoverableDeletion>, CoreError> {
        self.counters.scan_range_page_calls = self.counters.scan_range_page_calls.saturating_add(1);
        self.base.active_deletions_page(start_after, limit).await
    }

    pub(crate) async fn active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        if let Some(cached) = self.active_tombstone_cache.get(&root_inode_id).cloned() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let tombstones = self.base.tombstones_for_root(root_inode_id).await?;
        let tombstone = super::rows::active_tombstone_from_records(
            tombstones.iter().cloned(),
            self.base.visible_seq(),
        );
        self.active_tombstone_cache
            .insert(root_inode_id, tombstone.clone());
        Ok(tombstone)
    }

    async fn bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.clone(),
        };
        if let Some(cached) = self.bound_child_cache.get(&cache_key).cloned() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let bindings = self
            .base
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        let binding = bindings
            .iter()
            .filter(|direntry| direntry.bind_seq <= self.base.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned();
        self.bound_child_cache.insert(cache_key, binding.clone());
        Ok(binding)
    }

    async fn is_direntry_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, CoreError> {
        let cache_key = BindingCacheKey::from(direntry);
        if let Some(cached) = self.unbind_cache.get(&cache_key).copied() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let unbinds = self.base.direntry_unbinds_for_binding(direntry).await?;
        let unbound = unbinds
            .iter()
            .any(|unbind| unbind.unbind_seq <= self.base.visible_seq());
        self.unbind_cache.insert(cache_key, unbound);
        Ok(unbound)
    }
}

/// The session as a [`MetadataVisibilityReads`] source. Its primitives route
/// through the per-session caches, and it overrides exactly the composite
/// rules it memoizes (`visible_inode`, `current_parent_binding_for_child`,
/// `covering_subtree_tombstone`) so a cache hit short-circuits the walk;
/// every override's miss path, and the un-overridden composites, still decide
/// through the canonical [`super::visibility`] rule bodies.
impl<S: ObjectStore + ?Sized> MetadataVisibilityReads for MetadataViewSession<'_, '_, S> {
    type Error = CoreError;

    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.inode_at_seq(inode_id).await
    }

    async fn find_latest_bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.bound_child(parent_inode_id, name_key).await
    }

    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.latest_parent_binding_for_child(child_inode_id).await
    }

    async fn find_active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.active_subtree_tombstone(root_inode_id).await
    }

    async fn is_binding_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, Self::Error> {
        self.is_direntry_unbound(direntry).await
    }

    async fn current_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        MetadataViewSession::current_parent_binding_for_child(self, child_inode_id).await
    }

    async fn covering_subtree_tombstone(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        MetadataViewSession::covering_subtree_tombstone(self, inode_id).await
    }

    async fn visible_inode(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, Self::Error> {
        MetadataViewSession::visible_inode(self, inode_id).await
    }
}

/// Cursor state for the merged manifest+tail direntry-bind stream, grouped
/// by name key across calls.
struct DirentryBindNameGroupStream {
    manifest_after_row_key: Option<String>,
    manifest_exhausted: bool,
    manifest_candidates: VecDeque<DirentryBindPageCandidate>,
    tail_candidates: Vec<DirentryBindPageCandidate>,
    tail_index: usize,
    pushed_back: Option<DirentryBindPageCandidate>,
}

impl DirentryBindNameGroupStream {
    fn new(tail_candidates: Vec<DirentryBindPageCandidate>, manifest_exhausted: bool) -> Self {
        Self {
            manifest_after_row_key: None,
            manifest_exhausted,
            manifest_candidates: VecDeque::new(),
            tail_candidates,
            tail_index: 0,
            pushed_back: None,
        }
    }
}

/// Every bind row the stream carried for one name key, in row-key order.
struct DirentryBindNameGroup {
    name_key: NameKey,
    rows: Vec<DirentryBindRecord>,
}

#[derive(Clone)]
struct DirentryBindPageCandidate {
    row_key: String,
    record: DirentryBindRecord,
}

fn direntry_bind_row_key(record: &DirentryBindRecord) -> String {
    MetadataRow::DirentryBind {
        parent_inode_id: record.parent_inode_id,
        name_key: record.name_key.clone(),
        display_name: record.display_name.clone(),
        child_inode_id: record.child_inode_id,
        bind_seq: record.bind_seq,
        bind_delta_index: record.bind_delta_index,
    }
    .row_key_for_family(MetadataTableFamily::DirentryBinds)
}
