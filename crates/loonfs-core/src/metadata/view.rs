//! [`MetadataView`]: seq-scoped metadata lookups over manifest tables
//! merged with the WAL tail, plus the caching session wide reads use.

use super::durable_cache::{
    BindingCacheKey, DurableVisibilityCache, ParentNameCacheKey, SharedRows,
};
use super::manifest_index;
use super::visibility::{self, MetadataVisibilityReads};
use crate::checkpoint::VerifiedMetadataTables;
use crate::error::CoreError;
use crate::metadata::{
    unbind_matches_binding, CommitReceiptRecord, DirentryBindRecord, DirentryUnbindRecord,
    InodeRecord, MetadataState, ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use loonfs_api::wire::control::HeadState;
use loonfs_api::{AbsolutePath, ChangeSeq, CommitId, InodeId, InodeKind, NameKey, RevisionNo};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::sync::Arc;

pub(super) const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

#[derive(Clone, Copy)]
pub(crate) struct MetadataSnapshot {
    visible_seq: ChangeSeq,
}

pub(crate) struct MetadataSourceStack<'a, 'store, S: ObjectStore + ?Sized> {
    overlay: Option<&'a MetadataState>,
    wal_tail: Option<&'a MetadataState>,
    manifest: Option<&'a VerifiedMetadataTables<'store, S>>,
    in_memory_base: Option<&'a MetadataState>,
    durable_cache: Option<&'a DurableVisibilityCache>,
}

pub(crate) struct MetadataView<'a, 'store, S: ObjectStore + ?Sized> {
    snapshot: MetadataSnapshot,
    sources: MetadataSourceStack<'a, 'store, S>,
}

#[derive(Debug)]
pub(crate) struct InMemoryMetadataViewStore;

pub(crate) type InMemoryMetadataView<'a> = MetadataView<'a, 'a, InMemoryMetadataViewStore>;

impl<S: ObjectStore + ?Sized> Copy for MetadataSourceStack<'_, '_, S> {}

impl<S: ObjectStore + ?Sized> Clone for MetadataSourceStack<'_, '_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: ObjectStore + ?Sized> Copy for MetadataView<'_, '_, S> {}

impl<S: ObjectStore + ?Sized> Clone for MetadataView<'_, '_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

#[async_trait]
impl ObjectStore for InMemoryMetadataViewStore {
    async fn head(&self, _key: &str) -> Result<Option<ObjectMetadata>, ObjectStoreError> {
        Err(ObjectStoreError::Unsupported(
            "in-memory metadata view store",
        ))
    }

    async fn get_with_metadata(&self, _key: &str) -> Result<Option<ObjectBody>, ObjectStoreError> {
        Err(ObjectStoreError::Unsupported(
            "in-memory metadata view store",
        ))
    }

    async fn get(
        &self,
        _key: &str,
        _range: Option<ByteRange>,
    ) -> Result<Option<Bytes>, ObjectStoreError> {
        Err(ObjectStoreError::Unsupported(
            "in-memory metadata view store",
        ))
    }

    async fn put(
        &self,
        _key: &str,
        _bytes: Bytes,
        _mode: PutMode,
    ) -> Result<ObjectMetadata, ObjectStoreError> {
        Err(ObjectStoreError::Unsupported(
            "in-memory metadata view store",
        ))
    }

    async fn delete(&self, _key: &str) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::Unsupported(
            "in-memory metadata view store",
        ))
    }

    fn list_prefix_stream(
        &self,
        _prefix: &str,
    ) -> BoxStream<'static, Result<String, ObjectStoreError>> {
        Box::pin(stream::empty())
    }
}

impl<'a> InMemoryMetadataView<'a> {
    pub(crate) fn in_memory(
        base: &'a MetadataState,
        overlay: Option<&'a MetadataState>,
        visible_seq: ChangeSeq,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot { visible_seq },
            sources: MetadataSourceStack {
                overlay,
                wal_tail: None,
                manifest: None,
                in_memory_base: Some(base),
                durable_cache: None,
            },
        }
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    pub(crate) fn from_loaded_head(
        head: &'a HeadState,
        tables: &'a VerifiedMetadataTables<'store, S>,
        wal_tail_rows: &'a MetadataState,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot {
                visible_seq: head.seq,
            },
            sources: MetadataSourceStack {
                overlay: None,
                wal_tail: Some(wal_tail_rows),
                manifest: Some(tables),
                in_memory_base: None,
                durable_cache: None,
            },
        }
    }

    /// A view of exactly one manifest's tables at the sequence they
    /// materialize, with nothing layered over them.
    ///
    /// [`Self::from_loaded_head`] answers "the namespace as of its live
    /// head" by replaying the WAL tail over a basis; this answers "the
    /// namespace exactly as this manifest recorded it". Callers that pin an
    /// immutable manifest — a checkpoint's basis — read through this, so no
    /// row committed after the manifest can leak into the answer.
    pub(crate) fn over_manifest_tables(
        tables: &'a VerifiedMetadataTables<'store, S>,
        materialized_seq: ChangeSeq,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot {
                visible_seq: materialized_seq,
            },
            sources: MetadataSourceStack {
                overlay: None,
                wal_tail: None,
                manifest: Some(tables),
                in_memory_base: None,
                durable_cache: None,
            },
        }
    }

    pub(crate) fn with_overlay<'view>(
        &'view self,
        overlay: &'view MetadataState,
        visible_seq: ChangeSeq,
    ) -> MetadataView<'view, 'store, S> {
        MetadataView {
            snapshot: MetadataSnapshot { visible_seq },
            sources: MetadataSourceStack {
                overlay: Some(overlay),
                wal_tail: self.sources.wal_tail,
                manifest: self.sources.manifest,
                in_memory_base: self.sources.in_memory_base,
                durable_cache: self.sources.durable_cache,
            },
        }
    }

    pub(super) fn visible_seq(&self) -> ChangeSeq {
        self.snapshot.visible_seq
    }

    /// Attaches a batch-scoped durable-layer memo. Only attach where every
    /// composed view's `visible_seq` stays at or above the seq the durable
    /// layers were loaded at, so memoized durable answers never go stale;
    /// overlay rows are composed per lookup either way.
    pub(crate) fn with_durable_cache(
        mut self,
        durable_cache: &'a DurableVisibilityCache,
    ) -> MetadataView<'a, 'store, S> {
        self.sources.durable_cache = Some(durable_cache);
        self
    }

    pub(super) fn row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        [
            self.sources.overlay,
            self.sources.wal_tail,
            self.sources.in_memory_base,
        ]
        .into_iter()
        .flatten()
    }

    fn overlay_state(&self) -> Option<&'a MetadataState> {
        self.sources.overlay
    }

    fn durable_row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        [self.sources.wal_tail, self.sources.in_memory_base]
            .into_iter()
            .flatten()
    }

    pub(super) fn manifest_tables(&self) -> Option<&'a VerifiedMetadataTables<'store, S>> {
        self.sources.manifest
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    /// Adapter presenting this view's primitive lookups as the
    /// [`MetadataVisibilityReads`] contract, so the composite rules are
    /// decided once in [`super::visibility`]. The view is `Copy`, so the
    /// adapter owns a copy and needs no borrow of `self`.
    fn reads(&self) -> MetadataViewReads<'a, 'store, S> {
        MetadataViewReads { view: *self }
    }

    /// Whether the directory currently has at least one visible child,
    /// answered by a limit-1 page through a fresh session so the cost stays
    /// bounded no matter how wide the directory is.
    pub(crate) async fn has_visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<bool, CoreError> {
        let mut session = self.session();
        Ok(!session
            .visible_children_page_by_name_key(parent_inode_id, None, 1)
            .await?
            .is_empty())
    }

    pub(crate) async fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        visibility::resolve_visible_path(&mut self.reads(), absolute_path).await
    }

    pub(crate) async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        visibility::visible_child(&mut self.reads(), parent_inode_id, name_key).await
    }

    pub(crate) async fn visible_inode(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        visibility::visible_inode(&mut self.reads(), inode_id).await
    }

    pub(crate) async fn inode_at_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<InodeRecord>, CoreError> {
        if let Some(inode) = self
            .overlay_state()
            .and_then(|state| state.inode_at_seq(inode_id, self.visible_seq()))
        {
            return Ok(Some(inode));
        }
        if let Some(cache) = self.sources.durable_cache {
            if let Some(cached) = cache.get(|inner| &mut inner.inodes, &inode_id) {
                return Ok(cached);
            }
        }
        let mut durable = self
            .durable_row_states()
            .find_map(|state| state.inode_at_seq(inode_id, self.visible_seq()));
        if durable.is_none() {
            if let Some(tables) = self.manifest_tables() {
                durable = manifest_index::inode_at_seq(tables, inode_id).await?;
            }
        }
        if let Some(cache) = self.sources.durable_cache {
            cache.insert(|inner| &mut inner.inodes, inode_id, durable.clone());
        }
        Ok(durable)
    }

    pub(crate) async fn latest_revision_head(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        if self.visible_inode(inode_id).await?.is_none() {
            return Ok(None);
        }
        self.latest_revision_record(inode_id).await
    }

    pub(crate) async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        let row_revision = self.row_latest_revision_for_inode(inode_id);
        let manifest_revision = if let Some(tables) = self.manifest_tables() {
            manifest_index::latest_revision_for_inode(tables, inode_id).await?
        } else {
            None
        };
        Ok(row_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
    }

    pub(crate) async fn revision_for_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord, CoreError> {
        let inode = self
            .inode_at_seq(inode_id)
            .await?
            .ok_or_else(|| CoreError::PathNotFound(inode_id.to_string()))?;
        if inode.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                path: inode_id.to_string(),
                kind: inode.inode_kind,
            });
        }
        self.revision_at_head(inode_id, revision_no)
            .await?
            .ok_or(CoreError::RevisionNotFound {
                inode_id,
                revision_no,
            })
    }

    pub(crate) async fn revision_at_head(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        let row_revision = self.row_revision_for_inode_no(inode_id, revision_no);
        let manifest_revision = if let Some(tables) = self.manifest_tables() {
            manifest_index::revision_for_inode_no(tables, inode_id, revision_no).await?
        } else {
            None
        };
        Ok(row_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
    }

    pub(crate) async fn revisions_for_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut revisions = if let Some(tables) = self.manifest_tables() {
            manifest_index::revisions_for_inode_page_desc(tables, inode_id, start_after, limit)
                .await?
        } else {
            Vec::new()
        };
        revisions.extend(self.row_revisions_for_inode_page_desc(inode_id, start_after));
        revisions.retain(|revision| revision.committed_seq <= self.visible_seq());
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions.truncate(limit);
        Ok(revisions)
    }

    pub(crate) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>, CoreError> {
        // Each row state answers from its receipt index (newest per commit
        // id) instead of a scan over every receipt row; the seq guard stays
        // as written even though composed states never hold rows past the
        // visible seq.
        let row_receipt = self
            .row_states()
            .filter_map(|state| state.find_commit_receipt(commit_id))
            .filter(|receipt| receipt.committed_seq <= self.visible_seq())
            .max_by_key(|receipt| receipt.committed_seq)
            .cloned();
        let manifest_receipt = if let Some(tables) = self.manifest_tables() {
            manifest_index::commit_receipt(tables, commit_id).await?
        } else {
            None
        };
        Ok(row_receipt
            .into_iter()
            .chain(manifest_receipt)
            .max_by_key(|receipt| receipt.committed_seq))
    }

    pub(crate) async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        visibility::current_parent_binding_for_child(&mut self.reads(), child_inode_id).await
    }

    /// Latest binding whose child is `child_inode_id` at the visible seq,
    /// regardless of whether it has since been unbound. The
    /// [`MetadataVisibilityReads`] primitive backing
    /// [`Self::current_parent_binding_for_child`]'s canonical rule.
    async fn latest_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let bindings = self.direntry_binds_for_child(child_inode_id).await?;
        let latest = bindings
            .iter()
            .filter(|direntry| direntry.bind_seq <= self.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned();
        Ok(latest)
    }

    pub(crate) async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        visibility::covering_subtree_tombstone(&mut self.reads(), inode_id).await
    }

    pub(crate) async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool, CoreError> {
        visibility::would_create_directory_cycle(&mut self.reads(), inode_id, new_parent_inode_id)
            .await
    }

    pub(crate) async fn bound_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let bindings = self
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        let latest = bindings
            .iter()
            .filter(|direntry| direntry.bind_seq <= self.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index))
            .cloned();
        Ok(latest)
    }

    pub(crate) async fn is_direntry_unbound(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, CoreError> {
        let unbinds = self.direntry_unbinds_for_binding(direntry).await?;
        let unbound = unbinds
            .iter()
            .any(|unbind| unbind.unbind_seq <= self.visible_seq());
        Ok(unbound)
    }

    pub(crate) async fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let tombstones = self.tombstones_for_root(root_inode_id).await?;
        let active = super::rows::active_tombstone_from_records(
            tombstones.iter().cloned(),
            self.visible_seq(),
        );
        Ok(active)
    }

    pub(super) async fn direntry_binds_for_parent_name(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<SharedRows<DirentryBindRecord>, CoreError> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.clone(),
        };
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(|inner| &mut inner.binds_for_parent_name, &cache_key))
        {
            cached
        } else {
            let mut durable = if let Some(tables) = self.manifest_tables() {
                manifest_index::direntry_binds_for_parent_name(tables, parent_inode_id, name_key)
                    .await?
            } else {
                Vec::new()
            };
            durable.extend(self.durable_row_states().flat_map(|state| {
                state
                    .direntry_binds()
                    .iter()
                    .filter(move |direntry| {
                        direntry.parent_inode_id == parent_inode_id
                            && direntry.name_key == *name_key
                    })
                    .cloned()
            }));
            let durable = Arc::new(durable);
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.binds_for_parent_name,
                    cache_key,
                    Arc::clone(&durable),
                );
            }
            durable
        };
        let overlay = self
            .overlay_state()
            .into_iter()
            .flat_map(|state| {
                state
                    .direntry_binds()
                    .iter()
                    .filter(move |direntry| {
                        direntry.parent_inode_id == parent_inode_id
                            && direntry.name_key == *name_key
                    })
                    .cloned()
            })
            .collect();
        Ok(SharedRows { durable, overlay })
    }

    pub(super) async fn direntry_binds_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<SharedRows<DirentryBindRecord>, CoreError> {
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(|inner| &mut inner.binds_for_child, &child_inode_id))
        {
            cached
        } else {
            let mut durable = if let Some(tables) = self.manifest_tables() {
                manifest_index::direntry_binds_for_child(tables, child_inode_id).await?
            } else {
                Vec::new()
            };
            durable.extend(self.durable_row_states().flat_map(|state| {
                state
                    .direntry_binds()
                    .iter()
                    .filter(move |direntry| direntry.child_inode_id == child_inode_id)
                    .cloned()
            }));
            let durable = Arc::new(durable);
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.binds_for_child,
                    child_inode_id,
                    Arc::clone(&durable),
                );
            }
            durable
        };
        let overlay = self
            .overlay_state()
            .into_iter()
            .flat_map(|state| {
                state
                    .direntry_binds()
                    .iter()
                    .filter(move |direntry| direntry.child_inode_id == child_inode_id)
                    .cloned()
            })
            .collect();
        Ok(SharedRows { durable, overlay })
    }

    pub(super) async fn direntry_unbinds_for_binding(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<SharedRows<DirentryUnbindRecord>, CoreError> {
        let cache_key = BindingCacheKey::from(direntry);
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(|inner| &mut inner.unbinds_for_binding, &cache_key))
        {
            cached
        } else {
            let mut durable = if let Some(tables) = self.manifest_tables() {
                manifest_index::direntry_unbinds_for_binding(tables, direntry).await?
            } else {
                Vec::new()
            };
            durable.extend(self.durable_row_states().flat_map(|state| {
                state
                    .direntry_unbinds()
                    .iter()
                    .filter(move |unbind| unbind_matches_binding(unbind, direntry))
                    .cloned()
            }));
            let durable = Arc::new(durable);
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.unbinds_for_binding,
                    cache_key,
                    Arc::clone(&durable),
                );
            }
            durable
        };
        let overlay = self
            .overlay_state()
            .into_iter()
            .flat_map(|state| {
                state
                    .direntry_unbinds()
                    .iter()
                    .filter(move |unbind| unbind_matches_binding(unbind, direntry))
                    .cloned()
            })
            .collect();
        Ok(SharedRows { durable, overlay })
    }

    fn row_latest_revision_for_inode(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.row_states()
            .flat_map(|state| state.revisions())
            .filter(|revision| {
                revision.inode_id == inode_id && revision.committed_seq <= self.visible_seq()
            })
            .max_by_key(|revision| revision_order_key(revision))
            .cloned()
    }

    fn row_revision_for_inode_no(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Option<RevisionRecord> {
        self.row_states()
            .flat_map(|state| state.revisions())
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.revision_no == revision_no
                    && revision.committed_seq <= self.visible_seq()
            })
            .max_by_key(|revision| revision_order_key(revision))
            .cloned()
    }

    fn row_revisions_for_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
    ) -> Vec<RevisionRecord> {
        let mut revisions = self
            .row_states()
            .flat_map(|state| state.revisions())
            .filter(|revision| {
                revision.inode_id == inode_id
                    && revision.committed_seq <= self.visible_seq()
                    && start_after
                        .map(|position| revision_is_after_position_desc(revision, position))
                        .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions
    }

    /// Every tombstone event in the namespace, merged across manifest
    /// tables and row states. Uncached: the trash listing is a cold read
    /// over a family that per-root probes never enumerate.
    pub(super) async fn all_tombstones(
        &self,
    ) -> Result<SharedRows<SubtreeTombstoneRecord>, CoreError> {
        let mut durable = if let Some(tables) = self.manifest_tables() {
            manifest_index::all_subtree_tombstones(tables).await?
        } else {
            Vec::new()
        };
        durable.extend(
            self.durable_row_states()
                .flat_map(|state| state.subtree_tombstones().iter().cloned()),
        );
        let overlay = self
            .overlay_state()
            .into_iter()
            .flat_map(|state| state.subtree_tombstones().iter().cloned())
            .collect();
        Ok(SharedRows {
            durable: Arc::new(durable),
            overlay,
        })
    }

    pub(super) async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<SharedRows<SubtreeTombstoneRecord>, CoreError> {
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(|inner| &mut inner.tombstones_for_root, &root_inode_id))
        {
            cached
        } else {
            let mut durable = if let Some(tables) = self.manifest_tables() {
                manifest_index::tombstones_for_root(tables, root_inode_id).await?
            } else {
                Vec::new()
            };
            durable.extend(self.durable_row_states().flat_map(|state| {
                state
                    .subtree_tombstones()
                    .iter()
                    .filter(move |tombstone| tombstone.root_inode_id == root_inode_id)
                    .cloned()
            }));
            let durable = Arc::new(durable);
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.tombstones_for_root,
                    root_inode_id,
                    Arc::clone(&durable),
                );
            }
            durable
        };
        let overlay = self
            .overlay_state()
            .into_iter()
            .flat_map(|state| {
                state
                    .subtree_tombstones()
                    .iter()
                    .filter(move |tombstone| tombstone.root_inode_id == root_inode_id)
                    .cloned()
            })
            .collect();
        Ok(SharedRows { durable, overlay })
    }

    /// Every unbind for `parent_inode_id` with a name key in
    /// `[first_name_key, last_name_key]`, merged across manifest tables and
    /// row states. Complete over the range: callers treat absence as "no
    /// unbind exists".
    pub(super) async fn direntry_unbinds_for_parent_name_range(
        &self,
        parent_inode_id: InodeId,
        first_name_key: &NameKey,
        last_name_key: &NameKey,
    ) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
        let mut unbinds = if let Some(tables) = self.manifest_tables() {
            manifest_index::direntry_unbinds_for_parent_name_range(
                tables,
                parent_inode_id,
                first_name_key,
                last_name_key,
            )
            .await?
        } else {
            Vec::new()
        };
        unbinds.extend(self.row_states().flat_map(|state| {
            state
                .direntry_unbinds()
                .iter()
                .filter(move |unbind| {
                    unbind.parent_inode_id == parent_inode_id
                        && unbind.name_key >= *first_name_key
                        && unbind.name_key <= *last_name_key
                })
                .cloned()
        }));
        Ok(unbinds)
    }
}

/// [`MetadataView`] as a [`MetadataVisibilityReads`] source: it answers only
/// the primitive lookups (over the manifest tables merged with the row-state
/// tail) and takes every composite rule from the provided trait methods, so
/// the object-store-backed view decides visibility through the exact same
/// bodies as the in-memory state.
struct MetadataViewReads<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
}

impl<S: ObjectStore + ?Sized> MetadataVisibilityReads for MetadataViewReads<'_, '_, S> {
    type Error = CoreError;

    async fn find_inode(&mut self, inode_id: InodeId) -> Result<Option<InodeRecord>, Self::Error> {
        self.view.inode_at_seq(inode_id).await
    }

    async fn find_latest_bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view.bound_child(parent_inode_id, name_key).await
    }

    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindRecord>, Self::Error> {
        self.view
            .latest_parent_binding_for_child(child_inode_id)
            .await
    }

    async fn find_active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.view.active_subtree_tombstone(root_inode_id).await
    }

    async fn is_binding_unbound(
        &mut self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, Self::Error> {
        self.view.is_direntry_unbound(direntry).await
    }
}

fn revision_order_key(record: &RevisionRecord) -> (RevisionNo, loonfs_api::ChangeSeq, u32) {
    (
        record.revision_no,
        record.committed_seq,
        record.revision_delta_index,
    )
}

fn revision_is_after_position_desc(
    record: &RevisionRecord,
    position: manifest_index::RevisionPagePosition,
) -> bool {
    revision_order_key(record)
        < (
            position.revision_no,
            position.committed_seq,
            position.revision_delta_index,
        )
}
