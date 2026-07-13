//! [`MetadataView`]: seq-scoped metadata lookups over manifest tables
//! merged with the WAL tail, plus the caching session wide reads use.

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
use loonfs_api::wire::manifest::{MetadataRow, MetadataTableFamily};
use loonfs_api::{
    AbsolutePath, ChangeSeq, CommitId, InodeId, InodeKind, NameKey, NamePolicy, RevisionNo,
};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;

const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

#[derive(Clone, Copy)]
pub(crate) struct MetadataSnapshot {
    visible_seq: ChangeSeq,
    name_policy: NamePolicy,
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
        name_policy: NamePolicy,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot {
                visible_seq,
                name_policy,
            },
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
        name_policy: NamePolicy,
        tables: &'a VerifiedMetadataTables<'store, S>,
        wal_tail_rows: &'a MetadataState,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot {
                visible_seq: head.seq,
                name_policy,
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

    pub(crate) fn name_policy(&self) -> NamePolicy {
        self.snapshot.name_policy
    }

    pub(crate) fn with_overlay<'view>(
        &'view self,
        overlay: &'view MetadataState,
        visible_seq: ChangeSeq,
    ) -> MetadataView<'view, 'store, S> {
        MetadataView {
            snapshot: MetadataSnapshot {
                visible_seq,
                name_policy: self.snapshot.name_policy,
            },
            sources: MetadataSourceStack {
                overlay: Some(overlay),
                wal_tail: self.sources.wal_tail,
                manifest: self.sources.manifest,
                in_memory_base: self.sources.in_memory_base,
                durable_cache: self.sources.durable_cache,
            },
        }
    }

    fn visible_seq(&self) -> ChangeSeq {
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

    fn row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
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

    fn manifest_tables(&self) -> Option<&'a VerifiedMetadataTables<'store, S>> {
        self.sources.manifest
    }

    pub(crate) fn session(self) -> MetadataViewSession<'a, 'store, S> {
        MetadataViewSession::new(self)
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

    pub(crate) async fn visible_children(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let Some(parent) = self.visible_inode(parent_inode_id).await? else {
            return Ok(Vec::new());
        };
        if parent.inode_kind != InodeKind::Directory {
            return Ok(Vec::new());
        }

        let candidates = self.direntry_binds_for_parent(parent_inode_id).await?;
        let mut reads = self.reads();
        let mut children = Vec::new();
        for direntry in candidates {
            if visibility::is_visible_child_direntry(&mut reads, &direntry).await? {
                children.push(direntry);
            }
        }
        children.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.child_inode_id.0.cmp(&right.child_inode_id.0))
        });
        Ok(children)
    }

    pub(crate) async fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        visibility::resolve_visible_path(
            &mut self.reads(),
            absolute_path,
            self.snapshot.name_policy,
        )
        .await
    }

    pub(crate) async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
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
        let row_receipt = self
            .row_states()
            .flat_map(|state| state.commit_receipts())
            .filter(|receipt| receipt.commit_id == *commit_id)
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
        Ok(bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index)))
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
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let bindings = self
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        Ok(bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index)))
    }

    pub(crate) async fn is_direntry_unbound(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<bool, CoreError> {
        let unbinds = self.direntry_unbinds_for_binding(direntry).await?;
        Ok(unbinds
            .into_iter()
            .any(|unbind| unbind.unbind_seq <= self.visible_seq()))
    }

    pub(crate) async fn active_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        let tombstones = self.tombstones_for_root(root_inode_id).await?;
        Ok(tombstones
            .into_iter()
            .filter(|tombstone| tombstone.tombstone_seq <= self.visible_seq())
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index)))
    }

    async fn direntry_binds_for_parent(
        &self,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(|inner| &mut inner.binds_for_parent, &parent_inode_id))
        {
            cached
        } else {
            let mut durable = if let Some(tables) = self.manifest_tables() {
                manifest_index::direntry_binds_for_parent(tables, parent_inode_id).await?
            } else {
                Vec::new()
            };
            durable.extend(self.durable_row_states().flat_map(|state| {
                state
                    .direntry_binds()
                    .iter()
                    .filter(move |direntry| direntry.parent_inode_id == parent_inode_id)
                    .cloned()
            }));
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.binds_for_parent,
                    parent_inode_id,
                    durable.clone(),
                );
            }
            durable
        };
        let mut bindings = durable;
        bindings.extend(self.overlay_state().into_iter().flat_map(|state| {
            state
                .direntry_binds()
                .iter()
                .filter(move |direntry| direntry.parent_inode_id == parent_inode_id)
                .cloned()
        }));
        Ok(bindings)
    }

    async fn direntry_binds_for_parent_name(
        &self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.to_owned(),
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
                        direntry.parent_inode_id == parent_inode_id && direntry.name_key == name_key
                    })
                    .cloned()
            }));
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.binds_for_parent_name,
                    cache_key,
                    durable.clone(),
                );
            }
            durable
        };
        let mut bindings = durable;
        bindings.extend(self.overlay_state().into_iter().flat_map(|state| {
            state
                .direntry_binds()
                .iter()
                .filter(move |direntry| {
                    direntry.parent_inode_id == parent_inode_id && direntry.name_key == name_key
                })
                .cloned()
        }));
        Ok(bindings)
    }

    async fn direntry_binds_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Vec<DirentryBindRecord>, CoreError> {
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
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.binds_for_child,
                    child_inode_id,
                    durable.clone(),
                );
            }
            durable
        };
        let mut bindings = durable;
        bindings.extend(self.overlay_state().into_iter().flat_map(|state| {
            state
                .direntry_binds()
                .iter()
                .filter(move |direntry| direntry.child_inode_id == child_inode_id)
                .cloned()
        }));
        Ok(bindings)
    }

    async fn direntry_unbinds_for_binding(
        &self,
        direntry: &DirentryBindRecord,
    ) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
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
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.unbinds_for_binding,
                    cache_key,
                    durable.clone(),
                );
            }
            durable
        };
        let mut unbinds = durable;
        unbinds.extend(self.overlay_state().into_iter().flat_map(|state| {
            state
                .direntry_unbinds()
                .iter()
                .filter(move |unbind| unbind_matches_binding(unbind, direntry))
                .cloned()
        }));
        Ok(unbinds)
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

    async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Vec<SubtreeTombstoneRecord>, CoreError> {
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
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(
                    |inner| &mut inner.tombstones_for_root,
                    root_inode_id,
                    durable.clone(),
                );
            }
            durable
        };
        let mut tombstones = durable;
        tombstones.extend(self.overlay_state().into_iter().flat_map(|state| {
            state
                .subtree_tombstones()
                .iter()
                .filter(move |tombstone| tombstone.root_inode_id == root_inode_id)
                .cloned()
        }));
        Ok(tombstones)
    }

    /// Every unbind for `parent_inode_id` with a name key in
    /// `[first_name_key, last_name_key]`, merged across manifest tables and
    /// row states. Complete over the range: callers treat absence as "no
    /// unbind exists".
    async fn direntry_unbinds_for_parent_name_range(
        &self,
        parent_inode_id: InodeId,
        first_name_key: &str,
        last_name_key: &str,
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
                        && unbind.name_key.as_str() >= first_name_key
                        && unbind.name_key.as_str() <= last_name_key
                })
                .cloned()
        }));
        Ok(unbinds)
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
                        .map(|last_name_key| direntry.name_key.as_str() > last_name_key)
                        .unwrap_or(true)
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
        name_key: &str,
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

#[derive(Debug, Clone)]
pub(crate) struct VisibleChildEntry {
    pub(crate) binding: DirentryBindRecord,
    pub(crate) inode: InodeRecord,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct MetadataViewSessionCounters {
    pub(crate) visible_child_calls: u64,
    pub(crate) visible_inode_calls: u64,
    pub(crate) current_parent_binding_calls: u64,
    pub(crate) covering_tombstone_calls: u64,
    pub(crate) latest_revision_calls: u64,
    pub(crate) direntry_child_scan_calls: u64,
    pub(crate) scan_prefix_calls: u64,
    pub(crate) scan_range_page_calls: u64,
    pub(crate) list_preload_unbind_range_scans: u64,
    pub(crate) list_preload_child_lookups: u64,
}

pub(crate) struct MetadataViewSession<'a, 'store, S: ObjectStore + ?Sized> {
    base: MetadataView<'a, 'store, S>,
    inode_at_seq_cache: HashMap<InodeId, Option<InodeRecord>>,
    visible_inode_cache: HashMap<InodeId, Option<InodeRecord>>,
    bound_child_cache: HashMap<ParentNameCacheKey, Option<DirentryBindRecord>>,
    current_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
    latest_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
    latest_revision_head_cache: HashMap<InodeId, Option<RevisionRecord>>,
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
                .into_iter()
                .filter(|direntry| direntry.bind_seq <= visible_seq)
                .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
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
            let active_tombstone = tombstones
                .into_iter()
                .filter(|tombstone| tombstone.tombstone_seq <= visible_seq)
                .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index));
            self.active_tombstone_cache
                .insert(child_inode_id, active_tombstone);
        }
        Ok(())
    }

    /// Resolves `absolute_path` with the canonical visibility rules, after
    /// a pipelined preload of the walk's storage lookups.
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
        prefetch_leaf_revision: bool,
    ) -> Result<ResolvedVisiblePath, CoreError> {
        self.preload_path_walk(absolute_path, prefetch_leaf_revision)
            .await?;
        visibility::resolve_visible_path(self, absolute_path, self.base.name_policy()).await
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
        prefetch_leaf_revision: bool,
    ) -> Result<(), CoreError> {
        let visible_seq = self.base.visible_seq();
        let name_policy = self.base.name_policy();
        let component_name_keys: Vec<String> = absolute_path
            .components()
            .iter()
            .map(|component| {
                NameKey::for_display_name(name_policy, &component.to_display_name())
                    .as_str()
                    .to_owned()
            })
            .collect();

        let mut current_inode_id = InodeId(1);
        let mut pending_binding: Option<DirentryBindRecord> = None;
        for wave in 0..=component_name_keys.len() {
            let lookup_name_key = component_name_keys.get(wave);
            let is_leaf_wave = wave == component_name_keys.len();

            let want_inode = !self.inode_at_seq_cache.contains_key(&current_inode_id);
            let want_tombstone = !self.active_tombstone_cache.contains_key(&current_inode_id);
            let want_parent_binding = !self
                .latest_parent_binding_cache
                .contains_key(&current_inode_id);
            let arrived_by_binding = pending_binding.take();
            let unbind_lookup = arrived_by_binding
                .as_ref()
                .filter(|binding| {
                    !self
                        .unbind_cache
                        .contains_key(&BindingCacheKey::from(*binding))
                })
                .cloned();
            let bound_child_lookup = lookup_name_key
                .filter(|name_key| {
                    !self.bound_child_cache.contains_key(&ParentNameCacheKey {
                        parent_inode_id: current_inode_id,
                        name_key: (*name_key).clone(),
                    })
                })
                .cloned();
            let want_revision = is_leaf_wave
                && prefetch_leaf_revision
                && !self
                    .latest_revision_head_cache
                    .contains_key(&current_inode_id);

            let base = &self.base;
            let (inode, tombstones, child_bindings, unbinds, bound_rows, revision) = futures::try_join!(
                async {
                    match want_inode {
                        true => base.inode_at_seq(current_inode_id).await.map(Some),
                        false => Ok(None),
                    }
                },
                async {
                    match want_tombstone {
                        true => base.tombstones_for_root(current_inode_id).await.map(Some),
                        false => Ok(None),
                    }
                },
                async {
                    match want_parent_binding {
                        true => base
                            .direntry_binds_for_child(current_inode_id)
                            .await
                            .map(Some),
                        false => Ok(None),
                    }
                },
                async {
                    match &unbind_lookup {
                        Some(binding) => base.direntry_unbinds_for_binding(binding).await.map(Some),
                        None => Ok(None),
                    }
                },
                async {
                    match &bound_child_lookup {
                        Some(name_key) => base
                            .direntry_binds_for_parent_name(current_inode_id, name_key)
                            .await
                            .map(Some),
                        None => Ok(None),
                    }
                },
                async {
                    match want_revision {
                        true => base
                            .latest_revision_record(current_inode_id)
                            .await
                            .map(Some),
                        false => Ok(None),
                    }
                },
            )?;

            if let Some(inode) = inode {
                self.inode_at_seq_cache.insert(current_inode_id, inode);
            }
            if let Some(tombstones) = tombstones {
                let active = tombstones
                    .into_iter()
                    .filter(|tombstone| tombstone.tombstone_seq <= visible_seq)
                    .max_by_key(|tombstone| {
                        (tombstone.tombstone_seq, tombstone.tombstone_delta_index)
                    });
                self.active_tombstone_cache.insert(current_inode_id, active);
            }
            if let Some(bindings) = child_bindings {
                let latest = bindings
                    .into_iter()
                    .filter(|direntry| direntry.bind_seq <= visible_seq)
                    .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
                self.latest_parent_binding_cache
                    .insert(current_inode_id, latest);
            }
            if let Some(revision) = revision {
                self.latest_revision_head_cache
                    .insert(current_inode_id, revision);
            }
            if let Some(binding) = &arrived_by_binding {
                let cache_key = BindingCacheKey::from(binding);
                let unbound = match unbinds {
                    Some(rows) => {
                        let unbound = rows
                            .into_iter()
                            .any(|unbind| unbind.unbind_seq <= visible_seq);
                        self.unbind_cache.insert(cache_key, unbound);
                        unbound
                    }
                    None => self.unbind_cache.get(&cache_key).copied().unwrap_or(false),
                };
                if unbound {
                    // The walk dead-ends at the previous component; probing
                    // deeper would warm caches for nothing.
                    break;
                }
            }

            let Some(name_key) = lookup_name_key else {
                break;
            };
            let bound = match bound_rows {
                Some(rows) => {
                    let latest = rows
                        .into_iter()
                        .filter(|direntry| direntry.bind_seq <= visible_seq)
                        .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
                    self.bound_child_cache.insert(
                        ParentNameCacheKey {
                            parent_inode_id: current_inode_id,
                            name_key: name_key.clone(),
                        },
                        latest.clone(),
                    );
                    latest
                }
                None => self
                    .bound_child_cache
                    .get(&ParentNameCacheKey {
                        parent_inode_id: current_inode_id,
                        name_key: name_key.clone(),
                    })
                    .cloned()
                    .flatten(),
            };
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
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        self.counters.visible_child_calls = self.counters.visible_child_calls.saturating_add(1);
        visibility::visible_child(self, parent_inode_id, name_key).await
    }

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

    /// The head revision of an inode the caller has already established as
    /// visible at this session's seq — path resolution and child enumeration
    /// both have. Unlike [`MetadataView::latest_revision_head`], this skips
    /// re-deriving the inode's visibility: the re-check costs a full
    /// child-binds scan and tombstone probe per entry on a wide listing,
    /// and it can never disagree with the enumeration that just admitted
    /// the entry at the same seq.
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
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.base.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
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

    pub(crate) async fn active_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>, CoreError> {
        if let Some(cached) = self.active_tombstone_cache.get(&root_inode_id).cloned() {
            return Ok(cached);
        }
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let tombstones = self.base.tombstones_for_root(root_inode_id).await?;
        let tombstone = tombstones
            .into_iter()
            .filter(|tombstone| tombstone.tombstone_seq <= self.base.visible_seq())
            .max_by_key(|tombstone| (tombstone.tombstone_seq, tombstone.tombstone_delta_index));
        self.active_tombstone_cache
            .insert(root_inode_id, tombstone.clone());
        Ok(tombstone)
    }

    async fn bound_child(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &str,
    ) -> Result<Option<DirentryBindRecord>, CoreError> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.to_owned(),
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
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.base.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index));
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
            .into_iter()
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
        name_key: &str,
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

/// Batch-scoped memo of durable-layer lookups: everything except the
/// mutation overlay (manifest tables, WAL tail, in-memory base). Publish
/// batches attach one so repeated path walks across candidates scan each
/// durable key once; the overlay — the only layer that changes between
/// candidates — is composed per lookup on top.
///
/// Cached vectors are raw, unfiltered rows; callers apply their own seq
/// filters, so answers hold regardless of the composed view's visible seq.
#[derive(Debug, Default)]
pub(crate) struct DurableVisibilityCache {
    inner: Mutex<DurableVisibilityCacheInner>,
}

#[derive(Debug, Default)]
struct DurableVisibilityCacheInner {
    inodes: HashMap<InodeId, Option<InodeRecord>>,
    binds_for_parent_name: HashMap<ParentNameCacheKey, Vec<DirentryBindRecord>>,
    binds_for_parent: HashMap<InodeId, Vec<DirentryBindRecord>>,
    binds_for_child: HashMap<InodeId, Vec<DirentryBindRecord>>,
    unbinds_for_binding: HashMap<BindingCacheKey, Vec<DirentryUnbindRecord>>,
    tombstones_for_root: HashMap<InodeId, Vec<SubtreeTombstoneRecord>>,
    hits: u64,
    misses: u64,
}

/// Hit/miss counts, pinning cache engagement in tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct DurableVisibilityCacheStats {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
}

impl DurableVisibilityCache {
    #[cfg(test)]
    pub(crate) fn stats(&self) -> DurableVisibilityCacheStats {
        let inner = self.lock();
        DurableVisibilityCacheStats {
            hits: inner.hits,
            misses: inner.misses,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DurableVisibilityCacheInner> {
        self.inner
            .lock()
            .expect("durable visibility cache lock poisoned")
    }

    fn get<K: std::hash::Hash + Eq, V: Clone>(
        &self,
        map: impl FnOnce(&mut DurableVisibilityCacheInner) -> &mut HashMap<K, V>,
        key: &K,
    ) -> Option<V> {
        let mut inner = self.lock();
        let hit = map(&mut inner).get(key).cloned();
        if hit.is_some() {
            inner.hits += 1;
        } else {
            inner.misses += 1;
        }
        hit
    }

    fn insert<K: std::hash::Hash + Eq, V>(
        &self,
        map: impl FnOnce(&mut DurableVisibilityCacheInner) -> &mut HashMap<K, V>,
        key: K,
        value: V,
    ) {
        let mut inner = self.lock();
        map(&mut inner).insert(key, value);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParentNameCacheKey {
    parent_inode_id: InodeId,
    name_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BindingCacheKey {
    parent_inode_id: InodeId,
    name_key: String,
    child_inode_id: InodeId,
    bind_seq: loonfs_api::ChangeSeq,
    bind_delta_index: u32,
}

impl From<&DirentryBindRecord> for BindingCacheKey {
    fn from(record: &DirentryBindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
    }
}

/// An unbind carries the identity of the binding event it revokes, so it
/// keys the same cache slot as that binding.
impl From<&DirentryUnbindRecord> for BindingCacheKey {
    fn from(record: &DirentryUnbindRecord) -> Self {
        Self {
            parent_inode_id: record.parent_inode_id,
            name_key: record.name_key.clone(),
            child_inode_id: record.child_inode_id,
            bind_seq: record.bind_seq,
            bind_delta_index: record.bind_delta_index,
        }
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
    name_key: String,
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
        name_key: crate::metadata::record_name_key(&record.name_key),
        display_name: record.display_name.clone(),
        child_inode_id: record.child_inode_id,
        bind_seq: record.bind_seq,
        bind_delta_index: record.bind_delta_index,
    }
    .row_key_for_family(MetadataTableFamily::DirentryBinds)
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
