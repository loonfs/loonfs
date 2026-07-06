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
use loonfs_api::{AbsolutePath, ChangeSeq, CommitId, InodeId, InodeKind, NamePolicy, RevisionNo};
use loonfs_objectstore::{
    ByteRange, ObjectBody, ObjectMetadata, ObjectStore, ObjectStoreError, PutMode,
};
use std::collections::{BTreeSet, HashMap, VecDeque};

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
            },
        }
    }

    fn visible_seq(&self) -> ChangeSeq {
        self.snapshot.visible_seq
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
            .row_states()
            .find_map(|state| state.inode_at_seq(inode_id, self.visible_seq()))
        {
            return Ok(Some(inode));
        }
        if let Some(tables) = self.manifest_tables() {
            manifest_index::inode_at_seq(tables, inode_id).await
        } else {
            Ok(None)
        }
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
        let mut bindings = if let Some(tables) = self.manifest_tables() {
            manifest_index::direntry_binds_for_parent(tables, parent_inode_id).await?
        } else {
            Vec::new()
        };
        bindings.extend(self.row_states().flat_map(|state| {
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
        let mut bindings = if let Some(tables) = self.manifest_tables() {
            manifest_index::direntry_binds_for_parent_name(tables, parent_inode_id, name_key)
                .await?
        } else {
            Vec::new()
        };
        bindings.extend(self.row_states().flat_map(|state| {
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
        let mut bindings = if let Some(tables) = self.manifest_tables() {
            manifest_index::direntry_binds_for_child(tables, child_inode_id).await?
        } else {
            Vec::new()
        };
        bindings.extend(self.row_states().flat_map(|state| {
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
        let mut unbinds = if let Some(tables) = self.manifest_tables() {
            manifest_index::direntry_unbinds_for_binding(tables, direntry).await?
        } else {
            Vec::new()
        };
        unbinds.extend(self.row_states().flat_map(|state| {
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
        let mut tombstones = if let Some(tables) = self.manifest_tables() {
            manifest_index::tombstones_for_root(tables, root_inode_id).await?
        } else {
            Vec::new()
        };
        tombstones.extend(self.row_states().flat_map(|state| {
            state
                .subtree_tombstones()
                .iter()
                .filter(move |tombstone| tombstone.root_inode_id == root_inode_id)
                .cloned()
        }));
        Ok(tombstones)
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
}

pub(crate) struct MetadataViewSession<'a, 'store, S: ObjectStore + ?Sized> {
    base: MetadataView<'a, 'store, S>,
    inode_at_seq_cache: HashMap<InodeId, Option<InodeRecord>>,
    visible_inode_cache: HashMap<InodeId, Option<InodeRecord>>,
    bound_child_cache: HashMap<ParentNameCacheKey, Option<DirentryBindRecord>>,
    current_parent_binding_cache: HashMap<InodeId, Option<DirentryBindRecord>>,
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
        let mut manifest_after_row_key = None;
        let mut manifest_exhausted = self.base.manifest_tables().is_none();
        let mut manifest_candidates = VecDeque::<DirentryBindPageCandidate>::new();
        let tail_candidates = self
            .base
            .tail_direntry_bind_page_candidates(parent_inode_id, start_after_name_key);
        let mut tail_index = 0;
        let mut seen_name_keys = BTreeSet::new();
        let mut children = Vec::with_capacity(limit);
        while children.len() < limit {
            if manifest_candidates.is_empty() && !manifest_exhausted {
                self.counters.scan_range_page_calls =
                    self.counters.scan_range_page_calls.saturating_add(1);
                let page = if let Some(tables) = self.base.manifest_tables() {
                    manifest_index::direntry_binds_for_parent_name_key_page(
                        tables,
                        parent_inode_id,
                        start_after_name_key,
                        manifest_after_row_key.as_deref(),
                        raw_scan_limit,
                    )
                    .await?
                } else {
                    Vec::new()
                };
                if page.is_empty() {
                    manifest_exhausted = true;
                } else {
                    manifest_after_row_key = page.last().map(|candidate| candidate.row_key.clone());
                    manifest_candidates.extend(page.into_iter().map(|candidate| {
                        DirentryBindPageCandidate {
                            row_key: candidate.row_key,
                            record: candidate.record,
                        }
                    }));
                }
            }

            let next_manifest = manifest_candidates.front();
            let next_tail = tail_candidates.get(tail_index);
            let take_tail = match (next_manifest, next_tail) {
                (Some(manifest), Some(tail)) => tail.row_key < manifest.row_key,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => break,
            };
            let candidate = if take_tail {
                let candidate = tail_candidates[tail_index].clone();
                tail_index += 1;
                candidate
            } else {
                manifest_candidates
                    .pop_front()
                    .expect("manifest candidate should exist")
            };

            if !seen_name_keys.insert(candidate.record.name_key.clone()) {
                continue;
            }
            let Some(active) = self
                .visible_child(parent_inode_id, &candidate.record.name_key)
                .await?
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
        }
        Ok(children)
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

    pub(crate) async fn latest_revision_head(
        &mut self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>, CoreError> {
        self.counters.latest_revision_calls = self.counters.latest_revision_calls.saturating_add(1);
        if let Some(cached) = self.latest_revision_head_cache.get(&inode_id).cloned() {
            return Ok(cached);
        }
        let revision = self.base.latest_revision_head(inode_id).await?;
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
        self.counters.direntry_child_scan_calls =
            self.counters.direntry_child_scan_calls.saturating_add(1);
        self.counters.scan_prefix_calls = self.counters.scan_prefix_calls.saturating_add(1);
        let bindings = self.base.direntry_binds_for_child(child_inode_id).await?;
        Ok(bindings
            .into_iter()
            .filter(|direntry| direntry.bind_seq <= self.base.visible_seq())
            .max_by_key(|direntry| (direntry.bind_seq, direntry.bind_delta_index)))
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

/// Metadata view session as a cached [`MetadataVisibilityReads`] source.
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
