//! [`MetadataView`]: seq-scoped metadata lookups over manifest segments
//! merged with the WAL tail.

use super::durable_cache::{DurableVisibilityCache, DurableVisibilityCacheInner, SharedRows};
use super::manifest_index;
use super::view_session::{LeafRevisionPreload, MetadataViewSession};
use super::visibility::{self, MetadataVisibilityReads};
use crate::error::{CoreError, Result};
use crate::metadata::{
    AccessRevisionRecord, CommitReceiptRecord, ContentLayoutRecord, DirentryBindingRecord,
    InodeRecord, MetadataState, ResolvedVisiblePath, SubtreeTombstoneRecord,
};
use crate::namespace::state::NamespaceReadState;
use crate::segment::VerifiedMetadataSegments;
use crate::wal::ProjectedWalTail;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::wal::WalCommitPayload;
use loonfs_types::{AbsolutePath, ChangeSeq, CommitId, InodeId, NameKey};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) struct MetadataSourceStack<'a, 'store, S: ObjectStore + ?Sized> {
    overlay: Option<&'a MetadataState>,
    batch_accepted: Option<&'a MetadataState>,
    wal_tail: Option<&'a ProjectedWalTail>,
    manifest: &'a VerifiedMetadataSegments<'store, S>,
    durable_cache: Option<&'a DurableVisibilityCache>,
}

pub(crate) struct MetadataView<'a, 'store, S: ObjectStore + ?Sized> {
    visible_seq: ChangeSeq,
    sources: MetadataSourceStack<'a, 'store, S>,
}

#[async_trait::async_trait]
impl<S: ObjectStore + ?Sized> crate::storage::content_location::LayoutLookup
    for MetadataView<'_, '_, S>
{
    async fn content_layout(
        &self,
        content_id: &loonfs_types::ContentId,
    ) -> Result<Option<ContentLayoutRecord>> {
        self.content_layout(content_id).await
    }

    fn content_has_other_size(&self, content_ref: &loonfs_types::ContentRef) -> bool {
        self.row_states().any(|state| {
            state.revisions().iter().any(|revision| {
                revision.content_ref.content_id == content_ref.content_id
                    && revision.content_ref.size_bytes != content_ref.size_bytes
            })
        })
    }
}

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

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    /// Creates a view of `base`'s rows over `manifest`, for a test that
    /// builds its rows in memory.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn in_memory(
        manifest: &'a VerifiedMetadataSegments<'store, S>,
        base: &'a ProjectedWalTail,
        overlay: Option<&'a MetadataState>,
        visible_seq: ChangeSeq,
    ) -> Self {
        Self {
            visible_seq,
            sources: MetadataSourceStack {
                overlay,
                batch_accepted: None,
                wal_tail: Some(base),
                manifest,
                durable_cache: None,
            },
        }
    }

    pub(crate) fn from_loaded_head(
        head: &'a NamespaceReadState,
        segments: &'a VerifiedMetadataSegments<'store, S>,
        wal_tail: &'a ProjectedWalTail,
    ) -> Self {
        Self {
            visible_seq: head.seq,
            sources: MetadataSourceStack {
                overlay: None,
                batch_accepted: None,
                wal_tail: Some(wal_tail),
                manifest: segments,
                durable_cache: None,
            },
        }
    }

    /// Creates a view containing only one manifest at its materialized sequence.
    ///
    /// Unlike [`Self::from_loaded_head`], this view does not replay the current
    /// WAL tail. It is used for pinned manifests such as checkpoints, so later
    /// commits cannot affect the result.
    pub(crate) fn over_manifest_segments(
        segments: &'a VerifiedMetadataSegments<'store, S>,
        materialized_seq: ChangeSeq,
    ) -> Self {
        Self {
            visible_seq: materialized_seq,
            sources: MetadataSourceStack {
                overlay: None,
                batch_accepted: None,
                wal_tail: None,
                manifest: segments,
                durable_cache: None,
            },
        }
    }

    pub(crate) fn with_overlay<'view>(
        &'view self,
        overlay: Option<&'view MetadataState>,
        batch_accepted: &'view MetadataState,
        visible_seq: ChangeSeq,
    ) -> MetadataView<'view, 'store, S> {
        MetadataView {
            visible_seq,
            sources: MetadataSourceStack {
                overlay,
                batch_accepted: Some(batch_accepted),
                wal_tail: self.sources.wal_tail,
                manifest: self.sources.manifest,
                durable_cache: self.sources.durable_cache,
            },
        }
    }

    pub(super) fn visible_seq(&self) -> ChangeSeq {
        self.visible_seq
    }

    /// The retention floor of the view's manifest.
    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.sources
            .manifest
            .manifest()
            .payload()
            .retention_floor_seq
    }

    /// Attaches a cache for durable lookups performed during one batch.
    ///
    /// Use it only when every derived view has a `visible_seq` at or after the
    /// sequence used to load the durable layers. Overlay rows are still applied
    /// separately for each lookup.
    pub(crate) fn with_durable_cache(
        mut self,
        durable_cache: &'a DurableVisibilityCache,
    ) -> MetadataView<'a, 'store, S> {
        self.sources.durable_cache = Some(durable_cache);
        self
    }

    pub(super) fn row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        self.overlay_states().chain(self.durable_row_states())
    }

    fn overlay_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        [self.sources.overlay, self.sources.batch_accepted]
            .into_iter()
            .flatten()
    }

    fn durable_row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        let visible_seq = self.visible_seq;
        self.sources
            .wal_tail
            .into_iter()
            .flat_map(move |tail| tail.row_states(visible_seq))
    }

    pub(super) fn manifest_segments(&self) -> &'a VerifiedMetadataSegments<'store, S> {
        self.sources.manifest
    }

    /// Returns the rows of one key that a caller needs to find the key's
    /// newest row at the view's visible sequence.
    ///
    /// The manifest contributes every row that `fetch` reads. A row state (the
    /// rows of one WAL tail chunk, of the batch's earlier commits, or of the
    /// commit being validated) contributes only its newest row of the key when
    /// all of its rows are at or below the visible sequence, and `newest_in`
    /// reads that row from the state's index. Any other row state contributes
    /// each row that `matches` selects.
    ///
    /// Without the index, every lookup would read every row of the WAL tail
    /// and of the batch's earlier commits. Both sets of rows grow with the
    /// batch size, and commit validation makes several lookups per commit, so
    /// a batch's validation cost would grow with the square of its size.
    pub(super) async fn shared_rows<T, K>(
        &self,
        select_map: fn(&mut DurableVisibilityCacheInner) -> &mut HashMap<K, Arc<Vec<T>>>,
        key: K,
        fetch: Pin<Box<dyn Future<Output = Result<Vec<T>>> + Send + '_>>,
        rows_of: fn(&MetadataState) -> &[T],
        matches: impl Fn(&T) -> bool,
        newest_in: impl Fn(&MetadataState) -> Option<T>,
    ) -> Result<SharedRows<T>>
    where
        T: Clone,
        K: Clone + Eq + std::hash::Hash,
    {
        let visible_seq = self.visible_seq;
        let state_rows = |state: &MetadataState, rows: &mut Vec<T>| {
            if state.indexed_seq() <= visible_seq {
                rows.extend(newest_in(state));
            } else {
                rows.extend(rows_of(state).iter().filter(|row| matches(row)).cloned());
            }
        };
        let durable = if let Some(cached) = self
            .sources
            .durable_cache
            .and_then(|cache| cache.get(select_map, &key))
        {
            cached
        } else {
            let mut durable = fetch.await?;
            for state in self.durable_row_states() {
                state_rows(state, &mut durable);
            }
            let durable = Arc::new(durable);
            if let Some(cache) = self.sources.durable_cache {
                cache.insert(select_map, key, Arc::clone(&durable));
            }
            durable
        };
        let mut overlay = Vec::new();
        for state in self.overlay_states() {
            state_rows(state, &mut overlay);
        }
        Ok(SharedRows { durable, overlay })
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    /// Adapts this view to [`MetadataVisibilityReads`] so composite visibility
    /// rules remain centralized in [`super::visibility`].
    ///
    /// `MetadataView` is `Copy`, so the adapter owns a copy instead of borrowing
    /// `self`.
    pub(crate) fn reads(&self) -> MetadataViewReads<'a, 'store, S> {
        MetadataViewReads { view: *self }
    }

    /// Opens a fresh session over this view; the view is `Copy`, so the
    /// session owns its copy.
    pub(crate) fn session(self) -> MetadataViewSession<'a, 'store, S> {
        MetadataViewSession::new(self)
    }

    /// Returns whether the directory has at least one visible child.
    ///
    /// The lookup requests a single-entry page, so its work does not grow with
    /// the directory's total size.
    pub(crate) async fn has_visible_children(&self, parent_inode_id: InodeId) -> Result<bool> {
        let mut session = self.session();
        Ok(!session
            .visible_children_page_by_name_key(parent_inode_id, None, 1)
            .await?
            .is_empty())
    }

    pub(crate) async fn resolve_visible_path(
        &self,
        absolute_path: &AbsolutePath,
    ) -> Result<ResolvedVisiblePath> {
        self.session()
            .resolve_visible_path(absolute_path, LeafRevisionPreload::Skip)
            .await
    }

    pub(crate) async fn visible_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>> {
        visibility::visible_child(&mut self.reads(), parent_inode_id, name_key).await
    }

    pub(crate) async fn visible_inode(&self, inode_id: InodeId) -> Result<Option<InodeRecord>> {
        visibility::visible_inode(&mut self.reads(), inode_id).await
    }

    pub(crate) async fn inode_at_seq(&self, inode_id: InodeId) -> Result<Option<InodeRecord>> {
        if let Some(inode) = self
            .overlay_states()
            .find_map(|state| state.inode_at_seq(inode_id, self.visible_seq()))
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
            durable = manifest_index::inode_at_seq(self.manifest_segments(), inode_id).await?;
        }
        if let Some(cache) = self.sources.durable_cache {
            cache.insert(|inner| &mut inner.inodes, inode_id, durable.clone());
        }
        Ok(durable)
    }

    pub(crate) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>> {
        // Each row state looks up the commit in its receipt index (newest per commit
        // id) instead of a scan over every receipt row; the seq guard stays
        // as written even though composed states never contain rows past the
        // visible seq.
        let row_receipt = self
            .row_states()
            .filter_map(|state| state.find_commit_receipt(commit_id))
            .filter(|receipt| receipt.committed_seq <= self.visible_seq())
            .max_by_key(|receipt| receipt.committed_seq)
            .cloned();
        Ok(newest(
            row_receipt,
            manifest_index::commit_receipt(self.manifest_segments(), commit_id).await?,
            |receipt| receipt.committed_seq,
        ))
    }

    pub(crate) async fn commit_at_seq(&self, seq: ChangeSeq) -> Result<Option<WalCommitPayload>> {
        if seq > self.visible_seq() {
            return Ok(None);
        }
        if let Some(record) = self
            .row_states()
            .flat_map(MetadataState::commits)
            .find(|record| record.committed_seq == seq)
        {
            return Ok(Some(record.clone()));
        }
        manifest_index::commit_at_seq(self.manifest_segments(), seq).await
    }

    pub(crate) async fn commits_after(
        &self,
        after_seq: ChangeSeq,
        limit: usize,
    ) -> Result<Vec<WalCommitPayload>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let visible_seq = self.visible_seq();
        let mut page: Vec<WalCommitPayload> =
            manifest_index::commits_after_page(self.manifest_segments(), after_seq, limit)
                .await?
                .into_iter()
                .filter(|record| record.committed_seq <= visible_seq)
                .collect();
        let remaining = limit.saturating_sub(page.len());
        if remaining == 0 {
            return Ok(page);
        }
        let mut tail: Vec<&WalCommitPayload> = self
            .row_states()
            .flat_map(MetadataState::commits)
            .filter(|record| {
                after_seq < record.committed_seq && record.committed_seq <= visible_seq
            })
            .collect();
        tail.sort_by_key(|record| record.committed_seq);
        // Every manifest row is at or below the basis head and every tail or overlay row is
        // above it, so no sort across the two sides is needed.
        page.extend(tail.into_iter().take(remaining).cloned());
        Ok(page)
    }
}

/// [`MetadataView`] as a [`MetadataVisibilityReads`] source: it serves only
/// the primitive lookups (over the manifest segments merged with the row-state
/// tail) and takes every composite rule from the provided trait methods, so
/// the object-store-backed view determines visibility through the exact same
/// bodies as the in-memory state.
pub(crate) struct MetadataViewReads<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
}

impl<S: ObjectStore + ?Sized> MetadataVisibilityReads for MetadataViewReads<'_, '_, S> {
    type Error = CoreError;

    fn retention_floor_seq(&self) -> ChangeSeq {
        self.view.retention_floor_seq()
    }

    async fn find_inode(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<InodeRecord>, Self::Error> {
        self.view.inode_at_seq(inode_id).await
    }

    async fn find_latest_slot_binding(
        &mut self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> std::result::Result<Option<DirentryBindingRecord>, Self::Error> {
        self.view.bound_child(parent_inode_id, name_key).await
    }

    async fn find_latest_parent_binding_for_child(
        &mut self,
        child_inode_id: InodeId,
    ) -> std::result::Result<Option<DirentryBindingRecord>, Self::Error> {
        self.view
            .latest_parent_binding_for_child(child_inode_id)
            .await
    }

    async fn find_hiding_subtree_tombstone(
        &mut self,
        root_inode_id: InodeId,
    ) -> std::result::Result<Option<SubtreeTombstoneRecord>, Self::Error> {
        self.view.hiding_subtree_tombstone(root_inode_id).await
    }

    async fn find_access_row(
        &mut self,
        inode_id: InodeId,
    ) -> std::result::Result<Option<AccessRevisionRecord>, Self::Error> {
        self.view.latest_access_revision(inode_id).await
    }
}

pub(super) fn latest_visible_binding<'a>(
    rows: impl Iterator<Item = &'a DirentryBindingRecord>,
    visible_seq: ChangeSeq,
) -> Option<DirentryBindingRecord> {
    rows.filter(|direntry| direntry.committed_seq <= visible_seq)
        .max_by_key(|direntry| direntry.position())
        .cloned()
}

/// Returns the newer of a row-state row and a manifest row by `key`.
pub(crate) fn newest<T, K: Ord>(
    row: Option<T>,
    manifest: Option<T>,
    key: impl Fn(&T) -> K,
) -> Option<T> {
    row.into_iter().chain(manifest).max_by_key(key)
}
