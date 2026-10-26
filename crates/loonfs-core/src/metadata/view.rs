//! [`MetadataView`]: seq-scoped metadata lookups over manifest segments
//! merged with the WAL tail, plus the caching session wide reads use.

use super::durable_cache::{
    DurableVisibilityCache, DurableVisibilityCacheInner, ParentNameCacheKey, SharedRows,
};
use super::manifest_index;
use super::view_session::{latest_visible_binding, LeafRevisionPreload};
use super::visibility::{self, MetadataVisibilityReads};
use crate::error::{CoreError, Result};
use crate::manifest::VerifiedMetadataSegments;
use crate::metadata::{
    active_deletion_from_tombstone, recoverable_deletion_from_active_record, AccessRevisionRecord,
    ActiveDeletionRange, ActiveDeletionRecord, AttributesRevisionRecord, CommitReceiptRecord,
    ContentLayoutRecord, DirentryBindingRecord, InodeRecord, MetadataState, RecoverableDeletion,
    ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord,
};
use crate::namespace::state::NamespaceReadState;
use crate::wal::ProjectedWalTail;
#[cfg(test)]
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys;
use loonfs_types::format::wal::WalCommitPayload;
use loonfs_types::{
    AbsolutePath, AccessRevisionNo, Attributes, AttributesRevisionNo, ChangeSeq, CommitId, InodeId,
    InodeKind, NameKey, NamespaceNaming, RevisionNo, ROOT_INODE_ID,
};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The fewest binding rows that one directory scan reads per manifest
/// round-trip.
pub(crate) const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

/// Rows one trash page fetches per manifest round-trip. A page's entries are
/// listed rows; undelete markers share the range until compaction removes
/// each pair away, so the raw scan runs a little ahead of the page.
const ACTIVE_DELETION_RAW_SCAN_LIMIT: usize = 64;

/// The manifest half of the active-deletion merge: a key-ordered cursor that
/// refills from range scans and reports when the range is spent.
struct ActiveDeletionScan {
    lower_bound: String,
    exhausted: bool,
    buffered: VecDeque<(String, ActiveDeletionRecord)>,
}

impl ActiveDeletionScan {
    fn new(lower_bound: String, exhausted: bool) -> Self {
        Self {
            lower_bound,
            exhausted,
            buffered: VecDeque::new(),
        }
    }

    /// Takes one fetched page. A short page is the end of the range; a full
    /// one leaves the cursor just past its last row.
    fn absorb(&mut self, page: Vec<(String, ActiveDeletionRecord)>, requested: usize) {
        if page.len() < requested {
            self.exhausted = true;
        } else if let Some((last_row_key, _)) = page.last() {
            self.lower_bound = lookup_keys::after_row_key(last_row_key);
        }
        self.buffered.extend(page);
    }
}

pub(crate) struct MetadataSourceStack<'a, 'store, S: ObjectStore + ?Sized> {
    overlay: Option<&'a MetadataState>,
    batch_accepted: Option<&'a MetadataState>,
    wal_tail: Option<&'a ProjectedWalTail>,
    manifest: Option<&'a VerifiedMetadataSegments<'store, S>>,
    durable_cache: Option<&'a DurableVisibilityCache>,
}

pub(crate) struct MetadataView<'a, 'store, S: ObjectStore + ?Sized> {
    visible_seq: ChangeSeq,
    naming: NamespaceNaming,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttributesProjection {
    pub(crate) revision_no: AttributesRevisionNo,
    pub(crate) attributes: Attributes,
    pub(crate) updated_by: Option<loonfs_types::ActorId>,
    pub(crate) updated_at_ms: Option<u64>,
}

#[cfg(test)]
pub(crate) type InMemoryMetadataView<'a> = MetadataView<'a, 'a, LocalFsStore>;

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

#[cfg(test)]
impl<'a> InMemoryMetadataView<'a> {
    pub(crate) fn in_memory(
        base: &'a ProjectedWalTail,
        overlay: Option<&'a MetadataState>,
        visible_seq: ChangeSeq,
    ) -> Self {
        Self {
            visible_seq,
            naming: NamespaceNaming::CaseInsensitive,
            sources: MetadataSourceStack {
                overlay,
                batch_accepted: None,
                wal_tail: Some(base),
                manifest: None,
                durable_cache: None,
            },
        }
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    pub(crate) fn from_loaded_head(
        head: &'a NamespaceReadState,
        segments: &'a VerifiedMetadataSegments<'store, S>,
        wal_tail: &'a ProjectedWalTail,
    ) -> Self {
        Self {
            visible_seq: head.seq,
            naming: head.naming,
            sources: MetadataSourceStack {
                overlay: None,
                batch_accepted: None,
                wal_tail: Some(wal_tail),
                manifest: Some(segments),
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
            naming: segments.manifest().payload().naming,
            sources: MetadataSourceStack {
                overlay: None,
                batch_accepted: None,
                wal_tail: None,
                manifest: Some(segments),
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
            naming: self.naming,
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

    /// How the namespace compares sibling names.
    pub(crate) fn naming(&self) -> NamespaceNaming {
        self.naming
    }

    /// The retention floor of the view's manifest. A view without a
    /// manifest, which only tests build, reads every row it has as at or
    /// below its floor.
    pub(crate) fn retention_floor_seq(&self) -> ChangeSeq {
        self.sources.manifest.map_or(self.visible_seq, |segments| {
            segments.manifest().payload().retention_floor_seq
        })
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

    pub(super) fn manifest_segments(&self) -> Option<&'a VerifiedMetadataSegments<'store, S>> {
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
    async fn shared_rows<T, K>(
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
            if let Some(segments) = self.manifest_segments() {
                durable = manifest_index::inode_at_seq(segments, inode_id).await?;
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
    ) -> Result<Option<RevisionRecord>> {
        if self.visible_inode(inode_id).await?.is_none() {
            return Ok(None);
        }
        self.latest_revision_record(inode_id).await
    }

    pub(crate) async fn latest_revision_record(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<RevisionRecord>> {
        let row_revision = self.row_latest_revision_by_inode(inode_id);
        let manifest_revision = if let Some(segments) = self.manifest_segments() {
            manifest_index::latest_revision_by_inode(segments, inode_id).await?
        } else {
            None
        };
        Ok(row_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
    }

    pub(crate) async fn revision_by_inode(
        &self,
        inode_id: InodeId,
        revision_no: RevisionNo,
    ) -> Result<RevisionRecord> {
        let inode = self
            .inode_at_seq(inode_id)
            .await?
            .ok_or(CoreError::InodeNotFound(inode_id))?;
        if inode.inode_kind != InodeKind::File {
            return Err(CoreError::ExpectedFile {
                target: inode_id.to_string(),
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
    ) -> Result<Option<RevisionRecord>> {
        let row_revision = self.row_revision_by_inode_no(inode_id, revision_no);
        let manifest_revision = if let Some(segments) = self.manifest_segments() {
            manifest_index::revision_by_inode_no(segments, inode_id, revision_no).await?
        } else {
            None
        };
        Ok(row_revision
            .into_iter()
            .chain(manifest_revision)
            .max_by_key(revision_order_key))
    }

    pub(crate) async fn revisions_by_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut revisions = if let Some(segments) = self.manifest_segments() {
            manifest_index::revisions_by_inode_page_desc(segments, inode_id, start_after, limit)
                .await?
        } else {
            Vec::new()
        };
        revisions.extend(self.row_revisions_by_inode_page_desc(inode_id, start_after));
        revisions.retain(|revision| revision.committed_seq <= self.visible_seq());
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions.truncate(limit);
        Ok(revisions)
    }

    /// The newest attribute revision record for `inode_id` at the visible
    /// sequence, or `None` when the inode has never had attributes written.
    ///
    /// Both legs filter by `visible_seq`. The revision lookup this is modeled
    /// on filters only its row-state leg, because a file revision row is never
    /// rewritten and a revision read at the head is the same answer at any
    /// sequence at or above it. Attributes are not history: each record
    /// replaces the one before it, so a checkpoint-basis or fork-basis read
    /// that let a newer manifest row through would answer with a map that
    /// sequence never saw.
    pub(crate) async fn latest_attributes_revision(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<AttributesRevisionRecord>> {
        let row_record = self
            .row_states()
            .flat_map(|state| state.attributes_revisions())
            .filter(|record| {
                record.inode_id == inode_id && record.committed_seq <= self.visible_seq()
            })
            .max_by_key(|record| attributes_order_key(record))
            .cloned();
        let manifest_record = if let Some(segments) = self.manifest_segments() {
            manifest_index::attributes_by_inode(segments, inode_id, self.visible_seq()).await?
        } else {
            None
        };
        Ok(row_record
            .into_iter()
            .chain(manifest_record)
            .max_by_key(attributes_order_key))
    }

    /// Access rows are whole state, not history, so both sources are filtered at the visible sequence.
    pub(crate) async fn latest_access_revision(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<AccessRevisionRecord>> {
        let row_record = self
            .row_states()
            .flat_map(|state| state.access_revisions())
            .filter(|record| {
                record.inode_id == inode_id && record.committed_seq <= self.visible_seq()
            })
            .max_by_key(|record| access_order_key(record))
            .cloned();
        let manifest_record = if let Some(segments) = self.manifest_segments() {
            manifest_index::access_by_inode(segments, inode_id, self.visible_seq()).await?
        } else {
            None
        };
        Ok(row_record
            .into_iter()
            .chain(manifest_record)
            .max_by_key(access_order_key))
    }

    /// The inode's attribute state at the visible sequence: the revision
    /// counter and the complete map.
    ///
    /// An inode with no record anywhere is at revision 0 with an empty map.
    /// That state is concrete, not a missing answer, so nothing writes a
    /// durable row to represent it.
    pub(crate) async fn attributes_at_visible_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<(AttributesRevisionNo, Attributes)> {
        Ok(self
            .latest_attributes_revision(inode_id)
            .await?
            .map(|record| (record.attributes_revision_no, record.attributes))
            .unwrap_or_else(|| (AttributesRevisionNo(0), Attributes::default())))
    }

    /// Returns the inode's attributes with the actor and timestamp from the
    /// latest stored revision. Revision 0 returns `None` for both values.
    pub(crate) async fn attributes_projection_at_visible_seq(
        &self,
        inode_id: InodeId,
    ) -> Result<AttributesProjection> {
        Ok(self
            .latest_attributes_revision(inode_id)
            .await?
            .map(|record| AttributesProjection {
                revision_no: record.attributes_revision_no,
                attributes: record.attributes,
                updated_by: Some(record.committed_by),
                updated_at_ms: Some(record.committed_at_ms),
            })
            .unwrap_or_else(|| AttributesProjection {
                revision_no: AttributesRevisionNo(0),
                attributes: Attributes::default(),
                updated_by: None,
                updated_at_ms: None,
            }))
    }

    /// Returns the largest visible chain size so a shorter restore or copy cannot
    /// shrink the head and allow conflicting appends.
    pub(crate) async fn content_head(
        &self,
        content_id: &loonfs_types::ContentId,
    ) -> Result<Option<u64>> {
        let tail_size = self
            .row_states()
            .flat_map(|state| state.revisions())
            .filter(|row| {
                &row.content_ref.content_id == content_id && row.committed_seq <= self.visible_seq()
            })
            .map(|row| row.content_ref.size_bytes)
            .max();
        let layout_size = self
            .content_layout(content_id)
            .await?
            .map(|row| row.size_bytes);
        Ok(tail_size.into_iter().chain(layout_size).max())
    }

    pub(crate) async fn content_published(
        &self,
        content_id: &loonfs_types::ContentId,
    ) -> Result<bool> {
        if self.row_states().any(|state| {
            state.revisions().iter().any(|row| {
                &row.content_ref.content_id == content_id && row.committed_seq <= self.visible_seq()
            })
        }) {
            return Ok(true);
        }
        Ok(self.content_layout(content_id).await?.is_some())
    }

    pub(crate) async fn content_layout(
        &self,
        content_id: &loonfs_types::ContentId,
    ) -> Result<Option<ContentLayoutRecord>> {
        let tail = self
            .row_states()
            .flat_map(|state| state.content_layouts())
            .filter(|row| &row.content_id == content_id && row.committed_seq <= self.visible_seq())
            .max_by_key(|row| (row.size_bytes, row.committed_seq));
        let folded = match self.manifest_segments() {
            Some(segments) => manifest_index::content_layout(segments, content_id).await?,
            None => None,
        };
        Ok(tail
            .cloned()
            .into_iter()
            .chain(folded)
            .max_by_key(|row| (row.size_bytes, row.committed_seq)))
    }

    pub(crate) async fn find_commit_receipt(
        &self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitReceiptRecord>> {
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
        let manifest_receipt = if let Some(segments) = self.manifest_segments() {
            manifest_index::commit_receipt(segments, commit_id).await?
        } else {
            None
        };
        Ok(row_receipt
            .into_iter()
            .chain(manifest_receipt)
            .max_by_key(|receipt| receipt.committed_seq))
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
        match self.manifest_segments() {
            Some(segments) => manifest_index::commit_at_seq(segments, seq).await,
            None => Ok(None),
        }
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
        let mut page: Vec<WalCommitPayload> = if let Some(segments) = self.manifest_segments() {
            manifest_index::commits_after_page(segments, after_seq, limit)
                .await?
                .into_iter()
                .filter(|record| record.committed_seq <= visible_seq)
                .collect()
        } else {
            Vec::new()
        };
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

    pub(crate) async fn current_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>> {
        visibility::current_parent_binding_for_child(&mut self.reads(), child_inode_id).await
    }

    pub(super) async fn latest_parent_binding_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<Option<DirentryBindingRecord>> {
        if child_inode_id == ROOT_INODE_ID {
            return Ok(None);
        }
        let bindings = self.direntry_binds_for_child(child_inode_id).await?;
        let latest = latest_visible_binding(bindings.iter(), self.visible_seq());
        Ok(latest)
    }

    pub(crate) async fn covering_subtree_tombstone(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        visibility::covering_subtree_tombstone(&mut self.reads(), inode_id).await
    }

    pub(crate) async fn would_create_directory_cycle(
        &self,
        inode_id: InodeId,
        new_parent_inode_id: InodeId,
    ) -> Result<bool> {
        visibility::would_create_directory_cycle(&mut self.reads(), inode_id, new_parent_inode_id)
            .await
    }

    pub(crate) async fn bound_child(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<Option<DirentryBindingRecord>> {
        let bindings = self
            .direntry_binds_for_parent_name(parent_inode_id, name_key)
            .await?;
        let latest = latest_visible_binding(bindings.iter(), self.visible_seq());
        Ok(latest)
    }

    /// The root's latest tombstone event, when it hides the subtree.
    pub(crate) async fn hiding_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        let tombstones = self.tombstones_for_root(root_inode_id).await?;
        let hiding = super::rows::hiding_tombstone_from_records(
            tombstones.iter().cloned(),
            self.visible_seq(),
        );
        Ok(hiding)
    }

    /// The `set` event of the root's recoverable deletion: its latest
    /// tombstone event, when that event is a `set`.
    pub(crate) async fn recoverable_deletion(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        let tombstones = self.tombstones_for_root(root_inode_id).await?;
        let hiding = super::rows::hiding_tombstone_from_records(
            tombstones.iter().cloned(),
            self.visible_seq(),
        );
        Ok(hiding.filter(|tombstone| tombstone.action.is_recoverable()))
    }

    pub(super) async fn direntry_binds_for_parent_name(
        &self,
        parent_inode_id: InodeId,
        name_key: &NameKey,
    ) -> Result<SharedRows<DirentryBindingRecord>> {
        let cache_key = ParentNameCacheKey {
            parent_inode_id,
            name_key: name_key.clone(),
        };
        let slot = (parent_inode_id, name_key.clone());
        self.shared_rows(
            |inner| &mut inner.binds_for_parent_name,
            cache_key,
            Box::pin(async {
                if let Some(segments) = self.manifest_segments() {
                    manifest_index::direntry_binds_for_parent_name(
                        segments,
                        parent_inode_id,
                        name_key,
                        self.visible_seq(),
                    )
                    .await
                } else {
                    Ok(Vec::new())
                }
            }),
            MetadataState::direntry_binds,
            |direntry| {
                direntry.parent_inode_id == parent_inode_id && direntry.name_key == *name_key
            },
            |state| state.indexes.latest_bind_in_slot(&slot),
        )
        .await
    }

    pub(super) async fn direntry_binds_for_child(
        &self,
        child_inode_id: InodeId,
    ) -> Result<SharedRows<DirentryBindingRecord>> {
        self.shared_rows(
            |inner| &mut inner.binds_for_child,
            child_inode_id,
            Box::pin(async {
                if let Some(segments) = self.manifest_segments() {
                    manifest_index::direntry_binds_for_child(
                        segments,
                        child_inode_id,
                        self.visible_seq(),
                    )
                    .await
                } else {
                    Ok(Vec::new())
                }
            }),
            MetadataState::direntry_binds,
            |direntry| direntry.child_inode_id == child_inode_id,
            |state| state.indexes.latest_binding_for_child(child_inode_id),
        )
        .await
    }

    fn row_latest_revision_by_inode(&self, inode_id: InodeId) -> Option<RevisionRecord> {
        self.row_states()
            .flat_map(|state| state.revisions())
            .filter(|revision| {
                revision.inode_id == inode_id && revision.committed_seq <= self.visible_seq()
            })
            .max_by_key(|revision| revision_order_key(revision))
            .cloned()
    }

    fn row_revision_by_inode_no(
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

    fn row_revisions_by_inode_page_desc(
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
                        .is_none_or(|position| revision_is_after_position_desc(revision, position))
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions
    }

    /// One page of the namespace's recoverable deletions in `range`, oldest
    /// deletion first.
    ///
    /// Manifest rows merge with the rows the WAL tail derives, so a deletion
    /// committed seconds ago lists like a fresh file appears in `ls`. Both
    /// sides arrive in row-key order and a removal marker sorts ahead of the
    /// row it removes, so one ascending walk decides the page: a marker hides
    /// the position whose key it repeats, and every other listed row is an
    /// entry. Reads stop as soon as `limit` entries are in hand.
    pub(super) async fn active_deletions_page(
        &self,
        range: &ActiveDeletionRange,
        limit: usize,
    ) -> Result<Vec<RecoverableDeletion>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let visible_seq = self.visible_seq();

        let mut tail: Vec<(String, ActiveDeletionRecord)> = Vec::new();
        for tombstone in self
            .row_states()
            .flat_map(|state| state.subtree_tombstones())
            .filter(|tombstone| tombstone.committed_seq <= visible_seq)
        {
            let inode = self
                .inode_at_seq(tombstone.root_inode_id)
                .await?
                .ok_or_else(|| {
                    CoreError::NamespaceCorrupt(format!(
                        "deletion root inode `{}` is missing",
                        tombstone.root_inode_id
                    ))
                })?;
            let record = active_deletion_from_tombstone(tombstone, inode.inode_kind);
            let row_key = record.row_key();
            if range.contains(&row_key) {
                tail.push((row_key, record));
            }
        }
        tail.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut durable = ActiveDeletionScan::new(
            range.lower_bound.clone(),
            self.manifest_segments().is_none(),
        );
        let mut tail_index = 0usize;
        let mut entries = Vec::with_capacity(limit);
        let mut removed_position: Option<(ChangeSeq, InodeId)> = None;
        let mut last_row_key: Option<String> = None;
        while entries.len() < limit {
            if durable.buffered.is_empty() && !durable.exhausted {
                let raw_limit = limit.max(ACTIVE_DELETION_RAW_SCAN_LIMIT);
                let segments = self
                    .manifest_segments()
                    .expect("a view without manifest segments should start its scan exhausted");
                let page = manifest_index::active_deletions_page(
                    segments,
                    &durable.lower_bound,
                    range.upper_bound.as_deref(),
                    raw_limit,
                )
                .await?;
                durable.absorb(page, raw_limit);
                continue;
            }
            let take_tail = match (durable.buffered.front(), tail.get(tail_index)) {
                (Some((durable_key, _)), Some((tail_key, _))) => tail_key <= durable_key,
                (None, Some(_)) => true,
                (Some(_), None) => false,
                (None, None) => break,
            };
            let (row_key, record) = if take_tail {
                let row = tail[tail_index].clone();
                tail_index += 1;
                row
            } else {
                durable
                    .buffered
                    .pop_front()
                    .expect("a non-empty buffer should yield a row")
            };
            // A row present in both the manifest and the replayed tail is one
            // deletion, not two.
            if last_row_key.as_deref() == Some(row_key.as_str()) {
                continue;
            }
            last_row_key = Some(row_key);
            let position = (record.deletion_seq, record.root_inode_id);
            match recoverable_deletion_from_active_record(record) {
                None => removed_position = Some(position),
                Some(deletion) if removed_position != Some(position) => entries.push(deletion),
                Some(_) => {}
            }
        }
        Ok(entries)
    }

    pub(super) async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<SharedRows<SubtreeTombstoneRecord>> {
        self.shared_rows(
            |inner| &mut inner.tombstones_for_root,
            root_inode_id,
            Box::pin(async {
                if let Some(segments) = self.manifest_segments() {
                    manifest_index::tombstones_for_root(segments, root_inode_id).await
                } else {
                    Ok(Vec::new())
                }
            }),
            MetadataState::subtree_tombstones,
            |tombstone| tombstone.root_inode_id == root_inode_id,
            |state| state.indexes.latest_tombstone(root_inode_id),
        )
        .await
    }
}

/// [`MetadataView`] as a [`MetadataVisibilityReads`] source: it answers only
/// the primitive lookups (over the manifest segments merged with the row-state
/// tail) and takes every composite rule from the provided trait methods, so
/// the object-store-backed view decides visibility through the exact same
/// bodies as the in-memory state.
pub(crate) struct MetadataViewReads<'a, 'store, S: ObjectStore + ?Sized> {
    view: MetadataView<'a, 'store, S>,
}

impl<S: ObjectStore + ?Sized> MetadataVisibilityReads for MetadataViewReads<'_, '_, S> {
    type Error = CoreError;

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

fn attributes_order_key(
    record: &AttributesRevisionRecord,
) -> (AttributesRevisionNo, ChangeSeq, u32) {
    (
        record.attributes_revision_no,
        record.committed_seq,
        record.delta_index,
    )
}

fn access_order_key(record: &AccessRevisionRecord) -> (AccessRevisionNo, ChangeSeq, u32) {
    (
        record.access_revision_no,
        record.committed_seq,
        record.delta_index,
    )
}

fn revision_order_key(record: &RevisionRecord) -> (RevisionNo, loonfs_types::ChangeSeq, u32) {
    (record.revision_no, record.committed_seq, record.delta_index)
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
