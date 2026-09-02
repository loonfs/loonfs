//! [`MetadataView`]: seq-scoped metadata lookups over manifest segments
//! merged with the WAL tail, plus the caching session wide reads use.

use super::durable_cache::{
    BindingCacheKey, DurableVisibilityCache, ParentNameCacheKey, SharedRows,
};
use super::manifest_index;
use super::view_session::LeafRevisionPrefetch;
use super::visibility::{self, MetadataVisibilityReads};
use crate::checkpoint::VerifiedMetadataSegments;
use crate::error::CoreError;
use crate::metadata::{
    active_deletion_from_tombstone, recoverable_deletion_from_active_record,
    unbind_matches_binding, ActiveDeletionRecord, AttributesRevisionRecord, CommitReceiptRecord,
    DirentryBindRecord, DirentryUnbindRecord, InodeRecord, MetadataState, RecoverableDeletion,
    ResolvedVisiblePath, RevisionRecord, SubtreeTombstoneRecord,
};
use loonfs_api::wire::control::HeadState;
use loonfs_api::wire::manifest::lookup_keys;
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{
    AbsolutePath, AttributeRevisionNo, Attributes, ChangeSeq, CommitId, InodeId, InodeKind,
    NameKey, RevisionNo,
};
#[cfg(test)]
use loonfs_objectstore::local_fs_store::LocalFsStore;
use loonfs_objectstore::ObjectStore;
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) const DIRECTORY_PAGE_RAW_SCAN_LIMIT: usize = 64;

/// Rows one trash page fetches per manifest round-trip. A page's entries are
/// listed rows; undelete markers share the range until reorganization folds
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
            self.lower_bound = format!("{last_row_key}\0");
        }
        self.buffered.extend(page);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MetadataSnapshot {
    visible_seq: ChangeSeq,
}

pub(crate) struct MetadataSourceStack<'a, 'store, S: ObjectStore + ?Sized> {
    overlay: Option<&'a MetadataState>,
    batch_accepted: Option<&'a MetadataState>,
    wal_tail: Option<&'a MetadataState>,
    manifest: Option<&'a VerifiedMetadataSegments<'store, S>>,
    durable_cache: Option<&'a DurableVisibilityCache>,
}

pub(crate) struct MetadataView<'a, 'store, S: ObjectStore + ?Sized> {
    snapshot: MetadataSnapshot,
    sources: MetadataSourceStack<'a, 'store, S>,
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
        base: &'a MetadataState,
        overlay: Option<&'a MetadataState>,
        visible_seq: ChangeSeq,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot { visible_seq },
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
        head: &'a HeadState,
        segments: &'a VerifiedMetadataSegments<'store, S>,
        wal_tail_rows: &'a MetadataState,
    ) -> Self {
        Self {
            snapshot: MetadataSnapshot {
                visible_seq: head.seq,
            },
            sources: MetadataSourceStack {
                overlay: None,
                batch_accepted: None,
                wal_tail: Some(wal_tail_rows),
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
            snapshot: MetadataSnapshot {
                visible_seq: materialized_seq,
            },
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
        overlay: &'view MetadataState,
        batch_accepted: &'view MetadataState,
        visible_seq: ChangeSeq,
    ) -> MetadataView<'view, 'store, S> {
        MetadataView {
            snapshot: MetadataSnapshot { visible_seq },
            sources: MetadataSourceStack {
                overlay: Some(overlay),
                batch_accepted: Some(batch_accepted),
                wal_tail: self.sources.wal_tail,
                manifest: self.sources.manifest,
                durable_cache: self.sources.durable_cache,
            },
        }
    }

    pub(super) fn visible_seq(&self) -> ChangeSeq {
        self.snapshot.visible_seq
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
        [
            self.sources.overlay,
            self.sources.batch_accepted,
            self.sources.wal_tail,
        ]
        .into_iter()
        .flatten()
    }

    fn overlay_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        [self.sources.overlay, self.sources.batch_accepted]
            .into_iter()
            .flatten()
    }

    fn durable_row_states(&self) -> impl Iterator<Item = &'a MetadataState> + '_ {
        self.sources.wal_tail.into_iter()
    }

    pub(super) fn manifest_segments(&self) -> Option<&'a VerifiedMetadataSegments<'store, S>> {
        self.sources.manifest
    }
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
    /// Adapts this view to [`MetadataVisibilityReads`] so composite visibility
    /// rules remain centralized in [`super::visibility`].
    ///
    /// `MetadataView` is `Copy`, so the adapter owns a copy instead of borrowing
    /// `self`.
    fn reads(&self) -> MetadataViewReads<'a, 'store, S> {
        MetadataViewReads { view: *self }
    }

    /// Returns whether the directory has at least one visible child.
    ///
    /// The lookup requests a single-entry page, so its work does not grow with
    /// the directory's total size.
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
        self.session()
            .resolve_visible_path(absolute_path, LeafRevisionPrefetch::Skip)
            .await
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
        let manifest_revision = if let Some(segments) = self.manifest_segments() {
            manifest_index::latest_revision_for_inode(segments, inode_id).await?
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
    ) -> Result<Option<RevisionRecord>, CoreError> {
        let row_revision = self.row_revision_for_inode_no(inode_id, revision_no);
        let manifest_revision = if let Some(segments) = self.manifest_segments() {
            manifest_index::revision_for_inode_no(segments, inode_id, revision_no).await?
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
        let mut revisions = if let Some(segments) = self.manifest_segments() {
            manifest_index::revisions_for_inode_page_desc(segments, inode_id, start_after, limit)
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

    /// The newest attribute revision record for `inode_id` at the visible
    /// sequence, or `None` when the inode has never had attributes written.
    ///
    /// Both legs filter by `visible_seq`. The revision lookup this is modeled
    /// on filters only its row-state leg, because a file's revision history
    /// is append-only and a revision read at the head is the same answer at
    /// any sequence at or above it. Attributes are not history: each record
    /// replaces the one before it, so a checkpoint-basis or fork-basis read
    /// that let a newer manifest row through would answer with a map that
    /// sequence never saw.
    pub(crate) async fn latest_attributes_revision(
        &self,
        inode_id: InodeId,
    ) -> Result<Option<AttributesRevisionRecord>, CoreError> {
        let row_record = self
            .row_states()
            .flat_map(|state| state.attributes_revisions())
            .filter(|record| {
                record.inode_id == inode_id && record.committed_seq <= self.visible_seq()
            })
            .max_by_key(|record| attributes_order_key(record))
            .cloned();
        let manifest_record = if let Some(segments) = self.manifest_segments() {
            manifest_index::attributes_for_inode(segments, inode_id, self.visible_seq()).await?
        } else {
            None
        };
        Ok(row_record
            .into_iter()
            .chain(manifest_record)
            .max_by_key(attributes_order_key))
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
    ) -> Result<(AttributeRevisionNo, Attributes), CoreError> {
        Ok(self
            .latest_attributes_revision(inode_id)
            .await?
            .map(|record| (record.attributes_revision_no, record.attributes))
            .unwrap_or_else(|| (AttributeRevisionNo(0), Attributes::default())))
    }

    /// Returns the inode's attributes with the actor and timestamp from the
    /// latest stored revision. Revision 0 returns `None` for both values.
    pub(crate) async fn attributes_projection_at_visible_seq(
        &self,
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
        Ok(self
            .latest_attributes_revision(inode_id)
            .await?
            .map(|record| {
                (
                    record.attributes_revision_no,
                    record.attributes,
                    Some(record.updated_by),
                    Some(record.updated_at_ms),
                )
            })
            .unwrap_or_else(|| (AttributeRevisionNo(0), Attributes::default(), None, None)))
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
            let mut durable = if let Some(segments) = self.manifest_segments() {
                manifest_index::direntry_binds_for_parent_name(segments, parent_inode_id, name_key)
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
            .overlay_states()
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
            let mut durable = if let Some(segments) = self.manifest_segments() {
                manifest_index::direntry_binds_for_child(segments, child_inode_id).await?
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
            .overlay_states()
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
            let mut durable = if let Some(segments) = self.manifest_segments() {
                manifest_index::direntry_unbinds_for_binding(segments, direntry).await?
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
            .overlay_states()
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
                        .is_none_or(|position| revision_is_after_position_desc(revision, position))
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions
    }

    /// One page of the namespace's recoverable deletions, oldest deletion
    /// first, resuming strictly after `start_after`.
    ///
    /// Manifest rows merge with the rows the WAL tail derives, so a deletion
    /// committed seconds ago lists like a fresh file appears in `ls`. Both
    /// sides arrive in row-key order and a removal marker sorts ahead of the
    /// row it removes, so one ascending walk decides the page: a marker hides
    /// the generation whose key it repeats, and every other listed row is an
    /// entry. Reads stop as soon as `limit` entries are in hand.
    /// Point-reads one live recoverable deletion by its exact handle.
    ///
    /// Answers through the same merged walk the trash listing uses, so a
    /// revoked generation, a stale sequence, and a never-deleted inode all
    /// answer `None` — the pager's removal-marker rule stays the one
    /// authority, never a second decode of the family.
    pub(crate) async fn recoverable_deletion(
        &self,
        deletion_seq: ChangeSeq,
        root_inode_id: InodeId,
    ) -> Result<Option<RecoverableDeletion>, CoreError> {
        // The pager resumes strictly after a (sequence, inode) pair, and no
        // inode sits between a pair and its predecessor, so starting after
        // the predecessor makes the wanted pair the first candidate. Inode
        // zero is unallocated, so a zero predecessor cannot skip a real row.
        let Some(predecessor) = root_inode_id.0.checked_sub(1).map(InodeId) else {
            return Ok(None);
        };
        let mut page = self
            .active_deletions_page(Some((deletion_seq, predecessor)), 1)
            .await?;
        Ok(page.pop().filter(|deletion| {
            deletion.deletion_seq == deletion_seq && deletion.root_inode_id == root_inode_id
        }))
    }

    pub(super) async fn active_deletions_page(
        &self,
        start_after: Option<(ChangeSeq, InodeId)>,
        limit: usize,
    ) -> Result<Vec<RecoverableDeletion>, CoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let visible_seq = self.visible_seq();
        let lower_bound = match start_after {
            Some((deletion_seq, root_inode_id)) => {
                lookup_keys::active_deletion_key_after(deletion_seq, root_inode_id)
            }
            None => lookup_keys::ACTIVE_DELETION_ROW_PREFIX.to_owned(),
        };
        let upper_bound = string_prefix_upper_bound(lookup_keys::ACTIVE_DELETION_ROW_PREFIX);

        let mut tail: Vec<(String, ActiveDeletionRecord)> = self
            .row_states()
            .flat_map(|state| state.subtree_tombstones())
            .filter(|tombstone| tombstone.generation.seq <= visible_seq)
            .map(active_deletion_from_tombstone)
            .map(|record| (record.row_key(), record))
            .filter(|(row_key, _)| row_key.as_str() >= lower_bound.as_str())
            .collect();
        tail.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut durable = ActiveDeletionScan::new(lower_bound, self.manifest_segments().is_none());
        let mut tail_index = 0usize;
        let mut entries = Vec::with_capacity(limit);
        let mut removed_generation: Option<(ChangeSeq, InodeId)> = None;
        let mut last_row_key: Option<String> = None;
        while entries.len() < limit {
            if durable.buffered.is_empty() && !durable.exhausted {
                let raw_limit = limit.max(ACTIVE_DELETION_RAW_SCAN_LIMIT);
                let segments = self
                    .manifest_segments()
                    .expect("a view without manifest segments starts its scan exhausted");
                let page = manifest_index::active_deletions_page(
                    segments,
                    &durable.lower_bound,
                    upper_bound.as_deref(),
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
                    .expect("a non-empty buffer yields a row")
            };
            // A row present in both the manifest and the replayed tail is one
            // deletion, not two.
            if last_row_key.as_deref() == Some(row_key.as_str()) {
                continue;
            }
            last_row_key = Some(row_key);
            let generation = (record.deletion_seq, record.root_inode_id);
            match recoverable_deletion_from_active_record(record) {
                None => removed_generation = Some(generation),
                Some(deletion) if removed_generation != Some(generation) => entries.push(deletion),
                Some(_) => {}
            }
        }
        Ok(entries)
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
            let mut durable = if let Some(segments) = self.manifest_segments() {
                manifest_index::tombstones_for_root(segments, root_inode_id).await?
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
            .overlay_states()
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
    /// `[first_name_key, last_name_key]`, merged across manifest segments and
    /// row states. Complete over the range: callers treat absence as "no
    /// unbind exists".
    pub(super) async fn direntry_unbinds_for_parent_name_range(
        &self,
        parent_inode_id: InodeId,
        first_name_key: &NameKey,
        last_name_key: &NameKey,
    ) -> Result<Vec<DirentryUnbindRecord>, CoreError> {
        let mut unbinds = if let Some(segments) = self.manifest_segments() {
            manifest_index::direntry_unbinds_for_parent_name_range(
                segments,
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
/// the primitive lookups (over the manifest segments merged with the row-state
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

fn attributes_order_key(
    record: &AttributesRevisionRecord,
) -> (AttributeRevisionNo, ChangeSeq, u32) {
    (
        record.attributes_revision_no,
        record.committed_seq,
        record.delta_index,
    )
}

fn revision_order_key(record: &RevisionRecord) -> (RevisionNo, loonfs_api::ChangeSeq, u32) {
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
