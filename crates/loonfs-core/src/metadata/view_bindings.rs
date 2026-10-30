//! [`MetadataView`] lookups of directory bindings,
//! subtree tombstones, and active deletions.

use super::durable_cache::{ParentNameCacheKey, SharedRows};
use super::manifest_index;
use super::manifest_tail_merge::{ManifestPage, ManifestTailMerge, TieOrder};
use super::visibility;
use crate::error::{CoreError, Result};
use crate::metadata::view::latest_visible_binding;
use crate::metadata::view::MetadataView;
use crate::metadata::{
    active_deletion_from_tombstone, recoverable_deletion_from_active_record, ActiveDeletionRange,
    ActiveDeletionRecord, DirentryBindingRecord, DirentryRevisionRecord, MetadataState,
    RecoverableDeletion, SubtreeTombstoneRecord,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys;
use loonfs_types::{ChangeSeq, InodeId, NameKey, ROOT_INODE_ID};

/// Rows one trash page fetches per manifest round-trip. A page's entries are
/// listed rows; undelete markers share the range until compaction removes
/// each pair away, so the raw scan runs a little ahead of the page.
const ACTIVE_DELETION_RAW_SCAN_LIMIT: usize = 64;

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
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
    /// tombstone event, when that event is a `set` above the view's
    /// retention floor
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    pub(crate) async fn recoverable_deletion(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        let retention_floor_seq = self.retention_floor_seq();
        Ok(self
            .hiding_subtree_tombstone(root_inode_id)
            .await?
            .filter(|tombstone| tombstone.committed_seq > retention_floor_seq))
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
            Box::pin(manifest_index::direntry_binds_for_parent_name(
                self.manifest_segments(),
                parent_inode_id,
                name_key,
                self.visible_seq(),
            )),
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
            Box::pin(manifest_index::direntry_binds_for_child(
                self.manifest_segments(),
                child_inode_id,
                self.visible_seq(),
            )),
            MetadataState::direntry_binds,
            |direntry| direntry.child_inode_id == child_inode_id,
            |state| state.indexes.latest_binding_for_child(child_inode_id),
        )
        .await
    }

    /// Returns the bind rows of `parent_inode_id` that the row states contain
    /// after `start_after_name_key`, with their row keys, in row-key order.
    pub(super) fn tail_direntry_bind_page_candidates(
        &self,
        parent_inode_id: InodeId,
        start_after_name_key: Option<&NameKey>,
    ) -> Vec<(String, DirentryBindingRecord)> {
        let mut candidates = self
            .row_states()
            .flat_map(|state| state.direntry_binds())
            .filter(|direntry| {
                direntry.parent_inode_id == parent_inode_id
                    && start_after_name_key
                        .is_none_or(|last_name_key| &direntry.name_key > last_name_key)
            })
            .map(|record| {
                let row_key = lookup_keys::direntry_bind_row_key(
                    record.parent_inode_id,
                    &record.name_key,
                    record.child_inode_id,
                    record.committed_seq,
                    record.delta_index,
                );
                (row_key, record.clone())
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
        candidates
    }

    /// Returns the revision rows that the row states contain under the edges
    /// of `parent_inode_id` whose name keys are from `first_name_key`
    /// through `last_name_key`, with their row keys, in row-key order.
    pub(super) fn tail_direntry_revision_candidates(
        &self,
        parent_inode_id: InodeId,
        first_name_key: &NameKey,
        last_name_key: &NameKey,
    ) -> Vec<(String, DirentryRevisionRecord)> {
        let mut candidates = self
            .row_states()
            .flat_map(MetadataState::direntry_revisions)
            .filter(|revision| {
                revision.parent_inode_id == parent_inode_id
                    && (first_name_key..=last_name_key).contains(&&revision.name_key)
            })
            .map(|record| {
                let row_key = lookup_keys::direntry_revision_row_key(
                    record.parent_inode_id,
                    &record.name_key,
                    record.child_inode_id,
                    record.committed_seq,
                    record.delta_index,
                );
                (row_key, record.clone())
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|(left, _), (right, _)| left.cmp(right));
        candidates
    }

    /// One page of the namespace's deletions in `range` that no undelete has
    /// cancelled, oldest deletion first.
    ///
    /// Manifest rows merge with the rows the WAL tail derives, so a deletion
    /// committed seconds ago lists like a fresh file appears in `ls`. Both
    /// sides arrive in row-key order and a removal marker sorts ahead of the
    /// row it removes, so one ascending walk determines the page: a marker hides
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

        let segments = self.manifest_segments();
        let raw_limit = limit.max(ACTIVE_DELETION_RAW_SCAN_LIMIT);
        // A short page is the end of the range.
        let refill = |after: Option<String>| async move {
            let lower_bound = after.map_or_else(
                || range.lower_bound.clone(),
                |row_key| lookup_keys::after_row_key(&row_key),
            );
            let rows = manifest_index::active_deletions_page(
                segments,
                &lower_bound,
                range.upper_bound.as_deref(),
                raw_limit,
            )
            .await?;
            Ok(ManifestPage {
                more: rows.len() >= raw_limit,
                rows,
            })
        };
        let mut merge = ManifestTailMerge::new(tail, TieOrder::TailFirst);
        let mut entries = Vec::with_capacity(limit);
        let mut removed_position: Option<(ChangeSeq, InodeId)> = None;
        let mut last_row_key: Option<String> = None;
        while entries.len() < limit {
            let Some((row_key, record)) = merge.next(refill).await? else {
                break;
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
            Box::pin(manifest_index::tombstones_for_root(
                self.manifest_segments(),
                root_inode_id,
            )),
            MetadataState::subtree_tombstones,
            |tombstone| tombstone.root_inode_id == root_inode_id,
            |state| state.indexes.latest_tombstone(root_inode_id),
        )
        .await
    }
}
