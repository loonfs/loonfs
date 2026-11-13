//! [`MetadataView`] lookups of directory bindings,
//! subtree tombstones, and the trash.

use super::durable_cache::{ParentNameCacheKey, SharedRows};
use super::manifest_index;
use super::manifest_tail_combiner::{ManifestPage, ManifestTailCombiner, TieOrder};
use super::visibility;
use crate::error::Result;
use crate::metadata::view::latest_visible_binding;
use crate::metadata::view::MetadataView;
use crate::metadata::{
    listed_trash_entry_from_record, DirentryBindingRecord, ListedTrashEntry, MetadataState,
    SubtreeTombstoneRecord, TrashRange, TrashRecord,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{CommitSeq, InodeId, NameKey, ROOT_INODE_ID};

/// Rows one trash page fetches per manifest round-trip. A page's entries are
/// listed rows; restore markers share the range until compaction removes
/// each pair away, so the raw scan runs a little ahead of the page.
const TRASH_RAW_SCAN_LIMIT: usize = 64;

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

    /// Returns the root's newest tombstone event at the view's sequence, a
    /// `set` or a `revoke`.
    pub(crate) async fn newest_subtree_tombstone(
        &self,
        root_inode_id: InodeId,
    ) -> Result<Option<SubtreeTombstoneRecord>> {
        let tombstones = self.tombstones_for_root(root_inode_id).await?;
        Ok(super::rows::newest_tombstone_from_records(
            tombstones.iter().cloned(),
            self.visible_seq(),
        ))
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

    /// The `set` event of the root's deletion in the trash: its latest
    /// tombstone event, when that event is a `set` above the view's
    /// retention floor
    /// ([format section 1.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#16-file-and-subtree-deletion)).
    pub(crate) async fn tombstone_in_trash(
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
            |direntry| direntry.child_inode_id == child_inode_id && direntry.is_binding_event(),
            |state| state.indexes.latest_binding_for_child(child_inode_id),
        )
        .await
    }

    /// Returns one page of the namespace's deletions in `range` that no
    /// restore has cancelled, oldest deletion first.
    ///
    /// Manifest rows combine with the rows the WAL tail derives, so a deletion
    /// committed seconds ago lists like a fresh file appears in `ls`. Both
    /// sides arrive in row-key order and a removal marker sorts ahead of the
    /// row it removes, so one ascending walk determines the page: a marker hides
    /// the position whose key it repeats, and every other listed row is an
    /// entry. Reads stop as soon as `limit` entries are in hand.
    ///
    /// The listing serves a view at or above its manifest's head.
    /// The `trash` family is a query index, not history, and compaction
    /// removes a deletion's row together with the row its restore adds, so
    /// filtering these rows can't return the deletions of a view below the
    /// head.
    pub(super) async fn trash_entries_page(
        &self,
        range: &TrashRange,
        limit: usize,
    ) -> Result<Vec<ListedTrashEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let tail = self.tail_rows::<TrashRecord>(&range.lower_bound, range.upper_bound.as_deref());

        let segments = self.manifest_segments();
        let raw_limit = limit.max(TRASH_RAW_SCAN_LIMIT);
        // A short page is the end of the range.
        let refill = |lower_bound: String| async move {
            let rows = manifest_index::trash_rows_page(
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
        let mut combiner =
            ManifestTailCombiner::new(range.lower_bound.clone(), tail, TieOrder::TailFirst);
        let mut entries = Vec::with_capacity(limit);
        let mut removed_position: Option<(CommitSeq, InodeId)> = None;
        while entries.len() < limit {
            let Some((_, record)) = combiner.next(refill).await? else {
                break;
            };
            let position = (record.deletion_seq, record.root_inode_id);
            match listed_trash_entry_from_record(record) {
                None => removed_position = Some(position),
                Some(deletion) if removed_position != Some(position) => entries.push(deletion),
                Some(_) => {}
            }
        }
        Ok(entries)
    }

    /// Returns the root's tombstone events that its newest event at the
    /// view's sequence is among: the manifest's newest event at or below the
    /// sequence, and the row states' events.
    pub(super) async fn tombstones_for_root(
        &self,
        root_inode_id: InodeId,
    ) -> Result<SharedRows<SubtreeTombstoneRecord>> {
        let segments = self.manifest_segments();
        let visible_seq = self.visible_seq();
        self.shared_rows(
            |inner| &mut inner.tombstones_for_root,
            root_inode_id,
            Box::pin(async move {
                Ok(
                    manifest_index::newest_tombstone_for_root(segments, root_inode_id, visible_seq)
                        .await?
                        .into_iter()
                        .collect(),
                )
            }),
            MetadataState::subtree_tombstones,
            |tombstone| tombstone.root_inode_id == root_inode_id,
            |state| state.indexes.latest_tombstone(root_inode_id),
        )
        .await
    }
}
