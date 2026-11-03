//! [`MetadataView`] lookups of file revisions,
//! attributes and access revisions, and content layouts.

use super::manifest_index;
use super::manifest_tail_merge::{ManifestPage, ManifestTailMerge, TieOrder};
use crate::error::{CoreError, Result};
use crate::metadata::view::newest;
use crate::metadata::view::MetadataView;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, ContentLayoutRecord, RevisionRecord,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AccessRevisionNo, Attributes, AttributesProjection, AttributesRevisionNo, ChangeSeq, InodeId,
    InodeKind, NamespaceId, RevisionNo,
};

/// A chain's head in one view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContentHead {
    /// The chain's largest visible size.
    pub(crate) size_bytes: u64,
    /// The chain's creator, as the chain's layout row records it. `None` when
    /// the view has no layout row for the chain, which happens only when the
    /// chain exists as revisions in the namespace's own unfolded tail, so the
    /// namespace created it.
    pub(crate) creator_namespace_id: Option<NamespaceId>,
}

impl<'a, 'store, S: ObjectStore + ?Sized> MetadataView<'a, 'store, S> {
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
        Ok(newest(
            self.row_latest_revision_by_inode(inode_id),
            manifest_index::latest_revision_by_inode(
                self.manifest_segments(),
                inode_id,
                self.visible_seq(),
            )
            .await?,
            revision_order_key,
        ))
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
        Ok(newest(
            self.row_revision_by_inode_no(inode_id, revision_no),
            manifest_index::revision_by_inode_no(
                self.manifest_segments(),
                inode_id,
                revision_no,
                self.visible_seq(),
            )
            .await?,
            revision_order_key,
        ))
    }

    /// Returns up to `limit` of the inode's revisions after `start_after`,
    /// newest first.
    ///
    /// The lookup serves a view at or above its manifest's head. Below the
    /// head, a page can come back short, because the lookup reads `limit`
    /// manifest rows and then drops the rows above the view's sequence, so a
    /// caller that treats a short page as the end misses older revisions.
    pub(crate) async fn revisions_by_inode_page_desc(
        &self,
        inode_id: InodeId,
        start_after: Option<manifest_index::RevisionPagePosition>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut revisions = manifest_index::revisions_by_inode_page_desc(
            self.manifest_segments(),
            inode_id,
            start_after,
            limit,
        )
        .await?;
        revisions.extend(self.row_revisions_by_inode_page_desc(inode_id, start_after));
        revisions.retain(|revision| revision.committed_seq <= self.visible_seq());
        revisions.sort_by_key(|revision| std::cmp::Reverse(revision_order_key(revision)));
        revisions.truncate(limit);
        Ok(revisions)
    }

    /// Returns up to `limit` of the view's revisions whose row keys are at or
    /// after `lower_bound`, with their row keys, in key order (by inode, and
    /// newest first within each inode).
    ///
    /// The manifest's rows merge with the WAL tail's rows, and the page reads
    /// manifest rows until it has `limit` rows at or below the view's
    /// sequence, so a page below the manifest's head is short only at the end
    /// of the family.
    pub(crate) async fn revisions_page(
        &self,
        lower_bound: &str,
        limit: usize,
    ) -> Result<Vec<(String, RevisionRecord)>> {
        let visible_seq = self.visible_seq();
        let mut tail: Vec<(String, RevisionRecord)> = self
            .row_states()
            .flat_map(|state| state.revisions())
            .filter(|revision| revision.committed_seq <= visible_seq)
            .map(|revision| (revision_row_key(revision), revision.clone()))
            .filter(|(row_key, _)| row_key.as_str() >= lower_bound)
            .collect();
        tail.sort_by(|(left, _), (right, _)| left.cmp(right));
        let segments = self.manifest_segments();
        // A short page is the end of the family.
        let refill = |lower_bound: String| async move {
            let rows = manifest_index::revisions_page(segments, &lower_bound, limit).await?;
            Ok(ManifestPage {
                more: rows.len() >= limit,
                rows,
            })
        };
        let mut merge = ManifestTailMerge::new(lower_bound.to_owned(), tail, TieOrder::TailFirst);
        let mut rows: Vec<(String, RevisionRecord)> = Vec::with_capacity(limit);
        while rows.len() < limit {
            let Some((row_key, revision)) = merge.next(refill).await? else {
                break;
            };
            if revision.committed_seq > visible_seq {
                continue;
            }
            // A row present in both the manifest and the tail is one
            // revision, not two.
            if rows
                .last()
                .is_some_and(|(last_key, _)| *last_key == row_key)
            {
                continue;
            }
            rows.push((row_key, revision));
        }
        Ok(rows)
    }

    /// The newest attribute revision record for `inode_id` at the visible
    /// sequence, or `None` when the inode has never had attributes written.
    ///
    /// Both legs filter by `visible_seq`. Each attribute record replaces the
    /// one before it, so a checkpoint-basis or fork-basis read that let a
    /// newer manifest row through would return a map that sequence never saw.
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
        Ok(newest(
            row_record,
            manifest_index::attributes_by_inode(
                self.manifest_segments(),
                inode_id,
                self.visible_seq(),
            )
            .await?,
            attributes_order_key,
        ))
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
        Ok(newest(
            row_record,
            manifest_index::access_by_inode(self.manifest_segments(), inode_id, self.visible_seq())
                .await?,
            access_order_key,
        ))
    }

    /// Returns the inode's attributes at the visible sequence, with the actor
    /// and time of the latest stored revision.
    ///
    /// An inode with no record anywhere is at revision 0 with an empty map and
    /// no actor or time. That state is concrete, not a missing result, so
    /// nothing writes a durable row to represent it.
    pub(crate) async fn attributes(&self, inode_id: InodeId) -> Result<AttributesProjection> {
        Ok(match self.latest_attributes_revision(inode_id).await? {
            Some(record) => AttributesProjection {
                attributes_revision_no: record.attributes_revision_no,
                attributes_updated_by: Some(record.committed_by),
                attributes_updated_at_ms: Some(record.committed_at_ms),
                attributes: record.attributes,
            },
            None => AttributesProjection {
                attributes_revision_no: AttributesRevisionNo(0),
                attributes_updated_by: None,
                attributes_updated_at_ms: None,
                attributes: Attributes::default(),
            },
        })
    }

    /// Returns the chain's head: its largest visible size, so a shorter
    /// restore or copy cannot shrink the head and allow conflicting appends,
    /// and the creator that its longest layout row records.
    pub(crate) async fn content_head(
        &self,
        content_id: &loonfs_types::ContentId,
    ) -> Result<Option<ContentHead>> {
        let tail_size = self
            .row_states()
            .flat_map(|state| state.revisions())
            .filter(|row| {
                &row.content_ref.content_id == content_id && row.committed_seq <= self.visible_seq()
            })
            .map(|row| row.content_ref.size_bytes)
            .max();
        let layout = self.content_layout(content_id).await?;
        let Some(size_bytes) = tail_size
            .into_iter()
            .chain(layout.as_ref().map(|row| row.size_bytes))
            .max()
        else {
            return Ok(None);
        };
        Ok(Some(ContentHead {
            size_bytes,
            creator_namespace_id: layout.map(|row| row.layout.creator_namespace_id),
        }))
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
        let visible_seq = self.visible_seq();
        let tail = self
            .row_states()
            .filter_map(|state| {
                if state.indexed_seq() <= visible_seq {
                    state.indexes.content_layout(content_id)
                } else {
                    state
                        .content_layouts()
                        .iter()
                        .filter(|row| {
                            &row.content_id == content_id && row.committed_seq <= visible_seq
                        })
                        .max_by_key(|row| (row.size_bytes, row.committed_seq))
                }
            })
            .max_by_key(|row| (row.size_bytes, row.committed_seq));
        Ok(newest(
            tail.cloned(),
            manifest_index::content_layout(self.manifest_segments(), content_id).await?,
            |row| (row.size_bytes, row.committed_seq),
        ))
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

/// Returns the revisions family's row key of `revision`.
fn revision_row_key(revision: &RevisionRecord) -> String {
    loonfs_types::format::manifest::lookup_keys::revision_row_key(
        revision.inode_id,
        revision.revision_no,
        revision.committed_seq,
        revision.delta_index,
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
