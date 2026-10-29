//! [`MetadataView`] lookups of file revisions,
//! attributes and access revisions, and content layouts.

use super::manifest_index;
use crate::error::{CoreError, Result};
use crate::metadata::view::newest;
use crate::metadata::view::MetadataView;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, ContentLayoutRecord, RevisionRecord,
};
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    AccessRevisionNo, Attributes, AttributesProjection, AttributesRevisionNo, ChangeSeq, InodeId,
    InodeKind, RevisionNo,
};

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
            manifest_index::latest_revision_by_inode(self.manifest_segments(), inode_id).await?,
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
            manifest_index::revision_by_inode_no(self.manifest_segments(), inode_id, revision_no)
                .await?,
            revision_order_key,
        ))
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

    /// The newest attribute revision record for `inode_id` at the visible
    /// sequence, or `None` when the inode has never had attributes written.
    ///
    /// Both legs filter by `visible_seq`. The revision lookup this is modeled
    /// on filters only its row-state leg, because a file revision row is never
    /// rewritten and a revision read at the head is the same result at any
    /// sequence at or above it. Attributes are not history: each record
    /// replaces the one before it, so a checkpoint-basis or fork-basis read
    /// that let a newer manifest row through would return a map that
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
