//! [`MetadataView`] lookups of file revisions, the revisions that refer to
//! a content value, attributes and access revisions, and content layouts.

use super::manifest_index;
use crate::error::{CoreError, Result};
use crate::metadata::view::newest;
use crate::metadata::view::MetadataView;
use crate::metadata::{
    AccessRevisionRecord, AttributesRevisionRecord, ContentLayoutRecord, RevisionRecord,
};
use loonfs_objectstore::ObjectStore;
#[cfg(test)]
use loonfs_types::format::manifest::{lookup_keys, ContentRevisionRecord};
use loonfs_types::{
    AccessRevisionNo, Attributes, AttributesProjection, AttributesRevisionNo, ChangeSeq, InodeId,
    InodeKind, NamespaceId, RevisionNo,
};
#[cfg(test)]
use std::cmp::Reverse;

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

    /// Returns up to `limit` of a content value's `content_revisions` rows
    /// after `start_after`, in key order: by inode, and newest revision
    /// first within each inode.
    ///
    /// A base compaction can keep a row after the revision's own row is
    /// removed ([row retention during a base
    /// compaction](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#103-row-retention-during-a-base-compaction)),
    /// so a caller that needs the revision reads it with
    /// [`Self::revision_at_head`].
    #[cfg(test)]
    pub(crate) async fn content_revisions_page(
        &self,
        content_id: &loonfs_types::ContentId,
        size_bytes: u64,
        start_after: Option<(InodeId, RevisionNo)>,
        limit: usize,
    ) -> Result<Vec<ContentRevisionRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let lower_bound = match start_after {
            Some((inode_id, revision_no)) => {
                lookup_keys::after_row_key(&lookup_keys::content_revision_row_key(
                    content_id,
                    size_bytes,
                    inode_id,
                    revision_no,
                ))
            }
            None => lookup_keys::content_revision_prefix(content_id, size_bytes),
        };
        let mut rows = manifest_index::content_revisions_page(
            self.manifest_segments(),
            content_id,
            size_bytes,
            lower_bound,
            self.visible_seq(),
            limit,
        )
        .await?;
        rows.extend(
            self.row_states()
                .flat_map(|state| state.revisions())
                .filter(|revision| {
                    revision.content_ref.content_id == *content_id
                        && revision.content_ref.size_bytes == size_bytes
                        && revision.committed_seq <= self.visible_seq()
                })
                .map(ContentRevisionRecord::from_revision)
                .filter(|row| {
                    start_after.is_none_or(|(inode_id, revision_no)| {
                        content_revision_order_key(row) > (inode_id, Reverse(revision_no))
                    })
                }),
        );
        rows.sort_by_key(content_revision_order_key);
        rows.truncate(limit);
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

/// Orders content revision rows as their keys do within one content value:
/// by inode, and newest revision first within each inode.
#[cfg(test)]
fn content_revision_order_key(record: &ContentRevisionRecord) -> (InodeId, Reverse<RevisionNo>) {
    (record.inode_id, Reverse(record.revision_no))
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
