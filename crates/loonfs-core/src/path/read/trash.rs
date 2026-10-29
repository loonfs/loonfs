//! Trash listings over a loaded metadata view.

use super::listing::validate_cursor_head;
use crate::authorize::ReadAccess;
use crate::error::Result;
use crate::metadata::ActiveDeletionRange;
use crate::path::read::loaded_view::LoadedMetadataView;
use loonfs_objectstore::ObjectStore;
use loonfs_types::DirectoryBinding;
use loonfs_types::{ChangeSeq, InodeId, Page, PageRequest, TrashEntry, TrashPageCursor};

/// Rows one filtered trash page examines before it returns a short page with a
/// cursor.
const MAX_TRASH_ROWS_SCANNED_PER_PAGE: usize = 256;

impl<'a, S: ObjectStore + ?Sized> LoadedMetadataView<'a, S> {
    /// One page of the namespace's recoverable deletions, oldest deletion
    /// first, as one bounded range scan over the derived active-deletion
    /// family.
    ///
    /// The materializer keeps that family in step with the tombstone family
    /// (a delete adds the row, and an undelete removes it). The scan starts
    /// above the view's retention floor, so an expired deletion is never
    /// listed, and the page reads only the deletions it returns, never the
    /// namespace's whole deletion history. A fork's floor starts at its fork
    /// point, so a fork never lists a deletion it inherited
    /// ([API section 6.6](https://github.com/loonfs/loonfs/blob/main/docs/specs/api.md#66-get-filesystemtrash)).
    pub(crate) async fn list_trash_page(
        &self,
        request: PageRequest<TrashPageCursor>,
        access: &ReadAccess<'_, S>,
    ) -> Result<Page<TrashEntry, TrashPageCursor>> {
        validate_cursor_head(
            self.head.seq,
            request.cursor.as_ref().map(|cursor| cursor.head_seq),
        )?;
        let start_after = request
            .cursor
            .as_ref()
            .map(|cursor| (cursor.last_deletion_seq, cursor.last_root_inode_id));
        let retention_floor_seq = self.retention_floor_seq();
        let mut session = self.metadata_view().session();
        let cursor = |seq: ChangeSeq, inode_id: InodeId| TrashPageCursor {
            head_seq: self.head.seq,
            last_deletion_seq: seq,
            last_root_inode_id: inode_id,
        };
        let (deletions, next_cursor) = if access.is_unrestricted() {
            let mut deletions = session
                .active_deletions_page(
                    &ActiveDeletionRange::after(start_after).above_seq(retention_floor_seq),
                    request.limit.limit_plus_one(),
                )
                .await?;
            let next_cursor = request.limit.finish_page(&mut deletions, |last| {
                cursor(last.deletion_seq, last.root_inode_id)
            });
            (deletions, next_cursor)
        } else {
            let limit = request.limit.as_usize();
            let mut start_after = start_after;
            let mut kept = Vec::new();
            let mut scanned = 0;
            let mut last_scanned = None;
            let mut exhausted = false;
            while !exhausted && kept.len() < limit && scanned < MAX_TRASH_ROWS_SCANNED_PER_PAGE {
                let wanted = (limit - kept.len()).min(MAX_TRASH_ROWS_SCANNED_PER_PAGE - scanned);
                let batch = session
                    .active_deletions_page(
                        &ActiveDeletionRange::after(start_after).above_seq(retention_floor_seq),
                        wanted,
                    )
                    .await?;
                exhausted = batch.len() < wanted;
                for deletion in batch {
                    scanned += 1;
                    last_scanned = Some((deletion.deletion_seq, deletion.root_inode_id));
                    if access
                        .can_read(&mut session, deletion.deleted_binding.parent_inode_id)
                        .await?
                    {
                        kept.push(deletion);
                    }
                }
                start_after = last_scanned;
            }
            let next_cursor = if exhausted {
                None
            } else {
                last_scanned.map(|(seq, inode_id)| cursor(seq, inode_id))
            };
            (kept, next_cursor)
        };
        let entries = deletions
            .into_iter()
            .map(|deletion| TrashEntry {
                inode_id: deletion.root_inode_id,
                inode_kind: deletion.inode_kind.into(),
                deletion_seq: deletion.deletion_seq,
                deleted_at_ms: deletion.deleted_at_ms,
                deleted_by: deletion.deleted_by,
                deleted_binding: DirectoryBinding {
                    parent_inode_id: deletion.deleted_binding.parent_inode_id,
                    name_key: deletion.deleted_binding.name_key,
                    display_name: deletion.deleted_binding.display_name,
                },
            })
            .collect();
        Ok(Page {
            items: entries,
            next_cursor,
        })
    }
}
