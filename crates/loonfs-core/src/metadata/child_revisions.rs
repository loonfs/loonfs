//! The current revision of each file on one page of a directory's children,
//! read from the revisions stored under the directory's edges
//! ([format section 1.3](https://github.com/loonfs/loonfs/blob/main/docs/specs/format.md#13-directory-bindings)).

use super::children_page::VisibleChildEntry;
use super::manifest_index;
use super::manifest_tail_merge::{ManifestPage, ManifestTailMerge, TieOrder};
use super::view_session::MetadataViewSession;
use super::DirentryRevisionRecord;
use crate::error::{CoreError, Result};
use crate::limits::DIRECTORY_PAGE_RAW_SCAN_LIMIT;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::lookup_keys;
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{InodeId, InodeKind};

impl<S: ObjectStore + ?Sized> MetadataViewSession<'_, '_, S> {
    /// Returns the current revision of each file in `children`, and `None`
    /// for each directory, in the order of `children`.
    ///
    /// `children` are visible children of `parent_inode_id` in name-key
    /// order, as a listing page scans them. One scan of the parent's
    /// `direntry_revisions` rows, from the first file's name key through
    /// the last file's, merged with the WAL tail, reads every revision. A
    /// file's revision is its edge's newest row at or below the session's
    /// sequence, and a file with no such row is corruption.
    pub(crate) async fn child_file_revisions(
        &mut self,
        parent_inode_id: InodeId,
        children: &[VisibleChildEntry],
    ) -> Result<Vec<Option<DirentryRevisionRecord>>> {
        let files: Vec<(usize, &VisibleChildEntry)> = children
            .iter()
            .enumerate()
            .filter(|(_, child)| child.binding.child_kind == InodeKind::File)
            .collect();
        let mut revisions = vec![None; children.len()];
        let (Some((_, first)), Some((_, last))) = (files.first(), files.last()) else {
            return Ok(revisions);
        };
        let lower_bound =
            lookup_keys::direntry_revision_prefix(parent_inode_id, &first.binding.name_key);
        let upper_bound = string_prefix_upper_bound(&lookup_keys::direntry_revision_prefix(
            parent_inode_id,
            &last.binding.name_key,
        ));
        let tail = self.base.tail_direntry_revision_candidates(
            parent_inode_id,
            &first.binding.name_key,
            &last.binding.name_key,
        );
        let visible_seq = self.base.visible_seq();
        let segments = self.base.manifest_segments();
        let counters = &mut self.counters;
        // A short page is the end of the range.
        let mut refill = |after: Option<String>| {
            counters.scan_range_page_calls = counters.scan_range_page_calls.saturating_add(1);
            let lower_bound = after.map_or_else(
                || lower_bound.clone(),
                |row_key| lookup_keys::after_row_key(&row_key),
            );
            let upper_bound = upper_bound.clone();
            async move {
                let rows = manifest_index::direntry_revisions_page(
                    segments,
                    &lower_bound,
                    upper_bound.as_deref(),
                    DIRECTORY_PAGE_RAW_SCAN_LIMIT,
                )
                .await?;
                Ok(ManifestPage {
                    more: rows.len() >= DIRECTORY_PAGE_RAW_SCAN_LIMIT,
                    rows,
                })
            }
        };
        let mut merge = ManifestTailMerge::new(tail, TieOrder::ManifestFirst);
        let mut next_file = 0;
        while let Some((_, row)) = merge.next(&mut refill).await? {
            if row.committed_seq > visible_seq {
                continue;
            }
            // Both sides are in row-key order, which orders edges by name key
            // and then by child, so the files whose edges sort before this
            // row's edge have no rows left to come.
            let edge = (row.name_key.as_str(), row.child_inode_id);
            while files.get(next_file).is_some_and(|(_, child)| {
                (
                    child.binding.name_key.as_str(),
                    child.binding.child_inode_id,
                ) < edge
            }) {
                next_file += 1;
            }
            let Some((position, child)) = files.get(next_file) else {
                break;
            };
            if (
                child.binding.name_key.as_str(),
                child.binding.child_inode_id,
            ) != edge
            {
                continue;
            }
            let newest = &mut revisions[*position];
            if newest
                .as_ref()
                .is_none_or(|newest: &DirentryRevisionRecord| newest.position() < row.position())
            {
                *newest = Some(row);
            }
        }
        if let Some((_, child)) = files
            .iter()
            .find(|(position, _)| revisions[*position].is_none())
        {
            return Err(CoreError::NamespaceCorrupt(format!(
                "file `{}` is bound as `{}` under directory `{parent_inode_id}` with no directory \
                 entry revision at or below seq `{visible_seq}`",
                child.binding.child_inode_id, child.binding.name_key
            )));
        }
        Ok(revisions)
    }
}
