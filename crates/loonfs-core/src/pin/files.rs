//! Lists files from the manifest pinned by a snapshot.

use super::read_basis::{load_pin_basis, PinBasis};
use super::snapshot::ensure_unexpired;
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::{CoreError, Result};
use crate::metadata::MetadataView;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::format::manifest::{lookup_keys, MetadataRow, MetadataRowFamily};
use loonfs_types::format::segment_blocks::string_prefix_upper_bound;
use loonfs_types::{
    ChangeSeq, ContentRef, InodeId, InodeKind, PageRequest, PagedResponse, PinId, RevisionNo,
};

/// Minimum number of inode rows scanned at once.
const INODE_SCAN_WAVE_ROWS: usize = 64;

/// Resumes a file listing after this inode id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointFilesPageCursor {
    /// Last inode id returned by the previous page.
    pub after_inode_id: InodeId,
}

/// One file in the state a snapshot pins, with the content the snapshot
/// pinned for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFile {
    /// The file's inode id.
    pub inode_id: InodeId,
    /// The file revision at the snapshot's captured sequence.
    pub revision_no: RevisionNo,
    /// The revision's content reference.
    pub content_ref: ContentRef,
    /// The file size in bytes.
    pub size_bytes: u64,
}

/// Options for listing the files a snapshot pins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListCheckpointFilesOptions {
    /// Whether to include deleted files and files under a deleted directory,
    /// disabled by default.
    pub include_deleted: bool,
}

/// One page of the files a snapshot pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFilesPage {
    /// The sequence captured by the snapshot.
    pub captured_seq: ChangeSeq,
    /// Files in ascending inode-id order.
    pub files: Vec<CheckpointFile>,
    /// Resume position when more files remain.
    pub next_cursor: Option<CheckpointFilesPageCursor>,
}

impl PagedResponse for CheckpointFilesPage {
    type Item = CheckpointFile;
    type Cursor = CheckpointFilesPageCursor;

    fn items_mut(&mut self) -> &mut Vec<CheckpointFile> {
        &mut self.files
    }

    fn items(&self) -> &[CheckpointFile] {
        &self.files
    }

    fn next_cursor(&self) -> Option<CheckpointFilesPageCursor> {
        self.next_cursor
    }

    fn absorb(&mut self, mut later: Self) {
        self.files.append(&mut later.files);
        self.next_cursor = later.next_cursor;
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Lists files from the manifest pinned by the snapshot `checkpoint_id`,
    /// ordered by inode ID: the files visible at the snapshot, or every file it
    /// retains with `options.include_deleted`. Directories are omitted.
    ///
    /// The method ignores the current metadata basis and WAL tail, so every page
    /// comes from the same snapshot even when new commits arrive. The read
    /// context is used only to validate the namespace identity and confirm that
    /// it has not been deleted. An ID that doesn't identify a snapshot, or a
    /// snapshot that expired at or before the current wall-clock time, returns
    /// `snapshot_not_found`, and a snapshot whose manifest is gone returns
    /// `checkpoint_unavailable`.
    pub async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
        context: &RuntimeReadContext,
    ) -> Result<CheckpointFilesPage> {
        // Rejects a mismatched or deleted namespace before any read work.
        self.ensure_live_context(context)?;
        self.namespace
            .list_checkpoint_files_page(checkpoint_id, request, options)
            .await
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Lists one page of the files the snapshot `checkpoint_id` pins, as
    /// [`NamespaceEngine::list_checkpoint_files_page`] describes.
    pub(crate) async fn list_checkpoint_files_page(
        &self,
        checkpoint_id: &PinId,
        request: PageRequest<CheckpointFilesPageCursor>,
        options: ListCheckpointFilesOptions,
    ) -> Result<CheckpointFilesPage> {
        let now_ms = self.wall_clock.now_ms();
        let record = ensure_unexpired(self.load_user_pin(checkpoint_id).await?, now_ms)?.state;
        let PinBasis { manifest, segments } =
            load_pin_basis(&self.store, &self.segment_cache, record).await?;

        let captured_seq = manifest.head_seq;
        let view = MetadataView::over_manifest_segments(&segments, captured_seq);
        let mut session = view.session();

        // Read one extra file to determine whether another page exists.
        let wanted = request.limit.limit_plus_one();
        let wave_rows = wanted.max(INODE_SCAN_WAVE_ROWS);
        let mut lower_bound = match request.cursor {
            Some(cursor) => lookup_keys::inode_key_after(cursor.after_inode_id),
            None => lookup_keys::INODE_ROW_PREFIX.to_owned(),
        };
        let upper_bound = string_prefix_upper_bound(lookup_keys::INODE_ROW_PREFIX);
        let mut files = Vec::with_capacity(wanted);
        while files.len() < wanted {
            let rows = segments
                .scan_range_page_with_keys(
                    MetadataRowFamily::Inodes,
                    &lower_bound,
                    upper_bound.as_deref(),
                    wave_rows,
                )
                .await?;
            let family_exhausted = rows.len() < wave_rows;
            // Resume after the last scanned row, including rows filtered out below.
            match rows.last() {
                Some((row_key, _)) => lower_bound = lookup_keys::after_row_key(row_key),
                None => break,
            }
            let inode_rows = rows
                .into_iter()
                .map(|(row_key, row)| match row {
                    MetadataRow::Inode(crate::metadata::InodeRecord {
                        inode_id,
                        inode_kind,
                        ..
                    }) => Ok((inode_id, inode_kind)),
                    _ => Err(CoreError::NamespaceCorrupt(format!(
                        "inodes family returned a non-inode row at `{row_key}`"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            let file_inode_ids = inode_rows
                .iter()
                .filter(|(_, inode_kind)| *inode_kind == InodeKind::File)
                .map(|(inode_id, _)| *inode_id)
                .collect::<Vec<_>>();
            if !options.include_deleted {
                session.preload_visibility(&file_inode_ids).await?;
            }
            for inode_id in file_inode_ids {
                let revision = if options.include_deleted {
                    view.latest_revision_record(inode_id).await?
                } else if session.visible_inode(inode_id).await?.is_some() {
                    session.latest_revision_head_of_visible(inode_id).await?
                } else {
                    None
                };
                let Some(revision) = revision else {
                    continue;
                };
                files.push(CheckpointFile {
                    inode_id,
                    revision_no: revision.revision_no,
                    size_bytes: revision.content_ref.size_bytes,
                    content_ref: revision.content_ref,
                });
                if files.len() == wanted {
                    break;
                }
            }
            if family_exhausted {
                break;
            }
        }

        let next_cursor = request
            .limit
            .finish_page(&mut files, |last| CheckpointFilesPageCursor {
                after_inode_id: last.inode_id,
            });
        Ok(CheckpointFilesPage {
            captured_seq,
            files,
            next_cursor,
        })
    }
}
