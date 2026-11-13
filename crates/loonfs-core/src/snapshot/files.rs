//! Lists files from the manifest kept by a snapshot, or at a sequence of
//! the live namespace.

use super::lifecycle::ensure_live;
use super::read_basis::{load_snapshot_basis, SnapshotBasis};
use crate::engine::{NamespaceEngine, RuntimeReadContext};
use crate::error::Result;
use crate::metadata::MetadataView;
use crate::namespace::store::NamespaceStore;
use loonfs_objectstore::ObjectStore;
use loonfs_types::{
    CommitSeq, ContentRef, InodeId, InodeKind, PageRequest, PagedResponse, RevisionNo, SnapshotId,
};

/// Minimum number of inode rows scanned at once.
const INODE_SCAN_WAVE_ROWS: usize = 64;

/// Resumes a file listing after this inode id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotFilesPageCursor {
    /// Last inode id returned by the previous page.
    pub after_inode_id: InodeId,
}

/// One file in the state a snapshot keeps, or in the state at a sequence,
/// with the file's content in that state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFile {
    /// The file's inode id.
    pub inode_id: InodeId,
    /// The file revision at the listed sequence.
    pub revision_no: RevisionNo,
    /// The sequence of the commit that published the revision.
    pub committed_seq: CommitSeq,
    /// The revision's content reference.
    pub content_ref: ContentRef,
    /// The file size in bytes.
    pub size_bytes: u64,
}

/// Options for listing the files a snapshot keeps or a sequence has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ListSnapshotFilesOptions {
    /// Whether to include deleted files and files under a deleted directory,
    /// disabled by default.
    pub include_deleted: bool,
}

/// One page of the files a snapshot keeps or a sequence has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFilesPage {
    /// The sequence captured by the snapshot, or the listed sequence.
    pub captured_seq: CommitSeq,
    /// Files in ascending inode-id order.
    pub files: Vec<SnapshotFile>,
    /// Resume position when more files remain.
    pub next_cursor: Option<SnapshotFilesPageCursor>,
}

impl PagedResponse for SnapshotFilesPage {
    type Item = SnapshotFile;
    type Cursor = SnapshotFilesPageCursor;

    fn items_mut(&mut self) -> &mut Vec<SnapshotFile> {
        &mut self.files
    }

    fn items(&self) -> &[SnapshotFile] {
        &self.files
    }

    fn next_cursor(&self) -> Option<SnapshotFilesPageCursor> {
        self.next_cursor
    }

    fn absorb(&mut self, mut later: Self) {
        self.files.append(&mut later.files);
        self.next_cursor = later.next_cursor;
    }
}

impl<S: ObjectStore, M> NamespaceEngine<S, M> {
    /// Lists files from the manifest kept by the snapshot `snapshot_id`,
    /// ordered by inode ID: the files visible at the snapshot, or every file it
    /// retains with `options.include_deleted`. Directories are omitted.
    ///
    /// The method ignores the current metadata basis and WAL tail, so every page
    /// comes from the same snapshot even when new commits arrive. The read
    /// context is used only to validate the namespace identity and confirm that
    /// it has not been deleted. An ID that doesn't identify a snapshot, or a
    /// snapshot that expired at or before the current wall-clock time, returns
    /// `snapshot_not_found`, and a snapshot whose manifest is gone returns
    /// `snapshot_unavailable`.
    pub async fn list_snapshot_files_page(
        &self,
        snapshot_id: &SnapshotId,
        request: PageRequest<SnapshotFilesPageCursor>,
        options: ListSnapshotFilesOptions,
        context: &RuntimeReadContext,
    ) -> Result<SnapshotFilesPage> {
        // Rejects a mismatched or deleted namespace before any read work.
        self.ensure_live_context(context)?;
        self.namespace
            .list_snapshot_files_page(snapshot_id, request, options)
            .await
    }

    /// Lists files at the sequence that `context` reads at, ordered by inode
    /// ID, as [`Self::list_snapshot_files_page`] lists a snapshot's files.
    ///
    /// The listing requires namespace administrator access, as the change
    /// feed does. A sequence below the retention floor returns
    /// `below_retention_floor`.
    pub async fn list_files_page(
        &self,
        request: PageRequest<SnapshotFilesPageCursor>,
        options: ListSnapshotFilesOptions,
        context: &RuntimeReadContext,
    ) -> Result<SnapshotFilesPage> {
        self.ensure_administrator(context).await?;
        let view = self.namespace.load_metadata_view(context).await?;
        NamespaceStore::files_page(view.metadata_view(), request, options).await
    }
}

impl<S: ObjectStore> NamespaceStore<S> {
    /// Lists one page of the files that the snapshot `snapshot_id` keeps, as
    /// [`NamespaceEngine::list_snapshot_files_page`] describes.
    pub(crate) async fn list_snapshot_files_page(
        &self,
        snapshot_id: &SnapshotId,
        request: PageRequest<SnapshotFilesPageCursor>,
        options: ListSnapshotFilesOptions,
    ) -> Result<SnapshotFilesPage> {
        let now_ms = self.wall_clock.now_ms();
        let record = ensure_live(self.load_user_snapshot(snapshot_id).await?, now_ms)?.state;
        let SnapshotBasis { manifest, segments } =
            load_snapshot_basis(&self.store, &self.segment_cache, record).await?;

        let view = MetadataView::over_manifest_segments(&segments, manifest.head_seq);
        Self::files_page(view, request, options).await
    }

    /// Lists one page of the files that `view` reads at its sequence, as
    /// [`NamespaceEngine::list_snapshot_files_page`] and
    /// [`NamespaceEngine::list_files_page`] describe.
    async fn files_page(
        view: MetadataView<'_, '_, S>,
        request: PageRequest<SnapshotFilesPageCursor>,
        options: ListSnapshotFilesOptions,
    ) -> Result<SnapshotFilesPage> {
        let mut session = view.session();
        // Read one extra file to determine whether another page exists.
        let wanted = request.limit.limit_plus_one();
        let wave_rows = wanted.max(INODE_SCAN_WAVE_ROWS);
        let mut after = request.cursor.map(|cursor| cursor.after_inode_id);
        let mut files = Vec::with_capacity(wanted);
        while files.len() < wanted {
            let inodes = view.inodes_page(after, wave_rows).await?;
            let family_exhausted = inodes.len() < wave_rows;
            // Resume after the last scanned row, including rows filtered out below.
            match inodes.last() {
                Some(inode) => after = Some(inode.inode_id),
                None => break,
            }
            let file_inode_ids = inodes
                .iter()
                .filter(|inode| inode.inode_kind == InodeKind::File)
                .map(|inode| inode.inode_id)
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
                files.push(SnapshotFile {
                    inode_id,
                    revision_no: revision.revision_no,
                    committed_seq: revision.committed_seq,
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
            .finish_page(&mut files, |last| SnapshotFilesPageCursor {
                after_inode_id: last.inode_id,
            });
        Ok(SnapshotFilesPage {
            captured_seq: view.visible_seq(),
            files,
            next_cursor,
        })
    }
}
