//! Lists files from the manifest pinned by a checkpoint.

use super::cache::MetadataSegmentCache;
use super::read_basis::{load_pinned_checkpoint_basis, PinnedCheckpointBasis};
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::metadata::MetadataView;
use loonfs_api::wire::manifest::{lookup_keys, MetadataRow, MetadataRowFamily};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{
    ChangeSeq, CheckpointId, ContentRef, InodeId, InodeKind, NamespaceId, PageRequest, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

/// Minimum number of inode rows scanned at once.
const INODE_SCAN_WAVE_ROWS: usize = 64;

/// Resumes a file listing after this inode id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointFilesPageCursor {
    /// Last inode id returned by the previous page.
    pub after_inode_id: InodeId,
}

/// One file visible in the checkpointed state, with the content the
/// checkpoint pinned for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFile {
    /// The file's inode id.
    pub inode_id: InodeId,
    /// The file revision at the checkpointed sequence.
    pub revision_no: RevisionNo,
    /// The revision's content reference.
    pub content_ref: ContentRef,
    /// The file size in bytes.
    pub size_bytes: u64,
}

/// One page of the files a checkpoint pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFilesPage {
    /// The sequence captured by the checkpoint.
    pub checkpoint_seq: ChangeSeq,
    /// Files in ascending inode-id order.
    pub files: Vec<CheckpointFile>,
    /// Resume position when more files remain.
    pub next_cursor: Option<CheckpointFilesPageCursor>,
}

/// Lists files visible in the state pinned by `checkpoint_id`.
///
/// Later WAL entries are not replayed. Directories are omitted. Missing or
/// released checkpoints return `checkpoint_unavailable`.
pub(crate) async fn list_checkpoint_files_page<S: ObjectStore + ?Sized>(
    store: &S,
    segment_cache: Option<&MetadataSegmentCache>,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    request: PageRequest<CheckpointFilesPageCursor>,
) -> Result<CheckpointFilesPage> {
    let PinnedCheckpointBasis { manifest, segments } =
        load_pinned_checkpoint_basis(store, segment_cache, namespace_id, checkpoint_id).await?;

    let checkpoint_seq = manifest.manifest_head_seq;
    let view = MetadataView::over_manifest_segments(&segments, checkpoint_seq);
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
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        let family_exhausted = rows.len() < wave_rows;
        // Resume after the last scanned row, including rows filtered out below.
        match rows.last() {
            Some((row_key, _)) => lower_bound = format!("{row_key}\0"),
            None => break,
        }
        let inode_rows = rows
            .into_iter()
            .map(|(row_key, row)| match row {
                MetadataRow::Inode {
                    inode_id,
                    inode_kind,
                    ..
                } => Ok((inode_id, inode_kind)),
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
        session.preload_visibility(&file_inode_ids).await?;
        for inode_id in file_inode_ids {
            if session.visible_inode(inode_id).await?.is_none() {
                continue;
            }
            let Some(revision) = session.latest_revision_head_of_visible(inode_id).await? else {
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
        checkpoint_seq,
        files,
        next_cursor,
    })
}
