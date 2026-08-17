//! Enumerating the files a checkpoint pins.
//!
//! A checkpoint pins one immutable manifest. This module walks that
//! manifest's inode family in ascending inode-id order and answers, for
//! every file visible in exactly that pinned state, the revision and content
//! reference the checkpoint recorded for it. Nothing here consults the live
//! head, the WAL, or any later manifest: a consumer that wants what changed
//! after the pinned sequence reads the change feed.

use super::cache::MetadataTableCache;
use super::error::ManifestLoadError;
use super::load::load_verified_manifest_tables_with_cache;
use super::record::read_checkpoint_record;
use crate::error::{CoreError, MetadataProjectionLoadError, Result};
use crate::metadata::MetadataView;
use loonfs_api::wire::control::{CheckpointRecordLifecycle, CheckpointRecordState};
use loonfs_api::wire::manifest::{lookup_keys, MetadataRow, MetadataTableFamily};
use loonfs_api::wire::sst_blocks::string_prefix_upper_bound;
use loonfs_api::{
    ChangeSeq, CheckpointId, ContentRef, InodeId, InodeKind, NamespaceId, PageRequest, RevisionNo,
};
use loonfs_objectstore::ObjectStore;

/// Inode rows read per scan wave. Directories and invisible inodes are
/// filtered out after the read, so a page of files may need several waves;
/// the floor keeps a small page size from paying one scan per row.
const INODE_SCAN_WAVE_ROWS: usize = 64;

/// Where a file enumeration resumes: strictly after this inode id.
///
/// Enumeration order is ascending inode id, which is also the durable row
/// order of the inode family, so a resume is one range bound — it never
/// re-reads a row at or before `after_inode_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointFilesPageCursor {
    /// Last inode id returned by the previous page.
    pub after_inode_id: InodeId,
}

/// One file visible in the checkpointed state, with the content the
/// checkpoint pinned for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFile {
    /// The file's inode identity, stable across renames and replacements.
    pub inode_id: InodeId,
    /// The file's current revision as of the checkpointed sequence.
    pub revision_no: RevisionNo,
    /// Immutable bytes that revision published.
    pub content_ref: ContentRef,
    /// Byte length carried by `content_ref`, lifted out for planners that
    /// size work before reading anything.
    pub size_bytes: u64,
}

/// One page of the files a checkpoint pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFilesPage {
    /// The sequence the checkpoint pins. Every file in the page is the
    /// state at this sequence, and the change feed after it is exactly what
    /// this page does not cover.
    pub checkpoint_seq: ChangeSeq,
    /// Files in ascending inode-id order.
    pub files: Vec<CheckpointFile>,
    /// Resume position when more files remain.
    pub next_cursor: Option<CheckpointFilesPageCursor>,
}

/// Reads one page of the files visible in the state `checkpoint_id` pins.
///
/// The checkpoint's own manifest is the only state read: no WAL tail is
/// replayed over it, and no later manifest is consulted, so the answer is
/// the namespace exactly as of `checkpoint_seq`. Visibility is the same rule
/// every read applies — the inode exists, and no subtree tombstone covers it
/// or any of its ancestors — evaluated against that manifest. Directories
/// are not returned.
///
/// A record that is missing or released answers `checkpoint_unavailable`,
/// and so does a pinned manifest that is already gone. There is no fallback
/// to current state: a consumer that lost its checkpoint takes a new one and
/// starts over.
///
/// Release is the whole authority here — no clock. An active record whose
/// expiry has passed still pins its basis and still serves: garbage
/// collection is what turns a passed expiry into a release, and until it
/// does, the record is a root, so answering from it is answering from state
/// that is provably still there.
pub(crate) async fn list_checkpoint_files_page<S: ObjectStore + ?Sized>(
    store: &S,
    table_cache: Option<&MetadataTableCache>,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
    request: PageRequest<CheckpointFilesPageCursor>,
) -> Result<CheckpointFilesPage> {
    let record = read_pinning_checkpoint_record(store, namespace_id, checkpoint_id).await?;
    let tables = load_verified_manifest_tables_with_cache(
        store,
        table_cache,
        namespace_id,
        &record.manifest_object_id,
    )
    .await
    .map_err(|error| match error {
        ManifestLoadError::MissingManifest { object_key } => CoreError::CheckpointUnavailable(
            format!("checkpoint `{checkpoint_id}` pins manifest `{object_key}`, which is gone"),
        ),
        other => CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(other)),
    })?;
    let manifest = tables.manifest();
    if manifest.payload_checksum != record.manifest_payload_checksum
        || manifest.payload.head_seq != record.manifest_head_seq
    {
        return Err(CoreError::NamespaceCorrupt(format!(
            "checkpoint `{checkpoint_id}` basis does not match its manifest"
        )));
    }

    let checkpoint_seq = record.manifest_head_seq;
    let view = MetadataView::over_manifest_tables(&tables, checkpoint_seq);
    let mut session = view.session();

    // One row past the page proves whether another file exists, so a cursor
    // is handed out only when it will answer something.
    let wanted = request.limit.limit_plus_one();
    let wave_rows = wanted.max(INODE_SCAN_WAVE_ROWS);
    let mut lower_bound = match request.cursor {
        Some(cursor) => lookup_keys::inode_key_after(cursor.after_inode_id),
        None => lookup_keys::INODE_ROW_PREFIX.to_owned(),
    };
    let upper_bound = string_prefix_upper_bound(lookup_keys::INODE_ROW_PREFIX);
    let mut files = Vec::with_capacity(wanted);
    while files.len() < wanted {
        let rows = tables
            .scan_range_page_with_keys(
                MetadataTableFamily::Inodes,
                &lower_bound,
                upper_bound.as_deref(),
                wave_rows,
            )
            .await
            .map_err(|error| {
                CoreError::MetadataProjection(MetadataProjectionLoadError::ManifestLoad(error))
            })?;
        let family_exhausted = rows.len() < wave_rows;
        // Advance before consuming the wave: the next wave starts strictly
        // after the last row this one read, whatever the filters keep.
        match rows.last() {
            Some((row_key, _)) => lower_bound = format!("{row_key}\0"),
            None => break,
        }
        for (row_key, row) in rows {
            let MetadataRow::Inode {
                inode_id,
                inode_kind,
                ..
            } = row
            else {
                return Err(CoreError::NamespaceCorrupt(format!(
                    "inodes family returned a non-inode row at `{row_key}`"
                )));
            };
            if inode_kind != InodeKind::File {
                continue;
            }
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

/// Loads the record and refuses the one lifecycle that no longer pins its
/// basis, so a caller never reads state garbage collection may already be
/// reclaiming.
async fn read_pinning_checkpoint_record<S: ObjectStore + ?Sized>(
    store: &S,
    namespace_id: &NamespaceId,
    checkpoint_id: &CheckpointId,
) -> Result<CheckpointRecordState> {
    let Some(record) = read_checkpoint_record(store, namespace_id, checkpoint_id)
        .await?
        .map(|loaded| loaded.state)
    else {
        return Err(CoreError::CheckpointUnavailable(format!(
            "checkpoint `{checkpoint_id}` does not exist in namespace `{namespace_id}`"
        )));
    };
    if record.state != (CheckpointRecordLifecycle::Active {}) {
        return Err(CoreError::CheckpointUnavailable(format!(
            "checkpoint `{checkpoint_id}` is `{}` and no longer pins its basis",
            record.state
        )));
    }
    Ok(record)
}
