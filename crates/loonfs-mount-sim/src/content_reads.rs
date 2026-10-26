//! Reads of file content through the server: the whole file, or one byte range of it with a
//! `Range` header.

use crate::entries::{inode_not_found, path_lookup_error};
use crate::error::{MountSimError, Result};
use crate::model_view::ModelView;
use crate::query::{Query, ReadPoint};
use crate::reply::Reply;
use crate::state::ServerState;
use crate::trace::ByteRange;
use bytes::Bytes;
use http::header::{HeaderMap, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE};
use http::StatusCode;
use loonfs_model::metadata::{InodeRecord, RevisionRecord};
use loonfs_model::visibility::PathLookup;
use loonfs_types::options::ReadRange;
use loonfs_types::{ContentRef, ErrorCode, InodeId, InodeKind, NamespaceId, RevisionNo};

/// Reads the `Range` header of a content read, which can ask for one range of bytes.
pub(crate) fn requested_range(headers: &HeaderMap) -> Result<Option<ReadRange>> {
    let mut values = headers.get_all(RANGE).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let invalid = || {
        MountSimError::invalid_request(
            "the `Range` header must be one `bytes=start-end` or `bytes=start-` range",
        )
        .with_param("Range")
    };
    if values.next().is_some() {
        return Err(invalid());
    }
    value
        .to_str()
        .ok()
        .and_then(ReadRange::from_range_header)
        .map(Some)
        .ok_or_else(invalid)
}

pub(crate) fn get_file_content(
    state: &ServerState,
    namespace_id: &NamespaceId,
    query: &Query,
    range: Option<ReadRange>,
) -> Result<Reply> {
    query.ensure_only(&[
        "path",
        "revision_no",
        "snapshot_id",
        "consistency",
        "max_staleness_ms",
    ])?;
    let path = query.path("path")?;
    let revision_no = query.u64("revision_no")?.map(RevisionNo);
    let at = query.read_point()?;
    if revision_no.is_some() && at != ReadPoint::Live {
        return Err(MountSimError::invalid_request(
            "`revision_no` can't be combined with `snapshot_id`",
        )
        .with_param("revision_no"));
    }
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = match view.resolve_path(&path) {
        PathLookup::Found { inode, .. } => file(inode)?,
        lookup => return Err(path_lookup_error(&lookup)),
    };
    let revision = match revision_no {
        Some(revision_no) => find_revision(&view, inode.inode_id, revision_no)?,
        None => current_revision(&view, inode),
    };
    serve(state, &revision.content_ref, range)
}

pub(crate) fn get_file_content_by_inode(
    state: &ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    query: &Query,
    range: Option<ReadRange>,
) -> Result<Reply> {
    query.ensure_only(&["snapshot_id", "consistency", "max_staleness_ms"])?;
    let at = query.read_point()?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = file(
        view.visible_inode(inode_id)
            .ok_or_else(|| inode_not_found(inode_id))?,
    )?;
    serve(state, &current_revision(&view, inode).content_ref, range)
}

/// Reads one revision by inode, which works while a deletion that hides the inode can still be
/// undone.
pub(crate) fn get_file_revision_content_by_inode(
    state: &ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    revision_no: RevisionNo,
    query: &Query,
    range: Option<ReadRange>,
) -> Result<Reply> {
    query.ensure_only(&["consistency", "max_staleness_ms"])?;
    query.read_point()?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.head_seq());
    let inode = file(
        view.retained_inode(inode_id)
            .ok_or_else(|| inode_not_found(inode_id))?,
    )?;
    let revision = find_revision(&view, inode.inode_id, revision_no)?;
    serve(state, &revision.content_ref, range)
}

pub(crate) fn file(inode: &InodeRecord) -> Result<&InodeRecord> {
    match inode.inode_kind {
        InodeKind::File => Ok(inode),
        InodeKind::Directory => Err(MountSimError::new(
            ErrorCode::PathConflict,
            "the target is a directory, which has no content",
        )),
    }
}

pub(crate) fn current_revision<'a>(
    view: &ModelView<'a>,
    inode: &InodeRecord,
) -> &'a RevisionRecord {
    view.current_revision(inode.inode_id)
        .expect("a visible file should have a revision")
}

pub(crate) fn find_revision<'a>(
    view: &ModelView<'a>,
    inode_id: InodeId,
    revision_no: RevisionNo,
) -> Result<&'a RevisionRecord> {
    view.revision(inode_id, revision_no).ok_or_else(|| {
        MountSimError::new(
            ErrorCode::RevisionNotFound,
            format!("the file has no revision {revision_no}"),
        )
    })
}

/// Returns the content of `content_ref`, or the requested range of it.
fn serve(state: &ServerState, content_ref: &ContentRef, range: Option<ReadRange>) -> Result<Reply> {
    let bytes = state.content.read(content_ref);
    let size_bytes = content_ref.size_bytes;
    let max_bytes = state.config.max_proxied_read_bytes;
    let Some(range) = range else {
        if size_bytes > max_bytes {
            return Err(too_large(size_bytes, max_bytes));
        }
        let whole = ByteRange {
            start_offset: 0,
            end_offset: size_bytes,
        };
        return Ok(
            Reply::content(StatusCode::OK, Bytes::copy_from_slice(bytes), whole)
                .with_header(ACCEPT_RANGES, "bytes"),
        );
    };
    let Some(end_offset) = range.end_offset(size_bytes) else {
        return Err(MountSimError::new(
            ErrorCode::RangeNotSatisfiable,
            format!(
                "the range starts at {}, at or past the end of the {size_bytes}-byte file",
                range.start_offset
            ),
        )
        .with_details(|details| details.size_bytes = Some(size_bytes)));
    };
    let returned = ByteRange {
        start_offset: range.start_offset,
        end_offset,
    };
    if returned.length() > max_bytes {
        return Err(too_large(returned.length(), max_bytes));
    }
    let slice = &bytes[usize_offset(range.start_offset)..usize_offset(end_offset)];
    Ok(Reply::content(
        StatusCode::PARTIAL_CONTENT,
        Bytes::copy_from_slice(slice),
        returned,
    )
    .with_header(
        CONTENT_RANGE,
        format!(
            "bytes {}-{}/{size_bytes}",
            range.start_offset,
            end_offset - 1
        ),
    )
    .with_header(CONTENT_LENGTH, returned.length().to_string())
    .with_header(ACCEPT_RANGES, "bytes"))
}

fn too_large(size_bytes: u64, max_bytes: u64) -> MountSimError {
    MountSimError::new(
        ErrorCode::ContentTooLarge,
        format!(
            "the read returns {size_bytes} bytes, more than the proxied read limit of {max_bytes}"
        ),
    )
}

pub(crate) fn usize_offset(offset: u64) -> usize {
    usize::try_from(offset).expect("a simulated file's offsets fit in usize")
}
