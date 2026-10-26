//! Directory listings: `GET /filesystem/entries` and `GET /inodes/{inode_id}/children`, of one
//! directory's children or, with `recursive=true`, of its whole subtree in depth-first order.

use crate::entries::{inode_not_found, path_lookup_error};
use crate::error::{MountSimError, Result};
use crate::model_view::ModelView;
use crate::query::{Query, ReadPoint};
use crate::reply::Reply;
use crate::state::ServerState;
use loonfs_model::metadata::InodeRecord;
use loonfs_model::visibility::{ListedEntry, PathLookup};
use loonfs_types::api::v0::{ListInodeChildrenResponse, ListPathEntriesResponse, PathEntry};
use loonfs_types::{
    decode_cursor, encode_cursor, AbsolutePath, ChangeSeq, DirectoryPageCursor, DirectoryTreeFrame,
    DirectoryTreePageCursor, ErrorCode, InodeId, InodeKind, NameKey, NamespaceId, PinId,
};

/// How many levels below the listed directory a recursive listing descends (api.md, "`GET
/// /filesystem/entries`"). A directory at this depth is returned without the entries below it.
const MAX_RECURSIVE_LISTING_DEPTH: usize = 128;

const LISTING_PARAMS: &[&str] = &[
    "limit",
    "cursor",
    "include_attributes",
    "recursive",
    "consistency",
    "max_staleness_ms",
    "snapshot_id",
];

/// The parameters that every page of one listing shares.
struct ListingRequest {
    limit: usize,
    cursor: Option<String>,
    include_attributes: bool,
    recursive: bool,
    pin_id: Option<PinId>,
}

impl ListingRequest {
    fn parse(query: &Query, extra: &[&str]) -> Result<(Self, ReadPoint)> {
        let allowed: Vec<&str> = LISTING_PARAMS.iter().chain(extra).copied().collect();
        query.ensure_only(&allowed)?;
        let at = query.read_point()?;
        let pin_id = match &at {
            ReadPoint::Snapshot(snapshot_id) => Some(snapshot_id.clone()),
            ReadPoint::Live => None,
        };
        Ok((
            Self {
                limit: query.limit()?,
                cursor: query.get("cursor").map(str::to_owned),
                include_attributes: query.bool("include_attributes", false)?,
                recursive: query.bool("recursive", false)?,
                pin_id,
            },
            at,
        ))
    }
}

/// One page of entries, and the cursor for the next page.
struct Page {
    entries: Vec<PathEntry>,
    next_cursor: Option<String>,
}

pub(crate) fn list_path_entries(
    state: &ServerState,
    namespace_id: &NamespaceId,
    query: &Query,
) -> Result<Reply> {
    let (request, at) = ListingRequest::parse(query, &["path"])?;
    let path = query.path("path")?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = match view.resolve_path(&path) {
        PathLookup::Found { inode, .. } => inode,
        lookup => return Err(path_lookup_error(&lookup)),
    };
    let listed_path = view.inode_path(inode.inode_id);
    let page = match inode.inode_kind {
        // A file target returns the file's own entry (api.md, "`GET /filesystem/entries`").
        InodeKind::File => Page {
            entries: vec![view.path_entry(inode, listed_path.clone(), request.include_attributes)],
            next_cursor: None,
        },
        InodeKind::Directory => list_directory(&view, inode, &listed_path, &request)?,
    };
    Ok(Reply::json(&ListPathEntriesResponse {
        namespace_id: namespace_id.clone(),
        path: listed_path,
        head_seq: view.seq,
        entries: page.entries,
        next_cursor: page.next_cursor,
    }))
}

pub(crate) fn list_inode_children(
    state: &ServerState,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
    query: &Query,
) -> Result<Reply> {
    let (request, at) = ListingRequest::parse(query, &[])?;
    let namespace = state.namespace(namespace_id)?;
    let view = namespace.view(namespace.read_seq(&at, state.now_ms())?);
    let inode = view
        .visible_inode(inode_id)
        .ok_or_else(|| inode_not_found(inode_id))?;
    if inode.inode_kind != InodeKind::Directory {
        return Err(MountSimError::new(
            ErrorCode::PathConflict,
            "the inode is a file, which has no children",
        ));
    }
    let page = list_directory(&view, inode, &view.inode_path(inode_id), &request)?;
    Ok(Reply::json(&ListInodeChildrenResponse {
        namespace_id: namespace_id.clone(),
        parent_inode_id: inode_id,
        head_seq: view.seq,
        entries: page.entries,
        next_cursor: page.next_cursor,
    }))
}

fn list_directory(
    view: &ModelView<'_>,
    directory: &InodeRecord,
    path: &AbsolutePath,
    request: &ListingRequest,
) -> Result<Page> {
    if request.recursive {
        list_tree(view, directory, path, request)
    } else {
        list_children(view, directory, path, request)
    }
}

fn list_children(
    view: &ModelView<'_>,
    directory: &InodeRecord,
    path: &AbsolutePath,
    request: &ListingRequest,
) -> Result<Page> {
    let after = match &request.cursor {
        None => None,
        Some(cursor) => {
            let cursor: DirectoryPageCursor = decode_cursor(cursor).map_err(invalid_cursor)?;
            check_cursor(cursor.head_seq, cursor.pin_id.as_ref(), view.seq, request)?;
            if cursor.directory_inode_id != directory.inode_id {
                return Err(invalid_cursor("the cursor belongs to another directory"));
            }
            Some(cursor.last_name_key)
        }
    };
    let mut children: Vec<ListedEntry<'_>> = view
        .list_directory(directory.inode_id)
        .into_iter()
        .filter(|entry| {
            after
                .as_ref()
                .is_none_or(|after| entry.binding.name_key > *after)
        })
        .collect();
    let more = children.len() > request.limit;
    children.truncate(request.limit);
    let next_cursor = match (more, children.last()) {
        (true, Some(last)) => Some(
            encode_cursor(&DirectoryPageCursor {
                head_seq: view.seq,
                pin_id: request.pin_id.clone(),
                directory_inode_id: directory.inode_id,
                last_name_key: last.binding.name_key.clone(),
            })
            .expect("a cursor should encode"),
        ),
        _ => None,
    };
    let entries = children
        .iter()
        .map(|entry| {
            view.path_entry(
                entry.child,
                path.join(entry.display_name),
                request.include_attributes,
            )
        })
        .collect();
    Ok(Page {
        entries,
        next_cursor,
    })
}

/// One directory on the path from the listed directory to the walk's position.
struct Frame<'a> {
    directory_inode_id: InodeId,
    path: AbsolutePath,
    /// The listed directory is at depth 0, and its children are at depth 1.
    depth: usize,
    children: Vec<ListedEntry<'a>>,
    next: usize,
    last_name_key: Option<NameKey>,
}

impl<'a> Frame<'a> {
    fn enter(
        view: &ModelView<'a>,
        directory_inode_id: InodeId,
        path: AbsolutePath,
        depth: usize,
    ) -> Self {
        Self {
            directory_inode_id,
            path,
            depth,
            children: view.list_directory(directory_inode_id),
            next: 0,
            last_name_key: None,
        }
    }

    /// Moves the frame's position after `name_key`.
    fn skip_through(&mut self, name_key: &NameKey) {
        self.next = self
            .children
            .iter()
            .position(|child| child.binding.name_key > *name_key)
            .unwrap_or(self.children.len());
        self.last_name_key = Some(name_key.clone());
    }
}

fn list_tree(
    view: &ModelView<'_>,
    directory: &InodeRecord,
    path: &AbsolutePath,
    request: &ListingRequest,
) -> Result<Page> {
    let mut frames = match &request.cursor {
        None => vec![Frame::enter(view, directory.inode_id, path.clone(), 0)],
        Some(cursor) => {
            let cursor: DirectoryTreePageCursor = decode_cursor(cursor).map_err(invalid_cursor)?;
            check_cursor(cursor.head_seq, cursor.pin_id.as_ref(), view.seq, request)?;
            resume_frames(view, directory, path, &cursor.frames)?
        }
    };
    let mut entries = Vec::new();
    while entries.len() < request.limit {
        let Some((entry, path)) = next_tree_entry(view, &mut frames) else {
            break;
        };
        entries.push(view.path_entry(entry.child, path, request.include_attributes));
    }
    let position: Vec<DirectoryTreeFrame> = frames
        .iter()
        .map(|frame| DirectoryTreeFrame {
            directory_inode_id: frame.directory_inode_id,
            last_name_key: frame.last_name_key.clone(),
        })
        .collect();
    let next_cursor = next_tree_entry(view, &mut frames).map(|_| {
        encode_cursor(&DirectoryTreePageCursor {
            head_seq: view.seq,
            pin_id: request.pin_id.clone(),
            frames: position,
        })
        .expect("a cursor should encode")
    });
    Ok(Page {
        entries,
        next_cursor,
    })
}

/// Returns the next entry of a depth-first walk and its path, and enters the entry when it's a
/// directory that isn't at the depth limit.
fn next_tree_entry<'a>(
    view: &ModelView<'a>,
    frames: &mut Vec<Frame<'a>>,
) -> Option<(ListedEntry<'a>, AbsolutePath)> {
    loop {
        let top = frames.last_mut()?;
        let Some(entry) = top.children.get(top.next).copied() else {
            frames.pop();
            continue;
        };
        top.next += 1;
        top.last_name_key = Some(entry.binding.name_key.clone());
        let path = top.path.join(entry.display_name);
        let depth = top.depth + 1;
        if entry.child.inode_kind == InodeKind::Directory && depth < MAX_RECURSIVE_LISTING_DEPTH {
            frames.push(Frame::enter(
                view,
                entry.child.inode_id,
                path.clone(),
                depth,
            ));
        }
        return Some((entry, path));
    }
}

/// Rebuilds the walk's frames from a cursor. A directory that's no longer where the cursor
/// recorded it ends the rebuild, so the walk continues in its parent after its name.
fn resume_frames<'a>(
    view: &ModelView<'a>,
    directory: &InodeRecord,
    path: &AbsolutePath,
    recorded: &[DirectoryTreeFrame],
) -> Result<Vec<Frame<'a>>> {
    let Some((first, rest)) = recorded.split_first() else {
        return Err(invalid_cursor("the cursor records no position"));
    };
    if first.directory_inode_id != directory.inode_id {
        return Err(invalid_cursor("the cursor belongs to another directory"));
    }
    if recorded.len() > MAX_RECURSIVE_LISTING_DEPTH {
        return Err(invalid_cursor(
            "the cursor records more levels than a listing descends",
        ));
    }
    if recorded[..recorded.len() - 1]
        .iter()
        .any(|frame| frame.last_name_key.is_none())
    {
        return Err(invalid_cursor(
            "only the cursor's last frame can omit its name key",
        ));
    }
    let mut frames = vec![Frame::enter(view, directory.inode_id, path.clone(), 0)];
    if let Some(key) = &first.last_name_key {
        frames[0].skip_through(key);
    }
    for recorded_frame in rest {
        let parent = frames.last().expect("the walk has the listed directory");
        let parent_key = parent
            .last_name_key
            .as_ref()
            .expect("a frame with a child frame records its name key");
        let child = view
            .list_directory(parent.directory_inode_id)
            .into_iter()
            .find(|entry| entry.binding.name_key == *parent_key)
            .filter(|entry| {
                entry.child.inode_id == recorded_frame.directory_inode_id
                    && entry.child.inode_kind == InodeKind::Directory
            });
        let Some(child) = child else {
            break;
        };
        let mut frame = Frame::enter(
            view,
            child.child.inode_id,
            parent.path.join(child.display_name),
            parent.depth + 1,
        );
        if let Some(key) = &recorded_frame.last_name_key {
            frame.skip_through(key);
        }
        frames.push(frame);
    }
    Ok(frames)
}

/// Rejects a cursor from a newer head or from another read point (api.md, "`GET
/// /filesystem/entries`").
fn check_cursor(
    cursor_head_seq: ChangeSeq,
    cursor_pin_id: Option<&PinId>,
    read_seq: ChangeSeq,
    request: &ListingRequest,
) -> Result<()> {
    if cursor_pin_id != request.pin_id.as_ref() {
        return Err(invalid_cursor(
            "the cursor belongs to another snapshot or to a live read",
        ));
    }
    if cursor_head_seq > read_seq {
        return Err(invalid_cursor("the cursor was issued at a newer head"));
    }
    Ok(())
}

fn invalid_cursor(reason: impl std::fmt::Display) -> MountSimError {
    MountSimError::invalid_request(format!("invalid `cursor`: {reason}")).with_param("cursor")
}
